###############################################################################
# Maine Boil Water Notices (issue #38)
# Migrated from 1_downloaders/daily/me_worker/me_daily.R
#
# Maine's CDC publishes its drinking water safety alerts as a set of HTML tables,
# one per order type (boil water, do not drink, and occasionally do not use).
# Only advisories currently in effect are listed, so this is an active feed: a
# record disappearing implies it was lifted, and we date that closure to the day
# we noticed it ("Assumed"). Unlike Alaska, the order type is not a single
# constant, it comes from which table the row was scraped out of.
#
# Shared BWN helpers live in functions/bwn_helpers.R.
###############################################################################

# The alert tables in the order Maine publishes them. The third is only present
# when a do-not-use order is active, which is why the count is not assumed.
me_bwn_table_types <- c("Boil Water Order", "Do Not Drink Order",
                        "Do Not Use Order")

#' Pull Maine BWN data, then run the ME BWN clean pipeline
#' @param config Main config
#' @param dataset_id "raw_me_bwn"
run_me_bwn_pipeline <- function(config, dataset_id) {
  update_raw_me_bwn(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Fetch, reconcile against the previous run, validate, and save raw ME BWN data
#' @param config Main config
#' @param dataset_id "raw_me_bwn"
#' @return Reconciled raw BWN data frame
update_raw_me_bwn <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link

  message("Scraping the Maine CDC drinking water safety alerts page...")
  tables <- rvest::read_html(source_url) %>%
    rvest::html_nodes("table") %>%
    rvest::html_table(fill = TRUE)

  # The boil-water and do-not-drink tables are always published. Failing here
  # with the count is far easier to diagnose than a subscript error if Maine
  # restructures the page.
  if (length(tables) < 2) {
    stop(sprintf(
      "ME BWN: expected at least 2 alert tables on %s, found %d. The page layout has probably changed.",
      source_url, length(tables)), call. = FALSE)
  }

  # Tag each table with the order type it represents, then stack them. Maine
  # publishes boil water first and do not drink second; the legacy worker bound
  # them in the order do-not-drink, boil-water, do-not-use, which is preserved
  # here so a first run in an empty bucket reproduces the legacy row order.
  # Legacy took the do-not-use table only when the page had exactly three
  # tables, dropping it otherwise, so an unexpected fourth table is ignored
  # rather than being mislabelled as a do-not-use order.
  n_tables <- if (length(tables) == length(me_bwn_table_types))
    length(me_bwn_table_types) else 2L
  parsed <- lapply(seq_len(n_tables), function(i) {
    tables[[i]] %>%
      as.data.frame() %>%
      janitor::clean_names() %>%
      dplyr::mutate(type = me_bwn_table_types[i]) %>%
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  })
  names(parsed) <- me_bwn_table_types[seq_len(n_tables)]
  me_bwn <- dplyr::bind_rows(parsed[["Do Not Drink Order"]],
                             parsed[["Boil Water Order"]],
                             parsed[["Do Not Use Order"]])

  message("Filtering to community water systems...")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)

  me_bwn_tidy <- me_bwn %>%
    # a handful of systems are listed as "community" on the state page but do
    # not match a pwsid in the EPA SABs dataset, and their names do not match
    # either, so there is nothing to join them on
    filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
    # keep Maine's own rendering ("Apr 11, 2026") and derive the ISO date from it
    rename(date_issued_me = date_issued) %>%
    mutate(last_epic_run_date = as.character(Sys.Date()),
           date_issued = as.character(lubridate::mdy(date_issued_me)))

  message("Reading the previous run for reconciliation...")
  me_bwn_old <- read_prior_bwn(link)

  me_bwn_reconciled <- reconcile_me_bwn(me_bwn_tidy, me_bwn_old)

  message("Validating raw_me_bwn...")
  validate_raw_bwn(config, me_bwn_reconciled, me_bwn_old, dataset_id,
                   label = "ME Boil Water Notice Validation")

  message(sprintf("Writing raw_me_bwn to S3 to %s...", link))
  s3_write_csv(me_bwn_reconciled, link)

  return(me_bwn_reconciled)
}

#' Reconcile a fresh Maine pull against the previous run.
#' Maine lists only advisories currently in effect, so this uses the shared
#' active-feed reconcile. An advisory is identified by system, issue date and
#' reason: one system can hold concurrent orders issued the same day for
#' different reasons.
#' @param me_bwn_tidy Fresh pull, tidied
#' @param me_bwn_old Previous raw pull, or NULL on a first run
#' @return Combined data frame
reconcile_me_bwn <- function(me_bwn_tidy, me_bwn_old) {
  reconcile_bwn_active_feed(
    me_bwn_tidy, me_bwn_old,
    key_cols = c("pwsid", "date_issued", "reason")
  )
}

#' Standardize ME BWN into the shared cross-state BWN schema
#' @param config Main config
#' @param dataset_id "clean_me_bwn"
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_me_bwn_pipeline <- function(config, dataset_id = "clean_me_bwn",
                                      bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link

  if (is.null(bwn_raw)) {
    message("Downloading raw ME BWN from S3...")
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  me_bwn_clean <- bwn_raw %>%
    mutate(date_issued = as.Date(date_issued, tryFormats = c("%Y-%m-%d")),
           date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")),
           last_epic_run_date = as.Date(last_epic_run_date,
                                        tryFormats = c("%Y-%m-%d")),
           # Maine publishes only active advisories, so every closure we record
           # is inferred rather than reported. See the header note.
           epic_date_lifted_flag = "Assumed",
           state = "Maine") %>%
    finalize_bwn_clean()

  message("Validating clean_me_bwn...")
  validate_clean_bwn(config, me_bwn_clean, dataset_id,
                     label = "ME BWN Clean Validation")

  message(sprintf("Writing clean_me_bwn to S3 to %s...", link))
  s3_write_csv(me_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(me_bwn_clean)
}
