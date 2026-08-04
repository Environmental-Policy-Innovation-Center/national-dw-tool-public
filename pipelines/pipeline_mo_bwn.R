###############################################################################
# Missouri Boil Water Notices (issue #38)
# Migrated from 1_downloaders/daily/mo_worker/mo_daily.R
#
# Missouri publishes boil orders through a Socrata dataset (DNR WPP Boil Order
# Report). Like Alaska, the feed lists only advisories currently in effect, so a
# record disappearing implies it was lifted and we date that closure to the day
# we noticed it ("Assumed"). Unlike Alaska, Missouri's stored history can already
# contain "Reported" flags from records supplied directly by the state, so the
# clean step only fills the flag in where it is missing rather than overwriting.
#
# Shared BWN helpers live in functions/bwn_helpers.R.
###############################################################################

#' Pull Missouri BWN data, then run the MO BWN clean pipeline
#' @param config Main config
#' @param dataset_id "raw_mo_bwn"
run_mo_bwn_pipeline <- function(config, dataset_id) {
  mo_bwn_raw <- update_raw_mo_bwn(config, dataset_id)

  message("Running MO BWN clean pipeline...")
  run_clean_mo_bwn_pipeline(config, "clean_mo_bwn", bwn_raw = mo_bwn_raw)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Fetch, reconcile against the previous run, validate, and save raw MO BWN data
#' @param config Main config
#' @param dataset_id "raw_mo_bwn"
#' @return Reconciled raw BWN data frame
update_raw_mo_bwn <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link

  message("Pulling Missouri boil order data from the Socrata endpoint...")
  response <- httr::GET(source_url)
  if (httr::http_error(response)) {
    stop(sprintf("MO BWN request failed with HTTP %s", httr::status_code(response)),
         call. = FALSE)
  }
  mo_bwn <- jsonlite::fromJSON(httr::content(response, "text",
                                             encoding = "UTF-8")) %>%
    as.data.frame() %>%
    janitor::clean_names() %>%
    select(issue_date:geocoded_column) %>%
    mutate(issue_date = as.Date(issue_date, tryFormats = c("%Y-%m-%d")))

  message("Filtering to community water systems...")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)

  mo_bwn_tidy <- mo_bwn %>%
    # Socrata returns the location as a nested data frame, so flatten it out and
    # drop the nested column (it cannot be written to CSV as-is).
    mutate(geocode_lat = geocoded_column$latitude,
           geocode_long = geocoded_column$longitude) %>%
    select(-geocoded_column) %>%
    # Missouri's feed carries a large share of non-community systems
    filter(pws_id %in% epa_sabs_pwsids$pwsid) %>%
    mutate(last_epic_run_date = as.character(Sys.Date()))

  message("Reading the previous run for reconciliation...")
  mo_bwn_old <- read_prior_bwn(link)

  mo_bwn_reconciled <- reconcile_mo_bwn(mo_bwn_tidy, mo_bwn_old)

  message("Validating raw_mo_bwn...")
  validate_raw_bwn(config, mo_bwn_reconciled, mo_bwn_old, dataset_id,
                   label = "MO Boil Water Notice Validation",
                   pwsid_col = "pws_id")

  message(sprintf("Writing raw_mo_bwn to S3 to %s...", link))
  s3_write_csv(mo_bwn_reconciled, link)

  return(mo_bwn_reconciled)
}

#' Reconcile a fresh Missouri pull against the previous run.
#' Missouri's feed lists only active boil orders, so this uses the shared
#' active-feed reconcile. An advisory is identified by its issue date, system id
#' and contaminant, since one system can have concurrent orders for different
#' contaminants.
#' @param mo_bwn_tidy Fresh pull, tidied
#' @param mo_bwn_old Previous raw pull, or NULL on a first run
#' @return Combined data frame
reconcile_mo_bwn <- function(mo_bwn_tidy, mo_bwn_old) {
  reconcile_bwn_active_feed(
    mo_bwn_tidy, mo_bwn_old,
    key_cols = c("issue_date", "pws_id", "contaminant_of_concern")
  )
}

#' Standardize MO BWN into the shared cross-state BWN schema
#' @param config Main config
#' @param dataset_id "clean_mo_bwn"
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_mo_bwn_pipeline <- function(config, dataset_id = "clean_mo_bwn",
                                      bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link

  if (is.null(bwn_raw)) {
    message("Downloading raw MO BWN from S3...")
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  mo_bwn_clean <- bwn_raw %>%
    rename(date_issued = issue_date,
           pwsid = pws_id) %>%
    mutate(
      # Records supplied directly by the state can already carry a "Reported"
      # flag, so only fill in the ones we inferred ourselves.
      epic_date_lifted_flag = coalesce(epic_date_lifted_flag, "Assumed"),
      date_issued = as.Date(date_issued, tryFormats = c("%Y-%m-%d")),
      date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")),
      last_epic_run_date = as.Date(last_epic_run_date,
                                   tryFormats = c("%Y-%m-%d")),
      type = "Assumed - Boil Order",
      state = "Missouri"
    ) %>%
    finalize_bwn_clean()

  message("Validating clean_mo_bwn...")
  validate_clean_bwn(config, mo_bwn_clean, dataset_id,
                     label = "MO BWN Clean Validation")

  message(sprintf("Writing clean_mo_bwn to S3 to %s...", link))
  s3_write_csv(mo_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(mo_bwn_clean)
}
