###############################################################################
# Arkansas Boil Water Notices (issue #38)
# Migrated from 1_downloaders/daily/ar_worker/ar_daily.R
#
# Arkansas Health publishes a boil water order table that carries its own
# rescind dates, so this is a rolling window rather than an active feed: an
# advisory can come back with a lift date filled in, and records that age off
# the page must be retained from our own history. Lift dates are real, so
# epic_date_lifted_flag is "Reported".
#
# Like Washington, the source publishes no system id, so pwsid is recovered by
# joining to the EPA SABs name list and the advisory is keyed on the system
# NAME. The pwsid completeness checks therefore run at warning severity.
#
# Shared BWN helpers live in functions/bwn_helpers.R.
###############################################################################

# Texarkana straddles the state line and appears in the SABs list under both
# states. Dropping the Texas id keeps the name join from matching twice.
ar_excluded_pwsid <- "TX0190004"

#' Pull Arkansas BWN data, then run the AR BWN clean pipeline
#' @param config Main config
#' @param dataset_id "raw_ar_bwn"
run_ar_bwn_pipeline <- function(config, dataset_id) {
  update_raw_ar_bwn(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Fetch, reconcile against the previous run, validate, and save raw AR BWN data
#' @param config Main config
#' @param dataset_id "raw_ar_bwn"
#' @return Reconciled raw BWN data frame
update_raw_ar_bwn <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link

  message("Scraping the Arkansas Health boil water order page...")
  tables <- rvest::read_html(source_url) %>%
    rvest::html_nodes("table") %>%
    rvest::html_table(fill = TRUE)

  if (length(tables) < 1) {
    stop(sprintf(
      "AR BWN: no boil water order table found on %s. The page layout has probably changed.",
      source_url), call. = FALSE)
  }

  ar_bwn <- tables[[1]] %>%
    janitor::clean_names() %>%
    # the legacy worker had no guard here, so a header-only page would be filed
    # as a new advisory with no system and no dates
    drop_empty_scraped_rows(c("system", "county")) %>%
    # normalized here because this doubles as the join key to the SABs name list
    mutate(system = str_squish(str_to_title(system)))

  message("Recovering pwsids by system name from the EPA SABs name list...")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)
  ar_pwsids <- epa_sabs_pwsids %>%
    filter(pwsid != ar_excluded_pwsid) %>%
    filter(grepl("AR", states_intersect))

  ar_bwn_tidy <- merge(ar_bwn, ar_pwsids, by.x = "system",
                       by.y = "pws_name", all.x = TRUE) %>%
    mutate(last_epic_run_date = as.character(Sys.Date()))

  matched <- sum(!is.na(ar_bwn_tidy$pwsid))
  message(sprintf("Matched %d of %d scraped advisories to a pwsid.",
                  matched, nrow(ar_bwn_tidy)))

  message("Reading the previous run for reconciliation...")
  ar_bwn_old <- read_prior_bwn(link)

  ar_bwn_reconciled <- reconcile_ar_bwn(ar_bwn_tidy, ar_bwn_old)

  message("Validating raw_ar_bwn...")
  validate_raw_bwn(config, ar_bwn_reconciled, ar_bwn_old, dataset_id,
                   label = "AR Boil Water Notice Validation",
                   pwsid_severity = "warning")

  message(sprintf("Writing raw_ar_bwn to S3 to %s...", link))
  s3_write_csv(ar_bwn_reconciled, link)

  return(ar_bwn_reconciled)
}

#' Reconcile a fresh Arkansas pull against the previous run.
#' Keys on two ids because the source is a rolling window:
#'   full_id   = system + date_issued + date_lifted  (a specific version)
#'   update_id = system + date_issued                (the advisory itself)
#' A row whose update_id matches but whose full_id does not is the same advisory
#' with a lift date now reported, so the fresh version replaces the stored one.
#' @param ar_bwn_tidy Fresh pull, tidied
#' @param ar_bwn_old Previous raw pull, or NULL on a first run
#' @return Combined data frame
reconcile_ar_bwn <- function(ar_bwn_tidy, ar_bwn_old) {
  reconcile_bwn_rolling_window(
    ar_bwn_tidy, ar_bwn_old,
    key_cols        = c("system", "date_issued", "date_lifted"),
    update_key_cols = c("system", "date_issued")
  )
}

#' Standardize AR BWN into the shared cross-state BWN schema
#' @param config Main config
#' @param dataset_id "clean_ar_bwn"
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_ar_bwn_pipeline <- function(config, dataset_id = "clean_ar_bwn",
                                      bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link

  if (is.null(bwn_raw)) {
    message("Downloading raw AR BWN from S3...")
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  ar_bwn_clean <- bwn_raw %>%
    # the source stamps these with a time as well ("8/3/2026 4:27:45 PM");
    # strptime ignores the unmatched tail, leaving the date
    mutate(date_issued = as.Date(date_issued, tryFormats = c("%m/%d/%Y")),
           date_lifted = as.Date(date_lifted, tryFormats = c("%m/%d/%Y")),
           last_epic_run_date = as.Date(last_epic_run_date,
                                        tryFormats = c("%Y-%m-%d")),
           # the page carries no reason or advisory-type column
           type = "No Information",
           # Arkansas reports real rescind dates
           epic_date_lifted_flag = "Reported",
           state = "Arkansas") %>%
    finalize_bwn_clean()

  message("Validating clean_ar_bwn...")
  validate_clean_bwn(config, ar_bwn_clean, dataset_id,
                     label = "AR BWN Clean Validation",
                     pwsid_severity = "warning")

  message(sprintf("Writing clean_ar_bwn to S3 to %s...", link))
  s3_write_csv(ar_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(ar_bwn_clean)
}
