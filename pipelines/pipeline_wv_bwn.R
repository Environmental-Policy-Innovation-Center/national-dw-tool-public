###############################################################################
# West Virginia Boil Water Notices (issue #38)
# Migrated from 1_downloaders/quarterly/wv_worker/wv_quarterly.R
#
# West Virginia's portal exposes a JSON endpoint (found by inspecting the page)
# that returns a rolling window of roughly the past year. Two consequences drive
# the reconcile logic:
#   1. Advisories that age out of the window must be retained from our own
#      records, or history would silently disappear.
#   2. An advisory already in our records can come back with a lift date filled
#      in, so we need to detect an *update* to an existing row, not just new rows.
# That is why this state keys on two ids rather than one. Unlike Alaska, WV
# publishes real lift dates, so epic_date_lifted_flag is "Reported".
#
# Shared BWN helpers live in functions/bwn_helpers.R.
###############################################################################

#' Pull West Virginia BWN data, then run the WV BWN clean pipeline
#' @param config Main config
#' @param dataset_id "raw_wv_bwn"
run_wv_bwn_pipeline <- function(config, dataset_id) {
  wv_bwn_raw <- update_raw_wv_bwn(config, dataset_id)

  message("Running WV BWN clean pipeline...")
  run_clean_wv_bwn_pipeline(config, "clean_wv_bwn", bwn_raw = wv_bwn_raw)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Fetch, reconcile against the previous run, validate, and save raw WV BWN data
#' @param config Main config
#' @param dataset_id "raw_wv_bwn"
#' @return Reconciled raw BWN data frame
update_raw_wv_bwn <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link

  message("Pulling West Virginia BWN data from the WVDHHR endpoint...")
  response <- httr::GET(source_url)
  if (httr::http_error(response)) {
    stop(sprintf("WV BWN request failed with HTTP %s", httr::status_code(response)),
         call. = FALSE)
  }
  wv_bwn <- jsonlite::fromJSON(httr::content(response, "text",
                                             encoding = "UTF-8"))

  message("Filtering to community water systems...")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)

  wv_bwn_tidy <- wv_bwn %>%
    janitor::clean_names() %>%
    filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
    mutate(last_epic_run_date = as.character(Sys.Date()))

  message("Reading the previous run for reconciliation...")
  wv_bwn_old <- read_prior_bwn(link)

  wv_bwn_reconciled <- reconcile_wv_bwn(wv_bwn_tidy, wv_bwn_old)

  message("Validating raw_wv_bwn...")
  validate_raw_bwn(config, wv_bwn_reconciled, wv_bwn_old, dataset_id,
                   label = "WV Boil Water Notice Validation")

  message(sprintf("Writing raw_wv_bwn to S3 to %s...", link))
  s3_write_csv(wv_bwn_reconciled, link)

  return(wv_bwn_reconciled)
}

#' Reconcile a fresh West Virginia pull against the previous run.
#' Keys on two ids because the source is a rolling window:
#'   full_id   = pwsid + date_issued + date_lifted + details  (fully identical row)
#'   update_id = pwsid + date_issued + details                (same advisory)
#' A row whose update_id matches but whose full_id does not is the *same*
#' advisory with changed details (typically a lift date now reported), so the
#' fresh version replaces the stored one. Rows matching neither are new. Stored
#' rows that are not being updated are carried forward untouched, which is what
#' retains advisories that have aged out of the window.
#' @param wv_bwn_tidy Fresh pull, tidied
#' @param wv_bwn_old Previous raw pull, or NULL on a first run
#' @return Combined data frame
reconcile_wv_bwn <- function(wv_bwn_tidy, wv_bwn_old) {
  reconcile_bwn_rolling_window(
    wv_bwn_tidy, wv_bwn_old,
    key_cols        = c("pwsid", "date_issued", "date_lifted", "details"),
    update_key_cols = c("pwsid", "date_issued", "details")
  )
}

#' Standardize WV BWN into the shared cross-state BWN schema
#' @param config Main config
#' @param dataset_id "clean_wv_bwn"
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_wv_bwn_pipeline <- function(config, dataset_id = "clean_wv_bwn",
                                      bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link

  if (is.null(bwn_raw)) {
    message("Downloading raw WV BWN from S3...")
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  wv_bwn_clean <- bwn_raw %>%
    # keep the source values under state-suffixed names, then derive the
    # standardized date columns from them
    rename(date_issued_wv = date_issued,
           date_lifted_wv = date_lifted) %>%
    select(-any_of("id")) %>%
    mutate(date_issued = as.Date(date_issued_wv, tryFormats = c("%Y-%m-%d")),
           date_lifted = as.Date(date_lifted_wv, tryFormats = c("%Y-%m-%d")),
           last_epic_run_date = as.Date(last_epic_run_date,
                                        tryFormats = c("%Y-%m-%d")),
           type = "Assumed - Boil Water Notices",
           # WV reports real lift dates, unlike Alaska's inferred closures
           epic_date_lifted_flag = "Reported",
           state = "West Virginia") %>%
    finalize_bwn_clean()

  message("Validating clean_wv_bwn...")
  validate_clean_bwn(config, wv_bwn_clean, dataset_id,
                     label = "WV BWN Clean Validation")

  message(sprintf("Writing clean_wv_bwn to S3 to %s...", link))
  s3_write_csv(wv_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(wv_bwn_clean)
}
