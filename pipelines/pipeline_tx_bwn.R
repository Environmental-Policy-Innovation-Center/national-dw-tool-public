###############################################################################
# Texas Boil Water Notices & Advisories
#
# Texas BWN and advisories data come from FOIA'd export from TCEQ
# (received 2024-04-17, covers records since 2018).
###############################################################################

#' Pull and clean Texas BWN data.
#' @param config Main config
#' @param dataset_id "raw_tx_bwn"
run_tx_bwn_pipeline <- function(config, dataset_id) {
  update_raw_tx_bwn(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Pull and validate Texas BWN data.
#' @param config Main config
#' @param dataset_id "raw_tx_bwn"
update_raw_tx_bwn <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link
  date_foia <- sub_config$date_foia

  message("Reading FOIA'd Texas BWN workbook from S3...")
  s3_uri <- sub("^s3://[^/]+/", "", source_url)
  s3_source_bucket <- sub("^s3://([^/]+)/.*$", "\\1", source_url)
  tx_bwn_raw <- s3_read_xlsx(s3_uri, bucket = s3_source_bucket) %>%
    janitor::clean_names()

  message("Filtering to community water systems...")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)

  tx_bwn_tidy <- tx_bwn_raw %>%
    rename(pwsid = pws_id) %>%
    mutate(
      updtts = as.Date(updtts, tryFormats = c("%Y-%m-%d")),
      reported_date = openxlsx::convertToDate(reported_date),
      achieved_date = openxlsx::convertToDate(achieved_date),
      issued_date = openxlsx::convertToDate(issued),
      rescinded_date = openxlsx::convertToDate(rescinded)
    ) %>%
    # removing this date - presumably a human error since this hasn't occurred yet
    # as of writing - the is.na keeps the four instances w/o an issued date
    filter(issued_date != as.Date("2027-01-01") | is.na(issued_date)) %>%
    mutate(
      date_issued = as.character(issued_date),
      date_lifted = as.character(rescinded_date),
      epic_date_lifted_flag = "Reported",
      state = "Texas",
      type = paste0(status, "-", reason_activity),
      last_epic_run_date = date_foia
    ) %>%
    filter(pwsid %in% epa_sabs_pwsids$pwsid)

  message("Validating raw_tx_bwn...")
  validate_raw_bwn(config, tx_bwn_tidy, bwn_old = NULL, dataset_id,
                   label = "TX Boil Water Notice Validation")

  message(sprintf("Writing raw_tx_bwn to S3 to %s...", link))
  s3_write_csv(tx_bwn_tidy, link)

  return(tx_bwn_tidy)
}

#' Standardize TX BWN into the shared BWN schema.
#' @param config Main config
#' @param dataset_id "clean_tx_bwn"
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_tx_bwn_pipeline <- function(config, dataset_id = "clean_tx_bwn", bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link
  date_foia <- sub_config$date_foia

  if (is.null(bwn_raw)) {
    message("Downloading raw TX BWN from S3...")
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  tx_bwn_clean <- bwn_raw %>%
    mutate(
      date_issued = as.Date(date_issued, tryFormats = c("%Y-%m-%d")),
      date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")),
      last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d"))
    ) %>%
    finalize_bwn_clean() %>%
    mutate(date_worker_last_ran = as.Date(date_foia))

  message("Validating clean_tx_bwn...")
  validate_clean_bwn(config, tx_bwn_clean, dataset_id, label = "TX BWN Clean Validation")

  message(sprintf("Writing clean_tx_bwn to S3 to %s...", link))
  s3_write_csv(tx_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(tx_bwn_clean)
}
