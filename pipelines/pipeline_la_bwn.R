###############################################################################
# Louisiana Boil Water Notices & Advisories
#
# LA's portal doesn't have an exposed API so the data has to be manually
# downloaded for now.
#
# Steps:
# 1. Go to https://sdw.ldh.la.gov/
# 2. Click "Violations".
# 3. Set violation type to "BN - STATE ISSUED BOIL NOTICE".
# 4. Set the date range to a 5 year window ending today.
# 5. Set "water system type" to "Community".
# 6. Click "Search", then "Export Current Page".
# 7. Save as a CSV named la_bwn_5yr.csv, placed at ./data/la_bwn_5yr.csv
#    (relative to the repo root), matching raw_la_bwn_5yr.local_source_path in
#.   main_config.json.
# 7. Swap the violation type to "BA - SYSTEM ISSUED BOIL ADVISORY" and the date
#    range to the past 1 year (there are a ton of records and the export caps
#    at 5,000 records).
# 8. Click "Search", then "Export All" to your email.
# 9. Save as la_bwa_1yr.csv, placed at ./data/la_bwa_1yr.csv, matching
#    raw_la_bwa_1yr.local_source_path.
###############################################################################

#' Pull and clean Louisiana's 5-year boil water notices.
#' @param config Main config
#' @param dataset_id "raw_la_bwn_5yr"
run_la_bwn_5yr_pipeline <- function(config, dataset_id) {
  update_raw_la_bwn(config, dataset_id, type_label = "state_issued_boil_notice_5yr")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Pull and clean Louisiana's 1-year boil water advisories.
#' @param config Main config
#' @param dataset_id "raw_la_bwa_1yr"
run_la_bwa_1yr_pipeline <- function(config, dataset_id) {
  update_raw_la_bwn(config, dataset_id, type_label = "system_issued_boil_advisory_1yr")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Pull and validate one of Louisiana's BW datasets.
#' @param config Main config
#' @param dataset_id "raw_la_bwn_5yr" or "raw_la_bwa_1yr"
#' @param type_label Value to put in the "type" column
update_raw_la_bwn <- function(config, dataset_id, type_label) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  local_path <- sub_config$local_source_path
  link <- sub_config$link
  date_updated <- sub_config$date_updated

  if (!file.exists(local_path)) {
    stop(sprintf(
      "LA BWN source file not found at %s. Follow download steps in pipeline_la_bwn.R.",
      local_path
    ), call. = FALSE)
  }

  message(sprintf("Reading local LA BWN export from %s...", local_path))
  la_bwn_tidy <- read.csv(local_path, stringsAsFactors = FALSE) %>%
    janitor::clean_names() %>%
    mutate(
      type = type_label,
      last_epic_run_date = date_updated
    )

  message(sprintf("Validating %s...", dataset_id))
  validate_raw_bwn(config, la_bwn_tidy, bwn_old = NULL, dataset_id,
                   label = "LA Boil Water Notice Validation",
                   pwsid_col = "water_system_id")

  message(sprintf("Writing %s to S3 to %s...", dataset_id, link))
  s3_write_csv(la_bwn_tidy, link)

  return(la_bwn_tidy)
}

#' Standardize one of Louisiana's raw BW datasets into the shared BWN schema.
#' @param config Main config
#' @param dataset_id "clean_la_bwn_5yr" or "clean_la_bwa_1yr"
#' @param state_label Value to put in the "state" column
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_la_bwn_pipeline <- function(config, dataset_id, state_label, bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link

  if (is.null(bwn_raw)) {
    message(sprintf("Downloading raw %s from S3...", dataset_id))
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  la_bwn_clean <- bwn_raw %>%
    rename(pwsid = water_system_id) %>%
    mutate(
      date_issued = as.Date(violation_determination_date,
                            tryFormats = c("%m/%d/%y", "%m/%d/%Y")),
      date_lifted = as.Date(violation_period_end,
                            tryFormats = c("%m/%d/%y", "%m/%d/%Y")),
      epic_date_lifted_flag = "Reported",
      last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d")),
      state = state_label
    ) %>%
    finalize_bwn_clean()

  message(sprintf("Validating %s...", dataset_id))
  validate_clean_bwn(config, la_bwn_clean, dataset_id, label = "LA BWN Clean Validation")

  message(sprintf("Writing %s to S3 to %s...", dataset_id, link))
  s3_write_csv(la_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(la_bwn_clean)
}
