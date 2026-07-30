###############################################################################
# Alaska Boil Water Notices (issue #38)
# Migrated from 1_downloaders/daily/ak_worker/ak_daily.R
#
# Alaska's DEC publishes only *currently active* advisories, so this pipeline has
# to infer closures: a record that disappears from the feed is treated as lifted
# on the day we noticed, flagged epic_date_lifted_flag = "Assumed". States that
# publish a real lift date use "Reported" instead. That flag is how the national
# summary distinguishes the two, so it is preserved deliberately here.
#
# Shared BWN helpers live in functions/bwn_helpers.R.
###############################################################################

#' Pull Alaska BWN data, then run the AK BWN clean pipeline
#' @param config Main config
#' @param dataset_id "raw_ak_bwn"
run_ak_bwn_pipeline <- function(config, dataset_id) {
  ak_bwn_raw <- update_raw_ak_bwn(config, dataset_id)

  message("Running AK BWN clean pipeline...")
  run_clean_ak_bwn_pipeline(config, "clean_ak_bwn", bwn_raw = ak_bwn_raw)

  # Bridge last, once both datasets exist, matching the legacy worker order. If
  # the clean step aborts, the task manager keeps pointing at the last good run
  # rather than advertising a dataset that was never written.
  update_bwn_task_manager(
    config[[dataset_id]]$input_links$task_manager_link,
    "ak_bwn", config[[dataset_id]]$link, config$clean_ak_bwn$link
  )

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Fetch, reconcile against the previous run, validate, and save raw AK BWN data
#' @param config Main config
#' @param dataset_id "raw_ak_bwn"
#' @return Reconciled raw BWN data frame
update_raw_ak_bwn <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link

  message("Pulling Alaska BWN layer from DEC ArcGIS...")
  ak_bwn <- arcpullr::get_spatial_layer(source_url)

  message("Filtering to community water systems...")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)

  ak_bwn_tidy <- ak_bwn %>%
    as.data.frame() %>%
    janitor::clean_names() %>%
    mutate(pwsid = str_squish(pwsid)) %>%
    filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
    mutate(
      # the feed reports the issue date as epoch milliseconds
      bwn_issue_date_clean = as.character(as.POSIXct(bwn_issue_date / 1000,
                                                     origin = "1970-01-01",
                                                     tz = "America/Anchorage")),
      geoms = as.character(geoms),
      last_epic_run_date = as.character(Sys.Date())
    )

  message("Reading the previous run for reconciliation...")
  ak_bwn_old <- read_prior_bwn(link)

  ak_bwn_reconciled <- reconcile_ak_bwn(ak_bwn_tidy, ak_bwn_old)

  message("Validating raw_ak_bwn...")
  validate_raw_bwn(config, ak_bwn_reconciled, ak_bwn_old, dataset_id,
                   label = "AK Boil Water Notice Validation")

  message(sprintf("Writing raw_ak_bwn to S3 to %s...", link))
  s3_write_csv(ak_bwn_reconciled, link)

  return(ak_bwn_reconciled)
}

#' Reconcile a fresh Alaska pull against the previous run.
#' Records are keyed on pwsid + issue date. New records are added, records that
#' have dropped off the feed without a lift date are closed as of today, and
#' records still present keep their original detection date.
#' @param ak_bwn_tidy Fresh pull, tidied
#' @param ak_bwn_old Previous raw pull, or NULL on a first run
#' @return Combined data frame
reconcile_ak_bwn <- function(ak_bwn_tidy, ak_bwn_old) {
  reconcile_bwn_active_feed(ak_bwn_tidy, ak_bwn_old,
                            key_cols = c("pwsid", "bwn_issue_date"))
}

#' Standardize AK BWN into the shared cross-state BWN schema
#' @param config Main config
#' @param dataset_id "clean_ak_bwn"
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_ak_bwn_pipeline <- function(config, dataset_id = "clean_ak_bwn",
                                      bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link

  if (is.null(bwn_raw)) {
    message("Downloading raw AK BWN from S3...")
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  ak_bwn_clean <- bwn_raw %>%
    mutate(date_issued = as.Date(bwn_issue_date_clean, tryFormats = c("%Y-%m-%d")),
           date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")),
           last_epic_run_date = as.Date(last_epic_run_date,
                                        tryFormats = c("%Y-%m-%d")),
           # Alaska publishes only active advisories, so every closure we record
           # is inferred rather than reported. See the header note.
           epic_date_lifted_flag = "Assumed",
           type = name,
           state = "Alaska") %>%
    finalize_bwn_clean()

  message("Validating clean_ak_bwn...")
  validate_clean_bwn(config, ak_bwn_clean, dataset_id,
                     label = "AK BWN Clean Validation")

  message(sprintf("Writing clean_ak_bwn to S3 to %s...", link))
  s3_write_csv(ak_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(ak_bwn_clean)
}
