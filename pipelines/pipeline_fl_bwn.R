###############################################################################
# Florida Boil Water Notices (issue #38)
# Migrated from 1_downloaders/quarterly/fl_worker/fl_quarterly.R
#
# Florida Health publishes a notice table carrying its own rescind dates, so this
# is a rolling window: an advisory can come back with its rescind date filled in,
# and notices that drop off the page are retained from our own history.
# epic_date_lifted_flag is "Reported".
#
# Three deliberate differences from the legacy worker, all called out in the PR:
#
#  1. The legacy worker never joined a pwsid at all. The column only existed in
#     the stored file, so every genuinely new notice was written with a missing
#     system id, and a first run into an empty bucket would produce a dataset
#     with no pwsid column for the clean step to standardize. Florida publishes
#     only a system name, so pwsid is recovered from the EPA SABs name list the
#     same way Washington, Arkansas and New Mexico do. The completeness checks
#     run at warning severity for the same reason they do there.
#
#  2. The legacy guard that dropped the all-NA row of a header-only page was an
#     if() over a vector, so it only worked when the scrape produced exactly one
#     new row. Its intent is kept via drop_empty_scraped_rows(), applied to the
#     scrape. Florida's page is header-only whenever no notice is active, so
#     without this the reconcile files a system-less, date-less "advisory" that
#     then never ages out of the national summary.
#
#  3. The legacy clean step could emit epic_date_lifted_flag =
#     "Assumed for this one record", which is outside the two values the national
#     summary contract allows. That case is mapped to "Assumed", which is what it
#     means; the explanation stays in the comments column.
#
# Shared BWN helpers live in functions/bwn_helpers.R.
###############################################################################

# Marker the team writes into comments when a notice disappeared from the page
# without a rescind date and one was filled in by hand
fl_assumed_rescind_marker <- "EPIC ASSUMED DATE RESCINDED"

#' Pull Florida BWN data, then run the FL BWN clean pipeline
#' @param config Main config
#' @param dataset_id "raw_fl_bwn"
run_fl_bwn_pipeline <- function(config, dataset_id) {
  update_raw_fl_bwn(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Fetch, reconcile against the previous run, validate, and save raw FL BWN data
#' @param config Main config
#' @param dataset_id "raw_fl_bwn"
#' @return Reconciled raw BWN data frame
update_raw_fl_bwn <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link

  message("Scraping the Florida Health boil water notice page...")
  tables <- rvest::read_html(source_url) %>%
    rvest::html_nodes("table") %>%
    rvest::html_table(fill = TRUE)

  if (length(tables) < 1) {
    stop(sprintf(
      "FL BWN: no notice table found on %s. The page layout has probably changed.",
      source_url), call. = FALSE)
  }

  fl_bwn <- tables[[1]] %>%
    janitor::clean_names() %>%
    # Florida's page is header-only whenever no notice is active, and
    # html_table(fill = TRUE) renders that as one row of NAs. Without this the
    # reconcile files that row as a new advisory. See drop_empty_scraped_rows().
    drop_empty_scraped_rows(c("system_name", "county")) %>%
    # normalized here because this doubles as the join key to the SABs name list
    mutate(system_name = str_squish(str_to_title(system_name)))

  message("Recovering pwsids by system name from the EPA SABs name list...")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)
  fl_pwsids <- epa_sabs_pwsids %>% filter(grepl("FL", states_intersect))

  fl_bwn_tidy <- merge(fl_bwn, fl_pwsids, by.x = "system_name",
                       by.y = "pws_name", all.x = TRUE) %>%
    mutate(last_epic_run_date = as.character(Sys.Date()))

  matched <- sum(!is.na(fl_bwn_tidy$pwsid))
  message(sprintf("Matched %d of %d scraped notices to a pwsid.",
                  matched, nrow(fl_bwn_tidy)))

  message("Reading the previous run for reconciliation...")
  fl_bwn_old <- read_prior_bwn(link)

  fl_bwn_reconciled <- reconcile_fl_bwn(fl_bwn_tidy, fl_bwn_old)

  message("Validating raw_fl_bwn...")
  validate_raw_bwn(config, fl_bwn_reconciled, fl_bwn_old, dataset_id,
                   label = "FL Boil Water Notice Validation",
                   pwsid_severity = "warning")

  message(sprintf("Writing raw_fl_bwn to S3 to %s...", link))
  s3_write_csv(fl_bwn_reconciled, link)

  return(fl_bwn_reconciled)
}

#' Reconcile a fresh Florida pull against the previous run.
#' Keys on two ids because the source is a rolling window:
#'   full_id   = system_name + date_issued + date_rescinded
#'   update_id = system_name + date_issued
#' A row whose update_id matches but whose full_id does not is the same notice
#' with a rescind date now reported, so the fresh version replaces the stored one.
#'
#' The legacy worker guarded its bind with
#'   if (is.na(new_records$system_name) & is.na(new_records$county))
#' which evaluates a vector in an if(): with no new notices that is logical(0)
#' and errors, and with two or more rows it errors as well. Its INTENT, dropping
#' the all-NA row that a header-only page produces, is preserved, but it is
#' applied to the scrape itself via drop_empty_scraped_rows() so it works for any
#' number of rows rather than only when the scrape yields exactly one.
#' @param fl_bwn_tidy Fresh pull, tidied
#' @param fl_bwn_old Previous raw pull, or NULL on a first run
#' @return Combined data frame
reconcile_fl_bwn <- function(fl_bwn_tidy, fl_bwn_old) {
  reconcile_bwn_rolling_window(
    fl_bwn_tidy, fl_bwn_old,
    key_cols        = c("system_name", "date_issued", "date_rescinded"),
    update_key_cols = c("system_name", "date_issued")
  )
}

#' Standardize FL BWN into the shared cross-state BWN schema
#' @param config Main config
#' @param dataset_id "clean_fl_bwn"
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_fl_bwn_pipeline <- function(config, dataset_id = "clean_fl_bwn",
                                      bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link

  if (is.null(bwn_raw)) {
    message("Downloading raw FL BWN from S3...")
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  fl_bwn_clean <- bwn_raw %>%
    mutate(date_issued = as.Date(date_issued, tryFormats = c("%m/%d/%Y")),
           date_lifted = as.Date(date_rescinded, tryFormats = c("%m/%d/%Y")),
           last_epic_run_date = as.Date(last_epic_run_date,
                                        tryFormats = c("%Y-%m-%d")),
           type = "Assumed - Boil Water Notices",
           # Florida reports rescind dates, except where a notice vanished from
           # the page without one and the date was filled in by hand. See the
           # header note on the flag value.
           epic_date_lifted_flag = case_when(
             grepl(fl_assumed_rescind_marker, comments) ~ "Assumed",
             TRUE ~ "Reported"),
           state = "Florida") %>%
    finalize_bwn_clean()

  message("Validating clean_fl_bwn...")
  validate_clean_bwn(config, fl_bwn_clean, dataset_id,
                     label = "FL BWN Clean Validation",
                     pwsid_severity = "warning")

  message(sprintf("Writing clean_fl_bwn to S3 to %s...", link))
  s3_write_csv(fl_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(fl_bwn_clean)
}
