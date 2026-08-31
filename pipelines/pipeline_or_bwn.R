###############################################################################
# Oregon Boil Water Notices (issue #38)
# Migrated from 1_downloaders/quarterly/or_worker/or_quarterly.R
#
# Oregon's advisory portal lists open and lifted advisories together with real
# lift dates, so this is a rolling window: an advisory already in our records can
# come back with its lift date filled in, and records that drop off the listing
# are retained from our own history. epic_date_lifted_flag is "Reported".
#
# Oregon's tracking began in May 2017, so the clean step drops the single
# pre-2017 record rather than presenting a partial year.
#
# Shared BWN helpers live in functions/bwn_helpers.R.
###############################################################################

# The portal renders the table with the filter options appended to each header,
# so the columns are renamed positionally. Note "affcted_populations": the
# misspelling is carried forward deliberately, it is the stored column name.
or_bwn_col_names <- c("regulating_agency", "county_served", "pws", "pws_name",
                      "system_type", "population", "primary_source",
                      "advisory_type", "reason", "begin_date", "date_lifted",
                      "area_affected", "affcted_populations")

# Oregon writes an open advisory's lift date as this sentinel rather than a blank
or_open_sentinel <- "Open"

#' Pull Oregon BWN data, then run the OR BWN clean pipeline
#' @param config Main config
#' @param dataset_id "raw_or_bwn"
run_or_bwn_pipeline <- function(config, dataset_id) {
  update_raw_or_bwn(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Fetch, reconcile against the previous run, validate, and save raw OR BWN data
#' @param config Main config
#' @param dataset_id "raw_or_bwn"
#' @return Reconciled raw BWN data frame
update_raw_or_bwn <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link

  message("Scraping the Oregon drinking water advisory portal...")
  tables <- rvest::read_html(source_url) %>%
    rvest::html_nodes("table") %>%
    rvest::html_table(fill = TRUE)

  # the advisories are in the second table; the first is the filter form
  if (length(tables) < 2) {
    stop(sprintf(
      "OR BWN: expected at least 2 tables on %s, found %d. The page layout has probably changed.",
      source_url, length(tables)), call. = FALSE)
  }
  bwn <- tables[[2]]
  if (ncol(bwn) < length(or_bwn_col_names)) {
    stop(sprintf(
      "OR BWN: advisory table has %d columns, expected at least %d. The page layout has probably changed.",
      ncol(bwn), length(or_bwn_col_names)), call. = FALSE)
  }
  names(bwn)[seq_along(or_bwn_col_names)] <- or_bwn_col_names
  or_bwn <- bwn[, seq_along(or_bwn_col_names)]

  or_bwn_tidy <- or_bwn %>%
    # the listing carries one row with no system number
    filter(!is.na(pws)) %>%
    # the portal reports a bare system number, so rebuild the pwsid: zero-pad it
    # to five digits and prefix Oregon's "OR41". pmax guards a number longer than
    # five digits, which would make strrep() error; such a value simply fails the
    # SABs membership filter below.
    mutate(pwsid_char = as.character(pws),
           pwsid = paste0("OR41", strrep("0", pmax(0, 5 - nchar(pwsid_char))),
                          pwsid_char)) %>%
    select(-pwsid_char) %>%
    relocate(pwsid, .before = pws) %>%
    # some community systems on this listing are absent from the EPA SABs
    # database and so have no boundary to attach an advisory to
    filter(pwsid %in% s3_read_csv(pwsid_names_link)$pwsid) %>%
    mutate(last_epic_run_date = as.character(Sys.Date()))

  message("Reading the previous run for reconciliation...")
  or_bwn_old <- read_prior_bwn(link)

  or_bwn_reconciled <- reconcile_or_bwn(or_bwn_tidy, or_bwn_old)

  message("Validating raw_or_bwn...")
  validate_raw_bwn(config, or_bwn_reconciled, or_bwn_old, dataset_id,
                   label = "OR Boil Water Notice Validation")

  message(sprintf("Writing raw_or_bwn to S3 to %s...", link))
  s3_write_csv(or_bwn_reconciled, link)

  return(or_bwn_reconciled)
}

#' Reconcile a fresh Oregon pull against the previous run.
#' Keys on two ids because the source is a rolling window:
#'   full_id   = pwsid + advisory_type + begin_date + date_lifted
#'   update_id = pwsid + advisory_type + begin_date
#' A row whose update_id matches but whose full_id does not is the same advisory
#' with a lift date now reported, so the fresh version replaces the stored one.
#' advisory_type is part of the identity because one system can hold a boil water
#' and a do-not-drink advisory beginning the same day.
#' @param or_bwn_tidy Fresh pull, tidied
#' @param or_bwn_old Previous raw pull, or NULL on a first run
#' @return Combined data frame
reconcile_or_bwn <- function(or_bwn_tidy, or_bwn_old) {
  reconcile_bwn_rolling_window(
    or_bwn_tidy, or_bwn_old,
    key_cols        = c("pwsid", "advisory_type", "begin_date", "date_lifted"),
    update_key_cols = c("pwsid", "advisory_type", "begin_date")
  )
}

#' Standardize OR BWN into the shared cross-state BWN schema
#' @param config Main config
#' @param dataset_id "clean_or_bwn"
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_or_bwn_pipeline <- function(config, dataset_id = "clean_or_bwn",
                                      bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link

  if (is.null(bwn_raw)) {
    message("Downloading raw OR BWN from S3...")
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  or_bwn_clean <- bwn_raw %>%
    # Renamed rather than copied, so the standardized columns land in the same
    # positions the legacy worker left them in. Oregon's own rendering of the
    # lift date is kept as or_date_lifted and the standardized column derived
    # from it below.
    rename(or_date_lifted = date_lifted,
           type = advisory_type) %>%
    mutate(date_issued = lubridate::mdy(begin_date),
           begin_year = lubridate::year(date_issued),
           date_lifted = lubridate::mdy(na_if(or_date_lifted, or_open_sentinel)),
           lifted_year = lubridate::year(date_lifted),
           last_epic_run_date = as.Date(last_epic_run_date,
                                        tryFormats = c("%Y-%m-%d")),
           # Oregon reports real lift dates
           epic_date_lifted_flag = "Reported",
           state = "Oregon") %>%
    # advisory tracking began in May 2017. There is a single earlier record, and
    # the first 2017 advisory is from 2017-05-08, so this cutoff is unambiguous.
    filter(begin_year > 2016) %>%
    finalize_bwn_clean()

  message("Validating clean_or_bwn...")
  validate_clean_bwn(config, or_bwn_clean, dataset_id,
                     label = "OR BWN Clean Validation")

  message(sprintf("Writing clean_or_bwn to S3 to %s...", link))
  s3_write_csv(or_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(or_bwn_clean)
}
