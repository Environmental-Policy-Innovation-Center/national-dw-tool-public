###############################################################################
# New Mexico Boil Water Notices (issue #38)
# Migrated from 1_downloaders/quarterly/nm_worker/nm_quarterly.R
#
# New Mexico publishes one HTML table whose status column carries the lift date
# in prose ("Lifted on 09/21/2023"), so lift dates are real and this is a rolling
# window: epic_date_lifted_flag is "Reported" and records that drop off the page
# are retained from our own history.
#
# The system id is not a column. Older rows embed one or two pwsids in the system
# name in parentheses; newer rows stopped doing that, so those are matched to the
# EPA SABs name list instead. Some rows match neither, which is why the pwsid
# completeness checks run at warning severity.
#
# Shared BWN helpers live in functions/bwn_helpers.R.
###############################################################################

# The published table has no usable header row of its own
nm_bwn_col_names <- c("water_system", "county", "advisory_issue_date", "status")

# A pwsid the state publishes incorrectly; EPA SABs has the correct one
nm_pwsid_corrections <- c("NM355518" = "NM3535518")

#' Pull New Mexico BWN data, then run the NM BWN clean pipeline
#' @param config Main config
#' @param dataset_id "raw_nm_bwn"
run_nm_bwn_pipeline <- function(config, dataset_id) {
  update_raw_nm_bwn(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Fetch, reconcile against the previous run, validate, and save raw NM BWN data
#' @param config Main config
#' @param dataset_id "raw_nm_bwn"
#' @return Reconciled raw BWN data frame
update_raw_nm_bwn <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link

  message("Scraping the New Mexico Environment Department advisories page...")
  tables <- rvest::read_html(source_url) %>%
    rvest::html_nodes("table") %>%
    rvest::html_table(fill = TRUE)

  if (length(tables) < 1) {
    stop(sprintf(
      "NM BWN: no advisory table found on %s. The page layout has probably changed.",
      source_url), call. = FALSE)
  }
  page_data <- tables[[1]]
  if (ncol(page_data) < length(nm_bwn_col_names)) {
    stop(sprintf(
      "NM BWN: advisory table has %d columns, expected at least %d. The page layout has probably changed.",
      ncol(page_data), length(nm_bwn_col_names)), call. = FALSE)
  }
  if (nrow(page_data) < 2) {
    stop(sprintf(
      "NM BWN: advisory table has %d row(s), so there is nothing below the header row on %s.",
      nrow(page_data), source_url), call. = FALSE)
  }
  names(page_data)[seq_along(nm_bwn_col_names)] <- nm_bwn_col_names
  # the first row repeats the column names rather than carrying data. Guarded
  # above: with a header-only table, 2:nrow() would be c(2, 1) and promote the
  # label row into the data.
  nm_bwn <- page_data[2:nrow(page_data), ]

  # System names look like "Some Water System (NM1234567)" and occasionally carry
  # two ids. Splitting on the parentheses yields up to four pieces.
  nm_bwn_sep <- nm_bwn %>%
    tidyr::separate(water_system,
                    into = c("name_one", "pwsid_one", "name_two", "pwsid_two"),
                    sep = "\\(|\\)", fill = "right", extra = "drop")

  # rows that carry their pwsid inline: pivot the two id slots into rows
  bwn_pwsids <- nm_bwn_sep %>%
    filter(!is.na(pwsid_one)) %>%
    tidyr::pivot_longer(cols = c("pwsid_one", "pwsid_two")) %>%
    filter(!is.na(value)) %>%
    mutate(water_system_name = paste0(name_one, name_two)) %>%
    select(-c(name_one:name_two)) %>%
    relocate(water_system_name) %>%
    rename(pwsid = value, pwsid_number = name) %>%
    relocate(pwsid, .after = water_system_name)

  # rows with no inline pwsid, recovered by name against the EPA SABs list
  bwn_no_pwsids <- nm_bwn_sep %>%
    filter(is.na(pwsid_one)) %>%
    mutate(name_one = trimws(name_one))

  message("Recovering pwsids by system name from the EPA SABs name list...")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)
  nm_pwsids <- epa_sabs_pwsids %>% filter(grepl("NM", states_intersect))

  bwn_no_pwsids_merged <- merge(nm_pwsids, bwn_no_pwsids, by.x = "pws_name",
                                by.y = "name_one", all.y = TRUE) %>%
    select(-c(pwsid_one:pwsid_two)) %>%
    rename(water_system_name = pws_name)

  nm_bwn_tidy <- bind_rows(bwn_pwsids, bwn_no_pwsids_merged) %>%
    mutate(
      # The legacy worker chose between "%m/%d/%Y" and "%m/%d/%y" on nchar().
      # mdy() handles both two- and four-digit years directly and does not
      # depend on which row happens to come first, which as.Date(tryFormats=)
      # does. Verified to reproduce every stored issued_date_clean exactly.
      issued_date_clean = lubridate::mdy(advisory_issue_date),
      status_clean = case_when(grepl("Lifted", status) ~ "Lifted"),
      # the lift date is embedded in prose, e.g. "Lifted on 09/21/2023"
      lifted_date_clean = str_extract(status, "\\d{1,2}/\\d{1,2}/\\d{4}"),
      lifted_date_clean = as.Date(lifted_date_clean, "%m/%d/%Y"),
      # named-vector lookup returns NA for anything not being corrected (and for
      # a missing pwsid), so coalesce falls back to the original value
      pwsid = coalesce(unname(nm_pwsid_corrections[pwsid]), pwsid)) %>%
    mutate(last_epic_run_date = as.character(Sys.Date()))

  matched <- sum(!is.na(nm_bwn_tidy$pwsid))
  message(sprintf("Matched %d of %d scraped advisories to a pwsid.",
                  matched, nrow(nm_bwn_tidy)))

  message("Reading the previous run for reconciliation...")
  nm_bwn_old <- read_prior_bwn(link)

  nm_bwn_reconciled <- reconcile_nm_bwn(nm_bwn_tidy, nm_bwn_old)

  message("Validating raw_nm_bwn...")
  validate_raw_bwn(config, nm_bwn_reconciled, nm_bwn_old, dataset_id,
                   label = "NM Boil Water Notice Validation",
                   pwsid_severity = "warning")

  message(sprintf("Writing raw_nm_bwn to S3 to %s...", link))
  s3_write_csv(nm_bwn_reconciled, link)

  return(nm_bwn_reconciled)
}

#' Reconcile a fresh New Mexico pull against the previous run.
#' Keys on two ids because the source is a rolling window:
#'   full_id   = pwsid + issued_date_clean + lifted_date_clean
#'   update_id = pwsid + issued_date_clean
#' A row whose update_id matches but whose full_id does not is the same advisory
#' with a lift date now reported, so the fresh version replaces the stored one.
#' @param nm_bwn_tidy Fresh pull, tidied
#' @param nm_bwn_old Previous raw pull, or NULL on a first run
#' @return Combined data frame
reconcile_nm_bwn <- function(nm_bwn_tidy, nm_bwn_old) {
  reconcile_bwn_rolling_window(
    nm_bwn_tidy, nm_bwn_old,
    key_cols        = c("pwsid", "issued_date_clean", "lifted_date_clean"),
    update_key_cols = c("pwsid", "issued_date_clean")
  )
}

#' Standardize NM BWN into the shared cross-state BWN schema
#' @param config Main config
#' @param dataset_id "clean_nm_bwn"
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_nm_bwn_pipeline <- function(config, dataset_id = "clean_nm_bwn",
                                      bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link

  if (is.null(bwn_raw)) {
    message("Downloading raw NM BWN from S3...")
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  nm_bwn_clean <- bwn_raw %>%
    rename(date_issued = issued_date_clean,
           date_lifted = lifted_date_clean) %>%
    mutate(date_issued = as.Date(date_issued, tryFormats = c("%Y-%m-%d")),
           date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")),
           last_epic_run_date = as.Date(last_epic_run_date,
                                        tryFormats = c("%Y-%m-%d")),
           # New Mexico states the lift date in the status column
           epic_date_lifted_flag = "Reported",
           type = "Assumed - Boil Water Advisories",
           state = "New Mexico") %>%
    finalize_bwn_clean()

  message("Validating clean_nm_bwn...")
  validate_clean_bwn(config, nm_bwn_clean, dataset_id,
                     label = "NM BWN Clean Validation",
                     pwsid_severity = "warning")

  message(sprintf("Writing clean_nm_bwn to S3 to %s...", link))
  s3_write_csv(nm_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(nm_bwn_clean)
}
