###############################################################################
# Washington Boil Water Notices (issue #38)
# Migrated from 1_downloaders/daily/wa_worker/wa_daily.R
#
# Washington DOH publishes an "active alerts" HTML table, so this is an active
# feed: a record disappearing implies it was lifted and we date that closure to
# the day we noticed it ("Assumed").
#
# The source publishes no system id, only a system name, so pwsid is recovered
# by joining to the EPA SABs name list. That join is lossy (roughly a third of
# rows match today), which is why the advisory itself is keyed on the system
# NAME rather than pwsid, and why the pwsid completeness checks run at warning
# severity here. An unmatched advisory is still a real advisory the national
# summary has to count, so it is kept rather than filtered out.
#
# Shared BWN helpers live in functions/bwn_helpers.R.
###############################################################################

#' Pull Washington BWN data, then run the WA BWN clean pipeline
#' @param config Main config
#' @param dataset_id "raw_wa_bwn"
run_wa_bwn_pipeline <- function(config, dataset_id) {
  update_raw_wa_bwn(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Fetch, reconcile against the previous run, validate, and save raw WA BWN data
#' @param config Main config
#' @param dataset_id "raw_wa_bwn"
#' @return Reconciled raw BWN data frame
update_raw_wa_bwn <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link

  message("Scraping the Washington DOH active alerts page...")
  tables <- rvest::read_html(source_url) %>%
    rvest::html_nodes("table") %>%
    rvest::html_table(fill = TRUE)

  if (length(tables) < 1) {
    stop(sprintf(
      "WA BWN: no alert table found on %s. The page layout has probably changed.",
      source_url), call. = FALSE)
  }

  wa_bwn <- tables[[1]] %>%
    janitor::clean_names() %>%
    # "x" and "x_2" are unnamed layout columns in the published table. any_of()
    # rather than a bare select so a cosmetic change to the page does not fail
    # the run; a change to a column that actually matters is caught by the key
    # check inside the reconcile.
    select(-any_of(c("x", "x_2"))) %>%
    # the table repeats a header-ish row with no system name
    filter(!(is.na(water_system))) %>%
    # normalized here because this doubles as the join key to the SABs name list
    mutate(water_system = trimws(water_system),
           water_system = str_to_title(water_system))

  message("Recovering pwsids by system name from the EPA SABs name list...")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)
  wa_pwsids <- epa_sabs_pwsids %>% filter(grepl("WA", states_intersect))

  wa_bwn_tidy <- merge(wa_bwn, wa_pwsids, by.x = "water_system",
                       by.y = "pws_name", all.x = TRUE) %>%
    relocate(pwsid) %>%
    select(-any_of("states_intersect")) %>%
    mutate(last_epic_run_date = as.character(Sys.Date()))

  matched <- sum(!is.na(wa_bwn_tidy$pwsid))
  message(sprintf("Matched %d of %d scraped advisories to a pwsid.",
                  matched, nrow(wa_bwn_tidy)))

  message("Reading the previous run for reconciliation...")
  wa_bwn_old <- read_prior_bwn(link)

  wa_bwn_reconciled <- reconcile_wa_bwn(wa_bwn_tidy, wa_bwn_old)

  message("Validating raw_wa_bwn...")
  validate_raw_bwn(config, wa_bwn_reconciled, wa_bwn_old, dataset_id,
                   label = "WA Boil Water Notice Validation",
                   pwsid_severity = "warning")

  message(sprintf("Writing raw_wa_bwn to S3 to %s...", link))
  s3_write_csv(wa_bwn_reconciled, link)

  return(wa_bwn_reconciled)
}

#' Reconcile a fresh Washington pull against the previous run.
#' Washington lists only active alerts, so this uses the shared active-feed
#' reconcile. The advisory is identified by system name, issue date and the
#' recommended action, because pwsid is missing for most rows and so cannot
#' serve as the identity.
#' @param wa_bwn_tidy Fresh pull, tidied
#' @param wa_bwn_old Previous raw pull, or NULL on a first run
#' @return Combined data frame
reconcile_wa_bwn <- function(wa_bwn_tidy, wa_bwn_old) {
  reconcile_bwn_active_feed(
    wa_bwn_tidy, wa_bwn_old,
    key_cols = c("water_system", "date_issued_sort_ascending",
                 "action_recommended")
  )
}

#' Standardize WA BWN into the shared cross-state BWN schema
#' @param config Main config
#' @param dataset_id "clean_wa_bwn"
#' @param bwn_raw Optional pre-loaded raw BWN data. If NULL, downloaded from S3.
run_clean_wa_bwn_pipeline <- function(config, dataset_id = "clean_wa_bwn",
                                      bwn_raw = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link

  if (is.null(bwn_raw)) {
    message("Downloading raw WA BWN from S3...")
    bwn_raw <- s3_read_csv(raw_link)
  }

  message("Standardizing to the shared BWN schema...")
  wa_bwn_clean <- bwn_raw %>%
    rename(date_issued = date_issued_sort_ascending,
           type = action_recommended) %>%
    mutate(date_issued = as.Date(date_issued, tryFormats = c("%m/%d/%Y")),
           date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")),
           last_epic_run_date = as.Date(last_epic_run_date,
                                        tryFormats = c("%Y-%m-%d")),
           # Washington publishes only active alerts, so every closure we record
           # is inferred rather than reported. See the header note.
           epic_date_lifted_flag = "Assumed",
           state = "Washington") %>%
    finalize_bwn_clean()

  message("Validating clean_wa_bwn...")
  validate_clean_bwn(config, wa_bwn_clean, dataset_id,
                     label = "WA BWN Clean Validation",
                     pwsid_severity = "warning")

  message(sprintf("Writing clean_wa_bwn to S3 to %s...", link))
  s3_write_csv(wa_bwn_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(wa_bwn_clean)
}
