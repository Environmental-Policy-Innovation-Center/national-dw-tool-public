###############################################################################
# National BWN Summary and Highlevel Summary
#
# merged_national_bwn_summary - contains one row per advisory across each state.
# This function replaces rows only for the state that triggered this pipeline.
#
# merged_national_highlevel_summary - contains one row per state/pwsid and is
# an aggregated version of the summary. This function is triggered after a
# successful run of merged_national_bwn_summary.
###############################################################################

#' Reads the existing merged CSV or returns an empty dataframe to start fresh.
#' @param link S3 key of the merged file
#' @param bucket Bucket name
#' @return Data frame (0 rows if the object does not exist yet)
read_existing_merged_csv <- function(link, bucket = s3_bucket()) {
  successful_read <- tryCatch({
    s3_client()$head_object(Bucket = bucket, Key = link)
    FALSE
  }, error = function(e) {
    if (inherits(e, "http_404")) return(TRUE)
    stop(sprintf("Could not check for a previous merged file at %s: %s",
                 link, conditionMessage(e)), call. = FALSE)
  })

  if (successful_read) {
    message(sprintf("No previous merged file found at %s, starting empty dataframe.", link))
    return(data.frame())
  }
  s3_read_csv(link, bucket = bucket, coerce_character = FALSE)
}

#' Roll a single state's clean BWN dataset into the national BWN summary,
#' replacing that state's existing rows.
#' @param config Main config
#' @param dataset_id "merged_national_bwn_summary"
#' @param triggered_by dataset_id of the clean_<state>_bwn dataset that
#' triggered this run (e.g. "clean_ak_bwn").
#' @return The updated national BWN summary data frame
run_merged_national_bwn_summary_pipeline <- function(config, dataset_id = "merged_national_bwn_summary",
                                                      triggered_by = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  link <- sub_config$link

  if (is.null(triggered_by)) {
    stop(paste(
      "merged_national_bwn_summary must be triggered by a clean_<state>_bwn dataset."
    ), call. = FALSE)
  }

  triggering_sub_config <- config[[triggered_by]]
  state_label <- triggering_sub_config$bwn_state_label
  if (is.null(state_label) || state_label == "") {
    stop(sprintf(
      "%s has no bwn_state_label set in config. Can't determine which state's rows to replace.",
      triggered_by
    ), call. = FALSE)
  }

  message(sprintf("Downloading most recent BWN data for %s (%s)...", triggered_by, state_label))
  new_state_rows <- s3_read_csv(triggering_sub_config$link, coerce_character = FALSE) %>%
    select(all_of(bwn_contract_cols))

  message(sprintf("Downloading existing national BWN summary from %s...", link))
  bwn_summary_old <- read_prior_merged_csv(link)

  if (nrow(bwn_summary_old) == 0) {
    other_states_rows <- bwn_summary_old
  } else {
    other_states_rows <- bwn_summary_old %>% filter(state != state_label)
  }

  message(sprintf("Replacing %s's rows in the national BWN summary...", state_label))
  bwn_summary_updated <- bind_rows(other_states_rows, new_state_rows) %>%
    mutate(date_lifted = case_when(is.na(date_lifted) | date_lifted == "" ~ "Open",
                                   TRUE ~ date_lifted))

  message("Validating merged_national_bwn_summary...")
  validate_merged_national_bwn_summary(config, bwn_summary_updated, dataset_id)

  message(sprintf("Writing national BWN summary to S3 at %s...", link))
  s3_write_csv(bwn_summary_updated, link, acl = "public-read")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(bwn_summary_updated)
}

#' Pointblank validations for the national BWN summary
#' @param config Main config
#' @param bwn_summary National BWN summary data frame
#' @param dataset_id "merged_national_bwn_summary"
validate_merged_national_bwn_summary <- function(config, bwn_summary, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  agent <- new_check_agent(bwn_summary, label = "National BWN Summary Validation") %>%
    check_column_complete(pwsid, severity = "warning") %>%
    check_column_complete(state, severity = "warning") %>%
    interrogate()

  result <- summarize_checks(agent)
  message(sprintf("Validation result summary: %s", result$summary))

  report_link <- write_check_artifacts(
    agent = agent, report_df = result$report_df,
    checks_base = checks_base, tag = dataset_id, run_ts = run_ts
  )
  message(sprintf("Validation reports pushed to S3: %s", report_link))

  if (isTRUE(result$any_error)) {
    stop(sprintf("VALIDATION FAILED: %s", result$summary), call. = FALSE)
  }

  message("National BWN summary validation checks passed successfully.")
  return(TRUE)
}

#' Roll the national BWN summary up into the highlevel (per-state/pwsid)
#' summary.
#' @param config Main config
#' @param dataset_id "merged_national_highlevel_summary"
#' @return The highlevel summary data frame
run_merged_national_highlevel_summary_pipeline <- function(config, dataset_id = "merged_national_highlevel_summary") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  bwn_summary_link <- sub_config$input_links$bwn_summary_link
  link <- sub_config$link

  message(sprintf("Downloading national BWN summary from %s...", bwn_summary_link))
  bwn_summary <- s3_read_csv(bwn_summary_link, coerce_character = FALSE)

  bwn_tidy_dates <- bwn_summary %>%
    mutate(date_issued_tidy = as.Date(date_issued, tryFormats = c("%Y-%m-%d"))) %>%
    # removing entries where we don't have pwsids & therefore wouldn't have a 
    # boundary to match to 
    filter(!is.na(pwsid)) %>%
    # there are a few records in WV that had years of 7024 (which 
    # hasn't happened yet as of writing)
    filter(!(date_issued_tidy > Sys.Date() & !is.na(date_issued_tidy)))

  message("Reading state data full methods sheet...")
  state_data_context_link <- config$metadata$state_data_context_link
  googlesheets4::gs4_deauth()
  tidy_data_context <- googlesheets4::read_sheet(state_data_context_link, sheet = "full_methods") %>%
    janitor::clean_names() %>%
    select(tidy_state, data_tool_tip, download_link) %>%
    filter(!is.na(data_tool_tip)) %>%
    rename(state = tidy_state)

  # Function for collapsing LA's BWA and BWN data together
  collapse_louisiana <- function(df) {
    df %>%
      mutate(state = case_when(
        state %in% c("Louisiana - BWA, 1yr", "Louisiana - BWN, 5yr") ~ "Louisiana",
        TRUE ~ state
      ))
  }

  state_year_ranges <- bwn_tidy_dates %>%
    collapse_louisiana() %>%
    group_by(state) %>%
    summarize(min_reporting_year_for_state = min(date_issued_tidy, na.rm = TRUE),
              max_reporting_year_for_state = max(date_issued_tidy, na.rm = TRUE)) %>%
    left_join(tidy_data_context, by = "state")

  highlevel_summary <- bwn_tidy_dates %>%
    collapse_louisiana() %>%
    # NOTE - there are multiple instances in AK, WV, and AR (and likely others)
    # where a water system reported multiple advisories in the same day to note 
    # specific communities that were affected by presumably the same event. 
    # HERE - we are ASSUMING that a water system would have maximum one BWN 
    # on a given day. NOTE if the state changes any of their dates (date lifted 
    # or date issued), this creates an entirely new record since it is impossible
    # for the worker/me to determine what is a new record vs an existing record that 
    # has been edited 
    unique() %>%
    group_by(state, pwsid) %>%
    # note there may be some states that we don't have date issued, but 
    # I'd still like to have them noted as an advisory 
    summarize(total_bwn = n(),
              date_of_first_advisory = min(date_issued_tidy),
              date_of_last_advisory = max(date_issued_tidy),
              .groups = "drop") %>%
    left_join(state_year_ranges, by = "state") %>%
    mutate(clean_date_range = paste0(
      "Based on the records provided by the state, data covers ",
      year(min_reporting_year_for_state), " to ",
      year(max_reporting_year_for_state)
    ))

  message("Validating merged_national_highlevel_summary...")
  validate_merged_national_highlevel_summary(config, highlevel_summary, dataset_id)

  message(sprintf("Writing national BWN highlevel summary to S3 at %s...", link))
  s3_write_csv(highlevel_summary, link, acl = "public-read")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(highlevel_summary)
}

#' Pointblank validations for the national BWN highlevel summary
#' @param config Main config
#' @param highlevel_summary Highlevel summary data frame
#' @param dataset_id "merged_national_highlevel_summary"
validate_merged_national_highlevel_summary <- function(config, highlevel_summary, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  agent <- new_check_agent(highlevel_summary, label = "National BWN Highlevel Summary Validation") %>%
    check_column_complete(pwsid, severity = "warning") %>%
    check_column_complete(state, severity = "warning") %>%
    rows_distinct(
      columns = vars(state, pwsid),
      actions = action_levels(warn_at = 1),
      label = "No duplicate state/pwsid pairs"
    ) %>%
    interrogate()

  result <- summarize_checks(agent)
  message(sprintf("Validation result summary: %s", result$summary))

  report_link <- write_check_artifacts(
    agent = agent, report_df = result$report_df,
    checks_base = checks_base, tag = dataset_id, run_ts = run_ts
  )
  message(sprintf("Validation reports pushed to S3: %s", report_link))

  if (isTRUE(result$any_error)) {
    stop(sprintf("VALIDATION FAILED: %s", result$summary), call. = FALSE)
  }

  message("National BWN highlevel summary validation checks passed successfully.")
  return(TRUE)
}
