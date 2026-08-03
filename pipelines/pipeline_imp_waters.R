#' Pull EPA impaired waters data, then run the HUC12/impaired waters merge pipeline
#' @param config Main config
#' @param dataset_id "raw_imp_waters"
run_imp_waters_pipeline <- function(config, dataset_id) {
  imp_waters <- update_raw_imp_waters(config, dataset_id)
  
  message("Running HUC12 and impaired waters merge pipeline...")
  run_huc12_imp_waters_merge_pipeline(config, "clean_huc12_imp_waters", imp_waters)
  
  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Pull, validate, and save impaired waters geojson
#' @param config Main config
#' @param dataset_id "raw_imp_waters"
update_raw_imp_waters <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link

  message("Pulling impaired waters data...")
  imp_waters <- arcpullr::get_table_layer(source_url)
  
  message("Validating raw_imp_waters...")
  validate_raw_imp_waters(config, imp_waters, dataset_id)
  
  message(sprintf("Writing raw_imp_waters to S3 to %s...", link))
  s3_write_csv(imp_waters, link)
  return(imp_waters)
}

#' Pointblank validations for raw impaired waters data
#' @param imp_waters Raw impaired waters data frame
#' @param dataset_id "raw_imp_waters"
validate_raw_imp_waters <- function(config, imp_waters, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()
  
  # Create a pointblank df
  checks_df <- tibble(
    row_count = nrow(imp_waters)
  )
  print(checks_df)
  
  agent <- new_check_agent(checks_df, label = "Impaired Waters Validation") %>%
    col_vals_gt(
      columns = vars(row_count),
      value = 0,
      actions = action_levels(stop_at = 1),
      label = "Impaired waters dataset has > 0 rows"
    ) %>%
    interrogate()
  
  result <- summarize_checks(agent)
  message(sprintf("Validation result summary: %s", result$summary))
  
  # Console report
  report_card <- get_agent_report(agent, display_table = FALSE)
  print(report_card)
  
  # Write HTML report and CSV to S3
  report_link <- write_check_artifacts(
    agent       = agent, 
    report_df   = result$report_df, 
    checks_base = checks_base, 
    tag         = dataset_id, 
    run_ts      = run_ts
  )
  message(sprintf("Validation reports successfully pushed to S3: %s", report_link))
  
  # Abort without treating warnings as failures
  if (isTRUE(result$any_error)) {
    stop(sprintf("VALIDATION FAILED: %s", result$summary), call. = FALSE)
  }
  
  message("Impaired waters validation checks passed successfully.")
  return(TRUE)
}

#' Summarize stream counts by HUC12.
#' Can only be run within run_imp_waters_pipeline not through main_runner.
#' Note: the HUC12 summary can have duplicates since streams can extend beyond a single HUC.
#' @param config Main config
#' @param dataset_id "clean_huc12_imp_waters"
#' @param imp_waters Raw impaired waters data frame passed from update_raw_imp_waters()
run_huc12_imp_waters_merge_pipeline <- function(config, dataset_id = "clean_huc12_imp_waters", imp_waters) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  link <- sub_config$link
  
  imp_waters_summary <- imp_waters %>%
    group_by(huc12) %>%
    summarize(assessed_streams = sum(isassessed == "Y"), 
              not_assessed = sum(isassessed == "N"),
              impaired_streams = sum(isimpaired == "Y"), 
              threatened_streams = sum(isthreatened == "Y"), 
              # NOTE - this does not include all impaired waters - units that are 
              # impaired but have a TMDL would not be on this list
              streams_303d_list = sum(on303dlist == "Y")) %>%
    mutate(last_epic_run_date = Sys.Date())
  
  message("Validating clean_huc12_imp_waters...")
  validate_huc12_imp_waters_summary(config, imp_waters_summary, dataset_id)

  message("Writing clean_huc12_imp_waters to S3...")
  s3_write_csv(imp_waters_summary, link)
  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Pointblank validations for the HUC12/impaired-waters merge summary
#' @param config Main config
#' @param imp_waters_summary Stream counts summarized by HUC12
#' @param dataset_id "clean_huc12_imp_waters"
validate_huc12_imp_waters_summary <- function(config, imp_waters_summary, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  checks_df <- imp_waters_summary %>%
    mutate(huc12_valid_format = grepl("^[0-9]{12}$", huc12))
  print(checks_df)

  agent <- new_check_agent(checks_df, label = "HUC12/Impaired Waters Merge Validation") %>%
    check_row_count_range(min_rows = 1, max_rows = 110000, severity = "warning") %>%
    check_column_complete(huc12, severity = "warning") %>%
    check_column_all_true(huc12_valid_format, severity = "warning") %>%
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

  message("HUC12/impaired waters merge validation checks passed successfully.")
  return(TRUE)
}


# Keep notes for record:
# imp_metadata <- get_table_layer("https://gispub.epa.gov/arcgis/rest/services/OW/ATTAINS_Assessment/MapServer/10")
# isassessed = If the state has monitored a water and made an Assessment decision
#     about the Assessment Unit, it is considered Assessed.  If the state has 
#     defined the Assessment Unit but has not yet monitored and assessed it, 
#     then it is Not Assessed.
# impaired = If any part of the Assessment Unit fails to meet its water quality
#     standards, it is calculated as impaired.
# threatened = Threatened means that one or more Uses is Fully Supporting but 
#     experiencing a declining trend and likely to become impaired in the next 
#     reporting cycle.  Waters that are Threatened are part of the Clean Water 
#     Act Section 303(d) list, unless a TMDL has been created for them.  A null
#     value is the same as isThreatened = 'N'.
# 303d list: If the Assessment Unit is impaired by a pollutant and still needs 
#     to be addressed by a TMDL or other pollution control measure, it falls on 
#     the Clean Water Act (CWA) Section 303(d) List (which is also known as EPA 
#     IR Category 5).  Note:  This does not include all impaired waters.  For 
#     example, Assessment Units that are impaired but already have a TMDL would
#     fall into EPA IR Category 4a, and would not be on the CWA Section 303(d) list.
