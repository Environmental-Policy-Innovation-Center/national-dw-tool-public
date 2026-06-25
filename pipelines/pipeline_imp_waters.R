library(tidyverse)
library(janitor)
library(arcpullr)

# no scientific notation 
options(scipen = 999)
options(timeout = 900) # this should bump to 15 mins 

#' @param config Main config
#' @param dataset_id "raw_imp_waters"
run_imp_waters_pipeline <- function(config, dataset_id) {
  imp_waters <- update_raw_imp_waters(config, dataset_id)
  
  print("Running HUC12 and impaired waters merge pipeline...")
  run_huc12_imp_waters_merge_pipeline(config, imp_waters, "clean_huc12_imp_waters")
  
  print(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Pull and store impaired waters geojson
#' @param source_url Source url for impaired waters data
update_raw_imp_waters <- function(config, dataset_id) {
  print(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link

  print("Pulling impaired waters data...")
  imp_waters <- get_table_layer(source_url)
  
  print("Validating raw_imp_waters...")
  validate_raw_imp_waters(imp_waters, dataset_id)
  
  print(sprintf("Writing raw_imp_waters to S3 to %s...", link))
  tmp <- tempfile()
  write.csv(imp_waters, paste0(tmp, ".csv"), row.names = F)
  on.exit(unlink(tmp))
  put_object(
    file = paste0(tmp, ".csv"),
    object = link,
    bucket = "tech-team-data",
    multipart = T
  )
  return(imp_waters)
}

validate_raw_imp_waters <- function(imp_waters, dataset_id) {
  # Bucket for validation reports
  checks_base <- "s3://tech-team-data/national-dw-tool/development/validation"
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
  print(sprintf("Validation result summary: %s", result$summary))
  
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
  print(sprintf("Validation reports successfully pushed to S3: %s", report_link))
  
  # Abort without treating warnings as failures
  if (isTRUE(result$any_error)) {
    stop(sprintf("VALIDATION FAILED: %s", result$summary), call. = FALSE)
  }
  
  print("Impaired waters validation checks passed successfully.")
  return(TRUE)
}

# This pipeline can only run within run_imp_waters_pipeline, not through main router.
# Note: HUC12 summary can have duplicates due to streams extending beyond a single HUC
run_huc12_imp_waters_merge_pipeline <- function(config, imp_waters, dataset_id = "clean_huc12_imp_waters") {
  print(sprintf("Grabbing config variables for dataset %s...", dataset_id))
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
  
  print("Writing clean_huc12_imp_waters to S3...")
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp), add = TRUE)
  write.csv(imp_waters_summary, tmp, row.names = F)
  put_object(
    file = tmp,
    object = link,
    bucket = "tech-team-data",
    multipart = TRUE
  )
  print(sprintf("%s pipeline completed successfully.", dataset_id))
}


# Keep for now:
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
