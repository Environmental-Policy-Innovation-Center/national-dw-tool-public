################################################################################
# Controlboard For Running Data Pipelines, Updating Registries, and Staging Data
################################################################################

library(sf)
library(tidyverse)

lapply(list.files("./pipelines", full.names = TRUE, pattern = "\\.R$"), source)
source("functions/registry_updates.R")
source("functions/checks.R")
source("functions/s3_client.R")
source("functions/pipeline_helpers.R")
source("functions/bwn_helpers.R")
source("functions/xwalk_census_geo_sabs.R")

options(scipen = 999)

# Specify the correct bucket region for IAM role
Sys.setenv("AWS_DEFAULT_REGION" = 'us-east-1')

set_s3_bucket("tech-team-data")

################################################################################
# Parse Command-Line Arguments
################################################################################
parser <- argparse::ArgumentParser(description = "National Drinking Water Tool - Main Runner")
parser$add_argument(
  "--run-pipeline", 
  type = "character", 
  default = NULL,
  help = "The unique dataset_id string to execute (e.g., dwsrf, ust). Leave blank to skip."
)
parser$add_argument(
  "--update-registries", 
  type = "character", 
  default = "FALSE",
  help = "TRUE/FALSE - Compiles variable and dataset registries from main_config and cleaned dataset schemas."
)
parser$add_argument(
  "--stage-data", 
  type = "character", 
  default = "FALSE",
  help = "TRUE/FALSE - Merges cleaned datasets for staging tool."
)
parser$add_argument(
  "--testing", 
  type = "character", 
  default = "FALSE",
  help = "TRUE/FALSE - Turn off pipeline runs for testing. Only runs registry updates and staging"
)
parser$add_argument(
  "--dev", 
  type = "character", 
  default = "FALSE",
  help = "TRUE/FALSE - Dynamically routes all S3 output and input paths to the development bucket."
)

args <- parser$parse_args()

convert_to_bool <- function(val) {
  if (is.null(val)) return(FALSE)
  return(as.logical(toupper(trimws(val))))
}

dataset_id <- trimws(args$run_pipeline)
run_registry_flag <- convert_to_bool(args$update_registries)
run_staging_flag  <- convert_to_bool(args$stage_data)
testing_flag <- convert_to_bool(args$testing)
is_dev_mode <- convert_to_bool(args$dev)

message("==================================================")
message("Starting Main Runner")
message("==================================================")
message("WARNING: MAKE SURE main_config.json IS SYNCED IN S3")
config_metadata <- s3_client()$head_object(
  Bucket = s3_bucket(),
  Key = "national-dw-tool/development/pipeline-config/main_config.json"
)
message(sprintf("Config last updated in AWS on: %s", config_metadata$LastModified))

message("Grabbing main config...")
config_obj <- s3_client()$get_object(
  Bucket = s3_bucket(),
  Key = "national-dw-tool/development/pipeline-config/main_config.json"
)
config_raw <- rawToChar(config_obj$Body)

message("Validating main config JSON syntax...")
if (!jsonlite::validate(config_raw)) {
  stop("MAIN RUNNER FAILED: main_config.json is not valid JSON, check for syntax errors.")
}
config <- jsonlite::fromJSON(config_raw)

if (is_dev_mode) {
  message("IN DEVELOPMENT MODE: Remapping config to route to dev bucket...")
  config <- remap_to_dev(config)
}

################################################################################
# Run Pipeline
################################################################################
# Pipeline Router Map - maps dataset_id to correct pipeline run function
# Pipelines that are excluded from router and can't be run through main_runner:
# "raw_intake" - included in main_config for updates to dataset_registry
# "raw_wells" - included in main_config for updates to dataset_registry
# "clean_huc12_imp_waters" = run_huc12_imp_waters_pipeline
# "clean_huc12_rmp_sites" = run_huc12_rmp_sites_pipeline
pipeline_router <- list(
  "raw_huc12" = run_huc12_pipeline,
  "raw_open_usts" = run_ust_pipeline,
  "clean_huc12_open_usts" = run_clean_huc12_open_usts_pipeline,
  "raw_imp_waters" = run_imp_waters_pipeline,
  "raw_rmp_sites" = run_rmp_sites_pipeline,
  "raw_npdes_permits" = run_npdes_pipeline,
  "clean_huc12_npdes" = run_clean_huc12_npdes_pipeline,
  "clean_pwsid_intake_well_huc12" = run_clean_pwsid_intake_well_huc12_pipeline,
  "raw_epa_sabs" = run_epa_sabs_pipeline, # manual pipeline
  "clean_epa_sabs" = run_clean_epa_sabs_pipeline,
  "raw_svi" = run_svi_pipeline,
  "clean_sabs_svi" = run_clean_sabs_svi_pipeline,
  "raw_cejst" = run_cejst_pipeline,
  "clean_sabs_cejst" = run_clean_sabs_cejst_pipeline,
  "raw_ejscreen" = run_ejscreen_pipeline,
  "clean_sabs_ejscreen" = run_clean_sabs_ejscreen_pipeline,
  "staged_pwsid_npdes_usts_rmps_imp" = run_staged_pwsid_npdes_usts_rmps_imp_pipeline,
  "raw_ak_bwn" = run_ak_bwn_pipeline,
  "raw_wv_bwn" = run_wv_bwn_pipeline,
  "raw_mo_bwn" = run_mo_bwn_pipeline
  # "dwsrf" = run_dwsrf_pipeline,
  # "all_bwn" = run_bwn_merge_pipeline
)

if (!is.null(args$run_pipeline) && !testing_flag) {
  message("==================================================")
  message(sprintf("Starting Pipeline for Dataset: %s", dataset_id))
  message("==================================================")
  
  if (!dataset_id %in% names(pipeline_router)) {
    stop(sprintf("Error: Dataset_id '%s' is not registered in the pipeline router.", dataset_id))
  }
  
  tryCatch({
    message("Running pipeline function...")
    pipeline_function <- pipeline_router[[dataset_id]]
    pipeline_function(config, dataset_id)
    message("Pipeline ran successfully.")
  }, error = function(e) {
    message(paste("Pipeline failed with error: ", e$message))
    message("Writing error to dataset registry...")
    update_dataset_registry(config, dataset_id, fail_message = e$message)
    stop("Exiting main runner.")
  })
}

################################################################################
# Update Variable and Dataset Registries
################################################################################
if (!is.null(args$run_pipeline) && run_registry_flag) {
  message("==================================================")
  message(sprintf("Starting Registry Update for Dataset: %s", dataset_id))
  message("==================================================")
  
  tryCatch({
    update_registries(config, dataset_id)
    message("Registries updated successfully.")
  }, error = function(e) {
    message(paste("Registries update failed with error:", e$message))
  })
  
  message("Updating downstream dataset registries...")
  triggers <- NULL
  if (!is.null(config[[dataset_id]])) {
    triggers <- config[[dataset_id]]$triggers
  }
  
  if (!is.null(triggers) && length(triggers) > 0) {
    for (trigger_id in triggers) {
      message("==================================================")
      message(sprintf("Starting Registry Update for Triggered Dataset: %s", trigger_id))
      message("==================================================")
      
      tryCatch({
        update_registries(config, trigger_id)
        message("Registries updated successfully.")
      }, error = function(e) {
        message(paste("Registries update failed with error:", e$message))
      })
    }
  } else {
    message(sprintf("No downstream datasets defined for '%s'.", dataset_id))
  }
}

################################################################################
# Stage Data
################################################################################
if (run_staging_flag) {
  message("==================================================")
  message("Starting Data Staging")
  message("==================================================")
  
  tryCatch({
    stage_data(config)
    message("Data staged and written successfully.")
  }, error = function(e) {
    message(paste("Data staging failed with error:", e$message))
    stop("Exiting main runner.")
  })
}