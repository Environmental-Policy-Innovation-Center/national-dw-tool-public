################################################################################
# Controlboard For Running Data Pipelines, Updating Registries, and Staging Data
################################################################################

library(argparse)
library(jsonlite)
library(aws.s3)

# Load all utility files
# list.files("R", full.names = TRUE, pattern = "\\.R$") %>% walk(source)
# TODO: switch to loading all utility files once old files are removed
# For now, specify new files to load
targets::tar_source("./pipelines")
source("functions/registry_updates.R")

# Specify the correct bucket region for IAM role
Sys.setenv("AWS_DEFAULT_REGION" = 'us-east-1')

################################################################################
# Command-Line Arguments
################################################################################
# Define parser interface
parser <- ArgumentParser(description = "National Drinking Water Tool - Main Runner")
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
  help = "TRUE/FALSE - Turn off pipeline runs for testing."
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

print("==================================================")
print("Starting Main Runner")
print("==================================================")

print("Grabbing main config...")
config <- aws.s3::s3read_using(
  jsonlite::fromJSON, 
  object = "s3://tech-team-data/national-dw-tool/development/pipeline-config/main_config.json"
)

config_metadata <- aws.s3::head_object(
  object = "s3://tech-team-data/national-dw-tool/development/pipeline-config/main_config.json"
)
config_last_updated <- attr(config_metadata, "last-modified")
sprintf("Config last updated in AWS on: %s", config_last_updated)

################################################################################
# Run Pipeline
################################################################################
# Pipeline Router Map - maps dataset_id to correct pipeline run function
pipeline_router <- list(
  "huc12" = run_huc12_pipeline,
  "ust" = run_ust_pipeline,
  "huc12_ust_merge" = run_huc12_ust_merge_pipeline,
  "ak_bwn" = run_ak_bwn_pipeline,
  "ar_bwn" = run_ar_bwn_pipeline
  # "dwsrf" = run_dwsrf_pipeline,
  # "all_bwn" = run_bwn_merge_pipeline
)

if (!is.null(args$run_pipeline) && !testing_flag) {
  print("==================================================")
  print(sprintf("Starting Pipeline for Dataset: %s", dataset_id))
  print("==================================================")
  
  if (!dataset_id %in% names(pipeline_router)) {
    stop(sprintf("Error: Dataset_id '%s' is not registered in the pipeline router.", dataset_id))
  }
  
  tryCatch({
    print("Running pipeline function...")
    pipeline_function <- pipeline_router[[dataset_id]]
    pipeline_function(config, dataset_id)
    
    print("Pipeline ran successfully.")
  }, error = function(e) {
    print(paste("Pipeline failed with error: ", e$message))
    stop("Exiting main runner.")
  })
}

################################################################################
# Update Variable and Dataset Registries
################################################################################
if (!is.null(args$run_pipeline) && run_registry_flag) {
  print("==================================================")
  print(sprintf("Starting Registry Update for Dataset: %s", dataset_id))
  print("==================================================")
  
  tryCatch({
    update_registries(config, dataset_id)
    print("Registries updated successfully.")
  }, error = function(e) {
    print(paste("Registries update failed:", e$message))
    stop("Exiting main runner.")
  })
}

################################################################################
# Stage Data
################################################################################
if (run_staging_flag) {
  print("==================================================")
  print("Starting Data Staging")
  print("==================================================")
  
  tryCatch({
    stage_data(config)
    print("Data staged and written successfully.")
  }, error = function(e) {
    print(paste("Data staging failed:", e$message))
    stop("Exiting main runner.")
  })
}