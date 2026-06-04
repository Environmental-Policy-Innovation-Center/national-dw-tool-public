################################################################################
# Controlboard for running all data pipelines
################################################################################

library(jsonlite)
library(aws.s3)

# Load all utility files
# list.files("R", full.names = TRUE, pattern = "\\.R$") %>% walk(source)
# TODO: switch to loading all utility files once old files are removed
# For now, specify new files to load
source("./pipelines")

# Command arguments from Docker/Terraform
args <- commandArgs(trailingOnly = TRUE)
dataset_id <- args[1]

# Specify the correct bucket region for IAM role
Sys.setenv("AWS_DEFAULT_REGION" = 'us-east-1')

# Pipeline Router Map - maps dataset_id to correct pipeline run function
pipeline_router <- list(
  "huc12" = run_huc12_pipeline,
  "ust" = run_ust_pipeline,
  "huc12_ust_merge" = run_huc12_ust_merge_pipeline
  # "ak_bwn" = run_bwn_pipeline
  # "dwsrf" = run_dwsrf_pipeline
)

print("==================================================")
sprintf("Starting Pipeline for Dataset: '%s", dataset_id)
print("==================================================")

if (!dataset_id %in% names(pipeline_router)) {
  stop(sprintf("Error: Dataset_id '%s' is not registered in the pipeline router.", dataset_id))
}

tryCatch({
  # Grab specific dataset config from main config in S3
  print("Grabbing config...")
  config_raw <- aws.s3::s3read_using(readLines, object = "s3://tech-team-data/national-dw-tool/pipeline-configs/main_config.json")
  config <- jsonlite::fromJSON(config_raw)[[dataset_id]]

  # Run matching pipeline function from pipeline router map
  print("Running pipeline function...")
  pipeline_function <- pipeline_router[[dataset_id]]
  pipeline_function(config, dataset_id)
  
  print("Pipeline ran successfully.")
}, error = function(e) {
  print("Pipeline failed.")
  stop("Exiting pipeline.")
})