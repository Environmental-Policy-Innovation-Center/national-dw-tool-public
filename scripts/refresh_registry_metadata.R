################################################################################
# This script is helpful for keeping the dataset_registry up-to-date without 
# running a pipeline and updating date_updated. Make sure main_config.json in
# S3 is also synced to any local changes before running this script.
#
# How to use:
# - Sync main_config.json to S3
# - Update the list of dataset_ids below
# - Run: Rscript scripts/refresh_registry_metadata.R
################################################################################

library(sf)
library(tidyverse)

invisible(lapply(list.files("./pipelines", full.names = TRUE, pattern = "\\.R$"), source))
source("functions/registry_updates.R")
source("functions/checks.R")
source("functions/s3_client.R")
source("functions/pipeline_helpers.R")
source("functions/bwn_helpers.R")
source("functions/census_xwalk_helpers.R")
source("functions/xwalk_census_geo_sabs.R")
source("functions/spatial_coverage.R")

options(scipen = 999)
Sys.setenv("AWS_DEFAULT_REGION" = 'us-east-1')
set_s3_bucket("tech-team-data")

message("Grabbing main config from S3...")
config_obj <- s3_client()$get_object(
  Bucket = s3_bucket(),
  Key = "national-dw-tool/pipeline-config/main_config.json"
)
config_raw <- rawToChar(config_obj$Body)

if (!jsonlite::validate(config_raw)) {
  stop("main_config.json from S3 is not valid JSON, check for syntax errors.")
}
config <- jsonlite::fromJSON(config_raw)

# REPLACE THIS LIST WITH THE DATASET_ID(S) YOU WANT UPDATED
dataset_ids <- c(
  "clean_la_bwa_1yr",
  "raw_la_bwa_1yr",
  "clean_la_bwn_5yr",
  "raw_la_bwn_5yr"
)

for (dataset_id in dataset_ids) {
  message(sprintf("Refreshing dataset_registry row for %s...", dataset_id))
  tryCatch({
    update_dataset_registry(config, dataset_id)
    message(sprintf("  OK: %s", dataset_id))
  }, error = function(e) {
    message(sprintf("  FAILED: %s - %s", dataset_id, e$message))
  })
}

message("Done.")
