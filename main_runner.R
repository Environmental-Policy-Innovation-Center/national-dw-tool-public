################################################################################
# Controlboard For Running Data Pipelines, Updating Registries, and Staging Data
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
  default = "TRUE",
  help = "TRUE/FALSE - Compiles variable and dataset registries from main_config and cleaned dataset schemas."
)
parser$add_argument(
  "--skip-pipeline",
  type = "character",
  default = "FALSE",
  help = "TRUE/FALSE - Skip running the pipeline function itself. Registry updates (and triggers) still run normally."
)
parser$add_argument(
  "--dev",
  type = "character",
  default = "FALSE",
  help = "TRUE/FALSE - Dynamically routes all S3 output and input paths to the development bucket."
)
parser$add_argument(
  "--run-triggers",
  type = "character",
  default = "TRUE",
  help = "TRUE/FALSE - After --run-pipeline's dataset succeeds, also run its downstream triggers (pipeline + registry update, cascading). Defaults to TRUE."
)

args <- parser$parse_args()

convert_to_bool <- function(val) {
  if (is.null(val)) return(FALSE)
  return(as.logical(toupper(trimws(val))))
}

dataset_id <- trimws(args$run_pipeline)
run_registry_flag <- convert_to_bool(args$update_registries)
skip_pipeline_flag <- convert_to_bool(args$skip_pipeline)
is_dev_mode <- convert_to_bool(args$dev)
run_triggers_flag <- convert_to_bool(args$run_triggers)

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
pipeline_router <- list(
  "raw_huc12" = run_huc12_pipeline,
  "raw_open_usts" = run_ust_pipeline,
  "clean_huc12_open_usts" = run_clean_huc12_open_usts_pipeline,
  "raw_imp_waters" = run_imp_waters_pipeline,
  "clean_huc12_imp_waters" = run_huc12_imp_waters_merge_pipeline,
  "raw_rmp_sites" = run_rmp_sites_pipeline,
  "clean_huc12_rmp_sites" = run_clean_huc12_rmp_sites_pipeline,
  "raw_npdes_permits" = run_npdes_pipeline,
  "clean_huc12_npdes" = run_clean_huc12_npdes_pipeline,
  "clean_pwsid_intake_well_huc12" = run_clean_pwsid_intake_well_huc12_pipeline,
  "raw_epa_sabs" = run_epa_sabs_pipeline, # manual pipeline
  "clean_epa_sabs" = run_clean_epa_sabs_pipeline,
  "clean_epa_sabs_crosswalk" = run_epa_sabs_xwalk_pipeline,
  "clean_epa_sabs_crosswalk_pct_change" = run_epa_sabs_xwalk_pct_change_pipeline,
  "raw_sabs_county_served" = run_sabs_county_served_pipeline,
  "clean_sabs_county_served" = run_clean_sabs_county_served_pipeline,
  "raw_sdwa" = run_sdwa_pipeline,
  "clean_sdwis_viols" = run_clean_sdwis_viols_pipeline,
  "raw_dwsrf" = run_dwsrf_pipeline,
  "clean_dwsrf" = run_clean_dwsrf_pipeline,
  "merged_pwsid_funded_highlevel_summary" = run_merged_pwsid_funded_highlevel_summary_pipeline,
  "raw_svi" = run_svi_pipeline,
  "clean_sabs_svi" = run_clean_sabs_svi_pipeline,
  "raw_cvi" = run_cvi_pipeline,
  "clean_sabs_cvi" = run_clean_sabs_cvi_pipeline,
  "raw_cejst" = run_cejst_pipeline,
  "clean_sabs_cejst" = run_clean_sabs_cejst_pipeline,
  "raw_ejscreen" = run_ejscreen_pipeline,
  "clean_sabs_ejscreen" = run_clean_sabs_ejscreen_pipeline,
  "merged_pwsid_npdes_usts_rmps_imp" = run_merged_pwsid_npdes_usts_rmps_imp_pipeline,
  "raw_ak_bwn" = run_ak_bwn_pipeline,
  "clean_ak_bwn" = run_clean_ak_bwn_pipeline,
  "raw_wv_bwn" = run_wv_bwn_pipeline,
  "clean_wv_bwn" = run_clean_wv_bwn_pipeline,
  "raw_mo_bwn" = run_mo_bwn_pipeline,
  "clean_mo_bwn" = run_clean_mo_bwn_pipeline,
  "raw_tx_bwn" = run_tx_bwn_pipeline,
  "clean_tx_bwn" = run_clean_tx_bwn_pipeline,
  "raw_la_bwn_5yr" = run_la_bwn_5yr_pipeline,
  "clean_la_bwn_5yr" = function(config, dataset_id) {
    run_clean_la_bwn_pipeline(config, dataset_id, state_label = "Louisiana - BWN, 5yr")
  },
  "raw_la_bwa_1yr" = run_la_bwa_1yr_pipeline,
  "clean_la_bwa_1yr" = function(config, dataset_id) {
    run_clean_la_bwn_pipeline(config, dataset_id, state_label = "Louisiana - BWA, 1yr")
  },
  "raw_me_bwn" = run_me_bwn_pipeline,
  "clean_me_bwn" = run_clean_me_bwn_pipeline,
  "raw_wa_bwn" = run_wa_bwn_pipeline,
  "clean_wa_bwn" = run_clean_wa_bwn_pipeline,
  "raw_ar_bwn" = run_ar_bwn_pipeline,
  "clean_ar_bwn" = run_clean_ar_bwn_pipeline,
  "raw_or_bwn" = run_or_bwn_pipeline,
  "clean_or_bwn" = run_clean_or_bwn_pipeline,
  "raw_nm_bwn" = run_nm_bwn_pipeline,
  "clean_nm_bwn" = run_clean_nm_bwn_pipeline,
  "raw_fl_bwn" = run_fl_bwn_pipeline,
  "clean_fl_bwn" = run_clean_fl_bwn_pipeline,
  "merged_national_bwn_summary" = run_merged_national_bwn_summary_pipeline,
  "merged_national_highlevel_summary" = run_merged_national_highlevel_summary_pipeline
)

#' Run a dataset's pipeline, update registries, and stage data (if necessary).
#' If these run successfully, recurse through the dataset's triggers.
#' @param config Main config
#' @param dataset_id Dataset to run
#' @param skip_pipeline_flag Whether to skip running the pipeline function
#' @param run_registry_flag Whether to run sync_dataset()
#' @param run_triggers_flag Whether to recursively run trigger pipelines
#' @param triggered_by dataset_id of the upstream dataset
process_dataset <- function(config, dataset_id,
                                       skip_pipeline_flag, run_registry_flag = TRUE,
                                       run_triggers_flag = TRUE,
                                       triggered_by = NULL) {
  pipeline_failed <- FALSE

  if (!skip_pipeline_flag) {
    message("==================================================")
    message(sprintf("Starting Pipeline for Dataset: %s", dataset_id))
    message("==================================================")

    if (!dataset_id %in% names(pipeline_router)) {
      stop(sprintf("Error: Dataset_id '%s' is not registered in the pipeline router.", dataset_id))
    }

    pipeline_failed <- tryCatch({
      message("Running pipeline function...")
      pipeline_function <- pipeline_router[[dataset_id]]
      if (!is.null(triggered_by) && "triggered_by" %in% names(formals(pipeline_function))) {
        pipeline_function(config, dataset_id, triggered_by = triggered_by)
      } else {
        pipeline_function(config, dataset_id)
      }
      message("Pipeline ran successfully.")
      FALSE
    }, error = function(e) {
      message(paste("PIPELINE FAILED with error: ", e$message))
      message("Writing error to dataset registry...")
      update_dataset_registry(config, dataset_id, fail_message = e$message)
      TRUE
    })
  }

  if (pipeline_failed) {
    message(sprintf("PIPELINE FAILED. Skipping registry update and triggers for '%s'.", dataset_id))
    return()
  }

  sync_failed <- FALSE
  if (run_registry_flag) {
    message("==================================================")
    message(sprintf("Starting Registry & Staging Sync for Dataset: %s", dataset_id))
    message("==================================================")

    sync_failed <- tryCatch({
      sync_dataset(config, dataset_id)
      message("Dataset synced successfully.")
      FALSE
    }, error = function(e) {
      message(paste("Sync failed with error:", e$message))
      TRUE
    })
  }

  if (sync_failed) {
    message(sprintf("SYNC FAILED: Skipping triggers for '%s'.", dataset_id))
    return()
  }

  if (!run_triggers_flag) {
    message(sprintf("--run-triggers set to FALSE. Skipping any downstream triggers for '%s'.", dataset_id))
    return()
  }

  triggers <- NULL
  if (!is.null(config[[dataset_id]])) {
    triggers <- config[[dataset_id]]$triggers
  }

  if (!is.null(triggers) && length(triggers) > 0) {
    message(sprintf("Running %d downstream trigger(s) for '%s'...", length(triggers), dataset_id))
    for (trigger_id in triggers) {
      message("==================================================")
      message(sprintf("Starting Triggered Dataset: %s", trigger_id))
      message("==================================================")
      process_dataset(config, trigger_id,
                                 skip_pipeline_flag = skip_pipeline_flag,
                                 run_registry_flag = run_registry_flag,
                                 run_triggers_flag = run_triggers_flag,
                                 triggered_by = dataset_id)
    }
  } else {
    message(sprintf("No downstream datasets defined for '%s'.", dataset_id))
  }
}

if (!is.null(args$run_pipeline)) {
  process_dataset(config, dataset_id,
                             skip_pipeline_flag = skip_pipeline_flag,
                             run_registry_flag = run_registry_flag,
                             run_triggers_flag = run_triggers_flag)
} else {
  message("ERROR: no pipeline dataset_id was set for the --run-pipeline flag.")
}
