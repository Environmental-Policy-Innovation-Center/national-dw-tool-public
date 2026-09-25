################################################################################
# Archive the 4 .RData bundles (national_water_system, national_bwn,
# national_environmental, national_socioeconomic) and rebuild them from the
# current pipeline's data.
#
# Usage: Rscript scripts/generate_national_rdata_lists.R
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
Sys.setenv("AWS_DEFAULT_REGION" = "us-east-1")
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

NATIONAL_LIST_PREFIX <- "national-dw-tool/clean/national/"
ARCHIVE_PREFIX <- "national-dw-tool/clean/national/archived_lists/"
archive_date <- format(Sys.Date(), "%m%d%Y")

#' Archive the current .RData at <name>.RData to archived_lists/<name>_archived_<date>.RData
#' via a server-side S3 copy.
#' @param name Bundle name, e.g. "national_water_system"
archive_national_list <- function(name) {
  key <- paste0(NATIONAL_LIST_PREFIX, name, ".RData")
  archived_key <- paste0(ARCHIVE_PREFIX, name, "_archived_", archive_date, ".RData")

  exists <- tryCatch({
    s3_client()$head_object(Bucket = s3_bucket(), Key = key)
    TRUE
  }, error = function(e) {
    if (inherits(e, "http_404")) return(FALSE)
    stop(sprintf("Could not check for existing %s: %s", key, conditionMessage(e)), call. = FALSE)
  })

  if (!exists) {
    message(sprintf("No existing %s found, skipping archive step.", key))
    return(invisible(FALSE))
  }

  message(sprintf("Archiving %s -> %s...", key, archived_key))
  s3_client()$copy_object(
    Bucket = s3_bucket(),
    Key = archived_key,
    CopySource = paste0(s3_bucket(), "/", key),
    ACL = "public-read"
  )
  invisible(TRUE)
}

#' Write a named list to S3 as a .RData file.
#' @param list_obj Named list to write
#' @param name Bundle name, e.g. "national_water_system"
write_national_list <- function(list_obj, name) {
  key <- paste0(NATIONAL_LIST_PREFIX, name, ".RData")
  tmp <- tempfile(fileext = ".RData")
  on.exit(unlink(tmp))
  saveRDS(list_obj, file = tmp)
  s3_write_file(tmp, key, acl = "public-read")
  message(sprintf("Wrote %s (%d items).", key, length(list_obj)))
}

#' Read a dataset's clean output from its main_config.json `link`.
#' @param dataset_id e.g. "clean_epa_sabs"
read_dataset_output <- function(dataset_id) {
  link <- config[[dataset_id]]$link
  ext <- tolower(tools::file_ext(link))
  switch(ext,
    "geojson" = s3_read_geojson(link),
    "gpkg"    = s3_read_gpkg(link),
    s3_read_csv(link, coerce_character = FALSE)
  )
}

################################################################################
# 1. national_water_system
################################################################################
message("Working on national_water_system...")
archive_national_list("national_water_system")

national_water_system <- list(
  epa_sabs = read_dataset_output("clean_epa_sabs"),
  sdwis_viols = read_dataset_output("clean_sdwis_viols"),
  dwsrf_funded_projects = read_dataset_output("clean_dwsrf"),
  pwsid_funded_highlevel_summary = read_dataset_output("merged_pwsid_funded_highlevel_summary")
)
write_national_list(national_water_system, "national_water_system")

################################################################################
# 2. national_bwn
################################################################################
message("Working on national_bwn...")
archive_national_list("national_bwn")

bwn_state_dataset_ids <- Filter(function(id) {
  state_label <- config[[id]]$bwn_state_label
  !is.null(state_label) && state_label != ""
}, setdiff(names(config), "metadata"))

message(sprintf("Discovered %d BWN state datasets: %s",
               length(bwn_state_dataset_ids), paste(bwn_state_dataset_ids, collapse = ", ")))

national_bwn <- setNames(
  lapply(bwn_state_dataset_ids, read_dataset_output),
  bwn_state_dataset_ids
)
national_bwn[["merged_national_bwn_summary"]] <- read_dataset_output("merged_national_bwn_summary")
national_bwn[["merged_national_highlevel_summary"]] <- read_dataset_output("merged_national_highlevel_summary")

write_national_list(national_bwn, "national_bwn")

################################################################################
# 3. national_environmental
################################################################################
message("Working on national_environmental...")
archive_national_list("national_environmental")

national_environmental <- list(
  pwsid_intake_well_huc12 = read_dataset_output("clean_pwsid_intake_well_huc12"),
  cvi = read_dataset_output("clean_sabs_cvi"),
  pwsid_npdes_usts_rmps_imp = read_dataset_output("merged_pwsid_npdes_usts_rmps_imp")
)
write_national_list(national_environmental, "national_environmental")

################################################################################
# 4. national_socioeconomic
################################################################################
message("Working on national_socioeconomic...")
archive_national_list("national_socioeconomic")

national_socioeconomic <- list(
  epa_sabs_xwalk = read_dataset_output("clean_epa_sabs_xwalk"),
  svi = read_dataset_output("clean_sabs_svi"),
  ejscreen = read_dataset_output("clean_sabs_ejscreen"),
  cejst = read_dataset_output("clean_sabs_cejst"),
  xwalk_pct_change_10yr = read_dataset_output("clean_epa_sabs_crosswalk_pct_change")
)
write_national_list(national_socioeconomic, "national_socioeconomic")

message("National RData lists successfully archived and generated.")
