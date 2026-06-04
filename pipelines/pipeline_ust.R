library(aws.s3)
library(arcpullr)
library(tidyverse)
library(sf)
library(janitor)

#' Pull EPA Underground Storage Tank Points
#' @param config Parsed configuration list
#' @param dataset_id "ust"
run_ust_pipeline <- function(config, dataset_id) {
  # Grab config variables
  ust_source_url <- config$ust_source_url
  ust_raw_link <- config$ust_raw_link
  ust_clean_link <- config$ust_clean_link
  
  print("Fetching spatial layer from EPA ArcGIS REST API...")
  ust_raw <- arcpullr::get_spatial_layer(ust_source_url)
  
  ust_tidy <- ust_raw %>%
    janitor::clean_names() %>%
    filter(facility_status == "Open UST(s)") %>%
    mutate(last_epic_run_date = Sys.Date())
  
  # TODO: add pointblank validation
  
  print("Saving UST dataset to S3...")
  tmp <- tempfile(fileext = ".geojson")
  on.exit(unlink(tmp), add = TRUE)
  st_write(ust_tidy, dsn = tmp, quiet = TRUE)
  
  put_object(
    file = tmp,
    object = ust_raw_link,
    bucket = "tech-team-data",
    multipart = TRUE
  )
  print("UST Pipeline completed successfully.")
  
  # Every time the huc12 geoms are updated, the huc12 and ust merge should happen
  # However, if we are running both huc12 and ust pipelines concurrently, we
  # can save on computing power by avoiding two calls to the merge pipeline.
  # This would require adding dependency logic to terraform to only trigger
  # the merge pipeline after both huc12 and ust complete successfully.
  # TODO: this pipeline would likely need config variables passed in
  # Consider passing the whole main_config to each pipeline and then pulling
  # out specific configs within the pipeline
  run_huc12_ust_merge_pipeline()
}