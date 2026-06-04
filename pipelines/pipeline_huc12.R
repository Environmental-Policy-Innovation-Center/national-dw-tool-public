library(tidyverse)
library(sf)
library(aws.s3)
library(httr)

#' Run pipeline for pulling national HUC12 geometries
#' @param config Parsed configuration list
#' @param dataset_id "huc12"
run_huc12_pipeline <- function(config, dataset_id) {
  # Grab config variables
  # Consider passing in the config instead of pulling out config variables
  wbd_source_url <- config$wbd_source_url
  wbd_raw_link <- config$wbd_raw_link
  
  print("Pulling HUC12 geometries...")
  pull_huc12_geometries(wbd_source_url, wbd_raw_link)
  
  # Every time the huc12 geoms are updated, the huc12 and ust merge should run.
  # However, if we are running both huc12 and ust pipelines concurrently, we
  # can save on computing power by avoiding two calls to the merge pipeline.
  # This would require adding dependency logic to terraform to only trigger
  # the merge pipeline after both huc12 and ust complete successfully.
  print("Running HUC12 and UST merge pipeline...")
  # TODO: this pipeline would likely need config variables passed in
  # Consider passing the whole main_config to each pipeline and then pulling
  # out specific configs within the pipeline
  run_huc12_ust_merge_pipeline()
  
  print("HUC12 pipeline completed successfully.")
}

#' Pull and store national HUC12 geometries
#' @param download_url Remote URI for the USGS WBD zip file
pull_huc12_geometries <- function(wbd_source_url, wbd_raw_link) {
  print("Downloading National WBD Geodatabase zip archive (~1GB)...")
  tmp_zip <- tempfile(fileext = ".zip")
  on.exit(unlink(tmp_zip), add = TRUE)
  options(timeout = max(1200, getOption("timeout"))) # Longer timeout because file is large
  download.file(wbd_source_url, destfile = tmp_zip, mode = "wb", quiet = FALSE)

  # TODO: add pointblank validation
  
  print("Pushing raw WBD database archive to S3...")
  # This step took a long time
  aws.s3::put_object(
    file = tmp_zip,
    object = wbd_raw_link,
    multipart = TRUE
  )
}