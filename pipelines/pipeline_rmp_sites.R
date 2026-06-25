library(tidyverse)
library(janitor)
library(arcpullr)
library(sf)
library(tigris)

options(tigris_use_cache = TRUE)
options(timeout = 900) 

#' @param config Main config
#' @param dataset_id "raw_rmp_sites"
run_rmp_sites_pipeline <- function(config, dataset_id) {
  active_rmp <- update_raw_rmp_sites(config, dataset_id)
  
  print("Running HUC12 and RMP sites merge pipeline...")
  run_clean_huc12_rmp_sites_pipeline(config, active_rmp, "clean_huc12_rmp_sites")
  
  print(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Pull, validate, and store raw RMP sites geojson
#' @param source_url Source url for RMP feature server layer
#' @param link Target s3 link for raw geojson file
update_raw_rmp_sites <- function(config, dataset_id) {
  print(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link

  print("Pulling active RMP sites data...")
  rmp <- get_spatial_layer(source_url)

  active_rmp <- rmp %>%
    janitor::clean_names() %>%
    filter(active_status == "ACTIVE") %>%
    mutate(last_epic_run_date = Sys.Date())
  
  print("Validating raw RMP data...")
  validate_raw_rmp_sites(active_rmp, dataset_id)
  
  print("Writing raw_rmp_sites to S3...")
  tmp <- tempfile(fileext = ".geojson")
  on.exit(unlink(tmp), add = TRUE)
  st_write(active_rmp, dsn = tmp, quiet = TRUE)
  put_object(
    file = tmp,
    object = link,
    bucket = "tech-team-data",
    multipart = TRUE
  )
  
  return(active_rmp)
}

#' Pointblank validations for active RMP dataset
validate_raw_rmp_sites <- function(active_rmp, dataset_id) {
  checks_base <- "s3://tech-team-data/national-dw-tool/development/validation"
  run_ts <- Sys.time()
  
  crs_epsg <- sf::st_crs(active_rmp)$epsg
  
  checks_df <- tibble(
    row_count = nrow(active_rmp),
    crs_epsg  = ifelse(is.null(crs_epsg), 0, crs_epsg)
  )
  print(checks_df)
  
  agent <- new_check_agent(checks_df, label = "RMP Sites Validation") %>%
    col_vals_gt(
      columns = vars(row_count),
      value = 0,
      actions = action_levels(stop_at = 1),
      label = "RMP dataset has > 0 rows"
    ) %>%
    col_vals_equal(
      columns = vars(crs_epsg),
      value = 4326,
      actions = action_levels(warn_at = 1),
      label = "Dataset uses WGS84 projection (EPSG 4326)"
    ) %>%
    interrogate()
  
  result <- summarize_checks(agent)
  print(sprintf("Validation result summary: %s", result$summary))
  
  # Write HTML report/CSV to S3
  report_link <- write_check_artifacts(
    agent       = agent, 
    report_df   = result$report_df, 
    checks_base = checks_base, 
    tag         = dataset_id, 
    run_ts      = run_ts
  )
  print(sprintf("Validation reports pushed to S3: %s", report_link))
  
  if (isTRUE(result$any_error)) {
    stop(sprintf("VALIDATION FAILED: %s", result$summary), call. = FALSE)
  }
  
  print("RMP validation checks passed successfully.")
  return(TRUE)
}

#' Intersect RMP sites with national HUC12 layer
#' @param config Main config
#' @param active_rmp sf data frame passed from upstream pull step
#' @param dataset_id "clean_huc12_rmp_sites"
run_clean_huc12_rmp_sites_pipeline <- function(config, active_rmp, dataset_id = "clean_huc12_rmp_sites") {
  print(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  wbd_raw_link <- sub_config$input_links$wbd_raw_link
  link <- sub_config$link
  
  # TODO: wrap this in a function since it's used in the ust merge pipeline as well
  print("Downloading raw HUC12 geodatabase layer from S3...")
  tmp_zip <- tempfile(fileext = ".zip")
  on.exit(unlink(tmp_zip), add = TRUE)
  save_object(object = wbd_raw_link, bucket = "tech-team-data", file = tmp_zip)
  
  print("Unzipping HUC12 file...")
  ex_dir <- tempdir()
  on.exit(unlink(ex_dir, recursive = TRUE), add = TRUE)
  unzip(tmp_zip, exdir = ex_dir)
  gdb_path <- list.dirs(ex_dir, full.names = TRUE, recursive = TRUE)
  gdb_path <- gdb_path[grep("\\.gdb$", gdb_path)][1]
  
  huc12_geoms <- st_read(gdb_path, layer = "WBDHU12", quiet = TRUE) %>%
    st_transform(crs = 5070)
  
  print("Preparing RMP sites...")
  rmps <- active_rmp %>%
    st_transform(crs = 5070)
  
  print("Computing national HUC12 intersection...")
  # Turn off spherical geometry for running intersections
  sf_use_s2(FALSE)
  rmp_huc12 <- st_join(rmps, huc12_geoms, join = st_within, left = FALSE)
  sf_use_s2(TRUE)
  
  if (nrow(rmp_huc12) == 0) {
    stop("HUC12 and RMP spatial intersection failed - 0 records.")
  }
  
  print("Summarizing RMP facilities by HUC12...")
  rmp_huc12_summary <- rmp_huc12 %>%
    st_drop_geometry() %>%
    group_by(huc12) %>%
    summarize(
      total_facilities_w_rmps = n_distinct(registry_id),
      .groups = "drop"
    )
  
  # TODO: Validate summary data
  # Check that active_rmp and rmp_huc12 have same number of rows

  print("Writing clean dataset to S3...")
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp), add = TRUE)
  write.csv(rmp_huc12_summary, tmp, row.names = F)
  put_object(
    file = tmp,
    object = link,
    bucket = "tech-team-data",
    multipart = TRUE
  )

  print(sprintf("%s pipeline completed successfully.", dataset_id))
}