library(aws.s3)
library(arcpullr)
library(tidyverse)
library(sf)
library(janitor)

#' Pull EPA Underground Storage Tank points
#' @param config Main config
#' @param dataset_id "ust"
run_ust_pipeline <- function(config, dataset_id = "ust") {
  print("Grabbing config variables...")
  sub_config <- config[[dataset_id]]
  ust_source_url <- sub_config$source_url
  ust_raw_link <- sub_config$raw_link
  
  print("Fetching spatial layer from EPA ArcGIS REST API...")
  ust_raw <- arcpullr::get_spatial_layer(ust_source_url)
  
  ust_tidy <- ust_raw %>%
    janitor::clean_names() %>%
    filter(facility_status == "Open UST(s)") %>%
    mutate(last_epic_run_date = Sys.Date())
  
  validate_raw_ust(ust_tidy)
  
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
  
  # Every time the huc12 geoms are updated, the huc12 and ust merge should happen
  # However, if we are running both huc12 and ust pipelines concurrently, we
  # can save on computing power by avoiding two calls to the merge pipeline.
  # This would require adding dependency logic to terraform to only trigger
  # the merge pipeline after both huc12 and ust complete successfully.
  print("Running HUC12 and UST merge pipeline...")
  run_huc12_ust_merge_pipeline(config)
  print("UST Pipeline completed successfully.")
}

#' Validate raw UST data
#' @param ust_tidy Cleaned spatial df containing only open tanks
validate_raw_ust <- function(ust_path) {
  checks_base <- "s3://tech-team-data/national-dw-tool/development/validation"
  run_ts      <- Sys.time()
  
  print("Reading UST GeoJSON file into spatial object...")
  sf_obj <- tryCatch(
    sf::st_read(ust_path, quiet = TRUE),
    error = function(e) {
      stop(sprintf("VALIDATION FAILED: Unable to read GeoJSON file. Error: %s", conditionMessage(e)), call. = FALSE)
    }
  )
  
  coords <- sf::st_coordinates(sf_obj)
  checks_df <- sf_obj %>%
    mutate(
      geometry_valid = sf::st_is_valid(.),
      lon = coords[, 1],
      lat = coords[, 2]
    ) %>%
    sf::st_drop_geometry() %>%
    as_tibble()

  agent <- new_check_agent(checks_df, label = "Open UST GeoJSON Validation") %>%
    check_row_count_range(min_rows = 150000, max_rows = 200000, severity = "stop") %>%
    check_column_complete(column = objectid, severity = "warning") %>%
    check_column_complete(column = facility_id, severity = "warning") %>%
    col_vals_between(
      columns = vars(lon), left = -180, right = 180,
      actions = action_levels(warn_at = 1),
      label   = "Longitude is within CONUS + OCUNUS bounds"
    ) %>%
    col_vals_between(
      columns = vars(lat), left = -90, right = 90,
      actions = action_levels(warn_at = 1),
      label   = "Latitude is within CONUS + OCUNUS bounds"
    ) %>%
    check_column_all_true(column = geometry_valid, severity = "warning") %>%
    interrogate()
  
  # Below code checks facilities with longitude/latitude outside CONUS/OCONUS range:
  # June 17, 2026 run found 91 facilities with NA for lat and long
  # print("------------------------------------------")
  # bad_longitudes <- checks_df %>%
  #   filter(lon < -180 | lon > 180 | is.na(lon))
  # total_bad_lon <- nrow(bad_longitudes)
  # print(sprintf("Total facilities with bad or missing longitude: %d", total_bad_lon))
  # print(bad_longitudes, n = 90)
  # 
  # bad_latitudes <- checks_df %>%
  #   filter(lat < -90 | lat > 90 | is.na(lat))
  # total_bad_lat <- nrow(bad_latitudes)
  # print(sprintf("Total facilities with bad or missing latitude: %d", total_bad_lat))
  # print(bad_latitudes, n = 90)
  # print("------------------------------------------")

  result <- summarize_checks(agent)
  print(sprintf("Validation result summary: %s", result$summary))
  print(get_agent_report(agent))
  
  # Write HTML report and CSV to S3
  report_link <- write_check_artifacts(
    agent       = agent, 
    report_df   = result$report_df, 
    checks_base = checks_base, 
    tag         = "ust_geojson",
    run_ts      = run_ts
  )
  print(sprintf("Validation reports successfully pushed to S3: %s", report_link))
  
  # Abort without treating warnings as failures
  if (isTRUE(result$any_error)) {
    stop(sprintf("VALIDATION FAILED: %s. Review full report at: %s", result$summary, report_link), call. = FALSE)
  }
  
  message("UST geojson validation checks passed successfully.")
  return(TRUE)
}

# For testing locally downloaded ust geojson
# validate_raw_ust("./local_data/open_usts.geojson")
