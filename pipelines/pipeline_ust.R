library(arcpullr)
library(tidyverse)
library(sf)
library(janitor)

#' Pull EPA Underground Storage Tank points
#' @param config Main config
#' @param dataset_id "ust"
run_ust_pipeline <- function(config, dataset_id) {
  update_raw_ust(config, dataset_id)

  print("Running HUC12 and UST merge pipeline...")
  run_clean_huc12_open_usts_pipeline(config, "clean_huc12_open_usts")

  print(sprintf("%s pipeline completed successfully.", dataset_id))
}

update_raw_ust <- function(config, dataset_id) {
  print(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  
  print("Fetching spatial layer from EPA ArcGIS REST API...")
  ust_raw <- arcpullr::get_spatial_layer(source_url)
  
  ust_tidy <- ust_raw %>%
    janitor::clean_names() %>%
    filter(facility_status == "Open UST(s)") %>%
    mutate(last_epic_run_date = Sys.Date())
  
  print("Validating raw USTs...")
  validate_raw_ust(ust_tidy, dataset_id)
  
  print("Saving UST dataset to S3...")
  tmp <- tempfile(fileext = ".geojson")
  on.exit(unlink(tmp), add = TRUE)
  st_write(ust_tidy, dsn = tmp, quiet = TRUE)
  put_object(
    file = tmp,
    object = link,
    bucket = "tech-team-data",
    multipart = TRUE
  )
}

#' Validate raw UST data
#' @param ust_tidy Cleaned spatial df containing only open tanks
validate_raw_ust <- function(ust_tidy, dataset_id) {
  checks_base <- "s3://tech-team-data/national-dw-tool/development/validation"
  run_ts      <- Sys.time()
  
  if (is.null(ust_tidy) || nrow(ust_tidy) == 0) {
    stop("UST validation failed: input dataset is NULL or has 0 rows", call. = FALSE)
  }
  
  if (!inherits(ust_tidy, "sf")) {
    stop("UST validation failed: input is not an sf object", call. = FALSE)
  }
  
  coords <- sf::st_coordinates(ust_tidy)
  checks_df <- ust_tidy %>%
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
    tag         = dataset_id,
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
