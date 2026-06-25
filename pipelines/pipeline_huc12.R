library(tidyverse)
library(sf)
library(httr)

#' Pull national HUC12 geometries
#' @param config Main config
#' @param dataset_id "raw_huc12"
run_huc12_pipeline <- function(config, dataset_id) {
  update_raw_huc12(config, dataset_id)

  print("Running HUC12 and UST merge pipeline...")
  run_clean_huc12_open_usts_pipeline(config)
  
  print(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Pull and store national HUC12 geometries
#' @param wbd_source_url Remote URI for the USGS WBD zip file
#' @param wbd_raw_link S3 link for storing raw WBD
update_raw_huc12 <- function(config, dataset_id) {
  print(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  wbd_raw_link <- sub_config$wbd_raw_link
  
  print("Downloading National WBD Geodatabase zip archive (~1GB)...")
  tmp_zip <- tempfile(fileext = ".zip")
  on.exit(unlink(tmp_zip), add = TRUE)
  options(timeout = max(1200, getOption("timeout"))) # Longer timeout because file is large
  download.file(source_url, destfile = tmp_zip, mode = "wb", quiet = FALSE)
  
  tmp_exdir <- tempfile(pattern = "wbd_gdb_working_")
  on.exit(unlink(tmp_exdir, recursive = TRUE), add = TRUE)

  print("Validating HUC12 geometries...")
  validated_gdb_path <- validate_raw_huc12(tmp_zip, tmp_exdir, dataset_id)

  print("Pushing raw WBD database archive to S3...")
  # This step took a long time
  aws.s3::put_object(
    file = tmp_zip,
    object = wbd_raw_link,
    multipart = TRUE
  )
}

validate_raw_huc12 <- function(zip_path, tmp_exdir, dataset_id) {
  # Bucket for validation reports
  checks_base <- "s3://tech-team-data/national-dw-tool/development/validation"
  run_ts <- Sys.time()
  
  # Scan metadata headers
  zip_contents <- unzip(zip_path, list = TRUE)
  # Find any path containing '.gdb/' or ending in '.gdb'
  all_gdb_matches <- grep("\\.gdb($|/)", zip_contents$Name, ignore.case = TRUE, value = TRUE)
  
  if (length(all_gdb_matches) == 0) {
    stop("VALIDATION FAILED: Downloaded zip does not contain a valid internal '.gdb' directory.", call. = FALSE)
  }
  
  # Get the GDB folder prefix name ("WBD_National_GDB.gdb/")
  first_match <- all_gdb_matches[1]
  gdb_dir_name <- regmatches(first_match, regexpr("^.*\\.gdb/?", first_match, ignore.case = TRUE))
  
  # Extract only the specific files inside the gdb folder
  gdb_files <- grep(gdb_dir_name, zip_contents$Name, fixed = TRUE, value = TRUE)
  unzip(zip_path, files = gdb_files, exdir = tmp_exdir)
  gdb_full_path <- file.path(tmp_exdir, gdb_dir_name)
  
  gdb_layers <- sf::st_layers(gdb_full_path)
  huc12_idx  <- which(gdb_layers$name == "WBDHU12")
  
  # Create a pointblank df
  checks_df <- tibble(
    layer_exists    = "WBDHU12" %in% gdb_layers$name,
    features_count  = if(length(huc12_idx) > 0) gdb_layers$features[huc12_idx] else 0,
    geometry_type   = if(length(huc12_idx) > 0) gdb_layers$geomtype[[huc12_idx]][1] else "Missing",
    crs_epsg        = if(length(huc12_idx) > 0) gdb_layers$crs[[huc12_idx]]$epsg else as.numeric(NA),
    crs_input       = if(length(huc12_idx) > 0) gdb_layers$crs[[huc12_idx]]$input else "Missing"
  )
  print(checks_df)

  agent <- new_check_agent(checks_df, label = "WBD HUC12 Validation") %>%
    col_vals_equal(
      columns = vars(layer_exists), 
      value = TRUE,
      actions = action_levels(stop_at = 1),
      label = "WBDHU12 layer exists"
    ) %>%
    col_vals_gt(
      columns = vars(features_count), 
      value = 100000,
      actions = action_levels(stop_at = 1),
      label = "Features count > 100,000"
    ) %>%
    col_vals_in_set(
      columns = vars(geometry_type), 
      set = c("Multi Polygon", "Polygon"),
      actions = action_levels(stop_at = 1),
      label = "Geometry is valid type"
    ) %>%
    col_vals_expr(
      expr = ~ crs_epsg == 4269 | grepl("NAD83|GCS_North_American_1983", crs_input),
      actions = action_levels(stop_at = 1),
      label = "CRS is NAD83 (EPSG 4269)"
    ) %>%
    interrogate()
  
  result <- summarize_checks(agent)
  print(sprintf("Validation result summary: %s", result$summary))
  
  # Console report
  report_card <- get_agent_report(agent, display_table = FALSE)
  print(report_card)
  
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
    stop(sprintf("VALIDATION FAILED: %s", result$summary), call. = FALSE)
  }
  
  message("HUC12 validation checks passed successfully.")
  return(gdb_full_path)
}

# For testing locally downloaded WBD
# validate_raw_huc12("./local_data/test_wbd_snapshot.zip")
