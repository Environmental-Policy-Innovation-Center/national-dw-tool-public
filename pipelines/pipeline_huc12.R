#' Pull national HUC12 geometries then run huc12 and open usts merge pipeline.
#' @param config Main config
#' @param dataset_id "raw_huc12"
run_huc12_pipeline <- function(config, dataset_id) {
  huc12_optimized <- update_raw_huc12(config, dataset_id)

  message("Running HUC12 and UST merge pipeline...")
  run_clean_huc12_open_usts_pipeline(config, "clean_huc12_open_usts", huc12_geoms = huc12_optimized)

  message("Running HUC12 and RMP sites merge pipeline...")
  run_clean_huc12_rmp_sites_pipeline(config, "clean_huc12_rmp_sites", huc12_geoms = huc12_optimized)

  message("Running HUC12 and NPDES merge pipeline...")
  run_clean_huc12_npdes_pipeline(config, "clean_huc12_npdes", huc12_geoms = huc12_optimized)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Download the raw national WBD zip, extract and simplify the HUC12 layer, and
#' push the optimized layer to S3.
#' @param config Main config
#' @param dataset_id "raw_huc12"
update_raw_huc12 <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link
  wbd_raw_link <- sub_config$wbd_raw_link

  message("Downloading National WBD Geodatabase zip archive...")
  tmp_zip <- tempfile(fileext = ".zip")
  options(timeout = max(1200, getOption("timeout"))) # Longer timeout because file is large
  download.file(source_url, destfile = tmp_zip, mode = "wb", quiet = FALSE)

  # Optional step: save the full WBD database to S3
  # message("Pushing raw WBD database to S3...")
  # s3_write_file(tmp_zip, wbd_raw_link)

  message("Extracting and optimizing HUC12 layer...")
  huc12_optimized <- extract_huc12_from_zip(tmp_zip)

  message("Validating optimized HUC12 layer...")
  validate_optimized_huc12(config, huc12_optimized, dataset_id)

  message("Pushing optimized HUC12 layer to S3...")
  s3_write_gpkg(huc12_optimized, link)

  return(huc12_optimized)
}

#' Extract and simplify the HUC12 layer from a local WBD National GDB zip.
#' The huc12 geometry is simplified because the spatial joins with USTs and RMP
#' sites are point based and exact precision isn't needed for these use cases.
#' @param zip_path Local path to the WBD National GDB zip.
#' @param dTolerance Simplification tolerance in meters
extract_huc12_from_zip <- function(zip_path) {
  tmp_exdir <- tempfile(pattern = "wbd_gdb_working_")
  on.exit(unlink(tmp_exdir, recursive = TRUE), add = TRUE)

  zip_contents <- unzip(zip_path, list = TRUE)
  # Find any path containing '.gdb/' or ending in '.gdb'
  all_gdb_matches <- grep("\\.gdb($|/)", zip_contents$Name, ignore.case = TRUE, value = TRUE)
  if (length(all_gdb_matches) == 0) {
    stop("Downloaded zip does not contain a valid internal '.gdb' directory.", call. = FALSE)
  }

  # Get the GDB folder prefix name ("WBD_National_GDB.gdb/")
  gdb_dir_name <- regmatches(all_gdb_matches[1], regexpr("^.*\\.gdb/?", all_gdb_matches[1], ignore.case = TRUE))

  # Extract only the files inside the gdb folder
  gdb_files <- grep(gdb_dir_name, zip_contents$Name, fixed = TRUE, value = TRUE)
  unzip(zip_path, files = gdb_files, exdir = tmp_exdir)

  # Delete zip so it doesn't take up disk space.
  unlink(zip_path)

  message("Reading raw HUC12 layer from WBD geodatabase...")
  gdb_full_path <- file.path(tmp_exdir, gdb_dir_name)
  huc12_raw <- st_read(gdb_full_path, layer = "WBDHU12", query = "SELECT HUC12 FROM WBDHU12", quiet = TRUE) %>%
    rename_with(tolower) %>%
    select(huc12)

  message("Simplifying HUC12 geometry...")
  # Projects layer from original CRS to EPSG:5070 for the simplify step which is
  # distance-based. With the simplified polygon outline, any points within the
  # dTolerance (set to 100 meters) is dropped. preserveTopology TRUE ensures
  # that invalid polygons aren't created. We can end up with gaps between
  # adjacent polygons but this is fine for our level of spatial joins. The
  # final steps projects the layer back to its original CRS.
  huc12_raw %>%
    st_transform(crs = 5070) %>%
    st_simplify(dTolerance = 100, preserveTopology = TRUE) %>%
    st_transform(crs = st_crs(huc12_raw))
}

#' Validate the optimized HUC12 layer.
#' @param huc12_optimized sf object returned by extract_huc12_from_zip()
#' @param dataset_id "raw_huc12"
validate_optimized_huc12 <- function(config, huc12_optimized, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  # Precompute geometry-based columns before dropping geometry
  attrs <- huc12_optimized %>%
    mutate(
      geom_valid     = sf::st_is_valid(huc12_optimized),
      geom_not_empty = !sf::st_is_empty(huc12_optimized),
      crs_epsg       = sf::st_crs(huc12_optimized)$epsg
    ) %>%
    sf::st_drop_geometry()

  agent <- new_check_agent(attrs, label = "Optimized HUC12 Layer Validation") %>%
    # USGS's national WBD has around ~103,000 HUC12s
    check_row_count_range(min_rows = 100000, max_rows = 110000, severity = "warning") %>%
    check_column_complete(huc12, severity = "warning") %>%
    col_vals_regex(
      columns = vars(huc12), regex = "^[0-9]{12}$",
      actions = action_levels(warn_at = 1),
      label = "HUC12 is a 12-digit code"
    ) %>%
    rows_distinct(
      columns = vars(huc12),
      actions = action_levels(warn_at = 1),
      label = "No duplicate HUC12 codes"
    ) %>%
    check_column_all_true(geom_valid, severity = "warning") %>%
    check_column_all_true(geom_not_empty, severity = "warning") %>%
    col_vals_equal(
      columns = vars(crs_epsg), value = 4269,
      actions = action_levels(warn_at = 1),
      label = "CRS is NAD83 (EPSG 4269)"
    ) %>%
    interrogate()

  result <- summarize_checks(agent)
  message(sprintf("Validation result summary: %s", result$summary))

  report_link <- write_check_artifacts(
    agent = agent, report_df = result$report_df,
    checks_base = checks_base, tag = dataset_id, run_ts = run_ts
  )
  message(sprintf("Validation reports pushed to S3: %s", report_link))

  if (isTRUE(result$any_error)) {
    stop(sprintf("VALIDATION FAILED: %s", result$summary), call. = FALSE)
  }

  message("Optimized HUC12 layer validation checks passed successfully.")
}
