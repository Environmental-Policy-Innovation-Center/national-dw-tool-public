#' Pull active NPDES permits then run the HUC12/NPDES merge pipeline.
#' @param config Main config
#' @param dataset_id "raw_npdes_permits"
run_npdes_pipeline <- function(config, dataset_id) {
  update_raw_npdes_permits(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Download NPDES permits from EPA ECHO, filtering by active facilities and
#' effective/pending permits.
#' @param config Main config
#' @param dataset_id "raw_npdes_permits"
#' @return The filtered sf object of NPDES permit points
update_raw_npdes_permits <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link

  message("Downloading NPDES outfalls layer...")
  tmp_zip <- tempfile(fileext = ".zip")
  tmp_dir <- tempfile(pattern = "npdes_working_")
  on.exit(unlink(c(tmp_zip, tmp_dir), recursive = TRUE), add = TRUE)
  options(timeout = max(600, getOption("timeout")))
  download.file(source_url, destfile = tmp_zip, mode = "wb", quiet = FALSE)
  unzip(tmp_zip, exdir = tmp_dir)

  message("Reading NPDES permits...")
  npdes_permits <- read.csv(file.path(tmp_dir, "npdes_outfalls_layer.csv"))

  message("Filtering out non-active facilities...")
  # "Active facilities are those currently in operation (indicated by any status 
  # code except NON or TRM)."
  # data dictionary: https://echo.epa.gov/tools/data-downloads/icis-npdes-discharge-points-download-summary
  active_npdes_permits_tidy <- npdes_permits %>%
    janitor::clean_names() %>%
    filter(permit_status_code != "NON") %>%
    filter(permit_status_code != "TRM")

  message("Converting to points and filtering to effective/pending permits...")
  active_npdes_sf <- active_npdes_permits_tidy %>%
    select(external_permit_nmbr, permit_name, permit_effective_date, 
          permit_expiration_date, 
          permit_status_desc, 
          facility_type_desc, 
          # the permit feature number represents the unique outfall 
          # or pipe of interest 
          permit_type_desc, sic_descriptions, naics_codes, 
          major_minor_flag, 
          perm_feature_nmbr,
          cwa_current_status, 
          cwp_current_snc_status, cwp_current_viol,
          latitude83, longitude83) %>%
    st_as_sf(coords = c("longitude83", "latitude83"), crs = "NAD83") %>%
    # FILTERING permit status for those that are effective, admin continued 
    # (applied for renewal), or pending. Other categories (such as expired), were 
    # basically old permits that had expired and the facility applied for 
    # a new permit - this still has violation data and could skew results 
    # (even tho it is flagged as active)
    filter(permit_status_desc %in% c("Effective", "Admin Continued", "Pending")) %>%
    mutate(last_epic_run_date = Sys.Date())

  message("Validating raw NPDES permits...")
  validate_raw_npdes_permits(config, dataset_id, active_npdes_sf)

  message("Saving raw NPDES permits to S3...")
  s3_write_geojson(active_npdes_sf, link)

  return(active_npdes_sf)
}

#' Pointblank validations for raw NPDES permits
#' @param config Main config
#' @param dataset_id "raw_npdes_permits"
#' @param npdes_sf Filtered active NPDES permit points
validate_raw_npdes_permits <- function(config, dataset_id, npdes_sf) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  checks_df <- tibble(
    row_count      = nrow(npdes_sf),
    geometry_valid = all(sf::st_is_valid(npdes_sf))
  )
  print(checks_df)

  agent <- new_check_agent(checks_df, label = "NPDES Permits Validation") %>%
    col_vals_gt(
      columns = vars(row_count), value = 0,
      actions = action_levels(stop_at = 1),
      label = "NPDES dataset has > 0 rows"
    ) %>%
    col_vals_equal(
      columns = vars(geometry_valid), value = TRUE,
      actions = action_levels(warn_at = 1),
      label = "All geometries are valid"
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

  message("NPDES permits validation checks passed successfully.")
  return(TRUE)
}

#' Intersect NPDES permit points with the national HUC12 layer and summarize
#' permit/violation counts per HUC12.
#' @param config Main config
#' @param dataset_id "clean_huc12_npdes"
#' @param npdes_points Optional pre-loaded active NPDES permit points. If NULL, downloaded from S3.
#' @param huc12_geoms Optional pre-loaded HUC12 sf object. If NULL, downloaded from S3.
run_clean_huc12_npdes_pipeline <- function(config, dataset_id = "clean_huc12_npdes", npdes_points = NULL, huc12_geoms = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  npdes_raw_link <- sub_config$input_links$npdes_raw_link
  huc12_link <- sub_config$input_links$huc12_link
  link <- sub_config$link

  if (is.null(npdes_points)) {
    message("Downloading raw NPDES permits from S3...")
    npdes_points <- s3_read_geojson(npdes_raw_link)
  }
  npdes_points <- npdes_points %>% st_transform(crs = 5070)

  if (is.null(huc12_geoms)) {
    message("Downloading optimized HUC12 layer from S3...")
    huc12_geoms <- s3_read_gpkg(huc12_link)
  }
  huc12_geoms <- huc12_geoms %>% st_transform(crs = 5070)

  message("Spatial join of NPDES permits and HUC12 geoms...")
  sf_use_s2(FALSE)
  npdes_huc12 <- st_join(npdes_points, huc12_geoms, join = st_intersects, left = FALSE)
  sf_use_s2(TRUE)

  message("Summarizing permit/violation counts by HUC12...")
  # there are some mining companies that have ~5 permits, each with ~20 
  # permitted features that have the same cwa_status. Since cwa_status does not 
  # seem to be unique to specific features (i.e., outfall pipe). I'm opting to remove 
  # perm_feature_nmbr and focus on permits in violation. BUT, since 
  # we did the intersection with features included (with unique geometries), 
  # a single permit w/ violations could 
  # show up in multiple hucs and be noted as having violations even if the 
  # specific feature is not in violation (we could def fix this with dmrs, but 
  # that would take some additional time since downloading one takes like ~15
  # mins)
  npdes_huc12_summary <- npdes_huc12 %>%
    st_drop_geometry() %>%
    select(-perm_feature_nmbr) %>%
    unique() %>%
    group_by(huc12) %>%
    summarize(
      npdes_permits = length(unique(external_permit_nmbr)),
      total_permit_viols = sum(cwa_current_status %in%
        c("Violation Identified", "Significant/Category I Noncompliance")),
      total_permit_no_viols = sum(cwa_current_status == "No Violation Identified"),
      total_permit_not_applicable_viols = sum(cwa_current_status == "Not Applicable"),
      total_permit_unknown_viols = sum(cwa_current_status == "Unknown"),
      total_permit_sig_viols = sum(cwa_current_status == "Significant/Category I Noncompliance"),
      total_permit_eff_viols = sum(cwp_current_snc_status %in%
        c("Effluent - Monthly Average Limit", "Effluent - Non-monthly Average Limit")),
      .groups = "drop"
    )

  message("Validating clean_huc12_npdes...")
  validate_npdes_huc12_summary(config, npdes_huc12_summary, dataset_id)

  message("Writing clean dataset to S3...")
  s3_write_csv(npdes_huc12_summary, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(npdes_huc12_summary)
}

#' Pointblank validations for HUC12/NPDES merged data
#' @param config Main config
#' @param npdes_huc12_summary Permit/violation counts summarized by HUC12
#' @param dataset_id "clean_huc12_npdes"
validate_npdes_huc12_summary <- function(config, npdes_huc12_summary, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  checks_df <- npdes_huc12_summary %>%
    mutate(huc12_valid_format = grepl("^[0-9]{12}$", huc12))
  print(as_tibble(checks_df))

  agent <- new_check_agent(checks_df, label = "HUC12/NPDES Merge Validation") %>%
    check_row_count_range(min_rows = 1, max_rows = 110000, severity = "warning") %>%
    check_column_complete(huc12, severity = "stop") %>%
    check_column_all_true(huc12_valid_format, severity = "warning") %>%
    rows_distinct(
      columns = vars(huc12),
      actions = action_levels(stop_at = 1),
      label = "No duplicate HUC12 codes"
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

  message("HUC12/NPDES merge validation checks passed successfully.")
  return(TRUE)
}
