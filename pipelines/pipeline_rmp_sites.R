#' Pull EPA RMP sites, then run the HUC12/RMP-sites merge pipeline
#' @param config Main config
#' @param dataset_id "raw_rmp_sites"
run_rmp_sites_pipeline <- function(config, dataset_id) {
  active_rmp <- update_raw_rmp_sites(config, dataset_id)
  
  message("Running HUC12 and RMP sites merge pipeline...")
  run_clean_huc12_rmp_sites_pipeline(config, "clean_huc12_rmp_sites", active_rmp = active_rmp)
  
  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Pull, validate, and save raw RMP sites geojson
#' @param config Main config
#' @param dataset_id "raw_rmp_sites"
update_raw_rmp_sites <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link

  message("Pulling active RMP sites data...")
  rmp <- arcpullr::get_spatial_layer(source_url)

  active_rmp <- rmp %>%
    janitor::clean_names() %>%
    filter(active_status == "ACTIVE") %>%
    mutate(last_epic_run_date = Sys.Date())
  
  message("Validating raw RMP data...")
  validate_raw_rmp_sites(config, active_rmp, dataset_id)
  
  message("Writing raw_rmp_sites to S3...")
  s3_write_geojson(active_rmp, link)

  return(active_rmp)
}

#' Pointblank validations for active RMP dataset
#' @param active_rmp Cleaned, active RMP sites sf object
#' @param dataset_id "raw_rmp_sites"
validate_raw_rmp_sites <- function(config, active_rmp, dataset_id) {
  checks_base <- config$metadata$checks_link
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
  message(sprintf("Validation result summary: %s", result$summary))
  
  # Write HTML report/CSV to S3
  report_link <- write_check_artifacts(
    agent       = agent, 
    report_df   = result$report_df, 
    checks_base = checks_base, 
    tag         = dataset_id, 
    run_ts      = run_ts
  )
  message(sprintf("Validation reports pushed to S3: %s", report_link))
  
  if (isTRUE(result$any_error)) {
    stop(sprintf("VALIDATION FAILED: %s", result$summary), call. = FALSE)
  }
  
  message("RMP validation checks passed successfully.")
  return(TRUE)
}

#' Intersect RMP sites with national HUC12 layer
#' @param config Main config
#' @param dataset_id "clean_huc12_rmp_sites"
#' @param active_rmp Optional pre-loaded active RMP sites sf object. If NULL, downloaded from S3.
#' @param huc12_geoms Optional pre-loaded HUC12 sf object. If NULL, downloaded from S3.
run_clean_huc12_rmp_sites_pipeline <- function(config, dataset_id = "clean_huc12_rmp_sites", active_rmp = NULL, huc12_geoms = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_rmp_link <- sub_config$input_links$raw_rmp_sites
  huc12_link <- sub_config$input_links$huc12_link
  link <- sub_config$link

  if (is.null(active_rmp)) {
    message("Downloading raw RMP sites from S3...")
    active_rmp <- s3_read_geojson(raw_rmp_link)
  }

  if (is.null(huc12_geoms)) {
    message("Downloading optimized HUC12 layer from S3...")
    huc12_geoms <- s3_read_gpkg(huc12_link)
  }
  huc12_geoms <- huc12_geoms %>%
    st_transform(crs = 5070)

  message("Preparing RMP sites...")
  rmps <- active_rmp %>%
    st_transform(crs = 5070)
  
  message("Computing national HUC12 intersection...")
  # Turn off spherical geometry for running intersections
  sf_use_s2(FALSE)
  rmp_huc12 <- st_join(rmps, huc12_geoms, join = st_within, left = FALSE)
  sf_use_s2(TRUE)
  
  if (nrow(rmp_huc12) == 0) {
    stop("HUC12 and RMP spatial intersection failed - 0 records.")
  }
  
  message("Summarizing RMP facilities by HUC12...")
  rmp_huc12_summary <- rmp_huc12 %>%
    st_drop_geometry() %>%
    group_by(huc12) %>%
    summarize(
      total_facilities_w_rmps = n_distinct(registry_id),
      .groups = "drop"
    )

  message("Validating clean_huc12_rmp_sites...")
  validate_huc12_rmp_sites_summary(config, active_rmp, rmp_huc12, rmp_huc12_summary, dataset_id)

  message("Writing clean dataset to S3...")
  s3_write_csv(rmp_huc12_summary, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(rmp_huc12_summary)
}

#' Pointblank validations for the HUC12/RMP merge summary
#' @param config Main config
#' @param active_rmp Cleaned, active RMP sites sf object (pre-join)
#' @param rmp_huc12 RMP sites joined to HUC12 (post-join, pre-summarize)
#' @param rmp_huc12_summary Facility counts summarized by HUC12
#' @param dataset_id "clean_huc12_rmp_sites"
validate_huc12_rmp_sites_summary <- function(config, active_rmp, rmp_huc12, rmp_huc12_summary, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  n_input_facilities <- n_distinct(active_rmp$registry_id)
  n_matched_facilities <- n_distinct(rmp_huc12$registry_id)
  pct_facilities_matched <- 100 * n_matched_facilities / n_input_facilities

  checks_df <- rmp_huc12_summary %>%
    mutate(
      huc12_valid_format = grepl("^[0-9]{12}$", huc12),
      pct_facilities_matched = pct_facilities_matched
    )
  print(checks_df)

  agent <- new_check_agent(checks_df, label = "HUC12/RMP Merge Validation") %>%
    check_column_complete(huc12, severity = "stop") %>%
    check_column_all_true(huc12_valid_format, severity = "warning") %>%
    col_vals_gt(
      columns = vars(pct_facilities_matched), value = 90,
      actions = action_levels(warn_at = 1),
      label = "At least 90% of RMP facilities matched to a HUC12"
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

  message("HUC12/RMP merge validation checks passed successfully.")
  return(TRUE)
}