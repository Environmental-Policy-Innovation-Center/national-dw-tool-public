#' Intersect HUC12 boundaries with UST points
#' @param config Main config
#' @param dataset_id Unique dataset id
#' @param huc12_geoms Optional pre-loaded HUC12 sf object. If NULL,
#'   the optimized layer is downloaded from S3.
#' @param ust_points Optional pre-loaded UST sf object. If NULL,
#'   the UST geojson is downloaded from S3.
run_clean_huc12_open_usts_pipeline <- function(config, dataset_id, huc12_geoms = NULL, ust_points = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  huc12_link <- sub_config$input_links$huc12_link
  ust_raw_link <- sub_config$input_links$ust_raw_link
  link <- sub_config$link

  if (is.null(huc12_geoms)) {
    message("Downloading optimized HUC12 layer from S3...")
    huc12_geoms <- s3_read_gpkg(huc12_link)
  }
  huc12_geoms <- huc12_geoms %>%
    st_transform(crs = 5070)

  if (is.null(ust_points)) {
    message("Downloading raw UST GeoJSON from S3...")
    ust_points <- s3_read_geojson(ust_raw_link)
  }
  ust_points <- ust_points %>%
    st_transform(crs = 5070) %>%
    # Avoid hardcoding geometry column name (could be geoms or geometry
    # depending on how ust_points is read in)
    filter(!st_is_empty(.))

  message("Spatial join of HUC12 geoms and UST points...")
  sf_use_s2(FALSE)
  intersected_data <- st_join(ust_points, huc12_geoms, join = st_intersects)
  sf::sf_use_s2(TRUE)

  message("Summarizing USTs by HUC12...")
  # There are ~1000 HUC12s where the temporarily out of service USTs are NA,
  # HERE we're assuming these are zero.
  # The EPA considers temporarily out of service USTs as open.
  usts_huc12_summary <- intersected_data %>%
    as.data.frame() %>%
    mutate(
      open_us_ts_tidy = as.numeric(open_us_ts),
      tos_us_ts_tidy  = as.numeric(tos_us_ts)
    ) %>%
    group_by(huc12) %>%
    summarize(
      total_open_usts = sum(open_us_ts_tidy, na.rm = TRUE),
      total_tos_usts  = sum(tos_us_ts_tidy, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(epa_open_usts = total_open_usts + total_tos_usts)

  # TODO: add pointblank validation

  message("Saving cleaned USTs to S3...")
  s3_write_csv(usts_huc12_summary, link)
  
  message("HUC12 and UST merge pipeline completed successfully.")
}