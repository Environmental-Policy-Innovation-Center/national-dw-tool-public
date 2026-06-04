#' Merge and Intersect HUC12 Boundaries with EPA UST Points
#' @param config Parsed configuration list
#' @param dataset_id "huc12_ust_merge"
run_spatial_merge_pipeline <- function(config, dataset_id) {
  print("Downloading raw HUC12 geodatabase layer from S3...")
  tmp_zip <- tempfile(fileext = ".zip")
  on.exit(unlink(tmp_zip), add = TRUE)
  save_object(object = config$raw_huc12_s3_path, bucket = "tech-team-data", file = tmp_zip)
  
  print("Unzipping HUC12 file...")
  ex_dir <- tempdir()
  on.exit(unlink(ex_dir, recursive = TRUE), add = TRUE)
  unzip(tmp_zip, exdir = ex_dir)
  gdb_path <- list.dirs(ex_dir, full.names = TRUE, recursive = TRUE)
  gdb_path <- gdb_path[grep("\\.gdb$", gdb_path)][1]
  
  huc12_geoms <- st_read(gdb_path, layer = "WBDHU12", quiet = TRUE) %>%
    st_transform(crs = 5070)
  
  print("Downloading raw UST GeoJSON from S3...")
  # Transform to planar crs
  ust_points <- s3read_using(st_read, object = config$raw_ust_s3_path, bucket = "tech-team-data", quiet = TRUE) %>%
    st_transform(crs = 5070) %>%
    filter(!st_is_empty(geometry))

  print("Computing national spatial overlay bounds...")
  sf_use_s2(FALSE)
  intersected_data <- st_join(ust_points, huc12_geoms, join = st_intersects)
  sf::sf_use_s2(TRUE)

  print("Summarizing USTs by HUC12...")
  # there are ~1000 HUC12s where the temporarily out of service USTs are NA,
  # HERE we're assuming these are zero. The EPA considers temporarily 
  # out of service USTs as open. 
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
  
  print("Saving cleaned USTs to S3...")
  tmp_out <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp_out), add = TRUE)
  write.csv(usts_huc12_summary, file = tmp_out, row.names = FALSE)
  put_object(file = tmp_out, object = config$ust_clean_link, bucket = "tech-team-data")
  
  print("HUC12 and UST merge pipeline completed successfully.")
}