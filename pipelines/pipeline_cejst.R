################################################################################
# Both CEJST sources below are pulled. Data is split across two URLs.
#
# 1. Shapefile + codebook zip (a zip within a zip), from source_url:
#    https://dblew8dgr6ajz.cloudfront.net/data-versions/2.0/data/score/downloadable/2.0-shapefile-codebook.zip
#    Provides the 2010 census tract geometries (geoid10) - no scored variables.
#
# 2. Communities CSV, from communities_csv_url:
#    https://dblew8dgr6ajz.cloudfront.net/data-versions/2.0/data/score/downloadable/2.0-communities.csv
#    Provides the actual scored variables (joined to the shapefile by geoid10).
################################################################################

#' Pull CEJST (Climate and Economic Justice Screening Tool) and crosswalk onto EPA SABs
#' @param config Main config
#' @param dataset_id "raw_cejst"
run_cejst_pipeline <- function(config, dataset_id) {
  update_raw_cejst(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Download CEJST shapefile + communities CSV, then join, clean, and validate
update_raw_cejst <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  link <- sub_config$link
  communities_csv_url <- sub_config$communities_csv_url

  # NOTE - CEJST uses 2010 census tract boundaries. This is a zip within a zip.
  message("Downloading CEJST shapefile+codebook archive...")
  file_loc <- tempfile(pattern = "cejst_working_")
  on.exit(unlink(file_loc, recursive = TRUE), add = TRUE)
  outer_zip <- paste0(file_loc, ".zip")
  on.exit(unlink(outer_zip), add = TRUE)
  download.file(sub_config$source_url, destfile = outer_zip, mode = "wb", quiet = FALSE)
  unzip(zipfile = outer_zip, exdir = file_loc)

  message("Unzipping nested shapefile archive...")
  inner_zip <- paste0(file_loc, "/usa.zip")
  unzip(zipfile = inner_zip, exdir = file_loc)

  # we just want the geoid10 and geometries, to match with the communities csv
  cejst_geoms <- st_read(paste0(file_loc, "/usa.shp"), quiet = TRUE) %>%
    janitor::clean_names() %>%
    select(geoid10)

  cejst_vars <- get_census_var_interp_methods(config) %>%
    filter(dataset == "cejst") %>%
    select(var)

  message("Downloading CEJST communities CSV...")
  # in addition to the shapefile (which has geometries), we need columns
  # that aren't in the shapefile
  cejst_tidy <- read.csv(communities_csv_url) %>%
    janitor::clean_names() %>%
    select(census_tract_2010_id, all_of(cejst_vars$var)) %>%
    rename(geoid10 = census_tract_2010_id) %>%
    mutate(geoid10 = as.character(geoid10),
           geoid10 = case_when(nchar(geoid10) == 10 ~ paste0("0", geoid10), TRUE ~ geoid10)) %>%
    left_join(cejst_geoms, by = "geoid10") %>%
    st_as_sf() %>%
    mutate(last_epic_run_date = Sys.Date(),
           identified_as_disadvantaged = case_when(
             identified_as_disadvantaged == "True" ~ 1,
             identified_as_disadvantaged == "False" ~ 0
           )) %>%
    # from 1/29/2026 run, there are ~367 tracts with empty geometries
    filter(!st_is_empty(.))

  message("Validating raw CEJST...")
  validate_raw_cejst(config, cejst_tidy, dataset_id)

  message("Saving CEJST dataset to S3...")
  s3_write_geojson(cejst_tidy, link)
}

#' Pointblank validations for raw CEJST
validate_raw_cejst <- function(config, cejst_tidy, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  message(sprintf("row_count: %d", nrow(cejst_tidy)))
  attrs <- sf::st_drop_geometry(cejst_tidy)

  agent <- new_check_agent(attrs, label = "CEJST Validation") %>%
    check_row_count_range(min_rows = 70000, max_rows = 80000, severity = "stop") %>%
    interrogate()

  result <- summarize_checks(agent)
  message(sprintf("Validation result summary: %s", result$summary))

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

  message("CEJST validation checks passed successfully.")
  return(TRUE)
}

#' Crosswalk CEJST onto EPA SABs
#' @param dataset_id "clean_sabs_cejst"
run_clean_sabs_cejst_pipeline <- function(config, dataset_id = "clean_sabs_cejst") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link

  message("Downloading raw CEJST from S3...")
  cejst <- s3_read_geojson(raw_link) %>%
    mutate(geoid10 = case_when(nchar(geoid10) == 10 ~ paste0("0", geoid10), TRUE ~ geoid10))

  # NOTE - CEJST uses 2010 census tract geometries
  run_sabs_xwalk_pipeline(config, dataset_id, xwalk_dataset_name="cejst",
                           sf_data_census=cejst, fips_col = "geoid10",
                           blocks_2020 = FALSE)
}
