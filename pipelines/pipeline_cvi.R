################################################################################
# CVI (Climate Vulnerability Index) has no single public download API, so its
# baseline domain-score file must be downloaded by hand before this pipeline
# can run.
#
# Steps:
# 1. Go to https://climatevulnerabilityindex.org/resources/
# 2. Download the "Master CVI Dataset" Excel workbook.
# 3. Place it at ./data/Master CVI Dataset - Oct 2023.xlsx (relative to the
#    repo root), matching raw_cvi.local_source_path in main_config.json.
################################################################################

#' Pull CVI and crosswalk onto EPA SABs
#' @param config Main config
#' @param dataset_id "raw_cvi"
run_cvi_pipeline <- function(config, dataset_id) {
  update_raw_cvi(config, dataset_id)

  message("Running SABs and CVI crosswalk pipeline...")
  run_clean_sabs_cvi_pipeline(config, "clean_sabs_cvi")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Read the locally downloaded CVI baseline workbook, merge with CVI's live
#' raw-indicator CSV, then clean and validate
update_raw_cvi <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  local_path <- sub_config$local_source_path
  source_url <- sub_config$source_url
  link <- sub_config$link

  if (!file.exists(local_path)) {
    stop(sprintf(
      "CVI baseline file not found at %s. Follow download steps in pipeline_cvi.R.",
      local_path
    ), call. = FALSE)
  }

  cvi_vars <- get_census_var_interp_methods(config) %>%
    filter(dataset == "cvi") %>%
    select(var)

  message("Reading local CVI baseline workbook...")
  cvi_baselines <- readxl::read_excel(local_path, sheet = "Domain CVI Values") %>%
    janitor::clean_names() %>%
    rename(geoid_tract = fips_code, county_name = county)

  message("Pulling CVI raw indicator data from Github repository...")
  cvi_raw_ind <- st_read(source_url, quiet = TRUE) %>%
    janitor::clean_names()

  message("Merging CVI baseline and raw indicator data together...")
  cvi_tidy <- merge(cvi_baselines, cvi_raw_ind,
                     by = c("geoid_tract", "state", "county_name"), all = TRUE) %>%
    select(state:geoid_tract, all_of(cvi_vars$var)) %>%
    mutate(across(all_of(cvi_vars$var), as.numeric)) %>%
    mutate(last_epic_run_date = Sys.Date()) %>%
    # some geoid tracts (e.g. AK, CT) are missing a leading "0" from source
    mutate(geoid_tract = as.character(geoid_tract),
           geoid_tract = case_when(nchar(geoid_tract) == 10 ~ paste0("0", geoid_tract),
                                    TRUE ~ geoid_tract)) %>%
    unique()

  message("Validating raw CVI...")
  validate_raw_cvi(config, cvi_tidy, dataset_id)

  message("Saving CVI dataset to S3...")
  s3_write_csv(cvi_tidy, link)
}

#' Pointblank validations for raw CVI
validate_raw_cvi <- function(config, cvi_tidy, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  message(sprintf("row_count: %d", nrow(cvi_tidy)))

  agent <- new_check_agent(cvi_tidy, label = "CVI Validation") %>%
    check_row_count_range(min_rows = 60000, max_rows = 80000, severity = "stop") %>%
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

  message("CVI validation checks passed successfully.")
  return(TRUE)
}

#' Crosswalk CVI onto EPA SABs
#' @param dataset_id "clean_sabs_cvi"
run_clean_sabs_cvi_pipeline <- function(config, dataset_id = "clean_sabs_cvi") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link

  message("Downloading raw CVI from S3...")
  # coerce_character = FALSE - the score variables need to stay numeric for
  # the population-weighted interpolation step downstream
  cvi <- s3_read_csv(raw_link, coerce_character = FALSE) %>%
    mutate(geoid_tract = as.character(geoid_tract),
           geoid_tract = case_when(nchar(geoid_tract) == 10 ~ paste0("0", geoid_tract),
                                    TRUE ~ geoid_tract))

  message("Pulling 2010 census tract geometries...")
  census_tracts <- tidycensus::get_acs(
    geography = "tract",
    variables = c(total_pop = "B01003_001"),
    state = unique(cvi$state),
    year = 2010,
    geometry = TRUE
  )

  cvi_sf <- merge(cvi, census_tracts, by.x = "geoid_tract", by.y = "GEOID") %>%
    st_as_sf() %>%
    select(-c(NAME:moe)) %>%
    st_transform(crs = 5070)

  run_sabs_xwalk_pipeline(config, dataset_id, xwalk_dataset_name = "cvi",
                           sf_data_census = cvi_sf, fips_col = "geoid_tract",
                           blocks_2020 = FALSE)
}
