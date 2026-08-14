################################################################################
# SVI has no public download API, so it must be downloaded by hand before
# this pipeline can run.
# 
# Steps:
# 1. Go to the CDC/ATSDR SVI data download page:
#    https://www.atsdr.cdc.gov/place-health/php/svi/svi-data-documentation-download.html
# 2. Set these parameters:
#      - Year:           2022
#      - Geography:      United States
#      - Geography Type: Census Tracts
#      - File Type:      ESRI Geodatabase
# 3. Download the zip and unzip it. It contains a File Geodatabase folder
#    named "SVI2022_US_tract.gdb".
# 4. Place that folder at ./data/SVI2022_US_tract.gdb (relative to the repo
#    root), matching raw_svi.local_source_path in main_config.json.
################################################################################

#' Pull CDC/ATSDR Social Vulnerability Index and crosswalk onto EPA SABs
#' @param config Main config
#' @param dataset_id "raw_svi"
run_svi_pipeline <- function(config, dataset_id) {
  update_raw_svi(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Read, clean, and validate locally downloaded SVI geodatabase
update_raw_svi <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  local_path <- sub_config$local_source_path
  link <- sub_config$link

  if (!file.exists(local_path)) {
    stop(sprintf(
      "SVI source file not found at %s. Follow download steps in pipeline_svi.R.",
      local_path
    ), call. = FALSE)
  }

  message("Reading local SVI geodatabase...")
  svi_raw <- st_read(local_path, layer = "SVI2022_US_Tract", quiet = TRUE)

  svi_vars <- get_census_var_interp_methods(config) %>%
    filter(dataset == "svi") %>%
    select(var)

  # E_LIMENG = persons age >5 who speak English less than well
  # E_MOBILE = mobile home estimates
  # RPL_THEMES = overall percentile ranking summary variable and
  # disaggregated themes
  svi_tidy <- svi_raw %>%
    janitor::clean_names() %>%
    select(st:location, all_of(svi_vars$var)) %>%
    mutate(last_epic_run_date = Sys.Date()) %>%
    rename(geography = Shape)

  message("Validating raw SVI...")
  validate_raw_svi(config, svi_tidy, dataset_id)

  message("Saving SVI dataset to S3...")
  s3_write_geojson(svi_tidy, link)
}

#' Pointblank validations for raw SVI
validate_raw_svi <- function(config, svi_tidy, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  message(sprintf("row_count: %d", nrow(svi_tidy)))
  attrs <- sf::st_drop_geometry(svi_tidy)

  agent <- new_check_agent(attrs, label = "SVI Validation") %>%
    check_row_count_range(min_rows = 70000, max_rows = 90000, severity = "stop") %>%
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

  message("SVI validation checks passed successfully.")
  return(TRUE)
}

#' Crosswalk SVI onto EPA SABs
#' @param dataset_id "clean_sabs_svi"
run_clean_sabs_svi_pipeline <- function(config, dataset_id = "clean_sabs_svi") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link

  message("Downloading raw SVI from S3...")
  svi <- s3_read_geojson(raw_link) %>%
    # a value of -999 means it was unavailable/uncalculated in the source
    # data - treat as NA rather than a real value so it isn't interpolated
    # as actual -999s
    mutate(across(rpl_theme1:rpl_themes, ~ case_when(. < 0 ~ NA, TRUE ~ .)))

  # NOTE - the documentation doesn't explicitly say what census vintage is used, 
  # but it uses ACS 2022 5yr estimate, so I'm assuming 2020 
  run_sabs_xwalk_pipeline(config, dataset_id, xwalk_dataset_name="svi",
                           svi, fips_col = "fips",
                           blocks_2020 = TRUE)
}
