################################################################################
# EJScreen has no public download API, so it must be downloaded by hand
# before this pipeline can run.
#
# Steps:
# 1. Go to the Harvard Dataverse dataset page:
#    https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/RLR5AX
# 2. Download the file named "EJSCREEN_2024_BG_with_AS_CNMI_GU_VI.csv".
# 3. Place it at ./data/EJSCREEN_2024_BG_with_AS_CNMI_GU_VI.csv (relative to
#    the repo root), matching raw_ejscreen.local_source_path in main_config.json.
################################################################################

#' Pull EPA EJScreen and crosswalk onto EPA SABs
#' @param config Main config
#' @param dataset_id "raw_ejscreen"
run_ejscreen_pipeline <- function(config, dataset_id) {
  update_raw_ejscreen(config, dataset_id)

  message("Running SABs and EJScreen crosswalk pipeline...")
  run_clean_sabs_ejscreen_pipeline(config, "clean_sabs_ejscreen")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Read, clean, and validate locally downloaded EJScreen CSV
update_raw_ejscreen <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  local_path <- sub_config$local_source_path
  link <- sub_config$link

  if (!file.exists(local_path)) {
    stop(sprintf(
      "EJScreen source file not found at %s. Follow download steps in pipeline_ejscreen.R.",
      local_path
    ), call. = FALSE)
  }

  message("Reading local EJScreen CSV...")
  ejscreen_raw <- read.csv(local_path)

  ejscreen_vars <- get_census_var_interp_methods(config) %>%
    filter(dataset == "ejscreen") %>%
    select(var)

  # "PNPL", "PRMP", "PTSDF", "UST", "PWDIS" come from hydroshare, not here
  ejscreen_tidy <- ejscreen_raw %>%
    janitor::clean_names() %>%
    select(id:region, all_of(ejscreen_vars$var)) %>%
    mutate(last_epic_run_date = Sys.Date()) %>%
    # filtering for states/territories we have geoids for
    filter(!(st_abbrev %in% c("AS", "GU", "MP", "VI")))

  message("Validating raw EJScreen...")
  validate_raw_ejscreen(config, ejscreen_tidy, dataset_id)

  message("Saving EJScreen dataset to S3...")
  s3_write_csv(ejscreen_tidy, link)
}

#' Pointblank validations for raw EJScreen
validate_raw_ejscreen <- function(config, ejscreen_tidy, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  message(sprintf("row_count: %d", nrow(ejscreen_tidy)))

  agent <- new_check_agent(ejscreen_tidy, label = "EJScreen Validation") %>%
    check_row_count_range(min_rows = 200000, max_rows = 260000, severity = "stop") %>%
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

  message("EJScreen validation checks passed successfully.")
  return(TRUE)
}

#' Crosswalk EJScreen onto EPA SABs
#' @param dataset_id "clean_sabs_ejscreen"
run_clean_sabs_ejscreen_pipeline <- function(config, dataset_id = "clean_sabs_ejscreen") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link

  message("Downloading raw EJScreen from S3...")
  # coerce_character = FALSE - the score variables need to stay numeric for
  # the population-weighted interpolation step downstream
  ejscreen <- s3_read_csv(raw_link, coerce_character = FALSE) %>%
    mutate(id = as.character(id),
           geoid_tidy = case_when(nchar(id) < 12 ~ paste0("0", id), TRUE ~ id))

  # need to grab 2022 geometries because I found missing ones in CT 
  # because: "For 2022, the Census Bureau implemented changes in Connecticut due 
  # to new county equivalent geographic units." <- based on EJ screen documentation
  message("Pulling 2022 census block group geometries...")
  census_block_groups <- tidycensus::get_acs(
    geography = "block group",
    variables = c(total_pop = "B01003_001"),
    state = unique(ejscreen$st_abbrev),
    year = 2022,
    geometry = TRUE)

  ejscreen_sf <- merge(ejscreen, census_block_groups,
                       by.x = "geoid_tidy", by.y = "GEOID") %>%
    st_as_sf() %>%
    select(-c(NAME:moe))

  run_sabs_xwalk_pipeline(config, dataset_id, xwalk_dataset_name="ejscreen",
                           sf_data_census=ejscreen_sf, fips_col = "geoid_tidy",
                           blocks_2020 = TRUE)
}
