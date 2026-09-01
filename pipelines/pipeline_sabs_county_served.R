###############################################################################
# SABs County Served
#
# raw_sabs_county_served - intersects EPA SABs against 2024 TIGER county
# boundaries, state by state, to find every county a service area overlaps.
#
# clean_sabs_county_served - filters raw SABs county intersection to counties
# that a SAB has a 10% area overlap with. Each pwsid is collapsed to one string.
###############################################################################

#' Intersect EPA SABs against 2024 TIGER county boundaries, state by state.
#' @param config Main config
#' @param dataset_id "raw_sabs_county_served"
#' @return Data frame: pwsid, county_name, state_name, sab_area, county_area,
#'   pct_intersection, pct_inter_sab, pct_inter_county
run_sabs_county_served_pipeline <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  sabs_link <- sub_config$input_links$sabs_link
  link <- sub_config$link

  message("Reading EPA SABs from S3...")
  epa_sabs <- s3_read_geojson(sabs_link)

  message("Getting 2024 county boundaries from TIGER...")
  us_counties <- tigris::counties()

  message("Simplifying EPA SABs and US counties and finding original area...")
  epa_sabs_simp <- epa_sabs %>%
    select(pwsid, epic_states_intersect) %>%
    mutate(sab_area = sf::st_area(.))

  us_counties_simp <- us_counties %>%
    select(STATEFP, NAMELSAD) %>%
    mutate(county_area = sf::st_area(.))

  us_states <- tigris::states() %>%
    as.data.frame() %>%
    select(STATEFP, STUSPS)

  us_counties_simp_state <- merge(us_counties_simp, us_states, by = "STATEFP") %>%
    relocate(STUSPS) %>%
    sf::st_transform(crs = sf::st_crs(epa_sabs))

  states_to_loop <- unique(us_counties_simp_state$STUSPS)
  # we don't have SABs for USVI and AS (yet)
  states_to_loop <- states_to_loop[!grepl("AS|VI", states_to_loop)]

  message(sprintf("Intersecting SABs against counties for %d states...", length(states_to_loop)))
  pwsid_counties_list <- list()
  for (state_i in states_to_loop) {
    message(sprintf("On State: %s", state_i))

    us_county_i <- us_counties_simp_state %>% filter(STUSPS == state_i)
    message(sprintf("Counties Identified: %d", nrow(us_county_i)))

    sabs_state_i <- epa_sabs_simp %>% filter(grepl(state_i, epic_states_intersect))

    # I prefer st_intersection over a st_join so I can figure out how much the SAB
    # overlaps w/ the county boundary. This allows us to remove very slight 
    # overlaps due to boundary error. 
    sabs_intersection_i <- sf::st_intersection(sabs_state_i, us_county_i) %>%
      sf::st_make_valid()
    message("Intersection Complete.")

    sabs_county_i <- sabs_intersection_i %>%
      mutate(pct_intersection = sf::st_area(.),
             pct_inter_sab = 100 * (pct_intersection / sab_area)) %>%
      rename(county_name = NAMELSAD, state_name = STUSPS) %>%
      as.data.frame() %>%
      select(pwsid, county_name, state_name, sab_area, county_area,
             pct_intersection, pct_inter_sab)

    pwsid_counties_list[[state_i]] <- sabs_county_i
  }

  pwsid_counties_all <- bind_rows(pwsid_counties_list) %>%
    mutate(pct_inter_sab = as.numeric(pct_inter_sab),
           pct_inter_county = 100 * (as.numeric(pct_intersection) / as.numeric(county_area)))

  message("Validating raw_sabs_county_served...")
  validate_raw_sabs_county_served(config, pwsid_counties_all, dataset_id)

  message(sprintf("Writing raw SABs county intersection to S3 at %s...", link))
  s3_write_csv(pwsid_counties_all, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(pwsid_counties_all)
}

#' Pointblank validations for the raw SABs county intersection
#' @param config Main config
#' @param pwsid_counties_all Raw intersection data frame
#' @param dataset_id "raw_sabs_county_served"
validate_raw_sabs_county_served <- function(config, pwsid_counties_all, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  agent <- new_check_agent(pwsid_counties_all, label = "Raw SABs County Served Validation") %>%
    check_column_complete(pwsid, severity = "warn") %>%
    check_row_count_range(min_rows = 1, max_rows = 5000000, severity = "warn") %>%
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

  message("Raw SABs county served validation checks passed successfully.")
  return(TRUE)
}

#' Filter the raw SABs/county intersection to counties that are serviced based
#' on a >= 10% threshold for service area overlap. Collapse to one row per pwsid.
#' @param config Main config
#' @param dataset_id "clean_sabs_county_served"
#' @return Data frame: pwsid, county_served
run_clean_sabs_county_served_pipeline <- function(config, dataset_id = "clean_sabs_county_served") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  validation_sabs_link <- sub_config$validation_sabs_link
  link <- sub_config$link

  message("Reading raw SABs county intersection...")
  pwsid_counties_all <- s3_read_csv(raw_link, coerce_character = FALSE)

  message("Reading EPA SABs from S3 to confirm full SAB coverage...")
  epa_sabs <- s3_read_geojson(validation_sabs_link)

  # Using a 10% cutoff. 20% missed several large systems whose overlap with
  # any single county was under 20% of the SAB's total area because it spanned
  # many counties
  pwsid_counties_overcutoff <- pwsid_counties_all %>%
    filter(pct_inter_sab >= 10 | pct_inter_county >= 10) %>%
    mutate(county_state = paste0(county_name, ", ", state_name)) %>%
    group_by(pwsid) %>%
    summarize(county_served = paste(unique(county_state), collapse = "; "), .groups = "drop")

  message("Validating clean_sabs_county_served...")
  validate_clean_sabs_county_served(config, pwsid_counties_overcutoff, dataset_id, nrow(epa_sabs))

  message(sprintf("Writing clean SABs county served to S3 at %s...", link))
  s3_write_csv(pwsid_counties_overcutoff, link, acl = "public-read")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(pwsid_counties_overcutoff)
}

#' Pointblank validations for the clean SABs county served summary
#' @param config Main config
#' @param pwsid_counties_overcutoff Clean county-served data frame
#' @param dataset_id "clean_sabs_county_served"
#' @param epa_sabs_row_count Row count of the full EPA SABs dataset to
#'   confirm the 10% intersection cutoff didn't silently drop any SABs
validate_clean_sabs_county_served <- function(config, pwsid_counties_overcutoff, dataset_id, epa_sabs_row_count) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  agent <- new_check_agent(pwsid_counties_overcutoff, label = "Clean SABs County Served Validation") %>%
    check_column_complete(pwsid, severity = "warn") %>%
    check_column_complete(county_served, severity = "warn") %>%
    rows_distinct(columns = vars(pwsid), actions = .check_action_levels("warning"),
                  label = "No duplicate pwsid rows") %>%
    check_row_count_range(min_rows = floor(epa_sabs_row_count * 0.95), max_rows = epa_sabs_row_count,
                           severity = "warn") %>%
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

  message("Clean SABs county served validation checks passed successfully.")
  return(TRUE)
}
