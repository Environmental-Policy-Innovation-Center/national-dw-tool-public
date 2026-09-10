###############################################################################
# EPA SABs Census Crosswalk & 10-Year Percent Change
#
# clean_epa_sabs_xwalk pulls ACS demographic + income variables and
# weights them onto each SAB using EPA's ORD building-footprint crosswalk.
#
# clean_epa_sabs_crosswalk_pct_change re-runs the same crosswalk 10 years
# back using a manually downloaded NHGIS 2020-to-2010 tract crosswalkand diffing
# it against the current crosswalk. This pipeline is triggered after
# clean_epa_sabs_xwalk pipeline. Note: the NHGIS cross walk is less accurate
# than the ORD block/block-group data. Many of our census vars are only
# available at the tract level.
# 
# Instructions for downloading NHGIS 2020 to 2010 tract to tract data:
#   1. Go to https://www.nhgis.org/geographic-crosswalks
#   2. Download the national 2020 census tracts -> 2010 census tracts
#      crosswalk (tract-to-tract, 2020 vintage to 2010 vintage)
#   3. Save it at ./data/nhgis_tr2020_tr2010/nhgis_tr2020_tr2010.csv
#
# File layout:
#   1. Shared helpers - used by both the states and territory paths
#   2. States path - ORD building-weight crosswalk (50 states+DC+PR)
#   3. Territory path - AS/MP/GU/VI, no ORD crosswalk exists for these
#   4. clean_epa_sabs_xwalk pipeline
#   5. clean_epa_sabs_crosswalk_pct_change pipeline
###############################################################################

###############################################################################
## 1. SHARED HELPERS
###############################################################################

#' Census API key for get_acs(), or NULL if CENSUS_API_KEY isn't set.
#' get_acs() treats key = "" as an error, so this must return NULL when the
#' env var is not set.
.census_api_key <- function() {
  key <- Sys.getenv("CENSUS_API_KEY")
  if (key == "") NULL else key
}

###############################################################################
## 2. STATES PATH - ORD BUILDING-WEIGHT CROSSWALK (50 states + DC + PR)
###############################################################################

#' Pull EPA's ORD tract-to-SAB building weight crosswalk.
#' Also drops 7 pwsids where building weight is > 1
get_ord_tract_xwalk <- function() {
  read.csv("https://raw.githubusercontent.com/USEPA/ORD_SAB_Model/refs/heads/main/Version_History/2_1/Census_Tables/Tracts_V_2_1.csv") %>%
    janitor::clean_names() %>%
    mutate(geoid20 = as.character(geoid20),
           geoid20 = case_when(nchar(geoid20) == 10 ~ paste0("0", geoid20), TRUE ~ geoid20),
           pwsid = trimws(pwsid)) %>%
    filter(!(pwsid %in% c("FL1070685", "FL1190789", "FL6580531", "IA2573701",
                          "MT0001923", "VA5019052", "ND2801430")))
}

#' Get list of states for pulling ACS census data. Excludes GU/MP/AS/VI -
#' none of the island territories have standard ACS 5-year tract coverage
#' (confirmed: get_acs() errors on all four, both current-year and the
#' pct-change pipeline's 10-years-prior pull).
#' @param epa_sabs EPA SABs sf object
#' @return Character vector of state abbreviations
.census_xwalk_states <- function(epa_sabs) {
  states <- unique(epa_sabs$epic_states_intersect)
  states_filt <- states[!grepl(",", states)]
  states_filt <- states_filt[!grepl("GU|MP|AS|VI", states_filt)]
}

#' Turn a census_var_methods sheet into a named var_code vector for the ACS
#' pull. Drops NAs and income vars.
#' @param var_sheet Slice of the census_var_methods sheet
#' @return List: nona (full vector, NAs dropped), noinc (nona minus income vars)
.census_var_vectors <- function(var_sheet) {
  full <- setNames(var_sheet$var, var_sheet$name)
  nona <- full[!is.na(full)]
  list(
    nona = nona,
    noinc = nona[!(nona %in% c("B19013_001", "B19080_001"))]
  )
}

#' Turn each var's universe column into a percentage, e.g.
#' age_over_61_per = 100 * (age_over_61 / total_pop).
#' @param summed_totals pwsid-level summed raw counts
#' @param var_sheet Slice of the census_var_methods sheet used for this pull
#' @param var_vector Vector used for the pull
#' @return summed_totals with the percentage columns added
.apply_percentage_formulas <- function(summed_totals, var_sheet, var_vector) {
  percentage_functions <- var_sheet %>%
    filter(!is.na(universe)) %>%
    mutate(equation = paste0("100*(", name, "/", universe, ")")) %>%
    filter(var %in% var_vector)

  easy_percentages_functions <- setNames(
    lapply(percentage_functions$equation, function(eq) rlang::parse_expr(eq)),
    paste0(percentage_functions$name, "_per")
  )

  summed_totals %>% mutate(!!!easy_percentages_functions)
}

#' Apply the vars with more complicated formulas from the calc_after_interp col.
#' @param summed_totals pwsid-level summed raw counts
#' @param var_sheet Slice of the census_var_methods sheet used for this pull
#' @param raw_count_cols Raw summed-count columns to drop once consumed
#' @return Data frame with just pwsid + the computed columns
.apply_complicated_formulas <- function(summed_totals, var_sheet, raw_count_cols) {
  complicated_functions <- var_sheet %>%
    filter(!is.na(calc_after_interp) & is.na(interp_method))

  cols_to_drop <- setdiff(raw_count_cols, complicated_functions$name)

  complicated_functions <- setNames(
    lapply(complicated_functions$calc_after_interp, function(eq) rlang::parse_expr(eq)),
    complicated_functions$name
  )

  summed_totals %>%
    mutate(!!!complicated_functions) %>%
    select(-all_of(cols_to_drop))
}

#' Interpolate median household income and lowest-income-quintile household
#' count onto SABs, state by state.
#' @param config Main config
#' @param epa_sabs EPA SABs sf object
#' @param income_vars Vector of income ACS variable codes
#' @param acs_year Year to pull ACS data for
#' @param blocks_2020 Whether to use 2020 or 2010 census blocks as weights
#' @return Data frame: pwsid, mhi, hh_inc_lowest_quintile
interpolate_income_vars <- function(config, epa_sabs, income_vars, acs_year, blocks_2020 = TRUE) {
  states_filt <- .census_xwalk_states(epa_sabs)

  inc_vars_census <- tidycensus::get_acs(
    geography = "tract", variables = income_vars,
    state = states_filt, year = as.numeric(acs_year), geometry = TRUE,
    key = .census_api_key()
  )

  # Pivoting to wide format and cleaning names
  inc_vars_wide <- inc_vars_census %>%
    select(-moe) %>%
    pivot_wider(names_from = variable, values_from = estimate)

  # Grabbing total universes
  mhi_uni <- sum(inc_vars_wide$mhi_census, na.rm = TRUE)
  lowest_quintile_uni <- sum(inc_vars_wide$hh_inc_lowest_quintile_census, na.rm = TRUE)

  # Grabbing % of total universe to interpolate using spatially intensive method
  inc_vars_uni <- inc_vars_wide %>%
    mutate(mhi_pct_uni = mhi_census / mhi_uni,
           income_lowest_quintile_uni = hh_inc_lowest_quintile_census / lowest_quintile_uni) %>%
    relocate(geometry, .after = last_col()) %>%
    sf::st_transform(crs = 5070)

  # turning off spherical geometry because we are working off a projected CRS
  sf::sf_use_s2(FALSE)

  # Pulled once outside the loop - tigris::states() returns every state
  # regardless of state_i, so calling it per-iteration just re-fetched the
  # same data on every pass.
  all_states <- tigris::states()

  # Loop through states and weight by census blocks
  interp_inc <- data.frame()
  for (state_i in states_filt) {
    state_name <- all_states %>%
      filter(STUSPS == state_i) %>%
      select(NAME) %>% as.data.frame() %>% select(-geometry)
    message(sprintf("Interpolating income for %s (%s)...", state_i, state_name))

    sabs_i <- epa_sabs %>%
      filter(grepl(state_i, epic_states_intersect)) %>%
      sf::st_transform(crs = 5070) %>%
      select(pwsid)

    # NOTE: ACS data filtered on ", West Virginia" never matches anything
    # so WV tracts were being interpolated into Virginia's
    inc_vars_uni_i <- if (state_name == "Virginia") {
      inc_vars_uni %>%
        filter(stringr::str_detect(NAME, paste0(state_name, "$"))) %>%
        filter(!grepl("; West Virginia", NAME)) %>%
        select(GEOID, mhi_pct_uni, income_lowest_quintile_uni)
    } else {
      inc_vars_uni %>%
        filter(stringr::str_detect(NAME, paste0(state_name, "$"))) %>%
        select(GEOID, mhi_pct_uni, income_lowest_quintile_uni)
    }

    state_blocks <- .grab_census_blocks(sf_data_i = inc_vars_uni_i, fips_col = "GEOID",
                                        state_i = state_i, blocks_2020 = blocks_2020)

    pw_interp_inc_i <- tidycensus::interpolate_pw(
      from = inc_vars_uni_i, to = sabs_i, to_id = "pwsid",
      extensive = FALSE, weights = state_blocks,
      weight_column = "housing_weight", crs = 5070
    ) %>%
      mutate(state_interp = state_i)

    interp_inc <- rbind(interp_inc, pw_interp_inc_i)
  }
  sf::sf_use_s2(TRUE)

  # Relate back to universe
  interp_inc_df <- interp_inc %>%
    as.data.frame() %>%
    select(-geometry) %>%
    mutate(mhi = mhi_pct_uni * mhi_uni,
           hh_inc_lowest_quintile = income_lowest_quintile_uni * lowest_quintile_uni)

  # Handle systems that overlap with multiple states using a weighted mean: 
  # adding na.rm = T to handle systems that may slightly overlap with multiple states, 
  # and may therefore be "NA" on a specific state loop with very minimal overlap
  interp_inc_df %>%
    group_by(pwsid) %>%
    summarize(mhi = weighted.mean(mhi, na.rm = TRUE),
              hh_inc_lowest_quintile = weighted.mean(hh_inc_lowest_quintile, na.rm = TRUE),
              .groups = "drop")
}

###############################################################################
## 3. TERRITORY PATH - AS / MP / GU / VI
# Territory vars are pulled from the 2020 Decennial Census (DHC) using an API
# request because they are not included in the ACS 5-year data.
###############################################################################

# One row per territory: USPS code, state FIPS, county FIPS codes to loop
# over, and which territory_var_methods column names its variable codes.
.territory_defs <- list(
  as = list(usps = "AS", fips = "60", county_fips = c("010", "020", "030", "040", "050"), var_col = "as_var"),
  mp = list(usps = "MP", fips = "69", county_fips = c("085", "100", "110", "120"), var_col = "mp_var"),
  gu = list(usps = "GU", fips = "66", county_fips = c("010"), var_col = "gu_var"),
  vi = list(usps = "VI", fips = "78", county_fips = c("010", "020", "030"), var_col = "vi_var")
)
.territory_income_vars <- c("HCT11_001N", "PCT59_001N", "PCT58_001N", "PCT56_001N")

#' Reshape territory_var_methods to look like census_var_methods (var, name,
#' universe, calc_after_interp, interp_method) for one territory, so
#' .apply_percentage_formulas()/.apply_complicated_formulas() works for these.
#' @param config Main config
#' @param territory_key One of names(.territory_defs), e.g. "as"
#' @return Data frame shaped like census_var_sheet, one row per variable
get_territory_var_sheet <- function(config, territory_key) {
  var_col <- .territory_defs[[territory_key]]$var_col
  full_sheet <- get_census_var_methods_territory(config)
  full_sheet %>%
    filter(!is.na(.data[[var_col]]), .data[[var_col]] != "") %>%
    transmute(var = .data[[var_col]], name, universe, calc_after_interp, interp_method)
}

# Caches the territory_var_methods sheet per session.
.territory_var_env <- new.env(parent = emptyenv())

#' Grabs the territory_var_methods sheet (variable codes per territory, plus
#' the same universe/calc_after_interp columns census_var_methods has).
#' @param config Main config
get_census_var_methods_territory <- function(config) {
  if (is.null(.territory_var_env$data)) {
    googlesheets4::gs4_deauth()
    .territory_var_env$data <- googlesheets4::read_sheet(
      config$metadata$census_var_methods_sheet_url, sheet = "territory_var_methods"
    ) %>%
      janitor::clean_names()
  }
  .territory_var_env$data
}

#' Pull one territory's 2020 Decennial Census (DHC) data. The DHC API doesn't
#' support a state-wide tract query for territories, so this loops over each
#' county FIPS individually and combines the results.
#' @param territory_def One element of .territory_defs
#' @param var_codes Vector of census var codes to pull
#' @param decennial_year Decennial census year
#' @return Data frame: GEOID, NAME, and one column per var_codes name
pull_territory_decennial_data <- function(territory_def, var_codes, decennial_year = 2020) {
  census_api_key <- Sys.getenv("CENSUS_API_KEY")
  var_code_str <- paste(unname(var_codes), collapse = ",")
  if (var_code_str == "" || is.na(var_code_str)) {
    stop(sprintf("No valid census var codes for territory %s.", territory_def$usps), call. = FALSE)
  }

  message(sprintf("Pulling %d decennial variables for %s across %d counties...",
                  length(var_codes), territory_def$usps, length(territory_def$county_fips)))

  all_county_frames <- lapply(territory_def$county_fips, function(county) {
    message(sprintf("Fetching tract data for %s county FIPS %s...", territory_def$usps, county))
    # Construct the web address for the API call
    # 1. base url
    # 2. add the query parameters
    # 3. append the API key for authentication
    base_url <- sprintf("https://api.census.gov/data/%d/dec/dhc%s",
                        as.integer(decennial_year), tolower(territory_def$usps))
    req <- httr2::request(base_url) %>%
      httr2::req_url_query(
        `get` = paste0("NAME,", var_code_str),
        `for` = "tract:*",
        `in` = sprintf("state:%s county:%s", territory_def$fips, county)
      )
    if (census_api_key != "") {
      req <- req %>% httr2::req_url_query(`key` = census_api_key)
    }

    parsed_json <- req %>%
      httr2::req_perform() %>%
      httr2::resp_body_string() %>%
      jsonlite::fromJSON()

    col_names <- parsed_json[1, ]
    data_matrix <- parsed_json[-1, , drop = FALSE]
    message(sprintf("  Finished county pull with %d tracts returned.", nrow(data_matrix)))

    county_data <- tibble::as_tibble(data_matrix, .name_repair = "minimal")
    colnames(county_data) <- col_names

    county_data %>%
      rename(any_of(unlist(var_codes))) %>%
      mutate(GEOID = stringr::str_c(state, county, tract)) %>%
      select(!any_of(c("state", "county", "tract"))) %>%
      mutate(across(-any_of(c("NAME", "GEOID")), ~ type.convert(., as.is = TRUE)))
  })

  bind_rows(all_county_frames)
}

#' TODO: Fill in this function which is called in the commented out line 450.
#' Interpolate every territory's decennial census variables onto SABs/
#' @param config Main config
#' @param epa_sabs EPA SABs sf object
#' @param decennial_year Decennial census year
#' @return pwsid-level data frame with percentage/complicated-formula columns
#'   applied, same shape as the states-side pwsid_census_xwalk
interpolate_territory_vars <- function(config, epa_sabs, decennial_year = 2020) {
  # TODO: add interpolation code
}

###############################################################################
## 4. PIPELINE - CURRENT-YEAR CROSSWALK (dataset_id: clean_epa_sabs_xwalk)
###############################################################################

#' Build the current-year EPA SABs census crosswalk: ACS demographic and
#' income variables, weighted onto each pwsid via EPA's ORD building-footprint
#' crosswalk.
#' @param config Main config
#' @param dataset_id "clean_epa_sabs_xwalk"
#' @return pwsid-level crosswalk data frame
run_epa_sabs_xwalk_pipeline <- function(config, dataset_id = "clean_epa_sabs_xwalk") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  link <- sub_config$link
  epa_sabs_link <- sub_config$input_links$epa_sabs_link
  crosswalk_year <- sub_config$crosswalk_year

  message("Downloading EPA SABs...")
  epa_sabs <- s3_read_geojson(epa_sabs_link)

  message("Reading census var methods sheet...")
  census_var_sheet <- get_census_var_methods(config)
  census_vars <- .census_var_vectors(census_var_sheet)

  message("Downloading ORD tract-to-SAB building weight crosswalk...")
  ord_tract_xwalk <- get_ord_tract_xwalk()

  states_filt <- .census_xwalk_states(epa_sabs)

  message(sprintf("Pulling %d ACS demographic variables for %d states...",
                  length(census_vars$noinc), length(states_filt)))
  census <- tidycensus::get_acs(
    geography = "tract", variables = census_vars$noinc,
    state = states_filt, year = as.numeric(crosswalk_year), geometry = FALSE,
    key = .census_api_key()
  )

  # Pivoting to wide format and cleaning names
  census_wide <- census %>%
    select(-moe) %>%
    pivot_wider(names_from = variable, values_from = estimate)

  message("Merging with EPA ORD crosswalk...")
  merged_xwalk <- merge(census_wide, ord_tract_xwalk, by.x = "GEOID", by.y = "geoid20")

  # Multiplying by weights from EPA's ORD xwalk
  weighted_vars <- merged_xwalk %>%
    mutate(across(names(census_vars$noinc), ~ .x * bldg_weight))

  # Summing totals by pwsid
  summed_totals <- weighted_vars %>%
    group_by(pwsid) %>%
    summarize(across(total_pop:pop_pov_level_above_200, ~ sum(.x, na.rm = TRUE)), .groups = "drop")

  pwsid_census_pcts <- .apply_percentage_formulas(summed_totals, census_var_sheet, census_vars$noinc)

  more_complicated_pcts <- .apply_complicated_formulas(
    summed_totals, census_var_sheet, names(select(summed_totals, total_pop:pop_pov_level_above_200))
  ) %>%
    mutate(age_over_61_per = case_when(age_over_61_per < 0 ~ 0, TRUE ~ age_over_61_per))

  pwsid_census_xwalk <- merge(pwsid_census_pcts, more_complicated_pcts, by = "pwsid", all = TRUE)

  message("Interpolating income variables...")
  income_vars <- census_vars$nona[census_vars$nona %in% c("B19013_001", "B19080_001")]
  income_summary <- interpolate_income_vars(config, epa_sabs, income_vars, crosswalk_year, blocks_2020 = TRUE)

  pwsid_census_xwalk_f <- merge(pwsid_census_xwalk, income_summary, by = "pwsid", all = TRUE)

  message("Calculating population density...")
  pwsid_pops <- pwsid_census_xwalk_f %>% select(pwsid, total_pop)
  epa_sabs_pop_den <- epa_sabs %>%
    select(pwsid, epic_area_mi2) %>%
    left_join(pwsid_pops, by = "pwsid") %>%
    mutate(epic_pop_density = total_pop / epic_area_mi2) %>%
    relocate(epic_pop_density, .after = epic_area_mi2) %>%
    as.data.frame() %>%
    select(-geometry)

  epa_sabs_xwalk <- merge(pwsid_census_xwalk_f, epa_sabs_pop_den, all = TRUE)

  message("Summarizing most common water rate bin...")
  water_rates <- epa_sabs_xwalk %>%
    select(pwsid, water_rate_less_125_per:water_rate_over_1000_per) %>%
    distinct()

  most_common_rates <- water_rates %>%
    mutate(pwsid = as.character(pwsid)) %>%
    rowwise() %>%
    # picks each SAB's highest-percentage rate bin (which.max) then looks up
    # that bin's column name (+1 skips the pwsid column)
    mutate(most_common_rate = as.character(list(
      colnames(water_rates)[which.max(c_across(water_rate_less_125_per:water_rate_over_1000_per)) + 1]
    ))) %>%
    ungroup() %>%
    mutate(most_common_rate_tidy = case_when(
      most_common_rate == "water_rate_over_1000_per" ~ "Most people pay > $1000 for water & sewer annually",
      most_common_rate == "water_rate_less_125_per" ~ "Most people pay < $125 for water & sewer annually",
      most_common_rate == "water_rate_between_250_499_per" ~ "Most people pay between $250-499 for water & sewer annually",
      most_common_rate == "water_rate_between_500_749_per" ~ "Most people pay between $500-749 for water & sewer annually",
      most_common_rate == "water_rate_between_125_249_per" ~ "Most people pay between $125-249 for water & sewer annually",
      most_common_rate == "water_rate_between_750_999_per" ~ "Most people pay between $750-999 for water & sewer annually",
      TRUE ~ "No Information on annual water & sewer rates"
    )) %>%
    select(pwsid, most_common_rate_tidy)

  epa_sabs_xwalk_states <- merge(epa_sabs_xwalk, most_common_rates, by = "pwsid", all = TRUE) %>%
    # this column is an artifact of the crosswalking process
    select(-hh_num_vehicles_per) %>%
    # relocate for clarity 
    relocate(age_over_61_per, .after = age60_61_per)

  # Uncomment once territory interpolation code is completed
  # message("Interpolating island territory (AS/MP/GU/VI) variables...")
  # territory_xwalk <- interpolate_territory_vars(config, epa_sabs, decennial_year = 2020)

  # message("Binding states and territory xwalks together...")
  # epa_sabs_xwalk_final <- bind_rows(epa_sabs_xwalk_states, territory_xwalk)
  epa_sabs_xwalk_final <- epa_sabs_xwalk_states

  message("Validating clean_epa_sabs_xwalk...")
  validate_epa_sabs_xwalk(config, epa_sabs_xwalk_final, dataset_id)

  message(sprintf("Writing EPA SABs census crosswalk to S3 at %s...", link))
  s3_write_csv(epa_sabs_xwalk_final, link, acl = "public-read")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(epa_sabs_xwalk_final)
}

#' Pointblank validations for the EPA SABs census crosswalk
#' @param config Main config
#' @param epa_sabs_xwalk_final Crosswalk data frame
#' @param dataset_id "clean_epa_sabs_xwalk"
validate_epa_sabs_xwalk <- function(config, epa_sabs_xwalk_final, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  agent <- new_check_agent(epa_sabs_xwalk_final, label = "EPA SABs Census Crosswalk Validation") %>%
    check_column_complete(pwsid, severity = "stop") %>%
    rows_distinct(columns = vars(pwsid), actions = action_levels(warn_at = 1),
                  label = "No duplicate pwsid rows") %>%
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

  message("EPA SABs census crosswalk validation checks passed successfully.")
  return(TRUE)
}

###############################################################################
## 5. PIPELINE - 10-YEAR PERCENT CHANGE (dataset_id: clean_epa_sabs_crosswalk_pct_change)
###############################################################################

#' Re-run the states-path crosswalk 10 years back, then diff it against the
#' current clean_epa_sabs_xwalk to get a % change per pwsid. The
#' NHGIS 2020-to-2010 tract crosswalk must be downloaded manually and saved
#' at local_source_path.
#' @param config Main config
#' @param dataset_id "clean_epa_sabs_crosswalk_pct_change"
#' @return pwsid-level % change data frame
run_epa_sabs_xwalk_pct_change_pipeline <- function(config, dataset_id = "clean_epa_sabs_crosswalk_pct_change") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  link <- sub_config$link
  epa_sabs_link <- sub_config$input_links$epa_sabs_link
  xwalk_link <- sub_config$input_links$xwalk_link
  sdwis_viols_link <- sub_config$input_links$sdwis_viols_link
  nhgis_xwalk_path <- sub_config$local_source_path
  crosswalk_year <- sub_config$crosswalk_year

  if (!file.exists(nhgis_xwalk_path)) {
    stop(sprintf(
      "NHGIS 2020-to-2010 tract crosswalk not found at %s. Download it from https://www.nhgis.org/geographic-crosswalks and place it there.",
      nhgis_xwalk_path
    ), call. = FALSE)
  }

  year_lag <- as.numeric(crosswalk_year) - 10

  message("Downloading EPA SABs...")
  epa_sabs <- s3_read_geojson(epa_sabs_link)

  message("Reading census var methods sheet and filter out tables that aren't available...")
  census_var_sheet <- get_census_var_methods(config) %>%
    filter(!grepl("B25134", var)) %>% # water rates
    filter(!grepl("B15003", var)) %>% # schooling
    filter(!grepl("B28008", var)) %>% # total pop with a computer
    filter(!grepl("B27001", var)) %>% # health insurance
    filter(!(name %in% c("no_health_insurance_per")))

  census_vars <- .census_var_vectors(census_var_sheet)

  message("Downloading ORD tract-to-SAB building weight crosswalk...")
  ord_tract_xwalk <- get_ord_tract_xwalk()

  message(sprintf("Reading NHGIS 2020 to 2010 tract crosswalk from %s...", nhgis_xwalk_path))
  nhgis_crosswalk_simple <- read.csv(nhgis_xwalk_path) %>%
    select(tr2020ge, tr2010ge, wt_hu) %>%
    rename(tract_2020 = tr2020ge, tract_2010 = tr2010ge) %>%
    mutate(tract_2010 = as.character(tract_2010), tract_2020 = as.character(tract_2020),
           tract_2010 = case_when(nchar(tract_2010) == 10 ~ paste0("0", tract_2010), TRUE ~ tract_2010),
           tract_2020 = case_when(nchar(tract_2020) == 10 ~ paste0("0", tract_2020), TRUE ~ tract_2020))

  message("Re-weighting the ORD crosswalk from 2020 to 2010 tract geography...")
  main_xwalk <- merge(nhgis_crosswalk_simple, ord_tract_xwalk,
                      by.x = "tract_2020", by.y = "geoid20", all.y = TRUE) %>%
    filter(!is.na(pwsid))

  # unique() avoids double counting tract building totals that are
  # duplicated across every SAB overlapping the same tract
  total_buildings_tracts <- main_xwalk %>%
    mutate(tract_buildings_wt = tract_buildings * wt_hu) %>%
    group_by(tract_2010) %>%
    summarize(sum_2010_tract_buildings = sum(unique(tract_buildings_wt)), .groups = "drop")

  final_xwalk_geoids <- merge(main_xwalk, total_buildings_tracts, by = "tract_2010", all.x = TRUE) %>%
    mutate(tract_o_buildings_wt = tract_o_buildings * wt_hu) %>%
    group_by(tract_2010, pwsid, sum_2010_tract_buildings) %>%
    summarize(new_overlap = sum(tract_o_buildings_wt), .groups = "drop") %>%
    mutate(new_wt = new_overlap / sum_2010_tract_buildings,
    # NOTE - there are ~12 instances where a single 2010 tract was split into
    # 1+ 2020 tracts, and their tract_buildings from the ORD crosswalk
    # were the same. Capping these at a 1 weight, since often these 
    # kinds of sabs are rare and would likely cover 100% of the 2010 tract anyways. 
    # example: "DC0000002" & 2010 tract = "11001005500"
           new_wt = case_when(new_wt > 1 ~ 1, TRUE ~ new_wt))

  states_filt <- .census_xwalk_states(epa_sabs)

  message(sprintf("Pulling %d ACS demographic variables for %d states, %s...",
                  length(census_vars$noinc), length(states_filt), year_lag))
  census <- tidycensus::get_acs(
    geography = "tract", variables = census_vars$noinc,
    state = states_filt, year = as.numeric(year_lag), geometry = FALSE,
    key = .census_api_key()
  )

  census_wide <- census %>%
    select(-moe) %>%
    pivot_wider(names_from = variable, values_from = estimate)

  merged_xwalk <- merge(census_wide, final_xwalk_geoids, by.x = "GEOID", by.y = "tract_2010")

  weighted_vars <- merged_xwalk %>%
    mutate(across(names(census_vars$noinc), ~ .x * new_wt))

  summed_totals <- weighted_vars %>%
    group_by(pwsid) %>%
    summarize(across(total_pop:pop_pov_level_above_200, ~ sum(.x, na.rm = TRUE)), .groups = "drop")

  pwsid_census_pcts <- .apply_percentage_formulas(summed_totals, census_var_sheet, census_vars$noinc)

  more_complicated_pcts <- .apply_complicated_formulas(
    summed_totals, census_var_sheet, names(select(summed_totals, total_pop:pop_pov_level_above_200))
  )

  pwsid_census_xwalk <- merge(pwsid_census_pcts, more_complicated_pcts, by = "pwsid", all = TRUE)

  message("Interpolating 2010-vintage income variables...")
  income_vars <- census_vars$nona[census_vars$nona %in% c("B19013_001", "B19080_001")]
  income_summary <- interpolate_income_vars(config, epa_sabs, income_vars, year_lag, blocks_2020 = FALSE)

  pwsid_census_xwalk_f <- merge(pwsid_census_xwalk, income_summary, by = "pwsid", all = TRUE) %>%
    mutate(acs_year = year_lag)

  message("Adjusting income variables for inflation...")
  crosswalk_2011 <- pwsid_census_xwalk_f %>%
    select(-hh_num_vehicles_per) %>%
    # finding what to adjust by using https://www.bls.gov/data/inflation_calculator.htm
    # testing calculations = $45,216.00 from Jan 2011 = $53,707.79 in Jan 2021; 
    # so multiply 2011 income vars by 1.187805
    mutate(hh_inc_lowest_quintile = hh_inc_lowest_quintile * 1.187805,
           mhi = mhi * 1.187805)

  message(sprintf("Downloading current EPA SABs census crosswalk from %s...", xwalk_link))
  crosswalk_2021 <- s3_read_csv(xwalk_link, coerce_character = FALSE) %>%
    mutate(acs_year = as.numeric(crosswalk_year))

  full_pwsids <- crosswalk_2021 %>% select(pwsid)
  # rounding to remove decimal places (helps remove records that are 0.3
  # of a single person and would therefore potentially create a % change 
  # of like 5000%)
  crosswalk_2011_aligned <- merge(crosswalk_2011, full_pwsids, all.y = TRUE) %>%
    arrange(pwsid) %>%
    mutate(across(where(is.numeric), ~ round(., digits = 0)))
  crosswalk_2021_aligned <- crosswalk_2021 %>%
    arrange(pwsid) %>%
    select(names(crosswalk_2011_aligned)) %>%
    mutate(across(where(is.numeric), ~ round(., digits = 0)))

  message("Calculating 10-year percent change...")
  pct_change <- ((crosswalk_2021_aligned[, -1] - crosswalk_2011_aligned[, -1]) / crosswalk_2011_aligned[, -1]) * 100
  # note - inf = the 2011 value was 0, and NaN = the 2021 value was 0
  # add _pct_change to column names 
  colnames(pct_change) <- paste(colnames(pct_change), "pct_change_2011_2021", sep = "_")
  pct_change <- cbind(pwsid = crosswalk_2011_aligned$pwsid, pct_change)

  # to decode this a little bit better: 
  # inf = the 2011 value was 0 
  # nan = the 2021 and 2011 value was 0
  # NA = we simply just didnt have data for one/both of the years 
  # going to opt to change all of these to NA to avoid confusion in the tool
  pct_change_clean <- pct_change %>%
    mutate(across(where(is.numeric), ~ replace(., is.infinite(.) | is.nan(.), NA))) %>%
    select(-acs_year_pct_change_2011_2021) %>%
    # adding change flags for frontend 
    mutate(population_change_flag = case_when(
             total_pop_pct_change_2011_2021 > 0 ~ "Increase in people the last 10 years",
             total_pop_pct_change_2011_2021 < 0 ~ "Decrease in people the last 10 years",
             TRUE ~ NA),
           income_change_flag = case_when(
             mhi_pct_change_2011_2021 > 0 ~ "Increase in income the last 10 years",
             mhi_pct_change_2011_2021 < 0 ~ "Decrease in income the last 10 years",
             TRUE ~ NA))

  message(sprintf("Downloading SDWIS violations for < 10yr-operating flag from %s...", sdwis_viols_link))
  ws_info_less10yr <- tryCatch({
    s3_read_csv(sdwis_viols_link) %>%
      filter(total_viols_10yr == "Not Enough Data - Operating < 10 years") %>%
      select(pwsid)
  }, error = function(e) {
    message(sprintf(
      "Could not read %s (%s). Continuing without the < 10yr-operating flag.",
      sdwis_viols_link, conditionMessage(e)
    ))
    data.frame(pwsid = character())
  })

  pct_change_clean_nonew <- pct_change_clean %>%
    mutate(population_change_flag = case_when(
             pwsid %in% ws_info_less10yr$pwsid ~ "Not Enough Data - Operating < 10 years",
             TRUE ~ population_change_flag),
           income_change_flag = case_when(
             pwsid %in% ws_info_less10yr$pwsid ~ "Not Enough Data - Operating < 10 years",
             TRUE ~ income_change_flag))

  not_enough_data <- pct_change_clean_nonew %>%
    filter(population_change_flag == "Not Enough Data - Operating < 10 years") %>%
    mutate(across(where(is.numeric), ~ NA))
  enough_data <- pct_change_clean_nonew %>%
    filter(!(pwsid %in% not_enough_data$pwsid))

  final_pct_change_df <- bind_rows(not_enough_data, enough_data) %>%
    arrange(pwsid) %>%
    # creating a cap where if % change for pop or mhi is > 200 or < 200, it caps 
    # at 200 
    mutate(total_pop_pct_change_2011_2021_cap = case_when(
             total_pop_pct_change_2011_2021 > 200 | total_pop_pct_change_2011_2021 < -200 ~ 200,
             TRUE ~ total_pop_pct_change_2011_2021),
           mhi_pct_change_2011_2021_cap = case_when(
             mhi_pct_change_2011_2021 > 200 | mhi_pct_change_2011_2021 < -200 ~ 200,
             TRUE ~ mhi_pct_change_2011_2021))

  message("Validating clean_epa_sabs_crosswalk_pct_change...")
  validate_epa_sabs_xwalk_pct_change(config, final_pct_change_df, dataset_id)

  message(sprintf("Writing EPA SABs census crosswalk percent change to S3 at %s...", link))
  s3_write_csv(final_pct_change_df, link, acl = "public-read")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(final_pct_change_df)
}

#' Pointblank validations for the EPA SABs 10-year census crosswalk % change
#' @param config Main config
#' @param final_pct_change_df % change data frame
#' @param dataset_id "clean_epa_sabs_crosswalk_pct_change"
validate_epa_sabs_xwalk_pct_change <- function(config, final_pct_change_df, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  agent <- new_check_agent(final_pct_change_df, label = "EPA SABs Census Crosswalk Percent Change Validation") %>%
    check_column_complete(pwsid, severity = "stop") %>%
    rows_distinct(columns = vars(pwsid), actions = action_levels(warn_at = 1),
                  label = "No duplicate pwsid rows") %>%
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

  message("EPA SABs census crosswalk % change validation checks passed successfully.")
  return(TRUE)
}
