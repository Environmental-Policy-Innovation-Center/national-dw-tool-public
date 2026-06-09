# =============================================================================
# Libraries
# =============================================================================
library(tidyverse)
library(aws.s3)
library(tidycensus)
library(sf)
library(httr2)


# TODO - pull more recent data? CT and the ORD crosswalk vintage may present issues 
# TODO - double check all census mappings 
# TODO - revamp certain vars (age, education, etc.)
# TODO - does the worker need a secure way to access census keys? 

# =============================================================================
# Env variables
# =============================================================================
crosswalk_year <- "2021"
CENSUS_API_KEY = Sys.getenv("CENSUS_API_KEY")

# =============================================================================
# Supporting datasets for filtering and interpolating 
# =============================================================================
# method for interpolating census vars and geometries: 
gs4_deauth()
URL <- "https://docs.google.com/spreadsheets/d/15iVYq2v3Gpy5Zug3BhYC0vU4L-axV5g0drZt4-uLv-Q/edit?gid=1622581968#gid=1622581968"
# for crosswaking census variables
census_var_sheet <- read_sheet(URL, sheet = "census_var_methods") %>%
  janitor::clean_names()

# reading in SABs
epa_sabs <- aws.s3::s3read_using(st_read, 
                                 object = "s3://tech-team-data/national-dw-tool/clean/national/epa_sabs.geojson")

# ORD Tract + SAB Crosswalk - States
# While grabbing ORD crosswalks, this step:
# 1. Removes any odd characters from pwsid, since there is a small handful 
# with \n or \t
# 2. Filters out a few crosswalks. There are four instances where the building
# weight is >1. After giving these a deeper look, it is because these SABs were 
# duplicated in the original dataset. We've merged them for our work, but 
# since this merge did not occur before the crosswalk tables were developed, 
# we should remove them here to avoid any inaccurate results. 
ord_tract_xwalk <- read.csv("https://raw.githubusercontent.com/USEPA/ORD_SAB_Model/refs/heads/main/Version_History/2_1/Census_Tables/Tracts_V_2_1.csv") %>%
  janitor::clean_names() %>%
  mutate(geoid20 = as.character(geoid20)) %>%
  mutate(geoid20 = case_when(nchar(geoid20) == 10 ~ paste0("0", geoid20), 
                             TRUE~ geoid20)) %>%
  mutate(pwsid = trimws(pwsid)) %>%
  filter(!(pwsid %in% c("FL1070685", "FL1190789", "FL6580531", "IA2573701", 
                        "MT0001923", "VA5019052", "ND2801430")))

# =============================================================================
# American Community Survey (ACS) Census Data - States + DC + PR
# =============================================================================
# setnames census vars names to create a named vector: 
census_var_vector <- setNames(census_var_sheet$var, 
                              census_var_sheet$name)

# removing NAs from our named vector (these are calculated after crosswalking)
# and income vars (which need to be interpolated)
income_vars <- c("B19013_001", "B19080_001")
census_var_vector_nona <- census_var_vector[!is.na(census_var_vector)]
census_var_vector_noinc <- census_var_vector_nona[!(census_var_vector_nona %in% income_vars)]

# Get states for grabbing census data. Islands will be handled separately. 
states <- unique(epa_sabs$epic_states_intersect)
# these are water systems that intersect (even slightly) w/ multiple states
states_filt <- states[!grepl(",", states)]
# Filter out islands that are included in states.
states_filt <- states_filt[!grepl("GU|MP", states_filt)]

# pullin in da census: this is going to take a hot second to load
census <- tidycensus::get_acs(
  geography = "tract", 
  variables = census_var_vector_noinc, 
  state = states_filt, 
  year = as.numeric(crosswalk_year),
  geometry = F, # we don't need geoms anymore - yay!
)

# TODO - pivot to wide format 

# Summarize NAs
# Every location has NAs for hh_num_vehicle
# PR is missing data for all the age population variables
# TODO - this is awesome - we should really make this into a grid for all 
# variables! 
state_lookup <- states() %>%
  sf::st_drop_geometry() %>%
  select(STATEFP, STUSPS)
na_summary <- census %>%
  mutate(STATEFP = substr(GEOID, 1, 2)) %>%
  left_join(state_lookup, by = "STATEFP") %>%
  group_by(STUSPS, variable) %>%
  summarize(
    na_count = sum(is.na(estimate)),
    .groups = "drop"
  ) %>%
  filter(na_count > 0)

# =============================================================================
# Decennial Census Data - Island Territories
# Pulls from 2020 Decennial Census instead of ACS
# Notes:
# - decennial pull returns a "value" while ACS returns "estimate" and "moe"
# because these are actual census counts vs the ACS survey estimates
# =============================================================================
territory_var_sheet <- read_sheet(URL, sheet = "territory_var_methods") %>%
  janitor::clean_names()
territory_income_vars <- c("HCT11_001N", "PCT59_001N", "PCT58_001N", "PCT56_001N")

create_territory_vars_vector <- function(sheet, var_column, income_vars) {
  sheet %>%
    # Drop rows without a code
    filter(!is.na(.data[[var_column]]), .data[[var_column]] != "") %>%
    # Filter out income variables (to be interpolated later)
    filter(!(.data[[var_column]] %in% income_vars)) %>%
    # Create a named vector formatted: c("clean_name" = "CENSUS_CODE")
    { setNames(.[[var_column]], .$name) }
}

territory_config <- list(
  as = list(
    usps               = "as",
    fips               = "60",
    county_fips        = c("010", "020", "030", "040", "050"),
    vars               = create_territory_vars_vector(territory_var_sheet, "as_var", territory_income_vars)
  ),
  mp = list(
    usps               = "mp",
    fips               = "69",
    county_fips        = c("085", "100", "110", "120"),
    vars               = create_territory_vars_vector(territory_var_sheet, "mp_var", territory_income_vars)
  ),
  gu = list(
    usps               = "gu",
    fips               = "66",
    county_fips        = c("010"),
    vars               = create_territory_vars_vector(territory_var_sheet, "gu_var", territory_income_vars)
  ),
  vi = list(
    usps               = "vi",
    fips               = "78",
    county_fips        = c("010", "020", "030"),
    vars               = create_territory_vars_vector(territory_var_sheet, "vi_var", territory_income_vars)
  )
)


get_census_decennial_data <- function(territory_config, decennial_year = 2020) {
  census_api_key <- Sys.getenv("CENSUS_API_KEY")
  
  message("\n==================================================")
  message(sprintf("Starting decennial data pull for territory: %s", 
                  toupper(territory_config$usps)))
  message("==================================================")
  var_codes <- paste(unname(territory_config$vars), collapse = ",")
  
  if (var_codes == "" || is.na(var_codes)) {
    stop("Error: No valid census var codes!")
  }
  message(sprintf("Number of variables to pull: %d", 
                  length(territory_config$vars)))
  
  # batch through vars - census API can only handle 49 at a time 
  var_chunks <- split(unname(territory_config$vars), 
                      ceiling(seq_along(territory_config$vars) / 49))
  
  # creating base lists for vars and counties 
  var_chunk_frames <- list()
  all_county_frames <- list()
  for(i in 1:length(var_chunks)){
    var_codes <- paste(var_chunks[[i]], collapse = ",")
    
    for (county in territory_config$county_fips) {
      message(sprintf("Fetching tract data for county FIPS: %s...", county))
      # Construct the web address for the API call
      # 1. base url
      # 2. add the query parameters
      # 3. append the API key for authentication
      base_url <- sprintf(
        "https://api.census.gov/data/%d/dec/dhc%s",
        as.integer(decennial_year),
        territory_config$usps
      )
      req <- request(base_url) %>%
        req_url_query(
          `get` = paste0("NAME,", var_codes),
          `for` = "tract:*",
          `in`  = sprintf("state:%s county:%s", territory_config$fips, county)
        )
      if (census_api_key != "") {
        req <- req %>% req_url_query(`key` = census_api_key)
      }
      
      # Fetch and parse the response
      parsed_json <- req %>%
        req_perform() %>%
        resp_body_string() %>%
        jsonlite::fromJSON()
      
      col_names <- parsed_json[1, ]
      data_matrix <- parsed_json[-1, , drop = FALSE]
      
      message(sprintf("Finished county pull with %d tracts.", nrow(data_matrix)))
      
      territory_census_data <- as_tibble(data_matrix, .name_repair = "minimal")
      colnames(territory_census_data) <- col_names
      
      territory_census_data <- territory_census_data %>%
        # Rename var code column headers to clean names
        rename(any_of(unlist(territory_config$vars))) %>%
        # Create GEOID which combines fips codes of state + county + tract
        mutate(GEOID = stringr::str_c(state, county, tract)) %>%
        # Drop state, county, tract columns
        select(!any_of(c("state", "county", "tract"))) %>%
        # Fix data types from JSON loaded strings
        mutate(across(everything(), ~type.convert(., as.is = TRUE)))
      
      all_county_frames[[county]] <- territory_census_data
    }
    # Loop over every county fips for the territory
    message(sprintf("All counties complete."))
    all_counties <- bind_rows(all_county_frames)
    
    var_chunk_frames[[i]] <- all_counties
  }
  # bring back all the vars 
  message(sprintf("All vars complete."))
  territory_data_final <- reduce(var_chunk_frames, 
                                 left_join, by = c("NAME", "GEOID")) %>%
    relocate(GEOID)
  return(territory_data_final)
}

# as_data <- get_census_decennial_data(territory_config[[1]])
# View(as_data)
# mp_data <- get_census_decennial_data(territory_config[[2]])
# View(mp_data)
# gu_data <- get_census_decennial_data(territory_config[[3]])
# View(gu_data)
# vi_data <- get_census_decennial_data(territory_config[[4]])
# View(vi_data)

# Territory data is already in wide format so it doesn't need to be converted
territory_census_data <- territory_config %>%
  purrr::map(\(cfg) get_census_decennial_data(cfg)) %>%
  purrr::list_rbind()

# Summarize NAs
territory_na_summary <- territory_census_data %>%
  mutate(
    territory = dplyr::recode(
      substr(GEOID, 1, 2),
      "60" = "AS",
      "66" = "GU",
      "69" = "MP",
      "78" = "VI"
    )
  ) %>%
  pivot_longer(
    cols = !any_of(c("NAME", "GEOID", "territory")),
    names_to = "variable",
    values_to = "value"
  ) %>%
  group_by(territory, variable) %>%
  summarise(
    na_count = sum(is.na(value)),
    .groups = "drop"
  ) %>%
  filter(na_count > 0)


# TODO - emmali stopped here

# TODO - this is where the ord crosswalk code would go, with fixes for CT if needed 

# TODO - this is where the weighted mhi interpolation would go for systems w/ block data 

# TODO - this is where the territory code would go for general areal interpolation 

# TODO - this is where the mhi interpolation code would go, not weighted b/c territories often don't have 2020 block pops



# =============================================================================
# Income Interpolation - States
# TODO: merge territory census data and interpolate together
# =============================================================================
# pivoting to wide format and cleaning names: 
census_wide <- census %>%
  # removing moe, otherwise colnames will have prefix 
  select(-c(moe)) %>% 
  pivot_wider(., names_from = variable, values_from = estimate) 

# merging with the EPA ORD crosswalk: 
merged_xwalk <- merge(census_wide, ord_tract_xwalk, 
                      by.x = "GEOID", by.y = "geoid20")
# checking to confirm the merge was complete 
# test <- ord_tract_xwalk %>%
#   filter(!(pwsid %in% merged_xwalk$pwsid))

# multiplying by weights from EPA's ORD xwalk 
weighted_vars <- merged_xwalk %>%
  mutate(across(names(census_var_vector_noinc), ~.x*bldg_weight)) 

# summing totals by pwsid: 
summed_totals <- weighted_vars %>%
  group_by(pwsid) %>%
  summarize(across(total_pop:pop_pov_level_above_200, ~ sum(.x, na.rm = T)))

# checking output 
# weighted_vars %>%
#   filter(pwsid == "055294602")

# drafting up percentages to calculate the total universe: 
percentage_functions <- census_var_sheet %>%
  filter(!is.na(universe)) %>%
  mutate(equation = paste0("100*(", name, "/", universe, ")")) %>%
  filter(var %in% census_var_vector_noinc)

# creating list of functions: 
easy_percentages_functions <- setNames(
  lapply(percentage_functions$equation, 
         function(eq) rlang::parse_expr(eq)), 
  paste0(percentage_functions$name, "_per"))

# applying the function for percentages 
pwsid_census_pcts <- summed_totals %>%
  mutate(!!!easy_percentages_functions) 


# dope. now we need to handle the more complicated functions
more_complicated_functions <- census_var_sheet %>%
  filter(!is.na(calc_after_interp) & is.na(interp_method))

# create list of functions: 
more_complicated_functions <- setNames(
  lapply(more_complicated_functions$calc_after_interp, 
         function(eq) rlang::parse_expr(eq)), 
  more_complicated_functions$name)

# applying with mutate: 
more_complicated_pcts <- summed_totals %>%
  mutate(!!!more_complicated_functions) %>%
  # these are raw counts we had previously & I'll merge them in the next step
  select(-c(total_pop:pop_pov_level_above_200)) %>%
  # there are some slight rounding errors after weighting that causes ~3 
  # sabs to have values that are like -0.000003. Capping thees to zero
  mutate(age_over_61_per = case_when(age_over_61_per < 0 ~ 0, 
                                     TRUE ~ age_over_61_per))

# merging datasets together: 
pwsid_census_xwalk <- merge(pwsid_census_pcts, more_complicated_pcts,
                            by = "pwsid", all = T)

# testing: 
# test <- pwsid_census_xwalk %>%
#   select(contains("age"), total_pop)

# interpolate amhi & hh lowest quintile of income
income_vars <- census_var_vector_nona[(census_var_vector_nona %in% c("B19013_001", "B19080_001"))]
inc_vars_census <- tidycensus::get_acs(
  geography = "tract", 
  variables = income_vars, 
  state = states_filt, 
  year = as.numeric(crosswalk_year),
  geometry = T # note that we need geographies because we're going to 
  # interpolate these 
)  

# pivoting to wide format and cleaning names: 
inc_vars_wide <- inc_vars_census %>%
  # removing moe, otherwise colnames will have prefix 
  select(-c(moe)) %>% 
  pivot_wider(., names_from = variable, values_from = estimate) 

# grabbing total universes: 
mhi_uni <- sum(inc_vars_wide$mhi_census, na.rm = T)
lowest_quintile_uni <- sum(inc_vars_wide$hh_inc_lowest_quintile_census, na.rm = T)

# grabbing % of total universe to interpolate using spatially intensive method: 
inc_vars_uni <- inc_vars_wide %>%
  # calculating % of total universe
  mutate(mhi_pct_uni = mhi_census / mhi_uni, 
         income_lowest_quintile_uni = hh_inc_lowest_quintile_census / lowest_quintile_uni) %>%
  relocate(geometry, .after = last_col()) %>%
  # projecting to alberts equal area for all interpolations 
  st_transform(., crs = 5070) 


# turning off spherical geometry because we are working off a 
# projected CRS
sf_use_s2(F)

# okay, now I gotta loop through states because all of this needs to be weighted 
# by census blocks: 
interp_inc <- data.frame()
for(i in 1:length(states_filt)){
  # filtering key datasets: 
  state_i <- states_filt[i]
  # grabbing full name for filtering census data: 
  state_name <- tigris::states() %>% 
    filter(STUSPS == state_i) %>% 
    select(NAME) %>% as.data.frame() %>% select(-geometry)
  # print to watch the loop: 
  print(paste0("Working on: ", state_i, "; ", state_name))
  
  # filtering: 
  sabs_i <- epa_sabs %>% 
    filter(grepl(state_i, epic_states_intersect)) %>%
    # projecting to alberts equal area for all interpolations 
    st_transform(., crs = 5070) %>%
    select(pwsid)
  inc_vars_uni_i <- inc_vars_uni %>% filter(grepl(state_name, NAME)) %>%
    select(GEOID, mhi_pct_uni, income_lowest_quintile_uni)
  
  # grabbing weights: 
  print("Grabbing blocks")
  state_blocks <- tigris::blocks(
    state = state_i, 
    year = 2020) %>%
    # projecting to alberts equal area for all interpolations 
    st_transform(., crs = 5070) %>%
    # standardize column names: 
    rename(pop_weight = "POP20", 
           housing_weight = "HOUSING20")
  
  # interpolatin' 
  print("Interpolating")
  pw_interp_inc_i <- interpolate_pw(
    from = inc_vars_uni_i,
    to = sabs_i,
    to_id = "pwsid",
    extensive = FALSE, 
    weights = state_blocks,
    weight_column = "housing_weight", 
    crs = 5070)
  
  # keeping track of the state to handle sabs that overlap with multiple states: 
  pw_interp_inc_i_tidy <- pw_interp_inc_i %>%
    mutate(state_interp = state_i)
  
  # adding back to OG dataframe: 
  interp_inc <<- rbind(interp_inc, pw_interp_inc_i_tidy)
}
sf_use_s2(T)

# relate back to universe
interp_inc_df <- interp_inc %>%
  as.data.frame() %>%
  select(-geometry) %>%
  mutate(mhi = mhi_pct_uni*mhi_uni, 
         hh_inc_lowest_quintile = income_lowest_quintile_uni*lowest_quintile_uni)
# write.csv(interp_inc_df, "./data/inc_crosswalk_vars_2026.csv")

# handle systems that overlap with multiple states using a weighted mean: 
interp_inc_df_tidy <- interp_inc_df %>%
  group_by(pwsid) %>%
  # adding na.rm = T to handle systems that may slightly overlap with multiple states, 
  # and may therefore be "NA" on a specific state loop with very minimal overlap
  summarize(mhi = weighted.mean(mhi, na.rm = T), 
            hh_inc_lowest_quintile = weighted.mean(hh_inc_lowest_quintile, na.rm = T))

# merge back with xwalk 
pwsid_census_xwalk_f <- merge(pwsid_census_xwalk, interp_inc_df_tidy, 
                              by = "pwsid", all = T)
# write.csv(pwsid_census_xwalk_f, "./data/pwsid_xwalk.csv")


# investigating output: 
# nc_sabs <- pwsid_census_xwalk_f %>%
#   filter(grepl("NC", pwsid)) %>%
#   left_join(epa_sabs, by = "pwsid") %>%
#   st_as_sf()
# mapview(nc_sabs, zcol = "mhi")
# 
# what about those missing from this summary file? There are 87 and they're all
# from PR, GU, or MP
# missing_from_xwalk <- epa_sabs %>%
#   filter(!(pwsid %in% pwsid_census_xwalk_f$pwsid))
# mapview(missing_from_xwalk)

# calculating pop density
pwsid_pops <- pwsid_census_xwalk_f %>% 
  select(pwsid, total_pop)

# pull in area in mi2 we calculated from epa_sabs, using alberts equal area 
# projection 
epa_sabs_pop_den <- epa_sabs %>%
  select(pwsid, epic_area_mi2) %>%
  left_join(., pwsid_pops) %>%
  mutate(epic_pop_density = total_pop / epic_area_mi2) %>%
  relocate(epic_pop_density, .after = epic_area_mi2) %>%
  # transforming to simple df: 
  as.data.frame() %>%
  select(-geometry)

# merge with xwalk: 
epa_sabs_xwalk <- merge(pwsid_census_xwalk_f, epa_sabs_pop_den, all = "T")


# code to summarize main water rate bin:
water_rates <- epa_sabs_xwalk %>%
  select(pwsid, water_rate_less_125_per:water_rate_over_1000_per) %>%
  distinct()

most_common_rates <- water_rates %>%
  mutate(pwsid = as.character(pwsid)) %>%
  rowwise() %>%
  # this is plus one because we're offset due to pwsid being a character colummn 
  mutate(most_common_rate = as.character(list(colnames(.)[which.max(c_across(water_rate_less_125_per:water_rate_over_1000_per))+1]))) %>%
  # cleaning these up into tidy names
  mutate(most_common_rate_tidy = case_when(
    most_common_rate == "water_rate_over_1000_per" ~ "Most people pay > $1000 for water & sewer annually",
    most_common_rate == "water_rate_less_125_per" ~ "Most people pay < $125 for water & sewer annually", 
    most_common_rate == "water_rate_between_250_499_per" ~ "Most people pay between $250-499 for water & sewer annually",
    most_common_rate == "water_rate_between_500_749_per" ~ "Most people pay between $500-749 for water & sewer annually",
    most_common_rate == "water_rate_between_125_249_per" ~ "Most people pay between $125-249 for water & sewer annually",
    most_common_rate == "water_rate_between_750_999_per" ~ "Most people pay between $750-999 for water & sewer annually",
    TRUE ~ "No Information on annual water & sewer rates")) %>%
  # just select these two 
  select(pwsid, most_common_rate_tidy)

# merging back with OG list 
epa_sabs_xwalk_final <- merge(epa_sabs_xwalk, most_common_rates, 
                              by = "pwsid", all = T) %>% 
  # this column is an artifact of the crosswalking process
  select(-hh_num_vehicles_per) %>%
  # relocate for clarity 
  relocate(age_over_61_per, .after = age60_61_per) 


# # grabbing clean link: 
# xwalk_raw_link <- "NA - crosswalked" # I'd say this is already a cleaned dataset
# xwalk_clean_link <- "s3://tech-team-data/national-dw-tool/clean/national/epa_sabs_crosswalk.csv"
# 
# # writing this to s3: 
# tmp <- tempfile()
# write.csv(epa_sabs_xwalk_final, paste0(tmp, ".csv"), row.names = F)
# on.exit(unlink(tmp))
# put_object(
#   file = paste0(tmp, ".csv"),
#   object = xwalk_clean_link,
#   bucket = "tech-team-data",
#   multipart = T
# )
# 
# # update task manager: 
# update_data_summary("epa_sabs_xwalk", 
                    xwalk_raw_link, xwalk_clean_link)