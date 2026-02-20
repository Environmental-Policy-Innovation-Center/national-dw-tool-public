################################################################################
# xwalk_census_vars_sabs(sab_f, acs_year, census_var_sheet, save_data = F)
# EmmaLi Tsai 
# March 26th 2025
################################################################################
# This function takes an sf object of EPA's water utility service area 
# boundaries and estimates defined census variables (census_var_sheet and ACS
# year) using a tiered approach. Tier 1 estimates census variables using 
# population weighted interpolation, tier 2 uses the block-parcel crosswalk
# from Blue Conduit to weight variables, and tier 3 are variables that could 
# not be estimated using current approaches (i.e., too small or invalid 
# geometry). Due to the fact the tier 2 method was found to consistently 
# overestimate census populations at the tract level, SABs are capped to 
# the population reported by SDWIS and raw counts are scaled appropriately 
# (this is noted as tier_2_xwalk or tier_2_sdwispop). 
# 
# This function can handle a variety of census variables, years, and SABs 
# (including those that may intersect multiple states, like the Navajo Nation). 
# SABs are only considered as intersecting multiple states if >= 20%
# of total area is present in a different state. This was to avoid 
# slight SAB and state overlaps due to boundary issues. 
# 
# If you're using this function to crosswalk data from 2010 geometries, the 
# function will automatically download the appropriate census block housing and
# population data to use as weights. However, since the tier_2 crosswalk is 
# built off of 2020 geometries, tier 2 data, if the census blocks had shifted
# during that time period, are likely to be inaccurate. It is recommended 
# to only use tier 1 data if working with data with 2010 geometries. 
# 
# Inputs: 
# - sab_f = sf of EPA's water utility service area boundaries
# - acs_year = the ACS year that matches to the census variables 
# - census_var_sheet = data frame containing census variables, names, methods 
#     for interpolation, and universes for percentage calculations. 
# - save_data = a T/F on whether you want to save every iteration of your 
#     loop to a ./data/xwalk_census_vars_sabs/ folder. Helpful if your 
#     internet connection is spotty or if you're running this function on 
#     2010 variables (it currently takes longer to download census 
#     block geometries from 2010 b/c we have to loop through every county)
# 
# Outputs: 
# - sf object of all interpolated vars. Tier crosswalk and state note the 
#     crosswalk method(s) and state(s) used to estimate census vars. If the 
#     "universe" column is filled out in census_var_sheet, a new column 
#     was automatically added (suffix {varname}_per) for percentage 
#     calculations. 
# 
# Example of use: ##############################################################
# library(googlesheets4)
# library(aws.s3)
# 
# # function dependencies:
# library(tidyverse)
# library(sf)
# library(tidycensus)
# 
# # enable caching for the census
# options(tigris_use_cache = TRUE)
# 
# # reading in SABs
# epa_sabs <- aws.s3::s3read_using(st_read,
#                                  object = "/national-dw-tool/raw/national/water-system/epa_sabs.geojson",
#                                  bucket = "tech-team-data")
# 
# # reading in your census vars and calculations from google sheet
# gs4_deauth()
# URL <- "https://docs.google.com/spreadsheets/d/1UvFjxOm1Q06ZEDXr98Pt0uvLFabsGA8IT8eEJrQN9pg/edit?gid=1609612755#gid=1609612755"
# census_var_sheet <- read_sheet(URL, sheet = "census_var_table_test") %>%
#   janitor::clean_names()
# # 
# sab_test <- epa_sabs %>%
#   filter(state == "AL")
# # 
# test_mult_states <- crosswalk_census_vars(sab_test, "2021", census_var_sheet, save_data = F)
# 
# # run the xwalk nationally:
# full_xwalk <- crosswalk_census_vars(epa_sabs, "2021", census_var_sheet, save_data = F)
# # st_write(full_xwalk, "./epa-sabs-comparison/results/updated_refactored_xwalk.geojson")
################################################################################

xwalk_census_vars_sabs <- function(sab_f, acs_year, census_var_sheet, save_data = F) {
  
  # pull in helper functions: 
  # NOTE - April 15th - added helper functions, 2010 data functionality, and 
  # option to save data 
  source("./functions/helper_functions.R")

  ##############################################################################
  # Function checks
  ##############################################################################
  # make sure census vars are all uppercase and remove any stray spaces
  # from strings: 
  census_var_sheet <- census_var_sheet %>%
      mutate(var = toupper(var), 
             var = str_squish(var)) 
  
  # make sure total_pop is included: 
  if(!("total_pop" %in% census_var_sheet$name)){
   stop("The total_pop census variable is missing") 
  }
  
  # make sure all numerators have a universe value: 
  numerators <- census_var_sheet %>%
    filter(category == "numerator")
  if(any(is.na(numerators$universe))){
    stop("All numerator variables need a universe") 
  }
  
  # make sure all non-income variables have an interpolation value: 
  census_interp_vars <- census_var_sheet %>%
    filter(!is.na(var)) %>%
    filter(category == "numerator")
  if(any(is.na(census_interp_vars$interp_method))){
    stop("Provide an interpolation method for non-income variables") 
  }
  

  ##############################################################################
  # epa sabs tidying: 
  ##############################################################################
  # clean SABs using an internal function - handles duplicated sabs and 
  # invalid geometries, and runs an intersection to figure out what 
  # state data to pull for each PWSID
  sab <- .tidy_epa_sabs(sab_f)
  # NOTE - this also returns a "states_pwsids" data frame to the global 
  # environment, which is the % of the SAB that intersects w/ various SABs
  
  # states to loop through: 
  unique_states <- str_sort(unique(unlist(str_split(sab$crosswalk_states, ","))))
  
  # there are 13 SABs in CNMI - tidycensus is throwing an API error here 
  # so removing for now: 
  if(any(grepl("MP|Northern Mariana Islands", unique_states))){
    message("Removing Northern Mariana Islands - not currently supported")
    unique_states <- unique_states[!grepl("MP|Northern Mariana Islands", unique_states)]
    sab <- sab %>%
      filter(!grepl("MP", crosswalk_states))
  }  
  
  # tract crosswalk for tier 2 crosswalk:  
  message("Grabbing Tract Parcel Crosswalk")
  tract_crosswalk <- aws.s3::s3read_using(read.csv, 
                                          object = "s3://tech-team-data/pws_crosswalk/EPA_SABS_parcel_weighted_pwsid_census_tracts.csv") %>%
    select(-X) %>%
    # grab the pwsids from our SABs
    filter(pwsid %in% unique(sab$pwsid)) %>%
    # if the geoid is missing prefix 0, add it back in
    mutate(tract_geoid = as.character(tract_geoid),
           tract_geoid = case_when(
             str_length(tract_geoid) == 10 ~ paste0(0, tract_geoid),
             TRUE ~ tract_geoid
           )) %>%
    # there are some tract geoids in the crosswalk are NAs - assuming these are errors?
    filter(!(is.na(tract_geoid)))
  
  ##############################################################################
  # Handling SABs that intersect multiple states
  ##############################################################################  
  # if there are multiple states, loop through them. if not, 
  # just run the function once: 
  if(length(unique_states) > 1){
    
    # looping through each state: 
    xwalk_loop <- sab[0,]
    for(i in 1:length(unique_states)){
      message("On loop: ", i, " out of ", length(unique_states))
      # grabbing states & pwsids for iteration of the loop & filtering 
      state_i = unique_states[i]
      state_i_pwsids <- states_pwsids %>%
        filter(stusps == state_i)
      
      # filtering for pwsids that intersect with state_i
      sab_i <- sab %>%
        filter(pwsid %in% state_i_pwsids$pwsid)

      # run xwalk on these pwsids 
      xwalk_i <- .single_state_sab_crosswalk(sab_f = sab_i, 
                                             state_f = state_i, 
                                             acs_year = acs_year, 
                                             census_var_sheet = census_var_sheet, 
                                             tract_crosswalk = tract_crosswalk)
      # binding 
      xwalk_loop <- bind_rows(xwalk_loop, xwalk_i)
      
      # save data for every iteration of the loop, if desired
      if(save_data == TRUE){
        message("Saving Data for ", state_i, " loop ", i)
        dir.create(file.path("./data/xwalk_census_vars_sabs/"))
        xwalk_loop_df <- xwalk_loop %>%
          as.data.frame() %>%
          select(-starts_with("geom"))
        write.csv(xwalk_loop_df, paste0("./data/xwalk_census_vars_sabs/xwalk_vars_", state_i, i, ".csv"))
      }
    }
    ############################################################################
    # Handling SABs that intersect multiple states
    ############################################################################
    message("Fixing SABs that overlap with multiple states")
    
    # grabbing list of crosswalked states by pwsid: 
    pwsids_xwalk_states <- xwalk_loop %>%
      as.data.frame() %>%
      group_by(pwsid) %>%
      summarize(crosswalk_state = paste(crosswalk_state, collapse = ", "), 
                tier_crosswalk = paste(tier_crosswalk, collapse = ", "))
    
    # locating those with multiple states: 
    mult_states <- pwsids_xwalk_states %>%
      filter(grepl(",", crosswalk_state))
    
    # grabbing og boundaries (probably not needed but just in case): 
    mult_states_sab_geoms <- sab %>%
      filter(pwsid %in% mult_states$pwsid) %>%
      select("pwsid")
    
    # filtering these from xwalk: 
    mult_states_xwalk <- xwalk_loop %>%
      filter(pwsid %in% mult_states$pwsid)

    # so all counts need to be summed, weighted mean for income vars 
    # and then all percentages need to be run again   
    census_vars_summed <- census_var_sheet %>%
      filter(grepl("extensive", interp_method))
     
    mult_states_summed <- mult_states_xwalk %>%
      group_by(pwsid) %>% 
      mutate(across(c(census_vars_summed$name), ~(sum(.)))) %>%
      ungroup() 
    
    # calculating easy percentages: 
    need_calcs_after_interp_easy <- census_var_sheet %>%
      filter(!is.na(universe)) %>%
      mutate(equation = paste0("100*(", name, "/", universe, ")"))
    
    # creating list of expressions: 
    mutate_exprs_after_interp_easy <- setNames(
      lapply(need_calcs_after_interp_easy$equation, 
             function(eq) rlang::parse_expr(eq)), 
      paste0(need_calcs_after_interp_easy$name, "_per")
    )
    
    # applying with mutate: 
    mult_states_pct_easy <- mult_states_summed %>%
      mutate(!!!mutate_exprs_after_interp_easy)
    

    # doing the more complicated percentages: 
    # running calcs again on the new summed vars: 
    census_vars_calc <- census_var_sheet %>%
      # removing income vars, which need to be handled in a particular way 
      filter(!is.na(calc_after_interp) & is.na(interp_method))
    
    # creating list of expressions: 
    mutate_exprs_calc <- setNames(
      lapply(census_vars_calc$calc_after_interp, 
             function(eq) rlang::parse_expr(eq)), 
      census_vars_calc$name
    )
    
    # applying with mutate: 
    mult_states_pct_f <- mult_states_pct_easy %>%
      mutate(!!!mutate_exprs_calc) 
    
    
    # mean for income vars: 
    inc_vars <- census_var_sheet %>%
      filter(!is.na(interp_method) & !is.na(calc_after_interp))
    
    # grabbing means for income vars and also combining with OG sab geometries: 
    mult_stats_final <- mult_states_pct_f %>%
      group_by(pwsid) %>% 
      mutate(across(c(inc_vars$name), ~(mean(.)))) %>%
      ungroup() %>%
      as.data.frame() %>%
      select(-starts_with("geom")) %>%
      # removing these to overwrite them with better cols: 
      select(-c(crosswalk_state, tier_crosswalk)) %>%
      left_join(pwsids_xwalk_states) %>%
      left_join(mult_states_sab_geoms)
    
    # removing duplicates: (can confirm they're identical calculations)
    mult_states_no_dups <- mult_stats_final[duplicated(mult_stats_final[,"pwsid"]),]
    
    # rbinding with OG xwalk: 
    xwalk_loop_one_state <- xwalk_loop %>%
      filter(!(pwsid %in% mult_states_no_dups$pwsid))
    xwalk <- bind_rows(xwalk_loop_one_state, mult_states_no_dups)
    
   } else {
    
    # if there's just one state, run it once! 
    xwalk <- .single_state_sab_crosswalk(sab, unique_states, 
                                         acs_year, census_var_sheet,
                                         tract_crosswalk = tract_crosswalk)
  }
  
  # throw warning on tier 2 data if 2010 geographies are detected: 
  if((round(as.numeric(acs_year) / 10)*10) == 2010) {
    message("2010 census geometries detected - only tier 1 data should be used")
  } 
  
  # returning 
  return(xwalk)
}


## internal function to run crosswalk on one state 
.single_state_sab_crosswalk <- function(sab_f, state_f, 
                                        acs_year, census_var_sheet, 
                                        tract_crosswalk) {
  ##############################################################################
  # Downloading and organizing data for crosswalk tiers: 
  ##############################################################################
  message(paste0("Working on: ", state_f))
  
  # create copy of sab_f
  sab <- sab_f 
  # vector of pwsids for querying the tract parcel crosswalk for tier 2
  pwsids <- unique(sab$pwsid)
  
  # dataframe of pwsids & SDWIS reported populations for tier 2 SDWIS pop calcs: 
  sdwis_pop <- sab %>%
    as.data.frame() %>%
    select(-starts_with("geom")) %>%
    select(pwsid, population_served_count)
  
  ##############################################################################
  # Grabbing vars from the census
  ##############################################################################
  # generate named character vector of census variables: 
  census_var_need_interp <- census_var_sheet %>%
    # rows w/o vars are not from the census
    filter(!is.na(var)) 
  census_var_vector <- setNames(census_var_need_interp$var, 
                                census_var_need_interp$name)
  
  message("Grabbing Census Data")
  census <- tidycensus::get_acs(
    geography = "tract", 
    variables = census_var_vector, 
    state = state_f, 
    year = as.numeric(acs_year), # NOTE - changing to 2021 on Nov 15th 2024
    geometry = TRUE
  )
  
  # pivoting to wide format: 
  census_wide <- census %>%
    # removing moe, otherwise colnames will have prefix 
    select(-c(moe)) %>% 
    pivot_wider(., names_from = variable, values_from = estimate) %>%
    # making sure the crs is the same as our sabs for interpolation 
    st_transform(., crs = "WGS84")
  
  # loading block weights for pw_interpolation within the tier 1 crosswalk: 
  message("Grabbing Census Block Weights")
  if((round(as.numeric(acs_year) / 10)*10) == 2010) {
    blocks_flag = FALSE
  } else {
    blocks_flag = TRUE
  }
  
  state_blocks <- .grab_census_blocks(sf_data_i = census_wide, 
                                      fips_col = "GEOID", 
                                      state_i = state_f, 
                                      blocks_2020 = blocks_flag)

  ##############################################################################
  ## Handling census vars that need modifying before interpolation: 
  ##############################################################################
  # This is really just income vars
  need_calcs_before_interp <- census_var_sheet %>%
    filter(!is.na(calc_before_interp))

  # creating list of expressions: 
  mutate_exprs <- setNames(
    lapply(need_calcs_before_interp$calc_before_interp, 
           function(eq) rlang::parse_expr(eq)), 
    need_calcs_before_interp$name
  )
  
  # applying with mutate: 
  census_wide <- census_wide %>%
    mutate(!!!mutate_exprs)
  
  ##############################################################################
  ## Tier 1: Population-weighted interpolation: 
  ##############################################################################
  message("Starting Tier 1 Crosswalk: Population Weighted Interpolation")
  
   # isolating population vars
  extensive_pop_vars <- census_var_sheet %>%
    filter(interp_method == "extensive_pop")
  state_wide_pop <- census_wide %>%
    select(GEOID, extensive_pop_vars$name)
  # interpolatin' 
  message("Tier 1: population weighted interpolation on pop vars - extensive")
  pw_ext_pop <- interpolate_pw(
    from = state_wide_pop,
    to = sab,
    to_id = "pwsid",
    extensive = TRUE, 
    weights = state_blocks,
    weight_column = "pop_weight",
    crs = "WGS84")
  
  
  # isolating household vars
  extensive_hh_vars <- census_var_sheet %>%
    filter(interp_method == "extensive_hh")
  state_wide_hh <- census_wide %>%
    select(GEOID, extensive_hh_vars$name)
  # since we're using household data, should use housing20 as weights for
  # pw interpolation: 
  message("Tier 1: population weighted interpolation on household vars - extensive")
  pw_ext_hh <- interpolate_pw(
    from = state_wide_hh,
    to = sab,
    to_id = "pwsid",
    extensive = TRUE, 
    weights = state_blocks,
    weight_column = "housing_weight",
    crs = "WGS84")
  
  
  # isolating income percentage vars
  ## Median household income & lowest income quintile
  intensive_hh_vars <- census_var_sheet %>%
    filter(interp_method == "intensive_hh")
  state_wide_inc <- census_wide %>% 
    select(GEOID, intensive_hh_vars$name)
  # pop weighted interpolation on median household income using spatially 
  # intensive because we essentially want a weighted mean:
  message("Tier 1: population weighted interpolation on income variables - intensive")
  pw_interp_inc <- interpolate_pw(
    from = state_wide_inc,
    to = sab,
    to_id = "pwsid",
    extensive = FALSE, 
    weights = state_blocks,
    weight_column = "housing_weight", # NOTE - changing this to housing weights Nov 2024
    crs = "WGS84")
  
  # relating percentages back to income vars
  need_calcs_after_interp_inc <- census_var_sheet %>%
    # removing income vars, which need to be handled in a particular way 
    filter(!is.na(calc_after_interp) & !is.na(interp_method))
  
  # creating list of expressions: 
  mutate_exprs_after_interp_inc <- setNames(
    lapply(need_calcs_after_interp_inc$calc_after_interp, 
           function(eq) rlang::parse_expr(eq)), 
    need_calcs_after_interp_inc$name
  )
  
  # applying with mutate: 
  pw_interp_inc <- pw_interp_inc %>%
    mutate(!!!mutate_exprs_after_interp_inc)
  
  
  ## Bringing everything back together: 
  # NOTE - in maryland, there are multiple rows of "MD0070015", and in 
  # ohio, multiple rows for OH2956203
  pwsid_inc_df <- pw_interp_inc %>%
    as.data.frame() %>%
    select(-starts_with("geom"))
  
  pwsid_hh_df <- pw_ext_hh %>%
    as.data.frame() %>%
    select(-starts_with("geom"))
  
  # all census data: 
  all_census <- pw_ext_pop %>%
    left_join(pwsid_inc_df) %>%
    left_join(pwsid_hh_df)
  
  # filtering NAs for tier 1: in TX, pw interp covered 86.87% of systems
  pw <- all_census %>%
    filter(!(is.na(total_pop) | total_pop == 0)) %>%
    mutate(tier_crosswalk = "tier_1")
  
  ##############################################################################
  ## Tier 2: crosswalking using the blue conduit tract parcel crosswalk 
  ##############################################################################
  message("Starting Tier 2 Crosswalk: Tract Parcel Crosswalks")
  
  # locating the pwsids that fell out of pw interpolation: 
  xwalk <- all_census %>%
    filter(is.na(total_pop) | total_pop == 0) 
  
  # finding pwsids that exist in the tract parcel crosswalk. The crosswalk 
  # is missing tribal boundaries due to a parcel issue
  pwsid_cross <- tract_crosswalk %>%
    filter(pwsid %in% xwalk$pwsid) 
  
  # merging census stats with crosswalk - pwsi.ds that exist outside of the 
  # state but have state == "state abbr" will be dropped here 
  pwsid_xwalk <- merge(census_wide, pwsid_cross, 
                       by.x = "GEOID", by.y = "tract_geoid", all.y = T)
  
  # multiplying all extensive vars by tract weights: 
  pwsid_weight_xwalk <- pwsid_xwalk %>%
    mutate(across(c(extensive_pop_vars$name, extensive_hh_vars$name), 
                  ~.*tract_parcel_weight))
  
  
  # sum counts by pwsid 
  pwsid_weighted_xwalk <- pwsid_weight_xwalk %>%
    group_by(pwsid) %>% 
    summarize(across(c(extensive_pop_vars$name, extensive_hh_vars$name),
                     ~round(sum(.), 2)))
  
  
  # handle income vars that use a weighted mean 
  # creating list of expressions: 
  mutate_exprs_after_interp_wtmean <- setNames(
    lapply(need_calcs_after_interp_inc$interp_method, 
           function(eq) rlang::parse_expr(eq)), 
    need_calcs_after_interp_inc$name
  )
  
  # applying with mutate: 
  pwsid_weighted_inc <- pwsid_weight_xwalk %>%
    group_by(pwsid) %>%
    summarize(!!!mutate_exprs_after_interp_wtmean) %>%
    # prepping for merge:
    as.data.frame() %>%
    select(pwsid, !!need_calcs_after_interp_inc$name)
  

  # merging back with original: 
  pwsid_xwalk <- merge(pwsid_weighted_xwalk, pwsid_weighted_inc, 
                       by = "pwsid", all.x = T) %>%
    mutate(tier_crosswalk = "tier_2_xwalk")
  
  
  ##############################################################################
  ## Handling percentages - needed to scale certain estimates to SDWIS 
  ## population 
  ##############################################################################
  # let's bind these methods together and calculate percentages: 
  # fixing here if the pwsid_xwalk is empty! The case for DC: 
  if(nrow(pwsid_xwalk) == 0){
    pwsid_xwalk_tier1_2xwalk <- pw 
      # rename(geometry = geom)
  } else {
    pwsid_xwalk_tier1_2xwalk <- bind_rows(pw, pwsid_xwalk)
  }
  
  # calculating easy percentages, which are just the percentage of the universe: 
  need_calcs_after_interp_easy <- census_var_sheet %>%
    filter(!is.na(universe)) %>%
    mutate(equation = paste0("100*(", name, "/", universe, ")"))
  
  # creating list of expressions: 
  mutate_exprs_after_interp_easy <- setNames(
    lapply(need_calcs_after_interp_easy$equation, 
           function(eq) rlang::parse_expr(eq)), 
    paste0(need_calcs_after_interp_easy$name, "_per")
  )
  
  # applying with mutate: 
  pwsid_xwalk_tier1_2xwalk_pcts <- pwsid_xwalk_tier1_2xwalk %>%
    mutate(!!!mutate_exprs_after_interp_easy)
  
  
  # these are other percentage calculations that are more complicated -
  # NOTE: those with the same name ({name}_per) will be overwritten!!
  need_calcs_after_interp <- census_var_sheet %>%
    # removing income vars, which need to be handled in a particular way 
    filter(!is.na(calc_after_interp) & is.na(interp_method))
  
  # creating list of expressions: 
  mutate_exprs_after_interp <- setNames(
    lapply(need_calcs_after_interp$calc_after_interp, 
           function(eq) rlang::parse_expr(eq)), 
    need_calcs_after_interp$name
  )
  
  # applying with mutate: 
  pwsid_xwalk_tier1_2_pcts_final <- pwsid_xwalk_tier1_2xwalk_pcts %>%
    mutate(!!!mutate_exprs_after_interp) %>%
    relocate(c(tier_crosswalk, geometry), .after = last_col())
  

  ##############################################################################
  ## Tier 2 part 2: Capping population at SDWIS for xwalked vars
  ##############################################################################
  message("Tier 2 Crosswalk: Capping Population at SDWIS")
  # want to max total pop to those reported by SDWIS - find those whose 
  # max total pop was more than what was reported by SDWIS and scale 
  # appropriately 
  sdwis_xwalk <- pwsid_xwalk_tier1_2_pcts_final %>%
    left_join(sdwis_pop) %>% 
    filter(tier_crosswalk != "tier_1") %>%
    relocate(population_served_count, .before = total_pop) 
  
  # capping max pop at SDWIS values - this captures 362 out of 564 pwsids in TX
  sdwis_pop_need_scale <- sdwis_xwalk %>%
    filter(total_pop > population_served_count) 
  
  # grabbing population denominators 
  denoms_pop <- census_var_sheet %>%
    filter(category == "denominator" & !is.na(interp_method))
  
  # calculating current percentages of total population to scale counts to 
  # sdwis: 
  scale_refs <- sdwis_pop_need_scale %>%
    as.data.frame() %>%
    select(pwsid, population_served_count, denoms_pop$name) %>%
    # note that households are scaled by households/total population, to 
    # keep the relative proportions the same
    mutate(across(denoms_pop$name, ~(./total_pop), 
                  .names = "{col}_uni_per")) %>%
    mutate(across(ends_with("uni_per"), ~(.*population_served_count),
                  .names = "{col}_scaled")) %>%
    # selecting the new scaled columns and convert to original names: 
    select(pwsid, population_served_count, ends_with("_scaled")) %>%
    rename_with(~str_remove(., "_uni_per_scaled"), everything())
  
  # replace these scaled universe vars with those in the df: 
  sdwis_pop_need_scale[names(scale_refs)] <- scale_refs
  
  # ok, now generate equations to recalc raw counts 
  sdwis_scaled_eqns <- need_calcs_after_interp_easy %>%
    mutate(equation_scaled_sdiws = 
             paste0("(", paste0(name, "_per"), "/100)*", universe))
  
  # now run these equations to replace the census var raw counts with those 
  # scaled to SDWIS
  mutate_exprs_scaled_sdwis <- setNames(
    lapply(sdwis_scaled_eqns$equation_scaled_sdiws, 
           function(eq) rlang::parse_expr(eq)), 
    sdwis_scaled_eqns$name
  )
  
  # applying with mutate: 
  sdwis_pop_scaled_easy <- sdwis_pop_need_scale %>%
    mutate(!!!mutate_exprs_scaled_sdwis) 
  
  # these are other percentage calculations that are more complicated -
  # NOTE: those with the same name ({name}_per) will be overwritten!!
  # these need to be recalculated b/c they're not simple percentages -  
  # need to be calculated with the new scaled raw counts 
  sdwis_pop_scaled_final <- sdwis_pop_scaled_easy %>%
    mutate(!!!mutate_exprs_after_interp) %>%
    relocate(c(tier_crosswalk, geometry), .after = last_col()) %>%
    mutate(tier_crosswalk = "tier_2_sdwispop")
  
  
  ##############################################################################
  # Combining tier 1 and 2 methods & tier 3 designation: 
  ##############################################################################
  message("Recombining Data")
  
  # taking the pwsid tier 1_2 and binding the scaled rows: 
  pwsid_xwalk_no_scaled <- pwsid_xwalk_tier1_2_pcts_final %>%
    filter(!(pwsid %in% sdwis_pop_scaled_final$pwsid))
  all_xwalk <- bind_rows(pwsid_xwalk_no_scaled, 
                         # removing the sdwis pops for a clearer bind_rows
                         sdwis_pop_scaled_final %>% 
                           select(-population_served_count)) %>%
    as.data.frame() %>%
    select(-starts_with("geom")) %>%
    # noting xwalk state
    mutate(crosswalk_state = state_f) %>%
    # adding back OG geometries (probably not needed but makes me feel better)
    left_join(sab, by = "pwsid") %>% 
    relocate(crosswalk_state:shape_length, .after = pwsid)
  
  # just need to combine with missing pwsids, with NAs for all columns 
  missing <- sab %>%
    filter(!(pwsid %in% all_xwalk$pwsid))
  
  if(nrow(missing) != 0){
    # adding tier 3 and binding it to the bottom of all xwalk 
    # NOTE - in the last xwalk, those with geometry issues were flagged as 
    # tier_3_geom_issue, but I don't think that's necessary/really added 
    # anything for us 
    missing_tier3 <- missing %>%
      mutate(tier_crosswalk = "tier_3", 
             crosswalk_state = state_f)
    all_xwalk <- bind_rows(all_xwalk, missing_tier3)
  }
  
  # wooo! 
  return(all_xwalk)
}






