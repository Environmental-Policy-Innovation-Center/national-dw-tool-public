################################################################################
# xwalk_census_geo_sabs(sabs, sf_data_census, interp_methods, blocks_2020 = TRUE, fips_col, save_data = F)
# EmmaLi Tsai 
# March 26th 2025
################################################################################
# This function takes an sf object of EPA's water utility service area 
# boundaries and an sf object of data containing census geometries (i.e., 
# EJScreen, CEJST, SVI, etc.) and applies a variety of user-supplied 
# interpolation methods to estimate variables to water system boundaries. The 
# function can run weighted interpolation (population or housing weighted), 
# on spatially intensive (densities, percentages, etc.) or spatially 
# extensive (population, number of cars) variables, and also areal interpolation 
# (both intensive and extensive) to handle variables such as total rainfall or  
# drought variables. Here is this information summarized as bullet points: 
# 
# # - pw interpolation (on variables that relate to populations or houses)
#     - on block households as weights
#         - extensive (number of households)
#         - intensive (housing density)
#     - on block populations as weights
#         - extensive (number of people)
#         - intensive (population density)
# 
# - areal interpolation (on variables that are related to areas)
#     - extensive (don't see a ton of instances where this would be needed/appropriate)
#     - intensive (precipitation)
# 
# - all methods need to have group by and summarize after to handle instances
#   when the SAB intersects with multiple states: 
#     - sum on extensive variables (number of people or households)
#     - weighted mean on intensive variables (housing density, precipitation, etc.) 
#         - weighted by percent overlap by state
# 
# This function can handle a variety of variables, years, and SABs 
# (including those that may intersect multiple states, like the Navajo Nation). 
# SABs are only considered as intersecting multiple states if >= 20%
# of total area is present in a different state. This was to avoid 
# slight SAB and state overlaps due to boundary issues. 
# 
# Inputs: 
# - sabs = sf of EPA's water utility service area boundaries
# - sf_data_census = sf object containing census geometries 
# - interp_methods = data frame containing variables and methods 
#     for interpolation (see example linked below)
# - blocks_2020 = default is true - indicates whether geometries from 2020 or
#     2010 should be used as weights for interpolation. Due to current issues 
#     in downloading 2010 data, it is recommended you set save_data = T for 
#     any data containing 2010 census geographies. 
# - fips_col = the fips column name in your data frame 
# - save_data = a T/F on whether you want to save every iteration of your 
#     loop to a ./data/xwalk_census_geo_sabs/ folder. Helpful if your 
#     internet connection is spotty or if you're running this function on 
#     2010 variables (it currently takes longer to download census 
#     block geometries from 2010 b/c we have to loop through every county)
# 
# Outputs: 
# - sf object of your interpolated variables with water system boundaries. 
#     Column names will be concatenated with the specific method used for 
#     interpolating that variable. For example, if I was using population 
#     weighted interpolation to estimate populations, the column would be 
#     "pw_ext.population".
# 
# Example of use: ##############################################################
## prepping environment: 
# library(aws.s3)
# library(googlesheets4)
## function dependencies: 
# library(tidyverse)
# library(sf)
# # devtools::install_github('walkerke/tigris')
# library(tigris)
# library(tidycensus)
# library(areal)
# library(crsuggest)
# 
# options(tigris_use_cache = TRUE)
# 
# method for interpolating vars: 
# URL <- "https://docs.google.com/spreadsheets/d/1XsMHSUo_LZ-pc10Sdisa4EYaVIK9KBJVaE7eenYIwxw/edit?gid=0#gid=0"
# census_var_interp_methods <- read_sheet(URL, sheet = "polygon_data_inventory") %>%
#   janitor::clean_names()
# 
# grab sabs: 
# epa_sabs <- aws.s3::s3read_using(st_read,
#                                  object = "/national-dw-tool/raw/national/water-system/epa_sabs.geojson",
#                                  bucket = "tech-team-data")
# 
# grabbing data from CEJST: 
# cejst <- aws.s3::s3read_using(st_read, 
#                                  object = "s3://tech-team-data/national-dw-tool/raw/national/socioeconomic/cejst.geojson") 
# 
# cejst_interp_methods <- census_var_interp_methods %>%
#   filter(dataset == "cejst")
# 
# test_sabs <- epa_sabs %>%
#   filter(state == "AL")
# 
# test_cejst <- sab_crosswalk_data(test_sabs, cejst, cejst_interp_methods,
#                                  blocks_2020 = TRUE, fips_col = "geoid10", save_data = T)
################################################################################
xwalk_census_geo_sabs <- function(sabs, sf_data_census, interp_methods, 
                                  blocks_2020 = TRUE, fips_col, save_data = F){
  
  # clear these datasets if they're stored in the global environment - otherwise
  # they'll rbind the wrong things :) 
  rm(list = ls(pattern = 'a_ext|a_int|pw_ext_hh|pw_int_hh|pw_ext_pop|pw_int_pop|xwalk_data_loop'))
  
  # pull in helper functions used in the crosswalk: 
  source("./functions/helper_functions.R")
  
  # turn off spherical geoms, which should be fine since we're not really 
  # working towards the poles & data are on a projected CRS 
  sf_use_s2(F)
  
  # pullin in sabs 
  sab_t <- sabs %>%
    # projecting to alberts equal area for all interpolations 
    st_transform(., crs = 5070)

  # finding sabs that overlap with other states, filtered to those where at 
  # least 20% of the SAB area overlaps with the state boundary 
  states_pwsids <- .find_state_overlaps(sab_t) %>%
    filter(pct_overlap >= 20)
  
  # creating a new file to capture significant overlapping states for SABs.
  # This will avoid looping through states with little/no data for the SAB, 
  # which would return NA and propagate that NA across the entire service area 
  # since weighted means and sums are not na.rm = True. 
  # having the epic_states_intersect is still valuable for quickly filtering 
  # sabs to a particular state, this is just necessary for crosswalking data 
  pwsid_state_flag <- states_pwsids %>% 
    as.data.frame() %>%
    group_by(pwsid) %>%
    summarize(overlap_states = paste0(unique(stusps), collapse = ", ")) 
  
  sab_t <- merge(sab_t, pwsid_state_flag, by = "pwsid", all.x = T)
  
  # states to loop through: 
  unique_states <- str_sort(unique(unlist(str_split(sab_t$overlap_states, ",")))) %>%
    str_squish() %>% unique()
  
  # there are 13 SABs in CNMI - tidycensus is throwing an API error here 
  # so removing for now: 
  if(any(grepl("MP|Northern Mariana Islands|GU", unique_states))){
    message("Removing Northern Mariana Islands & Guam - not currently supported")
    unique_states_sabs <- unique_states[!grepl("MP|Northern Mariana Islands|GU", unique_states)]
  }  else {
    unique_states_sabs <- unique_states
  }
  
  ##############################################################################
  # census sf object tidying and finding state codes 
  ##############################################################################
  sf_data_census_tidy <- sf_data_census %>%
    st_transform(., crs = st_crs(sab_t)) %>%
    # find the census state code from the fips col: 
    mutate(state_code_function = substr(!!sym(fips_col), 1, 2)) %>%
    relocate(state_code_function)
  
  # translating state codes to state abbreviations
  sf_data_census_tidy_codes <- merge(sf_data_census_tidy, tigris::fips_codes %>% 
                                       select(state, state_code) %>% 
                                       unique() %>%
                                       rename(state_function = state), 
                                     by.x = "state_code_function", 
                                     by.y = "state_code", all.x = T) %>%
    relocate(state_function)

  ##############################################################################
  # finding states where we have data for sabs AND the census dataset to loop
  # through: 
  ##############################################################################  

  # all of the unique states in the sf_data_census_tidy dataset: 
  unique_sf_census <- unique(sf_data_census_tidy_codes$state_function)
  
  # finding states where we have data for sabs AND the census dataset: 
  unique_states_sab_both <- unique_states_sabs[(unique_states_sabs %in% unique_sf_census)]
  
  ##############################################################################
  # Looping through states and applying interpolation methods to appropriate 
  # variables
  ##############################################################################  
  # sorting variables into interpolation methods: 
  vars <- interp_methods %>%
    group_by(interp_method) %>%
    reframe(vars = unique(var))
  # how many interpolation methods are we workin' with: 
  unique_interp_methods <- unique(vars$interp_method)
  
  # looping through states: 
  xwalk_data_state <- sab_t %>% select(pwsid)
  xwalk_data_loop <- xwalk_data_state[0,]
  
  for(i in 1:length(unique_states_sab_both)){
    state_i <- unique_states_sab_both[i]
    message("Working on: ", state_i, ", loop ", i, " out of ", 
            length(unique_states_sab_both))
    
    # filtering sabs and census data - noting that crosswalk_states may have 
    # multiple entries 
    sab_i <- sab_t %>% 
      filter(grepl(state_i, overlap_states)) %>%
      select(pwsid)
    sf_data_i <- sf_data_census_tidy_codes %>% 
      filter(state_i == state_function)
    # grab the state code associated with the state we're working in: 
    state_i_code <- unique(sf_data_i$state_code_function)
    
    # turning estimation methods on/off based on presence in 
    # unique_interp_methods: 
    ###########################################################################
    # population weighted interpolation: 
    # - pw interpolation (on variables that relate to populations or houses)
    #     - on block households as weights
    #         - extensive (number of households)
    #         - intensive (housing density)
    #     - on block populations as weights
    #         - extensive (number of people)
    #         - intensive (population density)
    ###########################################################################
    if(any(c("intensive_pop", "intensive_hh", 
             "extensive_pop", "extensive_hh") %in% unique_interp_methods)){
      
      # loading block weights for pw_interpolation: 
      message("Grabbing Census Block Weights for Weighted Interpolation")
      # NOTE - grabbing 2010 blocks will take a bit longer because we
      # have to loop through every county within the state 
      state_blocks <- .grab_census_blocks(sf_data_i, fips_col,
                                          state_i, blocks_2020 = blocks_2020)
      
      # intensive interpolation using populations as weights
      if("intensive_pop" %in% vars$interp_method){
        message("Intensive Weighted Interpolation - Populations as Weights")
        
        pw_interp_intensive_pop <- vars %>%
          filter(interp_method %in% c("intensive_pop"))
        pw_interp_intensive_pop_data <- sf_data_i %>%
          select(!!sym(fips_col), pw_interp_intensive_pop$vars)
        
        # interpolatin' - starting with intensive population vars: 
        pw_int_pop <- interpolate_pw(
          from = pw_interp_intensive_pop_data,
          to = sab_i,
          to_id = "pwsid",
          extensive = FALSE, 
          weights = state_blocks,
          weight_column = "pop_weight",
          crs = 5070)
        
        # add interpolation method 
        pw_int_pop <- pw_int_pop %>%
          mutate(interp_method = "pw_intensive_pop_weights") %>%
          as.data.frame() %>%
          select(-starts_with("geom"))%>%
          arrange(pwsid)
      }    
      
      # intensive interpolation using households as weights
      if("intensive_hh" %in% vars$interp_method){
        message("Intensive Weighted Interpolation - Houses as Weights")
        
        pw_interp_intensive_hh <- vars %>%
          filter(interp_method %in% c("intensive_hh"))
        pw_interp_intensive_hh_data <- sf_data_i %>%
          select(!!sym(fips_col), pw_interp_intensive_hh$vars)
        
        # interpolatin' 
        pw_int_hh <- interpolate_pw(
          from = pw_interp_intensive_hh_data,
          to = sab_i,
          to_id = "pwsid",
          extensive = FALSE, 
          weights = state_blocks,
          weight_column = "housing_weight",
          crs = 5070)
        
        # add interpolation method 
        pw_int_hh <- pw_int_hh %>%
          mutate(interp_method = "pw_intensive_hh_weights") %>%
          as.data.frame() %>%
          select(-starts_with("geom"))%>%
          arrange(pwsid)
      } 
      
      # extensive interpolation using populations as weights
      if("extensive_pop" %in% vars$interp_method){
        message("Extensive Weighted Interpolation - Populations as Weights")
        
        pw_interp_extensive_pop <- vars %>%
          filter(interp_method %in% c("extensive_pop"))
        pw_interp_extensive_pop_data <- sf_data_i %>%
          select(!!sym(fips_col), pw_interp_extensive_pop$vars)
        
        # interpolatin' 
        pw_ext_pop <- interpolate_pw(
          from = pw_interp_extensive_pop_data,
          to = sab_i,
          to_id = "pwsid",
          extensive = TRUE, 
          weights = state_blocks,
          weight_column = "pop_weight",
          crs = 5070)
        
        # add interpolation method 
        pw_ext_pop <- pw_ext_pop %>%
          mutate(interp_method = "pw_extensive_pop_weights") %>%
          as.data.frame() %>%
          select(-starts_with("geom"))%>%
          arrange(pwsid)
      }    
      
      # extensive interpolation using households as weights
      if("extensive_hh" %in% vars$interp_method){
        message("Extensive Weighted Interpolation - Houses as Weights")
        
        pw_interp_extensive_hh <- vars %>%
          filter(interp_method %in% c("extensive_hh"))
        pw_interp_extensive_hh_data <- sf_data_i %>%
          select(!!sym(fips_col), pw_interp_extensive_hh$vars)
        
        # interpolatin' 
        pw_ext_hh <- interpolate_pw(
          from = pw_interp_extensive_hh_data,
          to = sab_i,
          to_id = "pwsid",
          extensive = TRUE, 
          weights = state_blocks,
          weight_column = "housing_weight",
          crs = 5070)
        
        # add interpolation method 
        pw_ext_hh <- pw_ext_hh %>%
          mutate(interp_method = "pw_extensive_hh_weights") %>%
          as.data.frame() %>%
          select(-starts_with("geom"))%>%
          arrange(pwsid)
      } 
      
      # end population weighted interpolation 
    } 
    
    ###########################################################################
    # Areal interpolation
    # - areal interpolation (on variables that are related to areas)
    #     - extensive (??? I feel like we'd normally just intersect - don't see a
    #       ton of instances where this would be needed/appropriate)
    #     - intensive (precipitation) - we should also summarize this to HUCs
    ###########################################################################
    # a great reference for areal interpolation:
    # https://cran.r-project.org/web/packages/areal/vignettes/data-preparation.html
    
    if(any(c("intensive_areal", "extensive_areal") %in% unique_interp_methods)){
      
      if("intensive_areal" %in% unique_interp_methods){
        message("Intensive Areal Interpolation")
        
        areal_interp_intensive <- vars %>%
          filter(interp_method %in% c("intensive_areal"))
        areal_interp_intensive_data <- sf_data_i %>%
          select(!!sym(fips_col), areal_interp_intensive$vars)
        
        # validating: 
        # ar_validate(sab_i_areal, sf_data_areal, 
        #             varList = areal_interp_intensive$vars, 
        #             method = "aw", verbose = T)
        
        # interpolating
        a_int <- areal::aw_interpolate(sab_i, 
                                       tid = "pwsid",
                                       source = areal_interp_intensive_data, 
                                       sid = !!sym(fips_col),
                                       # the weight is counter intuitive, but 
                                       # function documentation says "for intensive
                                       # interpolations, should be "sum"
                                       weight = "sum", 
                                       output = "sf", 
                                       intensive = areal_interp_intensive$vars)
        # add interpolation method 
        a_int <- a_int %>%
          mutate(interp_method = "areal_intensive") %>%
          as.data.frame() %>%
          select(-starts_with("geom"))%>%
          arrange(pwsid)
        
        
      }
      
      if("extensive_areal" %in% unique_interp_methods){
        message("Extensive Areal Interpolation")
        
        areal_interp_extensive <- vars %>%
          filter(interp_method %in% c("extensive_areal"))
        areal_interp_extensive_data <- sf_data_i %>%
          select(!!sym(fips_col), areal_interp_extensive$vars)
        
        # interpolating
        a_ext <- areal::aw_interpolate(sab_i, 
                                       tid = "pwsid",
                                       source = areal_interp_extensive_data, 
                                       sid = !!sym(fips_col),
                                       # the weight is counter intuitive, but 
                                       # function documentation says "for intensive
                                       # interpolations, should be "sum"
                                       weight = "sum", 
                                       output = "sf", 
                                       extensive = areal_interp_extensive_data$vars)
        # add interpolation method 
        a_ext <- a_ext %>%
          mutate(interp_method = "areal_extensive") %>%
          as.data.frame() %>%
          select(-starts_with("geom")) %>%
          arrange(pwsid)
        
        
      }
      
      # end population weighted interpolation 
    }
    
    message("Recombining Data")
    
    # grab relevant datasets from global environment and add them to a data frame
    data_bind <- mget(ls(pattern = "a_ext|a_int|pw_ext_hh|pw_int_hh|pw_ext_pop|pw_int_pop")) %>%
      as.data.frame() %>%
      # keep only one pwsid column (can confirm they all line up and were 
      # arranged by pwsid earlier in the code)
      rename(pwsid = colnames(.)[1]) %>%
      # removing duplicated column names
      select(-contains("interp_method")) %>%
      select(-contains(".pwsid")) %>%
      # recombining with OG sab geometries 
      left_join(sab_i) %>%
      st_as_sf() %>%
      # adding final columns to keep track of the state that was crosswalked: 
      mutate(crosswalk_state = state_i) %>%
      relocate(crosswalk_state, .after = pwsid)
    
    # binding for each iteration of the loop: 
    xwalk_data_loop <- bind_rows(xwalk_data_loop, data_bind)
    
    # essentially cache data if needed 
    if(save_data == TRUE){
      message("Saving Data for ", state_i, " loop ", i)
      base_name <- paste0("./data/xwalk_census_geo_sabs-", Sys.Date(), "-", unique(interp_methods$dataset), "/")
      dir.create(file.path(base_name))
      xwalk_data_loop_df <- xwalk_data_loop %>%
        as.data.frame() %>%
        select(-starts_with("geom"))
      write.csv(xwalk_data_loop_df, paste0(base_name, "xwalk_data", state_i, i, ".csv"))
    }
  }

  message("Handling Water Systems that Intersect w/ Multiple States")

  # detecting sabs that intersect multiple states:
  multiple_states <- xwalk_data_loop %>%
    as.data.frame() %>%
    group_by(pwsid) %>%
    summarize(crosswalk_state = paste0(unique(crosswalk_state), collapse = ", ")) %>%
    filter(grepl(", ", crosswalk_state))

  # grabbing the area overlap
  overlapping_states <- states_pwsids %>%
    filter(pwsid %in% multiple_states$pwsid) %>%
    select(pwsid, stusps, pct_overlap) %>%
    rename(crosswalk_state = stusps)
 
  # isolating them from xwalk_data_loop for mutating:
  xwalk_data_mult_states <- xwalk_data_loop %>%
    as.data.frame() %>%
    select(-starts_with("geom")) %>%
    filter(pwsid %in% multiple_states$pwsid) %>%
    left_join(overlapping_states,
              by = c("pwsid", "crosswalk_state")) %>%
    unique() %>%
    group_by(pwsid) %>%
    # intensive variables are calculated using a weighted mean, with the
    # percent overlap as the weight
    mutate(across(contains(".int"), ~weighted.mean(., w = pct_overlap))) %>%
    # extensive variables are simply summed
    mutate(across(contains(".ext"), ~sum(.))) %>%
    # grabbing the first one of each group, as they're duplicated
    slice(1) %>%
    ungroup() %>%
    # adding crosswalked states as appended together and reformatting to match
    # the main dataset
    select(-crosswalk_state) %>%
    left_join(multiple_states, by = "pwsid") %>%
    relocate(crosswalk_state, .after = pwsid) %>%
    left_join(xwalk_data_state) %>%
    st_as_sf()

  # recombining with og data:
  xwalk_data_loop_nodups <- xwalk_data_loop %>%
    filter(!(pwsid %in% xwalk_data_mult_states$pwsid))

  # binding!
  xwalk_data_tidy <- bind_rows(xwalk_data_loop_nodups, xwalk_data_mult_states) %>%
    select(-pct_overlap)

  # turn spherical geoms back on
  sf_use_s2(T)
  
  # woooooooo!!! 
  return(xwalk_data_tidy)
}


