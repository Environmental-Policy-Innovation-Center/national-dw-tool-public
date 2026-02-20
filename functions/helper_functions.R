######################## HELPER FUNCTIONS ######################################
# Collection of functions that get pulled into the xwalk_census_vars_sabs.R
# and xwalk_census_geo_sabs.R functions! 
################################################################################
# .tidy_epa_sabs(sab)
# EmmaLi Tsai 
# March 2025
# Deprecated as of Jan 26th, 2026 as we've moved to tidying manually & as needed
################################################################################
# Tidy the EPA SABs - This function organizes the EPA's service area boundary 
# dataset and makes sure the geometries have valid linework. The latest SABs
# Feb 2025, also contain duplicated service area boundaries & this function 
# removes the duplicated SAB with the later date_created field. 
# # 
# This function also runs an intersection with state
# boundaries and detects when >20% of the SAB area overlaps with a particular
# state, as a way to identify what state(s) data to use for crosswalking. 
# 
# inputs: 
#   - sabs - EPA's service area boundaries
# 
# outputs: 
#  - a tidy df with valid geometries of EPA's SABs, unique geometries for each 
#    water system, and returns a df "states_pwsids" of PWSIDs with the state(s)
#    the system overlaps with to the global environment. This df is used 
#    for other parts of the crosswalking function. 
################################################################################
.DEPRECATED_tidy_epa_sabs <- function(sabs) {
  ##############################################################################
  # Fixing geometry errors and handling duplicated pwsids 
  ##############################################################################
  # making sure the sab crs matches with census and block weights for 
  # interpolating
  sab_t <- sabs %>%
    st_transform(., crs = "WGS84") %>%
    st_cast(.) %>%
    # these geometries cannot be crosswalked at the moment - they are 
    # recombined as tier 3 - geometry issues at the end of the function 
    filter(st_geometry_type(.) != "GEOMETRYCOLLECTION") %>%
    # NOTE - in MD & OH, there is a duplicated pwsid with different dates 
    # created - "MD0070015" and "OH2956203" 
    # after the latest SABs update in Feb 2025
    group_by(pwsid) %>%
    arrange(desc(date_created)) %>%
    slice(1) %>%
    ungroup()
  
  # throwing a message to show how these are handled
  if(nrow(sab_t) != nrow(sabs)){
    message("NOTE - ", nrow(sabs) - nrow(sab_t), " duplicated SABs in EPA data, removing the duplicated SAB with the later date_created field")
  }
  
  # turn off spherical geometry - otherwise, errors are thrown during pw 
  # interpolation 
  sf_use_s2(FALSE)
  
  if(sum(!st_is_valid(sab_t)) > 0) {
    # needed to make multipolygons from EPAs dataset which has disconnected
    # polyons for pwsids and/or intersecting vertices 
    # NOTE: this DOES change the spatial footprint of the SAB, but valid geoms
    # is required for pw interpolation... there are a few states that have only
    # 8 invalid geoms, but TX has ~380
    sab_t <- sab_t %>%
      st_make_valid(., geos_method = "valid_linework")
  }
  
  ##############################################################################
  # Finding sab and state overlaps
  ##############################################################################
  
  # finding OG overlap to determine whether the overlap is simply a boundary 
  # error: 
  og_sab_area <- sab_t %>%
    mutate(og_area = as.numeric(st_area(.))) %>%
    as.data.frame() %>%
    select(pwsid, og_area)
  
  
  # grabbing state boundaries
  state_boundaries <- tigris::states() %>%
    janitor::clean_names() %>%
    st_transform(., crs = st_crs(sab_t))
  
  # finding where sabs are located - this is needed for some SABs that don't 
  # match to a census state code (i.e., Navajo Nation)
  message("Finding what state to pull data from")
  sab_state <- st_intersection(sab_t, state_boundaries)
  
  # make helper dataframe of states & pwsids 
  # TODO - adding this to the global environment (which is typically not 
  # good practice in functions), but I'd like to use these as weights 
  # when handling systems that intersect multiple water systems 
  states_pwsids <<- sab_state %>% 
    mutate(area_overlap = as.numeric(st_area(.))) %>%
    as.data.frame() %>%
    select(pwsid, stusps, statefp, area_overlap) %>%
    left_join(og_sab_area) %>%
    unique() %>%
    mutate(pct_overlap = 100*(area_overlap/og_area)) %>%
    # filtering for only meaningful area overlaps: 
    filter(pct_overlap >= 20)
  
  # adding a "crosswalk_states" field to add back to the sabs dataset: 
  pwsid_state_summary <- states_pwsids %>%
    group_by(pwsid) %>%
    summarize(crosswalk_states = paste(unique(stusps), collapse = ","))
  
  # merging back
  sab_t <- merge(sab_t, pwsid_state_summary, by = "pwsid", all.x = T) %>%
    relocate(crosswalk_states, .after = state)
  
  return(sab_t)
}

################################################################################
# .find_bc_xwalk(sf_data_census_tidy_codes, sab_t, fips_col)
# EmmaLi Tsai 
# March 2025
# Deprecated as of Jan 26th, 2026 as we've moved to EPA crosswalks
################################################################################
# This function uses the fips_column and detects and grab the appropriate 
# blue conduit crosswalk. This function currently isnt' used in any of our 
# crosswalking functions, but could be helpful in the future. These crosswalk
# files are based on 2020 census geographies and should not be used for 2010
# data. 
# 
# inputs:
#   - sf_data_census_tidy_codes: tidy census geography data with a tidy 
#     fips column (i.e., constant number of characters)
#   - sab_t: service area boundaries with pwsids
#   - fips_col: character of the fips column name in sf_data_census_tidy_codes
# 
# output: returns the correct blue conduit crosswalk based off whether census
#     block groups or tracts are detected.
#
################################################################################
.DEPRECATED_find_bc_xwalk <- function(sf_data_census_tidy_codes, sab_t, fips_col){
  # figuring out which parcel crosswalk to grab for tier 2:
  census_geo <- unique(nchar(sf_data_census_tidy_codes[[sym(fips_col)]]))
  # if this is >1 it needs to be fixed 
  if(length(census_geo) > 1){
    stop("There are multiple kinds of FIPS codes in your census_sf variable, please fix") 
  }
  # making sure the census_sf is at the appropriate geography: 
  if(!(census_geo %in% c(11, 12))){
    stop("Your census_sf needs to be at the tract or block group level.") 
  }
  
  # grabbing the correct crosswalk based on the number of characters 
  # detected in the FIPS code: 
  if(census_geo == 11) {
    # tract crosswalk for tier 2 crosswalk:  
    message("Grabbing Tract Parcel Crosswalk")
    bc_crosswalk <- aws.s3::s3read_using(read.csv, 
                                         object = "s3://tech-team-data/pws_crosswalk/EPA_SABS_parcel_weighted_pwsid_census_tracts.csv") %>%
      select(-X) %>%
      # grab the pwsids from our SABs
      filter(pwsid %in% unique(sab_t$pwsid)) %>%
      # if the geoid is missing prefix 0, add it back in
      mutate(tract_geoid = as.character(tract_geoid),
             tract_geoid = case_when(
               str_length(tract_geoid) == 10 ~ paste0(0, tract_geoid),
               TRUE ~ tract_geoid
             )) %>%
      # there are some tract geoids in the crosswalk are NAs - assuming these are errors?
      filter(!(is.na(tract_geoid))) %>%
      # standardizing name for later calculations
      rename(geoid = tract_geoid, 
             weight = tract_parcel_weight)
  } else {
    # block group crosswalk for tier 2 crosswalk:  
    message("Grabbing Block Group Parcel Crosswalk")
    bc_crosswalk <- aws.s3::s3read_using(read.csv, 
                                         object = "s3://tech-team-data/pws_crosswalk/EPA_SABS_parcel_weighted_pwsid_census_blockgroup.csv") %>%
      # grab the pwsids from our SABs
      filter(pwsid %in% unique(sab_t$pwsid)) %>%
      # if the geoid is missing prefix 0, add it back in
      mutate(census_blockgroup = as.character(census_blockgroup),
             census_blockgroup = case_when(
               str_length(census_blockgroup) == 11 ~ paste0(0, census_blockgroup),
               TRUE ~ census_blockgroup
             )) %>%
      # there are some tract geoids in the crosswalk are NAs - assuming these are errors?
      filter(!(is.na(census_blockgroup))) %>%
      # standardizing name for later calculations
      rename(geoid = census_blockgroup, 
             weight = parcel_weight)
  }
  
  return(bc_crosswalk)
}


################################################################################
# .grab_census_blocks
# EmmaLi Tsai 
# Jan 2026
################################################################################
# This function pulls the 2020 or 2010 census blocks for a particular state.
# 
# inputs:
#   - sf_data_i: sf file of data with census geographies (for example, CEJST) 
#   - state_i: state abbreviation
#   - fips_col: character of the fips column name in sf_data_i
#   - blocks_2020 : whether the function should pull 2020 or 2010 census blocks
# 
# output: returns the correct census blocks as an sf oject
#
################################################################################

# grab the appropriate census blocks: 
.grab_census_blocks <- function(sf_data_i, fips_col, 
                                state_i, blocks_2020 = TRUE){
  
  if(blocks_2020 == TRUE){
    message("Using 2020 Blocks as Weights")
    
    # grabbing 2020 from tigris (which has all of the columns we need): 
    state_blocks <- tigris::blocks(
      state = state_i, 
      year = 2020) %>%
      st_transform(., crs = 5070) %>%
      # standardize column names: 
      rename(pop_weight = "POP20", 
             housing_weight = "HOUSING20")
  } else {
    message("Using 2010 Blocks as Weights")

    # NOTES: Getting 2010 block geometries with populations and housing info 
    # to use as weights for pw_interpolation was hard. Using the blocks() 
    # function from tigris didn't return blocks or populations, and the 
    # get_decennial function from tidycensus requires you to loop through 
    # every county in the state (see here: https://github.com/walkerke/tidycensus/issues/598)
    # I also couldn't download the data directly 
    # from the census website in R (the census website blocked all of this 
    # traffic): https://www2.census.gov/geo/tiger/TIGER2010BLKPOPHU/
    
    # this is the new URL that the dev version of tigris is using to get around 
    # the fact the census website blocked all FTP traffic - using this 
    # to download block populations and houses for a whole state  
    state_blocks_2010_string <- paste0("s3://tech-team-data/national-dw-tool/census_blocks_2010/", state_i, "_blocks_2010.geojson")
    
    # pull blocks from where they're stored on AWS 
    state_blocks <- aws.s3::s3read_using(st_read, 
                                         object = state_blocks_2010_string) %>%
      st_transform(., crs = 5070) 
    
  #   code below commented out now that we've added 2010 state blocks to 
  #   AWS using the .add_2010_blocks_to_aws function. 
  #   # grabbing counties from the fips code: 
  #   counties <- sf_data_i[[sym(fips_col)]] %>%
  #     substr(., 3, 5) %>%
  #     unique() %>%
  #     as.character()
  #   
  #   # looping through counties and grabbing 2010 data: 
  #   # adding a trycatch since the API will boot me off for large states (CA, TX
  #   # and FL)
  #  state_blocks <- tryCatch(
  #     {tidycensus::get_decennial(
  #       geography = "block",
  #       variables = c(POP10 = "P001001",
  #                     HOUSING10 = "H001001"),
  #       state = state_i,
  #       county = counties,
  #       year = 2010,
  #       geometry = TRUE)}, 
  #     
  #     # if there is an error, break the counties into 3 segments and loop again
  #     error = function(e){
  #       message(paste("Census API request is too big - looping 
  #                     through counties in segments"))
  #       
  #       # find a pretty even split: 
  #       county_split <- round(seq(1, length(counties), length.out = 5))
  #       state_blocks <- data.frame() 
  #       
  #       # loop through - we only need two loops to capture the two segments 
  #       # we're interested in
  #       for(u in 1:4){
  #         message(paste("On loop: ", u))
  #         u_low <- county_split[u]
  #         u_high <- county_split[u+1]
  #         county_i <- counties[u_low:u_high]
  #         
  #         state_blocks_i <- tidycensus::get_decennial(
  #           geography = "block",
  #           variables = c(POP10 = "P001001",
  #                         HOUSING10 = "H001001"),
  #           state = state_i,
  #           county = county_i,
  #           year = 2010,
  #           geometry = TRUE)
  #         
  #         state_blocks <- rbind(state_blocks, state_blocks_i) 
  #       }
  #       # there might be an overlap with one county 
  #       state_blocks <- state_blocks %>% distinct()
  #       return(state_blocks)
  #     }
  #   )
  #  message(paste("Pivoting to Wide Format"))
  #   state_blocks_wide <- state_blocks %>%
  #     pivot_wider(., names_from = variable, values_from = value) %>%
  #     st_transform(., crs = "WGS84") 
  #   
  #   state_blocks <- state_blocks_wide %>%
  #     as_tibble() %>%
  #     # standardize column names:
  #     rename(pop_weight = POP10, 
  #            housing_weight = HOUSING10) %>%
  #     st_as_sf()
  #   
  }
  return(state_blocks)
}

################################################################################
# .add_2010_blocks_to_aws
# EmmaLi Tsai 
# Jan 2026
################################################################################
# This function pulls the 2010 census blocks for all states (with the exception
# of VI, MP, GU, and AS) and adds the census blocks to a folder in S3. 
# TODO - add arguments to this function so it can be used for other datasets 
################################################################################
.add_2010_blocks_to_aws <- function() {
  
  # grabbing counties from the fips code: 
  states_to_loop <- tigris::states() %>%
    as.data.frame() %>%
    select(STUSPS) %>%
    # note - no data for USVI, MP, GU or AS
    filter(!grepl("VI|MP|GU|AS", STUSPS))
  
  # loop through states and grab state blocks: 
  for(i in 1:nrow(states_to_loop)){
    state_i <- states_to_loop[i, ]
    message(paste0("Working on: ", state_i, "; ", i, " out of ", nrow(states_to_loop)))
    
    # grab county geoids to loop through: 
    state_counties <- tidycensus::get_decennial(
      geography = "county",
      variables = c(POP10 = "P001001",
                    HOUSING10 = "H001001"),
      state = state_i,
      year = 2010,
      geometry = F) 
    
    counties <- state_counties$GEOID %>% 
      unique() %>%
      substr(., 3, 5) %>%
      as.character()
    
    state_blocks <- tryCatch(
      {tidycensus::get_decennial(
        geography = "block",
        variables = c(POP10 = "P001001",
                      HOUSING10 = "H001001"),
        state = state_i,
        county = counties,
        year = 2010,
        geometry = TRUE)}, 
      
      # if there is an error, break the counties into 3 segments and loop again
      error = function(e){
        message(paste("Census API request is too big - looping 
                      through counties in segments"))
        
        # find a pretty even split: 
        county_split <- round(seq(1, length(counties), length.out = 5))
        state_blocks <- data.frame() 
        
        # loop through - we only need two loops to capture the two segments 
        # we're interested in
        for(u in 1:4){
          message(paste("On loop: ", u))
          u_low <- county_split[u]
          u_high <- county_split[u+1]
          county_i <- counties[u_low:u_high]
          
          state_blocks_i <- tidycensus::get_decennial(
            geography = "block",
            variables = c(POP10 = "P001001",
                          HOUSING10 = "H001001"),
            state = state_i,
            county = county_i,
            year = 2010,
            geometry = TRUE)
          
          state_blocks <- rbind(state_blocks, state_blocks_i) 
        }
        # there might be an overlap with one county 
        state_blocks <- state_blocks %>% distinct()
        return(state_blocks)
      }
    )
    
    message(paste("Pivoting to Wide Format"))
    state_blocks_wide <- state_blocks %>%
      pivot_wider(., names_from = variable, values_from = value) %>%
      st_transform(., crs = "WGS84") 
    
    state_blocks <- state_blocks_wide %>%
      as_tibble() %>%
      # standardize column names:
      rename(pop_weight = POP10, 
             housing_weight = HOUSING10) %>%
      st_as_sf()
    
    s3_loc_i <- paste0("s3://tech-team-data/national-dw-tool/census_blocks_2010/", state_i, "_blocks_2010.geojson")
    message(paste("Adding to S3: ", s3_loc_i))
    
    tmp <- tempfile()
    st_write(state_blocks, dsn = paste0(tmp, ".geojson"))
    on.exit(unlink(tmp))
    put_object(
      file = paste0(tmp, ".geojson"),
      object = s3_loc_i,
      multipart = T
    )
    # seeing how fast this actually is: 
    # tictoc::tic()
    # state_blocks <- aws.s3::s3read_using(st_read,
    #                                      object = "s3://tech-team-data/national-dw-tool/census_blocks_2010/FL_blocks_2010.geojson")
    # tictoc::toc()
    # mapview::mapview(state_blocks)
  }
}
  
################################################################################
# .relate_to_huc12(points)
# EmmaLi Tsai 
# July 2025
################################################################################
# This function uses a set of lat/long data and loops through each state 
# and assigns them a huc12 based on location. 
# 
# inputs:
#   - point_geojson - geojson of lat/long data
# 
# output: point data related to huc12 
################################################################################
.relate_to_huc12 <- function(point_geojson){
  # grab state data: 
  states <- states() %>% 
    arrange(NAME)
  
  # empty df to append to: 
  df_huc12 <- point_geojson[0,]
  for(i in 1:nrow(states)){
    state_i <- states[i,]
    message("On state ",state_i$NAME, ", ", i, ":", " out of ", nrow(states))
    # grab huc12 boundaries for the state: 
    state_i_huc_geom <- get_huc(states[i,], type = "huc12")
    # transform crs before intersecting: 
    tidy_state_i <- state_i_huc_geom %>%
      mutate(gnis_id = as.integer(gnis_id)) %>%
      st_transform(., crs = st_crs(point_geojson))
    # run intersection of rmps and the state 
    df_intersection <- st_intersection(point_geojson, tidy_state_i)
    # add this to global environment 
    df_huc12 <- bind_rows(df_huc12, df_intersection)
  }
  
  df_huc12_unique <- df_huc12 %>%
    unique() 
  
  return(df_huc12)
}

################################################################################
# update_data_summary
# EmmaLi Tsai 
# July 2025
################################################################################
# This function updates the data summary in the task manager
# 
# inputs:
#   - dataset_name - name of the dataset as it appears in the task manager - 
#         this is the same name that will get saved to s3
#   - raw_s3_link - where the raw dataset should live in s3
#   - clean_s3_link - where the clean dataset should live in s3
# 
# output: none  
################################################################################
update_data_summary <- function(dataset_name, raw_s3_link, clean_s3_link){
  
  # pulling in task manager for updating relevant sections: 
  task_manager <- aws.s3::s3read_using(read.csv, 
                                       object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv")%>%
    mutate(across(everything(), ~ as.character(.)))
  
  # create dataframe with cleaner info added
  task_manager_df <- data.frame(dataset = dataset_name, 
                                date_downloaded = Sys.Date(), 
                                raw_link = raw_s3_link,
                                clean_link = clean_s3_link)
  
  # add a new row if the dataset is not yet in task manager: 
  if(!(dataset_name %in% task_manager$dataset)){
    task_manager <- bind_rows(task_manager, data.frame(dataset = dataset_name))
  }
  
  # select the other columns from the task manager that we do not want to 
  # overwrite: 
  dont_touch_these_columns <- setdiff(names(task_manager), 
                                      names(task_manager_df))
  task_manger_simple <- task_manager %>% 
    select(dataset, all_of(dont_touch_these_columns))
  
  # merge them together to create the updated row
  updated_row <- merge(task_manger_simple, 
                       task_manager_df, by = "dataset") %>%
    mutate(across(everything(), ~ as.character(.)))
  
  # bind this row back to the task manager
  updated_task_manager <- task_manager %>%
    filter(dataset != dataset_name) %>% 
    bind_rows(., updated_row) %>%
    arrange(dataset)
  
  # write back to s3: 
  tmp <- tempfile()
  write.csv(updated_task_manager, file = paste0(tmp, ".csv"), row.names = F)
  on.exit(unlink(tmp))
  put_object(
    file = paste0(tmp, ".csv"),
    object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv",
    acl = "public-read"
  )

}
 

# find significant state overlaps: 
.find_state_overlaps <- function(sab_t){
  ##############################################################################
  # Finding sab and state overlaps
  ##############################################################################
  
  # finding OG overlap to determine whether the overlap is simply a boundary 
  # error: 
  og_sab_area <- sab_t %>%
    mutate(og_area = as.numeric(st_area(.))) %>%
    as.data.frame() %>%
    select(pwsid, og_area)
  
  
  # grabbing state boundaries
  state_boundaries <- tigris::states() %>%
    janitor::clean_names() %>%
    st_transform(., crs = st_crs(sab_t))
  
  # finding where sabs are located - this is needed for some SABs that don't 
  # match to a census state code (i.e., Navajo Nation)
  message("Finding what state to pull data from")
  sab_state <- st_intersection(sab_t, state_boundaries)
  
  # make helper dataframe of states, pwsids, and % overlap
  states_pwsids <- sab_state %>% 
    mutate(area_overlap = as.numeric(st_area(.))) %>%
    as.data.frame() %>%
    select(pwsid, stusps, statefp, area_overlap) %>%
    left_join(og_sab_area) %>%
    unique() %>%
    mutate(pct_overlap = 100*(area_overlap/og_area)) 
    # filtering for only meaningful area overlaps: 
    # filter(pct_overlap >= 20)
  
  return(states_pwsids)
}
