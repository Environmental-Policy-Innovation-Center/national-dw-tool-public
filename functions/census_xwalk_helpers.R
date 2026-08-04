################################################################################
# Census Crosswalk Helpers
################################################################################

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
    state_blocks_2010_key <- paste0("national-dw-tool/census_blocks_2010/", state_i, "_blocks_2010.geojson")

    # pull blocks from where they're stored on AWS
    state_blocks <- s3_read_geojson(state_blocks_2010_key) %>%
      st_transform(., crs = 5070)
  }
  return(state_blocks)
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
