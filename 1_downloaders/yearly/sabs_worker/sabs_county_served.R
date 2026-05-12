###############################################################################
# SABs -  Relate Service Area Boundaries to Counties 
#
# TODO - this is NOT currently part of the data pipeline - this script simply
# takes SABs and finds the county_served by running an intersection (2024 TIGER),
# resolving instances where a SAB intersects multiple counties and states (based 
# on SAB area overlap >= 10%), and pushes this to TEST-STAGED. It completely 
# bypasses certain parts of the data pipeline, which is not ideal, but was a 
# quick fix to make this available for the frontend ETL pipeline. 
# 
# Specifically this issue: https://github.com/orgs/Environmental-Policy-Innovation-Center/projects/4/views/1?pane=issue&itemId=185337757&issue=Environmental-Policy-Innovation-Center%7Cwater-data-tool%7C144
# 
# Note: this dataset is not currently provided in the data downloads for the
# tool. 
# 
# In the future, this code should run w/ the EPA SABs update & after any manual 
# corrections to the latest SABs. We should add this as a separate dataset 
# to the task manager (if the task manager continues to exist), 
# actually stand up a raw (w/o the 10% filter), and clean (with the 10% filter)
# datasets to s3. With the current structure of the pipeline, this should 
# get bundled as a new dataset in 3.2 Water System List section of 
# 2_summarize_data.Rmd (I can't do this now since it would create merge 
# conflicts, and we might be changing this in the reorg anyways). We will need 
# to add this dataset to the data inventory: https://docs.google.com/spreadsheets/d/15iVYq2v3Gpy5Zug3BhYC0vU4L-axV5g0drZt4-uLv-Q/edit?gid=2096327024#gid=2096327024
# and change the "use_in_tool" flag to "yes" so it gets pushed to staged 
# in 3_ui_prep.R. 
# 
# Date: May 12th, 2026
###############################################################################
# Libraries 
library(tidyverse)
library(aws.s3)
library(sf)
library(janitor)
library(tidycensus)
library(tigris)
library(mapview)
# no scientific notation! 
options(scipen = 9999)

# read in SABS: 
epa_sabs <- aws.s3::s3read_using(st_read, 
                                 object = "s3://tech-team-data/national-dw-tool/clean/national/epa_sabs.geojson") 

# default to 2024 county boundaries, from TIGER
us_counties <- counties()
# quick map to ensure the entire U.S. is covered 
# mapview(us_counties)

# for some of the coding bits for when this gets added to the task manager
dataset_i <- "sabs_pwsid_county"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/national/water-system/sabs_pwsid_county.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/national/sabs_pwsid_county.csv"
###############################################################################
# first, simplify epa_sabs and find the original area
epa_sabs_simp <- epa_sabs %>%
  select(pwsid, epic_states_intersect) %>%
  mutate(sab_area = st_area(.))

# do the same for counties 
us_counties_simp <- us_counties %>%
  select(STATEFP, NAMELSAD) %>%
  mutate(county_area = st_area(.))
# relate STATEFP to the state abb
us_states <- states() %>%
  as.data.frame() %>%
  select(STATEFP, STUSPS) 
# add states to us_counties_simp
us_counties_simp_state <- merge(us_counties_simp, us_states, by = "STATEFP") %>%
  # reorg so state abbreviation is at the beginning 
  relocate(STUSPS) %>%
  st_transform(., crs = st_crs(epa_sabs))

# unique states to loop through: 
states_to_loop <- unique(us_counties_simp_state$STUSPS) 
# we don't have SABs for USVI and AS (yet)
# TODO - this might change in the future 
states_to_loop <- states_to_loop[!grepl("AS|VI", states_to_loop)] 

# loopin
pwsid_counties_list <- list()
for(i in 1:length(states_to_loop)){
  state_i <- states_to_loop[i]
  print(paste0("On State: ", state_i))
  
  # filter counties: 
  us_county_i <- us_counties_simp_state %>%
    filter(STUSPS == state_i)
  print(paste0("Counties Identified: ", nrow(us_county_i)))
  
  # filter SABs: 
  sabs_state_i <- epa_sabs_simp %>%
    filter(grepl(state_i, epic_states_intersect))
  
  # run intersection: 
  # sabs_intersection_i <- st_join(sabs_state_i, us_county_i, 
  #                                join = st_intersects) 
  # I prefer st_intersection over a st_join so I can figure out how much the SAB
  # overlaps w/ the county boundary. This allows us to remove very slight 
  # overlaps due to boundary error. 
  print(paste0("Running Intersection"))
  sabs_intersection_i <- st_intersection(sabs_state_i, us_county_i) %>%
    # some intersections created an invalid geom, so fixing them before running 
    # an st_area 
    st_make_valid()
  print(paste0("Intersection Complete"))
  
  # filtering: 
  sabs_county_i <- sabs_intersection_i %>%
    # calculate percent intersection 
    mutate(pct_intersection = st_area(.), 
           pct_inter_sab = 100*(pct_intersection/sab_area)) %>%
    # tidy up names 
    rename(county_name = NAMELSAD, 
           state_name = STUSPS) %>%
    # drop intersection geoms to save memory 
    as.data.frame() %>%
    # we only need these columns 
    select(pwsid, county_name, state_name, sab_area, 
           county_area, pct_intersection, pct_inter_sab) 
  
  # quick map for qa/qc: 
  # test <- sabs_state_i %>% filter(pwsid == "AL0000547") 
  # mapview(test) + 
  #   mapview(us_county_i)
  
  # add the thing to da list 
  pwsid_counties_list[[i]] <- sabs_county_i
}

# bind the rows to create a data frame
pwsid_counties_all <- bind_rows(pwsid_counties_list) %>%
  # this needs to be numeric, rather than units type
  mutate(pct_inter_sab = as.numeric(pct_inter_sab), 
         pct_inter_county = 100*(as.numeric(pct_intersection)/as.numeric(county_area)))
 
# add this dataset, before filtering, to the raw bucket in s3
# writing this to s3: 
tmp <- tempfile()
write.csv(pwsid_counties_all, paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link,
  bucket = "tech-team-data",
  multipart = T
)

# identify a cutoff point for SAB overlap to remove those captured by slight 
# boundary issues 
# visually, there appears to be a breakpoint at 20%, which is a cutoff we've 
# used in the past. This would be a bit more conservative, but still capture 
# counties predominately served by a water system. However, when I tried this, 
# I was missing 4 large systems that span multiple counties, such that their 
# area overlap w/ counties was <20% total area. I've bumped this to 10%, which 
# is still decently conservative to remove boundary errors but should still
# accurately capture these larger systems. 
# ggplot(pwsid_counties_all %>% filter(pct_inter_sab < 50),
#        aes(x = pct_inter_sab)) +
#   geom_histogram()

# filtering for where >= 10% of the service area is within the county, 
# or where 10% of the county area is within the service area (to help more 
# accurately capture large SABs, such as  "ND0801154; ND0801502; ND1501653", where 
# Logan County was not being captured)
pwsid_counties_overcutoff <- pwsid_counties_all %>%
  filter(pct_inter_sab >= 10 | pct_inter_county >= 10) %>%
  mutate(county_state = paste0(county_name, ", ", state_name)) %>%
  group_by(pwsid) %>%
  summarize(county_served = paste(unique(county_state), collapse = "; "))

# Investigate missing values - ack, when the original cutoff was 
# 20%, these are all huge systems that span multiple counties, such that 
# every county captures < 20% of the system... I rolled this back to 10%, 
# and this is now an empty dataframe - good! 
# missing_from_county <- anti_join(epa_sabs, pwsid_counties_overcutoff, 
#                                  by = "pwsid")
# mapview(missing_from_county) + 
#   mapview(us_counties_simp)

# note: I also performed some spot checks against the county_served in 
# the CNT app, and did not find any discrepancies to note. The one I found here
# is a situation where only 0.3% of the service area overlapped 
# mapview(epa_sabs %>% filter(pwsid == "010106001")) +
#   mapview(us_counties_simp)

# this is another interesting one, where this service area overlaps with 6 
# counties
# mapview(epa_sabs %>% filter(pwsid == "ND0501057; ND0501127; ND4001153; ND3501476")) +
#   mapview(us_counties_simp)

# are there any NAs? 
any(is.na(pwsid_counties_overcutoff$county_served))
# False :) yeehaw

# do we have all of the SABs? 
nrow(epa_sabs) == nrow(pwsid_counties_overcutoff)
# True! woo!


# add this filtered dataset to s3 in a clean bucket
tmp <- tempfile()
write.csv(pwsid_counties_overcutoff, paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = clean_s3_link,
  bucket = "tech-team-data",
  multipart = T
)

# TODO - I'm pushing this to staged so our frontend folks can have it for their 
# ETL pipeline, but THIS SHOULD BE DELETED once we integrate this new 
# dataset in our pipeline (see specific steps at the top of this file).
# tmp <- tempfile()
# write.csv(pwsid_counties_overcutoff, paste0(tmp, ".csv"), row.names = F)
# on.exit(unlink(tmp))
# put_object(
#   file = paste0(tmp, ".csv"),
#   object = "s3://tech-team-data/national-dw-tool/test-staged/sabs_pwsid_county.csv",
#   bucket = "tech-team-data",
#   multipart = T,
#   acl = "public-read"
# )

# making sure everything looks good: 
test <- aws.s3::s3read_using(read.csv, 
                             object = "s3://tech-team-data/national-dw-tool/test-staged/sabs_pwsid_county.csv")
