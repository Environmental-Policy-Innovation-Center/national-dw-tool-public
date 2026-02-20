################################################################################
# Relating municipal finance data to EPA SABs
################################################################################
library(aws.s3)
library(googlesheets4)
library(readxl)
library(tidyverse)
library(sf)
library(mapview)
library(tidycensus)
library(janitor)

options(scipen = 99999999)
################################################################################
# applying our methods, with edits, to all of the financial datasets: 
################################################################################
# reading in national water system: 
national_water_system <- aws.s3::s3read_using(readRDS, 
                                              object = "s3://tech-team-data/national-dw-tool/clean/national/national_water_system_archived_09252025.RData")
# reading in national socioeconomic: 
national_socioeconomic <- s3read_using(readRDS,
                                       object = "s3://tech-team-data/national-dw-tool/clean/national/national_socioeconomic_archived_09252025.RData")
# grabbin sabs: 
epa_sabs <- national_water_system$epa_sabs
# sdwis info: 
sdwis_viols <- national_water_system$sdwis_viols
# xwalk
xwalk <- national_socioeconomic$epa_sabs_xwalk

################################################################################
# working on the municipal data
################################################################################
# grabbing muni data:
# (19639, 593)
muni_finance_all <- read.csv("./data/financial_datasets/raw_datasets/MunicipalData.csv") 
muni_tidy <- muni_finance_all %>%
  janitor::clean_names() %>%
  # there are two different state codes, but from doing spot checks, this is 
  # the right one 
  mutate(fips_code_state = as.character(fips_code_state), 
         fips_place = as.character(fips_place)) %>%
  # there's definitely a better way to do this, but this works for now: 
  mutate(fips_code_state_tidy = case_when(nchar(fips_code_state) == 1 ~ paste0("0", fips_code_state),
                                          TRUE ~ fips_code_state), 
         fips_place_tidy = case_when(nchar(fips_place) == 4 ~ paste0("0", fips_place), 
                                     nchar(fips_place) == 3 ~ paste0("00", fips_place), 
                                     nchar(fips_place) == 2 ~ paste0("000", fips_place), 
                                     TRUE ~ fips_place)) %>%
  # census place code = fips_code_state + fips_place codes
  mutate(census_place = paste0(fips_code_state_tidy, fips_place_tidy)) %>%
  # Manu said to filter to just 2022 data using this field 
  filter(year4 == 2022) %>%
  relocate(census_place, .after = fips_place)


# grab census place data and geometries from the census: 
# (31894, 5)
census_places <- tidycensus::get_acs(
  geography = "place",
  # just grabbing houses and populations 
  variables = c(total_pop = "B01003_001", 
                total_houses = "B17017_001"), 
  state =  unique(muni_tidy$fips_code_state),
  # NOTE - currently grabbing 2022 data to match the vintage 
  year = 2022,
  geometry = TRUE) %>%
  # we could keep margins of error if needed 
  select(-moe) %>%
  pivot_wider(., names_from = "variable", values_from = "estimate", 
              names_glue = "{variable}_{.value}") 


# filter municipal data to census places that we have boundaries for, 
# and fix a duplicated name
# there are only two muni_places that don't match to a geoid
# (19351, 596)
muni_places_w_data <- muni_tidy %>%
  filter(census_place %in% census_places$GEOID) %>%
  rename(muni_name = name)

# investigating the 18 that dropped: 
no_census_data <- muni_tidy %>%
  filter(!(census_place %in% census_places$GEOID))
# hm, yeah I'm not sure why these got dropped. they exist when I look them up, 
# but maybe the census geographies aren't available?

# merge the census place data and municipal data by geoid to get
# geometries 
muni_places_sf <- merge(census_places, 
                        muni_places_w_data, 
                        by.x = "GEOID", 
                        by.y = "census_place") %>%
  st_as_sf() %>%
  # grab area of the census place 
  mutate(census_place_area_m2 = as.numeric(st_area(.)))

################################################################################
# tidy EPA SABs data
################################################################################
# transform EPA sabs before intersection and grab original SAB areas:
# there are 16,967 locally owned systems that aren't schools that serve at least 
# 500 people (based on SDWIS)
epa_sabs_transform <- epa_sabs %>%
  # grab those sdwis viols
  left_join(sdwis_viols, by = "pwsid") %>%
  # make sure we have the same crs before performing a merge
  st_transform(., crs = st_crs(muni_places_sf)) %>%
  # calculate area in m2:
  mutate(sab_area_m2 = as.numeric(st_area(.))) %>%
  # we only want locally owned systems  - REMOVING THIS AS OF SEPT 19
  # filter(owner_type == "Local") %>%
  # we don't want any schools or daycares: 
  filter(is_school_or_daycare_ind == "N") %>%
  # want only larger systems
  filter(population_served_count >= 500)

################################################################################
# intersection - SABS & census places w/ municipal finance data 
################################################################################
sf_use_s2(F)
# running this intersection in R 
muni_sab_intersection <- st_intersection(epa_sabs_transform,
                                           muni_places_sf) %>%
  # grab the area of the intersection for weights
  mutate(sab_place_intersection_area_m2 = as.numeric(st_area(.))) %>%
  # reshuffle the areas to make them easier to see
  relocate(census_place_area_m2, .after = sab_area_m2) %>%
  relocate(sab_place_intersection_area_m2, .after = census_place_area_m2)
sf_use_s2(T)

# so we have 24,478 observations 
# this contains 14,077 unique water systems (out of a possible 16,967 systems) 
# that intersect with 13,733 census places (out of a possible 19,351 places)
length(unique(muni_sab_intersection$pwsid))
length(unique(muni_sab_intersection$GEOID))
length(unique(muni_places_sf$GEOID))

# looking at percent SAB overlap: 
percent_sab_overlap <- muni_sab_intersection %>%
  mutate(pct_sab_overlap = 100*(sab_place_intersection_area_m2/sab_area_m2), 
         pct_place_overlap = 100*(sab_place_intersection_area_m2/census_place_area_m2)) %>%
  relocate(pct_sab_overlap:pct_place_overlap, .after = "pwsid") %>%
  rename(census_place_fips_code = GEOID)

################################################################################
# Filtering 
################################################################################
# making a quick histogram or density plot:  
percent_sab_overlap_df <- as.data.frame(percent_sab_overlap) %>%
  select(-geometry)
ggplot(percent_sab_overlap_df, aes(x = pct_sab_overlap)) + 
  geom_histogram(binwidth = 8) + 
  geom_vline(xintercept = 30, color = "blue", lty = "dashed") + 
  theme_bw() +
  labs(x = "% SAB Overlapping with Census Place", y = "# Water Systems")

# filtering our sf object of locally owned systems with > 50% overlap 
# this reduces the data to 9,507 observations; with 40% overlap, this is 
# 10,116 observations; with 30% overlap, this is 10,668 observations
local_systems_over30_overlap <- percent_sab_overlap %>%
  filter(pct_sab_overlap > 30)

length(unique(local_systems_over30_overlap$pwsid)) # 10,547 water systems out of a possible 16,967
length(unique(local_systems_over30_overlap$census_place_fips_code)) # overlaps with 10,237 census places
length(unique(muni_places_sf$GEOID)) # out of a possible 19,351 census places 

# joining & exporting: (10,668 ob wth 833 variables)
muni_finance_30pct <- local_systems_over30_overlap %>% 
  as.data.frame() %>%
  select(-geometry) %>%
  left_join(xwalk, by = "pwsid") %>%
  relocate(crosswalk_state:tier_crosswalk, .after = open_health_viol) %>%
  relocate(sab_area_m2:total_houses_estimate, .after = pwsid) %>%
  rename(census_place_name = NAME, 
         census_place_pop = total_pop_estimate, 
         census_place_houses = total_houses_estimate) %>%
  select(-c(fips_code_state_tidy, fips_place_tidy)) %>%
  relocate(pct_sab_overlap:pct_place_overlap, .before = census_place_fips_code)

# write.csv(muni_finance_30pct, "./data/financial_datasets/merged_financial_data/all_muni_finance_30pct.csv")
# _all = has the owner_type == "Local" filter removed 


# SUMMARY - MUNICIPAL FINANCE: 
# this has locally owned systems (that are not schools or daycares and where 
# population as reported in SDWIs was >= 500) and where > 30% of the total sab 
# intersects with the census place boundary. Prior to this intersection, the 
# muni_finance data was filtered to only contain data for 2022. 

# after filtering EPA sabs data, there were 16,967 total water systems & 10,548 
# of these matched to the municipal finance database. There were 19,351 muni fincance
# census places (18 did not match to a known census place), and 10,237 of these 
# matched to a water system. 

################################################################################
# mapping the filtered data: 
################################################################################
sabs_muni <- epa_sabs %>% 
  filter(pwsid %in% local_systems_over30_overlap$pwsid) %>%
  select(pwsid, state)
muni_sab_overlap <- muni_places_sf %>%
  filter(GEOID %in% local_systems_over30_overlap$census_place_fips_code) %>%
  select(GEOID, fip_sid) 

local_systems_over30_overlap_mapping <- local_systems_over30_overlap %>% 
  select(census_place_fips_code, pwsid, 
         pct_sab_overlap, 
         pct_place_overlap) %>%
  mutate(pct_sab_overlap = round(pct_sab_overlap, 2), 
         pct_place_overlap = round(pct_place_overlap, 2))

# looking at the overlap - this contains the raw boundaries of sabs (sabs_muni)x 
# raw boundaries of census places (muni_sab_overlap)
mapview(sabs_muni) + 
  mapview(muni_sab_overlap, col.regions = "grey") 

# ok, this 30% actually looks pretty good

# mapview(local_systems_over50_overlap_mapping, zcol = "pct_sab_overlap") + 
#   mapview(muni_sab_overlap, col.regions = "grey") +
#   mapview(sabs_muni, col.regions = "black") 

# how many census places overlap with multiple water systems?
multiple_ws <- local_systems_over30_overlap_mapping %>%
  as.data.frame() %>%
  group_by(census_place_fips_code) %>%
  summarize(total = n())

# one of them overlaps with many of them: 
many_overlaps <- local_systems_over30_overlap_mapping %>%
  # ah - this is houston
  filter(census_place_fips_code == "4835000")

################################################################################
# mapping the original data without filters: 
################################################################################
# sabs_muni_all <- epa_sabs %>%
#   filter(pwsid %in% percent_sab_overlap$pwsid) %>%
#   select(pwsid, state)
# muni_sab_overlap_all <- muni_places_sf %>%
#   filter(GEOID %in% percent_sab_overlap$census_place_fips_code) %>%
#   select(GEOID, fip_sid) 
# 
# all_overlap_mapping <- percent_sab_overlap %>% 
#   select(census_place_fips_code, pwsid, 
#          pct_sab_overlap, 
#          pct_place_overlap) %>%
#   mutate(pct_sab_overlap = round(pct_sab_overlap, 2), 
#          pct_place_overlap = round(pct_place_overlap, 2))
# 
# mapview(all_overlap_mapping, 
#         zcol = "pct_sab_overlap") + 
#   mapview(muni_sab_overlap_all, col.regions = "grey") +
#   mapview(sabs_muni_all, col.regions = "black") 

################################################################################
# working on the township data
################################################################################
# has 328,962 rows, after filtering to 2022, this becomes 15,898 rows 
township_raw <- read.csv("./data/financial_datasets/raw_datasets/TownshipData.csv") %>%
  janitor::clean_names() 

# this contains both "towns" and "townships"
fin_township_tidy <- township_raw %>%
  filter(year4 == 2022) %>%
  mutate(fips_code_state = as.character(fips_code_state), 
         fips_place = as.character(fips_place)) %>%
  # there's definitely a better way to do this, but doing this for now: 
  mutate(fips_code_state_tidy = case_when(nchar(fips_code_state) == 1 ~ paste0("0", fips_code_state),
                                          TRUE ~ fips_code_state), 
         fips_place_tidy = case_when(nchar(fips_place) == 4 ~ paste0("0", fips_place), 
                                     nchar(fips_place) == 3 ~ paste0("00", fips_place), 
                                     nchar(fips_place) == 2 ~ paste0("000", fips_place), 
                                     TRUE ~ fips_place)) %>%
  # census place code = fips_code_state + fips_place codes
  mutate(census_place = paste0(fips_code_state_tidy, fips_place_tidy)) %>%
  relocate(census_place, .after = fips_place)

# (14286, 5)
township_df <- data.frame()
for(i in 1:length(unique(fin_township_tidy$fips_code_state))){
  state_i <- unique(fin_township_tidy$fips_code_state)[i]
  print(state_i)
  census_places_townships <- tidycensus::get_acs(
    geography = "county subdivision",
    # just grabbing houses and populations 
    variables = c(total_pop = "B01003_001", 
                  total_houses = "B17017_001"), 
    state = state_i,
    # NOTE - currently grabbing 2022 data to match the vintage 
    year = 2022,
    geometry = TRUE) %>%
    # we could keep margins of error if needed 
    select(-moe) %>%
    pivot_wider(., names_from = "variable", values_from = "estimate", 
                names_glue = "{variable}_{.value}") 
  township_df <<- rbind(township_df, census_places_townships)
}

# tracking east windsor: https://en.wikipedia.org/wiki/East_Windsor,_Connecticut
# fips code = 0924800
census_county_sub_tidy <- township_df %>%
  # need to extract the place code: 
  mutate(last_two_chars = substr(GEOID, nchar(GEOID) - 1, nchar(GEOID))) %>%
  # filter(last_two_chars == "00") %>%
  mutate(place_code_beginning = substr(GEOID, 1, 2), 
         place_code_end = substr(GEOID, 6, 10), 
         place_code = paste0(place_code_beginning, place_code_end)) 

# filter county data to census places that we have boundaries for, 
# and fix a duplicated name - now this matches 15892 out of 15898
fin_data_matched <- fin_township_tidy %>%
  filter(census_place %in% census_county_sub_tidy$place_code) %>%
  rename(county_name = name)

# checking out duplicates: 
# but then there are 278 duplicated ceneus place codes?
county_sub_code_dups <- census_county_sub_tidy %>% 
  filter(duplicated(place_code)) %>%
  # after this filter, now 199
  filter(!grepl("not defined", NAME))

# some of these are places that span multiple counties - this 
# returns 382 duplicated census_places 
duplicated_sub_code <- census_county_sub_tidy %>% 
  filter(place_code %in% county_sub_code_dups$place_code)

# ah okay - so they are separate geographies for the same "place" that span
# multiple counties.
mapview::mapview(county_sub_code_dups %>% filter(place_code == "5588150"))

# ... what about places?
# test <- fin_township_tidy %>%
#   filter(census_place %in% as.character(census_places$GEOID)) 

# update: okay so now I can take county subdivisions, extract the place
# code and that returns an almost complete merge, but there are duplicated place
# codes for towns that span multiple counties. I could union these? 

# okay cool can confirm the spot checks appear & the place codes match what 
# I found for the town in Wikipedia 
# test <- census_county_sub_tidy %>%
#   filter(grepl("Marlborough", NAME))

# okay so maybe group by and union the census_county_sub_tidy by place code?
# now we have 23,301
grouped_census_places <- census_county_sub_tidy %>%
  group_by(place_code) %>%
  summarize(total_subdiv = length(unique(GEOID)))

mapview::mapview(grouped_census_places %>% filter(place_code == "5588150"))
# okay cool! this is unioned now

# this results in 15,892 rows
merged_township_fin <- merge(fin_township_tidy, grouped_census_places, 
                             by.x = "census_place", by.y = "place_code") %>%
  st_as_sf() %>%
  mutate(census_place_area_m2 = st_area(.))
# flag that a town may span multiple counties, resulting in multiple census
# place codes

# alrightly now we merge with SABs
# transform EPA sabs before intersection and grab original SAB areas:
# there are 16,967 locally owned systems that aren't schools that serve at least 
# 500 people (based on SDWIS)
epa_sabs_transform <- epa_sabs %>%
  # grab those sdwis viols
  left_join(sdwis_viols, by = "pwsid") %>%
  # make sure we have the same crs before performing a merge
  st_transform(., crs = st_crs(merged_township_fin)) %>%
  # calculate area in m2:
  mutate(sab_area_m2 = as.numeric(st_area(.))) %>%
  # we only want locally owned systems 
  filter(owner_type == "Local") %>%
  # we don't want any schools or daycares: 
  filter(is_school_or_daycare_ind == "N") %>%
  # want only larger systems
  filter(population_served_count >= 500)


sf_use_s2(F)
# running this intersection in R 
town_sab_intersection <- st_intersection(epa_sabs_transform,
                                         merged_township_fin) %>%
  # grab the area of the intersection for weights
  mutate(sab_place_intersection_area_m2 = as.numeric(st_area(.))) %>%
  # reshuffle the areas to make them easier to see
  relocate(census_place_area_m2, .after = sab_area_m2) %>%
  relocate(sab_place_intersection_area_m2, .after = census_place_area_m2)
sf_use_s2(T)

# so we have 12,371 observations 
# this contains 5,947 unique water systems (out of a possible 16,967 systems) 
# that intersect with 7,522 census places (out of a possible 15,892 places)
length(unique(town_sab_intersection$pwsid))
length(unique(town_sab_intersection$census_place))
length(unique(merged_township_fin$census_place))

# looking at percent SAB overlap: 
town_percent_sab_overlap <- town_sab_intersection %>%
  mutate(pct_sab_overlap = 100*(sab_place_intersection_area_m2/sab_area_m2), 
         pct_place_overlap = 100*(sab_place_intersection_area_m2/census_place_area_m2)) %>%
  relocate(pct_sab_overlap:pct_place_overlap, .after = "pwsid") %>%
  rename(census_place_fips_code = census_place)

# making a quick histogram or density plot:  
town_percent_sab_overlap <- as.data.frame(town_percent_sab_overlap) %>%
  select(-geometry)
ggplot(town_percent_sab_overlap, aes(x = pct_sab_overlap)) + 
  geom_histogram(binwidth = 8) + 
  geom_vline(xintercept = 30, color = "blue", lty = "dashed") + 
  theme_bw() +
  labs(x = "% SAB Overlapping with Census Place", y = "# Water Systems")

# filtering our sf object of locally owned systems with > 50% overlap 
# this reduces the data to 9,507 observations; with 40% overlap, this is 
# 10,116 observations; with 30% overlap, this is 10,668 observations
local_systems_over30_overlap <- town_percent_sab_overlap %>%
  filter(pct_sab_overlap > 30)

length(unique(local_systems_over30_overlap$pwsid)) # 10,547 water systems out of a possible 16,967
length(unique(local_systems_over30_overlap$census_place_fips_code)) # overlaps with 10,237 census places
length(unique(merged_township_fin$census_place)) # out of a possible 19,351 census places 

# joining & exporting: (10,668 ob wth 833 variables)
town_finance_30pct <- local_systems_over30_overlap %>% 
  left_join(xwalk, by = "pwsid") %>%
  relocate(crosswalk_state:tier_crosswalk, .after = open_health_viol) %>%
  relocate(sab_area_m2:census_place_fips_code, .after = pwsid) %>%
  select(-c(fips_code_state_tidy, fips_place_tidy))

# mapping the raw data: 
intersecting_sabs <- epa_sabs_transform %>% 
  filter(pwsid %in% town_sab_intersection$pwsid)
intersecting_towns <- merged_township_fin %>% 
  filter(census_place %in% town_sab_intersection$census_place)
mapview(intersecting_towns) + 
  mapview(intersecting_sabs, col.regions = "red", alpha = 0.5)

# ugh this is so weird - the towns are basically little grids, but there might 
# be an entire cutout for a census place (?) where the SAB often is located

census_places
sf_use_s2(F)
# running this intersection in R 
test_sab_place_intersection <- st_intersection(epa_sabs_transform,
                                               census_places) 
sf_use_s2(T)
intersecting_places <- census_places %>% filter(GEOID %in% test_sab_place_intersection$GEOID)
mapview(intersecting_towns) + 
  # mapview(intersecting_sabs, col.regions = "red", alpha = 0.5) +
  mapview(intersecting_places, col.regions = "black")


# ah - ok. So since water systems serve populations, it seems like they would 
# overlap more frequently with census places & be in the "hole" of county 
# subdivision geometries. BUT - this varies based on the state. Chicago, 
# for example, all of the county subdivision geometries completely overlap
# with the census places (which we used to merge the muni finance data)

# given this discrepancy between states, I don't think merging this information 
# would really give us any other additional information compared to the original
# merge. The muni finance dataset would have geographies of similar shape/size
# to SABs, and would be more likely to cover populations that would be on a water system. 
# the county subdivision dataset is pretty grid-like and either already overlaps
# with census places (that were captured in the muni finance merge), or omits 
# that particular area (which is probably where a census place is & where a 
# population center served by a water system is)



##### old stuff I tried #######
# checking places as well
# census_places <- tidycensus::get_acs(
#   geography = "place",
#   # just grabbing houses and populations 
#   variables = c(total_pop = "B01003_001", 
#                 total_houses = "B17017_001"), 
#   state =  unique(township_tidy$fips_code_state),
#   # NOTE - currently grabbing 2022 data to match the vintage 
#   year = 2022,
#   geometry = TRUE) %>%
#   # we could keep margins of error if needed 
#   select(-moe) %>%
#   pivot_wider(., names_from = "variable", values_from = "estimate", 
#               names_glue = "{variable}_{.value}") 
# # data framing: 
# census_places_df <- census_places %>% as.data.frame() %>% select(-geometry)
# 
# library(tigris)
# lookup_code("1805716")
# test <- places(state = "09", year = 2022)
# library(fips)

# ugh omg it's in 2020 but not 2022 whattttt
# ct_subdivs <- tigris::county_subdivisions("09", year = 2020) 
# ct_subdivs %>%
#   filter(grepl("Marlborough", NAME))

# so - I'm getting like a 0% match for census places, and only a partial match 
# (1630 out of 15898) for census county_subdivison 2022 boundaries, specifically 
# I when I extract the census place code (the subdivision gets inserted in the 
# fips between the state and place code for some reason). Some boundaries (like
# Marlborough, CT, only match if census 2020 boudaries are used)

# maybe I need 2020?
# township_df_2020 <- data.frame()
# for(i in 1:length(unique(township_tidy$fips_code_state))){
#   state_i <- unique(township_tidy$fips_code_state)[i]
#   print(state_i)
#   census_places_townships <- tidycensus::get_acs(
#     geography = "county subdivision",
#     # just grabbing houses and populations 
#     variables = c(total_pop = "B01003_001", 
#                   total_houses = "B17017_001"), 
#     state = state_i,
#     # NOTE - currently grabbing 2022 data to match the vintage 
#     year = 2020,
#     geometry = TRUE) %>%
#     # we could keep margins of error if needed 
#     select(-moe) %>%
#     pivot_wider(., names_from = "variable", values_from = "estimate", 
#                 names_glue = "{variable}_{.value}") 
#   township_df_2020 <<- rbind(township_df_2020, census_places_townships)
# }
# 
# census_place_townships_df_2020 <- township_df_2020 %>%
#   as.data.frame() %>%
#   select(-geometry) %>%
#   # ned to extract the place code: 
#   mutate(last_two_chars = substr(GEOID, nchar(GEOID) - 1, nchar(GEOID))) %>%
#   # filter(last_two_chars == "00") %>%
#   mutate(place_code_beginning = substr(GEOID, 1, 2), 
#          place_code_end = substr(GEOID, 6, 10), 
#          place_code = paste0(place_code_beginning, place_code_end)) 
# 
# # filter county data to census places that we have boundaries for, 
# # and fix a duplicated name
# township_tidy_w_data <- township_tidy %>%
#   filter(census_place %in% census_place_townships_df_2020$place_code) %>%
#   rename(county_name = name)

################################################################################
# working on the county data
################################################################################
# there are 117,328 records, after filtering for 2022 data, we have 3,029 records 
county_raw <- read.csv("./data/financial_datasets/raw_datasets/CountyData.csv") %>%
  janitor::clean_names()
county_tidy <- county_raw %>%
  # Manu said to filter to just 2022 data using this field 
  filter(year4 == 2022)

# TODO - the fips_code_state != state_code .... why? seems like 
# state_code is the correct one based off a quick search with Franklin County,
county_tidy <- county_finance_all %>%
  mutate(fips_code_state = as.character(fips_code_state), 
         fips_place = as.character(fips_place)) %>%
  # there's definitely a better way to do this, but doing this for now: 
  mutate(fips_code_state_tidy = case_when(nchar(fips_code_state) == 1 ~ paste0("0", fips_code_state),
                                          TRUE ~ fips_code_state), 
         fips_place_tidy = case_when(nchar(fips_place) == 4 ~ paste0("0", fips_place), 
                                     nchar(fips_place) == 3 ~ paste0("00", fips_place), 
                                     nchar(fips_place) == 2 ~ paste0("000", fips_place), 
                                     TRUE ~ fips_place)) %>%
  # census place code = fips_code_state + fips_place codes
  mutate(census_place = paste0(fips_code_state_tidy, fips_place_tidy)) %>%
  relocate(census_place, .after = fips_place)


# grab census place data and geometries from the census: 
# (31642, 5)
census_places_county <- tidycensus::get_acs(
  geography = "county",
  # just grabbing houses and populations 
  variables = c(total_pop = "B01003_001", 
                total_houses = "B17017_001"), 
  state =  unique(county_tidy$fips_code_state),
  # NOTE - currently grabbing 2022 data to match the vintage 
  year = 2022,
  geometry = TRUE) %>%
  # we could keep margins of error if needed 
  select(-moe) %>%
  pivot_wider(., names_from = "variable", values_from = "estimate", 
              names_glue = "{variable}_{.value}") 


# filter county data to census places that we have boundaries for, 
# and fix a duplicated name
# ()
county_tidy_w_data <- county_tidy %>%
  filter(census_place %in% census_places_county$GEOID) %>%
  rename(county_name = name)

tx <- census_places_county %>%
  filter(grepl("Texas", NAME))

# investigating the 18 that dropped: 
# no_census_data <- muni_tidy %>%
#   filter(!(census_place %in% census_places$GEOID))
# hm, yeah I'm not sure why these got dropped. they exist when I look them up, 
# but maybe the census geographies aren't available?

# merge the census place data and municipal data by geoid to get
# geometries 
muni_places_sf <- merge(census_places, 
                        muni_places_w_data, 
                        by.x = "GEOID", 
                        by.y = "census_place") %>%
  st_as_sf() %>%
  # grab area of the census place 
  mutate(census_place_area_m2 = as.numeric(st_area(.)))

################################################################################
# what the heck is going on 
################################################################################



# okay so muni_tidy has towns, cities, buroughs, etc. 
muni_tidy %>%
  select(gov_sid:year_pop) %>%
  filter(grepl("HEBRON", name))
#   
# # townships just has towns
# township_tidy %>% 
#   select(gov_sid:year_pop) %>%
#   filter(grepl("HEBRON", name))

# is there complete overlap?
# muni_select <- muni_tidy %>% select(gov_sid:year_pop)
# township_select <- township_tidy %>% 
#   # select(gov_sid:year_pop) %>%
#   mutate(dup_flag = case_when(census_place %in% muni_select$census_place ~ "yes in muni", 
#                               TRUE ~ "no"))
# # 1265 of these out of 15,898 are already in the muni_finance database 
# sum(township_select$dup_flag == "yes in muni")
# township_overlap <- township_select %>% 
#   filter(dup_flag == "yes in muni")
# # is this information the exact same?
# muni_overlap <- muni_tidy %>% 
#   filter(census_place %in% census_place$name)
# 
# 
# 
# 
# # and the county data has ... counties, but for some reason still has fips_place
# # codes 
# head(county_finance_all %>% select(gov_sid:year_pop))



########### old code from piloting methods
# # grab data: ##################################################################
# # downloaded from: https://docs.google.com/spreadsheets/d/1KzsjBDnjmoy3htRSw3TtyZsFoKDzFEQr/edit?usp=drive_link&ouid=116701638044266002378&rtpof=true&sd=true
# # (939, 592)
# muni_finance <- read_xlsx("./data/municipalfinance2022.xlsx") %>%
#   select(-...1) %>%
#   janitor::clean_names()
# 
# # grabbin sabs: 
# epa_sabs <- aws.s3::s3read_using(st_read, 
#                                  object = "s3://tech-team-data/national-dw-tool/raw/national/water-system/epa_sabs.geojson") 
# 
# # filtering for only locally owned systems: 
# pwsid_water_systems <- aws.s3::s3read_using(read.csv, 
#                                             object = "s3://tech-team-data/national-dw-tool/raw/national/water-system/sdwa_pub_water_systems.csv")
# ################################################################################
# # grabbing census place codes:
# ################################################################################
# # (939, 593)
# muni_places <- muni_finance %>%
#   # census place code = fips_code_state + fips_place codes
#   mutate(census_place = paste0(fips_code_state, fips_place))
# 
# 
# # grab census place data and geometries from the census: 
# # (19032, 6)
# census_places <- tidycensus::get_acs(
#   geography = "place",
#   # just grabbing houses and populations 
#   variables = c(total_pop = "B01003_001", 
#                 total_houses = "B17017_001"), 
#   state =  unique(muni_places$fips_code_state),
#   # NOTE - currently grabbing 2022 data, as I think that's the correct vintage
#   # for these data (check)
#   year = 2022,
#   geometry = TRUE) %>%
#   # we could keep margins of error if needed 
#   select(-moe) %>%
#   pivot_wider(., names_from = "variable", values_from = "estimate", 
#               names_glue = "{variable}_{.value}") 
# 
# 
# # filter municipal data to census places that we have boundaries for, 
# # and fix a duplicated name
# # there are only two muni_places that don't match to a geoid
# # (937, 593)
# muni_places_w_data <- muni_places %>%
#   filter(census_place %in% census_places$GEOID) %>%
#   rename(muni_name = name)
# 
# # investigating the two that dropped: 
# no_census_data <- muni_places %>%
#   filter(!(census_place %in% census_places$GEOID))
# 
# 
# # merge the census place data and municipal data by geoid to get
# # geometries 
# muni_places_sf <- merge(census_places, 
#                         muni_places_w_data, 
#                         by.x = "GEOID", 
#                         by.y = "census_place") %>%
#   st_as_sf() %>%
#   # grab area of the census place 
#   mutate(census_place_area_m2 = as.numeric(st_area(.)))
# 
# ################################################################################
# # tidy EPA SABs data
# ################################################################################
# # transform EPA sabs before intersection and grab original SAB areas:
# epa_sabs_transform <- epa_sabs %>%
#   # make sure we have the same crs before performing a merge
#   st_transform(., crs = st_crs(muni_places_sf)) %>%
#   # calculate area in m2:
#   mutate(sab_area_m2 = as.numeric(st_area(.))) %>%
#   select(pwsid, sab_area_m2) 
# 
# 
# ################################################################################
# # intersection - SABS & census places w/ municipal finance data 
# ################################################################################
# # running this intersection in R takes quite a bit of time 
# # muni_sab_intersection <- st_intersection(epa_sabs_transform,
# #                                            muni_places_sf) %>%
# #   # grab the area of the intersection for weights
# #   mutate(sab_place_intersection_area_m2 = as.numeric(st_area(.))) %>%
# #   # reshuffle the areas to make them easier to see
# #   relocate(census_place_area_m2, .after = sab_area_m2) %>%
# #   relocate(sab_place_intersection_area_m2, .after = census_place_area_m2)
# 
# # writing locally and performing the intersection in QGIS (faster)
# # st_write(epa_sabs_transform, "./data/epa_sabs_transform.geojson")
# # st_write(muni_places_sf, "./data/muni_places_sf.geojson")
# 
# # reading intersecting dataset back here: 
# muni_sab_intersection <- st_read("./data/muni_sab_intersection.geojson")%>%
#   # make sure we have the same crs before performing a merge
#   st_transform(., crs = st_crs(muni_places_sf))%>%
#   # grab the area of the intersection for weights 
#   mutate(sab_place_intersection_area_m2 = as.numeric(st_area(.))) %>%
#   # reshuffle the areas to make them easier to see 
#   relocate(pwsid:sab_area_m2, .before = census_place_area_m2) %>%
#   relocate(sab_place_intersection_area_m2, .after = census_place_area_m2)
# 
# 
# # checking the intersection: 
# length(unique(muni_sab_intersection$GEOID)) # matched to 846 out of 939 places (some don't overlap with SABs)
# length(unique(muni_sab_intersection$pwsid)) # 1,286 water systems match to 846 census places 
# 
# 
# # looking at percent SAB overlap: 
# percent_sab_overlap <- muni_sab_intersection %>%
#   select(fip_sid, GEOID, NAME, pwsid, sab_area_m2, census_place_area_m2, 
#          sab_place_intersection_area_m2) %>%
#   mutate(pct_sab_overlap = 100*(sab_place_intersection_area_m2/sab_area_m2), 
#          pct_place_overlap = 100*(sab_place_intersection_area_m2/census_place_area_m2)) %>%
#   relocate(pct_sab_overlap:pct_place_overlap, .before = "geometry") %>%
#   rename(census_place_fips_code = GEOID)
# 
# 
# 
# # saving some files for Manu:
# # raw_intersection <- muni_sab_intersection %>% as.data.frame() %>%
# #   select(-geometry)
# # st_write(muni_sab_intersection, "./data/raw_muni_sab_intersection.geojson")
# # 
# # muni_sab_summary <- percent_sab_overlap %>%
# #   as.data.frame() %>%
# #   select(-geometry)
# # write.csv(muni_sab_summary, "./data/summary_muni_sab_intersection.csv")
# 
# # also wanting to combine w/ ACS data, and SDWIS: 
# national_socioeconomic <- s3read_using(readRDS,
#                                        object = "s3://tech-team-data/national-dw-tool/clean/national/national_socioeconomic.RData")
# # acs data: 
# sabs_xwalk <- national_socioeconomic$epa_sabs_xwalk
# # sdwis info: 
# national_water_system <- s3read_using(readRDS,
#                                       object = "s3://tech-team-data/national-dw-tool/clean/national/national_water_system.RData")
# sabs_viols <- national_water_system$sdwis_viols
# 
# muni_merged <- merge(raw_intersection, sabs_viols, by = "pwsid", all.x = T)
# muni_merged <- merge(muni_merged, sabs_xwalk, by = "pwsid", all.x = T)
# # write.csv(muni_merged, "./data/muni_pwsid_acs_sdwis.csv")
# ################################################################################
# # Filtering 
# ################################################################################
# # making a quick histogram or density plot:  
# percent_sab_overlap_df <- as.data.frame(percent_sab_overlap) %>%
#   select(-geometry)
# ggplot(percent_sab_overlap_df, aes(x = pct_sab_overlap)) + 
#   geom_density() + 
#   geom_vline(xintercept = 50, color = "blue", lty = "dashed") + 
#   theme_bw() +
#   labs(x = "% SAB Overlapping with Census Place")
# 
# 
# # filtering for locally owned systems:  
# sdwis_ws_info_tidy <- pwsid_water_systems %>%
#   filter(pwsid %in% epa_sabs$pwsid) %>%
#   filter(owner_type_code == "L")
# 
# 
# # filtering our sf object of locally owned systems with > 50% overlap 
# # this reduces the data to 551 observations 
# local_systems_over50_overlap <- percent_sab_overlap %>%
#   filter(pwsid %in% sdwis_ws_info_tidy$pwsid) %>%
#   filter(pct_sab_overlap > 50)
# 
# ################################################################################
# # mapping the filtered data: 
# ################################################################################
# sabs_muni <- epa_sabs %>% filter(pwsid %in% 
#                                    local_systems_over50_overlap$pwsid) %>%
#   select(pwsid, state)
# muni_sab_overlap <- muni_places_sf %>%
#   filter(GEOID %in% local_systems_over50_overlap$GEOID) %>%
#   select(GEOID, fip_sid) 
# 
# local_systems_over50_overlap_mapping <- local_systems_over50_overlap %>% 
#   select(GEOID, pwsid, 
#          pct_sab_overlap, 
#          pct_place_overlap) %>%
#   mutate(pct_sab_overlap = round(pct_sab_overlap, 2), 
#          pct_place_overlap = round(pct_place_overlap, 2))
# 
# # looking at the overlap: 
# mapview(sabs_muni) + 
#   mapview(muni_sab_overlap, col.regions = "grey") 
# 
# mapview(local_systems_over50_overlap_mapping, zcol = "pct_sab_overlap") + 
#   mapview(muni_sab_overlap, col.regions = "grey") +
#   mapview(sabs_muni, col.regions = "black") 
# 
# # how many census places overlap with multiple water systems?
# multiple_ws <- local_systems_over50_overlap %>%
#   as.data.frame() %>%
#   group_by(GEOID) %>%
#   summarize(total = n())
# 
# 
# ################################################################################
# # mapping the original data: 
# ################################################################################
# sabs_muni_all <- epa_sabs %>% filter(pwsid %in% 
#                                        percent_sab_overlap$pwsid) %>%
#   select(pwsid, state)
# muni_sab_overlap_all <- muni_places_sf %>%
#   filter(GEOID %in% percent_sab_overlap$GEOID) %>%
#   select(GEOID, fip_sid) 
# 
# all_overlap_mapping <- percent_sab_overlap %>% 
#   select(GEOID, pwsid, 
#          pct_sab_overlap, 
#          pct_place_overlap) %>%
#   mutate(pct_sab_overlap = round(pct_sab_overlap, 2), 
#          pct_place_overlap = round(pct_place_overlap, 2))
# mapview(all_overlap_mapping, 
#         zcol = "pct_sab_overlap") + 
#   mapview(muni_sab_overlap_all, col.regions = "grey") +
#   mapview(sabs_muni_all, col.regions = "black") 
# 
# ################################################################################
# # Exporting summary csv file
# ################################################################################
# percent_sab_overlap_df <- percent_sab_overlap %>%
#   filter(pwsid %in% sdwis_ws_info_tidy$pwsid) %>%
#   as.data.frame() %>%
#   select(-geometry)
# # write.csv(percent_sab_overlap_df, "./data/muni_finance_sab_overlap.csv")
# 
# head(muni_finance)
# any(duplicated(muni_finance$fip_sid))
# 
# 
# # merging PWSIDSprivate with violation & ACS data ##############################
# # reading in national water system: 
# national_water_system <- s3read_using(readRDS,
#                                       object = "s3://tech-team-data/national-dw-tool/clean/national/national_water_system.RData")
# # reading in national socioeconomic: 
# national_socioeconomic <- s3read_using(readRDS,
#                                        object = "s3://tech-team-data/national-dw-tool/clean/national/national_socioeconomic.RData")
# 
# viols <- national_water_system$sdwis_viols
# xwalk <- national_socioeconomic$epa_sabs_xwalk
# 
# # reading in pwsidsprivate.csv - downloaded from email and exported as a csv 
# pwsids_private <- read.csv("./data/PWSIDSprivate.csv") %>%
#   select(PWSID) %>%
#   janitor::clean_names()
# 
# priv_xwalk <- merge(pwsids_private, xwalk, by = "pwsid", all.x = T)
# priv_final <- merge(priv_xwalk, viols, by = "pwsid", all.x = T)
# # write.csv(priv_final, "./data/et_PWSIDSprivate.csv")
# 
# 
# # investigating NAs: 
# na_pwsids <- priv_final %>% filter(is.na(crosswalk_state))
# 
# # for filtering for CWS: 
# # epa_sabs_pwsids <- aws.s3::s3read_using(read.csv, 
# #                                         object = "s3://tech-team-data/national-dw-tool/raw/national/water-system/sabs_pwsid_names.csv")
# # merge(na_pwsids, epa_sabs_pwsids, by = "pwsid")
# # ^ lol - can confirm this is empty 


