###############################################################################
# NPDES Quarterly Worker
###############################################################################

# libraries needed: 
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(sf)
library(tigris)
library(units)
# library(nhdplusTools)
# currently using the dev version, which has the resolved huc12 issue
remotes::install_github("DOI-USGS/nhdplusTools", force = TRUE)
library(nhdplusTools)

# no scientific notation 
options(scipen = 999)
# let tigris use the cache 
options(tigris_use_cache = TRUE)
options(timeout = 600) # this should bump to 10 mins 

# make sure to specify the correct bucket region for IAM role: 
Sys.setenv("AWS_DEFAULT_REGION" = 'us-east-1')

# for logs: 
print("I'm running!")

# pulling in task manager for updating relevant sections: 
task_manager <- aws.s3::s3read_using(read.csv, 
                                     object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv")%>%
  mutate(across(everything(), ~ as.character(.)))

# for some of the coding bits: 
dataset_i <- "npdes_permits"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/national/environmental/active_npdes.geojson"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/national/huc12_active_npdes.csv"

# NPDES permits ###############################################################
print("on: environmental datasets - NPDES permits")

# grab permit numbers from here: https://echo.epa.gov/files/echodownloads/npdes_outfalls_layer.zip
# downloading to temporary directory: 
file_loc <- tempdir()
# data are refreshed quarterly 
download.file("https://echo.epa.gov/files/echodownloads/npdes_outfalls_layer.zip", 
              destfile = paste0(file_loc, ".zip"))
unzip(zipfile = paste0(file_loc, ".zip"), exdir = file_loc) 
file.remove(paste0(file_loc, ".zip"))

# reading data:
npdes_permits <- read.csv(paste0(file_loc, "/npdes_outfalls_layer.csv"))

# data dictionary: https://echo.epa.gov/tools/data-downloads/icis-npdes-discharge-points-download-summary
active_npdes_permits_tidy <- npdes_permits %>%
  janitor::clean_names() %>%
  # "Active facilities are those currently in operation (indicated by any status 
  # code except NON or TRM)."
  filter(permit_status_code != "NON") %>%
  filter(permit_status_code != "TRM")

# grabbing active permit ids: 
active_npdes_ids_sf <- active_npdes_permits_tidy %>%
  select(external_permit_nmbr, permit_name, permit_effective_date, 
         permit_expiration_date, 
         permit_status_desc, 
         facility_type_desc, 
         # the permit feature number represents the unique outfall 
         # or pipe of interest 
         permit_type_desc, sic_descriptions, naics_codes, 
         major_minor_flag, 
         perm_feature_nmbr,
         cwa_current_status, 
         cwp_current_snc_status, cwp_current_viol,
         latitude83, longitude83) %>%
  st_as_sf(., coords = c("longitude83", "latitude83"), crs = "NAD83") %>%
  # FILTERING permit status for those that are effective, admin continued 
  # (applied for renewal), or pending. Other categories (such as expired), were 
  # basically old permits that had expired and the facility applied for 
  # a new permit - this still has violation data and could skew results 
  # (even tho it is flagged as active)
  filter(permit_status_desc %in% c("Effective", "Admin Continued", "Pending"))%>%
  # transforming to planar CRS for intersecting 
  st_transform(., crs = 5070)

# mapping to make sure nothing weird happened: 
# mapview::mapview(active_npdes_ids_sf)
# um wow quite a few with geometries that don't exist in the U.S. 
# investigating a few of these: 
# active_npdes_permits_tidy %>%
#   # this exists in Canada and the latitude is 64 degrees 
#   filter(external_permit_nmbr == "TX0135569")

print("Running First Intersection - Based on U.S. Boundaries") ################
sf_use_s2(T)
# filtering npdes permits for those that exist in the U.S. - to help make our 
# intersection function faster 
# this is based off the definition of waters of the US, which we know is always 
# a moving target. We're also intersecting this to HUC12s, which are tied to 
# an AREA OF LAND and therefore would remove most offshore permits
# source: https://dep.wv.gov/wwe/getinvolved/sos/documents/basins/hucprimer.pdf
buffer_distance_meters <- set_units(50, mi) %>% set_units(m)
# setting distance buffer to 50 miles offshore 
states_union <- nation() %>%
  st_buffer(., dist = buffer_distance_meters)%>%
  # transforming to planar CRS for intersecting 
  st_transform(., crs = 5070)

# testing - based on this mapview, I think it sufficiently would capture 
# relatively nearshore permits - this is just the first filter. 
# la_npdes <- active_npdes_ids_sf %>% filter(grepl("^LA", external_permit_nmbr))
# mapview::mapview(states_union) +
#   mapview::mapview(la_npdes)

# running an intersection on buffered states:
us_active_npdes_sf <- active_npdes_ids_sf[st_within(active_npdes_ids_sf, 
                                                    states_union, sparse = F),] %>%
  mutate(last_epic_run_date = Sys.Date()) 
# this only removed like 2,000 
# mapview::mapview(us_active_npdes_sf)

print("Running Second Intersection - Relating Data to HUC12s")################## 
sf_use_s2(F)
# grabbing states to loop through: 
states <- states() %>% 
  arrange(NAME)
# npdes permits: 
npdes_huc12 <- us_active_npdes_sf[0,] %>%
  mutate(across(-geometry, as.character))
for(i in 1:nrow(states)){
  state_i <- states[i,]
  message("On state ", state_i$NAME, ", ", i, ":", " out of ", nrow(states))
  # grab huc12 boundaries for the state: 
  state_i_huc_geom <- get_huc(states[i,], type = "huc12")
  # transform crs before intersecting: 
  tidy_state_i <- state_i_huc_geom %>%
    mutate(gnis_id = as.integer(gnis_id)) %>%
    st_transform(., crs = st_crs(us_active_npdes_sf))
  # run intersection of usts and the state HUC12s
  npdes_intersection <- st_intersection(us_active_npdes_sf, tidy_state_i) %>%
    mutate(across(-geometry, as.character))
  # add this to global environment 
  npdes_huc12 <- bind_rows(npdes_huc12, npdes_intersection)
}

# should be 396,407 rows --> 396,338 (why are we missing 69?)
npdes_huc12_unique <- npdes_huc12 %>%
  unique()
# st_write(npdes_huc12_unique, "./data/npdes_huc12.geojson")
# npdes_huc12_unique <- st_read("./data/npdes_huc12.geojson")
row.names(npdes_huc12_unique) <- NULL

# NOTES ABOUT THIS DATA: 
# - offshore permits (37 of them to be exact), didn't match to a huc12
# - there are some permits at the nexus of multiple HUC12s MTR108729

# # checking out points that didn't match to a huc12
# test_df <- npdes_huc12_unique %>%
#   mutate(permit_feature = paste0(external_permit_nmbr, " - ", perm_feature_nmbr))
# missing <- us_active_npdes_sf %>%
#   mutate(permit_feature = paste0(external_permit_nmbr, " - ", perm_feature_nmbr)) %>%
#   filter(!(permit_feature %in% test_df$permit_feature))
# mapview::mapview(missing) # ah okay, all 37 of these are offshore activities, so they
# # didn't match to a huc polygon but were included after the buffer 
# 
# # investigating situations where the permit and feature number got duplicated 
# # in the final output - there are only ~5, and these seem like database 
# errors. 
# npdes_huc12_unique_df <- npdes_huc12_unique %>%
#   as.data.frame() %>%
#   mutate(permit_feature = paste0(external_permit_nmbr, " - ", perm_feature_nmbr)) %>%
#   group_by(permit_feature) %>%
#   summarize(number_rows = n())
# 
# # exploring situations where a single permit might've sorted into multiple hucs
# test <- npdes_huc12_unique %>%
#   mutate(permit_feature = paste0(external_permit_nmbr, " - ", perm_feature_nmbr)) %>%
#   filter(permit_feature == "MTR108729 - 047")
# # why did this single point match to three hucs? 
# mt_huc12 <-  get_huc(states %>% filter(STUSPS == "MT"), type = "huc12")
# test_huc <- mt_huc12 %>%
#   filter(huc12 %in% test$huc12)
# mapview(test) + mapview(test_huc)
# # ahahahah it's smack dab in the middle of all three huc12 lol whatttt 

# adding test if geojson is empty: 
if (nrow(npdes_huc12_unique) == 0){
  # throwing message and shutting down worker
  print("New Data Contain 0 Rows - Shutting Down Worker")
  
  # create dataframe with cleaner info added
  task_manager_df <- data.frame(dataset = dataset_i, 
                                date_downloaded = Sys.Date(), 
                                raw_link = "WORKER FAILED - EMPTY DATA",
                                clean_link = "WORKER FAILED - EMPTY DATA")
  
  # add a new row if the dataset is not yet in task manager: 
  if(!(dataset_i %in% task_manager$dataset)){
    task_manager <- bind_rows(task_manager, data.frame(dataset = dataset_i))
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
    filter(dataset != dataset_i) %>% 
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
  
  print("Update to Task Manager Complete - Shutting Down")
  
  # actually force R to quit
  quit(save = "no")
}

# updating dataset in S3 ######################################################
print("Updating S3 with Raw Data")
# translate to geojson and push to aws 
tmp <- tempfile()
st_write(npdes_huc12_unique, dsn = paste0(tmp, ".geojson"))
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".geojson"),
  object = raw_s3_link,
  bucket = "tech-team-data",
  multipart = T
)

# updating dataset in S3 ######################################################
print("Cleaning Data")
npdes_huc12_summary <- npdes_huc12_unique %>% 
  as.data.frame() %>%
  # there are some mining companies that have ~5 permits, each with ~20 
  # permitted features that have the same cwa_status. Since cwa_status does not 
  # seem to be unique to specific features (i.e., outfall pipe). I'm opting to remove 
  # perm_feature_nmbr and focus on permits in violation. BUT, since 
  # we did the intersection with features included (with unique geometries), 
  # a single permit w/ violations could 
  # show up in multiple hucs and be noted as having violations even if the 
  # specific feature is not in violation (we could def fix this with dmrs, but 
  # that would take some additional time since downloading one takes like ~15
  # mins)
  select(-c(perm_feature_nmbr, geometry)) %>%
  unique() %>%
  group_by(huc12) %>%
  # how many permitted features exist within the huc?
  # note that a single external_permit_nmbr may appear in multiple HUC12s
  summarize(npdes_permits = length(unique(external_permit_nmbr)), 
            # regardless of major minor flag, how many permitted features have violations? 
            total_permit_viols = sum(cwa_current_status %in% 
                                       c("Violation Identified", 
                                         "Significant/Category I Noncompliance")),
            total_permit_no_viols = sum(cwa_current_status == "No Violation Identified"),
            total_permit_not_applicable_viols = sum(cwa_current_status == "Not Applicable"),
            total_permit_unknown_viols = sum(cwa_current_status == "Unknown"),
            # how many of these violations are significant?
            total_permit_sig_viols = sum(cwa_current_status == c("Significant/Category I Noncompliance")),
            # how many of these violations are effluent exceedance? 
            total_permit_eff_viols = sum(cwp_current_snc_status %in% c("Effluent - Monthly Average Limit", 
                                                                       "Effluent - Non-monthly Average Limit")))

# testing; yup, can confirm each unique HUC12 & permit combination only 
# exists once 
# test <- npdes_huc12_unique %>%
#   as.data.frame() %>%
#   select(-c(perm_feature_nmbr, geometry)) %>%
#   unique() %>%
#   group_by(huc12, external_permit_nmbr) %>%
#   summarize(uniq_ext_nmbr = n())

# spot checks : 
# test <- npdes_huc12_unique %>%
#   as.data.frame() %>%
#   filter(huc12 == "220100000303")

# adding test if geojson is empty: 
if (nrow(npdes_huc12_summary) == 0){
  # throwing message and shutting down worker
  print("Cleaning Error - Shutting Down Worker")
  
  # create dataframe with cleaner info added
  task_manager_df <- data.frame(dataset = dataset_i, 
                                date_downloaded = Sys.Date(), 
                                raw_link = raw_s3_link,
                                clean_link = "WORKER FAILED - ERROR IN CLEANING")
  
  # add a new row if the dataset is not yet in task manager: 
  if(!(dataset_i %in% task_manager$dataset)){
    task_manager <- bind_rows(task_manager, data.frame(dataset = dataset_i))
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
    filter(dataset != dataset_i) %>% 
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
  
  print("Update to Task Manager Complete - Shutting Down")
  
  # actually force R to quit
  quit(save = "no")
}

# updating dataset in S3 ######################################################
print("Updating S3 with Clean Data")
# translate to geojson and push to aws 
tmp <- tempfile()
write.csv(npdes_huc12_summary, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = clean_s3_link,
  bucket = "tech-team-data",
  multipart = T
)

# updating task manager ########################################################
print("Updating Task Manager")

# create dataframe with cleaner info added
task_manager_df <- data.frame(dataset = dataset_i, 
                              date_downloaded = Sys.Date(), 
                              raw_link = raw_s3_link,
                              clean_link = clean_s3_link)

# add a new row if the dataset is not yet in task manager: 
if(!(dataset_i %in% task_manager$dataset)){
  task_manager <- bind_rows(task_manager, data.frame(dataset = dataset_i))
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
  filter(dataset != dataset_i) %>% 
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

print("Worker Job Complete - Shutting Down")

# keeping old notes ###########################################################
# DMR data Exploration Notes: 
# TLDR; decided to pause on processing specific DMR reports unless convos 
# with the data community indicate these data would valuable. These data 
# would take a long time to download and process. 
# 
# I summarized the dmr reports (below), and it seems like the original 
# data download might actually contain the most up-to-date (but preliminary)
# information on permits in violation?
# based on this: AK0021393 - the dmr data says there is a violation but 
# it was non_reportable, but it was noted as a cat 1 reportable 
# violation in the OG data download... circle back to this if needed 

# based on documentation: 
# Displays "Yes" if the facility is currently in any violation under the Clean Water Act, either in Noncompliance or Significant Noncompliance. Note: This field considers the most current data available, including effluent exceedances reported after the last Quarterly Noncompliance Report (QNCR) was issued; post-QNCR data are considered draft and have not been fully quality assured. If the official data show the facility is in violation and the post-QNCR data show the facility is not in violation or unknown, the status is based on the official QNCR data. Otherwise, the status is based on post-QNCR.

# Example: AK0040380 
# the active_npdes_ids_sf shows that all permits are expired as of 2022, but the
# dmr shows compliance monitoring activities that go to 2024 
# same with the echo explorer: https://echo.epa.gov/detailed-facility-report?fid=110002042897
# ah - they have a new permit pending: "AK0053368"
# test_new_permit <- active_npdes_ids_sf %>% filter(active_npdes_ids_sf$external_permit_nmbr == "AKR06AA88")

# e90 violations 
# number of facilities and number of facilities with violations over the past
# 10 years & number of facilities with open violations 
# test_dmr <- echoGetEffluent(p_id = active_npdes_ids_sf$external_permit_nmbr[1])
# data exploration: 
# unique(test$violation_code) # okay so e90 is the violation code

# function to download multiple DMRs at once - this is going to take a
# hot second 
# data dictionary: https://echo.epa.gov/tools/data-downloads/icis-npdes-dmr-summary
# also super helpful documentation: https://echo.epa.gov/system/files/NNCR_Technical_Support_Guide_0.pdf
# only grabbing the dmrs of the first 50 for now 
# bulk_dmrs <- downloadDMRs(us_active_npdes_sf, 
#                           idColumn = external_permit_nmbr)
# 
# # grab just the dmrs
# dmr <- bind_rows(bulk_dmrs$dmr)
# # filter for e90 volations, and do some mutating to translate detection 
# # dates to a date, locate the year of the detected violation, and translate 
# # resolution codes: 
# e90_viols <- dmr %>%
#   filter(violation_code == "E90") %>%
#   mutate(rnc_detection_date_fmt = as.Date(rnc_detection_date, 
#                                           tryFormats = c("%m/%d/%Y")), 
#          year_detected = year(rnc_detection_date_fmt), 
#          resolved_code = case_when(rnc_resolution_code %in% c("1", "A") ~ "Unresolved",
#                              # Non-Reportable Noncompliance Effluent Violation = 
#                              # not significant enough of a violation to warrant action  
#                              (nchar(rnc_resolution_code) == 0)~ "Non-Reportable", 
#                               TRUE ~ "Resolved"))
# # group by npdes id and the year detected, and summarize violations across
# # all years 
# e90_viol_summary <- e90_viols %>%
#   group_by(npdes_id) %>%
#   summarize(total_e90_viols = n(), 
#             reportable_resolved = sum(resolved_code == "Resolved"), 
#             reportable_unresolved = sum(resolved_code == "Unresolved"), 
#             non_reportable = sum(resolved_code == "Non-Reportable"))
# 
# # find the number of reportable violations over the past 10 years 
# e90_viol_summary_10yr <- e90_viols %>%
#   filter(year_detected >= (year(Sys.Date())-10)) %>%
#   group_by(npdes_id) %>%
#   # we can't look for non_reportable because the year_detected is 
#   # blank for those
#   summarize(reportable_resolved = sum(resolved_code == "Resolved"), 
#             reportable_unresolved = sum(resolved_code == "Unresolved"))
# colnames(e90_viol_summary_10yr)[-1] <- paste0(colnames(e90_viol_summary_10yr)[-1], "_10yr")
# 
# # merging summary together: 
# summary_npdes_viols <- merge(e90_viol_summary, e90_viol_summary_10yr, 
#                              by = "npdes_id", all.x = T) %>%
#   # these are true zeroes: 
#   replace(is.na(.), 0)
# 
# # merging with OG SF object 
# summary_npdes_sf <- merge(us_active_npdes_sf, summary_npdes_viols, 
#                           by.x = "external_permit_nmbr", by.y = "npdes_id", 
#                           all.y = T)
