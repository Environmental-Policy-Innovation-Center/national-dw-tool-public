###############################################################################
# RMP Sites Quarterly Worker
###############################################################################

# libraries needed: 
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(s2)
library(arcpullr)
library(tigris)
library(sf)
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
dataset_i <- "rmp_sites"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/national/environmental/active_rmp.geojson"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/national/huc12_active_rmp.csv"

# facilities with risk management plans #######################################
print("on: environmental datasets - rmp sites")

# this isn't easily available through EPA's API but I can query it from here 
# (which is updated monthly)
# available here: https://www.arcgis.com/home/item.html?id=477c87e78f5b4453b810520394277483#data
# more details here: https://azgeo-open-data-agic.hub.arcgis.com/datasets/geoplatform::epa-emergency-response-er-risk-management-plan-rmp-facilities/about
rmp_url <- "https://services.arcgis.com/cJ9YHowT8TU7DUyn/ArcGIS/rest/services/FRS_INTERESTS_RMP/FeatureServer/0"
rmp <- get_spatial_layer(rmp_url)
active_rmp <- rmp %>%
  janitor::clean_names() %>%
  # there are 702 sites where the active_status == NA; and 7973 that are
  # inactive - CRS is WGS84
  filter(active_status == "ACTIVE") %>%
  mutate(last_epic_run_date = Sys.Date())

# adding test if geojson is empty: 
if (nrow(active_rmp) == 0){
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

# checking output: 
# mapview(active_rmp)

# checking active status 
# rmp %>%
#   janitor::clean_names() %>%
#   group_by(active_status) %>%
#   summarize(totals = n())

# checking with the past dataset that was downloaded 
# active_rmp <- aws.s3::s3read_using(st_read,
#                              object = "s3://tech-team-data/national-dw-tool/raw/national/environmental/active_rmp.geojson")

# updating dataset in S3 ######################################################
print("Updating S3 with raw data")
# translate to geojson and push to aws 
tmp <- tempfile()
st_write(active_rmp, dsn = paste0(tmp, ".geojson"))
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".geojson"),
  object = raw_s3_link,
  bucket = "tech-team-data",
  multipart = T
)

# relating data to huc12s ######################################################
print("Relating RMPs to HUC12s")

# st_use_s2(F) <- these run a lot faster without spherical geometry, but 
# need to check how this affects intersections. should be okay if we're focusing 
# on a state-by-state basis. 
states <- states() %>% 
  arrange(NAME)

# turn off spherical geometry for running intersections - I've viewed the 
# output and I don't believe this affects intersections, since we're just 
# sorting the data into different huc12 bins. Plus, we project to a planar 
# CRS before intersecting B) 
sf_use_s2(F)
# renaming and creating empty vector for intersecting rmps to huc12
rmps <- active_rmp %>%
  st_transform(., crs = 5070)
rmp_huc12 <- rmps[0,] %>%
  mutate(across(-geoms, as.character))
# loopin through states
for(i in 1:nrow(states)){
  state_i <- states[i,]
  message("On state ", state_i$NAME, ", ", i, ":", " out of ", nrow(states))
  # grab huc12 boundaries for the state: 
  state_i_huc_geom <- get_huc(states[i,], type = "huc12")
  # transform crs before intersecting: 
  tidy_state_i <- state_i_huc_geom %>%
    mutate(gnis_id = as.integer(gnis_id)) %>%
    st_transform(., crs = st_crs(rmps))
  # run intersection of rmps and the state 
  rmps_intersection <- st_intersection(rmps, tidy_state_i) %>%
    mutate(across(-geoms, as.character))
  # add this to global environment 
  rmp_huc12 <- bind_rows(rmp_huc12, rmps_intersection)
}

# should be the same number of rows as active_rmp
rmp_huc12_unique <- rmp_huc12 %>%
  unique() %>%
  # renaming the original and not super filled out huc12 column: 
  rename(old_huc_12 = huc_12)

# summarizing rmps by huc12 
rmp_huc12_summary <- rmp_huc12_unique %>%
  as.data.frame() %>%
  group_by(huc12) %>%
  summarize(total_facilities_w_rmps = length(unique(registry_id)))

# quick check here to confirm sorting
# mapview::mapview(rmp_huc12_unique %>% filter(state_code == "WY") %>% select(huc12)) + 
#   mapview::mapview(tidy_state_i)

# checking out duplicated columns  - ah, these are all instances where the HUC12
# crosses state lines, so it got captured twice. 
# dups <- rmp_huc12[duplicated(rmp_huc12),]


# adding another check here: 
# adding test if geojson is empty: 
if (nrow(rmp_huc12_summary) == 0 | (nrow(rmp_huc12_unique) != nrow(active_rmp))){
  # throwing message and shutting down worker
  print("HUC12 Intersections Failed - Shutting Down Worker")
  
  # create dataframe with cleaner info added
  task_manager_df <- data.frame(dataset = dataset_i, 
                                date_downloaded = Sys.Date(), 
                                raw_link = raw_s3_link,
                                clean_link = "WORKER FAILED - ERROR IN HUC12 SUMMARY")
  
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



# relating data to huc12s ######################################################
print("Writing clean dataset to s3")

# translate to geojson and push to aws 
tmp <- tempfile()
write.csv(rmp_huc12_summary, paste0(tmp, ".csv"), row.names = F)
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
