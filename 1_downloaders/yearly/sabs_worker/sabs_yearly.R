###############################################################################
# SABs Yearly Worker
###############################################################################

# libraries needed: 
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(sf)
library(tigris)
library(units)
library(httr)
library(arcpullr)

# no scientific notation 
options(scipen = 999)
options(tigris_use_cache = TRUE)

# make sure to specify the correct bucket region for IAM role: 
Sys.setenv("AWS_DEFAULT_REGION" = 'us-east-1')

# for logs: 
print("I'm running!")

# pulling in task manager for updating relevant sections: 
task_manager <- aws.s3::s3read_using(read.csv, 
                                     object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv") |>
  mutate(across(everything(), ~ as.character(.)))

# for some of the coding bits: 
dataset_i <- "epa_sabs"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/national/water-system/epa_sabs.geojson"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/national/epa_sabs.geojson"

# updating EPA SABs: ##########################################################
print("on: water system datasets - EPA SABs")

# Opting to pull data using the arcREST API because the github data link
# has date stamps in the file name & the file structure changed since Feb, which 
# would create code that would be prone to error / break on the worker 

# grabbing some metadata to see how many records there are: 
layer_metadata <- get_layer_info("https://services.arcgis.com/cJ9YHowT8TU7DUyn/ArcGIS/rest/services/Water_System_Boundaries/FeatureServer/0/getEstimates")
print(paste("Number of Records:", layer_metadata$count))

# prepping to paginate using the API: 
full_count = layer_metadata$count + 1000 # adding a 1000 buffer 
i = 1
sabs_data_query <- list()
seq_to_loop <- seq(from = 0, to = full_count, by = 500)
base_url <- "https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/Water_System_Boundaries/FeatureServer/0/query"

# NOTE - someone may look at this code and ask: "why not use arcpullr"? WELL - 
# I tried using arcpullr::get_spatial_layer() but it was not reliably grabbing 
# the full set of records (I was consistently missing ~640), and it was dropping 
# OBJECTIDs that fell within my query range. It's possible I was hitting some 
# sort of hidden limit, and/or the objectIDs are not actually continuous (the 
# latter is more likely). So, I need to paginate the results: 
for (i in seq_along(seq_to_loop)){
  # find range boundaries
  range_min <- seq_to_loop[i]
  range_max <- seq_to_loop[i+1]
  # build query
  query <- list(
    where = "1=1",
    outFields = "*",
    returnGeometry = "true",
    f = "geojson",
    resultOffset = range_min,
    resultRecordCount = 500)
  # pass request
  req <- GET(base_url, query = query)
  full_count_i <- st_read(content(req, "text"), quiet = TRUE)

  # break loop if no data:   
  if(nrow(full_count_i) == 0){
    print(paste0("NO DATA ON LOOP: ", i))
    break
  }

  # print statement to observe progress
  print(paste0("Working On: ", range_min, " - ", range_max,
               "; Records: ", nrow(full_count_i), "; Time: ", Sys.time()))
  
  # add it to da list 
  sabs_data_query[[length(sabs_data_query) + 1]] <- full_count_i
  # chill for a second
  Sys.sleep(5)
}

print("Running Do.Call to combine")

# yank the list entries out and rbind 
epa_sabs_all <- do.call(rbind, sabs_data_query)

# check the entries to make sure we captured everything 
if(nrow(epa_sabs_all) != layer_metadata$count){
  print(paste0("API QUERY MISSED RECORDS, SUTTING DOWN WORKER"))
  # actually force R to quit
  quit(save = "no")
}

print("Running St_make_valid")
# do some light tidying 
epa_sabs <- epa_sabs_all |>
  janitor::clean_names() |>
  mutate(last_epic_run_date = Sys.Date()) |>
  # there are still errors from invalid geoms: 
  st_make_valid() 

# quick exploration: 
# update Jan 27th, 2026 - there are duplicated pwsids but they are essentially
# a multipolygon 
# 
# dups <- epa_sabs[duplicated(epa_sabs$pwsid),] |>
#   as.data.frame()
# duplicated_pwsids <- epa_sabs |>
#   filter(pwsid %in% dups$pwsid) 
# duplicated_pwsids %>%
# mapview::mapview(duplicated_pwsids %>% select(pwsid))
# duplicated_pwsids_df <- duplicated_pwsids %>%
#   as.data.frame() %>%
#   select(-c(last_epic_run_date, geometry))
# write.csv(duplicated_pwsids_df, "./data/duplicated_pwsids_Jan26.csv")

print("Check to make sure epa_sabs is not empty")
# check to see if the nrows new >= nrows old [it should be]
if(nrow(epa_sabs) == 0){
  # throwing message and shutting down worker
  print("New Data Contain Zero Records - Shutting Down Worker")
  
  # create dataframe with cleaner info added
  task_manager_df <- data.frame(dataset = dataset_i, 
                                date_downloaded = Sys.Date(), 
                                raw_link = "WORKER FAILED - NO DATA IN DOWNLOAD",
                                clean_link = "WORKER FAILED - NO DATA IN DOWNLOAD")
  
  # add a new row if the dataset is not yet in task manager: 
  if(!(dataset_i %in% task_manager$dataset)){
    task_manager <- bind_rows(task_manager, data.frame(dataset = dataset_i))
  }
  
  # select the other columns from the task manager that we do not want to 
  # overwrite: 
  dont_touch_these_columns <- setdiff(names(task_manager), 
                                      names(task_manager_df))
  task_manger_simple <- task_manager |> 
    select(dataset, all_of(dont_touch_these_columns))
  
  # merge them together to create the updated row
  updated_row <- merge(task_manger_simple, 
                       task_manager_df, by = "dataset") |>
    mutate(across(everything(), ~ as.character(.)))
  
  # bind this row back to the task manager
  updated_task_manager <- task_manager |>
    filter(dataset != dataset_i) |> 
    bind_rows(., updated_row) |>
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


# writing this to s3: #########################################################
print("Updating EPA SABs in S3")

tmp <- tempfile()
st_write(epa_sabs, dsn = paste0(tmp, ".geojson"))
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".geojson"),
  object = raw_s3_link,
  bucket = "tech-team-data",
  multipart = T
)

# checking 
# epa_sabs <- aws.s3::s3read_using(st_read, object = raw_s3_link)

# minor cleaning: ##############################################################
print("Cleaning Data")

# very light cleaning 
epa_sabs_clean <- epa_sabs |>
  # make it simple for easier rendering 
  st_simplify() |>
  # standardize pws names for merging in the future
  mutate(pws_name = str_to_title(pws_name), 
         pws_name = trimws(pws_name), 
         pwsid = trimws(pwsid)) 


# adding epic_state to keep track of intersected sabs 
print("Creating Small Helper Dataframe")

# also creating a simple df of CWS IDs and names for filtering other datasets
sabs_df <- epa_sabs_clean |>
  as.data.frame() |>
  select(pwsid, pws_name) |>
  arrange(pwsid) |>
  unique()

# i'd also like the matching state the system falls into for easy filtering 
# while not accidentally removing tribal SABs
sf_use_s2(F)
state_pwsid <- st_intersection(epa_sabs_clean, states() |> 
                                 st_transform(., crs = st_crs(epa_sabs_clean)))

# developing a summary df of pwsids, their name, and the states they 
# intersect with 
state_pwsid_summary <- state_pwsid |>
  as.data.frame() |>
  select(pwsid, STUSPS) |>
  rename(state_intersect = STUSPS) |>
  group_by(pwsid) |>
  # handling situations where a sab may be duplicated
  summarize(states_intersect = paste(unique(state_intersect), collapse = ",")) |>
  unique()
sf_use_s2(T)

# combine with sabs_df
sabs_df_summary <- merge(sabs_df, state_pwsid_summary, all = T)

# also writing this to s3: 
tmp <- tempfile()
write.csv(sabs_df_summary, paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = "/national-dw-tool/raw/national/water-system/sabs_pwsid_names.csv",
  bucket = "tech-team-data",
  acl = "public-read",
  multipart = T
)

# adding this as a new field to epa_sabs: 
epa_sabs_final <- merge(epa_sabs_clean, 
                        sabs_df_summary |> 
                          select(-pws_name), 
                        by = "pwsid", all.x = T) |>
  rename(epic_states_intersect = states_intersect) |>
  relocate(epic_states_intersect, .after = pwsid) |>
  # also adding EWG link: 
  mutate(ewg_report_link = paste0("https://www.ewg.org/tapwater/system.php?pws=", 
                                  pwsid)) |>
  relocate(ewg_report_link, .before = last_epic_run_date) |>
  # projecting to alberts equal area for st_area calculations 
  st_transform(., crs = 5070)

# adding area: 
epa_sabs_final$epic_area_mi2 <- units::set_units(st_area(epa_sabs_final), "mi^2") 
epa_sabs_final <- epa_sabs_final |>
  relocate(epic_area_mi2, .before = last_epic_run_date) |> 
  # transform back to original crs of WGS 84
  st_transform(., crs = st_crs(epa_sabs))

# removing this code to calculate pop density, to avoid accidental mismatches 
# between a population based on an older SAB and the updated boundary
  # left_join(., epa_sabs_xwalk_df) |>
  # mutate(epic_pop_density = total_pop / epic_area_mi2) |>
  # select(-total_pop) |>
  # relocate(epic_pop_density, .after = epic_area_mi2)

# writing this to s3: #########################################################
print("Updating Clean EPA SABs in S3")

tmp <- tempfile()
st_write(epa_sabs_final, dsn = paste0(tmp, ".geojson"))
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".geojson"),
  object = clean_s3_link,
  bucket = "tech-team-data",
  multipart = T
)

# test <-  aws.s3::s3read_using(st_read, 
#                               object = clean_s3_link)

# Part five: update task manager ###############################################
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
task_manger_simple <- task_manager |> 
  select(dataset, all_of(dont_touch_these_columns))

# merge them together to create the updated row
updated_row <- merge(task_manger_simple, 
                     task_manager_df, by = "dataset") |>
  mutate(across(everything(), ~ as.character(.)))

# bind this row back to the task manager
updated_task_manager <- task_manager |>
  filter(dataset != dataset_i) |> 
  bind_rows(updated_row) |>
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

