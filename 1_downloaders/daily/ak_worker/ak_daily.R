###############################################################################
# Alaska Daily Worker
###############################################################################

# libraries needed: 
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(Rcpp)
library(sf)
library(arcpullr)
library(stringr)

# no scientific notation 
options(scipen = 999)

# make sure to specify the correct bucket region for IAM role: 
Sys.setenv("AWS_DEFAULT_REGION" = 'us-east-1')

# for logs: 
print("I'm running!")

# for filtering for CWS: 
epa_sabs_pwsids <- aws.s3::s3read_using(read.csv, 
                                        object = "s3://tech-team-data/national-dw-tool/raw/national/water-system/sabs_pwsid_names.csv")

# pulling in task manager for updating relevant sections: 
task_manager <- aws.s3::s3read_using(read.csv, 
                                     object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv")%>%
  mutate(across(everything(), ~ as.character(.)))

# for some of the coding bits: 
dataset_i <- "ak_bwn"
state <- "Alaska"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/ak/water-system/ak_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/ak/ak_bwn.csv"

# updating BWN/BWA data: #######################################################
# for logs: 
print("on: water system datasets - Boil Water Notices & Advisories")

# Part one: scrape active boil water advisories: ###############################
print("Grabbing New Data")
url <- "https://dec.alaska.gov/arcgis/rest/services/EH/BWN_DND/FeatureServer/0"
ak_bwn <- get_spatial_layer(url) %>%
  janitor::clean_names()

# tidy response
ak_bwn_tidy <- ak_bwn %>%
  as.data.frame() %>%
  janitor::clean_names() %>%
  mutate(pwsid = str_squish(pwsid)) %>% 
  filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
  # time is in milliseconds from 1970 lol
  mutate(bwn_issue_date_clean = as.character(as.POSIXct(bwn_issue_date / 1000, 
                                                        origin = "1970-01-01", 
                                                        tz = "America/Anchorage")), 
         geoms = as.character(geoms)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))

# FOR TESTING:
# ak_bwn_tidy <- ak_bwn_tidy[5:nrow(ak_bwn_tidy),]

# Part two: add new advisories and detect lifted advisories ###################
print("Comparing with Old Data")
ak_bwn_old <- aws.s3::s3read_using(read.csv, 
                                   object = raw_s3_link) 

# okay - I need to both update existing records that are no longer on the latest
# data scrape, and add new ones 
ak_bwn_tidy_filt <- ak_bwn_tidy %>%
  # for identifying new records: 
  mutate(full_id = paste0(pwsid, bwn_issue_date)) 

# doing the same for older records: 
ak_bwn_old_filt <- ak_bwn_old %>%
  # for identifying new records: 
  mutate(full_id = paste0(pwsid, bwn_issue_date)) 


# finding new ones: 
ak_new_bwn <- ak_bwn_tidy_filt %>% 
  filter(!(full_id %in% ak_bwn_old_filt$full_id)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>% 
  mutate(across(where(is.Date), as.character))

# finding closed ones: 
ak_closed_bwn <- ak_bwn_old_filt %>% 
  filter(!(full_id %in% ak_bwn_tidy_filt$full_id)) %>%
  filter(is.na(date_lifted)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>%
  mutate(date_lifted = as.character(Sys.Date()), 
         epic_date_lifted_flag = "Assumed")%>% 
  mutate(across(where(is.Date), as.character))

# these are bwn that have already closed that we caught previously 
ak_already_closed <- ak_bwn_old_filt %>% 
  filter(!(full_id %in% ak_bwn_tidy_filt$full_id))%>%
  filter(!is.na(date_lifted)) %>%
  # keeping track of the date it was detected 
  select(-full_id)

# grabbing the records that are still active and have not changed
ak_still_active <- ak_bwn_old_filt %>% 
  filter(full_id %in% ak_bwn_tidy_filt$full_id) %>%
  # keeping track of the date it was originally detected 
  select(-full_id) %>% 
  mutate(across(where(is.Date), as.character))


# combining new bwn, still active, and "closed" ones based on the fact they're
# no longer present on the bwn list
ak_bwn_tidy_f <- bind_rows(ak_new_bwn, ak_still_active, 
                           ak_closed_bwn, ak_already_closed) 

# check to see if the nrows new >= nrows old [it should be]
if(nrow(ak_bwn_old) > nrow(ak_bwn_tidy_f) | nrow(ak_bwn_tidy_f) == 0){
  # throwing message and shutting down worker
  print("New Data Contain Less Rows than Older Data - Shutting Down Worker")
  
  # create dataframe with cleaner info added
  task_manager_df <- data.frame(dataset = dataset_i, 
                                date_downloaded = Sys.Date(), 
                                raw_link = "WORKER FAILED - SEE LOGS",
                                clean_link = "WORKER FAILED - SEE LOGS")
  
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

# if this check passes, overwrite data in the old bucket
tmp <- tempfile()
write.csv(ak_bwn_tidy_f, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link
)

# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")

ak_bwn_clean <- ak_bwn_tidy_f %>%
  mutate(date_issued = as.Date(bwn_issue_date_clean, tryFormats = c("%Y-%m-%d")),
         date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")), 
         last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d")), 
         epic_date_lifted_flag = "Assumed", 
         type = name, 
         state = state) %>%
  rename(date_epic_captured_advisory = last_epic_run_date) %>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran) 

# adding list to s3
tmp <- tempfile()
write.csv(ak_bwn_clean, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = clean_s3_link,
  bucket = "tech-team-data",
)

# Part four: update task manager ###############################################
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

print("updating task manager - adding back to s3")

put_object(
  file = paste0(tmp, ".csv"),
  object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv",
  acl = "public-read"
)

print("Worker Job Complete - Shutting Down")