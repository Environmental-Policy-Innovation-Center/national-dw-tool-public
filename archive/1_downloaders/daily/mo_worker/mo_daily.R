###############################################################################
# Missouri Daily Worker
###############################################################################

# libraries!
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(httr)
library(jsonlite)

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
dataset_i <- "mo_bwn"
state <- "Missouri"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/mo/water-system/mo_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/mo/mo_bwn.csv"

# updating BWN/BWA data: #######################################################
# for logs: 
print("on: water system datasets - Boil Water Notices & Advisories")

# Part one: scrape active boil water advisories: ###############################
# https://data.mo.gov/Regulatory/DNR-WPP-Boil-Order-Report/j2a5-itxh/data_preview
url <- "https://data.mo.gov/resource/j2a5-itxh.json"

response <- GET(url)
mo_bwn <- content(response, "text") %>% 
  fromJSON() %>%
  as.data.frame() %>%
  janitor::clean_names() %>%
  select(issue_date:geocoded_column) %>%
  mutate(issue_date = as.Date(issue_date, tryFormats = c("%Y-%m-%d"))) 

### start commented section to manually add older records
# # need to manually add the ones from Lance, but it's possible they're all non CWS
# # write.csv(mo_bwn, "./data/mo_bwn_old.csv")
# worksheet <- read.csv("./data/MO_BWO/worksheet.csv") %>%
#   mutate(status = "worksheet") %>%
#   janitor::clean_names()
# # note - manually fixed missing pwsids
# removed <- read.csv("./data/MO_BWO/removed.csv")%>%
#   mutate(status = "removed") %>%
#   janitor::clean_names()
# 
# # binding these together
# received_data <- bind_rows(worksheet, removed) %>%
#   mutate(end_date = remove_date,
#          #  NOTE - ASSUMING THESE ISSUE DATES SHOULD BE DATE_ADDED RATHER THAN
#          # THE REGION
#          issue_date = case_when(issue_date %in% c("KCRO", "SWRO") ~ added,
#                                 TRUE ~ issue_date)) %>%
#   # making issued date consistently formatted
#   mutate(issue_date = as.Date(issue_date, tryFormats = c("%m/%d/%y")))
# 
# # combining with current scraped data
# # first, need to fix dates
# mo_bwn_date_fixed <- mo_bwn %>%
#   mutate(issue_date = as.Date(issue_date, tryFormats = c("%Y-%m-%d"))) %>%
#   mutate(status = "on_website")
# 
# # then, rbind
# received_data <- bind_rows(received_data, mo_bwn_date_fixed) %>%
#   filter(pws_id %in% epa_sabs_pwsids$pwsid)
# 
# lat_long <- received_data$geocoded_column %>%
#   as.data.frame() %>%
#   rename(geocode_lat = latitude,
#          geocode_long = longitude)
# 
# mo_bwn_tidy <- cbind(received_data, lat_long) %>%
#   select(-geocoded_column) %>%
#   # mutate(across(all_of(c("geocode_lat", "geocode_long")), as.character)) %>%
#   mutate(last_epic_run_date = as.character(Sys.Date())) %>%
#   # making issued date consistently formatted
#   mutate(end_date = as.Date(end_date, tryFormats = c("%m/%d/%y"))) %>%
#   mutate(latitude = case_when(!is.na(geocode_lat) ~ as.numeric(geocode_lat),
#                               TRUE ~ latitude),
#          longitude = case_when(!is.na(geocode_long) ~ as.numeric(geocode_long),
#                               TRUE ~ longitude)) %>%
#   select(-c(geocode_lat:geocode_long)) %>%
#   mutate(date_lifted = end_date) %>%
#   mutate(across(everything(), ~as.character(.)))
# 
# tmp <- tempfile()
# write.csv(mo_bwn_tidy, paste0(tmp, ".csv"), row.names = F)
# on.exit(unlink(tmp))
# put_object(
#   file = paste0(tmp, ".csv"),
#   object = raw_s3_link,
#   multipart = T
# )
### end commented section 

# split the geocoded column, which is stored as a data frame for some reason: 
lat_long <- mo_bwn$geocoded_column %>%
  as.data.frame() %>%
  rename(geocode_lat = latitude, 
         geocode_long = longitude)
mo_bwn_tidy <- cbind(mo_bwn, lat_long) %>%
  select(-geocoded_column) %>%
  mutate(last_epic_run_date = as.character(Sys.Date())) %>%
  # oof - all of these are not CWS
  filter(pws_id %in% epa_sabs_pwsids$pwsid) %>%
  mutate(across(everything(), ~ as.character(.)))


## FOR TESTING: 
# mo_bwn_tidy <- mo_bwn_tidy %>% filter(pws_id != "MO4010490")
# mo_bwn_dummy <- mo_bwn_tidy
# mo_bwn_dummy$issue_date <- "2025-09-21" 
# mo_bwn_tidy <- bind_rows(mo_bwn_tidy, mo_bwn_dummy)

# Part two: add new advisories and detect lifted advisories ###################
print("Comparing with Old Data")
mo_bwn_old <- aws.s3::s3read_using(read.csv, 
                                   object = raw_s3_link) 


# okay - I need to both update existing records that are no longer on the latest
# data scrape, and add new ones 
mo_bwn_tidy_filt <- mo_bwn_tidy %>%
  # for identifying new records: 
  mutate(full_id = paste0(issue_date, pws_id, contaminant_of_concern)) 

# doing the same for older records: 
mo_bwn_old_filt <- mo_bwn_old %>%
  # for identifying new records: 
  mutate(full_id = paste0(issue_date, pws_id, contaminant_of_concern)) 

# finding new ones: 
mo_new_bwn <- mo_bwn_tidy_filt %>% 
  filter(!(full_id %in% mo_bwn_old_filt$full_id)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>%
  mutate(across(everything(), ~ as.character(.)))

# finding closed ones: 
mo_closed_bwn <- mo_bwn_old_filt %>% 
  filter(!(full_id %in% mo_bwn_tidy_filt$full_id)) %>%
  filter(is.na(date_lifted)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>%
  mutate(date_lifted = as.character(Sys.Date()), 
         epic_date_lifted_flag = "Assumed") %>%
  mutate(across(everything(), ~ as.character(.)))

# these are bwn that have already closed that we caught previously 
mo_already_closed <- mo_bwn_old_filt %>% 
  filter(!(full_id %in% mo_bwn_tidy_filt$full_id)) %>%
  filter(!is.na(date_lifted)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>%
  mutate(across(everything(), ~ as.character(.)))

# grabbing the records that are still active and have not changed
mo_still_active <- mo_bwn_old_filt %>% 
  filter(full_id %in% mo_bwn_tidy_filt$full_id) %>%
  # keeping track of the date it was originally detected 
  select(-full_id) %>%
  mutate(across(everything(), ~ as.character(.)))


# combining new bwn, still active, and "closed" ones based on the fact they're
# no longer present on the bwn list
mo_bwn_tidy_final <- bind_rows(mo_new_bwn, mo_still_active, 
                               mo_closed_bwn, mo_already_closed) 

# code below was to capture the reported date_lifteds from the data we received
# from the state
  # mutate(epic_date_lifted_flag = case_when(is.na(epic_date_lifted_flag) & !is.na(end_date)~ "Reported", 
  #                                          TRUE ~ epic_date_lifted_flag))


# check to see if the nrows new >= nrows old [it should be]
if(nrow(mo_bwn_old) > nrow(mo_bwn_tidy_final) | nrow(mo_bwn_tidy_final) == 0){
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

# adding to S3
tmp <- tempfile()
write.csv(mo_bwn_tidy_final, paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link,
  multipart = T
)

# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")

mo_bwn <- mo_bwn_tidy_final %>%
  # STANDARIZE COLUMNS: 
  rename(date_issued = issue_date, 
         pwsid = pws_id) %>%
  # handling bwn that were reported to us in historic log
  mutate(epic_date_lifted_flag = case_when(is.na(epic_date_lifted_flag) ~ "Assumed", 
                                           TRUE ~ epic_date_lifted_flag)) %>%
  mutate(date_issued = as.Date(date_issued, tryFormats = c("%Y-%m-%d")),
         # date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")), 
         last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d")), 
         type = "Assumed - Boil Order",
         state = state)  %>%
  rename(date_epic_captured_advisory = last_epic_run_date) %>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran) 

# adding to S3
tmp <- tempfile()
write.csv(mo_bwn, paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = clean_s3_link,
  bucket = "tech-team-data",
  multipart = T
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
put_object(
  file = paste0(tmp, ".csv"),
  object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv",
  acl = "public-read"
)

print("Worker Job Complete - Shutting Down")