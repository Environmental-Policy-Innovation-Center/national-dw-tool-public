###############################################################################
# West Virginia Quarterly Worker
###############################################################################

# libraries needed: 
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
dataset_i <- "wv_bwn"
state <- "West Virginia"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/wv/water-system/wv_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/wv/wv_bwn.csv"

# updating BWN/BWA data: #######################################################
# for logs: 
print("on: water system datasets - Boil Water Notices & Advisories")

# lives here: "https://oehsportal.wvdhhr.org/boilwater"
# url <-"https://oehsportal.wvdhhr.org/boilwater"
# this data is actually pulling from an API - I inspected the webpage 
# and located the endpoint it's pulling from: 
url <- "https://oehsportal.wvdhhr.org/boilwater/home/boilwaternotices"
response <- GET(url)
wv_bwn <- content(response, "text") %>% fromJSON()

# tidy response:  - looks like these data only pull records from the past 
# year, and miightttt contain information beyond boil water advisories? 
# On Apr 26th 2025, 65 of these were not CWS
wv_bwn_tidy <- wv_bwn %>%
  janitor::clean_names() %>%
  filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))


# this one is a little different, because the current webpage is a running 
# "window" of bwn of the past year, and we want to retain the ones that are no 
# longer in the current window but are in our previous records: 

# Part two: adding updated records ############################################
print("Adding Updates to Old Dataset")
# need to pull data back from s3, check for updated data and append 
# new records: 
# alrightly checking this against our other old dataset for updates: 
wv_bwn_old <- aws.s3::s3read_using(read.csv, 
                                   object = raw_s3_link) 


# filtering the old dataset and creating a unique id to look for changes: 
wv_bwn_tidy_filt <- wv_bwn_tidy %>%
  select(-last_epic_run_date) %>%
  # for identifying 100% new records: 
  mutate(full_id = paste0(pwsid, date_issued, date_lifted, details), 
         # for updating advisories that now have a date_lifted date: 
         update_id = paste0(pwsid, date_issued, details))
# older dataset: 
wv_bwn_old_filt <- wv_bwn_old %>%
  select(-last_epic_run_date) %>%
  # for identifying 100% new records: 
  mutate(full_id = paste0(pwsid, date_issued, date_lifted, details), 
         # for updating advisories that now have a date_lifted date: 
         update_id = paste0(pwsid, date_issued, details))


# these are records with an updated end date: 
updated_records <- wv_bwn_tidy_filt %>%
  filter((!(full_id %in% wv_bwn_old_filt$full_id)) & 
           (update_id %in% wv_bwn_old_filt$update_id))

# update the old records:
wv_bwn_no_updates <- wv_bwn_old %>%
  # gotta add the ids back for identifying updated records, while keeping 
  # the original last_epic_run_date
  mutate(full_id = paste0(pwsid, date_issued, date_lifted, details), 
         # for updating advisories that now have a date_lifted date: 
         update_id = paste0(pwsid, date_issued, details)) %>%
  # find updated records
  filter(!(update_id %in% updated_records$update_id)) %>%
  # remove these ID columns: 
  select(-c(full_id, update_id)) 
# find updated records: 
wv_bwn_updates <- updated_records %>%
  select(-c(full_id, update_id)) %>%
  # mutate(issued_date_clean = as.character(issued_date_clean), 
  #        lifted_date_clean = as.character(lifted_date_clean)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))
# bind to update: 
wv_updated_records <- bind_rows(wv_bwn_updates, wv_bwn_no_updates)


# add new records: 
# these are actually new records: 
new_records <- wv_bwn_tidy_filt %>%
  filter(!(full_id %in% wv_bwn_old_filt$full_id) & 
           !(update_id %in% wv_bwn_old_filt$update_id)) %>%
  select(-c(full_id, update_id)) %>%
  # mutate(issued_date_clean = as.character(issued_date_clean), 
  #        lifted_date_clean = as.character(lifted_date_clean)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))

# final! 
wv_bwn_final <- bind_rows(new_records, wv_updated_records)

# check to see if the nrows new >= nrows old [it should be]
if(nrow(wv_bwn_old) > nrow(wv_bwn_final) | nrow(wv_bwn_final) == 0){
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
write.csv(wv_bwn_final, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link
)

# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")

wv_bwn_clean <- wv_bwn_final %>%
  # renaming columns to make them easier to standardize/check 
  rename(date_issued_wv = date_issued, 
         date_lifted_wv = date_lifted) %>%
  select(-id) %>%
  mutate(date_issued = as.Date(date_issued_wv, tryFormats = c("%Y-%m-%d")), 
         date_lifted = as.Date(date_lifted_wv, tryFormats = c("%Y-%m-%d")),
         last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d")), 
         type = "Assumed - Boil Water Notices", 
         epic_date_lifted_flag = "Reported", 
         state = state) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           last_epic_run_date, type) %>%
  rename(date_epic_captured_advisory = last_epic_run_date) %>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran) 

# adding list to s3
tmp <- tempfile()
write.csv(wv_bwn_clean, file = paste0(tmp, ".csv"), row.names = F)
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
put_object(
  file = paste0(tmp, ".csv"),
  object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv",
  acl = "public-read"
)

print("Worker Job Complete - Shutting Down")
