###############################################################################
# Florida Quarterly Worker
###############################################################################

# libraries needed: 
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(rvest)

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
dataset_i <- "fl_bwn"
state <- "Florida"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/fl/water-system/fl_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/fl/fl_bwn.csv"

# updating BWN/BWA data: #######################################################
# for logs: 
print("on: water system datasets - Boil Water Notices & Advisories")

# Part one: scrape active boil water advisories: ###############################
url <- "https://www.floridahealth.gov/community-environmental-public-health/emergency-preparedness-response/boil-water-notices/"

# download html: 
page <- read_html(url)
# extract table: 
table <- page %>% 
  html_nodes("table") %>% 
  html_table(fill = TRUE)

fl_bwn <- table[[1]] %>%
  janitor::clean_names() %>%
  # filter(system_name != "=====HURRICANE MILTON ADVISORIES") %>%
  mutate(last_epic_run_date = as.character(Sys.Date()), 
         system_name = str_squish(str_to_title(system_name))) 

# testing here: 
# test <- fl_bwn_old %>% select(-c("pwsid", "states_intersect"))
# test$date_issued <- "10/1/2025"
# test$comments <- "test"
# fl_bwn <- test
# fl_bwn <- rbind(fl_bwn, test)

# Part two: add new advisories and detect lifted advisories ###################
fl_bwn_old <- aws.s3::s3read_using(read.csv,
                                   object = raw_s3_link)


# # okay - I need to both update existing records that have a new date_lifted 
# # and add new records. 
fl_bwn_tidy_filt <- fl_bwn %>%
  select(-last_epic_run_date) %>%
  # for identifying 100% new records:
  mutate(full_id = paste0(system_name, date_issued, date_rescinded),
         # for updating advisories that now have a date_lifted date:
         update_id = paste0(system_name, date_issued))

# doing the same for older records:
fl_bwn_old_filt <- fl_bwn_old %>%
  select(-last_epic_run_date) %>%
  # for identifying 100% new records:
  mutate(full_id = paste0(system_name, date_issued, date_rescinded),
         # for updating advisories that now have a date_lifted date:
         update_id = paste0(system_name, date_issued))


# these are records with an updated end date:
updated_records <- fl_bwn_tidy_filt %>%
  filter((!(full_id %in% fl_bwn_old_filt$full_id)) &
           (update_id %in% fl_bwn_old_filt$update_id))

# update the old records:
fl_bwn_no_updates <- fl_bwn_old %>%
  # gotta add the ids back for identifying updated records, while keeping
  # the original last_pic_run_date
  mutate(full_id = paste0(system_name, date_issued, date_rescinded),
         # for updating advisories that now have a date_lifted date:
         update_id = paste0(system_name, date_issued)) %>%
  # find updated records
  filter(!(update_id %in% updated_records$update_id)) %>%
  # remove these ID columns:
  select(-c(full_id, update_id)) %>%
  mutate(across(everything(), as.character))
# find updated records:
fl_bwn_updates <- updated_records %>%
  select(-c(full_id, update_id)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date())) %>%
  mutate(across(everything(), as.character))
# bind to update:
fl_updated_records <- bind_rows(fl_bwn_updates, fl_bwn_no_updates)


# add new records:
# these are actually new records:
new_records <- fl_bwn_tidy_filt %>%
  filter(!(full_id %in% fl_bwn_old_filt$full_id) &
           !(update_id %in% fl_bwn_old_filt$update_id)) %>%
  select(-c(full_id, update_id)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))%>%
  mutate(across(everything(), as.character))

# final!
if (is.na(new_records$system_name) & is.na(new_records$county)) {
  # catching if still empty: 
  fl_bwn_final <- bind_rows(fl_updated_records)
} else {
  fl_bwn_final <- bind_rows(new_records, fl_updated_records)
}

# quick hard code to handle an advisory that just got deleted w/o a date
# lifted 
# fl_bwn_final <- fl_bwn_final %>%
#   mutate(date_rescinded = "05/21/2025") %>%
#   mutate(comments = paste0("EPIC ASSUMED DATE RESCINDED - ADVISORY DELETED W/O A DATE PROVIDED. ",  comments))

# check to see if the nrows new >= nrows old [it should be]
if(nrow(fl_bwn_old) > nrow(fl_bwn_final) | nrow(fl_bwn_final) == 0){
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
write.csv(fl_bwn_final, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link
)

# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")

fl_bwn_tidy <- fl_bwn_final %>% 
  mutate(date_issued = as.Date(date_issued, tryFormats = c("%m/%d/%Y")), 
         date_lifted = as.Date(date_rescinded, tryFormats = c("%m/%d/%Y")),
         last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d")), 
         type = "Assumed - Boil Water Notices", 
         state = state)%>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  # there was one record that was deleted from the page w/o a date lifted - handling 
  # that here 
  mutate(epic_date_lifted_flag = case_when(!grepl("EPIC ASSUMED DATE RESCINDED", comments) ~ "Reported", 
                                           TRUE ~ "Assumed for this one record")) %>%
  rename(date_epic_captured_advisory = last_epic_run_date) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran)

# adding list to s3
tmp <- tempfile()
write.csv(fl_bwn_tidy, file = paste0(tmp, ".csv"), row.names = F)
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
