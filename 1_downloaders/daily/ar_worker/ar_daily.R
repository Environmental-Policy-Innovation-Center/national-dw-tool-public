###############################################################################
# Arkansas Quarterly Worker
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
dataset_i <- "ar_bwn"
state <- "Arkansas"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/ar/water-system/ar_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/ar/ar_bwn.csv"

# updating BWN/BWA data: #######################################################
# for logs: 
print("on: water system datasets - Boil Water Notices & Advisories")

# Part one: scrape active boil water advisories: ###############################
url <- "https://health.arkansas.gov/wa_engTraining/boilwaterorder.aspx"
# download html: 
page <- read_html(url)
# extract table: 
table <- page %>% 
  html_nodes("table") %>% 
  html_table(fill = TRUE)

# grabbin the data: 
ar_bwn <- table[[1]] %>%
  janitor::clean_names() %>%
  mutate(system = str_squish(str_to_title(system)))

# removing texarkana to avoid duplicates: 
ar_bwn_tidy <- merge(ar_bwn, epa_sabs_pwsids %>% 
                       filter(!pwsid == "TX0190004") %>%
                       filter(grepl("AR", states_intersect)),
                     by.x = "system", 
                     by.y ="pws_name", all.x = T) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))

### TEST - remove before pushing 
# ar_bwn_tidy$date_lifted[1] <- "5/20/2025 9:20:00 AM"

# Part two: add new advisories and detect lifted advisories ###################
ar_bwn_old <- aws.s3::s3read_using(read.csv, 
                                   object = raw_s3_link) 

# okay - I need to both update existing records that have a new date_lifted 
# and add new records. 
ar_bwn_tidy_filt <- ar_bwn_tidy %>%
  select(-last_epic_run_date) %>%
  # for identifying 100% new records: 
  mutate(full_id = paste0(system, date_issued, date_lifted), 
         # for updating advisories that now have a date_lifted date: 
         update_id = paste0(system, date_issued))

# doing the same for older records: 
ar_bwn_old_filt <- ar_bwn_old %>%
  select(-last_epic_run_date) %>%
  # for identifying 100% new records: 
  mutate(full_id = paste0(system, date_issued, date_lifted), 
         # for updating advisories that now have a date_lifted date: 
         update_id = paste0(system, date_issued))


# these are records with an updated end date: 
updated_records <- ar_bwn_tidy_filt %>%
  filter((!(full_id %in% ar_bwn_old_filt$full_id)) & 
           (update_id %in% ar_bwn_old_filt$update_id))

# update the old records:
ar_bwn_no_updates <- ar_bwn_old %>%
  # gotta add the ids back for identifying updated records, while keeping 
  # the original last_pic_run_date
  mutate(full_id = paste0(system, date_issued, date_lifted), 
         update_id = paste0(system, date_issued)) %>%
  # find updated records
  filter(!(update_id %in% updated_records$update_id)) %>%
  # remove these ID columns: 
  select(-c(full_id, update_id)) 
# find updated records: 
ar_bwn_updates <- updated_records %>%
  select(-c(full_id, update_id)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))
# bind to update: 
ar_updated_records <- bind_rows(ar_bwn_updates, ar_bwn_no_updates)


# add new records: 
# these are actually new records: 
new_records <- ar_bwn_tidy_filt %>%
  filter(!(full_id %in% ar_bwn_old_filt$full_id) & 
           !(update_id %in% ar_bwn_old_filt$update_id)) %>%
  select(-c(full_id, update_id)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))

# final! 
ar_bwn_final <- bind_rows(new_records, ar_updated_records)


# check to see if the nrows new >= nrows old [it should be]
if(nrow(ar_bwn_old) > nrow(ar_bwn_final) | nrow(ar_bwn_final) == 0){
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
write.csv(ar_bwn_final, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link
)

# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")
# 
# ar_bwn <- aws.s3::s3read_using(read.csv, 
#                                object = "s3://tech-team-data/national-dw-tool/raw/ar/water-system/ar_bwn.csv") 

# I want formatted date columns
ar_bwn_tidy <- ar_bwn_final %>% 
  mutate(date_issued = as.Date(date_issued, tryFormats = c("%m/%d/%Y")), 
         date_lifted = as.Date(date_lifted, tryFormats = c("%m/%d/%Y")),
         last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d")), 
         type = "No Information", 
         epic_date_lifted_flag = "Reported", 
         state = "Arkansas")%>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  rename(date_epic_captured_advisory = last_epic_run_date) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran) 

# adding list to s3
tmp <- tempfile()
write.csv(ar_bwn_tidy, file = paste0(tmp, ".csv"), row.names = F)
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

