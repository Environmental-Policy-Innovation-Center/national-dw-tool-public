###############################################################################
# Maine Daily Worker
###############################################################################

# libraries!
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
dataset_i <- "me_bwn"
state <- "Maine"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/me/water-system/me_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/me/me_bwn.csv"

# updating BWN/BWA data: #######################################################
# for logs: 
print("on: water system datasets - Boil Water Notices & Advisories")

# Part one: scrape active boil water advisories: ###############################
url <- "https://www.maine.gov/dhhs/mecdc/healthy-living/health-safety/drinking-water-safety/information-for-consumers/drinking-water-safety-alerts"
# download html: 
page <- read_html(url)
# extract table: 
table <- page %>% 
  html_nodes("table") %>% 
  html_table(fill = TRUE)

# the first one is do not drink, and the second is boil water orders
bwo <- table[[1]] %>%
  as.data.frame() %>%
  janitor::clean_names() %>%
  mutate(type = "Boil Water Order")%>%
  mutate(across(everything(), as.character))
dnd <- table[[2]] %>%
  as.data.frame() %>%
  janitor::clean_names() %>%
  mutate(type = "Do Not Drink Order")%>%
  mutate(across(everything(), as.character))

# this might be blank, so adding an if statement here to handle a potential
# error
if(length(table) == 3){
  dnu <- table[[3]]%>%
    as.data.frame() %>%
    janitor::clean_names() %>%
    mutate(type = "Do Not Use Order")%>%
    mutate(across(everything(), as.character))
} else {
  dnu <- NULL
}
 
# bind that together: 
me_bwn_tidy <- bind_rows(dnd, bwo, dnu) %>%
  # there's ~5 CWS that are listed as "community" on the state page, 
  # but actually don't match to a pwsid in the epa sabs dataset
  # can confirm the names also don't match up 
  filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
  rename(date_issued_me = date_issued) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()), 
         date_issued = as.character(lubridate::mdy(date_issued_me)))

# Part two: add new advisories and detect lifted advisories ###################
print("Comparing with Old Data")
me_bwn_old <- aws.s3::s3read_using(read.csv, 
                                   object = raw_s3_link) 

# alrighty creating a unique ID to find new advisories/notices: 
# okay - I need to both update existing records that are no longer on the latest
# data scrape, and add new ones 
me_bwn_tidy_filt <- me_bwn_tidy %>%
  # for identifying new records: 
  mutate(full_id = paste0(pwsid, date_issued, reason)) 

# doing the same for older records: 
me_bwn_old_filt <- me_bwn_old %>%
  # for identifying new records: 
  mutate(full_id = paste0(pwsid, date_issued, reason)) 

# finding new ones: there are 5 new ones 
me_new_bwn <- me_bwn_tidy_filt %>% 
  filter(!(full_id %in% me_bwn_old_filt$full_id)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>% 
  mutate(across(where(is.Date), as.character))%>%
  mutate(date_lifted = as.character(NA))

# finding closed ones: okay so two closed
me_closed_bwn <- me_bwn_old_filt %>% 
  filter(!(full_id %in% me_bwn_tidy_filt$full_id)) %>%
  filter(is.na(date_lifted)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>%
  mutate(date_lifted = as.character(Sys.Date()))

# these are bwn that have already closed that we caught previously 
me_already_closed <- me_bwn_old_filt %>% 
  filter(!(full_id %in% me_bwn_tidy_filt$full_id))%>%
  filter(!is.na(date_lifted)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>% 
  mutate(across(where(is.Date), as.character))%>%
  mutate(date_lifted = as.character(date_lifted))

# grabbing the records that are still active and have not changed
me_still_active <- me_bwn_old_filt %>% 
  filter(full_id %in% me_bwn_tidy_filt$full_id) %>%
  # keeping track of the date it was originally detected 
  select(-full_id) %>%
  mutate(date_lifted = as.character(date_lifted))

# combining new bwn, still active, and "closed" ones based on the fact they're
# no longer present on the bwn list
me_bwn_tidy_final <- bind_rows(me_new_bwn, me_still_active, me_closed_bwn, me_already_closed) 

# check to see if the nrows new >= nrows old [it should be]
if(nrow(me_bwn_old) > nrow(me_bwn_tidy_final) | nrow(me_bwn_tidy_final) == 0){
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
write.csv(me_bwn_tidy_final, paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link,
  multipart = T
)

# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")
# maine's website is currently down, but wanting to restandardize these cols: 
# me_old <- aws.s3::s3read_using(readRDS, 
#                                      object = "s3://tech-team-data/national-dw-tool/clean/me/me_water_system.RData")
# old_bwn_data <- me_old[[1]] %>%
#   mutate(state = state) %>%
#   rename(date_epic_captured_advisory = last_epic_run_date) %>%
#   mutate(date_worker_last_ran = "NA") %>%
#   relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
#            date_epic_captured_advisory, type, state, date_worker_last_ran) 

me_bwn <- me_bwn_tidy_final %>%
  # STANDARIZE COLUMNS: 
  mutate(date_issued = as.Date(date_issued, tryFormats = c("%Y-%m-%d")),
         # date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")), 
         last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d")), 
         epic_date_lifted_flag = "Assumed",
         state = state) %>%
  # doing some renaming to make the column definitions clear: 
  rename(date_epic_captured_advisory = last_epic_run_date) %>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran) 

# adding list to s3
tmp <- tempfile()
write.csv(me_bwn, file = paste0(tmp, ".csv"), row.names = F)
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