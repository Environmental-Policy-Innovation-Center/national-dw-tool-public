###############################################################################
# Washington Daily Worker
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
dataset_i <- "wa_bwn"
state <- "Washington"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/wa/water-system/wa_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/wa/wa_bwn.csv"

# updating BWN/BWA data: #######################################################
# for logs: 
print("on: water system datasets - Boil Water Notices & Advisories")

# Part one: scrape active boil water advisories: ###############################
# lives here: "https://doh.wa.gov/community-and-environment/drinking-water/active-alerts?county=All&combine="
url <- "https://doh.wa.gov/community-and-environment/drinking-water/active-alerts?county=All&combine="

# download html: 
page <- read_html(url)
# extract table: 
table <- page %>% 
  html_nodes("table") %>% 
  html_table(fill = TRUE)

# select the right one! 
wa_bwn <- table[[1]] %>%
  janitor::clean_names() %>%
  # these are ghost columns:
  select(-c(x, x_2)) %>%
  # this is also a random duplicated column: 
  filter(!(is.na(water_system))) %>%
  # prepping this column for a merge: 
  mutate(water_system = trimws(water_system), 
         water_system = str_to_title(water_system)) 

# test just pulling WA systems;
wa_pwsids <- epa_sabs_pwsids %>% 
  filter(grepl("WA", states_intersect))

# okay so we only have water system ids... 
wa_bwn_tidy <- merge(wa_bwn, wa_pwsids, by.x  = "water_system", 
                     by.y = "pws_name", all.x = T) %>%
  relocate(pwsid) %>%
  select(-states_intersect) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))
# irk - we only have ~6 that matched, and some names are soooo close, like: 
# Coleman Butte Water Assn (in epa sabs), and Coleman Butte Water Association (bwn)
# I tried some things (see code below, but then Afc Ranch 5 and Afc Ranch 7
# both matched to Afc Ranch 2....


# Part two: add new advisories and detect lifted advisories ###################
print("Comparing with Old Data")
wa_bwn_old <- aws.s3::s3read_using(read.csv, 
                                   object = "s3://tech-team-data/national-dw-tool/raw/wa/water-system/wa_bwn.csv") 

# alrighty creating a unique ID to find new advisories/notices: 
# okay - I need to both update existing records that are no longer on the latest
# data scrape, and add new ones 
wa_bwn_tidy_filt <- wa_bwn_tidy %>%
  # for identifying new records: 
  mutate(full_id = paste0(water_system, date_issued_sort_ascending, 
                          action_recommended)) 

# doing the same for older records: 
wa_bwn_old_filt <- wa_bwn_old %>%
  # for identifying new records: 
  mutate(full_id = paste0(water_system, date_issued_sort_ascending, 
                          action_recommended)) 


# finding new ones: there are 5 new ones 
wa_new_bwn <- wa_bwn_tidy_filt %>% 
  filter(!(full_id %in% wa_bwn_old_filt$full_id)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>% 
  mutate(across(where(is.Date), as.character))

# finding closed ones: okay so two closed
wa_closed_bwn <- wa_bwn_old_filt %>% 
  filter(!(full_id %in% wa_bwn_tidy_filt$full_id)) %>%
  filter(is.na(date_lifted)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>%
  mutate(date_lifted = as.character(Sys.Date()), 
         epic_date_lifted_flag = "Assumed")

# these are bwn that have already closed that we caught previously 
wa_already_closed <- wa_bwn_old_filt %>% 
  filter(!(full_id %in% wa_bwn_tidy_filt$full_id))%>%
  filter(!is.na(date_lifted)) %>%
  # keeping track of the date it was detected 
  select(-full_id)

# grabbing the records that are still active and have not changed
wa_still_active <- wa_bwn_old_filt %>% 
  filter(full_id %in% wa_bwn_tidy_filt$full_id) %>%
  # keeping track of the date it was originally detected 
  select(-full_id) 

# new record should be +3 new rows (it is), plus the two that are closed, plus 
# the records that are still active (47) == 54 rows 
# combining new bwn, still active, and "closed" ones based on the fact they're
# no longer present on the bwn list
wa_bwn_tidy_final <- bind_rows(wa_new_bwn, wa_still_active, wa_closed_bwn, wa_already_closed) 


# check to see if the nrows new >= nrows old [it should be]
if(nrow(wa_bwn_old) > nrow(wa_bwn_tidy_final) | nrow(wa_bwn_tidy_final) == 0){
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
write.csv(wa_bwn_tidy_final, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link
)

# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")

wa_bwn_tidy <- wa_bwn_tidy_final %>%
  # STANDARIZE COLUMNS: 
  rename(date_issued = date_issued_sort_ascending, 
         type = action_recommended) %>%
  mutate(date_issued = as.Date(date_issued, tryFormats = c("%m/%d/%Y")),
         date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")), 
         last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d")), 
         epic_date_lifted_flag = "Assumed", 
         state = "Washington") %>%
  rename(date_epic_captured_advisory = last_epic_run_date) %>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran) 

# adding list to s3
tmp <- tempfile()
write.csv(wa_bwn_tidy, file = paste0(tmp, ".csv"), row.names = F)
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