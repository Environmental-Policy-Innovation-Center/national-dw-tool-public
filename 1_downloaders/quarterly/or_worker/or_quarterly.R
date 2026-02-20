###############################################################################
# Oregon Quarterly Worker
###############################################################################

library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(xml2)
library(rvest)
library(lubridate)

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
dataset_i <- "or_bwn"
state <- "Oregon"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/or/water-system/or_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/or/or_bwn.csv"

# updating BWN/BWA data: #######################################################
# for logs: 
print("on: water system datasets - Boil Water Notices & Advisories")

# lives here: "https://yourwater.oregon.gov/advisories.php?areasw=x&areap=x&popa=x&popv=x&open=x&lifted=x&begin=&end=&sort=start"
# NOTE: Note:  Water advisory tracking began in May, 2017; most advisories from before this time will not be displayed here.

url <-"https://yourwater.oregon.gov/advisories.php?areasw=x&areap=x&popa=x&popv=x&open=x&lifted=x&begin=&end=&sort=start"

# read the html page and extract tables 
page <- read_html(url) %>% 
  html_nodes("table") %>% 
  html_table(fill = T)

# extract the second table, which has the bwn data: 
bwn <- page[[2]] 
# the names had a of the filtering options appended to them, which is not what 
# we want 
names(bwn)[1:13] <- c("regulating_agency", "county_served", "pws", "pws_name", 
                      "system_type", "population", "primary_source", "advisory_type", 
                      "reason", "begin_date", "date_lifted", "area_affected", 
                      "affcted_populations")
or_bwn <- bwn[, 1:13]

# the pwsid is not formatted correctly: 
or_bwn_tidy <- or_bwn %>%
  # there's one NA
  filter(!is.na(pws)) %>% 
  # find how many leading zeroes to add
  mutate(pwsid_char = as.character(pws), 
         pwsid_nchar = nchar(pwsid_char), 
         num_zeroes = 5 - as.numeric(pwsid_nchar), 
         zeroes = strrep(0, num_zeroes), 
         # paste OR41 at the beginning 
         pwsid_repaired = paste0("OR41", zeroes, pwsid_char)) %>%
  # tidy columns: 
  rename(pwsid = pwsid_repaired) %>%
  select(-c(pwsid_char:zeroes)) %>%
  relocate(pwsid, .before = pws) %>%
  # make sure we only have pwsids associated with a boundary: 
  # NOTE - there are some CWS on this list that aren't in the EPA SABs database
  # like OR4100174 (Knoll Terrace Park)m and OR410998 (Dexter Oaks Co-Op)
  filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))

# Part two: adding updated records ############################################
print("Adding Updates to Old Dataset")
# checking for updates: 
or_bwn_old <- aws.s3::s3read_using(read.csv, 
                                   object = raw_s3_link) 

# okay - I need to both update existing records that have a new date_lifted 
# and add new records. 
or_bwn_tidy_filt <- or_bwn_tidy %>%
  select(-last_epic_run_date) %>%
  # for identifying 100% new records: 
  mutate(full_id = paste0(pwsid, advisory_type, begin_date, date_lifted), 
         # for updating advisories that now have a date_lifted date: 
         update_id = paste0(pwsid, advisory_type, begin_date))
# doing the same for older records: 
or_bwn_old_filt <- or_bwn_old %>%
  select(-last_epic_run_date) %>%
  mutate(full_id = paste0(pwsid, advisory_type, begin_date, date_lifted), 
         update_id = paste0(pwsid, advisory_type, begin_date))


# these are records with an updated end date: 
updated_records <- or_bwn_tidy_filt %>%
  # the full ID w/ lifted date is not the same, but the update ID is
  filter((!(full_id %in% or_bwn_old_filt$full_id)) & 
           (update_id %in% or_bwn_old_filt$update_id))

# update the old records:
or_bwn_no_updates <- or_bwn_old %>%
  # gotta add the ids back for identifying updated records, while keeping 
  # the original last_epic_run_date
  mutate(full_id = paste0(pwsid, advisory_type, begin_date, date_lifted), 
         update_id = paste0(pwsid, advisory_type, begin_date)) %>%
  # find updated records
  filter(!(update_id %in% updated_records$update_id)) %>%
  # remove these ID columns: 
  select(-c(full_id, update_id)) 
# find updated records: 
or_bwn_updates <- updated_records %>%
  select(-c(full_id, update_id)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))
# bind to update: 
or_updated_records <- bind_rows(or_bwn_updates, or_bwn_no_updates)


# add new records: 
# these are actually new records: 
new_records <- or_bwn_tidy_filt %>%
  # the full ID and update ID are completely missing 
  filter(!(full_id %in% or_bwn_old_filt$full_id) & 
           !(update_id %in% or_bwn_old_filt$update_id)) %>%
  select(-c(full_id, update_id)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))

# final! 
# NOTE - there are 4 more, all by ROWENA CREST MANOR that are no longer 
# on the current list, but were in the historic data... keeping them for 
# now but just a flag
or_bwn_final <- bind_rows(new_records, or_updated_records)

# check to see if the nrows new >= nrows old [it should be]
if(nrow(or_bwn_old) > nrow(or_bwn_final) | nrow(or_bwn_final) == 0){
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
write.csv(or_bwn_final, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link
)

# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")

or_bwn_clean <- or_bwn_final %>% 
  mutate(begin_date_formatted = lubridate::mdy(begin_date), 
         begin_year = year(begin_date_formatted),
         date_lifted_formatted = case_when(
           # date_lifted should be NA for open ones
           date_lifted == "Open" ~ NA, 
           TRUE ~ lubridate::mdy(date_lifted)), 
         lifted_year = year(date_lifted_formatted),
         epic_date_lifted_flag = "Reported",
         state = state) %>%
  rename(or_date_lifted = date_lifted) %>%
  # there's only one from 2016, and the first advisory in 2017 was from May
  # 2017-05-08, so we can safely filter in this way: 
  filter(begin_year > 2016) %>%
  rename(date_issued = begin_date_formatted, 
         date_lifted = date_lifted_formatted, 
         date_epic_captured_advisory = last_epic_run_date,
         type = advisory_type) %>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran) 

print("Warning is ok - just boil water notices that are still open")

# adding list to s3
tmp <- tempfile()
write.csv(or_bwn_clean, file = paste0(tmp, ".csv"), row.names = F)
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