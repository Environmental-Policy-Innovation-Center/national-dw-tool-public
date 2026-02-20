###############################################################################
# New Mexico Quarterly Worker
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
dataset_i <- "nm_bwn"
state <- "New Mexico"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/nm/water-system/nm_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/nm/nm_bwn.csv"

# updating BWN/BWA data: #######################################################
# for logs: 
print("on: water system datasets - Boil Water Notices & Advisories")

# all located here: "https://www.env.nm.gov/drinking_water/boil-water-advisories/"
url <- "https://www.env.nm.gov/drinking_water/boil-water-advisories/"
# read the html page and extract tables 
page <- read_html(url) %>% 
  html_nodes("table") %>% 
  html_table(fill = T)

# unlist: 
page_data <- page[[1]]
names(page_data) <- c("water_system", "county", "advisory_issue_date", "status")
# the first row is just the column names, so removing that from our dataset: 
nm_bwn <- page_data[2:nrow(page_data),]


# REMOVE THIS AFTER AND RUN CLEAN - JUST FOR TESTING: 
# nm_bwn$status[4] <- "Lifted on 05/01/2025"
# new_record <- data.frame(water_system = "test new record", 
#                          county = "test", 
#                          advisory_issue_date = "05/01/2025", 
#                          status = "Precautionary Boil Advisory")
# nm_bwn <- bind_rows(new_record, nm_bwn)

# relating water systems to pwsids - some of these have pwsids, but seems like they 
# stopped collecting them in the water_system name recently
# test <- nm_bwn %>%
#   mutate(water_system = strsplit(water_system,"\\(|\\)")) %>%
#   unnest(water_system) 
nm_bwn_sep <- nm_bwn %>%
  separate(water_system, into = c("name_one", "pwsid_one", 
                                  "name_two", "pwsid_two"), sep = "\\(|\\)") 

# tidy and organize rows with pwsids: 
bwn_pwsids <- nm_bwn_sep %>%
  # filter those without pwsids for now 
  filter(!is.na(pwsid_one)) %>%
  # pivot separated pwsid columns 
  pivot_longer(., cols = c("pwsid_one", "pwsid_two")) %>%
  filter(!is.na(value)) %>%
  # rebuild water system name, and do some column reorg/tidying 
  mutate(water_system_name = paste0(name_one, name_two)) %>%
  select(-c(name_one:name_two)) %>%
  relocate(water_system_name) %>%
  rename(pwsid = value, 
         pwsid_number = name) %>%
  relocate(pwsid, .after = water_system_name)

# isolate those without pwsids: 
bwn_no_pwsids <- nm_bwn_sep %>%
  filter(is.na(pwsid_one)) %>%
  mutate(name_one = trimws(name_one))

# rectifying pwsids based on EPA SABs names 
nm_pwsids <- epa_sabs_pwsids %>%
  filter(grepl("NM", states_intersect))
bwn_no_pwsids_merged <- merge(nm_pwsids, bwn_no_pwsids, 
                              by.x = "pws_name", by.y = "name_one", 
                              all.y = T) %>%
  select(-c(pwsid_one:pwsid_two)) %>%
  rename(water_system_name = pws_name)

# combining back here: 
nm_bwn_tidy <- bind_rows(bwn_pwsids, bwn_no_pwsids_merged) %>%
  # issued dates are formatted in various ways: 
  mutate(issued_date_clean = case_when(
    nchar(advisory_issue_date) == 10 ~ as.Date(advisory_issue_date, tryFormats = c("%m/%d/%Y")), 
    nchar(advisory_issue_date) %in% c(7,8) ~ as.Date(advisory_issue_date, tryFormats = c("%m/%d/%y"))), 
    status_clean = case_when(grepl("Lifted", status) ~ "Lifted"), 
    # extract the date of the lift, if it is available
    lifted_date_clean = str_extract(status, "\\d{1,2}/\\d{1,2}/\\d{4}"),
    # convert to date to deal with weird formats: 
    lifted_date_clean = as.Date(lifted_date_clean, "%m/%d/%Y"),
    # fix this pwsid, which based on EPA SABs, is the correct one 
    pwsid = case_when(pwsid == "NM355518" ~ "NM3535518", 
                      TRUE ~ pwsid)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))


# alrightly checking this against our other old dataset for updates: 
nm_bwn_old <- aws.s3::s3read_using(read.csv, 
                                   object = raw_s3_link) 

# filtering the old dataset and creating a unique id to look for changes: 
nm_bwn_tidy_filt <- nm_bwn_tidy %>%
  select(-last_epic_run_date) %>%
  # for identifying 100% new records: 
  mutate(full_id = paste0(pwsid, issued_date_clean, lifted_date_clean), 
         # for updating advisories that now have a date_lifted date: 
         update_id = paste0(pwsid, issued_date_clean))
# older dataset: 
nm_bwn_old_filt <- nm_bwn_old %>%
  select(-last_epic_run_date) %>%
  # for identifying 100% new records: 
  mutate(full_id = paste0(pwsid, issued_date_clean, lifted_date_clean), 
         # for updating advisories that now have a date_lifted date: 
         update_id = paste0(pwsid, issued_date_clean))


# these are records with an updated end date: 
updated_records <- nm_bwn_tidy_filt %>%
  filter((!(full_id %in% nm_bwn_old_filt$full_id)) & 
           (update_id %in% nm_bwn_old_filt$update_id))

# update the old records:
nm_bwn_no_updates <- nm_bwn_old %>%
  # gotta add the ids back for identifying updated records, while keeping 
  # the original last_pic_run_date
  mutate(full_id = paste0(pwsid, issued_date_clean, lifted_date_clean), 
         update_id = paste0(pwsid, issued_date_clean)) %>%
  # find updated records
  filter(!(update_id %in% updated_records$update_id)) %>%
  # remove these ID columns: 
  select(-c(full_id, update_id)) 
# find updated records: 
nm_bwn_updates <- updated_records %>%
  select(-c(full_id, update_id)) %>%
  mutate(issued_date_clean = as.character(issued_date_clean), 
         lifted_date_clean = as.character(lifted_date_clean)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))
# bind to update: 
nm_updated_records <- bind_rows(nm_bwn_updates, nm_bwn_no_updates)


# add new records: 
# these are actually new records: 
new_records <- nm_bwn_tidy_filt %>%
  filter(!(full_id %in% nm_bwn_old_filt$full_id) & 
           !(update_id %in% nm_bwn_old_filt$update_id)) %>%
  select(-c(full_id, update_id)) %>%
  mutate(issued_date_clean = as.character(issued_date_clean), 
         lifted_date_clean = as.character(lifted_date_clean)) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))

# final! 
nm_bwn_final <- bind_rows(new_records, nm_updated_records)

# check to see if the nrows new >= nrows old [it should be]
if(nrow(nm_bwn_old) > nrow(nm_bwn_final) | nrow(nm_bwn_final) == 0){
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
write.csv(nm_bwn_final, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link
)


# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")

nm_bwn_tidy <- nm_bwn_final %>%
  rename(date_issued = issued_date_clean, 
         date_lifted = lifted_date_clean) %>%
  mutate(date_issued = as.Date(date_issued, tryFormats = c("%Y-%m-%d")),
         date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")), 
         last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d")), 
         epic_date_lifted_flag = "Reported", 
         type = "Assumed - Boil Water Advisories", 
         state = state)%>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  rename(date_epic_captured_advisory = last_epic_run_date) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran) 

# adding list to s3
tmp <- tempfile()
write.csv(nm_bwn_tidy, file = paste0(tmp, ".csv"), row.names = F)
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
