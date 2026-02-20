###############################################################################
# Ohio Daily Worker
###############################################################################

# libraries needed: 
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(arcpullr)

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
dataset_i <- "oh_bwn"
state <- "Ohio"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/oh/water-system/oh_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/oh/oh_bwn.csv"

# updating BWN/BWA data: #######################################################

# Part one: scrape active boil water advisories: ###############################
# notes associated with this layer: 
# note_url <- "https://geo.epa.ohio.gov/arcgis/rest/services/DrinkingWater/DAGW_advisories_Public/FeatureServer/1"
# notes <- get_table_layer(note_url)
# hm - doesn't seem super useful and has still has some rows with "tests" 

# this point layer only contains active advisories: 
# ack - the advisory begin date is not formatted correctly 
# can confirm this is now throwing a 404 error 
url <- "https://geo.epa.ohio.gov/arcgis/rest/services/DrinkingWater/DAGW_advisories_Public/MapServer/0"
oh_bwn <- get_spatial_layer(url)
# tidy response: 
oh_bwn_tidy <- oh_bwn %>%
  as.data.frame() %>%
  janitor::clean_names() %>%
  # time is in milliseconds from 1970 lol
  mutate(clean_advisory_begin_date = as.POSIXct(adv_begin_date / 1000, 
                                                origin = "1970-01-01", 
                                                tz = "America/New_York"), 
         clean_advisory_begin_date = format(clean_advisory_begin_date, 
                                            "%Y-%m-%d %H:%M:%S"),
         advisory_begin_year = year(clean_advisory_begin_date),
         clean_enforcement_date = as.Date(enforcement_action_date, 
                                          tryFormats = "%m/%d/%Y"), 
         enforcement_begin_year = year(clean_enforcement_date)) %>%
  # we only need data for CWS: 
  filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
  select(-objectid) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))

# removing row names and point geometry, since it makes things weird with
# write.csv
row.names(oh_bwn_tidy) <- NULL
oh_bwn_tidy_no_geoms <- oh_bwn_tidy %>%
  select(-geoms)

# OH0600912 = 5/18/2006, 8:00 PM - time is in milliseconds from 1970 lol
# testing code to fix dates: 
# fix_dates <- oh_bwn_tidy %>%
#   filter(pwsid == "OH0600912")
# fix_dates$adv_begin_date
# test <- as.POSIXct(fix_dates$adv_begin_date / 1000, origin = "1970-01-01", tz = "America/New_York")
# str(test)
# format(test, "%Y-%m-%d %H:%M:%S")


# Part two: add new advisories and detect lifted advisories ###################
print("Comparing with Old Data")
oh_bwn_old <- aws.s3::s3read_using(read.csv, 
                                   object = "s3://tech-team-data/national-dw-tool/raw/oh/water-system/oh_bwn.csv") %>%
  select(-X)


# TEST !!!! making sure "closed" advisories are captured appropriately 
# oh_bwn_tidy_no_geoms <- [3:nrow(oh_bwn_tidy_no_geoms),]


# okay - I need to both update existing records that are no longer on the latest
# data scrape, and add new ones 
oh_bwn_tidy_filt <- oh_bwn_tidy_no_geoms %>%
  # for identifying new records: 
  mutate(full_id = paste0(pwsid, globalid, clean_advisory_begin_date)) 

# doing the same for older records: 
oh_bwn_old_filt <- oh_bwn_old %>%
  # for identifying new records: 
  mutate(full_id = paste0(pwsid, globalid, clean_advisory_begin_date))


# finding new ones: 
oh_new_bwn <- oh_bwn_tidy_filt %>% 
  filter(!(full_id %in% oh_bwn_old_filt$full_id)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>% 
  mutate(across(where(is.Date), as.character))

# finding closed ones: 
oh_closed_bwn <- oh_bwn_old_filt %>% 
  filter(!(full_id %in% oh_bwn_tidy_filt$full_id)) %>%
  filter(is.na(date_lifted)) %>%
  # keeping track of the date it was detected 
  select(-full_id) %>%
  mutate(date_lifted = as.character(Sys.Date()), 
         epic_date_lifted_flag = "Assumed")

# these are bwn that have already closed that we caught previously 
oh_already_closed <- oh_bwn_old_filt %>% 
  filter(!(full_id %in% oh_bwn_tidy_filt$full_id)) %>%
  filter(!is.na(date_lifted)) %>%
  # keeping track of the date it was detected 
  select(-full_id)

# grabbing the records that are still active and have not changed
oh_still_active <- oh_bwn_old_filt %>% 
  filter(full_id %in% oh_bwn_tidy_filt$full_id) %>%
  # keeping track of the date it was originally detected 
  select(-full_id) 


# combining new bwn, still active, and "closed" ones based on the fact they're
# no longer present on the bwn list
oh_bwn_tidy <- bind_rows(oh_new_bwn, oh_still_active, oh_closed_bwn, oh_already_closed) 


# check to see if the nrows new >= nrows old [it should be]
if(nrow(oh_bwn_old) > nrow(oh_bwn_tidy) | nrow(oh_bwn_tidy) == 0){
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
write.csv(oh_bwn_tidy, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link,
  bucket = "tech-team-data",
)

# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")

oh_bwn <- oh_bwn_tidy %>%
  # STANDARIZE COLUMNS: 
  rename(date_issued = clean_advisory_begin_date, 
         type = warningtype) %>%
  mutate(date_issued = as.Date(date_issued, tryFormats = c("%Y-%m-%d")),
         date_lifted = as.Date(date_lifted, tryFormats = c("%Y-%m-%d")), 
         last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d")), 
         epic_date_lifted_flag = "Assumed",
         state = "Ohio") %>%
  rename(date_epic_captured_advisory = last_epic_run_date) %>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran) 

# adding list to s3
tmp <- tempfile()
write.csv(oh_bwn, file = paste0(tmp, ".csv"), row.names = F)
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
