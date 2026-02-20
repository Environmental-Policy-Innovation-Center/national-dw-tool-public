###############################################################################
# Rhode Island Quarterly Worker
###############################################################################

# libraries needed: 
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(googlesheets4)

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
dataset_i <- "ri_bwn"
state <- "Rhode Island"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/ri/water-system/ri_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/ri/ri_bwn.csv"

# updating BWN/BWA data: #######################################################
# for logs: 
print("on: water system datasets - Boil Water Notices & Advisories")

# after inspecting network data from this website: https://health.ri.gov/drinking-water-quality/information/public-water-emergency-information-consumers
# the data are being pulled from this spreadsheet: 
gs4_deauth()
url <- "https://docs.google.com/spreadsheets/d/1RYakppZA5sCJxChpwuWUA7L4YWnY90XxEGrWAMeFqNQ/edit?gid=223203438#gid=223203438"
# sheet = "Approved to Lift"
approved_lift <- read_sheet(url, sheet = "Approved to Lift") %>%
  janitor::clean_names() %>%
  mutate(type = "Approved Lifted - General")
approved_lift_pfas <- read_sheet(url, sheet = "Lifted PFAS Orders") %>%
  janitor::clean_names() %>%
  mutate(type = "Approved Lifted - PFAS")

# binding these together: 
approved_lifted <- bind_rows(approved_lift, approved_lift_pfas)

# ongoing notices: 
ongoing_notices <- read_sheet(url, sheet = "Ongoing Notices") %>%
  janitor::clean_names() %>%
  mutate(type = "Ongoing - General") %>%
  # v there's a couple of notes at the top of this sheet saying 
  # "data are current as of Dec 11th 2024" 
  filter(!is.na(water_system_id)) %>%
  filter(!grepl("This information is", water_system_id))

ongoing_pfas_notices <- read_sheet(url, sheet = "Ongoing PFAS Notices") %>%
  janitor::clean_names() %>%
  mutate(type = "Ongoing - PFAS")

# binding: 
ongoing <- bind_rows(ongoing_notices, ongoing_pfas_notices)


# adding all of these together: 
ri_bwn <- bind_rows(ongoing, approved_lifted) %>%
  # there's one pwsid that is missing a number and I can't seem to match it 
  # to a known pwsid, but looking on EPA SABs website, it doesn't seem to be 
  # a CWS
  rename(pwsid = water_system_id) %>%
  filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))


# Part two: add new advisories and detect lifted advisories ###################
ri_bwn_old <- aws.s3::s3read_using(read.csv,
                                   object = raw_s3_link)

# TODO - add update code - the CWS data are still the same, and it's difficult 
# to figure out the coding pieces to do updates since the state maintains 
# four different lists and contain different information 

# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")

ri_bwn_tidy <- ri_bwn_old %>%
  rename(date_issued = date_ordered) %>%
  # assuming - Do not drink orders were inferred where the column 
  # "water_systems_under_do_not_drink" was not NA, and boil water notices where 
  # the column water_systems_under_boil_water_order was not NA. Otherwise, the
  # type of notice or advisory is NA. 
  mutate(type = case_when(!is.na(water_systems_under_boil_water_order) ~ "Boil Water Order - Ongoing", 
                          !is.na(water_systems_approved_to_lift_notice) ~ "Boil Water Order - Approved to Lift",
                          !is.na(water_systems_under_do_not_drink) & !is.na(date_lifted) ~ "Do Not Drink - Approved to Lift", 
                          !is.na(water_systems_under_do_not_drink) & is.na(date_lifted) ~ "Do Not Drink - Ongoing"),
         epic_date_lifted_flag = "Reported", 
         state = state) %>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  rename(date_epic_captured_advisory = last_epic_run_date) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran) 

# adding list to s3
tmp <- tempfile()
write.csv(ri_bwn_tidy, file = paste0(tmp, ".csv"), row.names = F)
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
