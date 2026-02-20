###############################################################################
# SDWA Quarterly Worker
###############################################################################

# libraries needed: 
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(data.table)
library(curl)

# no scientific notation 
print("Increasing timeout to handle large files!")
options(scipen = 999)
options(timeout = 720) # this should bump to 12 mins 

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
dataset_i <- "sdwis_viols"

# updating SDWA data: ##########################################################
# for logs: 
print("on: water system datasets - SDWIS")

# downloading to temporary directory: 
file_loc <- tempdir()
# using curl to download sdwa data faster
curl_download("https://echo.epa.gov/files/echodownloads/SDWA_latest_downloads.zip", 
              destfile = paste0(file_loc, ".zip"))
# data are refreshed quarterly & data dictionary is located here: 
# https://echo.epa.gov/tools/data-downloads/sdwa-download-summary
# this currently takes ~5 mins to load 
# download.file("https://echo.epa.gov/files/echodownloads/SDWA_latest_downloads.zip", 
#               destfile = paste0(file_loc, ".zip"))
unzip(zipfile = paste0(file_loc, ".zip"), exdir = file_loc) 
file.remove(paste0(file_loc, ".zip"))

print("data downloaded - reading now & starting with water system info")
# grab and tidy the ones we need - basic water system info: 
ws_info <- fread(paste0(file_loc, "/SDWA_PUB_WATER_SYSTEMS.csv")) %>%
  janitor::clean_names() %>%
  # we just want active CWS and ones that are in our EPA SABs dataset: 
  filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
  mutate(last_epic_run_date = Sys.Date())

# adding a check if df is empty: 
if(nrow(ws_info) == 0){
  # throwing message and shutting down worker
  print("New Data Contain 0 Rows - Shutting Down Worker")
  quit(save = "no")
}

print("adding water system info")
write.csv(ws_info, file = paste0(file_loc, "/ws_info_tidy.csv"), row.names = F)
put_object(
  file = paste0(file_loc, "/ws_info_tidy.csv"),
  object = "/national-dw-tool/raw/national/water-system/sdwa_pub_water_systems.csv",
  bucket = "tech-team-data",
  multipart = T
)

print("starting violations and enforcement - this will take a hot second")
# grab and tidy the ones we need - violations and enforcement: 
viol_enf <- fread(paste0(file_loc, "/SDWA_VIOLATIONS_ENFORCEMENT.csv")) %>%
  janitor::clean_names() %>%
  filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
  mutate(last_epic_run_date = Sys.Date())

# adding a check if df is empty: 
if(nrow(viol_enf) == 0){
  # throwing message and shutting down worker
  print("New Data Contain 0 Rows - Shutting Down Worker")
  quit(save = "no")
}

# I want to add this dataset to aws since we might want the full violation 
# history for a system, rather than just aggregated stats. 
print("adding violations and enforcement")
write.csv(viol_enf, file = paste0(file_loc, "/viol_enf_tidy.csv"), row.names = F)
put_object(
  file = paste0(file_loc, "/viol_enf_tidy.csv"),
  object = "/national-dw-tool/raw/national/water-system/sdwa_violations_enforcement.csv",
  bucket = "tech-team-data",
  multipart = T
)

print("starting reference codes")
# we also need the reference codes to relate violation data to actual rule 
# codes: 
ref_codes <- fread(paste0(file_loc, "/SDWA_REF_CODE_VALUES.csv")) %>%
  janitor::clean_names() %>%
  filter(value_type %in% c("RULE_FAMILY_CODE")) %>%
  rename(rule = value_description)  %>%
  mutate(last_epic_run_date = Sys.Date(), 
         value_code = as.integer(value_code))

# adding a check if df is empty: 
if(nrow(ref_codes) == 0){
  # throwing message and shutting down worker
  print("New Data Contain 0 Rows - Shutting Down Worker")
  quit(save = "no")
}

print("adding ref codes")
write.csv(ref_codes, file = paste0(file_loc, "/sdwa_ref_codes.csv"), row.names = F)
put_object(
  file = paste0(file_loc, "/sdwa_ref_codes.csv"),
  object = "/national-dw-tool/raw/national/water-system/sdwa_ref_codes.csv",
  bucket = "tech-team-data",
  multipart = T
)

# cleaning SDWIS data #########################################################
print("Summarizing Datasets - water system information")

# data dictionary here for reference: https://echo.epa.gov/tools/data-downloads/sdwa-download-summary
# starting with water system information: 

# grabbing main columns of interest fromthe 
sdwis_ws_info_tidy <- ws_info %>%
  # translating this column back to human readable 
  mutate(owner_type = case_when(
    owner_type_code == "F" ~ "Federal", 
    owner_type_code == "L" ~ "Local", 
    owner_type_code == "M" ~ "Public/Private", 
    owner_type_code == "N" ~ "Native American", 
    owner_type_code == "P" ~ "Private", 
    owner_type_code == "S" ~ "State"), 
    # grabbing the date the system was first reported - 
    # "first reported date for the system" 
    first_reported_date = as.Date(first_reported_date, tryFormats = c("%m/%d/%Y")), 
    # identifying the number of years they've been operating 
    years_operating = year(Sys.Date()) - year(first_reported_date)) %>%
  # hm - note there are a couple of inactive systems where pws_activity_code == "I",
  # but this might be SABs that haven't been removed yet
  select(pwsid, pws_activity_code, 
         gw_sw_code, first_reported_date, years_operating, owner_type, 
         primacy_type:is_school_or_daycare_ind, 
         source_water_protection_code, outstanding_performer, city_name, 
         address_line1, address_line2, zip_code, phone_number) %>%
  relocate(primary_source_code, .after = gw_sw_code) %>%
  mutate(across(everything(), as.character)) %>%
  # I need to keep this as numeric to filter: 
  mutate(years_operating = as.numeric(years_operating))

# translating empty cells to no information, as there are many blanks 
# and some boolean values that are Y/N but also blank, and I don't want 
# these to accidentally get transformed 
sdwis_ws_info_tidy[sdwis_ws_info_tidy == ""] <- "No Information"


print("Summarizing Datasets - violation data") #################################
# helper dfs to capture systems that have been operating for < 10 or 5 years: 
less_10years <- sdwis_ws_info_tidy %>%
  filter(years_operating < 10)
less_5_years <- sdwis_ws_info_tidy %>%
  filter(years_operating < 5)

# SDWIS violations and enforcement actions
viol_rulecode <- merge(viol_enf, 
                       # remove this duplicated column 
                       ref_codes %>% select(-last_epic_run_date), 
                       # NOTE - I'm pretty positive the data dictionary is wrong, 
                       # and the rule family code [with the 120-430 code breakdowns] s
                       # hould be swapped with the rule group code [which 
                       # only have 5 codes]
                       by.x = "rule_family_code", 
                       by.y = "value_code", 
                       all.x = T) %>%
  filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
  ### START OF STEPS FROM EMAIL: ################################################
  # STEP ONE: concatenate PWSID and violation_id to create a unique identifier: 
  mutate(pwsid_viol_id = paste0(pwsid, "-", violation_id)) %>%
  # STEP TWO: remove duplicates, keep distinct records using unique ID from 
  # step 1: this helps grab one record per violation (violation ids can 
  # be duplicated and are repeated for various enforcement actions)
  distinct(pwsid_viol_id, .keep_all = T)

print("Working on Health-based violations, 10yr summary")
# please excuse the copy pasta below, just wanted to be extra cautious with 
# calculating these different year & violation type groups: 
# filtering for past 10 years of violations and for health-based: 
epa_hb_viol_simple_10yr <- viol_rulecode %>%
  # just grabbing health violations: 
  # Group the file and summarize totals
  # Group by Compliance Begin Year: COMPL_BEGIN_DATE
  # Group by Rule: RULE_CODE
  # Summarize totals
  filter(is_health_based_ind == "Y") %>%
  # Begin date of the noncompliance period during which a violation was identified. 
  # "Compliance Period Begin Date - represents the beginning of a period of time 
  # when a public water system was in violation of a primary drinking water regulation. 
  # This may also be the date when the public water system missed a monitoring 
  # event or the date when the public water system failed to complete a required 
  # action (for example, failing to install new treatment technology by a
  # required date).": https://enviro.epa.gov/enviro/EF_METADATA_HTML.sdwis_page?p_column_name=COMPL_PER_BEGIN_DATE
  # this is NOT in the data dictionary - EPA folks told us to use this field
  mutate(viol_date = as.Date(compl_per_begin_date, tryFormats = c("%m/%d/%Y")), 
         viol_year = year(viol_date)) %>%
  # filtering for past 10 years - keeping the filter the same as the helper dfs
  filter(viol_year > (year(Sys.Date())-10)) 

# summarizing by pwsid and rule: 
epa_hb_viol_10yr_summary <- epa_hb_viol_simple_10yr %>%
  group_by(pwsid, rule) %>%
  # we can safely grab rows here because we only have distinct records 
  summarize(total_health_violations_10yr = n()) %>%
  # adding note these are the number of hb rule violations over the past 10 years
  mutate(rule = paste0(rule, "_healthbased_10yr")) %>%
  # pivot wide to make it easier for analysis: 
  pivot_wider(., names_from = rule, values_from = total_health_violations_10yr) %>%
  # cleaning & replacing NAs w/ zeros, since these are true zeros: 
  janitor::clean_names() %>%
  mutate(across(everything(), ~replace_na(.x, 0))) %>%
  # summing across these columns to get total health violations over the past 
  # 10 years 
  mutate(health_viols_10yr = rowSums(across(where(is.numeric))))

# note that I need to check for systems that have been operating for <10 years, 
# but will do this at the end once we have the violation summarized in one place 

print("Working on Health-based violations, 5yr summary")
# want to do the same thing but over 5 years: #################################
# filtering for past 5 years of violations and for health-based: 
epa_hb_viol_simple_5yr <- viol_rulecode %>%
  filter(is_health_based_ind == "Y") %>%
  mutate(viol_date = as.Date(compl_per_begin_date, tryFormats = c("%m/%d/%Y")), 
         viol_year = year(viol_date)) %>%
  # filtering for past 5 years: 
  filter(viol_year > (year(Sys.Date())-5)) 

# summarizing by pwsid and rule: 
epa_hb_viol_5yr_summary <- epa_hb_viol_simple_5yr %>%
  group_by(pwsid, rule) %>%
  summarize(total_health_violations_5yr = n()) %>%
  # adding note these are the number of rule violations over the past 10 years
  mutate(rule = paste0(rule, "_healthbased_5yr")) %>%
  # pivot wide to make it easier for analysis: 
  pivot_wider(., names_from = rule, values_from = total_health_violations_5yr) %>%
  # cleaning & replacing NAs w/ zeros, since these are true zeros: 
  janitor::clean_names() %>%
  mutate(across(everything(), ~replace_na(.x, 0))) %>%
  # summing across these columns to get total health violations over the past 
  # 5 years 
  mutate(health_viols_5yr = rowSums(across(where(is.numeric))))


# okay let's merge these two violation datasets together: 
viol_disag_summary <- merge(epa_hb_viol_5yr_summary, 
                            epa_hb_viol_10yr_summary, 
                            by = "pwsid", 
                            # want to keep EVERYTHING because some water systems 
                            # might have violations over the past 5 years, but 
                            # not 10
                            all = T) %>%
  # once again all of these are true zeros
  mutate(across(everything(), ~replace_na(.x, 0)))


# alright now lets do paperwork violations ####################################
print("Working on paperwork violations, 5 & 10yr summaries")

# grabbing 5 and 10 year paperwork violations 
epa_pw_viol_simple_10yr <- viol_rulecode %>%
  filter(is_health_based_ind != "Y") %>%
  mutate(viol_date = as.Date(compl_per_begin_date, tryFormats = c("%m/%d/%Y")), 
         viol_year = year(viol_date)) %>%
  # filtering for past 10 years: 
  filter(viol_year > (year(Sys.Date())-10)) %>%
  group_by(pwsid) %>%
  summarize(total_paperwork_violations_10yr = n())

epa_pw_viol_simple_5yr <- viol_rulecode %>%
  filter(is_health_based_ind != "Y") %>%
  mutate(viol_date = as.Date(compl_per_begin_date, tryFormats = c("%m/%d/%Y")), 
         viol_year = year(viol_date)) %>%
  # filtering for past 10 years: 
  filter(viol_year > (year(Sys.Date())-5)) %>%
  group_by(pwsid) %>%
  summarize(total_paperwork_violations_5yr = n())

paperwork_viols_summary <- merge(epa_pw_viol_simple_5yr, 
                                 epa_pw_viol_simple_10yr, 
                                 by = "pwsid", all = T)

# mergin again!
viol_year_summaries <- merge(viol_disag_summary, paperwork_viols_summary, 
                             by = "pwsid", 
                             all = T) %>%
  # once again all of these are true zeros
  mutate(across(everything(), ~replace_na(.x, 0)), 
         # and I want to calculate total violations over the past 5 and 10 years
         total_viols_5yr = total_paperwork_violations_5yr + health_viols_5yr, 
         total_viols_10yr = total_paperwork_violations_10yr + health_viols_10yr) 

# what about tier 1 public notice violations? ##################################
print("Working on tier 1 PN violations")
# In tier 1, the water system would  have 24 hours to report violation to 
# customers (some/most would probably be boil water notices) 
# NOTE - these are NOT filtered by healthbased, but based on 
# any(tier_one_10yr$is_health_based_ind != "Y") == FALSE, these are all health
# based 
tier_one_10yr <- viol_rulecode %>%
  filter(public_notification_tier == 1) %>%
  mutate(viol_date = as.Date(compl_per_begin_date, tryFormats = c("%m/%d/%Y")), 
         viol_year = year(viol_date)) %>%
  # filtering for past 10 years: 
  filter(viol_year > (year(Sys.Date())-10)) %>%
  group_by(pwsid) %>%
  summarize(tier_1_pn_10yr = n()) 

tier_one_5yr <- viol_rulecode %>%
  filter(public_notification_tier == 1) %>%
  mutate(viol_date = as.Date(compl_per_begin_date, tryFormats = c("%m/%d/%Y")), 
         viol_year = year(viol_date)) %>%
  # filtering for past 10 years: 
  filter(viol_year > (year(Sys.Date())-5)) %>%
  group_by(pwsid) %>%
  summarize(tier_1_pn_5yr = n()) 

# yeehaw - mergin!
tier_one_viols <- merge(tier_one_5yr, 
                        tier_one_10yr, by = "pwsid", 
                        all = T)

viol_year_summaries_pn <- merge(viol_year_summaries, tier_one_viols, 
                                by = "pwsid", all = T) %>% 
  mutate(across(everything(), ~replace_na(.x, 0))) %>%
  # doing some reorg for clarity: 
  relocate(c(tier_1_pn_5yr, total_paperwork_violations_5yr, total_viols_5yr), .after = health_viols_5yr) %>%
  relocate(c(tier_1_pn_10yr, total_paperwork_violations_10yr, total_viols_10yr), .after = health_viols_10yr)

# alright now lets do all years & open violations ##############################
# we probably won't use this, but nice to have in our back pocket 
# NOTE - we don't want to overwrite these when the system has been operating < 
# a certain number of years 

# grabbing total violations over all of SDWIS: 
total_viols <- viol_rulecode %>% 
  group_by(pwsid) %>%
  summarize(violations_all_years = n(), 
            health_violations_all_years = sum(is_health_based_ind == "Y"))

# grabbing list of PWSIDs with open health violations 
open_viol <- viol_rulecode %>%
  filter(is_health_based_ind == "Y") %>%
  # Note - the other options here are "resolved" or "archived" - archived 
  # means that the violation no longer contributes to overall compliance status
  # addressed means the violation has a formal enforcement action, but is not 
  # resolved or archived. 
  filter(violation_status %in% c("Addressed", "Unaddressed"))


print("Bringing all datasets together")
# merging this back with the full violation data
viol_final_summary <- merge(viol_year_summaries_pn, 
                            total_viols, by = "pwsid", all = T) 

# combining this back with water system information 
ws_viol_final <- merge(sdwis_ws_info_tidy, viol_final_summary, by = "pwsid", all = T) %>%
  # identifying systems with open violations 
  mutate(open_health_viol = case_when(
    pwsid %in% open_viol$pwsid ~ "Yes", 
    TRUE ~ "No")) %>%
  # translating NAs for violation data to true zeroes 
  mutate(across(lead_and_copper_rule_healthbased_5yr:health_violations_all_years, 
                ~replace_na(.x, 0))) %>%
  # transforming to character so I can handle systems that have been operating 
  # for less than 5 or 10 years 
  mutate(across(everything(), as.character)) %>%
  mutate(across(ends_with("_5yr"), ~ifelse(pwsid %in% less_5_years$pwsid,
                                           "Not Enough Data - Operating < 5 years", .x))) %>% 
  mutate(across(ends_with("_10yr"), ~ifelse(pwsid %in% less_10years$pwsid, 
                                            "Not Enough Data - Operating < 10 years", .x))) %>%
  rename(paperwork_viols_5yr = total_paperwork_violations_5yr, 
         paperwork_viols_10yr = total_paperwork_violations_10yr, 
         health_viols_all_years = health_violations_all_years)
  
# adding a check if df is empty: 
if(nrow(ws_viol_final) == 0){
  # throwing message and shutting down worker
  print("Summarized Data Contain 0 Rows - Shutting Down Worker")
  quit(save = "no")
}

# pushing SDWIS data ##########################################################
print("Adding Cleaned Dataset to S3")
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/national/sdwis_viols.csv"
# ugh this is so ugly but the only way I can think to note how there are multiple 
# raw files that get summarized into a clean dataset 
raw_s3_link <- "Multiple - s3://tech-team-data/national-dw-tool/raw/national/water-system/sdwa_pub_water_systems.csv; s3://tech-team-data/national-dw-tool/raw/national/water-system/sdwa_violations_enforcement.csv; s3://tech-team-data/national-dw-tool/raw/national/water-system/sdwa_ref_codes.csv"

# adding to s3
tmp <- tempfile()
write.csv(ws_viol_final, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = clean_s3_link,
  bucket = "tech-team-data",
  multipart = T
)

# checkin - sick 
# test <- aws.s3::s3read_using(read.csv,
#                              object = "s3://tech-team-data/national-dw-tool/clean/national/sdwis_viols.csv")

# updating task manager ########################################################
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
