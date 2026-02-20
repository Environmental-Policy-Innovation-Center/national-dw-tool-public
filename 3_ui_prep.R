# libraries!
library(aws.s3)
library(tidyverse)
library(googlesheets4)
library(skimr)
library(sf)
library(googlesheets4)
library(tigris)

# options for scientific notation 
options(scipen = 99999999)
options(tigris_use_cache = TRUE)

# read in full data inventory from task manager spreadsheet
data_inventory <- read_sheet("https://docs.google.com/spreadsheets/d/15iVYq2v3Gpy5Zug3BhYC0vU4L-axV5g0drZt4-uLv-Q/edit?gid=1871103764#gid=1871103764", 
                             sheet = "full_data_inventory")
# filtering for just the ones in the tool & 
# grabbing data scores so some of the manual estimates can get folded into 
# data qual score: 
data_inventory_filt <- data_inventory %>%
  filter(nchar(`use_in_tool?`) != 0) %>%
  # minor string cleaning for main columns used in this dataset: 
  mutate(clean_name = trimws(clean_name), 
         data_score_coverage = trimws(data_score_coverage), 
         `do_dups_matter?` = trimws(str_to_lower(`do_dups_matter?`)), 
        round_digits = as.integer(str_squish(round_digits)))

# quick check to make sure all of these columns are filled out 
cols_of_interest <- data_inventory_filt %>% 
  select(clean_name:data_score_coverage)
if(any(is.na(cols_of_interest))){
  print("EMPTY COLUMNS IDENTIFIED - FIX")
} else {
  print("CHECKS PASSED - GOOD TO CONTINUE :)")
}

# read in our functions for qual checks: 
source("./functions/data_qual_checks.R")

## Reading in datasets ########################################################
# reading in national water system: 
national_water_system <- s3read_using(readRDS,
                                      object = "s3://tech-team-data/national-dw-tool/clean/national/national_water_system.RData")
# reading in national socioeconomic: 
national_socioeconomic <- s3read_using(readRDS,
                                       object = "s3://tech-team-data/national-dw-tool/clean/national/national_socioeconomic.RData")
# reading in national environmental: 
national_environmental <- s3read_using(readRDS,
                                       object = "s3://tech-team-data/national-dw-tool/clean/national/national_environmental.RData")
# reading in national bwn: 
national_bwn <- s3read_using(readRDS,
                             object = "s3://tech-team-data/national-dw-tool/clean/national/national_bwn.RData")

## running variable qual checks ################################################
# code below for testing:
# rds_list <- national_water_system
# data_inven <- data_inventory_filt

# run qual checks on national water system & sabs: 
ws_qual_var <- run_var_checks(national_water_system, data_inventory_filt)
sabs <- national_water_system[[1]] 
sf_use_s2(F)
epa_sabs_var <- run_var_checks(sabs, data_inventory_filt)
sf_use_s2(T)

# running socio:bwn checks 
socio_var_checks <- run_var_checks(national_socioeconomic, data_inventory_filt) 
enviro_var_checks <- run_var_checks(national_environmental, data_inventory_filt)
bwn_var_checks <- run_var_checks(national_bwn, data_inventory_filt)

# full var checks! 
var_checks <- bind_rows(ws_qual_var, epa_sabs_var, socio_var_checks, 
                        enviro_var_checks, bwn_var_checks) %>%
  arrange(dataset) %>%
  relocate(data_score_overlaps, .before = auto_data_score)

# update variable_summary in google sheets #####################################

# this overwrites the whole thing, which is fine because no other part of the 
# data pipeline would touch this sheet
range_write("https://docs.google.com/spreadsheets/d/15iVYq2v3Gpy5Zug3BhYC0vU4L-axV5g0drZt4-uLv-Q/edit?gid=682082669#gid=682082669",
            data = var_checks,
            sheet = "variable_summary",
            range = "variable_summary!A1")

# run data summary checks #####################################################
# code below for testing:
# rds_list <- national_bwn
# data_inven <- data_inventory_filt
# var_qual_checks <- bwn_var_checks
# var_score_i <- bwn_var_checks
# add_to_s3 = F
# test <- s3read_using(read.csv,
#                      object = "s3://tech-team-data/national-dw-tool/test-staged/sdwis_viols.csv")

# NOTE you also can just run "run_data_checks" and if the var_qual_checks argument is 
# empty, it'll run the var checks for you!
ws_qual_ds <- run_data_checks(national_water_system, data_inventory_filt, 
                              ws_qual_var, add_to_s3 = T)
# ^ I previously had strings as placeholders for some NAs in the sdwis_viols 
# and pwsid_summarized_funding data to note water systems that have been 
# operating for less than 5 or 10 years, or where IUPs had not been scraped. 
# Since this made rounding tricky, I converted these back to NAs in the staged
# data. 

## unfortunately this function is still separate for SABs, but could brainstorm 
# how this could be merged with run_data_checks in the future 
# code for testing: 
# sab <- sabs
# data_inven <- data_inventory_filt
# var_qual_checks <- epa_sabs_var
# add_to_s3 = F
epa_sabs_df <- run_sabs_df_checks(sabs, data_inventory_filt, epa_sabs_var, 
                                  add_to_s3 = T)
socio_qual_ds <- run_data_checks(national_socioeconomic, data_inventory_filt, 
                                 socio_var_checks, 
                                 add_to_s3 = T)
enviro_qual_ds <- run_data_checks(national_environmental, data_inventory_filt, 
                                  enviro_var_checks, 
                                  add_to_s3 = T)
bwn_qual_ds <- run_data_checks(national_bwn, data_inventory_filt, 
                               bwn_var_checks, 
                               add_to_s3 = T)

# just checking the right stuff gets added to s3
# test <- aws.s3::s3read_using(st_read,
#                              object = "s3://tech-team-data/national-dw-tool/test-staged/epa_sabs_geoms.geojson")
# test <- aws.s3::s3read_using(read.csv,
 #                             object = "s3://tech-team-data/national-dw-tool/test-staged/national_bwn_summary.csv")

# bindin'
data_summary_checks <- bind_rows(ws_qual_ds, epa_sabs_df, socio_qual_ds,
                                 enviro_qual_ds, bwn_qual_ds) %>%
  arrange(dataset)

# if you only updated one: 
# data_summary_checks <- ws_qual_ds %>%
#   arrange(dataset)

# update task manager: #########################################################
task_manager_enviro <- aws.s3::s3read_using(read.csv, 
                                            object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv") %>%
  mutate(across(everything(), ~ as.character(.)))

# don't touch anything not in the data_summary_checks list
dont_touch_these_columns <- setdiff(names(task_manager_enviro), 
                                    names(data_summary_checks))
task_manger_simple <- task_manager_enviro %>% 
  select(dataset, all_of(dont_touch_these_columns))

# merge them together to create the updated row
updated_row <- merge(task_manger_simple, 
                     data_summary_checks, by = "dataset", all.y = T) %>%
  mutate(across(everything(), ~ as.character(.)))

# bind this row back to the task manager
updated_task_manager <- task_manager_enviro %>%
  filter(!(dataset %in% updated_row$dataset)) %>% 
  bind_rows(., updated_row) %>%
  arrange(dataset) %>%
  relocate(mean_data_qual_score, .after = summarized_list_link)

# have the epa_sabs_geoms mirror the epa_sabs info in the task manger: 
epa_sabs_tm_info <- updated_task_manager %>% 
  filter(dataset == "epa_sabs") %>% select(update_freq:summarized_list_link)

# removing old entries that might've been staged in the past, but are no
# longer staged: 
updated_task_manager_final <- updated_task_manager %>%
  mutate(across(mean_data_qual_score:staged_link, ~ if_else(is.na(mean_data_qual_score), NA, .)))
  # # urg this is pretty gross code, but this should match the epa_sabs vintage
  # mutate(date_downloaded = case_when(dataset == "epa_sabs_geoms" ~ epa_sabs_tm_info$date_downloaded, 
  #                                    TRUE ~ date_downloaded), 
  #        raw_link = case_when(dataset == "epa_sabs_geoms" ~ epa_sabs_tm_info$raw_link, 
  #                             TRUE ~ raw_link),
  #        clean_link = case_when(dataset == "epa_sabs_geoms" ~ epa_sabs_tm_info$clean_link, 
  #                               TRUE ~ clean_link),
  #        date_summarized = case_when(dataset == "epa_sabs_geoms" ~ epa_sabs_tm_info$date_summarized, 
  #                                    TRUE ~ date_summarized),
  #        summarized_list_link = case_when(dataset == "epa_sabs_geoms" ~ epa_sabs_tm_info$summarized_list_link, 
  #                                         TRUE ~ summarized_list_link))

# write back to s3: 
tmp <- tempfile()
write.csv(updated_task_manager_final, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv",
  acl = "public-read"
)

# If this is the last update before the next version of the app, 
# grab a snapshot of this task manager version, but don't add it to the staged 
# folder (will be summarized and linked in the readme, adding it to the zipped 
# folder would be overkill)
version_date <- "Feb-05-2026"
write.csv(updated_task_manager_final,
          paste0("./data/task_manager_data_summary_", version_date, ".csv"))

###### staging step done!! #####################################################


###### Organizing data for sharing #############################################
# prepping a folder to store components: 
folder_path <- paste0("./data/staged_data/national-dw-tool-", version_date)
dir.create(folder_path)

# grab the staged links from the task manager 
# task_manager_copy <- read.csv(paste0("./data/task_manager_data_summary_", version_date, ".csv"))
task_manager_copy <- s3read_using(read.csv, object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv")
staged_links <- task_manager_copy$staged_link
staged_links_clean <- staged_links[!is.na(staged_links)]

# file 1: EPA SABs in a geojson, with just water system ID and boundary: 
epa_sabs_geo_link <- staged_links_clean[grepl("geojson", staged_links_clean)]
epa_sabs_geojson <- aws.s3::s3read_using(st_read, object = epa_sabs_geo_link)

# file 2: all other staged files merged by pwsid 
all_other_staged <- staged_links_clean[!grepl("geojson|national_bwn_summary|pwsid_npdes_usts_rmps_imp|pwsid_summarized_funding_data",
                                              staged_links_clean)]

# creating a clean df to merge to, with just pwsids: 
pwsid_df <- epa_sabs_geojson %>%
  as.data.frame() %>%
  select(pwsid)

# looping through each dataset and merging to pwsid dataset 
for(i in 1:length(all_other_staged)) {
  # select the correct dataset link
  dataset_i <- all_other_staged[i]
  print(paste0("On dataset: ", dataset_i))
  
  # read from aws
  df <- aws.s3::s3read_using(read.csv, object = dataset_i)
  # merge with main df: 
  pwsid_df <<- merge(pwsid_df, df, by = "pwsid", all.x = T)
}

# file 3: the huc12 dataset is separate, because water systems are duplicated by 
# the huc12s they pull from: 
pwsid_environmental <- aws.s3::s3read_using(read.csv, 
                                            object = "s3://tech-team-data/national-dw-tool/test-staged/pwsid_npdes_usts_rmps_imp.csv")


# add these three datasets to a folder - this should have: 
# - epa_sabs.geojson - contains the latest epa service area boundaries 
#     provided in the tool, as a geojson. The file can be merged with the 
#     two below using the "pwsid" field. 
st_write(epa_sabs_geojson, paste0(folder_path, "/epa_sabs.geojson"))

# - pwsid-water-socio-df.csv - contains the summary water system 
#     and socioeconomic infomation of the population served by water system ID
write.csv(pwsid_df, paste0(folder_path, "/pwsid-water-socio-df.csv"))

# - pwsid-enviro-df.csv - contains the summary potential environmental 
#     hazard information by the subwatershed the water system is collecting 
#     water from. 
write.csv(pwsid_environmental, paste0(folder_path, "/pwsid-enviro-df.csv"))


# Now we need the data dictionary w/ timestamps and methods: 
# NOTE - did this manually on 2/20 

# read in full data inventory from task manager spreadsheet
data_inventory <- read_sheet("https://docs.google.com/spreadsheets/d/15iVYq2v3Gpy5Zug3BhYC0vU4L-axV5g0drZt4-uLv-Q/edit?gid=1871103764#gid=1871103764", 
                             sheet = "full_data_inventory")
# filtering for just the ones in the tool & 
# grabbing data scores so some of the manual estimates can get folded into 
# data qual score: 
data_inventory_staged <- data_inventory %>% 
  filter(nchar(`use_in_tool?`) != 0) %>%
  select(-c(update_flag:round_digits))

## This code will change, but I downloaded the task manager the last time 
# the datasets were staged 
task_manager_snapshot <- read.csv(paste0("./data/task_manager_data_summary_", version_date, ".csv")) %>% 
  select(dataset, update_freq, date_downloaded, date_staged) %>%
  filter(!(dataset %in% c("pwsid_summarized_funding_data", 
                          "national_bwn_summary", "dwsrf_funding_tracker_projects", 
                          "huc12_npdes_usts_rmps_imp")))

# filter the data inventory for things currently in staged                                                                                              
data_inventory_filt <- data_inventory_staged %>%
  filter(dataset %in% task_manager_snapshot$dataset)

# merge the two together based on dataset field
data_summary <- merge(data_inventory_filt, task_manager_snapshot, 
                      by = "dataset", all = T) %>%
  select(-list)

# add data from quality checks: 
qual_checks <- read_sheet("https://docs.google.com/spreadsheets/d/15iVYq2v3Gpy5Zug3BhYC0vU4L-axV5g0drZt4-uLv-Q/edit?gid=1871103764#gid=1871103764", 
                             sheet = "variable_summary")
data_summary_f <- merge(data_summary, qual_checks, 
                        by = c("dataset", "variable"), all = T)

# add the dataset name associated with the each variable 
data_summary_tidy <- data_summary_f %>%
  select(-c(data_qual_flag, date_staged)) %>%
  mutate(data_download_name = case_when(dataset == "epa_sabs_geoms" ~ "epa_sabs.geojson",
                                        dataset == c("pwsid_npdes_usts_rmps_imp") ~ "pwsid-enviro-df.csv", 
                                        TRUE ~ "pwsid-water-socio-df.csv"))

# create the string for all of the boil water notice datasets: 
bwn_datasets <- data_summary_tidy %>%
  filter(grepl("_bwn$|la_bwa_1yr|la_bwn_5yr",  dataset)) %>%
  select(dataset, update_freq, date_downloaded) %>%
  mutate(state = str_to_upper(substr(dataset, 1, 2)), 
         tidy_date_downloaded = paste0("- ", state, " (", update_freq, ") :", 
                                       date_downloaded))

bwn_strings <- paste(unique(bwn_datasets$tidy_date_downloaded), collapse = " | ")

# filtering, renaming, etc. 
data_summary_filter <- data_summary_tidy %>%
  # remove the bwn datasets: 
  filter(!(dataset %in% bwn_datasets$dataset)) %>%
  # these are variables that get summarized into other staged datasets
  filter(!is.na(variable)) %>%
  # we only need the pwsid associated with the epa_sabs in the data dictionary 
  filter(!(variable == "pwsid" & dataset != "epa_sabs")) %>%
  # this is tracked separately in the task manager, not necessary to distinguish 
  # in the data dictionary 
  filter(dataset != "epa_sabs_geoms") %>%
  mutate(date_downloaded = case_when(dataset == "national_bwn_highlevel_summary" ~ bwn_strings, 
                                     TRUE ~ date_downloaded)) %>%
  relocate(data_download_name) %>%
  rename(raw_variable_name = variable)

# add the additional variables from the template: 
pub_data_dict_link <- "https://docs.google.com/spreadsheets/d/1nkV_p7tQwGMtShzWZX20V-MKw78TIR3swnGZmeUVn6M/edit?gid=1676182713#gid=1676182713"
pub_data_dict <- read_sheet(pub_data_dict_link, sheet = "data_dictionary")
# these are all columns I manually created
pub_data_dict_simple <- pub_data_dict %>%
  select(data_download_name, raw_variable_name, filter_name,
         tool_table_name, dataset_methods_name)

data_dict_merge <- merge(data_summary_filter, pub_data_dict_simple,
                         by = c("data_download_name", 
                                "raw_variable_name"), all = T)

# awesome - now we can export this to googlesheets! 
data_dict_merge_tidy <- data_dict_merge[, names(pub_data_dict)] %>%
  arrange(dataset_methods_name)
write.csv(data_dict_merge_tidy, paste0("./data/staged_data/public-data-dictionary-", version_date, ".csv"), row.names = F)

# TODO okay so from here there are a couple of manual steps: 
# - navigate to the public_data_downloads folder and add a new folder for 
#     the last bulk update: https://drive.google.com/drive/folders/19aylZZi97hQ3hm9gM9HISsPhXNTvK_oN
#  - add the data_dict_merge_tidy dataset to the folder you just created. Add the 
#     readme tab from the template,
#  - pull in the methods doc and add any other updates, release notes, etc. 
#       - update the links for methods link in the data dictionary
#       - update the links for data dictionary in the methods 
#  - navigate to the TEMPLATE_README in this repository ("./data/staged_data")
#     and add links to the methods and data dictionary. Make sure they're public 
#     view. 
#  - copy this file into the file that contains all of the exported dataasets 
#     you created above (I'm working on automating this - see below)
#  - continue with the rest of the code below 

# Add the readme of the files contained in the document 
# TODO - I've been wrestling with this file.copy code but to no avail 
# would be cool to have this automated, but the links also need to be updated 
# with the latest file version 
# file.copy("/Users/emmalitsai/national-dw-tool/data/staged_data/TEMPLATE_README.txt", 
# "/Users/emmalitsai/national-dw-tool/data/staged_data/")
# this contains links to the data dictionary and methods doc 

# zip the folder
folder_path = paste0("./data/staged_data/national-dw-tool-", version_date)
files <- list.files(folder_path, full.names = T)
zip_path <- paste0(folder_path, ".zip") 
zip(zip_path, files)

# TODO - create the public repo 
# TODO - pdf of the methods on aws 

# add to s3:
put_object(
  file = paste0("/Users/emmalitsai/national-dw-tool/data/staged_data/national-dw-tool-", version_date, ".zip"),
  object = paste0("s3://tech-team-data/national-dw-tool/public-data-downloads/national-dw-tool-", version_date, ".zip"),
  acl = "public-read"
)


###### create zipped files of state data #######################################
# grabbin sabs: 
epa_sabs <- aws.s3::s3read_using(st_read,
                                 object = "s3://tech-team-data/national-dw-tool/clean/national/epa_sabs.geojson")

state_boundaries <- states() %>%
  st_transform(., crs = st_crs(epa_sabs))
epa_sabs_centroid <- epa_sabs %>%
  st_centroid() %>%
  select(pwsid)

epa_sabs_states <- st_intersection(epa_sabs_centroid, state_boundaries) %>%
  unique()

# TODO - pull in this: s3://tech-team-data/national-dw-tool/public-data-downloads/national-dw-tool-Feb-05-2026.zip

for(i in 1:nrow(state_boundaries)) {
  state_i <- state_boundaries[i,]$STUSPS
  print(state_i)
  epa_sabs_i <- epa_sabs_states %>%
    filter(state_i %in% STUSPS)
  
  # TODO - filter epa_sabs, 
  # TODO - filter environmental 
  # TODO - filter pwsid df 
  
  # TODO - zip 
  # TODO - push to s3 in an organized way 

}

###### QA/QC checks with data in staged #######################################
## Jan 12, 2026
national_water_system <- s3read_using(readRDS,
                                      object = "s3://tech-team-data/national-dw-tool/clean/national/national_water_system.RData")
national_socioeconomic <- s3read_using(readRDS,
                                       object = "s3://tech-team-data/national-dw-tool/clean/national/national_socioeconomic.RData")
sabs <- national_water_system$epa_sabs
viols <- national_water_system$sdwis_viols
sabs_viols <- merge(sabs, viols, by = "pwsid", all = T)
xwalk <- national_socioeconomic$epa_sabs_xwalk

# filtering for PA based on sabs centroids: 
sabs_centroid_pa <- sabs %>%
  filter(grepl("PA", epic_states_intersect)) %>%
  st_centroid() 
state_pa <- states() %>%
  filter(STUSPS == "PA") %>%
  st_transform(., crs = st_crs(sabs_centroid_pa))

sabs_centroid_pa <- st_intersection(sabs_centroid_pa, state_pa)

# test one: the number of CWS in PA w/ open health-based violations
# staged = 1,853 CWS; 63 open health 
# when filtering based on centroid: 
# - 1,853 cws; 63 open health based violations 
test <- sabs_viols %>%
  as.data.frame() %>%
  filter(pwsid %in% sabs_centroid_pa$pwsid)
sum(test$open_health_viol == "Yes") # 63 - check


# test two: the number of private CWS in PA w/ % POC > 8% but <= 18%
private <- sabs_viols %>%
  as.data.frame() %>%
  filter(pwsid %in% sabs_centroid_pa$pwsid) %>%
  filter(owner_type == "Private") 
# count() # this is 1038 

private_socio <- private %>%
  left_join(., xwalk, by = "pwsid")
private_socio %>%
  filter(poc_alone_per <= 18 & poc_alone_per > 8) %>%
  count() 
# this is 182, which lines up with the SABs summary box but when 
# you sum the individual bars in the histogram you get 364....why?

# test three: the number of loally CWS in PA w/ % POC <= 20%
local <- sabs_viols %>%
  as.data.frame() %>%
  filter(pwsid %in% sabs_centroid_pa$pwsid) %>%
  filter(owner_type == "Local") 

local_socio <- local %>%
  left_join(., xwalk, by = "pwsid") %>%
  filter(poc_alone_per <= 20) 
# count() # this is 670

ggplot(local_socio, aes(x = poc_alone_per)) + 
  geom_histogram(binwidth = 2)

