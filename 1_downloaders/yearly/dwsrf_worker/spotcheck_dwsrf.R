source("./functions/check_env.R")
ENV <- check_env()

###############################################################################
# DWSRF Compare Existing vs New Files
###############################################################################

library(aws.s3)
library(readr)
library(dplyr)
library(lubridate)
library(janitor)
library(stringr)

# Note: "new" refers to the csv pulled using chromote from dwsrf_worker.R
# "original" refers to the existing csv file
# I'm comparing the raw files for both since the cleaned original file has some
# other pwsid filtering.

# Write new raw data to development bucket
dw_new_raw <- read.csv("Drinking Water Assistance Agreement Detail Report_20260417.csv")
dw_new_raw_link <- "s3://tech-team-data/national-dw-tool/development/dwsrf_funded_projects_test_raw.csv"
tmp <- tempfile()
write.csv(dw_new_raw, paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = dw_new_raw_link,
  bucket = "tech-team-data",
  multipart = T
)

# Read original clean data
# dw_original_clean_link <- "s3://tech-team-data/national-dw-tool/clean/national/dwsrf_funded_projects.csv"
# dw_original_clean <- aws.s3::s3read_using(read.csv, 
#                                    object = dw_original_clean_link)
# dw_original_clean <- dw_original_clean %>%
#   mutate(initial_agreement_date = mdy(initial_agreement_date))

# Read and Clean original raw data #########################
# Read original raw data
dw_original_raw_link <- "s3://tech-team-data/national-dw-tool/raw/national/water-system/Drinking_Water_Assistance_Agreement_Detail_Report_20260216- REPORT.csv"
dw_original_raw <- aws.s3::s3read_using(read.csv, 
                                   object = dw_original_raw_link) %>%
  # first three rows are just headers
  slice(-(1:3)) 
# column names are now the first row
colnames(dw_original_raw) <- dw_original_raw[1,] 

# Do basic cleaning steps (ignoring the pwsid filtering) to ensure the files are the same
dw_original_clean <- dw_original_raw %>% 
  # remove extra row 
  slice(-1) %>%
  janitor::clean_names()
###########################################################################

# Read and Clean new raw data #########################
dw_new_clean <- dw_new_raw %>%
  # first four rows are just headers
  slice(-(1:3))
# column names are now the first row
colnames(dw_new_clean) <- dw_new_clean[1,]

dw_new_clean <- dw_new_clean %>% 
  # remove extra row 
  slice(-1) %>%
  janitor::clean_names() %>%
  mutate(initial_agreement_date = mdy(initial_agreement_date),
         state_tracking_number = as.character(state_tracking_number)) %>%
  filter(initial_agreement_date >= as.Date("2020-01-01"))

# Spotchecks ##################################################
nrow(dw_new_clean)
nrow(dw_original_clean)

dw_new_sorted <- dw_new_clean %>% arrange(across(everything()))
dw_original_sorted <- dw_original_clean %>% arrange(across(everything()))

identical(dw_new_sorted, dw_original_sorted)
all.equal(dw_new_sorted, dw_original_sorted)

missing_from_original <- anti_join(dw_new_clean, dw_original_clean, by = "state_tracking_number")
missing_from_new <- anti_join(dw_original_clean, dw_new_clean, by = "state_tracking_number")

new_columns <- colnames(dw_new_clean)
original_columns <- colnames(dw_original_clean)
