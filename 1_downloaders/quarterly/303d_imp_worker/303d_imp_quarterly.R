###############################################################################
# RMP Sites Quarterly Worker
###############################################################################

# libraries needed: 
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(arcpullr)

# no scientific notation 
options(scipen = 999)
options(timeout = 900) # this should bump to 15 mins 

# make sure to specify the correct bucket region for IAM role: 
Sys.setenv("AWS_DEFAULT_REGION" = 'us-east-1')

# for logs: 
print("I'm running!")

# pulling in task manager for updating relevant sections: 
task_manager <- aws.s3::s3read_using(read.csv, 
                                     object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv")%>%
  mutate(across(everything(), ~ as.character(.)))

# for some of the coding bits: 
dataset_i <- "303d_imp_waters"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/national/environmental/impaired_waters.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/national/huc12_impaired_waters.csv"

# Finding 303d Impaired Waters #################################################
# for logs: 
print("on: environmental datasets - 303(d) impaired waters")
# NOTE based on the interactive map, data varies by state !!!!
# data also exist here but the download is the entire dataset & breaks in R: 
# https://catalog.data.gov/dataset/enviroatlas-303d-impairments-by-12-digit-huc-for-the-conterminous-united-states3
# https://enviroatlas.epa.gov/enviroatlas/interactivemap/
# if you select impaired waters, right click the legend and click "web services" 
# it takes you to this link: https://gispub.epa.gov/arcgis/rest/services/OW/ATTAINS_Assessment/MapServer/1
# but this is the raw line data (and took forever) and I want summaries huc12, 
# which should actually be this layer: 
# " State water quality assessment decisions reported to EPA under the 
# Integrated Report (IR), and Clean Water Act Sections 303(d) and 305(b). 
# This service provides summary information for each Assessment Unit. For more 
# detailed data, please reference the Assessment Total Maximum Daily Load (TMDL) 
# Tracking and Implementation System (ATTAINS) web services. Information on those 
# web services is provided on the public ATTAINS website: https://www.epa.gov/waterdata/attains"
imp_waters_url <- "https://gispub.epa.gov/arcgis/rest/services/OW/ATTAINS_Assessment/MapServer/7"
print("Pulling imp_waters")
imp_waters <- get_table_layer(imp_waters_url)
# this can take like 6-20 mins to load 
# write.csv(imp_waters, "./data/imp_waters_huc12.csv") <- writing for easy access 
# during data exploration phase 
print("Wrapped pulling imp_waters, starting to summarize")

# adding test if geojson is empty: 
if (nrow(imp_waters) == 0){
  # throwing message and shutting down worker
  print("New Data Contain 0 Rows - Shutting Down Worker")
  
  # create dataframe with cleaner info added
  task_manager_df <- data.frame(dataset = dataset_i, 
                                date_downloaded = Sys.Date(), 
                                raw_link = "WORKER FAILED - EMPTY DATA",
                                clean_link = "WORKER FAILED - EMPTY DATA")
  
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


# updating dataset in S3 ######################################################
print("Updating S3 with raw data")

# writing this to s3: 
tmp <- tempfile()
write.csv(imp_waters, paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link,
  bucket = "tech-team-data",
  multipart = T
)

# test <- aws.s3::s3read_using(read.csv, 
#                              object = raw_s3_link)
# cleaning the data#############################################################
print("Cleaning & Summarizing Impaired Waters")

# checking out duplicates: ah, these are streams that extend beyond a single 
# HUC, so we should keep them when doing our huc12 summary!
# dups <- imp_waters[duplicated(imp_waters$assessmentunitidentifier),]
# imp_waters_dups <- imp_waters %>%
#   filter(assessmentunitidentifier %in% dups$assessmentunitidentifier)

# checking out metadata: 
# imp_metadata <- get_table_layer("https://gispub.epa.gov/arcgis/rest/services/OW/ATTAINS_Assessment/MapServer/10")
# isassessed = If the state has monitored a water and made an Assessment decision
#     about the Assessment Unit, it is considered Assessed.  If the state has 
#     defined the Assessment Unit but has not yet monitored and assessed it, 
#     then it is Not Assessed.
# impaired = If any part of the Assessment Unit fails to meet its water quality
#     standards, it is calculated as impaired.
# threatened = Threatened means that one or more Uses is Fully Supporting but 
#     experiencing a declining trend and likely to become impaired in the next 
#     reporting cycle.  Waters that are Threatened are part of the Clean Water 
#     Act Section 303(d) list, unless a TMDL has been created for them.  A null
#     value is the same as isThreatened = 'N'.
# 303d list: If the Assessment Unit is impaired by a pollutant and still needs 
#     to be addressed by a TMDL or other pollution control measure, it falls on 
#     the Clean Water Act (CWA) Section 303(d) List (which is also known as EPA 
#     IR Category 5).  Note:  This does not include all impaired waters.  For 
#     example, Assessment Units that are impaired but already have a TMDL would
#     fall into EPA IR Category 4a, and would not be on the CWA Section 303(d) list.
imp_waters_summary <- imp_waters %>%
  group_by(huc12) %>%
  summarize(assessed_streams = sum(isassessed == "Y"), 
            not_assessed = sum(isassessed == "N"),
            impaired_streams = sum(isimpaired == "Y"), 
            threatened_streams = sum(isthreatened == "Y"), 
            # NOTE - this does not include all impaired waters - units that are 
            # impaired but have a TMDL would not be on this list
            streams_303d_list = sum(on303dlist == "Y")) %>%
  mutate(last_epic_run_date = Sys.Date())


# checking with the past dataset that was downloaded 
# test <- aws.s3::s3read_using(read.csv, 
#                              object = raw_s3_link)

# relating data to huc12s ######################################################
print("Writing clean dataset to s3")

tmp <- tempfile()
write.csv(imp_waters_summary, paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = clean_s3_link,
  bucket = "tech-team-data",
  multipart = T
)

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

# select the other columns from the task manager that we do not want to overwrite: 
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

