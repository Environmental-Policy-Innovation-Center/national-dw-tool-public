# Update Task Manager Google Spreadsheet #######################################
# A set of functions to complete manual updates to data_summary in the 
# task manager, and to update data inventory 

# libraries: 
library(tidyverse)
library(aws.s3)
library(googlesheets4)
library(purrr)


#  Update Manual Sections of Task Manager Data Summary #########################
manual_task_manager_updates <- read.csv("./data/task_manager_data_summary_manual.csv")

# update task manager: 
task_manager_enviro <- aws.s3::s3read_using(read.csv, 
                                            object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv")%>%
  mutate(across(everything(), ~ as.character(.)))

# don't touch anything that is not in the _manual.csv file: 
dont_touch_these_columns <- setdiff(names(task_manager_enviro), 
                                    names(manual_task_manager_updates))
# isolate these columns 
task_manger_simple <- task_manager_enviro %>% 
  select(dataset, all_of(dont_touch_these_columns))

# merge them together to add the updated columns 
updated_cols <- merge(task_manger_simple, 
                      manual_task_manager_updates, by = "dataset") %>%
  mutate(across(everything(), ~ as.character(.))) %>%
  # do some quick reorg
  relocate(update_freq, .after = dataset) %>%
  relocate(notes, .after = staged_link) 

# write back to s3: 
tmp <- tempfile()
write.csv(updated_cols, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv",
  acl = "public-read"
)


# Data Dictionary Updates ######################################################
# updating national water system: 
national_water_system <- s3read_using(readRDS,
                                      object = "s3://tech-team-data/national-dw-tool/clean/national/national_water_system.RData")

# run though each list item and grab the columns
list_names <- names(national_water_system)
column_names <- lapply(national_water_system, colnames) 

data_dict_ws <- data.frame(
  rds_list = rep("national_water_system"),
  list_tem = rep(names(national_water_system), sapply(column_names, length)),
  var = unlist(column_names)
)
row.names(data_dict_ws) <- NULL


# updating national socioeconomic: 
national_socioeconomic <- s3read_using(readRDS,
                                       object = "s3://tech-team-data/national-dw-tool/clean/national/national_socioeconomic.RData")
list_names <- names(national_socioeconomic)
column_names <- lapply(national_socioeconomic, colnames) 

# run though each list item and grab the columns
data_dict_socio <- data.frame(
  rds_list = rep("national_socioeconomic"),
  list_tem = rep(names(national_socioeconomic), sapply(column_names, length)),
  var = unlist(column_names)
)
row.names(data_dict_socio) <- NULL


# updating national environmental: 
national_environmental <- s3read_using(readRDS,
                                       object = "s3://tech-team-data/national-dw-tool/clean/national/national_environmental.RData")

# run though each list item and grab the columns
list_names <- names(national_environmental)
column_names <- lapply(national_environmental, colnames) 

data_dict_enviro <- data.frame(
  rds_list = rep("national_environmental"),
  list_tem = rep(names(national_environmental), sapply(column_names, length)),
  var = unlist(column_names)
)
row.names(data_dict_enviro) <- NULL


# updating bwn: 
national_bwn <- s3read_using(readRDS,
                             object = "s3://tech-team-data/national-dw-tool/clean/national/national_bwn.RData")

# run though each list item and grab the columns
list_names <- names(national_bwn)
column_names <- lapply(national_bwn, colnames) 

data_dict_bwn <- data.frame(
  rds_list = rep("national_bwn"),
  list_tem = rep(names(national_bwn), sapply(column_names, length)),
  var = unlist(column_names)
)
row.names(data_dict_bwn) <- NULL


# binding and adding to google sheets
results_f <- rbind(data_dict_ws, data_dict_socio, 
                   data_dict_enviro, data_dict_bwn) %>%
  rename(list = rds_list, 
         dataset = list_tem, 
         variable = var)

# check this against what's in the task manager: ######
data_inventory_gs <- read_sheet("https://docs.google.com/spreadsheets/d/15iVYq2v3Gpy5Zug3BhYC0vU4L-axV5g0drZt4-uLv-Q/edit?gid=153037363#gid=153037363", 
                                sheet = "full_data_inventory")

# don't touch anything that is not in the results_f dataframe: 
dont_touch_these_columns <- setdiff(names(data_inventory_gs), 
                                    names(results_f))

# isolate these columns 
data_inventory_dont_touch <- data_inventory_gs %>% 
  select(list, dataset, variable, all_of(dont_touch_these_columns))

# merge them together to add the updated columns 
updated_data_inventory <- merge(data_inventory_dont_touch, 
                                results_f, 
                                by = c("list", "dataset", "variable"), 
                                all = T, 
                                sort = F) %>%
  mutate(across(everything(), ~ as.character(.))) 

# I also want to add a flag for new columns (in results_f, but not data inventory), 
# and columns that may be old (in data inventory but not results_f)
# # generate full id: 
results_id <- results_f %>%
  mutate(id = paste0(list, dataset, variable))
data_inventory_id <- data_inventory_gs %>%
  mutate(id = paste0(list, dataset, variable))

# find presence of these IDs in the two dfs above, and throw a flag: 
updated_data_inventory_flag <- updated_data_inventory %>%
  mutate(id = paste0(list, dataset, variable)) %>%
  mutate(update_flag = case_when(id %in% results_id$id & !(id %in% data_inventory_id$id) ~ "New Variable - Info Needed", 
                                 id %in% data_inventory_id$id & !(id %in% results_id$id) ~ "Old Variable - Delete?", 
                                 TRUE ~ NA)) %>%
  select(-id)

# update!!! 
gs4_auth()
write_sheet(updated_data_inventory_flag, 
            ss = "https://docs.google.com/spreadsheets/d/15iVYq2v3Gpy5Zug3BhYC0vU4L-axV5g0drZt4-uLv-Q/edit?gid=2096327024#gid=2096327024", 
            sheet = "full_data_inventory")

# keep hyperlinks - should only need to run this code once, and then it can be 
# updated manually from here 
# old_inventory <- range_read_cells(ss = "https://docs.google.com/spreadsheets/d/15iVYq2v3Gpy5Zug3BhYC0vU4L-axV5g0drZt4-uLv-Q/edit?gid=2096327024#gid=2096327024",
#                                   sheet = "old_full_data_inventory", 
#                                   range = "A1:G690", 
#                                   cell_data = "full")
# 
# # source: https://stackoverflow.com/questions/78349603/how-to-read-hyperlinks-from-range-read-cells-in-googlesheets4
# extract_hyperlink_data <- function(cell) {
#   cell_unlisted <- unlist(cell)
#   list(
#     text = cell_unlisted[["formattedValue"]],
#     hyperlink = if ("hyperlink" %in% names(cell_unlisted)) cell_unlisted[["hyperlink"]] else NA
#   )
# }
# hyperlinks <- old_inventory %>%
#   filter(stringr::str_starts(loc, "G") & loc != "G1") %>%
#   pull(cell) %>%
#   purrr::map_df(., extract_hyperlink_data) %>%
#   rename(Website = text, Website_URL = hyperlink) %>%
#   janitor::clean_names() %>%
#   rename(source = website, 
#          source_url = website_url) %>%
#   unique() %>%
#   filter(!is.na(source)) %>%
#   filter(source != "Calculated") 
# 
# # recombine! 
# added_hyperlinks <- updated_data_inventory_flag %>%
#   left_join(hyperlinks, by = "source")  %>%
#   relocate(source_url, .after = source) %>%
#   relocate(`use_in_tool?`, .after = source_url) %>%
#   relocate(update_flag, .after = source_url)

