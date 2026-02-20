###############################################################################
# Massachusetts Quarterly Worker
###############################################################################

# libraries needed: 
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(RSelenium)
library(rvest)
# install.packages("wdman")
# install.packages("RSelenium")
# could be an error with this package: 
# library(wdman)
# library(remotes)

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
dataset_i <- "ma_bwn"
state <- "Massachusetts"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/ma/water-system/ma_bwn.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/ma/ma_bwn.csv"

# updating BWN/BWA data: #######################################################
# for logs: 
print("on: water system datasets - Boil Water Notices & Advisories")

# Steps for RSelenium: #########################################################
# done with help from: 
# https://www.r-bloggers.com/2021/04/using-rselenium-to-scrape-a-paginated-html-table-2/

# TODO - TO RUN THIS ON AN EC2 INSTANCE, YOU NEED TO RUN THIS IN A HEADLESS 
# BROWSER - IT WILL NOT WORK WITH FIREFOX!
# when adding to ec2, check this out: https://rpubs.com/grahamplace/rselenium-ec2


# NOTE - in command line, you need to run: 
# docker run -d -p 4445:4444 selenium/standalone-firefox:2.53.1
# docker run -d -p 4445:4444 selenium/standalone-firefox:latest
# docker run -d -p 4446:4444 selenium/standalone-firefox
# remDr <- remoteDriver(remoteServerAddr = "localhost", port = 4446L, browserName = "firefox")
# -- figure out how to do this in an instance?? -- 
# docker run -d -p 4445:4444 selenium/standalone-firefox:4.3.0-20220726

# export DOCKER_DEFAULT_PLATFORM=linux/amd64
# docker run -d -p 4445:4444 selenium/standalone-firefox:4.9.0
# docker run -d -p 4445:4444 selenium/standalone-firefox:4.8.3

# docker run -d -p 4445:4444 selenium/standalone-firefox
# docker run --platform linux/amd64 -d -p 4444:4444 -p 7900:7900 --shm-size="2g" selenium/standalone-firefox:latest
# docker run --platform linux/amd64 -d -p 4445:4444 --shm-size="2g" selenium/standalone-firefox:2.53.1

# docker run --platform linux/amd64 -d -p 4445:4444 -p 7900:7900 --shm-size="2g" selenium/standalone-firefox:2.53.1
# docker run --platform linux/amd64 -d -p 4444:4444 -p 7900:7900 --shm-size="2g" selenium/standalone-firefox:latest

# chrome?
# docker run --platform linux/amd64 -d \
# -p 4444:4444 \
# -p 7900:7900 \
# --shm-size="2g" \
# selenium/standalone-chrome:latest

# requires you to have docker installed
# remDr <- remoteDriver(
#   remoteServerAddr = "localhost",
#   port = 4445L,
#   browserName = "firefox"
# )

# docker run --platform linux/amd64 -d \
# -p 4444:4444 \
# -p 7900:7900 \
# --shm-size="2g" \
# selenium/standalone-chrome:3.141.59

remDr <- remoteDriver(
  remoteServerAddr = "127.0.0.1",
  port = 4444L,
)

# remDr <- remoteDriver(
#   remoteServerAddr = "127.0.0.1",
#   port = 4444L,
#   path = "/",
#   extraCapabilities = eCaps
# )


# if this is running automatically - give a moment for the instance to boot up
Sys.sleep(1)

# navigate to URL: 
remDr$open()

url <- "https://eeaonline.eea.state.ma.us/DEP/Boil_Order/"
remDr$navigate(url)
# check 
# remDr$getCurrentUrl()

# click on the search button to get all of the terminated and active advisories: 
webElem <- remDr$findElement(using = "css", "[name = 'ctl00$ContentPlaceHolder1$mmbtnSearch']")
webElem$clickElement()


# scrape the first table: 
elem_chemp <- remDr$findElement(using="xpath", 
                                value="//*[@id='ctl00_ContentPlaceHolder1_UpdatePanel1']")
# scrape the html table as a tibble
results_champ <- read_html(elem_chemp$getElementAttribute('innerHTML')[[1]]) %>% 
  html_table() 
results_p1 <- results_champ[[1]] %>%
  janitor::clean_names()
# removing rows with just numbers 
results_p1_no_nums <- subset(results_p1, !grepl("[0-9]", status))
results_p1_tidy <- results_p1_no_nums %>%
  select(city_town:date_order_terminated)

# grab pwsids: ---
pwsid_list <- data.frame() 
for (i in 1:nrow(results_p1_tidy)){
  value_i <- paste0("//*[@id='ctl00_ContentPlaceHolder1_GridView1_ctl0", 
                    i + 1, "_lnkbtnPWS']")
  
  # navigate to supplier & click the link: 
  pwsid_element <- remDr$findElement(using="xpath", 
                                     value=value_i)
  pwsid_element$clickElement()
  
  # sleep here - otherwise the popup might not load in time for the next 
  # section of code: 
  Sys.sleep(1)
  # scrape contents of popup: 
  pwsid_table <- remDr$findElement(using="xpath", value="//*[@id='ajaxresponse']")
  pwsid_info <- read_html(pwsid_table$getElementAttribute('innerHTML')[[1]])%>% 
    html_table() %>%
    as.data.frame() 
  # I just want the pwsid!!! 
  pwsid_i <- pwsid_info$X1[1]
  pwsid_i_df <- data.frame(pwsid_i)
  pwsid_list <- rbind(pwsid_list, pwsid_i_df)
  
  # close it: 
  close_pwsid <- remDr$findElement(using="xpath", 
                                   value="//*[@class='pwsinfo']")
  close_pwsid$clickElement()
  Sys.sleep(1)
}

# binding: 
page_1 <- cbind(pwsid_list, results_p1_tidy) %>%
  mutate(page_num = 1)


# looping across the other pages: 
for(z in 1:200){
  z = z + 1
  message(paste0("On Page: ", z ))
  
  # go to next page: 
  # click on the search button to get all of the terminated and active advisories: 
  page_num <- paste0("Page$", z+1)
  page_link <- paste0("//*[@href=\"javascript:__doPostBack('ctl00$ContentPlaceHolder1$GridView1','Page$", z, "')\"]")
  next_page_link <- paste0("//*[@href=\"javascript:__doPostBack('ctl00$ContentPlaceHolder1$GridView1','Page$", z+1, "')\"]")
  # next_page_link <- paste0("//*[@href=\"javascript:__doPostBack('ctl00$ContentPlaceHolder1$GridView1','Page$21')\"]")
  
  page_elem <- remDr$findElement(using = "xpath", 
                                 value = page_link)
  page_elem$clickElement()
  
  # wait for next page to load: 
  Sys.sleep(1)
  
  # if not empty, keep going! 
  # scrape the first table: 
  elem_chemp <- remDr$findElement(using="xpath", 
                                  value="//*[@id='ctl00_ContentPlaceHolder1_UpdatePanel1']")
  # scrape the html table as a tibble
  results_champ <- read_html(elem_chemp$getElementAttribute('innerHTML')[[1]]) %>% 
    html_table() 
  results_p1 <- results_champ[[1]] %>%
    janitor::clean_names()
  # removing rows with just numbers 
  results_p1_no_nums <- subset(results_p1, !grepl("[0-9]", status))
  results_p1_tidy <- results_p1_no_nums %>%
    select(city_town:date_order_terminated)
  
  # grab pwsids: ---
  pwsid_list <- data.frame() 
  for (y in 1:nrow(results_p1_tidy)){
    value_i <- paste0("//*[@id='ctl00_ContentPlaceHolder1_GridView1_ctl0", 
                      y + 1, "_lnkbtnPWS']")
    
    # navigate to supplier & click the link: 
    pwsid_element <- remDr$findElement(using="xpath", 
                                       value=value_i)
    pwsid_element$clickElement()
    
    # sleep here - otherwise the popup might not load in time for the next 
    # section of code: 
    Sys.sleep(1)
    # scrape contents of popup: 
    pwsid_table <- remDr$findElement(using="xpath", value="//*[@id='ajaxresponse']")
    pwsid_info <- read_html(pwsid_table$getElementAttribute('innerHTML')[[1]])%>% 
      html_table() %>%
      as.data.frame() 
    # I just want the pwsid!!! 
    pwsid_i <- pwsid_info$X1[1]
    pwsid_i_df <- data.frame(pwsid_i)
    pwsid_list <- rbind(pwsid_list, pwsid_i_df)
    
    # close it: 
    close_pwsid <- remDr$findElement(using="xpath", 
                                     value="//*[@class='pwsinfo']")
    close_pwsid$clickElement()
    Sys.sleep(1)
  }
  
  # binding: 
  page_i <<- cbind(pwsid_list, results_p1_tidy) %>%
    mutate(page_num = z)
  
  # combining back with the OG
  page_1 <<- bind_rows(page_1, page_i)
  
  
  # we can use this trycatch function to check for next pages: 
  next_btns <- tryCatch(
    remDr$findElements(using = "xpath", 
                       value = next_page_link),
    error = function(e) NULL
  )
  
  # if empty, break the loop: 
  if (length(next_btns) == 0) {
    message("No More Pages!")
    break
  }
  
}

# tidying: 
ma_bwn_tidy <- page_1 %>%
  distinct()
# quick check to make sure we only have 5 entries/page 
# ma_bwn_tidy %>%
#   group_by(page_num) %>%
#   summarize(total_entries = n()) %>%
#   view()

# need to separate out pwsid: 
ma_bwn_final <- ma_bwn_tidy %>%
  separate(pwsid_i, c("pws_name", "pwsid_raw"), sep = "PWS ID#: ") %>%
  mutate(pws_name = str_squish(str_sub(pws_name, end=-2)), 
         # NOTE - WE'RE ASSUMING ALL OF THESE START WITH MA, which means we
         # probably shouldn't filter for just CWS yet. 
         pwsid = paste0("MA", str_squish(str_sub(pwsid_raw, end =-2)))) %>%
  relocate(pwsid, .after = pws_name) %>%
  mutate(last_epic_run_date = as.character(Sys.Date()))

# we shouldn't have to do updates, it seems like this is a pretty comprehensive 
# historic record. 

# checking against older data we have: #########################################
ma_bwn_old <- aws.s3::s3read_using(read.csv, 
                                   object = raw_s3_link) 


# check to see if the nrows new >= nrows old [it should be]
if(nrow(ma_bwn_old) > nrow(ma_bwn_final) | nrow(ma_bwn_final) == 0){
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


# adding to S3
tmp <- tempfile()
write.csv(ma_bwn_final, paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link,
  bucket = "tech-team-data",
  multipart = T
)


# Part three: standardize and add to clean s3 bucket ###########################
print("Updating Clean Dataset")

ma_bwn_tidy <- ma_bwn_final %>%
  # renaming columns to make them easier to standardize/check 
  mutate(date_issued = as.Date(date_order_issued, tryFormats = c("%m/%d/%Y")),
         date_lifted = as.Date(date_order_terminated, tryFormats = c("%m/%d/%Y")), 
         last_epic_run_date = as.Date(last_epic_run_date, tryFormats = c("%Y-%m-%d")), 
         epic_date_lifted_flag = "Reported", 
         type = order_type, 
         state = state)%>%
  mutate(date_worker_last_ran = Sys.Date()) %>%
  rename(date_epic_captured_advisory = last_epic_run_date) %>%
  relocate(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran) 

# adding list to s3
tmp <- tempfile()
write.csv(ma_bwn_tidy, file = paste0(tmp, ".csv"), row.names = F)
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
