###############################################################################
# DWSRF Yearly Worker
###############################################################################

library(chromote)
library(aws.s3)
library(tidyverse)

source("./functions/update_task_manager_helper.R")

# write to development bucket if working in dev mode
source("./functions/check_env.R")
ENV <- check_env()

# no scientific notation 
options(scipen = 999)

# make sure to specify the correct bucket region for IAM role: 
Sys.setenv("AWS_DEFAULT_REGION" = 'us-east-1')

# for filtering for CWS: 
epa_sabs_pwsids <- aws.s3::s3read_using(read.csv, 
                                        object = "s3://tech-team-data/national-dw-tool/raw/national/water-system/sabs_pwsid_names.csv")
# for logs: 
print("I'm running!")

dataset_id <- "dwsrf"
raw_s3_link <- "s3://tech-team-data/national-dw-tool/raw/national/water-system/dwsrf_funded_projects.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/national/dwsrf_funded_projects.csv"

###############################################################################
print("==================================================")
print("Starting DWSRF yearly worker pipeline")
print("==================================================")
# helpful for troubleshooting: 
# b$screenshot()
# b$view()

# Setup Chromote Headless Browser session
print("Initializing Chromote Session...")
b <- ChromoteSession$new()
temp_dir <- tempdir()
b$Page$setDownloadBehavior(
  behavior = "allow", 
  downloadPath = temp_dir
)

# Navigate to the filter report page
print("Navigating to report page...")
b$Page$navigate("https://sdwis.epa.gov/ords/sfdw_pub/r/sfdw/owsrf_public/assistance-agreement-report-filters", timeout_ = 60)
Sys.sleep(15)

# click on view report: 
b$Runtime$evaluate("document.getElementById('B5084825615145083130').click()")

# increase the window to avoid any weird UI differences
b$set_viewport_size(width = 2000, height = 900)

# select columns: 
select_column_js <- "
  (function() {
    // Find all buttons on the page
    const buttons = document.querySelectorAll('button');
    
    for (const btn of buttons) {
      // Trim and check if the text matches 'select columns'
      if (btn.innerText.trim() === 'Select Columns') {
        btn.click();
        return 'Clicked!';
      }
    }
    return 'Button not found.';
  })();
"
b$Runtime$evaluate(select_column_js)

# move all of the columns over: 
b$Runtime$evaluate("document.querySelector('button[title=\"Move All\"').click()")

# click apply 
click_apply_js <- "
(function() {
  // Find all buttons on the page
  const buttons = document.querySelectorAll('button');
  
  for (const btn of buttons) {
    // Trim and check if the text matches 'apply' 
    if (btn.innerText.trim() === 'Apply') {
      btn.click();
      return 'Clicked!';
    }
  }
  return 'Button not found.';
})();
"
b$Runtime$evaluate(click_apply_js)


# Trigger download using the session ID from the current URL
print("Generating export link via APEX session ID injection...")
report_download_js <- "
(function() {
    try {
        // Get the Session ID from the internal APEX environment object
        var sid = apex.env.APP_SESSION || 
                  document.querySelector('#pInstance').value || 
                  window.location.href.split(':')[2];

        if (sid && sid !== 'undefined') {
            // Right clicking on the download button returns this redirect link address:
            // javascript:apex.navigation.redirect('f?p=410:11:11601062423425:XLXSM:NO:::%27);
            // Recreating this with the current session id
            var command = 'f?p=410:11:' + sid + ':XLSX:NO:::';
            apex.navigation.redirect(command);
            return 'Triggered download for Session ' + sid;
        } else {
            return 'Error: Could not find Session ID';
        }
    } catch (e) {
        return 'Error: ' + e.message;
    }
})()
"
report <- b$Runtime$evaluate(report_download_js)
print(paste("Browser JS Status:", report$result$value))

print("Waiting for browser download stream...")
Sys.sleep(25)

# Find downloaded file and upload to S3
downloaded_files <- list.files(temp_dir, pattern = "\\.(csv|xlsx|xls)$", full.names = TRUE)

# QA/QC Check
print("Validating data download...")
if (length(downloaded_files) == 0) {
  print("Zero records downloaded - SHUTTING DOWN WORKER")
  update_task_manager(dataset_id, "WORKER FAILED - NO FILE DOWNLOADED")
  # try attempts closing browser session without crashing engine
  try(b$close(), silent = TRUE)
  try(b$parent$close(), silent = TRUE)
  # quit() was causing R to abort locally
  stop("Exiting pipeline.")
}

print("Validating data...")
downloaded_file <- downloaded_files[1]
file_name <- basename(downloaded_file)
print(paste("Processing local file:", file_name))

ext <- tools::file_ext(downloaded_file)
data_sample <- if (ext %in% c("xlsx", "xls")) {
  if (!requireNamespace("readxl", quietly = TRUE)) install.packages("readxl")
  readxl::read_excel(downloaded_file, n_max = 5)
} else {
  read_csv(downloaded_file, n_max = 5, show_col_types = FALSE)
}

if (nrow(data_sample) == 0 || ncol(data_sample) < 2) {
  print("Downloaded record empty - SHUTTING DOWN WORKER")
  update_task_manager(dataset_id, "WORKER FAILED - DOWNLOAD EMPTY")
  try(b$parent$close(), silent = TRUE)
  stop("Exiting pipeline due to empty DWSRF report.")
}

###############################################################################
print("Updating DWSRF report in S3...")
# reading in the full dataset to push to raw: 
dwsrf_raw <- readxl::read_excel(downloaded_file)

# push to raw bucket: 
tmp <- tempfile()
write.csv(dwsrf_raw, file = paste0(tmp, ".csv"), row.names = F)
on.exit(unlink(tmp))
put_object(
  file = paste0(tmp, ".csv"),
  object = raw_s3_link
)
update_task_manager(dataset_id, raw_s3_link)

# clean up, and push to clean
dwsrf_clean <- dwsrf_raw %>%
  # first three rows are just headers
  slice(-(1:4)) 
# column names are now the first row
colnames(dwsrf_clean) <- dwsrf_clean[1,] 

# clean names, trim pwsid, and tidy numeric fields
# TODO - need to tidy any column that ends with "_date" from excel format 
# TODO - for some reason, the raw data are different from the pull we did
# in Feb. This might be fine, but I still want to check it out. 
# TODO - I need to also fix the task manager, as this should replace the 
# "dwsrf_funded_projects" dataset, and I need to remove the duplicated file 
# in the raw bucket. 
# TODO - I also introduced some new dependencies here & I gotta add them 
# up top and to the dockerfile 
srf_awards_tidy <- dwsrf_clean %>% 
  # remove extra column 
  slice(-1) %>%
  janitor::clean_names() %>%
  mutate(pwsid = trimws(pwsid), 
         # tidy key numeric columns: 
         current_agreement_amount_tidy = as.numeric(str_replace_all(current_agreement_amount, "[^0-9.-]", "")), 
         additional_subsidy_amount_tidy = as.numeric(str_replace_all(additional_subsidy_amount, "[^0-9.-]", ""))) %>%
  # filter for pwsids in epa_sabs
  filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
  mutate(last_epic_run_date = Sys.Date())

# adding: 
# tmp <- tempfile()
# write.csv(srf_awards_tidy, file = paste0(tmp, ".csv"), row.names = F)
# on.exit(unlink(tmp))
# put_object(
#   file = paste0(tmp, ".csv"),
#   object = clean_s3_link
# )
# update_task_manager(dataset_id, clean_s3_link)

# Close browser session
try(b$parent$close(), silent = TRUE)

print("Worker Job Complete - Shutting Down")
