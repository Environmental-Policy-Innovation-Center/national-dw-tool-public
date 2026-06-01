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

# for logs: 
print("I'm running!")

dataset_id <- "dwsrf"

print("\n==================================================")
print("Starting DWSRF yearly worker pipeline")
print("==================================================")

# Setup Chromote Headless Browser session
print("Initializing Chromote Session...")
b <- ChromoteSession$new()
temp_dir <- tempdir()
b$Browser$setDownloadBehavior(
  behavior = "allow", 
  downloadPath = temp_dir
)

# Navigate to the filter report page
print("Navigating to report page...")
b$Page$navigate("https://sdwis.epa.gov/ords/sfdw_pub/r/sfdw/owsrf_public/assistance-agreement-report-filters", timeout_ = 60)
Sys.sleep(15)

# Wait for results page to load ('apex' object and session state created)
# Explicitly target the primary action button
b$Runtime$evaluate("document.querySelector('button.t-Button--hot, button[id*=\"B\"]').click()")
Sys.sleep(15) # Wait for the database to return results

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

print("Updating DWSRF report in S3...")

print(paste("File located:", file_name))
clean_s3_link <- s3_path(file.path("raw/national/water-system/", file_name), ENV)
update_task_manager(dataset_id, clean_s3_link)
tmp <- tempfile()
s3_object <- s3_path(file.path("raw/national/water-system/", file_name), ENV)
if (!is.na(downloaded_file)) {
  put_object(
    file = downloaded_file,
    object = s3_object,
    acl = "public-read"
  )
}

# Close browser session
try(b$parent$close(), silent = TRUE)

print("Worker Job Complete - Shutting Down")
