source("./functions/check_env.R")
ENV <- check_env()

###############################################################################
# DWSRF Yearly Worker
###############################################################################

library(chromote)
library(aws.s3)
library(tidyverse)

# Setup Chromote Headless Browser session
b <- ChromoteSession$new()
temp_dir <- tempdir()
b$Browser$setDownloadBehavior(
  behavior = "allow", 
  downloadPath = temp_dir
)

# Navigate to the filter report page
b$Page$navigate("https://sdwis.epa.gov/ords/sfdw_pub/r/sfdw/owsrf_public/assistance-agreement-report-filters", timeout_ = 60)
Sys.sleep(10)

# Wait for results page to load ('apex' object and session state created)
b$Runtime$evaluate("document.querySelector('button[id*=\"B\"]').click()") 
Sys.sleep(10) # Wait for the database to return results

# Trigger download using the session ID from the current URL
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
            var command = 'f?p=410:11:' + sid + ':XLXSM:NO:::';
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
Sys.sleep(20)

# Find downloaded file and upload to S3
downloaded_file <- list.files(temp_dir, pattern = "\\.(csv|xlsx)$", full.names = TRUE)[1]
file_name <- basename(downloaded_file)
print(file_name)
s3_object <- s3_path(file.path("raw/national/water-system/", file_name), ENV)
if (!is.na(downloaded_file)) {
  put_object(
    file = downloaded_file,
    object = s3_object,
    acl = "public-read"
  )
}

# Close browser session
b$parent$close()

