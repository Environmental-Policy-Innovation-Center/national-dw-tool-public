###############################################################################
# DWSRF Funded Projects
#
# EPA's DWSRF report doesn't have a URL or API for bulk downloading. Instead we
# use the interactive APEX report UI with a headless Chrome session to
# trigger the data download. The browser session code below relies on the EPA's
# UI (might need updates if they change their UI).
#
# merged_pwsid_funded_highlevel_summary - rolls clean_dwsrf's per-award list up
# into one row per pwsid (times funded, total/median assistance, total
# principal forgiveness). Triggered after clean_dwsrf.
###############################################################################

#' Run a headless Chrome session to export the DWSRF report.
#' @param config Main config
#' @param dataset_id "raw_dwsrf"
run_dwsrf_pipeline <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link

  message("Initializing headless Chrome session...")
  # --no-sandbox needed for headless chrome to run in a container
  # --disable-dev-shm-usage avoids a crash from the /dev/shm being too small
  chrome_object <- chromote::Chromote$new(
    browser = chromote::Chrome$new(args = c(
      chromote::get_chrome_args(), "--no-sandbox", "--disable-dev-shm-usage"
    ))
  )
  b <- chrome_object$new_session()
  on.exit(try(b$close(), silent = TRUE), add = TRUE)
  temp_dir <- tempfile(pattern = "dwsrf_download_")
  dir.create(temp_dir)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)
  b$Page$setDownloadBehavior(behavior = "allow", downloadPath = temp_dir)

  message("Navigating to the DWSRF report page...")
  b$Page$navigate(source_url, timeout_ = 60)
  Sys.sleep(15)

  message("Clicking 'View Report' button...")
  b$Runtime$evaluate("document.getElementById('B5084825615145083130').click()")
  Sys.sleep(10)

  # increase the window to avoid any weird UI differences
  b$set_viewport_size(width = 2000, height = 900)
  Sys.sleep(5)

  message("Selecting and moving over all report columns...")
  select_columns_js <- "
    (function() {
      const buttons = document.querySelectorAll('button');
      for (const btn of buttons) {
        if (btn.innerText.trim() === 'Select Columns') {
          btn.click();
          return 'Clicked!';
        }
      }
      return 'Button not found.';
    })();
  "
  b$Runtime$evaluate(select_columns_js)
  Sys.sleep(15)
  b$Runtime$evaluate("document.querySelector('button[title=\"Move All\"').click()")
  Sys.sleep(10)

  apply_js <- "
    (function() {
      const buttons = document.querySelectorAll('button');
      for (const btn of buttons) {
        if (btn.innerText.trim() === 'Apply') {
          btn.click();
          return 'Clicked!';
        }
      }
      return 'Button not found.';
    })();
  "
  b$Runtime$evaluate(apply_js)
  Sys.sleep(15)

  message("Triggering export via APEX session ID injection...")
  export_js <- "
    (function() {
      try {
        var sid = apex.env.APP_SESSION ||
                  document.querySelector('#pInstance').value ||
                  window.location.href.split(':')[2];
        if (sid && sid !== 'undefined') {
          var command = 'f?p=410:11:' + sid + ':XLSX:NO:::';
          apex.navigation.redirect(command);
          return 'Triggered download for session ' + sid;
        }
        return 'Error: Could not find session ID';
      } catch (e) {
        return 'Error: ' + e.message;
      }
    })();
  "
  export_result <- b$Runtime$evaluate(export_js)
  message(sprintf("Browser JS status: %s", export_result$result$value))

  message("Waiting for the browser download stream to complete...")
  Sys.sleep(25)

  message("Validating downloaded data...")
  downloaded_files <- list.files(temp_dir, pattern = "\\.(csv|xlsx|xls)$", full.names = TRUE)
  if (length(downloaded_files) == 0) {
    stop("DWSRF report export downloaded zero records.", call. = FALSE)
  }
  downloaded_file <- downloaded_files[1]
  message(sprintf("Processing downloaded file: %s", basename(downloaded_file)))

  ext <- tolower(tools::file_ext(downloaded_file))
  data_sample <- if (ext %in% c("xlsx", "xls")) {
    readxl::read_excel(downloaded_file, n_max = 5)
  } else {
    read.csv(downloaded_file, nrows = 5)
  }
  if (nrow(data_sample) == 0 || ncol(data_sample) < 2) {
    stop("Downloaded DWSRF report was empty.", call. = FALSE)
  }

  message("Reading full DWSRF report...")
  dwsrf_raw <- readxl::read_excel(downloaded_file)

  message(sprintf("Writing raw DWSRF report to S3 at %s...", link))
  s3_write_csv(dwsrf_raw, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(dwsrf_raw)
}

#' Pointblank validations for the clean DWSRF funded projects dataset
#' @param config Main config
#' @param srf_awards_tidy Clean DWSRF data frame
#' @param dataset_id "clean_dwsrf"
validate_clean_dwsrf <- function(config, srf_awards_tidy, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  agent <- new_check_agent(srf_awards_tidy, label = "Clean DWSRF Validation") %>%
    check_column_complete(pwsid, severity = "warn") %>%
    check_row_count_range(min_rows = 1, max_rows = 1000000, severity = "warn") %>%
    interrogate()

  result <- summarize_checks(agent)
  message(sprintf("Validation result summary: %s", result$summary))

  report_link <- write_check_artifacts(
    agent = agent, report_df = result$report_df,
    checks_base = checks_base, tag = dataset_id, run_ts = run_ts
  )
  message(sprintf("Validation reports pushed to S3: %s", report_link))

  if (isTRUE(result$any_error)) {
    stop(sprintf("VALIDATION FAILED: %s", result$summary), call. = FALSE)
  }

  message("Clean DWSRF validation checks passed successfully.")
  return(TRUE)
}

#' Filter and clean the raw DWSRF report into community water systems with
#' agreements since 2020.
#' @param config Main config
#' @param dataset_id "clean_dwsrf"
run_clean_dwsrf_pipeline <- function(config, dataset_id = "clean_dwsrf") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link
  link <- sub_config$link

  message("Reading raw DWSRF report from S3...")
  dwsrf_raw <- s3_read_csv(raw_link, coerce_character = FALSE)

  message("Reading EPA SABs PWSIDs to filter for community water systems....")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)

  srf_awards_tidy <- dwsrf_raw %>%
    janitor::clean_names() %>%
    mutate(pwsid = trimws(pwsid),
           current_agreement_amount_tidy = as.numeric(gsub("[^0-9.-]", "", current_agreement_amount)),
           additional_subsidy_amount_tidy = as.numeric(gsub("[^0-9.-]", "", additional_subsidy_amount))) %>%
    filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
    filter(year(initial_agreement_date) >= 2020) %>%
    mutate(last_epic_run_date = Sys.Date())

  message("Validating clean_dwsrf...")
  validate_clean_dwsrf(config, srf_awards_tidy, dataset_id)

  message(sprintf("Writing clean DWSRF report to S3 at %s...", link))
  s3_write_csv(srf_awards_tidy, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(srf_awards_tidy)
}

#' Merge clean_dwsrf's per-award list into one row per pwsid.
#' @param config Main config
#' @param dataset_id "merged_pwsid_funded_highlevel_summary"
run_merged_pwsid_funded_highlevel_summary_pipeline <- function(config, dataset_id = "merged_pwsid_funded_highlevel_summary") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  dwsrf_link <- sub_config$input_links$dwsrf_link
  link <- sub_config$link

  message(sprintf("Downloading clean DWSRF report from %s...", dwsrf_link))
  srf_awards_tidy <- s3_read_csv(dwsrf_link, coerce_character = FALSE)

  # grouping and summarizing based on funding tracker team feedback
  srf_awards_summary <- srf_awards_tidy %>%
    group_by(pwsid) %>%
    summarize(times_funded = n(),
              total_srf_assistance = sum(current_agreement_amount_tidy),
              median_srf_assistance = median(current_agreement_amount_tidy),
              # finding total PF - NOTE this includes grants & negative interest
              total_principal_forgiveness = sum(additional_subsidy_amount_tidy),
              .groups = "drop")

  message("Validating merged_pwsid_funded_highlevel_summary...")
  validate_merged_pwsid_funded_highlevel_summary(config, srf_awards_summary, dataset_id)

  message(sprintf("Writing PWSID DWSRF funding highlevel summary to S3 at %s...", link))
  s3_write_csv(srf_awards_summary, link, acl = "public-read")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(srf_awards_summary)
}

#' Pointblank validations for the PWSID DWSRF funding highlevel summary
#' @param config Main config
#' @param srf_awards_summary Highlevel summary data frame
#' @param dataset_id "merged_pwsid_funded_highlevel_summary"
validate_merged_pwsid_funded_highlevel_summary <- function(config, srf_awards_summary, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  agent <- new_check_agent(srf_awards_summary, label = "PWSID DWSRF Funding Highlevel Summary Validation") %>%
    check_column_complete(pwsid, severity = "stop") %>%
    rows_distinct(columns = vars(pwsid), actions = action_levels(stop_at = 1),
                  label = "No duplicate pwsid rows") %>%
    interrogate()

  result <- summarize_checks(agent)
  message(sprintf("Validation result summary: %s", result$summary))

  report_link <- write_check_artifacts(
    agent = agent, report_df = result$report_df,
    checks_base = checks_base, tag = dataset_id, run_ts = run_ts
  )
  message(sprintf("Validation reports pushed to S3: %s", report_link))

  if (isTRUE(result$any_error)) {
    stop(sprintf("VALIDATION FAILED: %s", result$summary), call. = FALSE)
  }

  message("PWSID DWSRF funding highlevel summary validation checks passed successfully.")
  return(TRUE)
}
