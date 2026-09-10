###############################################################################
# Cartographic Boundaries Yearly Worker
# Downloads US Census Bureau cartographic boundary shapefiles (states, counties,
# and places) from the GENZ archive, runs light QA/QC checks, and pushes the
# ZIPs to raw, clean, and staging buckets in S3. These files support search
# functionality in the front-end app.
#
# Year is configurable via the CB_YEAR environment variable (default: "2022").
# Source format is the Census Bureau's 500k-scale (1:500,000) shapefile ZIPs:
#   https://www2.census.gov/geo/tiger/GENZ{YEAR}/shp/cb_{YEAR}_us_{layer}_500k.zip
###############################################################################

# libraries needed:
library(tidyverse)
library(aws.s3)
library(janitor)
library(sf)
library(tools)
library(pointblank)

# shared QA/QC helpers (issue #22)
source("./functions/checks.R")

# no scientific notation
options(scipen = 999)

# make sure to specify the correct bucket region for IAM role:
Sys.setenv("AWS_DEFAULT_REGION" = 'us-east-1')

# for logs:
print("I'm running!")

###############################################################################
# Config
###############################################################################
CB_YEAR <- Sys.getenv("CB_YEAR", unset = "2022")
print(paste0("Pulling Census Bureau cartographic boundaries for year: ", CB_YEAR))

# Per-layer config. expected_min_rows / expected_max_rows form a sanity range
# used in QA/QC; counts come from real 2022 data plus headroom.
LAYERS <- list(
  state = list(
    expected_min_rows = 50,     # 50 states (DC + territories may also be present)
    expected_max_rows = 100
  ),
  county = list(
    expected_min_rows = 3000,   # ~3,140 in real census data
    expected_max_rows = 5000
  ),
  place = list(
    expected_min_rows = 25000,  # ~30,000+ in real census data
    expected_max_rows = 50000
  )
)

# Fill in URLs and file names for the requested year
for (lyr in names(LAYERS)) {
  LAYERS[[lyr]]$file_name <- sprintf("cb_%s_us_%s_500k.zip", CB_YEAR, lyr)
  LAYERS[[lyr]]$shp_name  <- sprintf("cb_%s_us_%s_500k.shp", CB_YEAR, lyr)
  LAYERS[[lyr]]$url       <- sprintf(
    "https://www2.census.gov/geo/tiger/GENZ%s/shp/%s",
    CB_YEAR, LAYERS[[lyr]]$file_name
  )
}

dataset_i   <- "cartographic_boundaries"
raw_base     <- "s3://tech-team-data/national-dw-tool/raw/national/cartographic-boundaries"
clean_base   <- "s3://tech-team-data/national-dw-tool/clean/national/cartographic-boundaries"
staging_base <- "s3://tech-team-data/national-dw-tool/test-staged/cartographic-boundaries"
checks_base  <- "s3://tech-team-data/national-dw-tool/checks/cartographic-boundaries"

# single timestamp threaded through check artifacts and the task manager so the
# CSV, HTML filename, and last_check_run all agree
run_ts <- Sys.time()

###############################################################################
# Read task manager
###############################################################################
task_manager <- aws.s3::s3read_using(read.csv,
  object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv") |>
  mutate(across(everything(), ~ as.character(.)))

###############################################################################
# Failure helper: write "WORKER FAILED" row to task manager and quit. Mirrors
# the pattern in me_daily.R and awia_quarterly.R.
###############################################################################
abort_with_failure <- function(msg) {
  message("FAILURE: ", msg)
  print("Updating task manager with failure status and shutting down")

  task_manager_df <- data.frame(
    dataset = dataset_i,
    date_downloaded = Sys.Date(),
    raw_link   = "WORKER FAILED - SEE LOGS",
    clean_link = "WORKER FAILED - SEE LOGS"
  )

  if (!(dataset_i %in% task_manager$dataset)) {
    task_manager <- bind_rows(task_manager, data.frame(dataset = dataset_i))
  }

  dont_touch_these_columns <- setdiff(names(task_manager), names(task_manager_df))
  task_manger_simple <- task_manager |>
    select(dataset, all_of(dont_touch_these_columns))

  updated_row <- task_manger_simple |>
    inner_join(task_manager_df, by = "dataset") |>
    mutate(across(everything(), ~ as.character(.)))

  updated_task_manager <- task_manager |>
    filter(dataset != dataset_i) |>
    bind_rows(updated_row) |>
    arrange(dataset)

  tmp <- tempfile()
  write.csv(updated_task_manager, file = paste0(tmp, ".csv"), row.names = FALSE)
  on.exit(unlink(tmp), add = TRUE)
  put_object(
    file = paste0(tmp, ".csv"),
    object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv",
    acl = "public-read"
  )

  print("Update to Task Manager Complete - Shutting Down")
  quit(save = "no")
}

###############################################################################
# Part 1: download all three ZIP files
###############################################################################
print("Downloading cartographic boundary ZIPs from census.gov")
# Bump the default timeout because the place layer is ~70MB
options(timeout = max(600, getOption("timeout")))

tmp_dir <- tempdir()

for (lyr in names(LAYERS)) {
  info <- LAYERS[[lyr]]
  local_zip <- file.path(tmp_dir, info$file_name)
  LAYERS[[lyr]]$local_zip <- local_zip

  print(sprintf("  %s -> %s", lyr, info$url))
  ok <- tryCatch({
    download.file(info$url, local_zip, mode = "wb", quiet = TRUE)
    TRUE
  }, error = function(e) {
    message("Download error for ", lyr, ": ", conditionMessage(e))
    FALSE
  })

  if (!ok || !file.exists(local_zip) || file.info(local_zip)$size == 0) {
    abort_with_failure(sprintf(
      "Failed to download layer '%s' from %s. Year %s may not exist on census.gov.",
      lyr, info$url, CB_YEAR
    ))
  }
  print(sprintf("    downloaded %.1f MB", file.info(local_zip)$size / 1024 / 1024))
}

###############################################################################
# Part 2: QA/QC checks via pointblank (issue #22)
#
# File-level checks (unzip, shapefile present, readable) stay imperative since
# pointblank validates tables, not files. Once each layer is read into an sf
# object, the data checks run through the shared pointblank helpers:
#   - row count within the expected range  -> error (a wrong count means the
#     download is broken)
#   - all geometries valid                  -> warning (worth flagging, but one
#     bad geometry in authoritative census data shouldn't kill the run)
#   - GEOID column complete (no NAs)        -> error (GEOID is the join key)
#
# Each layer's HTML report and results CSV are written to the S3 checks/ dir.
# If any layer trips an error-level check, the worker aborts after all layers
# have been checked (so every report is captured for diagnostics).
###############################################################################
print("Running QA/QC checks")

check_summaries <- c()   # per-layer status strings, for the task manager
check_reports   <- c()   # per-layer report S3 links, for the task manager
any_check_error <- FALSE

for (lyr in names(LAYERS)) {
  info <- LAYERS[[lyr]]
  print(sprintf("  Checking %s", lyr))

  # --- file-level checks (imperative) ---
  extract_dir <- file.path(tmp_dir, paste0("extract_", lyr))
  dir.create(extract_dir, showWarnings = FALSE, recursive = TRUE)

  unzip_files <- tryCatch(
    unzip(info$local_zip, exdir = extract_dir),
    error = function(e) NULL
  )
  if (is.null(unzip_files) || length(unzip_files) == 0) {
    abort_with_failure(sprintf("Unzip failed for layer '%s'", lyr))
  }

  shp_path <- file.path(extract_dir, info$shp_name)
  if (!file.exists(shp_path)) {
    abort_with_failure(sprintf(
      "Expected shapefile '%s' not found in zip for layer '%s'",
      info$shp_name, lyr
    ))
  }

  sf_obj <- tryCatch(
    sf::st_read(shp_path, quiet = TRUE),
    error = function(e) {
      message("st_read error: ", conditionMessage(e))
      NULL
    }
  )
  if (is.null(sf_obj)) {
    abort_with_failure(sprintf("Could not read shapefile for layer '%s'", lyr))
  }

  # --- data checks (pointblank) ---
  # precompute geometry validity as a column, then drop geometry so every check
  # runs on a plain data frame (pointblank's column checks error on raw sf)
  attrs <- sf_obj |>
    mutate(geometry_valid = sf::st_is_valid(sf_obj)) |>
    sf::st_drop_geometry()

  agent <- new_check_agent(attrs, label = sprintf("cartographic_boundaries: %s", lyr)) |>
    check_row_count_range(info$expected_min_rows, info$expected_max_rows, severity = "stop") |>
    check_column_all_true(geometry_valid, severity = "warning") |>
    check_column_complete(GEOID, severity = "stop") |>
    interrogate()

  result <- summarize_checks(agent)
  print(sprintf("    %s: %s", lyr, result$summary))

  # write HTML report + results CSV to S3 checks/ dir
  report_link <- write_check_artifacts(agent, result$report_df,
                                       checks_base, tag = lyr, run_ts = run_ts)

  check_summaries <- c(check_summaries, sprintf("%s: %s", lyr, result$summary))
  check_reports   <- c(check_reports, report_link)
  if (isTRUE(result$any_error)) any_check_error <- TRUE
}

# abort if any layer tripped an error-level check (reports already written)
if (any_check_error) {
  abort_with_failure(sprintf(
    "QA/QC error-level failure. Per-layer status: %s",
    paste(check_summaries, collapse = " | ")
  ))
}

print("All QA/QC checks passed (warnings allowed)")

###############################################################################
# Part 3: upload ZIPs to raw, clean, and staging
###############################################################################
print("Uploading to S3 (raw, clean, staging)")

raw_links     <- c()
clean_links   <- c()
staging_links <- c()

for (lyr in names(LAYERS)) {
  info <- LAYERS[[lyr]]
  
  # keeping file names generic w/o year stamps so these can be easily over
  # written & updated on our end. 
  base_filename <-  sub("^([^_]+_)+?(\\d{4}_)?", "", info$file_name)

  raw_obj     <- file.path(raw_base,     base_filename)
  clean_obj   <- file.path(clean_base,   base_filename)
  staging_obj <- file.path(staging_base, base_filename)

  print(sprintf("  %s", base_filename))
  
  # unfortunately also need to unzip & make the file names generic, since the 
  # zipped folder contains files w/ year stamped names
  tmp_unzip_dir <- file.path(tempdir(), tools::file_path_sans_ext(base_filename))
  dir.create(tmp_unzip_dir, showWarnings = FALSE)
  unzip(info$local_zip, exdir = tmp_unzip_dir)
  
  # rename these files 
  all_files <- list.files(tmp_unzip_dir, recursive = T, full.names = T)
  for (f in all_files) {
    generic_name <- sub("^([^_]+_)+?(\\d{4}_)?", "", basename(f))
    new_path <- file.path(dirname(f), generic_name)
    if (f != new_path) file.rename(f, new_path)
  }
  
  # zippin'
  generic_local_zip <- file.path(tempdir(), base_filename)
  old_wd <- setwd(tmp_unzip_dir)
  zip(generic_local_zip, files = list.files(".", recursive = TRUE))
  setwd(old_wd)
  
  # Raw: pristine source ZIP, archival
  put_object(file = generic_local_zip, object = raw_obj)
  # Clean: same ZIP passthrough (no transformation needed - per EmmaLi on #23)
  put_object(file = generic_local_zip, object = clean_obj)
  # Staging: same ZIP, publicly readable for front-end consumption
  put_object(file = generic_local_zip, object = staging_obj, acl = "public-read")
  
  # clean up
  unlink(tmp_unzip_dir, recursive = T)
  file.remove(generic_local_zip)

  raw_links     <- c(raw_links,     raw_obj)
  clean_links   <- c(clean_links,   clean_obj)
  staging_links <- c(staging_links, staging_obj)
}

###############################################################################
# Part 4: update task manager (multi-file "Multiple - ..." pattern like SDWA)
###############################################################################
print("Updating Task Manager")

raw_link_combined     <- paste0("Multiple - ", paste(raw_links,     collapse = "; "))
clean_link_combined   <- paste0("Multiple - ", paste(clean_links,   collapse = "; "))
staged_link_combined  <- paste0("Multiple - ", paste(staging_links, collapse = "; "))

# This worker also pushes to the staged bucket (front-end reads from there
# directly for cartographic data), so fill in date_staged, staged_link, and
# data_qual_score in the task manager too. The QA/QC framework (issue #22) also
# fills in last_check_run, last_check_status, and last_check_report_link.
check_status_combined <- paste(check_summaries, collapse = " | ")
check_report_combined <- paste0("Multiple - ", paste(check_reports, collapse = "; "))

task_manager_df <- data.frame(
  dataset                 = dataset_i,
  date_downloaded         = Sys.Date(),
  raw_link                = raw_link_combined,
  clean_link              = clean_link_combined,
  date_staged             = Sys.Date(),
  staged_link             = staged_link_combined,
  data_qual_score         = check_status_combined,
  last_check_run          = format(run_ts, "%Y-%m-%dT%H:%M:%S%z"),
  last_check_status       = check_status_combined,
  last_check_report_link  = check_report_combined
)

if (!(dataset_i %in% task_manager$dataset)) {
  task_manager <- bind_rows(task_manager, data.frame(dataset = dataset_i))
}

dont_touch_these_columns <- setdiff(names(task_manager), names(task_manager_df))
task_manger_simple <- task_manager |>
  select(dataset, all_of(dont_touch_these_columns))

updated_row <- task_manger_simple |>
  inner_join(task_manager_df, by = "dataset") |>
  mutate(across(everything(), ~ as.character(.)))

updated_task_manager <- task_manager |>
  filter(dataset != dataset_i) |>
  bind_rows(updated_row) |>
  arrange(dataset)

tmp <- tempfile()
write.csv(updated_task_manager, file = paste0(tmp, ".csv"), row.names = FALSE)
on.exit(unlink(tmp), add = TRUE)
put_object(
  file = paste0(tmp, ".csv"),
  object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv",
  acl = "public-read"
)

print("Worker Job Complete - Shutting Down")
