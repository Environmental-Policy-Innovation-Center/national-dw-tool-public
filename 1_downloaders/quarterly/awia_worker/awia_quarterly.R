###############################################################################
# AWIA Certification Quarterly Worker
# Pulls AWIA Section 2013 / SDWA Section 1433 Risk and Resilience Assessment
# (RRA) and Emergency Response Plan (ERP) certification data from EPA's public
# Qlik dashboard via the underlying Qlik Engine WebSocket API. Filters to
# PWSIDs in EPA SABs, writes raw and clean CSVs to S3, and updates the task
# manager.
#
# Source dashboard:
# https://awsedap.epa.gov/public/extensions/awia-active-utility-public-data/index.html
###############################################################################

# libraries needed:
library(tidyverse)
library(aws.s3)
library(aws.ec2metadata)
library(janitor)
library(websocket)
library(later)
library(jsonlite)

# no scientific notation
options(scipen = 999)

# make sure to specify the correct bucket region for IAM role:
Sys.setenv("AWS_DEFAULT_REGION" = 'us-east-1')

# for logs:
print("I'm running!")

###############################################################################
# Qlik dashboard config
# These IDs are pulled from the dashboard's mashup JS bundle. If EPA
# reconfigures the Qlik app the IDs will change, and the schema/viz guards
# below will halt the worker rather than silently produce bad data.
###############################################################################
QLIK_HOST           <- "awsedap.epa.gov"
QLIK_PREFIX         <- "/public/"
QLIK_APP_ID         <- "819b04df-b1a2-49c4-b346-4165c30bc8ed"
QLIK_TABLE_VIZ_ID   <- "kPDXDM"
QLIK_TABLE_VIZ_TYPE <- "VizlibTable"
QLIK_WS_URL         <- paste0("wss://", QLIK_HOST, QLIK_PREFIX, "app/", QLIK_APP_ID)
QLIK_PAGE_HEIGHT    <- 800L

# Schema guard: these columns must be present in the Qlik response.
EXPECTED_COLUMNS <- c(
  "PWSID", "Community Water System Name", "Town/City", "State",
  "Wholesaler", "Population Served", "Water System Size",
  "RRA Certification", "ERP Certification"
)

###############################################################################
# Read EPA SABs PWSIDs (for filtering to community water systems) and the
# task manager from S3
###############################################################################

# for filtering to CWS:
epa_sabs_pwsids <- aws.s3::s3read_using(read.csv,
                                        object = "s3://tech-team-data/national-dw-tool/raw/national/water-system/sabs_pwsid_names.csv")

# pulling in task manager for updating relevant sections:
task_manager <- aws.s3::s3read_using(read.csv,
                                     object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv") %>%
  mutate(across(everything(), ~ as.character(.)))

# for some of the coding bits:
dataset_i     <- "awia_certification"
raw_s3_link   <- "s3://tech-team-data/national-dw-tool/raw/national/water-system/awia_certification.csv"
clean_s3_link <- "s3://tech-team-data/national-dw-tool/clean/national/awia_certification.csv"

###############################################################################
# Helper: write a "WORKER FAILED - SEE LOGS" row to the task manager and quit.
# Mirrors the failure pattern in me_daily.R.
###############################################################################
abort_with_failure <- function(msg) {
  message("FAILURE: ", msg)
  print("Updating task manager with failure status and shutting down")

  task_manager_df <- data.frame(dataset = dataset_i,
                                date_downloaded = Sys.Date(),
                                raw_link  = "WORKER FAILED - SEE LOGS",
                                clean_link = "WORKER FAILED - SEE LOGS")

  if (!(dataset_i %in% task_manager$dataset)) {
    task_manager <- bind_rows(task_manager, data.frame(dataset = dataset_i))
  }

  dont_touch_these_columns <- setdiff(names(task_manager), names(task_manager_df))
  task_manger_simple <- task_manager %>%
    select(dataset, all_of(dont_touch_these_columns))

  updated_row <- task_manger_simple %>%
    inner_join(task_manager_df, by = "dataset") %>%
    mutate(across(everything(), ~ as.character(.)))

  updated_task_manager <- task_manager %>%
    filter(dataset != dataset_i) %>%
    bind_rows(updated_row) %>%
    arrange(dataset)

  tmp <- tempfile()
  write.csv(updated_task_manager, file = paste0(tmp, ".csv"), row.names = FALSE)
  on.exit(unlink(tmp), add = TRUE)
  put_object(
    file   = paste0(tmp, ".csv"),
    object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv",
    acl    = "public-read"
  )

  print("Update to Task Manager Complete - Shutting Down")
  quit(save = "no")
}

###############################################################################
# WebSocket helpers for Qlik Engine API
###############################################################################

# Open a WebSocket and return the connection plus a response store keyed by
# JSON-RPC id. Server-side notifications (no id) are ignored.
open_qlik_session <- function(url, timeout_sec = 30) {
  ws <- WebSocket$new(url)
  store <- new.env()
  store$by_id <- list()

  ws$onMessage(function(event) {
    msg <- tryCatch(
      fromJSON(event$data, simplifyVector = FALSE),
      error = function(e) NULL
    )
    if (!is.null(msg) && !is.null(msg$id)) {
      store$by_id[[as.character(msg$id)]] <- msg
    }
  })

  deadline <- Sys.time() + timeout_sec
  while (ws$readyState() != 1L) {
    if (Sys.time() > deadline) {
      stop("Timed out connecting to Qlik WebSocket: ", url)
    }
    run_now(0.1)
  }
  list(ws = ws, store = store)
}

# Send a JSON-RPC request and block until the matching response arrives.
qlik_call <- function(session, req_id, method, handle, params, timeout_sec = 60) {
  payload <- list(
    jsonrpc = "2.0",
    id      = req_id,
    method  = method,
    handle  = handle,
    params  = params
  )
  session$ws$send(toJSON(payload, auto_unbox = TRUE))

  key <- as.character(req_id)
  deadline <- Sys.time() + timeout_sec
  while (is.null(session$store$by_id[[key]])) {
    if (Sys.time() > deadline) {
      stop("Timed out waiting for ", method, " (id=", req_id, ")")
    }
    run_now(0.2)
  }
  resp <- session$store$by_id[[key]]
  if (!is.null(resp$error)) {
    stop("Qlik API error on ", method, ": ", resp$error$message)
  }
  resp
}

###############################################################################
# Part one: pull AWIA data via paginated GetHyperCubeData calls
###############################################################################
print("on: AWIA Section 2013 / SDWA Section 1433 certification data")
print("Connecting to Qlik Engine API")

awia_raw <- tryCatch({
  session <- open_qlik_session(QLIK_WS_URL)
  on.exit(try(session$ws$close(), silent = TRUE), add = TRUE)

  # Open the doc on the global handle (-1)
  doc_resp   <- qlik_call(session, 1L, "OpenDoc", -1L,
                          list(qDocName = QLIK_APP_ID))
  doc_handle <- doc_resp$result$qReturn$qHandle

  # Get the table object
  obj_resp   <- qlik_call(session, 2L, "GetObject", doc_handle,
                          list(qId = QLIK_TABLE_VIZ_ID))
  obj_handle <- obj_resp$result$qReturn$qHandle
  viz_type   <- obj_resp$result$qReturn$qGenericType

  # Guard: viz type changed (EPA reconfigured the dashboard)
  if (!identical(viz_type, QLIK_TABLE_VIZ_TYPE)) {
    abort_with_failure(
      paste0("AWIA Qlik viz type changed (expected '", QLIK_TABLE_VIZ_TYPE,
             "', got '", viz_type, "'). Review dashboard configuration.")
    )
  }

  # Get layout to learn hypercube dimensions and column names
  layout_resp <- qlik_call(session, 3L, "GetLayout", obj_handle, list())
  hc          <- layout_resp$result$qLayout$qHyperCube
  total_rows  <- hc$qSize$qcy
  total_cols  <- hc$qSize$qcx
  dim_names   <- vapply(hc$qDimensionInfo, function(d) d$qFallbackTitle, "")
  meas_names  <- vapply(hc$qMeasureInfo,  function(m) m$qFallbackTitle, "")
  # Qlik's qMatrix returns measures first, then dimensions (the "Results" row
  # counter is the leftmost column in the table display)
  headers     <- c(meas_names, dim_names)

  print(paste0("HyperCube size: ", total_rows, " rows x ", total_cols, " cols"))

  # Guard: schema check
  missing_cols <- setdiff(EXPECTED_COLUMNS, headers)
  if (length(missing_cols) > 0) {
    abort_with_failure(
      paste0("AWIA schema changed; missing expected columns: ",
             paste(missing_cols, collapse = ", "))
    )
  }

  # Paginate through all rows
  all_rows <- list()
  top      <- 0L
  req_id   <- 10L
  while (top < total_rows) {
    height  <- as.integer(min(QLIK_PAGE_HEIGHT, total_rows - top))
    page_resp <- qlik_call(session, req_id, "GetHyperCubeData", obj_handle,
                           list(
                             qPath  = "/qHyperCubeDef",
                             qPages = list(list(
                               qLeft   = 0L,
                               qTop    = top,
                               qWidth  = total_cols,
                               qHeight = height
                             ))
                           ))
    matrix    <- page_resp$result$qDataPages[[1]]$qMatrix
    page_text <- lapply(matrix, function(row) {
      vapply(row, function(cell) {
        v <- cell$qText
        if (is.null(v)) NA_character_ else v
      }, character(1))
    })
    all_rows <- c(all_rows, page_text)
    top      <- top + height
    req_id   <- req_id + 1L
  }

  df <- as.data.frame(do.call(rbind, all_rows), stringsAsFactors = FALSE)
  colnames(df) <- headers
  df
}, error = function(e) {
  abort_with_failure(paste0("Qlik pull failed: ", conditionMessage(e)))
})

###############################################################################
# Part two: standardize columns and filter to CWS
###############################################################################
print("Standardizing columns and filtering to CWS")

awia_tidy <- awia_raw %>%
  janitor::clean_names() %>%
  # drop the Qlik-internal row counter
  select(-any_of("results")) %>%
  mutate(
    pwsid                       = str_squish(pwsid),
    community_water_system_name = str_squish(community_water_system_name),
    town_city                   = str_squish(town_city),
    state                       = str_squish(state),
    # ~50 systems carry "Under Review" in the population field; these become NA.
    population_served           = suppressWarnings(parse_number(population_served)),
    date_epic_captured          = as.character(Sys.Date()),
    date_worker_last_ran        = as.character(Sys.Date())
  ) %>%
  # keep only PWSIDs that are tracked in EPA SABs (matches the existing pattern
  # used by every state and federal worker in this repo)
  filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
  relocate(
    pwsid, community_water_system_name, town_city, state,
    wholesaler, population_served, water_system_size,
    rra_certification, erp_certification,
    date_epic_captured, date_worker_last_ran
  )

print(paste0("Filtered to ", nrow(awia_tidy), " CWS records (from ", nrow(awia_raw), " total)"))

# Sanity check: if the filter dropped everything, something's wrong
if (nrow(awia_tidy) == 0) {
  abort_with_failure("No AWIA records matched EPA SABs PWSIDs. Possible schema or filter mismatch.")
}

###############################################################################
# Part three: write raw to S3
###############################################################################
print("Writing raw dataset to S3")

tmp <- tempfile()
write.csv(awia_raw, file = paste0(tmp, ".csv"), row.names = FALSE)
on.exit(unlink(tmp), add = TRUE)
put_object(
  file   = paste0(tmp, ".csv"),
  object = raw_s3_link
)

###############################################################################
# Part four: write clean to S3
###############################################################################
print("Writing clean dataset to S3")

tmp <- tempfile()
write.csv(awia_tidy, file = paste0(tmp, ".csv"), row.names = FALSE)
on.exit(unlink(tmp), add = TRUE)
put_object(
  file   = paste0(tmp, ".csv"),
  object = clean_s3_link
)

###############################################################################
# Part five: update task manager
###############################################################################
print("Updating Task Manager")

task_manager_df <- data.frame(dataset = dataset_i,
                              date_downloaded = Sys.Date(),
                              raw_link   = raw_s3_link,
                              clean_link = clean_s3_link)

if (!(dataset_i %in% task_manager$dataset)) {
  task_manager <- bind_rows(task_manager, data.frame(dataset = dataset_i))
}

dont_touch_these_columns <- setdiff(names(task_manager), names(task_manager_df))
task_manger_simple <- task_manager %>%
  select(dataset, all_of(dont_touch_these_columns))

updated_row <- task_manger_simple %>%
  inner_join(task_manager_df, by = "dataset") %>%
  mutate(across(everything(), ~ as.character(.)))

updated_task_manager <- task_manager %>%
  filter(dataset != dataset_i) %>%
  bind_rows(updated_row) %>%
  arrange(dataset)

tmp <- tempfile()
write.csv(updated_task_manager, file = paste0(tmp, ".csv"), row.names = FALSE)
on.exit(unlink(tmp), add = TRUE)
put_object(
  file   = paste0(tmp, ".csv"),
  object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv",
  acl    = "public-read"
)

print("Worker Job Complete - Shutting Down")
