###############################################################################
# AWIA Certification
#
# raw_awia pulls AWIA Section 2013 / SDWA Section 1433 Risk and Resilience
# Assessment (RRA) and Emergency Response Plan (ERP) certification data from
# EPA's public Qlik dashboard via the underlying Qlik Engine WebSocket API.
#
# clean_awia filters the raw pull to PWSIDs in EPA SABs. This is the dataset
# that gets staged and shipped in the public data downloads.
#
# Source dashboard:
# https://awsedap.epa.gov/public/extensions/awia-active-utility-public-data/index.html
###############################################################################

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
AWIA_EXPECTED_COLUMNS <- c(
  "PWSID", "Community Water System Name", "Town/City", "State",
  "Wholesaler", "Population Served", "Water System Size",
  "RRA Certification", "ERP Certification"
)

#' Pull raw AWIA certification data then run the clean AWIA pipeline
#' @param config Main config
#' @param dataset_id "raw_awia_certification"
run_awia_certification_pipeline <- function(config, dataset_id) {
  update_raw_awia(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Open a WebSocket to the Qlik Engine API and return the connection plus a
#' response store keyed by JSON-RPC id. Server-side notifications (no id)
#' are ignored.
#' @param url Qlik Engine WebSocket URL
#' @param timeout_sec Seconds to wait for the connection to open
open_qlik_session <- function(url, timeout_sec = 30) {
  ws <- websocket::WebSocket$new(url)
  store <- new.env()
  store$by_id <- list()

  ws$onMessage(function(event) {
    msg <- tryCatch(
      jsonlite::fromJSON(event$data, simplifyVector = FALSE),
      error = function(e) NULL
    )
    if (!is.null(msg) && !is.null(msg$id)) {
      store$by_id[[as.character(msg$id)]] <- msg
    }
  })

  deadline <- Sys.time() + timeout_sec
  while (ws$readyState() != 1L) {
    if (Sys.time() > deadline) {
      stop("Timed out connecting to Qlik WebSocket: ", url, call. = FALSE)
    }
    later::run_now(0.1)
  }
  list(ws = ws, store = store)
}

#' Send a JSON-RPC request to the Qlik Engine API and block until the
#' matching response arrives.
#' @param session A session from open_qlik_session()
#' @param req_id JSON-RPC request id
#' @param method Qlik Engine API method name
#' @param handle Qlik object/doc handle
#' @param params Method params
#' @param timeout_sec Seconds to wait for the response
qlik_call <- function(session, req_id, method, handle, params, timeout_sec = 60) {
  payload <- list(
    jsonrpc = "2.0",
    id      = req_id,
    method  = method,
    handle  = handle,
    params  = params
  )
  session$ws$send(jsonlite::toJSON(payload, auto_unbox = TRUE))

  key <- as.character(req_id)
  deadline <- Sys.time() + timeout_sec
  while (is.null(session$store$by_id[[key]])) {
    if (Sys.time() > deadline) {
      stop("Timed out waiting for ", method, " (id=", req_id, ")", call. = FALSE)
    }
    later::run_now(0.2)
  }
  resp <- session$store$by_id[[key]]
  if (!is.null(resp$error)) {
    stop("Qlik API error on ", method, ": ", resp$error$message, call. = FALSE)
  }
  resp
}

#' Pull the AWIA hypercube from EPA's Qlik dashboard via paginated
#' GetHyperCubeData calls.
#' @return Data frame of raw AWIA rows, columns named from the Qlik layout
pull_awia_from_qlik <- function() {
  message("Connecting to Qlik Engine API...")
  session <- open_qlik_session(QLIK_WS_URL)
  on.exit(try(session$ws$close(), silent = TRUE), add = TRUE)

  # Open the doc on the global handle (-1)
  doc_resp   <- qlik_call(session, 1L, "OpenDoc", -1L, list(qDocName = QLIK_APP_ID))
  doc_handle <- doc_resp$result$qReturn$qHandle

  # Get the table object
  obj_resp   <- qlik_call(session, 2L, "GetObject", doc_handle, list(qId = QLIK_TABLE_VIZ_ID))
  obj_handle <- obj_resp$result$qReturn$qHandle
  viz_type   <- obj_resp$result$qReturn$qGenericType

  # Guard: viz type changed (EPA reconfigured the dashboard)
  if (!identical(viz_type, QLIK_TABLE_VIZ_TYPE)) {
    stop(sprintf("AWIA Qlik viz type changed (expected '%s', got '%s'). Review dashboard configuration.",
                 QLIK_TABLE_VIZ_TYPE, viz_type), call. = FALSE)
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

  message(sprintf("HyperCube size: %d rows x %d cols", total_rows, total_cols))

  # Guard: schema check
  missing_cols <- setdiff(AWIA_EXPECTED_COLUMNS, headers)
  if (length(missing_cols) > 0) {
    stop(sprintf("AWIA schema changed; missing expected columns: %s",
                 paste(missing_cols, collapse = ", ")), call. = FALSE)
  }

  message("Paginate through all rows...")
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
}

#' Pull raw AWIA certification data from EPA's Qlik dashboard, validate, and
#' write to S3.
#' @param config Main config
#' @param dataset_id "raw_awia"
update_raw_awia <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  link <- sub_config$link

  message("Pulling AWIA certification data from EPA's Qlik dashboard...")
  awia_raw <- pull_awia_from_qlik()

  message("Validating raw AWIA...")
  validate_raw_awia(config, awia_raw, dataset_id)

  message(sprintf("Writing raw AWIA to S3 at %s...", link))
  s3_write_csv(awia_raw, link)
}

#' Pointblank validations for raw AWIA
validate_raw_awia <- function(config, awia_raw, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  agent <- new_check_agent(awia_raw, label = "AWIA Validation") %>%
    check_column_complete(PWSID, severity = "stop") %>%
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

  message("AWIA validation checks passed successfully.")
  return(TRUE)
}

#' Filter raw AWIA to community water systems in EPA SABs
#' @param config Main config
#' @param dataset_id "clean_awia_certification"
run_clean_awia_certification_pipeline <- function(config, dataset_id = "clean_awia") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link
  link <- sub_config$link

  message("Downloading raw AWIA from S3...")
  awia_raw <- s3_read_csv(raw_link, coerce_character = FALSE)

  message("Reading EPA SABs PWSIDs to filter for community water systems...")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)

  message("Standardizing columns and filtering to community water systems...")
  awia_clean <- awia_raw %>%
    janitor::clean_names() %>%
    select(-any_of("results")) %>%
    mutate(
      pwsid = str_squish(pwsid),
      community_water_system_name = str_squish(community_water_system_name),
      town_city = str_squish(town_city),
      state = str_squish(state),
      # ~50 systems carry "Under Review" in the population field; these become NA.
      population_served = suppressWarnings(parse_number(population_served)),
      last_epic_run_date = Sys.Date()
    ) %>%
    # keep only PWSIDs that are tracked in EPA SABs
    filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
    relocate(
      pwsid, community_water_system_name, town_city, state,
      wholesaler, population_served, water_system_size,
      rra_certification, erp_certification,
      last_epic_run_date
    )

  message(sprintf("Filtered to %d CWS records (from %d total)", nrow(awia_clean), nrow(awia_raw)))

  message("Validating clean_awia...")
  validate_clean_awia(config, awia_clean, dataset_id)

  message(sprintf("Writing clean AWIA to S3 at %s...", link))
  s3_write_csv(awia_clean, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(awia_clean)
}

#' Pointblank validations for clean AWIA
validate_clean_awia <- function(config, awia_clean, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  agent <- new_check_agent(awia_clean, label = "Clean AWIA Validation") %>%
    check_column_complete(pwsid, severity = "stop") %>%
    rows_distinct(columns = vars(pwsid), actions = action_levels(warn_at = 1),
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

  message("Clean AWIA validation checks passed successfully.")
  return(TRUE)
}
