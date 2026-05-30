###############################################################################
# QA/QC Check Helpers (issue #22)
# Thin wrappers around the pointblank package that give every worker a
# consistent way to:
#   1. run validation checks on a data frame / sf object, with per-check
#      severity (warning vs error)
#   2. summarize the results into a short status string and an abort flag
#   3. write two artifacts to the S3 checks/ directory: pointblank's HTML
#      report and a CSV of the per-check results
#
# Design notes (verified against pointblank 0.12.x):
# - sf objects: pointblank's column checks (col_vals_*, rows_complete) call
#   dplyr::mutate() internally, which errors on a raw sf object. For checks
#   that operate on the whole table (row count, geometry validity) we use
#   specially(), which receives the table as an argument and avoids that.
#   For column-level checks, drop the geometry first with st_drop_geometry().
# - abort decision: use the "S" (stop) column from the agent report, not
#   all_passed(), because all_passed() also trips on warning-only failures
#   and would defeat the warn-and-continue behavior.
###############################################################################

library(pointblank)

# action_levels for a given severity. "stop" fails the worker, "warn" logs and
# continues. A single failing unit trips the level. Each severity sets only its
# own threshold so a failing check trips exactly one level (a stop-level failure
# sets S but not W), which keeps the summary counts unambiguous.
.check_action_levels <- function(severity = c("stop", "warning")) {
  severity <- match.arg(severity)
  if (severity == "stop") {
    action_levels(stop_at = 1)
  } else {
    action_levels(warn_at = 1)
  }
}

###############################################################################
# new_check_agent(data, label)
# Create a pointblank agent for a data frame or sf object. Add validation
# steps to the returned agent with the helpers below (or pointblank's own
# functions), then call interrogate().
###############################################################################
new_check_agent <- function(data, label) {
  create_agent(tbl = data, tbl_name = label, label = label)
}

###############################################################################
# check_row_count_range(agent, min_rows, max_rows, severity)
# Assert the table's row count falls within [min_rows, max_rows]. Uses
# specially() so it works on sf objects and reports a single validation unit.
###############################################################################
check_row_count_range <- function(agent, min_rows, max_rows, severity = "stop") {
  specially(
    agent,
    fn = function(t) {
      n <- nrow(t)
      n >= min_rows && n <= max_rows
    },
    actions = .check_action_levels(severity),
    label = sprintf("row_count in [%d, %d]", min_rows, max_rows)
  )
}

###############################################################################
# check_column_complete(agent, column, severity)
# Assert a column has no missing values (standard pointblank col_vals check).
# For sf objects, build the agent on st_drop_geometry(sf_obj) first, since
# pointblank's column checks call dplyr::mutate() which errors on raw sf.
###############################################################################
check_column_complete <- function(agent, column, severity = "stop") {
  col_vals_not_null(
    agent,
    columns = {{ column }},
    actions = .check_action_levels(severity),
    label = sprintf("%s is complete", rlang::as_label(rlang::enquo(column)))
  )
}

###############################################################################
# check_column_all_true(agent, column, severity)
# Assert every value in a logical column is TRUE. General-purpose; for spatial
# data, precompute geometry validity as a column and check it here, e.g.
#   attrs <- sf_obj |> mutate(geom_valid = sf::st_is_valid(sf_obj)) |>
#            sf::st_drop_geometry()
#   new_check_agent(attrs, ...) |> check_column_all_true(geom_valid, "warning")
# This keeps all checks on a plain data frame and avoids the sf mutate issue.
###############################################################################
check_column_all_true <- function(agent, column, severity = "stop") {
  col_vals_equal(
    agent,
    columns = {{ column }},
    value = TRUE,
    actions = .check_action_levels(severity),
    label = sprintf("%s all TRUE", rlang::as_label(rlang::enquo(column)))
  )
}

###############################################################################
# summarize_checks(agent)
# Run after interrogate(). Returns a list:
#   report_df  - the per-check results as a plain data.frame
#   summary    - a short "N passed, M warnings, K errors" string
#   any_error  - TRUE if any stop-level threshold fired OR a check errored
#                during evaluation (a broken check is treated as a failure)
#   any_warn   - TRUE if any warn-level threshold fired
###############################################################################
summarize_checks <- function(agent) {
  rpt <- as.data.frame(get_agent_report(agent, display_table = FALSE))

  n_passed  <- sum(rpt$eval == "OK" & rpt$n_pass == rpt$units, na.rm = TRUE)
  n_warned  <- sum(rpt$W %in% TRUE)
  n_stopped <- sum(rpt$S %in% TRUE)
  n_errored <- sum(rpt$eval == "ERROR", na.rm = TRUE)

  parts <- paste0(n_passed, " passed")
  if (n_warned  > 0) parts <- c(parts, paste0(n_warned,  " warning", if (n_warned  == 1) "" else "s"))
  if (n_stopped > 0) parts <- c(parts, paste0(n_stopped, " error",   if (n_stopped == 1) "" else "s"))
  if (n_errored > 0) parts <- c(parts, paste0(n_errored, " eval-error", if (n_errored == 1) "" else "s"))

  list(
    report_df = rpt,
    summary   = paste(parts, collapse = ", "),
    any_error = any(rpt$S %in% TRUE) || any(rpt$eval == "ERROR", na.rm = TRUE),
    any_warn  = any(rpt$W %in% TRUE)
  )
}

###############################################################################
# write_check_artifacts(agent, report_df, checks_base, tag, run_ts)
# Write pointblank's HTML report and a CSV of results to the S3 checks/
# directory. Returns the S3 URL of the HTML report (for the task manager).
# Filenames are date-stamped so a history is retained.
###############################################################################
write_check_artifacts <- function(agent, report_df, checks_base, tag, run_ts) {
  date_stamp <- format(run_ts, "%Y%m%d")

  # HTML report (self-contained; no headless browser needed for HTML export)
  html_local <- tempfile(fileext = ".html")
  export_report(agent, filename = html_local, quiet = TRUE)
  html_obj <- file.path(checks_base, sprintf("%s_report_%s.html", tag, date_stamp))
  put_object(file = html_local, object = html_obj, acl = "public-read")

  # CSV of per-check results. Flatten the list-column before writing.
  csv_df <- report_df[, c("i", "type", "columns", "values", "eval",
                          "units", "n_pass", "f_pass", "W", "S", "N")]
  csv_df$columns <- vapply(csv_df$columns,
                           function(x) paste(unlist(x), collapse = ";"),
                           character(1))
  csv_df$tag <- tag
  csv_df$check_run <- format(run_ts, "%Y-%m-%dT%H:%M:%S%z")
  csv_local <- tempfile(fileext = ".csv")
  write.csv(csv_df, csv_local, row.names = FALSE)
  csv_obj <- file.path(checks_base, sprintf("%s_checks_%s.csv", tag, date_stamp))
  put_object(file = csv_local, object = csv_obj, acl = "public-read")

  html_obj
}
