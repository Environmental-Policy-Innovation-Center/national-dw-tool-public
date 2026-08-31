###############################################################################
# Shared helpers for the state boil-water-notice (BWN) pipelines (issue #38)
#
# Each state fetches from a different source, but the reconcile logic falls into
# two families driven by how the source publishes advisories:
#
#   active feed     - the source lists only advisories currently in effect, so a
#                     record disappearing implies it was lifted and we date that
#                     closure to the day we noticed ("Assumed").
#                     e.g. Alaska, Missouri
#   rolling window  - the source publishes a moving window (roughly a year) and
#                     reports real lift dates, so we retain records that age out
#                     of the window and detect updates to advisories we already
#                     hold ("Reported").
#                     e.g. West Virginia
#
# Both families are parameterized by their key columns, so a new state is a
# key_cols vector rather than another copy of the diff. Everything else here
# (validation, the legacy abort guard, type normalization) is identical across
# states.
###############################################################################

# The columns the national BWN summary selects. Every clean_<st>_bwn dataset must
# lead with these, in this order (see 2_summarize_data.Rmd).
bwn_contract_cols <- c("pwsid", "date_issued", "date_lifted",
                       "epic_date_lifted_flag", "date_epic_captured_advisory",
                       "type", "state", "date_worker_last_ran")

# Columns the reconcile step maintains itself. They are absent from every source
# feed (the legacy workers appended them in the closed-advisory branch), so a
# first run has to seed them or the clean step fails on a missing column.
bwn_managed_cols <- c("date_lifted", "epic_date_lifted_flag")

#' Coerce every column of a data frame to character.
#' The reconcile step compares a freshly-pulled API response against a CSV
#' baseline, and the two arrive with different types, so both sides are flattened
#' through this function before being compared or bound. Applying it to BOTH
#' sides is what makes the join key reliable: run it on one side only and the
#' halves of the key can disagree.
#' Numerics are formatted per element rather than with as.character(). BWN join
#' keys include epoch-millisecond timestamps, and as.character() renders those as
#' 1.7e+12 at the default options(scipen), so the key would silently never match.
#' Per-element formatting at digits = 15 also avoids format()'s default 7
#' significant digits (which would truncate a coordinate like -159.167561616957)
#' and its habit of padding a whole column to a common width.
#' @param df Data frame
#' @return Data frame with every column as character
as_character_df <- function(df) {
  to_chr <- function(x) {
    if (!is.numeric(x)) return(as.character(x))
    vapply(
      x,
      function(v) if (is.na(v)) NA_character_ else
        format(v, scientific = FALSE, trim = TRUE, digits = 15),
      character(1)
    )
  }
  df %>% dplyr::mutate(dplyr::across(dplyr::everything(), to_chr))
}

#' Read a previous BWN raw pull, returning NULL only when it does not exist yet.
#' The legacy workers read the prior run with an unguarded s3read_using() and
#' died if the object was missing, which makes a genuinely new pipeline (or a
#' first run in the dev bucket) impossible.
#' Only a 404 is treated as "no baseline". Every other failure raises, because a
#' NULL here also zeroes the row-loss guard in validate_raw_bwn(), so a broad
#' catch would let a permissions change or a transient outage quietly replace the
#' accumulated history with a single feed pull.
#' @param link S3 key of the previous raw dataset
#' @param bucket Bucket name
#' @return Data frame, or NULL when the object does not exist
read_prior_bwn <- function(link, bucket = s3_bucket()) {
  absent <- tryCatch({
    s3_client()$head_object(Bucket = bucket, Key = link)
    FALSE
  }, error = function(e) {
    # ONLY a 404 means the object is not there. S3 answers 403 for an object
    # that exists when the caller lacks s3:ListBucket, and a throttle or outage
    # gives a 5xx: reading either as "first run" would zero out n_old, disarm
    # the row-loss guard, and let one feed pull overwrite accumulated history.
    if (inherits(e, "http_404")) return(TRUE)
    stop(sprintf("Could not check for a previous run at %s: %s",
                 link, conditionMessage(e)), call. = FALSE)
  })

  if (absent) {
    message(sprintf("No previous run found at %s, treating this as a first run.",
                    link))
    return(NULL)
  }
  # Any failure from here on is a real error and must not be swallowed either.
  # coerce_character = FALSE matters: s3_read_csv's own coercion uses
  # as.character(), which renders an epoch-millisecond value as "1.785024e+12"
  # at the default scipen. Reading typed and letting as_character_df() format
  # both sides keeps the two halves of the join key consistent.
  s3_read_csv(link, bucket = bucket, coerce_character = FALSE)
}

#' Drop scraped rows that carry no identity at all.
#' rvest's html_table(fill = TRUE) renders a header-only table as a single row
#' of NAs. Left alone, the reconcile sees a key of NA/NA/NA, finds it absent
#' from the baseline, and files it as a brand new advisory: a system-less,
#' date-less record that is written to the dataset and on into the national
#' summary, where it never ages out because it keeps matching itself.
#' This is a routine state, not an edge case. A state page with no active
#' notices publishes exactly this. The legacy Florida worker guarded against it
#' with an if() over a vector, which only worked when the scrape produced
#' exactly one new row; this is the same intent expressed so it works for any
#' number of rows.
#' @param df Fresh scrape
#' @param id_cols Columns that together identify a real advisory
#' @return df without the rows in which every id_col is missing
drop_empty_scraped_rows <- function(df, id_cols) {
  present <- intersect(id_cols, names(df))
  if (!length(present) || !nrow(df)) return(df)
  blank <- Reduce(`&`, lapply(present, function(col) {
    v <- as.character(df[[col]])
    is.na(v) | trimws(v) == ""
  }))
  if (any(blank)) {
    message(sprintf("Dropping %d scraped row(s) with no %s.",
                    sum(blank), paste(present, collapse = " or ")))
  }
  df[!blank, , drop = FALSE]
}

#' Ensure the columns the reconcile step maintains exist on a fresh pull.
#' @param df Fresh pull
#' @return Data frame with bwn_managed_cols present
.seed_bwn_managed_cols <- function(df) {
  for (col in bwn_managed_cols) {
    # rep_len keeps this working on a zero-row frame, where a scalar assignment
    # would throw "replacement has 1 row, data has 0". An empty pull is a real
    # scenario: Missouri's feed regularly has only a handful of CWS matches.
    if (!col %in% names(df)) df[[col]] <- rep_len(NA_character_, nrow(df))
  }
  df
}

#' Reconcile a fresh pull for a state whose source lists only ACTIVE advisories.
#' Records are partitioned four ways:
#'   new            - in the fresh pull only
#'   still active   - in both (the STORED row is kept, so the original detection
#'                    date survives)
#'   newly closed   - stored, gone from the feed, no lift date yet -> closed today
#'   already closed - stored, gone from the feed, already had a lift date
#' @param fresh Fresh pull, tidied
#' @param old Previous raw pull, or NULL on a first run
#' @param key_cols Character vector of columns identifying an advisory
#' @return Combined data frame
reconcile_bwn_active_feed <- function(fresh, old, key_cols) {
  fresh <- as_character_df(fresh)
  if (is.null(old) || nrow(old) == 0) {
    # Only a first run needs these seeded: `fresh` becomes the output directly
    # and the clean step reads both columns. With a baseline present, bind_rows
    # appends them from the stored side, which also preserves the legacy column
    # order (source columns first, maintained columns last).
    message("No baseline to reconcile against, keeping the fresh pull as-is.")
    return(.seed_bwn_managed_cols(fresh))
  }
  # Both sides go through the same formatter. read_prior_bwn() deliberately
  # reads the baseline typed so this call, not s3_read_csv's scipen-sensitive
  # as.character(), decides how a numeric key column is rendered.
  old <- as_character_df(old)
  .check_bwn_keys(fresh, old, key_cols)

  fresh_key <- .bwn_key(fresh, key_cols)
  old_key <- .bwn_key(old, key_cols)

  new_rows <- fresh[!(fresh_key %in% old_key), , drop = FALSE]
  still_active <- old[old_key %in% fresh_key, , drop = FALSE]

  dropped <- old[!(old_key %in% fresh_key), , drop = FALSE]
  newly_closed <- dropped %>%
    dplyr::filter(is.na(date_lifted)) %>%
    dplyr::mutate(date_lifted = as.character(Sys.Date()),
                  epic_date_lifted_flag = "Assumed")
  already_closed <- dplyr::filter(dropped, !is.na(date_lifted))

  dplyr::bind_rows(new_rows, still_active, newly_closed, already_closed)
}

#' Reconcile a fresh pull for a state whose source is a ROLLING WINDOW.
#' The feed only covers a recent period and reports real lift dates, so two keys
#' are needed:
#'   key_cols        identify a specific version of an advisory (including its
#'                   lift date), so a change to it means the advisory was updated
#'   update_key_cols identify the advisory itself, independent of its lift date
#' A fresh row whose update key matches but whose full key does not is the same
#' advisory with new information (typically a lift date now reported), so it
#' supersedes the stored row. Stored rows that are not superseded are carried
#' forward untouched, which is what retains advisories aged out of the window.
#' @param fresh Fresh pull, tidied
#' @param old Previous raw pull, or NULL on a first run
#' @param key_cols Columns identifying a specific version of an advisory
#' @param update_key_cols Columns identifying the advisory itself
#' @return Combined data frame
reconcile_bwn_rolling_window <- function(fresh, old, key_cols, update_key_cols) {
  # No managed-column seeding here: a rolling-window source reports its own lift
  # dates (date_lifted is part of key_cols), and the flag is set in the clean
  # step, so seeding would add a spurious all-NA column to the raw dataset.
  fresh <- as_character_df(fresh)
  if (is.null(old) || nrow(old) == 0) {
    message("No baseline to reconcile against, keeping the fresh pull as-is.")
    return(fresh)
  }
  old <- as_character_df(old)
  .check_bwn_keys(fresh, old, union(key_cols, update_key_cols))

  # last_epic_run_date records when we first saw an advisory, so it is excluded
  # from both keys: including it would make every stored row look changed.
  fresh_cmp <- dplyr::select(fresh, -dplyr::any_of("last_epic_run_date"))
  old_cmp <- dplyr::select(old, -dplyr::any_of("last_epic_run_date"))

  fresh_full <- .bwn_key(fresh_cmp, key_cols)
  old_full <- .bwn_key(old_cmp, key_cols)
  fresh_upd <- .bwn_key(fresh_cmp, update_key_cols)
  old_upd <- .bwn_key(old_cmp, update_key_cols)

  is_update <- !(fresh_full %in% old_full) & (fresh_upd %in% old_upd)
  is_new <- !(fresh_full %in% old_full) & !(fresh_upd %in% old_upd)

  updated_rows <- fresh_cmp[is_update, , drop = FALSE] %>%
    dplyr::mutate(last_epic_run_date = as.character(Sys.Date()))
  new_rows <- fresh_cmp[is_new, , drop = FALSE] %>%
    dplyr::mutate(last_epic_run_date = as.character(Sys.Date()))

  # stored rows not being superseded keep their original last_epic_run_date
  superseded <- .bwn_key(fresh_cmp[is_update, , drop = FALSE], update_key_cols)
  carried_forward <- old[!(old_upd %in% superseded), , drop = FALSE]

  dplyr::bind_rows(new_rows, updated_rows, carried_forward)
}

#' Paste the key columns of a data frame into a single identity string.
#' A separator is required: pasting bare would let neighbouring columns run
#' together, so ("AK1", "23456") and ("AK12", "3456") would produce the same key
#' and two distinct advisories would silently collapse into one. The unit
#' separator cannot occur in this data.
#' @param df Data frame
#' @param key_cols Character vector of column names
#' @return Character vector
.bwn_key <- function(df, key_cols) {
  do.call(paste, c(unname(as.list(df[key_cols])), sep = .bwn_key_sep))
}

# ASCII unit separator, chosen because it cannot appear in the source data
.bwn_key_sep <- "\u001f"

#' Stop with a clear message if either side is missing a key column.
#' @param fresh Fresh pull
#' @param old Stored pull
#' @param key_cols Required columns
.check_bwn_keys <- function(fresh, old, key_cols) {
  missing_fresh <- setdiff(key_cols, names(fresh))
  missing_old <- setdiff(key_cols, names(old))
  if (length(missing_fresh) || length(missing_old)) {
    stop(sprintf(
      "BWN key columns missing (fresh: %s | stored: %s)",
      if (length(missing_fresh)) paste(missing_fresh, collapse = ", ") else "none",
      if (length(missing_old)) paste(missing_old, collapse = ", ") else "none"
    ), call. = FALSE)
  }
  invisible(TRUE)
}

#' Pointblank validations shared by every raw BWN dataset.
#' Carries over the legacy abort guard: the workers refused to write when the new
#' pull had fewer rows than the previous one, or was empty. Here it is a
#' stop-severity check, so it produces a validation artifact and main_runner
#' records the failure in the dataset registry.
#' @param config Main config
#' @param bwn Reconciled raw BWN data
#' @param bwn_old Previous raw pull, or NULL on a first run
#' @param dataset_id e.g. "raw_ak_bwn"
#' @param label Human-readable agent label
#' @param pwsid_col Name of the system-id column in the RAW data. Most states use
#'   "pwsid", but Missouri's source calls it "pws_id" until the clean step
#'   renames it.
#' @param pwsid_severity Severity of the system-id completeness check. Defaults
#'   to "stop". Some sources publish only a system NAME, so their worker joins to
#'   sabs_pwsid_names by name and keeps the rows that do not match (Washington
#'   matches roughly a third of its rows). For those states this is "warning":
#'   an unmatched advisory is still a real advisory the national summary has to
#'   count, so dropping it would undercount, and stopping would abort every run.
validate_raw_bwn <- function(config, bwn, bwn_old, dataset_id, label,
                             pwsid_col = "pwsid", pwsid_severity = "stop") {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()
  n_old <- if (is.null(bwn_old)) 0 else nrow(bwn_old)

  if (!pwsid_col %in% names(bwn)) {
    stop(sprintf("%s: expected system-id column '%s' not found in the raw data",
                 dataset_id, pwsid_col), call. = FALSE)
  }

  checks_df <- bwn %>%
    dplyr::mutate(
      system_id_present = !is.na(.data[[pwsid_col]]) & .data[[pwsid_col]] != ""
    )
  print(tibble::tibble(rows = nrow(bwn), prior_rows = n_old,
                       no_row_loss = nrow(bwn) >= n_old))

  # The two scalar guards use specially(): it is handed the whole table and
  # always reports exactly one validation unit, so an EMPTY dataset still fails
  # them. Deriving them from the rows (as a col_vals_* check does) would give
  # zero units on an empty table and pointblank would report "passed" on
  # precisely the case the legacy guard existed to catch.
  agent <- new_check_agent(checks_df, label = label) %>%
    specially(
      fn = function(t) nrow(t) > 0,
      actions = action_levels(stop_at = 1),
      label = "BWN dataset has > 0 rows"
    ) %>%
    specially(
      fn = function(t) nrow(t) >= n_old,
      actions = action_levels(stop_at = 1),
      label = sprintf("no row loss vs the previous run (%d rows)", n_old)
    ) %>%
    check_column_all_true(system_id_present, severity = pwsid_severity) %>%
    interrogate()

  .report_bwn_checks(agent, checks_base, dataset_id, run_ts)
}

#' Pointblank validations shared by every clean BWN dataset.
#' Checks the contract the national BWN summary depends on.
#' @param config Main config
#' @param bwn_clean Standardized BWN data
#' @param dataset_id e.g. "clean_ak_bwn"
#' @param label Human-readable agent label
#' @param pwsid_severity Severity of the pwsid completeness check. See
#'   validate_raw_bwn(); the name-joined states pass "warning" here for the same
#'   reason.
validate_clean_bwn <- function(config, bwn_clean, dataset_id, label,
                               pwsid_severity = "stop") {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  checks_df <- bwn_clean %>%
    dplyr::mutate(
      lifted_flag_valid = epic_date_lifted_flag %in% c("Assumed", "Reported")
    )

  agent <- new_check_agent(checks_df, label = label) %>%
    check_column_complete(pwsid, severity = pwsid_severity) %>%
    check_column_complete(date_issued, severity = "warning") %>%
    check_column_all_true(lifted_flag_valid, severity = "stop") %>%
    interrogate()

  .report_bwn_checks(agent, checks_base, dataset_id, run_ts)
}

#' Summarize an interrogated agent, push its artifacts to S3, and abort on error.
#' Not BWN-specific: this is the summarize/report/abort block every pipeline in
#' pipelines/ repeats. Worth promoting into functions/checks.R eventually so the
#' other pipelines can drop their copies.
#' @param agent Interrogated pointblank agent
#' @param checks_base S3 prefix for check artifacts
#' @param dataset_id Dataset id, used as the artifact tag
#' @param run_ts Run timestamp
.report_bwn_checks <- function(agent, checks_base, dataset_id, run_ts) {
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
  message(sprintf("%s validation checks passed successfully.", dataset_id))
  invisible(TRUE)
}

#' Apply the shared tail of a clean BWN dataset.
#' The caller supplies the state-specific mapping (which source column becomes
#' date_issued / date_lifted, the type value, the state name, and the
#' epic_date_lifted_flag); this puts the contract columns in the required order
#' and stamps the run date.
#' @param df Data frame already carrying date_issued, date_lifted,
#'   epic_date_lifted_flag, type, state and last_epic_run_date
#' @return Data frame with the contract columns leading
finalize_bwn_clean <- function(df) {
  df %>%
    dplyr::rename(date_epic_captured_advisory = last_epic_run_date) %>%
    dplyr::mutate(date_worker_last_ran = Sys.Date()) %>%
    dplyr::relocate(dplyr::all_of(bwn_contract_cols))
}
