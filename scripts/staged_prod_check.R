###############################################################################
# Staged-vs-Prod Data Check (issue #26; uses the #35 coverage resolver)
#
# A pre-promotion sanity check: before staging data replaces prod, compare every
# staging file against its prod baseline and write a datasets-x-checks matrix
# report (HTML + CSV) to the S3 checks/ directory. Prod is the baseline, staging
# is the target.
#
# Run it (from the repo root, with AWS creds for the tech-team-data bucket):
#   Rscript scripts/staged_prod_check.R                    # upload report to S3 checks/
#   Rscript scripts/staged_prod_check.R --dry-run          # summary only, nothing written
#   Rscript scripts/staged_prod_check.R --dry-run --out=.  # also save the report locally
# The uploaded report lands at s3://<bucket>/national-dw-tool/checks/staged_prod_report_<date>.html
#
# This file has three parts:
#   1. diff_dataset()          - grade one staging table vs its prod baseline
#   2. the report builders     - assemble + render the matrix, write it to S3
#   3. run_staged_prod_check() - the CLI: list the buckets, run the per-file diff,
#                                add file-presence + last-pulled, handle the two
#                                non-tabular edge cases, write the report
# The spatial-coverage resolver lives in its own file (spatial_coverage.R,
# issue #35) because it is reused outside this check (dataset registry).
#
# Checks (Christine's #26 checklist):
#   file_present, staging_not_empty, missing_columns, new_columns, schema_types,
#   row_count_change, coverage_change, last_pulled
#
# Key design notes:
# - "warn" vs "fail": a missing column or a row change past the stop threshold is
#   a "fail"; a type change is a "warn" (CSV type inference is noisy: a column of
#   whole numbers can read as integer or double run to run). A new column or a
#   coverage change is a "warn".
# - coverage_change uses served state via the crosswalk (spatial_coverage.R);
#   UNKNOWN pwsids are excluded from the set diff.
# - the geojson is never loaded (1.1 GB); it is compared by object size as a
#   row-change proxy, and its column/coverage checks report "na".
# - the cartographic-boundaries folder holds zips: presence + last-pulled only.
# - last_pulled uses S3 LastModified per bucket (true per-object recency; the task
#   manager's date_downloaded is a single value and was stale). It is
#   informational: shows both dates, notes when staging is older, never warns.
# - the report colors pass/warn/fail/na, de-dups (dataset,check) worst-status-wins,
#   and HTML-escapes all detail text.
###############################################################################

if (!exists("get_spatial_coverage", mode = "function")) {
  source("scripts/spatial_coverage.R")
}
library(pointblank)
library(aws.s3)

###############################################################################
## Part 1 - per-file diff #####################################################
###############################################################################

# default_diff_thresholds(): the % row-count change bands. Tunable; the team can
# adjust once they see them on real data (per Christine's threshold request).
default_diff_thresholds <- function() {
  list(row_warn = 0.10, row_stop = 0.25)
}

# .schema_matches(staging, prod): boolean, does staging's schema (column names +
# types) match prod, via pointblank's col_schema_match built-in.
.schema_matches <- function(staging, prod) {
  agent <- create_agent(tbl = staging)
  # in_order = FALSE: a harmless column reorder should not read as a type change.
  # This helper is only called when the column sets already match, so it isolates
  # a genuine type difference.
  agent <- col_schema_match(agent, schema = col_schema(.tbl = prod),
                            in_order = FALSE)
  agent <- interrogate(agent)
  rpt <- as.data.frame(get_agent_report(agent, display_table = FALSE))
  # Fail closed: require a real, non-NA passing row. all(logical(0)) and
  # all(NA, na.rm = TRUE) both return TRUE, so an errored or empty report must
  # not be read as a match.
  nrow(rpt) > 0 && !anyNA(rpt$n_pass) && all(rpt$n_pass == rpt$units)
}

###############################################################################
# diff_dataset(staging, prod, dataset, lookup = NULL, thresholds = ...)
# staging, prod : data frames for the same dataset (staging = target).
# dataset       : dataset name (label for the result rows).
# lookup        : prebuilt coverage lookup (build_coverage_lookup()); required
#                 for the coverage check, else that check reports "na".
# returns       : data.frame(dataset, check, status, detail), status in
#                 {pass, warn, fail, na}. Geojson and the cartographic zip folder
#                 are NOT passed here (the CLI handles them) since they are not
#                 plain tabular comparisons.
###############################################################################
diff_dataset <- function(staging, prod, dataset, lookup = NULL,
                         thresholds = default_diff_thresholds()) {
  out <- list()
  add <- function(check, status, detail) {
    out[[length(out) + 1]] <<- data.frame(
      dataset = dataset, check = check, status = status, detail = detail,
      stringsAsFactors = FALSE
    )
  }

  n_stg <- nrow(staging)
  n_prod <- nrow(prod)
  cols_stg <- names(staging)
  cols_prod <- names(prod)

  # 1. staging not empty
  add("staging_not_empty", if (n_stg >= 1) "pass" else "fail",
      sprintf("%d rows", n_stg))

  # 2/3. columns. Christine's checklist names "missing columns" and "new columns"
  # separately: a missing column (in prod, absent from staging) is serious; a new
  # column (in staging, not prod) is expected and just flagged.
  added <- setdiff(cols_stg, cols_prod)    # new in staging
  removed <- setdiff(cols_prod, cols_stg)  # missing from staging
  add("missing_columns", if (length(removed) == 0) "pass" else "fail",
      if (length(removed) == 0) "none" else paste(removed, collapse = ", "))
  add("new_columns", if (length(added) == 0) "pass" else "warn",
      if (length(added) == 0) "none" else paste(added, collapse = ", "))

  # 4. column types. Only meaningful when the column sets match, otherwise the
  # missing/new checks already carry the signal. pointblank col_schema_match.
  # (warn, not fail: CSV type inference is noisy, so flag rather than block.)
  if (n_stg == 0) {
    add("schema_types", "na", "staging empty")
  } else if (length(added) == 0 && length(removed) == 0) {
    types_ok <- .schema_matches(staging, prod)
    add("schema_types", if (types_ok) "pass" else "warn",
        if (types_ok) "types match" else "column types differ")
  } else {
    add("schema_types", "na", "column set differs, see missing/new columns")
  }

  # 5. % row change (exact match is the 0% case)
  if (n_prod > 0) {
    pct <- (n_stg - n_prod) / n_prod
    status <- if (abs(pct) >= thresholds$row_stop) "fail" else
              if (abs(pct) >= thresholds$row_warn) "warn" else "pass"
    add("row_count_change", status,
        sprintf("prod=%d staging=%d (%+.1f%%)", n_prod, n_stg, 100 * pct))
  } else {
    add("row_count_change", "na", sprintf("prod=0 staging=%d", n_stg))
  }

  # 6. coverage change (served-state set diff, UNKNOWN excluded)
  has_pwsid <- "pwsid" %in% cols_stg && "pwsid" %in% cols_prod
  if (!has_pwsid || is.null(lookup)) {
    add("coverage_change", "na", "no pwsid column or no crosswalk")
  } else if (n_prod == 0) {
    add("coverage_change", "na", "prod baseline empty")
  } else {
    cov_s <- setdiff(get_spatial_coverage(staging, lookup = lookup), "UNKNOWN")
    cov_p <- setdiff(get_spatial_coverage(prod, lookup = lookup), "UNKNOWN")
    dropped <- setdiff(cov_p, cov_s)
    gained <- setdiff(cov_s, cov_p)
    if (length(dropped) == 0 && length(gained) == 0) {
      add("coverage_change", "pass",
          sprintf("%d states/territories, unchanged", length(cov_p)))
    } else {
      add("coverage_change", if (length(dropped) > 0) "warn" else "pass",
          paste(c(
            if (length(dropped)) paste0("dropped: ", paste(dropped, collapse = ", ")),
            if (length(gained))  paste0("added: ",   paste(gained, collapse = ", "))
          ), collapse = "; "))
    }
  }

  do.call(rbind, out)
}

###############################################################################
## Part 2 - the datasets-x-checks matrix report ###############################
###############################################################################

# Canonical left-to-right column order; unknown checks are appended in order seen.
# file_present and last_pulled come from the CLI; the middle six from diff_dataset.
.check_order <- c("file_present", "staging_not_empty", "missing_columns",
                  "new_columns", "schema_types", "row_count_change",
                  "coverage_change", "last_pulled")

# Human-friendly column headers for the report (the raw check keys are terse).
.check_labels <- c(
  file_present      = "file present",
  staging_not_empty = "not empty",
  missing_columns   = "missing columns",
  new_columns       = "new columns",
  schema_types      = "column types",
  row_count_change  = "row change",
  coverage_change   = "coverage change",
  last_pulled       = "last pulled"
)

# status -> cell colors (bg, fg). Christine's 4-level coloring: the threshold
# grading already happened in diff_dataset, so the report just colors by status.
.status_styles <- list(
  pass = c(bg = "#e6f4ea", fg = "#137333"),
  warn = c(bg = "#fef7e0", fg = "#8a5a00"),
  fail = c(bg = "#fce8e6", fg = "#c5221f"),
  na   = c(bg = "#f1f3f4", fg = "#5f6368"),
  none = c(bg = "#ffffff", fg = "#9aa0a6")
)

.esc <- function(x) {
  x <- gsub("&", "&amp;", x, fixed = TRUE)
  x <- gsub("<", "&lt;",  x, fixed = TRUE)
  x <- gsub(">", "&gt;",  x, fixed = TRUE)
  x
}

.cell_style <- function(status) {
  s <- .status_styles[[status]]
  if (is.null(s)) s <- .status_styles[["none"]]
  sprintf("background:%s;color:%s;", s[["bg"]], s[["fg"]])
}

# severity for worst-status-wins de-duplication
.severity <- c(fail = 3, warn = 2, pass = 1, na = 0)

# .normalize_results(results): coerce the four columns to character (a factor
# detail would crash rendering) and collapse any duplicate (dataset, check) rows
# to the WORST status, so a masked failure can never hide behind a pass and the
# matrix cannot disagree with the summary. Row (hence dataset) order is preserved.
.normalize_results <- function(results) {
  for (col in c("dataset", "check", "status", "detail")) {
    if (!is.null(results[[col]])) results[[col]] <- as.character(results[[col]])
  }
  if (nrow(results) <= 1) return(results)
  sev <- ifelse(results$status %in% names(.severity),
                .severity[results$status], -1)
  key <- paste(results$dataset, results$check, sep = "\r")
  keep <- vapply(split(seq_along(key), key),
                 function(ix) ix[which.max(sev[ix])], integer(1))
  results[sort(keep), , drop = FALSE]
}

# build_check_matrix(results): pivot the long results into a wide data frame
# (datasets x checks, holding the status). Not used by the report itself (the
# HTML and CSV work off the long form); provided for console inspection, e.g.
# print(build_check_matrix(res$results)).
build_check_matrix <- function(results) {
  results <- .normalize_results(results)
  datasets <- unique(results$dataset)
  present <- unique(results$check)
  checks <- c(intersect(.check_order, present), setdiff(present, .check_order))
  key <- paste(results$dataset, results$check, sep = "\r")
  m <- data.frame(dataset = datasets, stringsAsFactors = FALSE)
  for (ck in checks) {
    m[[ck]] <- results$status[match(paste(datasets, ck, sep = "\r"), key)]
  }
  m
}

# render_report_html(results, run_ts, title): build the self-contained HTML
# matrix report as a single string.
render_report_html <- function(results, run_ts,
                               title = "Staged vs prod data check") {
  results <- .normalize_results(results)
  datasets <- unique(results$dataset)
  present <- unique(results$check)
  checks <- c(intersect(.check_order, present), setdiff(present, .check_order))
  key <- paste(results$dataset, results$check, sep = "\r")

  cell <- function(ds, ck) {
    i <- match(paste(ds, ck, sep = "\r"), key)
    if (is.na(i)) {
      sprintf("<td style='%s'><div class='st'>&ndash;</div></td>", .cell_style("none"))
    } else {
      st <- results$status[i]
      dt <- results$detail[i]
      sprintf("<td style='%s'><div class='st'>%s</div>%s</td>",
              .cell_style(st), .esc(st),
              if (nzchar(dt)) sprintf("<div class='dt'>%s</div>", .esc(dt)) else "")
    }
  }

  labels <- vapply(checks, function(ck) {
    if (!is.na(.check_labels[ck])) .check_labels[[ck]] else ck
  }, character(1))
  header <- paste0("<th class='corner'>dataset</th>",
                   paste(sprintf("<th>%s</th>", .esc(labels)), collapse = ""))
  body <- paste(vapply(datasets, function(ds) {
    cells <- paste(vapply(checks, function(ck) cell(ds, ck), character(1)), collapse = "")
    sprintf("<tr><th class='ds'>%s</th>%s</tr>", .esc(ds), cells)
  }, character(1)), collapse = "\n")

  n_fail <- sum(results$status == "fail", na.rm = TRUE)
  n_warn <- sum(results$status == "warn", na.rm = TRUE)
  n_pass <- sum(results$status == "pass", na.rm = TRUE)
  n_na   <- sum(results$status == "na",   na.rm = TRUE)
  badge <- function(label, n, status) {
    sprintf("<span class='badge' style='%s'>%d %s</span>", .cell_style(status), n, label)
  }
  summary <- paste(badge("failed", n_fail, "fail"), badge("warnings", n_warn, "warn"),
                   badge("passed", n_pass, "pass"), badge("n/a", n_na, "na"))

  paste0(
"<!doctype html><html><head><meta charset='utf-8'>",
"<meta name='viewport' content='width=device-width, initial-scale=1'>",
sprintf("<title>%s</title>", .esc(title)),
"<style>",
"body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;",
"margin:24px;color:#202124;}",
"h1{font-size:20px;font-weight:600;margin:0 0 4px;}",
".meta{color:#5f6368;font-size:13px;margin-bottom:14px;}",
".summary{margin-bottom:16px;}",
".badge{display:inline-block;padding:3px 10px;border-radius:12px;font-size:13px;",
"font-weight:600;margin-right:8px;}",
".wrap{overflow-x:auto;border:1px solid #e0e0e0;border-radius:8px;}",
"table{border-collapse:collapse;width:100%;font-size:13px;}",
"th,td{border:1px solid #eceff1;padding:8px 10px;text-align:left;vertical-align:top;}",
"thead th{background:#f8f9fa;position:sticky;top:0;font-weight:600;white-space:nowrap;}",
"th.ds{background:#f8f9fa;font-weight:600;white-space:nowrap;}",
"td .st{font-weight:600;text-transform:capitalize;}",
"td .dt{font-size:11px;color:#5f6368;margin-top:2px;max-width:260px;}",
".corner{background:#f8f9fa;}",
"</style></head><body>",
sprintf("<h1>%s</h1>", .esc(title)),
sprintf("<div class='meta'>run %s &middot; %d datasets &middot; %d checks</div>",
        .esc(format(run_ts, "%Y-%m-%d %H:%M:%S %Z")), length(datasets), length(checks)),
sprintf("<div class='summary'>%s</div>", summary),
"<div class='wrap'><table><thead><tr>", header, "</tr></thead><tbody>",
body,
"</tbody></table></div></body></html>"
  )
}

# write_report_artifacts(results, checks_base, run_ts, upload = TRUE,
#                        local_dir = NULL): write the HTML report and a tidy
# (long-form) CSV, date-stamped so a history is retained. upload = TRUE puts them
# in the S3 checks/ dir. local_dir, when set, writes the local copies into that
# directory (created if needed) instead of a temp file, so a dry run lands
# somewhere findable. Returns the local paths and the intended S3 object keys.
write_report_artifacts <- function(results, checks_base, run_ts, upload = TRUE,
                                   local_dir = NULL) {
  results <- .normalize_results(results)
  checks_base <- sub("/+$", "", checks_base)  # avoid a double slash in the key
  date_stamp <- format(run_ts, "%Y%m%d")

  if (!is.null(local_dir) && !dir.exists(local_dir)) {
    dir.create(local_dir, recursive = TRUE)
  }
  local_path <- function(ext) {
    if (is.null(local_dir)) tempfile(fileext = ext)
    else file.path(local_dir, sprintf("staged_prod_%s_%s%s",
                                      if (ext == ".html") "report" else "checks",
                                      date_stamp, ext))
  }

  html_local <- local_path(".html")
  writeLines(render_report_html(results, run_ts), html_local)

  csv_df <- results
  csv_df$check_run <- format(run_ts, "%Y-%m-%dT%H:%M:%S%z")
  csv_local <- local_path(".csv")
  write.csv(csv_df, csv_local, row.names = FALSE)

  html_obj <- file.path(checks_base, sprintf("staged_prod_report_%s.html", date_stamp))
  csv_obj  <- file.path(checks_base, sprintf("staged_prod_checks_%s.csv", date_stamp))

  if (upload) {
    put_object(file = html_local, object = html_obj, acl = "public-read")
    put_object(file = csv_local,  object = csv_obj,  acl = "public-read")
  }

  list(html_local = html_local, csv_local = csv_local,
       html_obj = html_obj, csv_obj = csv_obj, uploaded = upload)
}

###############################################################################
## Part 3 - the CLI orchestrator ##############################################
###############################################################################

.parse_ts <- function(x) {
  if (inherits(x, "POSIXct")) return(x)
  s <- sub("T", " ", substr(as.character(x), 1, 19))  # tolerate "T" or space
  as.POSIXct(s, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
}

# latest timestamp of a (possibly empty) vector, NA instead of -Inf when empty
.latest <- function(ts) if (length(ts)) max(ts) else as.POSIXct(NA)

# .bucket_listing(bucket, prefix): objects under prefix as rel (path below
# prefix), size, last_modified. Folder markers and the zero-byte empty-name
# object are dropped.
.bucket_listing <- function(bucket, prefix) {
  df <- get_bucket_df(bucket = bucket, prefix = prefix, max = Inf)
  if (nrow(df) == 0) {
    return(data.frame(rel = character(0), key = character(0),
                      size = numeric(0), last_modified = as.POSIXct(character(0))))
  }
  rel <- sub(prefix, "", df$Key, fixed = TRUE)
  keep <- rel != "" & !grepl("/$", df$Key)
  data.frame(
    rel = rel[keep],
    key = df$Key[keep],
    size = as.numeric(df$Size[keep]),
    last_modified = .parse_ts(df$LastModified[keep]),
    stringsAsFactors = FALSE
  )
}

.read_csv_s3 <- function(bucket, key) {
  s3read_using(
    function(f) utils::read.csv(f, check.names = FALSE, stringsAsFactors = FALSE),
    object = key, bucket = bucket
  )
}

# .last_pulled_row: informational (Christine asked to *show* when each was pulled,
# not grade it). Shows both dates; when staging is older than prod it adds a
# "(staging older)" note, but stays a neutral pass so the resting state between
# promotions does not light up the whole report.
.last_pulled_row <- function(dataset, stg_ts, prod_ts) {
  fmt <- function(t) if (length(t) && !is.na(t)) format(t, "%Y-%m-%d") else "n/a"
  both <- length(stg_ts) == 1 && length(prod_ts) == 1 &&
    !is.na(stg_ts) && !is.na(prod_ts)
  # compare at day granularity to match the date-only display, so two objects
  # from the same day at different times do not read "(staging older)".
  note <- if (both && as.Date(stg_ts) < as.Date(prod_ts)) " (staging older)" else ""
  status <- if (both) "pass" else "na"
  data.frame(dataset = dataset, check = "last_pulled", status = status,
             detail = sprintf("staging %s, prod %s%s", fmt(stg_ts), fmt(prod_ts), note),
             stringsAsFactors = FALSE)
}

###############################################################################
# run_staged_prod_check(...)
# Orchestrate the full check and write the report. Returns (invisibly) the long
# results data frame plus the artifact paths.
###############################################################################
run_staged_prod_check <- function(
    bucket = "tech-team-data",
    prod_prefix = "national-dw-tool/prod/",
    staging_prefix = "national-dw-tool/staging/",
    crosswalk_rel = "sabs_pwsid_county.csv",
    checks_base = "s3://tech-team-data/national-dw-tool/checks",
    run_ts = Sys.time(),
    thresholds = default_diff_thresholds(),
    upload = TRUE,
    local_dir = NULL,
    verbose = TRUE) {

  say <- function(...) if (verbose) message(...)
  say("Listing prod and staging ...")
  prod <- .bucket_listing(bucket, prod_prefix)
  stg  <- .bucket_listing(bucket, staging_prefix)

  # top-level files vs the cartographic-boundaries subfolder
  is_sub <- function(rel) grepl("/", rel)
  prod_top <- prod[!is_sub(prod$rel), ]
  stg_top  <- stg[!is_sub(stg$rel), ]
  cart_prefix <- "cartographic-boundaries/"

  # coverage crosswalk (from prod, resolves served state for the coverage check)
  lookup <- NULL
  cw_key <- prod_top$key[prod_top$rel == crosswalk_rel]
  if (length(cw_key) == 1) {
    say("Loading coverage crosswalk ", crosswalk_rel, " ...")
    lookup <- tryCatch(
      build_coverage_lookup(.read_csv_s3(bucket, cw_key)),
      error = function(e) {
        say("  crosswalk load failed: ", conditionMessage(e))
        NULL
      })
  }

  rows <- list()
  add <- function(x) rows[[length(rows) + 1]] <<- x
  na_rows <- function(dataset, detail, checks) {
    data.frame(dataset = dataset, check = checks, status = "na",
               detail = detail, stringsAsFactors = FALSE)
  }
  diff_checks <- c("staging_not_empty", "missing_columns", "new_columns",
                   "schema_types", "row_count_change", "coverage_change")

  files <- sort(union(prod_top$rel, stg_top$rel))
  for (f in files) {
    dataset <- tools::file_path_sans_ext(f)
    in_prod <- f %in% prod_top$rel
    in_stg  <- f %in% stg_top$rel
    p_ts <- if (in_prod) prod_top$last_modified[prod_top$rel == f] else NA
    s_ts <- if (in_stg)  stg_top$last_modified[stg_top$rel == f]  else NA

    if (in_prod && !in_stg) {
      add(data.frame(dataset = dataset, check = "file_present", status = "fail",
                     detail = "missing from staging", stringsAsFactors = FALSE))
      add(na_rows(dataset, "file missing from staging", c(diff_checks, "last_pulled")))
      next
    }
    if (!in_prod && in_stg) {
      add(data.frame(dataset = dataset, check = "file_present", status = "warn",
                     detail = "new file in staging", stringsAsFactors = FALSE))
      add(na_rows(dataset, "no prod baseline", c(diff_checks, "last_pulled")))
      next
    }

    add(data.frame(dataset = dataset, check = "file_present", status = "pass",
                   detail = "present in both", stringsAsFactors = FALSE))
    add(.last_pulled_row(dataset, s_ts, p_ts))

    if (grepl("\\.geojson$", f, ignore.case = TRUE)) {
      # size-based proxy; never load the 1.1 GB geometry file
      p_sz <- prod_top$size[prod_top$rel == f]
      s_sz <- stg_top$size[stg_top$rel == f]
      pct <- if (p_sz > 0) (s_sz - p_sz) / p_sz else NA
      st <- if (is.na(pct)) "na" else if (abs(pct) >= thresholds$row_stop) "fail"
            else if (abs(pct) >= thresholds$row_warn) "warn" else "pass"
      pct_txt <- if (is.na(pct)) "n/a" else sprintf("%+.1f%%", 100 * pct)
      add(data.frame(dataset = dataset, check = "staging_not_empty",
                     status = if (s_sz > 0) "pass" else "fail",
                     detail = sprintf("%.1f MB", s_sz / 1e6), stringsAsFactors = FALSE))
      add(data.frame(dataset = dataset, check = "row_count_change", status = st,
                     detail = sprintf("size-based: prod=%.1fMB staging=%.1fMB (%s)",
                                      p_sz / 1e6, s_sz / 1e6, pct_txt),
                     stringsAsFactors = FALSE))
      add(na_rows(dataset, "geojson, compared by size only",
                  c("missing_columns", "new_columns", "schema_types", "coverage_change")))
      next
    }

    # tabular: load both and diff
    loaded <- tryCatch({
      p <- .read_csv_s3(bucket, prod_top$key[prod_top$rel == f])
      s <- .read_csv_s3(bucket, stg_top$key[stg_top$rel == f])
      list(p = p, s = s)
    }, error = function(e) {
      say("  load failed for ", f, ": ", conditionMessage(e))
      NULL
    })
    if (is.null(loaded)) {
      add(na_rows(dataset, "could not load", diff_checks))
      next
    }
    say("  diffing ", f, " ...")
    add(diff_dataset(loaded$s, loaded$p, dataset, lookup = lookup,
                     thresholds = thresholds))
  }

  # cartographic-boundaries folder: presence + last-pulled only
  cart_prod <- prod[startsWith(prod$rel, cart_prefix), ]
  cart_stg  <- stg[startsWith(stg$rel, cart_prefix), ]
  if (nrow(cart_prod) > 0 || nrow(cart_stg) > 0) {
    present <- nrow(cart_prod) > 0 && nrow(cart_stg) > 0
    add(data.frame(dataset = "cartographic-boundaries", check = "file_present",
                   status = if (present) "pass" else "fail",
                   detail = sprintf("%d staging / %d prod zip files",
                                    nrow(cart_stg), nrow(cart_prod)),
                   stringsAsFactors = FALSE))
    add(.last_pulled_row("cartographic-boundaries",
                         .latest(cart_stg$last_modified),
                         .latest(cart_prod$last_modified)))
    add(na_rows("cartographic-boundaries", "zip folder, presence only", diff_checks))
  }

  if (length(rows) == 0) {
    say("Nothing to check: no files found under either prefix.")
    return(invisible(list(results = data.frame(
      dataset = character(0), check = character(0),
      status = character(0), detail = character(0), stringsAsFactors = FALSE),
      artifacts = NULL)))
  }
  results <- do.call(rbind, rows)
  n_fail <- sum(results$status == "fail", na.rm = TRUE)
  n_warn <- sum(results$status == "warn", na.rm = TRUE)

  # A bare dry run (no upload, no --out) is a fast go/no-go: print the summary and
  # keep the rendered report in memory, but do not write a throwaway temp file
  # that the process deletes on exit. Ask for the file with --out, or upload it.
  if (!upload && is.null(local_dir)) {
    art <- list(html_content = render_report_html(results, run_ts),
                html_local = NULL, csv_local = NULL,
                html_obj = NULL, csv_obj = NULL, uploaded = FALSE)
    dest <- "summary only (add --out=<dir> to save the report)"
  } else {
    say("Writing report ...")
    art <- write_report_artifacts(results, checks_base, run_ts, upload = upload,
                                  local_dir = local_dir)
    dest <- if (upload) art$html_obj else art$html_local
  }
  say(sprintf("Done: %d datasets, %d failed, %d warnings. Report: %s",
              length(unique(results$dataset)), n_fail, n_warn, dest))
  invisible(list(results = results, artifacts = art))
}

# CLI main: `Rscript scripts/staged_prod_check.R [--dry-run] [--out=<dir>]`
if (sys.nframe() == 0 && !interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  dry <- "--dry-run" %in% args
  out_arg <- grep("^--out=", args, value = TRUE)
  local_dir <- if (length(out_arg)) sub("^--out=", "", out_arg[1]) else NULL
  run_staged_prod_check(upload = !dry, local_dir = local_dir)
}
