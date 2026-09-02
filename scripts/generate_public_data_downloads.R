###############################################################################
# Public Data Downloads - Methodology Docs + Bundled National/State Data
#
# This script produces both an internal and a public methodology doc using
# metadata stored in the registries. The internal methodology doc covers every
# dataset in main_config.json (raw, clean, and merged). The public methodology
# doc covers a subset of these and is added to the data download for users of
# the drinking water tool.
#
# Once the docs are built, this script bundles every staged dataset
# into a national zip plus one zip per state.
#
# Example commands to run from the repo root:
#   Rscript scripts/generate_public_data_downloads.R
#   Rscript scripts/generate_public_data_downloads.R --doc-only   # skips bundling
#   Rscript scripts/generate_public_data_downloads.R --dev        # route reads/writes to the dev bucket
#
# Packages needed to run this script:
# installing pandoc on macOS - $ brew install pandoc
# installing tinytex from R - $ tinytex::install_tinytex()
###############################################################################

library(tidyverse)
library(sf)

if (!exists("s3_read_csv", mode = "function")) source("functions/s3_client.R")

INTERNAL_TEMPLATE_PATH <- "scripts/methods_doc_template.Rmd"
PUBLIC_TEMPLATE_PATH <- "scripts/methods_doc_public_template.Rmd"
DATASETS_PLACEHOLDER <- "<!-- AUTO-GENERATED-DATASETS -->"
LINKS_PLACEHOLDER <- "<!-- AUTO-GENERATED-LINKS -->"
METADATA_PLACEHOLDER_PATTERN <- "^<!--\\s*CONFIG METADATA:\\s*(\\S+)\\s*-->$"
REPO_RMD_PATH <- tempfile(fileext = ".Rmd")
PUBLIC_REPO_RMD_PATH <- tempfile(fileext = ".Rmd")
PUBLIC_REPO_PDF_PATH <- tempfile(fileext = ".pdf")
PUBLIC_REPO_LINKS_TEX_PATH <- tempfile(fileext = ".tex")

###############################################################################
## Helper functions
###############################################################################

# Swap a placeholder with generated content.
.replace_placeholder <- function(template_lines, placeholder, replacement_lines) {
  placeholder_i <- which(trimws(template_lines) == placeholder)
  if (length(placeholder_i) != 1) {
    stop(sprintf("Expected exactly one %s placeholder, found %d.", placeholder, length(placeholder_i)), call. = FALSE)
  }
  c(
    template_lines[seq_len(placeholder_i - 1)],
    replacement_lines,
    template_lines[(placeholder_i + 1):length(template_lines)]
  )
}

# Create a metadata + variable table section for a single dataset.
.create_internal_doc_section <- function(dataset_id, dataset_registry, variable_registry) {
  row <- dataset_registry %>% filter(dataset == dataset_id)
  if (nrow(row) == 0) return(character(0))

  get_val <- function(col) {
    if (!(col %in% names(row))) return("N/A")
    val <- row[[col]][1]
    if (is.na(val) || val == "") "N/A" else val
  }

  meta_line <- function(label, value) sprintf("**%s:** %s  ", label, value)

  lines <- c(
    sprintf("### %s", row$clean_name[1] %||% dataset_id),
    "",
    meta_line("Dataset ID(s)", dataset_id),
    meta_line("Source", get_val("source")),
    meta_line("Source URL", get_val("source_url")),
    meta_line("Available for", get_val("coverage")),
    meta_line("Update frequency", get_val("update_freq")),
    meta_line("Spatial level", get_val("spatial_level")),
    meta_line("Date range", get_val("date_range")),
    meta_line("Last updated", get_val("date_updated")),
    "",
    "**Description:** _TODO: describe this dataset_",
    "",
    "**Caveats:** _TODO: list known caveats_",
    "",
    "**Recommended uses:** _TODO: list recommended uses_",
    "",
    "**Methods:** _TODO: describe how this dataset is built_",
    ""
  )

  var_rows <- variable_registry %>% filter(dataset == dataset_id)
  if (nrow(var_rows) == 0) {
    return(c(lines, "_No variables recorded in variable_registry.csv for this dataset._", ""))
  }

  var_table <- var_rows %>%
    arrange(variable) %>%
    transmute(
      Variable = variable,
      Description = ifelse(is.na(description) | description == "", "_(no description yet)_", description),
      Type = type,
      `Completeness %` = ifelse(is.na(data_score_completeness), "N/A", sprintf("%.1f", as.numeric(data_score_completeness))),
      `Quality Score` = ifelse(is.na(auto_data_score), "N/A", sprintf("%.1f", as.numeric(auto_data_score)))
    )

  table_lines <- knitr::kable(var_table, format = "pipe")

  c(lines, table_lines, "")
}

# Zip a folder's contents together
.zip_folder <- function(folder_path, zip_path) {
  old_wd <- getwd()
  on.exit(setwd(old_wd), add = TRUE)
  setwd(dirname(folder_path))
  utils::zip(zip_path, basename(folder_path))
}

###############################################################################
## Start of script
###############################################################################

args <- commandArgs(trailingOnly = TRUE)
doc_only <- "--doc-only" %in% args
dev_mode <- "--dev" %in% args

set_s3_bucket("tech-team-data")
message("Grabbing main config...")
config_obj <- s3_client()$get_object(
  Bucket = s3_bucket(),
  Key = "national-dw-tool/pipeline-config/main_config.json"
)
config_raw <- rawToChar(config_obj$Body)
if (!jsonlite::validate(config_raw)) {
  stop("main_config.json is not valid JSON, check for syntax errors.", call. = FALSE)
}
config <- jsonlite::fromJSON(config_raw)
if (dev_mode) {
  message("IN DEVELOPMENT MODE: Remapping config to route to dev bucket...")
  config <- remap_to_dev(config)
}

version_date <- format(Sys.Date(), "%Y-%m-%d")

message("Pulling dataset_registry.csv and variable_registry.csv...")
dataset_registry <- s3_read_csv(config$metadata$dataset_registry_link, coerce_character = FALSE)
variable_registry <- s3_read_csv(config$metadata$variable_registry_link, coerce_character = FALSE)

message("Building doc links from config metadata...")
metadata <- config$metadata
doc_links <- list(
  list("GitHub repository", metadata$github_link, TRUE),
  list("Data dictionary", metadata$data_dictionary_link, TRUE),
  list("Last application data update", metadata$app_last_updated, FALSE)
)

links_lines <- unlist(lapply(doc_links, function(x) {
  label <- x[[1]]; value <- x[[2]]; is_link <- x[[3]]
  value_md <- if (is_link) sprintf("[%s](%s)", value, value) else value
  c(sprintf("**%s:** %s", label, value_md), "")
}))

# LaTeX version of the links to put above the public PDF's TOC
links_tex_lines <- c(
  unlist(lapply(doc_links, function(x) {
    label <- x[[1]]; value <- x[[2]]; is_link <- x[[3]]
    value_tex <- if (is_link) sprintf("\\href{%s}{%s}", value, value) else value
    sprintf("\\textbf{%s:} %s\\newline", label, value_tex)
  })),
  "\\vspace{1em}"
)
dir.create(dirname(PUBLIC_REPO_LINKS_TEX_PATH), recursive = TRUE, showWarnings = FALSE)
writeLines(links_tex_lines, PUBLIC_REPO_LINKS_TEX_PATH)

###############################################################################
## Create the internal methodology doc with one section for each dataset_id
## defined in main_config.json.
###############################################################################

dataset_ids <- setdiff(names(config), "metadata")
message(sprintf("Building reference sections for %d dataset_ids...", length(dataset_ids)))
internal_doc_sections <- lapply(dataset_ids, function(id) {
  .create_internal_doc_section(id, dataset_registry, variable_registry)
})
generated_lines <- c("## Datasets", "", unlist(internal_doc_sections))

message("Splicing links + generated dataset content into the internal template...")
if (!file.exists(INTERNAL_TEMPLATE_PATH)) {
  stop(sprintf("Template not found at %s.", INTERNAL_TEMPLATE_PATH), call. = FALSE)
}
template_lines <- readLines(INTERNAL_TEMPLATE_PATH, warn = FALSE)
template_lines <- .replace_placeholder(template_lines, LINKS_PLACEHOLDER, links_lines)
template_lines <- .replace_placeholder(template_lines, DATASETS_PLACEHOLDER, generated_lines)
writeLines(template_lines, REPO_RMD_PATH)

###############################################################################
## Build the public data dictionary CSV: variable_registry.csv joined with
## dataset_registry.csv fields (source, source_url, update_freq,
## date last downloaded).
###############################################################################

message("Building data dictionary from variable_registry.csv + dataset_registry.csv...")
dataset_fields <- dataset_registry %>%
  select(dataset, source, source_url, update_freq, date_downloaded = date_updated)

data_dictionary <- variable_registry %>%
  left_join(dataset_fields, by = "dataset") %>%
  transmute(
    data_download_name,
    raw_variable_name = variable,
    clean_name,
    tool_table_name,
    filter_name,
    type,
    description,
    dataset_methods_name = dataset,
    source,
    source_url,
    update_freq,
    date_downloaded,
    data_score_completeness,
    data_score_duplicates,
    data_score_coverage,
    auto_data_score
  )

data_dictionary_key <- "national-dw-tool/public-data-downloads/data-dictionary.csv"
message(sprintf("Uploading data dictionary to S3 at %s...", data_dictionary_key))
s3_write_csv(data_dictionary, data_dictionary_key, acl = "public-read")

###############################################################################
## Create the public methodology doc by autogenerating a metadata block to fill
## in the CONFIG METADATA placeholder for each dataset using the information
## in main_config.json and dataset_registry.csv.
###############################################################################

message("Replacing CONFIG METADATA block in the public template...")
if (!file.exists(PUBLIC_TEMPLATE_PATH)) {
  stop(sprintf("Public template not found at %s.", PUBLIC_TEMPLATE_PATH), call. = FALSE)
}
public_template_lines <- readLines(PUBLIC_TEMPLATE_PATH, warn = FALSE)

out <- character(0)
i <- 1
n <- length(public_template_lines)
while (i <= n) {
  line <- public_template_lines[i]
  m <- regmatches(trimws(line), regexec(METADATA_PLACEHOLDER_PATTERN, trimws(line)))[[1]]
  if (length(m) == 2) {
    dataset_id <- m[2]
    if (!(dataset_id %in% names(config))) {
      out <- c(out, line, "", "No metadata exists for this dataset yet.", "")
      i <- i + 1
      while (i <= n && trimws(public_template_lines[i]) != "") i <- i + 1
      if (i <= n) i <- i + 1
      next
    }

    sub_config <- config[[dataset_id]]
    registry_row <- dataset_registry %>% filter(dataset == dataset_id)
    get_registry_val <- function(col) {
      if (nrow(registry_row) == 0 || !(col %in% names(registry_row))) return("N/A")
      val <- registry_row[[col]][1]
      if (is.null(val) || is.na(val) || val == "") "N/A" else val
    }
    meta_line <- function(label, value) sprintf("**%s:** %s  ", label, value)
    metadata_block <- c(
      meta_line("Maintained by", sub_config$source),
      meta_line("Website link", sub_config$source_url),
      meta_line("Update frequency", sub_config$update_freq),
      meta_line("Spatial level", sub_config$spatial_level),
      meta_line("Available for", get_registry_val("coverage")),
      meta_line("Date range", sub_config$date_range),
      meta_line("Last updated", get_registry_val("date_updated")),
      ""
    )

    out <- c(out, line, "", metadata_block)
    i <- i + 1
    while (i <= n && trimws(public_template_lines[i]) != "") i <- i + 1
    if (i <= n) i <- i + 1
    next
  }
  out <- c(out, line)
  i <- i + 1
}
public_template_lines <- out
writeLines(public_template_lines, PUBLIC_REPO_RMD_PATH)

message("Knitting public methodology doc into a PDF...")
methods_doc_public_pdf <- tryCatch({
  if (!rmarkdown::pandoc_available()) {
    stop(paste(
      "pandoc not found. Install and re-run."
    ), call. = FALSE)
  }
  has_latex <- nzchar(Sys.which("pdflatex")) || nzchar(Sys.which("xelatex")) ||
    (requireNamespace("tinytex", quietly = TRUE) && tinytex::is_tinytex())
  if (!has_latex) {
    stop(paste(
      "LaTeX engine not found. Install TinyTeX from R with `tinytex::install_tinytex()` and re-run."
    ), call. = FALSE)
  }
  abs_input <- normalizePath(PUBLIC_REPO_RMD_PATH)
  abs_output <- file.path(normalizePath(dirname(PUBLIC_REPO_PDF_PATH)), basename(PUBLIC_REPO_PDF_PATH))
  abs_links_tex <- normalizePath(PUBLIC_REPO_LINKS_TEX_PATH)
  rmarkdown::render(abs_input, output_file = abs_output, output_format = "pdf_document",
                    output_options = list(includes = rmarkdown::includes(before_body = abs_links_tex)),
                    quiet = TRUE, envir = new.env())
  PUBLIC_REPO_PDF_PATH
}, error = function(e) {
  message(sprintf("Skipping public PDF: %s", conditionMessage(e)))
  NULL
})

###############################################################################
## Upload both docs to S3
###############################################################################

doc_key <- sprintf("national-dw-tool/public-data-downloads/methodology-internal-%s.Rmd", version_date)
message(sprintf("Uploading internal methods doc to S3 at %s...", doc_key))
s3_write_file(REPO_RMD_PATH, doc_key, acl = "public-read")
if (!is.null(methods_doc_public_pdf)) {
  pdf_key <- sprintf("national-dw-tool/public-data-downloads/methodology-%s.pdf", version_date)
  message(sprintf("Uploading public methods doc to S3 at %s...", pdf_key))
  s3_write_file(methods_doc_public_pdf, pdf_key, acl = "public-read")
}

if (doc_only) {
  message("--doc-only set: skipping bundling.")
  quit(save = "no", status = 0)
}

###############################################################################
## Create and upload the national dataset bundle with every staged dataset.
###############################################################################

message("Building national bundle...")
excluded_merge_ids <- c(
  "clean_epa_sabs",
  "merged_national_bwn_summary",
  "merged_pwsid_npdes_usts_rmps_imp",
  "merged_pwsid_funded_highlevel_summary"
)

staged_datasets <- Filter(function(id) {
  link <- config[[id]]$staged_link
  !is.null(link) && !is.na(link) && !(link %in% c("N/A", ""))
}, dataset_ids)

message("Reading staged EPA SABs geometry...")
epa_sabs <- s3_read_geojson(config[["clean_epa_sabs"]]$staged_link)

message("Reading staged environmental dataset...")
pwsid_enviro <- s3_read_csv(config[["merged_pwsid_npdes_usts_rmps_imp"]]$staged_link, coerce_character = FALSE)

merge_ids <- setdiff(staged_datasets, excluded_merge_ids)
pwsid_water_socio <- epa_sabs %>% as.data.frame() %>% select(pwsid)
for (id in merge_ids) {
  message(sprintf("Merging staged dataset %s into pwsid-water-socio-df...", id))
  df <- tryCatch(
    s3_read_csv(config[[id]]$staged_link, coerce_character = FALSE),
    error = function(e) {
      message(sprintf("ERROR %s: could not read staged file at %s (%s). Continuing merge without this dataset.",
                      id, config[[id]]$staged_link, conditionMessage(e)))
      NULL
    }
  )
  if (is.null(df)) next
  pwsid_water_socio <- merge(pwsid_water_socio, df, by = "pwsid", all.x = TRUE)
}

message("Writing national bundle...")
national_folder <- file.path(tempdir(), sprintf("national-dw-tool-%s", version_date))
dir.create(national_folder, recursive = TRUE)

st_write(epa_sabs, file.path(national_folder, "epa_sabs.geojson"), quiet = TRUE)
write.csv(pwsid_water_socio, file.path(national_folder, "pwsid-water-socio-df.csv"), row.names = FALSE)
write.csv(pwsid_enviro, file.path(national_folder, "pwsid-enviro-df.csv"), row.names = FALSE)
if (!is.null(methods_doc_public_pdf)) {
  file.copy(methods_doc_public_pdf, file.path(national_folder, "methodology.pdf"))
}

national_zip <- file.path(tempdir(), sprintf("national-dw-tool-%s.zip", version_date))
.zip_folder(national_folder, national_zip)

national_key <- sprintf("national-dw-tool/public-data-downloads/national-dw-tool-%s.zip", version_date)
message(sprintf("Uploading national bundle to S3 at %s...", national_key))
s3_write_file(national_zip, national_key, acl = "public-read")

###############################################################################
## Bundle and upload state zipped files
###############################################################################

message("Building per-state bundles...")
message("Determining each SAB's state via centroid intersection...")
state_boundaries <- tigris::states() %>% st_transform(crs = st_crs(epa_sabs))
epa_sabs_centroid <- epa_sabs %>% st_centroid() %>% select(pwsid)

sf_use_s2(FALSE)
epa_sabs_states <- st_intersection(epa_sabs_centroid, state_boundaries) %>% unique()
sf_use_s2(TRUE)

states_to_loop <- unique(epa_sabs_states$STUSPS)
state_zips <- list()

for (state_i in states_to_loop) {
  message(sprintf("Building bundle for %s...", state_i))
  state_pwsids <- epa_sabs_states %>% filter(STUSPS == state_i) %>% select(pwsid) %>% as.data.frame()

  epa_sabs_i <- epa_sabs %>% filter(pwsid %in% state_pwsids$pwsid)
  pwsid_enviro_i <- pwsid_enviro %>% filter(pwsid %in% state_pwsids$pwsid)
  pwsid_water_socio_i <- pwsid_water_socio %>% filter(pwsid %in% state_pwsids$pwsid)

  state_folder <- file.path(tempdir(), "states", state_i)
  dir.create(state_folder, recursive = TRUE)

  st_write(epa_sabs_i, file.path(state_folder, "epa_sabs.geojson"), quiet = TRUE)
  write.csv(pwsid_water_socio_i, file.path(state_folder, "pwsid-water-socio-df.csv"), row.names = FALSE)
  write.csv(pwsid_enviro_i, file.path(state_folder, "pwsid-enviro-df.csv"), row.names = FALSE)
  if (!is.null(methods_doc_public_pdf)) {
    file.copy(methods_doc_public_pdf, file.path(state_folder, "methodology.pdf"))
  }

  state_zip <- file.path(tempdir(), sprintf("%s.zip", state_i))
  .zip_folder(state_folder, state_zip)
  state_zips[[state_i]] <- state_zip

  state_key <- sprintf("national-dw-tool/public-data-downloads/%s/states/%s.zip", version_date, state_i)
  message(sprintf("Uploading %s bundle to S3 at %s...", state_i, state_key))
  s3_write_file(state_zip, state_key, acl = "public-read")
}

message(sprintf(
  "Script Completed: methodology docs + national bundle + %d state bundles built for %s.",
  length(state_zips), version_date
))
