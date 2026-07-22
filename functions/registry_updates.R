#' Update both the dataset and variable registries for a dataset
#' @param config Main config
#' @param dataset_id Unique dataset id
update_registries <- function(config, dataset_id) {
  message("Updating dataset registry...")
  update_dataset_registry(config, dataset_id, date = Sys.Date())

  message("Updating variable registry...")
  update_variable_registry(config, dataset_id)
}

#' Merge cleaned datasets for the staging tool
#' @param config Main config
stage_data <- function(config) {
  message("Staging data...")
  # TODO
}

#' Updates the dataset registry for a single dataset. If the dataset exists,
#' updates it with any new data. If not, inserts a new row.
#' Manually-edited columns (any column not in tracked_cols) are preserved.
#' @param config Main config
#' @param dataset_id Unique dataset id
#' @param date Date to record in date_updated or NULL to leave it unchanged
#' @param fail_message If provided, overwrites the link column with a
#'   "PIPELINE FAILED" message instead of the dataset's normal link
update_dataset_registry <- function(config, dataset_id, date = NULL, fail_message = NULL) {
  message("Grabbing dataset sub-config...")
  sub_config <- config[[dataset_id]]
  if (is.null(sub_config)) {
    stop(paste("ERROR: dataset_id", dataset_id, "not found in config."))
  }

  registry <- tryCatch({
    dataset_registry_link <- config$metadata$dataset_registry_link
    message(sprintf("Pulling dataset registry from S3: %s", dataset_registry_link))
    obj <- s3_read_csv(dataset_registry_link)
  }, error = function(e) {
    print(e)
    message("Creating blank dataset registry because existing one not found...")
    tibble(dataset = character())
  })
  
  # Search JSON sub-config for specific column data
  tracked_cols <- c(
    "clean_name",
    "update_freq",
    "category",
    "link",
    "staged_link",
    "source",
    "source_url",
    "spatial_level",
    "date_range",
    "quality_check_link",
    "quality_score"
  )
  
  new_row_data <- list(dataset = dataset_id)
  for (col in tracked_cols) {
    val <- sub_config[[col]]
    new_row_data[[col]] <- if (!is.null(val) && !is.list(val)) as.character(val) else ""
  }
  # Flatten input_links list into a single string
  input_links <- sub_config[["input_links"]]
  new_row_data[["input_links"]] <- if (
    !is.null(input_links) &&
    length(input_links) > 0
  ) {
    paste(unlist(input_links), collapse = " | ")
  } else {
    ""
  }
  # Update date_updated only if a date is passed in
  if (!is.null(date)) {
    new_row_data[["date_updated"]] <- as.character(date)
  }
  # Overwrite link column if pipeline failed
  if (!is.null(fail_message)) {
    new_row_data[["link"]] <- paste0(
      "PIPELINE FAILED ON ",
      as.character(date %||% Sys.Date()),
      ": ",
      fail_message
    )
  }
  new_row_df <- as.data.frame(new_row_data, stringsAsFactors = FALSE)
  
  message("Merging new row data with manually updated columns...")
  dont_touch_these_columns <- setdiff(names(registry), names(new_row_df))
  preserved_cols <- registry %>% select(dataset, all_of(dont_touch_these_columns))
  updated_row <- merge(preserved_cols, new_row_df, by = "dataset", all.y = TRUE) %>%
    mutate(across(everything(), ~ as.character(.)))
  
  message("Add new row data to registry, replacing old row...")
  final_registry <- registry %>%
    filter(dataset != dataset_id) %>% 
    bind_rows(., updated_row) %>%
    arrange(dataset)

  message("Writing updated dataset registry to S3...")
  s3_write_csv(final_registry, dataset_registry_link, acl = "public-read")
}

#' Parse the cleaned dataset's variable names and types and upsert them into
#' the variable registry. Variables are only captured from datasets with a
#' "link" set in main config. Any manually-edited columns for existing variables
#' are kept; new variables are initialized with those columns blank.
#' @param config Main config
#' @param dataset_id Unique dataset id
update_variable_registry <- function(config, dataset_id) {
  # Columns in the variable registry that are manually updated.
  variable_registry_manual_cols <- c(
    "description",
    "variable_qual_check",
    "use_in_tool",
    "round_digits",
    "tool_table_name",
    "filter_name",
    "filter_category",
    "subheader",
    "filter_subheader_when_selected",
    "data_download_name"
  )

  message("Grabbing dataset sub-config...")
  sub_config <- config[[dataset_id]]
  if (is.null(sub_config)) {
    stop(paste("ERROR: dataset_id", dataset_id, "not found in config."))
  }

  dataset_link <- sub_config$link
  if (dataset_link == "") {
    message("Dataset doesn't have a clean link. Skip variable registry update.")
    return()
  }

  variable_registry_link <- config$metadata$variable_registry_link
  registry <- tryCatch({
    message(sprintf("Pulling variable registry from S3: %s", variable_registry_link))
    s3_read_csv(variable_registry_link)
  }, error = function(e) {
    message("Creating blank variable registry because existing one not found...")
    blank <- tibble(dataset = character(), variable = character(), type = character())
    blank[variable_registry_manual_cols] <- character()
    blank
  })

  message("Reading clean dataset to grab variable names and types...")
  ext <- tolower(tools::file_ext(dataset_link))
  # Read file based on file extension type
  clean_df <- switch(ext,
    "geojson" = sf::st_drop_geometry(s3_read_geojson(dataset_link)),
    "gpkg"    = sf::st_drop_geometry(s3_read_gpkg(dataset_link)),
    s3_read_csv(dataset_link, coerce_character = FALSE)
  )

  new_rows_df <- data.frame(
    dataset = dataset_id,
    variable = names(clean_df),
    type = vapply(clean_df, function(col) class(col)[1], character(1)),
    stringsAsFactors = FALSE
  )

  message("Merging new variable data with manually updated columns...")
  dont_touch_these_columns <- union(
    setdiff(names(registry), names(new_rows_df)),
    variable_registry_manual_cols
  )
  preserved_cols <- registry %>%
    filter(dataset == dataset_id) %>%
    select(dataset, variable, any_of(dont_touch_these_columns))
  # Initialize any manual columns the registry doesn't have yet
  missing_manual_cols <- setdiff(dont_touch_these_columns, names(preserved_cols))
  preserved_cols[missing_manual_cols] <- NA_character_

  updated_rows <- merge(preserved_cols, new_rows_df, by = c("dataset", "variable"), all.y = TRUE) %>%
    mutate(across(everything(), ~ as.character(.))) %>%
    mutate(across(all_of(dont_touch_these_columns), ~ ifelse(is.na(.), "", .)))

  message("Add new rows to registry, replacing old rows...")
  final_registry <- registry %>%
    filter(dataset != dataset_id) %>%
    bind_rows(., updated_rows) %>%
    mutate(across(all_of(variable_registry_manual_cols), ~ ifelse(is.na(.), "", .))) %>%
    arrange(dataset, variable)

  message("Writing updated variable registry to S3...")
  s3_write_csv(final_registry, variable_registry_link, acl = "public-read")
}