library(tidyverse)
library(googlesheets4)
library(aws.s3)

update_registries <- function(config, dataset_id) {
  print("Updating dataset registry...")
  update_dataset_registry(config, dataset_id)
  
  print("Updating variable registry...")
  update_variable_registry(config, dataset_id)
}

stage_data <- function(config) {
  print("Staging data...")
}

# Performs an upsert for the recently updated dataset.
# If the dataset exists, update with any new data.
# If the dataset doesn't exist, insert a new row.
update_dataset_registry <- function(config, dataset_id) {
  print("Grabbing dataset sub-config...")
  sub_config <- config[[dataset_id]]
  if (is.null(sub_config)) {
    stop(paste("Error: dataset_id", dataset_id, "not found in config."))
  }

  dataset_registry_link <- config$metadata$dataset_registry_link
  registry <- tryCatch({
    print(sprintf("Pulling dataset registry from S3: %s", dataset_registry_link))
    aws.s3::s3read_using(read.csv, object = dataset_registry_link) %>% 
      mutate(across(everything(), as.character))
  }, error = function(e) {
    print("Creating blank dataset registry because existing one not found...")
    tibble(dataset = character()) 
  })

  # Search JSON sub-config for specific column data
  cols_to_track <- c(
    "clean_name", "extent", "date_range", "methods", 
    "description", "source", "source_url", "update_freq", "date_downloaded", 
    "date_cleaned", "date_merged", "date_staged", "raw_link", "clean_link", 
    "merged_link", "staged_link", "quality_check_link", "quality_score"
  )
  new_row_data <- list(dataset = dataset_id)
  for (col in cols_to_track) {
    val <- sub_config[[col]]
    new_row_data[[col]] <- if (!is.null(val) && !is.list(val)) as.character(val) else ""
  }
  new_row_df <- as.data.frame(new_row_data, stringsAsFactors = FALSE)

  print("Merging new row data with manually updated columns...")
  dont_touch_these_columns <- setdiff(names(registry), names(new_row_df))
  preserved_cols <- registry %>% select(dataset, all_of(dont_touch_these_columns))
  updated_row <- merge(preserved_cols, new_row_df, by = "dataset", all.y = TRUE) %>%
    mutate(across(everything(), ~ as.character(.)))
  
  # Bind back to the registry, replacing the old entry
  final_registry <- registry %>%
    filter(dataset != dataset_id) %>% 
    bind_rows(., updated_row) %>%
    arrange(dataset)
  
  # Save to S3
  tmp <- tempfile()
  write.csv(final_registry, file = paste0(tmp, ".csv"), row.names = FALSE)
  on.exit(unlink(paste0(tmp, ".csv")), add = TRUE)
  aws.s3::put_object(file = paste0(tmp, ".csv"), object = dataset_registry_link, acl = "public-read")
  
  print(paste("Dataset registry row updated for: ", dataset_id))
}

# Note: never overwrites any manually filled data for variables that already exist
update_variable_registry <- function(config, dataset_id) {
  print("Grabbing dataset sub-config...")
  sub_config <- config[[dataset_id]]
  if (is.null(sub_config)) {
    stop(paste("Error: dataset_id", dataset_id, "not found in config."))
  }
  dataset_link <- sub_config$clean_link
  if (dataset_link == "") {
    print("Dataset doesn't have a clean link. Skip variable registry update.")
    return()
  }

  variable_registry_link <- config$metadata$variable_registry_link
  registry <- tryCatch({
    print(sprintf("Pulling variable registry from S3: %s", variable_registry_link))
    aws.s3::s3read_using(read.csv, object = variable_registry_link) %>% 
      mutate(across(everything(), as.character))
  }, error = function(e) {
    print("Creating blank variable registry because existing one not found...")
    tibble(dataset = character()) 
  })
  
  # TODO
}