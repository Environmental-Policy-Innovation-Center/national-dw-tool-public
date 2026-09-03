#' Update the variable registry, then stage the dataset if it has a
#' staged_link and passes its quality checks, then update the dataset
#' registry.
#'
#' stage_data() reads the quality scores from update_variable_registry()
#' and update_dataset_registry() writes staged metadata columns based on
#' the results of staged_data().
#' @param config Main config
#' @param dataset_id Unique dataset id
sync_dataset <- function(config, dataset_id) {
  message("Updating variable registry...")
  update_variable_registry(config, dataset_id)

  message("Staging dataset...")
  staging_result <- stage_data(config, dataset_id)

  message("Updating dataset registry...")
  update_dataset_registry(config, dataset_id, date = Sys.Date(), staging_result = staging_result)
}

#' Determine which states + territories a dataset actually covers.
#' Returns "N/A" if the dataset has no pwsid column or coverage can't be computed.
#' @param config Main config
#' @param dataset_id Unique dataset id
#' @return Character string (e.g. "CONUS, AK, PR" or "N/A")
compute_dataset_coverage <- function(config, dataset_id) {
  sub_config <- config[[dataset_id]]
  if (is.null(sub_config)) return("N/A")

  # Use the bwn_state_label for BWN datasets
  if (!is.null(sub_config$bwn_state_label) && sub_config$bwn_state_label != "") {
    return(sub_config$bwn_state_label)
  }

  dataset_link <- sub_config$link
  if (is.null(dataset_link) || dataset_link %in% c("", "N/A") ||
      grepl(" | ", dataset_link, fixed = TRUE)) {
    return("N/A")
  }

  ext <- tolower(tools::file_ext(dataset_link))
  clean_df <- tryCatch({
    switch(ext,
      "geojson" = sf::st_drop_geometry(s3_read_geojson(dataset_link)),
      "gpkg"    = sf::st_drop_geometry(s3_read_gpkg(dataset_link)),
      s3_read_csv(dataset_link, coerce_character = FALSE)
    )
  }, error = function(e) NULL)
  if (is.null(clean_df) || !("pwsid" %in% names(clean_df))) return("N/A")

  crosswalk_link <- config[["clean_sabs_county_served"]]$link
  crosswalk <- tryCatch(s3_read_csv(crosswalk_link, coerce_character = FALSE), error = function(e) NULL)
  if (is.null(crosswalk)) return("N/A")

  coverage <- tryCatch(
    get_spatial_coverage(clean_df, crosswalk = crosswalk, collapse_conus = TRUE),
    error = function(e) NULL
  )
  if (is.null(coverage) || coverage == "") return("N/A")
  coverage
}

#' Calculate each variable's completeness and duplicate rate and flag variables
#' below a 50% average for review.
#' @param clean_df The cleaned dataset
#' @param dataset_id Unique dataset id
#' @param dup_check_exempt Variables to skip the duplicate-rate check
#' @param data_score_coverage Vector with manually updated 0-100 coverage score
#' @return Data frame with one row per variable: dataset, variable,
#'   data_score_completeness, data_score_duplicates, data_score_coverage,
#'   auto_data_score, data_qual_flag
calculate_var_quality_scores <- function(clean_df, dataset_id, dup_check_exempt = character(0),
                                    data_score_coverage = numeric(0)) {
  if (inherits(clean_df, "sf")) {
    message("Drop geometry columns for spatial data frame...")
    clean_df <- sf::st_drop_geometry(clean_df)
  }

  n_rows <- nrow(clean_df)
  var_names <- names(clean_df)

  # Select denominator to divide by based on the dataset type.
  # Datasets with pwsid use unique pwsid count. Datasets without use row count.
  if ("pwsid" %in% var_names) {
    n_units <- length(unique(clean_df$pwsid))
  } else {
    message(sprintf(
      "calculate_variable_quality(%s): no pwsid column found - scoring against row count instead of unique water systems.",
      dataset_id
    ))
    n_units <- n_rows
  }

  message("Actually calculating variable quality scores...")
  scores <- lapply(var_names, function(var) {
    col <- clean_df[[var]]
    # NA and empty-string both count as missing
    n_missing <- sum(is.na(col) | (is.character(col) & col == ""))
    data_score_completeness <- 100 * (1 - n_missing / n_units)

    data_score_duplicates <- if (var %in% dup_check_exempt) {
      NA_real_
    } else {
      100 * (length(unique(col)) / n_units)
    }

    coverage_i <- unname(data_score_coverage[var])
    var_data_score_coverage <- if (length(coverage_i) == 0) NA_real_ else coverage_i

    data.frame(
      dataset = dataset_id,
      variable = var,
      data_score_completeness = data_score_completeness,
      data_score_duplicates = data_score_duplicates,
      data_score_coverage = var_data_score_coverage
    )
  })

  var_scores <- do.call(rbind, scores)
  var_scores$auto_data_score <- rowMeans(
    var_scores[, c("data_score_completeness", "data_score_duplicates", "data_score_coverage")],
    na.rm = TRUE
  )
  var_scores$data_qual_flag <- ifelse(
    var_scores$auto_data_score < 50, "NEEDS REVIEW", "PASSED CHECK"
  )

  var_scores
}

#' Calculate EPA SABs geometry quality: % of SABs without significant overlap
#' with another SAB, % with a non-duplicated pwsid, % with a non-empty
#' geometry.
#' @param sab_sf EPA SABs sf object
#' @return List with data_score_overlaps, data_score_duplicates,
#'   data_score_completeness, auto_data_score, data_qual_flag
calculate_sabs_geometry_quality <- function(sab_sf) {
  message("Identifying SAB overlaps...")
  sab_area <- sf::st_area(sab_sf)
  sab_intersect <- sf::st_intersects(sab_sf, sab_sf)
  multi_intersect_mask <- lengths(sab_intersect) > 1
  sab_filtered <- sab_sf[multi_intersect_mask, ]

  sf::sf_use_s2(FALSE)
  overlap_pwsids <- character(0)
  if (nrow(sab_filtered) > 0) {
    sab_intersection <- sf::st_intersection(sab_filtered, sab_filtered)
    sab_intersection$intersection_area <- sf::st_area(sab_intersection)
    sab_intersection$og_area <- sab_area[match(sab_intersection$pwsid, sab_sf$pwsid)]
    sab_intersection$percent_overlap <- as.numeric(
      sab_intersection$intersection_area / sab_intersection$og_area
    ) * 100
    overlap_pwsids <- unique(sab_intersection$pwsid[
      sab_intersection$pwsid != sab_intersection$pwsid.1 &
        sab_intersection$percent_overlap > 5
    ])
  }
  sf::sf_use_s2(TRUE)
  data_score_overlaps <- 100 * (1 - length(overlap_pwsids) / nrow(sab_sf))

  message("Identifying duplicated pwsids and empty geometries...")
  data_score_duplicates <- if (any(duplicated(sab_sf$pwsid))) {
    100 * (1 - sum(duplicated(sab_sf$pwsid)) / length(unique(sab_sf$pwsid)))
  } else {
    100
  }

  n_empty <- sum(sf::st_is_empty(sab_sf))
  data_score_completeness <- if (n_empty > 0) {
    100 * (1 - n_empty / length(unique(sab_sf$pwsid)))
  } else {
    100
  }

  auto_data_score <- mean(c(data_score_overlaps, data_score_duplicates, data_score_completeness))

  list(
    data_score_overlaps = data_score_overlaps,
    data_score_duplicates = data_score_duplicates,
    data_score_completeness = data_score_completeness,
    auto_data_score = auto_data_score,
    data_qual_flag = if (auto_data_score < 50) "NEEDS REVIEW" else "PASSED CHECK"
  )
}

#' Checks if a dataset needs to and is ready to be staged based on quality
#' scores in variable_registry.csv.
#' @param config Main config
#' @param dataset_id Unique dataset id
#' @return List with dataset, mean_data_qual_score, data_qual_score (pass
#'   count string), needs_review_flag, date_staged, staged_link - NA for the
#'   last two when the dataset fails review. NULL if the dataset has no
#'   staged_link.
stage_data <- function(config, dataset_id) {
  message(sprintf("Staging data for %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  if (is.null(sub_config)) {
    stop(paste("ERROR: dataset_id", dataset_id, "not found in config."))
  }
  dataset_link <- sub_config$link
  # hardcoded exception for raw_sdwa which has multiple raw s3 links, none of
  # which are staged
  if (is.null(dataset_link) || dataset_link %in% c("", "N/A") ||
      grepl(" | ", dataset_link, fixed = TRUE)) {
    message("Dataset has no single clean link. Skipping staging.")
    return(NULL)
  }

  staged_link <- sub_config$staged_link
  if (is.null(staged_link) || staged_link %in% c("", "N/A")) {
    message("Dataset doesn't have a staged link. Skipping staging.")
    return(NULL)
  }

  message("Reading variable registry...")
  variable_registry_link <- config$metadata$variable_registry_link
  registry <- s3_read_csv(variable_registry_link)
  var_rows <- registry %>% filter(dataset == dataset_id)
  if (nrow(var_rows) == 0) {
    stop(sprintf(
      "No variable_registry.csv rows found for %s. Run update_variable_registry() first.",
      dataset_id
    ))
  }

  use_in_tool_rows <- var_rows %>% filter(!is.na(use_in_tool), nchar(trimws(use_in_tool)) > 0)
  if (nrow(use_in_tool_rows) == 0) {
    message(sprintf(
      "%s NEEDS MANUAL REVIEW: no variables flagged use_in_tool yet. Skipping staging.",
      dataset_id
    ))
    return(list(
      dataset = dataset_id,
      mean_data_qual_score = NA_character_,
      data_qual_score = "0 / 0 variables passed checks",
      needs_review_flag = "NEEDS REVIEW",
      date_staged = NA_character_,
      staged_link = NA_character_
    ))
  }

  auto_scores <- suppressWarnings(as.numeric(use_in_tool_rows$auto_data_score))
  data_qual_flags <- use_in_tool_rows$data_qual_flag
  n_passed <- sum(data_qual_flags == "PASSED CHECK", na.rm = TRUE)
  data_qual_score <- sprintf("%d / %d variables passed checks", n_passed, nrow(use_in_tool_rows))
  all_vars_passed <- all(data_qual_flags == "PASSED CHECK", na.rm = TRUE)

  # EPA SABs geometry needs a dataset-level spatial score.
  if (dataset_id == "clean_epa_sabs") {
    message("Scoring SABs geometry quality...")
    sab_sf <- s3_read_geojson(dataset_link)
    sabs_quality <- calculate_sabs_geometry_quality(sab_sf)
    mean_data_qual_score <- mean(c(auto_scores, sabs_quality$auto_data_score), na.rm = TRUE)
    needs_review_flag <- if (all_vars_passed && sabs_quality$data_qual_flag == "PASSED CHECK") "PASSED" else "NEEDS REVIEW"
  } else {
    mean_data_qual_score <- if (length(auto_scores) == 0 || all(is.na(auto_scores))) {
      NA_real_
    } else {
      mean(auto_scores, na.rm = TRUE)
    }
    needs_review_flag <- if (all_vars_passed) "PASSED" else "NEEDS REVIEW"
  }

  result <- list(
    dataset = dataset_id,
    mean_data_qual_score = as.character(round(mean_data_qual_score, 2)),
    data_qual_score = data_qual_score,
    needs_review_flag = needs_review_flag,
    date_staged = NA_character_,
    staged_link = NA_character_
  )

  if (needs_review_flag != "PASSED") {
    message(sprintf("%s NEEDS MANUAL REVIEW (%s). Skipping staging.", dataset_id, data_qual_score))
    return(result)
  }

  message(sprintf("Checks passed. Staging %s to %s...", dataset_id, staged_link))
  ext <- tolower(tools::file_ext(dataset_link))
  clean_df <- switch(ext,
    "geojson" = s3_read_geojson(dataset_link),
    "gpkg"    = s3_read_gpkg(dataset_link),
    s3_read_csv(dataset_link, coerce_character = FALSE)
  )
  is_spatial <- inherits(clean_df, "sf")
  attrs_df <- if (is_spatial) sf::st_drop_geometry(clean_df) else clean_df

  round_digits <- var_rows %>%
    filter(!is.na(suppressWarnings(as.numeric(round_digits))), variable %in% names(attrs_df)) %>%
    select(variable, round_digits)
  for (i in seq_len(nrow(round_digits))) {
    col <- round_digits$variable[i]
    digits <- suppressWarnings(as.numeric(round_digits$round_digits[i]))
    attrs_df[[col]] <- round(as.numeric(attrs_df[[col]]), digits = digits)
  }

  if (is_spatial) {
    staged_df <- sf::st_sf(attrs_df, geometry = sf::st_geometry(clean_df))
    s3_write_geojson(staged_df, staged_link, acl = "public-read")
  } else {
    s3_write_csv(attrs_df, staged_link, acl = "public-read")
  }

  result$date_staged <- as.character(Sys.Date())
  result$staged_link <- staged_link
  result
}

#' Updates the dataset registry for a single dataset. If the dataset exists,
#' updates it with any new data. If not, inserts a new row.
#' Manually-edited columns are preserved.
#' @param config Main config
#' @param dataset_id Unique dataset id
#' @param date Date to append to date_updated, or NULL to leave it unchanged.
#'   date_updated keeps a history of the last 3 most recent run dates.
#' @param fail_message If provided, overwrites the link column with a
#'   "PIPELINE FAILED" message instead of the dataset's normal link
#' @param staging_result stage_data()'s return value, or NULL if staging was
#'   skipped.
update_dataset_registry <- function(config, dataset_id, date = NULL, fail_message = NULL,
                                    staging_result = NULL) {
  message("Grabbing dataset sub-config...")
  sub_config <- config[[dataset_id]]
  if (is.null(sub_config)) {
    stop(paste("ERROR: dataset_id", dataset_id, "not found in config."))
  }

  message("Computing spatial coverage...")
  coverage <- compute_dataset_coverage(config, dataset_id)

  registry <- tryCatch({
    dataset_registry_link <- config$metadata$dataset_registry_link
    message(sprintf("Pulling dataset registry from S3: %s", dataset_registry_link))
    obj <- s3_read_csv(dataset_registry_link)
  }, error = function(e) {
    print(e)
    message("Creating blank dataset registry because existing one not found...")
    tibble(dataset = character())
  })

  col_order <- c(
    "update_freq",
    "date_updated",
    "link",
    "date_staged",
    "staged_link",
    "quality_check_link",
    "mean_data_qual_score",
    "data_qual_score",
    "needs_review_flag",
    "clean_name",
    "category",
    "source",
    "source_url",
    "input_links",
    "spatial_level",
    "coverage",
    "date_range"
  )

  from_config <- function(col) {
    val <- sub_config[[col]]
    if (!is.null(val) && !is.list(val)) as.character(val) else ""
  }

  # Keep the last 3 successful run dates.
  old_date_updated <- registry$date_updated[registry$dataset == dataset_id][1]
  if (!is.null(date)) {
    parse_date_history <- function(x) {
      if (is.null(x) || length(x) == 0 || is.na(x)) return(character(0))
      trimws(strsplit(x, " \\| ")[[1]])
    }
    new_date_updated <- c(parse_date_history(old_date_updated), as.character(date))
    date_updated_val <- paste(tail(new_date_updated, 3), collapse = " | ")
  } else {
    date_updated_val <- old_date_updated %||% ""
  }

  # link is normally from_config("link"), but a pipeline failure overwrites it
  # with an error message
  link_val <- if (!is.null(fail_message)) {
    paste0("PIPELINE FAILED ON ", as.character(date %||% Sys.Date()), ": ", fail_message)
  } else {
    s3_public_urls(from_config("link"))
  }

  # These are overwritten during staging so keep whatever's in the config for now.
  date_staged_val <- from_config("date_staged")
  staged_link_val <- from_config("staged_link")
  mean_data_qual_score_val <- from_config("mean_data_qual_score")
  data_qual_score_val <- from_config("data_qual_score")
  needs_review_flag_val <- from_config("needs_review_flag")
  if (!is.null(staging_result)) {
    mean_data_qual_score_val <- staging_result$mean_data_qual_score
    data_qual_score_val <- staging_result$data_qual_score
    needs_review_flag_val <- staging_result$needs_review_flag
    if (!is.na(staging_result$date_staged)) date_staged_val <- staging_result$date_staged
    if (!is.na(staging_result$staged_link)) staged_link_val <- staging_result$staged_link
  }
  staged_link_val <- s3_public_url(staged_link_val)

  # Flatten input_links list into a single string
  input_links <- sub_config[["input_links"]]
  input_links_val <- if (!is.null(input_links) && length(input_links) > 0) {
    paste(unlist(input_links), collapse = " | ")
  } else {
    ""
  }

  col_values <- list(
    update_freq = from_config("update_freq"),
    date_updated = date_updated_val,
    link = link_val,
    date_staged = date_staged_val,
    staged_link = staged_link_val,
    quality_check_link = s3_public_url(from_config("quality_check_link")),
    mean_data_qual_score = mean_data_qual_score_val,
    data_qual_score = data_qual_score_val,
    needs_review_flag = needs_review_flag_val,
    clean_name = from_config("clean_name"),
    category = from_config("category"),
    source = from_config("source"),
    source_url = from_config("source_url"),
    input_links = input_links_val,
    spatial_level = from_config("spatial_level"),
    coverage = coverage,
    date_range = from_config("date_range")
  )

  new_row_data <- c(list(dataset = dataset_id), col_values[col_order])
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
  # Make sure col order is retained.
  final_col_order <- c("dataset", intersect(col_order, names(final_registry)),
                       setdiff(names(final_registry), c("dataset", col_order)))
  final_registry <- final_registry %>% select(all_of(final_col_order))

  message("Writing updated dataset registry to S3...")
  s3_write_csv(final_registry, dataset_registry_link, acl = "public-read")
}

#' Parse the cleaned dataset's variable names and types and upsert them into
#' the variable registry, scoring each variable's quality along the way.
#' Manually-edited columns are preserved; new variables are initialized
#' blank.
#' @param config Main config
#' @param dataset_id Unique dataset id
update_variable_registry <- function(config, dataset_id) {
  col_order <- c(
    "type",
    "description",
    "update_flag",
    "data_score_completeness",
    "data_score_duplicates",
    "data_score_coverage",
    "auto_data_score",
    "data_qual_flag",
    "clean_name",
    "use_in_tool",
    "round_digits",
    "tool_table_name",
    "filter_name",
    "filter_category",
    "subheader",
    "filter_subheader_when_selected",
    "dup_check_exempt",
    "slide_select"
  )

  # Columns in the variable registry that are manually updated.
  variable_registry_manual_cols <- c(
    "description",
    "clean_name",
    "use_in_tool",
    "round_digits",
    "tool_table_name",
    "filter_name",
    "filter_category",
    "subheader",
    "filter_subheader_when_selected",
    "dup_check_exempt",
    "slide_select"
  )

  message("Grabbing dataset sub-config...")
  sub_config <- config[[dataset_id]]
  if (is.null(sub_config)) {
    stop(paste("ERROR: dataset_id", dataset_id, "not found in config."))
  }

  dataset_link <- sub_config$link
  if (is.null(dataset_link) || dataset_link %in% c("", "N/A") ||
      grepl(" | ", dataset_link, fixed = TRUE)) {
    message("Dataset has no single clean link. Skip variable registry update.")
    return()
  }

  variable_registry_link <- config$metadata$variable_registry_link
  registry <- tryCatch({
    message(sprintf("Pulling variable registry from S3: %s", variable_registry_link))
    s3_read_csv(variable_registry_link)
  }, error = function(e) {
    message("Creating blank variable registry because existing one not found...")
    blank <- tibble(dataset = character(), variable = character(), type = character(), update_flag = character())
    blank[c(variable_registry_manual_cols, "data_score_coverage")] <- character()
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

  message("Calculating variable quality...")
  existing_rows_pre <- registry %>% filter(dataset == dataset_id)
  dup_check_exempt <- existing_rows_pre %>%
    filter(.data$dup_check_exempt %in% "TRUE") %>%
    pull(variable)
  coverage_rows <- existing_rows_pre %>%
    filter(!is.na(suppressWarnings(as.numeric(.data$data_score_coverage))))
  data_score_coverage <- setNames(
    suppressWarnings(as.numeric(coverage_rows$data_score_coverage)),
    coverage_rows$variable
  )
  var_scores <- calculate_var_quality_scores(
    clean_df, dataset_id,
    dup_check_exempt = dup_check_exempt,
    data_score_coverage = data_score_coverage
  )
  new_rows_df <- new_rows_df %>% left_join(var_scores, by = c("dataset", "variable"))

  message("Merging new variable data with manually updated columns...")
  dont_touch_these_columns <- setdiff(
    union(setdiff(names(registry), names(new_rows_df)), variable_registry_manual_cols),
    "update_flag"
  )
  existing_rows <- registry %>% filter(dataset == dataset_id)

  preserved_cols <- existing_rows %>%
    select(dataset, variable, any_of(dont_touch_these_columns))
  # Initialize any manual columns the registry doesn't have yet.
  missing_manual_cols <- setdiff(dont_touch_these_columns, names(preserved_cols))
  for (col in missing_manual_cols) {
    preserved_cols[[col]] <- rep(NA_character_, nrow(preserved_cols))
  }

  # Variables currently in clean data (new + still-tracked), merged with
  # whatever manual data already existed for them
  current_rows <- merge(preserved_cols, new_rows_df, by = c("dataset", "variable"), all.y = TRUE) %>%
    mutate(across(everything(), ~ as.character(.))) %>%
    mutate(across(all_of(dont_touch_these_columns), ~ ifelse(is.na(.), "", .)))
  current_rows$update_flag <- ifelse(current_rows$variable %in% existing_rows$variable, "", "new")

  # Variables tracked before but no longer in clean data
  removed_rows <- existing_rows %>%
    filter(!(variable %in% new_rows_df$variable)) %>%
    mutate(
      across(everything(), ~ as.character(.)),
      update_flag = "removed from source data"
    )

  updated_rows <- bind_rows(current_rows, removed_rows)

  message("Add updated rows to registry...")
  final_registry <- registry %>%
    filter(dataset != dataset_id) %>%
    bind_rows(., updated_rows) %>%
    mutate(across(all_of(c(variable_registry_manual_cols, "update_flag")), ~ ifelse(is.na(.), "", .))) %>%
    arrange(dataset, variable)
  final_col_order <- c("dataset", "variable", intersect(col_order, names(final_registry)),
                       setdiff(names(final_registry), c("dataset", "variable", col_order)))
  final_registry <- final_registry %>% select(all_of(final_col_order))

  message("Writing updated variable registry to S3...")
  s3_write_csv(final_registry, variable_registry_link, acl = "public-read")
}