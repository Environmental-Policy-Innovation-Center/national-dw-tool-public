#' Pull and clean EPA Service Area Boundaries (SABs)
#' @param config Main config
#' @param dataset_id "raw_epa_sabs"
run_epa_sabs_pipeline <- function(config, dataset_id) {
  update_raw_epa_sabs(config, dataset_id)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Pull raw EPA Service Area Boundaries with paginated ArcGIS REST query
update_raw_epa_sabs <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  link <- sub_config$link

  message("Fetching layer metadata from EPA ArcGIS REST API...")
  layer_metadata <- arcpullr::get_layer_info(paste0(source_url, "/getEstimates"))
  message(sprintf("Number of records: %d", layer_metadata$count))

  # prepping to paginate using the API:
  full_count <- layer_metadata$count + 1000  # buffer
  sabs_data_query <- list()
  seq_to_loop <- seq(from = 0, to = full_count, by = 500)
  query_url <- paste0(source_url, "/query")

  message("Paginate API results to avoid hitting hidden limit...")
  for (i in seq_along(seq_to_loop)) {
    # find range boundaries
    range_min <- seq_to_loop[i]
    range_max <- seq_to_loop[i + 1]
    # build query
    query <- list(
      where = "1=1",
      outFields = "*",
      returnGeometry = "true",
      f = "geojson",
      resultOffset = range_min,
      resultRecordCount = 500
    )
    # pass request
    req <- httr::GET(query_url, query = query)
    full_count_i <- sf::st_read(httr::content(req, "text"), quiet = TRUE)

    if (nrow(full_count_i) == 0) {
      message(sprintf("No data on page starting at %d - stopping pagination.", range_min))
      break
    }

    message(sprintf("Fetched %d - %d (%d records)", range_min, range_max, nrow(full_count_i)))
    sabs_data_query[[length(sabs_data_query) + 1]] <- full_count_i
    Sys.sleep(5)
  }

  message("Combining paginated results...")
  epa_sabs_all <- do.call(rbind, sabs_data_query)

  message("Checking that everything was captured...")
  if (nrow(epa_sabs_all) != layer_metadata$count) {
    stop(sprintf("API QUERY MISSED RECORD: expected %d, got %d.",
                 layer_metadata$count, nrow(epa_sabs_all)), call. = FALSE)
  }

  message("Running st_make_valid() for some light tidying...")
  epa_sabs <- epa_sabs_all %>%
    janitor::clean_names() %>%
    mutate(last_epic_run_date = Sys.Date()) %>%
    st_make_valid()

  message("Validating raw EPA SABs...")
  validate_raw_epa_sabs(config, dataset_id, epa_sabs)

  message("Saving raw EPA SABs to S3...")
  s3_write_geojson(epa_sabs, link)

  return(epa_sabs)
}

#' Pointblank validations for raw EPA SABs
validate_raw_epa_sabs <- function(config, dataset_id, epa_sabs) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  checks_df <- tibble(
    row_count      = nrow(epa_sabs),
    geometry_valid = all(sf::st_is_valid(epa_sabs))
  )
  print(checks_df)

  agent <- new_check_agent(checks_df, label = "EPA SABs Validation") %>%
    col_vals_gt(
      columns = vars(row_count),
      value = 0,
      actions = action_levels(stop_at = 1),
      label = "EPA SABs dataset has > 0 rows"
    ) %>%
    col_vals_equal(
      columns = vars(geometry_valid),
      value = TRUE,
      actions = action_levels(warn_at = 1),
      label = "All geometries are valid"
    ) %>%
    interrogate()

  result <- summarize_checks(agent)
  message(sprintf("Validation result summary: %s", result$summary))

  report_link <- write_check_artifacts(
    agent       = agent,
    report_df   = result$report_df,
    checks_base = checks_base,
    tag         = dataset_id,
    run_ts      = run_ts
  )
  message(sprintf("Validation reports pushed to S3: %s", report_link))

  if (isTRUE(result$any_error)) {
    stop(sprintf("VALIDATION FAILED: %s", result$summary), call. = FALSE)
  }

  message("EPA SABs validation checks passed successfully.")
  return(TRUE)
}

#' Light cleaning + state-intersection tagging for EPA SABs
#' @param config Main config
#' @param dataset_id "clean_epa_sabs"
#' @param epa_sabs Optional pre-loaded raw EPA SABs sf object. If NULL, downloaded from S3.
run_clean_epa_sabs_pipeline <- function(config, dataset_id = "clean_epa_sabs", epa_sabs = NULL) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  raw_link <- sub_config$input_links$raw_link
  link <- sub_config$link
  pwsid_names_link <- sub_config$pwsid_names_link

  if (is.null(epa_sabs)) {
    message("Downloading raw EPA SABs from S3...")
    epa_sabs <- s3_read_geojson(raw_link)
  }

  message("Simplifying geometries for easier rendering and standardizing names for future merging...")
  epa_sabs_clean <- epa_sabs %>%
    st_simplify() %>%
    mutate(pws_name = str_to_title(pws_name),
           pws_name = trimws(pws_name),
           pwsid = trimws(pwsid))

  message("Creating small helper SABs dataframe...")
  sabs_df <- epa_sabs_clean %>%
    as.data.frame() %>%
    select(pwsid, pws_name) %>%
    arrange(pwsid) %>%
    unique()

  message("Tagging SABs with intersecting states for easy filtering while avoiding removal of tribal SABs...")
  sf_use_s2(FALSE)
  state_pwsid <- st_intersection(epa_sabs_clean, tigris::states() %>%
                                    st_transform(crs = st_crs(epa_sabs_clean)))

  state_pwsid_summary <- state_pwsid %>%
    as.data.frame() %>%
    select(pwsid, STUSPS) %>%
    rename(state_intersect = STUSPS) %>%
    group_by(pwsid) %>%
    # Handles duplicate sabs
    summarize(states_intersect = paste(unique(state_intersect), collapse = ",")) %>%
    unique()
  sf_use_s2(TRUE)

  sabs_df_summary <- merge(sabs_df, state_pwsid_summary, all = TRUE)

  message("Writing intermediary pwsid/name/state-intersection table to S3...")
  s3_write_csv(sabs_df_summary, pwsid_names_link, acl = "public-read")

  message("Adding state-intersection flags, EWG links, and area...")
  epa_sabs_final <- merge(epa_sabs_clean,
                           sabs_df_summary %>% select(-pws_name),
                           by = "pwsid", all.x = TRUE) %>%
    rename(epic_states_intersect = states_intersect) %>%
    relocate(epic_states_intersect, .after = pwsid) %>%
    mutate(ewg_report_link = paste0("https://www.ewg.org/tapwater/system.php?pws=", pwsid)) %>%
    relocate(ewg_report_link, .before = last_epic_run_date) %>%
    # projecting to alberts equal area for st_area calculations 
    st_transform(crs = 5070)

  epa_sabs_final$epic_area_mi2 <- units::set_units(st_area(epa_sabs_final), "mi^2")
  epa_sabs_final <- epa_sabs_final %>%
    relocate(epic_area_mi2, .before = last_epic_run_date) %>%
    # transform back to original crs of WGS 84
    st_transform(crs = st_crs(epa_sabs))

  message("Resolving known duplicate/erroneous pwsid records...")
  epa_sabs_final <- resolve_epa_sabs_duplicates(config, dataset_id, epa_sabs = epa_sabs_final)

  message("Saving clean EPA SABs to S3...")
  s3_write_geojson(epa_sabs_final, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Merge known duplicate pwsids and fix known bad records in cleaned EPA SABs.
#' 
#' This can be run as a standalone function and will overwrite the clean
#' epa_sabs.geojson file if write_to_s3 is set to TRUE.
#' @param config Main config
#' @param dataset_id "clean_epa_sabs"
#' @param epa_sabs Optional pre-loaded, cleaned EPA SABs sf object. If NULL, downloaded from S3.
#' @param write_to_s3 If TRUE, writes the result back to S3. Defaults to FALSE.
resolve_epa_sabs_duplicates <- function(config, dataset_id = "clean_epa_sabs", epa_sabs = NULL, write_to_s3 = FALSE) {
  sub_config <- config[[dataset_id]]
  link <- sub_config$link

  if (is.null(epa_sabs)) {
    message("Downloading clean EPA SABs from S3...")
    epa_sabs <- s3_read_geojson(link)
  }

  # there are approximately 6 pwsids that are duplicated in this dataset
  # FL1070685, FL1190789, FL6580531, IA2573701, MT0001923, VA5019052
  # but confirmed these should just be grouped and merged
  duplicated_pwsid <- epa_sabs[duplicated(epa_sabs$pwsid), ]
  dups <- epa_sabs %>%
    filter(pwsid %in% duplicated_pwsid$pwsid) %>%
    # this is the one system that has duplicated information, but different
    # fields. Considering the source of this one is ND water districts and
    # the EPA data had been updated more recently, I'm opting to remove this
    # record.
    filter(pwsid != "ND2801430" & original_data_provider != "NDGISDP-DWR") %>%
    group_by(pwsid, pws_name) %>%
    summarize(geometry = st_union(geometry), .groups = "drop") %>%
    # convert everything to multipolygons to be consistent with all other SABs
    st_cast("MULTIPOLYGON")
  # NOTE - there is another SAB with two pwsids pasted together in PR:
  # pwsid == "PR0005086; PR0005066" and another of ND water systems:
  # pwsid == ND3401128; ND1001380; ND4801479
  # pwsid == ND5101125; ND5101065; ND3101807

  if (nrow(dups) == 0) {
    message("No duplicate pwsids found - skipping duplicate geometry merge.")
    epa_sabs_final <- epa_sabs
  } else {
    message(sprintf("Merging %d duplicated pwsid(s) into single multipolygons...", nrow(dups)))

    # creating a simple data frame to retain key information
    epa_sabs_df <- epa_sabs %>%
      filter(pwsid %in% dups$pwsid) %>%
      as.data.frame() %>%
      # NOTE - that any pwsids with NAs here had duplicates that needed to be
      # resolved
      select(-any_of(c("geometry", "shape_area", "shape_length", "epic_area_mi2", "objectid"))) %>%
      distinct()

    # merge back with resolved duplicate boundaries
    dups_sabs <- merge(dups, epa_sabs_df, all.x = TRUE)

    # remove dups from EPA SABs
    epa_sabs_nodups <- epa_sabs %>%
      filter(!(pwsid %in% dups$pwsid))

    # adding them back in, and recalculating our area in mi2
    epa_sabs_tidy <- bind_rows(epa_sabs_nodups, dups_sabs) %>%
      # projecting to alberts equal area for st_area calculations
      st_transform(crs = 5070) %>%
      # removing pwsid == "ND2801430" and original data provider = NDGISDP-DWR
      # because this pwsid is duplicated
      filter(!(pwsid == "ND2801430" & original_data_provider == "NDGISDP-DWR"))

    # adding area:
    epa_sabs_tidy$epic_area_mi2 <- units::set_units(st_area(epa_sabs_tidy), "mi^2")
    epa_sabs_final <- epa_sabs_tidy %>%
      relocate(epic_area_mi2, .before = last_epic_run_date) %>%
      # transform back to original geodetic crs of WGS 84
      st_transform(crs = st_crs(epa_sabs))
  }

  epa_sabs_final <- epa_sabs_final %>%
    # if the pwsid field actually contains multiple water system IDs,
    # remove the ewg link because it is probably incorrect
    mutate(ewg_report_link = case_when(grepl("; ", pwsid) ~ NA_character_, TRUE ~ ewg_report_link)) %>%
    # arrange by pwsid
    arrange(pwsid)

  if (write_to_s3) {
    message("Overwriting clean EPA SABs in S3 with resolved duplicates...")
    s3_write_geojson(epa_sabs_final, link)
  }

  epa_sabs_final
}
