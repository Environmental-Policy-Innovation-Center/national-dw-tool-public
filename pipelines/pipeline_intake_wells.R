#' Read and tidy the intake/well HUC12 input decks into one row per facility.
#' These are static files with no public API, so input_links are used instead
#' of a source_url.
#' @param config Main config
#' @param dataset_id "clean_pwsid_intake_well_huc12"
#' @return Tibble with columns: pwsid, huc12, facility_id, flag ("Intake"/"Wells")
get_tidy_intake_wells <- function(config, dataset_id = "clean_pwsid_intake_well_huc12") {
  sub_config <- config[[dataset_id]]
  intake_raw_link <- sub_config$input_links$intake_raw_link
  wells_raw_link <- sub_config$input_links$wells_raw_link
  sabs_link <- sub_config$input_links$sabs_link

  message("Downloading clean EPA SABs from S3 to get CWS pwsid list...")
  sabs_pwsids <- s3_read_geojson(sabs_link)$pwsid

  message("Downloading and tidying raw intake input deck from S3...")
  intake_tidy <- s3_read_csv(intake_raw_link) %>%
    janitor::clean_names() %>%
    filter(pwsid %in% sabs_pwsids) %>%
    # can confirm all facility_activity types are "active"
    # NOTE - there are 55 entries where the huc12s are null
    filter(!is.na(huc12)) %>%
    mutate(huc12 = fix_huc12(huc12), flag = "Intake")

  message("Downloading and tidying raw wells input deck from S3...")
  wells_tidy <- s3_read_csv(wells_raw_link) %>%
    janitor::clean_names() %>%
    # NOTE - there are 7 entries where the huc12s are null, most of these are in CNMI
    # or pwsid = "055293304"
    filter(!is.na(huc12)) %>%
    filter(pwsid %in% sabs_pwsids) %>%
    mutate(huc12 = fix_huc12(huc12), flag = "Wells")

  bind_rows(
    intake_tidy %>% select(pwsid, huc12, facility_id, flag),
    wells_tidy %>% select(pwsid, huc12, facility_id, flag)
  )
}

#' Crosswalk intake/well HUC12s to CWS pwsids, one row per pwsid with a
#' comma-separated list of intake/well HUC12s.
#' @param config Main config
#' @param dataset_id "clean_pwsid_intake_well_huc12"
run_clean_pwsid_intake_well_huc12_pipeline <- function(config, dataset_id = "clean_pwsid_intake_well_huc12") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  link <- sub_config$link

  intake_wells_long <- get_tidy_intake_wells(config, dataset_id)

  message("Collapsing to one row per pwsid...")
  intake_pwsid_hucs <- intake_wells_long %>%
    filter(flag == "Intake") %>%
    group_by(pwsid) %>%
    summarize(all_intake_hucs = paste(unique(huc12), collapse = ", "), .groups = "drop")
  wells_pwsid_hucs <- intake_wells_long %>%
    filter(flag == "Wells") %>%
    group_by(pwsid) %>%
    summarize(all_well_hucs = paste(unique(huc12), collapse = ", "), .groups = "drop")

  message("Merging intake and well HUC12s by pwsid...")
  pwsid_hucs <- full_join(wells_pwsid_hucs, intake_pwsid_hucs, by = "pwsid")

  message("Validating clean_pwsid_intake_well_huc12...")
  validate_pwsid_intake_well_huc12(config, pwsid_hucs, dataset_id)

  message("Writing clean dataset to S3...")
  s3_write_csv(pwsid_hucs, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(pwsid_hucs)
}

#' Pointblank validations for the intake/well HUC12 crosswalk
#' @param config Main config
#' @param pwsid_hucs pwsid-level intake/well HUC12 crosswalk
#' @param dataset_id "clean_pwsid_intake_well_huc12"
validate_pwsid_intake_well_huc12 <- function(config, pwsid_hucs, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  checks_df <- pwsid_hucs %>%
    mutate(has_any_huc12 = !is.na(all_intake_hucs) | !is.na(all_well_hucs))
  print(as_tibble(checks_df))

  agent <- new_check_agent(checks_df, label = "PWSID Intake/Well HUC12 Crosswalk Validation") %>%
    check_column_complete(pwsid, severity = "stop") %>%
    check_column_all_true(has_any_huc12, severity = "warning") %>%
    rows_distinct(
      columns = vars(pwsid),
      actions = action_levels(stop_at = 1),
      label = "No duplicate pwsids"
    ) %>%
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

  message("PWSID intake/well HUC12 crosswalk validation checks passed successfully.")
  return(TRUE)
}
