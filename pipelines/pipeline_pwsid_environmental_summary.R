#' Merge the 4 HUC12-level environmental summaries (NPDES, USTs, RMP sites,
#' impaired waters) into one table.
#' @param config Main config
#' @param dataset_id "merged_pwsid_npdes_usts_rmps_imp"
#' @return HUC12-level environmental summary
build_huc12_environmental_summary <- function(config, dataset_id = "merged_pwsid_npdes_usts_rmps_imp") {
  sub_config <- config[[dataset_id]]
  npdes_link <- sub_config$input_links$npdes_link
  ust_link <- sub_config$input_links$ust_link
  rmp_link <- sub_config$input_links$rmp_link
  imp_waters_link <- sub_config$input_links$imp_waters_link

  # Read numeric columns as-is (coerce_character = FALSE) so replace_na(.x, 0)
  # below doesn't need a stringify-then-reparse round trip -- only huc12 needs
  # repair, since read.csv()'s type inference can drop its leading zeros.
  message("Downloading HUC12 environmental summaries from S3 and fixing HUC12s...")
  npdes_fixed <- s3_read_csv(npdes_link, coerce_character = FALSE) %>% mutate(huc12 = fix_huc12(huc12))
  usts_fixed <- s3_read_csv(ust_link, coerce_character = FALSE) %>% mutate(huc12 = fix_huc12(huc12))
  rmps_fixed <- s3_read_csv(rmp_link, coerce_character = FALSE) %>% mutate(huc12 = fix_huc12(huc12))
  imp_fixed <- s3_read_csv(imp_waters_link, coerce_character = FALSE) %>%
    select(-last_epic_run_date) %>%
    mutate(huc12 = fix_huc12(huc12))

  message("Merging HUC12 environmental summaries...")
  npdes_fixed %>%
    full_join(usts_fixed, by = "huc12") %>%
    full_join(rmps_fixed, by = "huc12") %>%
    full_join(imp_fixed, by = "huc12") %>%
    # these are true zeros
    mutate(across(-huc12, ~ replace_na(.x, 0))) %>%
    # renaming here to keep the hookup the same for CNT
    rename(
      open_usts = total_open_usts,
      tos_usts = total_tos_usts,
      total_open_usts = epa_open_usts
    )
}

#' Merge NPDES/USTs/RMP sites/impaired waters HUC12-level summaries and roll
#' them up to pwsid level.
#' A pwsid can have multiple rows if it draws from multiple HUC12s.
#' @param config Main config
#' @param dataset_id "merged_pwsid_npdes_usts_rmps_imp"
run_merged_pwsid_npdes_usts_rmps_imp_pipeline <- function(config, dataset_id = "merged_pwsid_npdes_usts_rmps_imp") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  link <- sub_config$link

  huc12_env_summary <- build_huc12_environmental_summary(config, dataset_id)

  message("Downloading intake/well HUC12 table for pwsid/HUC12 facility counts...")
  intake_wells_long <- get_tidy_intake_wells(config, "clean_pwsid_intake_well_huc12")

  env_cols <- setdiff(names(huc12_env_summary), "huc12")

  message("Counting intake/well facilities per pwsid/HUC12 pair...")
  pwsid_huc12_facilities <- intake_wells_long %>%
    group_by(pwsid, huc12) %>%
    summarize(num_facilities = n_distinct(facility_id), .groups = "drop") %>%
    left_join(huc12_env_summary, by = "huc12") %>%
    # a pwsid/HUC12 pair with no match just means zero of that hazard type
    mutate(across(all_of(env_cols), ~ replace_na(.x, 0)))

  message("Validating merged_pwsid_npdes_usts_rmps_imp...")
  validate_merged_pwsid_npdes_usts_rmps_imp(config, pwsid_huc12_facilities, dataset_id)

  message(sprintf("Writing merged dataset to S3 at %s...", link))
  s3_write_csv(pwsid_huc12_facilities, link, acl = "public-read")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(pwsid_huc12_facilities)
}

#' Pointblank validations for the staged pwsid-level environmental summary
#' @param config Main config
#' @param pwsid_huc12_facilities pwsid/HUC12-level environmental summary
#' @param dataset_id "merged_pwsid_npdes_usts_rmps_imp"
validate_merged_pwsid_npdes_usts_rmps_imp <- function(config, pwsid_huc12_facilities, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  print(as_tibble(pwsid_huc12_facilities))

  agent <- new_check_agent(pwsid_huc12_facilities, label = "Staged PWSID Environmental Validation") %>%
    check_column_complete(pwsid, severity = "stop") %>%
    check_column_complete(huc12, severity = "stop") %>%
    rows_distinct(
      columns = vars(pwsid, huc12),
      actions = action_levels(stop_at = 1),
      label = "No duplicate pwsid/HUC12 pairs"
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

  message("Staged PWSID environmental hazard rollup validation checks passed successfully.")
  return(TRUE)
}
