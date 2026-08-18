###############################################################################
# SDWA / SDWIS Violations
#
# raw_sdwa - downloads EPA ECHO's SDWA bulk zip, extracts and filters the 3
# CSVs we care about (water system info, violations/enforcement, rule
# reference codes) to community water systems in our EPA SABs dataset, and
# writes each to its own S3 path.
#
# clean_sdwis_viols - reads those 3 raw files and summarizes each
# water system's health-based/paperwork/tier-1 violation counts over the
# past 5 and 10 years.
#
# Data are refreshed quarterly & data dictionary is located here: 
# https://echo.epa.gov/tools/data-downloads/sdwa-download-summary
###############################################################################

#' Download EPA ECHO's SDWA bulk zip, extract the 3 CSVs we use, filter each
#' to community water systems in our EPA SABs dataset, and write them to
#' their own S3 paths.
#' @param config Main config
#' @param dataset_id "raw_sdwa"
run_sdwa_pipeline <- function(config, dataset_id) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  source_url <- sub_config$source_url
  pwsid_names_link <- sub_config$input_links$pwsid_names_link
  ws_info_link <- sub_config$ws_info_link
  viol_enf_link <- sub_config$viol_enf_link
  ref_codes_link <- sub_config$ref_codes_link

  message("Reading EPA SABs PWSIDs to filter for community water systems....")
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)

  message("Downloading SDWA bulk zip from EPA ECHO...")
  tmp_zip <- tempfile(fileext = ".zip")
  tmp_exdir <- tempfile(pattern = "sdwa_working_")
  on.exit(unlink(tmp_exdir, recursive = TRUE), add = TRUE)
  options(timeout = max(720, getOption("timeout")))
  curl::curl_download(source_url, destfile = tmp_zip)
  unzip(tmp_zip, exdir = tmp_exdir)
  unlink(tmp_zip)

  message("Reading and filtering water system info to active CWS that are in the EPA SABs dataset...")
  ws_info <- data.table::fread(file.path(tmp_exdir, "SDWA_PUB_WATER_SYSTEMS.csv")) %>%
    janitor::clean_names() %>%
    filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
    mutate(last_epic_run_date = Sys.Date())
  if (nrow(ws_info) == 0) {
    stop("SDWA_PUB_WATER_SYSTEMS.csv had 0 rows after filtering to CWS pwsids.", call. = FALSE)
  }
  message(sprintf("Writing raw water system info to S3 at %s...", ws_info_link))
  s3_write_csv(ws_info, ws_info_link)

  message("Reading and filtering violations and enforcement to active CWS that are in the EPA SABs dataset...")
  viol_enf <- data.table::fread(file.path(tmp_exdir, "SDWA_VIOLATIONS_ENFORCEMENT.csv")) %>%
    janitor::clean_names() %>%
    filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
    mutate(last_epic_run_date = Sys.Date())
  if (nrow(viol_enf) == 0) {
    stop("SDWA_VIOLATIONS_ENFORCEMENT.csv had 0 rows after filtering to CWS pwsids.", call. = FALSE)
  }
  message(sprintf("Writing raw violations/enforcement to S3 at %s...", viol_enf_link))
  s3_write_csv(viol_enf, viol_enf_link)

  message("Reading and filtering rule reference codes to relate violation data to actual rule codes...")
  ref_codes <- data.table::fread(file.path(tmp_exdir, "SDWA_REF_CODE_VALUES.csv")) %>%
    janitor::clean_names() %>%
    filter(value_type == "RULE_FAMILY_CODE") %>%
    rename(rule = value_description) %>%
    mutate(last_epic_run_date = Sys.Date(), value_code = as.integer(value_code))
  if (nrow(ref_codes) == 0) {
    stop("SDWA_REF_CODE_VALUES.csv had 0 rows after filtering to RULE_FAMILY_CODE.", call. = FALSE)
  }
  message(sprintf("Writing raw rule reference codes to S3 at %s...", ref_codes_link))
  s3_write_csv(ref_codes, ref_codes_link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}

#' Summarize the raw SDWA files into the violation-summary schema.
#' @param config Main config
#' @param dataset_id "clean_sdwis_viols"
#' @return Clean SDWIS violations data frame
run_clean_sdwis_viols_pipeline <- function(config, dataset_id = "clean_sdwis_viols") {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  ws_info_link <- sub_config$input_links$ws_info_link
  viol_enf_link <- sub_config$input_links$viol_enf_link
  ref_codes_link <- sub_config$input_links$ref_codes_link
  pwsid_names_link <- sub_config$input_links$pwsid_names_link
  link <- sub_config$link

  message("Downloading raw SDWA files...")
  ws_info <- s3_read_csv(ws_info_link)
  viol_enf <- s3_read_csv(viol_enf_link)
  ref_codes <- s3_read_csv(ref_codes_link)
  epa_sabs_pwsids <- s3_read_csv(pwsid_names_link)

  message("Tidying water system info...")
  sdwis_ws_info_tidy <- ws_info %>%
    mutate(owner_type = case_when(
             owner_type_code == "F" ~ "Federal",
             owner_type_code == "L" ~ "Local",
             owner_type_code == "M" ~ "Public/Private",
             owner_type_code == "N" ~ "Native American",
             owner_type_code == "P" ~ "Private",
             owner_type_code == "S" ~ "State"),
           first_reported_date = as.Date(first_reported_date, tryFormats = c("%m/%d/%Y")),
           years_operating = year(Sys.Date()) - year(first_reported_date)) %>%
    # note there are a couple of inactive systems where pws_activity_code == "I",
    # but this might be SABs that haven't been removed yet
    select(pwsid, pws_activity_code, gw_sw_code, primary_source_code,
           first_reported_date, years_operating, owner_type,
           primacy_type:is_school_or_daycare_ind,
           source_water_protection_code, outstanding_performer, city_name,
           address_line1, address_line2, zip_code, phone_number) %>%
    mutate(across(everything(), as.character),
           years_operating = as.numeric(years_operating))

  # translating empty cells to no information, as there are many blanks 
  # and some boolean values that are Y/N but also blank, and I don't want 
  # these to accidentally get transformed 
  sdwis_ws_info_tidy[sdwis_ws_info_tidy == ""] <- "No Information"

  less_10years <- sdwis_ws_info_tidy %>% filter(years_operating < 10)
  less_5_years <- sdwis_ws_info_tidy %>% filter(years_operating < 5)

  message("Joining violations to rule reference codes...")
  # NOTE - I'm pretty positive the data dictionary is wrong, 
  # and the rule family code [with the 120-430 code breakdowns] s
  # hould be swapped with the rule group code [which 
  # only have 5 codes]
  viol_rulecode <- merge(
    viol_enf,
    ref_codes %>% select(-last_epic_run_date),
    by.x = "rule_family_code", by.y = "value_code", all.x = TRUE
  ) %>%
    filter(pwsid %in% epa_sabs_pwsids$pwsid) %>%
    # Step 1: concatenate PWSID and violation_id to create a unique identifier
    mutate(pwsid_viol_id = paste0(pwsid, "-", violation_id)) %>%
    # Step 2: remove duplicates, keep distinct records using unique ID from 
    # step 1: this helps grab one record per violation (violation ids can 
    # be duplicated and are repeated for various enforcement actions)
    distinct(pwsid_viol_id, .keep_all = TRUE)

  message("Summarizing and merging health-based violations...")
  hb_summary <- function(years_back, suffix) {
    viol_rulecode %>%
      filter(is_health_based_ind == "Y") %>%
      mutate(viol_date = as.Date(compl_per_begin_date, tryFormats = c("%m/%d/%Y")),
             viol_year = year(viol_date)) %>%
      filter(viol_year > (year(Sys.Date()) - years_back)) %>%
      group_by(pwsid, rule) %>%
      summarize(total = n(), .groups = "drop") %>%
      mutate(rule = paste0(rule, "_healthbased", suffix)) %>%
      pivot_wider(names_from = rule, values_from = total) %>%
      janitor::clean_names() %>%
      mutate(across(everything(), ~ replace_na(.x, 0))) %>%
      mutate("health_viols{suffix}" := rowSums(across(where(is.numeric))))
  }
  viol_disag_summary <- merge(hb_summary(5, "_5yr"), hb_summary(10, "_10yr"),
                              by = "pwsid", all = TRUE) %>%
    mutate(across(everything(), ~ replace_na(.x, 0)))

  message("Summarizing and merging paperwork violations...")
  pw_summary <- function(years_back, colname) {
    viol_rulecode %>%
      filter(is_health_based_ind != "Y") %>%
      mutate(viol_date = as.Date(compl_per_begin_date, tryFormats = c("%m/%d/%Y")),
             viol_year = year(viol_date)) %>%
      filter(viol_year > (year(Sys.Date()) - years_back)) %>%
      group_by(pwsid) %>%
      summarize("{colname}" := n(), .groups = "drop")
  }
  paperwork_viols_summary <- merge(
    pw_summary(5, "total_paperwork_violations_5yr"),
    pw_summary(10, "total_paperwork_violations_10yr"),
    by = "pwsid", all = TRUE
  )

  message("Merging health-based and paperwork violations...")
  viol_year_summaries <- merge(viol_disag_summary, paperwork_viols_summary, by = "pwsid", all = TRUE) %>%
    mutate(across(everything(), ~ replace_na(.x, 0)),
           total_viols_5yr = total_paperwork_violations_5yr + health_viols_5yr,
           total_viols_10yr = total_paperwork_violations_10yr + health_viols_10yr)

  message("Summarizing tier 1 public notice violations...")
  # In tier 1, the water system would  have 24 hours to report violation to 
  # customers (some/most would probably be boil water notices) 
  # NOTE - these are NOT filtered by healthbased, but based on 
  # any(tier_one_10yr$is_health_based_ind != "Y") == FALSE, these are all health
  # based 
  tier1_summary <- function(years_back, colname) {
    viol_rulecode %>%
      filter(public_notification_tier == 1) %>%
      mutate(viol_date = as.Date(compl_per_begin_date, tryFormats = c("%m/%d/%Y")),
             viol_year = year(viol_date)) %>%
      filter(viol_year > (year(Sys.Date()) - years_back)) %>%
      group_by(pwsid) %>%
      summarize("{colname}" := n(), .groups = "drop")
  }
  tier_one_viols <- merge(
    tier1_summary(5, "tier_1_pn_5yr"),
    tier1_summary(10, "tier_1_pn_10yr"),
    by = "pwsid", all = TRUE
  )

  viol_year_summaries_pn <- merge(viol_year_summaries, tier_one_viols, by = "pwsid", all = TRUE) %>%
    mutate(across(everything(), ~ replace_na(.x, 0))) %>%
    relocate(c(tier_1_pn_5yr, total_paperwork_violations_5yr, total_viols_5yr), .after = health_viols_5yr) %>%
    relocate(c(tier_1_pn_10yr, total_paperwork_violations_10yr, total_viols_10yr), .after = health_viols_10yr)

  message("Summarizing all-time totals and open violations...")
  total_viols <- viol_rulecode %>%
    group_by(pwsid) %>%
    summarize(violations_all_years = n(),
              health_violations_all_years = sum(is_health_based_ind == "Y"),
              .groups = "drop")

  open_viol <- viol_rulecode %>%
    filter(is_health_based_ind == "Y") %>%
    # Note - the other options here are "resolved" or "archived" - archived 
    # means that the violation no longer contributes to overall compliance status
    # addressed means the violation has a formal enforcement action, but is not 
    # resolved or archived. 
    filter(violation_status %in% c("Addressed", "Unaddressed"))

  message("Merging all datasets together...")
  viol_final_summary <- merge(viol_year_summaries_pn, total_viols, by = "pwsid", all = TRUE)

  message("Merging violation summary with water system info...")
  ws_viol_final <- merge(sdwis_ws_info_tidy, viol_final_summary, by = "pwsid", all = TRUE) %>%
    mutate(open_health_viol = case_when(pwsid %in% open_viol$pwsid ~ "Yes", TRUE ~ "No")) %>%
    mutate(across(lead_and_copper_rule_healthbased_5yr:health_violations_all_years, ~ replace_na(.x, 0))) %>%
    # transforming to character so I can handle systems that have been operating 
    # for less than 5 or 10 years 
    mutate(across(everything(), as.character)) %>%
    mutate(across(ends_with("_5yr"), ~ ifelse(pwsid %in% less_5_years$pwsid,
                                              "Not Enough Data - Operating < 5 years", .x))) %>%
    mutate(across(ends_with("_10yr"), ~ ifelse(pwsid %in% less_10years$pwsid,
                                               "Not Enough Data - Operating < 10 years", .x))) %>%
    rename(paperwork_viols_5yr = total_paperwork_violations_5yr,
           paperwork_viols_10yr = total_paperwork_violations_10yr,
           health_viols_all_years = health_violations_all_years)

  if (nrow(ws_viol_final) == 0) {
    stop("Summarized SDWIS violations data had 0 rows.", call. = FALSE)
  }

  message("Validating clean_sdwis_viols...")
  validate_clean_sdwis_viols(config, ws_viol_final, dataset_id)

  message(sprintf("Writing clean SDWIS violations to S3 at %s...", link))
  s3_write_csv(ws_viol_final, link, acl = "public-read")

  message(sprintf("%s pipeline completed successfully.", dataset_id))
  return(ws_viol_final)
}

#' Pointblank validations for the clean SDWIS violations dataset
#' @param config Main config
#' @param ws_viol_final Clean SDWIS violations data frame
#' @param dataset_id "clean_sdwis_viols"
validate_clean_sdwis_viols <- function(config, ws_viol_final, dataset_id) {
  checks_base <- config$metadata$checks_link
  run_ts <- Sys.time()

  agent <- new_check_agent(ws_viol_final, label = "Clean SDWIS Violations Validation") %>%
    check_column_complete(pwsid, severity = "stop") %>%
    rows_distinct(columns = vars(pwsid), actions = action_levels(stop_at = 1),
                  label = "No duplicate pwsid rows") %>%
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

  message("Clean SDWIS violations validation checks passed successfully.")
  return(TRUE)
}
