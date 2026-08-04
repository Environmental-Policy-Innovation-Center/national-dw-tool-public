# Shared crosswalk step for datasets that get interpolated onto EPA Service
# Area Boundaries (SVI, CEJST, EJScreen) using xwalk_census_geo_sabs().
# Each dataset's own pipeline file handles its dataset-specific raw pull and
# prep, then calls run_sabs_xwalk_pipeline() to do the shared crosswalk +
# write step.
#
# get_census_var_interp_methods() lives in functions/pipeline_helpers.R.

#' Crosswalk a census-geometry dataset onto EPA SABs and write the result
#' @param config Main config
#' @param dataset_id The "clean_X_xwalk" dataset_id (for config + registry)
#' @param xwalk_dataset_name The value of the `dataset` column in the
#'   interpolation methods sheet (e.g. "svi", "cejst", "ejscreen")
#' @param sf_data_census sf object of the prepped census-geometry dataset
#' @param fips_col The fips column name in sf_data_census
#' @param blocks_2020 Whether to use 2020 or 2010 census blocks as weights
run_sabs_xwalk_pipeline <- function(config, dataset_id, xwalk_dataset_name,
                                     sf_data_census, fips_col, blocks_2020) {
  message(sprintf("Grabbing config variables for dataset %s...", dataset_id))
  sub_config <- config[[dataset_id]]
  sabs_link <- sub_config$input_links$sabs_link
  link <- sub_config$link

  message("Downloading clean EPA SABs from S3...")
  epa_sabs <- s3_read_geojson(sabs_link)

  message(sprintf("Grabbing interpolation methods for %s...", xwalk_dataset_name))
  interp_methods <- get_census_var_interp_methods(config) %>%
    filter(dataset == xwalk_dataset_name)

  message(sprintf("Crosswalking %s onto SABs...", xwalk_dataset_name))
  sab_xwalk <- xwalk_census_geo_sabs(epa_sabs, sf_data_census, interp_methods,
                                     blocks_2020 = blocks_2020, fips_col = fips_col,
                                     save_data = FALSE)

  sab_xwalk_df <- sab_xwalk %>%
    as.data.frame() %>%
    select(-starts_with("geom"))

  message("Saving crosswalked dataset to S3...")
  s3_write_csv(sab_xwalk_df, link)

  message(sprintf("%s pipeline completed successfully.", dataset_id))
}
