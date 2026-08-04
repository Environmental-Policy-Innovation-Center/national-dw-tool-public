#' Pad a HUC12 code back out to the standard 12 digits. HUC12 codes are
#' sometimes read back with leading zeros dropped (e.g. 10 or 11 digits
#' instead of 12).
#' @param huc12 Character or numeric vector of HUC12 codes
#' @return Character vector of HUC12 codes, left-padded with zeros to 12 digits
fix_huc12 <- function(huc12) stringr::str_pad(huc12, width = 12, side = "left", pad = "0")

# Caches the census variable interpolation methods sheet per session.
# get_census_var_interp_methods() is called twice per pipeline run so we only
# need one Google Sheets fetch instead of two.
.census_interp_env <- new.env(parent = emptyenv())

#' Grabs the census variable interpolation methods sheet.
#' @param config Main config
get_census_var_interp_methods <- function(config) {
  if (is.null(.census_interp_env$data)) {
    googlesheets4::gs4_deauth()
    .census_interp_env$data <- googlesheets4::read_sheet(
      config$metadata$census_var_methods_sheet_url, sheet = "census_geo_methods"
    ) %>%
      janitor::clean_names()
  }
  .census_interp_env$data
}
