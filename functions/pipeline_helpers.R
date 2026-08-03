#' Pad a HUC12 code back out to the standard 12 digits. HUC12 codes are
#' sometimes read back with leading zeros dropped (e.g. 10 or 11 digits
#' instead of 12).
#' @param huc12 Character or numeric vector of HUC12 codes
#' @return Character vector of HUC12 codes, left-padded with zeros to 12 digits
fix_huc12 <- function(huc12) stringr::str_pad(huc12, width = 12, side = "left", pad = "0")
