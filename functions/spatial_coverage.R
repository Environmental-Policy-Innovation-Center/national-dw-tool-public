###############################################################################
# Spatial Coverage (issue #35)
# get_spatial_coverage(data, crosswalk): given a dataset with a pwsid column,
# return the set of state/territory abbreviations the dataset serves.
#
# Used two ways:
#   1. #35: populate the dataset registry's spatial_coverage column
#      (collapse_conus = TRUE gives the registry string, e.g. "CONUS, AK, PR")
#   2. #26: the coverage-change check compares two of these sets (staging vs prod)
#
# Design notes (grounded in the real prod data + Emma's crosswalk):
# - State is the SERVED state, resolved through sabs_pwsid_county (Emma's file,
#   built by 1_downloaders/yearly/sabs_worker/sabs_county_served.R). This is the
#   definition the team chose on #26. We do NOT read a dataset's own `state`
#   column: awia's is the regulating state (a different definition) and
#   national_bwn_summary's is dirty ("Alaska", "Louisiana - BWA, 1yr").
# - The crosswalk's county_served looks like "County A, ST1; County B, ST2", so a
#   single system can serve multiple states. We keep all of them.
# - Compound pwsid keys: ~9 crosswalk rows (and the matching dataset rows) carry a
#   semicolon-joined group of pwsids in one cell, e.g. "ND0501057; ND0501127".
#   We explode both sides on ";" so a member pwsid resolves whether it appears
#   alone or inside a group.
# - PWSIDs absent from the crosswalk resolve to "UNKNOWN" (never silently dropped);
#   the count is available via attr(x, "n_unknown") so the report can surface it.
#   Note for #26: "UNKNOWN" is a member of the returned set, so a staging-vs-prod
#   coverage diff should compare setdiff(x, "UNKNOWN") and report the unknown count
#   separately, otherwise a change in unknown-ness alone looks like a coverage change.
###############################################################################

library(stringr)

# The 48 contiguous states drive the CONUS collapse. DC is treated as a separate
# jurisdiction: #35's abbreviation list does not name it, and only ~2 systems
# nationwide serve DC, so requiring DC would stop genuinely national datasets
# (e.g. npdes covers all 48 contiguous but not DC) from collapsing to CONUS. So DC
# never blocks the collapse; it is folded into CONUS when the 48 are complete, and
# listed alongside the states otherwise.
.states_48 <- setdiff(state.abb, c("AK", "HI"))
.territories <- c("AS", "GU", "MP", "PR", "VI")

###############################################################################
# .split_pwsids(x)
# Split a pwsid cell into its member ids. Handles the semicolon-joined groups.
###############################################################################
.split_pwsids <- function(x) {
  parts <- str_split(x, ";")[[1]]
  # Uppercase both sides (crosswalk keys and incoming pwsids run through here) so
  # a lowercase id resolves instead of silently falling through to UNKNOWN.
  toupper(str_trim(parts[parts != ""]))
}

###############################################################################
# .states_from_county_served(county_served)
# Extract every 2-letter state/territory code from a "County, ST; County, ST"
# string. Returns a character vector (possibly length > 1), unique.
###############################################################################
.states_from_county_served <- function(county_served) {
  codes <- str_match_all(county_served, ",\\s*([A-Z]{2})\\b")[[1]][, 2]
  unique(codes)
}

###############################################################################
# build_coverage_lookup(crosswalk)
# Turn the sabs_pwsid_county crosswalk (columns pwsid, county_served) into a
# named list: member pwsid -> character vector of served states. Exploding the
# compound keys means each member id is directly resolvable.
###############################################################################
build_coverage_lookup <- function(crosswalk) {
  stopifnot(all(c("pwsid", "county_served") %in% names(crosswalk)))
  lookup <- new.env(parent = emptyenv())
  for (i in seq_len(nrow(crosswalk))) {
    states <- .states_from_county_served(crosswalk$county_served[i])
    if (length(states) == 0) next
    for (pw in .split_pwsids(crosswalk$pwsid[i])) {
      prev <- if (!is.null(lookup[[pw]])) lookup[[pw]] else character(0)
      lookup[[pw]] <- union(prev, states)
    }
  }
  lookup
}

###############################################################################
# format_coverage(states)
# Collapse a raw state set into the registry string. If every contiguous state
# is present it becomes "CONUS"; otherwise the contiguous states are listed.
# AK / HI and territories are appended in a stable order.
###############################################################################
format_coverage <- function(states) {
  states <- unique(states)
  parts <- character(0)

  if (all(.states_48 %in% states)) {
    parts <- c(parts, "CONUS")            # continental US (DC absorbed) collapsed
  } else {
    contig <- intersect(.states_48, states)
    if (length(contig) > 0) parts <- c(parts, sort(contig))
    if ("DC" %in% states) parts <- c(parts, "DC")
  }
  if ("AK" %in% states) parts <- c(parts, "AK")
  if ("HI" %in% states) parts <- c(parts, "HI")
  parts <- c(parts, intersect(.territories, states))
  # "UNKNOWN" is intentionally NOT written to the registry string (it is not a
  # #35 abbreviation); the count is exposed via attr(x, "n_unknown") and belongs
  # to the data-quality columns, not spatial_coverage.

  paste(parts, collapse = ", ")
}

###############################################################################
# get_spatial_coverage(data, crosswalk = NULL, lookup = NULL, collapse_conus = FALSE)
# data           : data frame / sf with a `pwsid` column
# crosswalk      : sabs_pwsid_county data frame (pwsid, county_served). Ignored
#                  if `lookup` is supplied.
# lookup         : a prebuilt lookup from build_coverage_lookup(); pass this when
#                  resolving many datasets so the crosswalk is parsed once.
# collapse_conus : FALSE -> sorted character vector of abbreviations (for #26's
#                  set diff). TRUE -> single registry string (for #35).
#
# returns: character vector (or a length-1 string when collapse_conus = TRUE).
#          attr(., "n_unknown") holds the count of pwsids not found in the
#          crosswalk.
###############################################################################
get_spatial_coverage <- function(data, crosswalk = NULL, lookup = NULL,
                                 collapse_conus = FALSE) {
  if (!"pwsid" %in% names(data)) {
    stop("get_spatial_coverage(): `data` has no `pwsid` column")
  }
  if (is.null(lookup)) {
    if (is.null(crosswalk)) {
      stop("get_spatial_coverage(): supply `crosswalk` or a prebuilt `lookup`")
    }
    lookup <- build_coverage_lookup(crosswalk)
  }

  pwsids <- unique(as.character(data$pwsid))
  states <- character(0)
  n_unknown <- 0L

  for (raw in pwsids) {
    members <- .split_pwsids(raw)
    hit <- character(0)
    for (pw in members) {
      if (!is.null(lookup[[pw]])) hit <- union(hit, lookup[[pw]])
    }
    if (length(hit) == 0) {
      n_unknown <- n_unknown + 1L
    } else {
      states <- union(states, hit)
    }
  }
  if (n_unknown > 0) states <- c(states, "UNKNOWN")

  out <- if (collapse_conus) format_coverage(states) else sort(unique(states))
  attr(out, "n_unknown") <- n_unknown
  out
}
