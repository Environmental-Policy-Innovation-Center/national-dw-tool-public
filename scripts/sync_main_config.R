###############################################################################
# This script merges the local version of main_config.json with the S3 version
# since this file can be edited and pushed to AWS by multiple people.
# 
# The merged local copy unions the dataset_ids, merges the "triggers" array as
# sets, and flags fields with different values. A merge summary is output to
# show any merge conflicts. Once these conflicts are fixed, the new local
# version can be pushed to AWS using the printed aws command.
#
# Run from the repo root with command: Rscript scripts/sync_main_config.R
###############################################################################

library(jsonlite)

source("functions/s3_client.R")
set_s3_bucket("tech-team-data")

LOCAL_CONFIG_PATH <- "main_config.json"
S3_CONFIG_KEY <- "national-dw-tool/pipeline-config/main_config.json"

# jsonlite (simplifyVector = FALSE) parses {} as names = character(0) and []
# as names = NULL, so checking names() is how we tell object vs array apart
.is_json_object <- function(x) {
  is.list(x) && !is.null(names(x)) && all(names(x) != "")
}

.is_json_array_of_scalars <- function(x) {
  is.list(x) && length(x) > 0 && (is.null(names(x)) || any(names(x) == "")) &&
    all(vapply(x, function(el) !is.list(el) && length(el) == 1, logical(1)))
}

#' Recursively merge two parsed config trees
#' returns list(merged = ..., conflicts = list of path/local/remote for
#' anything that couldn't just be unioned)
.merge_config <- function(local, remote, path = character(0)) {
  conflicts <- list()

  if (.is_json_object(local) && .is_json_object(remote)) {
    all_names <- union(names(local), names(remote))
    # keep it named even when empty, otherwise an empty object roundtrips
    # as [] instead of {} once write_json gets to it
    merged <- setNames(list(), character(0))
    for (nm in all_names) {
      sub_path <- c(path, nm)
      if (!(nm %in% names(local))) {
        merged[[nm]] <- remote[[nm]]
      } else if (!(nm %in% names(remote))) {
        merged[[nm]] <- local[[nm]]
      } else {
        result <- .merge_config(local[[nm]], remote[[nm]], sub_path)
        merged[[nm]] <- result$merged
        conflicts <- c(conflicts, result$conflicts)
      }
    }
    return(list(merged = merged, conflicts = conflicts))
  }

  if (.is_json_array_of_scalars(local) && .is_json_array_of_scalars(remote)) {
    local_vals <- unlist(local, use.names = FALSE)
    remote_vals <- unlist(remote, use.names = FALSE)
    return(list(merged = as.list(union(local_vals, remote_vals)), conflicts = list()))
  }

  if (identical(local, remote)) {
    return(list(merged = local, conflicts = list()))
  }

  conflicts <- list(list(path = paste(path, collapse = "."), local_value = local, remote_value = remote))
  list(merged = local, conflicts = conflicts)
}

message(sprintf("Reading local config: %s", LOCAL_CONFIG_PATH))
local_config <- jsonlite::fromJSON(LOCAL_CONFIG_PATH, simplifyVector = FALSE)

message(sprintf("Grabbing remote config from S3: %s", S3_CONFIG_KEY))
tmp <- tempfile(fileext = ".json")
s3_client()$download_file(Bucket = s3_bucket(), Key = S3_CONFIG_KEY, Filename = tmp)
remote_config <- jsonlite::fromJSON(tmp, simplifyVector = FALSE)
unlink(tmp)

result <- .merge_config(local_config, remote_config)
merged_config <- result$merged
conflicts <- result$conflicts

message("Merge summary:")
only_local <- setdiff(names(local_config), names(remote_config))
only_remote <- setdiff(names(remote_config), names(local_config))
if (length(only_local) > 0) {
  message(sprintf("  only in local (kept): %s", paste(only_local, collapse = ", ")))
}
if (length(only_remote) > 0) {
  message(sprintf("  only in remote (added): %s", paste(only_remote, collapse = ", ")))
}
if (length(only_local) == 0 && length(only_remote) == 0) {
  message("  no dataset entries added or removed on either side")
}

if (length(conflicts) > 0) {
  message(sprintf("  %d conflict(s) - kept local copy, manually verify:", length(conflicts)))
  for (c in conflicts) {
    message(sprintf("    %s", c$path))
    message(sprintf("      local:  %s", jsonlite::toJSON(c$local_value, auto_unbox = TRUE)))
    message(sprintf("      remote: %s", jsonlite::toJSON(c$remote_value, auto_unbox = TRUE)))
  }
} else {
  message("  no conflicts, everything either merged cleanly or matched")
}

jsonlite::write_json(merged_config, LOCAL_CONFIG_PATH, auto_unbox = TRUE, pretty = TRUE, null = "null")
message(sprintf("Wrote merged config to %s.", LOCAL_CONFIG_PATH))
message("Once merge conflicts are fixed, push the merged local copy to S3 using:")
message(sprintf("  aws s3 cp %s s3://tech-team-data/%s", LOCAL_CONFIG_PATH, S3_CONFIG_KEY))
