.s3_env <- new.env(parent = emptyenv())

#' Get and lazily initialize the shared paws S3 client
#' s3 client handles IAM roles, keys, and tokens
s3_client <- function() {
  if (is.null(.s3_env$client)) {
    Sys.setenv("AWS_DEFAULT_REGION" = "us-east-1")
    .s3_env$client <- paws::s3(config = list(region = "us-east-1"))
  }
  .s3_env$client
}

#' Set the default S3 bucket used by the s3_* helpers
#' @param bucket Bucket name
set_s3_bucket <- function(bucket) {
  .s3_env$bucket <- bucket
}

#' Get the default S3 bucket
s3_bucket <- function() {
  if (is.null(.s3_env$bucket)) {
    stop("No default S3 bucket set. Call set_s3_bucket() first.")
  }
  .s3_env$bucket
}

#' Transforms S3 key into an S3 console URL that is viewable with AWS access.
#' @param key S3 object key
#' @param bucket S3 bucket
#' @return Full URL or the original value if no key is passed in
s3_public_url <- function(key, bucket = s3_bucket()) {
  if (is.null(key) || is.na(key) || key %in% c("", "N/A")) {
    return(key)
  }
  sprintf("https://us-east-1.console.aws.amazon.com/s3/object/%s?region=us-east-1&prefix=%s",
          bucket, key)
}

#' Similar to s3_public_url() but transforms a " | "-joined multi-key string
#' into a joined list of console URLs.
#' @param link_field A single S3 key, or several joined with " | "
#' @param bucket S3 bucket
#' @return Joined list of console URLs
s3_public_urls <- function(link_field, bucket = s3_bucket()) {
  if (is.null(link_field) || is.na(link_field) || link_field %in% c("", "N/A")) {
    return(link_field)
  }
  keys <- trimws(strsplit(link_field, "\\|")[[1]])
  paste(vapply(keys, s3_public_url, character(1), bucket = bucket), collapse = " | ")
}

#' Upload a large local file to S3 using multipart upload
#' @param path Local file path to upload
#' @param key S3 object key to write to
#' @param bucket S3 bucket
#' @param acl S3 ACL to apply (e.g. "public-read"). Default is NULL.
s3_write_large_file <- function(path, key, bucket = s3_bucket(), acl = NULL) {
  # Initialize the multipart upload
  init <- s3_client()$create_multipart_upload(Bucket = bucket, Key = key, ACL = acl)
  upload_id <- init$UploadId
  file_size <- file.info(path)$size
  chunk_size <- 20 * 1024 * 1024 # 20MB chunk size
  num_parts <- ceiling(file_size / chunk_size)
  
  message(sprintf("Starting chunked multipart upload for %s (%s parts)...", key, num_parts))
  con <- file(path, "rb")
  on.exit(close(con))
  parts <- list()
  tryCatch({
    for (i in seq_len(num_parts)) {
      chunk_data <- readBin(con, "raw", n = chunk_size)
      
      part_res <- s3_client()$upload_part(
        Bucket = bucket,
        Key = key,
        UploadId = upload_id,
        PartNumber = i,
        Body = chunk_data
      )
      
      parts[[i]] <- list(ETag = part_res$ETag, PartNumber = i)
      message(sprintf("Uploaded chunk %d/%d (%.1f MB)", i, num_parts, length(chunk_data)/(1024*1024)))
    }
    
    s3_client()$complete_multipart_upload(
      Bucket = bucket,
      Key = key,
      UploadId = upload_id,
      MultipartUpload = list(Parts = parts)
    )
    message("Multipart upload completed successfully.")

  }, error = function(e) {
    message("Error encountered during upload. Aborting multipart upload on S3...")
    tryCatch(
      s3_client()$abort_multipart_upload(Bucket = bucket, Key = key, UploadId = upload_id),
      error = function(abort_err) message(paste0("Failed to abort multipart upload: ", conditionMessage(abort_err)))
    )
    stop(e)
  })
}

#' Upload a local file to S3, redirecting large files (>50MB) to multipart upload
#' @param path Local file path to upload
#' @param key S3 object key to write to
#' @param bucket S3 bucket
#' @param acl S3 ACL to apply (e.g. "public-read"). Default is NULL.
s3_write_file <- function(path, key, bucket = s3_bucket(), acl = NULL) {
  file_size <- file.info(path)$size
  if (file_size > (50 * 1024 * 1024)) {
    invisible(s3_write_large_file(path, key, bucket, acl = acl))
  } else {
    invisible(s3_client()$put_object(
      Bucket = bucket,
      Key = key,
      Body = readBin(path, "raw", n = file_size),
      ACL = acl
    ))
  }
}

#' Read a CSV from S3 into a data frame
#' @param key S3 object key
#' @param bucket S3 bucket
#' @param coerce_character If TRUE, coerce all columns to character
s3_read_csv <- function(key, bucket = s3_bucket(), coerce_character = TRUE) {
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp))
  s3_client()$download_file(Bucket = bucket, Key = key, Filename = tmp)
  if (!file.exists(tmp)) {
    stop(sprintf("S3 file download failed for key: %s", key))
  }

  df <- read.csv(tmp, stringsAsFactors = FALSE)

  if (coerce_character) {
    df <- df %>% mutate(across(everything(), as.character))
  }
  df
}

#' Write a data frame to S3 as a CSV
#' @param df Data frame to write
#' @param key S3 object key to write to
#' @param bucket S3 bucket
#' @param acl S3 ACL to apply (e.g. "public-read"). Default is NULL.
s3_write_csv <- function(df, key, bucket = s3_bucket(), acl = NULL) {
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp))
  write.csv(df, tmp, row.names = FALSE)
  s3_write_file(tmp, key, bucket = bucket, acl = acl)
}

#' Read a GeoJSON from S3 into a sf object
#' @param key S3 object key
#' @param bucket S3 bucket
s3_read_geojson <- function(key, bucket = s3_bucket()) {
  tmp <- tempfile(fileext = ".geojson")
  on.exit(unlink(tmp))
  s3_client()$download_file(Bucket = bucket, Key = key, Filename = tmp)
  if (!file.exists(tmp)) {
    stop(sprintf("S3 file download failed for key: %s", key))
  }
  sf::st_read(tmp, quiet = TRUE)
}

#' Write an sf object to S3 as a GeoJSON
#' @param sf_obj sf object to write
#' @param key S3 object key to write to
#' @param bucket S3 bucket
#' @param acl S3 ACL to apply (e.g. "public-read"). Default is NULL.
s3_write_geojson <- function(sf_obj, key, bucket = s3_bucket(), acl = NULL) {
  tmp <- tempfile(fileext = ".geojson")
  on.exit(unlink(tmp))
  sf::st_write(sf_obj, tmp, driver = "GeoJSON", delete_dsn = TRUE, quiet = TRUE)
  s3_write_file(tmp, key, bucket = bucket, acl = acl)
}

#' Read an Excel spreadsheet from S3 into a data frame
#' @param key S3 object key
#' @param bucket S3 bucket
s3_read_xlsx <- function(key, bucket = s3_bucket()) {
  tmp <- tempfile(fileext = ".xlsx")
  on.exit(unlink(tmp))
  s3_client()$download_file(Bucket = bucket, Key = key, Filename = tmp)
  if (!file.exists(tmp)) {
    stop(sprintf("S3 file download failed for key: %s", key))
  }
  readxl::read_excel(tmp)
}

#' Read a GeoPackage from S3 into an sf object
#' @param key S3 object key
#' @param bucket S3 bucket
s3_read_gpkg <- function(key, bucket = s3_bucket()) {
  tmp <- tempfile(fileext = ".gpkg")
  on.exit(unlink(tmp))
  s3_client()$download_file(Bucket = bucket, Key = key, Filename = tmp)
  if (!file.exists(tmp)) {
    stop(sprintf("S3 file download failed for key: %s", key))
  }
  sf::st_read(tmp, quiet = TRUE)
}

#' Write an sf object to S3 as a GeoPackage
#' @param sf_obj sf object to write
#' @param key S3 object key to write to
#' @param bucket S3 bucket
#' @param acl S3 ACL to apply (e.g. "public-read"). Default is NULL.
s3_write_gpkg <- function(sf_obj, key, bucket = s3_bucket(), acl = NULL) {
  tmp <- tempfile(fileext = ".gpkg")
  on.exit(unlink(tmp))
  sf::st_write(sf_obj, tmp, driver = "GPKG", delete_dsn = TRUE, quiet = TRUE)
  s3_write_file(tmp, key, bucket = bucket, acl = acl)
}

#' Recursively rewrite S3 links in main_config to point to the development bucket
#' @param config Main config JSON
remap_to_dev <- function(config) {
  if (is.list(config)) {
    return(lapply(config, remap_to_dev))
  } else if (is.character(config) && length(config) == 1) {
    base_pattern <- "national-dw-tool/"
    dev_pattern  <- "national-dw-tool/development/"
    
    # Only rewrite strings matching our target project bucket prefix that don't already have the /development suffix
    is_target_s3   <- grepl(base_pattern, config, fixed = TRUE)
    already_remapped <- grepl(dev_pattern, config, fixed = TRUE)
    
    if (is_target_s3 && !already_remapped) {
      return(gsub(base_pattern, dev_pattern, config, fixed = TRUE))
    }
  }
  return(config)
}