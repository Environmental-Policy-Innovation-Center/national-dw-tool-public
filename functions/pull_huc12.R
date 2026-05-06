source("./functions/check_env.R")
ENV <- check_env()

###############################################################################
# Pull HUC12 codes from WBD GDB
# GDB sourced from USGS: https://www.usgs.gov/national-hydrography/access-national-hydrography-products
###############################################################################

library(httr)
library(aws.s3)

# Download National WBD Geodatabase zip file.
download_url <- "https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/National/GDB/WBD_National_GDB.zip"
tmp_zip <- tempfile(fileext = ".zip")
on.exit(unlink(tmp_zip))
# Use a longer timeout because the file is ~1GB
options(timeout = max(600, getOption("timeout"))) 
download.file(download_url, destfile = tmp_zip, mode = "wb", quiet = FALSE)

# Upload zip to S3
# This step took a long time
wbd_s3_link <- s3_path(file.path("wbd_national_gdb_raw.zip", file_name), ENV)
put_object(
  file = tmp_zip,
  object = wbd_s3_link,
  multipart = TRUE
)

# Unzip locally downloaded zip file
ex_dir <- tempdir() # A temporary directory for unzipped files
unzip(tmp_zip, exdir = ex_dir)

# Find the .gdb folder inside the unzipped content
gdb_path <- list.dirs(ex_dir, full.names = TRUE, recursive = TRUE)
gdb_path <- gdb_path[grep("\\.gdb$", gdb_path)][1] # Take the first GDB found

# Pull HUC12 geometry
huc12_df <- st_read(
  gdb_path, 
  query = "SELECT HUC12 FROM WBDHU12",
  quiet = TRUE
)
huc12_codes <- huc12_df$HUC12
length(huc12_codes) # 103068 codes

# This pulls all attributes
# full_huc12_df <- st_read(gdb_path, 
#                         layer = "WBDHU12", 
#                         query = "SELECT * FROM WBDHU12")

# Sanity check - Plot the boundaries
plot(st_geometry(huc12_df), lwd = 0.5, border = 'blue')
