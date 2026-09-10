# NOTE ON BUILD CONTEXT:
# This worker sources the shared QA/QC helper at ./functions/checks.R, so the
# image must be built from the REPO ROOT (not this directory) so that file is
# in the build context:
#
#   docker build -f 1_downloaders/yearly/cartographic_boundaries_worker/Dockerfile \
#                -t cartographic-boundaries-worker .
#
# This is the first worker to depend on a shared functions/ file at build time.
# dwsrf_worker sources functions/check_env.R and will need the same treatment.
# Flagged on issue #22 for alignment with the pipeline reorg.

FROM rocker/rstudio:latest

RUN mkdir -p /home/epic/functions
WORKDIR /home/epic

# system libs: sf needs gdal/geos/proj/udunits; the fs package (pulled in by
# pointblank's export_report) needs libuv
RUN apt-get update && apt-get install -y \
    libudunits2-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libuv1-dev \
    && rm -rf /var/lib/apt/lists/*

# Install required R packages from CRAN
RUN install2.r --error \
    tidyverse \
    janitor \
    aws.s3 \
    sf \
    pointblank

# Copy the worker script and the shared QA/QC helper it sources
COPY 1_downloaders/yearly/cartographic_boundaries_worker/cartographic_boundaries_yearly.R /home/epic/
COPY functions/checks.R /home/epic/functions/checks.R

# Expose ports
EXPOSE 2000 2001

# Set the working directory and command to run the file
WORKDIR /home/epic
CMD R -e "source('./cartographic_boundaries_yearly.R')"
