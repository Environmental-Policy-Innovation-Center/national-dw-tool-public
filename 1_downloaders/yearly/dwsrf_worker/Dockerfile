# NOTE ON BUILD CONTEXT: -- copied from cartographic_boundaries_worker, since 
# same docker considerations apply 
# This worker sources the shared QA/QC helper at ./functions/, so the
# image must be built from the REPO ROOT (not this directory) so that file is
# in the build context:
#
#   docker build -f 1_downloaders/yearly/cartographic_boundaries_worker/Dockerfile \
#                -t cartographic-boundaries-worker .
#
# This is the first worker to depend on a shared functions/ file at build time.
# dwsrf_worker sources functions/check_env.R and will need the same treatment.
# Flagged on issue #22 for alignment with the pipeline reorg.

# TODO: use tidyverse image which is lighter than rstudio
FROM rocker/rstudio:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN mkdir -p /home/epic/functions
WORKDIR /home/epic

# chromium system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium-browser \
    chromium-chromedriver \
    libnss3 \
    libnspr4 \
    libcups2 \
    libgbm1 \
    libasound2t64 \
    && rm -rf /var/lib/apt/lists/*

ENV CHROMOTE_CHROME=/usr/bin/chromium-browser
  
# Install required R packages from CRAN
RUN install2.r --error \
    chromote \
    aws.s3 \
    tidyverse \
    readxl

# Copy R scripts into home directory
COPY 1_downloaders/yearly/dwsrf_worker/dwsrf_yearly.R /home/epic/dwsrf_yearly.R
COPY functions/ /home/epic/functions/

# Expose ports
EXPOSE 2000 2001

# Set the working directory and command to run the file
WORKDIR /home/epic

# Set command to run file
CMD ["Rscript", "dwsrf_yearly.R"]