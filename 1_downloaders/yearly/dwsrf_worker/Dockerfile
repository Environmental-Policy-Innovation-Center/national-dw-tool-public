# TODO: use tidyverse image which is lighter than rstudio
FROM rocker/rstudio:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN mkdir -p /home/epic
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
    readxl

# Copy R scripts into home directory
COPY 1_downloaders/yearly/dwsrf_worker/dwsrf_yearly.R /home/epic/dwsrf_yearly.R
COPY functions/ /home/epic/functions/
  
# Set command to run file
CMD ["Rscript", "dwsrf_yearly.R"]