# TODO: use tidyverse image which is lighter than rstudio
FROM rocker/rstudio:latest

ENV DEBIAN_FRONTEND=noninteractive

ENV CHROMOTE_CHROME=/usr/bin/chromium

RUN mkdir -p /home/epic
WORKDIR /home/epic

# chromium system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixnum1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    && rm -rf /var/lib/apt/lists/*
  
# Install required R packages from CRAN
RUN install2.r --error \
    chromote \
    aws.s3 \
    readxl

# Copy R scripts into home directory
COPY dwsrf_yearly.R /home/epic/
COPY functions/ /home/epic/functions/
  
# Set command to run file
CMD ["Rscript", "dwsrf_yearly.R"]