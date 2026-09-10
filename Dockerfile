FROM rocker/geospatial:latest

LABEL org.opencontainers.image.title="national-dw-tool-main-runner"
LABEL org.opencontainers.image.description="ECS task image for the National Drinking Water Tool main runner"

ENV AWS_DEFAULT_REGION=us-east-1
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /home/epic

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    libnss3 \
    libnspr4 \
    libcups2 \
    libgbm1 \
    libasound2t64 \
    # Downloads Google's statically built chrome package
    && wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y --no-install-recommends /tmp/chrome.deb \
    && rm -f /tmp/chrome.deb \
    && rm -rf /var/lib/apt/lists/*

ENV CHROMOTE_CHROME=/usr/bin/google-chrome-stable

# rocker/geospatial includes the heavy geospatial stack; these packages cover
# the main runner, startup-sourced pipeline files, and registry helpers.
RUN install2.r --error --skipinstalled \
    areal \
    argparse \
    arcpullr \
    chromote \
    curl \
    data.table \
    googlesheets4 \
    httr \
    httr2 \
    janitor \
    jsonlite \
    later \
    openxlsx \
    paws \
    pointblank \
    readxl \
    rvest \
    tidycensus \
    tigris \
    tidyverse \
    websocket

COPY main_runner.R ./
COPY functions/ functions/
COPY pipelines/ pipelines/

# ECS task definitions can override the command, for example:
# ["--run-pipeline", "raw_huc12", "--update-registries", "TRUE", "--dev", "TRUE"]
ENTRYPOINT ["Rscript", "main_runner.R"]
CMD ["--help"]
