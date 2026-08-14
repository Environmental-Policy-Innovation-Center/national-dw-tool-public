FROM rocker/geospatial:latest

LABEL org.opencontainers.image.title="national-dw-tool-main-runner"
LABEL org.opencontainers.image.description="ECS task image for the National Drinking Water Tool main runner"

ENV AWS_DEFAULT_REGION=us-east-1

WORKDIR /home/epic

# rocker/geospatial includes the heavy geospatial stack; these packages cover
# the main runner, startup-sourced pipeline files, and registry helpers.
RUN install2.r --error --skipinstalled \
    areal \
    argparse \
    arcpullr \
    googlesheets4 \
    httr \
    janitor \
    jsonlite \
    openxlsx \
    paws \
    pointblank \
    readxl \
    tidycensus \
    tigris \
    tidyverse

COPY main_runner.R ./
COPY functions/ functions/
COPY pipelines/ pipelines/

# ECS task definitions can override the command, for example:
# ["--run-pipeline", "raw_huc12", "--update-registries", "TRUE", "--dev", "TRUE"]
ENTRYPOINT ["Rscript", "main_runner.R"]
CMD ["--help"]
