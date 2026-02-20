FROM rocker/rstudio:latest

RUN mkdir -p /home/epic
WORKDIR /home/epic

# required for sf dependencies 
RUN apt-get update && apt-get install -y \
    libudunits2-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# Install required R packages from CRAN
RUN install2.r --error \
    tidyverse \
    janitor \
    aws.s3 \
    aws.ec2metadata \
    sf \
    units \
    arcpullr \
    httr \
    tigris


# Copy the R script
ADD sabs_yearly.R /home/epic/

# Expose ports
EXPOSE 2000 2001

# Set the working directory and command to run the file
WORKDIR /home/epic
CMD R -e "source('./sabs_yearly.R')"
