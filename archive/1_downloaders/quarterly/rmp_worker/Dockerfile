FROM rocker/rstudio:latest

RUN mkdir -p /home/epic
WORKDIR /home/epic

# required for sf dependencies 
RUN apt-get update && apt-get install -y \
    libudunits2-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libsqlite3-dev \
    libcairo2-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*
    
# Install required R packages from CRAN
RUN install2.r --error \
    tidyverse \
    janitor \
    aws.s3 \
    s2 \
    sf \
    aws.ec2metadata \
    arcpullr \
    tigris \
    nhdplusTools 
     
# Copy the R script
ADD rmp_quarterly.R /home/epic/

# Expose ports
EXPOSE 2000 2001

# Set the working directory and command to run the file
WORKDIR /home/epic
CMD R -e "source('./rmp_quarterly.R')"
