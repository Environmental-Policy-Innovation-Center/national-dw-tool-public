FROM rocker/rstudio:latest

RUN mkdir -p /home/epic
WORKDIR /home/epic

# Install required R packages from CRAN
RUN install2.r --error \
    tidyverse \
    janitor \
    aws.s3 \
    aws.ec2metadata \
    httr \
    jsonlite


# Copy the R script
ADD mo_daily.R /home/epic/

# Expose ports
EXPOSE 2000 2001

# Set the working directory and command to run the file
WORKDIR /home/epic
CMD R -e "source('./mo_daily.R')"
