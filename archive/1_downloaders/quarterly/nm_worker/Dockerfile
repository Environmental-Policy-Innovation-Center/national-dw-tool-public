FROM rocker/rstudio:latest

RUN mkdir -p /home/epic
WORKDIR /home/epic

# Install required R packages from CRAN
RUN install2.r --error \
    tidyverse \
    janitor \
    aws.s3 \
    aws.ec2metadata \
    rvest 


# Copy the R script
ADD nm_quarterly.R /home/epic/

# Expose ports
EXPOSE 2000 2001

# Set the working directory and command to run the file
WORKDIR /home/epic
CMD R -e "source('./nm_quarterly.R')"
