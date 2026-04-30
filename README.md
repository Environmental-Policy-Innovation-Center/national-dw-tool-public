![](www/epic-logo-transparent.png)

# EPIC's Drinking Water Explorer Tool Data Pipeline

Hey! You're viewing the back-end R based data pipeline. Looking for the [web-application](https://www.policyinnovation.org/drinking-water-explorer-tool) or [front end](https://github.com/Environmental-Policy-Innovation-Center/water-data-tool) repository?

This repository maintains the code and data powering the backend of the National Drinking Water Tool. The data pipeline includes:

-   **Data Downloading** - organized in the `1_downloaders` folder by worker frequency, this code collects and cleans datasets for the tool. Each folder contains a .R file that contains the code to download the dataset, along with a docker file for the AWS instance. These files are configured to run as scheduled tasks on our AWS cluster.

-   **Data Summarizing** - located in `2_summarize_data.Rmd`, this file collects datasets that require manual updates, performs any crosswalking (relating census geographies to EPA's Service Area Boundaries), and builds summary RDS lists.

-   **UI Prep** - located in `3_ui_prep.R`, this file runs quality checks on variables used in the tool and if they pass our checks, adds these to the staged environment.

In addition to these three steps, there are a handful of other files that support this pipeline:

-   **functions** - this folder holds a variety of functions that are used for crosswalking, updating the task manager, running qual checks, etc. They are sourced in when necessary, but do not need to be added to any workers.

-   **figures** - this folder contains files that produce figures for blog posts.

-   **data** - this folder holds any small data files used in this project.
