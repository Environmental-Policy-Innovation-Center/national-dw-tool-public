# Figure for looking at the years we have BWN data for various states
library(tidyverse)
library(aws.s3)
library(ComplexUpset)
library(UpSetR)
library(ggplot2)

# reading in national bwn: 
national_bwn <- s3read_using(readRDS,
                             object = "s3://tech-team-data/national-dw-tool/clean/national/national_bwn.RData")


# recreating 
# pulling in task manager for updating relevant sections: 
task_manager_enviro <- aws.s3::s3read_using(read.csv, 
                                            object = "s3://tech-team-data/national-dw-tool/task_manager_data_summary.csv")

bwns <- task_manager_enviro %>%
  filter(grepl("^[a-z]{2}_(bwn|bwa)(_[^\\s]+)?(\\.csv)?$", task_manager_enviro$dataset)) %>%
  filter(clean_link != "WORKER FAILED - SEE LOGS")

bwn_list <- list() 
bwn_all_summary <- data.frame()
name_vector <- c()
for(i in 1:nrow(bwns)){
  # grabbing all of the bwn data we have and appending to a list 
  bwn_clean_link_i <- bwns$clean_link[i]
  bwn_i <- aws.s3::s3read_using(read.csv, 
                                object = bwn_clean_link_i)
  print(paste0("On State: ", unique(bwn_i$state)))
  
  # addint it to the list and updating the name based on the state 
  bwn_list[[i]] <- bwn_i
  name_vector <- c(name_vector, bwns$dataset[i])
  
  # I also want all of them summarized by standardized columns: 
  bwn_all_summary_i <- bwn_i %>%
    select(pwsid, date_issued, date_lifted, epic_date_lifted_flag, 
           date_epic_captured_advisory, type, state, date_worker_last_ran)
  bwn_all_summary <- bind_rows(bwn_all_summary, bwn_all_summary_i)
}


# adding the summary data to the end of the file: 
bwn_list[[length(bwn_list) + 1]] <- bwn_all_summary %>%
  mutate(date_lifted = case_when(is.na(date_lifted) ~ "Open", 
                                 TRUE ~ date_lifted))

# fixing names
names(bwn_list) <- c(name_vector, "national_bwn_summary")

# shoud use bwn_all_summary, which has the standardized columns
bwn_summary <- bwn_all_summary %>%
  unique() %>%
  mutate(date_issued = as.Date(date_issued)) %>%
  filter(date_issued <= Sys.Date()) %>%
  mutate(state_simple = case_when(grepl("Louisiana", state) ~ "Louisiana", 
                                  TRUE ~ state)) %>%
  group_by(state_simple) %>%
  summarize(min_year = as.Date(min(date_issued, na.rm = T)), 
            max_year = as.Date(max(date_issued, na.rm = T))) %>%
            # total_records = n())%>%
  # the FOIA'd dataset contains records through "2026-04-30"
  mutate(max_year = case_when(grepl("Texas", state_simple) ~ as.Date("2026-04-30"), 
                        TRUE ~ max_year), 
         # Rhode island does not reliably report date issued, but they report 
         # the date the advisory was lifted, so I'm basing this off that
         max_year = case_when(grepl("Rhode Island", state_simple) ~ as.Date("2023-09-27"), 
                              TRUE ~ max_year))

# ---- dumbbell plot of date_issued coverage by state ----
# one dot at min_year, one at max_year, connected by a line, per state
ggplot(bwn_summary, aes(y = fct_rev(state_simple))) +
  geom_segment(aes(x = min_year, xend = max_year, yend = state_simple),
               color = "grey70", linewidth = 1) +
  geom_point(aes(x = min_year), color = "steelblue", size = 3) +
  geom_point(aes(x = max_year), color = "firebrick", size = 3) +
  scale_x_date(date_breaks = "1 year", date_labels = "%Y") +
  labs(x = "Date Issued", y = "", title = "Boil Water Notices - Date Coverage by State") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5))

# ---- map of date coverage by state ----
# fill = whether we have data at all; text label = the min-max year range
library(usmap)

# all 50 states, so states with no data still show up (filled grey, no label)
all_states <- tibble(state = state.name)

coverage_map_data <- all_states %>%
  left_join(bwn_summary %>%
              transmute(state = state_simple,
                        has_data = TRUE,
                        year_range = paste0(format(min_year, "%Y"), "-", format(max_year, "%Y"))),
            by = "state") %>%
  mutate(has_data = replace_na(has_data, FALSE))

# base R's state.center gives lon/lat centroids per state, in usmap's expected format
state_centers <- tibble(state = state.name, lon = state.center$x, lat = state.center$y)

label_data <- coverage_map_data %>%
  filter(has_data) %>%
  left_join(state_centers, by = "state") %>%
  filter(!is.na(lon)) %>%
  usmap_transform(input_names = c("lon", "lat"))

plot_usmap(regions = "states", data = coverage_map_data, values = "has_data") +
  scale_fill_manual(values = c(`TRUE` = "#b3e59f", `FALSE` = "grey90"),
                     name = "Has Data", labels = c("No", "Yes")) +
  geom_sf_text(data = label_data, aes(label = year_range), size = 2.5) +
  labs(title = "Boil Water Notices - States with Data & Year Range") +
  theme(legend.position = "right")

