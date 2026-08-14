# Let's figure out better methods for handling small systems: ##################
library(sf)
library(tidycensus)
library(tidyverse)
library(tigris)
library(leaflet)

# cache tigris files 
options(tigris_use_cache = TRUE)
sf_use_s2(F)
options(scipen = 999999)

# palette for mappin' 
cont_palette <- colorRampPalette(c("#172f60","#4ea324"))

# grabbing datasets:############################################################
epa_sabs_geoms <- aws.s3::s3read_using(st_read,
                                       object = "s3://tech-team-data/national-dw-tool/test-staged/epa_sabs_geoms.geojson") %>%
  janitor::clean_names() 

# for filtering for CWS - which already has an intersection 
epa_sabs_pwsids <- aws.s3::s3read_using(read.csv, 
                                        object = "s3://tech-team-data/national-dw-tool/raw/national/water-system/sabs_pwsid_names.csv")

# for comparing epa pop served with our interpolated number: 
socio <- aws.s3::s3read_using(readRDS, 
                              object = "s3://tech-team-data/national-dw-tool/clean/national/national_socioeconomic.RData")
xwalk <- socio$epa_sabs_xwalk

# for comparing with SDWIS pop: 
water_sys <- aws.s3::s3read_using(readRDS, 
                                  object = "s3://tech-team-data/national-dw-tool/clean/national/national_water_system.RData")
sdwis_pop <- water_sys$epa_sabs
################################################################################
# merging to filter epa_sabs_geoms such that it's not running an intersection 
# on the whole dataset
epa_sabs <- merge(epa_sabs_pwsids, epa_sabs_geoms, by = "pwsid") %>% 
  st_as_sf()

# states to loop through- removing ones that intersect multiple states: 
state_loop <- unique(epa_sabs$states_intersect)
state_loop_tidy <- state_loop[!grepl(",", state_loop)]

# summary to bind to: 
intersected_summary <- data.frame() 
for(i in 1:length(state_loop_tidy)){
  state_i <- state_loop_tidy[i]
  print(paste0("Working on: ", state_i))
  # grabbing state blocks: 
  state_blocks <- tigris::blocks(state = state_i,
                                 year = 2020) %>%
    st_transform(., crs = st_crs(epa_sabs_geoms))
  
  # running intersection
  intersected_blocks <- st_intersection(epa_sabs %>% 
                                          # this needs to be a grepl to catch 
                                          # multiple overlaps
                                          filter(grepl(state_i, states_intersect)),
                                        state_blocks)
  # grabbing num intersected 
  intersected_summary_i <- intersected_blocks %>% 
    as.data.frame() %>% 
    group_by(pwsid) %>%
    summarize(num_blocks_intersected = length(unique(GEOID20)))
  
  # binding: 
  intersected_summary <<- rbind(intersected_summary, intersected_summary_i)
}

# handling sabs that overlap with multiple states: 
summary_final <- intersected_summary %>%
  group_by(pwsid) %>%
  summarize(total_intersected_blocks = sum(num_blocks_intersected))

# write.csv(summary_final, "./data/sab_block_intersection.csv")
summary_final <- read.csv("./data/sab_block_intersection.csv") %>%
  select(-X)

# finding total number of SABs within various group cutoffs
cutoff_summaries <- summary_final %>%
  mutate(intersect_group = cut(total_intersected_blocks, 
                               breaks = c(0, 1, 5, 10, 15, 20, 30, 50, 100000))) %>%
  group_by(intersect_group) %>%
  summarize(total_pwsids = length(unique(pwsid))) %>%
  # grabbing % of total and running percentage based on cutoff 
  mutate(pct_of_total = 100*(total_pwsids/length(unique(epa_sabs_pwsids$pwsid))), 
         running_pct = cumsum(pct_of_total))
cutoff_summaries



# checking out super small ones: ###############################################
less_30_blocks <- summary_final %>% 
  filter(total_intersected_blocks < 30)

# checking the distribution:
ggplot(less_30_blocks, 
       aes(x = total_intersected_blocks)) + 
  geom_histogram()

# what do these look like?
less_30_blocks_geoms <- epa_sabs %>% 
  filter(pwsid %in% less_30_blocks$pwsid)

# I also want the block intersection number
less_30_blocks_geoms_f <- merge(less_30_blocks_geoms, 
                                less_30_blocks, by = "pwsid")
intersect_pal <- colorNumeric(
  palette = cont_palette(5),
  domain = less_30_blocks_geoms_f$total_intersected_blocks)

leaflet() %>%
  addProviderTiles(providers$CartoDB.Positron, group = "Positron (gray)") %>%
  addPolygons(data = less_30_blocks_geoms_f, 
              fillColor = ~intersect_pal(total_intersected_blocks),
              fillOpacity = 0.8,
              color = "black",
              weight = 2, 
              smoothFactor = 0, 
              label = paste0(less_30_blocks_geoms_f$pwsid, 
                             "; Blocks Intersected : ", 
                             less_30_blocks_geoms_f$total_intersected_blocks)) %>%
  addLegend("bottomright",
            pal = intersect_pal,
            values = less_30_blocks_geoms_f$total_intersected_blocks,
            title = "# Intersecting Blocks",
            opacity = 1)

# curious about NC blocks and sabs overlaps
# nc_sabs <- less_30_blocks_geoms_f %>% filter(grepl("NC", states_intersect))
# nc_blocks <- tigris::blocks(state = "NC",
#                             year = 2020) %>%
#   st_transform(., crs = st_crs(epa_sabs_geoms))
# 
# leaflet() %>%
#   addProviderTiles(providers$CartoDB.Positron, group = "Positron (gray)") %>%
#   addPolygons(data = nc_blocks, 
#               fillColor = "grey",
#               fillOpacity = 0.8,
#               color = "black",
#               weight = 2, 
#               smoothFactor = 0) %>%
#   addPolygons(data = nc_sabs, 
#               fillColor = "red",
#               fillOpacity = 0.8,
#               color = "black",
#               weight = 2, 
#               smoothFactor = 0) 

# look at relationships between block overlap & pop estimates  ################
xwalk_pop <- xwalk %>% select(pwsid, total_pop, tier_crosswalk)
# merging with other dataset
pop_comps <- merge(summary_final, sdwis_pop) %>%
  select(pwsid, epic_states_intersect, 
         population_served_count, total_intersected_blocks) %>%
  rename(sdwis_pop = population_served_count) %>%
  left_join(., xwalk_pop, by = "pwsid") %>%
  rename(xwalk_pop = total_pop) %>%
  mutate(sdwis_pct_diff = abs((sdwis_pop - xwalk_pop)) / ((sdwis_pop + xwalk_pop) / 2)) %>%
  mutate(sdwis_xwalk_pct_diff = (sdwis_pop - xwalk_pop) / (sdwis_pop)) %>%
  mutate(intersect_group = cut(total_intersected_blocks, 
                               breaks = c(0, 1, 5, 10, 15, 20, 30, 50, 100, 
                                          200, 500, 10000, 50000)))

# filtering certain xwalk groups, since some of them are just the sdwis pop: 
pop_comps_filt <- pop_comps %>%
  # these are sabs that intersected with multiple states, or MP
  filter(!(tier_crosswalk %in% c("tier_2_sdwispop", "tier_1, tier_2_xwalk", 
                                 "tier_1, tier_1", NA)))

ggplot(pop_comps_filt, aes(x = sdwis_pct_diff*100, 
                           # y = xwalk_pop, 
                           fill = tier_crosswalk)) + 
  geom_histogram(alpha = 0.6) + 
  # geom_point(alpha = 0.2) + 
  # geom_smooth(method = "lm") + 
  theme_bw() + 
  facet_wrap(~intersect_group, scales = "free_y")  + 
  geom_vline(xintercept = 0, lty = "dashed") + 
  labs(x = "SDWIS Population % Differece", 
       y = "Number of Water Systems")
  # geom_abline(intercept = 0, slope = 1, color = "black", linetype = "dashed")


# --- 
# notes - plot faceted by the number of blocks the SAB intersects with 
# left of y = 0 means our crosswalk population is higher
# right of y = 0 means SDWIS population is higher 
# --- 
# there are 91 SABs with sdiws pop == 0; creating a sdwis_pop_diff of -2
# seems like things start to converge after an overlap of 5 blocks
# okay what would this mean as a cutoff point?
pop_comps %>%
  group_by(intersect_group) %>%
  summarize(total_ws = length(unique(pwsid)), 
            mean_sdwis_pct_diff = mean(sdwis_pct_diff, na.rm = T), 
            mean_diff_sdwis_xwalk = mean(sdwis_pop - xwalk_pop, na.rm = T), 
            median_diff_sdwis_xwalk = median(sdwis_pop - xwalk_pop, na.rm = T), 
            total_sdwis_pop = sum(sdwis_pop, na.rm = T),
            total_xwalk_pop = sum(xwalk_pop, na.rm = T)) %>%
  # grabbing % of total and running percentage based on cutoff 
  mutate(pct_of_total_ws = 100*(total_ws/length(unique(epa_sabs_pwsids$pwsid))), 
         running_pct_ws = cumsum(pct_of_total_ws), 
         pct_sdwis_pop = 100*(total_sdwis_pop/sum(total_sdwis_pop)), 
         running_pct_sdwis_pop = cumsum(pct_sdwis_pop),
         pct_xwalk_pop = 100*(total_xwalk_pop/sum(total_xwalk_pop)), 
         running_pct_xwalk_pop = cumsum(pct_xwalk_pop)) %>%
  relocate(pct_of_total_ws:running_pct_ws, .after = total_ws)


# want to create a different viz for this: 
comps_long <- pop_comps_filt %>%
  select(pwsid, intersect_group, sdwis_pop, xwalk_pop) %>%
  pivot_longer(., cols = c("sdwis_pop", "xwalk_pop"))

ggplot(comps_long, aes(x = value, 
                       # y = xwalk_pop, 
                       fill = name)) + 
  facet_wrap(~intersect_group, scales = "free")  +
  geom_histogram(alpha = 0.6, binwidth = 1000) + 
  theme_bw() + 
  labs(x = "Estimated Population", 
       y = "Number of Water Systems")


# there is one creating lots of skew in (0,1]
# MA4076004
# sdwis_pop
# 105643.0000

# what about just straight up difference?
diffs <- pop_comps_filt %>%
  mutate(sdwis_xwalk_diff = sdwis_pop - xwalk_pop, na.rm = T) %>%
  filter(sdwis_pop != 0)

ggplot(diffs, aes(x = sdwis_xwalk_diff)) + 
  geom_histogram(alpha = 0.5) + 
  facet_wrap(~intersect_group, scales = "free") + 
  geom_vline(xintercept = 0, lty = "dashed") + 
  theme_bw() + 
  labs(x = "Estimated Population Difference: SDWIS - crosswalk", 
       y = "Number of Water Systems")



# Revisiting on Aug 26th #######################################################
# I think I should filter some of these out: 41,365
pop_comps_filt <- pop_comps %>%
  # these are where our estimate pop was just identical to the SDWIS pop, because 
  # the block parcel crosswalk was > SDWIS reported pop
  filter(!grepl("sdwispop", tier_crosswalk)) %>%
  # these are ones we couldn't intersect 
  filter(!grepl("tier_3", tier_crosswalk)) %>%
  # this is MP
  filter(!is.na(tier_crosswalk)) %>%
  filter(sdwis_pop != 0)

# grouping by block intersection bin: 
pop_comps_filt %>%
  group_by(intersect_group) %>%
  summarize(total_ws = length(unique(pwsid)), 
            mean_sdwis_pct_diff = mean(sdwis_pct_diff*100, na.rm = T), 
            mean_diff_sdwis_xwalk = mean(sdwis_pop - xwalk_pop, na.rm = T), 
            median_diff_sdwis_xwalk = median(sdwis_pop - xwalk_pop, na.rm = T), 
            total_sdwis_pop = sum(sdwis_pop, na.rm = T),
            total_xwalk_pop = sum(xwalk_pop, na.rm = T)) %>%
  # grabbing % of total and running percentage based on cutoff 
  mutate(pct_of_total_ws = 100*(total_ws/length(unique(pop_comps_filt$pwsid))), 
         running_pct_ws = cumsum(pct_of_total_ws), 
         pct_sdwis_pop = 100*(total_sdwis_pop/sum(total_sdwis_pop)), 
         running_pct_sdwis_pop = cumsum(pct_sdwis_pop),
         pct_xwalk_pop = 100*(total_xwalk_pop/sum(total_xwalk_pop)), 
         running_pct_xwalk_pop = cumsum(pct_xwalk_pop)) %>%
  relocate(pct_of_total_ws:running_pct_ws, .after = total_ws) %>%
  # I actually don't think the totals are really adding that much 
  select(-c(total_sdwis_pop, total_xwalk_pop)) %>%
  rename("Overlapping Blocks" = intersect_group, 
         "# Water Systems" = total_ws, 
         "% of Water Systems" = pct_of_total_ws, 
         "Running % Water Systems" = running_pct_ws, 
         "Mean % Difference - Pop" = mean_sdwis_pct_diff, 
         "Mean SDWIS - Xwalk Pop" = mean_diff_sdwis_xwalk, 
         "Median SDWIS - Xwalk Pop" = median_diff_sdwis_xwalk, 
         "% SDWIS Pop" = pct_sdwis_pop, 
         "Running % SDWIS Pop" = running_pct_sdwis_pop, 
         "% Xwalk Pop" = pct_xwalk_pop, 
         "Running % Xwalk Pop" = running_pct_xwalk_pop)

# looking at % difference
ggplot(pop_comps_filt, aes(x = sdwis_pct_diff)) + 
  geom_histogram(alpha = 0.6) + 
  # geom_point(alpha = 0.2) + 
  # geom_smooth(method = "lm") + 
  theme_bw() + 
  facet_wrap(~intersect_group, scales = "free_y")  + 
  geom_vline(xintercept = 0, lty = "dashed") + 
  labs(x = "SDWIS Population % Differece", 
       y = "Number of Water Systems")

# looking at raw difference - also removing zero SDWIS pops 
diffs <- pop_comps_filt %>%
  mutate(sdwis_xwalk_diff = sdwis_pop - xwalk_pop, na.rm = T) %>%
  filter(sdwis_pop != 0)

ggplot(diffs, aes(x = sdwis_xwalk_diff)) + 
  geom_histogram(alpha = 0.5) + 
  facet_wrap(~intersect_group, scales = "free") + 
  geom_vline(xintercept = 0, lty = "dashed") + 
  theme_bw() + 
  labs(x = "Estimated Population Difference: SDWIS - crosswalk", 
       y = "Number of Water Systems")

# checking out this big one: 
# mapview(epa_sabs %>% filter(pwsid == "MA4076004"))

# Checking out states ##########################################################
state_df <- pop_comps_filt 
  # filter(grepl("KS", epic_states_intersect))

mean(state_df$sdwis_pct_diff, na.rm = T)*100
# national - 63.35768
# TX - 55.80047
# CA - 66.50756
# KS - 26.91853
# KY - 28.15147

median(state_df$sdwis_pct_diff, na.rm = T)*100
# national - 44.36563
# TX - 39.66636
# CA - 50.16764
# KS - 9.216474
# KY - 18.00864


mean(state_df$sdwis_pop - state_df$xwalk_pop, na.rm = T)
# national - 666.0371
# TX - 716.2411
# CA - -306.7636
# KS - 192.9043
# KY - 237.1335

median(state_df$sdwis_pop - state_df$xwalk_pop, na.rm = T)
# national - 95.88836
# TX - 183.2933
# CA - 107.4289
# KS - 3.545332
# KY - 359.5801


# grouping by block intersection bin: 
state_df %>%
  group_by(intersect_group) %>%
  summarize(total_ws = length(unique(pwsid)), 
            mean_sdwis_pct_diff = mean(sdwis_pct_diff*100, na.rm = T), 
            mean_diff_sdwis_xwalk = mean(sdwis_pop - xwalk_pop, na.rm = T), 
            median_diff_sdwis_xwalk = median(sdwis_pop - xwalk_pop, na.rm = T), 
            total_sdwis_pop = sum(sdwis_pop, na.rm = T),
            total_xwalk_pop = sum(xwalk_pop, na.rm = T)) %>%
  # grabbing % of total and running percentage based on cutoff 
  mutate(pct_of_total_ws = 100*(total_ws/length(unique(state_df$pwsid))), 
         running_pct_ws = cumsum(pct_of_total_ws), 
         pct_sdwis_pop = 100*(total_sdwis_pop/sum(total_sdwis_pop)), 
         running_pct_sdwis_pop = cumsum(pct_sdwis_pop),
         pct_xwalk_pop = 100*(total_xwalk_pop/sum(total_xwalk_pop)), 
         running_pct_xwalk_pop = cumsum(pct_xwalk_pop)) %>%
  relocate(pct_of_total_ws:running_pct_ws, .after = total_ws) %>%
  # I actually don't think the totals are really adding that much 
  select(-c(total_sdwis_pop, total_xwalk_pop)) %>%
  rename("Overlapping Blocks" = intersect_group, 
         "# Water Systems" = total_ws, 
         "% of Water Systems" = pct_of_total_ws, 
         "Running % Water Systems" = running_pct_ws, 
         "Mean % Difference - Pop" = mean_sdwis_pct_diff, 
         "Mean SDWIS - Xwalk Pop" = mean_diff_sdwis_xwalk, 
         "Median SDWIS - Xwalk Pop" = median_diff_sdwis_xwalk, 
         "% SDWIS Pop" = pct_sdwis_pop, 
         "Running % SDWIS Pop" = running_pct_sdwis_pop, 
         "% Xwalk Pop" = pct_xwalk_pop, 
         "Running % Xwalk Pop" = running_pct_xwalk_pop)

# looking at % difference
ggplot(state_df, aes(x = sdwis_pct_diff*100)) + 
  geom_histogram(alpha = 0.6) + 
  # geom_point(alpha = 0.2) + 
  # geom_smooth(method = "lm") + 
  theme_bw() + 
  facet_wrap(~intersect_group)  + 
  geom_vline(xintercept = 0, lty = "dashed") + 
  labs(x = "SDWIS Population % Differece", 
       y = "Number of Water Systems")

ggplot(state_df, aes(x = sdwis_pct_diff*100)) + 
  geom_histogram(alpha = 0.6) + 
  # geom_point(alpha = 0.2) + 
  # geom_smooth(method = "lm") + 
  theme_bw() + 
  # facet_wrap(~intersect_group)  + 
  geom_vline(xintercept = 0, lty = "dashed") + 
  labs(x = "SDWIS Population % Difference", 
       y = "Number of Water Systems")  + 
  geom_vline(xintercept=mean(state_df$sdwis_pct_diff*100, na.rm = T), 
             color="red", 
             lty ="dotted") + 
  geom_vline(xintercept=median(state_df$sdwis_pct_diff*100, na.rm = T), color="blue", 
             lty ="dotted")


# looking at raw difference - also removing zero SDWIS pops 
diffs <- state_df %>%
  mutate(sdwis_xwalk_diff = sdwis_pop - xwalk_pop) 

ggplot(diffs, aes(x = sdwis_xwalk_diff)) + 
  geom_histogram(alpha = 0.5) + 
  facet_wrap(~intersect_group, scales = "free") + 
  geom_vline(xintercept = 0, lty = "dashed") + 
  theme_bw() + 
  labs(x = "Estimated Population Difference: SDWIS - crosswalk", 
       y = "Number of Water Systems")

ggplot(diffs, aes(x = sdwis_xwalk_diff)) + 
  geom_histogram(alpha = 0.6) + 
  # geom_point(alpha = 0.2) + 
  # geom_smooth(method = "lm") + 
  theme_bw() + 
  # facet_wrap(~intersect_group)  + 
  geom_vline(xintercept = 0, lty = "dashed") + 
  labs(x = "Estimated Population Difference: SDWIS - crosswalk", 
       y = "Number of Water Systems")  + 
  geom_vline(xintercept=mean(diffs$sdwis_xwalk_diff, na.rm = T), color="red", 
             lty = "solid") + 
  geom_vline(xintercept=median(diffs$sdwis_xwalk_diff, na.rm = T), color="blue", 
             lty = "solid")


# summary views: 
state_test <- pop_comps_filt %>%
  # filter(grepl("TX|KS|KY|CA", epic_states_intersect)) %>%
  filter(nchar(epic_states_intersect)< 3)
ggplot(state_test, aes(x = sdwis_pct_diff*100)) + 
  geom_histogram() + 
  theme_bw() + 
  facet_wrap(~epic_states_intersect, scales = "free_y") + 
  labs(y = "Number of Water Systems",
       x = "SDWIS Population % Difference")

ggplot(state_test, aes(x = sdwis_pop - xwalk_pop)) + 
  geom_histogram() + 
  theme_bw() + 
  facet_wrap(~epic_states_intersect, scales = "free") + 
  labs(y = "Number of Water Systems",
       x = "Estimated Population Difference: SDWIS - crosswalk") + 
  geom_vline(xintercept = 0, color = "red")

# I also want state summaries: 
state_summaries <- state_test %>%
  mutate(sdiws_minus_xwalk_pop = sdwis_pop - xwalk_pop) %>%
  group_by(epic_states_intersect) %>%
  summarize(total_sabs = n(), 
            mean_intersecting_blocks = mean(total_intersected_blocks),
            mean_pct_diff = mean(sdwis_pct_diff*100, na.rm = T),
            mean_sdwis_xwalk_diff = mean(sdiws_minus_xwalk_pop, na.rm = T), 
            median_sdwis_xwalk_diff = median(sdiws_minus_xwalk_pop, na.rm = T))

ggplot(state_summaries, aes(x = mean_intersecting_blocks, 
                            y = mean_pct_diff, 
                            label = epic_states_intersect)) + 
  geom_point() + 
  geom_smooth() + 
  theme_bw() + 
  geom_text(aes(group = epic_states_intersect), 
            position=position_jitter(width=6,height=6)) + 
  labs(x = "Mean # of Intersecting Blocks for SABs", 
       y = "Mean % Diffrence in Population - SDWIS vs Xwalk")

ggplot(state_summaries %>% filter(epic_states_intersect != "DC"), 
       aes(x = mean_intersecting_blocks, 
                            y = median_sdwis_xwalk_diff, 
                            label = epic_states_intersect)) + 
  geom_point() + 
  geom_smooth() + 
  theme_bw() + 
  geom_text(aes(group = epic_states_intersect), 
            position=position_jitter(width=6,height=6)) + 
  labs(x = "Mean # of Intersecting Blocks for SABs", 
       y = "Median SDWIS - Xwalk Pop")





x <- state_test %>%
  group_by(epic_states_intersect, intersect_group) %>%
  summarize(total_ws = n(), 
            total_pop_sdwis = sum(sdwis_pop, na.rm = T), 
            total_pop_xwalk = sum(xwalk_pop, na.rm = T))

ggplot(x, aes(x = intersect_group, y = total_pop_sdwis)) + 
  geom_bar(stat = "identity") + 
  theme_bw() + 
  # facet_wrap(~epic_states_intersect, scales = "free_y") + 
  labs(y = "Number of Water Systems",
       x = "# of Census Block Overlaps") + 
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))

ggplot(x, aes(x = intersect_group, y = total_pop_sdwis)) + 
  geom_bar(stat = "identity") + 
  theme_bw() + 
  # facet_wrap(~epic_states_intersect, scales = "free_y") + 
  labs(y = "Total SDWIS Population",
       x = "# of Census Block Overlaps") + 
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))

ggplot(x, aes(x = intersect_group, y = total_pop_sdwis)) + 
  geom_bar(stat = "identity") + 
  theme_bw() + 
  facet_wrap(~epic_states_intersect, scales = "free_y") +
  labs(y = "Total SDWIS Population",
       x = "# of Census Block Overlaps") + 
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))

