# figure for boil water notice blog
library(aws.s3)
library(googlesheets4)
library(tidyverse)
library(janitor)
library(reactable)
library(reactablefmtr)

## JK we're doing a reactable! ################################################
# pull data from google sheets
url <- "https://docs.google.com/spreadsheets/d/15iVYq2v3Gpy5Zug3BhYC0vU4L-axV5g0drZt4-uLv-Q/edit?gid=24413099#gid=24413099"
state_data <-  read_sheet(url, sheet = "state_bwn_schema")

# tidying & organizing names 
state_table <- state_data %>%
  # removing the states I got stuck on & therefore we do not have data for them
  filter(!is.na(clean_state)) %>%
  # extra columns for my own purposes
  select(-c(`Temporal Range`, `Notes`, `data?`, `Recommendation`, `mean`, 
            `advisory types`)) %>%
  arrange(state) %>%
  # make column names pretty
  rename(State = state, 
         `State Name` = clean_state, 
         `Water System ID` = pwsid_type, 
         `Date Issued` = date_issued, 
         `Date Lifted` = date_lifted,
         `Advisory Types` = advisory_simple, 
         `Link` = link) %>%
  # format numeric columns to colored icons 
  mutate(across(.cols = c(pwsid:adv_type),
                ~case_when(.x == 1 ~ "#b3e59f", 
                           .x == 0.5 ~ "#face9a", 
                           TRUE ~ "#cc7e7f")))

# reactable table
state_reactable <- state_table %>% 
  reactable(
    defaultColDef = colDef(
      align = 'center'),
    defaultSorted = "State", 
    columns = list(
      `Water System ID` = colDef(
        cell = pill_buttons(state_table, color_ref = 'pwsid')), 
      pwsid = colDef(show = FALSE),
      `Date Issued` = colDef(
        cell = pill_buttons(state_table, color_ref = 'date_iss')), 
      date_iss = colDef(show = FALSE),
      `Date Lifted` = colDef(
        cell = pill_buttons(state_table, color_ref = 'date_lift')), 
      date_lift = colDef(show = FALSE), 
      `Advisory Types` = colDef(
        cell = pill_buttons(state_table, color_ref = 'adv_type')), 
      adv_type = colDef(show = FALSE), 
      `contact email` = colDef(show = FALSE), 
      `contact phone` = colDef(show = FALSE), 
      `contact status` = colDef(show = FALSE), 
      Link = colDef(name = "Link To Website",
                    cell = function(value){
                      # add hyperlinked text
                      url <- as.character(value)
                      htmltools::tags$a(href = url, target = "_blank", "Link")
                    })),
    highlight = TRUE,
    bordered = TRUE,
    resizable = TRUE,
    showSortable = TRUE,
    searchable = TRUE,
    defaultPageSize = 16,
    theme = reactableTheme(
      style = list(fontFamily = "-system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, Helvetica, Arial, sans-serif")))

state_reactable 

# save!
# htmlwidgets::saveWidget((state_reactable),
#                         "figures/bwn_summary_table.html")

# add to s3!
# put_object(
#   file = file.path("./figures/bwn_summary_table.html"),
#   object = "national-dw-tool/figures/bwn_summary_table.html",
#   bucket = "tech-team-data",
#   acl = "public-read"
# )


##### Older code for when we were attempting a map #############################
# library(sysfonts)
# library(showtext)
# library(colorspace)
# # libraries for maps: 
# library(leaflet)
# library(basemapR)
# library(sf)
# # state boundaries: 
# us_state_map <- aws.s3::s3read_using(st_read, 
#                                      object = "base-layers/states_ak_hi_v3.geojson",
#                                      bucket = "tech-team-data",
#                                      quiet = TRUE)
# cat_palette <- colorRampPalette(c("#172f60","#1054a8",
#                                   "#791a7b","#de9d29", 
#                                   "#b15712","#4ea324"))
# # continuous palettes - based on EPIC's primary colors: 
# cont_palette <- colorRampPalette(c("#172f60","#4ea324"))
# 
# # pull data from gogle sheets
# url <- "https://docs.google.com/spreadsheets/d/1XsMHSUo_LZ-pc10Sdisa4EYaVIK9KBJVaE7eenYIwxw/edit?gid=1066853710#gid=1066853710"
# state_data <-  read_sheet(url, sheet = "state_bwn_schema")
# 
# # tidying for consistency 
# state_sf <- merge(us_state_map, state_data, 
#                   by.x = "NAME", 
#                   by.y = "clean_state", all.x = T) %>%
#   mutate(have_data = case_when(`data?` == "yes" ~ "Yes", 
#                                TRUE ~ "No"), 
#          pwsid_type = case_when(is.na(pwsid_type) ~ "Advisories Not Available", 
#                                 TRUE ~ pwsid_type), 
#          date_issued = case_when(is.na(date_issued) ~ "Advisories Not Available", 
#                                 TRUE ~ date_issued),
#          date_lifted = case_when(is.na(date_lifted) ~ "Advisories Not Available", 
#                                  TRUE ~ date_lifted), 
#          advisory_simple = case_when(is.na(advisory_simple) ~ "Advisories Not Available", 
#                                  TRUE ~ advisory_simple), 
#          mean = 100*(as.numeric(mean))) %>%
#   st_as_sf() %>%
#   relocate(have_data, .after = state)
# 
# 
# # palettes
# categorical_palette <- colorFactor(palette = c("darkgray", "#4EA324"), 
#                                    domain = state_sf$have_data)
# categorical_palette_cols <- colorFactor(palette = c("darkgray",  "#4EA324", "red", "orange"), 
#                                    domain = state_sf$pwsid_type)
# color_function <- colorNumeric(palette = cont_palette(3),
#                                domain = state_sf$mean, na.color = "darkgrey")
# 
# # plottin' 
# leaflet() %>%
#   addPolygons(data = state_sf, 
#               fillColor = ~categorical_palette(have_data),
#               fillOpacity = 0.7,
#               group = "Advisories",
#               color = "white",
#               weight = 1, 
#               smoothFactor = 0, 
#               label = paste0(state_sf$NAME, "; Do they report advisories? ", 
#                              state_sf$have_data)) %>%
#   addPolygons(data = state_sf, 
#               fillColor = ~categorical_palette_cols(pwsid_type), 
#               group = "PWSID",
#               fillOpacity = 0.7,
#               color = "white",
#               weight = 1, 
#               smoothFactor = 0, 
#               label = paste0(state_sf$NAME, "; Feature coverage for PWSIDs: ", 
#                              state_sf$pwsid_type)) %>%
#   addPolygons(data = state_sf, 
#               fillColor = ~categorical_palette_cols(date_issued), 
#               group = "Date Issued",
#               fillOpacity = 0.7,
#               color = "white",
#               weight = 1, 
#               smoothFactor = 0, 
#               label = paste0(state_sf$NAME, "; Feature coverage for date issued: ", 
#                              state_sf$date_issued)) %>%
#   addPolygons(data = state_sf, 
#               fillColor = ~categorical_palette_cols(date_lifted), 
#               group = "Date Lifted",
#               fillOpacity = 0.7,
#               color = "white",
#               weight = 1, 
#               smoothFactor = 0, 
#               label = paste0(state_sf$NAME, "; Feature coverage for date lifted: ", 
#                              state_sf$date_lifted)) %>%
#   addPolygons(data = state_sf, 
#               fillColor = ~categorical_palette_cols(advisory_simple), 
#               group = "Advisory Type",
#               fillOpacity = 0.7,
#               color = "white",
#               weight = 1, 
#               smoothFactor = 0, 
#               label = paste0(state_sf$NAME, "; Feature coverage for advisory type: ", 
#                              state_sf$advisory_simple)) %>%
#   addPolygons(data = state_sf, 
#               fillColor = ~color_function(mean), 
#               group = "Overall Coverage (%)",
#               fillOpacity = 0.7,
#               color = "white",
#               weight = 1, 
#               smoothFactor = 0, 
#               label = paste0(state_sf$NAME, "; Overall coverage across features:  ", 
#                              state_sf$mean, "%")) %>%
#   # add a basemap if you're working with states so they're not floating in
#   # ~space~
#   addLegend("bottomright",
#             group =  "Advisories",
#             pal = categorical_palette,
#             values = c("Yes", "No"), 
#             labels = c("Available", "Unavailable"),
#             # if working with money or percentages, use the 
#             # follow label formats: 
#             # labFormat = labelFormat(prefix = "$"),
#             # labFormat = labelFormat(prefix = "%"),
#             title = "Boil Water Advisories",
#             opacity = 1) %>%
#   addLegend("topleft",
#             group =  "Overall Coverage (%)",
#             pal = color_function,
#             values = state_sf$mean, 
#             # if working with money or percentages, use the 
#             # follow label formats: 
#             # labFormat = labelFormat(prefix = "$"),
#             # labFormat = labelFormat(prefix = "%"),
#             title = "Overall Field Coverage",
#             opacity = 1) %>%
#   addLegend("bottomleft",
#             group =  c("PWSID", "Date Issued", "Date Lifted", "Advisory Type"),
#             pal = categorical_palette_cols,
#             values = c("Unavailable", "Complete", "No Information", "Partially Complete"), 
#             # if working with money or percentages, use the 
#             # follow label formats: 
#             # labFormat = labelFormat(prefix = "$"),
#             # labFormat = labelFormat(prefix = "%"),
#             title = "Field Coverage",
#             opacity = 1) %>%
#   hideGroup(c( "Overall Coverage (%)",
#                "PWSID", 
#                "Date Issued", 
#                "Date Lifted", 
#                "Advisory Type")) %>%
#   # Layers control
#   addLayersControl(
#     baseGroups = c(
#       "Advisories", 
#       "Overall Coverage (%)",
#       "PWSID", 
#       "Date Issued", 
#       "Date Lifted", 
#       "Advisory Type"), 
#     options = layersControlOptions(collapsed = FALSE)
#   ) 
# 
# 
# # trying somethign else out:
# state_sf_df <- state_sf %>% as.data.frame()
# # urgg this is crazy and I can't turn legends on/off without shiny 
# labs <- lapply(seq(nrow(state_sf_df)), function(i) {
#   paste0( '<p> <b>State: </b>', state_sf_df[i, "NAME"], '</p><p><b>Feature Coverage (%): </b>', 
#           state_sf_df[i, "mean"], '%</p><p><b>PWSID: </b>',
#           state_sf_df[i, "pwsid_type"], '</p><p><b>Date Issued: </b>', 
#           state_sf_df[i, "date_issued"],'</p><p><b>Date Lifted: </b>', 
#           state_sf_df[i, "date_lifted"],'</p><p><b>Advisory Type: </b>',
#           state_sf_df[i, "advisory_simple"], '</p>' ) 
# })
# 
# bwn_fig <- leaflet() %>%
#   addPolygons(data = state_sf, 
#               fillColor = ~color_function(mean), 
#               group = "Overall Feature Coverage (%)",
#               fillOpacity = 0.7,
#               color = "black",
#               weight = 1, 
#               smoothFactor = 0, 
#               label = lapply(labs, htmltools::HTML)) %>%
#   # add a basemap if you're working with states so they're not floating in
#   # ~space~
#   addLegend("bottomright",
#             group =  "Overall Coverage (%)",
#             pal = color_function,
#             values = state_sf$mean, 
#             # if working with money or percentages, use the 
#             # follow label formats: 
#             # labFormat = labelFormat(prefix = "$"),
#             # labFormat = labelFormat(prefix = "%"),
#             title = "Overall Feature Coverage (%)",
#             opacity = 1) 
# 
# bwn_fig
# 
# htmlwidgets::saveWidget(bwn_fig, "./figures/boil_water_advisories.html", 
#                         background = "white")
# 
# put_object(
#   file = file.path("./figures/boil_water_advisories.html"), 
#   object = "national-dw-tool/figures/boil_water_advisories_data.html", 
#   bucket = "tech-team-data",
#   acl = "public-read"
# )


