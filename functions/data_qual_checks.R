##############################################################################
# run_var_checks(rds_list, data_inven) 
# author: emmali
# date: Aug 8, 2025
# 
# Function to run variable checks, given a RDS data list and the data inventory, 
# specifically to help update the variable summary in the task manager. This 
# function can also handle EPA SABs
# 
# arguments: 
# - rds_list = RDS list containing data 
# - data_inven = data inventory from the task manager
# 
# returns: 
# a data frame containing the variable, clean name, data scores, and whether 
# the variable meets the 50% threshold for quality checks
##############################################################################
run_var_checks <- function(rds_list, data_inven){ 
  
  # check to see if the rds_lit is actually just SABs. If so, run the 
  # sabs_var_checks function: 
  if(inherits(rds_list, "sf")){
    print("Running SABs Checks!")
    
    # run the sabs var checks
    var_scores <- .run_sabs_var_checks(rds_list, data_inven)
    #  return the result
    return(var_scores)
    
  } else {
    # otherwise - run normal checks
    print("Running Variable Checks!")
    # empty dataframe for storing results: 
    var_scores <- data.frame() # for storing variable scores 
    
    # remove datasets that are not used (i.e., use_in_tool == blank for all rows)
    datasets <- unique(data_inven$dataset)
    mini_list_i <- rds_list[names(rds_list) %in% datasets]
    
    # columns and names to loop through: 
    list_names <- names(mini_list_i)
    column_names <- lapply(mini_list_i, colnames) 
    
    # loop through list entries: 
    for(i in 1:length(list_names)){
      # filtering for the vars relevant for the list item 
      data_inven_i <- data_inven %>% 
        filter(dataset == list_names[i]) %>%
        filter(variable != "geometry")
      
      # grabbing manually entered data scores for easy merging: 
      manual_data_scores_i <- data_inven_i %>%
        select(dataset, variable, clean_name, starts_with("data_score"))
      
      # yanking out a list of vars where it wouldn't make sense to check for dups: 
      dont_check_dups <- data_inven_i %>%
        filter(`do_dups_matter?` == "no") %>%
        select(variable)
      
      # selecting the columns for that data frame
      df_i <- mini_list_i[[i]] %>%
        select(data_inven_i$variable) %>%
        # removing geometries - only relevant for SABs and we don't have any other 
        # geometry files 
        as.data.frame() %>%
        select(-contains("geometry")) 
      
      # run checks: 
      skim_df_i <- skim(df_i)
      var_score_i <- data.frame(dataset = list_names[i],
                                variable = skim_df_i$skim_variable, 
                                # grab key calculations after skim: 
                                missing = skim_df_i$n_missing, 
                                completeness = 100*(skim_df_i$complete_rate), 
                                empty = skim_df_i$character.empty,
                                unique = skim_df_i$character.n_unique) %>%
        # I want to consider both empty and missing entries in my calculation of 
        # completeness - fixing that here: 
        mutate(empty_no_na = case_when(is.na(empty) ~ 0, TRUE ~ empty)) %>%
        mutate(empty_or_missing = missing + empty_no_na) %>%
        # translate these to percentages: 
        mutate(data_score_completeness = (100-100*(empty_or_missing/length(unique(df_i$pwsid)))), 
               data_score_duplicates = 100*(unique/length(unique(df_i$pwsid)))) %>%
        # don't check for duplicates for certain variables 
        mutate(data_score_duplicates = case_when(variable %in% dont_check_dups$variable ~ NA, 
                                                 TRUE ~ data_score_duplicates)) %>%
        # filtering the extra fields we don't need: 
        select(-c(missing:empty_or_missing)) %>%
        # add manual scores 
        left_join(., manual_data_scores_i) %>%
        # transforming back to numeric values
        mutate(across(starts_with("data_score"), as.numeric)) %>%
        # take the mean across all columns that start with data_score:
        mutate(auto_data_score = rowMeans(select(., starts_with("data_score")), 
                                          na.rm = T)) %>%
        # if quality is < 50%, flag for human review: 
        mutate(data_qual_flag = case_when(auto_data_score < 50 ~ "NEEDS REVIEW", 
                                          TRUE ~ "PASSED CHECK")) %>%
        relocate(clean_name, .after = variable)
      
      var_scores <- rbind(var_scores, var_score_i)
    }
    return(var_scores)
  }
}

##############################################################################
# run_data_checks(rds_list, data_inven, var_qual_checks, add_to_s3 = F) 
# author: emmali
# date: Aug 8, 2025
# 
# Function to run variable checks, given a RDS data list and the data inventory, 
# specifically to help update the variable summary in the task manager. This 
# function can also handle EPA SABs
# 
# arguments: 
# - rds_list = RDS list containing data 
# - data_inven = data inventory from the task manager
# - var_qual_checks = variable quality checks from run_var_checks function above.
#     If this argument is not supplied, it goes ahead and runs it for you!
# - add_to_s3 = whether you want to add datasets that automatically pass checks 
#     to staged environment. Helpful to turn this off for function testing. 
# 
# returns: 
# a data frame containing the datasset name, summary quality score, the number of 
# vars that passed from run_var_checks, date it was staged, and the 
# link to staged dataset
##############################################################################
run_data_checks <- function(rds_list, data_inven, var_qual_checks, add_to_s3 = F){
  # remove datasets that are not used (i.e., use_in_tool == blank for all rows)
  datasets <- unique(data_inven$dataset)
  mini_list_i <- rds_list[names(rds_list) %in% datasets]
  
  # columns and names to loop through: 
  list_names <- names(mini_list_i)
  
  # empty dataframe for storing results: 
  manual_checks <- list() # for storing data that need manual checks 
  z = 0 # <- this helps append datasets that need manual checks to the list above
  manual_checks_names <- c()
  
  # run the var checks
  if(missing(var_qual_checks)){
    var_score_i <- run_var_checks(rds_list, data_inven)
  } else {
    var_score_i <- var_qual_checks
  }
  
  
  # group by and summarize the var checks 
  data_score_i <- var_score_i %>%
    group_by(dataset) %>%
    reframe(mean_data_qual_score = as.character(mean(auto_data_score)),
            data_qual_score = paste0(sum(data_qual_flag == "PASSED CHECK"), 
                                     " / ", 
                                     n(),
                                     " variables passed checks"),
            needs_review_flag = case_when(all(data_qual_flag == "PASSED CHECK") ~ "PASSED", 
                                          TRUE ~ "NEEDS REVIEW"),
            date_staged = as.character(Sys.Date()),
            staged_link = paste0("s3://tech-team-data/national-dw-tool/test-staged/", dataset, ".csv")) %>%
    mutate(date_staged = case_when(needs_review_flag == "NEEDS REVIEW" ~ NA, 
                                   TRUE ~ date_staged), 
           staged_link = case_when(needs_review_flag == "NEEDS REVIEW" ~NA, 
                                   TRUE ~ staged_link)) %>%
    unique()
  
  # fix the arrangement, if needed
  data_score_i <- data_score_i[match(list_names, data_score_i$dataset), ]    
  
  # loop through the data checks and see if it passed
  for(i in 1:nrow(data_score_i)){
    # if it passed, rename the cols based on clean_name and add to s3
    if(data_score_i$needs_review_flag[i] == "PASSED" & add_to_s3 == T){
      
      # find the dataset in the loop: 
      staged_link_i <- data_score_i$staged_link[i]
      
      # print which one it is adding to make sure they align :) 
      print(paste0("Checks passed - adding to S3: " , list_names[i], " to: ", staged_link_i))
      
      # filtering for the vars relevant for the list item 
      data_inven_i <- data_inven %>% 
        filter(dataset == list_names[i]) %>%
        filter(variable != "geometry")
      
      df_i <- mini_list_i[[list_names[i]]] %>%
        select(data_inven_i$variable) %>%
        # removing geometries - only relevant for SABs and we don't have any other 
        # geometry files 
        as.data.frame() %>%
        select(-contains("geometry")) 
      
      # handle rounding - make sure each column has its own function
      need_rounding <-  data_inven_i %>%
        filter(!is.na(round_digits)) %>%
        # founding function depends on the round_digits field
        mutate(round_func = map(round_digits, ~ function(x) round(x, digits = .x)))
      # okay so this storing a function in the environment - you can see 
      # the actual number of digits that get rounded if you use the code 
      # below - 
      # get(".x", envir = environment(need_rounding$round_func[[1]]))
      # get(".x", envir = environment(need_rounding$round_func[[5]]))
      
      # handle situations where the dataframe doesn't need any kind of rounding 
      # [the bwn dataset]
      if(nrow(need_rounding) == 0){
        print("No Rounding Needed")
        # if need_rounding is empty, just swap this over
        df_i_rounded <- df_i
      } else {
        # otherwise - apply rounding: 
        
        # some variables, like the SDWA summaries, are actually 
        # characters to handle situations where a water system may not have 
        # enough data to be comparable. 
        df_i_rounded <- df_i %>%
          mutate(across(need_rounding$variable, str_squish)) %>%
          # if I added placeholder strings (i.e., "water system operating < 5 years") 
          # transform all the strings I added back to "NA"
          mutate(across(need_rounding$variable, 
                        # if the entire string is NOT just numbers, transform this 
                        # to NA - this should capture all kinds of numbers 
                        ~case_when(!grepl("^-?[0-9]+\\.[0-9]+$|^-?[0-9]+$", .x) ~ NA, 
                                   TRUE ~ .x))) %>%
          # transform to numeric 
          mutate(across(need_rounding$variable, as.double))
        
        # for each col that needs rounding:
        for (x in 1:nrow(need_rounding)) {
          # isolate the variable and function 
          col <- need_rounding$variable[x]
          # find the rounding function 
          fn  <- need_rounding$round_func[[x]]
          # apply function we built earlier 
          df_i_rounded[[col]] <- fn(df_i_rounded[[col]])
          # print(head(df_i_rounded))
        }
      }
      
      # write to s3: 
      tmp <- tempfile()
      write.csv(df_i_rounded, file = paste0(tmp, ".csv"), row.names = F)
      on.exit(unlink(tmp))
      put_object(
        file = paste0(tmp, ".csv"),
        object = staged_link_i,
        acl = "public-read", 
        multipart = T
      )
    } 
    
    if(data_score_i$needs_review_flag[i] == "NEEDS REVIEW") {
      # if it failed, write to the manual_checks list, add to Z so the next one 
      # appends below, and keep track of the list names 
      print(paste0("Checks failed for: ", list_names[i], ", adding to list called manual_checks"))
      z = z + 1
      manual_checks[[z]] <<- df_i
      manual_checks_names <<- c(manual_checks_names, list_names[i])
    } else {
      next()
    }
  }
  return(data_score_i)
  
}

##############################################################################
# .run_sabs_var_checks(rds_list, data_inven) 
# author: emmali & gabe
# date: Aug 8, 2025
# 
# Internal function to run variable checks, given SABs and the data inventory, 
# specifically to help update the variable summary in the task manager. This 
# function gets pulled into run_var_checks to handle SABs
# 
# arguments: 
# - sab = the EPA SABs dataset
# - data_inven = data inventory from the task manager
# 
# returns: 
# a data frame containing the variable, clean name, data scores, and whether 
# the variable meets the 50% threshold for quality checks
##############################################################################
.run_sabs_var_checks <- function(sab, data_inven){
  ## Overlaps - yes/no
  ## sab_overlap = sabs with w/o overlap / nsabs
  sab_stage <- sab %>% 
    select(pwsid, geometry, population_served_count, epic_states_intersect) %>%
    mutate(last_epic_run_date = Sys.Date())%>%
    # left_join(national_socioeconomic[[1]] %>% select(pwsid,total_pop))%>%
    mutate(sab_area = st_area(.))
  
  print("Identifying Overlaps")
  # Get intersection list for all polygons (this runs much faster than st_intersection)
  sab_intersect <- st_intersects(sab_stage, sab_stage)
  # Filter to only keep polygons where intersection length > 1
  multi_intersect_mask <- lengths(sab_intersect) > 1
  # now filter to our sabs that intersect another sab 
  sab_filtered <- sab_stage[multi_intersect_mask, ]
  
  ## getting actual area overlap to then exclude > 95% overlap (spatial
  # processing errors/negligible overlap). Looking for TRUE overlaps here)
  ## Note this takes a long time to run - turn off spherical geometry. sf_use_s2(F)
  ## Note pwsid is not unique 
  sf_use_s2(F)
  sab_intersection <- st_intersection(sab_filtered, sab_filtered) %>%
    filter(pwsid != pwsid.1) %>%
    mutate(intersection_area = st_area(.))%>%
    mutate(percent_overlap = as.numeric(intersection_area / sab_area) * 100)%>%  
    filter(percent_overlap > 5)
  
  data_score_overlaps <- 100*(1 - length(unique(sab_intersection$pwsid)) / nrow(sab_stage))
  
  
  ## Checking for duplicated PWSIDs, which may indicate duplicated system 
  # boundaries 
  print("Identifying Duplicates & Empty Geometries")
  # are there any duplicated pwsids and presumably water system boundaries? 
  if(any(duplicated(sab_stage$pwsid))){
    epa_data_score_duplicates <- 100 -(sum(duplicated(sab_stage$pwsid)) / length(unique(sab_stage$pwsid)))
    
  } else {
    epa_data_score_duplicates <- 100
  }
  
  ## check completeness score - are there any geometries that are NA or 
  # empty?
  if(any(st_is_empty(sab_stage) | any(is.na(sab_stage$geometry)))){
    num_empty <- sum(st_is_empty(sab_stage)) 
    num_na <- sum(is.na(sab_stage$geometry))
    epa_data_score_completeness <- 100 - ((num_na + num_empty) / length(unique(sab_stage$pwsid)))
  } else {
    epa_data_score_completeness <- 100
  }
  
  print("Creating Var Summary")
  # creating var summary: 
  epa_var_scores <- data_inven %>% 
    filter(grepl("SAB", `use_in_tool?`)) %>%
    select(variable, clean_name, data_score_coverage) %>%
    mutate(dataset = "epa_sabs_geoms", 
           data_score_overlaps = data_score_overlaps, 
           data_score_duplicates = epa_data_score_duplicates, 
           data_score_completeness = epa_data_score_completeness)  %>%
    # transforming back to numeric values
    mutate(across(starts_with("data_score"), as.numeric)) %>%
    # take the mean across all columns that start with data_score:
    mutate(auto_data_score = rowMeans(select(., starts_with("data_score")), 
                                      na.rm = T)) %>%
    # if quality is < 50%, flag for human review: 
    mutate(data_qual_flag = case_when(auto_data_score < 50 ~ "NEEDS REVIEW", 
                                      TRUE ~ "PASSED CHECK")) 
  
  return(epa_var_scores)
}

##############################################################################
# run_sabs_df_checks(rds_list, data_inven) 
# author: emmali 
# date: Aug 8, 2025
# 
# Function to run variable checks, given SABs and the data inventory, 
# specifically to help update the data summary in the task manager. This 
# function is specific to EPA SABs, but we might be able to merge this with 
# run_data_checks() above. 
# 
# arguments: 
# - sab = EPA SABs
# - data_inven = data inventory from the task manager
# - var_qual_checks = variable quality checks from run_var_checks function above.
#     If this argument is not supplied, it goes ahead and runs it for you!
# - add_to_s3 = whether you want to add datasets that automatically pass checks 
#     to staged environment. Helpful to turn this off for function testing. 
# 
# returns: 
# a data frame containing the dataset name, summary quality score, the number of 
# vars that passed from run_var_checks, date it was staged, and the 
# link to staged dataset
##############################################################################
run_sabs_df_checks <- function(sab, data_inven, var_qual_checks, add_to_s3 = F){
  
  # run the var checks, if the argument is blank
  if(missing(var_qual_checks)){
    var_score_i <- run_sabs_var_checks(sab, data_inven)
  } else {
    var_score_i <- var_qual_checks
  }
  
  # group by and summarize the var checks 
  epa_staged_link <- paste0("s3://tech-team-data/national-dw-tool/test-staged/", var_score_i$dataset, ".geojson")
  epa_df_score <- data.frame(dataset = var_score_i$dataset,
                             mean_data_qual_score = as.character(mean(var_score_i$auto_data_score)),
                             data_qual_score = paste0(sum(var_score_i$data_qual_flag == "PASSED CHECK"), 
                                                      " / ", 
                                                      nrow(var_score_i),
                                                      " variables passed checks"), 
                             needs_review_flag = case_when(all(var_score_i$data_qual_flag == "PASSED CHECK") ~ "PASSED", 
                                                           TRUE ~ "NEEDS REVIEW"),
                             date_staged = as.character(Sys.Date()), 
                             staged_link = epa_staged_link) %>%
    mutate(date_staged = case_when(needs_review_flag == "NEEDS REVIEW" ~ NA, 
                                   TRUE ~ date_staged), 
           staged_link = case_when(needs_review_flag == "NEEDS REVIEW" ~NA, 
                                   TRUE ~ staged_link))
  
  # if it passed, rename the cols based on clean_name and add to s3
  if(epa_df_score$needs_review_flag == "PASSED" & add_to_s3 == T){
    print(paste0("Checks passed - adding to S3: " , var_score_i$dataset, " to: ", epa_staged_link))
    
    # keeping this geojson to just service areas and pwsid: 
    sab_stage_simple <- sab %>%
      select(pwsid)
    
    # write to s3: 
    tmp <- tempfile()
    st_write(sab_stage_simple, dsn = paste0(tmp, ".geojson"))
    on.exit(unlink(tmp))
    put_object(
      file = paste0(tmp, ".geojson"),
      object = epa_staged_link,
      bucket = "tech-team-data",
      acl = "public-read", 
      multipart = T
    )
  }  
  
  if(epa_df_score$needs_review_flag == "NEEDS REVIEW") {
    # if it failed, write to the manual_checks list, add to Z so the next one 
    # appends below, and keep track of the list names 
    print(paste0("Checks failed for: ", var_score_i$dataset, ", needs manual check"))
  }
  
  return(epa_df_score)
  
}