source("./functions/check_env.R")
# remove this prompt since this gets pulled into the workers, and needs to 
# run automatically
ENV <- check_env(interactive_confirm = F)

###########################################################################
# Function for updating task manager with dataset status or link
###########################################################################
update_task_manager <- function(dataset_id, raw_s3_link, 
                                # setting default value for the clean link if 
                                # only the raw_s3_link is passed to the function
                                clean_s3_link = "WORKER FAILED - NO FILE DOWNLOADED") {
  print("Updating Task Manager...")
  
  # Pull fresh state from S3
  s3_object <- s3_path("task_manager_data_summary.csv", ENV)
  task_manager <- aws.s3::s3read_using(
    read.csv, 
    object = s3_object
  ) |> mutate(across(everything(), ~ as.character(.)))
  
  # Create the target updates row
  task_manager_df <- data.frame(
    dataset = dataset_id, 
    date_downloaded = as.character(Sys.Date()), 
    # make sure these aren't duplicated links
    raw_link = raw_s3_link,
    clean_link = clean_s3_link
  )
  
  # Add a new row if the dataset is not yet in task manager
  if (!(dataset_id %in% task_manager$dataset)) {
    task_manager <- bind_rows(task_manager, data.frame(dataset = dataset_id))
  }
  
  # Select other columns we do not want to overwrite
  dont_touch_columns <- setdiff(names(task_manager), names(task_manager_df))
  task_manager_simple <- task_manager |> select(dataset, all_of(dont_touch_columns))
  
  # Merge, stack, and sort rows
  updated_task_manager <- task_manager |>
    filter(dataset != dataset_id) |> 
    bind_rows(merge(task_manager_simple, task_manager_df, by = "dataset")) |>
    arrange(dataset) |>
    mutate(across(everything(), ~ as.character(.)))
  
  # Write back to s3
  tmp <- tempfile(fileext = ".csv")
  write.csv(updated_task_manager, file = tmp, row.names = FALSE)
  on.exit(unlink(tmp))
  put_object(
    file = tmp,
    object = s3_object,
    acl = "public-read"
  )
  print("Update to Task Manager Complete.")
}
