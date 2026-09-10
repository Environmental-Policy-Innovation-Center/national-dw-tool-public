##############################################################################
# Finds and returns the system env for prod or dev work. If env is set to
# prod and interactive check is on, this will confirm with the user before
# continuing in prod.
##############################################################################
check_env <- function(interactive_confirm = TRUE) {
  env <- Sys.getenv("APP_ENV", unset = "dev")
  message("Environment set to: ", env)
  
  # Require interactive confirmation if prod is set.
  if (env == "prod" && interactive_confirm && interactive()) {
    response <- readline("Continue in production mode? (y/n): ")
    if (trimws(response) != "y") {
      stop("Aborted.")
    }}
  # if prod is set and we don't want an interactive confirm (workers, for example)
  if (env == "prod" && !interactive_confirm) {
    message("Running in Production Mode.")
  }
  else {
    message("Running in Development Mode.")
  }
  return(env)
}

##############################################################################
# Builds S3 bucket path based on whether env is prod or dev.
##############################################################################
s3_path <- function(file, env) {
  base <- "s3://tech-team-data/national-dw-tool"
  if (env == "prod") {
    file.path(base, file)
  } else {
    file.path(base, "development", file)
  }
}