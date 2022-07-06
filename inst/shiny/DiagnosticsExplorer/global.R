library(magrittr)
diagExpEnv <- new.env()
diagExpEnv$shinyConfigPath <- "config.yml"

# Source all app files in to isolated namespace
lapply(file.path("R", list.files("R", pattern = "*.R")), source, local = diagExpEnv)

diagExpEnv$appVersionNum <- "Version: 3.0.2"

#### Set enableAnnotation to true to enable annotation in deployed apps
#### Not recommended outside of secure firewalls deployments
diagExpEnv$enableAnnotation <- TRUE
diagExpEnv$enableAuthorization <- TRUE
diagExpEnv$activeUser <- NULL


if (exists("shinySettings")) {
  diagExpEnv$shinySettings <- shinySettings
  diagExpEnv$activeUser <- Sys.info()[['user']]
} else {
  writeLines("Using settings provided by user")
  diagExpEnv$shinySettings <- diagExpEnv$loadShinySettings(diagExpEnv$shinyConfigPath)
}

# Init tables and other parameters in global session
diagExpEnv$initializeEnvironment(diagExpEnv$shinySettings, envir = diagExpEnv)