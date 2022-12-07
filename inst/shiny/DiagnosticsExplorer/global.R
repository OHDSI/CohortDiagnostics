library(magrittr)
diagExpEnv <- new.env()
diagExpEnv$shinyConfigPath <- getOption("CD-shiny-config", default = "config.yml")

# Source all app files in to isolated namespace
lapply(file.path("R", list.files("R", pattern = "*.R")), source, local = diagExpEnv)

diagExpEnv$appVersionNum <- "Version: 3.1.1"

if (exists("shinySettings")) {
  diagExpEnv$shinySettings <- shinySettings
  diagExpEnv$activeUser <- Sys.info()[['user']]
} else {
  writeLines("Using settings provided by user")
  diagExpEnv$shinySettings <- diagExpEnv$loadShinySettings(diagExpEnv$shinyConfigPath)
  diagExpEnv$activeUser <- NULL
}

# Init tables and other parameters in global session
diagExpEnv$initializeEnvironment(diagExpEnv$shinySettings, envir = diagExpEnv)


