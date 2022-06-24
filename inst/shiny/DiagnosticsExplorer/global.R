library(magrittr)
diagExpEnv <- new.env()
diagExpEnv$shinyConfigPath <- "config.yml"

# Source all app files in to isolated namespace
lapply(file.path("R", list.files("R", pattern = "*.R")), source, local = diagExpEnv)

diagExpEnv$appVersionNum <- "Version: 3.0.1"

#### Set enableAnnotation to true to enable annotation in deployed apps
#### Not recommended outside of secure firewalls deployments
diagExpEnv$enableAnnotation <- TRUE
diagExpEnv$enableAuthorization <- TRUE
diagExpEnv$activeUser <- NULL


if (exists("shinySettings")) {
  diagExpEnv$shinySettings <- shinySettings
  diagExpEnv$enableAnnotation <- getOption("enableCdAnnotation", default = FALSE)
  diagExpEnv$activeUser <- Sys.info()[['user']]
} else {
  diagExpEnv$shinySettings <- loadShinySettings(diagExpEnv$shinyConfigPath)
}

### if you need a way to authorize users
### generate hash using code like digest::digest("diagnostics",algo = "sha512")
### store in external file called UserCredentials.csv - with fields userId, hashCode
### place the file in the root folder
diagExpEnv$userCredentials <- data.frame()
if (diagExpEnv$enableAuthorization & !is.null(diagExpEnv$shinySettings$userCredentialsFile)) {
  if (file.exists(diagExpEnv$shinySettings$userCredentialsFile)) {
    diagExpEnv$userCredentials <-
      readr::read_csv(file = diagExpEnv$shinySettings$userCredentialsFile, col_types = readr::cols())
  }
}

if (nrow(diagExpEnv$userCredentials) == 0) {
  diagExpEnv$enableAuthorization <- FALSE
}


diagExpEnv$connectionPool <- diagExpEnv$getConnectionPool(diagExpEnv$shinySettings$connectionDetails)
shiny::onStop(function() {
  if (DBI::dbIsValid(diagExpEnv$connectionPool)) {
    writeLines("Closing database pool")
    pool::poolClose(diagExpEnv$connectionPool)
  }
})

diagExpEnv$dataSource <-
  diagExpEnv$createDatabaseDataSource(
    connection = diagExpEnv$connectionPool,
    resultsDatabaseSchema = diagExpEnv$shinySettings$resultsDatabaseSchema,
    vocabularyDatabaseSchema = diagExpEnv$shinySettings$vocabularyDatabaseSchemas,
    dbms = diagExpEnv$shinySettings$connectionDetails$dbms
  )

# Init tables and other parameters in global session
initializeEnvironment(diagExpEnv$dataSource, envir = diagExpEnv)
