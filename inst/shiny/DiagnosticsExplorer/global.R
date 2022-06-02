library(magrittr)
shinyConfigPath <- "config.yml"

source("R/StartUpScripts.R")
source("R/DisplayFunctions.R")
source("R/Tables.R")
source("R/Plots.R")
source("R/Results.R")
source("R/Annotation.R")
source("R/ResultRetrieval.R")

# Modules
source("R/Annotation.R")
source("R/CohortCountsModule.R")
source("R/CharacterizationModule.R")
source("R/CohortDefinitionModule.R")
source("R/CohortOverlapModule.R")
source("R/CompareCharacterizationModule.R")
source("R/ConceptsInDataSourceModule.R")
source("R/DatabaseInformationModule.R")
source("R/InclusionRulesModule.R")
source("R/IndexEventModule.R")
source("R/OrphanConceptsModule.R")
source("R/TemporalCharacterizationModule.R")
source("R/TimeDistributionsModule.R")
source("R/VisitContextModule.R")

appVersionNum <- "Version: 3.0.1"

#### Set enableAnnotation to true to enable annotation in deployed apps
#### Not recommended outside of secure firewalls deployments
enableAnnotation <- TRUE
enableAuthorization <- TRUE
activeUser <- NULL


if (exists("shinySettings")) {
  enableAnnotation <- getOption("enableCdAnnotation", default = FALSE)
  activeUser <- Sys.info()[['user']]
} else {
  shinySettings <- loadShinySettings(shinyConfigPath)
}

### if you need a way to authorize users
### generate hash using code like digest::digest("diagnostics",algo = "sha512")
### store in external file called UserCredentials.csv - with fields userId, hashCode
### place the file in the root folder
userCredentials <- data.frame()
if (enableAuthorization & !is.null(shinySettings$userCredentialsFile)) {
  if (file.exists(shinySettings$userCredentialsFile)) {
    userCredentials <-
      readr::read_csv(file = shinySettings$userCredentialsFile, col_types = readr::cols())
  }
}

if (nrow(userCredentials) == 0) {
  enableAuthorization <- FALSE
}


connectionPool <- getConnectionPool(shinySettings$connectionDetails)
shiny::onStop(function() {
  if (DBI::dbIsValid(connectionPool)) {
    writeLines("Closing database pool")
    pool::poolClose(connectionPool)
  }
})

dataSource <-
  createDatabaseDataSource(
    connection = connectionPool,
    resultsDatabaseSchema = shinySettings$resultsDatabaseSchema,
    vocabularyDatabaseSchema = shinySettings$vocabularyDatabaseSchemas,
    dbms = shinySettings$connectionDetails$dbms
  )

# Init tables and other parameters in global session
initializeEnvironment(dataSource)

