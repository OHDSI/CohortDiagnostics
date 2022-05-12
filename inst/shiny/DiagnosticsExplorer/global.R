library(magrittr)
### Change this lane if deploying shiny files directly with sqlite database
sqliteDbPath <- file.path("data", "MergedCohortDiagnosticsData.sqlite")
source("R/StartUpScripts.R")
source("R/DisplayFunctions.R")
source("R/Tables.R")
source("R/Plots.R")
source("R/Results.R")
source("R/Annotation.R")
source("R/CirceRendering.R")
source("R/ResultRetrieval.R")

appVersionNum <- "Version: 3.0.0"
appInformationText <- paste("Powered by OHDSI Cohort Diagnostics application", paste0(appVersionNum, "."))
appInformationText <- paste0(
  appInformationText,
  "Application was last initated on ",
  lubridate::now(tzone = "EST"),
  " EST. Cohort Diagnostics website is at https://ohdsi.github.io/CohortDiagnostics/"
)

#### Set enableAnnotation to true to enable annotation in deployed apps
#### Not recommended outside of secure firewalls deployments
enableAnnotation <- TRUE
enableAuthorization <- TRUE
activeUser <- NULL

### if you need a way to authorize users
### generate hash using code like digest::digest("diagnostics",algo = "sha512")
### store in external file called UserCredentials.csv - with fields userId, hashCode
### place the file in the root folder
userCredentials <- data.frame()
if (enableAuthorization) {
  if (file.exists("UserCredentials.csv")) {
    userCredentials <-
      readr::read_csv(file = "UserCredentials.csv", col_types = readr::cols())
  } else {
    enableAuthorization <- FALSE
  }
}

if (exists("shinySettings")) {
  writeLines("Using settings provided by user")
  shinyConnectionDetails <- shinySettings$connectionDetails
  dbms <- shinyConnectionDetails$dbms
  resultsDatabaseSchema <- shinySettings$resultsDatabaseSchema
  vocabularyDatabaseSchemas <- shinySettings$vocabularyDatabaseSchemas
  enableAnnotation <- getOption("enableCdAnnotation", default = FALSE)
  activeUser <- Sys.info()[['user']]
} else if (file.exists(sqliteDbPath)) {
  writeLines("Using data directory")
  sqliteDbPath <- normalizePath(sqliteDbPath)
  resultsDatabaseSchema <- "main"
  vocabularyDatabaseSchemas <- "main"
  dbms <- "sqlite"
  shinyConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqliteDbPath)
} else {
  writeLines("Connecting to remote database")
  dbms <- Sys.getenv("shinydbDatabase", unset = "postgresql")
  shinyConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = dbms,
    server = Sys.getenv("shinydbServer"),
    port = Sys.getenv("shinydbPort", unset = 5432),
    user = Sys.getenv("shinydbUser"),
    password = Sys.getenv("shinydbPw")
  )
  resultsDatabaseSchema <- Sys.getenv("shinydbResultsSchema", unset = "thrombosisthrombocytopenia")
  vocabularyDatabaseSchemas <- resultsDatabaseSchema
  alternateVocabularySchema <- Sys.getenv("shinydbVocabularySchema", unset = c("vocabulary"))
  vocabularyDatabaseSchemas <-
    setdiff(
      x = c(vocabularyDatabaseSchemas, alternateVocabularySchema),
      y = resultsDatabaseSchema
    ) %>%
    unique() %>%
    sort()
}

connectionPool <- getConnectionPool(shinyConnectionDetails)

dataModelSpecifications <-
  read.csv("resultsDataModelSpecification.csv")
# Cleaning up any tables in memory:
suppressWarnings(rm(
  list = SqlRender::snakeCaseToCamelCase(dataModelSpecifications$tableName)
))
onStop(function() {
  if (DBI::dbIsValid(connectionPool)) {
    writeLines("Closing database pool")
    pool::poolClose(connectionPool)
  }
})

resultsTablesOnServer <-
  tolower(DatabaseConnector::dbListTables(connectionPool, schema = resultsDatabaseSchema))

showAnnotation <- FALSE
if (enableAnnotation &
  "annotation" %in% resultsTablesOnServer &
  "annotation_link" %in% resultsTablesOnServer &
  "annotation_attributes" %in% resultsTablesOnServer) {
  showAnnotation <- TRUE
  options("showDiagnosticsExplorerAnnotation" = TRUE)
} else {
  enableAnnotation <- FALSE
  showAnnotation <- FALSE
  enableAuthorization <- FALSE
}

dataSource <-
  createDatabaseDataSource(
    connection = connectionPool,
    resultsDatabaseSchema = resultsDatabaseSchema,
    vocabularyDatabaseSchema = resultsDatabaseSchema,
    dbms = dbms
  )

# Init tables in global session
initializeTables(dataSource, dataModelSpecifications)

prettyTable1Specifications <- readr::read_csv(
  file = "Table1SpecsLong.csv",
  col_types = readr::cols(),
  guess_max = min(1e7),
  lazy = FALSE
)
analysisIdInCohortCharacterization <- c(
  1, 3, 4, 5, 6, 7,
  203, 403, 501, 703,
  801, 901, 903, 904,
  -301, -201
)

analysisIdInTemporalCharacterization <- c(
  101, 401, 501, 701,
  -301, -201
)
