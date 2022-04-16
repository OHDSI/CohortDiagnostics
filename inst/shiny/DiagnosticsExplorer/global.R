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
appInformationText <- paste0(appInformationText,
                             "Application was last initated on ",
                             lubridate::now(tzone = "EST"),
                             " EST. Cohort Diagnostics website is at https://ohdsi.github.io/CohortDiagnostics/")

#### Set enableAnnotation to true to enable annotation in deployed apps
#### Not recommended outside of secure firewalls deployments
enableAnnotation <- TRUE
enableAuthorization <- TRUE

### if you need a way to authorize users
### generate hash using code like digest::digest("diagnostics",algo = "sha512")
### store in external file called UserCredentials.csv - with fields userId, hashCode
### place the file in the root folder

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
  connectionDetails <- shinySettings$connectionDetails
  dbms <- connectionDetails$dbms
  resultsDatabaseSchema <- shinySettings$resultsDatabaseSchema
  vocabularyDatabaseSchemas <- shinySettings$vocabularyDatabaseSchemas
  enableAnnotation <- shinySettings$enableAnnotation
} else if (file.exists(sqliteDbPath)){
  writeLines("Using data directory")
  sqliteDbPath <- normalizePath(sqliteDbPath)
  resultsDatabaseSchema <- "main"
  vocabularyDatabaseSchemas <- "main"
  dbms <- "sqlite"
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqliteDbPath)
} else {
  writeLines("Connecting to remote database")
  dbms <- Sys.getenv("shinydbDatabase", unset = "postgresql")
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = dbms,
    server = Sys.getenv("shinydbServer"),
    port = Sys.getenv("shinydbPort", unset = 5432),
    user = Sys.getenv("shinydbUser"),
    password = Sys.getenv("shinydbPw")
  )
  
  resultsDatabaseSchema <- Sys.getenv("shinydbResultsSchema", unset = "thrombosisthrombocytopenia")
  vocabularyDatabaseSchemas <- resultsDatabaseSchema
  alternateVocabularySchema <-  Sys.getenv("shinydbVocabularySchema", unset = c("vocabulary"))
  
  vocabularyDatabaseSchemas <-
    setdiff(x = c(vocabularyDatabaseSchemas, alternateVocabularySchema),
            y = resultsDatabaseSchema) %>%
    unique() %>%
    sort()
}

if (is(connectionDetails$server, "function")) {
  connectionPool <-
    pool::dbPool(
      drv = DatabaseConnector::DatabaseConnectorDriver(),
      dbms = connectionDetails$dbms,
      server = connectionDetails$server(),
      port = connectionDetails$port(),
      user = connectionDetails$user(),
      password = connectionDetails$password(),
      connectionString = connectionDetails$connectionString()
    )
} else {
  # For backwards compatibility with older versions of DatabaseConnector:
  connectionPool <-
    pool::dbPool(
      drv = DatabaseConnector::DatabaseConnectorDriver(),
      dbms = connectionDetails$dbms,
      server = connectionDetails$server,
      port = connectionDetails$port,
      user = connectionDetails$user,
      password = connectionDetails$password,
      connectionString = connectionDetails$connectionString
    )
}

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
} else {
  enableAnnotation <- FALSE
  showAnnotation <- FALSE
  enableAuthorization <- FALSE
}


loadResultsTable("database", required = TRUE)
loadResultsTable("cohort", required = TRUE)
loadResultsTable("metadata", required = TRUE)
loadResultsTable("temporal_time_ref")
loadResultsTable("temporal_analysis_ref")
loadResultsTable("concept_sets")
loadResultsTable("cohort_count", required = TRUE)

for (table in c(dataModelSpecifications$tableName)) {
  #, "recommender_set"
  if (table %in% resultsTablesOnServer &&
      !exists(SqlRender::snakeCaseToCamelCase(table)) &&
      !isEmpty(table)) {
    #if table is empty, nothing is returned because type instability concerns.
    assign(SqlRender::snakeCaseToCamelCase(table),
           dplyr::tibble())
  }
}

dataSource <-
  createDatabaseDataSource(
    connection = connectionPool,
    resultsDatabaseSchema = resultsDatabaseSchema,
    vocabularyDatabaseSchema = resultsDatabaseSchema,
    dbms = dbms
  )

if (exists("database")) {
  if (nrow(database) > 0 &&
      "vocabularyVersion" %in% colnames(database)) {
    database <- database %>%
      dplyr::mutate(
        databaseIdWithVocabularyVersion = paste0(databaseId, " (", .data$vocabularyVersion, ")")
      )
  }
}

if (exists("cohort")) {
  cohort <- get("cohort")
  cohort <- cohort %>%
    dplyr::arrange(.data$cohortId) %>%
    dplyr::mutate(shortName = paste0("C", .data$cohortId)) %>%
    dplyr::mutate(compoundName = paste0(.data$shortName, ": ", .data$cohortName))
}

if (exists("database")) {
  database <- get("database")
  databaseMetadata <- processMetadata(get("metadata"))
  database <- database %>%
    dplyr::distinct() %>%
    dplyr::mutate(id = dplyr::row_number()) %>%
    dplyr::mutate(shortName = paste0("D", .data$id)) %>% 
    dplyr::left_join(databaseMetadata, 
                     by = "databaseId") %>% 
    dplyr::relocate(.data$id, .data$databaseId, .data$shortName)
  rm("databaseMetadata")
}

if (exists("temporalTimeRef")) {
  temporalChoices <-
    getResultsTemporalTimeRef(dataSource = dataSource)
  
  temporalCharacterizationTimeIdChoices <-
    temporalChoices %>%
    dplyr::arrange(.data$sequence)
  
  characterizationTimeIdChoices <-
    temporalChoices %>%
    dplyr::filter(.data$isTemporal == 0)  %>%
    dplyr::filter(.data$primaryTimeId == 1) %>%
    dplyr::arrange(.data$sequence) 
}

if (exists("temporalAnalysisRef")) {
  
  temporalAnalysisRef <- dplyr::bind_rows(
    temporalAnalysisRef,
    dplyr::tibble(
      analysisId = c(-201,-301),
      analysisName = c("CohortEraStart", "CohortEraOverlap"),
      domainId = "Cohort",
      isBinary = "Y",
      missingMeansZero = "Y"
    )
  )
  
  domainIdOptions <- temporalAnalysisRef %>% 
    dplyr::select(.data$domainId) %>% 
    dplyr::pull(.data$domainId) %>% 
    unique() %>% 
    sort()
  
  analysisNameOptions <- temporalAnalysisRef %>% 
    dplyr::select(.data$analysisName) %>% 
    dplyr::pull(.data$analysisName) %>% 
    unique() %>% 
    sort()
  
}

prettyTable1Specifications <- readr::read_csv(
  file = "Table1SpecsLong.csv",
  col_types = readr::cols(),
  guess_max = min(1e7),
  lazy = FALSE
)

analysisIdInCohortCharacterization <- c(1, 3, 4, 5, 6, 7,
                                        203, 403, 501, 703,
                                        801, 901, 903, 904,
                                        -301, -201)
analysisIdInTemporalCharacterization <- c(101, 401, 501, 701,
                                          -301, -201)
