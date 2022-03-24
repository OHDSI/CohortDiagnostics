library(magrittr)

### Change this lane if deploying shiny files directly with sqlite database
sqliteDbPath <- file.path("data", "MergedCohortDiagnosticsData.sqlite")
source("R/StartUpScripts.R")
source("R/DisplayFunctions.R")
source("R/Tables.R")
source("R/Plots.R")
source("R/Results.R")
source("R/Annotation.R")

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
### you can store them as a comma separated array to object storedHash like below
storedHash <- c("52e6000f483fe4602b3234f1a686d69f2ca3219fea796ae601a573451944d2d1b130b630ba18e7b1ac06928443bf89fa6ab49ded04245e6d9c9ea1956967e01e",
                "4823cb731badf383a0b09cc09ac0c5904f315d256c66a7bbd5032efd2df39bc8eaf979333ed97f580c734b53965f2c2b57920239d78f8df031a0e198a8e5740c")

if (enableAuthorization) {
  if (!exists("storedHash") ||
      length(storedHash) == 0 || storedHash == "") {
    if (exists("storedHash")) {
      rm("storedHash")
    }
    enableAuthorization <- FALSE
  } else {
    enableAuthorization <- TRUE
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
    vocabularyDatabaseSchema = resultsDatabaseSchema
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
  temporalCovariateChoices <- get("temporalTimeRef") %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>%
    dplyr::arrange(.data$timeId)
}

if (exists("covariateRef")) {
  specifications <- readr::read_csv(
    file = "Table1Specs.csv",
    col_types = readr::cols(),
    guess_max = min(1e7)
  )
  prettyAnalysisIds <- specifications$analysisId
} else {
  prettyAnalysisIds <- c(0)
}
