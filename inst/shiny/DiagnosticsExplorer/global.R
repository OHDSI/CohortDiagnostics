library(magrittr)

### Change this lane if deploying shiny files directly with sqlite database
sqliteDbPath <- file.path("data", "MergedCohortDiagnosticsData.sqlite")

source("R/StartUpScripts.R")
source("R/DisplayFunctions.R")
source("R/Tables.R")
source("R/Plots.R")
source("R/Results.R")

appVersionNum <- "Version: 2.2.1"
appInformationText <- paste("Powered by OHDSI Cohort Diagnostics application", paste0(appVersionNum, "."))
appInformationText <- paste0(appInformationText, 
                             "Application was last initated on ",
                             lubridate::now(tzone = "EST"),
                             " EST. Cohort Diagnostics website is at https://ohdsi.github.io/CohortDiagnostics/")


if (exists("shinySettings")) {
  writeLines("Using settings provided by user")
  connectionDetails <- shinySettings$connectionDetails
  dbms <- connectionDetails$dbms
  resultsDatabaseSchema <- shinySettings$resultsDatabaseSchema
  vocabularyDatabaseSchemas <- shinySettings$vocabularyDatabaseSchemas
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

loadResultsTable("database_info", required = TRUE)
loadResultsTable("cohort", required = TRUE)
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


if (exists("databaseInfo")) {
  databaseInfo <- get("databaseInfo")
  if (nrow(databaseInfo) > 0 &&
      "vocabularyVersion" %in% colnames(databaseInfo)) {
    databaseInfo <- databaseInfo %>%
      dplyr::mutate(
        databaseIdWithVocabularyVersion = paste0(databaseId, " (", .data$vocabularyVersion, ")")
      )
    database <- databaseInfo
  }
}

if (exists("cohort")) {
  cohort <- get("cohort")
  cohort <- cohort %>%
    dplyr::arrange(.data$cohortId) %>%
    dplyr::mutate(shortName = paste0("C", dplyr::row_number())) %>%
    dplyr::mutate(compoundName = paste0(.data$shortName, ": ", .data$cohortName,"(", .data$cohortId, ")"))
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
