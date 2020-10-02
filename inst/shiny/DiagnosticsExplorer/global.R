library(magrittr)

source("R/Tables.R")
source("R/Plots.R")
source("R/Results.R")

# Settings when running on server:

defaultLocalDataFolder <- "data"
defaultLocalDataFile <- "PreMerged.RData"

connectionPool <- NULL
defaultServer <- Sys.getenv("phenotypeLibraryDbServer")
defaultDatabase <- Sys.getenv("phenotypeLibraryDbDatabase")
defaultPort <- Sys.getenv("phenotypeLibraryDbPort")
defaultUser <- Sys.getenv("phenotypeLibraryDbUser")
defaultPassword <- Sys.getenv("phenotypeLibraryDbPassword")
defaultResultsSchema <- Sys.getenv("phenotypeLibraryDbResultsSchema")
defaultVocabularySchema <- Sys.getenv("phenotypeLibraryDbVocabularySchema")

defaultDatabaseMode <- TRUE # Use file system if FALSE

cohortBaseUrl <- "https://atlas.ohdsi.org/#/cohortdefinition/"
conceptBaseUrl <- "https://athena.ohdsi.org/search-terms/terms/"
cohortDiagnosticModeDefaultTitle <- "Cohort Diagnostics"
phenotypeLibraryModeDefaultTitle <- "Phenotype Library"

thresholdCohortSubjects <- 0
thresholdCohortEntries <- 0


if (!exists("shinySettings")) {
  writeLines("Using default settings")
  databaseMode <- defaultDatabaseMode
  if (databaseMode) {
    connectionPool <- pool::dbPool(
      drv = DatabaseConnector::DatabaseConnectorDriver(),
      dbms = "postgresql",
      server = paste(defaultServer, defaultDatabase, sep = "/"),
      port = defaultPort,
      user = defaultUser,
      password = defaultPassword
    )
    resultsDatabaseSchema <- defaultResultsSchema
    vocabularyDatabaseSchema <- defaultVocabularySchema
  } else {
    dataFolder <- defaultLocalDataFolder
  }
} else {
  writeLines("Using settings provided by user")
  databaseMode <- !is.null(shinySettings$server)
  if (databaseMode) {
    connectionPool <- pool::dbPool(
      drv = DatabaseConnector::DatabaseConnectorDriver(),
      dbms = "postgresql",
      server = paste(shinySettings$server, shinySettings$database, sep = "/"),
      port = shinySettings$port,
      user = shinySettings$user,
      password = shinySettings$password
    )
    resultsDatabaseSchema <- defaultResultsSchema
    vocabularyDatabaseSchema <- defaultVocabularySchema
  } else {
    dataFolder <- shinySettings$dataFolder
  }
}

dataModelSpecifications <- read.csv("resultsDataModelSpecification.csv")
# Cleaning up any tables in memory:
suppressWarnings(rm(list = SqlRender::snakeCaseToCamelCase(dataModelSpecifications$tableName)))

if (databaseMode) {

  onStop(function() {
    if (DBI::dbIsValid(connectionPool)) {
      writeLines("Closing database pool")
      pool::poolClose(connectionPool)
    }
  })
  
  resultsTablesOnServer <- tolower(DatabaseConnector::dbListTables(connectionPool, schema = resultsDatabaseSchema))
  
  loadResultsTable <- function(tableName, required = FALSE) {
    if (required || tableName %in% resultsTablesOnServer) {
      table <- DatabaseConnector::dbReadTable(connectionPool, 
                                              paste(resultsDatabaseSchema, tableName, sep = "."))
      colnames(table) <- SqlRender::snakeCaseToCamelCase(colnames(table))
      return(table)
    }
  }

  database <- loadResultsTable("database", required = TRUE)
  cohort <- loadResultsTable("cohort", required = TRUE)
  phenotypeDescription <- loadResultsTable("phenotype_description")
  temporalTimeRef <- loadResultsTable("temporal_time_ref")
  conceptSets <- loadResultsTable("concept_sets")
  
  # Create empty objects in memory for all other tables. This is used by the Shiny app to decide what tabs to show:
  for (table in dataModelSpecifications$tableName) {
    if (table %in% resultsTablesOnServer && !exists(SqlRender::snakeCaseToCamelCase(table))) {
      assign(SqlRender::snakeCaseToCamelCase(table), dplyr::tibble())
    }
  }
  
} else {
  localDataPath <- file.path(dataFolder, defaultLocalDataFile)
  if (!file.exists(localDataPath)) {
    stop(sprintf("Local data file %s does not exist.", localDataPath))
  }
  load(localDataPath)
} 

if (exists("temporalTimeRef")) {
  temporalCovariateChoices <- temporalTimeRef %>%
    dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
    dplyr::select(.data$timeId, .data$choices) %>% 
    dplyr::arrange(.data$timeId) %>% 
    dplyr::slice_head(n = 5)
}
