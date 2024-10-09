#' utility function to make sure connection is closed after usage
with_dbc_connection <- function(connection, code) {
  on.exit({
    DatabaseConnector::disconnect(connection)
  })
  eval(substitute(code), envir = connection, enclos = parent.frame())
}

#' Only works with postgres > 9.4
.pgTableExists <- function(connection, schema, tableName) {
  return(!is.na(
    DatabaseConnector::renderTranslateQuerySql(
      connection,
      "SELECT to_regclass('@schema.@table');",
      table = tableName,
      schema = schema
    )
  )[[1]])
}

getDefaultSubsetDefinition <- function() {
  CohortGenerator::createCohortSubsetDefinition(
    name = "subsequent GI bleed with 365 days prior observation",
    definitionId = 1,
    subsetOperators = list(
      # here we are saying 'first subset to only those patients in cohort 1778213'
      CohortGenerator::createCohortSubset(
        name = "with GI bleed within 30 days of cohort start",
        cohortIds = 14909,
        cohortCombinationOperator = "any",
        negate = FALSE,
        startWindow = CohortGenerator::createSubsetCohortWindow(
          startDay = 0,
          endDay = 30,
          targetAnchor = "cohortStart"
        ),
        endWindow = CohortGenerator::createSubsetCohortWindow(
          startDay = 0,
          endDay = 9999999,
          targetAnchor = "cohortStart"
        )
      ),
      # Next, subset to only those with 365 days of prior observation
      CohortGenerator::createLimitSubset(
        name = "Observation of at least 365 days prior",
        priorTime = 365,
        followUpTime = 0,
        limitTo = "firstEver"
      )
    )
  )
}

# Create a cohort definition set from test cohorts
loadTestCohortDefinitionSet <- function(cohortIds = NULL, useSubsets = TRUE) {
  if (grepl("testthat", getwd())) {
    cohortPath <- "cohorts"
  } else {
    cohortPath <- file.path("tests", "testthat", "cohorts")
  }

  creationFile <- file.path(cohortPath, "CohortsToCreate.csv")
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = creationFile,
    sqlFolder = cohortPath,
    jsonFolder = cohortPath,
    cohortFileNameValue = c("cohortId")
  )
  if (!is.null(cohortIds)) {
    cohortDefinitionSet <- dplyr::filter(cohortDefinitionSet, .data$cohortId %in% cohortIds)
  }

  if (useSubsets) {
    cohortDefinitionSet <- CohortGenerator::addCohortSubsetDefinition(cohortDefinitionSet, getDefaultSubsetDefinition(), targetCohortIds = c(18345))
  }

  cohortDefinitionSet$checksum <- CohortGenerator::computeChecksum(cohortDefinitionSet$sql)

  return(dplyr::tibble(cohortDefinitionSet))
}

#' Use to create test fixture for shiny tests
#' Required when data model changes
createTestShinyDb <- function(connectionDetails = Eunomia::getEunomiaConnectionDetails(),
                              cohortDefinitionSet = loadTestCohortDefinitionSet(),
                              outputPath = "inst/shiny/DiagnosticsExplorer/tests/testDb.sqlite",
                              cohortTable = "cohort",
                              vocabularyDatabaseSchema = "main",
                              cohortDatabaseSchema = "main",
                              cdmDatabaseSchema = "main") {
  folder <- tempfile()
  dir.create(folder)
  on.exit(unlink(folder, recursive = TRUE, force = TRUE))

  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
  # Next create the tables on the database
  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = FALSE
  )

  # Generate the cohort set
  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )

  executeDiagnostics(
    cohortDefinitionSet = cohortDefinitionSet,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    exportFolder = file.path(folder, "export"),
    databaseId = "Eunomia",
    incremental = FALSE
  )

  createMergedResultsFile(
    dataFolder = file.path(folder, "export"),
    sqliteDbPath = outputPath,
    overwrite = TRUE
  )
}



createCustomCdm <- function(jsonDataFilePath){
  
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  
  tablesToTruncate <- c("person", "observation_period", "visit_occurrence", "visit_detail", 
                        "condition_occurrence", "drug_exposure", "procedure_occurrence", 
                        "device_exposure", "measurement", "observation", "death", "note", 
                        "note_nlp", "specimen", "fact_relationship", "location", "care_site", 
                        "provider", "payer_plan_period", "cost", "drug_era", "dose_era", 
                        "condition_era", "metadata", "cdm_source", "cohort_definition", 
                        "attribute_definition")
  
  connection <- DatabaseConnector::connect(connectionDetails)
  
  # remove tables that are not vocabulary tables
  for (tbl in tablesToTruncate) {
    DatabaseConnector::executeSql(connection, paste("delete from ", tbl, ";"), progressBar = FALSE)
  }
  
  jsonData <- jsonlite::fromJSON(system.file(jsonDataFilePath, package = "CohortDiagnostics"))
  
  # Convert the JSON data into a data frame and append it to the blank CDM
  for (tableName in names(jsonData)) {
    patientData <- as.data.frame(jsonData[[tableName]]) %>% 
      dplyr::mutate(dplyr::across(dplyr::matches("date$"), ~as.Date(.))) %>% 
      dplyr::mutate(dplyr::across(dplyr::matches("datetime$"), ~as.POSIXct(., format = "")))
    
    DBI::dbAppendTable(connection, tableName, patientData)
  }

  cli::cli_alert_success("Patients pushed to blank CDM successfully")
  
  return(connectionDetails)
  
  }




