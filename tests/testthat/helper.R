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
    cohortDefinitionSet <- cohortDefinitionSet %>% dplyr::filter(.data$cohortId %in% cohortIds)
  }

  if (useSubsets) {
    cohortDefinitionSet <- CohortGenerator::addCohortSubsetDefinition(cohortDefinitionSet, getDefaultSubsetDefinition(), targetCohortIds = c(18345))
  }

  cohortDefinitionSet$checksum <- computeChecksum(cohortDefinitionSet$sql)

  return(cohortDefinitionSet)
}

#' Use to create test fixture for shiny tests
#' Required when data model changes
createTestShinyDb <- function(connectionDetails = Eunomia::getEunomiaConnectionDetails(),
                              cohortDefinitionSet = loadTestCohortDefinitionSet(),
                              outputPath = "inst/shiny/DiagnosticsExplorer/tests/testDb.sqlite",
                              cohortTable = "cohort",
                              vocabularyDatabaseSchema = "main",
                              cohortDatabaseSchema = "main",
                              cdmDatabaseSchema = "main", 
                              ...) {
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
    incremental = FALSE,
    ...
  )

  createMergedResultsFile(
    dataFolder = file.path(folder, "export"),
    sqliteDbPath = outputPath,
    overwrite = TRUE
  )
}
