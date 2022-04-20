# Tests for platforms not available to Travis
library(CohortDiagnostics)
library(testthat)

# Settings -----------------------------------------------------------

# BigQuery
connectionDetails <- createConnectionDetails(dbms = "bigquery",
                                             connectionString = keyring::key_get("bigQueryConnString"),
                                             user = "",
                                             password = "")

cdmDatabaseSchema <- "synpuf_2m"
cohortDatabaseSchema <- "synpuf_2m_results"
cohortTable <- "cohortdiagnostics_test"
options("sqlRenderTempEmulationSchema" = "synpuf_2m_results")
folder <- "s:/temp/CdBqTest"

# Drop temp tables not cleaned up:
connection = connect(connectionDetails)
dropEmulatedTempTables(connection = connection)
disconnect(connection)

# Cohort generation using CohortDiagnostics' instantiateCohortSet function -------------------------------
loadTestCohortDefinitionSet <- function(cohortIds = NULL) {
  creationFile <- file.path("tests/testthat/cohorts", "CohortsToCreate.csv")
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    settingsFileName = creationFile,
    sqlFolder = "tests/testthat/cohorts",
    jsonFolder = "tests/testthat/cohorts",
    cohortFileNameValue = c("cohortId")
  )
  if (!is.null(cohortIds)) {
    cohortDefinitionSet <- cohortDefinitionSet %>% dplyr::filter(.data$cohortId %in% cohortIds)
  }
  
  cohortDefinitionSet
}
cohortIds <- c(17492, 17493, 17720, 14909, 18342, 18345, 18346, 18347, 18348, 18349, 18350, 14906)
cohortDefinitionSet <- loadTestCohortDefinitionSet(cohortIds)


test_that("Cohort instantiation", {
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
  # Next create the tables on the database
  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = FALSE
  )
  
  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )

  connection <- DatabaseConnector::connect(connectionDetails)
  sql <-
    "SELECT COUNT(*) AS cohort_count, cohort_definition_id
  FROM @cohort_database_schema.@cohort_table
  GROUP BY cohort_definition_id;"
  counts <-
    DatabaseConnector::renderTranslateQuerySql(
      connection,
      sql,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      snakeCaseToCamelCase = TRUE
    )
  testthat::expect_gt(nrow(counts), 2)
  DatabaseConnector::disconnect(connection)
})

# Test CohortDiagnostics -------------------------------------------------------------------------
test_that("Cohort diagnostics", {
  
  executeDiagnostics(connectionDetails = connectionDetails,
                     cohortDefinitionSet = cohortDefinitionSet,
                     databaseId = "Synpuf",
                     exportFolder =  file.path(folder, "export"),
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     cohortTableNames = cohortTableNames,
                     runInclusionStatistics = TRUE,
                     runIncludedSourceConcepts = TRUE,
                     runOrphanConcepts = TRUE,
                     runTimeSeries = TRUE,
                     runVisitContext = TRUE,
                     runBreakdownIndexEvents = TRUE,
                     runIncidenceRate = TRUE,
                     runCohortRelationship = TRUE,
                     runTemporalCohortCharacterization = TRUE)

  testthat::expect_true(file.exists(file.path(
    folder, "export", "Results_SynPuf.zip"
  )))
})
