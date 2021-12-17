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
folder <- "s:/temp"

# Drop temp tables not cleaned up:
# connection = connect(connectionDetails)
# dropEmulatedTempTables(connection = connection, tempEmulationSchema = cohortDatabaseSchema)
# disconnect(connection)

# RedShift 
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                connectionString = keyring::key_get("redShiftConnectionStringOhdaMdcd"),
                                                                user = keyring::key_get("redShiftUserName"),
                                                                password = keyring::key_get("redShiftPassword"))

cdmDatabaseSchema <- "cdm_truven_mdcd_v1734"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "cohortdiagnostics_test"
folder <- "s:/temp/RS"


# Cohort generation using CohortDiagnostics' instantiateCohortSet function -------------------------------

test_that("Cohort instantiation", {
  instantiateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = cohortDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    packageName = "CohortDiagnostics",
    cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
    generateInclusionStats = TRUE,
    createCohortTable = TRUE,
    inclusionStatisticsFolder = file.path(folder, "incStats")
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


# Cohort instantiation using CohortGenerator -----------------------------------------------------------
cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSetFromPackage(packageName = "CohortDiagnostics",
                                                                          fileName = "settings/CohortsToCreateForTesting.csv")
cohortDefinitionSet <- cohortDefinitionSet[1:2, ] # Two cohorts is enough to test everything
tableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                    cohortDatabaseSchema = cohortDatabaseSchema,
                                    cohortTableNames = tableNames)
CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   tempEmulationSchema = cohortDatabaseSchema,
                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                   cohortTableNames = tableNames,
                                   cohortDefinitionSet = cohortDefinitionSet)

# Test CohortDiagnostics -------------------------------------------------------------------------
test_that("Cohort diagnostics", {
  
  cohortDefinitionSet$atlasId <- cohortDefinitionSet$cohortId
  
  
  runCohortDiagnostics(packageName = "CohortDiagnostics",
                       cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
                       connectionDetails = connectionDetails,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       cohortDatabaseSchema = cohortDatabaseSchema,
                       cohortTable = cohortTable,
                       databaseId = "Synpuf",
                       exportFolder =  file.path(folder, "export"),
                       runBreakdownIndexEvents = TRUE,
                       runCohortCharacterization = TRUE,
                       runTemporalCohortCharacterization = TRUE,
                       runCohortOverlap = TRUE,
                       runIncidenceRate = TRUE,
                       runIncludedSourceConcepts = TRUE,
                       runOrphanConcepts = TRUE,
                       runTimeDistributions = TRUE)
  
  
  executeDiagnostics(
    cohortDefinitionSet = cohortDefinitionSet,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = cohortDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    inclusionStatisticsFolder = file.path(folder, "incStats"),
    exportFolder =  file.path(folder, "export"),
    databaseId = "SynPuf",
    runBreakdownIndexEvents = TRUE,
    runCohortCharacterization = TRUE,
    runTemporalCohortCharacterization = TRUE,
    runCohortOverlap = TRUE,
    runIncidenceRate = TRUE,
    runIncludedSourceConcepts = TRUE,
    runOrphanConcepts = TRUE,
    runTimeDistributions = TRUE,
    minCellCount = 5
  )
  
  
  testthat::expect_true(file.exists(file.path(
    folder, "export", "Results_SynPuf.zip"
  )))
})
