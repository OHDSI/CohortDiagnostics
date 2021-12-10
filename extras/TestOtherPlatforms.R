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
# tables <- getTableNames(connection, cohortDatabaseSchema)
# tables <- tables[grepl("YEA7", tables)]
# tables <- tolower(paste(cohortDatabaseSchema, tables, sep = "."))
# sql <- paste(sprintf("TRUNCATE TABLE %s; DROP TABLE %s;", tables, tables), collapse = "\n")
# executeSql(connection, sql)
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
