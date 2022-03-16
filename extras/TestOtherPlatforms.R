# Performing tests outside github
library(CohortDiagnostics)
library(CohortGenerator)
library(SkeletonCohortDiagnostics)
library(testthat)



# Settings -----------------------------------------------------------
cdmDatabaseSchema <- "CDMV5"
cohortDatabaseSchema <- "OHDSI"
vocabularyDatabaseSchema <- "CDMV5"
rootFolder <- file.path("d:", "temp", "test")
outputFolder <- file.path(rootFolder, "results")
incrementalFolder <- file.path(rootFolder, "incremental")
minCellCountValue <- 2


cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
# get cohort definitions from study package
cohortDefinitionSet <-
  dplyr::tibble(CohortGenerator::getCohortDefinitionSet(packageName = "SkeletonCohortDiagnosticsStudy", 
                                                        cohortFileNameValue = "cohortId",
                                                        settingsFileName = file.path("settings", "CohortsToCreate.csv")))

# BigQuery ---------------------------------
# connectionDetails <- createConnectionDetails(dbms = "bigquery",
#                                              connectionString = keyring::key_get("bigQueryConnString"),
#                                              user = "",
#                                              password = "")
# tempEmulationSchema <- NULL

# cdmDatabaseSchema <- "synpuf_2m"
# cohortDatabaseSchema <- "synpuf_2m_results"

# Drop temp tables not cleaned up:
# connection = connect(connectionDetails)
# dropEmulatedTempTables(connection = connection, tempEmulationSchema = cohortDatabaseSchema)
# disconnect(connection)

# RedShift  ---------------------------------
# connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
#                                                                 connectionString = keyring::key_get("redShiftConnectionStringOhdaMdcd"),
#                                                                 user = keyring::key_get("redShiftUserName"),
#                                                                 password = keyring::key_get("redShiftPassword"))
# tempEmulationSchema <- NULL

# cdmDatabaseSchema <- "cdm_truven_mdcd_v1734"
# cohortDatabaseSchema <- "scratch_mschuemi"
# cohortTable <- "cohortdiagnostics_test"
# folder <- "s:/temp/RS"


# Oracle test ---------------------------------
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "oracle",
                                                                server = Sys.getenv("CDM5_ORACLE_SERVER"),
                                                                user = Sys.getenv("CDM5_ORACLE_USER"),
                                                                password = Sys.getenv("CDM5_ORACLE_PASSWORD"))
tempEmulationSchema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")

# postgres test ---------------------------------
# connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
#                                                                 server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
#                                                                 user = Sys.getenv("CDM5_POSTGRESQL_USER"),
#                                                                 password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
# tempEmulationSchema <- NULL

# SqlServer test ---------------------------------
# connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sql server",
#                                                                 server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
#                                                                 user = Sys.getenv("CDM5_SQL_SERVER_USER"),
#                                                                 password = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"))
# cdmDatabaseSchema = 'cdmv5.dbo'
# cohortDatabaseSchema = 'ohdsi.dbo'
# tempEmulationSchema <- NULL

# Cohort generation using CohortDiagnostics' instantiateCohortSet function -------------------------------
unlink(x = rootFolder, recursive = TRUE, force = TRUE)
test_that("Cohort instantiation", {
  
  # Next create the tables on the database
  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = TRUE
  )
  
  # Generate the cohort set
  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    incrementalFolder = incrementalFolder,
    incremental = TRUE
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
      cohort_table = cohortTableNames$cohortTable,
      snakeCaseToCamelCase = TRUE
    )
  testthat::expect_gt(nrow(counts), 2)
  DatabaseConnector::disconnect(connection)
})



test_that("Concept set diagnostics - with cohort table but not instantiated", {
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
  conceptSetDiagnostics <-
    runConceptSetDiagnostics(
      connectionDetails = connectionDetails,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = cdmDatabaseSchema,
      cohorts = cohortDefinitionSet,
      cohortIds = NULL,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTableNames$cohortTable,
      keep2BillionConceptId = TRUE,
      runConceptSetOptimization = TRUE,
      runExcludedConceptSet = TRUE,
      runOrphanConcepts = FALSE,
      runBreakdownIndexEvents = TRUE,
      runBreakdownIndexEventRelativeDays = c(-5:5),
      runIndexDateConceptCoOccurrence = TRUE,
      runStandardToSourceMappingCount = TRUE,
      runConceptCount = TRUE,
      runConceptCountByCalendarPeriod = TRUE,
      minCellCount = 0
    )
  
  testthat::expect_true(object = Andromeda::isAndromeda(conceptSetDiagnostics))
  
})


# Test CohortDiagnostics -------------------------------------------------------------------------
test_that("Cohort diagnostics", {
  CohortDiagnostics::executeDiagnostics(
    cohortDefinitionSet = cohortDefinitionSet,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    exportFolder = outputFolder,
    databaseId = "SynPuf",
    minCellCount = minCellCountValue,
    incremental = TRUE,
    incrementalFolder = incrementalFolder
  )
  testthat::expect_true(file.exists(file.path(
    folder, "export", "Results_SynPuf.zip"
  )))
})


