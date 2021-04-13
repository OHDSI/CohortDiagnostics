# Disabling until new version of DatabaseConnector is released:
library(testthat)
library(CohortDiagnostics)
library(Eunomia)

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
vocabularyDatabaseSchema <- cohortDatabaseSchema
cohortTable <- "cohort"
oracleTempSchema <- NULL
folder <- tempfile()
dir.create(folder, recursive = TRUE)

test_that("Cohort instantiation", {
  CohortDiagnostics::instantiateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    oracleTempSchema = oracleTempSchema,
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

test_that("Cohort diagnostics in incremental mode", {
  firstTime <- system.time(
    CohortDiagnostics::runCohortDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      oracleTempSchema = oracleTempSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "Eunomia",
      runInclusionStatistics = TRUE,
      runBreakdownIndexEvents = TRUE,
      runCohortCharacterization = TRUE,
      runTemporalCohortCharacterization = TRUE,
      runCohortOverlap = TRUE,
      runIncidenceRate = TRUE,
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = TRUE,
      runTimeDistributions = TRUE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental")
    )
  )
  
  testthat::expect_true(file.exists(file.path(
    folder, "export", "Results_Eunomia.zip"
  )))
  
  secondTime <- system.time(
    CohortDiagnostics::runCohortDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      oracleTempSchema = oracleTempSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "Eunomia",
      runInclusionStatistics = TRUE,
      runBreakdownIndexEvents = TRUE,
      runCohortCharacterization = TRUE,
      runCohortOverlap = TRUE,
      runIncidenceRate = TRUE,
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = TRUE,
      runTimeDistributions = TRUE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental")
    )
  )
  testthat::expect_lt(secondTime[1], firstTime[1])
})

unlink(folder, recursive = TRUE)
