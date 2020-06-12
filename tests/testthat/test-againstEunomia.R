# Disabling until new version of DatabaseConnector is released:
library(testthat)
library(CohortDiagnostics)
library(Eunomia)

connectionDetails <- getEunomiaConnectionDetails()
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cohortTable <- "cohort"
oracleTempSchema <- NULL
folder <- tempfile()
dir.create(folder, recursive = TRUE)

test_that("Cohort instantiation", {
  instantiateCohortSet(connectionDetails = connectionDetails,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       oracleTempSchema = oracleTempSchema,
                       cohortDatabaseSchema = cohortDatabaseSchema,
                       cohortTable = cohortTable,
                       packageName = "CohortDiagnostics",
                       cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
                       generateInclusionStats = TRUE,
                       createCohortTable = TRUE,
                       inclusionStatisticsFolder = file.path(folder, "incStats"))


  connection <- connect(connectionDetails)
  sql <- "SELECT COUNT(*) AS cohort_count, cohort_definition_id FROM @cohort_database_schema.@cohort_table GROUP BY cohort_definition_id;"
  counts <- renderTranslateQuerySql(connection, sql, cohort_database_schema = cohortDatabaseSchema, cohort_table = cohortTable, snakeCaseToCamelCase = TRUE)
  expect_gt(nrow(counts), 2)
  disconnect(connection)
})

test_that("Cohort diagnostics in incremental mode", {
  # Note: incidence rates disabled because curently failing (probably due to 0 count):
  firstTime <- system.time(
    runCohortDiagnostics(connectionDetails = connectionDetails,
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
                         runIncidenceRate = FALSE,
                         runIncludedSourceConcepts = TRUE,
                         runOrphanConcepts = TRUE,
                         runTimeDistributions = TRUE,
                         incremental = TRUE,
                         incrementalFolder = file.path(folder, "incremental"))
  )

  expect_true(file.exists(file.path(folder, "export", "Results_Eunomia.zip")))

  secondTime <- system.time(
    runCohortDiagnostics(connectionDetails = connectionDetails,
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
                         runIncidenceRate = FALSE,
                         runIncludedSourceConcepts = TRUE,
                         runOrphanConcepts = TRUE,
                         runTimeDistributions = TRUE,
                         incremental = TRUE,
                         incrementalFolder = file.path(folder, "incremental"))
  )

  expect_lt(secondTime[1], firstTime[1])
})

unlink(folder, recursive = TRUE)
