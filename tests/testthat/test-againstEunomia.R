
library(testthat)
library(CohortDiagnostics)
library(Eunomia)

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
vocabularyDatabaseSchema <- cohortDatabaseSchema
cohortTable <- "cohort"
tempEmulationSchema <- NULL
folder <- tempfile()
dir.create(folder, recursive = TRUE)
minCellCountValue <- 5

connection <- DatabaseConnector::connect(connectionDetails)
withr::defer({
  DatabaseConnector::disconnect(connection)
  unlink(folder)
}, testthat::teardown_env())

test_that("Cohort instantiation", {
  expect_warning(
    instantiateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      generateInclusionStats = TRUE,
      createCohortTable = TRUE,
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  )

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

  cohortDefinitionSet <- loadCohortsFromPackage(
    packageName = "CohortDiagnostics",
    cohortToCreateFile = "settings/CohortsToCreateForTesting.csv"
  )

  # Testing usage of inclusion stats folder - backawards compatability
  expect_warning(
    executeDiagnostics(
      cohortDefinitionSet = cohortDefinitionSet,
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = c(17492),
      exportFolder = file.path(folder, "export"),
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      databaseId = "Eunomia",
      runInclusionStatistics = TRUE,
      runBreakdownIndexEvents = FALSE,
      runCohortCharacterization = FALSE,
      runTemporalCohortCharacterization = FALSE,
      runCohortOverlap = FALSE,
      runIncidenceRate = FALSE,
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runTimeDistributions = FALSE,
      runTimeSeries = FALSE,
      minCellCount = minCellCountValue,
      incremental = FALSE
    )
  )
})

test_that("Cohort diagnostics in incremental mode", {

  cohortDefinitionSet <- loadCohortsFromPackage(
    packageName = "CohortDiagnostics",
    cohortToCreateFile = "settings/CohortsToCreateForTesting.csv"
  )

  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
  # Next create the tables on the database
  CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                      cohortTableNames = cohortTableNames,
                                      cohortDatabaseSchema = cohortDatabaseSchema,
                                      incremental = FALSE)

  # Generate the cohort set
  CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                     cohortTableNames = cohortTableNames,
                                     cohortDefinitionSet = cohortDefinitionSet,
                                     incremental = FALSE)

  firstTime <- system.time(
    executeDiagnostics(
      cohortDefinitionSet = cohortDefinitionSet,
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      cohortIds = c(17492, 17493, 17720, 14909, 18342, 18345, 18346, 18347, 18348, 18349, 18350, 14906),
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
      runTimeSeries = TRUE,
      minCellCount = minCellCountValue,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental")
    )
  )
  
  testthat::expect_true(file.exists(file.path(
    folder, "export", "Results_Eunomia.zip"
  )))

  # We now run it with all cohorts without specifying ids - testing incremental mode
  secondTime <- system.time(
    executeDiagnostics(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      cohortDefinitionSet = cohortDefinitionSet,
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
      runTimeSeries = TRUE,
      minCellCount = minCellCountValue,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental")
    )
  )
  testthat::expect_lt(secondTime[1], firstTime[1])
  
  # generate premerged file
  preMergeDiagnosticsFiles(dataFolder = file.path(folder, "export"))
  testthat::expect_true(file.exists(file.path(folder, "export", "PreMerged.RData")))
  
  output <- read.csv(file.path(folder, "export", "covariate_value.csv"))
  expect_equal(output$sum_value[2], -minCellCountValue)
  expect_lt(output$mean[2], 0)
})
