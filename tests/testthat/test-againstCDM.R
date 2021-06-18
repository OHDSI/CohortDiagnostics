# Disabling until new version of DatabaseConnector is released:
library(testthat)
library(CohortDiagnostics)

folder <- tempfile()
dir.create(folder, recursive = TRUE)
withr::defer({
  unlink(folder)
}, testthat::teardown_env())

test_that("Cohort instantiation", {
  CohortDiagnostics::instantiateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "eunomia",
    vocabularyDatabaseSchema = "eunomia",
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    packageName = "CohortDiagnostics",
    cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
    generateInclusionStats = TRUE,
    createCohortTable = TRUE,
    inclusionStatisticsFolder = file.path(folder, "incStats")
  )
  
  # set up new connection and check if the cohort was instantiated Disconnect after
  testthat::expect_true(CohortDiagnostics:::checkIfCohortInstantiated(connectionDetails = connectionDetails,
                                                                      cohortDatabaseSchema = cohortDatabaseSchema,
                                                                      cohortTable = cohortTable,
                                                                      cohortIds = 17492))
  
  # Expect cohort table to have atleast 0 records
  testthat::expect_gte(CohortDiagnostics:::renderTranslateQuerySql(connectionDetails = connectionDetails,
                                                                   sql = "select count(*) from @cohort_database_schema.@cohort_table;",
                                                                   cohort_database_schema = cohortDatabaseSchema,
                                                                   cohort_table = cohortTable), 0)
  
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
      cdmDatabaseSchema = "eunomia",
      vocabularyDatabaseSchema = "eunomia",
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "cdmV5",
      runInclusionStatistics = TRUE,
      runBreakdownIndexEvents = TRUE,
      runCohortCharacterization = TRUE,
      runTemporalCohortCharacterization = FALSE,
      runCohortOverlap = TRUE,
      runIncidenceRate = FALSE,
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = TRUE,
      runTimeDistributions = TRUE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental")
    )
  )
  
  testthat::expect_true(file.exists(file.path(
    folder, "export", "Results_CDMv5.zip"
  )))
  
  secondTime <- system.time(
    CohortDiagnostics::runCohortDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "eunomia",
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "cdmV5",
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
  
  # generate premerged file
  CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = file.path(folder, "export"))
  testthat::expect_true(file.exists(file.path(folder, "export", "PreMerged.RData")))
})


test_that("Query premerged file", {
  
  dataSourcePreMergedFile <- CohortDiagnostics::createFileDataSource(
    premergedDataFile = file.path(folder, "export", "PreMerged.RData")
  )
  
  cohortCountFromFile <- CohortDiagnostics::getResultsFromCohortCount(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(14906, 14907, 14909, 17492, 17493, 18342, 18345, 18346, 18347, 18348, 18349, 18350, 21402),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(cohortCountFromFile) > 0)
  
  timeSeriesFromFile <- CohortDiagnostics::getResultsFromTimeSeries(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(timeSeriesFromFile) >= 0)
  
  timeDistributionFromFile <- CohortDiagnostics::getResultsFromTimeDistribution(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(timeDistributionFromFile) >= 0)
  
  incidenceRateFromFile <- CohortDiagnostics::getResultsFromIncidenceRate(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(incidenceRateFromFile) >= 0) # no data in eunomia
  
  inclusionRulesFromFile <- CohortDiagnostics::getResultsFromInclusionRuleStatistics(
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(inclusionRulesFromFile) >= 0)
  
  indexEventBreakdownFromFile <- CohortDiagnostics::getResultsFromIndexEventBreakdown(
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(indexEventBreakdownFromFile) >= 0)
  
  visitContextFromFile <- CohortDiagnostics::getResultsFromVisitContext(
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(visitContextFromFile) >= 0)
  
  includedConceptFromFile <- CohortDiagnostics::getResultsFromIncludedConcept(
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(includedConceptFromFile) >= 0)
  
  orphanConceptFromFile <- CohortDiagnostics::getResultsFromOrphanConcept(
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(orphanConceptFromFile) >= 0)
})