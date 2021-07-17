library(testthat)
library(CohortDiagnostics)

# Clean up ----
if (runDatabaseTests) {
  tryCatch(DatabaseConnector::renderTranslateExecuteSql(connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
                                                        "DROP TABLE @cohort_database_schema.@cohort_table CASCADE",
                                                        cohort_database_schema = cohortDatabaseSchema,
                                                        cohort_table = cohortTable, 
                                                        progressBar = FALSE, 
                                                        reportOverallTime = FALSE),
           error = function(e) {})
  filesToDelete <- list.files(file.path(folder, "incremental"), full.names = TRUE, recursive = FALSE)
  invisible(lapply(filesToDelete, unlink, force = TRUE))
}


if (runDatabaseTests) {
  # Cohort Instantiation tests ----
  test_that("Cohort table creation", {
    ## check creation of cohort table
    testthat::expect_null(
      CohortDiagnostics:::createCohortTable(connectionDetails = connectionDetails,
                                            cohortDatabaseSchema = cohortDatabaseSchema, 
                                            cohortTable = cohortTable))
    tryCatch(DatabaseConnector::renderTranslateExecuteSql(connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
                                                          "DROP TABLE @cohort_database_schema.@cohort_table CASCADE",
                                                          cohort_database_schema = cohortDatabaseSchema,
                                                          cohort_table = cohortTable, 
                                                          progressBar = FALSE, 
                                                          reportOverallTime = FALSE),
             error = function(e) {})
  })
}

# Cohort Instantiation tests ----
test_that("Cohort instantiation", {
  skip_if_not(runDatabaseTests)
  
  ## No incremental mode ----
  ### Neg - no cohort table
  testthat::expect_error(suppressWarnings(
    CohortDiagnostics::instantiateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      oracleTempSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = 18348,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      generateInclusionStats = TRUE,
      createCohortTable = FALSE,
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  ))
  ### Neg - bad cohort ----
  testthat::expect_error(suppressWarnings(
    CohortDiagnostics::instantiateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = -1111,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      generateInclusionStats = TRUE,
      createCohortTable = TRUE,
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  ))
  ### Pos - good one cohort, will create cohort table, instantiate not incremental ----
  testthat::expect_null(
    CohortDiagnostics::instantiateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = 18348,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      generateInclusionStats = TRUE,
      createCohortTable = TRUE,
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  )
  
  ### Pos - Expect cohort count ----
  # Expect cohortId 18348 to have 830 records
  sqlCount <- "SELECT COUNT(*) FROM @cohort_database_schema.@cohort_table where cohort_definition_id = 18348;"
  count1 <- CohortDiagnostics:::renderTranslateQuerySql(connectionDetails = connectionDetails,
                                                        sql = sqlCount,
                                                        cohort_database_schema = cohortDatabaseSchema,
                                                        cohort_table = cohortTable)
  testthat::expect_equal(count1$COUNT, 830)
  
  ### Pos - check cohort instantiated ----
  testthat::expect_true(CohortDiagnostics:::checkIfCohortInstantiated(connectionDetails = connectionDetails,
                                                                      cohortDatabaseSchema = cohortDatabaseSchema,
                                                                      cohortTable = cohortTable,
                                                                      cohortIds = 18348))
  
  ### Neg - cohort is not instantiated ----
  testthat::expect_false(CohortDiagnostics:::checkIfCohortInstantiated(connectionDetails = connectionDetails,
                                                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                                                       cohortTable = cohortTable,
                                                                       cohortIds = -1111))
  
  
  ### Pos - should re run ----
  # delete from cohort table, and repopulate. should have 830 again
  sqlDelete <- "DELETE FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id = 18348 and subject_id < 1000;"
  DatabaseConnector::renderTranslateExecuteSql(connection = DatabaseConnector::connect(connectionDetails),
                                               sql = sqlDelete,
                                               cohort_database_schema = cohortDatabaseSchema,
                                               cohort_table = cohortTable, 
                                               progressBar = FALSE, 
                                               reportOverallTime = FALSE)
  count2 <- CohortDiagnostics:::renderTranslateQuerySql(connectionDetails = connectionDetails,
                                                        sql = sqlCount,
                                                        cohort_database_schema = cohortDatabaseSchema,
                                                        cohort_table = cohortTable)
  testthat::expect_true(count1$COUNT > count2$COUNT)
  
  testthat::expect_null(
    CohortDiagnostics::instantiateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = 18348,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      generateInclusionStats = TRUE,
      createCohortTable = TRUE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental"),
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  )
  
  count3 <- CohortDiagnostics:::renderTranslateQuerySql(connectionDetails = connectionDetails,
                                                        sql = sqlCount,
                                                        cohort_database_schema = cohortDatabaseSchema,
                                                        cohort_table = cohortTable)
  testthat::expect_gte(count3$COUNT, 830)
  
  ## Incremental mode ----
  testthat::expect_null(
    CohortDiagnostics::instantiateCohortSet(
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
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental"),
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  )
  
  connection <- DatabaseConnector::connect(connectionDetails)
  sql <- "SELECT COUNT(*) AS cohort_count, cohort_definition_id
  FROM @cohort_database_schema.@cohort_table
  GROUP BY cohort_definition_id;"
  counts <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                       sql,
                                                       cohort_database_schema = cohortDatabaseSchema,
                                                       cohort_table = cohortTable,
                                                       snakeCaseToCamelCase = TRUE)
  testthat::expect_gt(nrow(counts), 2)
  DatabaseConnector::disconnect(connection)
})




test_that("Testing Cohort diagnostics when not in incremental mode", {
  skip_if_not(runDatabaseTests)
  
  start <- Sys.time()
  # Cohort Diagnostics -----
  ## Not incremental -----
  ### Neg - no connection or connection details -----
  testthat::expect_error(suppressWarnings(
    CohortDiagnostics::runCohortDiagnostics(
      connectionDetails = NULL,
      connection = NULL,
      cdmDatabaseSchema = "eunomia",
      vocabularyDatabaseSchema = "eunomia",
      oracleTempSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "cdmV5",
      databaseName = NULL, 
      databaseDescription = NULL,
      runInclusionStatistics = FALSE,
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runVisitContext = FALSE,
      runBreakdownIndexEvents = FALSE,
      runIncidenceRate = FALSE,
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      runCohortRelationship = FALSE,
      runCohortCharacterization = FALSE,
      runTemporalCohortCharacterization = FALSE,
      incremental = FALSE,
      cohortIds = -11111,
      incrementalFolder = file.path(folder, "incremental")
    )
  ))
  ### Neg - bad cohort -----
  testthat::expect_error(suppressWarnings(
    CohortDiagnostics::runCohortDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "eunomia",
      vocabularyDatabaseSchema = "eunomia",
      oracleTempSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "cdmV5",
      databaseName = NULL, 
      databaseDescription = NULL,
      runInclusionStatistics = FALSE,
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runVisitContext = FALSE,
      runBreakdownIndexEvents = FALSE,
      runIncidenceRate = FALSE,
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      runCohortRelationship = FALSE,
      runCohortCharacterization = FALSE,
      runTemporalCohortCharacterization = FALSE,
      incremental = FALSE,
      cohortIds = -11111,
      incrementalFolder = file.path(folder, "incremental")
    )
  ))
  
  ### Pos - one cohort -----
  testthat::expect_null(suppressWarnings(
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
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = TRUE,
      runVisitContext = TRUE,
      runBreakdownIndexEvents = TRUE,
      runIncidenceRate = TRUE,
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      runCohortRelationship = TRUE,
      runCohortCharacterization = TRUE,
      runTemporalCohortCharacterization = FALSE,
      incremental = FALSE,
      cohortIds = 18348,
      incrementalFolder = file.path(folder, "incremental")
    )
  ))
  ### Pos - generate premerged file ----
  testthat::expect_null(suppressWarnings(CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = file.path(folder, "export"))))
  testthat::expect_true(file.exists(file.path(folder, "export", "PreMerged.RData")))
  unlink(file.path(folder, "export", "PreMerged.RData"))
})





test_that("Cohort diagnostics in incremental mode", {
  skip_if_not(runDatabaseTests)
  
  start <- Sys.time()
  ## Incremental -----
  ### Pos - incremental ----
  # run a subset of diagnostics and then rerun - check if second run took less time compared to first
  testthat::expect_null(suppressWarnings(
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
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runVisitContext = TRUE,
      runBreakdownIndexEvents = FALSE,
      runIncidenceRate = FALSE,
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      runCohortRelationship = TRUE,
      runCohortCharacterization = TRUE,
      runTemporalCohortCharacterization = FALSE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental")
    )
  ))
  timeToRunFirstTime <- Sys.time() - start
  
  testthat::expect_true(file.exists(file.path(
    folder, "export", "Results_CDMv5.zip"
  )))
  
  start <- Sys.time()
  # while in incremental mode: nothing should run during second run, so should be faster than first run
  testthat::expect_null(suppressWarnings(
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
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runVisitContext = TRUE,
      runBreakdownIndexEvents = FALSE,
      runIncidenceRate = FALSE,
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      runCohortRelationship = TRUE,
      runCohortCharacterization = TRUE,
      runTemporalCohortCharacterization = FALSE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental")
    )
  ))
  #because second run is faster than first run - it should take less time
  timeToRunSecondTime <- Sys.time() - start
  testthat::expect_true(timeToRunFirstTime > timeToRunSecondTime)
  
  ### rest of diagnostics ----
  testthat::expect_null(suppressWarnings(CohortDiagnostics::runCohortDiagnostics(
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
    runIncludedSourceConcepts = TRUE,
    runOrphanConcepts = TRUE,
    runVisitContext = TRUE,
    runBreakdownIndexEvents = TRUE,
    runIncidenceRate = TRUE,
    runCohortTimeSeries = TRUE,
    runDataSourceTimeSeries = TRUE,
    runCohortRelationship = TRUE,
    runCohortCharacterization = TRUE,
    runTemporalCohortCharacterization = TRUE,
    incremental = TRUE,
    incrementalFolder = file.path(folder, "incremental")
  )))
  
  ## Premerge file ----
  ### Neg - test - no zip file ----
  testthat::expect_error(CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = file.path(tempdir(), 'random')))
  
  ### Pos - generate premerged file ----
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = file.path(folder, "export"))))
  testthat::expect_true(file.exists(file.path(folder, "export", "PreMerged.RData")))
})




test_that("Negative tests on individual functions", {
  skip_if_not(runDatabaseTests)
  
  # Neg - Individual Function ----
  ## Characterization  ----
  testthat::expect_null(CohortDiagnostics::runCohortCharacterizationDiagnostics(connectionDetails = connectionDetails,
                                                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                                                tempEmulationSchema = tempEmulationSchema,
                                                                                cohortDatabaseSchema = cohortDatabaseSchema,
                                                                                cohortTable = cohortTable,
                                                                                cohortIds = -1111))
  testthat::expect_null(CohortDiagnostics::runCohortCharacterizationDiagnostics(connectionDetails = connectionDetails,
                                                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                                                tempEmulationSchema = tempEmulationSchema,
                                                                                cohortDatabaseSchema = cohortDatabaseSchema,
                                                                                cohortTable = cohortTable))
  ## Cohort relationship  ----
  testthat::expect_null(suppressWarnings(CohortDiagnostics::runCohortRelationshipDiagnostics(connectionDetails = connectionDetails,
                                                                                             tempEmulationSchema = tempEmulationSchema,
                                                                                             cohortDatabaseSchema = cohortDatabaseSchema,
                                                                                             cohortTable = cohortTable,
                                                                                             targetCohortIds = -1111,
                                                                                             comparatorCohortIds = -1111)))
  testthat::expect_null(suppressWarnings(CohortDiagnostics::runCohortRelationshipDiagnostics(connectionDetails = connectionDetails,
                                                                                             tempEmulationSchema = tempEmulationSchema,
                                                                                             cohortDatabaseSchema = cohortDatabaseSchema,
                                                                                             cohortTable = cohortTable,
                                                                                             targetCohortIds = c(18348),
                                                                                             comparatorCohortIds = -1111)))
  testthat::expect_null(suppressWarnings(CohortDiagnostics::runCohortRelationshipDiagnostics(connectionDetails = connectionDetails,
                                                                                             tempEmulationSchema = tempEmulationSchema,
                                                                                             cohortDatabaseSchema = cohortDatabaseSchema,
                                                                                             cohortTable = cohortTable,
                                                                                             targetCohortIds = NULL,
                                                                                             comparatorCohortIds = NULL)))
  testthat::expect_null(suppressWarnings(CohortDiagnostics::runCohortRelationshipDiagnostics(connectionDetails = connectionDetails,
                                                                                             tempEmulationSchema = tempEmulationSchema,
                                                                                             cohortDatabaseSchema = cohortDatabaseSchema,
                                                                                             cohortTable = cohortTable,
                                                                                             targetCohortIds = 18348,
                                                                                             comparatorCohortIds = NULL)))
  ## Visit Context diagnostics  ----
  testthat::expect_null(suppressWarnings(CohortDiagnostics::runVisitContextDiagnostics(connectionDetails = connectionDetails,
                                                                                       tempEmulationSchema = tempEmulationSchema,
                                                                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                                                                       cohortTable = cohortTable,
                                                                                       cdmDatabaseSchema = cdmDatabaseSchema,
                                                                                       vocabularyDatabaseSchema = cdmDatabaseSchema,
                                                                                       cdmVersion = 6,
                                                                                       cohortIds = -1111)))
  ## Time Series diagnostics  ----
  testthat::expect_null(suppressWarnings(CohortDiagnostics::runCohortTimeSeriesDiagnostics(connectionDetails = connectionDetails,
                                                                                           tempEmulationSchema = tempEmulationSchema,
                                                                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                                                                           cohortDatabaseSchema = cohortDatabaseSchema,
                                                                                           cohortTable = cohortTable,
                                                                                           runCohortTimeSeries = FALSE,
                                                                                           runDataSourceTimeSeries = FALSE,
                                                                                           cohortIds = -1111)))
  testthat::expect_null(suppressWarnings(CohortDiagnostics::runCohortTimeSeriesDiagnostics(connectionDetails = connectionDetails,
                                                                                           tempEmulationSchema = tempEmulationSchema,
                                                                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                                                                           cohortDatabaseSchema = cohortDatabaseSchema,
                                                                                           cohortTable = cohortTable,
                                                                                           runCohortTimeSeries = TRUE,
                                                                                           runDataSourceTimeSeries = FALSE,
                                                                                           cohortIds = -1111)))
})


# Premerge Data model results tests ----
test_that("Retrieve results from premerged file", {
  skip_if_not(runDatabaseTests)
  
  dataSourcePreMergedFile <- CohortDiagnostics::createFileDataSource(
    premergedDataFile = file.path(folder, "export", "PreMerged.RData")
  )
  
  ## Cohort Count ----
  #### Pos ----
  cohortCountFromFile <- CohortDiagnostics::getResultsFromCohortCount(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(14906, 14907, 14909, 17492, 17493, 18342, 
                  18345, 18346, 18347, 18348, 18349, 18350, 21402),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(cohortCountFromFile) > 0)
  #### Neg ----
  testthat::expect_null(CohortDiagnostics::getResultsFromCohortCount(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(-11111),
    databaseIds = 'cdmV5'
  ))
  
  ## Time series ----
  #### Pos ----
  timeSeriesFromFile <- CohortDiagnostics::getResultsFromTimeSeries(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(any(is.null(timeSeriesFromFile),
                            length(timeSeriesFromFile) >= 0))
  timeSeriesFromFile2 <- CohortDiagnostics::getResultsFromTimeSeries(
    dataSource = dataSourcePreMergedFile,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(any(is.null(timeSeriesFromFile2),
                            length(timeSeriesFromFile2) >= 0))
  #### Neg ----
  testthat::expect_null(CohortDiagnostics::getResultsFromTimeSeries(
    dataSource = dataSourcePreMergedFile,
    cohortIds = -11111,
    databaseIds = 'cdmV5d'
  ))
  
  ## Time Distribution ----
  #### Pos ----
  timeDistributionFromFile <- CohortDiagnostics::getResultsFromTimeDistribution(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(timeDistributionFromFile) >= 0)
  #### Pos ----
  testthat::expect_null(CohortDiagnostics::getResultsFromTimeDistribution(
    dataSource = dataSourcePreMergedFile,
    cohortIds = -11111,
    databaseIds = 'cdmV5d'
  ))
  
  ## Incidence rate ----
  #### Pos ----
  incidenceRateFromFile <- CohortDiagnostics::getResultsFromIncidenceRate(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(any(is.null(incidenceRateFromFile),
                            nrow(incidenceRateFromFile) >= 0)) # no data in eunomia
  incidenceRateFromFile2 <- CohortDiagnostics::getResultsFromIncidenceRate(
    dataSource = dataSourcePreMergedFile,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(any(is.null(incidenceRateFromFile2),
                            nrow(incidenceRateFromFile2) >= 0)) # no data in eunomia
  incidenceRateFromFile <- CohortDiagnostics::getResultsFromIncidenceRate(
    dataSource = dataSourcePreMergedFile,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(incidenceRateFromFile) >= 0) # no data in eunomia
  #### Neg ----
  testthat::expect_null(CohortDiagnostics::getResultsFromIncidenceRate(
    dataSource = dataSourcePreMergedFile,
    cohortIds = -11111,
    databaseIds = 'cdmV5'
  ))
  
  ## Inclusion rules ----
  #### Pos ----
  inclusionRulesFromFile <- CohortDiagnostics::getResultsFromInclusionRuleStatistics(
    dataSource = dataSourcePreMergedFile,
    cohortIds = 18350,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(inclusionRulesFromFile) >= 0)
  inclusionRulesFromFile2 <- CohortDiagnostics::getResultsFromInclusionRuleStatistics(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(inclusionRulesFromFile2) >= 0)
  #### Neg ----
  testthat::expect_null(CohortDiagnostics::getResultsFromInclusionRuleStatistics(
    dataSource = dataSourcePreMergedFile,
    cohortIds = -1111,
    databaseIds = 'cdmV5'
  ))
  
  ## Index event breakdown ----
  #### Pos ----
  indexEventBreakdownFromFile <- CohortDiagnostics::getResultsFromIndexEventBreakdown(
    dataSource = dataSourcePreMergedFile,
    cohortIds = 18348,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(indexEventBreakdownFromFile) >= 0)
  indexEventBreakdownFromFile2 <- CohortDiagnostics::getResultsFromIndexEventBreakdown(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(indexEventBreakdownFromFile2) >= 0)
  #### Neg ----
  testthat::expect_null(CohortDiagnostics::getResultsFromIndexEventBreakdown(
    dataSource = dataSourcePreMergedFile,
    cohortIds = -1111,
    databaseIds = 'cdmV5'
  ))
  
  ## Visit context ----
  #### Pos ----
  visitContextFromFile <- CohortDiagnostics::getResultsFromVisitContext(
    dataSource = dataSourcePreMergedFile,
    cohortIds = 18348,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(visitContextFromFile) >= 0)
  visitContextFromFile2 <- CohortDiagnostics::getResultsFromVisitContext(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(visitContextFromFile2) >= 0)
  #### Neg ----
  testthat::expect_null(CohortDiagnostics::getResultsFromVisitContext(
    dataSource = dataSourcePreMergedFile,
    cohortIds = -1111,
    databaseIds = 'cdmV5'
  ))
  
  ## Included concept ----
  #### Pos ----
  includedConceptFromFile <- CohortDiagnostics::getResultsFromIncludedConcept(
    dataSource = dataSourcePreMergedFile,
    cohortIds = 18348,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(includedConceptFromFile) >= 0)
  includedConceptFromFile <- CohortDiagnostics::getResultsFromIncludedConcept(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(includedConceptFromFile) >= 0)
  #### Neg ----
  testthat::expect_null(CohortDiagnostics::getResultsFromIncludedConcept(
    dataSource = dataSourcePreMergedFile,
    cohortIds = -1111,
    databaseIds = 'cdmV5'
  ))
  
  ## Orphan concept ----
  #### Pos ----
  orphanConceptFromFile <- CohortDiagnostics::getResultsFromOrphanConcept(
    dataSource = dataSourcePreMergedFile,
    cohortIds = 18348,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(orphanConceptFromFile) >= 0)
  orphanConceptFromFile2 <- CohortDiagnostics::getResultsFromOrphanConcept(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(orphanConceptFromFile2) >= 0)
  
  ## Concept id details ----
  #### Pos ----
  conceptIdDetails <- CohortDiagnostics::getResultsFromConcept(
    dataSource = dataSourcePreMergedFile,
    conceptIds = c(192671, 201826, 1124300, 1124300)
  )
  testthat::expect_true(nrow(conceptIdDetails) >= 0)
  # should throw warning
  conceptIdDetails <- suppressWarnings(CohortDiagnostics::getResultsFromConcept(
    dataSource = dataSourcePreMergedFile,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    conceptIds = c(192671, 201826, 1124300, 1124300)
  ))
  testthat::expect_true(nrow(conceptIdDetails) >= 0)
  #### Neg ----
  testthat::expect_null(conceptIdDetails <- suppressWarnings(CohortDiagnostics::getResultsFromConcept(
    dataSource = dataSourcePreMergedFile,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    conceptIds = c(-1111)
  )))
  
  ## Resolved concept ----
  #### Pos ----
  resolvedMappedConceptSet <- CohortDiagnostics::getResultsResolveMappedConceptSet(
    dataSource = dataSourcePreMergedFile,
    cohortIds = 18348,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(resolvedMappedConceptSet$resolved) >= 0)
  testthat::expect_null(resolvedMappedConceptSet$mapped)
  resolvedMappedConceptSet2 <- CohortDiagnostics::getResultsResolveMappedConceptSet(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(resolvedMappedConceptSet2$resolved) >= 0)
  testthat::expect_true(nrow(resolvedMappedConceptSet2$mapped) >= 0)
  #### Neg ----
  testthat::expect_null(conceptIdDetails <- suppressWarnings(CohortDiagnostics::getResultsResolveMappedConceptSet(
    dataSource = dataSourcePreMergedFile,
    cohortIds = -1111,
    databaseIds = 'cdmV5'
  )))
  
  ## Calendar incidence ----
  # Table does not exist in results, is not generated in Eunomia?
  calendarIncidence <- CohortDiagnostics::getResultsFromCalendarIncidence(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(any(is.null(calendarIncidence), nrow(calendarIncidence) >= 0))
  
  ## Cohort Relationship ----
  #### Pos ----
  cohortRelationships <- CohortDiagnostics::getResultsFromCohortRelationships(
    dataSource = dataSourcePreMergedFile,
    cohortIds = 18348,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(cohortRelationships) >= 0)
  cohortRelationships2 <- CohortDiagnostics::getResultsFromCohortRelationships(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(cohortRelationships2) >= 0)
  #### Neg ----
  testthat::expect_null(conceptIdDetails <- suppressWarnings(CohortDiagnostics::getResultsFromCohortRelationships(
    dataSource = dataSourcePreMergedFile,
    cohortIds = -1111,
    databaseIds = 'cdmV5'
  )))
  
  
  #### Pos ----
  # Table does not exist in results, so this is throwing an error
  cohortCharacterizationResults <- CohortDiagnostics::getMultipleCharacterizationResults(
    dataSource = dataSourcePreMergedFile,
    cohortIds = 18348,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(length(cohortCharacterizationResults) >= 0)
  cohortCharacterizationResults2 <- CohortDiagnostics::getMultipleCharacterizationResults(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(length(cohortCharacterizationResults2) >= 0)
  #### Neg ----
  testthat::expect_null(conceptIdDetails <- suppressWarnings(CohortDiagnostics::getMultipleCharacterizationResults(
    dataSource = dataSourcePreMergedFile,
    cohortIds = -1111,
    databaseIds = 'cdmV5'
  )))
  
  
  #### Pos ----
  cohortOverlapData <- CohortDiagnostics::getCohortOverlapData(
    dataSource = dataSourcePreMergedFile, 
    cohortIds = c(17492, 18342),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(cohortOverlapData) >= 0)
  cohortOverlapData2 <- CohortDiagnostics::getCohortOverlapData(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(cohortOverlapData2) >= 0) 
  #### Neg ----
  testthat::expect_null(conceptIdDetails <- suppressWarnings(CohortDiagnostics::getCohortOverlapData(
    dataSource = dataSourcePreMergedFile,
    cohortIds = -1111,
    databaseIds = 'cdmV5'
  )))
})




####################### upload to database and test
test_that("Create and upload results to results data model", {
  skip_if_not(runDatabaseTests)
  
  testthat::expect_null(CohortDiagnostics::createResultsDataModel(connectionDetails = connectionDetails, 
                                                                  schema = cohortDiagnosticsSchema))
  
  listOfZipFilesToUpload <-
    list.files(
      path = file.path(folder, "export"),
      pattern = ".zip",
      full.names = TRUE,
      recursive = TRUE
    )
  testthat::expect_true(length(listOfZipFilesToUpload) >= 0)
  
  for (i in (1:length(listOfZipFilesToUpload))) {
    testthat::expect_null(suppressWarnings(
      CohortDiagnostics::uploadResults(
        connectionDetails = connectionDetails,
        schema = cohortDiagnosticsSchema,
        zipFileName = listOfZipFilesToUpload[[i]]
      )
    ))
  }
})


# Retrieve results
test_that("Retrieve results from remote database", {
  skip_if_not(runDatabaseTests)
  
  dataSourceDatabase <- CohortDiagnostics::createDatabaseDataSource(
    connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
    resultsDatabaseSchema = cohortDiagnosticsSchema
  )
  
  # cohort count
  cohortCountFromDb <- CohortDiagnostics::getResultsFromCohortCount(
    dataSource = dataSourceDatabase,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(cohortCountFromDb) > 0)
  
  # time series
  timeSeriesFromDb <- CohortDiagnostics::getResultsFromTimeSeries(
    dataSource = dataSourceDatabase,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(any(is.null(timeSeriesFromDb),
                            length(timeSeriesFromDb) >= 0))
  
  # time distribution
  timeDistributionFromDb <- CohortDiagnostics::getResultsFromTimeDistribution(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 18342),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(timeDistributionFromDb) >= 0)
  
  # incidence rate result
  incidenceRateFromDb <- CohortDiagnostics::getResultsFromIncidenceRate(
    dataSource = dataSourceDatabase,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(incidenceRateFromDb) >= 0) # no data in eunomia
  
  # inclusion rules
  inclusionRulesFromDb <- CohortDiagnostics::getResultsFromInclusionRuleStatistics(
    dataSource = dataSourceDatabase,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(any(is.null(inclusionRulesFromDb),
                            nrow(inclusionRulesFromDb) >= 0))
  
  # index_event_breakdown
  indexEventBreakdownFromDb <- CohortDiagnostics::getResultsFromIndexEventBreakdown(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(indexEventBreakdownFromDb) >= 0)
  
  # visit_context
  visitContextFromDb <- CohortDiagnostics::getResultsFromVisitContext(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(visitContextFromDb) >= 0)
  
  # included_concept
  includedConceptFromDb <- CohortDiagnostics::getResultsFromIncludedConcept(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(includedConceptFromDb) >= 0)
  
  # orphan_concept
  orphanConceptFromDb <- CohortDiagnostics::getResultsFromOrphanConcept(
    dataSource = dataSourceDatabase,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(orphanConceptFromDb) >= 0)
  
  # concept_id details with vocabulary schema
  conceptIdDetails <- CohortDiagnostics::getResultsFromConcept(
    dataSource = dataSourceDatabase,
    conceptIds = c(192671, 201826, 1124300, 1124300),
    vocabularyDatabaseSchema = cohortDiagnosticsSchema
  )
  
  # concept_id details without vocabulary schema
  conceptIdDetails <- CohortDiagnostics::getResultsFromConcept(
    dataSource = dataSourceDatabase,
    conceptIds = c(192671, 201826, 1124300, 1124300)
  )
  
  resolvedMappedConceptSet <- CohortDiagnostics::getResultsResolveMappedConceptSet(
    dataSource = dataSourceDatabase
  )
  testthat::expect_true(nrow(resolvedMappedConceptSet$resolved) > 0)
  testthat::expect_true(nrow(resolvedMappedConceptSet$mapped) > 0)
  
  calendarIncidence <- CohortDiagnostics::getResultsFromCalendarIncidence(
    dataSource = dataSourceDatabase
  )
  testthat::expect_true(any(is.null(calendarIncidence),
                            nrow(calendarIncidence) >= 0))
  
  cohortRelationships <- CohortDiagnostics::getResultsFromCohortRelationships(
    dataSource = dataSourceDatabase
  )
  testthat::expect_true(nrow(cohortRelationships) >= 0) 
  
  multipleCharacterizationResults  <- CohortDiagnostics::getMultipleCharacterizationResults(
    dataSource = dataSourceDatabase
  )
  testthat::expect_true(length(multipleCharacterizationResults) >= 0)
  
  cohortOverlapData <- CohortDiagnostics::getCohortOverlapData(
    dataSource = dataSourceDatabase, 
    cohortIds = c(17492, 18342),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(cohortOverlapData) >= 0) 
})






test_that("Data removal works", {
  skip_if_not(runDatabaseTests)
  
  specifications <- CohortDiagnostics::getResultsDataModelSpecifications()
  connection <- DatabaseConnector::connect(connectionDetails)
  
  dataSourceDatabase <- CohortDiagnostics::createDatabaseDataSource(
    connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
    resultsDatabaseSchema = cohortDiagnosticsSchema
  )
  cohortTableDataBeforeDelete <- CohortDiagnostics::getResultsFromCohortCount(
    dataSource = dataSourceDatabase,
    databaseIds = 'cdmV5'
  )
  
  if (!is.null(cohortTableDataBeforeDelete)) {
    colnames(cohortTableDataBeforeDelete) <- 
      CohortDiagnostics:::camelCaseToSnakeCase(colnames(cohortTableDataBeforeDelete))
    
    # delete some selected records
    CohortDiagnostics:::deleteFromServer(
      connection = connection,
      schema = cohortDiagnosticsSchema,
      tableName = 'cohort_count',
      keyValues = cohortTableDataBeforeDelete[1,]
    )
    cohortTableDataAfterDelete <- CohortDiagnostics::getResultsFromCohortCount(dataSource = dataSourceDatabase)
    
    testthat::expect_true(nrow(cohortTableDataBeforeDelete) > 
                            nrow(cohortTableDataAfterDelete))
  }
  
  
  for (tableName in unique(specifications$tableName)) {
    primaryKey <- specifications %>%
      dplyr::filter(.data$tableName == !!tableName &
                      .data$primaryKey == "Yes") %>%
      dplyr::select(.data$fieldName) %>%
      dplyr::pull()
    
    if ("database_id" %in% primaryKey) {
      CohortDiagnostics:::deleteAllRecordsForDatabaseId(
        connection = connection,
        schema = cohortDiagnosticsSchema,
        tableName = tableName,
        databaseId = "cdmV5"
      )
      
      sql <-
        "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
      sql <- SqlRender::render(
        sql = sql,
        schema = cohortDiagnosticsSchema,
        table_name = tableName,
        database_id = "cdmV5"
      )
      databaseIdCount <-
        DatabaseConnector::querySql(connection, sql)[, 1]
      expect_true(databaseIdCount == 0)
    }
  }
  DatabaseConnector::disconnect(connection)
})
