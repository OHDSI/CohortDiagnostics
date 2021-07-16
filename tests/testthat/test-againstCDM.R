library(testthat)
library(CohortDiagnostics)

# Cohort Instantiation tests ----
test_that("Cohort instantiation", {
  skip_if_not(runDatabaseTests)
  
  ## No incremental mode ----
  ### Neg - no cohort table
  testthat::expect_error(
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
      createCohortTable = FALSE,
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  )
  ### Neg - bad cohort ----
  testthat::expect_error(
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
  )
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
  sql <- "SELECT COUNT(*) FROM @cohort_database_schema.@cohort_table;"
  count <- CohortDiagnostics:::renderTranslateQuerySql(connectionDetails = connectionDetails,
                                                       sql = sql,
                                                       cohort_database_schema = cohortDatabaseSchema,
                                                       cohort_table = cohortTable)
  testthat::expect_equal(count$COUNT, 830)
  
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
  sql <- "DELETE FROM @cohort_database_schema.@cohort_table WHERE SUBJECT_ID < 1000;"
  DatabaseConnector::renderTranslateExecuteSql(connection = DatabaseConnector::connect(connectionDetails),
                                               sql = sql,
                                               cohort_database_schema = cohortDatabaseSchema,
                                               cohort_table = cohortTable)
  
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
  # Expect cohortId 18348 to have 830 records
  sql <- "SELECT COUNT(*) FROM @cohort_database_schema.@cohort_table;"
  count <- CohortDiagnostics:::renderTranslateQuerySql(connectionDetails = connectionDetails,
                                                       sql = sql,
                                                       cohort_database_schema = cohortDatabaseSchema,
                                                       cohort_table = cohortTable)
  testthat::expect_equal(count$COUNT, 830)
  
  ## Incremental mode ----
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

test_that("Cohort diagnostics in not in incremental mode", {
  skip_if_not(runDatabaseTests)
  
  start <- Sys.time()
  # Cohort Diagnostics -----
  ## Not incremental -----
  ### Neg - bad cohort -----
  testthat::expect_error(
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
      cohortIds = -23423,
      incrementalFolder = file.path(folder, "incremental")
    )
  )
  
  ### Pos - one cohort -----
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
  timeToRunFirstTime <- Sys.time() - start
})


test_that("Cohort diagnostics in incremental mode", {
  skip_if_not(runDatabaseTests)
  
  start <- Sys.time()
  ## Incremental -----
  # run all except time series and temporal characterization
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
    incremental = TRUE,
    incrementalFolder = file.path(folder, "incremental")
  )
  timeToRunFirstTime <- Sys.time() - start
  
  testthat::expect_true(file.exists(file.path(
    folder, "export", "Results_CDMv5.zip"
  )))
  
  start <- Sys.time()
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
    runCohortTimeSeries = TRUE,
    runDataSourceTimeSeries = TRUE,
    runCohortRelationship = TRUE,
    runCohortCharacterization = TRUE,
    runTemporalCohortCharacterization = TRUE,
    incremental = TRUE,
    incrementalFolder = file.path(folder, "incremental")
  )
  timeToRunSecondTime <- Sys.time() - start
  testthat::expect_true(timeToRunFirstTime > timeToRunSecondTime)
  
  # generate premerged file
  CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = file.path(folder, "export"))
  testthat::expect_true(file.exists(file.path(folder, "export", "PreMerged.RData")))
})

test_that("Negative tests on individual functions", {
  skip_if_not(runDatabaseTests)
  
  #Characterization
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
  
})

test_that("Retrieve results from premerged file", {
  skip_if_not(runDatabaseTests)
  
  dataSourcePreMergedFile <- CohortDiagnostics::createFileDataSource(
    premergedDataFile = file.path(folder, "export", "PreMerged.RData")
  )
  
  cohortCountFromFile <- CohortDiagnostics::getResultsFromCohortCount(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(14906, 14907, 14909, 17492, 17493, 18342, 18345, 18346, 18347, 18348, 18349, 18350, 21402),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(cohortCountFromFile) > 0)
  
  timeSeriesFromFile <- CohortDiagnostics::getResultsFromTimeSeries(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(any(is.null(timeSeriesFromFile),
                            length(timeSeriesFromFile) >= 0))
  
  timeSeriesFromFile <- CohortDiagnostics::getResultsFromTimeSeries(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(-1111),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(any(is.null(timeSeriesFromFile),
                            length(timeSeriesFromFile) >= 0))
  
  timeSeriesFromFile <- CohortDiagnostics::getResultsFromTimeSeries(
    dataSource = dataSourcePreMergedFile,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(any(is.null(timeSeriesFromFile),
                            length(timeSeriesFromFile) >= 0))
  if (length(timeSeriesFromFile) > 0) {
    testthat::expect_true(nrow(timeSeriesFromFile$y) > 0)
  }
  
  timeDistributionFromFile <- CohortDiagnostics::getResultsFromTimeDistribution(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(timeDistributionFromFile) >= 0)
  
  timeDistributionFromFile <- CohortDiagnostics::getResultsFromTimeDistribution(
    dataSource = dataSourcePreMergedFile,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(timeDistributionFromFile) >= 0)
  
  incidenceRateFromFile <- CohortDiagnostics::getResultsFromIncidenceRate(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(incidenceRateFromFile) >= 0) # no data in eunomia
  
  incidenceRateFromFile <- CohortDiagnostics::getResultsFromIncidenceRate(
    dataSource = dataSourcePreMergedFile,
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(incidenceRateFromFile) >= 0) # no data in eunomia
  
  inclusionRulesFromFile <- CohortDiagnostics::getResultsFromInclusionRuleStatistics(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(any(is.null(inclusionRulesFromFile), 
                            nrow(inclusionRulesFromFile) >= 0))
  
  indexEventBreakdownFromFile <- CohortDiagnostics::getResultsFromIndexEventBreakdown(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(indexEventBreakdownFromFile) >= 0)
  
  visitContextFromFile <- CohortDiagnostics::getResultsFromVisitContext(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(visitContextFromFile) >= 0)
  
  includedConceptFromFile <- CohortDiagnostics::getResultsFromIncludedConcept(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(includedConceptFromFile) >= 0)
  
  orphanConceptFromFile <- CohortDiagnostics::getResultsFromOrphanConcept(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(orphanConceptFromFile) >= 0)
  
  conceptIdDetails <- CohortDiagnostics::getResultsFromConcept(
    dataSource = dataSourcePreMergedFile,
    conceptIds = c(192671, 201826, 1124300, 1124300)
  )
  testthat::expect_true(nrow(conceptIdDetails) >= 0)
  
  # should provide warning
  conceptIdDetails <- CohortDiagnostics::getResultsFromConcept(
    dataSource = dataSourcePreMergedFile,
    conceptIds = c(192671, 201826, 1124300, 1124300),
    vocabularyDatabaseSchema = 'vocabulary'
  )
  testthat::expect_true(nrow(conceptIdDetails) >= 0)
  
  resolvedMappedConceptSet <- CohortDiagnostics::getResultsResolveMappedConceptSet(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(resolvedMappedConceptSet$resolved) >= 0)
  testthat::expect_true(nrow(resolvedMappedConceptSet$mapped) >= 0)
  
  # Table does not exist in results, is not generated in Eunomia?
  calendarIncidence <- CohortDiagnostics::getResultsFromCalendarIncidence(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(any(is.null(calendarIncidence), nrow(calendarIncidence) >= 0))
  
  cohortRelationships <- CohortDiagnostics::getResultsFromCohortRelationships(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(nrow(cohortRelationships) >= 0)
  
  # Table does not exist in results, so this is throwing an error
  cohortCharacterizationResults <- CohortDiagnostics::getMultipleCharacterizationResults(
    dataSource = dataSourcePreMergedFile
  )
  testthat::expect_true(length(cohortCharacterizationResults) >= 0)
  
  cohortOverlapData <- CohortDiagnostics::getCohortOverlapData(
    dataSource = dataSourcePreMergedFile, 
    cohortIds = c(17492, 18342),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(cohortOverlapData) >= 0) 

})


####################### upload to database and test
test_that("Create and upload results to results data model", {
  skip_if_not(runDatabaseTests)
  
  CohortDiagnostics::createResultsDataModel(connectionDetails = connectionDetails, 
                                            schema = cohortDiagnosticsSchema)
  
  listOfZipFilesToUpload <-
    list.files(
      path = file.path(folder, "export"),
      pattern = ".zip",
      full.names = TRUE,
      recursive = TRUE
    )
  
  for (i in (1:length(listOfZipFilesToUpload))) {
    CohortDiagnostics::uploadResults(
      connectionDetails = connectionDetails,
      schema = cohortDiagnosticsSchema,
      zipFileName = listOfZipFilesToUpload[[i]]
    )
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
    cohortIds = c(17492, 17692),
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


