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
      runTimeSeries = FALSE,
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = TRUE,
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
      runTimeSeries = TRUE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental")
    )
  )
  testthat::expect_lt(secondTime[1], firstTime[1])
  
  # generate premerged file
  CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = file.path(folder, "export"))
  testthat::expect_true(file.exists(file.path(folder, "export", "PreMerged.RData")))
})


test_that("Retrieve results from premerged file", {
  
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
  # expect_true(nrow(timeSeriesFromFile) >= 0)
  
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
  
  conceptIdDetails <- CohortDiagnostics::getResultsFromConcept(
    dataSource = dataSourcePreMergedFile,
    conceptIds = c(192671, 201826, 1124300, 1124300)
  )
  expect_true(nrow(conceptIdDetails) >= 0)
  
  resolvedMappedConceptSet <- CohortDiagnostics::getResultsResolveMappedConceptSet(
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(resolvedMappedConceptSet$resolved) >= 0)
  expect_true(nrow(resolvedMappedConceptSet$mapped) >= 0)
  
  calendarIncidence <- CohortDiagnostics::getResultsFromCalendarIncidence(
    dataSource = dataSourcePreMergedFile
  )
  # expect_true(nrow(calendarIncidence) >= 0)
  
  cohortRelationships <- CohortDiagnostics::getResultsFromCohortRelationships(
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(cohortRelationships) >= 0)
  
  cohortCharacterizationResults <- CohortDiagnostics::getCohortCharacterizationResults(
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(cohortCharacterizationResults) >= 0)
  
  temporalCohortCharacterizationResults <- CohortDiagnostics::getTemporalCohortCharacterizationResults(
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(temporalCohortCharacterizationResults) >= 0)
  
  cohortAsFeatureCharacterizationResults <- CohortDiagnostics::getCohortAsFeatureCharacterizationResults(
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(cohortAsFeatureCharacterizationResults) >= 0)
  
  cohortAsFeatureTemporalCharacterizationResults <- CohortDiagnostics::getCohortAsFeatureTemporalCharacterizationResults(
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(cohortAsFeatureTemporalCharacterizationResults) >= 0)
  
  multipleCharacterizationResults  <- CohortDiagnostics::getMultipleCharacterizationResults (
    dataSource = dataSourcePreMergedFile
  )
  expect_true(nrow(multipleCharacterizationResults) >= 0)
  
})


####################### upload to database and test
test_that("Create and upload results to results data model", {
  createResultsDataModel(connectionDetails = connectionDetails, schema = cohortDiagnosticsSchema)
  
  listOfZipFilesToUpload <-
    list.files(
      path = file.path(folder, "export"),
      pattern = ".zip",
      full.names = TRUE,
      recursive = TRUE
    )
  
  for (i in (1:length(listOfZipFilesToUpload))) {
    uploadResults(
      connectionDetails = connectionDetails,
      schema = cohortDiagnosticsSchema,
      zipFileName = listOfZipFilesToUpload[[i]]
    )
  }
})


# Retrieve results
test_that("Retrieve results from remote database", {
  
  dataSourceDatabase <- CohortDiagnostics::createDatabaseDataSource(
    connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
    resultsDatabaseSchema = cohortDiagnosticsSchema
  )
  
  # cohort count
  cohortCountFromDb <- CohortDiagnostics::getResultsFromCohortCount(
    dataSource = dataSourceDatabase,
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(cohortCountFromDb) > 0)
  
  # time series
  timeSeriesFromDb <- CohortDiagnostics::getResultsFromTimeSeries(
    dataSource = dataSourceDatabase,
    databaseIds = 'cdmV5'
  )
  # expect_true(nrow(timeSeriesFromDb) >= 0)
  
  # time distribution
  timeDistributionFromDb <- CohortDiagnostics::getResultsFromTimeDistribution(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(timeDistributionFromDb) >= 0)
  
  # incidence rate result
  incidenceRateFromDb <- CohortDiagnostics::getResultsFromIncidenceRate(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(incidenceRateFromDb) >= 0) # no data in eunomia
  
  # inclusion rules
  inclusionRulesFromDb <- CohortDiagnostics::getResultsFromInclusionRuleStatistics(
    dataSource = dataSourceDatabase,
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(inclusionRulesFromDb) >= 0)
  
  # index_event_breakdown
  indexEventBreakdownFromDb <- CohortDiagnostics::getResultsFromIndexEventBreakdown(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(indexEventBreakdownFromDb) >= 0)
  
  # visit_context
  visitContextFromDb <- CohortDiagnostics::getResultsFromVisitContext(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(visitContextFromDb) >= 0)
  
  # included_concept
  includedConceptFromDb <- CohortDiagnostics::getResultsFromIncludedConcept(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(includedConceptFromDb) >= 0)
  
  # orphan_concept
  orphanConceptFromDb <- CohortDiagnostics::getResultsFromOrphanConcept(
    dataSource = dataSourceDatabase,
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(orphanConceptFromDb) >= 0)
  
  # concept_id details with vocabulary schema
  # conceptIdDetails <- CohortDiagnostics::getResultsFromConcept(
  #   dataSource = dataSourceDatabase,
  #   conceptIds = c(192671, 201826, 1124300, 1124300), 
  #   vocabularyDatabaseSchema = cohortDiagnosticsSchema
  # )
  
  # concept_id details without vocabulary schema
  conceptIdDetails <- CohortDiagnostics::getResultsFromConcept(
    dataSource = dataSourceDatabase,
    conceptIds = c(192671, 201826, 1124300, 1124300)
  )
  
  resolvedMappedConceptSet <- CohortDiagnostics::getResultsResolveMappedConceptSet(
    dataSource = dataSourceDatabase
  )
  expect_true(nrow(resolvedMappedConceptSet$resolved) > 0)
  expect_true(nrow(resolvedMappedConceptSet$mapped) > 0)
  
  calendarIncidence <- CohortDiagnostics::getResultsFromCalendarIncidence(
    dataSource = dataSourceDatabase
  )
  # expect_true(nrow(calendarIncidence) >= 0)
  
  cohortRelationships <- CohortDiagnostics::getResultsFromCohortRelationships(
    dataSource = dataSourceDatabase
  )
  expect_true(nrow(cohortRelationships) >= 0) 
  
  cohortCharacterizationResults <- CohortDiagnostics::getCohortCharacterizationResults(
    dataSource = dataSourceDatabase
  )
  expect_true(nrow(cohortCharacterizationResults) >= 0)
  
  temporalCohortCharacterizationResults <- CohortDiagnostics::getTemporalCohortCharacterizationResults(
    dataSource = dataSourceDatabase
  )
  expect_true(nrow(temporalCohortCharacterizationResults) >= 0)
  
  cohortAsFeatureCharacterizationResults <- CohortDiagnostics::getCohortAsFeatureCharacterizationResults(
    dataSource = dataSourceDatabase
  )
  expect_true(nrow(cohortAsFeatureCharacterizationResults) >= 0)
  
  cohortAsFeatureTemporalCharacterizationResults <- CohortDiagnostics::getCohortAsFeatureTemporalCharacterizationResults(
    dataSource = dataSourceDatabase
  )
  expect_true(nrow(cohortAsFeatureTemporalCharacterizationResults) >= 0)
  
  multipleCharacterizationResults  <- CohortDiagnostics::getMultipleCharacterizationResults (
    dataSource = dataSourceDatabase
  )
  expect_true(nrow(multipleCharacterizationResults) >= 0)
})



test_that("Data removal works", {
  specifications <- getResultsDataModelSpecifications()
  connection <- DatabaseConnector::connect(connectionDetails)
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


test_that("util functions", {
  expect_true(naToEmpty(NA) == "")
  expect_true(naToZero(NA) == 0)
})
