createResultsDataModel(connectionDetails = connectionDetails, schema = cohortDiagnosticsSchema)

test_that("Results upload", {
  instantiateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "eunomia",
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDiagnosticsSchema,
    cohortTable = cohortTable,
    cohortIds = c(17492, 17692),
    packageName = "CohortDiagnostics",
    cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
    generateInclusionStats = TRUE,
    createCohortTable = TRUE,
    inclusionStatisticsFolder = file.path(folder, "incStats")
  )
  
  runCohortDiagnostics(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "eunomia",
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDiagnosticsSchema,
    cohortTable = cohortTable,
    cohortIds = c(17492, 17692),
    packageName = "CohortDiagnostics",
    cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
    inclusionStatisticsFolder = file.path(folder, "incStats"),
    exportFolder = file.path(folder, "export"),
    databaseId = "cdmv5",
    incremental = TRUE,
    incrementalFolder = file.path(folder, "incremental")
  )
  
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
  
  specifications <- getResultsDataModelSpecifications()
  
  for (tableName in unique(specifications$tableName)) {
    primaryKey <- specifications %>%
      dplyr::filter(.data$tableName == !!tableName &
                      .data$primaryKey == "Yes") %>%
      dplyr::select(.data$fieldName) %>%
      dplyr::pull()
    
    if ("database_id" %in% primaryKey) {
      sql <-
        "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
      sql <- SqlRender::render(
        sql = sql,
        schema = cohortDiagnosticsSchema,
        table_name = tableName,
        database_id = "cdmv5"
      )
      databaseIdCount <-
        DatabaseConnector::querySql(connection, sql)[, 1]
    }
  }
})

test_that("Retrieve results from remote database", {
  CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = folder)
  
  dataSourceDatabase <- CohortDiagnostics::createDatabaseDataSource(
    connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
    resultsDatabaseSchema = cohortDiagnosticsSchema
  )
  dataSourcePreMergedFile <- CohortDiagnostics::createFileDataSource(
    premergedDataFile = file.path(folder, "PreMerged.RData")
  )
  
  # cohort count
  cohortCountFromDb <- CohortDiagnostics::getResultsFromCohortCount(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(cohortCountFromDb) > 0)
  cohortCountFromFile <- CohortDiagnostics::getResultsFromCohortCount(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(cohortCountFromFile) > 0)
  
  # time series
  timeSeriesFromDb <- CohortDiagnostics::getResultsFromTimeSeries(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(timeSeriesFromDb) > 0)
  timeSeriesFromFile <- CohortDiagnostics::getResultsFromTimeSeries(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(timeSeriesFromFile) > 0)
  
  # time distribution
  timeDistributionFromDb <- CohortDiagnostics::getResultsFromTimeDistribution(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(timeDistributionFromDb) > 0)
  timeDistributionFromFile <- CohortDiagnostics::getResultsFromTimeDistribution(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(timeDistributionFromFile) > 0)
  
  # incidence rate result
  incidenceRateFromDb <- CohortDiagnostics::getResultsFromIncidenceRate(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  # expect_true(nrow(incidenceRateFromDb) >= 0) - no data in eunomia
  incidenceRateFromFile <- CohortDiagnostics::getResultsFromIncidenceRate(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  # expect_true(nrow(incidenceRateFromFile) >= 0) - no data in eunomia
  
  # inclusion rules
  inclusionRulesFromDb <- CohortDiagnostics::getResultsFromInclusionRuleStatistics(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(inclusionRulesFromDb) > 0)
  inclusionRulesFromFile <- CohortDiagnostics::getResultsFromInclusionRuleStatistics(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(inclusionRulesFromFile) > 0)
  
  
  # index_event_breakdown
  indexEventBreakdownFromDb <- CohortDiagnostics::getResultsFromIndexEventBreakdown(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(indexEventBreakdownFromDb) > 0)
  indexEventBreakdownFromFile <- CohortDiagnostics::getResultsFromIndexEventBreakdown(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(indexEventBreakdownFromFile) > 0)
  
  # visit_context
  visitContextFromDb <- CohortDiagnostics::getResultsFromVisitContext(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(visitContextFromDb) > 0)
  visitContextFromFile <- CohortDiagnostics::getResultsFromVisitContext(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(visitContextFromFile) > 0)
  
  # visit_context
  visitContextFromDb <- CohortDiagnostics::getResultsFromVisitContext(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(visitContextFromDb) > 0)
  visitContextFromFile <- CohortDiagnostics::getResultsFromVisitContext(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(visitContextFromFile) > 0)
  
  # included_concept
  includedConceptFromDb <- CohortDiagnostics::getResultsFromIncludedConcept(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(includedConceptFromDb) > 0)
  includedConceptFromFile <- CohortDiagnostics::getResultsFromIncludedConcept(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(includedConceptFromFile) > 0)
  
  # orphan_concept
  orphanConceptFromDb <- CohortDiagnostics::getResultsFromOrphanConcept(
    dataSource = dataSourceDatabase,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(orphanConceptFromDb) > 0)
  orphanConceptFromFile <- CohortDiagnostics::getResultsFromOrphanConcept(
    dataSource = dataSourcePreMergedFile,
    cohortIds = c(17492, 17692),
    databaseIds = 'cdmV5'
  )
  expect_true(nrow(orphanConceptFromFile) > 0)
})

test_that("Data removal works", {
  specifications <- getResultsDataModelSpecifications()
  
  for (tableName in unique(specifications$tableName)) {
    primaryKey <- specifications %>%
      dplyr::filter(.data$tableName == !!tableName &
                      .data$primaryKey == "Yes") %>%
      dplyr::select(.data$fieldName) %>%
      dplyr::pull()
    
    if ("database_id" %in% primaryKey) {
      deleteAllRecordsForDatabaseId(
        connection = connection,
        schema = cohortDiagnosticsSchema,
        tableName = tableName,
        databaseId = "cdmv5"
      )
      
      sql <-
        "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
      sql <- SqlRender::render(
        sql = sql,
        schema = cohortDiagnosticsSchema,
        table_name = tableName,
        database_id = "cdmv5"
      )
      databaseIdCount <-
        DatabaseConnector::querySql(connection, sql)[, 1]
      expect_true(databaseIdCount == 0)
    }
  }
  
})

test_that("util functions", {
  expect_true(naToEmpty(NA) == "")
  expect_true(naToZero(NA) == 0)
})
