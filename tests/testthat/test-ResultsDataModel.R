jdbcDriverFolder <- tempfile("jdbcDrivers")
DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = jdbcDriverFolder)
connectionDetails <-
  DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
    pathToDriver = jdbcDriverFolder
  )

cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
vocabularyDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
cohortDiagnosticsSchema <-
  Sys.getenv("CDM5_POSTGRESQL_COHORT_DIAGNOSTICS_SCHEMA")
oracleTempSchema <- NULL
cohortTable <- "cohort"
connection <-
  DatabaseConnector::connect(connectionDetails = connectionDetails)
folder <- tempfile("cohortDiagnosticsTest")

withr::defer({
  DatabaseConnector::disconnect(connection)
  unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
  unlink(folder, recursive = TRUE, force = TRUE)
}, testthat::teardown_env())


#' Only works with postgres > 9.4
.tableExists <- function(connection, schema, tableName) {
  return(!is.na(
    DatabaseConnector::renderTranslateQuerySql(
      connection,
      "SELECT to_regclass('@schema.@table');",
      table = tableName,
      schema = schema
    )
  )[[1]])
}


test_that("Create schema", {
  createResultsDataModel(connectionDetails = connectionDetails, schema = cohortDiagnosticsSchema)
  
  specifications <- getResultsDataModelSpecifications()
  
  for (tableName in unique(specifications$tableName)) {
    expect_true(.tableExists(connection, cohortDiagnosticsSchema, tableName))
  }
  # Bad schema name
  expect_error(createResultsDataModel(connection = connection, schema = "non_existant_schema"))
})


test_that("Results upload", {
  instantiateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    oracleTempSchema = oracleTempSchema,
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
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    oracleTempSchema = oracleTempSchema,
    cohortDatabaseSchema = cohortDiagnosticsSchema,
    cohortTable = cohortTable,
    cohortIds = c(17492, 17692),
    packageName = "CohortDiagnostics",
    cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
    inclusionStatisticsFolder = file.path(folder, "incStats"),
    exportFolder = file.path(folder, "export"),
    databaseId = "cdmv5",
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
        schema = schema,
        table_name = tableName,
        database_id = databaseId
      )
      databaseIdCount <-
        DatabaseConnector::querySql(connection, sql)[, 1]
      expect_true(databaseIdCount = !0)
    }
  }
})

test_that("Retrieve results from remote database", {
  CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = folder)
  
  dataSourceDatabase <- CohortDiagnostics::createDatabaseDataSource(
    connectionDetails = connectionDetails,
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
        database_id = databaseId
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
