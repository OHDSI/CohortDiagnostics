
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
    cdmDatabaseSchema = cdmDatabaseSchema,
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
