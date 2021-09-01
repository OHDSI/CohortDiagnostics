library(CohortDiagnostics)
library(testthat)

if (Sys.getenv("DONT_DOWNLOAD_JDBC_DRIVERS", "") == "TRUE") {
  jdbcDriverFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
} else {
  jdbcDriverFolder <- tempfile("jdbcDrivers")
  DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = jdbcDriverFolder)
  
  withr::defer({
    unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
  }, testthat::teardown_env())
}

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  user = Sys.getenv("CDM5_POSTGRESQL_USER"),
  password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
  server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
  pathToDriver = jdbcDriverFolder
)

cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
vocabularyDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
cohortDiagnosticsSchema <- Sys.getenv("CDM5_POSTGRESQL_COHORT_DIAGNOSTICS_SCHEMA")
resultsDatabaseSchema <- paste0("r", 
                                as.character(gsub("[: -]", "" , Sys.time(), perl=TRUE)),
                                as.character(sample(1:100, 1)))
oracleTempSchema <- NULL
cohortTable <- "cohort"
connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
folder <- tempfile("cohortDiagnosticsTest")

withr::defer({
  DatabaseConnector::disconnect(connection)
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
  dropSchemaIfExists <- paste0("DROP SCHEMA IF EXISTS ", resultsDatabaseSchema, " CASCADE; CREATE SCHEMA ", resultsDatabaseSchema,";")
  DatabaseConnector::renderTranslateExecuteSql(sql = dropSchemaIfExists,
                                               connection = DatabaseConnector::connect(connectionDetails = connectionDetails))
  createResultsDataModel(connectionDetails = connectionDetails,
                         schema = resultsDatabaseSchema)
  
  specifications <- getResultsDataModelSpecifications()
  
  for (tableName in unique(specifications$tableName)) {
    expect_true(.tableExists(connection, resultsDatabaseSchema, tableName))
  }
  # Bad schema name
  expect_error(createResultsDataModel(connection = connection,
                                      schema = "non_existant_schema"))
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
      schema = resultsDatabaseSchema,
      zipFileName = listOfZipFilesToUpload[[i]]
    )
  }
  
  specifications <- getResultsDataModelSpecifications()
  
  # for (tableName in unique(specifications$tableName)) {
  #   primaryKey <- specifications %>%
  #     dplyr::filter(.data$tableName == !!tableName &
  #                     .data$primaryKey == "Yes") %>%
  #     dplyr::select(.data$fieldName) %>%
  #     dplyr::pull()
  #   
  #   if ("database_id" %in% primaryKey) {
  #     sql <-
  #       "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
  #     sql <- SqlRender::render(
  #       sql = sql,
  #       schema = resultsDatabaseSchema,
  #       table_name = tableName,
  #       database_id = "cdmv5"
  #     )
  #     databaseIdCount <- DatabaseConnector::querySql(connection, sql)[, 1]
  #     expect_true(databaseIdCount >= 0)
  #   }
  # }
})

test_that("Data removal works", {
  specifications <- getResultsDataModelSpecifications()
  
  if (DBI::dbIsValid(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
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
        schema = resultsDatabaseSchema,
        tableName = tableName,
        databaseId = "cdmv5"
      )
      
      sql <- "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
      sql <- SqlRender::render(
        sql = sql,
        schema = resultsDatabaseSchema,
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

test_that("Drop schema", {
  dropSchemaIfExists <- paste0("DROP SCHEMA IF EXISTS ", resultsDatabaseSchema, " CASCADE;")
  DatabaseConnector::renderTranslateExecuteSql(sql = dropSchemaIfExists,
                                               connection = DatabaseConnector::connect(connectionDetails = connectionDetails))
})
