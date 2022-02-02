skipResultsDm <- FALSE
if (Sys.getenv("CDM5_POSTGRESQL_SERVER") == "") {
  skipResultsDm <- TRUE
} else {
  postgresConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
    pathToDriver = jdbcDriverFolder
  )

  resultsDatabaseSchema <- paste0("r", gsub("[: -]", "", Sys.time(), perl = TRUE), sample(1:100, 1))

  # Always clean up
  withr::defer({
    pgConnection <- DatabaseConnector::connect(connectionDetails = postgresConnectionDetails)
    sql <- "DROP SCHEMA IF EXISTS @resultsDatabaseSchema CASCADE;"
    DatabaseConnector::renderTranslateExecuteSql(sql = sql,
                                                 resultsDatabaseSchema = resultsDatabaseSchema,
                                                 connection = pgConnection)

    DatabaseConnector::disconnect(pgConnection)
    unlink(folder, recursive = TRUE, force = TRUE)
  }, testthat::teardown_env())
}

test_that("Create schema", {
  skip_if(skipResultsDm | skipCdmTests, 'results data model test server not set')
  pgConnection <- DatabaseConnector::connect(connectionDetails = postgresConnectionDetails)
  with_dbc_connection(pgConnection, {
    sql <- "CREATE SCHEMA @resultsDatabaseSchema;"
    DatabaseConnector::renderTranslateExecuteSql(sql = sql,
                                                 resultsDatabaseSchema = resultsDatabaseSchema,
                                                 connection = pgConnection)
    createResultsDataModel(connectionDetails = postgresConnectionDetails,
                           schema = resultsDatabaseSchema)

    specifications <- getResultsDataModelSpecifications()

    for (tableName in unique(specifications$tableName)) {
      expect_true(.pgTableExists(pgConnection, resultsDatabaseSchema, tableName))
    }
    # Bad schema name
    expect_error(createResultsDataModel(connection = pgConnection,
                                        schema = "non_existant_schema"))
  })
})

test_that("Results upload", {
  skip_if(skipResultsDm | skipCdmTests, 'results data model test server not set')
  cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
    packageName = "CohortDiagnostics",
    settingsFileName = "settings/CohortsToCreateForTesting.csv",
    cohortFileNameValue = c("cohortId")
  ) %>% dplyr::filter(cohortId %in% cohortIds)

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

  executeDiagnostics(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortIds = cohortIds,
    cohortDefinitionSet = cohortDefinitionSet,
    exportFolder = file.path(folder, "export"),
    databaseId = dbms,
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
    incrementalFolder = file.path(folder, "incremental"),
    covariateSettings = covariateSettings,
    temporalCovariateSettings = temporalCovariateSettings
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
      connectionDetails = postgresConnectionDetails,
      schema = resultsDatabaseSchema,
      zipFileName = listOfZipFilesToUpload[[i]]
    )
  }

  specifications <- getResultsDataModelSpecifications()
  pgConnection <- DatabaseConnector::connect(connectionDetails = postgresConnectionDetails)
  with_dbc_connection(pgConnection, {
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
          schema = resultsDatabaseSchema,
          table_name = tableName,
          database_id = "cdmv5"
        )
        databaseIdCount <- DatabaseConnector::querySql(pgConnection, sql)[, 1]
        expect_true(databaseIdCount >= 0)
      }
    }
  })
})

test_that("Sqlite results data model", {
  dbFile <- tempfile(fileext = ".sqlite")
  createMergedResultsFile(dataFolder = file.path(folder, "export"), sqliteDbPath = dbFile, overwrite = TRUE)
  connectionDetailsSqlite <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = dbFile)
  connectionSqlite <- DatabaseConnector::connect(connectionDetails = connectionDetailsSqlite)
  with_dbc_connection(connectionSqlite, {
    # Bad schema name
    expect_error(createResultsDataModel(connection = connectionSqlite,
                                        schema = "non_existant_schema"))

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
          schema = "main",
          table_name = tableName,
          database_id = "cdmv5"
        )
        databaseIdCount <- DatabaseConnector::querySql(connectionSqlite, sql)[, 1]
        expect_true(databaseIdCount >= 0)
      }
    }
  })
})




test_that("Data removal works", {
  skip_if(skipResultsDm | skipCdmTests, 'results data model test server not set')
  specifications <- getResultsDataModelSpecifications()

  pgConnection <- DatabaseConnector::connect(connectionDetails = postgresConnectionDetails)
  with_dbc_connection(pgConnection, {
    for (tableName in unique(specifications$tableName)) {
      primaryKey <- specifications %>%
        dplyr::filter(.data$tableName == !!tableName &
                        .data$primaryKey == "Yes") %>%
        dplyr::select(.data$fieldName) %>%
        dplyr::pull()

      if ("database_id" %in% primaryKey) {
        deleteAllRecordsForDatabaseId(
          connection = pgConnection,
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
          DatabaseConnector::querySql(pgConnection, sql)[, 1]
        expect_true(databaseIdCount == 0)
      }
    }
  })
})

test_that("util functions", {
  expect_true(naToEmpty(NA) == "")
  expect_true(naToZero(NA) == 0)
})

