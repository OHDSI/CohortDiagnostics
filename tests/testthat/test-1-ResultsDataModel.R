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

  resultsDatabaseSchema <- paste0("r", Sys.getpid(), format(Sys.time(), "%s"), sample(1:100, 1))

  # Always clean up
  withr::defer(
    {
      pgConnection <- DatabaseConnector::connect(connectionDetails = postgresConnectionDetails)
      sql <- "DROP SCHEMA IF EXISTS @resultsDatabaseSchema CASCADE;"
      DatabaseConnector::renderTranslateExecuteSql(
        sql = sql,
        resultsDatabaseSchema = resultsDatabaseSchema,
        connection = pgConnection
      )

      DatabaseConnector::disconnect(pgConnection)
      unlink(folder, recursive = TRUE, force = TRUE)
    },
    testthat::teardown_env()
  )
}

test_that("Create schema", {
  skip_if(skipResultsDm | skipCdmTests, "results data model test server not set")
  pgConnection <- DatabaseConnector::connect(connectionDetails = postgresConnectionDetails)
  with_dbc_connection(pgConnection, {
    sql <- "CREATE SCHEMA @resultsDatabaseSchema;"
    DatabaseConnector::renderTranslateExecuteSql(
      sql = sql,
      resultsDatabaseSchema = resultsDatabaseSchema,
      connection = pgConnection
    )
    createResultsDataModel(
      connectionDetails = postgresConnectionDetails,
      databaseSchema = resultsDatabaseSchema,
      tablePrefix = "cd_"
    )

    specifications <- getResultsDataModelSpecifications()

    for (tableName in unique(specifications$tableName)) {
      expect_true(.pgTableExists(pgConnection, resultsDatabaseSchema, paste0("cd_", tableName)))
    }
    # Bad schema name
    expect_error(createResultsDataModel(
      connectionDetails = postgresConnectionDetails,
      databaseSchema = "non_existant_schema"
    ))
  })
})

test_that("Results upload", {
  skip_if(skipResultsDm | skipCdmTests, "results data model test server not set")
  if (dbms == "sqlite") {
    # Checks to see if adding extra OMOP vocab, unexpectedly breaks things
    connection <- DatabaseConnector::connect(connectionDetails)
    with_dbc_connection(connection, {
      DatabaseConnector::renderTranslateExecuteSql(connection, "
INSERT INTO main.vocabulary
(VOCABULARY_ID, VOCABULARY_NAME, VOCABULARY_REFERENCE, VOCABULARY_VERSION, VOCABULARY_CONCEPT_ID) VALUES
('None','OMOP Standardized Vocabularies','OMOP generated','v5.5 17-FEB-22',44819096);

INSERT INTO CDM_SOURCE
(CDM_SOURCE_NAME,CDM_SOURCE_ABBREVIATION,CDM_HOLDER,SOURCE_DESCRIPTION,SOURCE_DOCUMENTATION_REFERENCE,CDM_ETL_REFERENCE,SOURCE_RELEASE_DATE,CDM_RELEASE_DATE,CDM_VERSION,VOCABULARY_VERSION)
VALUES ('Synthea','Synthea','OHDSI Community','SyntheaTM is a Synthetic Patient Population Simulator.','https://synthetichealth.github.io/synthea/','https://github.com/OHDSI/ETL-Synthea',1558742400,1558742400,'v5.4','v5.0 22-JAN-22');")

      # Check to see if non-standard extra columns are handled
      DatabaseConnector::renderTranslateExecuteSql(
        connection,
        "ALTER TABLE VOCABULARY ADD TEST_COLUMN varchar(255) DEFAULT 'foo';"
      )
    })
  }

  if (dbms == "sqlite") {
    expect_warning(
      {
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
          runTemporalCohortCharacterization = TRUE,
          runIncidenceRate = TRUE,
          runIncludedSourceConcepts = TRUE,
          runOrphanConcepts = TRUE,
          incremental = TRUE,
          incrementalFolder = file.path(folder, "incremental"),
          temporalCovariateSettings = temporalCovariateSettings,
          runFeatureExtractionOnSample = TRUE
        )
      },
      "CDM Source table has more than one record while only one is expected."
    )
  } else {
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
      runTemporalCohortCharacterization = TRUE,
      runIncidenceRate = TRUE,
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = TRUE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental"),
      temporalCovariateSettings = temporalCovariateSettings,
      runFeatureExtractionOnSample = TRUE
    )
  }

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
      zipFileName = listOfZipFilesToUpload[[i]],
      tablePrefix = "cd_"
    )
  }

  specifications <- getResultsDataModelSpecifications()
  pgConnection <- DatabaseConnector::connect(connectionDetails = postgresConnectionDetails)
  with_dbc_connection(pgConnection, {
    for (tableName in unique(specifications$tableName)) {
      primaryKey <- specifications %>%
        dplyr::filter(tableName == !!tableName &
          primaryKey == "Yes") %>%
        dplyr::select("columnName") %>%
        dplyr::pull()

      if ("database_id" %in% primaryKey) {
        sql <-
          "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
        sql <- SqlRender::render(
          sql = sql,
          schema = resultsDatabaseSchema,
          table_name = paste0("cd_", tableName),
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
  expect_error(createMergedResultsFile(
    dataFolder = file.path("non_existant_export_folder"),
    sqliteDbPath = dbFile,
    overwrite = TRUE,
    tablePrefix = "cd_"
  ))
  connectionDetailsSqlite <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = dbFile)
  connectionSqlite <- DatabaseConnector::connect(connectionDetails = connectionDetailsSqlite)
  with_dbc_connection(connectionSqlite, {
    # Bad schema name
    expect_error(createResultsDataModel(
      connectionDetails = connectionDetailsSqlite,
      databaseSchema = "non_existant_schema"
    ))
  })
})
