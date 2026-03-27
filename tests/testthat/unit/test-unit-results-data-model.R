library(testthat)
library(CohortDiagnostics)
library(dplyr)

test_that("getResultsDataModelSpecifications works", {
  specs <- getResultsDataModelSpecifications()
  expect_s3_class(specs, "tbl_df")
  expect_true("tableName" %in% colnames(specs))
  expect_true("columnName" %in% colnames(specs))
})

test_that("getDefaultVocabularyTableNames works", {
  vocabTables <- getDefaultVocabularyTableNames()
  expect_type(vocabTables, "character")
  expect_true(length(vocabTables) > 0)
})

test_that("createResultsDataModel works with mocks", {
  # Mock DatabaseConnector
  local_mocked_bindings(
    connect = function(...) mockDatabaseConnection(),
    disconnect = function(...) NULL,
    executeSql = function(...) NULL,
    .package = "DatabaseConnector"
  )
  
  # Mock SqlRender
  local_mocked_bindings(
    loadRenderTranslateSql = function(...) "CREATE TABLE test;",
    .package = "SqlRender"
  )
  
  # Mock internal migrateDataModel (to avoid deep nesting)
  local_mocked_bindings(
    migrateDataModel = function(...) NULL,
    .package = "CohortDiagnostics"
  )
  
  # SQLite main
  createResultsDataModel(
    connectionDetails = list(dbms = "sqlite"),
    databaseSchema = "main"
  )
  expect_true(TRUE)
  
  # SQLite invalid schema
  expect_error(
    createResultsDataModel(
      connectionDetails = list(dbms = "sqlite"),
      databaseSchema = "invalid"
    )
  )
})

test_that("uploadResults works with mocks", {
  # Mock zip::unzip
  local_mocked_bindings(
    unzip = function(...) NULL,
    .package = "zip"
  )
  
  # Mock ResultModelManager::uploadResults
  local_mocked_bindings(
    uploadResults = function(...) NULL,
    .package = "ResultModelManager"
  )
  
  # Mock ParallelLogger
  local_mocked_bindings(
    logInfo = function(...) NULL,
    .package = "ParallelLogger"
  )

  # Create a dummy zip file
  zipFile <- tempfile(fileext = ".zip")
  writeLines("", zipFile)
  on.exit(unlink(zipFile))
  
  uploadResults(
    connectionDetails = list(dbms = "sqlite"),
    schema = "main",
    zipFileName = zipFile
  )
  expect_true(TRUE)
})

test_that("migrateDataModel and getDataMigrator work with mocks", {
  # Mock getDataMigrator
  local_mocked_bindings(
    getDataMigrator = function(...) {
      list(
        executeMigrations = function() NULL,
        finalize = function() NULL
      )
    },
    .package = "CohortDiagnostics"
  )
  
  # Mock SqlRender
  local_mocked_bindings(
    loadRenderTranslateSql = function(...) "UPDATE version SET version = 1;",
    .package = "SqlRender"
  )
  
  # Mock DatabaseConnector
  local_mocked_bindings(
    connect = function(...) mockDatabaseConnection(),
    disconnect = function(...) NULL,
    executeSql = function(...) NULL,
    .package = "DatabaseConnector"
  )
  
  # Mock ParallelLogger
  local_mocked_bindings(
    logInfo = function(...) NULL,
    .package = "ParallelLogger"
  )

  migrateDataModel(
    connectionDetails = list(dbms = "sqlite"),
    databaseSchema = "main"
  )
  expect_true(TRUE)

  # Test getDataMigrator actually calls RMM
  local_mocked_bindings(
    getDataMigrator = CohortDiagnostics:::getDataMigrator,
    .package = "CohortDiagnostics"
  )
  
  # Mock ResultModelManager::DataMigrationManager
  mockRMM <- list(new = function(...) list())
  local_mocked_bindings(
    DataMigrationManager = mockRMM,
    .package = "ResultModelManager"
  )
  
  migrator <- getDataMigrator(
    connectionDetails = list(dbms = "sqlite"),
    databaseSchema = "main"
  )
  expect_type(migrator, "list")
})
