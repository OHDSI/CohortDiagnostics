library(testthat)
library(CohortDiagnostics)
library(dplyr)

test_that("initializeDiagnostics sets up context correctly", {
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))
  
  context <- createDiagnosticsContext(
    databaseId = "test",
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "results",
    cohortTable = "cohort",
    exportFolder = exportFolder,
    databaseName = "TestDB",
    databaseDescription = "TestDesc"
  )
  # Set connectionDetails to avoid initializeDiagnostics stopping
  context$connectionDetails <- list(dbms = "sqlite")
  
  cohortDefinitionSet <- createMockCohortDefinitionSet(numCohorts = 1)
  
  # Mock all dependencies in DatabaseConnector specifically
  local_mocked_bindings(
    connect = function(...) mockDatabaseConnection(),
    existsTable = function(...) TRUE,
    getTableNames = function(...) c("cdm_source"),
    renderTranslateQuerySql = function(...) {
      dplyr::tibble(
        observationPeriodMinDate = as.Date("2000-01-01"),
        observationPeriodMaxDate = as.Date("2020-12-31"),
        persons = 100,
        records = 1000,
        personDays = 100000
      )
    },
    renderTranslateExecuteSql = function(...) NULL,
    dbms = function(...) "sqlite",
    .package = "DatabaseConnector"
  )
  local_mocked_bindings(
    dbms = function(...) "sqlite",
    renderTranslateQuerySql = function(...) {
      dplyr::tibble(
        observationPeriodMinDate = as.Date("2000-01-01"),
        observationPeriodMaxDate = as.Date("2020-12-31"),
        persons = 100,
        records = 1000,
        personDays = 100000
      )
    },
    .package = "CohortDiagnostics"
  )
  
  local_mocked_bindings(
    getCdmDataSourceInformation = function(...) {
      dplyr::tibble(
        cdmSourceName = "Test DB",
        sourceDescription = "Test Desc",
        sourceReleaseDate = as.Date("2021-01-01"),
        cdmReleaseDate = as.Date("2021-01-01"),
        cdmVersion = "5.3",
        vocabularyVersion = "v5.0"
      )
    },
    getVocabularyVersion = function(...) "v5.0",
    saveDatabaseMetaData = function(...) NULL,
    createConceptTable = function(...) NULL,
    computeCohortCounts = function(...) {
      dplyr::tibble(
        cohortId = 1,
        cohortEntries = 10,
        cohortSubjects = 10
      )
    },
    .package = "CohortDiagnostics"
  )
  
  # Silent ParallelLogger
  local_mocked_bindings(
    logInfo = function(...) NULL,
    logTrace = function(...) NULL,
    addDefaultFileLogger = function(...) NULL,
    addDefaultErrorReportLogger = function(...) NULL,
    unregisterLogger = function(...) NULL,
    .package = "ParallelLogger"
  )

  updatedContext <- initializeDiagnostics(context, cohortDefinitionSet)
  
  expect_true(updatedContext$isInitialized)
  expect_equal(updatedContext$databaseName, "TestDB")
  expect_equal(length(updatedContext$instantiatedCohorts), 1)
  expect_true(file.exists(file.path(exportFolder, "cohort.csv")))
})

test_that("extractConceptSetsSqlFromCohortSql handles different casing", {
  sql <- "SELECT 0 as codeset_id (SELECT 1) C with primary_events"
  result <- CohortDiagnostics:::extractConceptSetsSqlFromCohortSql(sql)
  expect_equal(nrow(result), 1)
  expect_equal(result$conceptSetId[1], 0)
})

test_that("initializeDiagnostics fails with no cohorts", {
  context <- createDiagnosticsContext(
    databaseId = "test", 
    cdmDatabaseSchema = "cdm", 
    cohortDatabaseSchema = "results",
    exportFolder = tempfile()
  )
  expect_error(initializeDiagnostics(context, data.frame(cohortId = 1)), "sql")
})
