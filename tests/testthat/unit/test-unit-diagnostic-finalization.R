library(testthat)
library(CohortDiagnostics)
library(dplyr)

test_with_mock_context <- function() {
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  
  context <- createDiagnosticsContext(
    databaseId = "test",
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "results",
    cohortTable = "cohort",
    exportFolder = exportFolder,
    databaseName = "TestDB",
    databaseDescription = "TestDesc"
  )
  context$isInitialized <- TRUE
  context$startTime <- Sys.time()
  context$connection <- mockDatabaseConnection()
  context$createdConnection <- FALSE
  context$vocabularyDatabaseSchema <- "vocab"
  context$tempEmulationSchema <- NULL
  context$incremental <- FALSE
  context$cdmSourceInformation <- dplyr::tibble(
    sourceDescription = "Desc",
    cdmSourceName = "Name",
    sourceReleaseDate = as.Date("2021-01-01"),
    cdmVersion = "5.3",
    cdmReleaseDate = as.Date("2021-01-01")
  )
  context$vocabularyVersion <- "v5.0"
  context$observationPeriodDateRange <- dplyr::tibble(
    observationPeriodMinDate = as.Date("2000-01-01"),
    observationPeriodMaxDate = as.Date("2020-12-31"),
    persons = 100,
    records = 1000,
    personDays = 100000
  )
  context$databaseName <- "TestDB"
  context$databaseDescription <- "TestDesc"
  
  return(context)
}

test_that("finalizeDiagnostics works as expected", {
  context <- test_with_mock_context()
  on.exit(unlink(context$exportFolder, recursive = TRUE))

  # Mock all dependencies in both packages to be sure
  local_mocked_bindings(
    renderTranslateExecuteSql = function(...) NULL,
    disconnect = function(...) NULL,
    .package = "DatabaseConnector"
  )
  
  local_mocked_bindings(
    renderTranslateExecuteSql = function(...) NULL,
    disconnect = function(...) NULL,
    .package = "CohortDiagnostics"
  )
  
  local_mocked_bindings(
    exportConceptInformation = function(...) NULL,
    writeResultsZip = function(...) NULL,
    .package = "CohortDiagnostics"
  )
  
  # Silent ParallelLogger
  local_mocked_bindings(
    logInfo = function(...) NULL,
    logTrace = function(...) NULL,
    .package = "ParallelLogger"
  )

  result <- finalizeDiagnostics(context)
  
  expect_true(file.exists(file.path(context$exportFolder, "metadata.csv")))
  expect_invisible(finalizeDiagnostics(context))
})

test_that("finalizeDiagnostics fails if not initialized", {
  context <- createDiagnosticsContext(
    databaseId = "test", 
    cdmDatabaseSchema = "cdm", 
    cohortDatabaseSchema = "results",
    exportFolder = tempfile()
  )
  expect_error(finalizeDiagnostics(context), "not initialized")
})
