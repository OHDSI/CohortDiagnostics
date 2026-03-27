library(testthat)
library(CohortDiagnostics)
library(dplyr)

test_that("runInclusionStatisticsDiagnostic requires initialized context", {
  context <- list(isInitialized = FALSE)
  class(context) <- "DiagnosticsContext"
  
  expect_error(runInclusionStatisticsDiagnostic(context), "Diagnostics context not initialized")
})

test_that("runInclusionStatisticsDiagnostic exits early if no cohorts", {
  skip_if_not_installed("testthat", "3.0.0")
  context <- list(isInitialized = TRUE, cohortDefinitionSet = data.frame())
  class(context) <- "DiagnosticsContext"
  
  local_mocked_bindings(
    logWarn = function(msg) { },
    .package = "ParallelLogger"
  )
  
  expect_invisible(runInclusionStatisticsDiagnostic(context))
})

test_that("runInclusionStatisticsDiagnostic executes getInclusionStats correctly", {
  skip_if_not_installed("testthat", "3.0.0")
  
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))
  
  context <- createDiagnosticsContext(
    connectionDetails = list(dbms = "sqlite"),
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "cohort",
    databaseId = "test",
    exportFolder = exportFolder
  )
  context$isInitialized <- TRUE
  context$incrementalFolder <- file.path(exportFolder, "incremental")
  dir.create(context$incrementalFolder, showWarnings = FALSE)
  
  cohortDefinitionSet <- createMockCohortDefinitionSet(numCohorts = 2)
  
  calls <- list()
  local_mocked_bindings(
    getInclusionStats = function(...) {
      calls <<- c(calls, "getInclusionStats")
    },
    timeExecution = function(folder, taskName, ...) {
      calls <<- c(calls, taskName)
      args <- list(...)
      eval(args$expr)
    },
    .package = "CohortDiagnostics"
  )
  
  # Cohort subsetting
  runInclusionStatisticsDiagnostic(context, cohortDefinitionSet = cohortDefinitionSet, cohortIds = c(1))
  
  expect_true("getInclusionStats" %in% calls)
})
