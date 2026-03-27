library(testthat)
library(CohortDiagnostics)

# To avoid infinite recursion when mocking base functions
orig_requireNamespace <- base::requireNamespace

test_that("launchDiagnosticsExplorer stops if OhdsiShinyModules is missing", {
  # Mock requireNamespace to return FALSE for OhdsiShinyModules
  local_mocked_bindings(
    requireNamespace = function(package, ...) {
      if (package == "OhdsiShinyModules") return(FALSE)
      orig_requireNamespace(package, ...)
    },
    .package = "base"
  )
  
  expect_error(launchDiagnosticsExplorer(), "OhdsiShinyModules must be installed")
})

test_that("launchDiagnosticsExplorer stops if sqliteDbPath does not exist", {
  # Mock requireNamespace to return TRUE for OhdsiShinyModules
  local_mocked_bindings(
    requireNamespace = function(package, ...) {
      if (package == "OhdsiShinyModules") return(TRUE)
      orig_requireNamespace(package, ...)
    },
    .package = "base"
  )
  
  expect_error(launchDiagnosticsExplorer(sqliteDbPath = "non_existent.sqlite"), "not found")
})

test_that("launchDiagnosticsExplorer sets up global settings and calls runApp", {
  # Mock requireNamespace to return TRUE for OhdsiShinyModules
  local_mocked_bindings(
    requireNamespace = function(package, ...) {
      if (package == "OhdsiShinyModules") return(TRUE)
      orig_requireNamespace(package, ...)
    },
    .package = "base"
  )
  
  # Create a dummy sqlite file
  tempSqlite <- tempfile(fileext = ".sqlite")
  cat("dummy", file = tempSqlite)
  on.exit(unlink(tempSqlite))
  
  runAppCalled <- FALSE
  # Mock shiny::runApp
  local_mocked_bindings(
    runApp = function(appDir, ...) {
      runAppCalled <<- TRUE
      return(NULL)
    },
    .package = "shiny"
  )
  
  # Mock DatabaseConnector::createConnectionDetails
  local_mocked_bindings(
    createConnectionDetails = function(...) {
      list(dbms = "sqlite", server = "dummy")
    },
    .package = "DatabaseConnector"
  )

  # Check if it runs (should not error)
  launchDiagnosticsExplorer(sqliteDbPath = tempSqlite)
  
  expect_true(runAppCalled)
})

test_that("launchDiagnosticsExplorer handles shinyConfigPath", {
  # Mock requireNamespace to return TRUE for OhdsiShinyModules
  local_mocked_bindings(
    requireNamespace = function(package, ...) {
      if (package == "OhdsiShinyModules") return(TRUE)
      orig_requireNamespace(package, ...)
    },
    .package = "base"
  )
  
  tempConfig <- tempfile(fileext = ".yml")
  cat("dummy: config", file = tempConfig)
  on.exit(unlink(tempConfig))
  
  runAppCalled <- FALSE
  local_mocked_bindings(
    runApp = function(appDir, ...) {
      runAppCalled <<- TRUE
      return(NULL)
    },
    .package = "shiny"
  )
  
  launchDiagnosticsExplorer(shinyConfigPath = tempConfig)
  expect_true(runAppCalled)
})

test_that("createMergedResultsFile stops if file exists and no overwrite", {
  tmpFile <- tempfile(fileext = ".sqlite")
  cat("dummy", file = tmpFile)
  on.exit(unlink(tmpFile))
  
  expect_error(createMergedResultsFile(dataFolder = "dummy", sqliteDbPath = tmpFile, overwrite = FALSE), "already exists")
})

test_that("createDiagnosticsExplorerZip validates paths", {
  expect_error(createDiagnosticsExplorerZip(sqliteDbPath = "non_existent.sqlite"), "not TRUE")
})

test_that("createDiagnosticsExplorerZip works with mocks", {
  tmpSqlite <- tempfile(fileext = ".sqlite")
  cat("dummy", file = tmpSqlite)
  on.exit(unlink(tmpSqlite))
  
  tmpZip <- tempfile(fileext = ".zip")
  on.exit(unlink(tmpZip))
  
  # Mock DatabaseConnector::createZipFile
  zipCalled <- FALSE
  local_mocked_bindings(
    createZipFile = function(...) {
      zipCalled <<- TRUE
    },
    .package = "DatabaseConnector"
  )
  
  # Mock dir.exists for shinyDirectory
  local_mocked_bindings(
    dir.exists = function(...) TRUE,
    .package = "base"
  )

  createDiagnosticsExplorerZip(outputZipfile = tmpZip, sqliteDbPath = tmpSqlite, overwrite = TRUE)
  expect_true(zipCalled)
})
