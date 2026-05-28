library(testthat)
library(CohortDiagnostics)
library(dplyr)

test_that("launchDiagnosticsExplorer works as expected with mocks", {
  testthat::skip_if_not_installed("OhdsiShinyModules")
  
  # Mocking requireNamespace in the package namespace
  origRequireNamespace <- base::requireNamespace
  local_mocked_bindings(
    requireNamespace = function(package, ...) {
      if (package == "OhdsiShinyModules") return(TRUE)
      origRequireNamespace(package, ...)
    },
    .package = "base"
  )
  
  # Use real temp files instead of mocking base file functions
  tempDir <- tempfile("shinytest")
  dir.create(tempDir)
  on.exit(unlink(tempDir, recursive = TRUE))
  
  sqlitePath <- file.path(tempDir, "test.sqlite")
  writeLines("", sqlitePath)
  
  # Mocking DatabaseConnector
  local_mocked_bindings(
    createConnectionDetails = function(...) list(dbms = "sqlite"),
    .package = "DatabaseConnector"
  )
  
  # Mocking shiny::runApp
  local_mocked_bindings(
    runApp = function(app, ...) {
      return("App launched")
    },
    .package = "shiny"
  )

  # Mocking requireNamespace is still hard, but let's assume it's installed
  # or it will fail gracefully with a message we can catch.
  
  # 1. Test basic launch with sqlite path
  result <- launchDiagnosticsExplorer(sqliteDbPath = sqlitePath, runOverNetwork = FALSE)
  expect_equal(result, "App launched")
  
  # 2. Test makePublishable
  publishDir <- file.path(tempDir, "publish")
  # Use a real dir
  result <- launchDiagnosticsExplorer(
    sqliteDbPath = sqlitePath, 
    makePublishable = TRUE, 
    publishDir = publishDir,
    overwritePublishDir = TRUE
  )
  expect_equal(result, "App launched")
  expect_true(file.exists(publishDir))
  
  # 3. Test with connectionDetails
  mockConnDetails <- list(dbms = "postgresql", server = "localhost")
  result <- launchDiagnosticsExplorer(
    connectionDetails = mockConnDetails,
    resultsDatabaseSchema = "main"
  )
  expect_equal(result, "App launched")
})
