library(testthat)
library(CohortDiagnostics)
library(dplyr)

test_that("launchDiagnosticsExplorer works as expected with mocks", {
  # Mocking file operations in the CohortDiagnostics namespace
  local_mocked_bindings(
    file.exists = function(...) TRUE,
    normalizePath = function(path, ...) path,
    dir.create = function(...) TRUE,
    file.copy = function(...) TRUE,
    .package = "CohortDiagnostics"
  )
  
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

  # 1. Test basic launch with sqlite path
  # We use a dummy path that 'exists' because of our mock
  result <- launchDiagnosticsExplorer(sqliteDbPath = "test.sqlite", runOverNetwork = FALSE)
  expect_equal(result, "App launched")
  
  # 2. Test makePublishable
  publishDir <- tempfile("publish")
  result <- launchDiagnosticsExplorer(
    sqliteDbPath = "test.sqlite", 
    makePublishable = TRUE, 
    publishDir = publishDir,
    overwritePublishDir = TRUE
  )
  expect_equal(result, "App launched")
  
  # 3. Test with connectionDetails
  mockConnDetails <- list(dbms = "postgresql", server = "localhost")
  result <- launchDiagnosticsExplorer(
    connectionDetails = mockConnDetails,
    resultsDatabaseSchema = "main"
  )
  expect_equal(result, "App launched")
})
