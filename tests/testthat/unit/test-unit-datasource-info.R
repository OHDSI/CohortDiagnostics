library(testthat)
library(dplyr)
library(CohortDiagnostics)

test_that("getCdmDataSourceInformation works as expected", {
  mockConn <- mockDatabaseConnection()
  mockResult <- dplyr::tibble(
    cdmSourceName = "Test DB",
    cdmVersion = "5.4",
    vocabularyVersion = "v1"
  )

  local_mocked_bindings(
    existsTable = function(...) TRUE,
    getTableNames = function(...) c("cdm_source"),
    renderTranslateQuerySql = function(...) mockResult,
    dbms = function(...) "sqlite",
    .package = "DatabaseConnector"
  )
  
  local_mocked_bindings(
    dbms = function(...) "sqlite",
    renderTranslateQuerySql = function(...) mockResult,
    .package = "CohortDiagnostics"
  )
  
  info <- getCdmDataSourceInformation(
    connection = mockConn,
    cdmDatabaseSchema = "cdm"
  )

  expect_equal(info$cdmSourceName, "Test DB")
  expect_equal(info$cdmVersion, "5.4")
})

test_that("getCdmDataSourceInformation handles missing table", {
  mockConn <- mockDatabaseConnection()

  local_mocked_bindings(
    existsTable = function(...) FALSE,
    .package = "DatabaseConnector"
  )
  
  expect_warning(
    info <- getCdmDataSourceInformation(
      connection = mockConn,
      cdmDatabaseSchema = "cdm"
    ),
    "CDM Source table not found"
  )

  expect_null(info)
})

test_that("getCdmDataSourceInformation handles edge cases", {
  connection <- mockDatabaseConnection()
  
  # Mock existsTable to return TRUE
  local_mocked_bindings(
    existsTable = function(...) TRUE,
    .package = "DatabaseConnector"
  )
  
  # Case 1: Empty table
  local_mocked_bindings(
    renderTranslateQuerySql = function(...) data.frame(),
    .package = "CohortDiagnostics"
  )
  expect_warning(getCdmDataSourceInformation(connection = connection, cdmDatabaseSchema = "cdm"), "does not have any records")
  
  # Case 2: Multiple rows
  local_mocked_bindings(
    renderTranslateQuerySql = function(...) data.frame(a = c(1, 2)),
    .package = "CohortDiagnostics"
  )
  expect_warning(getCdmDataSourceInformation(connection = connection, cdmDatabaseSchema = "cdm"), "has more than one record")
  
  # Case 3: String dates instead of Date objects
  local_mocked_bindings(
    renderTranslateQuerySql = function(...) {
      data.frame(
        sourceDescription = "Test",
        cdmSourceName = "Test",
        sourceReleaseDate = "2023-01-01",
        cdmReleaseDate = "2023-01-01",
        cdmVersion = "5.3",
        vocabularyVersion = "v5.0"
      )
    },
    .package = "CohortDiagnostics"
  )
  info <- getCdmDataSourceInformation(connection = connection, cdmDatabaseSchema = "cdm")
  expect_s3_class(info$sourceReleaseDate, "Date")
  expect_s3_class(info$cdmReleaseDate, "Date")
  
  # Case 4: No connection or connectionDetails
  expect_error(getCdmDataSourceInformation(cdmDatabaseSchema = "cdm"), "Please provide either connection or connectionDetails")
})
