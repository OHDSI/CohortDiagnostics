library(testthat)
library(CohortDiagnostics)
library(dplyr)

test_that("createConceptTable executes SQL correctly", {
  skip_if_not_installed("testthat", "3.0.0")
  connection <- mockDatabaseConnection("sqlite")
  
  calls <- list()
  local_mocked_bindings(
    executeSql = function(conn, sql, ...) {
      calls <<- c(calls, sql)
    },
    .package = "DatabaseConnector"
  )
  
  CohortDiagnostics:::createConceptTable(connection, tempEmulationSchema = "temp")
  
  expect_equal(length(calls), 1)
  expect_match(calls[[1]], "CREATE TEMP TABLE concept_ids")
})

test_that("exportConceptInformation handles empty concept IDs correctly", {
  skip_if_not_installed("testthat", "3.0.0")
  connection <- mockDatabaseConnection("sqlite")
  
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))
  
  local_mocked_bindings(
    getTableNames = function(...) c("concept"),
    renderTranslateQuerySql = function(connection, sql, ...) {
      return(data.frame(concept_id = numeric(0)))
    },
    .package = "DatabaseConnector"
  )
  
  res <- CohortDiagnostics:::exportConceptInformation(
    connection = connection,
    vocabularyDatabaseSchema = "cdm",
    tempEmulationSchema = "temp",
    conceptIdTable = "#concept_ids",
    vocabularyTableNames = c("concept"),
    incremental = FALSE,
    exportFolder = exportFolder
  )
  
  expect_null(res)
})

test_that("exportConceptInformation exports tables correctly", {
  skip_if_not_installed("testthat", "3.0.0")
  connection <- mockDatabaseConnection("sqlite")
  
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))
  
  local_mocked_bindings(
    getTableNames = function(...) c("concept", "domain", "relationship"),
    dbms = function(connection) connection@dbms,
    renderTranslateQuerySql = function(connection, sql, ...) {
      params <- list(...)
      if (!is.null(params$table) && params$table == "domain") {
        return(data.frame(domainId = "Condition", domainName = "Condition", domainConceptId = 1))
      } else if (!is.null(params$table) && params$table == "relationship") {
         return(data.frame(
             relationshipId = "Is a",
             relationshipName = "Is a",
             isHierarchical = "1",
             definesAncestry = "1",
             reverseRelationshipId = "Is a",
             relationshipConceptId = 1
         ))
      } else if (!is.null(params$table) && params$table == "concept") {
        return(data.frame(
            conceptId = c(1, 2),
            conceptName = c("A", "B"),
            domainId = "Condition",
            vocabularyId = "SNOMED",
            conceptClassId = "Clinical Finding",
            conceptCode = "123",
            validStartDate = as.Date("2000-01-01"),
            validEndDate = as.Date("2099-12-31")
        ))
      } else {
        return(data.frame(conceptId = c(1, 2)))
      }
    },
    .package = "DatabaseConnector"
  )
  
  CohortDiagnostics:::exportConceptInformation(
    connection = connection,
    vocabularyDatabaseSchema = "cdm",
    tempEmulationSchema = "temp",
    conceptIdTable = "#concept_ids",
    vocabularyTableNames = c("concept", "domain", "relationship"),
    incremental = FALSE,
    exportFolder = exportFolder
  )
  
  expect_true(file.exists(file.path(exportFolder, "concept.csv")))
  expect_true(file.exists(file.path(exportFolder, "domain.csv")))
  expect_true(file.exists(file.path(exportFolder, "relationship.csv")))
})
