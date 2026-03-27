library(testthat)
library(CohortDiagnostics)
library(dplyr)

test_that("runConceptSetDiagnostics works with mocks", {
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))
  
  context <- list(
    connection = mockDatabaseConnection(),
    cdmDatabaseSchema = "cdm",
    vocabularyDatabaseSchema = "vocab",
    tempEmulationSchema = NULL,
    databaseId = "test",
    exportFolder = exportFolder,
    minCellCount = 5,
    incremental = FALSE,
    isInitialized = TRUE,
    incrementalFolder = tempfile("inc"),
    instantiatedCohorts = 1,
    cohortDefinitionSet = dplyr::tibble(
      cohortId = 1,
      cohortName = "Test",
      sql = "SELECT 1",
      json = "{}",
      checksum = "abc",
      isSubset = FALSE
    ),
    recordKeepingFile = tempfile()
  )
  class(context) <- "DiagnosticsContext"
  dir.create(context$incrementalFolder)
  
  # Mock all internal calls
  local_mocked_bindings(
    loadRenderTranslateSql = function(...) "SELECT 1;",
    .package = "SqlRender"
  )
  
  local_mocked_bindings(
    executeSql = function(...) NULL,
    renderTranslateQuerySql = function(connection, sql, ...) {
      if (grepl("inst_concept_sets", sql)) {
        return(dplyr::tibble(
          uniqueConceptSetId = 1,
          cohortId = 1,
          conceptSetId = 1,
          conceptSetSql = "SELECT 1"
        ))
      } else if (grepl("include_source_concept_table", sql)) {
        return(dplyr::tibble(
          conceptSetId = 1,
          conceptId = 100,
          sourceConceptId = 101,
          conceptCount = 10,
          conceptSubjects = 10
        ))
      } else if (grepl("orphan_concepts", sql)) {
          return(dplyr::tibble(
              conceptId = 200,
              conceptCount = 5,
              conceptSubjects = 5
          ))
      }
      return(dplyr::tibble(dummy = 1)[0, ])
    },
    renderTranslateExecuteSql = function(...) NULL,
    .package = "DatabaseConnector"
  )
  
  local_mocked_bindings(
    makeDataExportable = function(x, ...) x,
    writeToCsv = function(...) NULL,
    recordTasksDone = function(...) NULL,
    .package = "CohortDiagnostics"
  )
  
  # Silent ParallelLogger
  local_mocked_bindings(
    logInfo = function(...) NULL,
    logTrace = function(...) NULL,
    .package = "ParallelLogger"
  )

  # Run the diagnostic with all flags TRUE
  runConceptSetDiagnostic(
    context = context,
    runIncludedSourceConcepts = TRUE,
    runOrphanConcepts = TRUE,
    runBreakdownIndexEvents = TRUE
  )
  
  expect_true(TRUE)
})
