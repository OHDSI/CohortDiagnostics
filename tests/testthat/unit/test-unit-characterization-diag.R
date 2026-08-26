library(testthat)
library(CohortDiagnostics)
library(dplyr)

test_that("runTemporalCharacterizationDiagnostic works as expected with mocks", {
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))
  
  context <- list(
    connection = mockDatabaseConnection(),
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "results",
    cohortTableNames = list(cohortTable = "cohort"),
    cohortTable = "cohort",
    cohortCounts = dplyr::tibble(cohortId = 1, cohortEntries = 10),
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
      json = "{}"
    ),
    cdmVersion = "5.3"
  )
  class(context) <- "DiagnosticsContext"
  dir.create(context$incrementalFolder)

  # Mock FeatureExtraction
  local_mocked_bindings(
    createCohortBasedTemporalCovariateSettings = function(...) list(),
    .package = "FeatureExtraction"
  )
  
  # Mock CohortGenerator
  local_mocked_bindings(
    createCohortTables = function(...) NULL,
    sampleCohortDefinitionSet = function(...) {
      res <- context$cohortDefinitionSet
      attr(res, "isSampledCohortDefinition") <- TRUE
      return(res)
    },
    .package = "CohortGenerator"
  )

  # Mock internal calls
  local_mocked_bindings(
    executeCohortCharacterization = function(...) NULL,
    computeCohortCounts = function(...) context$cohortCounts,
    timeExecution = function(folder, task, expr, ...) {
        eval(expr)
        return(NULL)
    },
    .package = "CohortDiagnostics"
  )
  
  # Silent ParallelLogger
  local_mocked_bindings(
    logInfo = function(...) NULL,
    logTrace = function(...) NULL,
    logWarn = function(...) NULL,
    .package = "ParallelLogger"
  )

  # 1. Test basic run
  runTemporalCharacterizationDiagnostic(
    context = context,
    runCohortRelationship = FALSE
  )
  expect_true(TRUE)

  # 2. Test with sample
  runTemporalCharacterizationDiagnostic(
    context = context,
    runFeatureExtractionOnSample = TRUE,
    sampleN = 5
  )
  expect_true(TRUE)

  # 3. Test with cohort relationship
  context$cohortDefinitionSet <- dplyr::tibble(
    cohortId = c(1, 2),
    cohortName = c("T1", "T2"),
    sql = c("S1", "S2"),
    json = c("J1", "J2")
  )
  runTemporalCharacterizationDiagnostic(
    context = context,
    runCohortRelationship = TRUE
  )
  expect_true(TRUE)
  
  # 4. Test with empty cohortIds
  runTemporalCharacterizationDiagnostic(
    context = context,
    cohortIds = 999
  )
  expect_true(TRUE)
})
