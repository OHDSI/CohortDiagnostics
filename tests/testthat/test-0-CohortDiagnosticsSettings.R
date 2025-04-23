test_that("createCohortDiagnosticsSettings creates an object with valid settings", {
  cohortDefinitionSet <- data.frame(
    cohortId = 1:2,
    cohortName = c("Cohort A", "Cohort B"),
    json = c("{}", "{}"),
    sql = c("SELECT * FROM cdm.cohort WHERE cohort_id = 1", "SELECT * FROM cdm.cohort WHERE cohort_id = 2")
  )

  cohortTableNames <- list(
    cohortTable = "cohort",
    cohortInclusionTable = "cohort_inclusion",
    cohortInclusionResultTable = "cohort_inclusion_result",
    cohortInclusionStatsTable = "cohort_inclusion_stats",
    cohortSummaryStatsTable = "cohort_summary_stats",
    cohortCensorStatsTable = "cohort_censor_stats"
  )

  settings <- createCohortDiagnosticsSettings(
    cohortDefinitionSet = cohortDefinitionSet,
    exportFolder = "output_folder",
    databaseId = "test_db",
    cohortDatabaseSchema = "cdm",
    cdmDatabaseSchema = "cdm",
    cohortTableNames = cohortTableNames,
    minCellCount = 3
  )

  checkmate::expect_class(settings, "CohortDiagnosticsSettings")
  checkmate::expect_r6(settings, "R6")

  # Check that various public attributes are correctly assigned.
  expect_equal(settings$exportFolder, "output_folder")
  expect_equal(settings$databaseId, "test_db")
  expect_equal(settings$minCellCount, 3)
})

test_that("createCohortDiagnosticsSettings throws errors for invalid inputs", {
  cohortDefinitionSet <- data.frame(
    cohortId = 1:2,
    cohortName = c("Cohort A", "Cohort B"),
    json = c("{}", "{}"),
    sql = c("SELECT * FROM cdm.cohort WHERE cohort_id = 1", "SELECT * FROM cdm.cohort WHERE cohort_id = 2")
  )

  expect_error(
    createCohortDiagnosticsSettings(
      cohortDefinitionSet = cohortDefinitionSet,
      exportFolder = NULL, # Invalid input
      databaseId = "test_db",
      cohortDatabaseSchema = "cdm",
      cdmDatabaseSchema = "cdm"
    )
  )

  expect_error(
    createCohortDiagnosticsSettings(
      cohortDefinitionSet = NULL, # Invalid input
      exportFolder = "output_folder",
      databaseId = "test_db",
      cohortDatabaseSchema = "cdm",
      cdmDatabaseSchema = "cdm"
    )
  )

  expect_error(
    createCohortDiagnosticsSettings(
      cohortDefinitionSet = cohortDefinitionSet,
      exportFolder = "output_folder",
      databaseId = NULL, # Invalid input
      cohortDatabaseSchema = "cdm",
      cdmDatabaseSchema = "cdm"
    ),
    "databaseId"
  )
})