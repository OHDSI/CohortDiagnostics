library(testthat)
library(dplyr)

test_that("getInclusionStats works as expected", {
    exportFolder <- tempfile()
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    mockConn <- mockDatabaseConnection()
    cohortDefinitionSet <- createMockCohortDefinitionSet(numCohorts = 1)
    cohortTableNames <- list(cohortInclusionTable = "inc_table")

    mockStats <- list(
        cohortInclusionTable = dplyr::tibble(cohortId = 1, ruleName = "Rule 1"),
        cohortInclusionStatsTable = dplyr::tibble(cohortId = 1, personCount = 100),
        cohortInclusionResultTable = dplyr::tibble(cohortId = 1, modeId = 0),
        cohortSummaryStatsTable = dplyr::tibble(cohortId = 1, baseCount = 100)
    )

    with_mocked_bindings(
        {
            with_mocked_bindings(
                {
                    CohortDiagnostics:::getInclusionStats(
                        connection = mockConn,
                        exportFolder = exportFolder,
                        databaseId = "test",
                        cohortDefinitionSet = cohortDefinitionSet,
                        cohortDatabaseSchema = "cohort",
                        cohortTableNames = cohortTableNames,
                        incremental = FALSE,
                        instantiatedCohorts = c(1),
                        minCellCount = 5,
                        recordKeepingFile = tempfile()
                    )
                },
                insertInclusionRuleNames = function(...) NULL,
                getCohortStats = function(...) mockStats,
                .package = "CohortGenerator"
            )
        },
        subsetToRequiredCohorts = function(cohorts, ...) cohorts,
        makeDataExportable = function(x, ...) x,
        writeToCsv = function(data, fileName, ...) {
            readr::write_csv(data, fileName)
        },
        recordTasksDone = function(...) NULL,
        .package = "CohortDiagnostics"
    )

    expect_true(file.exists(file.path(exportFolder, "cohort_inclusion.csv")))
    expect_true(file.exists(file.path(exportFolder, "cohort_inc_stats.csv")))
})
