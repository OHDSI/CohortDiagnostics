library(testthat)
library(dplyr)

test_that("getCohortCounts works as expected", {
    mockConn <- mockDatabaseConnection()
    mockResult <- dplyr::tibble(cohortId = 1, cohortEntries = 10, cohortSubjects = 8)

    with_mocked_bindings(
        {
            with_mocked_bindings(
                {
                    counts <- CohortDiagnostics:::getCohortCounts(
                        connection = mockConn,
                        cohortDatabaseSchema = "cohort",
                        cohortIds = c(1)
                    )
                },
                querySql = function(...) mockResult,
                .package = "DatabaseConnector"
            )
        },
        loadRenderTranslateSql = function(...) "SELECT 1;",
        .package = "SqlRender"
    )

    expect_equal(nrow(counts), 1)
    expect_equal(counts$cohortEntries, 10)
})

test_that("checkIfCohortInstantiated works as expected", {
    mockConn <- mockDatabaseConnection()
    mockResult <- dplyr::tibble(COUNT = 1)

    with_mocked_bindings(
        {
            result <- CohortDiagnostics:::checkIfCohortInstantiated(
                connection = mockConn,
                cohortDatabaseSchema = "cohort",
                cohortTable = "cohort",
                cohortId = 1
            )
        },
        renderTranslateQuerySql = function(...) mockResult,
        .package = "DatabaseConnector"
    )

    expect_true(result)
})

test_that("computeCohortCounts works as expected", {
    exportFolder <- tempfile()
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    mockConn <- mockDatabaseConnection()
    cohorts <- dplyr::tibble(cohortId = 1)

    with_mocked_bindings(
        {
            CohortDiagnostics:::computeCohortCounts(
                connection = mockConn,
                cohortDatabaseSchema = "cohort",
                cohortTable = "cohort",
                cohorts = cohorts,
                exportFolder = exportFolder,
                minCellCount = 5,
                databaseId = "test"
            )
        },
        getCohortCounts = function(...) dplyr::tibble(cohortId = 1, cohortEntries = 10, cohortSubjects = 8),
        makeDataExportable = function(x, ...) x,
        writeToCsv = function(...) NULL,
        .package = "CohortDiagnostics"
    )

    expect_true(TRUE)
})
