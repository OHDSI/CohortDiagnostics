library(testthat)
library(dplyr)

test_that("getVisitContext executes R logic with database mocks", {
    mockConn <- mockDatabaseConnection()
    mockResult <- dplyr::tibble(
        cohortId = 1,
        visitConceptId = 9201,
        visitCount = 10
    )

    with_mocked_bindings(
        {
            with_mocked_bindings(
                {
                    result <- CohortDiagnostics:::getVisitContext(
                        connection = mockConn,
                        cdmDatabaseSchema = "cdm",
                        cohortIds = c(1)
                    )
                },
                executeSql = function(...) NULL,
                renderTranslateQuerySql = function(...) mockResult,
                renderTranslateExecuteSql = function(...) NULL,
                .package = "DatabaseConnector"
            )
        },
        loadRenderTranslateSql = function(...) "SELECT 1;",
        .package = "SqlRender"
    )

    expect_gt(nrow(result), 0)
    expect_equal(result$visitConceptId[1], 9201)
})

test_that("executeVisitContextDiagnostics executes R logic with mocks", {
    exportFolder <- tempfile()
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    mockConn <- mockDatabaseConnection()
    cohorts <- dplyr::tibble(cohortId = 1, checksum = "abc")

    with_mocked_bindings(
        {
            CohortDiagnostics:::executeVisitContextDiagnostics(
                connection = mockConn,
                tempEmulationSchema = NULL,
                cdmDatabaseSchema = "cdm",
                cohortDatabaseSchema = "cohort",
                cohortTable = "cohort",
                cdmVersion = 5,
                databaseId = "test",
                exportFolder = exportFolder,
                minCellCount = 5,
                cohorts = cohorts,
                instantiatedCohorts = c(1),
                recordKeepingFile = tempfile(),
                incremental = FALSE
            )
        },
        getVisitContext = function(...) dplyr::tibble(cohortId = 1, visitConceptId = 9201, visitCount = 10),
        makeDataExportable = function(x, ...) x,
        writeToCsv = function(data, fileName, ...) {
            readr::write_csv(data, fileName)
        },
        recordTasksDone = function(...) NULL,
        subsetToRequiredCohorts = function(cohorts, ...) cohorts,
        .package = "CohortDiagnostics"
    )

    expect_true(file.exists(file.path(exportFolder, "visit_context.csv")))
})
