library(testthat)
library(dplyr)

test_that("swapColumnContents works as expected", {
    df <- dplyr::tibble(targetId = 1, comparatorId = 2)
    swapped <- CohortDiagnostics:::swapColumnContents(df)
    expect_equal(swapped$targetId, 2)
    expect_equal(swapped$comparatorId, 1)
})

test_that("enforceMinCellValue works as expected", {
    df <- dplyr::tibble(count = c(1, 10, 3, 0))
    censored <- CohortDiagnostics:::enforceMinCellValue(df, "count", 5)
    expect_equal(censored$count, c(-5, 10, -5, 0))
})

test_that("naToZero works as expected", {
    x <- c(1, NA, 3)
    expect_equal(CohortDiagnostics:::naToZero(x), c(1, 0, 3))
})

test_that("nullToEmpty works as expected", {
    expect_equal(CohortDiagnostics:::nullToEmpty(NULL), "")
    expect_equal(CohortDiagnostics:::nullToEmpty("test"), "test")
})

test_that("makeDataExportable works as expected", {
    df <- dplyr::tibble(cohortDefinitionId = 1, someValue = 100, cohortEntries = 50)

    with_mocked_bindings(
        {
            with_mocked_bindings(
                {
                    exportable <- CohortDiagnostics:::makeDataExportable(
                        x = df,
                        tableName = "cohort_count",
                        databaseId = "test_db",
                        minCellCount = 5
                    )
                },
                getResultsDataModelSpecifications = function() {
                    dplyr::tibble(
                        tableName = "cohort_count",
                        columnName = c("cohort_id", "database_id", "cohort_entries"),
                        isRequired = "Yes",
                        primaryKey = "No",
                        minCellCount = "No"
                    )
                },
                .package = "CohortDiagnostics"
            )
        },
        snakeCaseToCamelCase = function(x) {
            mapping <- c(
                "cohort_id" = "cohortId",
                "database_id" = "databaseId",
                "cohort_entries" = "cohortEntries"
            )
            res <- x
            mask <- x %in% names(mapping)
            res[mask] <- mapping[x[mask]]
            return(res)
        },
        .package = "SqlRender"
    )

    expect_true("cohortId" %in% colnames(exportable))
    expect_equal(exportable$databaseId, "test_db")
})

test_that("timeExecution works as expected", {
    exportFolder <- tempfile()
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    result <- CohortDiagnostics:::timeExecution(
        exportFolder = exportFolder,
        taskName = "testTask",
        expr = {
            Sys.sleep(0.01)
        }
    )

    expect_true(file.exists(file.path(exportFolder, "executionTimes.csv")))
    expect_equal(result$task, "testTask")
})
