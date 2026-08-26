library(testthat)
library(dplyr)

test_that(".findOrphanConcepts works as expected", {
    mockConn <- mockDatabaseConnection()
    mockResult <- dplyr::tibble(conceptId = 1, conceptName = "Orphan")

    with_mocked_bindings(
        {
            with_mocked_bindings(
                {
                    result <- CohortDiagnostics:::.findOrphanConcepts(
                        connection = mockConn,
                        cdmDatabaseSchema = "cdm"
                    )
                },
                executeSql = function(...) NULL,
                renderTranslateQuerySql = function(...) mockResult,
                .package = "DatabaseConnector"
            )
        },
        loadRenderTranslateSql = function(...) "SELECT 1;",
        .package = "SqlRender"
    )

    expect_equal(nrow(result), 1)
    expect_equal(result$conceptName, "Orphan")
})

test_that("createConceptCountsTable works as expected", {
    mockConn <- mockDatabaseConnection()

    with_mocked_bindings(
        {
            with_mocked_bindings(
                {
                    expect_no_error(
                        CohortDiagnostics:::createConceptCountsTable(
                            connection = mockConn,
                            cdmDatabaseSchema = "cdm"
                        )
                    )
                },
                executeSql = function(...) NULL,
                .package = "DatabaseConnector"
            )
        },
        loadRenderTranslateSql = function(...) "SELECT 1;",
        .package = "SqlRender"
    )
})
