library(testthat)
library(dplyr)

test_that("getCdmDataSourceInformation works as expected", {
    mockConn <- mockDatabaseConnection()
    mockResult <- dplyr::tibble(
        cdmSourceName = "Test DB",
        cdmVersion = "5.4",
        vocabularyVersion = "v1"
    )

    with_mocked_bindings(
        {
            with_mocked_bindings(
                {
                    info <- CohortDiagnostics::getCdmDataSourceInformation(
                        connection = mockConn,
                        cdmDatabaseSchema = "cdm"
                    )
                },
                existsTable = function(...) TRUE,
                .package = "DatabaseConnector"
            )
        },
        renderTranslateQuerySql = function(...) mockResult,
        .package = "CohortDiagnostics"
    )

    expect_equal(info$cdmSourceName, "Test DB")
    expect_equal(info$cdmVersion, "5.4")
})

test_that("getCdmDataSourceInformation handles missing table", {
    mockConn <- mockDatabaseConnection()

    with_mocked_bindings(
        {
            expect_warning(
                info <- CohortDiagnostics::getCdmDataSourceInformation(
                    connection = mockConn,
                    cdmDatabaseSchema = "cdm"
                ),
                "CDM Source table not found"
            )
        },
        existsTable = function(...) FALSE,
        .package = "DatabaseConnector"
    )

    expect_null(info)
})
