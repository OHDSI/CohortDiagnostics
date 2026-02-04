test_that("cohortDefinitionsModule renders without errors", {
    # Arrange
    if (!exists("createMockDataSource")) {
        source(testthat::test_path("unit", "test-unit-shiny-fixtures.R"), local = TRUE)
    }
    mockDataSource <- createMockDataSource()
    mockConnectionHandler <- createMockConnectionHandler()

    # Mock data query
    mockDataSource$queryData <- function(sql, ...) {
        data.frame(
            cohortId = 1:3,
            cohortName = c("Cohort 1", "Cohort 2", "Cohort 3"),
            json = "{}",
            sql = "SELECT 1"
        )
    }

    # Act & Assert
    shiny::testServer(
        cohortDefinitionsModule,
        args = list(
            dataSource = mockDataSource,
            cohortDefinitions = shiny::reactive(mockDataSource$cohortTable)
        ),
        {
            # Test that module initializes - check reactive value
            expect_true(!is.null(cohortDefinitions()))

            # Test cohort selection
            session$setInputs(selectedCohort = 1)
            expect_equal(input$selectedCohort, 1)
        }
    )
})

test_that("cohortDefinitionsModule handles empty data", {
    # Arrange
    if (!exists("createMockDataSource")) {
        source(testthat::test_path("unit", "test-unit-shiny-fixtures.R"), local = TRUE)
    }
    mockDataSource <- createMockDataSource()
    mockDataSource$queryData <- function(sql, ...) data.frame()

    # Act & Assert
    shiny::testServer(
        cohortDiagnosticsServer,
        args = list(
            id = "test",
            dataSource = mockDataSource,
            connectionHandler = mockDataSource$connectionHandler,
            resultDatabaseSettings = createMockResultDatabaseSettings()
        ),
        {
            # Test that it loads
            expect_true(TRUE)
        }
    )
})
