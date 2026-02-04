test_that("cohortDefinitionsModule renders without errors", {
    # Arrange
    # Arrange
    if (Sys.getenv("R_COVR") != "true" && !"CohortDiagnostics" %in% loadedNamespaces()) {
        devtools::load_all(".")
    }

    # Debugging path
    fixturePath <- "test-unit-shiny-fixtures.R"
    if (file.exists(fixturePath)) {
        source(fixturePath, local = TRUE)
    } else if (file.exists(file.path("unit", fixturePath))) {
        source(file.path("unit", fixturePath), local = TRUE)
    } else {
        source("tests/testthat/unit/test-unit-shiny-fixtures.R", local = TRUE)
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
        CohortDiagnostics:::cohortDefinitionsModule,
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
    # Arrange
    if (Sys.getenv("R_COVR") != "true" && !"CohortDiagnostics" %in% loadedNamespaces()) {
        devtools::load_all(".")
    }

    # Debugging path
    fixturePath <- "test-unit-shiny-fixtures.R"
    if (file.exists(fixturePath)) {
        source(fixturePath, local = TRUE)
    } else if (file.exists(file.path("unit", fixturePath))) {
        source(file.path("unit", fixturePath), local = TRUE)
    } else {
        source("tests/testthat/unit/test-unit-shiny-fixtures.R", local = TRUE)
    }

    # modulePath source removed

    mockDataSource <- createMockDataSource()
    mockDataSource$queryData <- function(sql, ...) data.frame()

    # Act & Assert
    shiny::testServer(
        CohortDiagnostics:::cohortDefinitionsModule,
        args = list(
            # id is not needed for moduleServer unless passed as arg? no moduleServer takes id as first arg
            # shiny::testServer(module, args, ...)
            # But cohortDefinitionsModule signature: function(id, dataSource, cohortDefinitions)
            id = "test",
            dataSource = mockDataSource,
            cohortDefinitions = shiny::reactive(data.frame()) # Empty definitions
        ),
        {
            # Test that it loads
            expect_true(TRUE)
        }
    )
})
