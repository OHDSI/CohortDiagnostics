library(testthat)
library(dplyr)

test_that("getIncidenceRate executes R logic with database mocks", {
    mockConn <- mockDatabaseConnection()
    mockConn@dbms <- "sqlite"

    # Mock database results
    mockYearRange <- dplyr::tibble(startYear = 2010, endYear = 2012)
    mockRatesSummary <- dplyr::tibble(
        ageGroup = 0,
        gender = "MALE",
        cohortCount = 10,
        personYears = 100,
        calendarYear = 2011
    )

    with_mocked_bindings(
        {
            with_mocked_bindings(
                {
                    with_mocked_bindings(
                        {
                            result <- CohortDiagnostics:::getIncidenceRate(
                                connection = mockConn,
                                cohortDatabaseSchema = "cohort",
                                cohortTable = "cohort",
                                cdmDatabaseSchema = "cdm",
                                cohortId = 1
                            )
                        },
                        querySql = function(...) mockYearRange,
                        insertTable = function(...) NULL,
                        executeSql = function(...) NULL,
                        renderTranslateQuerySql = function(...) mockRatesSummary,
                        renderTranslateExecuteSql = function(...) NULL,
                        .package = "DatabaseConnector"
                    )
                },
                loadRenderTranslateSql = function(...) "SELECT 1;",
                .package = "SqlRender"
            )
        },
        checkIfCohortInstantiated = function(...) TRUE,
        .package = "CohortDiagnostics"
    )

    expect_gt(nrow(result), 0)
    expect_true("incidenceRate" %in% names(result))
})

test_that("computeIncidenceRates executes R logic with mocks", {
    exportFolder <- tempfile()
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    mockConn <- mockDatabaseConnection()
    cohorts <- dplyr::tibble(cohortId = 1, cohortName = "Test", checksum = "abc")

    with_mocked_bindings(
        {
            CohortDiagnostics:::computeIncidenceRates(
                connection = mockConn,
                tempEmulationSchema = NULL,
                cdmDatabaseSchema = "cdm",
                cohortDatabaseSchema = "cohort",
                cohortTable = "cohort",
                databaseId = "test",
                exportFolder = exportFolder,
                minCellCount = 5,
                cohorts = cohorts,
                instantiatedCohorts = c(1),
                recordKeepingFile = tempfile(),
                washoutPeriod = 365,
                incremental = FALSE
            )
        },
        getIncidenceRate = function(...) {
            dplyr::tibble(
                ageGroup = "0-9",
                gender = "Male",
                cohortCount = 10,
                personYears = 100,
                calendarYear = 2011,
                incidenceRate = 100
            )
        },
        makeDataExportable = function(x, ...) x,
        enforceMinCellValue = function(x, ...) x,
        writeToCsv = function(data, fileName, ...) {
            readr::write_csv(data, fileName)
        },
        recordTasksDone = function(...) NULL,
        subsetToRequiredCohorts = function(cohorts, ...) cohorts,
        .package = "CohortDiagnostics"
    )

    expect_true(file.exists(file.path(exportFolder, "incidence_rate.csv")))
})
