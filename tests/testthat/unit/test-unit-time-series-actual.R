library(testthat)
library(dplyr)

test_that("runCohortTimeSeriesDiagnostics executes R logic with database mocks", {
    mockConn <- mockDatabaseConnection()
    mockConn@dbms <- "sqlite"

    # Mock database results
    mockCohortCount <- dplyr::tibble(cohortDefinitionId = 1, count = 100)
    mockTimeSeriesData <- dplyr::tibble(
        cohortId = 1,
        timeId = 1,
        recordsCount = 10,
        personDays = 1000,
        seriesType = "T1",
        ageGroup = 0,
        gender = "MALE"
    )

    with_mocked_bindings(
        {
            with_mocked_bindings(
                {
                    with_mocked_bindings(
                        {
                            with_mocked_bindings(
                                {
                                    result <- CohortDiagnostics:::runCohortTimeSeriesDiagnostics(
                                        connection = mockConn,
                                        cdmDatabaseSchema = "cdm",
                                        cohortDatabaseSchema = "cohort",
                                        cohortTable = "cohort",
                                        runCohortTimeSeries = TRUE,
                                        runDataSourceTimeSeries = FALSE,
                                        cohortIds = c(1)
                                    )
                                },
                                renderTranslateQuerySql = function(...) mockCohortCount,
                                querySql = function(...) mockCohortCount,
                                querySqlToAndromeda = function(andromeda, andromedaTableName, ...) {
                                    if (andromedaTableName == "calendarPeriods") {
                                        andromeda[[andromedaTableName]] <- dplyr::tibble(timeId = 1, periodBegin = as.Date("2020-01-01"), calendarInterval = "month")
                                    } else {
                                        andromeda[[andromedaTableName]] <- mockTimeSeriesData
                                    }
                                },
                                dbms = function(connection) connection@dbms,
                                insertTable = function(...) NULL,
                                renderTranslateExecuteSql = function(...) NULL,
                                .package = "DatabaseConnector"
                            )
                        },
                        dbIsValid = function(...) TRUE,
                        .package = "DBI"
                    )
                },
                loadRenderTranslateSql = function(...) "SELECT 1;",
                render = function(sql, ...) sql,
                .package = "SqlRender"
            )
        },
        timeExecution = function(..., expr) eval(expr),
        .package = "CohortDiagnostics"
    )

    expect_gt(nrow(result), 0)
    expect_true("seriesType" %in% names(result))
})

test_that("aggregateTimeSeriesData works as expected", {
    data <- dplyr::tibble(
        date = as.Date(c("2020-01-01", "2020-01-15", "2020-02-01")),
        count = c(5, 5, 10),
        personDays = c(100, 100, 200),
        cohortId = 1
    )

    result <- CohortDiagnostics:::aggregateTimeSeriesData(
        data = data,
        calendarInterval = "month",
        startDate = "2020-01-01",
        endDate = "2020-03-01"
    )

    # Grouped by month: Jan and Feb. March is padded.
    expect_equal(nrow(result), 3)
    expect_equal(result$recordsCount[1], 10) # Jan total
    expect_equal(result$recordsCount[2], 10) # Feb total
    expect_equal(result$recordsCount[3], 0) # March padded
})
