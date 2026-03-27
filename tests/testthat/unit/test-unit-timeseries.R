library(testthat)
library(CohortDiagnostics)
library(dplyr)
library(Andromeda)

test_that("runCohortTimeSeriesDiagnostics works as expected with mocks", {
  # Mock database connection
  connection <- mockDatabaseConnection()
  
  # Mock result from the first query to avoid exiting
  # TimeSeries.R expects cohort_definition_id and count (or cohortDefinitionId and count after camelCase)
  testCohortCount <- data.frame(cohortDefinitionId = 1, count = 10)
  
  # Mock calendar periods
  calendarPeriods <- data.frame(
    timeId = 1,
    periodBegin = as.Date("2020-01-01"),
    periodEnd = as.Date("2020-03-31"),
    calendarInterval = "q"
  )
  
  local_mocked_bindings(
    connect = function(...) connection,
    disconnect = function(...) NULL,
    insertTable = function(...) NULL,
    renderTranslateExecuteSql = function(...) NULL,
    querySqlToAndromeda = function(connection, sql, andromeda, andromedaTableName, ...) {
      if (andromedaTableName == "allData") {
        andromeda[[andromedaTableName]] <- data.frame(
          cohortId = 1,
          timeId = 1,
          recordsCount = 5,
          personDays = 100,
          personYears = 100/365.25,
          ageGroup = NA_integer_,
          gender = NA_character_
        )
      } else if (andromedaTableName == "gender") {
        andromeda[[andromedaTableName]] <- data.frame(
          cohortId = 1,
          timeId = 1,
          gender = "MALE",
          recordsCount = 3,
          personDays = 60,
          personYears = 60/365.25,
          ageGroup = NA_integer_
        )
      } else if (andromedaTableName == "ageGroup") {
        andromeda[[andromedaTableName]] <- data.frame(
          cohortId = 1,
          timeId = 1,
          ageGroup = 3,
          recordsCount = 2,
          personDays = 40,
          personYears = 40/365.25,
          gender = NA_character_
        )
      } else if (andromedaTableName == "ageGroupGender") {
        andromeda[[andromedaTableName]] <- data.frame(
          cohortId = 1,
          timeId = 1,
          gender = "MALE",
          ageGroup = 3,
          recordsCount = 1,
          personDays = 20,
          personYears = 20/365.25
        )
      }
      return(NULL)
    },
    renderTranslateQuerySql = function(...) testCohortCount,
    .package = "DatabaseConnector"
  )
  
  local_mocked_bindings(
    renderTranslateQuerySql = function(...) testCohortCount,
    .package = "CohortDiagnostics"
  )
  
  local_mocked_bindings(
    loadRenderTranslateSql = function(...) "SELECT 1;",
    render = function(sql, ...) sql,
    .package = "SqlRender"
  )
  
  # Silent ParallelLogger
  local_mocked_bindings(
    logInfo = function(...) NULL,
    logTrace = function(...) NULL,
    .package = "ParallelLogger"
  )

  # Prepare calendarPeriods
  # In TimeSeries.R, calendarPeriods is a tibble created internally and assigned to resultsInAndromeda$calendarPeriods
  # We can't easily mock that part without mocking clock functions, but let's see.

  result <- runCohortTimeSeriesDiagnostics(
    connection = connection,
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "results",
    cohortTable = "cohort",
    runCohortTimeSeries = TRUE,
    runDataSourceTimeSeries = FALSE,
    stratifyByGender = TRUE,
    stratifyByAgeGroup = TRUE,
    cohortIds = 1
  )
  
  expect_true(is.data.frame(result))
  expect_true(nrow(result) > 0)
  expect_true("ageGroup" %in% colnames(result))
  expect_true("gender" %in% colnames(result))
})

test_that("executeTimeSeriesDiagnostics works with mocks", {
  connection <- mockDatabaseConnection()
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))
  recordKeepingFile <- file.path(exportFolder, "record.csv")
  
  cohortDefinitionSet <- dplyr::tibble(
    cohortId = 1,
    cohortName = "Test",
    json = "{}",
    sql = "SELECT 1;",
    checksum = "123"
  )
  
  observationPeriodDateRange <- list(
    observationPeriodMinDate = as.Date("2020-01-01"),
    observationPeriodMaxDate = as.Date("2020-12-31")
  )
  
  local_mocked_bindings(
    runCohortTimeSeriesDiagnostics = function(...) {
      data.frame(cohortId = 1, date = as.Date("2020-01-01"), count = 10, personDays = 100)
    },
    makeDataExportable = function(x, ...) x,
    writeToCsv = function(...) NULL,
    recordTasksDone = function(...) NULL,
    timeExecution = function(folder, taskName, ...) {
      args <- list(...)
      eval(args$expr)
    },
    .package = "CohortDiagnostics"
  )
  
  executeTimeSeriesDiagnostics(
    connection = connection,
    tempEmulationSchema = "temp",
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "results",
    cohortTable = "cohort",
    cohortDefinitionSet = cohortDefinitionSet,
    runCohortTimeSeries = TRUE,
    runDataSourceTimeSeries = FALSE,
    databaseId = "test",
    exportFolder = exportFolder,
    minCellCount = 5,
    instantiatedCohorts = 1,
    incremental = FALSE,
    recordKeepingFile = recordKeepingFile,
    observationPeriodDateRange = observationPeriodDateRange
  )
  
  expect_true(TRUE)
})

test_that("aggregateTimeSeriesData works", {
  data <- data.frame(
    date = as.Date(c("2020-01-01", "2020-01-15", "2020-02-01")),
    count = c(10, 20, 30),
    cohortId = 1,
    personDays = c(100, 200, 300)
  )
  
  # Monthly
  agg <- CohortDiagnostics:::aggregateTimeSeriesData(
    data = data,
    calendarInterval = "month",
    startDate = as.Date("2020-01-01"),
    endDate = as.Date("2020-03-01")
  )
  
  expect_equal(nrow(agg), 3) # Jan, Feb, Mar (padded)
  expect_equal(agg$recordsCount[1], 30) # Jan (10+20)
  expect_equal(agg$recordsCount[2], 30) # Feb (30)
  expect_equal(agg$recordsCount[3], 0)  # Mar (padded)
})

test_that("runCohortTimeSeriesDiagnostics exits when no cohorts provide rows", {
  connection <- mockDatabaseConnection()
  emptyCohortCount <- data.frame(cohortDefinitionId = numeric(), count = numeric())
  
  local_mocked_bindings(
    renderTranslateQuerySql = function(...) emptyCohortCount,
    .package = "DatabaseConnector"
  )
  
  local_mocked_bindings(
    renderTranslateQuerySql = function(...) emptyCohortCount,
    .package = "CohortDiagnostics"
  )
  
  # The warning actually comes from line 102 of TimeSeries.R
  expect_warning(
    runCohortTimeSeriesDiagnostics(
      connection = connection,
      cdmDatabaseSchema = "cdm",
      runCohortTimeSeries = TRUE,
      cohortIds = 1
    )
  )
})

test_that("runCohortTimeSeriesDiagnostics returns NULL if both run flags are FALSE", {
  expect_warning(
    runCohortTimeSeriesDiagnostics(
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      cdmDatabaseSchema = "cdm"
    ),
    "Exiting time series diagnostics"
  )
})
