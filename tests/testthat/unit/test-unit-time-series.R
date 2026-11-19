# Unit tests for Time Series module
# These tests are isolated and require no database connections

library(CohortDiagnostics)

# Helper for creating sample data
create_sample_data <- function() {
  dplyr::tibble(
    cohortId = 1,
    date = as.Date(c("2020-01-15", "2020-01-20", "2020-02-10", "2020-03-05")),
    count = c(10, 15, 20, 25),
    personDays = c(100, 150, 200, 250),
    ageGroup = "20-29",
    gender = "Female"
  )
}

# --- Calendar Intervals ---

test_that("aggregateTimeSeriesData bins by day correctly", {
  data <- create_sample_data()
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "day")
  expect_equal(nrow(result), 4)
  expect_true(all(result$recordsCount %in% c(10, 15, 20, 25)))
})

test_that("aggregateTimeSeriesData bins by week correctly", {
  # 2020-01-15 (Wed) and 2020-01-20 (Mon)
  data <- create_sample_data()
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "week")
  expect_equal(nrow(result), 4)
  # 2020-01-15 is a Wednesday. Monday of that week is 2020-01-13.
  expect_equal(sort(result$periodBegin)[1], as.Date("2020-01-13"))
})

test_that("aggregateTimeSeriesData bins by month correctly", {
  data <- create_sample_data()
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month")
  expect_equal(nrow(result), 3) # Jan, Feb, Mar
  expect_equal(result$recordsCount, c(25, 20, 25))
})

test_that("aggregateTimeSeriesData bins by quarter correctly", {
  data <- create_sample_data()
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "quarter")
  expect_equal(nrow(result), 1)
  expect_equal(result$periodBegin[1], as.Date("2020-01-01"))
  expect_equal(result$recordsCount[1], 70)
})

test_that("aggregateTimeSeriesData bins by year correctly", {
  data <- create_sample_data()
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "year")
  expect_equal(nrow(result), 1)
  expect_equal(result$periodBegin[1], as.Date("2020-01-01"))
})

test_that("aggregateTimeSeriesData handles dates spanning multiple years", {
  data <- dplyr::tibble(
    date = as.Date(c("2020-12-31", "2021-01-01")),
    count = c(1, 1)
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "year")
  expect_equal(nrow(result), 2)
  expect_equal(result$periodBegin, as.Date(c("2020-01-01", "2021-01-01")))
})

test_that("aggregateTimeSeriesData handles incomplete periods correctly", {
  data <- dplyr::tibble(
    date = as.Date(c("2020-01-15")),
    count = 10
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month")
  expect_equal(result$periodBegin[1], as.Date("2020-01-01"))
  expect_equal(result$recordsCount[1], 10)
})

# --- Date Binning ---

test_that("lubridate::floor_date bins to start of day", {
  date <- as.Date("2020-01-15")
  expect_equal(lubridate::floor_date(date, "day"), as.Date("2020-01-15"))
})

test_that("lubridate::floor_date bins to start of week", {
  date <- as.Date("2020-01-15") # Wed
  # Default Sunday start
  expect_equal(lubridate::floor_date(date, "week"), as.Date("2020-01-12"))
})

test_that("lubridate::floor_date bins to start of month", {
  date <- as.Date("2020-01-15")
  expect_equal(lubridate::floor_date(date, "month"), as.Date("2020-01-01"))
})

test_that("lubridate::floor_date bins to start of quarter", {
  date <- as.Date("2020-05-15")
  expect_equal(lubridate::floor_date(date, "quarter"), as.Date("2020-04-01"))
})

test_that("lubridate::floor_date bins to start of year", {
  date <- as.Date("2020-05-15")
  expect_equal(lubridate::floor_date(date, "year"), as.Date("2020-01-01"))
})

# --- Stratification ---

test_that("aggregateTimeSeriesData stratifies by age group", {
  data <- dplyr::tibble(
    date = as.Date(c("2020-01-15", "2020-01-15")),
    count = c(10, 20),
    ageGroup = c("20-29", "30-39")
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month", stratifyByAgeGroup = TRUE)
  expect_equal(nrow(result), 2)
  expect_true(all(c("20-29", "30-39") %in% result$ageGroup))
})

test_that("aggregateTimeSeriesData stratifies by gender", {
  data <- dplyr::tibble(
    date = as.Date(c("2020-01-15", "2020-01-15")),
    count = c(10, 20),
    gender = c("Male", "Female")
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month", stratifyByGender = TRUE)
  expect_equal(nrow(result), 2)
  expect_true(all(c("Male", "Female") %in% result$gender))
})

test_that("aggregateTimeSeriesData stratifies by age and gender", {
  data <- dplyr::tibble(
    date = as.Date(c("2020-01-15", "2020-01-15")),
    count = c(10, 20),
    ageGroup = c("20-29", "20-29"),
    gender = c("Male", "Female")
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month", stratifyByAgeGroup = TRUE, stratifyByGender = TRUE)
  expect_equal(nrow(result), 2)
})

test_that("aggregateTimeSeriesData handles multiple cohorts correctly", {
  data <- dplyr::tibble(
    cohortId = c(1, 2),
    date = as.Date(c("2020-01-15", "2020-01-15")),
    count = c(10, 20)
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month")
  expect_equal(nrow(result), 2)
  expect_equal(sort(unique(result$cohortId)), c(1, 2))
})

# --- Aggregation Logic ---

test_that("aggregateTimeSeriesData sums record counts correctly", {
  data <- dplyr::tibble(
    date = as.Date(c("2020-01-15", "2020-01-20")),
    count = c(10, 15)
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month")
  expect_equal(result$recordsCount, 25)
})

test_that("aggregateTimeSeriesData sums person-time correctly", {
  data <- dplyr::tibble(
    date = as.Date(c("2020-01-15", "2020-01-20")),
    count = c(1, 1),
    personDays = c(30, 30)
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month")
  expect_equal(result$personDays, 60)
})

test_that("aggregateTimeSeriesData calculates incidence rates correctly", {
  data <- dplyr::tibble(
    date = as.Date("2020-01-15"),
    count = 1,
    personDays = 365.25
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month")
  # recordsCount = 1, personYears = 1, rate = 1000 * 1 / 1 = 1000
  expect_equal(result$incidenceRate, 1000)
})

test_that("aggregateTimeSeriesData handles missing periods by padding with zeros", {
  data <- dplyr::tibble(
    date = as.Date("2020-01-15"),
    count = 10
  )
  # Jan to March
  result <- CohortDiagnostics:::aggregateTimeSeriesData(
    data,
    "month",
    startDate = as.Date("2020-01-01"),
    endDate = as.Date("2020-03-01")
  )
  expect_equal(nrow(result), 3)
  expect_equal(result$recordsCount, c(10, 0, 0))
})

# --- Edge Cases ---

test_that("aggregateTimeSeriesData handles empty data", {
  data <- dplyr::tibble(date = as.Date(character()), count = numeric())
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month")
  expect_equal(nrow(result), 0)
})

test_that("aggregateTimeSeriesData handles single date", {
  data <- dplyr::tibble(date = as.Date("2020-01-15"), count = 10)
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month")
  expect_equal(nrow(result), 1)
})

test_that("aggregateTimeSeriesData handles leap year correctly", {
  data <- dplyr::tibble(
    date = as.Date(c("2020-02-29", "2021-02-28")),
    count = c(10, 10)
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "year")
  expect_equal(nrow(result), 2)
  expect_equal(result$periodBegin, as.Date(c("2020-01-01", "2021-01-01")))
})

test_that("aggregateTimeSeriesData enforces min cell count", {
  data <- dplyr::tibble(
    date = as.Date("2020-01-15"),
    count = 3 # Below 5
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month", minCellCount = 5)
  expect_equal(result$recordsCount, -5)
})

test_that("aggregateTimeSeriesData handles NA dates", {
  data <- dplyr::tibble(
    date = as.Date(c("2020-01-15", NA)),
    count = c(10, 20)
  )
  result <- CohortDiagnostics:::aggregateTimeSeriesData(data, "month")
  expect_true(any(is.na(result$periodBegin)))
})

# test_that("getTimeSeriesData returns expected structure (mocked)", {
#   mockConn <- mockDatabaseConnection()
#   mockData <- dplyr::tibble(cohortId = 1, date = as.Date("2020-01-15"), count = 10)
#
#   testthat::with_mocked_bindings(
#     {
#       result <- CohortDiagnostics:::getTimeSeriesData(
#         connection = mockConn,
#         cdmDatabaseSchema = "main",
#         cohortDatabaseSchema = "main",
#         cohortTable = "cohort",
#         cohortIds = 1,
#         timeSeriesMinDate = as.Date("2020-01-01"),
#         timeSeriesMaxDate = as.Date("2020-12-31")
#       )
#     },
#     renderTranslateQuerySql = function(...) mockData,
#     .package = "DatabaseConnector"
#   )
#
#   expect_equal(result, mockData)
# })

test_that("runTimeSeriesDiagnostic orchestrates sub-functions correctly", {
  skip_if_not_installed("testthat", "3.0.0")

  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))

  context <- createDiagnosticsContext(
    connectionDetails = list(dbms = "sqlite"),
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "cohort",
    databaseId = "test",
    exportFolder = exportFolder
  )
  context$isInitialized <- TRUE
  context$incrementalFolder <- file.path(exportFolder, "incremental")
  dir.create(context$incrementalFolder, showWarnings = FALSE)

  cohortDefinitionSet <- createMockCohortDefinitionSet(numCohorts = 1)

  # Track calls
  calls <- list()

  local_mocked_bindings(
    executeTimeSeriesDiagnostics = function(...) {
      calls <<- c(calls, "executeTimeSeriesDiagnostics")
    },
    timeExecution = function(folder, taskName, ...) {
      calls <<- c(calls, taskName)
      args <- list(...)
      eval(args$expr)
    },
    .package = "CohortDiagnostics"
  )

  runTimeSeriesDiagnostic(context, cohortDefinitionSet = cohortDefinitionSet)

  expect_true("executeTimeSeriesDiagnostics" %in% calls)
})
