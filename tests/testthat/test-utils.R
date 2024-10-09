library(testthat)

# check makeDataExportable function
test_that("Check function makeDataExportable", {
  cohortCountTableCorrect <- dplyr::tibble(
    cohortId = 3423,
    cohortEntries = 432432,
    cohortSubjects = 34234,
    databaseId = "ccae"
  )
  cohortCountTableCorrect <-
    CohortDiagnostics:::makeDataExportable(
      x = cohortCountTableCorrect,
      tableName = "cohort_count"
    )
  cohortCountTableCorrectNames <-
    SqlRender::camelCaseToSnakeCase(names(cohortCountTableCorrect)) %>% sort()

  resultsDataModel <- getResultsDataModelSpecifications() %>%
    dplyr::filter(tableName == "cohort_count") %>%
    dplyr::select(columnName) %>%
    dplyr::pull() %>%
    sort()

  expect_true(identical(cohortCountTableCorrectNames, resultsDataModel))

  cohortCountTableInCorrect <- dplyr::tibble(
    cohortIdXXX = 3423,
    cohortEntryXXX = 432432,
    cohortSubjectsXXX = 34234
  )

  expect_error(
    CohortDiagnostics:::makeDataExportable(
      x = cohortCountTableInCorrect,
      tableName = "cohort_count"
    )
  )

  cohortCountTableCorrectDuplicated <-
    dplyr::bind_rows(
      cohortCountTableCorrect,
      cohortCountTableCorrect
    )
  expect_error(
    CohortDiagnostics:::makeDataExportable(
      x = cohortCountTableCorrectDuplicated,
      tableName = "cohort_count"
    )
  )
})

test_that("timeExecutions function", {
  readr::local_edition(1)
  temp <- tempfile()
  on.exit(unlink(temp, force = TRUE, recursive = TRUE))
  dir.create(temp)

  # Basic test
  timeExecution(
    exportFolder = temp,
    taskName = "test_task1",
    cohortIds = c(1, 2, 3, 4),
    expr = {
      Sys.sleep(0.001)
    }
  )
  expectedFilePath <- file.path(temp, "executionTimes.csv")
  checkmate::expect_file_exists(expectedFilePath)
  result <- readr::read_csv(expectedFilePath, col_types = readr::cols())
  checkmate::expect_data_frame(result, nrows = 1, ncols = 5)

  expect_false(all(is.na(result$startTime)))
  expect_false(all(is.na(result$executionTime)))

  # Test append
  timeExecution(
    exportFolder = temp,
    taskName = "test_task2",
    cohortIds = NULL,
    expr = {
      Sys.sleep(0.001)
    }
  )

  result <- readr::read_csv(expectedFilePath, col_types = readr::cols())
  checkmate::expect_data_frame(result, nrows = 2, ncols = 5)

  # Parent string
  timeExecution(
    exportFolder = temp,
    taskName = "test_task3",
    parent = "testthat",
    cohortIds = NULL,
    expr = {
      Sys.sleep(0.001)
    }
  )

  result <- readr::read_csv(expectedFilePath, col_types = readr::cols())
  checkmate::expect_data_frame(result, nrows = 3, ncols = 5)

  # custom start/end times
  timeExecution(
    exportFolder = temp,
    taskName = "test_task4",
    parent = "testthat",
    cohortIds = NULL,
    start = "foo",
    execTime = "Foo"
  )

  result <- readr::read_csv(expectedFilePath, col_types = readr::cols())
  checkmate::expect_data_frame(result, nrows = 4, ncols = 5)

  timeExecution(
    exportFolder = temp,
    taskName = "test_task5",
    parent = "testthat",
    cohortIds = NULL,
    start = Sys.time()
  )

  result <- readr::read_csv(expectedFilePath, col_types = readr::cols())
  checkmate::expect_data_frame(result, nrows = 5, ncols = 5)
  expect_false(all(is.na(result$startTime)))
})

test_that("enforceMinCellValue replaces values below minimum with negative of minimum", {
  data <- data.frame(a = c(1, 2, 3, 4, 5))
  minValues <- 3
  result <- enforceMinCellValue(data, "a", minValues, silent = TRUE)

  expect_equal(result$a, c(-3, -3, 3, 4, 5))
})

test_that("enforceMinCellValue does not replace NA values", {
  data <- data.frame(a = c(1, 2, NA, 4, 5))
  minValues <- 3
  result <- enforceMinCellValue(data, "a", minValues, silent = TRUE)

  expect_equal(result$a, c(-3, -3, NA, 4, 5))
})

test_that("enforceMinCellValue does not replace zero values", {
  data <- data.frame(a = c(0, 2, 3, 4, 5))
  minValues <- 3
  result <- enforceMinCellValue(data, "a", minValues, silent = TRUE)

  expect_equal(result$a, c(0, -3, 3, 4, 5))
})

test_that("enforceMinCellValue works with vector of minimum values", {
  data <- data.frame(a = c(1, 2, 3, 4, 5))
  minValues <- c(1, 2, 3, 4, 5)
  result <- enforceMinCellValue(data, "a", minValues, silent = TRUE)

  expect_equal(result$a, c(1, 2, 3, 4, 5))
})

test_that("timeExecution uses minutes as unit", {
  exportFolder <- tempfile()
  dir.create(exportFolder)
  timeExecution(exportFolder,
                taskName = "test 1 second",
                expr = Sys.sleep(1))
  
  start <- as.POSIXct("2024-10-09 03:37:46") 
  oneMinute <- start - as.POSIXct("2024-10-09 03:36:46")
  timeExecution(exportFolder,
                taskName = "test 1 minute",
                start = start,
                execTime = oneMinute)
  
  start <- as.POSIXct("2024-10-09 03:37:46") 
  oneHour <- start - as.POSIXct("2024-10-09 02:37:46")
  timeExecution(exportFolder,
                taskName = "test 1 hour",
                start = start,
                execTime = oneHour)
  
  list.files(exportFolder)
  df <- readr::read_csv(file.path(exportFolder, "executionTimes.csv"), show_col_types = F)
  
  expect_equal(df$task, c("test 1 second", "test 1 minute", "test 1 hour"))
  expect_equal(df$executionTime, c(round(1/60, 4), 1, 60))
})

for (server in testServers) {
  test_that(paste("tempTableExists works on ", server$connectionDetails$dbms), {
    con <- DatabaseConnector::connect(server$connectionDetails)
    DatabaseConnector::renderTranslateExecuteSql(con, "create table #tmp110010 (a int);", 
                                                 progressBar = F, 
                                                 reportOverallTime = F)
    expect_false(tempTableExists(con, "tmp98765"))
    expect_true(tempTableExists(con, "tmp110010"))
    DatabaseConnector::renderTranslateExecuteSql(con, "drop table #tmp110010;", 
                                                 progressBar = F, 
                                                 reportOverallTime = F)
    DatabaseConnector::disconnect(con)
  })
}

test_that("assertCohortDefinitionSetContainsAllParents works", {
  cohorts <- loadTestCohortDefinitionSet() 
  
  expect_no_error(
    CohortDiagnostics:::assertCohortDefinitionSetContainsAllParents(cohorts)
  )
  
  expect_error(
    CohortDiagnostics:::assertCohortDefinitionSetContainsAllParents(
      dplyr::filter(cohorts, !(.data$cohortId  %in% cohorts$subsetParent))
    )
  )
})

