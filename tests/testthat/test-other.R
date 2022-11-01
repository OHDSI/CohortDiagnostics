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
    dplyr::filter(.data$tableName == "cohort_count") %>%
    dplyr::select(.data$columnName) %>%
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
  result <- readr::read_csv(expectedFilePath)
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

  result <- readr::read_csv(expectedFilePath)
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

  result <- readr::read_csv(expectedFilePath)
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

  result <- readr::read_csv(expectedFilePath)
  checkmate::expect_data_frame(result, nrows = 4, ncols = 5)

  timeExecution(
    exportFolder = temp,
    taskName = "test_task5",
    parent = "testthat",
    cohortIds = NULL,
    start = Sys.time()
  )

  result <- readr::read_csv(expectedFilePath)
  checkmate::expect_data_frame(result, nrows = 5, ncols = 5)
  expect_false(all(is.na(result$startTime)))
})
