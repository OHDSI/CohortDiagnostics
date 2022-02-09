library(testthat)
test_that("Check if package is installed", {
  expect_true(CohortDiagnostics:::is_installed('dplyr'))
  expect_false(CohortDiagnostics:::is_installed('abcd'))
})


#check makeDataExportable function
test_that("Check function makeDataExportable", {
  cohortCountTableCorrect <- dplyr::tibble(
    cohortId = 3423,
    cohortEntries = 432432,
    cohortSubjects = 34234,
    databaseId = "ccae"
  )
  cohortCountTableCorrect <-
    CohortDiagnostics:::makeDataExportable(x = cohortCountTableCorrect,
                                           tableName = "cohort_count")
  cohortCountTableCorrectNames <-
    SqlRender::camelCaseToSnakeCase(names(cohortCountTableCorrect)) %>% sort()
  
  resultsDataModel <- getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == "cohort_count") %>%
    dplyr::select(.data$fieldName) %>%
    dplyr::pull() %>%
    sort()
  
  expect_true(identical(cohortCountTableCorrectNames, resultsDataModel))
  
  cohortCountTableInCorrect <- dplyr::tibble(
    cohortIdXXX = 3423,
    cohortEntryXXX = 432432,
    cohortSubjectsXXX = 34234
  )
  
  expect_error(
    CohortDiagnostics:::makeDataExportable(x = cohortCountTableInCorrect,
                                           tableName = "cohort_count")
  )
  
  cohortCountTableCorrectDuplicated <-
    dplyr::bind_rows(cohortCountTableCorrect,
                     cohortCountTableCorrect)
  expect_error(
    CohortDiagnostics:::makeDataExportable(x = cohortCountTableCorrectDuplicated,
                                           tableName = "cohort_count")
  )
})
