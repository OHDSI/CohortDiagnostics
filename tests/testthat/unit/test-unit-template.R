# Unit Test Template
# This file demonstrates the pattern for writing isolated unit tests

# Source fixtures and mocks
source(testthat::test_path("..", "fixtures", "mock_data.R"))
source(testthat::test_path("..", "fixtures", "test_cohorts.R"))

# Example 1: Testing a pure function with no dependencies
test_that("computeChecksum returns consistent hash for same input", {
  # Arrange
  sql <- "SELECT * FROM cohort WHERE cohort_id = 1;"
  
  # Act
  checksum1 <- CohortDiagnostics:::computeChecksum(sql)
  checksum2 <- CohortDiagnostics:::computeChecksum(sql)
  
  # Assert
  expect_equal(checksum1, checksum2)
  expect_type(checksum1, "character")
  expect_true(nchar(checksum1) > 0)
})

# Example 2: Testing with different inputs
test_that("computeChecksum returns different hash for different input", {
  # Arrange
  sql1 <- "SELECT * FROM cohort WHERE cohort_id = 1;"
  sql2 <- "SELECT * FROM cohort WHERE cohort_id = 2;"
  
  # Act
  checksum1 <- CohortDiagnostics:::computeChecksum(sql1)
  checksum2 <- CohortDiagnostics:::computeChecksum(sql2)
  
  # Assert
  expect_false(checksum1 == checksum2)
})

# Example 3: Testing with mock data
test_that("function processes mock cohort definitions correctly", {
  # Arrange
  cohortDefs <- createMockCohortDefinitionSet(numCohorts = 3)
  
  # Act
  # Replace with actual function call
  result <- cohortDefs
  
  # Assert
  expect_equal(nrow(result), 3)
  expect_true(all(c("cohortId", "cohortName", "sql", "json") %in% names(result)))
})

# Example 4: Testing with temporary files
test_that("function writes to file correctly", {
  # Arrange
  tmpFile <- tempfile(fileext = ".csv")
  data <- dplyr::tibble(
    cohortId = c(1, 2, 3),
    count = c(100, 200, 300)
  )
  
  # Ensure cleanup
  withr::defer(unlink(tmpFile))
  
  # Act
  readr::write_csv(data, tmpFile)
  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  
  # Assert
  expect_equal(nrow(result), 3)
  expect_equal(result$cohortId, c(1, 2, 3))
})

# Example 5: Testing empty/null inputs
test_that("computeChecksum handles NULL input gracefully", {
  # Arrange
  invalidInput <- NULL
  
  # Act
  result <- CohortDiagnostics:::computeChecksum(invalidInput)
  
  # Assert
  expect_equal(length(result), 0)
})

# Example 6: Testing with edge cases
test_that("function handles empty input", {
  # Arrange
  emptyData <- dplyr::tibble()
  
  # Act
  # Replace with actual function call
  result <- emptyData
  
  # Assert
  expect_equal(nrow(result), 0)
})

# Example 7: Testing incremental behavior
test_that("incremental save updates existing data", {
  # Arrange
  tmpFile <- tempfile(fileext = ".csv")
  withr::defer(unlink(tmpFile))
  
  initialData <- dplyr::tibble(
    cohortId = c(1, 2),
    count = c(100, 200)
  )
  
  newData <- dplyr::tibble(
    cohortId = c(1, 3),
    count = c(150, 300)
  )
  
  # Act
  CohortDiagnostics:::saveIncremental(initialData, tmpFile, cohortId = c(1, 2))
  CohortDiagnostics:::saveIncremental(newData, tmpFile, cohortId = c(1, 3))
  
  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  
  # Assert
  expect_equal(nrow(result), 3)
  expect_true(1 %in% result$cohortId)
  expect_true(2 %in% result$cohortId)
  expect_true(3 %in% result$cohortId)
  
  # Check that cohort 1 was updated
  cohort1Count <- result %>% 
    dplyr::filter(cohortId == 1) %>% 
    dplyr::pull(count)
  expect_equal(cohort1Count, 150)
})

# Example 8: Testing with fixtures
test_that("function works with test cohort definitions", {
  # Arrange
  cohortDef <- getSingleCohortDefinition(cohortId = 1)
  
  # Act
  # Replace with actual function call
  result <- cohortDef
  
  # Assert
  expect_equal(result$cohortId, 1)
  expect_true(nchar(result$sql) > 0)
  expect_true(nchar(result$json) > 0)
})

# Example 9: Testing data transformations
test_that("function transforms data correctly", {
  # Arrange
  inputData <- dplyr::tibble(
    id = 1:5,
    value = c(10, 20, 30, 40, 50)
  )
  
  # Act
  result <- inputData %>%
    dplyr::mutate(doubledValue = value * 2)
  
  # Assert
  expect_equal(result$doubledValue, c(20, 40, 60, 80, 100))
})

# Example 10: Testing with multiple scenarios
test_that("function handles various input sizes", {
  # Test with different sizes
  for (n in c(0, 1, 10, 100)) {
    # Arrange
    data <- createMockCohortDefinitionSet(numCohorts = n)
    
    # Act
    result <- data
    
    # Assert
    expect_equal(nrow(result), n)
  }
})
