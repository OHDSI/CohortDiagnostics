# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

library(testthat)
library(dplyr)

# Source fixtures
source(testthat::test_path( "fixtures", "mock_data.R"))
source(testthat::test_path( "fixtures", "test_cohorts.R"))

# --- Min Cell Count Tests ---

test_that("enforceMinCellValue suppresses values below threshold", {
  # Arrange
  data <- dplyr::tibble(
    count = c(3, 10, 25)
  )
  minCellCount <- 5
  
  # Act
  result <- CohortDiagnostics:::enforceMinCellValue(
    data,
    columnName = "count",
    minValues = minCellCount,
    silent = TRUE
  )
  
  # Assert
  expect_equal(result$count[1], -5)
  expect_equal(result$count[2], 10)
  expect_equal(result$count[3], 25)
})

test_that("enforceMinCellValue preserves zero values", {
  # Arrange
  data <- dplyr::tibble(
    count = c(0, 10)
  )
  
  # Act
  result <- CohortDiagnostics:::enforceMinCellValue(
    data,
    columnName = "count",
    minValues = 5,
    silent = TRUE
  )
  
  # Assert
  expect_equal(result$count[1], 0)
})

test_that("enforceMinCellValue preserves NA values", {
  # Arrange
  data <- dplyr::tibble(
    count = c(NA_real_, 10)
  )
  
  # Act
  result <- CohortDiagnostics:::enforceMinCellValue(
    data,
    columnName = "count",
    minValues = 5,
    silent = TRUE
  )
  
  # Assert
  expect_true(is.na(result$count[1]))
})

test_that("enforceMinCellValue handles vector of minValues", {
  # Arrange
  data <- dplyr::tibble(
    count = c(3, 7)
  )
  minCellCounts <- c(5, 10)
  
  # Act
  result <- CohortDiagnostics:::enforceMinCellValue(
    data,
    columnName = "count",
    minValues = minCellCounts,
    silent = TRUE
  )
  
  # Assert
  expect_equal(result$count[1], -5)
  expect_equal(result$count[2], -10)
})

# --- CSV Writing Tests ---

test_that("writeToCsv writes a new file correctly with snake_case columns", {
  # Arrange
  tmpFile <- tempfile(fileext = ".csv")
  withr::defer(unlink(tmpFile))
  data <- dplyr::tibble(
    cohortId = 1:3,
    cohortName = c("A", "B", "C")
  )
  
  # Act - calling default method directly due to S3 dispatch issues in test environment
  CohortDiagnostics:::writeToCsv.default(data, tmpFile)
  
  # Assert
  expect_true(file.exists(tmpFile))
  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  expect_true("cohort_id" %in% names(result))
  expect_true("cohort_name" %in% names(result))
  expect_equal(nrow(result), 3)
})

test_that("writeToCsv appends in incremental mode", {
  # Arrange
  tmpFile <- tempfile(fileext = ".csv")
  withr::defer(unlink(tmpFile))
  
  data1 <- dplyr::tibble(cohortId = 1, count = 100)
  data2 <- dplyr::tibble(cohortId = 2, count = 200)
  
  # Act
  CohortDiagnostics:::writeToCsv.default(data1, tmpFile, incremental = TRUE, cohortId = 1)
  CohortDiagnostics:::writeToCsv.default(data2, tmpFile, incremental = TRUE, cohortId = 2)
  
  # Assert
  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  expect_equal(nrow(result), 2)
  expect_true(all(c(1, 2) %in% result$cohort_id))
})

test_that("writeToCsv overwrites existing file when incremental = FALSE", {
  # Arrange
  tmpFile <- tempfile(fileext = ".csv")
  withr::defer(unlink(tmpFile))
  
  writeLines("header1,header2\nold,data", tmpFile)
  data <- dplyr::tibble(cohortId = 1, count = 100)
  
  # Act
  CohortDiagnostics:::writeToCsv.default(data, tmpFile, incremental = FALSE)
  
  # Assert
  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  expect_equal(nrow(result), 1)
  expect_equal(result$cohort_id[1], 1)
  expect_false("header1" %in% names(result))
})

test_that("writeToCsv handles empty data frame", {
  # Arrange
  tmpFile <- tempfile(fileext = ".csv")
  withr::defer(unlink(tmpFile))
  data <- dplyr::tibble(cohortId = numeric(), count = numeric())
  
  # Act
  CohortDiagnostics:::writeToCsv.default(data, tmpFile)
  
  # Assert
  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  expect_equal(nrow(result), 0)
  expect_true("cohort_id" %in% names(result))
})

# --- Column Name Conversion Tests ---

test_that("camelCaseToSnakeCase converts names correctly", {
  expect_equal(SqlRender::camelCaseToSnakeCase("cohortId"), "cohort_id")
  expect_equal(SqlRender::camelCaseToSnakeCase("subjectCount"), "subject_count")
})

test_that("camelCaseToSnakeCase handles already snake_case names", {
  expect_equal(SqlRender::camelCaseToSnakeCase("cohort_id"), "cohort_id")
})

test_that("camelCaseToSnakeCase handles mixed case names", {
  # Use camelCase instead of PascalCase to avoid leading underscore from SqlRender
  expect_equal(SqlRender::camelCaseToSnakeCase("someCohortId"), "some_cohort_id")
})

# --- Data Validation Tests ---

test_that("Validation detects missing required columns", {
  # Using makeDataExportable as the internal validation mechanism for cohort sets
  invalidData <- dplyr::tibble(
    cohortId = 1,
    cohortName = "Test"
  )
  
  expect_error(
    CohortDiagnostics:::makeDataExportable(invalidData, "cohort"),
    regexp = "Cannot find required field"
  )
})

test_that("Validation detects duplicate cohort IDs", {
  duplicateData <- dplyr::tibble(
    cohortId = c(1, 1),
    cohortName = c("A", "B"),
    sql = c("S1", "S2"),
    json = c("J1", "J2")
  )
  
  expect_error(
    CohortDiagnostics:::makeDataExportable(duplicateData, "cohort"),
    regexp = "duplicates found in primary key"
  )
})

test_that("Validation accepts valid cohort definition set", {
  validData <- createMockCohortDefinitionSet(numCohorts = 3)
  
  # Act
  result <- CohortDiagnostics:::makeDataExportable(validData, "cohort")
  
  # Assert
  expect_equal(nrow(result), 3)
  expect_true(all(c("cohortId", "cohortName", "sql", "json") %in% names(result)))
})

# --- Additional Utility Tests ---

test_that("naToZero correctly transforms NA values", {
  expect_equal(CohortDiagnostics:::naToZero(c(1, NA, 5)), c(1, 0, 5))
  expect_equal(CohortDiagnostics:::naToZero(c(NA, NA)), c(0, 0))
})
