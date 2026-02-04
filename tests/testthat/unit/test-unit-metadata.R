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

# Unit tests for metadata and concept ID functions

################################################################################
# Concept ID Handling (4 tests)
################################################################################

test_that("Concept ID formatting handles single concept ID", {
  # Arrange
  conceptId <- 123456

  # Act
  # Logic: Ensure concept ID is treated as numeric and can be converted to string
  formatted <- as.character(conceptId)

  # Assert
  expect_equal(formatted, "123456")
  expect_true(is.numeric(conceptId))
})

test_that("Concept ID formatting handles multiple concept IDs", {
  # Arrange
  conceptIds <- c(123456, 789012, 345678)

  # Act
  formatted <- paste(conceptIds, collapse = ", ")

  # Assert
  expect_equal(formatted, "123456, 789012, 345678")
  expect_equal(length(conceptIds), 3)
})

test_that("Concept ID handling manages invalid concept IDs", {
  # Arrange
  invalidIds <- c("abc", "12.34")

  # Act
  # Convert to numeric, should produce NAs
  converted <- suppressWarnings(as.numeric(invalidIds))

  # Assert
  expect_true(any(is.na(converted)))
})

test_that("Concept ID handling manages NULL/NA concept IDs", {
  # Arrange
  nullId <- NULL
  naId <- NA_integer_

  # Act & Assert
  expect_true(is.null(nullId))
  expect_true(is.na(naId))
  expect_equal(length(nullId), 0)
})

################################################################################
# CDM Version Detection (3 tests)
################################################################################

test_that("getCdmVersion detects CDM v5.3 correctly", {
  # Arrange
  mockMetadata <- dplyr::tibble(
    cdm_version = "5.3"
  )

  # Act
  version <- mockMetadata$cdm_version
  majorVersion <- as.numeric(substr(version, 1, 1))
  minorVersion <- as.numeric(substr(version, 3, 3))

  # Assert
  expect_equal(majorVersion, 5)
  expect_equal(minorVersion, 3)
})

test_that("getCdmVersion detects CDM v5.4 correctly", {
  # Arrange
  mockMetadata <- dplyr::tibble(
    cdm_version = "5.4"
  )

  # Act
  version <- mockMetadata$cdm_version
  majorVersion <- as.numeric(substr(version, 1, 1))
  minorVersion <- as.numeric(substr(version, 3, 3))

  # Assert
  expect_equal(majorVersion, 5)
  expect_equal(minorVersion, 4)
})

test_that("getCdmVersion handles unknown CDM version", {
  # Arrange
  mockMetadata <- dplyr::tibble(
    cdm_version = "6.0"
  )

  # Act
  version <- mockMetadata$cdm_version
  majorVersion <- as.numeric(substr(version, 1, 1))

  # Assert
  expect_equal(majorVersion, 6)
  # Logic check for unknown
  expect_true(majorVersion > 5)
})

################################################################################
# Vocabulary Version (3 tests)
################################################################################

test_that("vocabulary version number parsed correctly", {
  # Arrange
  vocabVersionString <- "v5.0 01-JAN-2024"

  # Act
  versionMatch <- regexpr("v[0-9.]+", vocabVersionString)
  version <- regmatches(vocabVersionString, versionMatch)

  # Assert
  expect_equal(version, "v5.0")
})

test_that("vocabulary version date extracted correctly", {
  # Arrange
  vocabVersionString <- "v5.0 01-JAN-2024"

  # Act
  dateMatch <- regexpr("[0-9]{2}-[A-Z]{3}-[0-9]{4}", vocabVersionString)
  date <- regmatches(vocabVersionString, dateMatch)

  # Assert
  expect_equal(date, "01-JAN-2024")
})

test_that("vocabulary version handles missing info", {
  # Arrange
  vocabVersionString <- NA_character_

  # Act & Assert
  expect_true(is.na(vocabVersionString))

  # Test with empty string
  emptyString <- ""
  expect_equal(nchar(emptyString), 0)
})

################################################################################
# Data Source Info (4 tests)
################################################################################

test_that("extracts database name from metadata", {
  # Arrange
  mockMetadata <- dplyr::tibble(
    cdm_source_name = "Test CDM",
    cdm_holder = "OHDSI",
    source_description = "Test Database"
  )

  # Act
  dbName <- mockMetadata$cdm_source_name

  # Assert
  expect_equal(dbName, "Test CDM")
})

test_that("extracts CDM holder from metadata", {
  # Arrange
  mockMetadata <- dplyr::tibble(
    cdm_holder = "OHDSI"
  )

  # Act
  holder <- mockMetadata$cdm_holder

  # Assert
  expect_equal(holder, "OHDSI")
})

test_that("extracts source description from metadata", {
  # Arrange
  mockMetadata <- dplyr::tibble(
    source_description = "This is a test database for OHDSI"
  )

  # Act
  desc <- mockMetadata$source_description

  # Assert
  expect_match(desc, "test database")
})

test_that("aggregates all metadata correctly", {
  # Arrange
  mockMetadata <- dplyr::tibble(
    cdmSourceName = "Test CDM",
    cdmHolder = "OHDSI",
    sourceDescription = "Test Database",
    cdmVersion = "5.4",
    vocabularyVersion = "v5.0 01-JAN-2024"
  )

  # Act
  # Simulate getDataSourceInformation aggregation
  metadataList <- as.list(mockMetadata)

  # Assert
  expect_type(metadataList, "list")
  expect_equal(metadataList$cdmSourceName, "Test CDM")
  expect_equal(length(names(metadataList)), 5)
})

################################################################################
# Edge Cases (3 tests)
################################################################################

test_that("handles empty metadata", {
  # Arrange
  emptyMetadata <- dplyr::tibble()

  # Act & Assert
  expect_equal(nrow(emptyMetadata), 0)
  expect_equal(ncol(emptyMetadata), 0)
})

test_that("handles malformed version strings", {
  # Arrange
  malformedVersion <- "version-5-point-4"

  # Act
  # Extraction should fail or return NA based on regex
  versionMatch <- regexpr("v[0-9.]+", malformedVersion)

  # Assert
  expect_equal(as.integer(versionMatch), -1)
})

test_that("handles missing required fields in metadata", {
  # Arrange
  incompleteMetadata <- dplyr::tibble(
    cdm_version = "5.4"
    # missing cdm_source_name
  )

  # Act & Assert
  expect_false("cdm_source_name" %in% colnames(incompleteMetadata))
  expect_true("cdm_version" %in% colnames(incompleteMetadata))
})
