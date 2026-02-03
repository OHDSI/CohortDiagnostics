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

# Load mock data generators
source(testthat::test_path("..", "fixtures", "mock_data.R"))


# --- Schema Specifications Tests ---

test_that("getResultsDataModelSpecifications returns correct structure", {
  specs <- getResultsDataModelSpecifications()
  expect_s3_class(specs, "tbl_df")
  requiredCols <- c("tableName", "columnName", "dataType", "isRequired", "primaryKey")
  expect_true(all(requiredCols %in% colnames(specs)))
})

test_that("getResultsDataModelSpecifications contains core tables", {
  specs <- getResultsDataModelSpecifications()
  tables <- unique(specs$tableName)
  expect_true("cohort" %in% tables)
  expect_true("cohort_count" %in% tables)
  expect_true("database" %in% tables)
})

test_that("getDefaultVocabularyTableNames returns expected tables", {
  vocabTables <- getDefaultVocabularyTableNames()
  expect_type(vocabTables, "character")
  expect_true("concept" %in% vocabTables)
  expect_true("vocabulary" %in% vocabTables)
  # Verify CamelCase
  expect_false(any(grepl("_", vocabTables)))
})

test_that("Schema specs define primary keys for core tables", {
  specs <- getResultsDataModelSpecifications()
  pkSpecs <- specs %>% dplyr::filter(primaryKey == "Yes")
  expect_true(nrow(pkSpecs) > 0)
  expect_true("cohort" %in% (pkSpecs %>% dplyr::filter(columnName == "cohort_id") %>% pull(tableName)))
})

test_that("All columns in specs have data types", {
  specs <- getResultsDataModelSpecifications()
  expect_false(any(is.na(specs$dataType)))
  expect_type(specs$dataType, "character")
})

# --- Schema Validation Tests (using makeDataExportable) ---

test_that("makeDataExportable detects missing required columns", {
  # Missing cohort_name and sql which are required for 'cohort' table
  invalidData <- dplyr::tibble(
    cohortId = c(1, 2)
  )
  
  expect_error(
    CohortDiagnostics:::makeDataExportable(invalidData, "cohort"),
    regexp = "Cannot find required field"
  )
})

test_that("makeDataExportable detects primary key violations", {
  # duplicate cohortId for the same database (though databaseId is not in PK for cohort table, cohortId is)
  invalidData <- dplyr::tibble(
    cohortId = c(1, 1),
    cohortName = c("Test 1", "Test 1"),
    sql = c("SELECT 1", "SELECT 1")
  )
  
  expect_error(
    CohortDiagnostics:::makeDataExportable(invalidData, "cohort"),
    regexp = "duplicates found in primary key"
  )
})

test_that("makeDataExportable filters out unexpected columns", {
  data <- dplyr::tibble(
    cohortId = 1,
    cohortName = "Test 1",
    sql = "SELECT 1",
    extraColumn = "Ignore me"
  )
  
  exportable <- CohortDiagnostics:::makeDataExportable(data, "cohort")
  expect_false("extraColumn" %in% colnames(exportable))
  expect_true("cohortId" %in% colnames(exportable))
})

test_that("makeDataExportable enforces min cell count", {
  data <- dplyr::tibble(
    cohortId = 1,
    databaseId = "test",
    cohortEntries = 3, # Below default 5
    cohortSubjects = 3
  )
  
  # Mocking loggers to avoid output clutter
  withr::with_options(list(ParallelLogger.suppressMessages = TRUE), {
    exportable <- CohortDiagnostics:::makeDataExportable(data, "cohort_count", minCellCount = 5)
    expect_equal(exportable$cohortEntries, -5)
    expect_equal(exportable$cohortSubjects, -5)
  })
})

# --- Data Upload & Logic Mock Tests ---

test_that("uploadResults calls ResultModelManager with correct arguments", {
  # Mocking RMM::uploadResults and zip::unzip
  testthat::with_mocked_bindings(
    {
      testthat::with_mocked_bindings(
        {
          testthat::with_mocked_bindings(
            {
              testthat::expect_no_error(
                uploadResults(
                  connectionDetails = list(dbms = "sqlite"),
                  schema = "main",
                  zipFileName = "test.zip"
                )
              )
            },
            uploadResults = function(...) {
              args <- list(...)
              expect_equal(args$schema, "main")
              expect_equal(args$databaseIdentifierFile, "database.csv")
            },
            .package = "ResultModelManager"
          )
        },
        unzip = function(...) {
          NULL
        },
        .package = "zip"
      )
    },
    dir.create = function(...) {
      NULL
    },
    unlink = function(...) {
      NULL
    },
    .package = "base"
  )
})

test_that("createResultsDataModel validates sqlite schema", {
  expect_error(
    createResultsDataModel(
      connectionDetails = list(dbms = "sqlite"),
      databaseSchema = "not_main"
    ),
    regexp = "Invalid schema for sqlite"
  )
})

test_that("migrateDataModel instantiates migrator", {
  testthat::with_mocked_bindings(
    {
      testthat::with_mocked_bindings(
        {
          testthat::with_mocked_bindings(
            {
              testthat::with_mocked_bindings(
                {
                  testthat::expect_no_error(
                    migrateDataModel(
                      connectionDetails = list(dbms = "sqlite"),
                      databaseSchema = "main"
                    )
                  )
                },
                getDataMigrator = function(...) {
                  list(executeMigrations = function() {
                    NULL
                  }, finalize = function() {
                    NULL
                  })
                },
                .package = "CohortDiagnostics"
              )
            },
            connect = function(...) {
              "mock_conn"
            },
            disconnect = function(...) {
              NULL
            },
            executeSql = function(...) {
              NULL
            },
            .package = "DatabaseConnector"
          )
        },
        loadRenderTranslateSql = function(...) {
          "UPDATE version"
        },
        .package = "SqlRender"
      )
    },
    logInfo = function(...) {
      NULL
    },
    .package = "ParallelLogger"
  )
})

# --- Results Merging Tests ---

test_that("createMergedResultsFile fails if file exists without overwrite", {
  tempSqlite <- tempfile(fileext = ".sqlite")
  writeLines("", tempSqlite)
  on.exit(unlink(tempSqlite))
  
  expect_error(
    createMergedResultsFile(dataFolder = "test", sqliteDbPath = tempSqlite, overwrite = FALSE),
    regexp = "already exists"
  )
})

test_that("createMergedResultsFile aborts if no zip files found", {
  emptyDir <- tempfile()
  dir.create(emptyDir)
  on.exit(unlink(emptyDir, recursive = TRUE))

  # We need to mock DatabaseConnector and createResultsDataModel
  testthat::with_mocked_bindings(
    {
      testthat::with_mocked_bindings(
        {
          expect_error(
            createMergedResultsFile(dataFolder = emptyDir, sqliteDbPath = "test.sqlite"),
            regexp = "No result files found"
          )
        },
        createConnectionDetails = function(...) {
          list(dbms = "sqlite")
        },
        connect = function(...) {
          "conn"
        },
        disconnect = function(...) {
          NULL
        },
        .package = "DatabaseConnector"
      )
    },
    createResultsDataModel = function(...) {
      NULL
    },
    .package = "CohortDiagnostics"
  )
})

# --- Edge Cases ---

test_that("makeDataExportable handles empty data frames", {
  data <- dplyr::tibble(
    cohortId = numeric(),
    cohortName = character(),
    sql = character()
  )
  
  exportable <- CohortDiagnostics:::makeDataExportable(data, "cohort")
  expect_equal(nrow(exportable), 0)
  expect_equal(ncol(exportable), 3)
})

test_that("titleCaseToCamelCase converts correctly", {
  expect_equal(CohortDiagnostics:::titleCaseToCamelCase("My Table Name"), "myTableName")
  expect_equal(CohortDiagnostics:::titleCaseToCamelCase("Test"), "test")
})

test_that("naToZero works as expected", {
  vec <- c(1, NA, 3)
  expect_equal(CohortDiagnostics:::naToZero(vec), c(1, 0, 3))
})
