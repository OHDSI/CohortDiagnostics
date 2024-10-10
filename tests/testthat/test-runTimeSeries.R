# Copyright 2024 Observational Health Data Sciences and Informatics
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

# Test getTimeSeries on all testServers
for (nm in names(testServers)) {

  server <- testServers[[nm]]
  con <- connect(server$connectionDetails)
  exportFolder <- file.path(tempdir(), paste0(nm, "exp"))
  recordKeepingFile <- file.path(exportFolder, "record.csv")

  test_that("Testing time series logic", {
    skip_if(skipCdmTests, "cdm settings not configured")

    # to do - with incremental = FALSE
    with_dbc_connection(con, {
      # manually create cohort table and load to table
      #   Cohort table has a total of four records, with each cohort id having two each
      #   cohort 1 has one subject with two different cohort entries
      #   cohort 2 has two subject with two different cohort entries
      cohort <- dplyr::tibble(
        cohortDefinitionId = c(1, 1, 2, 2),
        subjectId = c(1, 1, 1, 2),
        cohortStartDate = c(as.Date("2005-01-15"), as.Date("2005-07-15"), as.Date("2005-01-15"), as.Date("2005-07-15")),
        cohortEndDate = c(as.Date("2005-05-15"), as.Date("2005-09-15"), as.Date("2005-05-15"), as.Date("2005-09-15"))
      )

      cohortTable <-
        paste0("ct_", Sys.getpid(), format(Sys.time(), "%s"), sample(1:100, 1))

      DatabaseConnector::insertTable(
        connection = con,
        databaseSchema = server$cohortDatabaseSchema,
        tableName = cohortTable,
        data = cohort,
        dropTableIfExists = TRUE,
        createTable = TRUE,
        tempTable = FALSE,
        camelCaseToSnakeCase = TRUE,
        progressBar = FALSE
      )

      timeSeries <- CohortDiagnostics:::getTimeSeries(
        connection = con,
        tempEmulationSchema = server$tempEmulationSchema,
        cdmDatabaseSchema = server$cdmDatabaseSchema,
        cohortDatabaseSchema = server$cohortDatabaseSchema,
        cohortTable = cohortTable,
        runCohortTimeSeries = TRUE,
        runDataSourceTimeSeries = FALSE, # cannot test data source time series because we are using simulated cohort table
        timeSeriesMinDate = as.Date("2004-01-01"),
        timeSeriesMaxDate = as.Date("2006-12-31"),
        cohortIds = c(1, 2),
        stratifyByGender = FALSE, # cannot test stratification because it will require cohort table to be built from cdm
        stratifyByAgeGroup = FALSE # this test is using simulated cohort table
      )

      # testing if values returned for cohort 1 is as expected
      timeSeriesCohort <- timeSeries %>%
        dplyr::filter(.data$cohortId == 1) %>%
        dplyr::filter(.data$seriesType == "T1") %>%
        dplyr::filter(.data$calendarInterval == "m")

      # there should be 8 records in this data frame, representing 8 months for the one subject in the cohort id  = 1
      testthat::expect_equal(
        object = nrow(timeSeriesCohort),
        expected = 8
      )

      # there should be 2 records in this data frame, representing the 2 starts for the one subject in the cohort id  = 1
      testthat::expect_equal(
        object = nrow(timeSeriesCohort %>% dplyr::filter(.data$recordsStart == 1)),
        expected = 2
      )

      # there should be 1 records in this data frame, representing the 1 incident start for the one subject in the cohort id  = 1
      testthat::expect_equal(
        object = nrow(timeSeriesCohort %>% dplyr::filter(.data$subjectsStartIn == 1)),
        expected = 1
      )
    })
  })
}

test_that("Testing cohort time series execution, incremental = FALSE", {
  testServer <- "sqlite"
  skip_if_not(testServer %in% names(testServers))
  server <- testServers[[testServer]]
  con <- DatabaseConnector::connect(server$connectionDetails)
  exportFolder <- file.path(tempdir(), paste0(testServer, "exp"))
  recordKeepingFile <- file.path(exportFolder, "record.csv")
  incremental <- FALSE

  with_dbc_connection(con, {
    cohort <- dplyr::tibble(
      cohortDefinitionId = c(1, 1, 2, 2),
      subjectId = c(1, 1, 1, 2),
      cohortStartDate = c(
        as.Date("2005-01-15"),
        as.Date("2005-07-15"),
        as.Date("2005-01-15"),
        as.Date("2005-07-15")
      ),
      cohortEndDate = c(
        as.Date("2005-05-15"),
        as.Date("2005-09-15"),
        as.Date("2005-05-15"),
        as.Date("2005-09-15")
      )
    )

    cohort <- dplyr::bind_rows(
      cohort,
      cohort %>%
        dplyr::mutate(cohortDefinitionId = cohortDefinitionId * 1000)
    )

    cohortDefinitionSet <-
      cohort %>%
      dplyr::select(cohortDefinitionId) %>%
      dplyr::distinct() %>%
      dplyr::rename("cohortId" = "cohortDefinitionId") %>%
      dplyr::rowwise() %>%
      dplyr::mutate(json = RJSONIO::toJSON(list(
        cohortId = cohortId,
        randomString = c(
          sample(x = LETTERS, 5, replace = TRUE),
          sample(x = LETTERS, 4, replace = TRUE),
          sample(LETTERS, 1, replace = TRUE)
        )
      ))) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(
        sql = json,
        checksum = as.character(CohortDiagnostics:::computeChecksum(json))
      ) %>%
      dplyr::ungroup()

    unlink(
      x = exportFolder,
      recursive = TRUE,
      force = TRUE
    )
    dir.create(
      path = exportFolder,
      showWarnings = FALSE,
      recursive = TRUE
    )

    cohortTable <-
      paste0(
        "ct_",
        format(Sys.time(), "%s"),
        sample(1:100, 1)
      )

    DatabaseConnector::insertTable(
      connection = con,
      databaseSchema = server$cohortDatabaseSchema,
      tableName = cohortTable,
      data = cohort,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = FALSE,
      camelCaseToSnakeCase = TRUE,
      progressBar = FALSE
    )

    CohortDiagnostics::runTimeSeries(
      connection = con,
      tempEmulationSchema = server$tempEmulationSchema,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionSet = cohortDefinitionSet %>%
        dplyr::filter(cohortId %in% c(1, 2)),
      runCohortTimeSeries = TRUE,
      runDataSourceTimeSeries = FALSE,
      databaseId = "testDatabaseId",
      exportFolder = exportFolder,
      minCellCount = 0,
      instantiatedCohorts = cohort$cohortDefinitionId,
      incremental = incremental,
      recordKeepingFile = recordKeepingFile,
      observationPeriodDateRange = dplyr::tibble(
        observationPeriodMinDate = as.Date("2004-01-01"),
        observationPeriodMaxDate = as.Date("2007-12-31")
      ),
      batchSize = 1
    )

    # result
    timeSeriesResults <-
      readr::read_csv(
        file = file.path(exportFolder, "time_series.csv"),
        col_types = readr::cols()
      )
    print(timeSeriesResults)

    testthat::expect_equal(
      object = timeSeriesResults$cohort_id %>% unique() %>% sort(),
      expected = c(1, 2)
    )

    subset <- CohortDiagnostics:::subsetToRequiredCohorts(
      cohorts = cohortDefinitionSet,
      task = "runCohortTimeSeries",
      incremental = incremental
    ) %>%
      dplyr::arrange(cohortId)

    testthat::expect_equal(
      object = subset$cohortId,
      expected = c(1, 2, 1000, 2000)
    )
  })
})

test_that("Testing cohort time series execution, incremental = TRUE", {
  testServer <- "sqlite"
  skip_if_not(testServer %in% names(testServers))
  server <- testServers[[testServer]]
  con <- DatabaseConnector::connect(server$connectionDetails)
  exportFolder <- file.path(tempdir(), paste0(testServer, "exp"))
  recordKeepingFile <- file.path(exportFolder, "record.csv")
  incremental <- TRUE

  with_dbc_connection(con, {
    cohort <- dplyr::tibble(
      cohortDefinitionId = c(1, 1, 2, 2),
      subjectId = c(1, 1, 1, 2),
      cohortStartDate = c(
        as.Date("2005-01-15"),
        as.Date("2005-07-15"),
        as.Date("2005-01-15"),
        as.Date("2005-07-15")
      ),
      cohortEndDate = c(
        as.Date("2005-05-15"),
        as.Date("2005-09-15"),
        as.Date("2005-05-15"),
        as.Date("2005-09-15")
      )
    )
    
    cohort <- dplyr::bind_rows(
      cohort,
      cohort %>%
        dplyr::mutate(cohortDefinitionId = cohortDefinitionId * 1000)
    )
    
    cohortDefinitionSet <-
      cohort %>%
      dplyr::select(cohortDefinitionId) %>%
      dplyr::distinct() %>%
      dplyr::rename("cohortId" = "cohortDefinitionId") %>%
      dplyr::rowwise() %>%
      dplyr::mutate(json = RJSONIO::toJSON(list(
        cohortId = cohortId,
        randomString = c(
          sample(x = LETTERS, 5, replace = TRUE),
          sample(x = LETTERS, 4, replace = TRUE),
          sample(LETTERS, 1, replace = TRUE)
        )
      ))) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(
        sql = json,
        checksum = as.character(CohortDiagnostics:::computeChecksum(json))
      ) %>%
      dplyr::ungroup()
    
    unlink(
      x = exportFolder,
      recursive = TRUE,
      force = TRUE
    )
    dir.create(
      path = exportFolder,
      showWarnings = FALSE,
      recursive = TRUE
    )
    
    cohortTable <-
      paste0(
        "ct_",
        format(Sys.time(), "%s"),
        sample(1:100, 1)
      )
    
    DatabaseConnector::insertTable(
      connection = con,
      databaseSchema = server$cohortDatabaseSchema,
      tableName = cohortTable,
      data = cohort,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = FALSE,
      camelCaseToSnakeCase = TRUE,
      progressBar = FALSE
    )
    
    CohortDiagnostics::runTimeSeries(
      connection = con,
      tempEmulationSchema = server$tempEmulationSchema,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionSet = cohortDefinitionSet %>%
        dplyr::filter(cohortId %in% c(1, 2)),
      runCohortTimeSeries = TRUE,
      runDataSourceTimeSeries = FALSE,
      databaseId = "testDatabaseId",
      exportFolder = exportFolder,
      minCellCount = 0,
      instantiatedCohorts = cohort$cohortDefinitionId,
      incremental = incremental,
      recordKeepingFile = recordKeepingFile,
      observationPeriodDateRange = dplyr::tibble(
        observationPeriodMinDate = as.Date("2004-01-01"),
        observationPeriodMaxDate = as.Date("2007-12-31")
      ),
      batchSize = 1
    )
    
    recordKeepingFileData <-
      readr::read_csv(
        file = recordKeepingFile,
        col_types = readr::cols()
      )
    
    # testing if check sum is written
    testthat::expect_true("checksum" %in% colnames(recordKeepingFileData))
    
    # result
    timeSeriesResults1 <-
      readr::read_csv(
        file = file.path(exportFolder, "time_series.csv"),
        col_types = readr::cols()
      )
    
    subset <- CohortDiagnostics:::subsetToRequiredCohorts(
      cohorts = cohortDefinitionSet,
      task = "runCohortTimeSeries",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    ) %>%
      dplyr::arrange(cohortId)
    
    testthat::expect_equal(
      object = subset$cohortId,
      expected = c(1000, 2000)
    )
    
    # delete the previously written results file. To see if the previously executed cohorts will have results after deletion
    unlink(
      x = file.path(exportFolder, "time_series.csv"),
      recursive = TRUE,
      force = TRUE
    )
    
    CohortDiagnostics::runTimeSeries(
      connection = con,
      tempEmulationSchema = server$tempEmulationSchema,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionSet = cohortDefinitionSet,
      runCohortTimeSeries = TRUE,
      runDataSourceTimeSeries = FALSE,
      databaseId = "testDatabaseId",
      exportFolder = exportFolder,
      minCellCount = 0,
      instantiatedCohorts = cohort$cohortDefinitionId,
      incremental = incremental,
      recordKeepingFile = recordKeepingFile,
      observationPeriodDateRange = dplyr::tibble(
        observationPeriodMinDate = as.Date("2004-01-01"),
        observationPeriodMaxDate = as.Date("2007-12-31")
      ),
      batchSize = 100
    )
    resultsNew <-
      readr::read_csv(
        file = file.path(exportFolder, "time_series.csv"),
        col_types = readr::cols()
      )
    
    testthat::expect_equal(
      object = resultsNew$cohort_id %>% unique() %>% sort(),
      expected = c(1000, 2000)
    )
  })
})

test_that("Testing Data source time series execution, incremental = FALSE ", {
  testServer <- "sqlite"
  skip_if_not(testServer %in% names(testServers))
  server <- testServers[[testServer]]
  con <- DatabaseConnector::connect(server$connectionDetails)
  exportFolder <- file.path(tempdir(), paste0(testServer, "exp"))
  recordKeepingFile <- file.path(exportFolder, "record.csv")
  incremental <- FALSE

  with_dbc_connection(con, {
    cohortDefinitionSet <- dplyr::tibble(
      cohortId = -44819062,
      # cohort id is identified by an omop concept id https://athena.ohdsi.org/search-terms/terms/44819062
      checksum = CohortDiagnostics:::computeChecksum(column = "data source time series")
    )

    unlink(
      x = exportFolder,
      recursive = TRUE,
      force = TRUE
    )
    dir.create(
      path = exportFolder,
      showWarnings = FALSE,
      recursive = TRUE
    )

    CohortDiagnostics::runTimeSeries(
      connection = con,
      tempEmulationSchema = server$tempEmulationSchema,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortDefinitionSet = data.frame(),
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = TRUE,
      databaseId = "testDatabaseId",
      exportFolder = exportFolder,
      minCellCount = 0,
      incremental = incremental,
      observationPeriodDateRange = dplyr::tibble(
        observationPeriodMinDate = as.Date("2004-01-01"),
        observationPeriodMaxDate = as.Date("2007-12-31")
      )
    )

    # result
    dataSourceTimeSeriesResult <- readr::read_csv(file = file.path(exportFolder, "time_series.csv"),
                                                  col_types = readr::cols())

    subset <- subsetToRequiredCohorts(
      cohorts = cohortDefinitionSet,
      task = "runDataSourceTimeSeries",
      incremental = incremental
    ) %>%
      dplyr::arrange(cohortId)

    testthat::expect_equal(
      object = nrow(subset),
      expected = 1
    )
  })
})

test_that("Testing Data source time series execution, incremental = TRUE ", {
  testServer <- "sqlite"
  skip_if_not(testServer %in% names(testServers))
  server <- testServers[[testServer]]
  con <- DatabaseConnector::connect(server$connectionDetails)
  exportFolder <- file.path(tempdir(), paste0(testServer, "exp"))
  recordKeepingFile <- file.path(exportFolder, "record.csv")
  incremental <- TRUE

  with_dbc_connection(con, {
    cohortDefinitionSet <- dplyr::tibble(
      cohortId = -44819062,
      # cohort id is identified by an omop concept id https://athena.ohdsi.org/search-terms/terms/44819062
      checksum = CohortDiagnostics:::computeChecksum(column = "data source time series")
    )

    unlink(
      x = exportFolder,
      recursive = TRUE,
      force = TRUE
    )
    dir.create(
      path = exportFolder,
      showWarnings = FALSE,
      recursive = TRUE
    )

    CohortDiagnostics::runTimeSeries(
      connection = con,
      tempEmulationSchema = server$tempEmulationSchema,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortDefinitionSet = data.frame(),
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = TRUE,
      databaseId = "testDatabaseId",
      exportFolder = exportFolder,
      minCellCount = 0,
      incremental = incremental,
      recordKeepingFile = recordKeepingFile,
      observationPeriodDateRange = dplyr::tibble(
        observationPeriodMinDate = as.Date("2004-01-01"),
        observationPeriodMaxDate = as.Date("2007-12-31")
      )
    )

    recordKeepingFileData <- readr::read_csv(file = recordKeepingFile,
                                             col_types = readr::cols())

    # testing if check sum is written
    testthat::expect_true("checksum" %in% colnames(recordKeepingFileData))
    testthat::expect_equal(object = recordKeepingFileData$cohortId, expected = -44819062)

    # result
    dataSourceTimeSeriesResult <- readr::read_csv(file = file.path(exportFolder, "time_series.csv"),
                                                  col_types = readr::cols())

    subset <- subsetToRequiredCohorts(
      cohorts = cohortDefinitionSet,
      task = "runDataSourceTimeSeries",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    ) %>%
      dplyr::arrange(cohortId)

    testthat::expect_equal(
      object = nrow(subset),
      expected = 0
    )
  })
})
