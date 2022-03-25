test_that("Testing time series logic", {
  skip_if(skipCdmTests, "cdm settings not configured")

  connectionTimeSeries <-
    DatabaseConnector::connect(connectionDetails)

  # to do - with incremental = FALSE
  with_dbc_connection(connectionTimeSeries, {

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
      paste0(
        "ct_",
        gsub("[: -]", "", Sys.time(), perl = TRUE),
        sample(1:100, 1)
      )
    DatabaseConnector::insertTable(
      connection = connectionTimeSeries,
      databaseSchema = cohortDatabaseSchema,
      tableName = cohortTable,
      data = cohort,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = FALSE,
      camelCaseToSnakeCase = TRUE,
      progressBar = FALSE
    )

    timeSeries <-
      runCohortTimeSeriesDiagnostics(
        connection = connectionTimeSeries,
        tempEmulationSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
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
