test_that("Testing time series logic", {
  skip_if(skipCdmTests, 'cdm settings not configured')
  
  connectionTimeSeries <-
    DatabaseConnector::connect(connectionDetails)
  
  # to do - with incremental = FALSE
  with_dbc_connection(connectionTimeSeries, {
    # manually create cohort table and load to table
    cohort <- dplyr::tibble(
      cohortDefinitionId = c(1),
      subjectId = c(1, 2),
      cohortStartDate = c(as.Date("2000-01-15"), as.Date("2000-04-15")),
      cohortEndDate = c(as.Date("2000-05-15"), as.Date("2000-08-15"))
    )
    
    cohortTable <-
      paste0("ct_",
             gsub("[: -]", "", Sys.time(), perl = TRUE),
             sample(1:100, 1))
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
        runCohortTimeSeries =  TRUE,
        runDataSourceTimeSeries = TRUE
      )
    
    
    
    
  })
})
