test_that("Testing time series logic", {
  skip_if(skipCdmTests, 'cdm settings not configured')
  
  # manually create cohort table and load to table
  connectionCohortRelationship <-
    DatabaseConnector::connect(connectionDetails)
  
  # to do - with incremental = FALSE
  with_dbc_connection(connectionCohortRelationship, {
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
      connection = connectionCohortRelationship,
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
        connectionDetails = connectionDetails,
        tempEmulationSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        runCohortTimeSeries =  TRUE,
        runDataSourceTimeSeries = TRUE
      )
    
    browser()
    
    cohortRelationshipT1C10 <- cohortRelationship %>%
      dplyr::filter(.data$cohortId == 1) %>%
      dplyr::filter(.data$comparatorCohortId == 10)
    
    testthat::expect_equal(object = cohortRelationshipT1C10$subCsBeforeTs,
                           expected = 1) # there is one subject in comparator that starts before target
    
    testthat::expect_equal(object = cohortRelationshipT1C10$subCsBeforeTe,
                           expected = 1) # there is one subject in comparator that starts before target end
    
    testthat::expect_equal(object = cohortRelationshipT1C10$subCsAfterTs,
                           expected = 1) # there is one subject in comparator that starts after target start
    
    testthat::expect_equal(object = cohortRelationshipT1C10$subCsAfterTs,
                           expected = 1) # there is one subject in comparator that starts after target start
    
    testthat::expect_equal(object = cohortRelationshipT1C10$subCsOnTe,
                           expected = 1) # there is one subject in comparator that starts on target end
    
    testthat::expect_equal(object = cohortRelationshipT1C10$subCsWindowT,
                           expected = 1) # there is one subject in comparator that started within the window of Target cohort
    
    testthat::expect_equal(object = cohortRelationshipT1C10$subCeWindowT,
                           expected = 1) # there is one subject in comparator that ended within the window of Target cohort
    
    
  })
})
