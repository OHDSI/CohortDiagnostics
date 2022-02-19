test_that("Testing cohort relationship logic - incremental FALSE", {
  skip_if(skipCdmTests, 'cdm settings not configured')
  
  # manually create cohort table and load to table
  # for the logic to work - there has to be some overlap of the comparator cohort over target cohort
  # note - we will not be testing offset in this test. it is expected to work as it is a simple substraction
  
  temporalStartDays <- c(0)
  temporalEndDays <- c(0)
  
  targetCohort <- dplyr::tibble(
    cohortDefinitionId = c(1),
    subjectId = c(1),
    cohortStartDate = c(as.Date("1900-01-15")),
    cohortEndDate = c(as.Date("1900-01-31"))
  )#target cohort always one row
  
  comparatorCohort <- #all records here overlap with targetCohort
    dplyr::tibble(
      cohortDefinitionId = c(10, 10, 10),
      subjectId = c(1, 1, 1),
      cohortStartDate = c(
        as.Date("1900-01-01"), # starts before target cohort start
        as.Date("1900-01-22"), # starts during target cohort period and ends during target cohort period
        as.Date("1900-01-31")
      ),
      cohortEndDate = c(
        as.Date("1900-01-20"),
        as.Date("1900-01-29"),
        as.Date("1900-01-31")
      )
    )
  
  cohort <- dplyr::bind_rows(targetCohort, comparatorCohort)
  
  connectionCohortRelationship <-
    DatabaseConnector::connect(connectionDetails)
  
  # to do - with incremental = FALSE
  with_dbc_connection(connectionCohortRelationship, {
    
    sysTime <- as.numeric(Sys.time()) * 100000
    tableName <- paste0("cr", sysTime)
    observationTableName <- paste0("op", sysTime)
    
    DatabaseConnector::insertTable(
      connection = connectionCohortRelationship,
      databaseSchema = cohortDatabaseSchema,
      tableName = tableName,
      data = cohort,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = FALSE,
      camelCaseToSnakeCase = TRUE,
      progressBar = FALSE
    )

    cohortRelationship <- runCohortRelationshipDiagnostics(
      connection = connectionCohortRelationship,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cohortDatabaseSchema,
      cohortTable = tableName,
      targetCohortIds = c(1),
      comparatorCohortIds = c(10),
      relationshipDays = dplyr::tibble(startDay = temporalStartDays,
                                       endDay = temporalEndDays),
      observationPeriodRelationship = FALSE, #this is set to FALSE because it will otherwise use real CDM observation table
                                            #which is not possible because we are simulating cohort table/person_id
    )
    
    sqlDrop <-
      "IF OBJECT_ID('@cohort_database_schema.@cohort_relationship_cohort_table', 'U') IS NOT NULL
            DROP TABLE @cohort_database_schema.@cohort_relationship_cohort_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connectionCohortRelationship,
      sql = sqlDrop,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_relationship_cohort_table = tableName,
      profile = FALSE,
      progressBar = FALSE
    )
    
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


