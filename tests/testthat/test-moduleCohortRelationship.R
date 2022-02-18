test_that("Testing cohort relationship logic", {
  # manually create cohort table and load to table
  
  # connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  # cdmDatabaseSchema <- "main"
  # cohortDatabaseSchema <- "main"
  
  temporalStartDays = c(seq(from = -42,
                            to = 42,
                            by = 7))
  temporalEndDays = c(seq(from = -36,
                          to = 48,
                          by = 7))
  
  targetCohort <- dplyr::tibble(
    cohortDefinitionId = c(1),
    subjectId = c(1),
    cohortStartDate = c(as.Date("1900-01-15")),
    cohortEndDate = c(as.Date("1900-01-21"))
  )
  
  comparatorCohort <-
    dplyr::tibble(
      cohortDefinitionId = c(10, 10, 10, 10),
      subjectId = c(1, 1, 1, 1),
      cohortStartDate = c(
        as.Date("2000-01-01"),
        as.Date("1900-01-01"),
        as.Date("1900-01-16"),
        as.Date("1900-01-20")
      ),
      cohortEndDate = c(
        as.Date("2000-01-31"),
        as.Date("1900-01-05"),
        as.Date("1900-01-18"),
        as.Date("1900-01-30")
      )
    )
  
  cohort <- dplyr::bind_rows(targetCohort, comparatorCohort)
  
  connectionCohortRelationship <-
    DatabaseConnector::connect(connectionDetails)
  
  # to do - with incremental = FALSE
  with_dbc_connection(connectionCohortRelationship, {
    
    tableName <- paste0("cr", as.numeric(Sys.time()) * 100000)
    
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
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortTable = tableName,
      targetCohortIds = c(1, 2, 3),
      comparatorCohortIds = c(10, 11, 12),
      relationshipDays = dplyr::tibble(startDay = temporalStartDays,
                                       endDay = temporalEndDays), 
      incremental = FALSE
    )
    # TO DO
    # READ THE CSV FILE
    
  })
 # testthat::expect_true(is.data.frame(cohortRelationship))
 # testthat::expect_true(nrow(cohortRelationship) > 0)
  
  
  # to do - with incremental = TRUE
})
