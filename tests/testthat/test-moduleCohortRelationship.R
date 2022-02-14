test_that("Testing cohort relationship logic", {
  # manually create cohort table and load to table
  
  # connection <-
  #   DatabaseConnector::connect(Eunomia::getEunomiaConnectionDetails())
  # cdmDatabaseSchema <- "main"
  
  temporalStartDays = c(seq(
    from = -421,
    to = -31,
    by = 30
  ))
  temporalEndDays = c(seq(
    from = -391,
    to = -1,
    by = 30
  ))
  
  targetCohort <- dplyr::tibble(cohortDefinitionId = c(1, 1, 1, 1),
                                subjectId = c(1, 1, 2, 2),
                                cohortStartDate = c(as.Date("1900-01-15"), as.Date("1900-05-01"), as.Date("1901-01-15"), as.Date("1901-05-01")),
                                cohortEndDate = c(as.Date("1900-03-31"), as.Date("1900-07-31"), as.Date("1901-03-31"), as.Date("1901-07-31")))
  
  comparatorCohort <- dplyr::tibble(cohortDefinitionId = c(10, 10, 10, 10),
                                subjectId = c(1, 1, 2, 2),
                                cohortStartDate = c(as.Date("1900-01-01"), as.Date("1900-04-01"), as.Date("1901-01-01"), as.Date("1901-05-01")),
                                cohortEndDate = c(as.Date("1900-02-28"), as.Date("1900-05-31"), as.Date("1901-02-15"), as.Date("1901-06-30")))
  
  cohort <- dplyr::bind_rows(targetCohort, comparatorCohort)
  
  DatabaseConnector::insertTable(connection = connection, 
                                 tableName = "#cohort_relationship", 
                                 data = cohort,
                                 dropTableIfExists = TRUE, 
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 camelCaseToSnakeCase = TRUE, 
                                 progressBar = FALSE)
  debug(CohortDiagnostics::runCohortRelationshipDiagnostics)
  runCohortRelationshipDiagnostics(
    connection = connection,
    cohortDatabaseSchema = NULL,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#cohort_relationship",
    targetCohortIds = c(1, 2, 3),
    comparatorCohortIds = c(10, 11, 12),
    relationshipDays = dplyr::tibble(startDay = temporalStartDays,
                                     endDay = temporalEndDays)
  )
  
  
})
