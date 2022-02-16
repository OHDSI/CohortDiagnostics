test_that("Testing cohort relationship logic", {
  # manually create cohort table and load to table

  # connection <-
  #   DatabaseConnector::connect(Eunomia::getEunomiaConnectionDetails())
  # cdmDatabaseSchema <- "main"

  temporalStartDays = c(seq(
    from = -42,
    to = 42,
    by = 7
  ))
  temporalEndDays = c(seq(
    from = -36,
    to = 48,
    by = 7
  ))

  targetCohort <- dplyr::tibble(cohortDefinitionId = c(1),
                                subjectId = c(1),
                                cohortStartDate = c(as.Date("1900-01-15")),
                                cohortEndDate = c(as.Date("1900-01-21")))

  comparatorCohort <- dplyr::tibble(cohortDefinitionId = c(10, 10, 10, 10),
                                subjectId = c(1, 1, 1, 1),
                                cohortStartDate = c(as.Date("2000-01-01"), as.Date("1900-01-01"), as.Date("1900-01-16"), as.Date("1900-01-20")),
                                cohortEndDate = c(as.Date("2000-01-31"), as.Date("1900-01-05"), as.Date("1900-01-18"), as.Date("1900-01-30")))

  cohort <- dplyr::bind_rows(targetCohort, comparatorCohort)

  connectionCohortRelationship <- DatabaseConnector::connect(connectionDetails)

  DatabaseConnector::insertTable(connection = connectionCohortRelationship,
                                 tableName = "#cohort_relationship",
                                 data = cohort,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 camelCaseToSnakeCase = TRUE,
                                 progressBar = FALSE)
  writeLines("Successfully inserted data into temp table")
  # debug(CohortDiagnostics::runCohortRelationshipDiagnostics)
  runCohortRelationshipDiagnostics(
    connection = connectionCohortRelationship,
    cohortDatabaseSchema = NULL,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#cohort_relationship",
    targetCohortIds = c(1, 2, 3),
    comparatorCohortIds = c(10, 11, 12),
    relationshipDays = dplyr::tibble(startDay = temporalStartDays,
                                     endDay = temporalEndDays)
  )
  writeLines("Successfully COMPLETED runCohortRelationshipDiagnostics")
  DatabaseConnector::disconnect(connectionCohortRelationship)
  
  # TO DO
    # READ THE CSV FILE

})
