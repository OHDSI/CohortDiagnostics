test_that("Testing getIncidenceRate", {
  
  skip_if(skipCdmTests, "cdm settings not configured")
  exportFolder <- tempfile()
  dir.create(exportFolder)
  on.exit(unlink(exportFolder), add = TRUE)
  
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
  # Next create the tables on the database
  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = FALSE
  )
  
  
  
  #Cleanup
  on.exit({
    CohortGenerator::dropCohortStatsTables(connectionDetails = connectionDetails,
                                           cohortDatabaseSchema = cohortDatabaseSchema,
                                           cohortTableNames = cohortTableNames)
    connection <- DatabaseConnector::connect(connectionDetails)
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 "DROP TABLE @cohortDatabaseSchema.@cohortTable",
                                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                                 cohortTable = cohortTable)
  }, add = TRUE)
  
  # Generate the cohort set
  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    incremental = FALSE
  )
  
  
  cohortCounts <- getCohortCounts(connectionDetails = connectionDetails,
                         cohortDatabaseSchema = cohortDatabaseSchema,
                         cohortIds = cohortDefinitionSet$cohortId)
  testthat::expect_s3_class(cohortCounts, "data.frame")

})