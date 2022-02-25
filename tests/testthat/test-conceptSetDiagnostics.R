test_that("Concept set diagnostics - without cohort table", {
  skip_if(skipCdmTests, 'cdm settings not configured')
  
  cohortIdForConceptSetDiagnostic <- c(18342)
  cohortDefinitionSet1 <-
    loadTestCohortDefinitionSet(cohortIdForConceptSetDiagnostic)
  conceptSetDiagnostics <-
    runConceptSetDiagnostics(
      connectionDetails = connectionDetails,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = cdmDatabaseSchema,
      cohorts = cohortDefinitionSet1,
      keep2BillionConceptId = TRUE,
      runConceptSetOptimization = TRUE,
      runExcludedConceptSet = TRUE,
      runOrphanConcepts = FALSE,
      runBreakdownIndexEvents = TRUE,
      runBreakdownIndexEventRelativeDays = 0,
      runIndexDateConceptCoOccurrence = TRUE,
      runStandardToSourceMappingCount = FALSE,
      runConceptCount = TRUE,
      runConceptCountByCalendarPeriod = TRUE,
      minCellCount = 0
    )
  
  testthat::expect_true(object = Andromeda::isAndromeda(conceptSetDiagnostics))
  
})

test_that("Concept set diagnostics - with cohort table but not instantiated", {
  skip_if(skipCdmTests, 'cdm settings not configured')
  
  cohortIdForConceptSetDiagnostic <- c(18342)
  cohortDefinitionSet1 <-
    loadTestCohortDefinitionSet(cohortIdForConceptSetDiagnostic)
  
  cohortTableNames <-
    CohortGenerator::getCohortTableNames(cohortTable = "conceptSetCohortNo")
  conceptSetDiagnostics <-
    runConceptSetDiagnostics(
      connectionDetails = connectionDetails,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = cdmDatabaseSchema,
      cohorts = cohortDefinitionSet1,
      cohortIds = cohortIdForConceptSetDiagnostic,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTableNames$cohortTable,
      keep2BillionConceptId = TRUE,
      runConceptSetOptimization = TRUE,
      runExcludedConceptSet = TRUE,
      runOrphanConcepts = TRUE,
      runBreakdownIndexEvents = TRUE,
      runBreakdownIndexEventRelativeDays = c(-5:5),
      runIndexDateConceptCoOccurrence = TRUE,
      runStandardToSourceMappingCount = TRUE,
      runConceptCount = TRUE,
      runConceptCountByCalendarPeriod = TRUE,
      minCellCount = 0
    )
  
  testthat::expect_true(object = Andromeda::isAndromeda(conceptSetDiagnostics))
  
})


test_that("Concept set diagnostics - with instantiation", {
  skip_if(skipCdmTests, 'cdm settings not configured')
  
  cohortIdForConceptSetDiagnostic <- c(18342)
  cohortDefinitionSet1 <-
    loadTestCohortDefinitionSet(cohortIdForConceptSetDiagnostic)
  
  cohortTableNames <-
    CohortGenerator::getCohortTableNames(cohortTable = "conceptSetCohort")
  # Next create the tables on the database
  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = FALSE
  )
  
  # Generate the cohort set
  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet1,
    incremental = FALSE
  )
  
  conceptSetDiagnostics <-
    runConceptSetDiagnostics(
      connectionDetails = connectionDetails,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = cdmDatabaseSchema,
      cohorts = cohortDefinitionSet1,
      cohortIds = cohortIdForConceptSetDiagnostic,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTableNames$cohortTable,
      keep2BillionConceptId = TRUE,
      runConceptSetOptimization = TRUE,
      runExcludedConceptSet = TRUE,
      runOrphanConcepts = TRUE,
      runBreakdownIndexEvents = TRUE,
      runBreakdownIndexEventRelativeDays = c(-5:5),
      runIndexDateConceptCoOccurrence = TRUE,
      runStandardToSourceMappingCount = TRUE,
      runConceptCount = TRUE,
      runConceptCountByCalendarPeriod = TRUE,
      minCellCount = 0
    )
  
  testthat::expect_true(object = Andromeda::isAndromeda(conceptSetDiagnostics))
  
})

