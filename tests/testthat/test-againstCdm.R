test_that("Cohort diagnostics in incremental mode", {
  skip_if(skipCdmTests, 'cdm settings not configured')
  cohortDefinitionSet <- loadCohortsFromPackage(
    packageName = "CohortDiagnostics",
    cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
    cohortIds = cohortIds
  )

  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
  # Next create the tables on the database
  CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                      cohortTableNames = cohortTableNames,
                                      cohortDatabaseSchema = cohortDatabaseSchema,
                                      incremental = FALSE)

  # Generate the cohort set
  CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                     cohortTableNames = cohortTableNames,
                                     cohortDefinitionSet = cohortDefinitionSet,
                                     incremental = FALSE)


  firstTime <- system.time(
    executeDiagnostics(
      cohortDefinitionSet = cohortDefinitionSet,
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = cohortIds,
      exportFolder = file.path(folder, "export"),
      databaseId = dbms,
      runInclusionStatistics = TRUE,
      runBreakdownIndexEvents = TRUE,
      runCohortCharacterization = TRUE,
      runTemporalCohortCharacterization = TRUE,
      runCohortOverlap = TRUE,
      runIncidenceRate = TRUE,
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = TRUE,
      runTimeDistributions = TRUE,
      runTimeSeries = TRUE,
      minCellCount = minCellCountValue,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental"),
      covariateSettings = covariateSettings,
      temporalCovariateSettings = temporalCovariateSettings
    )
  )

  expect_true(file.exists(file.path(
    folder, "export", paste0("Results_", dbms ,".zip")
  )))

  # We now run it with all cohorts without specifying ids - testing incremental mode
  secondTime <- system.time(
    executeDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      cohortDefinitionSet = cohortDefinitionSet,
      exportFolder = file.path(folder, "export"),
      databaseId = dbms,
      runInclusionStatistics = TRUE,
      runBreakdownIndexEvents = TRUE,
      runCohortCharacterization = TRUE,
      runCohortOverlap = TRUE,
      runIncidenceRate = TRUE,
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = TRUE,
      runTimeDistributions = TRUE,
      runTimeSeries = TRUE,
      minCellCount = minCellCountValue,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental"),
      covariateSettings = covariateSettings,
      temporalCovariateSettings = temporalCovariateSettings
    )
  )
  expect_lt(secondTime[1], firstTime[1])

  # generate premerged file
  preMergeDiagnosticsFiles(dataFolder = file.path(folder, "export"))
  expect_true(file.exists(file.path(folder, "export", "PreMerged.RData")))

  output <- read.csv(file.path(folder, "export", "covariate_value.csv"))

  expect_true(is.numeric(output$sum_value[2]))
  expect_true(is.numeric(output$mean[2]))
})

