
test_that("Cohort instantiation", {
  skip_if(skipCdmTests, 'cdm settings not configured')
  instantiateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    packageName = "CohortDiagnostics",
    cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
    cohortIds = cohortIds,
    generateInclusionStats = TRUE,
    createCohortTable = TRUE,
    inclusionStatisticsFolder = file.path(folder, "incStats")
  )

  connection <- DatabaseConnector::connect(connectionDetails)
  with_dbc_connection(connection, {
    sql <-
      "SELECT COUNT(*) AS cohort_count, cohort_definition_id
    FROM @cohort_database_schema.@cohort_table
    GROUP BY cohort_definition_id;"
    counts <-
      DatabaseConnector::renderTranslateQuerySql(
        connection,
        sql,
        cohort_database_schema = cohortDatabaseSchema,
        cohort_table = cohortTable,
        snakeCaseToCamelCase = TRUE
      )
    testthat::expect_gt(nrow(counts), 2)
  })
})

test_that("Cohort diagnostics in incremental mode", {
  skip_if(skipCdmTests, 'cdm settings not configured')
  cohortDefinitionSet <- loadCohortsFromPackage(
    packageName = "CohortDiagnostics",
    cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
    cohortIds = cohortIds
  )

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
      inclusionStatisticsFolder = file.path(folder, "incStats"),
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

  testthat::expect_true(file.exists(file.path(
    folder, "export", paste0("Results_", dbms ,".zip")
  )))

  # We now run it with all cohorts without specifying ids - testing incremental mode
  secondTime <- system.time(
    executeDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionSet = cohortDefinitionSet,
      inclusionStatisticsFolder = file.path(folder, "incStats"),
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
  testthat::expect_lt(secondTime[1], firstTime[1])

  # generate premerged file
  preMergeDiagnosticsFiles(dataFolder = file.path(folder, "export"))
  testthat::expect_true(file.exists(file.path(folder, "export", "PreMerged.RData")))

  output <- read.csv(file.path(folder, "export", "covariate_value.csv"))
  expect_equal(output$sum_value[2], -minCellCountValue)
  expect_lt(output$mean[2], 0)
})

