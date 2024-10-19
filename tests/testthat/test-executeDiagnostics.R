
nm = "sqlite"
# for (nm in names(testServers)) {
for (nm in "sqlite") {
  server <- testServers[[nm]]

  test_that(paste("Cohort diagnostics in incremental mode on", nm), {

    exportFolder <- tempfile()
    dir.create(exportFolder)

    incrementalFolder <- tempfile()
    dir.create(incrementalFolder)
    databaseId <- nm

    firstTime <- system.time(
      executeDiagnostics(
        cohortDefinitionSet = server$cohortDefinitionSet,
        connectionDetails = server$connectionDetails,
        cdmDatabaseSchema = server$cdmDatabaseSchema,
        vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
        tempEmulationSchema = server$tempEmulationSchema,
        cohortDatabaseSchema = server$cohortDatabaseSchema,
        cohortTable = server$cohortTable,
        cohortIds = server$cohortIds,
        exportFolder = exportFolder,
        databaseId = databaseId,
        runInclusionStatistics = TRUE,
        runBreakdownIndexEvents = TRUE,
        runTemporalCohortCharacterization = TRUE,
        runIncidenceRate = TRUE,
        runIncludedSourceConcepts = TRUE,
        runOrphanConcepts = TRUE,
        runTimeSeries = TRUE,
        runCohortRelationship = TRUE,
        minCellCount = 0,
        incremental = TRUE,
        incrementalFolder = incrementalFolder,
        temporalCovariateSettings = server$temporalCovariateSettings,
        runFeatureExtractionOnSample = FALSE
      )
    )

    expect_true(file.exists(file.path(
      exportFolder, paste0("Results_", databaseId, ".zip")
    )))

    # We now run it with all cohorts without specifying ids - testing incremental mode
    secondTime <- system.time(
      executeDiagnostics(
        connectionDetails = server$connectionDetails,
        cdmDatabaseSchema = server$cdmDatabaseSchema,
        tempEmulationSchema = server$tempEmulationSchema,
        cohortDatabaseSchema = server$cohortDatabaseSchema,
        cohortTable = server$cohortTable,
        cohortDefinitionSet = server$cohortDefinitionSet,
        exportFolder = exportFolder,
        databaseId = databaseId,
        runInclusionStatistics = TRUE,
        runBreakdownIndexEvents = TRUE,
        runTemporalCohortCharacterization = TRUE,
        runIncidenceRate = TRUE,
        runIncludedSourceConcepts = TRUE,
        runOrphanConcepts = TRUE,
        runTimeSeries = TRUE,
        runCohortRelationship = TRUE,
        minCellCount = 5,
        incremental = TRUE,
        incrementalFolder = exportFolder,
        temporalCovariateSettings = server$temporalCovariateSettings
      )
    )
    # generate sqlite file
    sqliteDbPath <- tempfile(fileext = ".sqlite")
    createMergedResultsFile(dataFolder = exportFolder, sqliteDbPath = sqliteDbPath)
    expect_true(file.exists(sqliteDbPath))

    # File exists
    expect_error(createMergedResultsFile(dataFolder = exportFolder, sqliteDbPath = sqliteDbPath))

    if (dbms == "sqlite") {
      # Get file sizes of batch computed results
      batchedResultsFiles <- c("temporal_covariate_value.csv", "cohort_relationships.csv", "time_series.csv")
      bacthFiles <- file.path(exportFolder, batchedResultsFiles)
      fileSizes <- list()
      for (filePath in batchedResultsFiles) {
        fileSizes[[filePath]] <- file.size(filePath)
      }

      ## Repeat tests with incremental set to false to ensure better code coverage
      withr::with_options(list(
        "CohortDiagnostics-TimeSeries-batch-size" = 1,
        "CohortDiagnostics-FE-batch-size" = 1,
        "CohortDiagnostics-Relationships-batch-size" = 50
      ), {
        executeDiagnostics(
          connectionDetails = connectionDetails,
          cdmDatabaseSchema = cdmDatabaseSchema,
          tempEmulationSchema = tempEmulationSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTableNames = cohortTableNames,
          cohortDefinitionSet = cohortDefinitionSet,
          exportFolder = exportFolder,
          databaseId = dbms,
          runInclusionStatistics = TRUE,
          runBreakdownIndexEvents = TRUE,
          runTemporalCohortCharacterization = TRUE,
          runIncidenceRate = TRUE,
          runIncludedSourceConcepts = TRUE,
          runOrphanConcepts = TRUE,
          runTimeSeries = TRUE,
          runCohortRelationship = TRUE,
          minCellCount = minCellCountValue,
          incremental = FALSE,
          incrementalFolder = exportFolder,
          temporalCovariateSettings = temporalCovariateSettings,
          runOnSample = TRUE
        )
      })

      for (filePath in names(fileSizes)) {
        # Because we set options to small batches if these were written correctly they shouldn't have changed in size
        expect_equal(file.size(filePath), fileSizes[[filePath]])
      }
    }

    # Test zip works
    DiagnosticsExplorerZip <- tempfile(fileext = "de.zip")
    unlink(DiagnosticsExplorerZip)
    on.exit(unlink(DiagnosticsExplorerZip))
    createDiagnosticsExplorerZip(outputZipfile = DiagnosticsExplorerZip, sqliteDbPath = sqliteDbPath)

    expect_true(file.exists(DiagnosticsExplorerZip))
    # already exists
    expect_error(createDiagnosticsExplorerZip(outputZipfile = DiagnosticsExplorerZip, sqliteDbPath = sqliteDbPath))
    # Bad filepath
    expect_error(createDiagnosticsExplorerZip(outputZipfile = "foo", sqliteDbPath = "sdlfkmdkmfkd"))
    output <- read.csv(file.path(exportFolder, "temporal_covariate_value.csv"))

    expect_true(is.numeric(output$sum_value[2]))
    expect_true(is.numeric(output$mean[2]))
  })

}

