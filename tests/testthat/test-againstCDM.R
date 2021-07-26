# Clean up ----
if (runDatabaseTests) {
  tryCatch(
    DatabaseConnector::renderTranslateExecuteSql(
      connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
      "DROP TABLE @cohort_database_schema.@cohort_table CASCADE",
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      progressBar = FALSE,
      reportOverallTime = FALSE
    ),
    error = function(e) {
      
    }
  )
  filesToDelete <-
    list.files(file.path(folder),
               full.names = TRUE,
               recursive = TRUE)
  invisible(lapply(filesToDelete, unlink, force = TRUE))
}

if (runDatabaseTests) {
  testthat::test_that("Run Cohort Diagnostics without instantiated cohorts", {
    
    # Diagnostics before instantiation ----
    ## no cohort table get cohort count ----
    testthat::expect_warning(
      CohortDiagnostics::getCohortCounts(
        connectionDetails = connectionDetails,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable
      )
    )
    
    ## no cohort table run diagnostics----
    testthat::expect_error(suppressWarnings(
      CohortDiagnostics::runCohortDiagnostics(
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = "eunomia",
        vocabularyDatabaseSchema = "eunomia",
        tempEmulationSchema = tempEmulationSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        packageName = "CohortDiagnostics",
        cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
        inclusionStatisticsFolder = file.path(folder, "incStats"),
        exportFolder =  file.path(folder, "export"),
        databaseId = "cdmV5",
        runInclusionStatistics = TRUE,
        runIncludedSourceConcepts = FALSE,
        runOrphanConcepts = FALSE,
        runVisitContext = TRUE,
        runBreakdownIndexEvents = FALSE,
        runIncidenceRate = FALSE,
        runCohortTimeSeries = FALSE,
        runDataSourceTimeSeries = FALSE,
        runCohortRelationship = TRUE,
        runCohortCharacterization = TRUE,
        runTemporalCohortCharacterization = FALSE,
        incremental = TRUE,
        incrementalFolder = file.path(folder, "incremental")
      )
    ))
    
    ## check creation of cohort table ----
    testthat::expect_null(
      CohortDiagnostics:::createCohortTable(
        connectionDetails = connectionDetails,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable
      )
    )
    
    ## with cohort table ----
    testthat::expect_null(suppressWarnings(
      CohortDiagnostics::runCohortDiagnostics(
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = "eunomia",
        vocabularyDatabaseSchema = "eunomia",
        tempEmulationSchema = tempEmulationSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        packageName = "CohortDiagnostics",
        cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
        inclusionStatisticsFolder = file.path(folder, "incStats"),
        exportFolder =  file.path(folder, "export"),
        databaseId = "cdmV5",
        runInclusionStatistics = TRUE,
        runIncludedSourceConcepts = FALSE,
        runOrphanConcepts = FALSE,
        runVisitContext = TRUE,
        runBreakdownIndexEvents = FALSE,
        runIncidenceRate = FALSE,
        runCohortTimeSeries = FALSE,
        runDataSourceTimeSeries = FALSE,
        runCohortRelationship = TRUE,
        runCohortCharacterization = TRUE,
        runTemporalCohortCharacterization = FALSE,
        incremental = TRUE,
        incrementalFolder = file.path(folder, "incremental")
      )
    ))
    
    # no inclusion folder specified
    testthat::expect_error(
      CohortDiagnostics::instantiateCohortSet(
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
        generateInclusionStats = TRUE,
        createCohortTable = TRUE,
        incremental = TRUE, 
        incrementalFolder = tempdir(),
        inclusionStatisticsFolder = NULL,
        baseUrl = baseUrl,
        cohortIds = -1
      )
    )
    
    # no incremental folder specified
    testthat::expect_error(
      CohortDiagnostics::instantiateCohortSet(
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
        generateInclusionStats = TRUE,
        createCohortTable = TRUE,
        incremental = TRUE, 
        incrementalFolder = NULL,
        inclusionStatisticsFolder = tempdir(),
        baseUrl = baseUrl,
        cohortIds = -1
      )
    )
    
    tryCatch(
      DatabaseConnector::renderTranslateExecuteSql(
        connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
        "DROP TABLE @cohort_database_schema.@cohort_table CASCADE",
        cohort_database_schema = cohortDatabaseSchema,
        cohort_table = cohortTable,
        progressBar = FALSE,
        reportOverallTime = FALSE
      ),
      error = function(e) {
        
      }
    )
  })
}


# Cohort Instantiation tests ----
test_that("Cohort instantiation", {
  skip_if_not(runDatabaseTests)
  
  ## No incremental mode ----
  ### Neg - no cohort table
  testthat::expect_error(suppressWarnings(
    CohortDiagnostics::instantiateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      oracleTempSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = 18348,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      generateInclusionStats = TRUE,
      createCohortTable = FALSE,
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  ))
  ### Neg - bad cohort ----
  testthat::expect_error(suppressWarnings(
    CohortDiagnostics::instantiateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = -1111,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      generateInclusionStats = TRUE,
      createCohortTable = TRUE,
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  ))
  ### Pos - good one cohort, will create cohort table, instantiate not incremental ----
  testthat::expect_null(
    CohortDiagnostics::instantiateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = 18348,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      generateInclusionStats = TRUE,
      createCohortTable = TRUE,
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  )
  
  ### Pos - Expect cohort count ----
  # Expect cohortId 18348 to have 830 records
  sqlCount <-
    "SELECT COUNT(*) FROM @cohort_database_schema.@cohort_table where cohort_definition_id = 18348;"
  count1 <-
    CohortDiagnostics:::renderTranslateQuerySql(
      connectionDetails = connectionDetails,
      sql = sqlCount,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable
    )
  testthat::expect_equal(count1$COUNT, 830)
  
  ### Pos - check cohort instantiated ----
  testthat::expect_true(
    CohortDiagnostics:::checkIfCohortInstantiated(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = 18348
    )
  )
  
  ### Neg - cohort is not instantiated ----
  testthat::expect_false(
    CohortDiagnostics:::checkIfCohortInstantiated(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = -1111
    )
  )
  
  
  ### Pos - should re run ----
  # delete from cohort table, and repopulate. should have 830 again
  sqlDelete <-
    "DELETE FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id = 18348 and subject_id < 1000;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = DatabaseConnector::connect(connectionDetails),
    sql = sqlDelete,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  count2 <-
    CohortDiagnostics:::renderTranslateQuerySql(
      connectionDetails = connectionDetails,
      sql = sqlCount,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable
    )
  testthat::expect_true(count1$COUNT > count2$COUNT)
  
  testthat::expect_null(
    CohortDiagnostics::instantiateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = 18348,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      generateInclusionStats = TRUE,
      createCohortTable = TRUE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental"),
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  )
  
  count3 <-
    CohortDiagnostics:::renderTranslateQuerySql(
      connectionDetails = connectionDetails,
      sql = sqlCount,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable
    )
  testthat::expect_gte(count3$COUNT, 830)
  
  ## Incremental mode ----
  testthat::expect_null(
    CohortDiagnostics::instantiateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      generateInclusionStats = TRUE,
      createCohortTable = TRUE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental"),
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  )
  
  connection <- DatabaseConnector::connect(connectionDetails)
  sql <- "SELECT COUNT(*) AS cohort_count, cohort_definition_id
  FROM @cohort_database_schema.@cohort_table
  GROUP BY cohort_definition_id;"
  counts <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    snakeCaseToCamelCase = TRUE
  )
  testthat::expect_gt(nrow(counts), 2)
  DatabaseConnector::disconnect(connection)
})




test_that("Testing Cohort diagnostics when not in incremental mode", {
  skip_if_not(runDatabaseTests)
  
  # Cohort Diagnostics -----
  ## Not incremental -----
  ### Neg - no connection or connection details -----
  testthat::expect_error(suppressWarnings(
    CohortDiagnostics::runCohortDiagnostics(
      connectionDetails = NULL,
      connection = NULL,
      cdmDatabaseSchema = "eunomia",
      vocabularyDatabaseSchema = "eunomia",
      oracleTempSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "cdmV5",
      databaseName = NULL,
      databaseDescription = NULL,
      runInclusionStatistics = FALSE,
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runVisitContext = FALSE,
      runBreakdownIndexEvents = FALSE,
      runIncidenceRate = FALSE,
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      runCohortRelationship = FALSE,
      runCohortCharacterization = FALSE,
      runTemporalCohortCharacterization = FALSE,
      incremental = FALSE,
      cohortIds = -11111,
      incrementalFolder = file.path(folder, "incremental")
    )
  ))
  ### Neg - bad cohort -----
  testthat::expect_error(suppressWarnings(
    CohortDiagnostics::runCohortDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "eunomia",
      vocabularyDatabaseSchema = "eunomia",
      oracleTempSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "cdmV5",
      databaseName = NULL,
      databaseDescription = NULL,
      runInclusionStatistics = FALSE,
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runVisitContext = FALSE,
      runBreakdownIndexEvents = FALSE,
      runIncidenceRate = FALSE,
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      runCohortRelationship = FALSE,
      runCohortCharacterization = FALSE,
      runTemporalCohortCharacterization = FALSE,
      incremental = FALSE,
      cohortIds = -11111,
      incrementalFolder = file.path(folder, "incremental")
    )
  ))
  
  ### Pos - one cohort -----
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runCohortDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "eunomia",
      vocabularyDatabaseSchema = "eunomia",
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "cdmV5",
      runInclusionStatistics = TRUE,
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = TRUE,
      runVisitContext = TRUE,
      runBreakdownIndexEvents = TRUE,
      runIncidenceRate = TRUE,
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      runCohortRelationship = TRUE,
      runCohortCharacterization = TRUE,
      runTemporalCohortCharacterization = FALSE,
      incremental = FALSE,
      cohortIds = 18348,
      incrementalFolder = file.path(folder, "incremental")
    )
  ))
  ### Pos - generate premerged file ----
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = file.path(folder, "export"))
  ))
  testthat::expect_true(file.exists(file.path(folder, "export", "PreMerged.RData")))
  unlink(file.path(folder, "export", "PreMerged.RData"))
})





test_that("Cohort diagnostics in incremental mode", {
  skip_if_not(runDatabaseTests)
  
  start <- Sys.time()
  ## Incremental -----
  ### Pos - incremental ----
  # run a subset of diagnostics and then rerun - check if second run took less time compared to first
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runCohortDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "eunomia",
      vocabularyDatabaseSchema = "eunomia",
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "cdmV5",
      runInclusionStatistics = TRUE,
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runVisitContext = TRUE,
      runBreakdownIndexEvents = FALSE,
      runIncidenceRate = FALSE,
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      runCohortRelationship = TRUE,
      runCohortCharacterization = TRUE,
      runTemporalCohortCharacterization = FALSE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental")
    )
  ))
  timeToRunFirstTime <- Sys.time() - start
  
  testthat::expect_true(file.exists(file.path(folder, "export", "Results_CDMv5.zip")))
  
  start <- Sys.time()
  # while in incremental mode: nothing should run during second run, so should be faster than first run
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runCohortDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "eunomia",
      vocabularyDatabaseSchema = "eunomia",
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "cdmV5",
      runInclusionStatistics = TRUE,
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runVisitContext = TRUE,
      runBreakdownIndexEvents = FALSE,
      runIncidenceRate = FALSE,
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      runCohortRelationship = TRUE,
      runCohortCharacterization = TRUE,
      runTemporalCohortCharacterization = FALSE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental")
    )
  ))
  #because second run is faster than first run - it should take less time
  timeToRunSecondTime <- Sys.time() - start
  testthat::expect_true(timeToRunFirstTime > timeToRunSecondTime)
  
  ### rest of diagnostics ----
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runCohortDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = "eunomia",
      vocabularyDatabaseSchema = "eunomia",
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      packageName = "CohortDiagnostics",
      cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
      inclusionStatisticsFolder = file.path(folder, "incStats"),
      exportFolder =  file.path(folder, "export"),
      databaseId = "cdmV5",
      runInclusionStatistics = TRUE,
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = TRUE,
      runVisitContext = TRUE,
      runBreakdownIndexEvents = TRUE,
      runIncidenceRate = TRUE,
      runCohortTimeSeries = TRUE,
      runDataSourceTimeSeries = TRUE,
      runCohortRelationship = TRUE,
      runCohortCharacterization = TRUE,
      runTemporalCohortCharacterization = TRUE,
      incremental = TRUE,
      incrementalFolder = file.path(folder, "incremental")
    )
  ))
  
  ## Premerge file ----
  ### Neg - test - no zip file ----
  testthat::expect_error(CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = file.path(tempdir(), 'random')))
  
  ### Pos - generate premerged file ----
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = file.path(folder, "export"))
  ))
  testthat::expect_true(file.exists(file.path(folder, "export", "PreMerged.RData")))
})




test_that("Negative tests on individual functions", {
  skip_if_not(runDatabaseTests)
  
  # Neg - Individual Function ----
  ## Characterization  ----
  testthat::expect_null(
    CohortDiagnostics::runCohortCharacterizationDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = -1111
    )
  )
  testthat::expect_null(
    CohortDiagnostics::runCohortCharacterizationDiagnostics(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable
    )
  )
  ## Cohort relationship  ----
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runCohortRelationshipDiagnostics(
      connectionDetails = connectionDetails,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      targetCohortIds = -1111,
      comparatorCohortIds = -1111
    )
  ))
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runCohortRelationshipDiagnostics(
      connectionDetails = connectionDetails,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      targetCohortIds = c(18348),
      comparatorCohortIds = -1111
    )
  ))
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runCohortRelationshipDiagnostics(
      connectionDetails = connectionDetails,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      targetCohortIds = NULL,
      comparatorCohortIds = NULL
    )
  ))
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runCohortRelationshipDiagnostics(
      connectionDetails = connectionDetails,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      targetCohortIds = 18348,
      comparatorCohortIds = NULL
    )
  ))
  ## Visit Context diagnostics  ----
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runVisitContextDiagnostics(
      connectionDetails = connectionDetails,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = cdmDatabaseSchema,
      cdmVersion = 6,
      cohortIds = -1111
    )
  ))
  ## Time Series diagnostics  ----
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runCohortTimeSeriesDiagnostics(
      connectionDetails = connectionDetails,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE,
      cohortIds = -1111
    )
  ))
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runCohortTimeSeriesDiagnostics(
      connectionDetails = connectionDetails,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      runCohortTimeSeries = TRUE,
      runDataSourceTimeSeries = FALSE,
      cohortIds = -1111
    )
  ))
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::runIncidenceRateDiagnostics(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      vocabularyDatabaseSchema = cohortDatabaseSchema,
      cohortId = -1111
    )
  ))
})


# Upload Results test ----
####################### upload to database and test
test_that("Create and upload results to results data model", {
  skip_if_not(runDatabaseTests)
  
  testthat::expect_null(
    CohortDiagnostics::createResultsDataModel(connectionDetails = connectionDetails,
                                              schema = cohortDiagnosticsSchema)
  )
  
  listOfZipFilesToUpload <-
    list.files(
      path = file.path(folder, "export"),
      pattern = ".zip",
      full.names = TRUE,
      recursive = TRUE
    )
  testthat::expect_true(length(listOfZipFilesToUpload) >= 0)
  
  for (i in (1:length(listOfZipFilesToUpload))) {
    testthat::expect_null(suppressWarnings(
      CohortDiagnostics::uploadResults(
        connectionDetails = connectionDetails,
        schema = cohortDiagnosticsSchema,
        zipFileName = listOfZipFilesToUpload[[i]]
      )
    ))
  }
})




test_that("Data Retrieval", {
  skip_if_not(runDatabaseTests)
  
  # Data Retrieval Premerged file ----
  dataSourcePreMergedFile <-
    CohortDiagnostics::createFileDataSource(premergedDataFile = file.path(folder, "export", "PreMerged.RData"))
  
  ## Cohort Count ----
  #### Pos ----
  cohortCountFromFile <-
    CohortDiagnostics::getResultsFromCohortCount(
      dataSource = dataSourcePreMergedFile,
      cohortIds = c(
        14906,
        14907,
        14909,
        17492,
        17493,
        18342,
        18345,
        18346,
        18347,
        18348,
        18349,
        18350,
        21402
      ),
      databaseIds = 'cdmV5'
    )
  testthat::expect_true(nrow(cohortCountFromFile) > 0)
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromCohortCount(
      dataSource = dataSourcePreMergedFile,
      cohortIds = c(-11111),
      databaseIds = 'cdmV5'
    )
  )
  
  ## Time series ----
  #### Pos ----
  timeSeriesFromFile <-
    CohortDiagnostics::getResultsFromFixedTimeSeries(
      dataSource = dataSourcePreMergedFile,
      cohortIds = c(17492, 17692),
      databaseIds = 'cdmV5'
    )
  testthat::expect_true(any(
    is.null(timeSeriesFromFile),
    length(timeSeriesFromFile) >= 0
  ))
  timeSeriesFromFile2 <-
    CohortDiagnostics::getResultsFromFixedTimeSeries(dataSource = dataSourcePreMergedFile,
                                                     databaseIds = 'cdmV5')
  testthat::expect_true(any(
    is.null(timeSeriesFromFile2),
    length(timeSeriesFromFile2) >= 0
  ))
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromFixedTimeSeries(
      dataSource = dataSourcePreMergedFile,
      cohortIds = -11111,
      databaseIds = 'cdmV5d'
    )
  )
  
  ## Time Distribution ----
  #### Pos ----
  timeDistributionFromFile <-
    CohortDiagnostics::getResultsFromTimeDistribution(
      dataSource = dataSourcePreMergedFile,
      cohortIds = c(17492, 17692),
      databaseIds = 'cdmV5'
    )
  testthat::expect_true(nrow(timeDistributionFromFile) >= 0)
  #### Pos ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromTimeDistribution(
      dataSource = dataSourcePreMergedFile,
      cohortIds = -11111,
      databaseIds = 'cdmV5d'
    )
  )
  
  ## Incidence rate ----
  #### Pos ----
  incidenceRateFromFile <-
    CohortDiagnostics::getResultsFromIncidenceRate(
      dataSource = dataSourcePreMergedFile,
      cohortIds = c(17492, 17692),
      databaseIds = 'cdmV5'
    )
  testthat::expect_true(any(
    is.null(incidenceRateFromFile),
    nrow(incidenceRateFromFile) >= 0
  )) # no data in eunomia
  incidenceRateFromFile2 <-
    CohortDiagnostics::getResultsFromIncidenceRate(dataSource = dataSourcePreMergedFile,
                                                   databaseIds = 'cdmV5')
  testthat::expect_true(any(
    is.null(incidenceRateFromFile2),
    nrow(incidenceRateFromFile2) >= 0
  )) # no data in eunomia
  incidenceRateFromFile3 <-
    CohortDiagnostics::getResultsFromIncidenceRate(dataSource = dataSourcePreMergedFile,
                                                   databaseIds = 'cdmV5')
  testthat::expect_true(any(
    is.null(incidenceRateFromFile3),
    nrow(incidenceRateFromFile3) >= 0
  )) # no data in eunomia
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromIncidenceRate(
      dataSource = dataSourcePreMergedFile,
      cohortIds = -11111,
      databaseIds = 'cdmV5'
    )
  )
  
  ## Inclusion rules ----
  #### Pos ----
  inclusionRulesFromFile <-
    CohortDiagnostics::getResultsFromInclusionRuleStatistics(dataSource = dataSourcePreMergedFile,
                                                             cohortIds = 18350,
                                                             databaseIds = 'cdmV5')
  testthat::expect_true(nrow(inclusionRulesFromFile) >= 0)
  inclusionRulesFromFile2 <-
    CohortDiagnostics::getResultsFromInclusionRuleStatistics(dataSource = dataSourcePreMergedFile)
  testthat::expect_true(nrow(inclusionRulesFromFile2) >= 0)
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromInclusionRuleStatistics(
      dataSource = dataSourcePreMergedFile,
      cohortIds = -1111,
      databaseIds = 'cdmV5'
    )
  )
  
  ## Index event breakdown ----
  #### Pos ----
  indexEventBreakdownFromFile <-
    CohortDiagnostics::getResultsFromIndexEventBreakdown(dataSource = dataSourcePreMergedFile,
                                                         cohortIds = 18348,
                                                         databaseIds = 'cdmV5')
  testthat::expect_true(nrow(indexEventBreakdownFromFile) >= 0)
  indexEventBreakdownFromFile2 <-
    CohortDiagnostics::getResultsFromIndexEventBreakdown(dataSource = dataSourcePreMergedFile)
  testthat::expect_true(nrow(indexEventBreakdownFromFile2) >= 0)
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromIndexEventBreakdown(
      dataSource = dataSourcePreMergedFile,
      cohortIds = -1111,
      databaseIds = 'cdmV5'
    )
  )
  
  ## Visit context ----
  #### Pos ----
  visitContextFromFile <-
    CohortDiagnostics::getResultsFromVisitContext(dataSource = dataSourcePreMergedFile,
                                                  cohortIds = 18348,
                                                  databaseIds = 'cdmV5')
  testthat::expect_true(nrow(visitContextFromFile) >= 0)
  visitContextFromFile2 <-
    CohortDiagnostics::getResultsFromVisitContext(dataSource = dataSourcePreMergedFile)
  testthat::expect_true(nrow(visitContextFromFile2) >= 0)
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromVisitContext(
      dataSource = dataSourcePreMergedFile,
      cohortIds = -1111,
      databaseIds = 'cdmV5'
    )
  )
  
  ## Orphan concept ----
  #### Pos ----
  # orphanConceptFromFile <-
  #   CohortDiagnostics::getResultsFromOrphanConcept(dataSource = dataSourcePreMergedFile,
  #                                                  cohortIds = 18348,
  #                                                  databaseIds = 'cdmV5')
  # testthat::expect_true(nrow(orphanConceptFromFile) >= 0)
  # orphanConceptFromFile2 <-
  #   CohortDiagnostics::getResultsFromOrphanConcept(dataSource = dataSourcePreMergedFile)
  # testthat::expect_true(nrow(orphanConceptFromFile2) >= 0)
  
  ## Concept id details ----
  #### Pos ----
  conceptIdDetailsFromFile <-
    CohortDiagnostics::getResultsFromConcept(dataSource = dataSourcePreMergedFile,
                                             conceptIds = c(192671, 201826, 1124300, 1124300))
  testthat::expect_true(nrow(conceptIdDetailsFromFile) >= 0)
  # should throw warning
  conceptIdDetailsFromFile2 <-
    suppressWarnings(
      CohortDiagnostics::getResultsFromConcept(
        dataSource = dataSourcePreMergedFile,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        conceptIds = c(192671, 201826, 1124300, 1124300)
      )
    )
  testthat::expect_true(nrow(conceptIdDetailsFromFile2) >= 0)
  #### Neg ----
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::getResultsFromConcept(
      dataSource = dataSourcePreMergedFile,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      conceptIds = c(-1111)
    )
  ))
  
   ## Cohort Relationship ----
  #### Pos ----
  cohortRelationshipsFromFile <-
    CohortDiagnostics::getResultsFromCohortRelationships(dataSource = dataSourcePreMergedFile,
                                                         cohortIds = 18348,
                                                         databaseIds = 'cdmV5')
  testthat::expect_true(nrow(cohortRelationshipsFromFile) >= 0)
  cohortRelationshipsFromFile2 <-
    CohortDiagnostics::getResultsFromCohortRelationships(dataSource = dataSourcePreMergedFile)
  testthat::expect_true(nrow(cohortRelationshipsFromFile2) >= 0)
  #### Neg ----
  testthat::expect_null(
    conceptIdDetails <-
      suppressWarnings(
        CohortDiagnostics::getResultsFromCohortRelationships(
          dataSource = dataSourcePreMergedFile,
          cohortIds = -1111,
          databaseIds = 'cdmV5'
        )
      )
  )
  
  
  #### Pos ----
  # Table does not exist in results, so this is throwing an error
  cohortCharacterizationResultsFromFile <-
    CohortDiagnostics::getMultipleCharacterizationResults(dataSource = dataSourcePreMergedFile,
                                                          cohortIds = 18348,
                                                          databaseIds = 'cdmV5')
  testthat::expect_true(length(cohortCharacterizationResultsFromFile) >= 0)
  testthat::expect_true(nrow(cohortCharacterizationResultsFromFile$analysisRef) > 0)
  testthat::expect_true(nrow(cohortCharacterizationResultsFromFile$covariateValue) > 0)
  cohortCharacterizationResultsFromFile2 <-
    CohortDiagnostics::getMultipleCharacterizationResults(dataSource = dataSourcePreMergedFile)
  testthat::expect_true(length(cohortCharacterizationResultsFromFile2) >= 0)
  testthat::expect_true(nrow(cohortCharacterizationResultsFromFile2$analysisRef) > 0)
  testthat::expect_true(nrow(cohortCharacterizationResultsFromFile2$covariateValue) > 0)
  #### Neg ----
  cohortCharacterizationResultsFromFile3 <-
    CohortDiagnostics::getMultipleCharacterizationResults(
      dataSource = dataSourcePreMergedFile,
      cohortIds = -1111,
      databaseIds = 'cdmV5'
    )
  testthat::expect_true(nrow(cohortCharacterizationResultsFromFile3$analysisRef) > 0)
  testthat::expect_null(cohortCharacterizationResultsFromFile3$covariateValue)
  
  
  #### Pos ----
  cohortOverlapDataFromFile <-
    CohortDiagnostics::getCohortOverlapData(
      dataSource = dataSourcePreMergedFile,
      cohortIds = c(18348, 18350),
      databaseIds = 'cdmV5'
    )
  testthat::expect_true(nrow(cohortOverlapDataFromFile) >= 0)
  #### Neg ----
  testthat::expect_null(conceptIdDetails <-
                          suppressWarnings(
                            CohortDiagnostics::getCohortOverlapData(
                              dataSource = dataSourcePreMergedFile,
                              cohortIds = -1111,
                              databaseIds = 'cdmV5'
                            )
                          ))
  
  
  
  
  # Data Retrieval Database mode ----
  
  skip_if_not(runDatabaseTests)
  
  # ## Connection pool----
  # connectionPool <- pool::dbPool(
  #   drv = jdbcDriverFolder,
  #   dbms = connectionDetails$dbms,
  #   server = connectionDetails$server(),
  #   port = 5432,
  #   user = connectionDetails$user(),
  #   password = connectionDetails$password()
  # )
  # dataSourceDatabase <- CohortDiagnostics::createDatabaseDataSource(
  #   connectionDetails = NULL,
  #   connection = connectionPool,
  #   resultsDatabaseSchema = cohortDiagnosticsSchema,
  #   vocabularyDatabaseSchema = cohortDiagnosticsSchema
  # )
  # # get data on instantiated cohorts usin dbpool connection
  # testthat::expect_true(nrow(
  #   CohortDiagnostics::getResultsFromCohortCount(dataSource = dataSourceDatabase)
  # ) >= 0)
  # pool::poolClose(connectionPool)
  # dataSourceDatabase <- NULL
  
  ## non pool connection -----
  if (!exists('connection')) {
    connection <- DatabaseConnector::connect(connectionDetails)
  }
  if (!DatabaseConnector::dbIsValid(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
  }
  dataSourceDatabase <- CohortDiagnostics::createDatabaseDataSource(
    connectionDetails = connectionDetails,
    connection = connection,
    resultsDatabaseSchema = cohortDiagnosticsSchema,
    vocabularyDatabaseSchema = cohortDiagnosticsSchema
  )
  
  ## Cohort Count ----
  #### Pos ----
  cohortCountFromDb <- CohortDiagnostics::getResultsFromCohortCount(
    dataSource = dataSourceDatabase,
    cohortIds = c(
      14906,
      14907,
      14909,
      17492,
      17493,
      18342,
      18345,
      18346,
      18347,
      18348,
      18349,
      18350,
      21402
    ),
    databaseIds = 'cdmV5'
  )
  testthat::expect_true(nrow(cohortCountFromDb) > 0)
  testthat::expect_true(dplyr::all_equal(cohortCountFromDb, cohortCountFromFile))
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromCohortCount(
      dataSource = dataSourceDatabase,
      cohortIds = c(-11111),
      databaseIds = 'cdmV5'
    )
  )
  
  ## Time series ----
  #### Pos ----
  timeSeriesFromDb <-
    CohortDiagnostics::getResultsFromFixedTimeSeries(
      dataSource = dataSourceDatabase,
      cohortIds = c(17492, 17692),
      databaseIds = 'cdmV5'
    )
  testthat::expect_true(length(timeSeriesFromDb) >= 0)
  testthat::expect_true(nrow(timeSeriesFromDb$m) >= 0)
  testthat::expect_true(dplyr::all_equal(timeSeriesFromDb$m, timeSeriesFromFile$m))
  testthat::expect_true(dplyr::all_equal(timeSeriesFromDb$y, timeSeriesFromFile$y))
  testthat::expect_true(dplyr::all_equal(timeSeriesFromDb$q, timeSeriesFromFile$q))
  timeSeriesFromDb2 <-
    CohortDiagnostics::getResultsFromFixedTimeSeries(dataSource = dataSourceDatabase,
                                                     databaseIds = 'cdmV5')
  testthat::expect_true(length(timeSeriesFromDb2) >= 0)
  testthat::expect_true(dplyr::all_equal(timeSeriesFromDb2$m, timeSeriesFromFile2$m))
  testthat::expect_true(dplyr::all_equal(timeSeriesFromDb2$y, timeSeriesFromFile2$y))
  testthat::expect_true(dplyr::all_equal(timeSeriesFromDb2$q, timeSeriesFromFile2$q))
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromFixedTimeSeries(
      dataSource = dataSourceDatabase,
      cohortIds = -11111,
      databaseIds = 'cdmV5d'
    )
  )
  
  ## Time Distribution ----
  #### Pos ----
  timeDistributionFromDb <-
    CohortDiagnostics::getResultsFromTimeDistribution(
      dataSource = dataSourceDatabase,
      cohortIds = c(17492, 17692),
      databaseIds = 'cdmV5'
    )
  testthat::expect_true(nrow(timeDistributionFromDb) >= 0)
  testthat::expect_true(dplyr::all_equal(timeDistributionFromDb, timeDistributionFromFile))
  #### Pos ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromTimeDistribution(
      dataSource = dataSourceDatabase,
      cohortIds = -11111,
      databaseIds = 'cdmV5d'
    )
  )
  
  ## Incidence rate ----
  #### Pos ----
  incidenceRateFromDb <-
    CohortDiagnostics::getResultsFromIncidenceRate(
      dataSource = dataSourceDatabase,
      cohortIds = c(17492, 17692),
      databaseIds = 'cdmV5'
    )
  testthat::expect_true(any(
    is.null(incidenceRateFromDb),
    nrow(incidenceRateFromDb) >= 0
  )) # no data in eunomia
  # so expecting NULL
  testthat::expect_true(is.null(incidenceRateFromDb) &&
                          is.null(incidenceRateFromFile))
  incidenceRateFromDb2 <-
    CohortDiagnostics::getResultsFromIncidenceRate(dataSource = dataSourceDatabase,
                                                   databaseIds = 'cdmV5')
  testthat::expect_true(any(
    is.null(incidenceRateFromDb2),
    nrow(incidenceRateFromDb2) >= 0
  )) # no data in eunomia
  testthat::expect_true(is.null(incidenceRateFromDb2) &&
                          is.null(incidenceRateFromFile2))
  
  incidenceRateFromDb3 <-
    CohortDiagnostics::getResultsFromIncidenceRate(dataSource = dataSourceDatabase,
                                                   databaseIds = 'cdmV5')
  testthat::expect_true(any(
    is.null(incidenceRateFromDb3),
    nrow(incidenceRateFromDb3) >= 0
  )) # no data in eunomia
  testthat::expect_true(is.null(incidenceRateFromDb3) &&
                          is.null(incidenceRateFromFile3))
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromIncidenceRate(
      dataSource = dataSourceDatabase,
      cohortIds = -11111,
      databaseIds = 'cdmV5'
    )
  )
  
  ## Inclusion rules ----
  #### Pos ----
  inclusionRulesFromDb <-
    CohortDiagnostics::getResultsFromInclusionRuleStatistics(dataSource = dataSourceDatabase,
                                                             cohortIds = 18350,
                                                             databaseIds = 'cdmV5')
  testthat::expect_true(nrow(inclusionRulesFromDb) >= 0)
  testthat::expect_true(dplyr::all_equal(inclusionRulesFromDb, inclusionRulesFromFile))
  inclusionRulesFromDb2 <-
    CohortDiagnostics::getResultsFromInclusionRuleStatistics(dataSource = dataSourceDatabase)
  testthat::expect_true(nrow(inclusionRulesFromDb2) >= 0)
  testthat::expect_true(dplyr::all_equal(inclusionRulesFromDb2, inclusionRulesFromFile2))
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromInclusionRuleStatistics(
      dataSource = dataSourceDatabase,
      cohortIds = -1111,
      databaseIds = 'cdmV5'
    )
  )
  
  ## Index event breakdown ----
  #### Pos ----
  indexEventBreakdownFromDb <-
    CohortDiagnostics::getResultsFromIndexEventBreakdown(dataSource = dataSourceDatabase,
                                                         cohortIds = 18348,
                                                         databaseIds = 'cdmV5')
  testthat::expect_true(nrow(indexEventBreakdownFromDb) >= 0)
  testthat::expect_true(dplyr::all_equal(indexEventBreakdownFromDb, indexEventBreakdownFromFile))
  indexEventBreakdownFromDb2 <-
    CohortDiagnostics::getResultsFromIndexEventBreakdown(dataSource = dataSourceDatabase)
  testthat::expect_true(nrow(indexEventBreakdownFromDb2) >= 0)
  testthat::expect_true(dplyr::all_equal(indexEventBreakdownFromDb2, indexEventBreakdownFromFile2))
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromIndexEventBreakdown(
      dataSource = dataSourceDatabase,
      cohortIds = -1111,
      databaseIds = 'cdmV5'
    )
  )
  
  ## Visit context ----
  #### Pos ----
  visitContextFromDb <-
    CohortDiagnostics::getResultsFromVisitContext(dataSource = dataSourceDatabase,
                                                  cohortIds = 18348,
                                                  databaseIds = 'cdmV5')
  testthat::expect_true(nrow(visitContextFromDb) >= 0)
  testthat::expect_true(dplyr::all_equal(visitContextFromDb, visitContextFromFile))
  visitContextFromDb2 <-
    CohortDiagnostics::getResultsFromVisitContext(dataSource = dataSourceDatabase)
  testthat::expect_true(nrow(visitContextFromDb2) >= 0)
  testthat::expect_true(dplyr::all_equal(visitContextFromDb2, visitContextFromFile2))
  #### Neg ----
  testthat::expect_null(
    CohortDiagnostics::getResultsFromVisitContext(
      dataSource = dataSourceDatabase,
      cohortIds = -1111,
      databaseIds = 'cdmV5'
    )
  )
  
  ## Orphan concept ----
  #### Pos ----
  # orphanConceptFromDb <-
  #   CohortDiagnostics::getResultsFromOrphanConcept(dataSource = dataSourceDatabase,
  #                                                  cohortIds = 18348,
  #                                                  databaseIds = 'cdmV5')
  # testthat::expect_true(nrow(orphanConceptFromDb) >= 0)
  # testthat::expect_true(dplyr::all_equal(orphanConceptFromDb, orphanConceptFromFile))
  # orphanConceptFromDb2 <-
  #   CohortDiagnostics::getResultsFromOrphanConcept(dataSource = dataSourceDatabase)
  # testthat::expect_true(nrow(orphanConceptFromDb2) >= 0)
  # testthat::expect_true(dplyr::all_equal(orphanConceptFromDb2, orphanConceptFromFile2))
  # 
  ## Concept id details ----
  #### Pos ----
  conceptIdDetailsFromDb <-
    CohortDiagnostics::getResultsFromConcept(dataSource = dataSourceDatabase,
                                             conceptIds = c(192671, 201826, 1124300, 1124300))
  testthat::expect_true(nrow(conceptIdDetailsFromDb) >= 0)
  # testthat::expect_true(dplyr::all_equal(conceptIdDetailsFromDb, conceptIdDetailsFromFile))
  # should throw warning
  conceptIdDetailsFromDb2 <-
    suppressWarnings(
      CohortDiagnostics::getResultsFromConcept(
        dataSource = dataSourceDatabase,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        conceptIds = c(192671, 201826, 1124300, 1124300)
      )
    )
  testthat::expect_true(nrow(conceptIdDetailsFromDb2) >= 0)
  #!!!!!!!!!!!!!!! BUG -- NA is not handled in same way
  # testthat::expect_true(dplyr::all_equal(conceptIdDetailsFromDb2, conceptIdDetailsFromFile2))
  #### Neg ----
  testthat::expect_null(suppressWarnings(
    CohortDiagnostics::getResultsFromConcept(
      dataSource = dataSourceDatabase,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      conceptIds = c(-1111)
    )
  ))
  

  ## Cohort Relationship ----
  #### Pos ----
  cohortRelationshipsFromDb <-
    CohortDiagnostics::getResultsFromCohortRelationships(dataSource = dataSourceDatabase,
                                                         cohortIds = 18348,
                                                         databaseIds = 'cdmV5')
  testthat::expect_true(nrow(cohortRelationshipsFromDb) >= 0)
  testthat::expect_true(dplyr::all_equal(cohortRelationshipsFromDb, cohortRelationshipsFromFile))
  cohortRelationshipsFromDb2 <-
    CohortDiagnostics::getResultsFromCohortRelationships(dataSource = dataSourceDatabase)
  testthat::expect_true(nrow(cohortRelationshipsFromDb2) >= 0)
  testthat::expect_true(dplyr::all_equal(cohortRelationshipsFromDb2, cohortRelationshipsFromFile2))
  #### Neg ----
  testthat::expect_null(
    conceptIdDetails <-
      suppressWarnings(
        CohortDiagnostics::getResultsFromCohortRelationships(
          dataSource = dataSourceDatabase,
          cohortIds = -1111,
          databaseIds = 'cdmV5'
        )
      )
  )
  
  ## Cohort Characterization results ----
  #### Pos ----
  # Table does not exist in results, so this is throwing an error
  cohortCharacterizationResultsFromDb <-
    CohortDiagnostics::getMultipleCharacterizationResults(dataSource = dataSourceDatabase,
                                                          cohortIds = 18348,
                                                          databaseIds = 'cdmV5')
  testthat::expect_true(length(cohortCharacterizationResultsFromDb) >= 0)
  testthat::expect_true(nrow(cohortCharacterizationResultsFromDb$analysisRef) > 0)
  testthat::expect_true(nrow(cohortCharacterizationResultsFromDb$covariateValue) > 0)
  #!!!!!!!!!!!!!!! BUG
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb$analysisRef,
  #                        expected = cohortCharacterizationResultsFromFile$analysisRef)
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb$covariateRef,
  #                        expected = cohortCharacterizationResultsFromFile$covariateRef)
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb$covariateValue,
  #                        expected = cohortCharacterizationResultsFromFile$covariateValue)
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb$covariateValueDist,
  #                        expected = cohortCharacterizationResultsFromFile$covariateValueDist)
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb$concept,
  #                        expected = cohortCharacterizationResultsFromFile$concept)
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb$temporalTimeRef,
  #                        expected = cohortCharacterizationResultsFromFile$temporalTimeRef)
  cohortCharacterizationResultsFromDb2 <-
    CohortDiagnostics::getMultipleCharacterizationResults(dataSource = dataSourceDatabase)
  testthat::expect_true(length(cohortCharacterizationResultsFromDb2) >= 0)
  testthat::expect_true(nrow(cohortCharacterizationResultsFromDb2$analysisRef) > 0)
  testthat::expect_true(nrow(cohortCharacterizationResultsFromDb2$covariateValue) > 0)
  #!!!!!!!!!!!!!!! BUG
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb2$analysisRef,
  #                        expected = cohortCharacterizationResultsFromFile2$analysisRef)
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb2$covariateRef,
  #                        expected = cohortCharacterizationResultsFromFile2$covariateRef)
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb2$covariateValue,
  #                        expected = cohortCharacterizationResultsFromFile2$covariateValue)
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb2$covariateValueDist,
  #                        expected = cohortCharacterizationResultsFromFile2$covariateValueDist)
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb2$concept,
  #                        expected = cohortCharacterizationResultsFromFile2$concept)
  # testthat::expect_equal(object = cohortCharacterizationResultsFromDb2$temporalTimeRef,
  #                        expected = cohortCharacterizationResultsFromFile2$temporalTimeRef)
  #### Neg ----
  cohortCharacterizationResultsFromDb3 <-
    CohortDiagnostics::getMultipleCharacterizationResults(
      dataSource = dataSourceDatabase,
      cohortIds = -1111,
      databaseIds = 'cdmV5'
    )
  testthat::expect_true(nrow(cohortCharacterizationResultsFromDb3$analysisRef) > 0)
  testthat::expect_null(cohortCharacterizationResultsFromDb3$covariateValue)
  
  ## Cohort Overlap ----
  #### Pos ----
  cohortOverlapDataFromDb <-
    CohortDiagnostics::getCohortOverlapData(
      dataSource = dataSourceDatabase,
      cohortIds = c(18348, 18350),
      databaseIds = 'cdmV5'
    )
  testthat::expect_true(nrow(cohortOverlapDataFromDb) >= 0)
  testthat::expect_equal(object = cohortOverlapDataFromDb, expected = cohortOverlapDataFromFile)
  #### Neg ----
  testthat::expect_null(conceptIdDetails <-
                          suppressWarnings(
                            CohortDiagnostics::getCohortOverlapData(
                              dataSource = dataSourceDatabase,
                              cohortIds = -1111,
                              databaseIds = 'cdmV5'
                            )
                          ))
  
})




test_that("Data removal works", {
  skip_if_not(runDatabaseTests)
  
  specifications <-
    CohortDiagnostics::getResultsDataModelSpecifications(packageName = 'CohortDiagnostics')
  connection <- DatabaseConnector::connect(connectionDetails)
  
  dataSourceDatabase <- CohortDiagnostics::createDatabaseDataSource(
    connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
    resultsDatabaseSchema = cohortDiagnosticsSchema
  )
  cohortTableDataBeforeDelete <-
    CohortDiagnostics::getResultsFromCohortCount(dataSource = dataSourceDatabase,
                                                 databaseIds = 'cdmV5')
  
  if (!is.null(cohortTableDataBeforeDelete)) {
    colnames(cohortTableDataBeforeDelete) <-
      CohortDiagnostics:::camelCaseToSnakeCase(colnames(cohortTableDataBeforeDelete))
    
    # delete some selected records
    CohortDiagnostics:::deleteFromServer(
      connection = connection,
      schema = cohortDiagnosticsSchema,
      tableName = 'cohort_count',
      keyValues = cohortTableDataBeforeDelete[1,]
    )
    cohortTableDataAfterDelete <-
      CohortDiagnostics::getResultsFromCohortCount(dataSource = dataSourceDatabase)
    
    testthat::expect_true(nrow(cohortTableDataBeforeDelete) >
                            nrow(cohortTableDataAfterDelete))
  }
  
  
  for (tableName in unique(specifications$tableName)) {
    primaryKey <- specifications %>%
      dplyr::filter(.data$tableName == !!tableName &
                      .data$primaryKey == "Yes") %>%
      dplyr::select(.data$fieldName) %>%
      dplyr::pull()
    
    if ("database_id" %in% primaryKey) {
      CohortDiagnostics:::deleteAllRecordsForDatabaseId(
        connection = connection,
        schema = cohortDiagnosticsSchema,
        tableName = tableName,
        databaseId = "cdmV5"
      )
      
      sql <-
        "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
      sql <- SqlRender::render(
        sql = sql,
        schema = cohortDiagnosticsSchema,
        table_name = tableName,
        database_id = "cdmV5"
      )
      databaseIdCount <-
        DatabaseConnector::querySql(connection, sql)[, 1]
      expect_true(databaseIdCount == 0)
    }
  }
  DatabaseConnector::disconnect(connection)
})
