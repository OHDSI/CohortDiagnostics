test_that("Execute and export characterization", {
  skip_if(skipCdmTests, "cdm settings not configured")
  tConnection <-
    DatabaseConnector::connect(connectionDetails)

  with_dbc_connection(tConnection, {
    exportFolder <- tempfile()
    recordKeepingFile <- tempfile(fileext="csv")
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

    on.exit({
      CohortGenerator::dropCohortStatsTables(connection = tConnection,
                                             cohortDatabaseSchema = cohortDatabaseSchema,
                                             cohortTableNames = cohortTableNames)

      DatabaseConnector::renderTranslateExecuteSql(tConnection,
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

    # Required for function use
    cohortCounts <- computeCohortCounts(
      connection = tConnection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohorts = cohortDefinitionSet,
      exportFolder = exportFolder,
      minCellCount = 5,
      databaseId = "Testdb"
    )
    checkmate::expect_file_exists(file.path(exportFolder, "cohort_count.csv"))

    executeCohortCharacterization(connection = tConnection,
                                  databaseId = "Testdb",
                                  exportFolder = exportFolder,
                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                  cohortDatabaseSchema = cohortDatabaseSchema,
                                  cohortTable = cohortTable,
                                  covariateSettings = temporalCovariateSettings,
                                  tempEmulationSchema = tempEmulationSchema,
                                  cdmVersion = 5,
                                  cohorts = cohortDefinitionSet[1:3,],
                                  cohortCounts = cohortCounts,
                                  minCellCount = 5,
                                  instantiatedCohorts = cohortDefinitionSet$cohortId,
                                  incremental = TRUE,
                                  recordKeepingFile = recordKeepingFile,
                                  task = "runTemporalCohortCharacterization",
                                  jobName = "Temporal Cohort characterization")
    
    # Check all files are created
    checkmate::expect_file_exists(file.path(exportFolder, "temporal_covariate_ref.csv"))
    checkmate::expect_file_exists(file.path(exportFolder, "temporal_analysis_ref.csv"))
    checkmate::expect_file_exists(file.path(exportFolder, "temporal_covariate_value.csv"))
    checkmate::expect_file_exists(file.path(exportFolder, "temporal_covariate_value_dist.csv"))
    checkmate::expect_file_exists(file.path(exportFolder, "temporal_time_ref.csv"))
    
    recordKeepingFileData <- readr::read_csv(file = recordKeepingFile, col_types = readr::cols())
    testthat::expect_equal(object = nrow(recordKeepingFileData), expected = 3)
    
    # check if subset works
    subset <- subsetToRequiredCohorts(
      cohorts = cohortDefinitionSet,
      task = "runTemporalCohortCharacterization",
      incremental = TRUE,
      recordKeepingFile = recordKeepingFile
    )
    
    #should not have the cohorts that were previously run
    testthat::expect_equal(object = nrow(subset %>%
                                           dplyr::filter(
                                             .data$cohortId %in% c(cohortDefinitionSet[1:3, ]$cohortId)
                                           )),
                           expected = 0)
    
    # finish the rest of characterization
    executeCohortCharacterization(connection = tConnection,
                                  databaseId = "Testdb",
                                  exportFolder = exportFolder,
                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                  cohortDatabaseSchema = cohortDatabaseSchema,
                                  cohortTable = cohortTable,
                                  covariateSettings = temporalCovariateSettings,
                                  tempEmulationSchema = tempEmulationSchema,
                                  cdmVersion = 5,
                                  cohorts = cohortDefinitionSet,
                                  cohortCounts = cohortCounts,
                                  minCellCount = 5,
                                  instantiatedCohorts = cohortDefinitionSet$cohortId,
                                  incremental = TRUE,
                                  recordKeepingFile = recordKeepingFile,
                                  task = "runTemporalCohortCharacterization",
                                  jobName = "Temporal Cohort characterization")
    
    # Check no time ids are NA/NULL
    tdata <- readr::read_csv(file.path(exportFolder, "temporal_covariate_value_dist.csv"))
    expect_false(any(is.na(tdata$time_id) | is.null(tdata$time_id)))
    
    tdata <- readr::read_csv(file.path(exportFolder, "temporal_covariate_value.csv"))
    expect_false(any(is.na(tdata$time_id) | is.null(tdata$time_id)))
    
    # It would make no sense if there were NA values here
    tdata <- readr::read_csv(file.path(exportFolder, "temporal_time_ref.csv"))
    expect_false(any(is.na(tdata$time_id) | is.null(tdata$time_id)))
    
  })
})
