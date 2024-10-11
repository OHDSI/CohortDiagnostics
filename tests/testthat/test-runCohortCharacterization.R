# test getCohortCharacteristics on all databases
for (nm in names(testServers)) {

  server <- testServers[[nm]]
  con <- connect(server$connectionDetails)
  exportFolder <- file.path(tempdir(), paste0(nm, "exp"))
  recordKeepingFile <- file.path(exportFolder, "record.csv")
  minCharacterizationMean <- 0.001

  test_that("Testing getCohortCharacteristics", {
    skip_if(skipCdmTests, "cdm settings not configured")
    
    results <- getCohortCharacteristics(
      connection = con,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      tempEmulationSchema = server$tempEmulationSchema,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortTable = server$cohortTable,
      cohortIds = server$cohortIds,
      covariateSettings = temporalCovariateSettings,
      exportFolder = exportFolder,
      minCharacterizationMean = minCharacterizationMean
    )
    
    # check characteristics
    expect_equal(class(results), "Andromeda")
    expect_equal(names(results), c("analysisRef", "covariateRef", "covariates", "covariatesContinuous", "timeRef"))
    
    analysisRef <- results$analysisRef
    analysisIds <- analysisRef %>% pull(analysisId)
    expect_true(analysisRef %>% pull(analysisName) %in% c("Measurement", "ConditionOccurence", "DrugEraStart", "CharlsonIndex", "ProcedureOccurence"))
    
    covariateRef <- results$covariateRef
    expect_true(covariateRef %>% pull(analysisId) %in% analysisIds)
    
    covariates <- results$covariates
    expect_true(covariates %>% pull(cohortId) %in% server$cohortIds)
    expect_true(covariates %>% pull(mean) %>% min() >= minCharacterizationMean)
    
    covariatesCont <- results$covariatesContinuous
    expect_true(covariatesCont %>% pull(cohortId) %in% server$cohortIds)
    
    timeRef <- results$timeRef
    expect_true(timeRef %>% pull(startDay), c(-365, -30, 0, 1, 31))
    expect_true(timeRef %>% pull(endDay), c(-31, -1, 0, 30, 365))
  })
}


test_that("Execute and export characterization", {
  skip_if(skipCdmTests, "cdm settings not configured")
  skip_if_not("sqlite" %in% names(testServers))
  server <- testServers[["sqlite"]]

  tConnection <- DatabaseConnector::connect(server$connectionDetails)

  with_dbc_connection(tConnection, {
    exportFolder <- tempfile()
    recordKeepingFile <- tempfile(fileext = "csv")
    dir.create(exportFolder)
    on.exit(unlink(exportFolder), add = TRUE)

    # Required for function use
    cohortCounts <- CohortGenerator::getCohortCounts(
      connection = tConnection,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortTable = server$cohortTable,
      cohortDefinitionSet = server$cohortDefinitionSet,
      databaseId = "Testdb"
    )
    exportDataToCsv(
      data = cohortCounts,
      tableName = "cohort_count",
      fileName = file.path(exportFolder, "cohort_count.csv"),
      minCellCount = 5,
      databaseId = "Testdb"
    )
    checkmate::expect_file_exists(file.path(exportFolder, "cohort_count.csv"))

    runCohortCharacterization(
      connection = tConnection,
      databaseId = "Testdb",
      exportFolder = exportFolder,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortTable = server$cohortTable,
      covariateSettings = temporalCovariateSettings,
      tempEmulationSchema = server$tempEmulationSchema,
      cdmVersion = 5,
      cohorts = server$cohortDefinitionSet[1:3, ],
      cohortCounts = cohortCounts,
      minCellCount = 5,
      instantiatedCohorts = server$cohortDefinitionSet$cohortId,
      incremental = TRUE,
      recordKeepingFile = recordKeepingFile,
      minCharacterizationMean = 0.3
    )

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
      cohorts = server$cohortDefinitionSet,
      task = "runCohortCharacterization",
      incremental = TRUE,
      recordKeepingFile = recordKeepingFile
    )

    # should not have the cohorts that were previously run
    testthat::expect_equal(
      object = nrow(subset %>%
        dplyr::filter(
          cohortId %in% c(server$cohortDefinitionSet[1:3, ]$cohortId)
        )),
      expected = 0
    )

    # finish the rest of characterization
    runCohortCharacterization(
      connection = tConnection,
      databaseId = "Testdb",
      exportFolder = exportFolder,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortTable = server$cohortTable,
      covariateSettings = temporalCovariateSettings,
      tempEmulationSchema = server$tempEmulationSchema,
      cdmVersion = 5,
      cohorts = server$cohortDefinitionSet,
      cohortCounts = cohortCounts,
      minCellCount = 5,
      instantiatedCohorts = server$cohortDefinitionSet$cohortId,
      incremental = TRUE,
      recordKeepingFile = recordKeepingFile,
      minCharacterizationMean = 0.3
    )

    # Check no time ids are NA/NULL
    readr::local_edition(1)
    tdata <- readr::read_csv(file.path(exportFolder, "temporal_covariate_value_dist.csv"))
    expect_false(any(is.na(tdata$time_id) | is.null(tdata$time_id)))

    tdata <- readr::read_csv(file.path(exportFolder, "temporal_covariate_value.csv"))
    expect_false(any(is.na(tdata$time_id) | is.null(tdata$time_id)))

    # It would make no sense if there were NA values here
    tdata <- readr::read_csv(file.path(exportFolder, "temporal_time_ref.csv"))
    expect_false(any(is.na(tdata$time_id) | is.null(tdata$time_id)))
  })
})
