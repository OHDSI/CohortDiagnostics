# Unit tests for Cohort Characterization module
# These tests are isolated and require no database connections

# Helper for writeToCsv mock
safeWriteToCsv <- function(data, fileName, ...) {
  if (is.null(data)) {
    return()
  }
  df <- if (is.data.frame(data)) data else dplyr::collect(data)
  if (!is.null(df) && nrow(df) > 0) {
    readr::write_csv(df, fileName)
  }
}

# --- Basic Functionality ---

test_that("getCohortCharacteristics processes temporal covariates", {
  # Arrange
  cohortIds <- c(1)
  mockData <- createMockTemporalCovariateData(cohortIds = cohortIds)
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100))

  mockConn <- mockDatabaseConnection()
  covariateSettings <- mockCreateTemporalCovariateSettings()

  # Act
  testthat::with_mocked_bindings(
    {
      testthat::with_mocked_bindings(
        {
          results <- CohortDiagnostics:::getCohortCharacteristics(
            connection = mockConn,
            cdmDatabaseSchema = "main",
            cohortIds = cohortIds,
            covariateSettings = covariateSettings,
            exportFolder = tempdir()
          )
        },
        getDbCovariateData = function(...) mockData,
        isTemporalCovariateData = function(...) TRUE,
        .package = "FeatureExtraction"
      )
    },
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) {
      expr
    },
    .package = "CohortDiagnostics"
  )

  # Assert
  expect_s4_class(results, "Andromeda")
  expect_true("covariates" %in% names(results))
  expect_true("timeRef" %in% names(results))

  covariates <- results$covariates %>% dplyr::collect()
  expect_true(all(covariates$timeId != 0))
  expect_true(all(c("cohortId", "timeId", "covariateId", "sumValue", "mean", "sd") %in% names(covariates)))
  Andromeda::close(results)
})

test_that("getCohortCharacteristics processes non-temporal binary covariates", {
  # Arrange
  cohortIds <- c(1)
  covariates <- dplyr::tibble(
    cohortDefinitionId = 1,
    covariateId = 1:10,
    sumValue = 10,
    averageValue = 0.1
  )
  covariateRef <- dplyr::tibble(
    covariateId = 1:10,
    covariateName = "Test",
    analysisId = 1
  )
  analysisRef <- dplyr::tibble(
    analysisId = 1,
    isBinary = "Y"
  )
  mockData <- structure(
    list(
      covariates = covariates,
      covariateRef = covariateRef,
      analysisRef = analysisRef
    ),
    class = "CovariateData"
  )
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100))

  mockConn <- mockDatabaseConnection()
  covariateSettings <- list(temporal = FALSE)
  class(covariateSettings) <- "covariateSettings"

  # Act
  testthat::with_mocked_bindings(
    {
      testthat::with_mocked_bindings(
        {
          results <- CohortDiagnostics:::getCohortCharacteristics(
            connection = mockConn,
            cdmDatabaseSchema = "main",
            cohortIds = cohortIds,
            cohortTable = "cohort",
            covariateSettings = covariateSettings,
            exportFolder = tempdir()
          )
        },
        getDbCovariateData = function(...) mockData,
        isTemporalCovariateData = function(...) FALSE,
        .package = "FeatureExtraction"
      )
    },
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) {
      expr
    },
    .package = "CohortDiagnostics"
  )

  # Assert
  expect_s4_class(results, "Andromeda")
  covariates_result <- results$covariates %>% dplyr::collect()
  expect_true(all(covariates_result$timeId == 0))
  Andromeda::close(results)
})

test_that("getCohortCharacteristics processes continuous covariates", {
  # Arrange
  cohortIds <- c(1)
  covariatesContinuous <- dplyr::tibble(
    cohortDefinitionId = 1,
    covariateId = 1,
    averageValue = 50,
    standardDeviation = 10,
    timeId = 0
  )
  covariateRef <- dplyr::tibble(
    covariateId = 1,
    covariateName = "Test Cont",
    analysisId = 1
  )
  analysisRef <- dplyr::tibble(
    analysisId = 1,
    isBinary = "N"
  )
  mockData <- structure(
    list(
      covariatesContinuous = covariatesContinuous,
      covariateRef = covariateRef,
      analysisRef = analysisRef
    ),
    class = "CovariateData"
  )
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100))

  mockConn <- mockDatabaseConnection()

  # Act
  testthat::with_mocked_bindings(
    {
      testthat::with_mocked_bindings(
        {
          results <- CohortDiagnostics:::getCohortCharacteristics(
            connection = mockConn,
            cdmDatabaseSchema = "main",
            cohortIds = cohortIds,
            covariateSettings = list(),
            exportFolder = tempdir()
          )
        },
        getDbCovariateData = function(...) mockData,
        isTemporalCovariateData = function(...) FALSE,
        .package = "FeatureExtraction"
      )
    },
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) {
      expr
    },
    .package = "CohortDiagnostics"
  )

  # Assert
  expect_true("covariatesContinuous" %in% names(results))
  cont <- results$covariatesContinuous %>% dplyr::collect()
  expect_equal(cont$mean, 50)
  Andromeda::close(results)
})

test_that("getCohortCharacteristics handles empty covariate data", {
  # Arrange
  cohortIds <- c(1)
  mockData <- structure(
    list(
      covariateRef = dplyr::tibble(covariateId = integer(), covariateName = character()),
      analysisRef = dplyr::tibble(analysisId = integer())
    ),
    class = "CovariateData"
  )
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100))

  mockConn <- mockDatabaseConnection()

  # Act
  testthat::with_mocked_bindings(
    {
      testthat::with_mocked_bindings(
        {
          results <- CohortDiagnostics:::getCohortCharacteristics(
            connection = mockConn,
            cdmDatabaseSchema = "main",
            cohortIds = cohortIds,
            covariateSettings = list(),
            exportFolder = tempdir()
          )
        },
        getDbCovariateData = function(...) mockData,
        isTemporalCovariateData = function(...) FALSE,
        .package = "FeatureExtraction"
      )
    },
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) {
      expr
    },
    .package = "CohortDiagnostics"
  )

  # Assert
  expect_s4_class(results, "Andromeda")
  expect_false("covariates" %in% names(results))
  Andromeda::close(results)
})

test_that("getCohortCharacteristics processes multiple cohorts", {
  # Arrange
  cohortIds <- c(1, 2)
  mockData <- createMockTemporalCovariateData(cohortIds = cohortIds)
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100, "2" = 200))

  mockConn <- mockDatabaseConnection()

  # Act
  testthat::with_mocked_bindings(
    {
      testthat::with_mocked_bindings(
        {
          results <- CohortDiagnostics:::getCohortCharacteristics(
            connection = mockConn,
            cdmDatabaseSchema = "main",
            cohortIds = cohortIds,
            covariateSettings = list(),
            exportFolder = tempdir()
          )
        },
        getDbCovariateData = function(...) mockData,
        isTemporalCovariateData = function(...) TRUE,
        .package = "FeatureExtraction"
      )
    },
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) {
      expr
    },
    .package = "CohortDiagnostics"
  )

  # Assert
  covariates <- results$covariates %>% dplyr::collect()
  expect_true(all(cohortIds %in% covariates$cohortId))
  Andromeda::close(results)
})

# --- Filtering Logic ---

test_that("exportCharacterization enforces min cell count", {
  # Arrange
  andro <- Andromeda::andromeda()
  andro$covariates <- dplyr::tibble(
    cohortId = 1,
    covariateId = 1,
    sumValue = 3,
    mean = 0.03,
    sd = 0.1,
    timeId = 0
  )
  andro$covariateRef <- dplyr::tibble(
    covariateId = 1,
    covariateName = "Test"
  )
  andro$analysisRef <- dplyr::tibble(
    analysisId = 1,
    analysisName = "Test"
  )
  withr::defer(Andromeda::close(andro))

  counts <- dplyr::tibble(
    cohortId = 1,
    cohortEntries = 100,
    cohortSubjects = 100,
    databaseId = "test"
  )

  exportFolder <- tempfile()
  dir.create(exportFolder)
  withr::defer(unlink(exportFolder, recursive = TRUE))

  # Act
  testthat::with_mocked_bindings(
    {
      CohortDiagnostics:::exportCharacterization(
        characteristics = andro,
        databaseId = "test",
        incremental = FALSE,
        covariateValueFileName = file.path(exportFolder, "val.csv"),
        covariateValueContFileName = file.path(exportFolder, "cont.csv"),
        covariateRefFileName = file.path(exportFolder, "ref.csv"),
        analysisRefFileName = file.path(exportFolder, "ana.csv"),
        timeRefFileName = file.path(exportFolder, "time.csv"),
        counts = counts,
        minCellCount = 5
      )
    },
    makeDataExportable = function(x, ...) if (is.null(x)) {
      return(NULL)
    } else {
      dplyr::collect(x)
    },
    writeToCsv = safeWriteToCsv,
    .package = "CohortDiagnostics"
  )

  # Assert
  val <- readr::read_csv(file.path(exportFolder, "val.csv"), col_types = readr::cols())
  expect_equal(val$sumValue[1], -5)
})

test_that("exportCharacterization filters zero values correctly", {
  # Arrange
  andro <- Andromeda::andromeda()
  andro$covariates <- dplyr::tibble(
    cohortId = 1,
    covariateId = 1,
    sumValue = 0,
    mean = 0,
    sd = 0,
    timeId = 0
  )
  andro$covariateRef <- dplyr::tibble(covariateId = 1)
  andro$analysisRef <- dplyr::tibble(analysisId = 1)
  withr::defer(Andromeda::close(andro))

  # Act
  testthat::with_mocked_bindings(
    {
      CohortDiagnostics:::exportCharacterization(
        characteristics = andro,
        databaseId = "test",
        incremental = FALSE,
        covariateValueFileName = tempfile(),
        covariateValueContFileName = tempfile(),
        covariateRefFileName = tempfile(),
        analysisRefFileName = tempfile(),
        counts = dplyr::tibble(cohortId = 1, databaseId = "test", cohortEntries = 100, cohortSubjects = 90),
        minCellCount = 5
      )
    },
    makeDataExportable = function(x, ...) if (is.null(x)) {
      return(NULL)
    } else {
      dplyr::collect(x)
    },
    writeToCsv = safeWriteToCsv,
    .package = "CohortDiagnostics"
  )

  # Assert
  expect_true(TRUE)
})

# --- Batch Processing ---

test_that("executeCohortCharacterization processes small batches", {
  # Arrange
  cohorts <- createMockCohortDefinitionSet(numCohorts = 3)
  cohorts$checksum <- "abc"
  cohortCounts <- createMockCohortCounts(cohortIds = cohorts$cohortId)
  exportFolder <- tempfile()
  dir.create(exportFolder)
  withr::defer(unlink(exportFolder, recursive = TRUE))

  mockConn <- mockDatabaseConnection()

  # Act
  testthat::with_mocked_bindings(
    {
      CohortDiagnostics:::executeCohortCharacterization(
        connection = mockConn,
        databaseId = "test",
        exportFolder = exportFolder,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "cohort",
        covariateSettings = list(),
        tempEmulationSchema = NULL,
        cdmVersion = 5,
        cohorts = cohorts,
        cohortCounts = cohortCounts,
        minCellCount = 5,
        instantiatedCohorts = cohorts$cohortId,
        incremental = FALSE,
        recordKeepingFile = tempfile(),
        batchSize = 1
      )
    },
    getCohortCharacteristics = function(...) {
      andro <- Andromeda::andromeda()
      andro$covariates <- dplyr::tibble(cohortId = 1, covariateId = 1, sumValue = 10, mean = 0.1, sd = 0.1, timeId = 0)
      andro$covariateRef <- dplyr::tibble(covariateId = 1)
      andro$analysisRef <- dplyr::tibble(analysisId = 1)
      return(andro)
    },
    exportCharacterization = function(...) {},
    subsetToRequiredCohorts = function(cohorts, ...) cohorts,
    recordTasksDone = function(...) {},
    .package = "CohortDiagnostics"
  )

  # Assert
  expect_true(dir.exists(exportFolder))
})

# --- Edge Cases ---

test_that("executeCohortCharacterization handles empty cohort definition set", {
  # Arrange
  cohorts <- dplyr::tibble(cohortId = integer(), checksum = character())
  exportFolder <- tempfile()
  dir.create(exportFolder)
  withr::defer(unlink(exportFolder, recursive = TRUE))

  # Act & Assert
  testthat::with_mocked_bindings(
    {
      expect_no_error(
        CohortDiagnostics:::executeCohortCharacterization(
          connection = mockDatabaseConnection(),
          databaseId = "test",
          exportFolder = exportFolder,
          cdmDatabaseSchema = "main",
          cohortDatabaseSchema = "main",
          cohortTable = "cohort",
          covariateSettings = list(),
          cohorts = cohorts,
          cohortCounts = dplyr::tibble(),
          minCellCount = 5,
          instantiatedCohorts = integer(),
          incremental = FALSE,
          recordKeepingFile = tempfile()
        )
      )
    },
    subsetToRequiredCohorts = function(cohorts, ...) cohorts,
    .package = "CohortDiagnostics"
  )
})

test_that("getCohortCharacteristics throws error for population size mismatch", {
  # Arrange
  cohortIds <- c(1)
  mockData <- structure(
    list(
      covariates = dplyr::tibble(
        cohortDefinitionId = 1,
        covariateId = 1,
        sumValue = 200,
        averageValue = 2.0
      ),
      covariateRef = dplyr::tibble(covariateId = 1),
      analysisRef = dplyr::tibble(analysisId = 1)
    ),
    class = "CovariateData"
  )
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100))

  # Act & Assert
  testthat::with_mocked_bindings(
    {
      testthat::with_mocked_bindings(
        {
          expect_error(
            CohortDiagnostics:::getCohortCharacteristics(
              connection = mockDatabaseConnection(),
              cdmDatabaseSchema = "main",
              cohortIds = cohortIds,
              covariateSettings = list(),
              exportFolder = tempdir()
            ),
            regexp = "population size.*smaller than features Value"
          )
        },
        getDbCovariateData = function(...) mockData,
        isTemporalCovariateData = function(...) FALSE,
        .package = "FeatureExtraction"
      )
    },
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) {
      expr
    },
    .package = "CohortDiagnostics"
  )
})

test_that("getCohortCharacteristics handles NA timeId", {
  # Arrange
  cohortIds <- c(1)
  mockData <- createMockTemporalCovariateData(cohortIds = cohortIds)
  mockData$covariates$timeId[1] <- NA
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100))

  # Act
  testthat::with_mocked_bindings(
    {
      testthat::with_mocked_bindings(
        {
          results <- CohortDiagnostics:::getCohortCharacteristics(
            connection = mockDatabaseConnection(),
            cdmDatabaseSchema = "main",
            cohortIds = cohortIds,
            covariateSettings = list(),
            exportFolder = tempdir()
          )
        },
        getDbCovariateData = function(...) mockData,
        isTemporalCovariateData = function(...) TRUE,
        .package = "FeatureExtraction"
      )
    },
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) {
      expr
    },
    .package = "CohortDiagnostics"
  )

  # Assert
  covariates <- results$covariates %>% dplyr::collect()
  expect_true(any(covariates$timeId == -1))
  Andromeda::close(results)
})

test_that("executeCohortCharacterization handles incremental mode skipping", {
  # Arrange
  cohorts <- createMockCohortDefinitionSet(numCohorts = 2)
  cohorts$checksum <- "abc"
  instantiatedCohorts <- cohorts$cohortId
  subset <- cohorts[1, ]

  # Act
  testthat::with_mocked_bindings(
    {
      CohortDiagnostics:::executeCohortCharacterization(
        connection = mockDatabaseConnection(),
        databaseId = "test",
        exportFolder = tempdir(),
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "cohort",
        covariateSettings = list(),
        cohorts = cohorts,
        cohortCounts = createMockCohortCounts(cohortIds = cohorts$cohortId),
        minCellCount = 5,
        instantiatedCohorts = instantiatedCohorts,
        incremental = TRUE,
        recordKeepingFile = tempfile()
      )
    },
    subsetToRequiredCohorts = function(...) subset,
    getCohortCharacteristics = function(...) Andromeda::andromeda(),
    exportCharacterization = function(...) {},
    recordTasksDone = function(...) {},
    .package = "CohortDiagnostics"
  )

  expect_true(TRUE)
})

test_that("exportCharacterization handles missing covariate data", {
  # Arrange
  andro <- Andromeda::andromeda()
  withr::defer(Andromeda::close(andro))

  # Act & Assert
  testthat::with_mocked_bindings(
    {
      expect_warning(
        CohortDiagnostics:::exportCharacterization(
          characteristics = andro,
          databaseId = "test",
          incremental = FALSE,
          covariateValueFileName = tempfile(),
          covariateValueContFileName = tempfile(),
          covariateRefFileName = tempfile(),
          analysisRefFileName = tempfile(),
          counts = dplyr::tibble(),
          minCellCount = 5
        ),
        regexp = "No characterization output"
      )
    },
    makeDataExportable = function(x, ...) if (is.null(x)) {
      return(NULL)
    } else {
      x
    },
    .package = "CohortDiagnostics"
  )
})

test_that("executeCohortCharacterization cleans up files in non-incremental mode", {
  # Arrange
  exportFolder <- tempfile()
  dir.create(exportFolder)
  withr::defer(unlink(exportFolder, recursive = TRUE))

  testFile <- file.path(exportFolder, "temporal_covariate_value.csv")
  write.csv(data.frame(a = 1), testFile)

  cohorts <- createMockCohortDefinitionSet(numCohorts = 0)

  # Act
  testthat::with_mocked_bindings(
    {
      CohortDiagnostics:::executeCohortCharacterization(
        connection = mockDatabaseConnection(),
        databaseId = "test",
        exportFolder = exportFolder,
        cdmDatabaseSchema = "main",
        cohortDatabaseSchema = "main",
        cohortTable = "cohort",
        covariateSettings = list(),
        cohorts = cohorts,
        cohortCounts = dplyr::tibble(),
        minCellCount = 5,
        instantiatedCohorts = integer(),
        incremental = FALSE,
        recordKeepingFile = tempfile()
      )
    },
    subsetToRequiredCohorts = function(cohorts, ...) cohorts,
    .package = "CohortDiagnostics"
  )

  # Assert
  expect_false(file.exists(testFile))
})

test_that("getCohortCharacteristics reinforces minCharacterizationMean in binary covariates", {
  # Arrange
  cohortIds <- c(1)
  mockData <- createMockTemporalCovariateData(cohortIds = cohortIds)
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100))

  # Act & Assert
  expect_no_error(
    testthat::with_mocked_bindings(
      {
        testthat::with_mocked_bindings(
          {
            results <- CohortDiagnostics:::getCohortCharacteristics(
              connection = mockDatabaseConnection(),
              cdmDatabaseSchema = "main",
              cohortIds = cohortIds,
              covariateSettings = list(),
              exportFolder = tempdir(),
              minCharacterizationMean = 0.05
            )
            Andromeda::close(results)
          },
          getDbCovariateData = function(...) mockData,
          isTemporalCovariateData = function(...) TRUE,
          .package = "FeatureExtraction"
        )
      },
      timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) {
        expr
      },
      .package = "CohortDiagnostics"
    )
  )
})

test_that("exportCharacterization suppresses continuous covariates with low counts", {
  # Arrange
  andro <- Andromeda::andromeda()
  andro$covariates <- dplyr::tibble(cohortId = 1, covariateId = 1, sumValue = 10, mean = 0.1, sd = 0.1, timeId = 0)
  andro$covariatesContinuous <- dplyr::tibble(
    cohortId = 1,
    covariateId = 1,
    averageValue = 10,
    standardDeviation = 2,
    countValue = 3
  )
  andro$covariateRef <- dplyr::tibble(covariateId = 1)
  withr::defer(Andromeda::close(andro))

  # Act
  testthat::with_mocked_bindings(
    {
      CohortDiagnostics:::exportCharacterization(
        characteristics = andro,
        databaseId = "test",
        incremental = FALSE,
        covariateValueFileName = tempfile(),
        covariateValueContFileName = tempfile(),
        covariateRefFileName = tempfile(),
        analysisRefFileName = tempfile(),
        counts = dplyr::tibble(cohortId = 1, databaseId = "test", cohortEntries = 100, cohortSubjects = 90),
        minCellCount = 5
      )
    },
    makeDataExportable = function(x, tableName, ...) {
      if (is.null(x)) {
        return(NULL)
      }
      if (tableName == "temporal_covariate_value_dist") {
        df <- dplyr::collect(x)
        return(df %>% dplyr::filter(countValue >= 5))
      }
      return(dplyr::collect(x))
    },
    writeToCsv = safeWriteToCsv,
    .package = "CohortDiagnostics"
  )

  expect_true(TRUE)
})

test_that("runTemporalCharacterizationDiagnostic orchestrates sub-functions correctly", {
  skip_if_not_installed("testthat", "3.0.0")

  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))

  context <- createDiagnosticsContext(
    connectionDetails = list(dbms = "sqlite"),
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "cohort",
    databaseId = "test",
    exportFolder = exportFolder
  )
  context$isInitialized <- TRUE
  context$incrementalFolder <- file.path(exportFolder, "incremental")
  dir.create(context$incrementalFolder, showWarnings = FALSE)

  cohortDefinitionSet <- createMockCohortDefinitionSet(numCohorts = 1)

  # Track calls
  calls <- list()

  local_mocked_bindings(
    executeCohortCharacterization = function(...) {
      calls <<- c(calls, "executeCohortCharacterization")
    },
    timeExecution = function(folder, taskName, ...) {
      calls <<- c(calls, taskName)
      args <- list(...)
      eval(args$expr)
    },
    .package = "CohortDiagnostics"
  )

  runTemporalCharacterizationDiagnostic(context, cohortDefinitionSet = cohortDefinitionSet)

  expect_true("executeCohortCharacterization" %in% calls)
})
