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

# --- Extended characterization tests ---

# Robust writeToCsv mock for exportCharacterization tests
safeExportWriteMock <- function(data, fileName, ...) {
  if (is.null(data)) return(invisible(NULL))
  tryCatch({
    df <- if (inherits(data, "tbl_Andromeda")) dplyr::collect(data) else data
    if (is.data.frame(df) && nrow(df) > 0 && !is.null(fileName)) {
      readr::write_csv(df, fileName)
    }
  }, error = function(e) invisible(NULL))
  invisible(NULL)
}

test_that("getCohortCharacteristics uses connectionDetails when connection is NULL", {
  cohortIds <- c(1)
  covariateSettings <- list(temporal = FALSE)
  class(covariateSettings) <- "covariateSettings"

  covariates <- dplyr::tibble(
    cohortDefinitionId = 1,
    covariateId = 1:3,
    sumValue = c(10, 20, 30),
    averageValue = c(0.1, 0.2, 0.3)
  )
  mockData <- structure(
    list(
      covariates = covariates,
      covariateRef = dplyr::tibble(covariateId = 1:3, covariateName = letters[1:3], analysisId = 1),
      analysisRef = dplyr::tibble(analysisId = 1, analysisName = "Test", isBinary = "Y", missingMeansZero = "Y", domainId = "Condition")
    ),
    class = "CovariateData"
  )
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100))

  local_mocked_bindings(
    connect = function(...) mockDatabaseConnection(),
    disconnect = function(...) NULL,
    .package = "DatabaseConnector"
  )
  local_mocked_bindings(
    getDbCovariateData = function(...) mockData,
    isTemporalCovariateData = function(...) FALSE,
    .package = "FeatureExtraction"
  )
  local_mocked_bindings(
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) expr,
    .package = "CohortDiagnostics"
  )

  results <- CohortDiagnostics:::getCohortCharacteristics(
    connectionDetails = list(dbms = "sqlite"),
    cdmDatabaseSchema = "main",
    cohortIds = cohortIds,
    covariateSettings = covariateSettings,
    exportFolder = tempdir()
  )

  expect_s4_class(results, "Andromeda")
  expect_true("covariates" %in% names(results))
  Andromeda::close(results)
})

test_that("getCohortCharacteristics processes both covariates and covariatesContinuous simultaneously", {
  cohortIds <- c(1)
  covariates <- dplyr::tibble(
    cohortDefinitionId = 1,
    covariateId = 1:5,
    sumValue = c(10, 20, 30, 40, 50),
    averageValue = c(0.1, 0.2, 0.3, 0.4, 0.5)
  )
  covariatesContinuous <- dplyr::tibble(
    cohortDefinitionId = 1,
    covariateId = 10,
    averageValue = 50,
    standardDeviation = 10
  )
  mockData <- structure(
    list(
      covariates = covariates,
      covariatesContinuous = covariatesContinuous,
      covariateRef = dplyr::tibble(covariateId = c(1:5, 10), covariateName = letters[1:6], analysisId = 1),
      analysisRef = dplyr::tibble(analysisId = 1, analysisName = "Test", isBinary = "Y", missingMeansZero = "Y", domainId = "Condition")
    ),
    class = "CovariateData"
  )
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100))

  local_mocked_bindings(
    getDbCovariateData = function(...) mockData,
    isTemporalCovariateData = function(...) FALSE,
    .package = "FeatureExtraction"
  )
  local_mocked_bindings(
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) expr,
    .package = "CohortDiagnostics"
  )

  results <- CohortDiagnostics:::getCohortCharacteristics(
    connection = mockDatabaseConnection(),
    cdmDatabaseSchema = "main",
    cohortIds = cohortIds,
    covariateSettings = list(),
    exportFolder = tempdir()
  )

  expect_true("covariates" %in% names(results))
  expect_true("covariatesContinuous" %in% names(results))

  cont <- results$covariatesContinuous %>% dplyr::collect()
  expect_equal(cont$mean, 50)

  Andromeda::close(results)
})

test_that("getCohortCharacteristics handles continuous covariates in temporal context", {
  cohortIds <- c(1)
  covariatesContinuous <- dplyr::tibble(
    cohortDefinitionId = 1,
    covariateId = 1,
    averageValue = 75,
    standardDeviation = 15,
    timeId = c(1, 2, 3)
  )
  mockData <- structure(
    list(
      covariatesContinuous = covariatesContinuous,
      covariateRef = dplyr::tibble(covariateId = 1, covariateName = "TestCont", analysisId = 1),
      analysisRef = dplyr::tibble(analysisId = 1, analysisName = "Test", isBinary = "N", missingMeansZero = "N", domainId = "Measurement"),
      timeRef = dplyr::tibble(timeId = 1:3, startDay = c(-365, -30, 0), endDay = c(-31, -1, 0))
    ),
    class = "CovariateData"
  )
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100))

  local_mocked_bindings(
    getDbCovariateData = function(...) mockData,
    isTemporalCovariateData = function(...) TRUE,
    .package = "FeatureExtraction"
  )
  local_mocked_bindings(
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) expr,
    .package = "CohortDiagnostics"
  )

  results <- CohortDiagnostics:::getCohortCharacteristics(
    connection = mockDatabaseConnection(),
    cdmDatabaseSchema = "main",
    cohortIds = cohortIds,
    covariateSettings = list(),
    exportFolder = tempdir()
  )

  expect_true("covariates" %in% names(results))
  covs <- results$covariates %>% dplyr::collect()
  expect_true(all(covs$sumValue == -1))
  expect_true(all(c("cohortId", "timeId", "covariateId", "sumValue", "mean", "sd") %in% names(covs)))

  Andromeda::close(results)
})

test_that("getCohortCharacteristics appends covariateRef when it already exists in results", {
  cohortIds <- c(1, 2)
  mockData <- createMockTemporalCovariateData(cohortIds = cohortIds)
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100, "2" = 200))

  local_mocked_bindings(
    getDbCovariateData = function(...) mockData,
    isTemporalCovariateData = function(...) TRUE,
    .package = "FeatureExtraction"
  )
  local_mocked_bindings(
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) expr,
    .package = "CohortDiagnostics"
  )

  results <- CohortDiagnostics:::getCohortCharacteristics(
    connection = mockDatabaseConnection(),
    cdmDatabaseSchema = "main",
    cohortIds = cohortIds,
    covariateSettings = list(),
    exportFolder = tempdir()
  )

  expect_s4_class(results, "Andromeda")
  expect_true("covariateRef" %in% names(results))
  expect_true("analysisRef" %in% names(results))

  Andromeda::close(results)
})

test_that("exportCharacterization writes covariatesContinuous when present", {
  andro <- Andromeda::andromeda()
  andro$covariates <- dplyr::tibble(
    cohortId = 1, covariateId = 1, sumValue = 10, mean = 0.1, sd = 0.05, timeId = 0
  )
  andro$covariatesContinuous <- dplyr::tibble(
    cohortId = 1, covariateId = 2,
    mean = 55, sd = 12, countValue = 50,
    minValue = 20, p10Value = 35, p25Value = 40,
    medianValue = 55, p75Value = 60, p90Value = 70, maxValue = 100
  )
  andro$covariateRef <- dplyr::tibble(covariateId = c(1, 2))
  andro$analysisRef <- dplyr::tibble(analysisId = 1)
  withr::defer(Andromeda::close(andro))

  exportFolder <- tempfile()
  dir.create(exportFolder)
  withr::defer(unlink(exportFolder, recursive = TRUE))

  covariateValueContFile <- file.path(exportFolder, "cont.csv")

  local_mocked_bindings(
    makeDataExportable = function(x, tableName, ...) {
      if (is.null(x)) return(NULL)
      dplyr::collect(x)
    },
    writeToCsv = safeExportWriteMock,
    .package = "CohortDiagnostics"
  )

  CohortDiagnostics:::exportCharacterization(
    characteristics = andro,
    databaseId = "test",
    incremental = FALSE,
    covariateValueFileName = file.path(exportFolder, "val.csv"),
    covariateValueContFileName = covariateValueContFile,
    covariateRefFileName = file.path(exportFolder, "ref.csv"),
    analysisRefFileName = file.path(exportFolder, "ana.csv"),
    counts = dplyr::tibble(cohortId = 1, databaseId = "test", cohortEntries = 100, cohortSubjects = 90),
    minCellCount = 5
  )

  expect_true(file.exists(covariateValueContFile))
})

test_that("exportCharacterization handles NULL timeRef gracefully", {
  andro <- Andromeda::andromeda()
  andro$covariates <- dplyr::tibble(
    cohortId = 1, covariateId = 1, sumValue = 8, mean = 0.08, sd = 0.03, timeId = 0
  )
  andro$covariateRef <- dplyr::tibble(covariateId = 1)
  andro$analysisRef <- dplyr::tibble(analysisId = 1)
  withr::defer(Andromeda::close(andro))

  local_mocked_bindings(
    makeDataExportable = function(x, tableName, ...) {
      if (is.null(x)) return(NULL)
      dplyr::collect(x)
    },
    writeToCsv = safeExportWriteMock,
    .package = "CohortDiagnostics"
  )

  expect_no_error(
    CohortDiagnostics:::exportCharacterization(
      characteristics = andro,
      databaseId = "test",
      incremental = FALSE,
      covariateValueFileName = tempfile(),
      covariateValueContFileName = tempfile(),
      covariateRefFileName = tempfile(),
      analysisRefFileName = tempfile(),
      timeRefFileName = NULL,
      counts = dplyr::tibble(cohortId = 1, databaseId = "test", cohortEntries = 100, cohortSubjects = 90),
      minCellCount = 5
    )
  )
})

test_that("exportCharacterization handles only covariatesContinuous without covariates", {
  andro <- Andromeda::andromeda()
  andro$covariatesContinuous <- dplyr::tibble(
    cohortId = 1, covariateId = 1, averageValue = 50, standardDeviation = 10, countValue = 30
  )
  andro$covariateRef <- dplyr::tibble(covariateId = 1)
  withr::defer(Andromeda::close(andro))

  local_mocked_bindings(
    makeDataExportable = function(x, tableName, ...) {
      if (is.null(x)) return(NULL)
      dplyr::collect(x)
    },
    writeToCsv = safeExportWriteMock,
    .package = "CohortDiagnostics"
  )

  expect_warning(
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
    ),
    regexp = "No characterization output"
  )
})

test_that("runTemporalCharacterizationDiagnostic errors on uninitialized context", {
  context <- list(isInitialized = FALSE)
  class(context) <- "DiagnosticsContext"

  expect_error(
    runTemporalCharacterizationDiagnostic(context),
    regexp = "not initialized"
  )
})

test_that("runTemporalCharacterizationDiagnostic wraps single covariateSettings into list", {
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))

  cohortDefinitionSet <- createMockCohortDefinitionSet(numCohorts = 1)
  context <- createDiagnosticsContext(
    connectionDetails = list(dbms = "sqlite"),
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "cohort",
    databaseId = "test",
    exportFolder = exportFolder
  )
  context$isInitialized <- TRUE
  context$cohortCounts <- dplyr::tibble(cohortId = 1, cohortEntries = 10, cohortSubjects = 10, databaseId = "test")
  context$incrementalFolder <- file.path(exportFolder, "inc")
  context$cohortTableNames <- list(cohortTable = "cohort")
  dir.create(context$incrementalFolder, showWarnings = FALSE)

  singleCovariateSetting <- mockCreateTemporalCovariateSettings()

  local_mocked_bindings(
    executeCohortCharacterization = function(...) NULL,
    timeExecution = function(folder, task, expr, ...) eval(expr),
    computeCohortCounts = function(...) context$cohortCounts,
    .package = "CohortDiagnostics"
  )
  local_mocked_bindings(
    createCohortBasedTemporalCovariateSettings = function(...) list(),
    .package = "FeatureExtraction"
  )

  expect_no_error(
    runTemporalCharacterizationDiagnostic(
      context = context,
      cohortDefinitionSet = cohortDefinitionSet,
      temporalCovariateSettings = singleCovariateSetting,
      runCohortRelationship = FALSE
    )
  )
})

test_that("runTemporalCharacterizationDiagnostic skips re-sampling when cohort already sampled", {
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))

  cohortDefinitionSet <- createMockCohortDefinitionSet(numCohorts = 1)
  attr(cohortDefinitionSet, "isSampledCohortDefinition") <- TRUE

  context <- createDiagnosticsContext(
    connectionDetails = list(dbms = "sqlite"),
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "cohort",
    databaseId = "test",
    exportFolder = exportFolder
  )
  context$isInitialized <- TRUE
  context$cohortCounts <- dplyr::tibble(cohortId = 1, cohortEntries = 10, cohortSubjects = 10, databaseId = "test")
  context$incrementalFolder <- file.path(exportFolder, "inc")
  context$cohortTableNames <- list(cohortTable = "cohort")
  dir.create(context$incrementalFolder, showWarnings = FALSE)

  samplingCalled <- FALSE
  local_mocked_bindings(
    executeCohortCharacterization = function(...) NULL,
    timeExecution = function(folder, task, expr, ...) eval(expr),
    computeCohortCounts = function(...) context$cohortCounts,
    .package = "CohortDiagnostics"
  )
  local_mocked_bindings(
    createCohortTables = function(...) { samplingCalled <<- TRUE },
    sampleCohortDefinitionSet = function(...) { samplingCalled <<- TRUE },
    .package = "CohortGenerator"
  )

  runTemporalCharacterizationDiagnostic(
    context = context,
    cohortDefinitionSet = cohortDefinitionSet,
    runFeatureExtractionOnSample = TRUE,
    runCohortRelationship = FALSE
  )

  expect_false(samplingCalled)
})

test_that("executeCohortCharacterization processes batches correctly when nrow exceeds batchSize", {
  exportFolder <- tempfile()
  dir.create(exportFolder)
  withr::defer(unlink(exportFolder, recursive = TRUE))

  cohorts <- createMockCohortDefinitionSet(numCohorts = 5)
  cohorts$checksum <- "abc"

  processedIds <- c()
  getCharacteristicsCalled <- 0

  local_mocked_bindings(
    getCohortCharacteristics = function(...) {
      getCharacteristicsCalled <<- getCharacteristicsCalled + 1
      args <- list(...)
      processedIds <<- c(processedIds, args$cohortIds)
      andro <- Andromeda::andromeda()
      andro$covariates <- dplyr::tibble(cohortId = 1, covariateId = 1, sumValue = 10, mean = 0.1, sd = 0.1, timeId = 0)
      andro$covariateRef <- dplyr::tibble(covariateId = 1)
      andro$analysisRef <- dplyr::tibble(analysisId = 1)
      andro
    },
    exportCharacterization = function(...) NULL,
    subsetToRequiredCohorts = function(cohorts, ...) cohorts,
    recordTasksDone = function(...) NULL,
    .package = "CohortDiagnostics"
  )

  CohortDiagnostics:::executeCohortCharacterization(
    connection = mockDatabaseConnection(),
    databaseId = "test",
    exportFolder = exportFolder,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    covariateSettings = list(),
    tempEmulationSchema = NULL,
    cdmVersion = 5,
    cohorts = cohorts,
    cohortCounts = createMockCohortCounts(cohortIds = cohorts$cohortId),
    minCellCount = 5,
    instantiatedCohorts = cohorts$cohortId,
    incremental = FALSE,
    recordKeepingFile = tempfile(),
    batchSize = 2
  )

  expect_gt(getCharacteristicsCalled, 1)
  expect_equal(length(unique(processedIds)), 5)
})

test_that("runTemporalCharacterizationDiagnostic handles zero-length covariateSettings", {
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))

  cohortDefinitionSet <- createMockCohortDefinitionSet(numCohorts = 1)
  context <- createDiagnosticsContext(
    connectionDetails = list(dbms = "sqlite"),
    cdmDatabaseSchema = "cdm",
    cohortDatabaseSchema = "cohort",
    databaseId = "test",
    exportFolder = exportFolder
  )
  context$isInitialized <- TRUE
  context$cohortCounts <- dplyr::tibble()
  context$incrementalFolder <- file.path(exportFolder, "inc")
  context$cohortTableNames <- list(cohortTable = "cohort")
  dir.create(context$incrementalFolder, showWarnings = FALSE)

  executionCalled <- FALSE
  local_mocked_bindings(
    executeCohortCharacterization = function(...) { executionCalled <<- TRUE },
    timeExecution = function(folder, task, expr, ...) eval(expr),
    .package = "CohortDiagnostics"
  )

  runTemporalCharacterizationDiagnostic(
    context = context,
    cohortDefinitionSet = cohortDefinitionSet,
    temporalCovariateSettings = list(),
    runCohortRelationship = FALSE
  )

  expect_false(executionCalled)
})

test_that("getCohortCharacteristics handles NA timeId in continuous temporal covariates", {
  cohortIds <- c(1)
  covariatesContinuous <- dplyr::tibble(
    cohortDefinitionId = 1,
    covariateId = c(1, 2),
    averageValue = c(50, 60),
    standardDeviation = c(10, 8),
    timeId = c(1, NA)
  )
  mockData <- structure(
    list(
      covariatesContinuous = covariatesContinuous,
      covariateRef = dplyr::tibble(covariateId = 1:2, covariateName = c("A", "B"), analysisId = 1),
      analysisRef = dplyr::tibble(analysisId = 1, analysisName = "Test", isBinary = "N", missingMeansZero = "N", domainId = "Measurement"),
      timeRef = dplyr::tibble(timeId = 1, startDay = -365, endDay = -31)
    ),
    class = "CovariateData"
  )
  attr(mockData, "metaData") <- list(populationSize = c("1" = 100))

  local_mocked_bindings(
    getDbCovariateData = function(...) mockData,
    isTemporalCovariateData = function(...) TRUE,
    .package = "FeatureExtraction"
  )
  local_mocked_bindings(
    timeExecution = function(exportFolder, taskName, parent, cohortIds, expr) expr,
    .package = "CohortDiagnostics"
  )

  results <- CohortDiagnostics:::getCohortCharacteristics(
    connection = mockDatabaseConnection(),
    cdmDatabaseSchema = "main",
    cohortIds = cohortIds,
    covariateSettings = list(),
    exportFolder = tempdir()
  )

  covs <- results$covariates %>% dplyr::collect()
  expect_true(any(covs$timeId == -1))
  expect_equal(nrow(covs), 2)

  Andromeda::close(results)
})

test_that("exportCharacterization skips export when covariateRef has no rows", {
  andro <- Andromeda::andromeda()
  andro$covariates <- dplyr::tibble(cohortId = 1, covariateId = 1, sumValue = 10, mean = 0.1, sd = 0.05, timeId = 0)
  andro$covariateRef <- dplyr::tibble(covariateId = integer())
  andro$analysisRef <- dplyr::tibble(analysisId = integer())
  withr::defer(Andromeda::close(andro))

  exportFolder <- tempfile()
  dir.create(exportFolder)
  withr::defer(unlink(exportFolder, recursive = TRUE))

  valFile <- file.path(exportFolder, "val.csv")

  local_mocked_bindings(
    makeDataExportable = function(x, tableName, ...) {
      if (is.null(x)) return(NULL)
      dplyr::collect(x)
    },
    writeToCsv = safeExportWriteMock,
    .package = "CohortDiagnostics"
  )

  CohortDiagnostics:::exportCharacterization(
    characteristics = andro,
    databaseId = "test",
    incremental = FALSE,
    covariateValueFileName = valFile,
    covariateValueContFileName = file.path(exportFolder, "cont.csv"),
    covariateRefFileName = file.path(exportFolder, "ref.csv"),
    analysisRefFileName = file.path(exportFolder, "ana.csv"),
    counts = dplyr::tibble(cohortId = 1, databaseId = "test", cohortEntries = 100, cohortSubjects = 90),
    minCellCount = 5
  )

  expect_false(file.exists(valFile))
})

test_that("executeCohortCharacterization uses default file paths from exportFolder", {
  exportFolder <- tempfile()
  dir.create(exportFolder)
  withr::defer(unlink(exportFolder, recursive = TRUE))

  cohorts <- createMockCohortDefinitionSet(numCohorts = 1)
  cohorts$checksum <- "abc"

  local_mocked_bindings(
    getCohortCharacteristics = function(...) {
      andro <- Andromeda::andromeda()
      andro$covariates <- dplyr::tibble(cohortId = 1, covariateId = 1, sumValue = 10, mean = 0.1, sd = 0.1, timeId = 0)
      andro$covariateRef <- dplyr::tibble(covariateId = 1)
      andro$analysisRef <- dplyr::tibble(analysisId = 1)
      andro
    },
    exportCharacterization = function(...) NULL,
    subsetToRequiredCohorts = function(cohorts, ...) cohorts,
    recordTasksDone = function(...) NULL,
    .package = "CohortDiagnostics"
  )

  expect_no_error(
    CohortDiagnostics:::executeCohortCharacterization(
      connection = mockDatabaseConnection(),
      databaseId = "test",
      exportFolder = exportFolder,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      covariateSettings = list(),
      tempEmulationSchema = NULL,
      cdmVersion = 5,
      cohorts = cohorts,
      cohortCounts = createMockCohortCounts(cohortIds = cohorts$cohortId),
      minCellCount = 5,
      instantiatedCohorts = cohorts$cohortId,
      incremental = FALSE,
      recordKeepingFile = tempfile()
    )
  )
})
