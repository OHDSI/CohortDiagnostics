# Tests for DiagnosticsContext
source(testthat::test_path( "fixtures", "mock_data.R"))

test_that("createDiagnosticsContext initializes correctly with minimal arguments", {
    exportFolder <- tempfile("export")
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    context <- createDiagnosticsContext(
        connectionDetails = list(),
        cdmDatabaseSchema = "cdm_schema",
        cohortDatabaseSchema = "cohort_schema",
        databaseId = "test_db",
        exportFolder = exportFolder
    )

    expect_s3_class(context, "DiagnosticsContext")
    expect_equal(context$cdmDatabaseSchema, "cdm_schema")
    expect_equal(context$cohortDatabaseSchema, "cohort_schema")
    expect_equal(context$databaseId, "test_db")
    expect_equal(context$exportFolder, normalizePath(exportFolder))
    expect_false(context$isInitialized)
    expect_equal(context$cdmVersion, 5)
    expect_equal(context$minCellCount, 5) # Default
})

test_that("createDiagnosticsContext validation allows missing connection (validation happens at initialization)", {
    exportFolder <- tempfile("export")

    expect_error(
        createDiagnosticsContext(
            # connectionDetails missing
            cdmDatabaseSchema = "cdm_schema",
            cohortDatabaseSchema = "cohort_schema",
            databaseId = "test_db",
            exportFolder = exportFolder
        ),
        NA
    )
})

test_that("createDiagnosticsContext accepts custom parameters", {
    exportFolder <- tempfile("export")
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    context <- createDiagnosticsContext(
        connectionDetails = list(),
        cdmDatabaseSchema = "cdm_schema",
        cohortDatabaseSchema = "cohort_schema",
        cohortTable = "my_cohort_table",
        databaseId = "test_db",
        databaseName = "My Database",
        databaseDescription = "A test database",
        exportFolder = exportFolder,
        minCellCount = 10,
        incremental = TRUE,
        cdmVersion = 5
    )

    expect_equal(context$cohortTable, "my_cohort_table")
    expect_equal(context$databaseName, "My Database")
    expect_equal(context$databaseDescription, "A test database")
    expect_equal(context$minCellCount, 10)
    expect_true(context$incremental)
})

test_that("initializeDiagnostics initializes context correctly using mocks", {
    # Skip if testthat version is too old for local_mocked_bindings
    skip_if_not_installed("testthat", "3.0.0")

    exportFolder <- tempfile("export")
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    # Mock data
    mockCdmSource <- list(
        cdmSourceName = "Test DB",
        sourceDescription = "Description",
        sourceReleaseDate = "2023-01-01",
        cdmVersion = "5.3",
        cdmReleaseDate = "2023-01-01",
        vocabularyVersion = "v5"
    )

    mockObsPeriod <- data.frame(
        observationPeriodMinDate = as.Date("2020-01-01"),
        observationPeriodMaxDate = as.Date("2022-12-31"),
        persons = 100,
        records = 1000,
        personDays = 10000
    )

    mockCohortCounts <- data.frame(
        cohortId = 1,
        cohortEntries = 100,
        cohortSubjects = 100
    )

    # Mock internal functions
    local_mocked_bindings(
        getCdmDataSourceInformation = function(...) mockCdmSource,
        getVocabularyVersion = function(...) "v5",
        computeChecksum = function(...) "hash123",
        renderTranslateQuerySql = function(...) mockObsPeriod,
        saveDatabaseMetaData = function(...) NULL,
        createConceptTable = function(...) NULL,
        computeCohortCounts = function(...) mockCohortCounts,
        createIfNotExist = function(...) NULL,
        makeDataExportable = function(x, ...) x,
        writeToCsv = function(...) NULL,
        getResultsDataModelSpecifications = function() {
            dplyr::tibble(
                tableName = "cohort",
                columnName = c("cohort_id", "cohort_name", "json", "sql"), # Simplified
                isRequired = "Yes"
            )
        },
        .package = "CohortDiagnostics"
    )

    # Create context
    context <- createDiagnosticsContext(
        connectionDetails = list(), # Mocked connection implies this won't be used to connect
        connection = "mock_conn", # Pass a mock connection object
        cdmDatabaseSchema = "cdm",
        cohortDatabaseSchema = "cohorts",
        databaseId = "test_db",
        exportFolder = exportFolder
    )

    cohortDefinitionSet <- createMockCohortDefinitionSet(numCohorts = 1)

    # Act
    context <- initializeDiagnostics(context, cohortDefinitionSet)

    # Assert
    expect_true(context$isInitialized)
    expect_equal(context$databaseName, "Test DB")
    expect_equal(context$vocabularyVersion, "v5")
    expect_equal(nrow(context$cohortCounts), 1)
})

test_that("finalizeDiagnostics cleans up and exports metadata using mocks", {
    # Skip if testthat version is too old for local_mocked_bindings
    skip_if_not_installed("testthat", "3.0.0")

    exportFolder <- tempfile("export")
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    # Create an initialized context mock state
    context <- createDiagnosticsContext(
        connectionDetails = list(),
        connection = "mock_conn",
        cdmDatabaseSchema = "cdm",
        cohortDatabaseSchema = "cohorts",
        databaseId = "test_db",
        exportFolder = exportFolder
    )
    context$isInitialized <- TRUE
    context$startTime <- Sys.time()
    context$cdmSourceInformation <- list(
        sourceDescription = "Desc",
        cdmSourceName = "Src",
        sourceReleaseDate = "2023",
        cdmVersion = "5",
        cdmReleaseDate = "2023",
        vocabularyVersion = "v5"
    )
    context$observationPeriodDateRange <- list(
        observationPeriodMinDate = "2022-01-01",
        observationPeriodMaxDate = "2022-12-31",
        persons = 100,
        records = 100,
        personDays = 1000
    )
    context$databaseName <- "TestDB"
    context$databaseDescription <- "Test Description"
    context$vocabularyVersion <- "v5"
    context$createdConnection <- FALSE # Don't try to disconnect

    # Mock internal functions
    local_mocked_bindings(
        exportConceptInformation = function(...) NULL,
        writeResultsZip = function(...) NULL,
        makeDataExportable = function(x, ...) x,
        writeToCsv = function(...) NULL,
        .package = "CohortDiagnostics"
    )

    # Mock DatabaseConnector functions in initialized context if possible,
    # but since calling pkg::fun, we might need to rely on dummy objects behaving nicely or errors ignored.
    # initializeDiagnostics test passed because it called local mocked functions.
    # finalizeDiagnostics calls DatabaseConnector::renderTranslateExecuteSql directly.
    # We can try to mock it via local_mocked_bindings if possible, or just let it fail/warn if safe.
    # But failing is bad.
    # We can use testthat::mock (for package calls) only if we use `with_mock` or `local_mocked_bindings` on the called package.
    # testthat 3 doesn't support mocking external packages easily without specific setup.
    # However, since we provided "mock_conn" as connection, DatabaseConnector will likely throw error if called on it.

    # Workaround: Mock DatabaseConnector::renderTranslateExecuteSql by defining it in the test environment
    # and hoping R's search path finds it? No, namespaced calls are strict.

    # However, we can use `testthat::with_mock` (legacy) which works on namespaced calls sometimes.
    # Or, verify if we can mock `DatabaseConnector` functions via `local_mocked_bindings(..., .package="DatabaseConnector")`.
    # This requires `DatabaseConnector` to be loaded.

    # Let's try mocking DatabaseConnector functions
    local_mocked_bindings(
        renderTranslateExecuteSql = function(...) NULL,
        disconnect = function(...) NULL,
        .package = "DatabaseConnector"
    )

    # Act
    result <- finalizeDiagnostics(context)

    # Assert
    expect_s3_class(result, "DiagnosticsContext")
})
