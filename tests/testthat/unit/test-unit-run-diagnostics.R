test_that("executeDiagnostics calls expected sub-diagnostics based on flags", {
    skip_if_not_installed("testthat", "3.0.0")

    exportFolder <- tempfile("export")
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    # Track which functions were called
    calls <- list()

    # Mock all sub-diagnostic functions
    local_mocked_bindings(
        createDiagnosticsContext = function(...) {
            calls[[length(calls) + 1]] <<- "createDiagnosticsContext"
            list(databaseId = "test")
        },
        initializeDiagnostics = function(context, ...) {
            calls[[length(calls) + 1]] <<- "initializeDiagnostics"
            context$isInitialized <- TRUE
            context
        },
        runInclusionStatisticsDiagnostic = function(...) {
            calls[[length(calls) + 1]] <<- "runInclusionStatisticsDiagnostic"
        },
        runConceptSetDiagnostic = function(...) {
            calls[[length(calls) + 1]] <<- "runConceptSetDiagnostic"
        },
        runTimeSeriesDiagnostic = function(...) {
            calls[[length(calls) + 1]] <<- "runTimeSeriesDiagnostic"
        },
        runVisitContextDiagnostic = function(...) {
            calls[[length(calls) + 1]] <<- "runVisitContextDiagnostic"
        },
        runIncidenceRateDiagnostic = function(...) {
            calls[[length(calls) + 1]] <<- "runIncidenceRateDiagnostic"
        },
        runTemporalCharacterizationDiagnostic = function(...) {
            calls[[length(calls) + 1]] <<- "runTemporalCharacterizationDiagnostic"
        },
        finalizeDiagnostics = function(...) {
            calls[[length(calls) + 1]] <<- "finalizeDiagnostics"
        },
        .package = "CohortDiagnostics"
    )

    cohortDefinitionSet <- createMockCohortDefinitionSet(numCohorts = 1)

    # Run with all diagnostics enabled
    executeDiagnostics(
        cohortDefinitionSet = cohortDefinitionSet,
        exportFolder = exportFolder,
        databaseId = "test",
        cdmDatabaseSchema = "cdm",
        cohortDatabaseSchema = "cohort",
        runInclusionStatistics = TRUE,
        runIncludedSourceConcepts = TRUE,
        runOrphanConcepts = TRUE,
        runBreakdownIndexEvents = TRUE,
        runTimeSeries = TRUE,
        runVisitContext = TRUE,
        runIncidenceRate = TRUE,
        runTemporalCohortCharacterization = TRUE
    )

    expect_true("createDiagnosticsContext" %in% calls)
    expect_true("initializeDiagnostics" %in% calls)
    expect_true("runInclusionStatisticsDiagnostic" %in% calls)
    expect_true("runConceptSetDiagnostic" %in% calls)
    expect_true("runTimeSeriesDiagnostic" %in% calls)
    expect_true("runVisitContextDiagnostic" %in% calls)
    expect_true("runIncidenceRateDiagnostic" %in% calls)
    expect_true("runTemporalCharacterizationDiagnostic" %in% calls)
    expect_true("finalizeDiagnostics" %in% calls)
})

test_that("executeDiagnostics skips diagnostics when flags are FALSE", {
    skip_if_not_installed("testthat", "3.0.0")

    exportFolder <- tempfile("export")
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))

    calls <- list()

    local_mocked_bindings(
        createDiagnosticsContext = function(...) list(databaseId = "test"),
        initializeDiagnostics = function(context, ...) {
            context$isInitialized <- TRUE
            context
        },
        runInclusionStatisticsDiagnostic = function(...) {
            calls <<- c(calls, "runInclusion")
        },
        runConceptSetDiagnostic = function(...) {
            calls <<- c(calls, "runConceptSet")
        },
        runTimeSeriesDiagnostic = function(...) {
            calls <<- c(calls, "runTimeSeries")
        },
        runVisitContextDiagnostic = function(...) {
            calls <<- c(calls, "runVisitContext")
        },
        runIncidenceRateDiagnostic = function(...) {
            calls <<- c(calls, "runIncidenceRate")
        },
        runTemporalCharacterizationDiagnostic = function(...) {
            calls <<- c(calls, "runCharacterization")
        },
        finalizeDiagnostics = function(...) NULL,
        .package = "CohortDiagnostics"
    )

    cohortDefinitionSet <- createMockCohortDefinitionSet(numCohorts = 1)

    # Run with no diagnostics enabled
    executeDiagnostics(
        cohortDefinitionSet = cohortDefinitionSet,
        exportFolder = exportFolder,
        databaseId = "test",
        cdmDatabaseSchema = "cdm",
        cohortDatabaseSchema = "cohort",
        runInclusionStatistics = FALSE,
        runIncludedSourceConcepts = FALSE,
        runOrphanConcepts = FALSE,
        runBreakdownIndexEvents = FALSE,
        runTimeSeries = FALSE,
        runVisitContext = FALSE,
        runIncidenceRate = FALSE,
        runTemporalCohortCharacterization = FALSE
    )

    expect_false("runInclusion" %in% calls)
    expect_false("runConceptSet" %in% calls)
    expect_false("runTimeSeries" %in% calls)
    expect_false("runVisitContext" %in% calls)
    expect_false("runIncidenceRate" %in% calls)
    expect_false("runCharacterization" %in% calls)
})

test_that("writeResultsZip collections CSV files", {
    exportFolder <- tempfile("export")
    dir.create(exportFolder)
    on.exit(unlink(exportFolder, recursive = TRUE))
    
    # Create some dummy csv files
    cat("a,b\n1,2", file = file.path(exportFolder, "test1.csv"))
    cat("c,d\n3,4", file = file.path(exportFolder, "test2.csv"))
    
    # Mock DatabaseConnector::createZipFile
    zipFileCalled <- NULL
    filesZipped <- NULL
    local_mocked_bindings(
        createZipFile = function(zipFile, files) {
            zipFileCalled <<- zipFile
            filesZipped <<- files
        },
        .package = "DatabaseConnector"
    )
    
    CohortDiagnostics:::writeResultsZip(exportFolder, "testDb")
    
    expect_match(zipFileCalled, "Results_testDb.zip")
    expect_true(all(c("test1.csv", "test2.csv") %in% filesZipped))
})
