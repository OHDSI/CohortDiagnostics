# Package-owned behavior test run against the DBMS selected by HADES_TEST_DBMS.
# The assertions are identical for every platform declared in
# inst/test-config/hades-database-platforms.yml.

test_that("executeDiagnostics works on the selected database", {
  skip_on_cran()

  config <- Panoptes::readDatabaseTestConfig(package = "CohortDiagnostics")
  localPanoptesSqliteFixture(config)
  Panoptes::skipIfNoLiveDatabase(config)

  dbms <- Panoptes::getSelectedTestDbms(config)
  Panoptes::withDatabaseTestContext(config, verify = TRUE, legacyAliases = panoptesLegacyAliases(dbms), {
    # Short derived names keep every table within Oracle's 30 character limit
    base <- Panoptes::createUniqueDatabaseObjectName("cd", 16)
    cohortTableNames <- CohortGenerator::getCohortTableNames(
      cohortTable = base,
      cohortSampleTable = paste0(base, "_smp"),
      cohortInclusionTable = paste0(base, "_inc"),
      cohortInclusionResultTable = paste0(base, "_ir"),
      cohortInclusionStatsTable = paste0(base, "_is"),
      cohortSummaryStatsTable = paste0(base, "_ss"),
      cohortCensorStatsTable = paste0(base, "_cs"),
      cohortSubsetAttritionTable = paste0(base, "_sa"),
      cohortChecksumTable = paste0(base, "_ck")
    )
    exportFolder <- withr::local_tempdir()

    dropTables <- function() {
      connection <- DatabaseConnector::connect(context$connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
      for (tableName in unique(unlist(cohortTableNames))) {
        DatabaseConnector::renderTranslateExecuteSql(
          connection,
          "IF OBJECT_ID('@schema.@table', 'U') IS NOT NULL DROP TABLE @schema.@table;",
          schema = context$workDatabaseSchema,
          table = tableName,
          tempEmulationSchema = context$tempEmulationSchema,
          progressBar = FALSE,
          reportOverallTime = FALSE
        )
      }
    }
    on.exit(try(dropTables(), silent = TRUE), add = TRUE)

    source(testthat::test_path("integration", "helpers-integration.R"), local = TRUE)
    cohortDefinitionSet <- loadTestCohortDefinitionSet(c(18345, 17720), useSubsets = FALSE)

    CohortGenerator::createCohortTables(
      connectionDetails = context$connectionDetails,
      cohortTableNames = cohortTableNames,
      cohortDatabaseSchema = context$workDatabaseSchema,
      incremental = FALSE
    )
    CohortGenerator::generateCohortSet(
      connectionDetails = context$connectionDetails,
      cdmDatabaseSchema = context$cdmDatabaseSchema,
      cohortDatabaseSchema = context$workDatabaseSchema,
      cohortTableNames = cohortTableNames,
      cohortDefinitionSet = cohortDefinitionSet,
      incremental = FALSE
    )

    executeDiagnostics(
      cohortDefinitionSet = cohortDefinitionSet,
      connectionDetails = context$connectionDetails,
      cdmDatabaseSchema = context$cdmDatabaseSchema,
      vocabularyDatabaseSchema = context$vocabularyDatabaseSchema,
      cohortDatabaseSchema = context$workDatabaseSchema,
      cohortTableNames = cohortTableNames,
      tempEmulationSchema = context$tempEmulationSchema,
      exportFolder = exportFolder,
      databaseId = "panoptes",
      runInclusionStatistics = TRUE,
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = TRUE,
      runBreakdownIndexEvents = TRUE,
      runIncidenceRate = TRUE,
      runTimeSeries = FALSE,
      runCohortRelationship = FALSE,
      runTemporalCohortCharacterization = FALSE,
      minCellCount = 5,
      incremental = FALSE
    )

    zipFile <- file.path(exportFolder, "Results_panoptes.zip")
    expect_true(file.exists(zipFile))
    exported <- utils::unzip(zipFile, list = TRUE)$Name
    expect_true("cohort.csv" %in% exported)
    expect_true("database.csv" %in% exported)

    unzipFolder <- withr::local_tempdir()
    utils::unzip(zipFile, exdir = unzipFolder)
    cohorts <- readr::read_csv(file.path(unzipFolder, "cohort.csv"), show_col_types = FALSE)
    expect_setequal(cohorts$cohort_id, c(18345, 17720))

    Panoptes::requireDatabaseTestConnectionAttempted(context)
  })
})
