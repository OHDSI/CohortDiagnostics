# Package-owned behavior test run against the DBMS selected by HADES_TEST_DBMS.
# The assertions are identical for every platform declared in
# inst/test-config/hades-database-platforms.yml.

test_that("executeDiagnostics runs every diagnostic on the selected database", {
  skip_on_cran()

  config <- Panoptes::readDatabaseTestConfig(package = "CohortDiagnostics")
  localPanoptesSqliteFixture(config)
  Panoptes::skipIfNoLiveDatabase(config)

  dbms <- Panoptes::getSelectedTestDbms(config)
  Panoptes::withDatabaseTestContext(config, verify = TRUE, legacyAliases = panoptesLegacyAliases(dbms), {
    # Panoptes' generated names are too long to carry derived table suffixes on
    # Oracle (30 characters), so build a short unique base that starts with a letter.
    base <- sprintf("cd%s%04d", format(Sys.time(), "%d%H%M%S"), sample(0:9999, 1))
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
    cohortDefinitionSet <- loadTestCohortDefinitionSet(c(18345, 17720, 14907), useSubsets = FALSE)

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

    # The purpose of this test is to prove the SQL runs on each platform, so every
    # diagnostic is enabled. Compact covariate settings keep runtime reasonable.
    temporalCovariateSettings <- FeatureExtraction::createTemporalCovariateSettings(
      useConditionOccurrence = TRUE,
      useCharlsonIndex = TRUE,
      temporalStartDays = c(-1, 0, 1),
      temporalEndDays = c(-1, 0, 1)
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
      runTimeSeries = TRUE,
      runVisitContext = TRUE,
      runBreakdownIndexEvents = TRUE,
      runIncidenceRate = TRUE,
      runCohortRelationship = TRUE,
      runTemporalCohortCharacterization = TRUE,
      temporalCovariateSettings = temporalCovariateSettings,
      runFeatureExtractionOnSample = TRUE,
      minCharacterizationMean = 0.8,
      minCellCount = 5,
      incremental = FALSE
    )

    zipFile <- file.path(exportFolder, "Results_panoptes.zip")
    expect_true(file.exists(zipFile))
    exported <- utils::unzip(zipFile, list = TRUE)$Name
    expectedFiles <- c(
      "cohort.csv", "database.csv", "cohort_count.csv", "concept_sets.csv",
      "incidence_rate.csv", "index_event_breakdown.csv", "time_series.csv",
      "visit_context.csv", "temporal_covariate_ref.csv", "temporal_covariate_value.csv"
    )
    expect_setequal(intersect(expectedFiles, exported), expectedFiles)

    unzipFolder <- withr::local_tempdir()
    utils::unzip(zipFile, exdir = unzipFolder)
    cohorts <- readr::read_csv(file.path(unzipFolder, "cohort.csv"), show_col_types = FALSE)
    expect_setequal(cohorts$cohort_id, c(18345, 17720, 14907))

    Panoptes::requireDatabaseTestConnectionAttempted(context)
  })
})
