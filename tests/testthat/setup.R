# Test Setup - Conditional for Integration Tests Only
# This setup only runs when INTEGRATION_TESTS environment variable is set to TRUE
# Unit tests do not require this setup and should run without database connections

library(testthat)
library(CohortDiagnostics)

# Check if we're running integration tests
if (Sys.getenv("INTEGRATION_TESTS") == "TRUE") {
  # ============================================================================
  # INTEGRATION TEST SETUP
  # ============================================================================
  # This section only runs for integration tests that require database connections

  library(Eunomia)
  library(dplyr)

  # Source integration test helpers
  source(testthat::test_path("integration/helpers-integration.R"))

  dbms <- getOption("dbms", default = "sqlite")
  message("************* Testing on ", dbms, " *************")

  if (dir.exists(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"))) {
    jdbcDriverFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  } else {
    jdbcDriverFolder <- "~/.jdbcDrivers"
    dir.create(jdbcDriverFolder, showWarnings = FALSE)
    DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = jdbcDriverFolder)

    if (!dbms %in% c("postgresql", "sqlite")) {
      DatabaseConnector::downloadJdbcDrivers(dbms, pathToDriver = jdbcDriverFolder)
    }

    withr::defer(
      {
        unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
      },
      testthat::teardown_env()
    )
  }

  folder <- tempfile()
  dir.create(folder, recursive = TRUE)
  minCellCountValue <- 5
  skipCdmTests <- FALSE

  if (dbms == "sqlite") {
    databaseFile <- paste0(Sys.getpid(), "testEunomia.sqlite")

    connectionDetails <- Eunomia::getEunomiaConnectionDetails(databaseFile = databaseFile)
    withr::defer(
      {
        unlink(databaseFile, recursive = TRUE, force = TRUE)
      },
      testthat::teardown_env()
    )
    cdmDatabaseSchema <- "main"
    cohortDatabaseSchema <- "main"
    vocabularyDatabaseSchema <- cohortDatabaseSchema
    cohortTable <- "cohort"
    tempEmulationSchema <- NULL
    cohortIds <- c(17492, 17493, 17720, 14909, 18342, 18345, 18346, 18347, 18348, 18349, 18350, 14906)


    if (getOption("useAllCovariates", default = FALSE)) {
      temporalCovariateSettings <- getDefaultCovariateSettings()
    } else {
      temporalCovariateSettings <- FeatureExtraction::createTemporalCovariateSettings(
        useConditionOccurrence = TRUE,
        useDrugEraStart = TRUE,
        useProcedureOccurrence = TRUE,
        useMeasurement = TRUE,
        useCharlsonIndex = TRUE,
        temporalStartDays = c(-365, -30, 0, 1, 31),
        temporalEndDays = c(-31, -1, 0, 30, 365)
      )
    }
  } else {
    # only test all cohorts in sqlite
    cohortIds <- c(18345, 17720, 14907) # Celecoxib, Type 2 diabetes, diclofenac (no history of GIH)
    cohortTable <- paste0("ct_", Sys.getpid(), format(Sys.time(), "%s"), sample(1:100, 1))
    if (getOption("useAllCovariates", default = FALSE)) {
      temporalCovariateSettings <- getDefaultCovariateSettings()
    } else {
      temporalCovariateSettings <- FeatureExtraction::createTemporalCovariateSettings(
        useConditionOccurrence = TRUE,
        useCharlsonIndex = TRUE,
        temporalStartDays = c(-1, 0, 1),
        temporalEndDays = c(-1, 0, 1)
      )
    }
    if (dbms == "postgresql") {
      dbUser <- Sys.getenv("CDM5_POSTGRESQL_USER")
      dbPassword <- Sys.getenv("CDM5_POSTGRESQL_PASSWORD")
      dbServer <- Sys.getenv("CDM5_POSTGRESQL_SERVER")
      cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
      tempEmulationSchema <- NULL
      cohortDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA")
    } else if (dbms == "oracle") {
      dbUser <- Sys.getenv("CDM5_ORACLE_USER")
      dbPassword <- Sys.getenv("CDM5_ORACLE_PASSWORD")
      dbServer <- Sys.getenv("CDM5_ORACLE_SERVER")
      cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
      tempEmulationSchema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
      options(sqlRenderTempEmulationSchema = tempEmulationSchema)
    } else if (dbms == "redshift") {
      dbUser <- Sys.getenv("CDM5_REDSHIFT_USER")
      dbPassword <- Sys.getenv("CDM5_REDSHIFT_PASSWORD")
      dbServer <- Sys.getenv("CDM5_REDSHIFT_SERVER")
      cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
      tempEmulationSchema <- NULL
      cohortDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_OHDSI_SCHEMA")
    } else if (dbms == "sql server") {
      dbUser <- Sys.getenv("CDM5_SQL_SERVER_USER")
      dbPassword <- Sys.getenv("CDM5_SQL_SERVER_PASSWORD")
      dbServer <- Sys.getenv("CDM5_SQL_SERVER_SERVER")
      cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
      tempEmulationSchema <- NULL
      cohortDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_OHDSI_SCHEMA")
    }

    connectionDetails <- DatabaseConnector::createConnectionDetails(
      dbms = dbms,
      user = dbUser,
      password = URLdecode(dbPassword),
      server = dbServer,
      pathToDriver = jdbcDriverFolder
    )

    if (cdmDatabaseSchema == "" || dbServer == "") {
      skipCdmTests <- TRUE
    }

    # Cleanup
    sql <- "IF OBJECT_ID('@cohort_database_schema.@cohort_table', 'U') IS NOT NULL
                DROP TABLE @cohort_database_schema.@cohort_table;"

    withr::defer(
      {
        if (!skipCdmTests) {
          connection <- DatabaseConnector::connect(connectionDetails)
          DatabaseConnector::renderTranslateExecuteSql(connection,
            sql,
            cohort_database_schema = cohortDatabaseSchema,
            cohort_table = cohortTable
          )
          DatabaseConnector::disconnect(connection)
        }
      },
      testthat::teardown_env()
    )
  }

  # Generate cohorts once only
  cohortDefinitionSet <- loadTestCohortDefinitionSet(cohortIds)

  if (!skipCdmTests) {
    cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
    # Next create the tables on the database
    CohortGenerator::createCohortTables(
      connectionDetails = connectionDetails,
      cohortTableNames = cohortTableNames,
      cohortDatabaseSchema = cohortDatabaseSchema,
      incremental = FALSE
    )

    # Generate the cohort set
    CohortGenerator::generateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      cohortDefinitionSet = cohortDefinitionSet,
      incremental = FALSE
    )
  }

  message("************* Integration test setup complete *************")
} else {
  # ============================================================================
  # UNIT TEST SETUP
  # ============================================================================
  # Minimal setup for unit tests - no database connections required

  message("************* Running unit tests (no database setup) *************")

  # During R CMD check and devtools::test(), the package is already loaded.
  # We only load from source when running interactively outside of check.
  if (Sys.getenv("R_CMD_CHECK") == "" && Sys.getenv("R_COVR") != "true") {
    if (interactive() && requireNamespace("devtools", quietly = TRUE)) {
      devtools::load_all(".")
    }
  }

  # Source fixtures and mocks for unit tests
  # Try multiple possible locations for fixture and mock directories
  find_dir <- function(name) {
    paths <- c(
      testthat::test_path(name), # tests/testthat/name or tests/testthat/unit/name
      testthat::test_path("..", name), # tests/testthat/name when run from unit/
      file.path("tests", "testthat", name) # from package root
    )
    for (p in paths) {
      if (dir.exists(p)) {
        return(p)
      }
    }
    return(NULL)
  }

  fixtureDir <- find_dir("fixtures")
  if (!is.null(fixtureDir)) {
    fixtureFiles <- list.files(fixtureDir, pattern = "\\.R$", full.names = TRUE)
    for (file in fixtureFiles) {
      source(file)
    }
  }

  # Source helper-* files (testthat convention for test helpers and mocks)
  helperFiles <- list.files(
    testthat::test_path(),
    pattern = "^helper.*\\.R$",
    full.names = TRUE
  )
  for (file in helperFiles) {
    source(file)
  }
}
