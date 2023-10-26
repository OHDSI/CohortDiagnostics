library(testthat)
library(CohortDiagnostics)
library(Eunomia)
library(dplyr)

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

cohortDefinitionSet <- loadTestCohortDefinitionSet(cohortIds)
