library(testthat)
library(CohortDiagnostics)
library(Eunomia)

dbms <- getOption("dbms", default = "sqlite")
message("************* Testing on ", dbms, " *************")

if (dir.exists(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"))) {
  jdbcDriverFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
} else {
  jdbcDriverFolder <- tempfile("jdbcDrivers")
  dir.create(jdbcDriverFolder, showWarnings = FALSE)
  DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = jdbcDriverFolder)

  if (!dbms %in% c("postgresql", "sqlite")) {
    DatabaseConnector::downloadJdbcDrivers(dbms, pathToDriver = jdbcDriverFolder)
  }

  withr::defer({
    unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
  }, testthat::teardown_env())
}

folder <- tempfile()
dir.create(folder, recursive = TRUE)
minCellCountValue <- 5

if (dbms == "sqlite") {
  connectionDetails <- Eunomia::getEunomiaConnectionDetails()
  cdmDatabaseSchema <- "main"
  cohortDatabaseSchema <- "main"
  vocabularyDatabaseSchema <- cohortDatabaseSchema
  cohortTable <- "cohort"
  tempEmulationSchema <- NULL
  cohortIds <- c(17492, 17493, 17720, 14909, 18342, 18345, 18346, 18347, 18348, 18349, 18350, 14906)

  covariateSettings <- FeatureExtraction::createDefaultCovariateSettings()
  temporalCovariateSettings <- FeatureExtraction::createTemporalCovariateSettings(useConditionOccurrence = TRUE,
                                                                                  useDrugEraStart = TRUE,
                                                                                  useProcedureOccurrence = TRUE,
                                                                                  useMeasurement = TRUE,
                                                                                  temporalStartDays = c(-365, -30, 0, 1, 31),
                                                                                  temporalEndDays = c(-31, -1, 0, 30, 365))

} else {
  # only test all cohorts in sqlite
  cohortIds <- c(18345, 17720) # Celecoxib, Type 2 diabetes
  cohortTable <- paste0("ct_", gsub("[: -]", "", Sys.time(), perl = TRUE), sample(1:100, 1))

  covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsAge = TRUE, useDemographicsAgeGroup = TRUE)
  temporalCovariateSettings <- FeatureExtraction::createTemporalCovariateSettings(useConditionOccurrence = TRUE,
                                                                                  temporalStartDays = c(-1, 0, 1),
                                                                                  temporalEndDays = c(-1, 0, 1))

  if (dbms == "postgresql") {
    cohortDatabaseSchema <- paste0("cd_", gsub("[: -]", "", Sys.time(), perl = TRUE), sample(1:100, 1))
    connectionDetails <- DatabaseConnector::createConnectionDetails(
      dbms = "postgresql",
      user = Sys.getenv("CDM5_POSTGRESQL_USER"),
      password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
      server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
      pathToDriver = jdbcDriverFolder
    )

    cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
    vocabularyDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
    tempEmulationSchema <- NULL
    cohortDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA")
  } else if (dbms == "oracle") {
    connectionDetails <- DatabaseConnector::createConnectionDetails(
      dbms = "oracle",
      user = Sys.getenv("CDM5_ORACLE_USER"),
      password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
      server = Sys.getenv("CDM5_ORACLE_SERVER"),
      pathToDriver = jdbcDriverFolder
    )
    cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
    vocabularyDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
    tempEmulationSchema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
    cohortDatabaseSchema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
    options(sqlRenderTempEmulationSchema = tempEmulationSchema)
  } else if (dbms == "redshift") {
    connectionDetails <- DatabaseConnector::createConnectionDetails(
      dbms = "redshift",
      user = Sys.getenv("CDM5_REDSHIFT_USER"),
      password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
      server = Sys.getenv("CDM5_REDSHIFT_SERVER"),
      pathToDriver = jdbcDriverFolder
    )
    cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
    vocabularyDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
    tempEmulationSchema <- NULL
    cohortDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_OHDSI_SCHEMA")

  } else if (dbms == "sql server") {
    connectionDetails <- DatabaseConnector::createConnectionDetails(
      dbms = "sql server",
      user = Sys.getenv("CDM5_SQL_SERVER_USER"),
      password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
      server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
      pathToDriver = jdbcDriverFolder
    )
    cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
    vocabularyDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
    tempEmulationSchema <- NULL
    cohortDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_OHDSI_SCHEMA")
  }

  # Cleanup
  sql <- "IF OBJECT_ID('@cohort_database_schema.@cohort_table', 'U') IS NOT NULL
              DROP TABLE @cohort_database_schema.@cohort_table;"

  withr::defer({
    connection <- DatabaseConnector::connect(connectionDetails)
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 cohort_database_schema = cohortDatabaseSchema,
                                                 cohort_table = cohortTable)
    DatabaseConnector::disconnect(connection)
  }, testthat::teardown_env())
}

skipCdmTests <- FALSE
if (cdmDatabaseSchema == "") {
  skipCdmTests <- TRUE
}