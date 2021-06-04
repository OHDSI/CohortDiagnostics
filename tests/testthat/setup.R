jdbcDriverFolder <- tempfile("jdbcDrivers")
DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = jdbcDriverFolder)
connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
    pathToDriver = jdbcDriverFolder)

cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
vocabularyDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
cohortDiagnosticsSchema <- Sys.getenv("CDM5_POSTGRESQL_COHORT_DIAGNOSTICS_SCHEMA")
tempEmulationSchema <- NULL
# Temp Random Schema to prevent collisions/issues when multiple test instances run
cohortDatabaseSchema <- paste0("cohort_diagnostics_test", stringi::stri_rand_strings(1,5))

cohortTable <- "cohort"
connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
DatabaseConnector::renderTranslateExecuteSql(connection, "CREATE SCHEMA @cohort_database_schema", cohort_database_schema = cohortDatabaseSchema)
folder <- tempfile("cohortDiagnosticsTest")

withr::defer({
  DatabaseConnector::renderTranslateExecuteSql(connection, "DROP SCHEMA @cohort_database_schema", cohort_database_schema = cohortDatabaseSchema)
  DatabaseConnector::disconnect(connection)
  unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
  unlink(folder, recursive = TRUE, force = TRUE)
}, testthat::teardown_env())
