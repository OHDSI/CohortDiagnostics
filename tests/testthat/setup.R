jdbcDriverFolder <- tempfile("jdbcDrivers")
dir.create(jdbcDriverFolder, showWarnings = FALSE)
DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = jdbcDriverFolder)
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  user = Sys.getenv("CDM5_POSTGRESQL_USER"),
  password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
  server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
  pathToDriver = jdbcDriverFolder)

cdmDatabaseSchema <- 'eunomia' #Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
vocabularyDatabaseSchema <- 'eunomia' #Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
cohortDiagnosticsSchema <- Sys.getenv("CDM5_POSTGRESQL_COHORT_DIAGNOSTICS_SCHEMA")
tempEmulationSchema <- NULL
# Temp Random Schema to prevent collisions/issues when multiple test instances run
cohortDatabaseSchema <-  Sys.getenv("CDM5_POSTGRESQL_COHORT_DIAGNOSTICS_SCHEMA")

cohortTable <- paste0("cd_test_", .Platform$OS.type, stringi::stri_rand_strings(1,9))
folder <- tempfile(paste0("cd_test_", .Platform$OS.type, stringi::stri_rand_strings(1,9)))

if (stringr::str_detect(string = tolower(.Platform$OS.type), pattern = "mac")) {
  withr::defer({
    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 "DROP TABLE @cohort_database_schema.@cohort_table CASCADE",
                                                 cohort_database_schema = cohortDatabaseSchema,
                                                 cohort_table = cohortTable)
    DatabaseConnector::disconnect(connection)
    unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
    unlink(folder, recursive = TRUE, force = TRUE)
  }, testthat::teardown_env())
}
