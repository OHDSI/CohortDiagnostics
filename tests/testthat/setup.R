# On GitHub Actions, only run database tests on MacOS to avoid overloading database server:
runDatabaseTests <- Sys.getenv("CDM5_POSTGRESQL_SERVER", unset = "") != "" &&
  (Sys.getenv("GITHUB_ACTIONS", unset = "") == "" ||
  stringr::str_detect(string = tolower(.Platform$OS.type), pattern = "mac")) 

if (runDatabaseTests) {
  
  # Download the PostreSQL driver ---------------------------
  # If DATABASECONNECTOR_JAR_FOLDER exists, assume driver has been downloaded
  jdbcDriverFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER", unset = "")
  if (jdbcDriverFolder == "") {
    jdbcDriverFolder <- tempfile("jdbcDrivers")
    dir.create(jdbcDriverFolder)
    DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = jdbcDriverFolder)
    withr::defer({
      unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
    }, testthat::teardown_env())
  }
  
  # Set connection details, and schema and table names --------------------------
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
    pathToDriver = jdbcDriverFolder)
  
  cdmDatabaseSchema <- "eunomia"
  vocabularyDatabaseSchema <- "eunomia"
  cohortDiagnosticsSchema <- Sys.getenv("CDM5_POSTGRESQL_COHORT_DIAGNOSTICS_SCHEMA")
  tempEmulationSchema <- NULL
  cohortDatabaseSchema <-  Sys.getenv("CDM5_POSTGRESQL_COHORT_DIAGNOSTICS_SCHEMA")
  cohortTable <- tolower(paste0("cd_test_", gsub("[^a-zA-Z]", "", .Platform$OS.type), stringi::stri_rand_strings(1,9)))
  folder <- tempfile(paste0("cd_test_", gsub("[^a-zA-Z]", "", .Platform$OS.type), stringi::stri_rand_strings(1,9)))
  
  # Clean up when exiting testing -----------------------------------
  withr::defer({
    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 "DROP TABLE @cohort_database_schema.@cohort_table CASCADE",
                                                 cohort_database_schema = cohortDatabaseSchema,
                                                 cohort_table = cohortTable)
    DatabaseConnector::disconnect(connection)
    unlink(folder, recursive = TRUE, force = TRUE)
  }, testthat::teardown_env())
}
