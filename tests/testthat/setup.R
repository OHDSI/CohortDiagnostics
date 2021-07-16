# Function to check if environment variable exists that works across Windows and Linux systems
env_exists <- function(varname) {
   Sys.getenv(varname,unset = "") != ""
}

# Function which warns user if an expected environment variable is not defined
check_env <- function(varname) {
  if (!env_exists(varname)) {
    warning(sprintf("Environment variable %s is not defined",varname))
  }
}

# Check for expected environment variables
check_env("CDM5_POSTGRESQL_SERVER")
check_env("CDM5_POSTGRESQL_USER")
check_env("CDM5_POSTGRESQL_PASSWORD")
check_env("CDM5_POSTGRESQL_CDM_SCHEMA")
check_env("CDM5_POSTGRESQL_OHDSI_SCHEMA")

# On GitHub Actions, only run database tests on MacOS to avoid overloading database server:
runDatabaseTests <- env_exists("CDM5_POSTGRESQL_SERVER") &&
 (!env_exists("GITHUB_ACTIONS") || Sys.getenv("RUNNER_OS", unset = "") == "macOS") 

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
  cohortDiagnosticsSchema <- "cohort_diagnostics"
  tempEmulationSchema <- NULL
  cohortDatabaseSchema <- "cohort_diagnostics"
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
