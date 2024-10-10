library(CohortDiagnostics)
library(testthat)

dbmsToTest <- c(
  "sqlite"#,
  # "duckdb"#,
  # "postgresql"#,
  # "redshift",
  # "sql server",
)

useAllCovariates <- FALSE

# Download the JDBC drivers used in the tests ----------------------------------
if (Sys.getenv("DATABASECONNECTOR_JAR_FOLDER") == "") stop("set the enviroment variable DATABASECONNECTOR_JAR_FOLDER")

if (Sys.getenv("DONT_DOWNLOAD_JDBC_DRIVERS", "") != "TRUE") {
  oldJarFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = tempfile("jdbcDrivers"))
  dir.create(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"))
  
  if ("postgresql" %in% dbmsToTest) downloadJdbcDrivers("postgresql")
  if ("sql server" %in% dbmsToTest) downloadJdbcDrivers("sql server")
  if ("oracle" %in% dbmsToTest) downloadJdbcDrivers("oracle")
  if ("redshift" %in% dbmsToTest) downloadJdbcDrivers("redshift")
  if ("spark" %in% dbmsToTest) downloadJdbcDrivers("spark")
  if ("snowflake" %in% dbmsToTest) downloadJdbcDrivers("snowflake")
  if ("bigquery" %in% dbmsToTest) downloadJdbcDrivers("snowflake")

  if (testthat::is_testing()) {
    withr::defer({
      unlink(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"), recursive = TRUE, force = TRUE)
      Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = oldJarFolder)
    },
    testthat::teardown_env()
    )
  }
}

temporalCovariateSettings <- FeatureExtraction::createTemporalCovariateSettings(
  useConditionOccurrence = TRUE,
  temporalStartDays = c(-365, -30, 0, 1, 31),
  temporalEndDays = c(-31, -1, 0, 30, 365)
)
cohortTableName <- "cohortdiagnostics_v330_cohort"

# minCellCountValue <- 5
skipCdmTests <- FALSE

# testServers list contains all the parameters to run each test file on each database
testServers <- list()
if ("sqlite" %in% dbmsToTest) {
  cohortIds = c(17492, 17493, 17720, 14909, 18342, 18345, 18346, 18347, 18348, 18349, 18350, 14906)
  testServers[["sqlite"]] <- list(
    connectionDetails = Eunomia::getEunomiaConnectionDetails(),
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    vocabularyDatabaseSchema = "main",
    useAchilles = FALSE,
    cohortTable = cohortTableName,
    tempEmulationSchema = NULL,
    cohortIds = cohortIds,
    cohortDefinitionSet = loadTestCohortDefinitionSet(cohortIds),
    temporalCovariateSettings = temporalCovariateSettings
  )
} 

if ("duckdb" %in% dbmsToTest) {
  
  synpufDuckdbPath <- Sys.getenv("SYNPUF_DUCKDB_PATH")
  
  # download.file("https://example-data.ohdsi.dev/synpuf-54.duckdb", "synpuf-1k_54.duckdb")
  if (synpufDuckdbPath == "" || !file.exists(synpufDuckdbPath)) {
    stop('Please run `download.file("https://example-data.ohdsi.dev/synpuf-54.duckdb", "synpuf-1k_54.duckdb")`,
          and set the SYNPUF_DUCKDB_PATH to the location of the file.')
  }
  
  cohortIds <- c(17492, 17493, 17720, 14909, 18342, 18345, 18346, 18347, 18348, 18349, 18350, 14906)
  
  testServers[["duckdb"]] <- list(
    connectionDetails = DatabaseConnector::createConnectionDetails(dbms = "duckdb", server = synpufDuckdbPath),
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    vocabularyDatabaseSchema = "main",
    useAchilles = TRUE,
    achillesDatabaseSchema = "achilles",
    cohortTable = cohortTableName,
    tempEmulationSchema = NULL,
    cohortIds = cohortIds,
    cohortDefinitionSet = loadTestCohortDefinitionSet(cohortIds),
    temporalCovariateSettings = temporalCovariateSettings
  )
} 

if ("postgresql" %in% dbmsToTest) {
  cohortIds <- c(18345, 17720, 14907)
  testServers[["postgresql"]] <- list(
    connectionDetails = DatabaseConnector::createConnectionDetails(
      dbms = "postgresql",
      user = Sys.getenv("CDM5_POSTGRESQL_USER"),
      password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
      server = Sys.getenv("CDM5_POSTGRESQL_SERVER")
    ),
    cdmDatabaseSchema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"),
    tempEmulationSchema = NULL,
    cohortDatabaseSchema = Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA"),
    cohortIds = cohortIds,
    cohortDefinitionSet = loadTestCohortDefinitionSet(cohortIds),
    cohortTable = cohortTableName,
    temporalCovariateSettings = temporalCovariateSettings
  )
}

if ("oracle" %in% dbmsToTest) {
  cohortIds <- c(18345, 17720, 14907)
  
  testServers[["oracle"]] <- list(
    connectionDetails = DatabaseConnector::createConnectionDetails(
      dbms = "oracle",
      user = Sys.getenv("CDM5_ORACLE_USER"),
      password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
      server = Sys.getenv("CDM5_ORACLE_SERVER")
    ),
    cdmDatabaseSchema = Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"),
    tempEmulationSchema = Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA"),
    cohortDatabaseSchema = Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA"),
    cohortIds = cohortIds,
    cohortDefinitionSet = loadTestCohortDefinitionSet(cohortIds),
    cohortTable = cohortTableName,
    temporalCovariateSettings = temporalCovariateSettings
  )
}

if ("redshift" %in% dbmsToTest) {
  cohortIds <- c(18345, 17720, 14907)
  
  testServers[["redshift"]] <- list(
    connectionDetails = DatabaseConnector::createConnectionDetails(
      dbms = "redshift",
      user = Sys.getenv("CDM5_REDSHIFT_USER"),
      password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
      server = Sys.getenv("CDM5_REDSHIFT_SERVER")
    ),
    cdmDatabaseSchema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"),
    tempEmulationSchema = NULL,
    cohortDatabaseSchema = Sys.getenv("CDM5_REDSHIFT_OHDSI_SCHEMA"),
    cohortIds = cohortIds,
    cohortDefinitionSet = loadTestCohortDefinitionSet(cohortIds),
    cohortTable = cohortTableName,
    temporalCovariateSettings = temporalCovariateSettings
  )
}

if ("sql server" %in% dbmsToTest) {
  cohortIds <- c(18345, 17720, 14907)
  testServers[["sql server"]] <- list(
    connectionDetails = DatabaseConnector::createConnectionDetails(
      dbms = "sql server",
      user = Sys.getenv("CDM5_SQL_SERVER_USER"),
      password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
      server = Sys.getenv("CDM5_SQL_SERVER_SERVER")
    ),
    cdmDatabaseSchema = Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"),
    vocabularyDatabaseSchema = Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"),
    tempEmulationSchema = NULL,
    cohortDatabaseSchema = Sys.getenv("CDM5_SQL_SERVER_OHDSI_SCHEMA"),
    cohortIds = cohortIds,
    cohortDefinitionSet = loadTestCohortDefinitionSet(cohortIds),
    cohortTable = cohortTableName,
    temporalCovariateSettings = temporalCovariateSettings
  )
}

# generate cohorts on databases if they don't already exist
# If the cohort table already exists then we skip generation and use the cohort table in the database
for (nm in names(testServers)) {
  server <- testServers[[nm]]
  con <- DatabaseConnector::connect(server$connectionDetails)
  tablesInCohortSchema <- DatabaseConnector::getTableNames(con, databaseSchema = server$cohortDatabaseSchema)
  DatabaseConnector::disconnect(con)
  
  if (!(cohortTableName %in% tablesInCohortSchema)) {
    message(paste("Generating cohorts on", nm, "test database"))
    cohortDefinitionSet <- loadTestCohortDefinitionSet(server$cohortIds)

    cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = server$cohortTable)

    CohortGenerator::createCohortTables(
      connectionDetails = server$connectionDetails,
      cohortTableNames = cohortTableNames,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      incremental = FALSE
    )

    CohortGenerator::generateCohortSet(
      connectionDetails = server$connectionDetails,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      cohortDefinitionSet = cohortDefinitionSet,
      incremental = FALSE
    )
  } else {
    message(paste("Skipping cohort generation on test server", nm))
  }
  
}
