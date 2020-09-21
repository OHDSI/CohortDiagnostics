# Use Eunomia docker https://githib.com/azimov/EunomiaDocker

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  user = "postgres",
  password = "postgres",
  server = "localhost/eunomia",
  port = 54321
)

connection <- DatabaseConnector::connect(connectionDetails)
schemaName <- paste0("test_schema_", stringi::stri_rand_strings(1, 5))


CohortDiagnostics::buildPostgresDatabaseSchema(connectionDetails, schemaName, overwrite = TRUE)

cdmDatabaseSchema <- "eunomia"
cohortDatabaseSchema <- schemaName
cohortTable <- "cohort"
oracleTempSchema <- NULL
folder <- stringi::stri_rand_strings(1, 5)
unlink(x = folder, recursive = TRUE, force = TRUE)
dir.create(folder, recursive = TRUE, showWarnings = FALSE)


CohortDiagnostics::instantiateCohortSet(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        oracleTempSchema = oracleTempSchema,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTable = cohortTable,
                                        packageName = "CohortDiagnostics",
                                        cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
                                        generateInclusionStats = TRUE,
                                        createCohortTable = TRUE,
                                        # incremental = TRUE,
                                        # incrementalFolder = file.path(folder, "incremental"),
                                        inclusionStatisticsFolder = file.path(folder, "inclusionStatistics"))

CohortDiagnostics::runCohortDiagnostics(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        oracleTempSchema = oracleTempSchema,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTable = cohortTable,
                                        packageName = "CohortDiagnostics",
                                        cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
                                        inclusionStatisticsFolder = file.path(folder, "inclusionStatistics"),
                                        exportFolder =  file.path(folder, "export"),
                                        databaseId = "Eunomia",
                                        runInclusionStatistics = TRUE,
                                        runBreakdownIndexEvents = TRUE,
                                        runCohortCharacterization = TRUE,
                                        runTemporalCohortCharacterization = TRUE,
                                        runCohortOverlap = TRUE,
                                        runIncidenceRate = TRUE,
                                        runIncludedSourceConcepts = TRUE,
                                        runOrphanConcepts = TRUE,
                                        runTimeDistributions = TRUE,
                                        incremental = TRUE,
                                        minCellCount = 10,
                                        incrementalFolder = file.path(folder, "incremental"))

CohortDiagnostics::importCsvFilesToPostgres(connectionDetails, schemaName, file.path(folder, "export"))

DatabaseConnector::disconnect(connection)
unlink(folder, recursive=TRUE)