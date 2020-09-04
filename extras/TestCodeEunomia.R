# Disabling until new version of DatabaseConnector is released:
library(testthat)
library(CohortDiagnostics)
library(Eunomia)

baseUrl <- Sys.getenv("OHDSIbaseUrl")

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cohortTable <- "cohort"
oracleTempSchema <- NULL
folder <- "d:\\eunomia" #tempfile()
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

# debug(CohortDiagnostics::runCohortDiagnostics)
# debug(CohortDiagnostics::breakDownIndexEvents)

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
                                        incrementalFolder = file.path(folder, "incremental"))

CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = file.path(folder, "export"))

