library(testthat)
library(CohortDiagnostics)
library(Eunomia)

baseUrl <- Sys.getenv("OHDSIbaseUrl")

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cohortTable <- "cohort"
oracleTempSchema <- NULL
folder <- "D:\\git\\data"
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
                                        inclusionStatisticsFolder = file.path(folder, "incStats"))

undebug(CohortDiagnostics::runCohortDiagnostics)
debug(CohortDiagnostics:::resolveUniqueConceptIds)
debug(CohortDiagnostics:::getOmopVocabularyTables)
debug(CohortDiagnostics::runConceptSetDiagnostics)
CohortDiagnostics::runCohortDiagnostics(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        oracleTempSchema = oracleTempSchema,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTable = cohortTable,
                                        packageName = "CohortDiagnostics",
                                        cohortToCreateFile = "settings/CohortsToCreateForTesting.csv",
                                        inclusionStatisticsFolder = file.path(folder, "incStats"),
                                        exportFolder =  file.path(folder, "export"),
                                        databaseId = "Eunomia",
                                        runInclusionStatistics = FALSE,
                                        runBreakdownIndexEvents = TRUE,
                                        runCohortCharacterization = TRUE,
                                        runTemporalCohortCharacterization = TRUE,
                                        runCohortOverlap = FALSE,
                                        runIncidenceRate = FALSE,
                                        runIncludedSourceConcepts = TRUE,
                                        runOrphanConcepts = TRUE,
                                        runTimeDistributions = FALSE,
                                        incremental = TRUE,
                                        incrementalFolder = file.path(folder, "incremental"))
