library(CohortDiagnostics)
options(fftempdir = "c:/FFtemp")

#Testing new logger
ParallelLogger::addDefaultErrorReportLogger()

# PDW --------------------------------------------------------
connectionDetails <- createConnectionDetails(dbms = "pdw",
                                             server = Sys.getenv("PDW_SERVER"),
                                             port = Sys.getenv("PDW_PORT"))
oracleTempSchema <- NULL
workDatabaseSchema <- "scratch.dbo"


# Using private cohort table: cdmDatabaseSchema <- 'CDM_IBM_MDCR_V1062.dbo'
cdmDatabaseSchema <- "CDM_jmdc_v1063.dbo"
cohortDatabaseSchema <- workDatabaseSchema
resultsDatabaseSchema <- workDatabaseSchema
cohortTable <- "mschuemi_temp"
databaseId <- "JMDC"

# Using ATLAS cohort table:
cohortDatabaseSchema <- "CDM_IBM_MDCR_V1062.dbo"
resultsDatabaseSchema <- "CDM_IBM_MDCR_V1062.ohdsi_results"

# RedShift --------------------------------------------------------
connectionDetails <- createConnectionDetails(dbms = "redshift",
                                             connectionString = Sys.getenv("jmdcRedShiftConnectionString"),
                                             user = Sys.getenv("redShiftUser"),
                                             password = Sys.getenv("redShiftPassword"))
oracleTempSchema <- NULL
workDatabaseSchema <- "scratch_mschuemi"
cdmDatabaseSchema <- "cdm"
cohortDatabaseSchema <- workDatabaseSchema
resultsDatabaseSchema <- workDatabaseSchema
cohortTable <- "mschuemi_temp"
databaseId <- "JMDC"

connection <- connect(connectionDetails)
DatabaseConnector::getTableNames(connection, "scratch_mschuemI")
disconnect(connection)

baseUrl <- Sys.getenv("baseUrl")

cohortId <- 7399  # LEGEND Cardiac Arrhythmia

cohortId <- 7362  # LEGEND cardiovascular-related mortality

cohortId <- 13567  # Test cohort with two initial event criteria

cohortId <- 5665  # Zoledronic acid new users with prostate cancer (many inclusion rules)


# Cohort construction (when not using ATLAS cohorts) -------------------------------------
createCohortTable(connectionDetails = connectionDetails,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable,
                  createInclusionStatsTables = TRUE)

instantiateCohort(connectionDetails = connectionDetails,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  oracleTempSchema = oracleTempSchema,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable,
                  baseUrl = baseUrl,
                  webApiCohortId = cohortId,
                  generateInclusionStats = TRUE)

inclusionStatistics <- getInclusionStatistics(connectionDetails = connectionDetails,
                                              resultsDatabaseSchema = resultsDatabaseSchema,
                                              cohortId = cohortId,
                                              cohortTable = cohortTable)

# Source concepts -------------------------------------------------------------------------
createConceptCountsTable(connectionDetails = connectionDetails,
                         cdmDatabaseSchema = cdmDatabaseSchema,
                         conceptCountsDatabaseSchema = workDatabaseSchema)

includedSourceConcepts <- findCohortIncludedSourceConcepts(connectionDetails = connectionDetails,
                                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                                           oracleTempSchema = oracleTempSchema,
                                                           baseUrl = baseUrl,
                                                           webApiCohortId = cohortId,
                                                           byMonth = FALSE,
                                                           useSourceValues = FALSE)
system.time(
orphanConcepts <- findCohortOrphanConcepts(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           oracleTempSchema = oracleTempSchema,
                                           conceptCountsDatabaseSchema = workDatabaseSchema,
                                           baseUrl = baseUrl,
                                           webApiCohortId = cohortId)
)

# saveRDS(orphanConcepts, "c:/temp/orphanConcepts.rds")
oldOcs <- readRDS("c:/temp/orphanConcepts.rds")


# Cohort-level ------------------------------------------------------------------
counts <- getCohortCounts(connectionDetails = connectionDetails,
                          cohortDatabaseSchema = cohortDatabaseSchema,
                          cohortTable = cohortTable,
                          cohortIds = cohortId) 

timeDist <- getTimeDistributions(connectionDetails = connectionDetails,
                                 oracleTempSchema = oracleTempSchema,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 cohortId = cohortId)


breakdown <- breakDownIndexEvents(connectionDetails = connectionDetails,
                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                  oracleTempSchema = oracleTempSchema,
                                  baseUrl = baseUrl,
                                  webApiCohortId = cohortId,
                                  cohortDatabaseSchema = cohortDatabaseSchema,
                                  cohortTable = cohortTable,
                                  cohortId = cohortId)

incidenceRate <- getIncidenceRate(connectionDetails = connectionDetails,
                                              cohortDatabaseSchema = cohortDatabaseSchema,
                                              cohortTable = cohortTable,
                                              oracleTempSchema = oracleTempSchema,
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              firstOccurrenceOnly = TRUE,
                                              washoutPeriod = 365,
                                              cohortId = cohortId)

plotincidenceRate(incidenceRate)


overlap <- computeCohortOverlap(connectionDetails = connectionDetails,
                                cohortDatabaseSchema = cohortDatabaseSchema,
                                cohortTable = cohortTable,
                                targetCohortId = 7399,
                                comparatorCohortId = 5665)

# Cohort characteristics level ---------------------------------------------------
chars <- getCohortCharacteristics(connectionDetails = connectionDetails,
                                  oracleTempSchema = oracleTempSchema,
                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                  cohortDatabaseSchema = cohortDatabaseSchema,
                                  cohortTable = cohortTable,
                                  cohortId = cohortId)

chars1 <- getCohortCharacteristics(connectionDetails = connectionDetails,
                                   oracleTempSchema = oracleTempSchema,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                   cohortTable = cohortTable,
                                   cohortId = 7362)

chars2 <- getCohortCharacteristics(connectionDetails = connectionDetails,
                                   oracleTempSchema = oracleTempSchema,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                   cohortTable = cohortTable,
                                   cohortId = 5665)

# saveRDS(chars1, 'c:/temp/chars1.rds') saveRDS(chars2, 'c:/temp/chars2.rds')

comparison <- compareCohortCharacteristics(chars1, chars2)


# Launch Diagnostics Explorer app ----------------------------------------------
preMergeDiagnosticsFiles("C:/temp/exampleStudy")

launchDiagnosticsExplorer("C:/temp/exampleStudy")


# Launch Cohort Explorer app ---------------------------------------------------
launchCohortExplorer(connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     cohortTable = cohortTable,
                     cohortId = cohortId)



dbms <- "pdw"
user <- NULL
pw <- NULL
server <- Sys.getenv("PDW_SERVER")
port <- Sys.getenv("PDW_PORT")
oracleTempSchema <- NULL
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = pw,
                                                                port = port)

studyFolder <- "c:/BarcelonaStudyAThon"

cdmDatabaseSchema <- "cdm_synpuf_v667.dbo"
cohortDatabaseSchema <- "scratch.dbo"
cohortTable <- "barca_synpuf"
databaseId <- "Synpuf"
databaseName <- "Medicare Claims Synthetic Public Use Files (SynPUFs)"
databaseDescription <- "Medicare Claims Synthetic Public Use Files (SynPUFs) were created to allow interested parties to gain familiarity using Medicare claims data while protecting beneficiary privacy. These files are intended to promote development of software and applications that utilize files in this format, train researchers on the use and complexities of Centers for Medicare and Medicaid Services (CMS) claims, and support safe data mining innovations. The SynPUFs were created by combining randomized information from multiple unique beneficiaries and changing variable values. This randomization and combining of beneficiary information ensures privacy of health information."
folder <- file.path(studyFolder, "synpuf")


inclusionStatisticsFolder <- folder
cohortId <- 13666

packageName <- "BarcelonaStudyAThon"
exportFolder <- file.path(folder, "export")

library(CohortDiagnostics)
runStudyDiagnostics(packageName = "BarcelonaStudyAThon",
                    connectionDetails = connectionDetails,
                    cdmDatabaseSchema = cdmDatabaseSchema,
                    oracleTempSchema = oracleTempSchema,
                    cohortDatabaseSchema = cohortDatabaseSchema,
                    cohortTable = cohortTable,
                    inclusionStatisticsFolder = outputFolder,
                    exportFolder = file.path(outputFolder, "export"),
                    databaseId = databaseId,
                    databaseName = databaseName,
                    databaseDescription = databaseDescription,
                    runInclusionStatistics = TRUE,
                    runIncludedSourceConcepts = TRUE,
                    runOrphanConcepts = TRUE,
                    runBreakdownIndexEvents = TRUE,
                    runIncidenceProportion = TRUE,
                    runCohortOverlap = TRUE,
                    runCohortCharacterization = TRUE)


# Using cohort set with WebAPI ---------------------------------------------------
baseUrl <- Sys.getenv("ohdsiBaseUrl")
inclusionStatisticsFolder <- "c:/temp/incStats"
exportFolder <- "s:/noPackage/export"
cohortSetReference <- data.frame(atlasId = c(1770710, 1770713),
                                 atlasName = c("New users of ACE inhibitors as first-line monotherapy for hypertension", "Acute myocardial infarction outcome"),
                                 cohortId = c(1770710, 1770713),
                                 name = c("New_users_of_ACE_inhibitors_as_firstline_monotherapy_for_hypertension_2", "Acute_myocardial_infarction_outcome"))

createCohortTable(connectionDetails = connectionDetails,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable)

instantiateCohortSet(connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     oracleTempSchema = oracleTempSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     cohortTable = cohortTable,
                     baseUrl = baseUrl,
                     cohortSetReference = cohortSetReference,
                     generateInclusionStats = TRUE,
                     inclusionStatisticsFolder = inclusionStatisticsFolder)

runCohortDiagnostics(baseUrl = baseUrl,
                     cohortSetReference = cohortSetReference,
                     connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     oracleTempSchema = oracleTempSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     cohortTable = cohortTable,
                     inclusionStatisticsFolder = inclusionStatisticsFolder,
                     exportFolder = exportFolder,
                     databaseId = databaseId,
                     runInclusionStatistics = TRUE,
                     runIncludedSourceConcepts = TRUE,
                     runOrphanConcepts = FALSE,
                     runTimeDistributions = FALSE,
                     runBreakdownIndexEvents = TRUE,
                     runIncidenceRate = TRUE,
                     runCohortOverlap = TRUE,
                     runCohortCharacterization = FALSE,
                     minCellCount = 5)

launchDiagnosticsExplorer(exportFolder)

# Using 'external' concept counts -------------------------------------------------
cdmDatabaseSchema <- 'CDM_IBM_MDCR_V1062.dbo'
conceptCountsDatabaseSchema <- "scratch.dbo"
conceptCountsTable <- "mschuemi_concept_counts"
databaseId <- "MDCR"
exportFolder <- "s:/noPackage/export/mdcr"

createConceptCountsTable(connectionDetails = connectionDetails,
                         cdmDatabaseSchema = cdmDatabaseSchema,
                         conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                         conceptCountsTable = conceptCountsTable)

runCohortDiagnosticsUsingExternalCounts(baseUrl = baseUrl,
                                        cohortSetReference = cohortSetReference,
                                        connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        oracleTempSchema = oracleTempSchema,
                                        conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                                        conceptCountsTable = conceptCountsTable,
                                        exportFolder = exportFolder,
                                        databaseId = databaseId,
                                        runIncludedSourceConcepts = TRUE,
                                        runOrphanConcepts = TRUE,
                                        minCellCount = 5)
# connection <- DatabaseConnector::connect(connectionDetails)
# DatabaseConnector::querySql(connection, "SELECT * FROM scratch.dbo.mschuemi_concept_counts WHERE concept_id = 35207668;")


# As verification: same DB, but not using 'external' concept counts:
runCohortDiagnostics(baseUrl = baseUrl,
                     cohortSetReference = cohortSetReference,
                     connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     oracleTempSchema = oracleTempSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     cohortTable = cohortTable,
                     exportFolder = paste0(exportFolder, "_check"),
                     databaseId = paste0(databaseId, "_check"),
                     runBreakdownIndexEvents = FALSE,
                     runCohortCharacterization = FALSE,
                     runCohortOverlap = FALSE,
                     runIncidenceRate = FALSE,
                     runInclusionStatistics = FALSE,
                     runTimeDistributions = FALSE,
                     runIncludedSourceConcepts = TRUE,
                     runOrphanConcepts = TRUE,
                     minCellCount = 5)

# Test incremental mode ------------------------------------------------------------------------------

baseUrl <- Sys.getenv("ohdsiBaseUrl")
cohortSetReferenceFile <- file.path("exampleComparativeCohortStudy", "inst", "settings", "CohortsToCreate.csv")
cohortSetReference <- read.csv(cohortSetReferenceFile)
folder <- "c:/temp/cdTest"
inclusionStatisticsFolder <- file.path(folder, "incStats")
incrementalFolder <- file.path(folder, "incremental")
exportFolder <- file.path(folder, "export")
# unlink(folder, recursive = TRUE)

# Drop old cohort table:
connection <- DatabaseConnector::connect(connectionDetails)
sql <- "DROP TABLE @cohort_database_schema.@cohort_table;"
DatabaseConnector::renderTranslateExecuteSql(connection, sql, cohort_database_schema = cohortDatabaseSchema, cohort_table = cohortTable)
DatabaseConnector::disconnect(connection)

# First run subset:
subset <- cohortSetReference[c(1,3), ]
instantiateCohortSet(connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     oracleTempSchema = oracleTempSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     cohortTable = cohortTable,
                     createCohortTable = TRUE,
                     baseUrl = baseUrl,
                     cohortSetReference = subset,
                     generateInclusionStats = TRUE,
                     inclusionStatisticsFolder = inclusionStatisticsFolder,
                     incremental = TRUE,
                     incrementalFolder = incrementalFolder)

runCohortDiagnostics(connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     oracleTempSchema = oracleTempSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     cohortTable = cohortTable,
                     baseUrl = baseUrl,
                     cohortSetReference = subset,
                     exportFolder = exportFolder,
                     databaseId = databaseId,
                     runInclusionStatistics = TRUE,
                     runIncludedSourceConcepts = TRUE,
                     runOrphanConcepts = TRUE,
                     runTimeDistributions = TRUE,
                     runBreakdownIndexEvents = TRUE,
                     runIncidenceRate = TRUE,
                     runCohortOverlap = TRUE,
                     runCohortCharacterization = TRUE,
                     inclusionStatisticsFolder = inclusionStatisticsFolder,
                     incremental = TRUE,
                     incrementalFolder = incrementalFolder)

# Then run all:
instantiateCohortSet(connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     oracleTempSchema = oracleTempSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     cohortTable = cohortTable,
                     createCohortTable = TRUE,
                     baseUrl = baseUrl,
                     cohortSetReference = cohortSetReference,
                     generateInclusionStats = TRUE,
                     inclusionStatisticsFolder = inclusionStatisticsFolder,
                     incremental = TRUE,
                     incrementalFolder = incrementalFolder)

runCohortDiagnostics(connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     oracleTempSchema = oracleTempSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     cohortTable = cohortTable,
                     baseUrl = baseUrl,
                     cohortSetReference = cohortSetReference,
                     exportFolder = exportFolder,
                     databaseId = databaseId,
                     runInclusionStatistics = TRUE,
                     runIncludedSourceConcepts = TRUE,
                     runOrphanConcepts = TRUE,
                     runTimeDistributions = TRUE,
                     runBreakdownIndexEvents = TRUE,
                     runIncidenceRate = TRUE,
                     runCohortOverlap = TRUE,
                     runCohortCharacterization = TRUE,
                     inclusionStatisticsFolder = inclusionStatisticsFolder,
                     incremental = TRUE,
                     incrementalFolder = incrementalFolder)

preMergeDiagnosticsFiles(exportFolder)


launchDiagnosticsExplorer(exportFolder)

