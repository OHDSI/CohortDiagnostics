library(CohortDiagnostics)
options(fftempdir = "c:/FFtemp")
connectionDetails <- createConnectionDetails(dbms = "pdw",
                                             server = Sys.getenv("PDW_SERVER"),
                                             port = Sys.getenv("PDW_PORT"),
                                             schema = "CDM_Truven_MDCR_V415")
oracleTempSchema <- NULL
workDatabaseSchema <- "scratch.dbo"
baseUrl <- Sys.getenv("baseUrl")

# Using private cohort table: cdmDatabaseSchema <- 'CDM_IBM_MDCR_V1062.dbo'
cdmDatabaseSchema <- "CDM_jmdc_v1063.dbo"
cohortDatabaseSchema <- workDatabaseSchema
resultsDatabaseSchema <- workDatabaseSchema
cohortTable <- "mschuemi_temp"

# Using ATLAS cohort table:
cohortDatabaseSchema <- "CDM_IBM_MDCR_V1062.dbo"
resultsDatabaseSchema <- "CDM_IBM_MDCR_V1062.ohdsi_results"

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

orphanConcepts <- findCohortOrphanConcepts(connectionDetails = connectionDetails,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           oracleTempSchema = oracleTempSchema,
                                           conceptCountsDatabaseSchema = workDatabaseSchema,
                                           baseUrl = baseUrl,
                                           webApiCohortId = cohortId)

# Cohort-level ------------------------------------------------------------------
counts <- getCohortCounts(connectionDetails = connectionDetails,
                          oracleTempSchema = oracleTempSchema,
                          cdmDatabaseSchema = cdmDatabaseSchema,
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

incidenceProportion <- getIncidenceProportion(connectionDetails = connectionDetails,
                                              cohortDatabaseSchema = cohortDatabaseSchema,
                                              cohortTable = cohortTable,
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              firstOccurrenceOnly = TRUE,
                                              minObservationTime = 365,
                                              cohortId = cohortId)

plotIncidenceProportionByYear(incidenceProportion)

plotIncidenceProportion(incidenceProportion, restrictToFullAgeData = F)

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
                     cohortId = 1770710)



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
