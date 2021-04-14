source(Sys.getenv("startUpScriptLocation"))
library(CohortDiagnostics)
library('examplePackagePhenotypeLibrary')

# Connection specification
connectionSpecifications <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'truven_ccae')
dbms <- connectionSpecifications$dbms
port <- connectionSpecifications$port
server <- connectionSpecifications$server
cdmDatabaseSchema <- connectionSpecifications$cdmDatabaseSchema
vocabDatabaseSchema <- connectionSpecifications$vocabDatabaseSchema
databaseId <- connectionSpecifications$database
tempEmulationSchema <- NULL
cohortDatabaseSchema = 'scratch_grao9'
userNameService = "OHDA_USER"
passwordService = "OHDA_PASSWORD"

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = dbms,
  user = keyring::key_get(service = userNameService),
  password = keyring::key_get(service = passwordService),
  port = port,
  server = server
)

ParallelLogger::addDefaultErrorReportLogger()



# Cohort construction (when not using ATLAS cohorts) -------------------------------------
createCohortTable(connectionDetails = connectionDetails,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable,
                  createInclusionStatsTables = TRUE)

instantiateCohort(connectionDetails = connectionDetails,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  tempEmulationSchema = tempEmulationSchema,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable,
                  baseUrl = baseUrl,
                  cohortId = cohortId,
                  generateInclusionStats = TRUE)

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
tempEmulationSchema <- NULL
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
                     tempEmulationSchema = tempEmulationSchema,
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
                     tempEmulationSchema = tempEmulationSchema,
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
                                        tempEmulationSchema = tempEmulationSchema,
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
                     tempEmulationSchema = tempEmulationSchema,
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
                     tempEmulationSchema = tempEmulationSchema,
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
                     tempEmulationSchema = tempEmulationSchema,
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
                     tempEmulationSchema = tempEmulationSchema,
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
                     tempEmulationSchema = tempEmulationSchema,
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

