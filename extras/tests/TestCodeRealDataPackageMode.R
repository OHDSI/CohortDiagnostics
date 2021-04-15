# remotes::install_github('OHDSI/SkeletonCohortDiagnosticsStudy')

source(Sys.getenv("startUpScriptLocation")) # this sources information for cdmSources and dataSourceInformation.

library(CohortDiagnostics)
library('SkeletonCohortDiagnosticsStudy')
packageName <- 'SkeletonCohortDiagnosticsStudy'

connectionSpecifications <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'truven_ccae')

dbms <- connectionSpecifications$dbms # example: 'redshift'
port <- connectionSpecifications$port # example: 2234
server <-
  connectionSpecifications$server # example: 'fdsfd.yourdatabase.yourserver.com"
cdmDatabaseSchema <-
  connectionSpecifications$cdmDatabaseSchema # example: "cdm"
vocabDatabaseSchema <-
  connectionSpecifications$vocabDatabaseSchema # example: "vocabulary"
databaseId <-
  connectionSpecifications$database # example: "truven_ccae"
userNameService = "OHDSI_USER" # example: "this is key ring service that securely stores credentials"
passwordService = "OHDSI_PASSWORD" # example: "this is key ring service that securely stores credentials"

cohortDatabaseSchema = paste0('scratch_', keyring::key_get(service = userNameService))
# scratch - usually something like 'scratch_grao'

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = dbms,
  user = keyring::key_get(service = userNameService),
  password = keyring::key_get(service = passwordService),
  port = port,
  server = server
)

cohortTable <- # example: 'cohort'
  paste0("s", connectionSpecifications$sourceId, "_", packageName)

outputFolder <-
  file.path(rstudioapi::getActiveProject(), "outputFolder", databaseId)
unlink(x = outputFolder,
       recursive = TRUE,
       force = TRUE)
dir.create(path = outputFolder,
           showWarnings = FALSE,
           recursive = TRUE)

dataSouceInformation <-
  getDataSourceInformation(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabDatabaseSchema = vocabDatabaseSchema
  )

SkeletonCohortDiagnosticsStudy::runCohortDiagnostics(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  vocabularyDatabaseSchema = vocabDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  outputFolder = outputFolder,
  databaseId = databaseId,
  databaseName = dataSouceInformation$cdmSourceName,
  databaseDescription = dataSouceInformation$sourceDescription,
  runCohortCharacterization = TRUE,
  runCohortOverlap = TRUE,
  runOrphanConcepts = TRUE,
  runVisitContext = TRUE,
  runIncludedSourceConcepts = TRUE,
  runTimeDistributions = TRUE,
  runTemporalCohortCharacterization = TRUE,
  runBreakdownIndexEvents = TRUE,
  runInclusionStatistics = TRUE,
  runIncidenceRates = TRUE,
  createCohorts = TRUE,
  minCellCount = 0
)

CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)

CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)

# connectionDetailsToUpload <- createConnectionDetails(dbms = "postgresql",
#                                              server = paste(Sys.getenv("shinydbServer"),
#                                                             Sys.getenv("shinydbDatabase"),
#                                                             sep = "/"),
#                                              port = Sys.getenv("shinydbPort"),
#                                              user = Sys.getenv("shinyDbUserGowtham"),
#                                              password = Sys.getenv("shinyDbPasswordGowtham"))
#
#
# resultsSchema <- "SkeletonCohortDiagnosticsStudyCdTruven"
# createResultsDataModel(connectionDetails = connectionDetailsToUpload, schema = resultsSchema)
#
#
# path = outputFolder
# zipFilesToUpload <- list.files(path = path,
#                                pattern = ".zip",
#                                recursive = TRUE,
#                                full.names = TRUE)
#
# for (i in (1:length(zipFilesToUpload))) {
#   uploadResults(connectionDetails = connectionDetailsToUpload,
#                 schema = resultsSchema,
#                 zipFileName = zipFilesToUpload[[i]])
# }
