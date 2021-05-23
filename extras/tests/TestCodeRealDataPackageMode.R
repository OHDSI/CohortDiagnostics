# remotes::install_github('OHDSI/SkeletonCohortDiagnosticsStudy')

library(CohortDiagnostics)
library('SkeletonCohortDiagnosticsStudy')
packageName <- 'SkeletonCohortDiagnosticsStudy'

outputLocation <- "D:\\temp"

connectionSpecifications <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'jmdc')

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
# cohortDatabaseSchema = paste0('scratch_rao_', databaseId)
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
  file.path(outputLocation, "outputFolder", 'packageMode', databaseId)
# Please delete previous content if needed
# unlink(x = outputFolder,
#        recursive = TRUE,
#        force = TRUE)
dir.create(path = outputFolder,
           showWarnings = FALSE,
           recursive = TRUE)

dataSouceInformation <-
  getDataSourceInformation(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabDatabaseSchema = vocabDatabaseSchema
  )

execute(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  vocabularyDatabaseSchema = vocabDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  outputFolder = outputFolder,
  databaseId = databaseId,
  databaseName = dataSouceInformation$cdmSourceName,
  databaseDescription = dataSouceInformation$sourceDescription
)

CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)

CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)

# connectionDetailsToUpload <- createConnectionDetails(dbms = "postgresql",
#                                                      server = paste(Sys.getenv("shinydbServer"),
#                                                                     Sys.getenv("shinydbDatabase"),
#                                                                     sep = "/"),
#                                                      port = Sys.getenv("shinydbPort"),
#                                                      user = Sys.getenv("shinyDbUser"),
#                                                      password = Sys.getenv("shinyDbPassword"))
# 
# 
# resultsSchema <- "CdSkeletonCohortDiagnosticsStudy"
# CohortDiagnostics::createResultsDataModel(connectionDetails = connectionDetailsToUpload, schema = resultsSchema)
# 
# 
# path = outputFolder
# zipFilesToUpload <- list.files(path = path,
#                                pattern = ".zip",
#                                recursive = TRUE,
#                                full.names = TRUE)
# 
# for (i in (1:length(zipFilesToUpload))) {
#   CohortDiagnostics::uploadResults(connectionDetails = connectionDetailsToUpload,
#                                    schema = resultsSchema,
#                                    zipFileName = zipFilesToUpload[[i]])
# }
