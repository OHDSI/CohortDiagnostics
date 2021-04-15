source(Sys.getenv("startUpScriptLocation"))

library(CohortDiagnostics)
library("examplePackagePhenotypeLibrary")
packageName <- "examplePackagePhenotypeLibrary"

connectionSpecifications <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == "truven_ccae")


dbms <- connectionSpecifications$dbms
port <- connectionSpecifications$port
server <- connectionSpecifications$server
cdmDatabaseSchema <- connectionSpecifications$cdmDatabaseSchema
vocabDatabaseSchema <- connectionSpecifications$vocabDatabaseSchema
databaseId <- connectionSpecifications$database
userNameService <- "OHDA_USER"
passwordService <- "OHDA_PASSWORD"

cohortDatabaseSchema <- paste0("scratch_", keyring::key_get(service = userNameService))

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                user = keyring::key_get(service = userNameService),
                                                                password = keyring::key_get(service = passwordService),
                                                                port = port,
                                                                server = server)

cohortTable <- paste0("s", connectionSpecifications$sourceId, "_", packageName)

outputFolder <- file.path(rstudioapi::getActiveProject(), "outputFolder", databaseId)
unlink(x = outputFolder, recursive = TRUE, force = TRUE)
dir.create(path = outputFolder, showWarnings = FALSE, recursive = TRUE)

dataSouceInformation <- getDataSourceInformation(connectionDetails = connectionDetails,
                                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                                 vocabDatabaseSchema = vocabDatabaseSchema)


examplePackagePhenotypeLibrary::runCohortDiagnostics(connectionDetails = connectionDetails,
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
                                                     minCellCount = 0)

CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)

CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)



# connectionDetailsToUpload <- createConnectionDetails(dbms = 'postgresql', server =
# paste(Sys.getenv('shinydbServer'), Sys.getenv('shinydbDatabase'), sep = '/'), port =
# Sys.getenv('shinydbPort'), user = Sys.getenv('shinyDbUserGowtham'), password =
# Sys.getenv('shinyDbPasswordGowtham')) resultsSchema <- 'examplePackagePhenotypeLibraryCdTruven'
# createResultsDataModel(connectionDetails = connectionDetailsToUpload, schema = resultsSchema) path
# = outputFolder zipFilesToUpload <- list.files(path = path, pattern = '.zip', recursive = TRUE,
# full.names = TRUE) for (i in (1:length(zipFilesToUpload))) { uploadResults(connectionDetails =
# connectionDetailsToUpload, schema = resultsSchema, zipFileName = zipFilesToUpload[[i]]) }
