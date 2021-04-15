# remotes::install_github('OHDSI/SkeletonCohortDiagnosticsStudy')

source(Sys.getenv("startUpScriptLocation")) # this sources information for cdmSources and dataSourceInformation.

library(CohortDiagnostics)

temporaryLocation <- tempdir()

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

cohortTable <-
  paste0("s", connectionSpecifications$sourceId, "_webapi")

library(magrittr)
# Set up
baseUrl <- Sys.getenv("baseUrlUnsecure")
# list of cohort ids
cohortIds <- c(18345, 18346)

# get specifications for the cohortIds above
webApiCohorts <-
  ROhdsiWebApi::getCohortDefinitionsMetaData(baseUrl = baseUrl) %>%
  dplyr::filter(.data$id %in% cohortIds)

cohortsToCreate <- list()
for (i in (1:nrow(webApiCohorts))) {
  cohortId <- webApiCohorts$id[[i]]
  cohortDefinition <-
    ROhdsiWebApi::getCohortDefinition(cohortId = cohortId,
                                      baseUrl = baseUrl)
  cohortsToCreate[[i]] <- tidyr::tibble(
    atlasId = webApiCohorts$id[[i]],
    atlasName = stringr::str_trim(string = stringr::str_squish(cohortDefinition$name)),
    cohortId = webApiCohorts$id[[i]],
    name = stringr::str_trim(stringr::str_squish(cohortDefinition$name))
  )
}
cohortsToCreate <- dplyr::bind_rows(cohortsToCreate)

readr::write_excel_csv(
  x = cohortsToCreate,
  na = "",
  file = file.path(temporaryLocation, "CohortsToCreate.csv"),
  append = FALSE
)

outputFolder <-
  file.path(temporaryLocation,
            "outputFolder",
            "webApiMode",
            "realData",
            databaseId)
# Please delete previous content if needed
# unlink(x = outputFolder,
#        recursive = TRUE,
#        force = TRUE)
dir.create(path = outputFolder,
           showWarnings = FALSE,
           recursive = TRUE)

cohortSetReference <-
  readr::read_csv(
    file = file.path(temporaryLocation, "CohortsToCreate.csv"),
    col_types = readr::cols()
  )

CohortDiagnostics::instantiateCohortSet(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  baseUrl = baseUrl,
  cohortSetReference = cohortSetReference,
  generateInclusionStats = TRUE,
  inclusionStatisticsFolder = file.path(outputFolder,
                                        'inclusionStatisticsFolder')
)

CohortDiagnostics::runCohortDiagnostics(
  baseUrl = baseUrl,
  cohortSetReference = cohortSetReference,
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  inclusionStatisticsFolder = file.path(outputFolder,
                                        'inclusionStatisticsFolder'),
  exportFolder = file.path(outputFolder,
                           'exportFolder'),
  databaseId = databaseId,
  runInclusionStatistics = TRUE,
  runIncludedSourceConcepts = TRUE,
  runOrphanConcepts = TRUE,
  runTimeDistributions = TRUE,
  runBreakdownIndexEvents = TRUE,
  runIncidenceRate = TRUE,
  runCohortOverlap = TRUE,
  runCohortCharacterization = TRUE,
  minCellCount = 5
)

CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)

CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)

# connectionDetailsToUpload <- createConnectionDetails(dbms = "postgresql",
#                                              server = paste(Sys.getenv("shinydbServer"),
#                                                             Sys.getenv("shinydbDatabase"),
#                                                             sep = "/"),
#                                              port = Sys.getenv("shinydbPort"),
#                                              user = Sys.getenv("shinyDbUser"),
#                                              password = Sys.getenv("shinyDbPassword"))
#
#
# resultsSchema <- "eunomiaCd"
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
