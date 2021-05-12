# remotes::install_github('OHDSI/SkeletonCohortDiagnosticsStudy')

library(CohortDiagnostics)

temporaryLocation <- rstudioapi::getActiveProject()

connectionSpecifications <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == 'truven_ccae')

dbms <- connectionSpecifications$dbms # example: 'redshift'
port <- connectionSpecifications$port # example: 2234
server <-
  connectionSpecifications$serverRHealth # example: 'fdsfd.yourdatabase.yourserver.com"
cdmDatabaseSchema <-
  connectionSpecifications$cdmDatabaseSchemaRhealth # example: "cdm"
vocabDatabaseSchema <-
  connectionSpecifications$vocabDatabaseSchemaRhealth # example: "vocabulary"
databaseId <-
  connectionSpecifications$database # example: "truven_ccae"
userNameService = "OHDSI_USER" # example: "this is key ring service that securely stores credentials"
passwordService = "OHDSI_PASSWORD" # example: "this is key ring service that securely stores credentials"

# cohortDatabaseSchema = paste0('scratch_', keyring::key_get(service = userNameService))
cohortDatabaseSchema = paste0('scratch_rao_', databaseId)
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
baseUrl <- Sys.getenv("baseUrl")
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
  minCellCount = 5
)

CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)

CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)

# 
# connectionDetailsToUpload <- createConnectionDetails(dbms = "postgresql",
#                                                      server = paste(Sys.getenv("shinydbServer"),
#                                                                     Sys.getenv("shinydbDatabase"),
#                                                                     sep = "/"),
#                                                      port = Sys.getenv("shinydbPort"),
#                                                      user = Sys.getenv("shinyDbUserGowtham"),
#                                                      password = Sys.getenv("shinyDbPasswordGowtham"))
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
