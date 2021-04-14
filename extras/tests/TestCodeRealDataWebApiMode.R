source(Sys.getenv("startUpScriptLocation"))

temporaryLocation <- tempdir()

library(CohortDiagnostics)

connectionSpecifications <- cdmSources %>%
  dplyr::filter(.data$sequence == 1) %>%
  dplyr::filter(.data$database == 'truven_ccae')

dbms <- connectionSpecifications$dbms
port <- connectionSpecifications$port
server <- connectionSpecifications$server
cdmDatabaseSchema <- connectionSpecifications$cdmDatabaseSchema
vocabDatabaseSchema <- connectionSpecifications$vocabDatabaseSchema
databaseId <- connectionSpecifications$database
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

cohortTable <- "cohort"

library(magrittr)
# Set up
baseUrl <- Sys.getenv("baseUrlUnsecure")
# list of cohort ids
cohortIds <- c(18345,18346)

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

readr::write_excel_csv(x = cohortsToCreate, na = "", 
                       file = file.path(temporaryLocation, "CohortsToCreate.csv"), 
                       append = FALSE)


outputFolder <- file.path(temporaryLocation, "outputFolder", "webApiMode", "realData", databaseId)
unlink(x = outputFolder, recursive = TRUE, force = TRUE)
dir.create(path = outputFolder, showWarnings = FALSE, recursive = TRUE)

cohortSetReference <- readr::read_csv(file = file.path(temporaryLocation, "CohortsToCreate.csv"), 
                                      col_types = readr::cols())

CohortDiagnostics::instantiateCohortSet(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTable = cohortTable,
                                        baseUrl = baseUrl,
                                        cohortSetReference = cohortSetReference,
                                        generateInclusionStats = TRUE,
                                        inclusionStatisticsFolder = file.path(outputFolder, 
                                                                              'inclusionStatisticsFolder'))

CohortDiagnostics::runCohortDiagnostics(baseUrl = baseUrl,
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
                                        minCellCount = 5)

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

