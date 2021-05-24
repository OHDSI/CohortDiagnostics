# remotes::install_github('OHDSI/SkeletonCohortDiagnosticsStudy')
# remotes::install_github('OHDSI/Eunomia')

library(CohortDiagnostics)
library(SkeletonCohortDiagnosticsStudy)

temporaryLocation <- tempdir()

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cohortTable <- "cohort"
databaseId <- "Eunomia"

library(magrittr)
# Set up
baseUrl <- Sys.getenv("baseUrl")
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


outputFolder <- file.path(temporaryLocation, "outputFolder", "webApiMode", "eunomia")
# Please delete previous content if needed
# unlink(x = outputFolder,
#        recursive = TRUE,
#        force = TRUE)
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
                                        minCellCount = 0)

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

