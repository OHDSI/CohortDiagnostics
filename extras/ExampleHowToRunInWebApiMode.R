#remotes::install_github("OHDSI/CohortDiagnostics",ref = "develop")

library(CohortDiagnostics)
library(magrittr)

################################################################################
# VARIABLES
################################################################################
projectId <- "epi_869"
temporaryLocation <- "S:/BitBucket/epi_869/programs/results"
outputFolder <- paste0(temporaryLocation,"/outputFolder/webApiMode")
baseUrl <- Sys.getenv('baseUrl')
scratchSpace <- "scratch_rao"
userName <- keyring::key_get("redShiftUserName")
password <- keyring::key_get("redShiftPassword")
dataServer <- Sys.getenv("DB_SERVER") #without the DB on the end
database <- 'optum_ehr'
#database <- 'optum_extended_dod'
#database <- 'truven_ccae'
#database <- 'truven_mdcr'
#database <- 'truven_mdcd'
cohortIds <- c(458,577,578,579)

################################################################################
# WORK
################################################################################

#Get all data source information
ROhdsiWebApi::authorizeWebApi(baseUrl, "windows") # Windows authentication

cdmSources <- ROhdsiWebApi::getCdmSources(baseUrl = baseUrl) %>%
  dplyr::mutate(baseUrl = baseUrl,
                dbms = 'redshift',
                sourceDialect = 'redshift',
                port = 5439,
                version = .data$sourceKey %>% substr(., nchar(.) - 3, nchar(.)) %>% as.integer(),
                database = .data$sourceKey %>% substr(., 5, nchar(.) - 6)) %>%
  dplyr::group_by(.data$database) %>%
  dplyr::arrange(dplyr::desc(.data$version)) %>%
  dplyr::mutate(sequence = dplyr::row_number()) %>%
  dplyr::ungroup() %>%
  dplyr::arrange(.data$database, .data$sequence) %>%
  dplyr::mutate(server = tolower(paste0(Sys.getenv("serverRoot"),"/", .data$database)))

connectionSpecifications <- cdmSources %>%
  dplyr::filter(sequence == 1) %>%
  dplyr::filter(database == get("database",envir = .env))

dbms <- connectionSpecifications$dbms # example: 'redshift'
port <- connectionSpecifications$port # example: 2234
server <- paste0(dataServer,connectionSpecifications$server) # example: 'fdsfd.yourdatabase.yourserver.com"
cdmDatabaseSchema <- connectionSpecifications$sourceKey # example: "cdm"
vocabDatabaseSchema <- connectionSpecifications$vocabDatabaseSchema # example: "vocabulary"
databaseId <- connectionSpecifications$database # example: "truven_ccae"
userNameService = userName
passwordService = password

cohortDatabaseSchema = scratchSpace  #assume standard area to write to

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = dbms,
  user = userName,
  password = password,
  port = port,
  server = server
)

cohortTable <- paste0(projectId,"_",connectionSpecifications$database, "_webapi")

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

################################################################################
# PRESENTATION
################################################################################
CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)

CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)