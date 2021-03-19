library(CohortDiagnostics)
library(Eunomia)
library(EunomiaCohortDiagnostics)


packageName <- 'EunomiaCohortDiagnostics'
baseUrl <- Sys.getenv("OHDSIbaseUrl")

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cohortTable <- "cohort"
oracleTempSchema <- NULL

outputFolder <- file.path(rstudioapi::getActiveProject(), "outputFolder")
unlink(x = outputFolder, recursive = TRUE, force = TRUE)
dir.create(path = outputFolder, showWarnings = FALSE, recursive = TRUE)

EunomiaCohortDiagnostics::runCohortDiagnostics(
  packageName = packageName,
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  oracleTempSchema = oracleTempSchema,
  outputFolder = outputFolder,
  databaseId = "Eunomia",
  databaseName = "Eunomia Test",
  databaseDescription = "This is a test data base called Eunomia",
  runCohortCharacterization = TRUE,
  runCohortOverlap = TRUE,
  runOrphanConcepts = FALSE,
  runIncludedSourceConcepts = FALSE,
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
# 
# connectionDetailsToUpload <- createConnectionDetails(dbms = "postgresql",
#                                              server = paste(Sys.getenv("shinydbServer"),
#                                                             Sys.getenv("shinydbDatabase"),
#                                                             sep = "/"),
#                                              port = Sys.getenv("shinydbPort"),
#                                              user = Sys.getenv("shinyDbUserGowtham"),
#                                              password = Sys.getenv("shinyDbPasswordGowtham"))
# 
# 
# resultsSchema <- "eunomia"
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

