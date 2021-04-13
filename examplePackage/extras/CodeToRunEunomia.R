library(CohortDiagnostics)
library(Eunomia)
library(examplePackage)

packageName <- 'examplePackage'

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cohortTable <- "cohort"
databaseId <- "Eunomia"

outputFolder <- file.path(rstudioapi::getActiveProject(), "outputFolder", databaseId)
unlink(x = outputFolder, recursive = TRUE, force = TRUE)
dir.create(path = outputFolder, showWarnings = FALSE, recursive = TRUE)

examplePackage::runCohortDiagnostics(
  packageName = packageName,
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  vocabularyDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTable,
  outputFolder = outputFolder,
  databaseId = databaseId,
  databaseName = "Eunomia Test",
  databaseDescription = "This is a test data base called Eunomia",
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

