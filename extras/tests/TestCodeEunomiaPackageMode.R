# remotes::install_github('OHDSI/SkeletonCohortDiagnosticsStudy')
# remotes::install_github('OHDSI/Eunomia')

library(CohortDiagnostics)
library(SkeletonCohortDiagnosticsStudy)

temporaryLocation <- tempdir()
outputFolder <- file.path(temporaryLocation, "outputFolder", "packageMode", "eunomia", databaseId)
unlink(x = outputFolder, recursive = TRUE, force = TRUE)
dir.create(path = outputFolder, showWarnings = FALSE, recursive = TRUE)

SkeletonCohortDiagnosticsStudy::runCohortDiagnostics(
  connectionDetails = Eunomia::getEunomiaConnectionDetails(),
  cdmDatabaseSchema = cdmDatabaseSchema,
  vocabularyDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohortEunomia",
  outputFolder = outputFolder,
  databaseId = "Eunomia",
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

