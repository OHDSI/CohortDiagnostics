# remotes::install_github('OHDSI/SkeletonCohortDiagnosticsStudy')
# remotes::install_github('OHDSI/Eunomia')

library(CohortDiagnostics)
library(SkeletonCohortDiagnosticsStudy)

outputLocation <- "D:\\temp"
outputFolder <-
  file.path(outputLocation, "outputFolder", "packageMode", "eunomia")
# Please delete previous content if needed
unlink(x = outputFolder,
       recursive = TRUE,
       force = TRUE)
dir.create(path = outputFolder,
           showWarnings = FALSE,
           recursive = TRUE)

execute(
  connectionDetails = Eunomia::getEunomiaConnectionDetails(),
  cdmDatabaseSchema = 'main',
  vocabularyDatabaseSchema = "main",
  cohortDatabaseSchema = "main",
  cohortTable = "cohortEunomia",
  outputFolder = outputFolder,
  databaseId = "Eunomia",
  databaseName = "Eunomia Test",
  databaseDescription = "This is a test data base called Eunomia"
)

CohortDiagnostics::preMergeDiagnosticsFiles(dataFolder = outputFolder)



connectionDetailsToUpload <- createConnectionDetails(dbms = "postgresql",
                                                     server = paste(Sys.getenv("shinydbServer"),
                                                                    Sys.getenv("shinydbDatabase"),
                                                                    sep = "/"),
                                                     port = Sys.getenv("shinydbPort"),
                                                     user = Sys.getenv("shinyDbUser"),
                                                     password = Sys.getenv("shinydbPw"))


resultsSchema <- "eunomiaCd"
createResultsDataModel(connectionDetails = connectionDetailsToUpload, schema = resultsSchema)


path = outputFolder
zipFilesToUpload <- list.files(path = path,
                               pattern = ".zip",
                               recursive = TRUE,
                               full.names = TRUE)

for (i in (1:length(zipFilesToUpload))) {
  CohortDiagnostics::uploadResults(connectionDetails = connectionDetailsToUpload,
                                   schema = resultsSchema,
                                   zipFileName = zipFilesToUpload[[i]])
}


CohortDiagnostics::launchDiagnosticsExplorer(dataFolder = outputFolder)
