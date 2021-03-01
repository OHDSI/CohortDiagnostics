library(CohortDiagnostics)
library(Eunomia)
library(EunomiaPhenotypeLibrary)


packageName <- 'EunomiaPhenotypeLibrary'
# baseUrl <- Sys.getenv("OHDSIbaseUrl")

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cohortTable <- "cohort"
oracleTempSchema <- NULL

outputFolder <- file.path(rstudioapi::getActiveProject(), "outputFolder")
dir.create(path = outputFolder, showWarnings = FALSE, recursive = TRUE)

EunomiaPhenotypeLibrary::runCohortDiagnostics(
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
  runIncidenceRates = TRUE,
  createCohorts = TRUE,
  minCellCount = 0
)


connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = paste(Sys.getenv("shinydbServer"),
                                                            Sys.getenv("shinydbDatabase"),
                                                            sep = "/"),
                                             port = Sys.getenv("shinydbPort"),
                                             user = Sys.getenv("shinyDbUserGowtham"),
                                             password = Sys.getenv("shinyDbPasswordGowtham"))


resultsSchema <- "eunomia"
createResultsDataModel(connectionDetails = connectionDetails, schema = resultsSchema)



Sys.setenv("POSTGRES_PATH" = "C:/Program Files/PostgreSQL/13/bin")
path = outputFolder
zipFilesToUpload <- list.files(path = path, 
                               pattern = ".zip", 
                               recursive = TRUE, 
                               full.names = TRUE)

for (i in (1:length(zipFilesToUpload))) {
  uploadResults(connectionDetails = connectionDetails,
                schema = resultsSchema,
                zipFileName = zipFilesToUpload[[i]])
}
