# connection details ----
remotes::install_github('OHDSI/Eunomia')
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
databaseId = "Eunomia"
databaseName = "Eunomia Test"
databaseDescription = "This is a test data base called Eunomia"
cdmDatabaseSchema = 'main'
vocabularyDatabaseSchema = "main"
cohortDatabaseSchema = "main"
tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")




# Cohort Definitions ----
remotes::install_github('OHDSI/SkeletonCohortDiagnosticsStudy', ref = "develop")
studyName <- 'epi999'
## get cohort definition set ----
cohortDefinitionSet <-
  CohortGenerator::getCohortDefinitionSet(
    settingsFileName = "settings/CohortsToCreate.csv",
    jsonFolder = "cohorts",
    sqlFolder = "sql/sql_server",
    packageName = "SkeletonCohortDiagnosticsStudy",
    cohortFileNameValue = "cohortId"
  ) %>%  dplyr::tibble()
cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = "cohortEunomia")

# output folder information ----
outputFolder <-
  file.path("D:", "temp", "outputFolder", studyName, "eunomia")
## optionally delete previous execution ----
unlink(x = outputFolder,
       recursive = TRUE,
       force = TRUE)
dir.create(path = outputFolder,
           showWarnings = FALSE,
           recursive = TRUE)

# Execution ----
## Create cohort tables on remote ----
CohortGenerator::createCohortTables(connectionDetails = connectionDetails, 
                                    cohortDatabaseSchema = cohortDatabaseSchema, 
                                    cohortTableNames = cohortTableNames, 
                                    incremental = TRUE)
## Generate cohort on remote ----
CohortGenerator::generateCohortSet(connectionDetails = connectionDetails, 
                                   cdmDatabaseSchema = cdmDatabaseSchema, 
                                   tempEmulationSchema = tempEmulationSchema, 
                                   cohortTableNames = cohortTableNames, 
                                   cohortDefinitionSet = cohortDefinitionSet, 
                                   cohortDatabaseSchema = cohortDatabaseSchema, 
                                   incremental = TRUE, 
                                   incrementalFolder = file.path(outputFolder, "incremental"))

## Execute Cohort Diagnostics on remote ----
CohortDiagnostics::executeDiagnostics(
  cohortDefinitionSet = cohortDefinitionSet,
  exportFolder = outputFolder,
  databaseId = databaseId,
  databaseName = databaseName,
  databaseDescription = databaseDescription,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cdmDatabaseSchema = cdmDatabaseSchema,
  tempEmulationSchema = tempEmulationSchema,
  connectionDetails = connectionDetails,
  cohortTableNames = cohortTableNames,
  vocabularyDatabaseSchema = vocabularyDatabaseSchema,
  incremental = TRUE
)



# example of how to run full time series diagnostics outside executeDiagnostics
data <-
  CohortDiagnostics::runCohortTimeSeriesDiagnostics(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    runCohortTimeSeries = TRUE,
    runDataSourceTimeSeries = TRUE
  )
# to export data into csv in Cohort Diagnostics compatible form
data <- CohortDiagnostics:::makeDataExportable(x = data,
                                               tableName = "time_series",
                                               databaseId = databaseId)
CohortDiagnostics:::writeToCsv(
  data = data,
  fileName = file.path(outputFolder, "time_series.csv"),
  incremental = FALSE,
  cohortId = data$cohortId %>% unique()
)


# package results ----
CohortDiagnostics::createMergedResultsFile(dataFolder = outputFolder, overwrite = TRUE)
# Launch diagnostics explorer shiny app ----
CohortDiagnostics::launchDiagnosticsExplorer()


# upload to postgres db ----
connectionDetailsToUpload <- createConnectionDetails(dbms = "postgresql",
                                                     server = paste(Sys.getenv("shinydbServer"),
                                                                    Sys.getenv("shinydbDatabase"),
                                                                    sep = "/"),
                                                     port = Sys.getenv("shinydbPort"),
                                                     user = Sys.getenv("shinyDbUser"),
                                                     password = Sys.getenv("shinydbPw"))
resultsSchema <- "eunomiaCd"
CohortDiagnostics::createResultsDataModel(connectionDetails = connectionDetailsToUpload, 
                                          schema = resultsSchema)
zipFilesToUpload <- list.files(path = outputFolder,
                               pattern = ".zip",
                               recursive = TRUE,
                               full.names = TRUE)

for (i in (1:length(zipFilesToUpload))) {
  CohortDiagnostics::uploadResults(connectionDetails = connectionDetailsToUpload,
                                   schema = resultsSchema,
                                   zipFileName = zipFilesToUpload[[i]])
}

CohortDiagnostics::launchDiagnosticsExplorer(connectionDetails = connectionDetailsToUpload, 
                                             resultsDatabaseSchema = resultsSchema)
