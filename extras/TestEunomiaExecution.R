## For developer convenience to create small test data sets

devtools::load_all()

cohortDefinitionSet <- loadTestCohortDefinitionSet()
connectionDetails <- Eunomia::getEunomiaConnectionDetails()
resFile <- tempfile(fileext = ".sqlite")

cohortTable <- "cohort"
vocabularyDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cdmDatabaseSchema <- "main"

createTestShinyDb(connectionDetails = connectionDetails,
                  outputPath = resFile,
                  cohortTable = "cohort",
                  vocabularyDatabaseSchema = "main",
                  cohortDatabaseSchema = "main",
                  cdmDatabaseSchema = "main",
                  cohortDefinitionSet = cohortDefinitionSet, 
                  runTemporalCohortCharacterization = FALSE)


devtools::load_all("../OhdsiShinyModules/")

CohortDiagnostics::launchDiagnosticsExplorer(sqliteDbPath = resFile)


shinyCd <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = resFile)
connectionHandler <- ResultModelManager::ConnectionHandler$new(connectionDetails = shinyCd)
resultDatabaseSettings <- list(
  dbms = 'sqlite',
  cdTablePrefix = '',
  schema = "main",
  vocabularyDatabaseSchema = "main"
)


dataSource <- OhdsiShinyModules::createCdDatabaseDataSource(connectionHandler, resultDatabaseSettings)

res <- getResultsCohortOverlapFe(dataSource = dataSource, cohortIds = 14906)
