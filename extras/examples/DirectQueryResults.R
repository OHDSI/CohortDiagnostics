library(CohortDiagnostics)
# dataSource <-
#   CohortDiagnostics::createFileDataSource(premergedDataFile = "D:\\git\\github\\gowthamrao\\CohortDiagnostics\\CohortDiagnostics\\inst\\shiny\\DiagnosticsExplorer\\data\\PreMerged.RData",
#                                           envir = .GlobalEnv)


defaultServer <- Sys.getenv("phoebedbServer")
defaultDatabase <- Sys.getenv("phoebedb")
defaultPort <- 5432
defaultUser <- Sys.getenv("phoebedbUser")
defaultPassword <- Sys.getenv("phoebedbPw")
defaultResultsSchema <- "phenotypeLibrary"
defaultVocabularySchema <- "phenotypeLibrary"


connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = 'postgresql',
  user = defaultUser,
  password = defaultPassword,
  port = defaultPort,
  server = paste0(defaultServer, "/", defaultDatabase)
)
# connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
dataSource <-
  CohortDiagnostics::createDatabaseDataSource(connection = connection,
                                              connectionDetails = connectionDetails, 
                                              resultsDatabaseSchema = defaultResultsSchema, 
                                              vocabularyDatabaseSchema = defaultResultsSchema)

cohort <- getResultsCohort(dataSource = dataSource)
databaseId <- getResultsDatabase(dataSource = dataSource) 

resolvedConcepts <- getResultsResolvedConcepts(dataSource = dataSource, 
                                               databaseIds = "optum_ehr_v1705_20210811",
                                               cohortIds = c(2,3))


# DatabaseConnector::disconnect(connection)
