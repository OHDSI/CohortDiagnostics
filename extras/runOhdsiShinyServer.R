connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server = paste(Sys.getenv("phenotypeLibraryDbServer"),
                                                                               Sys.getenv("phenotypeLibraryDbDatabase"),
                                                                               sep = "/"),
                                                                port = Sys.getenv("phenotypeLibraryDbPort"),
                                                                user = Sys.getenv("phenotypeLibraryDbUser"),
                                                                password = Sys.getenv("phenotypeLibraryDbPassword"))

CohortDiagnostics::launchDiagnosticsExplorer(connectionDetails = connectionDetails, 
                                             resultsDatabaseSchema = Sys.getenv("phenotypeLibraryDbResultsSchema"), 
                                             vocabularyDatabaseSchema = Sys.getenv("phenotypeLibraryDbVocabularySchema"), 
                                             cohortBaseUrl = "https://epi.jnj.com/atlas/#/cohortdefinition/", 
                                             conceptBaseUrl = "https://epi.jnj.com/atlas/#/concept/", 
                                             runOverNetwork = TRUE
)
