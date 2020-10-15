connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server = paste(Sys.getenv("phoebedbServer"),
                                                                               Sys.getenv("phoebedb"),
                                                                               sep = "/"),
                                                                port = Sys.getenv("phenotypeLibraryDbPort"),
                                                                user = Sys.getenv("phoebedbUser"),
                                                                password = Sys.getenv("phoebedbPw"))

CohortDiagnostics::launchDiagnosticsExplorer(connectionDetails = connectionDetails, 
                                             resultsDatabaseSchema = Sys.getenv("phoebedbTargetSchema"), 
                                             vocabularyDatabaseSchema = Sys.getenv("phoebedbVocabSchema"), 
                                             cohortBaseUrl = "https://epi.jnj.com/atlas/#/cohortdefinition/", 
                                             conceptBaseUrl = "https://epi.jnj.com/atlas/#/concept/", 
                                             runOverNetwork = TRUE
)