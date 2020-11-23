connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                server = paste(Sys.getenv("phoebedbServer"),
                                                                               Sys.getenv("phoebedb"),
                                                                               sep = "/"),
                                                                port = Sys.getenv("phenotypeLibraryDbPort"),
                                                                user = Sys.getenv("phoebedbUser"),
                                                                password = Sys.getenv("phoebedbPw"))

