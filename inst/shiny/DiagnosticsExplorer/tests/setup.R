library(testthat)

connectionDetails <- DatabaseConnector::createConnectionDetails("sqlite",
                                                                server = "testDb.sqlite")