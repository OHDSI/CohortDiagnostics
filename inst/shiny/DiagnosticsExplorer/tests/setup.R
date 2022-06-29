library(testthat)

lapply(file.path("../R", list.files("../R", pattern = "*.R")), source)

dataModelSpecificationsPath <- "../data/resultsDataModelSpecification.csv"
table1SpecPath <- "../data/Table1SpecsLong.csv"
activeUser <- Sys.info()[['user']]

connectionDetails <- DatabaseConnector::createConnectionDetails("sqlite",
                                                                server = "testDb.sqlite")

shinySettings <- list(
    connectionDetails = connectionDetails,
    resultsDatabaseSchema = c("main"),
    vocabularyDatabaseSchemas = c("main"),
    enableAnnotation = TRUE,
    enableAuthorization = TRUE,
    userCredentialsFile = "../UserCredentials.csv",
    tablePrefix = "",
    cohortTableName = "cohort",
    databaseTableName = "database"
  )