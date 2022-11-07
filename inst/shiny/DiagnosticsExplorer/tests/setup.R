library(testthat)

lapply(file.path("../R", list.files("../R", pattern = "*.R")), source)

dataModelSpecificationsPath <- "../data/resultsDataModelSpecification.csv"
table1SpecPath <- normalizePath("../data/Table1SpecsLong.csv")
options("CD-spec-1-path" = table1SpecPath)
withr::defer({
  options("CD-spec-1-path" = NULL)
}, testthat::teardown_env())


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

withr::defer({
  # objects from local env to avoid issues with development
  rm("shinySettings", envir = .GlobalEnv)
}, testthat::teardown_env())
