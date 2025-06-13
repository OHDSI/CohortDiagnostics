test_that("Time dynamic network analysis executes", {
  tConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = Eunomia::getDatabaseFile("Synthea27Nj", cdmVersion = "5.4")
  )

  runTimeDynamicNetworkAnalysis(connectionDetails = tConnectionDetails,
                                cohortDatabaseSchema = "main",
                                cdmDatabaseSchema = "main",
                                cohortDefinitionSet = cohortDefinitionSet,
                                exportFolder = tempfile(),
                                databaseId = "Eunomia",
                                cohortIds = cohortDefinitionSet$cohortId,
                                timeBinSize = 30




})