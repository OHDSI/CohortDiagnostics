
# test getIncidenceRate on all dbms
for (nm in names(testServers)) {
  server <- testServers[[nm]]

  test_that(paste("getIncidenceRate works on", nm), {

    connection <- DatabaseConnector::connect(server$connectionDetails)
    result <- getIncidenceRate(
      connection = connection,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortTable = server$cohortTable,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
      tempEmulationSchema = server$tempEmulationSchema,
      firstOccurrenceOnly = TRUE,
      washoutPeriod = 365,
      cohortId = server$cohortDefinitionSet$cohortId[1])

    expect_true(is.data.frame(result))

    # getResultsDataModelSpecifications("incidence_rate")$columnName
    expect_equal(
      names(result),
      c("cohortCount", "personYears", "gender", "ageGroup", "calendarYear", "incidenceRate")
    )

    DatabaseConnector::disconnect(connection)
  })
}


# only test runIncidenceRate on sqlite (or duckdb)
test_that("runIncidenceRate", {
  skip_if_not("sqlite" %in% names(testServers))

  server <- testServers[["sqlite"]]
  exportFolder <- getUniqueTempDir()
  dir.create(exportFolder, recursive = TRUE)

  connection <- DatabaseConnector::connect(server$connectionDetails)

  runIncidenceRate(
    connection,
    cohortDefinitionSet = server$cohortDefinitionSet[1:2,],
    tempEmulationSchema = server$tempEmulationSchema,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    cohortDatabaseSchema = server$cohortDatabaseSchema,
    cohortTable = server$cohortTable,
    databaseId = "GiBleed",
    exportFolder = exportFolder,
    minCellCount = 1,
    washoutPeriod = 0,
    incremental =  F)
  DatabaseConnector::disconnect(connection)
  expect_true(file.exists(file.path(exportFolder, "incidence_rate.csv")))
})



