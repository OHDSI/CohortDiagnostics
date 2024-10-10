
for (nm in names(testServers)) {
  server <- testServers[[nm]]

  test_that(paste("getBreakdownIndexsEvents works on", nm), {

    connection <- DatabaseConnector::connect(server$connectionDetails)

    # #inst_concept_set table is required by getBreakdownIndexEvents
    resolvedConcepts <- getResolvedConceptSets(
      connection = connection,
      cohortDefinitionSet = server$cohortDefinitionSet[2,], # must contain any parents of subset cohort definitions
      vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
      tempEmulationSchema = server$tempEmulationSchema
    )

    conceptSets <- combineConceptSetsFromCohorts(server$cohortDefinitionSet)

    result <- getBreakdownIndexEvents(
      connection = connection,
      cohort = server$cohortDefinitionSet[2,], # must be one row
      conceptSets = conceptSets,
      tempEmulationSchema = server$tempEmulationSchema,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
      cohortDatabaseSchema = server$cohortDatabaseSchema,
      cohortTable = server$cohortTable)

    expect_true(is.data.frame(result))

    expect_equal(
      names(result),
      c("domainTable", "domainField", "conceptId", "conceptCount", "subjectCount", "cohortId")
    )

    DatabaseConnector::disconnect(connection)
  })
}

test_that("runBreakdownIndexEvents", {
  skip_if_not("sqlite" %in% names(testServers))

  server <- testServers[["sqlite"]]
  exportFolder <- tempfile()
  dir.create(exportFolder)

  incrementalFolder <- tempfile()
  dir.create(incrementalFolder)

  connection <- DatabaseConnector::connect(server$connectionDetails)

  # #inst_concept_set table is required by getBreakdownIndexEvents
  resolvedConcepts <- getResolvedConceptSets(
    connection = connection,
    cohortDefinitionSet = server$cohortDefinitionSet[2,], # must contain any parents of subset cohort definitions
    vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema
  )


  runBreakdownIndexEvents(
    connection = connection,
    cohortDefinitionSet = server$cohortDefinitionSet[2,],
    tempEmulationSchema = server$tempEmulationSchema,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
    cohortDatabaseSchema = server$cohortDatabaseSchema,
    databaseId = "eunomia",
    exportFolder = exportFolder,
    minCellCount = 1,
    cohortTable = server$cohortTable,
    incremental = FALSE,
    incrementalFolder = incrementalFolder)

  DatabaseConnector::disconnect(connection)
  expect_true(file.exists(file.path(exportFolder, "index_event_breakdown.csv")))
})



