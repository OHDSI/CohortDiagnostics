
server <- testServers[[1]]

for (server in testServers) {
  test_that(paste("getResolvedConceptSets works on", server$connectionDetails$dbms), {
    
    connection <- DatabaseConnector::connect(server$connectionDetails)
    result <- getResolvedConceptSets(
      connection = connection,
      cohortDefinitionSet = server$cohortDefinitionSet,
      vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
      tempEmulationSchema = server$tempEmulationSchema
    )
    
    expect_true(is.data.frame(result))
    expect_named(result, c("cohortId", "conceptSetId", "conceptId"))
    expect_true(tempTableExists("concept_ids"))
    expect_true(tempTableExists("inst_concept_sets"))
    DatabaseConnector::disconnect(connection)
  })
}

test_that("runResolvedConceptSets works", {
  skip_if_not("sqlite" %in% names(testServers))
  server <- testServers[["sqlite"]]
  connection <- DatabaseConnector::connect(server$connectionDetails)
  exportFolder <- tempfile()
  dir.create(exportFolder)
  
  runResolvedConceptSets(
    connection = connection,
    cohortDefinitionSet = server$cohortDefinitionSet,
    databaseId = server$connectionDetails$dbms,
    exportFolder = exportFolder,
    minCellCount = 1,
    vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
    tempEmulationSchema = server$tempEmulationSchema
  )
  
  DatabaseConnector::disconnect(connection)
  result <- readr::read_csv(file.path(exportFolder, "resolved_concepts.csv"), show_col_types = F)
  expect_true(is.data.frame(result))
  expect_named(result, c("cohort_id", "concept_set_id", "concept_id", "database_id"))
})

