


for (server in testServers) {
  test_that(paste("getResolvedConceptSets works on", server$connectionDetails$dbms), {
    
    connection <- DatabaseConnector::connect(server$connectionDetails)
    
    # used to create the #inst_concept_sets
    invisible(
      getResolvedConceptSets(
        connection = connection,
        cohortDefinitionSet = server$cohortDefinitionSet,
        vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
        tempEmulationSchema = server$tempEmulationSchema
      )
    )
    
    result <- getIncludedSourceConcepts(
      connection = connection,
      cohortDefinitionSet = server$cohortDefinitionSet,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      tempEmulationSchema = server$tempEmulationSchema)
 
    expect_true(is.data.frame(result))
    expect_named(result, c("cohortId", "conceptSetId", "conceptId", "sourceConceptId", "conceptCount", "conceptSubjects"))
    
    # empty cohort set works
    result <- getIncludedSourceConcepts(
      connection = connection,
      cohortDefinitionSet = server$cohortDefinitionSet[-1:-100,],
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      tempEmulationSchema = server$tempEmulationSchema)
    
    expect_true(is.data.frame(result))
    expect_named(result, c("cohortId", "conceptSetId", "conceptId", "sourceConceptId", "conceptCount", "conceptSubjects"))
    
    DatabaseConnector::disconnect(connection)
  })
}

test_that("runIncludedSourceConcepts works", {
  skip_if_not("sqlite" %in% names(testServers))
  server <- testServers[["sqlite"]]
  connection <- DatabaseConnector::connect(server$connectionDetails)
  exportFolder <- tempfile()
  dir.create(exportFolder)

  
  invisible(
    runResolvedConceptSets(
      connection = connection,
      cohortDefinitionSet = server$cohortDefinitionSet,
      vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
      databaseId = server$connectionDetails$dbms,
      exportFolder = exportFolder,
      minCellCount = 1,
      tempEmulationSchema = server$tempEmulationSchema
    )
  )
  
  runIncludedSourceConcepts(
    connection = connection,
    cohortDefinitionSet = server$cohortDefinitionSet,
    cdmDatabaseSchema = server$cdmDatabaseSchema,
    databaseId = server$connectionDetails$dbms,
    exportFolder = exportFolder,
    minCellCount = 1,
    tempEmulationSchema = server$tempEmulationSchema
  )
  
  DatabaseConnector::disconnect(connection)
  result <- readr::read_csv(file.path(exportFolder, "resolved_concepts.csv"), show_col_types = F)
  expect_true(is.data.frame(result))
  expect_named(result, c("cohort_id", "concept_set_id", "concept_id", "database_id"))
})



