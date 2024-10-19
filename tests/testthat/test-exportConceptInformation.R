for (nm in names(testServers)) {
  
  test_that(paste("exportConceptInformation works on ", nm), {
    
    server <- testServers[[nm]]
    
    exportFolder <- tempfile()
    dir.create(exportFolder)
    
    connection <- DatabaseConnector::connect(server$connectionDetails)
    
    invisible(runResolvedConceptSets(
      connection = connection,
      cohortDefinitionSet = server$cohortDefinitionSet[1:2, ],
      databaseId = nm,
      exportFolder = exportFolder,
      minCellCount = 0,
      vocabularyDatabaseSchema = server$vocabularyDatabaseSchema,
      tempEmulationSchema = server$tempEmulationSchema
    ))
    
    exportConceptInformation(
      connection = connection,
      cdmDatabaseSchema = server$cdmDatabaseSchema,
      tempEmulationSchema = server$tempEmulationSchema,
      conceptIdTable = "concept_ids",
      vocabularyTableNames = getDefaultVocabularyTableNames(),
      incremental = FALSE,
      exportFolder = exportFolder
    )
  
    expectedFiles <- c("concept_ancestor.csv", "concept_synonym.csv", 
                       "concept.csv", "domain.csv", "executionTimes.csv", "relationship.csv", 
                       "resolved_concepts.csv", "vocabulary.csv")
    
    expect_true(all(expectedFiles %in%  list.files(exportFolder)))
    
    DatabaseConnector::disconnect(connection)
  })
}