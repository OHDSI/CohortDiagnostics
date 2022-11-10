test_that("Testing Concept Set Diagnostics", {
  
  skip_if(skipCdmTests, "cdm settings not configured")
  skip_if(skipCdmTests, "cdm settings not configured")
  tConnection <-DatabaseConnector::connect(connectionDetails)
  
  with_dbc_connection(tConnection, {
    exportFolder <- tempfile()
    recordKeepingFile <- tempfile(fileext="csv")
    dir.create(exportFolder)
    on.exit(unlink(exportFolder), add = TRUE)
    cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = cohortTable)
    # Next create the tables on the database
    CohortGenerator::createCohortTables(
      connectionDetails = connectionDetails,
      cohortTableNames = cohortTableNames,
      cohortDatabaseSchema = cohortDatabaseSchema,
      incremental = FALSE
    )
    
    on.exit({
      CohortGenerator::dropCohortStatsTables(connection = tConnection,
                                             cohortDatabaseSchema = cohortDatabaseSchema,
                                             cohortTableNames = cohortTableNames)
      
      DatabaseConnector::renderTranslateExecuteSql(tConnection,
                                                   "DROP TABLE @cohortDatabaseSchema.@cohortTable",
                                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                                   cohortTable = cohortTable)
    }, add = TRUE)
    
    # Generate the cohort set
    CohortGenerator::generateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      cohortDefinitionSet = cohortDefinitionSet,
      incremental = FALSE
    )
    
    createConceptTable(connection, tempEmulationSchema)
    
    computeConceptSetDiagnostics(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionSet = cohortDefinitionSet,
      databaseId = databaseId,
      exportFolder = exportFolder,
      minCellCount = minCellCount,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental,
      runIncludedSourceConcepts = runIncludedSourceConcepts,
      runOrphanConcepts = runOrphanConcepts,
      runBreakdownIndexEvents = runBreakdownIndexEvents,
      conceptCountsDatabaseSchema = NULL,
      conceptCountsTable = "#concept_counts",
      conceptCountsTableIsTemp = TRUE,
      useExternalConceptCountsTable = FALSE,
      conceptIdTable = "#concept_ids"
    )
    
    
    visitContext <- getVisitContext(connection = connection,
                                    cdmDatabaseSchema = cdmDatabaseSchema,
                                    tempEmulationSchema = tempEmulationSchema,
                                    cohortDatabaseSchema = cohortDatabaseSchema,
                                    cohortTable = cohortTable,
                                    cdmVersion = 5,
                                    cohortIds = cohortDefinitionSet$cohortId,
                                    conceptIdTable = "#concept_ids")
    
  })
  #TODO add the tests
  
})