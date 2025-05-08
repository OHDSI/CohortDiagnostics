test_that("Creating and checking externalConceptCounts table", {
  if (dbms == "sqlite") {
    # Creating externalConceptCounts
    sql_lite_path <- file.path(test_path(), databaseFile)
    connectionDetails <- createConnectionDetails(dbms= "sqlite", server = sql_lite_path)
    connection <- connect(connectionDetails)
    cdmDatabaseSchema <- "main"
    conceptCountsTable <- "concept_counts"
    CohortDiagnostics::createConceptCountsTable(connectionDetails = connectionDetails,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                tempEmulationSchema = NULL,
                                                conceptCountsTable = "concept_counts",
                                                conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                                conceptCountsTableIsTemp = FALSE,
                                                removeCurrentTable = TRUE)
    
    concept_counts_info <- querySql(connection, "PRAGMA table_info(concept_counts)")
    expect_equal(concept_counts_info$NAME, c("concept_id", 
                                             "concept_count", 
                                             "concept_subjects", 
                                             "vocabulary_version"))
    checkConceptCountsTableExists <- DatabaseConnector::dbExistsTable(connection,
                                                                      name = conceptCountsTable,
                                                                      databaseSchema = cdmDatabaseSchema)
    expect_true(checkConceptCountsTableExists)
    
    # Checking vocab version matches 
    useExternalConceptCountsTable <- TRUE
    conceptCountsTable <- "concept_counts"
    conceptCountsTable <- conceptCountsTable
    dataSourceInfo <- getCdmDataSourceInformation(connection = connection, cdmDatabaseSchema = cdmDatabaseSchema)
    vocabVersion <- dataSourceInfo$vocabularyVersion
    vocabVersionExternalConceptCountsTable <- renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT DISTINCT vocabulary_version FROM @work_database_schema.@concept_counts_table;",
      work_database_schema = cdmDatabaseSchema,
      concept_counts_table = conceptCountsTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = getOption("sqlRenderTempEmulationSchena")
    )
    
    expect_equal(vocabVersion, vocabVersionExternalConceptCountsTable[1,1])
  }
  
})