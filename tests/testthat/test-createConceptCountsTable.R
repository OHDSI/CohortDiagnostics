skipConceptCountsTable <- FALSE
if (Sys.getenv("CDM5_POSTGRESQL_SERVER") == "") {
  skipConceptCountsTable <- TRUE
} else {
  postgresConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
    pathToDriver = jdbcDriverFolder
  )
  
  resultsDatabaseSchema <- paste0("r", gsub("[: -]", "", Sys.time(), perl = TRUE), sample(1:100, 1))
  
  # Always clean up
  withr::defer({
    pgConnection <- DatabaseConnector::connect(connectionDetails = postgresConnectionDetails)
    sql <- "DROP SCHEMA IF EXISTS @resultsDatabaseSchema CASCADE;"
    DatabaseConnector::renderTranslateExecuteSql(sql = sql,
                                                 resultsDatabaseSchema = resultsDatabaseSchema,
                                                 connection = pgConnection)
    
    DatabaseConnector::disconnect(pgConnection)
    unlink(folder, recursive = TRUE, force = TRUE)
  }, testthat::teardown_env())
}

test_that("Create concept counts table", {
  skip_if(skipConceptCountsTable | skipCdmTests, 'create concept counts test not set')
  
  pgConnection <- DatabaseConnector::connect(connectionDetails = postgresConnectionDetails)
  with_dbc_connection(pgConnection, {
    sql <- "CREATE SCHEMA @resultsDatabaseSchema;"
    DatabaseConnector::renderTranslateExecuteSql(sql = sql,
                                                 resultsDatabaseSchema = resultsDatabaseSchema,
                                                 connection = pgConnection)
    
    #create concepts count table
    conceptCountsTableName <- "concept_count_test"
    createConceptCountsTable(
      connection = pgConnection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      conceptCountsDatabaseSchema = resultsDatabaseSchema,
      conceptCountsTable = conceptCountsTableName,
      conceptCountsTableIsTemp = FALSE)
    
    expect_true(.pgTableExists(pgConnection, resultsDatabaseSchema, conceptCountsTableName))
    
    # Bad schema name
    expect_error(createConceptCountsTable(
      connection = pgConnection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      conceptCountsDatabaseSchema = "non_existant_schema",
      conceptCountsTable = conceptCountsTableName,
      conceptCountsTableIsTemp = FALSE))
  })
})