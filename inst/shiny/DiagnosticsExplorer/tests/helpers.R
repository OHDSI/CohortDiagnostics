getTestDataSource <- function(connectionDetails) {
  connectionPool <- getConnectionPool(connectionDetails)

  createDatabaseDataSource(
    connection = connectionPool,
    resultsDatabaseSchema = "main",
    vocabularyDatabaseSchema = "main",
    dbms = "sqlite"
  )
}
