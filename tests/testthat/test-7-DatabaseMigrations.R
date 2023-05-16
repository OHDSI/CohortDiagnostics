if (dbms == "postgresql") {
  resultsDatabaseSchema <- paste0(
    "r",
    format(Sys.time(), "%s"),
    sample(1:100, 1)
  )

  # Always clean up
  withr::defer(
    {
      pgConnection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
      sql <- "DROP SCHEMA IF EXISTS @resultsDatabaseSchema CASCADE;"
      DatabaseConnector::renderTranslateExecuteSql(
        sql = sql,
        resultsDatabaseSchema = resultsDatabaseSchema,
        connection = pgConnection
      )
      DatabaseConnector::disconnect(pgConnection)
    },
    testthat::teardown_env()
  )
}


test_that("Database Migrations execute without error", {
  skip_if_not(dbms %in% c("sqlite", "postgresql"))

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  if (dbms == "postgresql") {
    sql <- "CREATE SCHEMA @resultsDatabaseSchema;"
    DatabaseConnector::renderTranslateExecuteSql(
      sql = sql,
      resultsDatabaseSchema = resultsDatabaseSchema,
      connection = connection
    )
  } else {
    resultsDatabaseSchema <- "main"
  }

  migrator <- getDataMigrator(
    connectionDetails = connectionDetails,
    databaseSchema = resultsDatabaseSchema,
    tablePrefix = "cd_"
  )
  expect_true(migrator$check())

  .createDataModel(
    connection = connection,
    databaseSchema = resultsDatabaseSchema,
    tablePrefix = "cd_"
  )

  expect_false(all(migrator$getStatus()$executed))


  migrateDataModel(
    connectionDetails = connectionDetails,
    databaseSchema = resultsDatabaseSchema,
    tablePrefix = "cd_"
  )

  expect_true(all(migrator$getStatus()$executed))

  ## Reruning migrations should not cause an error
  migrateDataModel(
    connectionDetails = connectionDetails,
    databaseSchema = resultsDatabaseSchema,
    tablePrefix = "cd_"
  )
})
