if (dbms == "postgresql") {
  resultsDatabaseSchema <- paste0("r",
                                  gsub("[: -]", "", Sys.time(), perl = TRUE),
                                  sample(1:100, 1))

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

test_that("regexp pattern works", {
  expect_true(grepl(.migrationFileRexp, "Migration_1-MyMigration.sql") > 0 )
  expect_true(grepl(.migrationFileRexp, "Migration_2-v3.2whaterver.sql") > 0 )
  expect_true(grepl(.migrationFileRexp, "Migration_4-TEST.sql") > 0 )

  expect_false(grepl(.migrationFileRexp, "Migration_4-.sql") > 0 )
  expect_false(grepl(.migrationFileRexp, "Migration_4-missing_letter.sl") > 0 )
  expect_false(grepl(.migrationFileRexp, "Migraton_4-a.sql") > 0 )
  expect_false(grepl(.migrationFileRexp, "Migration_2v3.2whaterver.sql") > 0 )
  expect_false(grepl(.migrationFileRexp, "foo.sql") > 0 )
  expect_false(grepl(.migrationFileRexp, "UpdateVersionNumber.sql") > 0 )
})


test_that("Database Migrations execute without error", {
  skip_if_not(dbms %in% c("sqlite", "postgresql"))
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  if (dbms == "postgresql") {
    sql <- "CREATE SCHEMA @resultsDatabaseSchema;"
    DatabaseConnector::renderTranslateExecuteSql(sql = sql,
                                                 resultsDatabaseSchema = resultsDatabaseSchema,
                                                 connection = connection)
  } else {
    resultsDatabaseSchema <- "main"
  }

  .createDataModel(connection = connection,
                   schema = resultsDatabaseSchema,
                   tablePrefix = "cd_")

  migrateDataModel(connection = connection,
                   schema = resultsDatabaseSchema,
                   tablePrefix = "cd_")

  completedMigrations <- getCompletedMigrations(connection = connection,
                                                schema = resultsDatabaseSchema,
                                                tablePrefix = "cd_")
  migrationDir <- system.file("sql", "sql_server", "migrations", package = utils::packageName())
  availableMigrations <- list.files(migrationDir, pattern = .migrationFileRexp)

  expect_true(length(setdiff(availableMigrations, completedMigrations$migrationFile)) == 0)
  expect_true(length(setdiff(completedMigrations$migrationFile, availableMigrations)) == 0)

  ## Reruning migrations should not cause an error
  migrateDataModel(connection = connection,
                 schema = resultsDatabaseSchema,
                 tablePrefix = "cd_")

  completedMigrations2 <- getCompletedMigrations(connection = connection,
                                                 schema = resultsDatabaseSchema,
                                                 tablePrefix = "cd_")

  checkmate::expect_set_equal(completedMigrations$migrationFile, completedMigrations2$migrationFile)
})

# This is an internal function so it is assumed that strings passed follow correct pattern
test_that("Migration order from string pattern is correct", {
  testSet <- c("Migration_11-test.sql", "Migration_2-test.sql")
  execOrder <- .getMigrationOrder(testSet)

  expect_true(execOrder[1,]$migrationFile == "Migration_2-test.sql")
  expect_true(execOrder[2,]$migrationFile == "Migration_11-test.sql")

  testSet2 <- c("Migration_13-test.sql", "Migration_21-test.sql")
  execOrder <- .getMigrationOrder(testSet2)

  expect_true(execOrder[1,]$migrationFile == "Migration_13-test.sql")
  expect_true(execOrder[2,]$migrationFile == "Migration_21-test.sql")
})

test_that("Migration check utility", {
  expect_true(checkMigrationFiles())

  # Check the function works
  testDir <- tempfile()
  dir.create(testDir)
  on.exit(unlink(testDir, force = TRUE, recursive = TRUE), add = TRUE)
  file.create(file.path(testDir, "TEST.sql"))
  file.create(file.path(testDir, "TEST2.sql"))

  expect_false(checkMigrationFiles(testDir))
})