# Boundary test: verifies Panoptes can find this package's platform declaration
# and that the declaration and live workflow have not drifted. No database needed.

Panoptes::skipIfLiveDatabaseTest()

test_that("Panoptes finds the package declaration", {
  config <- Panoptes::readDatabaseTestConfig(package = "CohortDiagnostics")

  expect_equal(config$package, "CohortDiagnostics")
  expect_equal(config$databaseConnection, "subset")
  expect_setequal(
    vapply(config$platforms, `[[`, character(1), "dbms"),
    c("sqlite", "postgresql", "sql server", "oracle", "redshift", "spark", "snowflake", "iris")
  )
})

test_that("Workflow matrix matches the platform declaration", {
  workflowPath <- testthat::test_path("..", "..", ".github", "workflows", "database-platform-tests.yml")
  skip_if_not(file.exists(workflowPath), "Workflow file not available (installed package)")

  config <- Panoptes::readDatabaseTestConfig(package = "CohortDiagnostics")
  expect_no_error(Panoptes::validateDatabaseTestWorkflow(config, workflowPath))
})
