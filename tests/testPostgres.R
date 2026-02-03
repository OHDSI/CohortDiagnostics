library(testthat)

# Determine which tests to run
# Unit tests are typically run via testSqlite.R to avoid redundancy
runUnitTests <- Sys.getenv("RUN_UNIT_TESTS") == "TRUE"
runIntegrationTests <- (Sys.getenv("INTEGRATION_TESTS") == "TRUE" || 
                        Sys.getenv("RUN_ALL_TESTS") == "TRUE") &&
                       Sys.getenv("SKIP_DB_TESTS") != "TRUE"

if (runUnitTests) {
  message("========================================")
  message("Running UNIT TESTS (Postgres context)")
  message("========================================")
  
  options(dbms = "postgresql")
  
  test_check(
    "CohortDiagnostics", 
    filter = "unit-",
    reporter = "progress"
  )
}

if (runIntegrationTests) {
  message("\n========================================")
  message("Running INTEGRATION TESTS (Postgres)")
  message("========================================")
  
  options(dbms = "postgresql")
  
  test_check(
    "CohortDiagnostics",
    filter = "integration-",
    reporter = "progress"
  )
} else {
  message("\nℹ Skipping Postgres integration tests")
}

