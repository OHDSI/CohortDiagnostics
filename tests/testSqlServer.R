library(testthat)

# Determine which tests to run
runUnitTests <- Sys.getenv("RUN_UNIT_TESTS") == "TRUE"
runIntegrationTests <- (Sys.getenv("INTEGRATION_TESTS") == "TRUE" || 
                        Sys.getenv("RUN_ALL_TESTS") == "TRUE") &&
                       Sys.getenv("SKIP_DB_TESTS") != "TRUE"

if (runUnitTests) {
  message("========================================")
  message("Running UNIT TESTS (SQL Server context)")
  message("========================================")
  
  options(dbms = "sql server")
  
  test_check(
    "CohortDiagnostics", 
    filter = "unit-",
    reporter = "progress"
  )
}

if (runIntegrationTests) {
  message("\n========================================")
  message("Running INTEGRATION TESTS (SQL Server)")
  message("========================================")
  
  options(dbms = "sql server")
  
  test_check(
    "CohortDiagnostics",
    filter = "integration-",
    reporter = "progress"
  )
} else {
  message("\nℹ Skipping SQL Server integration tests")
}

