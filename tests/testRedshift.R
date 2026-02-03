library(testthat)

# Determine which tests to run
runUnitTests <- Sys.getenv("RUN_UNIT_TESTS") == "TRUE"
runIntegrationTests <- (Sys.getenv("INTEGRATION_TESTS") == "TRUE" || 
                        Sys.getenv("RUN_ALL_TESTS") == "TRUE") &&
                       Sys.getenv("SKIP_DB_TESTS") != "TRUE"

if (runUnitTests) {
  message("========================================")
  message("Running UNIT TESTS (Redshift context)")
  message("========================================")
  
  options(dbms = "redshift")
  
  test_check(
    "CohortDiagnostics", 
    filter = "unit-",
    reporter = "progress"
  )
}

if (runIntegrationTests) {
  message("\n========================================")
  message("Running INTEGRATION TESTS (Redshift)")
  message("========================================")
  
  options(dbms = "redshift")
  
  test_check(
    "CohortDiagnostics",
    filter = "integration-",
    reporter = "progress"
  )
} else {
  message("\nℹ Skipping Redshift integration tests")
}

