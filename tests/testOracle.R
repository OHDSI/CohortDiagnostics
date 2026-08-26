library(testthat)

# Determine which tests to run
runUnitTests <- Sys.getenv("RUN_UNIT_TESTS") == "TRUE"
runIntegrationTests <- (Sys.getenv("INTEGRATION_TESTS") == "TRUE" || 
                        Sys.getenv("RUN_ALL_TESTS") == "TRUE") &&
                       Sys.getenv("SKIP_DB_TESTS") != "TRUE"

if (runUnitTests) {
  message("========================================")
  message("Running UNIT TESTS (Oracle context)")
  message("========================================")
  
  options(dbms = "oracle")
  
  test_check(
    "CohortDiagnostics", 
    filter = "unit-",
    reporter = "progress"
  )
}

if (runIntegrationTests) {
  message("\n========================================")
  message("Running INTEGRATION TESTS (Oracle)")
  message("========================================")
  
  options(dbms = "oracle")
  
  test_check(
    "CohortDiagnostics",
    filter = "integration-",
    reporter = "progress"
  )
} else {
  message("\nℹ Skipping Oracle integration tests")
}

