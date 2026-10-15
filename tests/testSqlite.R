library(testthat)

# Determine which tests to run
runUnitTests <- Sys.getenv("RUN_UNIT_TESTS") != "FALSE" # Default to TRUE
runIntegrationTests <- Sys.getenv("INTEGRATION_TESTS") == "TRUE" ||
  Sys.getenv("RUN_ALL_TESTS") == "TRUE"


if (runUnitTests) {
  message("========================================")
  message("Running UNIT TESTS (fast, no database)")
  message("========================================")

  options(dbms = "sqlite")

  # Run only unit tests
  test_results_unit <- test_check(
    "CohortDiagnostics",
    filter = "unit-",
    reporter = "progress"
  )

  message("\n✓ Unit tests complete")
}

if (runIntegrationTests) {
  message("\n========================================")
  message("Running INTEGRATION TESTS (slow, requires database)")
  message("========================================")

  # Set environment variable to trigger integration setup
  Sys.setenv(INTEGRATION_TESTS = "TRUE")
  options(dbms = "sqlite")

  # Run integration tests
  test_results_integration <- test_check(
    "CohortDiagnostics",
    filter = "integration-",
    reporter = "progress"
  )

  message("\n✓ Integration tests complete")
} else {
  message("\nℹ Skipping integration tests")
  message("  To run: INTEGRATION_TESTS=TRUE Rscript tests/testSqlite.R")
}

message("\n========================================")
message("Test Summary")
message("========================================")
if (runUnitTests) {
  message("✓ Unit tests: PASSED")
}
if (runIntegrationTests) {
  message("✓ Integration tests: PASSED")
}
message("========================================")
