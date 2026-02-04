# Runner for integration tests in subdirectory
# This allows devtools::test() and test_check() to find tests in tests/testthat/integration/
# These tests run ONLY if INTEGRATION_TESTS environment variable is "TRUE"

if (Sys.getenv("INTEGRATION_TESTS") == "TRUE" && dir.exists(testthat::test_path("integration"))) {
    # List all test files in the integration directory
    integration_tests <- list.files(
        testthat::test_path("integration"),
        pattern = "^test.*\\.R$",
        full.names = TRUE
    )

    # Source each file to run the tests
    # We use source() to preserve the environment
    for (test_file in integration_tests) {
        source(test_file, local = TRUE)
    }
}
