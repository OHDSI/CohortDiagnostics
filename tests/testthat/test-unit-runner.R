# Runner for unit tests in subdirectory
# This allows devtools::test() and test_check() to find tests in tests/testthat/unit/

if (dir.exists(testthat::test_path("unit"))) {
    # List all test files in the unit directory
    unit_tests <- list.files(
        testthat::test_path("unit"),
        pattern = "^test.*\\.R$",
        full.names = TRUE
    )

    # Source each file to run the tests
    # We use source() to preserve the testthat environment (helpers, setup)
    for (test_file in unit_tests) {
        testthat::test_that(paste("Sourcing", basename(test_file)), {
            source(test_file, local = TRUE)
        })
    }
}
