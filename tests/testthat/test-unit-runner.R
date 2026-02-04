# Runner for unit tests in subdirectory
# This allows devtools::test() and test_check() to find tests in tests/testthat/unit/

# Only run unit tests if they exist
if (dir.exists(testthat::test_path("unit"))) {
    # Use test_dir to run all tests in the unit directory
    # This properly loads setup.R and respects the fixture loading system
    testthat::test_dir(
        testthat::test_path("unit"),
        stop_on_failure = FALSE,
        reporter = testthat::ProgressReporter$new(max_failures = Inf)
    )
}
