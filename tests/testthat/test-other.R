test_that("Check if package is installed", {
  testthat::expect_true(CohortDiagnostics:::is_installed('dplyr'))
  testthat::expect_false(CohortDiagnostics:::is_installed('rqrwqrewrqwRANDOMSTRINGdfdsfdsfds')) # just a random string to represent package does not exst
})

test_that("Check helper functions", {
  testthat::expect_equal(CohortDiagnostics:::camelCaseToTitleCase('appleTree'), 'Apple Tree')
  testthat::expect_equal(CohortDiagnostics:::snakeCaseToCamelCase('apple_tree'), 'appleTree')
  testthat::expect_equal(CohortDiagnostics:::quoteLiterals(NULL), '')
})

test_that("Ensure Cohort Diagnostics is installed", {
  testthat::expect_true(CohortDiagnostics:::ensure_installed('CohortDiagnostics'))
})