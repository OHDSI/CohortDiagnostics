
testthat::test_that("Check if package is installed", {
  testthat::expect_true(CohortDiagnostics:::is_installed('dplyr'))
  testthat::expect_false(CohortDiagnostics:::is_installed('rqrwqrewrqwRANDOMSTRINGdfdsfdsfds')) # just a random string to represent package does not exst
})

testthat::test_that("Check helper functions", {
  testthat::expect_equal(CohortDiagnostics:::camelCaseToTitleCase('appleTree'),
                         'Apple Tree')
  testthat::expect_equal(CohortDiagnostics:::snakeCaseToCamelCase('apple_tree'),
                         'appleTree')
  testthat::expect_equal(CohortDiagnostics:::quoteLiterals(NULL), '')
})

testthat::test_that("Ensure Cohort Diagnostics is installed", {
  testthat::expect_null(CohortDiagnostics:::ensure_installed('CohortDiagnostics'))
})


testthat::test_that("util functions", {
  testthat::expect_true(CohortDiagnostics:::naToEmpty(NA) == "")
  testthat::expect_true(CohortDiagnostics:::naToZero(NA) == 0)
})

testthat::test_that("Test file encoding - using latin", {
  # Latin encoding
  tempfileLatin <- tempfile()
  latinEncoding <- "fa\xE7ile"
  writeChar(object = latinEncoding, con = tempfileLatin)
  testthat::expect_error(CohortDiagnostics:::checkInputFileEncoding(fileName = tempfileLatin))
  unlink(tempfileLatin)
})
