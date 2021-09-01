library(testthat)
test_that("Check if package is installed", {
  expect_true(CohortDiagnostics:::is_installed('dplyr'))
  expect_false(CohortDiagnostics:::is_installed('abcd'))
})
