library(testthat)
library(CohortDiagnostics)

test_that("CohortDiagnostics:::hasData works as expected", {
  # NULL
  expect_false(CohortDiagnostics:::hasData(NULL))
  
  # Data frame
  expect_true(CohortDiagnostics:::hasData(data.frame(a = 1)))
  expect_false(CohortDiagnostics:::hasData(data.frame()))
  expect_false(CohortDiagnostics:::hasData(data.frame(a = logical()))) # Added from instruction
  
  # Vector/List
  expect_true(CohortDiagnostics:::hasData(c(1, 2)))
  expect_false(CohortDiagnostics:::hasData(c()))
  
  # NA
  expect_false(CohortDiagnostics:::hasData(NA))
  expect_true(CohortDiagnostics:::hasData(c(1, NA)))
})
