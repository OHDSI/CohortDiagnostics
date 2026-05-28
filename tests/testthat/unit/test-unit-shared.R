library(testthat)
library(CohortDiagnostics)

test_that("hasData works as expected", {
  # NULL
  expect_false(hasData(NULL))
  
  # Data frame
  expect_true(hasData(data.frame(a = 1)))
  expect_false(hasData(data.frame()))
  expect_false(hasData(data.frame(a = logical()))) # Added from instruction
  
  # Vector/List
  expect_true(hasData(c(1, 2)))
  expect_false(hasData(c()))
  
  # NA
  expect_false(hasData(NA))
  expect_true(hasData(c(1, NA)))
})
