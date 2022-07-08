test_that("getCharacterizationOutput", {
  testDataSource <- getTestDataSource(connectionDetails)

  on.exit(on.exit({
    pool::poolClose(pool = testDataSource$connection)
  }))

  # Very slow function currently
  queryRes <- queryResultCovariateValue(testDataSource,
                                        cohortIds = c(17492),
                                        analysisIds = NULL,
                                        databaseIds = c("Eunomia"),
                                        startDay = NULL,
                                        endDay = NULL,
                                        temporalCovariateValue = TRUE,
                                        temporalCovariateValueDist = TRUE)


  # Check full results
  results <- getCharacterizationOutput(testDataSource,
                                       cohortIds = c(17492),
                                       analysisIds = NULL,
                                       databaseIds = c("Eunomia"),
                                       startDay = NULL,
                                       endDay = NULL,
                                       temporalCovariateValue = TRUE,
                                       temporalCovariateValueDist = TRUE)

  expect_true(is.list(results))
  expect_true("covariateValue" %in% names(results))
  expect_true("covariateValueDist" %in% names(results))
})