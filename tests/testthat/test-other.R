testthat::test_that("Check if package is installed", {
  testthat::expect_true(CohortDiagnostics:::is_installed('dplyr'))
  testthat::expect_false(CohortDiagnostics:::is_installed('rqrwqrewrqwRANDOMSTRINGdfdsfdsfds')) # just a random string to represent package does not exst
})

testthat::test_that("Check if package is installed", {
  testthat::expect_null(CohortDiagnostics:::ensure_installed('dplyr'))
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
  writeLines(latinEncoding, con = tempfileLatin)
  testthat::expect_warning(CohortDiagnostics:::checkInputFileEncoding(fileName = tempfileLatin))
  unlink(tempfileLatin)
})


testthat::test_that("Check mismatch between SQL and inclusion rules", {
  fileJson <- system.file("cohorts", "17492.json", package = 'CohortDiagnostics')

  cohortJson <-
    RJSONIO::fromJSON(content = fileJson, digits = 23) %>%
    RJSONIO::toJSON(digits = 23, pretty = TRUE)

  expression <-
    CirceR::cohortExpressionFromJson(expressionJson = cohortJson)

  sqlWithoutInclusionRule <-
    CirceR::buildCohortQuery(
      expression = expression,
      options = CirceR::createGenerateOptions(generateStats = FALSE)
    ) %>% SqlRender::render()
  sqlWithInclusionRule <-
    CirceR::buildCohortQuery(
      expression = expression,
      options = CirceR::createGenerateOptions(generateStats = TRUE)
    ) %>% SqlRender::render()

  # expect warning
  testthat::expect_warning(
    CohortDiagnostics:::.warnMismatchSqlInclusionStats(sql = sqlWithoutInclusionRule, generateInclusionStats = TRUE)
  )
  # no warning
  testthat::expect_null(
    CohortDiagnostics:::.warnMismatchSqlInclusionStats(sql = sqlWithoutInclusionRule, generateInclusionStats = FALSE)
  )

  # no warning
  CohortDiagnostics:::.warnMismatchSqlInclusionStats(sql = sqlWithInclusionRule, generateInclusionStats = TRUE)
  # expect warning
  testthat::expect_warning(
    CohortDiagnostics:::.warnMismatchSqlInclusionStats(sql = sqlWithInclusionRule,
                                                       generateInclusionStats = FALSE)
  )

})

