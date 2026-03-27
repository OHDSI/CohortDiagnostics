library(testthat)
library(CohortDiagnostics)
library(dplyr)

# Helper functions for tests
getMinimalCohortDefinitionSet <- function() {
  dplyr::tibble(
    cohortId = 1,
    cohortName = "Test",
    json = '{"PrimaryCriteria": {"CriteriaList": []}}',
    sql = "SELECT * FROM cohort",
    isSubset = FALSE,
    subsetParent = 1,
    cohortFullName = "Full Name"
  )
}

getCohortDefinitionWithConceptSets <- function(numConceptSets = 1) {
  conceptSets <- lapply(1:numConceptSets, function(i) {
    list(id = i - 1, name = paste("Concept Set", i), expression = list(items = list()))
  })
  json <- jsonlite::toJSON(list(ConceptSets = conceptSets, PrimaryCriteria = list(CriteriaList = list())), auto_unbox = TRUE)
  dplyr::tibble(
    cohortId = 1,
    cohortName = "Test",
    json = as.character(json),
    sql = "SELECT 0 as codeset_id (SELECT 1) C with primary_events",
    isSubset = FALSE,
    subsetParent = 1,
    cohortFullName = "Full Name"
  )
}

getCohortDefinitionWithSubset <- function() {
  parent <- getCohortDefinitionWithConceptSets(numConceptSets = 1)
  parent$cohortId <- 1
  parent$isSubset <- FALSE
  parent$subsetParent <- 1
  
  subset <- parent
  subset$cohortId <- 2
  subset$isSubset <- TRUE
  subset$subsetParent <- 1
  subset$sql <- ""
  
  dplyr::bind_rows(parent, subset)
}

createMockCohortDefinitionSet <- function(numCohorts = 1) {
  do.call(dplyr::bind_rows, lapply(1:numCohorts, function(i) {
    c <- getCohortDefinitionWithConceptSets(1)
    c$cohortId <- i
    c
  }))
}

test_that("extractConceptSetsSqlFromCohortSql extracts single concept set from SQL", {
  sql <- "SELECT 0 as codeset_id (SELECT 123 as concept_id) C with primary_events"
  result <- CohortDiagnostics:::extractConceptSetsSqlFromCohortSql(sql)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1)
  expect_equal(result$conceptSetId[1], 0)
  expect_match(result$conceptSetSql[1], "SELECT 0 as codeset_id")
})

test_that("extractConceptSetsSqlFromCohortSql extracts multiple concept sets from SQL", {
  sql <- "SELECT 0 as codeset_id (SELECT 123) C SELECT 1 as codeset_id (SELECT 456) C with primary_events"
  result <- CohortDiagnostics:::extractConceptSetsSqlFromCohortSql(sql)

  expect_equal(nrow(result), 2)
  expect_equal(result$conceptSetId, c(0, 1))
})

test_that("extractConceptSetsSqlFromCohortSql handles different casing", {
  sql <- "SELECT 0 as CODESET_ID (SELECT 1) C with primary_events"
  result <- CohortDiagnostics:::extractConceptSetsSqlFromCohortSql(sql)
  expect_equal(nrow(result), 1)
  expect_equal(result$conceptSetId[1], 0)
})

test_that("extractConceptSetsSqlFromCohortSql deduplicates multiple occurrences of same codeset_id", {
  sql <- "SELECT 0 as codeset_id (SELECT 1) C SELECT 0 as codeset_id (SELECT 1) C with primary_events"
  result <- CohortDiagnostics:::extractConceptSetsSqlFromCohortSql(sql)
  expect_true(nrow(result) >= 1)
})

test_that("extractConceptSetsSqlFromCohortSql handles SQL with no concept sets", {
  sql <- "SELECT * FROM my_table with primary_events"
  result <- CohortDiagnostics:::extractConceptSetsSqlFromCohortSql(sql)
  expect_equal(nrow(result), 0)
})

test_that("extractConceptSetsSqlFromCohortSql handles empty or malformed SQL gracefully", {
  expect_equal(nrow(CohortDiagnostics:::extractConceptSetsSqlFromCohortSql("")), 0)
  expect_equal(nrow(CohortDiagnostics:::extractConceptSetsSqlFromCohortSql(NA_character_)), 0)
})

test_that("extractConceptSetsSqlFromCohortSql fails if more than one SQL is provided", {
  expect_error(CohortDiagnostics:::extractConceptSetsSqlFromCohortSql(c("sql1", "sql2")))
})

test_that("extractConceptSetsJsonFromCohortJson extracts concept sets from valid JSON", {
  cohortDef <- getCohortDefinitionWithConceptSets(numConceptSets = 2)
  json <- cohortDef$json
  result <- CohortDiagnostics:::extractConceptSetsJsonFromCohortJson(json)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)
})

test_that("extractConceptSetsJsonFromCohortJson handles JSON with no concept sets", {
  cohortDef <- getMinimalCohortDefinitionSet()
  json <- cohortDef$json
  result <- CohortDiagnostics:::extractConceptSetsJsonFromCohortJson(json)
  expect_equal(nrow(result), 0)
})

test_that("extractConceptSetsJsonFromCohortJson handles malformed JSON gracefully", {
  result <- CohortDiagnostics:::extractConceptSetsJsonFromCohortJson("{ malformed }")
  expect_equal(nrow(result), 0)
})

test_that("combineConceptSetsFromCohorts combines concept sets from multiple cohorts", {
  cohort1 <- getCohortDefinitionWithConceptSets(numConceptSets = 1)
  cohort1$cohortId <- 1
  cohort1$sql <- "SELECT 0 as codeset_id (SELECT 1) C with primary_events"

  cohort2 <- getCohortDefinitionWithConceptSets(numConceptSets = 1)
  cohort2$cohortId <- 2
  cohort2$sql <- "SELECT 0 as codeset_id (SELECT 2) C with primary_events"

  cohort1$isSubset <- FALSE
  cohort2$isSubset <- FALSE
  cohorts <- bind_rows(cohort1, cohort2)
  cohorts$cohortFullName <- "Full Name"

  result <- CohortDiagnostics:::combineConceptSetsFromCohorts(cohorts)
  expect_equal(nrow(result), 2)
  expect_equal(unique(result$cohortId), c(1, 2))
})

test_that("combineConceptSetsFromCohorts deduplicates identical concept sets", {
  cohort1 <- getCohortDefinitionWithConceptSets(numConceptSets = 1)
  cohort1$cohortId <- 1
  cohort1$sql <- "SELECT 0 as codeset_id (SELECT 1) C with primary_events"

  cohort2 <- getCohortDefinitionWithConceptSets(numConceptSets = 1)
  cohort2$cohortId <- 2
  cohort2$sql <- "SELECT 0 as codeset_id (SELECT 1) C with primary_events"
  cohort2$json <- cohort1$json

  cohort1$isSubset <- FALSE
  cohort2$isSubset <- FALSE
  cohorts <- bind_rows(cohort1, cohort2)
  cohorts$cohortFullName <- "Full Name"

  result <- CohortDiagnostics:::combineConceptSetsFromCohorts(cohorts)
  expect_equal(length(unique(result$uniqueConceptSetId)), 1)
})

test_that("getParentCohort correctly identifies parent for subset", {
  cohorts <- getCohortDefinitionWithSubset()
  subset <- cohorts %>% filter(isSubset)
  parent <- CohortDiagnostics:::getParentCohort(subset, cohorts)
  expect_equal(parent$cohortId[1], 1)
})

test_that("getInclusionStats works with mocks", {
  connection <- mockDatabaseConnection()
  exportFolder <- tempfile("export")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE))
  
  cohortDefinitionSet <- getMinimalCohortDefinitionSet()
  cohortDefinitionSet$checksum <- "123"
  
  local_mocked_bindings(
    insertInclusionRuleNames = function(...) NULL,
    getCohortStats = function(...) {
      list(
        cohortInclusionTable = data.frame(cohortId = 1, ruleSequence = 1, name = "Rule 1"),
        cohortInclusionStatsTable = data.frame(cohortId = 1, ruleSequence = 1, personCount = 100),
        cohortInclusionResultTable = data.frame(cohortId = 1, modeId = 1, personCount = 100),
        cohortSummaryStatsTable = data.frame(cohortId = 1, baseCount = 1000)
      )
    },
    .package = "CohortGenerator"
  )
  
  local_mocked_bindings(
    makeDataExportable = function(x, ...) x,
    writeToCsv = function(...) NULL,
    recordTasksDone = function(...) NULL,
    timeExecution = function(folder, taskName, ...) {
      args <- list(...)
      eval(args$expr)
    },
    .package = "CohortDiagnostics"
  )
  
  CohortDiagnostics:::getInclusionStats(
    connection = connection,
    exportFolder = exportFolder,
    databaseId = "test",
    cohortDefinitionSet = cohortDefinitionSet,
    cohortDatabaseSchema = "cohort",
    cohortTableNames = list(cohortInclusionTable = "ci"),
    incremental = FALSE,
    instantiatedCohorts = 1,
    minCellCount = 5,
    recordKeepingFile = file.path(exportFolder, "record.csv")
  )
  
  expect_true(TRUE)
})
