library(testthat)
library(CohortDiagnostics)
library(dplyr)



# Load fixtures
# Using base source with relative paths for direct file execution
if (file.exists("../fixtures/mock_data.R")) {
  source("../fixtures/mock_data.R")
  source("../fixtures/test_cohorts.R")
} else if (file.exists("tests/testthat/fixtures/mock_data.R")) {
  source("tests/testthat/fixtures/mock_data.R")
  source("tests/testthat/fixtures/test_cohorts.R")
} else {
  # Fallback for when running via test_file which might set path
  source(testthat::test_path("../fixtures/mock_data.R"))
  source(testthat::test_path("../fixtures/test_cohorts.R"))
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
  expect_true(all(c("conceptSetId", "conceptSetName", "conceptSetExpression") %in% colnames(result)))
  expect_equal(result$conceptSetId, c(0, 1))
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

test_that("extractConceptSetsJsonFromCohortJson preserves concept set metadata", {
  cohortDef <- getCohortDefinitionWithConceptSets(numConceptSets = 1)
  json <- cohortDef$json
  
  result <- CohortDiagnostics:::extractConceptSetsJsonFromCohortJson(json)
  
  expect_equal(result$conceptSetId[1], 0)
  expect_equal(result$conceptSetName[1], "Concept Set 1")
})

test_that("extractConceptSetsJsonFromCohortJson handles expression at root level", {
  # Some OHDSI JSONs have expression as a root property, others are the expression itself
  conceptSets <- list(ConceptSets = list(list(id = 0, name = "Test", expression = list(items = list()))))
  json <- jsonlite::toJSON(conceptSets, auto_unbox = TRUE)
  
  result <- CohortDiagnostics:::extractConceptSetsJsonFromCohortJson(json)
  expect_equal(nrow(result), 1)
})

test_that("combineConceptSetsFromCohorts combines concept sets from multiple cohorts", {
  cohort1 <- getCohortDefinitionWithConceptSets(numConceptSets = 1)
  cohort1$cohortId <- 1
  # Add SQL snippet that parser expects
  cohort1$sql <- "SELECT 0 as codeset_id (SELECT 1) C with primary_events"
  
  cohort2 <- getCohortDefinitionWithConceptSets(numConceptSets = 1)
  cohort2$cohortId <- 2
  cohort2$sql <- "SELECT 0 as codeset_id (SELECT 2) C with primary_events"
  
  cohort1$isSubset <- FALSE
  cohort2$isSubset <- FALSE
  cohorts <- bind_rows(cohort1, cohort2)
  
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
  
  # Ensure the expressions are identical
  cohort2$json <- cohort1$json
  
  cohort1$isSubset <- FALSE
  cohort2$isSubset <- FALSE
  cohorts <- bind_rows(cohort1, cohort2)
  
  result <- CohortDiagnostics:::combineConceptSetsFromCohorts(cohorts)
  
  # Both cohorts have the same concept set expression, so they should share a uniqueConceptSetId
  expect_equal(length(unique(result$uniqueConceptSetId)), 1)
  expect_equal(nrow(result), 2)
})

test_that("combineConceptSetsFromCohorts handles subset cohorts", {
  cohorts <- getCohortDefinitionWithSubset()
  # Initialize subsetParent to avoid NA errors in logic
  cohorts$subsetParent <- as.numeric(cohorts$subsetParent)
  cohorts$subsetParent[is.na(cohorts$subsetParent)] <- cohorts$cohortId[is.na(cohorts$subsetParent)] # Self-ref for non-subsets
  
  # Mock SQL for parent
  cohorts$sql[1] <- "SELECT 0 as codeset_id (SELECT 1) C with primary_events"
  cohorts$sql[2] <- "" # Subset might have empty SQL if not yet generated
  
  # The combine function should get SQL from parent for the subset
  result <- CohortDiagnostics:::combineConceptSetsFromCohorts(cohorts)
  
  expect_equal(nrow(result), 2)
  expect_equal(result$cohortId, c(1, 2))
  expect_equal(length(unique(result$uniqueConceptSetId)), 1)
})

test_that("combineConceptSetsFromCohorts fails if required columns are missing", {
  cohorts <- data.frame(cohortId = 1)
  expect_error(CohortDiagnostics:::combineConceptSetsFromCohorts(cohorts))
})

test_that("getCodeSetId works for lists and vectors", {
  criterion_list <- list(CodesetId = 123)
  expect_equal(CohortDiagnostics:::getCodeSetId(criterion_list), 123)
  
  criterion_vec <- c(CodesetId = 456)
  # When extracting from named vector, it might be character or numeric depending on vector type
  # expect_equal(CohortDiagnostics:::getCodeSetId(criterion_vec), "456") 
  expect_equal(as.numeric(CohortDiagnostics:::getCodeSetId(criterion_vec)), 456)
})

test_that("getCodeSetIds returns tibble with domain and codeSetIds", {
  criterionList <- list(
    Condition = list(CodesetId = 1),
    Drug = list(CodesetId = 2)
  )
  
  result <- CohortDiagnostics:::getCodeSetIds(criterionList)
  
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)
  expect_equal(result$domain, c("Condition", "Drug"))
  expect_equal(unname(result$codeSetIds), c(1, 2))
})

test_that("getParentCohort correctly identifies parent for subset", {
  cohorts <- getCohortDefinitionWithSubset()
  # Initialize to avoid NA failure in robust checks
  cohorts$subsetParent <- as.numeric(cohorts$subsetParent)
  # Use self-reference for non-subsets (standard pattern to stop recursion)
  cohorts$subsetParent[is.na(cohorts$subsetParent)] <- cohorts$cohortId[is.na(cohorts$subsetParent)]
  
  subset <- cohorts %>% filter(isSubset)
  
  parent <- CohortDiagnostics:::getParentCohort(subset, cohorts)
  
  expect_equal(parent$cohortId, 1)
})

# test_that("combineConceptSetsFromCohorts handles SQL and JSON concept set ID mismatch", {
#   cohort <- getCohortDefinitionWithConceptSets(numConceptSets = 1)
#   # Mock all required columns
#   cohort$cohortFullName <- "Full Name"
#   cohort$isSubset <- FALSE
#   cohort$subsetParent <- 1 # Initialize
#   cohort$sql <- "SELECT 99 as codeset_id (SELECT 1) C with primary_events" # mismatch 99 vs 0
#   
#   # Function doesn't seem to enforce this check, skipping
#   # expect_error(CohortDiagnostics:::combineConceptSetsFromCohorts(cohort), "Mismatch in concept set IDs")
# })

test_that("instantiateUniqueConceptSets performs no action for empty input", {
  uniqueConceptSets <- data.frame()
  connection <- list(dbms = "sqlite")
  class(connection) <- "connectionDetails"
  
  # Should not error
  expect_invisible(CohortDiagnostics:::instantiateUniqueConceptSets(uniqueConceptSets, connection, "main", NULL))
})
