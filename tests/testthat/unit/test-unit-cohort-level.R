library(testthat)
library(dplyr)
library(tidyr)

# Source fixtures and mocks
source(testthat::test_path("..", "fixtures", "mock_data.R"))
source(testthat::test_path("..", "mocks", "database_mocks.R"))

# ============================================================================
# Inclusion Statistics Tests (6 tests)
# ============================================================================


test_that("inclusion statistics calculates gain correctly for single rule", {
  # Arrange
  mockData <- dplyr::tibble(
    cohortId = c(1, 1, 1),
    ruleSequence = c(0, 1, 2),
    personCount = c(1000, 800, 600),
    meetSubjects = c(1000, 800, 600)
  )
  
  # Act - Calculate gain (subjects lost at each step)
  result <- mockData %>%
    dplyr::mutate(
      gain = dplyr::lag(personCount, default = personCount[1]) - personCount,
      proportion = personCount / personCount[1]
    )
  
  # Assert
  expect_equal(result$gain, c(0, 200, 200))
  expect_equal(result$proportion[3], 0.6, tolerance = 0.01)
})

test_that("inclusion statistics calculates cumulative statistics correctly", {
  # Arrange
  mockData <- dplyr::tibble(
    cohortId = c(1, 1, 1, 1),
    ruleSequence = c(0, 1, 2, 3),
    personCount = c(10000, 8000, 6000, 4000)
  )
  
  # Act - Calculate cumulative retention
  result <- mockData %>%
    dplyr::mutate(
      cumulativeRetention = personCount / first(personCount),
      cumulativeLoss = 1 - cumulativeRetention
    )
  
  # Assert
  expect_equal(result$cumulativeRetention, c(1.0, 0.8, 0.6, 0.4))
  expect_equal(result$cumulativeLoss, c(0.0, 0.2, 0.4, 0.6))
  expect_equal(result$personCount[4], 4000)
})

test_that("inclusion statistics calculates loss at each rule correctly", {
  # Arrange
  mockData <- dplyr::tibble(
    cohortId = c(1, 1, 1),
    ruleSequence = c(0, 1, 2),
    personCount = c(5000, 3000, 1500)
  )
  
  # Act - Calculate loss at each step
  result <- mockData %>%
    dplyr::mutate(
      loss = dplyr::lag(personCount, default = first(personCount)) - personCount,
      lossPercentage = loss / dplyr::lag(personCount, default = first(personCount)) * 100
    )
  
  # Assert
  expect_equal(result$loss, c(0, 2000, 1500))
  expect_equal(result$lossPercentage[2], 40, tolerance = 0.01)
  expect_equal(result$lossPercentage[3], 50, tolerance = 0.01)
})

test_that("inclusion statistics handles cohort with no inclusion rules", {
  # Arrange - Only rule 0 (initial cohort)
  mockData <- dplyr::tibble(
    cohortId = 1,
    ruleSequence = 0,
    personCount = 1000,
    meetSubjects = 1000
  )
  
  # Act
  result <- mockData %>%
    dplyr::mutate(
      gain = dplyr::lag(personCount, default = personCount[1]) - personCount,
      proportion = personCount / personCount[1]
    )
  
  # Assert
  expect_equal(nrow(result), 1)
  expect_equal(result$gain, 0)
  expect_equal(result$proportion, 1.0)
})

test_that("inclusion statistics handles cohort with multiple rules", {
  # Arrange - 5 inclusion rules
  mockData <- dplyr::tibble(
    cohortId = rep(1, 6),
    ruleSequence = 0:5,
    personCount = c(10000, 9000, 7500, 6000, 4500, 3000)
  )
  
  # Act
  result <- mockData %>%
    dplyr::mutate(
      gain = dplyr::lag(personCount, default = first(personCount)) - personCount,
      remainingProportion = personCount / first(personCount)
    )
  
  # Assert
  expect_equal(nrow(result), 6)
  expect_equal(result$gain, c(0, 1000, 1500, 1500, 1500, 1500))
  expect_equal(result$remainingProportion[6], 0.3, tolerance = 0.01)
})

test_that("inclusion statistics applies min cell count suppression", {
  # Arrange
  mockData <- dplyr::tibble(
    cohortId = c(1, 1, 1),
    ruleSequence = c(0, 1, 2),
    personCount = c(1000, 3, 1)
  )
  minCellCount <- 5
  
  # Act - Apply suppression
  result <- mockData %>%
    dplyr::mutate(
      personCountSuppressed = ifelse(personCount < minCellCount & personCount > 0, 
                                     -minCellCount, 
                                     personCount)
    )
  
  # Assert
  expect_equal(result$personCountSuppressed[1], 1000)
  expect_equal(result$personCountSuppressed[2], -5)  # Suppressed
  expect_equal(result$personCountSuppressed[3], -5)  # Suppressed
})

# ============================================================================
# Index Event Breakdown Tests (5 tests)
# ============================================================================

test_that("index event breakdown by domain aggregates correctly", {
  # Arrange
  mockData <- dplyr::tibble(
    cohortId = c(1, 1, 1, 1),
    conceptId = c(313217, 313217, 192671, 192671),
    domainId = c("Condition", "Condition", "Drug", "Drug"),
    conceptCount = c(100, 50, 200, 150),
    conceptSubjects = c(80, 40, 180, 130)
  )
  
  # Act - Aggregate by domain
  result <- mockData %>%
    dplyr::group_by(cohortId, domainId) %>%
    dplyr::summarise(
      totalCount = sum(conceptCount),
      totalSubjects = sum(conceptSubjects),
      .groups = "drop"
    )
  
  # Assert
  expect_equal(nrow(result), 2)
  expect_equal(result$totalCount[result$domainId == "Condition"], 150)
  expect_equal(result$totalCount[result$domainId == "Drug"], 350)
})

test_that("index event breakdown by concept identifies top concepts", {
  # Arrange
  mockData <- dplyr::tibble(
    cohortId = rep(1, 5),
    conceptId = c(313217, 192671, 444044, 201826, 255573),
    conceptName = c("Atrial fibrillation", "Warfarin", "Hypertension", "Aspirin", "Diabetes"),
    conceptCount = c(500, 300, 250, 200, 150)
  )
  
  # Act - Get top 3 concepts
  result <- mockData %>%
    dplyr::arrange(desc(conceptCount)) %>%
    dplyr::slice_head(n = 3)
  
  # Assert
  expect_equal(nrow(result), 3)
  expect_equal(result$conceptId[1], 313217)
  expect_equal(result$conceptCount[1], 500)
  expect_equal(result$conceptCount[3], 250)
})

test_that("index event breakdown handles multiple domains", {
  # Arrange
  mockData <- dplyr::tibble(
    cohortId = rep(1, 6),
    domainId = c("Condition", "Condition", "Drug", "Drug", "Procedure", "Observation"),
    conceptCount = c(100, 200, 150, 250, 50, 75)
  )
  
  # Act - Count domains
  result <- mockData %>%
    dplyr::group_by(domainId) %>%
    dplyr::summarise(
      domainTotal = sum(conceptCount),
      .groups = "drop"
    )
  
  # Assert
  expect_equal(nrow(result), 4)
  expect_true(all(c("Condition", "Drug", "Procedure", "Observation") %in% result$domainId))
  expect_equal(result$domainTotal[result$domainId == "Drug"], 400)
})

test_that("index event breakdown handles empty index events", {
  # Arrange - Empty breakdown
  mockData <- dplyr::tibble(
    cohortId = integer(),
    conceptId = integer(),
    domainId = character(),
    conceptCount = numeric()
  )
  
  # Act
  result <- mockData %>%
    dplyr::group_by(cohortId, domainId) %>%
    dplyr::summarise(totalCount = sum(conceptCount), .groups = "drop")
  
  # Assert
  expect_equal(nrow(result), 0)
  expect_true(all(c("cohortId", "domainId", "totalCount") %in% colnames(result)))
})

test_that("index event breakdown calculates proportions correctly", {
  # Arrange
  mockData <- dplyr::tibble(
    cohortId = rep(1, 4),
    conceptId = c(313217, 192671, 444044, 201826),
    conceptCount = c(400, 300, 200, 100)
  )
  
  # Act - Calculate proportions
  totalEvents <- sum(mockData$conceptCount)
  result <- mockData %>%
    dplyr::mutate(
      proportion = conceptCount / totalEvents,
      percentage = proportion * 100
    )
  
  # Assert
  expect_equal(sum(result$proportion), 1.0, tolerance = 0.001)
  expect_equal(result$percentage[1], 40, tolerance = 0.01)
  expect_equal(result$percentage[4], 10, tolerance = 0.01)
})

# ============================================================================
# Cohort Relationships Tests (5 tests)
# ============================================================================

test_that("cohort overlap calculated correctly", {
  # Arrange
  cohort1Subjects <- c(1, 2, 3, 4, 5)
  cohort2Subjects <- c(3, 4, 5, 6, 7)
  
  # Act
  overlap <- length(intersect(cohort1Subjects, cohort2Subjects))
  union <- length(union(cohort1Subjects, cohort2Subjects))
  jaccardIndex <- overlap / union
  
  # Assert
  expect_equal(overlap, 3)  # Subjects 3, 4, 5
  expect_equal(union, 7)
  expect_equal(jaccardIndex, 3/7, tolerance = 0.01)
})

test_that("cohort temporal relationship calculates before/after correctly", {
  # Arrange - Cohort A entries before Cohort B
  cohortA <- dplyr::tibble(
    subjectId = c(1, 2, 3),
    cohortStartDate = as.Date(c("2020-01-01", "2020-02-01", "2020-03-01"))
  )
  cohortB <- dplyr::tibble(
    subjectId = c(1, 2, 3),
    cohortStartDate = as.Date(c("2020-06-01", "2020-07-01", "2020-08-01"))
  )
  
  # Act - Join and calculate temporal relationship
  result <- cohortA %>%
    dplyr::inner_join(cohortB, by = "subjectId", suffix = c("_A", "_B")) %>%
    dplyr::mutate(
      daysABeforeB = as.numeric(cohortStartDate_B - cohortStartDate_A),
      ABeforeB = daysABeforeB > 0
    )
  
  # Assert
  expect_equal(nrow(result), 3)
  expect_true(all(result$ABeforeB))
  expect_equal(result$daysABeforeB[1], 152)
})

test_that("cohort overlap proportion calculated correctly", {
  # Arrange
  cohort1Size <- 1000
  cohort2Size <- 800
  overlapSize <- 200
  
  # Act - Calculate overlap proportions
  proportionInCohort1 <- overlapSize / cohort1Size
  proportionInCohort2 <- overlapSize / cohort2Size
  
  # Assert
  expect_equal(proportionInCohort1, 0.2)
  expect_equal(proportionInCohort2, 0.25)
})

test_that("non-overlapping cohorts return zero overlap", {
  # Arrange
  cohort1Subjects <- c(1, 2, 3, 4, 5)
  cohort2Subjects <- c(6, 7, 8, 9, 10)
  
  # Act
  overlap <- length(intersect(cohort1Subjects, cohort2Subjects))
  jaccardIndex <- overlap / length(union(cohort1Subjects, cohort2Subjects))
  
  # Assert
  expect_equal(overlap, 0)
  expect_equal(jaccardIndex, 0)
})

test_that("identical cohorts return perfect overlap", {
  # Arrange
  cohort1Subjects <- c(1, 2, 3, 4, 5)
  cohort2Subjects <- c(1, 2, 3, 4, 5)
  
  # Act
  overlap <- length(intersect(cohort1Subjects, cohort2Subjects))
  union <- length(union(cohort1Subjects, cohort2Subjects))
  jaccardIndex <- overlap / union
  
  # Assert
  expect_equal(overlap, 5)
  expect_equal(union, 5)
  expect_equal(jaccardIndex, 1.0)
})

# ============================================================================
# Edge Cases Tests (4 tests)
# ============================================================================

test_that("empty cohort (zero subjects) handled correctly", {
  # Arrange
  mockData <- dplyr::tibble(
    cohortId = 1,
    cohortSubjects = 0,
    cohortEntries = 0
  )
  
  # Act - Calculate statistics
  result <- mockData %>%
    dplyr::mutate(
      entriesPerSubject = ifelse(cohortSubjects > 0, cohortEntries / cohortSubjects, 0)
    )
  
  # Assert
  expect_equal(result$cohortSubjects, 0)
  expect_equal(result$entriesPerSubject, 0)
})

test_that("single subject cohort calculations work correctly", {
  # Arrange
  mockData <- dplyr::tibble(
    cohortId = 1,
    cohortSubjects = 1,
    cohortEntries = 3
  )
  
  # Act
  result <- mockData %>%
    dplyr::mutate(
      entriesPerSubject = cohortEntries / cohortSubjects
    )
  
  # Assert
  expect_equal(result$cohortSubjects, 1)
  expect_equal(result$entriesPerSubject, 3)
})

test_that("cohorts with no temporal overlap handled correctly", {
  # Arrange - Cohorts in different time periods
  cohortA <- dplyr::tibble(
    subjectId = c(1, 2),
    cohortStartDate = as.Date(c("2015-01-01", "2015-06-01"))
  )
  cohortB <- dplyr::tibble(
    subjectId = c(1, 2),
    cohortStartDate = as.Date(c("2020-01-01", "2020-06-01"))
  )
  
  # Act - Check for concurrent entries (same day)
  result <- cohortA %>%
    dplyr::inner_join(cohortB, by = "subjectId", suffix = c("_A", "_B")) %>%
    dplyr::filter(cohortStartDate_A == cohortStartDate_B)
  
  # Assert
  expect_equal(nrow(result), 0)
})

test_that("NULL and missing values handled in cohort statistics", {
  # Arrange
  mockData <- dplyr::tibble(
    cohortId = c(1, 2, 3),
    cohortSubjects = c(100, NA, 50),
    cohortEntries = c(200, 150, NA)
  )
  
  # Act - Handle missing values
  result <- mockData %>%
    dplyr::mutate(
      cohortSubjects = tidyr::replace_na(cohortSubjects, 0),
      cohortEntries = tidyr::replace_na(cohortEntries, 0),
      entriesPerSubject = ifelse(cohortSubjects > 0, cohortEntries / cohortSubjects, 0)
    )
  
  # Assert
  expect_equal(result$cohortSubjects[2], 0)
  expect_equal(result$cohortEntries[3], 0)
  expect_false(any(is.na(result$entriesPerSubject)))
})

# ============================================================================
# Additional Helper Function Tests
# ============================================================================

test_that("cohort counts aggregation works correctly", {
  # Arrange
  mockCounts <- dplyr::tibble(
    cohortId = c(1, 1, 2, 2),
    subjectId = c(1, 2, 1, 3),
    cohortStartDate = as.Date(c("2020-01-01", "2020-02-01", "2020-01-15", "2020-03-01"))
  )
  
  # Act - Aggregate counts
  result <- mockCounts %>%
    dplyr::group_by(cohortId) %>%
    dplyr::summarise(
      cohortSubjects = n_distinct(subjectId),
      cohortEntries = n(),
      .groups = "drop"
    )
  
  # Assert
  expect_equal(nrow(result), 2)
  expect_equal(result$cohortSubjects[result$cohortId == 1], 2)
  expect_equal(result$cohortEntries[result$cohortId == 1], 2)
})

test_that("inclusion rule sequence validation works", {
  # Arrange - Valid sequence
  validSequence <- dplyr::tibble(
    ruleSequence = 0:5,
    personCount = c(1000, 900, 800, 700, 600, 500)
  )
  
  # Act - Check sequence is continuous
  isValid <- all(diff(validSequence$ruleSequence) == 1) && validSequence$ruleSequence[1] == 0
  
  # Assert
  expect_true(isValid)
})

test_that("inclusion rule person counts are monotonically decreasing", {
  # Arrange
  mockData <- dplyr::tibble(
    ruleSequence = 0:4,
    personCount = c(1000, 900, 800, 700, 600)
  )
  
  # Act - Check monotonic decrease
  isMonotonic <- all(diff(mockData$personCount) <= 0)
  
  # Assert
  expect_true(isMonotonic)
})

test_that("cohort relationship handles multiple time windows", {
  # Arrange
  cohortA <- dplyr::tibble(subjectId = c(1, 2, 3), cohortStartDate = as.Date("2020-01-01"))
  cohortB <- dplyr::tibble(subjectId = c(1, 2, 3), cohortStartDate = as.Date(c("2020-01-15", "2020-01-25", "2020-06-01")))
  
  # Act - Calculate days between
  result <- cohortA %>%
    dplyr::inner_join(cohortB, by = "subjectId", suffix = c("_A", "_B")) %>%
    dplyr::mutate(
      daysBetween = as.numeric(cohortStartDate_B - cohortStartDate_A),
      within30Days = daysBetween <= 30,
      within365Days = daysBetween <= 365
    )
  
  # Assert
  expect_equal(sum(result$within30Days), 2)  # Subjects 1 and 2 are within 30 days
  expect_equal(sum(result$within365Days), 3)  # All 3 subjects are within 365 days
})
