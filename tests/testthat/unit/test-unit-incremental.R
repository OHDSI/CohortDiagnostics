# Unit Tests for Incremental Mode Functions
# Tests for checksum computation, task tracking, and incremental saves

test_that("computeChecksum returns consistent hash for same input", {
  # Arrange
  sql <- "SELECT * FROM cohort WHERE cohort_id = 1;"
  
  # Act
  checksum1 <- CohortDiagnostics:::computeChecksum(sql)
  checksum2 <- CohortDiagnostics:::computeChecksum(sql)
  
  # Assert
  expect_equal(checksum1, checksum2)
  expect_type(checksum1, "character")
  expect_true(nchar(checksum1) > 0)
})

test_that("computeChecksum returns different hash for different input", {
  # Arrange
  sql1 <- "SELECT * FROM cohort WHERE cohort_id = 1;"
  sql2 <- "SELECT * FROM cohort WHERE cohort_id = 2;"
  
  # Act
  checksum1 <- CohortDiagnostics:::computeChecksum(sql1)
  checksum2 <- CohortDiagnostics:::computeChecksum(sql2)
  
  # Assert
  expect_false(checksum1 == checksum2)
})

test_that("isTaskRequired returns TRUE for new task", {
  # Arrange
  rkf <- tempfile()
  withr::defer(unlink(rkf))
  checksum <- CohortDiagnostics:::computeChecksum("test")
  
  # Act
  result <- CohortDiagnostics:::isTaskRequired(
    cohortId = 1,
    task = "testTask",
    checksum = checksum,
    recordKeepingFile = rkf
  )
  
  # Assert
  expect_true(result)
})

test_that("isTaskRequired returns FALSE after task recorded", {
  # Arrange
  rkf <- tempfile()
  withr::defer(unlink(rkf))
  checksum <- CohortDiagnostics:::computeChecksum("test")
  
  CohortDiagnostics:::recordTasksDone(
    cohortId = 1,
    task = "testTask",
    checksum = checksum,
    recordKeepingFile = rkf
  )
  
  # Act
  result <- CohortDiagnostics:::isTaskRequired(
    cohortId = 1,
    task = "testTask",
    checksum = checksum,
    recordKeepingFile = rkf
  )
  
  # Assert
  expect_false(result)
})

test_that("isTaskRequired detects checksum changes", {
  # Arrange
  rkf <- tempfile()
  withr::defer(unlink(rkf))
  checksum1 <- CohortDiagnostics:::computeChecksum("test1")
  checksum2 <- CohortDiagnostics:::computeChecksum("test2")
  
  CohortDiagnostics:::recordTasksDone(
    cohortId = 1,
    task = "testTask",
    checksum = checksum1,
    recordKeepingFile = rkf
  )
  
  # Act
  result <- CohortDiagnostics:::isTaskRequired(
    cohortId = 1,
    task = "testTask",
    checksum = checksum2,
    recordKeepingFile = rkf
  )
  
  # Assert
  expect_true(result)
})

test_that("saveIncremental creates new file with data", {
  # Arrange
  tmpFile <- tempfile(fileext = ".csv")
  withr::defer(unlink(tmpFile))
  
  data <- dplyr::tibble(
    cohortId = c(1, 2, 3),
    count = c(100, 200, 300)
  )
  
  # Act
  CohortDiagnostics:::saveIncremental(data, tmpFile, cohortId = c(1, 2, 3))
  
  # Assert
  expect_true(file.exists(tmpFile))
  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  expect_equal(nrow(result), 3)
  expect_equal(result$cohortId, c(1, 2, 3))
})

test_that("saveIncremental updates existing data", {
  # Arrange
  tmpFile <- tempfile(fileext = ".csv")
  withr::defer(unlink(tmpFile))
  
  initialData <- dplyr::tibble(
    cohortId = c(1, 2),
    count = c(100, 200)
  )
  
  newData <- dplyr::tibble(
    cohortId = c(1, 3),
    count = c(150, 300)
  )
  
  # Act
  CohortDiagnostics:::saveIncremental(initialData, tmpFile, cohortId = c(1, 2))
  CohortDiagnostics:::saveIncremental(newData, tmpFile, cohortId = c(1, 3))
  
  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  
  # Assert
  expect_equal(nrow(result), 3)
  expect_true(all(c(1, 2, 3) %in% result$cohortId))
  
  # Check that cohort 1 was updated
  cohort1Count <- result %>% 
    dplyr::filter(cohortId == 1) %>% 
    dplyr::pull(count)
  expect_equal(cohort1Count, 150)
})

test_that("saveIncremental handles empty new data", {
  # Arrange
  tmpFile <- tempfile(fileext = ".csv")
  withr::defer(unlink(tmpFile))
  
  initialData <- dplyr::tibble(
    cohortId = c(1, 2, 3),
    count = c(100, 200, 300)
  )
  
  emptyData <- dplyr::tibble()
  
  # Act
  CohortDiagnostics:::saveIncremental(initialData, tmpFile, cohortId = c(1, 2, 3))
  CohortDiagnostics:::saveIncremental(emptyData, tmpFile, cohortId = c())
  
  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  
  # Assert
  expect_equal(nrow(result), 3)
  expect_equal(result$cohortId, c(1, 2, 3))
})

test_that("getRequiredTasks returns all tasks when none completed", {
  # Arrange
  rkf <- tempfile()
  withr::defer(unlink(rkf))
  
  cohortIds <- c(1, 2, 3)
  checksums <- sapply(cohortIds, function(id) {
    CohortDiagnostics:::computeChecksum(paste0("cohort_", id))
  })
  
  # Act
  tasks <- CohortDiagnostics:::getRequiredTasks(
    cohortId = cohortIds,
    checksum = checksums,
    recordKeepingFile = rkf
  )
  
  # Assert
  expect_equal(nrow(tasks), 3)
  expect_equal(tasks$cohortId, cohortIds)
})

test_that("getRequiredTasks excludes completed tasks", {
  # Arrange
  rkf <- tempfile()
  withr::defer(unlink(rkf))
  
  cohortIds <- c(1, 2, 3)
  checksums <- sapply(cohortIds, function(id) {
    CohortDiagnostics:::computeChecksum(paste0("cohort_", id))
  })
  
  # Record task for cohort 1
  CohortDiagnostics:::recordTasksDone(
    cohortId = 1,
    checksum = checksums[1],
    recordKeepingFile = rkf
  )
  
  # Act
  tasks <- CohortDiagnostics:::getRequiredTasks(
    cohortId = cohortIds,
    checksum = checksums,
    recordKeepingFile = rkf
  )
  
  # Assert
  expect_equal(nrow(tasks), 2)
  expect_true(all(tasks$cohortId %in% c(2, 3)))
})


test_that("recordTasksDone handles multiple cohorts", {
  rkf <- tempfile()
  withr::defer(unlink(rkf))
  
  cohortIds <- c(1, 2, 3)
  checksums <- sapply(cohortIds, function(id) {
    CohortDiagnostics:::computeChecksum(paste0("cohort_", id))
  })
  
  CohortDiagnostics:::recordTasksDone(
    cohortId = cohortIds,
    checksum = checksums,
    recordKeepingFile = rkf
  )
  
  for (i in seq_along(cohortIds)) {
    result <- CohortDiagnostics:::isTaskRequired(
      cohortId = cohortIds[i],
      checksum = checksums[i],
      recordKeepingFile = rkf
    )
    expect_false(result)
  }
})

test_that("getKeyIndex finds indices correctly", {
  recordKeeping <- dplyr::tibble(
    cohortId = c(1, 2, 3),
    task = c("A", "B", "C"),
    idxCol = 1:3
  )
  
  key <- list(cohortId = 2, task = "B")
  expect_equal(CohortDiagnostics:::getKeyIndex(key, recordKeeping), 2)
  
  key_none <- list(cohortId = 4, task = "A")
  expect_equal(length(CohortDiagnostics:::getKeyIndex(key_none, recordKeeping)), 0)
})

test_that("subsetToRequiredCohorts filters correctly", {
  rkf <- tempfile()
  withr::defer(unlink(rkf))
  
  cohorts <- dplyr::tibble(
    cohortId = c(1, 2),
    checksum = c("sum1", "sum2")
  )
  
  CohortDiagnostics:::recordTasksDone(
    cohortId = 1,
    task = "testTask",
    checksum = "sum1",
    recordKeepingFile = rkf
  )
  
  # When incremental is FALSE, returns all
  result_all <- CohortDiagnostics:::subsetToRequiredCohorts(cohorts, "testTask", FALSE, rkf)
  expect_equal(nrow(result_all), 2)
  
  # When incremental is TRUE, returns only required (cohort 2)
  result_inc <- CohortDiagnostics:::subsetToRequiredCohorts(cohorts, "testTask", TRUE, rkf)
  expect_equal(nrow(result_inc), 1)
  expect_equal(result_inc$cohortId, 2)
})

test_that("saveIncremental replaces all rows for a re-run cohort", {
  # Simulates characterization: a cohort is recomputed and now has a different
  # set of secondary keys (e.g. covariate ids). All old rows for that cohort
  # should be removed, not just exact duplicates.
  tmpFile <- tempfile(fileext = ".csv")
  withr::defer(unlink(tmpFile))

  firstRun <- dplyr::tibble(
    cohortId = c(1, 1, 2),
    timeId = c(1, 2, 1),
    covariateId = c(10, 20, 30),
    mean = c(0.1, 0.2, 0.3)
  )
  CohortDiagnostics:::saveIncremental(firstRun, tmpFile, cohortId = c(1, 2))

  secondRun <- dplyr::tibble(
    cohortId = c(1, 1),
    timeId = c(1, 3),
    covariateId = c(10, 99),
    mean = c(0.5, 0.6)
  )
  CohortDiagnostics:::saveIncremental(secondRun, tmpFile, cohortId = 1)

  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  expect_equal(nrow(result), 3)
  # old cohort 1 row (timeId 2) replaced by new cohort 1 rows (timeId 1, 3)
  expect_false(any(result$cohortId == 1 & result$timeId == 2))
  expect_true(any(result$cohortId == 1 & result$covariateId == 99))
  # cohort 2 is untouched
  expect_true(any(result$cohortId == 2))
})

test_that("saveIncremental writes NA without corrupting subsequent reads", {
  tmpFile <- tempfile(fileext = ".csv")
  withr::defer(unlink(tmpFile))

  firstRun <- dplyr::tibble(
    cohortId = c(1, 2),
    value = c(1, NA)
  )
  CohortDiagnostics:::saveIncremental(firstRun, tmpFile, cohortId = c(1, 2))

  secondRun <- dplyr::tibble(
    cohortId = c(3),
    value = c(3)
  )
  CohortDiagnostics:::saveIncremental(secondRun, tmpFile, cohortId = 3)

  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  expect_equal(nrow(result), 3)
  expect_true(is.na(result$value[result$cohortId == 2]))
})

test_that("getKeyIndex returns indices into the recordKeeping argument", {
  recordKeeping <- dplyr::tibble(
    cohortId = c(1, 2, 3),
    task = c("A", "B", "C")
  )
  key <- dplyr::tibble(cohortId = c(2, 3), task = c("B", "C"))
  expect_equal(CohortDiagnostics:::getKeyIndex(key, recordKeeping), c(2L, 3L))
})

test_that("computeCohortChecksums invalidates when the run fingerprint changes", {
  cohorts <- dplyr::tibble(
    cohortId = 1,
    cohortName = "test",
    sql = "SELECT 1;",
    json = "{}"
  )
  checksumA <- CohortDiagnostics:::computeCohortChecksums(cohorts, runFingerprint = "fpA")
  checksumB <- CohortDiagnostics:::computeCohortChecksums(cohorts, runFingerprint = "fpB")
  expect_false(checksumA == checksumB)

  # stable for the same inputs
  checksumA2 <- CohortDiagnostics:::computeCohortChecksums(cohorts, runFingerprint = "fpA")
  expect_equal(checksumA, checksumA2)
})

test_that("computeRunFingerprint depends on settings", {
  baseContext <- list(
    callingArgsJson = "{}",
    cdmVersion = 5,
    databaseId = "db"
  )
  fp1 <- CohortDiagnostics:::computeRunFingerprint(baseContext)

  changedContext <- baseContext
  changedContext$callingArgsJson <- '{"minCellCount":10}'
  fp2 <- CohortDiagnostics:::computeRunFingerprint(changedContext)
  expect_false(fp1 == fp2)
})

test_that("pruneRecordKeeping drops records for cohorts no longer present", {
  rkf <- tempfile()
  withr::defer(unlink(rkf))

  CohortDiagnostics:::recordTasksDone(
    cohortId = c(1, 2, 3),
    task = "testTask",
    checksum = c("a", "b", "c"),
    recordKeepingFile = rkf
  )

  CohortDiagnostics:::pruneRecordKeeping(rkf, cohortIds = c(1, 3))
  result <- readr::read_csv(rkf, col_types = readr::cols())
  expect_equal(sort(result$cohortId), c(1, 3))
})
