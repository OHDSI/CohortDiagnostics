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

test_that("subsetToRequiredCombis filters correctly", {
  rkf <- tempfile()
  withr::defer(unlink(rkf))
  
  combis <- dplyr::tibble(
    targetCohortId = c(1, 1),
    comparatorCohortId = c(2, 3),
    targetChecksum = c("t1", "t1"),
    comparatorChecksum = c("c2", "c3"),
    checksum = c("sum12", "sum13")
  )
  
  CohortDiagnostics:::recordTasksDone(
    cohortId = 1,
    comparatorId = 2,
    targetChecksum = "t1",
    comparatorChecksum = "c2",
    task = "testTask",
    checksum = "sum12",
    recordKeepingFile = rkf
  )
  
  result_inc <- CohortDiagnostics:::subsetToRequiredCombis(combis, "testTask", TRUE, rkf)
  expect_equal(nrow(result_inc), 1)
  expect_equal(result_inc$comparatorCohortId[1], 3)
})

test_that("writeToCsv.tbl_Andromeda handles incremental mode", {
  data <- dplyr::tibble(cohortId = 1, value = 10)
  
  tmpFile <- tempfile(fileext = ".csv")
  withr::defer(unlink(tmpFile))
  
  # Mock Andromeda::batchApply to work with data.frame
  local_mocked_bindings(
    batchApply = function(data, fun, ...) {
      fun(data)
    },
    .package = "Andromeda"
  )

  # First write non-incremental
  CohortDiagnostics:::writeToCsv.tbl_Andromeda(data, tmpFile, incremental = FALSE)
  expect_true(file.exists(tmpFile))
  
  # Update and write incremental
  data2 <- dplyr::tibble(cohortId = 2, value = 20)
  CohortDiagnostics:::writeToCsv.tbl_Andromeda(data2, tmpFile, incremental = TRUE)
  
  result <- readr::read_csv(tmpFile, col_types = readr::cols())
  expect_equal(nrow(result), 2)
  expect_true(all(c(1, 2) %in% result$cohort_id))
})
