library(testthat)

test_that("Record keeping of single type tasks", {
  
  rkf <- tempfile()  
  
  sql1 <- "SELECT * FROM my_table WHERE x = 1;"
  checksum1 <- CohortDiagnostics:::computeChecksum(sql1)
  expect_true(CohortDiagnostics:::isTaskRequired(cohortId = 1, 
                                                 runSql = TRUE, 
                                                 checksum = checksum1, 
                                                 recordKeepingFile = rkf))
  
  CohortDiagnostics:::recordTasksDone(cohortId = 1, 
                                      runSql = TRUE, 
                                      checksum = checksum1,
                                      recordKeepingFile = rkf)
  
  
  expect_false(CohortDiagnostics:::isTaskRequired(cohortId = 1, 
                                                  runSql = TRUE, 
                                                  checksum = checksum1, 
                                                  recordKeepingFile = rkf))
  
  sql2 <- "SELECT * FROM my_table WHERE x = 2;"
  checksum2 <- CohortDiagnostics:::computeChecksum(sql2)
  expect_true(CohortDiagnostics:::isTaskRequired(cohortId = 2, 
                                                 runSql = TRUE, 
                                                 checksum = checksum2, 
                                                 recordKeepingFile = rkf))
  
  CohortDiagnostics:::recordTasksDone(cohortId = 2, 
                                      runSql = TRUE, 
                                      checksum = checksum2,
                                      recordKeepingFile = rkf)
  
  sql1a <- "SELECT * FROM my_table WHERE x = 1 AND y = 2;"
  checksum1a <- CohortDiagnostics:::computeChecksum(sql1a)
  expect_true(CohortDiagnostics:::isTaskRequired(cohortId = 1, 
                                                 runSql = TRUE, 
                                                 checksum = checksum1a, 
                                                 recordKeepingFile = rkf))
  
  CohortDiagnostics:::recordTasksDone(cohortId = 1, 
                                      runSql = TRUE, 
                                      checksum = checksum1a,
                                      recordKeepingFile = rkf)
  
  expect_false(CohortDiagnostics:::isTaskRequired(cohortId = 1, 
                                                  runSql = TRUE, 
                                                  checksum = checksum1a, 
                                                  recordKeepingFile = rkf))
  
  unlink(rkf)
})

test_that("Record keeping of multiple type tasks", {
  
  rkf <- tempfile()  
  
  sql1 <- "SELECT * FROM my_table WHERE x = 1;"
  checksum1 <- CohortDiagnostics:::computeChecksum(sql1)
  expect_true(CohortDiagnostics:::isTaskRequired(cohortId = 1, 
                                                 task = "Run SQL",
                                                 checksum = checksum1, 
                                                 recordKeepingFile = rkf))
  
  CohortDiagnostics:::recordTasksDone(cohortId = 1,
                                      task = "Run SQL",
                                      checksum = checksum1,
                                      recordKeepingFile = rkf)
  
  
  expect_false(CohortDiagnostics:::isTaskRequired(cohortId = 1, 
                                                  task = "Run SQL",
                                                  checksum = checksum1, 
                                                  recordKeepingFile = rkf))
  
  sql2 <- "SELECT * FROM my_table WHERE x = 1 AND y = 1;"
  checksum2 <- CohortDiagnostics:::computeChecksum(sql2)
  expect_true(CohortDiagnostics:::isTaskRequired(cohortId = 1,
                                                 cohortId2 = 2,
                                                 task = "Compare cohorts", 
                                                 checksum = checksum2, 
                                                 recordKeepingFile = rkf))
  
  CohortDiagnostics:::recordTasksDone(cohortId = 1,
                                      cohortId2 = 2,
                                      task = "Compare cohorts",
                                      checksum = checksum2,
                                      recordKeepingFile = rkf)
  
  expect_false(CohortDiagnostics:::isTaskRequired(cohortId = 1, 
                                                  task = "Run SQL",
                                                  checksum = checksum1, 
                                                  recordKeepingFile = rkf))
  
  
  sql2a <- "SELECT * FROM my_table WHERE x = 1 AND y = 2 AND z = 3;"
  checksum2a <- CohortDiagnostics:::computeChecksum(sql2a)
  expect_true(CohortDiagnostics:::isTaskRequired(cohortId = 1,
                                                 cohortId2 = 2,
                                                 task = "Compare cohorts",
                                                 checksum = checksum2a, 
                                                 recordKeepingFile = rkf))
  
  CohortDiagnostics:::recordTasksDone(cohortId = 1,
                                      cohortId2 = 2,
                                      task = "Compare cohorts",
                                      checksum = checksum2a,
                                      recordKeepingFile = rkf)
  
  expect_false(CohortDiagnostics:::isTaskRequired(cohortId = 1,
                                                  cohortId2 = 2,
                                                  task = "Compare cohorts",
                                                  checksum = checksum2a, 
                                                  recordKeepingFile = rkf))
  
  unlink(rkf)
})

test_that("Record keeping of multiple tasks at once", {
  
  rkf <- tempfile()  
  
  task <- dplyr::tibble(cohortId = c(1, 2),
                         sql = c("SELECT * FROM my_table WHERE x = 1;", "SELECT * FROM my_table WHERE x = 2;"))
  task$checksum <- CohortDiagnostics:::computeChecksum(task$sql)
  expect_true(CohortDiagnostics:::isTaskRequired(cohortId = task$cohortId[1], 
                                                 checksum = task$checksum[1], 
                                                 recordKeepingFile = rkf))
  
  CohortDiagnostics:::recordTasksDone(cohortId = task$cohortId, 
                                      checksum = task$checksum,
                                      recordKeepingFile = rkf)
  
  
  expect_false(CohortDiagnostics:::isTaskRequired(cohortId = task$cohortId[1], 
                                                  checksum = task$checksum[1], 
                                                  recordKeepingFile = rkf))
  
  expect_false(CohortDiagnostics:::isTaskRequired(cohortId = task$cohortId[2], 
                                                  checksum = task$checksum[2], 
                                                  recordKeepingFile = rkf))
  
  
  task <- dplyr::tibble(cohortId = c(1, 2, 3),
                         sql = c("SELECT * FROM my_table WHERE x = 3;", "SELECT * FROM my_table WHERE x = 4;", "SELECT * FROM my_table WHERE x = 5;"))
  task$checksum <- CohortDiagnostics:::computeChecksum(task$sql)
  
  expect_true(CohortDiagnostics:::isTaskRequired(cohortId = task$cohortId[1], 
                                                 checksum = task$checksum[1], 
                                                 recordKeepingFile = rkf))
  
  tasks <- CohortDiagnostics:::getRequiredTasks(cohortId = task$cohortId, 
                                               checksum = task$checksum, 
                                               recordKeepingFile = rkf)
  expect_equal(nrow(tasks), 3)
  
  CohortDiagnostics:::recordTasksDone(cohortId = task$cohortId, 
                                      checksum = task$checksum,
                                      recordKeepingFile = rkf)
  
  expect_false(CohortDiagnostics:::isTaskRequired(cohortId = task$cohortId[1], 
                                                  checksum = task$checksum[1], 
                                                  recordKeepingFile = rkf))
  
  expect_false(CohortDiagnostics:::isTaskRequired(cohortId = task$cohortId[2], 
                                                  checksum = task$checksum[2], 
                                                  recordKeepingFile = rkf))
  
  expect_false(CohortDiagnostics:::isTaskRequired(cohortId = task$cohortId[3], 
                                                  checksum = task$checksum[3], 
                                                  recordKeepingFile = rkf))
  
  tasks <- CohortDiagnostics:::getRequiredTasks(cohortId = task$cohortId[2], 
                                                checksum = task$checksum[2], 
                                                recordKeepingFile = rkf)
  expect_equal(nrow(tasks), 0)
  
  unlink(rkf)
})


test_that("Incremental save", {
  tmpFile <- tempfile()
  tmpFile <- "D:\\git\\github\\test.csv"
  data <- dplyr::tibble(cohortId = c(1, 1, 2, 2, 3), 
                         count = c(100, 200, 300, 400, 500)) 
  CohortDiagnostics:::saveIncremental(data, tmpFile, cohortId = c(1, 2, 3))
  
  newData <- dplyr::tibble(cohortId = c(1, 2, 2), 
                            count = c(600, 700, 800)) 
  
  CohortDiagnostics:::saveIncremental(newData, tmpFile, cohortId = c(1, 2))
  
  
  
  goldStandard <- dplyr::tibble(cohortId = c(3, 1, 2, 2), 
                                 count = c(500, 600, 700, 800)) 
  
  
  expect_equivalent(readr::read_csv(tmpFile, 
                                    col_types = readr::cols(),
                                    guess_max = min(1e7)),
                    goldStandard)
  unlink(tmpFile)
})

test_that("Incremental save with empty key", {
  tmpFile <- tempfile()
  data <- dplyr::tibble(cohortId = c(1, 1, 2, 2, 3), 
                         count = c(100, 200, 300, 400, 500)) 
  CohortDiagnostics:::saveIncremental(data, tmpFile, cohortId = c(1, 2, 3))
  
  newData <- dplyr::tibble() 
  
  CohortDiagnostics:::saveIncremental(newData, tmpFile, cohortId = c())
  
  expect_equivalent(readr::read_csv(tmpFile, 
                                    col_types = readr::cols(),
                                    guess_max = min(1e7)),
                    data)
  unlink(tmpFile)
})
