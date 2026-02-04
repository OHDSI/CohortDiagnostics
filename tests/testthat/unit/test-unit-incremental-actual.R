library(testthat)
library(dplyr)

test_that("computeChecksum works as expected", {
    checksum1 <- CohortDiagnostics:::computeChecksum("test")
    checksum2 <- CohortDiagnostics:::computeChecksum("test")
    checksum3 <- CohortDiagnostics:::computeChecksum("different")

    expect_equal(checksum1, checksum2)
    expect_false(checksum1 == checksum3)
})

test_that("isTaskRequired works as expected", {
    recordKeepingFile <- tempfile()
    checksum <- "abc"

    # Task required if file doesn't exist
    expect_true(CohortDiagnostics:::isTaskRequired(cohortId = 1, task = "test", checksum = checksum, recordKeepingFile = recordKeepingFile))

    # Record task done
    CohortDiagnostics:::recordTasksDone(cohortId = 1, task = "test", checksum = checksum, recordKeepingFile = recordKeepingFile, incremental = TRUE)

    # Task not required if checksum matches
    expect_false(CohortDiagnostics:::isTaskRequired(cohortId = 1, task = "test", checksum = checksum, recordKeepingFile = recordKeepingFile))

    # Task required if checksum changes
    expect_true(CohortDiagnostics:::isTaskRequired(cohortId = 1, task = "test", checksum = "different", recordKeepingFile = recordKeepingFile))
})

test_that("getRequiredTasks works as expected", {
    recordKeepingFile <- tempfile()
    tasks <- dplyr::tibble(cohortId = c(1, 2), task = "test")
    checksums <- c("abc", "def")

    # All tasks required initially
    required <- CohortDiagnostics:::getRequiredTasks(cohortId = tasks$cohortId, task = tasks$task, checksum = checksums, recordKeepingFile = recordKeepingFile)
    expect_equal(nrow(required), 2)

    # Record one task done
    CohortDiagnostics:::recordTasksDone(cohortId = 1, task = "test", checksum = "abc", recordKeepingFile = recordKeepingFile, incremental = TRUE)

    # Only remaining task required
    required <- CohortDiagnostics:::getRequiredTasks(cohortId = tasks$cohortId, task = tasks$task, checksum = checksums, recordKeepingFile = recordKeepingFile)
    expect_equal(nrow(required), 1)
    expect_equal(required$cohortId, 2)
})

test_that("subsetToRequiredCohorts works as expected", {
    recordKeepingFile <- tempfile()
    cohorts <- dplyr::tibble(cohortId = c(1, 2), checksum = c("abc", "def"))

    # Not incremental: return all
    expect_equal(nrow(CohortDiagnostics:::subsetToRequiredCohorts(cohorts, "test", FALSE, recordKeepingFile)), 2)

    # Incremental: return all initially
    expect_equal(nrow(CohortDiagnostics:::subsetToRequiredCohorts(cohorts, "test", TRUE, recordKeepingFile)), 2)

    # Record one done
    CohortDiagnostics:::recordTasksDone(cohortId = 1, task = "test", checksum = "abc", recordKeepingFile = recordKeepingFile, incremental = TRUE)

    # Incremental: return only remaining
    subset <- CohortDiagnostics:::subsetToRequiredCohorts(cohorts, "test", TRUE, recordKeepingFile)
    expect_equal(nrow(subset), 1)
    expect_equal(subset$cohortId, 2)
})
