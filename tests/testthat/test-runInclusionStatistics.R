# test runInclusionStatistics for all databases
# for (nm in names(testServers)) {
# only run this on sqlite since no SQL is used? Assuming that CohortGenerator::getCohortStats is tested on all dbms.

test_that("test runRnclusionStatistics on sqlite", {
  skip_if_not("sqlite" %in% names(testServers))
  server <- testServers[["sqlite"]]
  con <- connect(server$connectionDetails)
  
  exportFolder <- file.path(tempdir(), paste0(nm, "exp"))
  databaseId <- "myDB"
  minCellCount <- 5
  recordKeepingFile <- file.path(exportFolder, "CreatedDiagnostics.csv")
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = server$cohortTable)

  dir.create(exportFolder)
  runInclusionStatistics(connection = con,
                         exportFolder = exportFolder,
                         databaseId = databaseId,
                         cohortDefinitionSet = server$cohortDefinitionSet,
                         cohortDatabaseSchema = server$cohortDatabaseSchema,
                         cohortTableNames = cohortTableNames,
                         incremental = TRUE,
                         minCellCount = minCellCount)

  # Check cohort_inc_result
  expect_true(file.exists(file.path(exportFolder, "cohort_inc_result.csv")))
  incResult <- read.csv(file.path(exportFolder, "cohort_inc_result.csv"))
  expect_equal(colnames(incResult), c("database_id", "cohort_id", "mode_id", "inclusion_rule_mask" , "person_count"))
  expect_equal(unique(incResult$database_id), databaseId)
  expect_true(all(incResult$cohort_id %in% server$cohortDefinitionSet$cohortId))

  # Check cohort_inc_stats
  expect_true(file.exists(file.path(exportFolder, "cohort_inc_stats.csv")))
  incStatsResult <- read.csv(file.path(exportFolder, "cohort_inc_stats.csv"))
  expect_equal(colnames(incStatsResult), c("cohort_definition_id", "rule_sequence", "person_count", "gain_count" , "person_total", "mode_id"))

  # Check cohort_inclusion
  expect_true(file.exists(file.path(exportFolder, "cohort_inclusion.csv")))
  inclusionResult <- read.csv(file.path(exportFolder, "cohort_inclusion.csv"))
  expect_equal(colnames(inclusionResult), c("database_id", "cohort_id", "rule_sequence", "name" , "description"))
  expect_equal(unique(inclusionResult$database_id), databaseId)
  expect_true(all(inclusionResult$cohort_id %in% server$cohortDefinitionSet$cohortId))

  # Check cohort_summary_stats
  expect_true(file.exists(file.path(exportFolder, "cohort_summary_stats.csv")))
  sumStatsResult <- read.csv(file.path(exportFolder, "cohort_summary_stats.csv"))
  expect_equal(colnames(sumStatsResult), c("database_id", "cohort_id", "mode_id", "base_count" , "final_count"))
  expect_equal(unique(sumStatsResult$database_id), databaseId)
  expect_true(all(sumStatsResult$cohort_id %in% server$cohortDefinitionSet$cohortId))

  # Check recordKeepingFile
  expect_true(file.exists(recordKeepingFile))
  recordKeeping <- read.csv(recordKeepingFile)
  expect_equal(colnames(recordKeeping), c("cohortId", "task", "checksum" , "timeStamp"))
  expect_equal(unique(recordKeeping$task), "runInclusionStatistics")
  expect_true(all(recordKeeping$cohortId %in% server$cohortDefinitionSet$cohortId))

  unlink(exportFolder)
})
