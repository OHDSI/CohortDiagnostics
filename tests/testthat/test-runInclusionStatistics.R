# Copyright 2024 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

for (nm in names(testServers)) {
  
  server <- testServers[[nm]]
  con <- connect(server$connectionDetails)
  
  exportFolder <- file.path(tempdir(), paste0(nm, "exp"))
  databaseId <- "myDB"
  minCellCount <- 5
  recordKeepingFile <- file.path(exportFolder, "record.csv")
  cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = server$cohortTable)
  
  test_that(paste("test run inclusion statistics output", nm), { 
    
    dir.create(exportFolder)
    runInclusionStatistics(connection = con,
                           exportFolder = exportFolder,
                           databaseId = databaseId,
                           cohortDefinitionSet = server$cohortDefinitionSet,
                           cohortDatabaseSchema = server$cohortDatabaseSchema,
                           cohortTableNames = cohortTableNames,
                           incremental = TRUE,
                           minCellCount = minCellCount,
                           recordKeepingFile = recordKeepingFile)
    
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
}
