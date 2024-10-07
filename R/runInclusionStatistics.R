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

#' Title
#' 
#' @template Connection
#' @template CohortDatabaseSchema
#' 
#' @param exportFolder 
#' @param databaseId 
#' @param cohortDefinitionSet
#' @param cohortTableNames 
#' @param incremental 
#' @param minCellCount 
#' @param recordKeepingFile 
#'
#' @return
#' @export
runInclusionStatistics <- function(connection,
                                   exportFolder,
                                   databaseId,
                                   cohortDefinitionSet,
                                   cohortDatabaseSchema,
                                   cohortTableNames,
                                   incremental,
                                   minCellCount,
                                   recordKeepingFile) {
  
  ParallelLogger::logInfo("Fetching inclusion statistics from files")
  
  if (incremental) {
    subset <- subsetToRequiredCohorts(
      cohorts = cohortDefinitionSet,
      task = "runInclusionStatistics",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    
    numConceptsToSkip <- length(cohortDefinitionSet$cohortId) - nrow(subset)
    if (numConceptsToSkip > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", numConceptsToSkip))
    }
  } else {
    subset <- cohortDefinitionSet
  }
  
  if (nrow(subset) > 0) {
    ParallelLogger::logInfo("Exporting inclusion rules with CohortGenerator")

    CohortGenerator::insertInclusionRuleNames(
      connection = connection,
      cohortDefinitionSet = subset,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortInclusionTable = cohortTableNames$cohortInclusionTable
    )

    stats <- CohortGenerator::getCohortStats(
      connection = connection,
      cohortTableNames = cohortTableNames,
      cohortDatabaseSchema = cohortDatabaseSchema
    )
    
    if (!is.null(stats)) {
      if ("cohortInclusionTable" %in% (names(stats))) {
        cohortInclusion <- makeDataExportable(
          x = stats$cohortInclusionTable,
          tableName = "cohort_inclusion",
          databaseId = databaseId,
          minCellCount = minCellCount
        )
        writeToCsv(
          data = cohortInclusion,
          fileName = file.path(exportFolder, "cohort_inclusion.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      }
      
      if ("cohortInclusionStatsTable" %in% (names(stats))) {
        cohortIncStats <- makeDataExportable(
          x = stats$cohortInclusionStatsTable,
          tableName = "cohort_inc_stats",
          databaseId = databaseId,
          minCellCount = minCellCount
        )
        writeToCsv(
          data = cohortIncStats,
          fileName = file.path(exportFolder, "cohort_inc_stats.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      }
      
      if ("cohortInclusionResultTable" %in% (names(stats))) {
        cohortIncResult <- makeDataExportable(
          x = stats$cohortInclusionResultTable,
          tableName = "cohort_inc_result",
          databaseId = databaseId,
          minCellCount = minCellCount
        )
        writeToCsv(
          data = cohortIncResult,
          fileName = file.path(exportFolder, "cohort_inc_result.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      }
      
      if ("cohortSummaryStatsTable" %in% (names(stats))) {
        cohortSummaryStats <- makeDataExportable(
          x = stats$cohortSummaryStatsTable,
          tableName = "cohort_summary_stats",
          databaseId = databaseId,
          minCellCount = minCellCount
        )
        writeToCsv(
          data = cohortSummaryStats,
          fileName = file.path(exportFolder, "cohort_summary_stats.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      }

      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runInclusionStatistics",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    }
  }
}
