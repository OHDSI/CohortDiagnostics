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
  
  subset <- subsetToRequiredCohorts(
    cohorts = cohortDefinitionSet,
    task = "runInclusionStatistics",
    incremental = incremental,
    recordKeepingFile = recordKeepingFile
  )
  if (incremental) {
    numConceptsToSkip <- length(cohortDefinitionSet$cohortId) - nrow(subset)
    if (numConceptsToSkip > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", numConceptsToSkip))
    }
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
      cohortInclusionList <- list("cohortInclusionTable" = "cohort_inclusion",
                                  "cohortInclusionStatsTable" = "cohort_inc_stats",
                                  "cohortInclusionResultTable" = "cohort_inc_result",
                                  "cohortSummaryStatsTable" = "cohort_summary_stats")
      
      lapply(names(cohortInclusionList), FUN = function(cohortInclusionName) {
        if (cohortInclusionName %in% (names(stats))) {
          cohortTableName <- cohortInclusionList[[cohortInclusionName]]
          data <- makeDataExportable(
            x = stats[[cohortInclusionName]],
            tableName = cohortTableName,
            databaseId = databaseId,
            minCellCount = minCellCount
          )
          writeToCsv(
            data = data,
            fileName = file.path(exportFolder, paste0(cohortTableName, ".csv")),
            incremental = incremental,
            cohortId = subset$cohortId
          )
        }
      })
      
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
