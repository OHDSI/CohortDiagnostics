# Copyright 2025 Observational Health Data Sciences and Informatics
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

#' Run concept set diagnostics
#'
#' @description
#' Runs the concept set diagnostics (included source concepts, orphan concepts, and/or breakdown of index events).
#'
#' @param context                     A DiagnosticsContext object.
#' @param cohortDefinitionSet         Data.frame of cohorts must include columns cohortId, cohortName, json, sql
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the diagnostics to.
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#'
#' @return Invisible NULL
#' @export
runConceptSetDiagnostic <- function(context,
                                    cohortDefinitionSet = NULL,
                                    cohortIds = NULL,
                                    runIncludedSourceConcepts = TRUE,
                                    runOrphanConcepts = TRUE,
                                    runBreakdownIndexEvents = TRUE) {
  checkmate::assertClass(context, "DiagnosticsContext")
  if (!context$isInitialized) {
    stop("Diagnostics context not initialized. Call initializeDiagnostics() first.")
  }

  if (is.null(cohortDefinitionSet)) {
    cohortDefinitionSet <- context$cohortDefinitionSet
  }
  
  if (!is.null(cohortIds)) {
    cohortDefinitionSet <- cohortDefinitionSet %>% 
      dplyr::filter(.data$cohortId %in% cohortIds)
  }

  if (nrow(cohortDefinitionSet) == 0) {
    ParallelLogger::logWarn("No cohorts to run concept set diagnostics for.")
    return(invisible(NULL))
  }

  recordKeepingFile <- file.path(context$incrementalFolder, "CreatedDiagnostics.csv")

  # Always export concept sets to csv if not already done
  exportConceptSets(
    cohortDefinitionSet = cohortDefinitionSet,
    exportFolder = context$exportFolder,
    minCellCount = context$minCellCount,
    databaseId = context$databaseId
  )

  if (runIncludedSourceConcepts || runOrphanConcepts || runBreakdownIndexEvents) {
    timeExecution(
      context$exportFolder,
      taskName = "runConceptSetDiagnostics",
      cohortIds = cohortDefinitionSet$cohortId,
      parent = "runConceptSetDiagnostic",
      expr = {
        runConceptSetDiagnostics(
          connection = context$connection,
          tempEmulationSchema = context$tempEmulationSchema,
          cdmDatabaseSchema = context$cdmDatabaseSchema,
          vocabularyDatabaseSchema = context$vocabularyDatabaseSchema,
          databaseId = context$databaseId,
          cohorts = cohortDefinitionSet,
          runIncludedSourceConcepts = runIncludedSourceConcepts,
          runOrphanConcepts = runOrphanConcepts,
          runBreakdownIndexEvents = runBreakdownIndexEvents,
          exportFolder = context$exportFolder,
          minCellCount = context$minCellCount,
          conceptCountsDatabaseSchema = NULL,
          conceptCountsTable = "#concept_counts",
          conceptCountsTableIsTemp = TRUE,
          cohortDatabaseSchema = context$cohortDatabaseSchema,
          cohortTable = context$cohortTable,
          useExternalConceptCountsTable = FALSE,
          incremental = context$incremental,
          conceptIdTable = "#concept_ids",
          recordKeepingFile = recordKeepingFile
        )
      }
    )
  }

  return(invisible(NULL))
}
