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

#' Run inclusion statistics diagnostic
#'
#' @description
#' Runs the inclusion statistics diagnostic for a set of cohorts.
#'
#' @param context              A DiagnosticsContext object.
#' @param cohortDefinitionSet  Data.frame of cohorts must include columns cohortId, cohortName, json, sql
#' @param cohortIds            Optionally, provide a subset of cohort IDs to restrict the diagnostics to.
#'
#' @return Invisible NULL
#' @export
runInclusionStatisticsDiagnostic <- function(context,
                                              cohortDefinitionSet = NULL,
                                              cohortIds = NULL) {
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
    ParallelLogger::logWarn("No cohorts to run inclusion statistics for.")
    return(invisible(NULL))
  }

  recordKeepingFile <- file.path(context$incrementalFolder, "CreatedDiagnostics.csv")

  timeExecution(
    context$exportFolder,
    "getInclusionStats",
    cohortIds = cohortDefinitionSet$cohortId,
    parent = "runInclusionStatisticsDiagnostic",
    expr = {
      getInclusionStats(
        connection = context$connection,
        exportFolder = context$exportFolder,
        databaseId = context$databaseId,
        cohortDefinitionSet = cohortDefinitionSet,
        cohortDatabaseSchema = context$cohortDatabaseSchema,
        cohortTableNames = context$cohortTableNames,
        incremental = context$incremental,
        instantiatedCohorts = context$instantiatedCohorts,
        minCellCount = context$minCellCount,
        recordKeepingFile = recordKeepingFile
      )
    }
  )

  return(invisible(NULL))
}
