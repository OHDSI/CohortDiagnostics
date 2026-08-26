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

#' Run time series diagnostic
#'
#' @description
#' Runs the time series diagnostic for a set of cohorts.
#'
#' @param context              A DiagnosticsContext object.
#' @param cohortDefinitionSet  Data.frame of cohorts must include columns cohortId, cohortName, json, sql
#' @param cohortIds            Optionally, provide a subset of cohort IDs to restrict the diagnostics to.
#'
#' @return Invisible NULL
#' @export
runTimeSeriesDiagnostic <- function(context,
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
        ParallelLogger::logWarn("No cohorts to run time series diagnostic for.")
        return(invisible(NULL))
    }

    recordKeepingFile <- file.path(context$incrementalFolder, "CreatedDiagnostics.csv")

    timeExecution(
        context$exportFolder,
        "executeTimeSeriesDiagnostics",
        cohortIds = cohortDefinitionSet$cohortId,
        parent = "runTimeSeriesDiagnostic",
        expr = {
            executeTimeSeriesDiagnostics(
                connection = context$connection,
                tempEmulationSchema = context$tempEmulationSchema,
                cdmDatabaseSchema = context$cdmDatabaseSchema,
                cohortDatabaseSchema = context$cohortDatabaseSchema,
                cohortTable = context$cohortTable,
                cohortDefinitionSet = cohortDefinitionSet,
                databaseId = context$databaseId,
                exportFolder = context$exportFolder,
                minCellCount = context$minCellCount,
                instantiatedCohorts = context$instantiatedCohorts,
                incremental = context$incremental,
                recordKeepingFile = recordKeepingFile,
                observationPeriodDateRange = context$observationPeriodDateRange
            )
        }
    )

    return(invisible(NULL))
}
