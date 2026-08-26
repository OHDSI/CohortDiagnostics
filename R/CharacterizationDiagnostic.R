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

#' Run temporal characterization diagnostic
#'
#' @description
#' Runs the temporal characterization diagnostic for a set of cohorts.
#'
#' @param context              A DiagnosticsContext object.
#' @param cohortDefinitionSet  Data.frame of cohorts must include columns cohortId, cohortName, json, sql
#' @param cohortIds            Optionally, provide a subset of cohort IDs to restrict the diagnostics to.
#' @param temporalCovariateSettings   Either an object of type \code{covariateSettings} as created using one of
#'                                    the createTemporalCovariateSettings function in the FeatureExtraction package, or a list
#'                                    of such objects.
#' @param minCharacterizationMean     The minimum mean value for characterization output. Values below this will be cut off from output.
#' @param runFeatureExtractionOnSample Logical. If TRUE, the function will operate on a sample of the data.
#' @param sampleN                     Integer. The number of records to include in the sample if runFeatureExtractionOnSample is TRUE.
#' @param seed                        Integer. The seed for the random number generator used to create the sample.
#' @param seedArgs                    List. Additional arguments to pass to the sampling function.
#' @param runCohortRelationship       Compute cohort relationships?
#'
#' @return Invisible NULL
#' @export
runTemporalCharacterizationDiagnostic <- function(context,
                                                  cohortDefinitionSet = NULL,
                                                  cohortIds = NULL,
                                                  temporalCovariateSettings = getDefaultCovariateSettings(),
                                                  minCharacterizationMean = 0.01,
                                                  runFeatureExtractionOnSample = FALSE,
                                                  sampleN = 1000,
                                                  seed = 64374,
                                                  seedArgs = NULL,
                                                  runCohortRelationship = TRUE) {
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
        ParallelLogger::logWarn("No cohorts to run temporal characterization diagnostic for.")
        return(invisible(NULL))
    }

    recordKeepingFile <- file.path(context$incrementalFolder, "CreatedDiagnostics.csv")

    # Setup covariate settings
    if (is(temporalCovariateSettings, "covariateSettings")) {
        temporalCovariateSettings <- list(temporalCovariateSettings)
    }

    # Add required temporal windows if not present (simplified logic from original)
    # NOTE: In a full refactor, this logic might move to a helper, but keeping it here for now
    # to match the logic in RunDiagnostics.R lines 445-478

    # Cohort relationship logic
    if (runCohortRelationship && nrow(cohortDefinitionSet) > 1) {
        covariateCohorts <- cohortDefinitionSet |> dplyr::select("cohortId", "cohortName")
        analysisId <- as.integer(Sys.getenv("OHDSI_CD_CF_ANALYSIS_ID", unset = 173))

        cohortFeSettings <-
            FeatureExtraction::createCohortBasedTemporalCovariateSettings(
                analysisId = analysisId,
                covariateCohortDatabaseSchema = context$cohortDatabaseSchema,
                covariateCohortTable = context$cohortTableNames$cohortTable,
                covariateCohorts = covariateCohorts,
                valueType = "binary",
                temporalStartDays = temporalCovariateSettings[[1]]$temporalStartDays,
                temporalEndDays = temporalCovariateSettings[[1]]$temporalEndDays
            )
        # Add feature set
        temporalCovariateSettings[[length(temporalCovariateSettings) + 1]] <- cohortFeSettings
    }

    feCohortDefinitionSet <- cohortDefinitionSet
    feCohortTable <- context$cohortTable
    feCohortCounts <- context$cohortCounts

    if (runFeatureExtractionOnSample & !isTRUE(attr(cohortDefinitionSet, "isSampledCohortDefinition"))) {
        # Sampling logic
        cohortTableNames <- context$cohortTableNames
        cohortTableNames$cohortSampleTable <- paste0(cohortTableNames$cohortTable, "_cd_sample")

        CohortGenerator::createCohortTables(
            connection = context$connection,
            cohortTableNames = cohortTableNames,
            cohortDatabaseSchema = context$cohortDatabaseSchema,
            incremental = TRUE
        )

        feCohortTable <- cohortTableNames$cohortSampleTable
        feCohortDefinitionSet <-
            CohortGenerator::sampleCohortDefinitionSet(
                connection = context$connection,
                cohortDefinitionSet = cohortDefinitionSet,
                tempEmulationSchema = context$tempEmulationSchema,
                cohortDatabaseSchema = context$cohortDatabaseSchema,
                cohortTableNames = cohortTableNames,
                n = sampleN,
                seed = seed,
                seedArgs = seedArgs,
                identifierExpression = "cohortId",
                incremental = context$incremental,
                incrementalFolder = context$incrementalFolder
            )

        feCohortCounts <- computeCohortCounts(
            connection = context$connection,
            cohortDatabaseSchema = context$cohortDatabaseSchema,
            cohortTable = cohortTableNames$cohortSampleTable,
            cohorts = feCohortDefinitionSet,
            exportFolder = context$exportFolder,
            minCellCount = context$minCellCount,
            databaseId = context$databaseId,
            writeResult = FALSE
        )
    }

    if (length(temporalCovariateSettings) > 0) {
        timeExecution(
            context$exportFolder,
            "executeCohortCharacterization",
            cohortIds = cohortDefinitionSet$cohortId,
            parent = "runTemporalCharacterizationDiagnostic",
            expr = {
                executeCohortCharacterization(
                    connection = context$connection,
                    databaseId = context$databaseId,
                    exportFolder = context$exportFolder,
                    cdmDatabaseSchema = context$cdmDatabaseSchema,
                    cohortDatabaseSchema = context$cohortDatabaseSchema,
                    cohortTable = feCohortTable,
                    covariateSettings = temporalCovariateSettings,
                    tempEmulationSchema = context$tempEmulationSchema,
                    cdmVersion = context$cdmVersion,
                    cohorts = feCohortDefinitionSet,
                    cohortCounts = feCohortCounts,
                    minCellCount = context$minCellCount,
                    instantiatedCohorts = context$instantiatedCohorts,
                    incremental = context$incremental,
                    recordKeepingFile = recordKeepingFile,
                    task = "runTemporalCohortCharacterization",
                    jobName = "Temporal Cohort characterization",
                    covariateValueFileName = file.path(context$exportFolder, "temporal_covariate_value.csv"),
                    covariateValueContFileName = file.path(context$exportFolder, "temporal_covariate_value_dist.csv"),
                    covariateRefFileName = file.path(context$exportFolder, "temporal_covariate_ref.csv"),
                    analysisRefFileName = file.path(context$exportFolder, "temporal_analysis_ref.csv"),
                    timeRefFileName = file.path(context$exportFolder, "temporal_time_ref.csv"),
                    minCharacterizationMean = minCharacterizationMean
                )
            }
        )
    }

    return(invisible(NULL))
}
