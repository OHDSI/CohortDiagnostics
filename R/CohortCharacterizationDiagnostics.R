# Copyright 2022 Observational Health Data Sciences and Informatics
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

getCohortCharacteristics <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     tempEmulationSchema = NULL,
                                     cohortDatabaseSchema = cdmDatabaseSchema,
                                     cohortTable = "cohort",
                                     cohortIds,
                                     cdmVersion = 5,
                                     covariateSettings,
                                     batchSize = 100) {
  startTime <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  results <- Andromeda::andromeda()
  for (start in seq(1, length(cohortIds), by = batchSize)) {
    end <- min(start + batchSize - 1, length(cohortIds))
    if (length(cohortIds) > batchSize) {
      ParallelLogger::logInfo(sprintf(
        "Batch characterization. Processing cohorts %s through %s",
        start,
        end
      ))
    }
    featureExtractionOutput <-
      FeatureExtraction::getDbCovariateData(
        connection = connection,
        oracleTempSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cdmVersion = cdmVersion,
        cohortTable = cohortTable,
        cohortId = cohortIds[start:end],
        covariateSettings = covariateSettings,
        aggregated = TRUE
      )

    populationSize <-
      attr(x = featureExtractionOutput, which = "metaData")$populationSize
    populationSize <-
      dplyr::tibble(
        cohortId = names(populationSize) %>% as.numeric(),
        populationSize = populationSize
      )

    if (!"analysisRef" %in% names(results)) {
      results$analysisRef <- featureExtractionOutput$analysisRef
    }
    if (!"covariateRef" %in% names(results)) {
      results$covariateRef <- featureExtractionOutput$covariateRef
    } else {
      covariateIds <- results$covariateRef %>%
        dplyr::select(.data$covariateId)
      Andromeda::appendToTable(
        results$covariateRef,
        featureExtractionOutput$covariateRef %>%
          dplyr::anti_join(covariateIds, by = "covariateId", copy = TRUE)
      )
    }
    if ("timeRef" %in% names(featureExtractionOutput) &&
        !"timeRef" %in% names(results)) {
      results$timeRef <- featureExtractionOutput$timeRef
    }

    if ("covariates" %in% names(featureExtractionOutput) &&
        dplyr::pull(dplyr::count(featureExtractionOutput$covariates)) > 0) {
      covariates <- featureExtractionOutput$covariates %>%
        dplyr::rename(cohortId = .data$cohortDefinitionId) %>%
        dplyr::left_join(populationSize, by = "cohortId", copy = TRUE) %>%
        dplyr::mutate(p = .data$sumValue / .data$populationSize)

      if (nrow(covariates %>%
               dplyr::filter(.data$p > 1) %>%
               dplyr::collect()) > 0) {
        stop(
          paste0(
            "During characterization, population size (denominator) was found to be smaller than features Value (numerator).",
            "- this may have happened because of an error in Feature generation process. Please contact the package developer."
          )
        )
      }

      covariates <- covariates %>%
        dplyr::mutate(sd = sqrt(.data$p * (1 - .data$p))) %>%
        dplyr::select(-.data$p) %>%
        dplyr::rename(mean = .data$averageValue) %>%
        dplyr::select(-.data$populationSize)

      if (FeatureExtraction::isTemporalCovariateData(featureExtractionOutput)) {
        covariates <- covariates %>%
          dplyr::select(.data$cohortId,
                        .data$timeId,
                        .data$covariateId,
                        .data$sumValue,
                        .data$mean,
                        .data$sd)
      } else {
        covariates <- covariates %>%
          dplyr::select(.data$cohortId,
                        .data$covariateId,
                        .data$sumValue,
                        .data$mean,
                        .data$sd)
      }
      if ("covariates" %in% names(results)) {
        Andromeda::appendToTable(results$covariates, covariates)
      } else {
        results$covariates <- covariates
      }
    }

    if ("covariatesContinuous" %in% names(featureExtractionOutput) &&
        dplyr::pull(dplyr::count(featureExtractionOutput$covariatesContinuous)) > 0) {
      covariates <- featureExtractionOutput$covariatesContinuous %>%
        dplyr::rename(
          mean = .data$averageValue,
          sd = .data$standardDeviation,
          cohortId = .data$cohortDefinitionId
        )
      covariatesContinuous <- covariates
      if (FeatureExtraction::isTemporalCovariateData(featureExtractionOutput)) {
        covariates <- covariates %>%
          dplyr::mutate(sumValue = -1) %>%
          dplyr::select(.data$cohortId,
                        .data$timeId,
                        .data$covariateId,
                        .data$sum,
                        .data$mean,
                        .data$sd)
      } else {
        covariates <- covariates %>%
          dplyr::mutate(sumValue = -1) %>%
          dplyr::select(.data$cohortId,
                        .data$covariateId,
                        .data$sumValue,
                        .data$mean,
                        .data$sd)
      }
      if ("covariates" %in% names(results)) {
        Andromeda::appendToTable(results$covariates, covariates)
      } else {
        results$covariates <- covariates
      }
      if ("covariatesContinuous" %in% names(results)) {
        Andromeda::appendToTable(results$covariatesContinuous, covariatesContinuous)
      } else {
        results$covariatesContinuous <- covariatesContinuous
      }
    }
  }

  delta <- Sys.time() - startTime
  ParallelLogger::logInfo("Cohort characterization took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
  return(results)
}

executeCohortCharacterization <- function(connection,
                                          databaseId,
                                          exportFolder,
                                          cdmDatabaseSchema,
                                          cohortDatabaseSchema,
                                          cohortTable,
                                          covariateSettings,
                                          tempEmulationSchema,
                                          cdmVersion,
                                          cohorts,
                                          cohortCounts,
                                          minCellCount,
                                          instantiatedCohorts,
                                          incremental,
                                          recordKeepingFile,
                                          task = "runCohortCharacterization",
                                          jobName = "Cohort Charachterization",
                                          covariateValueFileName = file.path(exportFolder, "covariate_value.csv"),
                                          covariateValueContFileName = file.path(exportFolder, "covariate_value_dist.csv"),
                                          covariateRefFileName = file.path(exportFolder, "covariate_ref.csv"),
                                          analysisRefFileName = file.path(exportFolder, "analysis_ref.csv"),
                                          timeRefFileName = NULL) {
  ParallelLogger::logInfo("Running ", jobName)
  startCohortCharacterization <- Sys.time()
  subset <- subsetToRequiredCohorts(
    cohorts = cohorts %>%
      dplyr::filter(.data$cohortId %in% instantiatedCohorts),
    task = task,
    incremental = incremental,
    recordKeepingFile = recordKeepingFile
  )

  if (incremental &&
    (length(instantiatedCohorts) - nrow(subset)) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Skipping %s cohorts in incremental mode.",
      length(instantiatedCohorts) - nrow(subset)
    ))
  }
  if (nrow(subset) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Starting large scale characterization of %s cohort(s)",
      nrow(subset)
    ))
    characteristics <-
      getCohortCharacteristics(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cohortIds = subset$cohortId,
        covariateSettings = covariateSettings,
        cdmVersion = cdmVersion
      )
    exportCharacterization(
      characteristics = characteristics,
      databaseId = databaseId,
      incremental = incremental,
      covariateValueFileName = covariateValueFileName,
      covariateValueContFileName = covariateValueContFileName,
      covariateRefFileName = covariateRefFileName,
      analysisRefFileName = analysisRefFileName,
      timeRefFileName = timeRefFileName,
      counts = cohortCounts,
      minCellCount = minCellCount
    )
  }
  recordTasksDone(
    cohortId = subset$cohortId,
    task = task,
    checksum = subset$checksum,
    recordKeepingFile = recordKeepingFile,
    incremental = incremental
  )
  delta <- Sys.time() - startCohortCharacterization
  ParallelLogger::logInfo("Running ", jobName, " took",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
}
