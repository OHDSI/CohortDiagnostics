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
                                     exportFolder) {
  startTime <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  results <- Andromeda::andromeda()
  timeExecution(
    exportFolder,
    taskName = "getDbCovariateData",
    parent = "getCohortCharacteristics",
    cohortIds = cohortIds,
    expr = {
      featureExtractionOutput <-
        FeatureExtraction::getDbCovariateData(
          connection = connection,
          oracleTempSchema = tempEmulationSchema,
          cdmDatabaseSchema = cdmDatabaseSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cdmVersion = cdmVersion,
          cohortTable = cohortTable,
          cohortId = cohortIds,
          covariateSettings = covariateSettings,
          aggregated = TRUE
        )
    })
  populationSize <-
    attr(x = featureExtractionOutput, which = "metaData")$populationSize
  populationSize <-
    dplyr::tibble(cohortId = names(populationSize) %>% as.numeric(),
                  populationSize = populationSize)

  if (!"analysisRef" %in% names(results)) {
    results$analysisRef <- featureExtractionOutput$analysisRef
  }
  if (!"covariateRef" %in% names(results)) {
    results$covariateRef <- featureExtractionOutput$covariateRef
  } else {
    covariateIds <- results$covariateRef %>%
      dplyr::select(covariateId)
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
      dplyr::rename(cohortId = cohortDefinitionId) %>%
      dplyr::left_join(populationSize, by = "cohortId", copy = TRUE) %>%
      dplyr::mutate(p = sumValue / populationSize)

    if (nrow(covariates %>%
               dplyr::filter(p > 1) %>%
               dplyr::collect()) > 0) {
      stop(
        paste0(
          "During characterization, population size (denominator) was found to be smaller than features Value (numerator).",
          "- this may have happened because of an error in Feature generation process. Please contact the package developer."
        )
      )
    }

    covariates <- covariates %>%
      dplyr::mutate(sd = sqrt(p * (1 - p))) %>%
      dplyr::select(-p) %>%
      dplyr::rename(mean = averageValue) %>%
      dplyr::select(-populationSize)

      if (FeatureExtraction::isTemporalCovariateData(featureExtractionOutput)) {
        covariates <- covariates %>%
          dplyr::select(
            cohortId,
            timeId,
            covariateId,
            sumValue,
            mean,
            sd
          )
          if (length(is.na(covariates$timeId)) > 0) {
            covariates[is.na(covariates$timeId),]$timeId <- -1
          }
      } else {
        covariates <- covariates %>%
          dplyr::mutate(timeId = 0) %>%
          dplyr::select(
            cohortId,
            timeId,
            covariateId,
            sumValue,
            mean,
            sd
          )
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
        mean = averageValue,
        sd = standardDeviation,
        cohortId = cohortDefinitionId
      )
    covariatesContinuous <- covariates
    if (FeatureExtraction::isTemporalCovariateData(featureExtractionOutput)) {
      covariates <- covariates %>%
        dplyr::mutate(sumValue = -1) %>%
        dplyr::select(
          cohortId,
          timeId,
          covariateId,
          sumValue,
          mean,
          sd
        )
      if (length(is.na(covariates$timeId)) > 0) {
        covariates[is.na(covariates$timeId),]$timeId <- -1
      }
    } else {
      covariates <- covariates %>%
        dplyr::mutate(sumValue = -1,
                      timeId = 0) %>%
        dplyr::select(
          cohortId,
          timeId,
          covariateId,
          sumValue,
          mean,
          sd
        )
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
                                          task = "runTemporalCohortCharacterization",
                                          jobName = "Temporal Cohort characterization",
                                          covariateValueFileName = file.path(exportFolder, "temporal_covariate_value.csv"),
                                          covariateValueContFileName = file.path(exportFolder, "temporal_covariate_value_dist.csv"),
                                          covariateRefFileName = file.path(exportFolder, "temporal_covariate_ref.csv"),
                                          analysisRefFileName = file.path(exportFolder, "temporal_analysis_ref.csv"),
                                          timeRefFileName = file.path(exportFolder, "temporal_time_ref.csv"),
                                          minCharacterizationMean = 0.001,
                                          batchSize = getOption("CohortDiagnostics-FE-batch-size", default = 5)) {
  ParallelLogger::logInfo("Running ", jobName)
  startCohortCharacterization <- Sys.time()
  subset <- subsetToRequiredCohorts(
    cohorts = cohorts %>%
      dplyr::filter(.data$cohortId %in% instantiatedCohorts),
    task = task,
    incremental = incremental,
    recordKeepingFile = recordKeepingFile
  )

  if (!incremental) {
    for (outputFile in c(covariateValueFileName, covariateValueContFileName,
                         covariateRefFileName, analysisRefFileName, timeRefFileName)) {

      if (file.exists(outputFile)) {
        ParallelLogger::logInfo("Not in incremental mode - Removing file", outputFile, " and replacing")
        unlink(outputFile)
      }
    }
  }

  if (incremental &&
    (length(instantiatedCohorts) - nrow(subset)) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Skipping %s instantiated cohorts in incremental mode.",
      length(instantiatedCohorts) - nrow(subset)
    ))
  }
  if (nrow(subset) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Starting large scale characterization of %s cohort(s)",
      nrow(subset)
    ))

    for (start in seq(1, nrow(subset), by = batchSize)) {
      end <- min(start + batchSize - 1, nrow(subset))
      if (nrow(subset) > batchSize) {
        ParallelLogger::logInfo(
          sprintf(
            "Batch characterization. Processing rows %s through %s of total %s.",
            start,
            end,
            nrow(subset)
          )
        )
      }

      characteristics <-
        getCohortCharacteristics(
          connection = connection,
          cdmDatabaseSchema = cdmDatabaseSchema,
          tempEmulationSchema = tempEmulationSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          cohortIds = subset[start:end, ]$cohortId,
          covariateSettings = covariateSettings,
          cdmVersion = cdmVersion,
          exportFolder = exportFolder
        )

      on.exit(Andromeda::close(characteristics), add = TRUE)
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
        minCharacterizationMean = minCharacterizationMean,
        minCellCount = minCellCount
      )

      recordTasksDone(
        cohortId = subset[start:end, ]$cohortId,
        task = task,
        checksum = subset[start:end, ]$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )

      deltaIteration <- Sys.time() - startCohortCharacterization
      ParallelLogger::logInfo(
        "    - Running Cohort Characterization iteration with batchsize ",
        batchSize,
        " from row number ",
        start,
        " to ",
        end,
        " took ",
        signif(deltaIteration, 3),
        " ",
        attr(deltaIteration, "units")
      )
    }
  }
  delta <- Sys.time() - startCohortCharacterization
  ParallelLogger::logInfo("Running ",
                          jobName,
                          " took",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
}
