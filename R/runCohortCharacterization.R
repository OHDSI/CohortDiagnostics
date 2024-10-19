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

# export characteristics to csv files
exportCharacterization <- function(characteristics,
                                   databaseId,
                                   incremental,
                                   covariateValueFileName,
                                   covariateValueContFileName,
                                   covariateRefFileName,
                                   analysisRefFileName,
                                   timeRefFileName,
                                   counts,
                                   minCellCount) {
  
  if (!"covariates" %in% names(characteristics)) {
    warning("No characterization output for submitted cohorts")
  } else if (dplyr::pull(dplyr::count(characteristics$covariateRef)) > 0) {
    characteristics$filteredCovariates <-
      characteristics$covariates %>%
      dplyr::mutate(databaseId = !!databaseId) %>%
      dplyr::left_join(counts,
                       by = c("cohortId", "databaseId"),
                       copy = TRUE
      ) %>%
      dplyr::mutate(
        mean = dplyr::if_else(
          .data$mean != 0 & .data$mean < minCellCount / as.numeric(.data$cohortEntries),
          -minCellCount / as.numeric(.data$cohortEntries),
          .data$mean
        ),
        sumValue = dplyr::if_else(
          .data$sumValue != 0 & .data$sumValue < minCellCount,
          -minCellCount,
          .data$sumValue
        )
      ) %>%
      dplyr::mutate(sd = dplyr::if_else(mean >= 0, .data$sd, 0)) %>%
      dplyr::mutate(
        mean = round(.data$mean, digits = 4),
        sd = round(.data$sd, digits = 4)
      ) %>%
      dplyr::select(-"cohortEntries", -"cohortSubjects") %>%
      dplyr::distinct() %>%
      exportDataToCsv(
        tableName = "temporal_covariate_value",
        fileName = covariateValueFileName,
        minCellCount = minCellCount,
        databaseId = databaseId,
        incremental = TRUE)
    
    if (dplyr::pull(dplyr::count(characteristics$filteredCovariates)) > 0) {
      
      covariateRef <- characteristics$covariateRef
      exportDataToCsv(
        data = characteristics$covariateRef,
        tableName = "temporal_covariate_ref",
        fileName = covariateRefFileName,
        minCellCount = minCellCount,
        incremental = TRUE,
        covariateId = covariateRef %>% dplyr::pull(covariateId)
      )

      analysisRef <- characteristics$analysisRef
      exportDataToCsv(
        data = analysisRef,
        tableName = "temporal_analysis_ref",
        fileName = analysisRefFileName,
        minCellCount = minCellCount,
        incremental = TRUE,
        analysisId = analysisRef %>% dplyr::pull(analysisId)
      )
      
      timeRef <- characteristics$timeRef
      exportDataToCsv(
        data = characteristics$timeRef,
        tableName = "temporal_time_ref",
        fileName = timeRefFileName,
        minCellCount = minCellCount,
        incremental = TRUE,
        analysisId = timeRef %>% dplyr::pull(timeId)
      )
    }
  }
  
  if (!"covariatesContinuous" %in% names(characteristics)) {
    ParallelLogger::logInfo("No continuous characterization output for submitted cohorts")
  } else if (dplyr::pull(dplyr::count(characteristics$covariateRef)) > 0) {
    exportDataToCsv(
      data = characteristics$covariatesContinuous,
      tableName = "temporal_covariate_value_dist",
      fileName = covariateValueContFileName,
      minCellCount = minCellCount,
      databaseId = databaseId,
      incremental = TRUE
    )
  }
}

mutateCovariateOutput <- function(results, featureExtractionOutput, populationSize, binary) {
  if (binary) {
    covariates <- featureExtractionOutput$covariates %>%
      dplyr::rename("cohortId" = "cohortDefinitionId") %>%
      dplyr::left_join(populationSize, by = "cohortId", copy = TRUE) %>%
      dplyr::mutate("p" = .data$sumValue / populationSize)
    
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
      dplyr::mutate("sd" = sqrt(.data$p * (1 - .data$p))) %>%
      dplyr::select(-"p") %>%
      dplyr::rename("mean" = "averageValue") %>%
      dplyr::select(-populationSize)
    
    if (FeatureExtraction::isTemporalCovariateData(featureExtractionOutput)) {
      covariates <- covariates %>%
        dplyr::select(
          "cohortId",
          "timeId",
          "covariateId",
          "sumValue",
          "mean",
          "sd"
        )
      
      tidNaCount <- covariates %>%
        dplyr::filter(is.na(.data$timeId)) %>%
        dplyr::count() %>%
        dplyr::pull()
      
      if (tidNaCount > 0) {
        covariates <- covariates %>%
          dplyr::mutate(timeId = dplyr::if_else(is.na(.data$timeId), -1, .data$timeId))
      }
    } else {
      covariates <- covariates %>%
        dplyr::mutate(timeId = 0) %>%
        dplyr::select(
          "cohortId",
          "timeId",
          "covariateId",
          "sumValue",
          "mean",
          "sd"
        )
    }
    if ("covariates" %in% names(results)) {
      Andromeda::appendToTable(results$covariates, covariates)
    } else {
      results$covariates <- covariates
    }
  } else {
    covariates <- featureExtractionOutput$covariatesContinuous %>%
      dplyr::rename(
        "mean" = "averageValue",
        "sd" = "standardDeviation",
        "cohortId" = "cohortDefinitionId"
      )
    covariatesContinuous <- covariates
    if (FeatureExtraction::isTemporalCovariateData(featureExtractionOutput)) {
      covariates <- covariates %>%
        dplyr::mutate(sumValue = -1) %>%
        dplyr::select(
          "cohortId",
          "timeId",
          "covariateId",
          "sumValue",
          "mean",
          "sd"
        )
      
      tidNaCount <- covariates %>%
        dplyr::filter(is.na(.data$timeId)) %>%
        dplyr::count() %>%
        dplyr::pull()
      
      if (tidNaCount > 0) {
        covariates <- covariates %>%
          dplyr::mutate("timeId" = dplyr::if_else(is.na(.data$timeId), -1, .data$timeId))
      }
    } else {
      covariates <- covariates %>%
        dplyr::mutate(
          sumValue = -1,
          timeId = 0
        ) %>%
        dplyr::select(
          "cohortId",
          "timeId",
          "covariateId",
          "sumValue",
          "mean",
          "sd"
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
  return(results)
}

getCohortCharacteristics <- function(connection = NULL,
                                     cdmDatabaseSchema,
                                     tempEmulationSchema = NULL,
                                     cohortDatabaseSchema = cdmDatabaseSchema,
                                     cohortTable = "cohort",
                                     cohortIds,
                                     cdmVersion = 5,
                                     covariateSettings,
                                     exportFolder,
                                     minCharacterizationMean = 0.001) {
  startTime <- Sys.time()
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
          cohortIds = cohortIds,
          covariateSettings = covariateSettings,
          aggregated = TRUE,
          minCharacterizationMean = minCharacterizationMean
        )
    }
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
      dplyr::select("covariateId")
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
    
    results <- mutateCovariateOutput(results, featureExtractionOutput, populationSize, binary = TRUE)
  }

  if ("covariatesContinuous" %in% names(featureExtractionOutput) &&
    dplyr::pull(dplyr::count(featureExtractionOutput$covariatesContinuous)) > 0) {
    
    results <- mutateCovariateOutput(results, featureExtractionOutput, populationSize, binary = FALSE)
  }

  delta <- Sys.time() - startTime
  ParallelLogger::logInfo(
    "Cohort characterization took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
  return(results)
}

#' runCohortCharacterization
#' 
#' @description
#' This function takes cohorts as input and generates the covariates for these cohorts.
#' The covariates are generated using FeatureExtraction. The output from this package 
#' is slightly modified before the output is written to disk. 
#' These are the files written to disk, if available:
#'  * temporal_analysis_ref.csv
#'  * temporal_covariate_ref.csv
#'  * temporal_covariate_value.csv
#'  * temporal_covariate_value_dist.csv
#'  * temporal_time_ref.csv
#' 
#' @template connection 
#' @template databaseId 
#' @template exportFolder 
#' @template cdmDatabaseSchema 
#' @template cohortDatabaseSchema 
#' @template cohortTable 
#' @template tempEmulationSchema 
#' @template cdmVersion 
#' @template minCellCount 
#' @template instantiatedCohorts 
#' @template Incremental
#' @template batchSize 
#'
#' @param cohorts                    The cohorts for which the covariates need to be obtained
#' @param cohortCounts               A dataframe with the cohort counts
#' @param covariateSettings          Either an object of type \code{covariateSettings} as created using one of
#'                                   the createTemporalCovariateSettings function in the FeatureExtraction package, or a list
#'                                   of such objects.
#' @param minCharacterizationMean    The minimum mean value for characterization output. Values below this will be cut off from output. This
#'                                   will help reduce the file size of the characterization output, but will remove information
#'                                   on covariates that have very low values. The default is 0.001 (i.e. 0.1 percent)
#'
#' @return None, it will write results to disk
#' @export
#'
#' @examples
runCohortCharacterization <- function(connection,
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
                                      incrementalFolder = exportFolder,
                                      minCharacterizationMean = 0.001,
                                      batchSize = getOption("CohortDiagnostics-FE-batch-size", default = 20)) {
  
  errorMessage <- checkmate::makeAssertCollection()
  checkArg(connection, add = errorMessage)
  checkArg(databaseId, add = errorMessage)
  checkArg(exportFolder, add = errorMessage)
  checkArg(cdmDatabaseSchema, add = errorMessage)
  checkArg(cohortDatabaseSchema, add = errorMessage)
  checkArg(cohortTable, add = errorMessage)
  # checkArg(cohortTable, add = errorMessage) not available
  checkArg(tempEmulationSchema, add = errorMessage)
  # checkArg(cohorts, add = errorMessage) not available
  # checkArg(cohortCounts, add = errorMessage) not available
  checkArg(minCellCount, add = errorMessage)
  # checkArg(instantiatedCohorts, add = errorMessage) not available
  checkArg(incremental, add = errorMessage)
  checkArg(incrementalFolder, add = errorMessage)
  checkArg(minCharacterizationMean, add = errorMessage)
  # checkArg(batchSize, add = errorMessage) not available
  checkmate::reportAssertions(errorMessage)
  
  recordKeepingFile <- file.path(incrementalFolder, "CreatedDiagnostics.csv")
  
  # Filename of the binary covariates output
  covariateValueFileName = file.path(exportFolder, "temporal_covariate_value.csv")
  # Filename of the covariate reference output
  covariateRefFileName = file.path(exportFolder, "temporal_covariate_ref.csv")
  # Filename of the continuous covariate output
  covariateValueContFileName = file.path(exportFolder, "temporal_covariate_value_dist.csv")
  # Filename of the analysis reference output
  analysisRefFileName = file.path(exportFolder, "temporal_analysis_ref.csv")
  # Filename of the time reference output
  timeRefFileName = file.path(exportFolder, "temporal_time_ref.csv")
  
  jobName <- "Cohort characterization"
  task <- "runCohortCharacterization"
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
    for (outputFile in c(
      covariateValueFileName, covariateValueContFileName,
      covariateRefFileName, analysisRefFileName, timeRefFileName
    )) {
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

    # Processing cohorts loop
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

      characteristics <- getCohortCharacteristics(
          connection = connection,
          cdmDatabaseSchema = cdmDatabaseSchema,
          tempEmulationSchema = tempEmulationSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          cohortIds = subset[start:end, ]$cohortId,
          covariateSettings = covariateSettings,
          cdmVersion = cdmVersion,
          exportFolder = exportFolder,
          minCharacterizationMean = minCharacterizationMean
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
  ParallelLogger::logInfo(
    "Running ",
    jobName,
    " took",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
}
