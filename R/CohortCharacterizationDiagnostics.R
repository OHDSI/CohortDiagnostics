# Copyright 2021 Observational Health Data Sciences and Informatics
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


#' Get Feature Extraction output for set of cohorts
#'
#' @description
#' Given a set of instantiated cohorts get Characteristics for the cohort using \code{FeatureExtraction::getDbCovariateData}.
#'
#' If runTemporalCohortCharacterization argument is TRUE, then the following default covariateSettings object will be created
#' using \code{RFeatureExtraction::createTemporalCovariateSettings}.
#'
#' Because of the large file size, the returned object is an \code{Andromeda::andromeda} class object. Use
#' \code{CohortDiagnostics::exportFeatureExtractionOutput} to export the characterization results to csv.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template CohortDatabaseSchema
#'
#' @template TempEmulationSchema
#'
#' @template CohortTable
#'
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    diagnostics to.
#'
#' @template cdmVersion
#'
#' @param cutOff                                 Minimum value of the covariate value, below which data is censored.
#'
#' @param covariateSettings           Either an object of type \code{covariateSettings} as created using one of
#'                                    the createCovariateSettings (createTemporalCovariateSettings if temporal
#'                                    characterization) function in the FeatureExtraction package, or a list
#'                                    of such objects. If unspecified, default covariate settings as specified
#'                                    by FeatureExtraction is computed, this is sufficient for presenting default
#'                                    table 1. See documentation of FeatureExtraction on how to specify
#'                                    CovariateSettings object.
#'
#' @param batchSize                   {Optional, default set to 100} If running characterization on larget set
#'                                    of cohorts, this function allows you to batch them into chunks that run
#'                                    as a batch.
#'
#' @export
runCohortCharacterizationDiagnostics <-
  function(connectionDetails = NULL,
           connection = NULL,
           cdmDatabaseSchema,
           tempEmulationSchema = NULL,
           cohortDatabaseSchema = cdmDatabaseSchema,
           cohortTable = "cohort",
           cohortIds = NULL,
           cdmVersion = 5,
           cutOff = 0.0001,
           covariateSettings,
           batchSize = 50) {
    if (any(!"temporal" %in% names(covariateSettings),
            covariateSettings$temporal != TRUE)) {
      warning(
        paste0(
          "    covariateSettings specification should be of temporalCovariateSetting object \n",
          "    as described in FeatureExtraction - exiting runCohortCharacterization."
        )
      )
      return(NULL)
    }
    startTime <- Sys.time()
    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
    cohortCounts <- getCohortCounts(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = cohortIds
    )
    cohortIdsNew <- cohortCounts$cohortCount %>%
      dplyr::pull(.data$cohortId) %>%
      unique()
    
    if (is.null(cohortCounts$cohortCount)) {
      warning(" --- No instantiated cohorts found. Exiting characterization.")
      return(NULL)
    } else if (any(is.null(cohortIds), length(cohortIds) == 0)) {
      ParallelLogger::logInfo("   - No cohortIds provided. Exiting characterization.")
      return(NULL)
    } else if (any(is.null(cohortIdsNew), length(cohortIdsNew) == 0)) {
      ParallelLogger::logInfo(
        "   - All cohorts are either not instantiated or have no records. Exiting Characterization."
      )
      return(NULL)
    } else if (length(cohortIds) > length(cohortIdsNew)) {
      ParallelLogger::logInfo(
        paste0(
          "   - Of the ",
          scales::comma(length(cohortIds), accuracy = 1),
          " provided, found ",
          scales::comma(length(cohortIdsNew), accuracy = 1),
          " to be instantiated. Starting Characterization."
        )
      )
    } else if (length(cohortIds) == length(cohortIdsNew)) {
      ParallelLogger::logInfo(paste0("   - Starting Characterization."))
    }
    
    results <- Andromeda::andromeda()
    batchSize <-
      max(1, round(batchSize / length(covariateSettings$temporalStartDays)))
    ParallelLogger::logInfo(
      "    - Starting batch charactetrization, ",
      scales::comma(batchSize),
      " cohorts at a time."
    )
    
    for (start in seq(1, length(cohortIdsNew), by = batchSize)) {
      end <- min(start + batchSize - 1, length(cohortIdsNew))
      if (length(cohortIdsNew) > batchSize) {
        ParallelLogger::logInfo(
          paste0(
            "     - Processing ",
            scales::comma(start),
            " through ",
            scales::comma(end),
            " of total ",
            scales::comma(length(cohortIdsNew)),
            " . Cohorts (",
            paste0(cohortIdsNew[start:end], collapse = ", "),
            ")"
          )
        )
      }
      featureExtractionOutput <-
        FeatureExtraction::getDbCovariateData(
          connection = connection,
          oracleTempSchema = tempEmulationSchema,
          cdmDatabaseSchema = cdmDatabaseSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cdmVersion = cdmVersion,
          cohortTable = cohortTable,
          cohortId = cohortIdsNew[start:end],
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
      
      if ("covariates" %in% names(featureExtractionOutput) &&
          dplyr::pull(dplyr::count(featureExtractionOutput$covariates)) > 0) {
        ParallelLogger::logTrace("      - appending covariates")
        covariates <- featureExtractionOutput$covariates  %>%
          dplyr::filter(.data$averageValue >= !!cutOff) %>%
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
          dplyr::select(-.data$populationSize) %>%
          dplyr::select(
            .data$cohortId,
            .data$timeId,
            .data$covariateId,
            .data$sumValue,
            .data$mean,
            .data$sd
          ) %>%
          dplyr::left_join(featureExtractionOutput$timeRef,
                           by = "timeId") %>%
          dplyr::select(
            .data$cohortId,
            .data$covariateId,
            .data$startDay,
            .data$endDay,
            .data$sumValue,
            .data$mean,
            .data$sd
          ) %>%
          dplyr::distinct()
        
        if ("covariates" %in% names(results)) {
          Andromeda::appendToTable(results$covariates, covariates)
        } else {
          results$covariates <- covariates
        }
      }
      if ("covariatesContinuous" %in% names(featureExtractionOutput) &&
          dplyr::pull(dplyr::count(featureExtractionOutput$covariatesContinuous)) > 0) {
        ParallelLogger::logTrace("      - appending covariate continouous")
        
        if (!(
          featureExtractionOutput$covariatesContinuous %>%
          dplyr::select(.data$timeId) %>%
          dplyr::collect() %>%
          dplyr::distinct() %>%
          dplyr::pull() %>% is.na()
        )) {
          stop(
            "Expecting timeId to be NA/NULL for covariateContinous but value found. This suggests a change in OHDSI/FeatureExtraction package. Please contact developer."
          )
        }
        
        # covariatesContinous in feature extraction does not return results
        # by time window. So time id is NA i.e. startDay and endDay are NA
        covariatesContinuous <-
          featureExtractionOutput$covariatesContinuous %>%
          dplyr::rename(
            mean = .data$averageValue,
            sd = .data$standardDeviation,
            cohortId = .data$cohortDefinitionId
          ) %>%
          dplyr::mutate("startDay" = .data$timeId) %>%
          dplyr::mutate("endDay" = .data$timeId) %>%
          dplyr::select(-.data$timeId)
        
        covariates <- covariatesContinuous %>%
          dplyr::mutate(sumValue = -1) %>%
          dplyr::select(
            .data$cohortId,
            .data$startDay,
            .data$endDay,
            .data$covariateId,
            .data$sumValue,
            .data$mean,
            .data$sd
          )
        
        covariatesContinuous <- covariatesContinuous %>%
          dplyr::mutate(sumValue = -1) %>%
          dplyr::select(
            .data$cohortId,
            .data$covariateId,
            .data$startDay,
            .data$endDay,
            .data$sumValue,
            .data$mean,
            .data$sd,
            .data$medianValue,
            .data$p10Value,
            .data$p25Value,
            .data$p75Value,
            .data$p90Value
          ) %>%
          dplyr::distinct()
        
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
      
      if ("analysisRef" %in% names(results)) {
        Andromeda::appendToTable(results$analysisRef, analysisRef)
      } else {
        results$analysisRef <- featureExtractionOutput$analysisRef
      }
      
      if ("covariateRef" %in% names(results)) {
        Andromeda::appendToTable(results$covariateRef, covariateRef)
      } else {
        results$covariateRef <- featureExtractionOutput$covariateRef
      }
      
      if ("temporalTimeRef" %in% names(results)) {
        Andromeda::appendToTable(results$covariateRef, covariateRef)
      } else  {
        results$temporalTimeRef <- featureExtractionOutput$timeRef %>%
          dplyr::select(.data$startDay, .data$endDay) %>%
          dplyr::distinct()
      }
    }
    
    if ("analysisRef" %in% names(results)) {
      results$analysisRef <- results$analysisRef %>%
        dplyr::distinct()
    }
    
    if ("covariateRef" %in% names(results)) {
      results$covariateRef <- results$covariateRef %>%
        dplyr::distinct()
    }
    
    if ("temporalTimeRef" %in% names(results)) {
      results$temporalTimeRef <- results$temporalTimeRef %>%
        dplyr::distinct() %>% 
        dplyr::mutate(temporalName = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
        dplyr::mutate(
          temporalName = dplyr::case_when(
            .data$endDay == 0 & .data$startDay == -9999 ~ "Any Time Prior",
            .data$endDay == 0 &
              .data$startDay == -365 ~ "Long Term",
            .data$endDay == 0 &
              .data$startDay == -180 ~ "Medium Term",
            .data$endDay == 0 &
              .data$startDay == -9999 ~ "Short Term",
            TRUE ~ .data$temporalName
          )
        )
    }
    
    delta <- Sys.time() - startTime
    ParallelLogger::logTrace(" - Cohort characterization took ",
                             signif(delta, 3),
                             " ",
                             attr(delta, "units"))
    return(results)
  }
