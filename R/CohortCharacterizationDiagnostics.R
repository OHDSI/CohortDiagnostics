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
           covariateSettings = createDefaultCovariateSettings(),
           batchSize = 100) {
    
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
      ParallelLogger::logInfo(
        paste0(
          "   - Starting Characterization."
        )
      )
    }
    
    results <- Andromeda::andromeda()
    
    if (all(!is.null(covariateSettings$temporal),
            isTRUE(covariateSettings$temporal))) {
      batchSize <-
        max(1, round(batchSize / length(
          covariateSettings$temporalStartDays
        )))
    }
    
    for (start in seq(1, length(cohortIdsNew), by = batchSize)) {
      end <- min(start + batchSize - 1, length(cohortIdsNew))
      if (length(cohortIdsNew) > batchSize) {
        ParallelLogger::logTrace(
          sprintf(
            "    - Batch characterization. Processing cohorts %s through %s",
            start,
            end
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
          dplyr::select(-.data$populationSize)
        
        if (FeatureExtraction::isTemporalCovariateData(featureExtractionOutput)) {
          covariates <- covariates %>%
            dplyr::select(
              .data$cohortId,
              .data$timeId,
              .data$covariateId,
              .data$sumValue,
              .data$mean,
              .data$sd
            )
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
          dplyr::pull(dplyr::count(featureExtractionOutput$covariatesContinuous)) > 0 &&
          (!FeatureExtraction::isTemporalCovariateData(featureExtractionOutput))) {
        #   covariatesContinous seems to return NA for timeId
        #    in featureExtractionOutput$covariatesContinuous
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
            dplyr::select(
              .data$cohortId,
              .data$timeId,
              .data$covariateId,
              .data$sumValue,
              .data$mean,
              .data$sd
            )
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
    ParallelLogger::logTrace(" - Cohort characterization took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
    return(results)
  }
