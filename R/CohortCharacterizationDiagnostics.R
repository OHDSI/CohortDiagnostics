# Copyright 2020 Observational Health Data Sciences and Informatics
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

#' Create characterization of a cohort
#'
#' @description
#' Computes features using all drugs, conditions, procedures, etc. observed on or prior to the cohort
#' index date.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template OracleTempSchema
#'
#' @template CohortTable
#'
#' @param cohortIds           A vector of cohortIds (1 or more) used to reference the cohort in the cohort
#'                            table. 
#'
#' @template  cdmVersion
#' 
#' @param covariateSettings   Either an object of type \code{covariateSettings} as created using one of
#'                            the createCovariate functions in the FeatureExtraction package, or a list
#'                            of such objects.
#'                            
#' @param batchSize           Maximum number of cohorts to characterize at once. A larger batch size will
#'                            be quicker, but may run out of resources on the server.
#'
#' @return
#' An Andromeda object with information on the covariates.
#'
#' @export
getCohortCharacteristics <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     oracleTempSchema = NULL,
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
      ParallelLogger::logInfo(sprintf("Batch characterization. Processing cohorts %s through %s",
                                      start,
                                      end))
    }
    featureExtractionOutput <- FeatureExtraction::getDbCovariateData(connection = connection,
                                                                     oracleTempSchema = oracleTempSchema,
                                                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                                                     cdmVersion = cdmVersion,
                                                                     cohortTable = cohortTable,
                                                                     cohortId = cohortIds[start:end],
                                                                     covariateSettings = covariateSettings,
                                                                     aggregated = TRUE)
    
    populationSize <- attr(x = featureExtractionOutput, which = "metaData")$populationSize
    populationSize <- dplyr::tibble(cohortId = names(populationSize),
                                    populationSize = populationSize)
    
    if (!"analysisRef" %in% names(results)) {
      results$analysisRef <- featureExtractionOutput$analysisRef
    }
    if (!"covariateRef" %in% names(results)) {
      results$covariateRef <- featureExtractionOutput$covariateRef 
    } else {
      covariateIds <- results$covariateRef %>%
        dplyr::select(.data$covariateId) 
      Andromeda::appendToTable(results$covariateRef, featureExtractionOutput$covariateRef %>% 
                                 dplyr::anti_join(covariateIds, by = "covariateId", copy = TRUE))
    }
    if ("timeRef" %in% names(featureExtractionOutput) && !"timeRef" %in% names(results)) {
      results$timeRef <- featureExtractionOutput$timeRef
    }
    
    if ("covariates" %in% names(featureExtractionOutput) && 
        dplyr::pull(dplyr::count(featureExtractionOutput$covariates)) > 0) {
      
      covariates <- featureExtractionOutput$covariates %>% 
        dplyr::rename(cohortId = .data$cohortDefinitionId) %>% 
        dplyr::left_join(populationSize, by = "cohortId", copy = TRUE) %>% 
        dplyr::mutate(sd = sqrt(((populationSize * .data$sumValue) + .data$sumValue)/(populationSize^2))) %>% 
        dplyr::rename(mean = .data$averageValue) %>% 
        dplyr::select(-.data$sumValue, -.data$populationSize)
      
      if (FeatureExtraction::isTemporalCovariateData(featureExtractionOutput)) {
        covariates <- covariates %>% 
          dplyr::select(.data$cohortId, .data$timeId, .data$covariateId, .data$mean, .data$sd)
      } else {
        covariates <- covariates %>% 
          dplyr::select(.data$cohortId, .data$covariateId, .data$mean, .data$sd)
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
        dplyr::rename(mean = .data$averageValue, 
                      sd = .data$standardDeviation, 
                      cohortId = .data$cohortDefinitionId)
      if (FeatureExtraction::isTemporalCovariateData(featureExtractionOutput)) {
        covariates <- covariates %>% 
          dplyr::select(.data$cohortId, .data$timeId, .data$covariateId, .data$mean, .data$sd)
      } else {
        covariates <- covariates %>% 
          dplyr::select(.data$cohortId, .data$covariateId, .data$mean, .data$sd)
      }
      if ("covariates" %in% names(results)) {
        Andromeda::appendToTable(results$covariates, covariates) 
      } else {
        results$covariates <- covariates
      }
    }
  }
  
  delta <- Sys.time() - startTime
  ParallelLogger::logInfo("Cohort characterization took ", signif(delta, 3), " ", attr(delta, "units"))
  return(results)
}

#' Compare cohort characteristics
#'
#' @description
#' Compare the characteristics of two cohorts, computing the standardized difference of the mean.
#'
#' @param characteristics1   Characteristics of the first cohort, as created using the
#'                           \code{\link{getCohortCharacteristics}} function.
#' @param characteristics2   Characteristics of the second cohort, as created using the
#'                           \code{\link{getCohortCharacteristics}} function.
#'
#' @return
#' A data frame comparing the characteristics of the two cohorts.
#'
#' @export
compareCohortCharacteristics <- function(characteristics1, characteristics2) {
  m <- dplyr::full_join(x = characteristics1 %>% dplyr::distinct(), 
                        y = characteristics2 %>% dplyr::distinct(),
                        suffix = c("1", "2")) %>%
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = 0)),
                  sd = sqrt(.data$sd1^2 + .data$sd2^2),
                  stdDiff = (.data$mean2 - .data$mean1)/.data$sd) %>% 
    dplyr::arrange(-abs(.data$stdDiff))
  return(m)
}
