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
#' @param cohortId            The cohort definition ID used to reference the cohort in the cohort
#'                            table.
#' @param covariateSettings   Either an object of type \code{covariateSettings} as created using one of
#'                            the createCovariate functions in the FeatureExtraction package, or a list
#'                            of such objects.
#'
#' @return
#' A data frame with cohort characteristics.
#'
#' @export
getCohortCharacteristics <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     oracleTempSchema = NULL,
                                     cohortDatabaseSchema = cdmDatabaseSchema,
                                     cohortTable = "cohort",
                                     cohortId,
                                     covariateSettings) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (!checkIfCohortInstantiated(connection = connection,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 cohortId = cohortId)) {
    warning("Cohort with ID ", cohortId, " appears to be empty. Was it instantiated? Skipping Characterization.")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Cohort characterization took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    data <- list()
    data$result <- tidyr::tibble()
    return(data)
  }
  
  data <- FeatureExtraction::getDbCovariateData(connection = connection,
                                                oracleTempSchema = oracleTempSchema,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                cohortDatabaseSchema = cohortDatabaseSchema,
                                                cohortTable = cohortTable,
                                                cohortId = cohortId,
                                                covariateSettings = covariateSettings,
                                                aggregated = TRUE)
  
  result <- tidyr::tibble()
  if (!is.null(data$covariates)) {
    n <- attr(x = data, which = "metaData")$populationSize
    if (FeatureExtraction::isTemporalCovariateData(data)) {
      counts <- data$covariates %>% 
        dplyr::collect() %>% 
        dplyr::mutate(sd = sqrt(((n * .data$sumValue) + .data$sumValue)/(n^2)))
      
      binaryCovs <- data$covariates %>% 
        dplyr::select(.data$timeId, .data$covariateId, .data$averageValue) %>% 
        dplyr::rename(mean = .data$averageValue) %>% 
        dplyr::collect() %>% 
        dplyr::left_join(counts) %>% 
        dplyr::select(-.data$sumValue)
    } else {
      counts <- data$covariates %>% 
        dplyr::collect() %>% 
        dplyr::mutate(sd = sqrt(((n * .data$sumValue) + .data$sumValue)/(n^2)))
      
      binaryCovs <- data$covariates %>% 
        dplyr::select(.data$covariateId, .data$averageValue) %>% 
        dplyr::rename(mean = .data$averageValue) %>% 
        dplyr::collect() %>% 
        dplyr::left_join(counts, by = "covariateId") %>% 
        dplyr::select(-.data$sumValue)
    }
    result <- dplyr::bind_rows(result, binaryCovs)
  }
  
  if (!is.null(data$covariatesContinuous)) {
    if (FeatureExtraction::isTemporalCovariateData(data)) {
      continuousCovs <- data$covariatesContinuous %>% 
        dplyr::select(.data$timeId, .data$covariateId, .data$averageValue, .data$standardDeviation) %>% 
        dplyr::rename(mean = .data$averageValue, sd = .data$standardDeviation) %>% 
        dplyr::collect()
    } else {
      continuousCovs <- data$covariatesContinuous %>% 
        dplyr::select(.data$covariateId, .data$averageValue, .data$standardDeviation) %>% 
        dplyr::rename(mean = .data$averageValue, sd = .data$standardDeviation) %>% 
        dplyr::collect()
    }
    result <- dplyr::bind_rows(result, continuousCovs)
  }
  if (nrow(result) > 0) {
    result <- result %>% dplyr::left_join(y = data$covariateRef %>% dplyr::collect(), by = ("covariateId"))
    if (FeatureExtraction::isTemporalCovariateData(data)) {
      result <- result %>% 
        dplyr::left_join(y = data$timeRef %>% dplyr::collect()) %>% 
        dplyr::rename(startDayTemporalCharacterization = .data$startDay,
                      endDayTemporalCharacterization = .data$endDay)
    }
  }
  attr(result, "cohortSize") <- attr(data, "metaData")$populationSize
  delta <- Sys.time() - start
  if (FeatureExtraction::isTemporalCovariateData(data)) {
    ParallelLogger::logInfo(paste("Temporal Cohort characterization took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  } else {
    ParallelLogger::logInfo(paste("Cohort characterization took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  }
  data$result <- result
  return(data)
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
                        by = c("covariateId", "databaseId", "covariateName", "covariateAnalysisId"),
                        suffix = c("1", "2")) %>%
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = 0)),
                  sd = sqrt(.data$sd1^2 + .data$sd2^2),
                  stdDiff = (.data$mean2 - .data$mean1)/.data$sd) %>% 
    dplyr::arrange(-abs(.data$stdDiff))
  return(m)
}
