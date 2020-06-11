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

#' Get time distributions of a cohort
#'
#' @description
#' Computes the distribution of the observation time before and after index, and time within a cohort.
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
#'
#' @return
#' A data frame with time distributions
#'
#' @export
getTimeDistributions <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 oracleTempSchema = NULL,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortId) {

  start <- Sys.time()

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (!checkIfCohortInstantiated(connection = connection,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 cohortId = cohortId)) {
    warning("Cohort with ID ", cohortId, " appears to be empty. Was it instantiated?")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Cohort characterization took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    return(data.frame())
  }
  
  covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsPriorObservationTime = TRUE,
                                                                  useDemographicsPostObservationTime = TRUE,
                                                                  useDemographicsTimeInCohort = TRUE)

  data <- FeatureExtraction::getDbCovariateData(connection = connection,
                                                oracleTempSchema = oracleTempSchema,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                cohortDatabaseSchema = cohortDatabaseSchema,
                                                cohortTable = cohortTable,
                                                cohortId = cohortId,
                                                covariateSettings = covariateSettings,
                                                aggregated = TRUE)
  
  if (is.null(data$covariatesContinuous)) {
    result <- data.frame()
  } else {
    result <- data$covariatesContinuous %>%
      inner_join(data$covariateRef) %>%
      select(-.data$conceptId, -.data$analysisId, -.data$covariateId, -.data$result$countValue) %>%
      rename(timeMetric = .data$covariateName) %>%
      collect()
  }
  attr(result, "cohortSize") <- data$metaData$populationSize
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing time distributions took",
                                signif(delta, 3),
                                attr(delta, "units")))
  return(result)
}

