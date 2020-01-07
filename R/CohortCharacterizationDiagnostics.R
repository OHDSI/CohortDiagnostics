# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of StudyDiagnostics
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
#' Computes features using all drugs, conditions, procedures, etc. observed on or prior to the cohort index date.
#' 
#' @template Connection
#' 
#' @template CdmDatabaseSchema
#' 
#' @template OracleTempSchema
#' 
#' @template CohortTable
#' 
#' @param instantiatedCohortId       The cohort definition ID used to reference the cohort in the cohort table.
#' @param covariateSettings          Either an object of type \code{covariateSettings} as created using one of the createCovariate 
#'                                   functions in the FeatureExtraction package, or a list of such objects.
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
                                     instantiatedCohortId,
                                     covariateSettings = FeatureExtraction::createDefaultCovariateSettings()) {
  if (!file.exists(getOption("fftempdir"))) {
    stop("This function uses ff, but the fftempdir '", getOption("fftempdir"), "' does not exist. Either create it, or set fftempdir to another location using options(fftempdir = \"<path>\")")
  }
  
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails) 
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (!checkIfCohortInstantiated(connection = connection,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 instantiatedCohortId = instantiatedCohortId)) {
    warning("Cohort with ID ", instantiatedCohortId, " appears to be empty. Was it instantiated?")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Cohort characterization took", signif(delta, 3), attr(delta, "units")))
    return(data.frame())
  } 
  
  data <- FeatureExtraction::getDbCovariateData(connection = connection,
                                                oracleTempSchema = oracleTempSchema,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                cohortDatabaseSchema = cohortDatabaseSchema,
                                                cohortTable = cohortTable,
                                                cohortId = instantiatedCohortId,
                                                covariateSettings = covariateSettings,
                                                aggregated = TRUE)
  result <- data.frame()
  if (!is.null(data$covariates)) {
    counts <- as.numeric(ff::as.ram(data$covariates$sumValue))
    n <- data$metaData$populationSize
    binaryCovs <- data.frame(covariateId = ff::as.ram(data$covariates$covariateId),
                             mean = ff::as.ram(data$covariates$averageValue))
    binaryCovs$sd <-  sqrt((n * counts + counts)/(n^2))
    result <- rbind(result, binaryCovs)
  }
  if (!is.null(data$covariatesContinuous)) {
    continuousCovs <- data.frame(covariateId = ff::as.ram(data$covariatesContinuous$covariateId),
                                 mean = ff::as.ram(data$covariatesContinuous$averageValue),
                                 sd =  ff::as.ram(data$covariatesContinuous$standardDeviation))
    result <- rbind(result, continuousCovs)
  }
  if (nrow(result) > 0) {
    result <- merge(result, ff::as.ram(data$covariateRef))
    result$conceptId <- NULL
  }
  attr(result, "cohortSize") <- data$metaData$populationSize
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Cohort characterization took", signif(delta, 3), attr(delta, "units")))
  return(result)
}

#' Compare cohort characteristics
#' 
#' @description 
#' Compare the characteristics of two cohorts, computing the standardized difference of the mean.
#'
#' @param characteristics1 Characteristics of the first cohort, as created using the \code{\link{getCohortCharacteristics}} function.
#' @param characteristics2 Characteristics of the second cohort, as created using the \code{\link{getCohortCharacteristics}} function.
#'
#' @return
#' A data frame comparing the characteristics of the two cohorts.
#' 
#' @export
compareCohortCharacteristics <- function(characteristics1,
                                         characteristics2) {
  
  m <- merge(data.frame(covariateId = characteristics1$covariateId,
                        mean1 = characteristics1$mean,
                        sd1 = characteristics1$sd),
             data.frame(covariateId = characteristics2$covariateId,
                        mean2 = characteristics2$mean,
                        sd2 = characteristics2$sd), 
             all = TRUE)
  m$sd <- sqrt(m$sd1^2 + m$sd2^2)
  m$stdDiff <- (m$mean2 - m$mean1)/m$sd
  
  ref <- unique(rbind(characteristics1[,c("covariateId", "covariateName")],
                      characteristics2[,c("covariateId", "covariateName")]))
  m <- merge(ref, m)
  m <- m[order(-abs(m$stdDiff)), ]
  return(m)
}
