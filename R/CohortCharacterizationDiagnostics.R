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
    populationSize <- dplyr::tibble(cohortId = names(populationSize) %>% as.numeric(),
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
        dplyr::mutate(p = .data$sumValue / .data$populationSize)  %>% 
        dplyr::mutate(sd = sqrt(.data$p * (1 - .data$p))) %>%
        dplyr::select(-.data$p) %>%
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
