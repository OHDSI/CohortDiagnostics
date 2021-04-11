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

getTimeDistributions <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 oracleTempSchema = NULL,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortIds,
                                 cdmVersion = 5) {
  
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsPriorObservationTime = TRUE,
                                                                  useDemographicsPostObservationTime = TRUE,
                                                                  useDemographicsTimeInCohort = TRUE)
  
  data <- FeatureExtraction::getDbCovariateData(connection = connection,
                                                oracleTempSchema = oracleTempSchema,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                cohortDatabaseSchema = cohortDatabaseSchema,
                                                cohortTable = cohortTable,
                                                cohortId = cohortIds,
                                                covariateSettings = covariateSettings,
                                                cdmVersion = cdmVersion,
                                                aggregated = TRUE)
  
  if (is.null(data$covariatesContinuous)) {
    result <- tidyr::tibble()
  } else {
    result <- data$covariatesContinuous %>%
      dplyr::inner_join(data$covariateRef, by = "covariateId") %>%
      dplyr::select(-.data$conceptId, -.data$analysisId, -.data$covariateId, -.data$result$countValue) %>%
      dplyr::rename(timeMetric = .data$covariateName,
                    cohortId = .data$cohortDefinitionId) %>%
      dplyr::collect()
  }
  attr(result, "cohortSize") <- data$metaData$populationSize
  delta <- Sys.time() - start
  ParallelLogger::logInfo("Computing time distributions took ", signif(delta, 3), " ", attr(delta, "units"))
  return(result)
}
