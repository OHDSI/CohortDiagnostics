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
#

exportCharacterization <- function(characteristics,
                                   databaseId,
                                   incremental,
                                   covariateValueFileName,
                                   covariateValueContFileName,
                                   covariateRefFileName,
                                   analysisRefFileName,
                                   timeRefFileName = NULL,
                                   counts,
                                   cutOff = 0.0001,
                                   minCellCount) {
  if (!"covariates" %in% names(characteristics)) {
    warning("No characterization output for submitted cohorts")
  } else if (dplyr::pull(dplyr::count(characteristics$covariateRef)) > 0) {
    characteristics$filteredCovariates <-
      characteristics$covariates %>%
      dplyr::filter(mean >= cutOff) %>%
      dplyr::mutate(databaseId = !!databaseId) %>%
      dplyr::left_join(counts,
                       by = c("cohortId", "databaseId"),
                       copy = TRUE) %>%
      dplyr::mutate(
        mean = dplyr::if_else(
          .data$mean != 0 & .data$mean < minCellCount / as.numeric(.data$cohortEntries),
          -minCellCount /  as.numeric(.data$cohortEntries),
          .data$mean
        ),
        sumValue  = dplyr::if_else(
          .data$sumValue  != 0 & .data$sumValue  < minCellCount,
          -minCellCount,
          .data$sumValue
        )
      ) %>%
      dplyr::mutate(sd = dplyr::if_else(.data$mean >= 0, .data$sd, 0)) %>%
      dplyr::mutate(mean = round(.data$mean, digits = 4),
                    sd = round(.data$sd, digits = 4)) %>%
      dplyr::select(-.data$cohortEntries, -.data$cohortSubjects)
    
    if (dplyr::pull(dplyr::count(characteristics$filteredCovariates)) > 0) {
      covariateRef <- dplyr::collect(characteristics$covariateRef)
      writeToCsv(
        data = covariateRef,
        fileName = covariateRefFileName,
        incremental = incremental,
        covariateId = covariateRef$covariateId
      )
      analysisRef <- dplyr::collect(characteristics$analysisRef)
      writeToCsv(
        data = analysisRef,
        fileName = analysisRefFileName,
        incremental = incremental,
        analysisId = analysisRef$analysisId
      )
      if (!is.null(timeRefFileName)) {
        timeRef <- dplyr::collect(characteristics$timeRef)
        writeToCsv(
          data = timeRef,
          fileName = timeRefFileName,
          incremental = incremental,
          analysisId = timeRef$timeId
        )
      }
      writeCovariateDataAndromedaToCsv(
        data = characteristics$filteredCovariates,
        fileName = covariateValueFileName,
        incremental = incremental
      )
    }
  }
  
  if (!"covariatesContinuous" %in% names(characteristics)) {
    ParallelLogger::logInfo("No continuous characterization output for submitted cohorts")
  } else if (dplyr::pull(dplyr::count(characteristics$covariateRef)) > 0) {
    characteristics$filteredCovariatesContinous <-
      characteristics$covariatesContinuous %>%
      dplyr::filter(.data$countValue >= minCellCount) %>%
      dplyr::mutate(databaseId = !!databaseId)
    
    if (dplyr::pull(dplyr::count(characteristics$filteredCovariatesContinous)) > 0) {
      writeCovariateDataAndromedaToCsv(
        data = characteristics$filteredCovariatesContinous,
        fileName = covariateValueContFileName,
        incremental = incremental
      )
    }
  }
}
