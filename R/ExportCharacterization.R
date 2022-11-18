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
                                   minCharacterizationMean = 0.001,
                                   minCellCount) {
  if (!"covariates" %in% names(characteristics)) {
    warning("No characterization output for submitted cohorts")
  } else if (dplyr::pull(dplyr::count(characteristics$covariateRef)) > 0) {
    characteristics$filteredCovariates <-
      characteristics$covariates %>%
      dplyr::filter(mean >= minCharacterizationMean) %>%
      dplyr::mutate(databaseId = !!databaseId) %>%
      dplyr::left_join(counts,
        by = c("cohortId", "databaseId"),
        copy = TRUE
      ) %>%
      dplyr::mutate(
        mean = dplyr::if_else(
          mean != 0 & mean < minCellCount / as.numeric(cohortEntries),
          -minCellCount / as.numeric(cohortEntries),
          mean
        ),
        sumValue = dplyr::if_else(
          sumValue != 0 & sumValue < minCellCount,
          -minCellCount,
          sumValue
        )
      ) %>%
      dplyr::mutate(sd = dplyr::if_else(mean >= 0, sd, 0)) %>%
      dplyr::mutate(
        mean = round(mean, digits = 4),
        sd = round(sd, digits = 4)
      ) %>%
      dplyr::select(-cohortEntries, -cohortSubjects) %>%
      dplyr::distinct() %>% makeDataExportable(
        tableName = "temporal_covariate_value",
        minCellCount = minCellCount,
        databaseId = databaseId
      )

    if (dplyr::pull(dplyr::count(characteristics$filteredCovariates)) > 0) {
      covariateRef <- makeDataExportable(
        x = characteristics$covariateRef,
        tableName = "temporal_covariate_ref",
        minCellCount = minCellCount
      )
      writeToCsv(
        data = covariateRef,
        fileName = covariateRefFileName,
        incremental = TRUE,
        covariateId = covariateRef$covariateId
      )

      analysisRef <-  makeDataExportable(
        x = characteristics$analysisRef,
        tableName = "temporal_analysis_ref",
        minCellCount = minCellCount
      )
      writeToCsv(
        data = analysisRef,
        fileName = analysisRefFileName,
        incremental = TRUE,
        analysisId = analysisRef$analysisId
      )

      timeRef <- makeDataExportable(
        x = characteristics$timeRef,
        tableName = "temporal_time_ref",
        minCellCount = minCellCount
      )
      writeToCsv(
        data = timeRef,
        fileName = timeRefFileName,
        incremental = TRUE,
        analysisId = timeRef$timeId
      )

      writeToCsv(
        data = characteristics$filteredCovariates,
        fileName = covariateValueFileName,
        incremental = TRUE
      )
    }
  }

  if (!"covariatesContinuous" %in% names(characteristics)) {
    ParallelLogger::logInfo("No continuous characterization output for submitted cohorts")
  } else if (dplyr::pull(dplyr::count(characteristics$covariateRef)) > 0) {
     characteristics$filteredCovariatesContinous <- makeDataExportable(
       x = characteristics$covariatesContinuous,
       tableName = "temporal_covariate_value_dist",
       minCellCount = minCellCount,
       databaseId = databaseId
     )

    if (dplyr::pull(dplyr::count(characteristics$filteredCovariatesContinous)) > 0) {
      writeToCsv(
        data = characteristics$filteredCovariatesContinous,
        fileName = covariateValueContFileName,
        incremental = TRUE
      )
    }
  }
}
