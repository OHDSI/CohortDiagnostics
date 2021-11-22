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
#

#' Export Feature Extraction output to csv
#'
#' @description
#' Exports the output of \code{FeatureExtraction::getDbCovariateData} into CSV.
#'
#' @param featureExtractionDbCovariateData       An Andromeda object returned by \code{CohortDiagonstics::runCohortCharacterizationDiagnostics}
#'
#' @param covariateValueFileName                 The full path (including file name) for the csv file with covariate value data.
#'                                               e.g. "covariate_value.csv" or "temporal_covariate_value.csv"
#'
#' @param covariateValueContFileName             The full path (including file name) for the csv file with covariate value distribution data.
#'                                               e.g. "covariate_value_dist.csv" or "temporal_covariate_value_dist.csv"
#'
#' @param covariateRefFileName                   The full path (including file name) for the csv file with covariate reference data.
#'                                               e.g. "covariate_ref.csv" or "temporal_covariate_ref.csv"
#'
#' @param analysisRefFileName                    The full path (including file name) for the csv file with analysis reference data.
#'                                               e.g. "analysis_ref.csv" or "temporal_analysis_ref.csv"
#'
#' @param timeDistributionFileName               The full path (including file name) for the csv file with time distribution data.
#'                                               e.g. "time_distribution.csv"
#'
#' @param timeRefFileName                        The full path (including file name) for the csv file with time reference data.
#'                                               e.g. "temporal_time_ref.csv"
#'
#' @param cohortCounts                           Output \code{CohortDiagnostics::getCohortCounts}
#'
#' @param minCellCount                           (Optional). Default value = 5. The minimum cell count for fields contains person counts or fractions.
#'
#' @param incremental                            Create only cohort diagnostics that haven't been created before?
#'
#' @param databaseId                             A short string for identifying the database (e.g. 'Synpuf').
#'
#' @export
exportFeatureExtractionOutput <-
  function(featureExtractionDbCovariateData,
           databaseId,
           incremental = FALSE,
           covariateValueFileName = "covariate_value.csv",
           covariateValueContFileName = "covariate_value_dist.csv",
           covariateRefFileName = "covariate_ref.csv",
           analysisRefFileName = "analysis_ref.csv",
           timeDistributionFileName = NULL,
           timeRefFileName = NULL,
           cohortCounts,
           minCellCount = 5) {
    if (!'databaseId' %in% colnames(cohortCounts)) {
      cohortCounts <- cohortCounts %>%
        dplyr::collect() %>%
        dplyr::mutate(databaseId = !!databaseId)
    }
    if (nrow(cohortCounts) == 0) {
      stop(
        'Cant export Feature Extraction output, because all cohorts are reported to have a zero record count.'
      )
    }
    
    if (!"covariates" %in% names(featureExtractionDbCovariateData)) {
      warning("No characterization output for submitted cohorts")
    } else if (dplyr::pull(dplyr::count(featureExtractionDbCovariateData$covariateRef)) > 0) {
      featureExtractionDbCovariateData$filteredCovariates <-
        featureExtractionDbCovariateData$covariates %>%
        dplyr::mutate(databaseId = !!databaseId) %>%
        dplyr::left_join(cohortCounts,
                         by = c("cohortId", "databaseId"),
                         copy = TRUE) %>%
        dplyr::mutate(
          mean = dplyr::if_else(
            .data$mean != 0 &
              .data$mean < minCellCount / as.numeric(.data$cohortEntries),
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
      
      if (dplyr::pull(dplyr::count(featureExtractionDbCovariateData$filteredCovariates)) > 0) {
        covariateRef <-
          dplyr::collect(featureExtractionDbCovariateData$covariateRef)
        writeToCsv(
          data = covariateRef,
          fileName = covariateRefFileName,
          incremental = incremental,
          covariateId = covariateRef$covariateId
        )
        analysisRef <-
          dplyr::collect(featureExtractionDbCovariateData$analysisRef)
        writeToCsv(
          data = analysisRef,
          fileName = analysisRefFileName,
          incremental = incremental,
          analysisId = analysisRef$analysisId
        )
        if (!is.null(timeRefFileName)) {
          timeRef <-
            dplyr::collect(featureExtractionDbCovariateData$temporalTimeRef)
          writeToCsv(
            data = timeRef,
            fileName = timeRefFileName,
            incremental = incremental
          )
        }
        writeCovariateDataAndromedaToCsv(
          data = featureExtractionDbCovariateData$filteredCovariates,
          fileName = covariateValueFileName,
          incremental = incremental
        )
      }
    }
    
    if (!"covariatesContinuous" %in% names(featureExtractionDbCovariateData)) {
      ParallelLogger::logInfo("  - No continuous characterization output for submitted cohorts")
    } else if (dplyr::pull(dplyr::count(featureExtractionDbCovariateData$covariateRef)) > 0) {
      featureExtractionDbCovariateData$filteredCovariatesContinous <-
        featureExtractionDbCovariateData$covariatesContinuous %>%
        dplyr::filter(.data$countValue >= minCellCount) %>%
        dplyr::mutate(databaseId = !!databaseId)
      
      if (dplyr::pull(dplyr::count(
        featureExtractionDbCovariateData$filteredCovariatesContinous
      )) > 0) {
        writeCovariateDataAndromedaToCsv(
          data = featureExtractionDbCovariateData$filteredCovariatesContinous,
          fileName = covariateValueContFileName,
          incremental = incremental
        )
      }
      
      if (!is.null(timeDistributionFileName)) {
        featureExtractionDbCovariateData$timeDistribution <-
          featureExtractionDbCovariateData$covariatesContinuous %>%
          dplyr::inner_join(featureExtractionDbCovariateData$covariateRef, by = "covariateId") %>%
          dplyr::filter(.data$analysisId %in% c(8, 9, 10)) %>%
          dplyr::mutate(databaseId = !!databaseId) %>%
          dplyr::rename(
            standardDeviation = .data$sd,
            averageValue = .data$mean,
            timeMetric = .data$covariateName
          ) %>%
          dplyr::select(
            -.data$conceptId,
            -.data$analysisId,
            -.data$covariateId,
            -.data$result$countValue
          ) %>%
          dplyr::collect()
        if (dplyr::pull(dplyr::count(featureExtractionDbCovariateData$timeDistribution)) > 0) {
          writeCovariateDataAndromedaToCsv(
            data = featureExtractionDbCovariateData$timeDistribution,
            fileName = timeDistributionFileName,
            incremental = incremental
          )
        }
      }
    }
  }
