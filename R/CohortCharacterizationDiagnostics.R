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
runCohortCharacterizationDiagnostics <- function(connectionDetails = NULL,
                                                 connection = NULL,
                                                 cdmDatabaseSchema,
                                                 tempEmulationSchema = NULL,
                                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                                 cohortTable = "cohort",
                                                 cohortIds = NULL,
                                                 cdmVersion = 5,
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
    cohortTable = cohortTable
  )
  cohortIdsNew <- cohortCounts$cohortId %>% unique()
  
  if (is.null(cohortCounts)) {
    warning("No instantiated cohorts found.")
    return(NULL)
  } else if (any(is.null(cohortIds), length(cohortIds) == 0)) {
    ParallelLogger::logInfo(paste0(
      "No cohortIds provided. Found ",
      scales::comma(length(cohortIdsNew), accuracy = 1),
      " instantiated cohorts."
    ))
  } else {
    ParallelLogger::logInfo(
      paste0(
        "Of the ",
        scales::comma(length(cohortIds), accuracy = 1),
        " provided, found ",
        scales::comma(length(cohortIdsNew), accuracy = 1),
        " to be instantiated."
      )
    )
  }
  
  results <- Andromeda::andromeda()
  for (start in seq(1, length(cohortIds), by = batchSize)) {
    end <- min(start + batchSize - 1, length(cohortIds))
    if (length(cohortIds) > batchSize) {
      ParallelLogger::logInfo(sprintf(
        "Batch characterization. Processing cohorts %s through %s",
        start,
        end
      ))
    }
    
    if (all(covariateSettings$temporal,
            length(covariateSettings$temporalStartDays) > 5)) {
      pb <- utils::txtProgressBar(style = 3)
      
      covariateSettingTemporal <- NULL
      counterTemporal <- 0
      resultsTemporalBatch <- Andromeda::andromeda()
      
      for (startTemporal in seq(1, length(covariateSettings$temporalStartDays), by = 5)) {
        utils::setTxtProgressBar(pb, startTemporal/length(covariateSettings$temporalStartDays))
        counterTemporal <- counterTemporal + 1
        endTemporal <-
          min(startTemporal + 5 - 1,
              length(covariateSettings$temporalStartDays))
        covariateSettingTemporal[[counterTemporal]] <-
          covariateSettings
        covariateSettingTemporal[[counterTemporal]]$temporalStartDays <-
          covariateSettings$temporalStartDays[startTemporal:endTemporal]
        covariateSettingTemporal[[counterTemporal]]$temporalEndDays <-
          covariateSettings$temporalEndDays[startTemporal:endTemporal]
        # cant seem to suppress messages, maybe create a parallel logger cluster of one thread?
        featureExtractionOutput <-
          suppressMessages(invisible(FeatureExtraction::getDbCovariateData(
              connection = connection,
              oracleTempSchema = tempEmulationSchema,
              cdmDatabaseSchema = cdmDatabaseSchema,
              cohortDatabaseSchema = cohortDatabaseSchema,
              cdmVersion = cdmVersion,
              cohortTable = cohortTable,
              cohortId = cohortIds[start:end],
              covariateSettings = covariateSettingTemporal[[counterTemporal]],
              aggregated = TRUE
            )))
        
        if (!"analysisRef" %in% names(resultsTemporalBatch)) {
          resultsTemporalBatch$analysisRef <- featureExtractionOutput$analysisRef
        } else {
          analysisId <- resultsTemporalBatch$analysisRef %>%
            dplyr::select(.data$analysisId) %>% 
            dplyr::distinct()
          Andromeda::appendToTable(
            resultsTemporalBatch$analysisRef,
            featureExtractionOutput$analysisRef %>%
              dplyr::anti_join(analysisId, by = "analysisId", copy = TRUE)
          )
        }
        
        if (!"covariateRef" %in% names(resultsTemporalBatch)) {
          resultsTemporalBatch$covariateRef <- featureExtractionOutput$covariateRef
        } else {
          covariateRef <- resultsTemporalBatch$covariateRef %>%
            dplyr::select(.data$covariateId) %>% 
            dplyr::distinct()
          Andromeda::appendToTable(
            resultsTemporalBatch$covariateRef,
            featureExtractionOutput$covariateRef %>%
              dplyr::anti_join(covariateRef, by = "covariateId", copy = TRUE)
          )
        }
        
        if (!"timeRef" %in% names(resultsTemporalBatch)) {
          resultsTemporalBatch$timeRef <- featureExtractionOutput$timeRef
        } else {
          timeRef <- resultsTemporalBatch$timeRef %>%
            dplyr::select(.data$timeId) %>% 
            dplyr::distinct()
          Andromeda::appendToTable(
            resultsTemporalBatch$timeRef,
            featureExtractionOutput$timeRef %>%
              dplyr::anti_join(timeRef, by = "timeId", copy = TRUE)
          )
        }
        
        if (!"covariates" %in% names(resultsTemporalBatch)) {
          resultsTemporalBatch$covariates <- featureExtractionOutput$covariates
        } else {
          Andromeda::appendToTable(
            resultsTemporalBatch$covariates,
            featureExtractionOutput$covariates
          )
        }
      }
      featureExtractionOutput$analysisRef <- resultsTemporalBatch$analysisRef
      featureExtractionOutput$covariateRef <- resultsTemporalBatch$covariateRef
      featureExtractionOutput$timeRef <- resultsTemporalBatch$timeRef
      featureExtractionOutput$covariates <- resultsTemporalBatch$covariates
    } else {
      featureExtractionOutput <-
        FeatureExtraction::getDbCovariateData(
          connection = connection,
          oracleTempSchema = tempEmulationSchema,
          cdmDatabaseSchema = cdmDatabaseSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cdmVersion = cdmVersion,
          cohortTable = cohortTable,
          cohortId = cohortIds[start:end],
          covariateSettings = covariateSettings,
          aggregated = TRUE
        )
    }
    
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
      covariates <- featureExtractionOutput$covariates %>%
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
  ParallelLogger::logInfo("Cohort characterization took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
  return(results)
}
