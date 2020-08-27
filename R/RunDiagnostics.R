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

#' Run cohort diagnostics
#'
#' @description
#' Runs the cohort diagnostics on all (or a subset of) the cohorts instantiated using the
#' \code{ROhdsiWebApi::insertCohortDefinitionSetInPackage} function. Assumes the cohorts have already been instantiated.
#' 
#' Characterization:
#' If runTemporalCohortCharacterization argument is TRUE, then the following default covariateSettings object will be created
#' using \code{RFeatureExtraction::createTemporalCovariateSettings}
#' Alternatively, a covariate setting object may be created using the above as an example.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template OracleTempSchema
#'
#' @template CohortTable
#'
#' @template CohortSetSpecs
#' 
#' @template CohortSetReference
#' 
#' @param vocabularyDatabaseSchema    Schema name where your OMOP vocabulary resides. It is common for this
#'                                    to be the same as cdm database schema. Note that for SQL Server, 
#'                                    this should include both the database and schema name, for example
#'                                    'cdm_data.dbo'.
#' @param inclusionStatisticsFolder   The folder where the inclusion rule statistics are stored. Can be
#'                                    left NULL if \code{runInclusionStatistics = FALSE}.
#' @param exportFolder                The folder where the output will be exported to. If this folder
#'                                    does not exist it will be created.
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    diagnostics to.
#' @param databaseId                  A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName                The full name of the database.
#' @param databaseDescription         A short description (several sentences) of the database.
#' @param runInclusionStatistics      Generate and export statistic on the cohort inclusion rules?
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runTimeDistributions        Generate and export cohort time distributions?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#' @param runIncidenceRate            Generate and export the cohort incidence  rates?
#' @param runCohortOverlap            Generate and export the cohort overlap?
#' @param runCohortCharacterization   Generate and export the cohort characterization?
#' @param refreshVocabulary           Do you want to refresh the copy of vocabulary?
#' @param covariateSettings           Either an object of type \code{covariateSettings} as created using one of
#'                                    the createCovariateSettings function in the FeatureExtraction package, or a list
#'                                    of such objects.
#' @param runTemporalCohortCharacterization   Generate and export the temporal cohort characterization?
#' @param temporalCovariateSettings   Either an object of type \code{covariateSettings} as created using one of
#'                                    the createTemporalCovariateSettings function in the FeatureExtraction package, or a list
#'                                    of such objects.
#' @param minCellCount                The minimum cell count for fields contains person counts or fractions.
#' @param incremental                 Create only cohort diagnostics that haven't been created before?
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are kept
#'                                    of which cohort diagnostics has been executed.
#'
#' @export
runCohortDiagnostics <- function(packageName = NULL,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
                                 baseUrl = NULL,
                                 cohortSetReference = NULL,
                                 connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 oracleTempSchema = NULL,
                                 cohortDatabaseSchema,
                                 vocabularyDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortIds = NULL,
                                 inclusionStatisticsFolder = NULL,
                                 exportFolder,
                                 databaseId,
                                 databaseName = databaseId,
                                 databaseDescription = "",
                                 runInclusionStatistics = TRUE,
                                 runIncludedSourceConcepts = TRUE,
                                 runOrphanConcepts = TRUE,
                                 runTimeDistributions = TRUE,
                                 runBreakdownIndexEvents = TRUE,
                                 runIncidenceRate = TRUE,
                                 runCohortOverlap = TRUE,
                                 runCohortCharacterization = TRUE,
                                 refreshVocabulary = TRUE,
                                 covariateSettings = FeatureExtraction::createDefaultCovariateSettings(),
                                 runTemporalCohortCharacterization = TRUE,
                                 temporalCovariateSettings = FeatureExtraction::createTemporalCovariateSettings(useConditionOccurrence = TRUE,
                                                                                                                useDrugEraStart = TRUE,
                                                                                                                useProcedureOccurrence = TRUE,
                                                                                                                useMeasurement = TRUE,
                                                                                                                temporalStartDays = c(-365,-30,0,1,31, 
                                                                                                                                      seq(from = -30, to = -420, by = -30), 
                                                                                                                                      seq(from = 1, to = 390, by = 30)),
                                                                                                                temporalEndDays = c(-31,-1,0,30,365,
                                                                                                                                    seq(from = 0, to = -390, by = -30),
                                                                                                                                    seq(from = 31, to = 420, by = 30))),
                                 minCellCount = 5,
                                 incremental = FALSE,
                                 incrementalFolder = exportFolder) {
  
  start <- Sys.time()
  ParallelLogger::logInfo("Run Cohort Diagnostics started at ", start)
  
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(runInclusionStatistics, add = errorMessage)
  checkmate::assertLogical(runIncludedSourceConcepts, add = errorMessage)
  checkmate::assertLogical(runOrphanConcepts, add = errorMessage)
  checkmate::assertLogical(runTimeDistributions, add = errorMessage)
  checkmate::assertLogical(runBreakdownIndexEvents, add = errorMessage)
  checkmate::assertLogical(runIncidenceRate, add = errorMessage)
  checkmate::assertLogical(runCohortOverlap, add = errorMessage)
  checkmate::assertLogical(runCohortCharacterization, add = errorMessage)
  
  if (any(runInclusionStatistics, runIncludedSourceConcepts, runOrphanConcepts, 
          runTimeDistributions, runBreakdownIndexEvents, runIncidenceRate,
          runCohortOverlap, runCohortCharacterization)) {
    checkmate::assertCharacter(x = cdmDatabaseSchema, min.len = 1, add = errorMessage)
    checkmate::assertCharacter(x = cohortDatabaseSchema, min.len = 1, add = errorMessage)
    checkmate::assertCharacter(x = vocabularyDatabaseSchema, min.len = 1, add = errorMessage)
    checkmate::assertCharacter(x = cohortTable, min.len = 1, add = errorMessage)
    checkmate::assertCharacter(x = databaseId, min.len = 1, add = errorMessage)
    checkmate::assertCharacter(x = databaseDescription, min.len = 1, add = errorMessage)
  }
  
  minCellCount <- utils::type.convert(minCellCount)
  checkmate::assertInteger(x = minCellCount, lower = 0, add = errorMessage)
  checkmate::assertLogical(incremental, add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  # checking folders
  createIfNotExist(type = 'folder', name = exportFolder)
  checkmate::assertDirectory(x = exportFolder, access = 'x')
  createIfNotExist(type = 'folder', name = incrementalFolder)
  checkmate::assertDirectory(x = incrementalFolder, access = 'x')
  if (isTRUE(runInclusionStatistics)) {
    createIfNotExist(type = 'folder', name = inclusionStatisticsFolder)
    checkmate::assertDirectory(x = inclusionStatisticsFolder, access = 'x')
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  cohorts <- getCohortsJsonAndSql(packageName = packageName,
                                  cohortToCreateFile = cohortToCreateFile,
                                  baseUrl = baseUrl,
                                  cohortSetReference = cohortSetReference,
                                  cohortIds = cohortIds)
  
  # # set up connection to server
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  ##############################
  
  writeToCsv(cohorts, file.path(exportFolder, "cohort.csv"))
  
  if (incremental) {
    cohorts$checksum <- computeChecksum(cohorts$sql)
    recordKeepingFile <- file.path(incrementalFolder, "CreatedDiagnostics.csv")
  }
  
  
  ParallelLogger::logInfo("Saving database metadata")
  database <- tibble::tibble(databaseId = databaseId,
                             databaseName = databaseName,
                             description = databaseDescription,
                             isMetaAnalysis = 0)
  writeToCsv(database, file.path(exportFolder, "database.csv"))
  
  # Counting cohorts -----------------------------------------------------------------------
  ParallelLogger::logInfo("Counting cohorts")
  subset <- subsetToRequiredCohorts(cohorts = cohorts, 
                                    task = "getCohortCounts", 
                                    incremental = incremental, 
                                    recordKeepingFile = recordKeepingFile)
  
  if (nrow(subset) > 0) {
    counts <- getCohortCounts(connection = connection,
                              cohortDatabaseSchema = cohortDatabaseSchema,
                              cohortTable = cohortTable,
                              cohortIds = subset$cohortId)
    if (nrow(counts) > 0) {
      counts$databaseId <- databaseId
      counts <- enforceMinCellValue(counts, "cohortEntries", minCellCount)
      counts <- enforceMinCellValue(counts, "cohortSubjects", minCellCount)
    }
    writeToCsv(counts, file.path(exportFolder, "cohort_count.csv"), incremental = incremental, cohortId = subset$cohortId)
    recordTasksDone(cohortId = subset$cohortId,
                    task = "getCohortCounts",
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
  }
  
  
  if (runInclusionStatistics) {
    # Inclusion statistics -----------------------------------------------------------------------
    ParallelLogger::logInfo("Fetching inclusion rule statistics. Started at ", Sys.time())
    subset <- subsetToRequiredCohorts(cohorts = cohorts, 
                                      task = "runInclusionStatistics", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      runInclusionStatistics <- function(row) {
        ParallelLogger::logInfo("- Fetching inclusion rule statistics for cohort ", row$cohortName)
        stats <- getInclusionStatisticsFromFiles(cohortId = row$cohortId,
                                                 folder = inclusionStatisticsFolder,
                                                 simplify = TRUE)
        if (nrow(stats) > 0) {
          stats$cohortId <- row$cohortId
        }
        return(stats)
      }
      stats <- lapply(split(subset, subset$cohortId), runInclusionStatistics)
      stats <- do.call(rbind, stats)
      if (nrow(stats) > 0) {
        stats$databaseId <- databaseId
        stats <- enforceMinCellValue(stats, "meetSubjects", minCellCount)
        stats <- enforceMinCellValue(stats, "gainSubjects", minCellCount)
        stats <- enforceMinCellValue(stats, "totalSubjects", minCellCount)
        stats <- enforceMinCellValue(stats, "remainSubjects", minCellCount)
      }
      writeToCsv(stats, file.path(exportFolder, "inclusion_rule_stats.csv"), incremental = incremental, cohortId = subset$cohortId)
      recordTasksDone(cohortId = subset$cohortId,
                      task = "runInclusionStatistics",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
  }
  
  if (runIncludedSourceConcepts || runOrphanConcepts) {
    
    # Concept set diagnostics -----------------------------------------------
    runConceptSetDiagnostics(connection = connection,
                             oracleTempSchema = oracleTempSchema,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             databaseId = databaseId,
                             cohorts = cohorts,
                             runIncludedSourceConcepts = runIncludedSourceConcepts,
                             runOrphanConcepts = runOrphanConcepts,
                             exportFolder = exportFolder,
                             minCellCount = minCellCount,
                             conceptCountsDatabaseSchema = NULL,
                             conceptCountsTable = "#concept_counts",
                             conceptCountsTableIsTemp = TRUE,
                             useExternalConceptCountsTable = FALSE,
                             incremental = incremental,
                             recordKeepingFile = recordKeepingFile)
  }
  
  if (runTimeDistributions) {
    startTimeDistribution <- Sys.time()
    # Time distributions ----------------------------------------------------------------------
    ParallelLogger::logInfo("Creating time distributions")
    subset <- subsetToRequiredCohorts(cohorts = cohorts, 
                                      task = "runTimeDistributions", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      runTimeDist <- function(row) {
        ParallelLogger::logInfo("- Creating time distributions for cohort ", row$cohortName)
        data <- getTimeDistributions(connection = connection,
                                     oracleTempSchema = oracleTempSchema,
                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                     cohortTable = cohortTable,
                                     cohortId = row$cohortId)
        if (nrow(data) > 0) {
          data$cohortId <- row$cohortId
        }
        return(data)
      }
      data <- lapply(split(subset, subset$cohortId), runTimeDist)
      data <- do.call(rbind, data)
      if (nrow(data) > 0) {
        data$databaseId <- databaseId
        writeToCsv(data, file.path(exportFolder, "time_distribution.csv"), incremental = incremental, cohortId = subset$cohortId)
      }
      recordTasksDone(cohortId = subset$cohortId,
                      task = "runTimeDistributions",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
    delta <- Sys.time() - startTimeDistribution
    ParallelLogger::logInfo(paste("Running time distribution took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  }
  
  if (runBreakdownIndexEvents) {
    startRunBreakdownIndexEvents <- Sys.time()
    # Index event breakdown ---------------------------------------------------------------------
    ParallelLogger::logInfo("Breaking down index events")
    subset <- subsetToRequiredCohorts(cohorts = cohorts, 
                                      task = "runBreakdownIndexEvents", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      runBreakdownIndexEvents <- function(row) {
        ParallelLogger::logInfo("- Breaking down index events for cohort ", row$cohortName)
        data <- breakDownIndexEvents(connection = connection,
                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                     oracleTempSchema = oracleTempSchema,
                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                     cohortTable = cohortTable,
                                     cohortId = row$cohortId,
                                     cohortJson = row$json,
                                     cohortSql = row$sql)
        if (nrow(data) > 0) {
          data$cohortId <- row$cohortId
        }
        return(data)
      }
      data <- lapply(split(subset, subset$cohortId), runBreakdownIndexEvents)
      data <- do.call(rbind, data)
      if (nrow(data) > 0) {
        data$databaseId <- databaseId
        data <- enforceMinCellValue(data, "conceptCount", minCellCount)
      }
      writeToCsv(data, file.path(exportFolder, "index_event_breakdown.csv"), incremental = incremental, cohortId = subset$cohortId)
      recordTasksDone(cohortId = subset$cohortId,
                      task = "runBreakdownIndexEvents",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
    
    delta <- Sys.time() - startRunBreakdownIndexEvents
    ParallelLogger::logInfo(paste("Running index event breakdown took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  }
  
  if (runIncidenceRate) {
    startIncidenceRate <- Sys.time()
    # Incidence rates --------------------------------------------------------------------------------------
    ParallelLogger::logInfo("Computing incidence rate")
    subset <- subsetToRequiredCohorts(cohorts = cohorts, 
                                      task = "runIncidenceRate", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      runIncidenceRate <- function(row) {
        ParallelLogger::logInfo("- Computing incidence rate for cohort ", row$cohortName)
        cohortExpression <- RJSONIO::fromJSON(row$json)
        washoutPeriod <- tryCatch({
          cohortExpression$PrimaryCriteria$ObservationWindow$PriorDays
        }, error = function(e) {
          0
        })
        data <- getIncidenceRate(connection = connection,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 oracleTempSchema = oracleTempSchema,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 cohortId = row$cohortId,
                                 firstOccurrenceOnly = TRUE,
                                 washoutPeriod = washoutPeriod)
        if (nrow(data) > 0) {
          data$cohortId <- row$cohortId
        }
        return(data)
      }
      data <- lapply(split(subset, subset$cohortId), runIncidenceRate)
      data <- do.call(rbind, data)
      if (nrow(data) > 0) {
        data$databaseId <- databaseId
        data <- enforceMinCellValue(data, "cohortCount", minCellCount)
        data <- enforceMinCellValue(data, "incidenceRate", 1000*minCellCount/data$personYears)
      }
      writeToCsv(data, file.path(exportFolder, "incidence_rate.csv"), incremental = incremental, cohortId = subset$cohortId)
    }
    recordTasksDone(cohortId = subset$cohortId,
                    task = "runIncidenceRate",
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
    delta <- Sys.time() - startIncidenceRate
    ParallelLogger::logInfo(paste("Running Incidence Rate took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  }
  
  if (runCohortOverlap) {
    startCohortOverlap <- Sys.time()
    # Cohort overlap ---------------------------------------------------------------------------------
    ParallelLogger::logInfo("Computing cohort overlap")
    combis <- expand.grid(targetCohortId = cohorts$cohortId, comparatorCohortId = cohorts$cohortId)
    combis <- combis[combis$targetCohortId < combis$comparatorCohortId, ]
    if (incremental) {
      combis <- merge(combis, tibble::tibble(targetCohortId = cohorts$cohortId, targetChecksum = cohorts$checksum))
      combis <- merge(combis, tibble::tibble(comparatorCohortId = cohorts$cohortId, comparatorChecksum = cohorts$checksum))
      combis$checksum <- paste(combis$targetChecksum, combis$comparatorChecksum)
    }
    subset <- subsetToRequiredCombis(combis = combis, 
                                     task = "runCohortOverlap", 
                                     incremental = incremental, 
                                     recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      runCohortOverlap <- function(row) {
        ParallelLogger::logInfo("- Computing overlap for cohorts ",
                                row$targetCohortId,
                                " and ",
                                row$comparatorCohortId)
        data <- computeCohortOverlap(connection = connection,
                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                     cohortTable = cohortTable,
                                     targetCohortId = row$targetCohortId,
                                     comparatorCohortId = row$comparatorCohortId)
        if (nrow(data) > 0) {
          data$targetCohortId <- row$targetCohortId
          data$comparatorCohortId <- row$comparatorCohortId
        }
        return(data)
      }
      
      data <- lapply(split(subset, 1:nrow(subset)), runCohortOverlap)
      data <- do.call(rbind, data)
      if (nrow(data) > 0) {
        revData <- data
        revData <- swapColumnContents(revData, "targetCohortId", "comparatorCohortId")
        revData <- swapColumnContents(revData, "tOnlySubjects", "cOnlySubjects")
        revData <- swapColumnContents(revData, "tBeforeCSubjects", "cBeforeTSubjects")
        revData <- swapColumnContents(revData, "tInCSubjects", "cInTSubjects")
        data <- rbind(data, revData)
        data$databaseId <- databaseId
        data <- enforceMinCellValue(data, "eitherSubjects", minCellCount)
        data <- enforceMinCellValue(data, "bothSubjects", minCellCount)
        data <- enforceMinCellValue(data, "tOnlySubjects", minCellCount)
        data <- enforceMinCellValue(data, "cOnlySubjects", minCellCount)
        data <- enforceMinCellValue(data, "tBeforeCSubjects", minCellCount)
        data <- enforceMinCellValue(data, "cBeforeTSubjects", minCellCount)
        data <- enforceMinCellValue(data, "sameDaySubjects", minCellCount)
        data <- enforceMinCellValue(data, "tInCSubjects", minCellCount)
        data <- enforceMinCellValue(data, "cInTSubjects", minCellCount)
        writeToCsv(data = data, 
                   fileName = file.path(exportFolder, "cohort_overlap.csv"), 
                   incremental = incremental, 
                   targetCohortId = subset$targetCohortId, 
                   comparatorCohortId = subset$comparatorCohortId)
      }
      recordTasksDone(cohortId = subset$targetCohortId,
                      comparatorId = subset$comparatorCohortId,
                      task = "runCohortOverlap",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
    
    delta <- Sys.time() - startCohortOverlap
    ParallelLogger::logInfo(paste("Running Cohort Overlap took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  }
  
  if (runCohortCharacterization) {
    startCohortCharacterization <- Sys.time()
    # Cohort characterization ---------------------------------------------------------------
    ParallelLogger::logInfo("Creating cohort characterizations - started at ", Sys.time())
    subset <- subsetToRequiredCohorts(cohorts = cohorts, 
                                      task = "runCohortCharacterization", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      runCohortCharacterization <- function(row) {
        ParallelLogger::logInfo("- Creating characterization for cohort ", row$cohortName)
        data <- getCohortCharacteristics(connection = connection,
                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                         oracleTempSchema = oracleTempSchema,
                                         cohortDatabaseSchema = cohortDatabaseSchema,
                                         cohortTable = cohortTable,
                                         cohortId = row$cohortId,
                                         covariateSettings = covariateSettings)
        return(data)
      }
      data <- lapply(split(subset, subset$cohortId), runCohortCharacterization)
      
      result <- list()
      analysisRef <- list()
      covariateRef <- list()
      
      for (i in (1:length(data))) {
        result[[i]] <- data[[i]]$result %>% dplyr::collect()
        analysisRef[[i]] <- data[[i]]$analysisRef %>% dplyr::collect()
        covariateRef[[i]] <- data[[i]]$covariateRef %>% dplyr::collect()
      }
      result <- dplyr::bind_rows(result) %>% dplyr::distinct()
      analysisRef <- dplyr::bind_rows(analysisRef) %>% dplyr::distinct()
      covariateRef <- dplyr::bind_rows(covariateRef)  %>% dplyr::distinct()
      
      if (nrow(result) > 0) {
        writeToCsv(
          data = analysisRef,
          fileName = file.path(exportFolder, "analysis_ref.csv"),
          incremental = incremental
        )
        writeToCsv(
          data = covariateRef,
          fileName = file.path(exportFolder, "covariate_ref.csv"),
          incremental = incremental
        )
        
        if (!exists("counts")) {
          counts <- readr::read_csv(file = file.path(exportFolder, "cohort_count.csv"), col_types = readr::cols())
          names(counts) <- SqlRender::snakeCaseToCamelCase(names(counts))
        }
        result <- result  %>% 
          dplyr::mutate(databaseId = !!databaseId) %>% 
          dplyr::left_join(y = counts, by = c("cohortId", "databaseId"))
        result <- enforceMinCellValue(result, "mean", minCellCount/result$cohortEntries)
        result <- result %>% 
          dplyr::mutate(sd = dplyr::case_when(mean >= 0 ~ sd)) %>% 
          dplyr::mutate(mean = round(.data$mean, digits = 3),
                        sd = round(.data$sd, digits = 3)) %>% 
          dplyr::select(-.data$cohortEntries, -.data$cohortSubjects)
      }
      writeToCsv(result, file.path(exportFolder, "covariate_value.csv"), incremental = incremental, cohortId = subset$cohortId)
      
      recordTasksDone(cohortId = subset$cohortId,
                      task = "runCohortCharacterization",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
    
    delta <- Sys.time() - startCohortCharacterization
    ParallelLogger::logInfo(paste("Running Characterization took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  }
  
  if (runTemporalCohortCharacterization) {
    startTemporalCohortCharacterization <- Sys.time()
    # Temporal Cohort characterization ---------------------------------------------------------------
    ParallelLogger::logInfo("Creating Temporal cohort characterizations - started at ", Sys.time())
    subset <- subsetToRequiredCohorts(cohorts = cohorts, 
                                      task = "runTemporalCohortCharacterization", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      runTemporalCohortCharacterization <- function(row) {
        ParallelLogger::logInfo("- Creating temporal characterization for cohort ", row$cohortName)
        
        if (is.null(temporalCovariateSettings)) {
          ParallelLogger::logWarn("Temporal covariates not specified. skipping.")
        }
        
        data <- getCohortCharacteristics(connection = connection,
                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                         oracleTempSchema = oracleTempSchema,
                                         cohortDatabaseSchema = cohortDatabaseSchema,
                                         cohortTable = cohortTable,
                                         cohortId = row$cohortId,
                                         covariateSettings = temporalCovariateSettings)
        return(data)
      }
      data <- lapply(split(subset, subset$cohortId), runTemporalCohortCharacterization)
      result <- list()
      analysisRef <- list()
      covariateRef <- list()
      timeRef <- list()
      
      for (i in (1:length(data))) {
        result[[i]] <- data[[i]]$result %>% dplyr::collect()
        analysisRef[[i]] <- data[[i]]$analysisRef %>% dplyr::collect()
        covariateRef[[i]] <- data[[i]]$covariateRef %>% dplyr::collect()
        timeRef[[i]] <- data[[i]]$timeRef %>% dplyr::collect()
      }
      result <- dplyr::bind_rows(result) %>% dplyr::distinct()
      temporalAnalysisRef <- dplyr::bind_rows(analysisRef) %>% dplyr::distinct()
      temporalCovariateRef <- dplyr::bind_rows(covariateRef)  %>% dplyr::distinct()
      timeRef <- dplyr::bind_rows(timeRef) %>% dplyr::distinct()
      
      if (nrow(result) > 0) {
        result <- result %>% dplyr::mutate(mean = round(x = mean, digits = 3))
        writeToCsv(
          data = temporalAnalysisRef,
          fileName = file.path(exportFolder, "temporal_analysis_ref.csv"),
          incremental = incremental
        )
        writeToCsv(
          data = temporalCovariateRef,
          fileName = file.path(exportFolder, "temporal_covariate_ref.csv"),
          incremental = incremental
        )
        writeToCsv(
          data = timeRef,
          fileName = file.path(exportFolder, "time_ref.csv"),
          incremental = incremental
        )
        if (!exists("counts")) {
          counts <- readr::read_csv(file = file.path(exportFolder, "cohort_count.csv"), col_types = readr::cols())
          names(counts) <- SqlRender::snakeCaseToCamelCase(names(counts))
        }
        
        result <- result %>% 
          dplyr::mutate(databaseId = !!databaseId) %>% 
          dplyr::left_join(y = counts, by = c("cohortId", "databaseId"))
        result <- enforceMinCellValue(result, "mean", minCellCount/result$cohortEntries)
        result <- result %>% 
          dplyr::mutate(sd = dplyr::case_when(mean >= 0 ~ sd)) %>% 
          dplyr::mutate(mean = round(.data$mean, digits = 3),
                        sd = round(.data$sd, digits = 3)) %>% 
          dplyr::select(-.data$cohortEntries, -.data$cohortSubjects)
        writeToCsv(result, file.path(exportFolder, "temporal_covariate_value.csv"), incremental = incremental, cohortId = subset$cohortId)
      }
      recordTasksDone(cohortId = subset$cohortId,
                      task = "runTemporalCohortCharacterization",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
    delta <- Sys.time() - startTemporalCohortCharacterization
    ParallelLogger::logInfo(paste("Running Temporal Characterization took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  }
  
  ParallelLogger::logInfo("Getting concept sets from all cohort definitions.")
  conceptSetsFromCohorts <-
    combineConceptSetsFromCohorts(cohorts = readr::read_csv(file.path(exportFolder, "cohort.csv"), 
                                                            col_types = readr::cols()) %>% 
                                    dplyr::rename_with(SqlRender::snakeCaseToCamelCase)) %>% 
    dplyr::select(-"uniqueConceptSetId")
  writeToCsv(data = conceptSetsFromCohorts, 
             fileName = file.path(exportFolder, "concept_sets.csv"), 
             incremental = incremental)
  
  if (refreshVocabulary) {
    ParallelLogger::logInfo("Refreshing vocabulary copy.")
    unqiueConceptIds <- CohortDiagnostics::getUniqueConceptIds(exportFolder = exportFolder)
    
    if (length(unqiueConceptIds) > 0) {
      writeOmopvocabularyTables(connectionDetails =  connectionDetails, 
                                vocabularyDatabaseSchema = vocabularyDatabaseSchema, 
                                conceptIds = unqiueConceptIds, 
                                exportFolder = exportFolder)
    }
  }
  
  # Add all to zip file -------------------------------------------------------------------------------
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <- file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at: ", zipName)
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing all diagnostics took",
                                signif(delta, 3),
                                attr(delta, "units")))
}


swapColumnContents <- function(df, column1 = "targetId", column2 = "comparatorId") {
  temp <- df[, column1]
  df[, column1] <- df[, column2]
  df[, column2] <- temp
  return(df)
}


enforceMinCellValue <- function(data, fieldName, minValues, silent = FALSE) {
  toCensor <- !is.na(data[, fieldName]) & data[, fieldName] < minValues & data[, fieldName] != 0
  if (!silent) {
    percent <- round(100 * sum(toCensor)/nrow(data), 1)
    ParallelLogger::logInfo("   censoring ",
                            sum(toCensor),
                            " values (",
                            percent,
                            "%) from ",
                            fieldName,
                            " because value below minimum")
  }
  if (length(minValues) == 1) {
    data[toCensor, fieldName] <- -minValues
  } else {
    data[toCensor, fieldName] <- -minValues[toCensor]
  }
  return(data)
}
