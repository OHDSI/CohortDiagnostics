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
#'                                    Only records with values greater than 0.001 are returned.
#' @param covariateSettings           Either an object of type \code{covariateSettings} as created using one of
#'                                    the createCovariateSettings function in the FeatureExtraction package, or a list
#'                                    of such objects.
#' @param runTemporalCohortCharacterization   Generate and export the temporal cohort characterization?
#'                                    Only records with values greater than 0.001 are returned.
#' @param temporalCovariateSettings   Either an object of type \code{covariateSettings} as created using one of
#'                                    the createTemporalCovariateSettings function in the FeatureExtraction package, or a list
#'                                    of such objects.
#' @param minCellCount                The minimum cell count for fields contains person counts or fractions.
#' @param incremental                 Create only cohort diagnostics that haven't been created before?
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are kept
#'                                    of which cohort diagnostics has been executed.
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
                                 covariateSettings = FeatureExtraction::createDefaultCovariateSettings(),
                                 runTemporalCohortCharacterization = TRUE,
                                 temporalCovariateSettings = FeatureExtraction::createTemporalCovariateSettings(useConditionOccurrence = TRUE,
                                                                                                                useDrugEraStart = TRUE,
                                                                                                                useProcedureOccurrence = TRUE,
                                                                                                                useMeasurement = TRUE,
                                                                                                                temporalStartDays = c(-365,-30,0,1,31),
                                                                                                                temporalEndDays = c(-31,-1,0,30,365)),
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
        
        if (nrow(data) > 0) {
          data$cohortId <- row$cohortId
        }
        return(data)
      }
      data <- lapply(split(subset, subset$cohortId), runCohortCharacterization)
      data <- dplyr::bind_rows(data)
      
      dataFiltered <- data %>% 
        dplyr::mutate(mean = round(x = mean, digits = 3)) %>% 
        dplyr::filter(mean != 0) # Drop covariates with mean = 0 after rounding to 3 digits
      
      counts <- data %>% 
        dplyr::group_by(.data$cohortId) %>% 
        dplyr::summarise(n = dplyr::n())
      
      countsFiltered <- dataFiltered %>% 
        dplyr::group_by(.data$cohortId) %>% 
        dplyr::summarise(n = dplyr::n())
      
      ParallelLogger::logInfo(paste0("Characterization was run for cohort ids ", 
                                     paste0(subset$cohortId, collapse = ","),
                                     ". Of these ",
                                     nrow(x = counts),
                                     " returned characterization results, with ",
                                     nrow(x = countsFiltered),
                                     " cohorts having atleast one covariate with mean > 0.001"))
                              
      if (nrow(x = countsFiltered) == 0 || sort(dataFiltered$cohortId) != sort(subset$cohortId)) {
        message <- cohorts %>% 
          dplyr::filter(cohortId %in% 
                          setdiff(x = subset$cohortId, y = countsFiltered$cohortId)) %>% 
          dplyr::mutate(cohorts = paste0(.data$cohortFullName, " (", .data$cohortId, ")")) %>% 
          dplyr::pull(.data$cohorts) %>% 
          paste0(collapse = ",\n")
        ParallelLogger::logWarn(paste0("Characterization results not captured for \n", 
                                       message,
                                       " cohorts."))
      }
                              
      if (nrow(dataFiltered) > 0) {
        data <- dataFiltered
        covariates <- data %>% 
          dplyr::select(.data$covariateId, .data$covariateName, .data$analysisId, .data$conceptId) %>% 
          dplyr::rename(covariateAnalysisId = .data$analysisId)
        writeToCsv(
          data = covariates,
          fileName = file.path(exportFolder, "covariate.csv"),
          incremental = incremental,
          covariateId = covariates$covariateId
        )
        
        if (!exists("counts")) {
          counts <- readr::read_csv(file = file.path(exportFolder, "cohort_count.csv"), col_types = readr::cols())
          names(counts) <- SqlRender::snakeCaseToCamelCase(names(counts))
        }
        
        data <- data %>% 
          dplyr::select(-.data$covariateName, -.data$analysisId)
        
        if (nrow(data) > 0) {
          data <- data  %>% 
            dplyr::mutate(databaseId = !!databaseId) %>% 
            dplyr::left_join(y = counts)
          data <- enforceMinCellValue(data, "mean", minCellCount/data$cohortEntries)
          data <- data %>% 
            dplyr::mutate(sd = dplyr::case_when(mean >= 0 ~ sd)) %>% 
            dplyr::mutate(mean = round(.data$mean, digits = 3),
                          sd = round(.data$sd, digits = 3)) %>% 
            dplyr::select(-.data$cohortEntries, -.data$cohortSubjects)
        }
        writeToCsv(data, file.path(exportFolder, "covariate_value.csv"), incremental = incremental, cohortId = subset$cohortId)
      }
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
          temporalCovariateSettings <- FeatureExtraction::createTemporalCovariateSettings(useConditionOccurrence = TRUE,
                                                                                          useConditionEraStart = TRUE,
                                                                                          useDrugEraStart = TRUE,
                                                                                          useProcedureOccurrence = TRUE,
                                                                                          useMeasurement = TRUE,
                                                                                          useObservation = TRUE,
                                                                                          temporalStartDays = c(-365,-30,0,1,31),
                                                                                          temporalEndDays = c(-31,-1,0,30,365))
        }
        
        data <- getCohortCharacteristics(connection = connection,
                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                         oracleTempSchema = oracleTempSchema,
                                         cohortDatabaseSchema = cohortDatabaseSchema,
                                         cohortTable = cohortTable,
                                         cohortId = row$cohortId,
                                         covariateSettings = temporalCovariateSettings)
        
        if (nrow(data) > 0) {
          data$cohortId <- row$cohortId
        }
        return(data)
      }
      data <- lapply(split(subset, subset$cohortId), runTemporalCohortCharacterization)
      data <- dplyr::bind_rows(data)
      
      dataFiltered <- data %>% 
        dplyr::mutate(mean = round(x = mean, digits = 3)) %>% 
        dplyr::filter(mean != 0) # Drop covariates with mean = 0 after rounding to 3 digits
      
      counts <- data %>% 
        dplyr::group_by(.data$cohortId) %>% 
        dplyr::summarise(n = dplyr::n())
      
      countsFiltered <- dataFiltered %>% 
        dplyr::group_by(.data$cohortId) %>% 
        dplyr::summarise(n = dplyr::n())
      
      ParallelLogger::logInfo(paste0("Temporal characterization was run for cohort ids ", 
                                     paste0(subset$cohortId, collapse = ","),
                                     ". Of these ",
                                     nrow(x = counts),
                                     " returned temporal characterization results, with ",
                                     nrow(x = countsFiltered),
                                     " cohorts having atleast one covariate with mean > 0.001"))
      
      if (nrow(x = countsFiltered) == 0 || sort(dataFiltered$cohortId) != sort(subset$cohortId)) {
        message <- cohorts %>% 
          dplyr::filter(cohortId %in% 
                          setdiff(x = subset$cohortId, y = countsFiltered$cohortId)) %>% 
          dplyr::mutate(cohorts = paste0(.data$cohortFullName, " (", .data$cohortId, ")")) %>% 
          dplyr::pull(.data$cohorts) %>% 
          paste0(collapse = ",\n")
        ParallelLogger::logWarn(paste0("Temporal characterization results not captured for \n", 
                                       message,
                                       " cohorts."))
      }
      

      if (nrow(dataFiltered) > 0) {
        data <- dataFiltered
        
        temporalCovariates <- data %>% 
          dplyr::select(.data$covariateId, .data$covariateName, .data$analysisId, .data$conceptId, .data$timeId, 
                        .data$startDayTemporalCharacterization, .data$endDayTemporalCharacterization) %>% 
          dplyr::rename(covariateAnalysisId = .data$analysisId) %>% 
          dplyr::distinct()
        writeToCsv(
          data = temporalCovariates,
          fileName = file.path(exportFolder, "temporal_covariate.csv"),
          incremental = incremental,
          covariateId = temporalCovariates$covariateId
        )
        
        if (!exists("counts")) {
          counts <- readr::read_csv(file = file.path(exportFolder, "cohort_count.csv"), col_types = readr::cols())
          names(counts) <- SqlRender::snakeCaseToCamelCase(names(counts))
        }
        
        data <- data %>% 
          dplyr::select(-.data$covariateName, 
                        -.data$analysisId, 
                        -.data$startDayTemporalCharacterization, 
                        -.data$endDayTemporalCharacterization) %>% 
          dplyr::mutate(databaseId = databaseId) %>% 
          dplyr::left_join(y = counts, by = c("cohortId", "databaseId"))
        data <- enforceMinCellValue(data, "mean", minCellCount/data$cohortEntries)
        data <- data %>% 
          dplyr::mutate(sd = dplyr::case_when(mean >= 0 ~ sd)) %>% 
          dplyr::mutate(mean = round(.data$mean, digits = 3),
                        sd = round(.data$sd, digits = 3)) %>% 
          dplyr::select(-.data$cohortEntries, -.data$cohortSubjects)
        writeToCsv(data, file.path(exportFolder, "temporal_covariate_value.csv"), incremental = incremental, cohortId = subset$cohortId)
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
