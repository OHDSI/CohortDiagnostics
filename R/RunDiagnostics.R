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
#' @param covariateSettings           Either an object of type \code{covariateSettings} as created using one of
#'                                    the createCovariate functions in the FeatureExtraction package, or a list
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
      }
      writeToCsv(data, file.path(exportFolder, "time_distribution.csv"), incremental = incremental, cohortId = subset$cohortId)
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
      recordTasksDone(cohortId = subset$cohortId,
                      task = "runIncidenceRate",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
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
      }
      writeToCsv(data = data, 
                 fileName = file.path(exportFolder, "cohort_overlap.csv"), 
                 incremental = incremental, 
                 targetCohortId = subset$targetCohortId, 
                 comparatorCohortId = subset$comparatorCohortId)
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
      data <- do.call(rbind, data)
      # Drop covariates with mean = 0 after rounding to 3 digits:
      data <- data[round(data$mean, 3) != 0, ]
      covariates <- unique(data[, c("covariateId", "covariateName", "analysisId")])
      colnames(covariates)[[3]] <- "covariateAnalysisId"
      writeToCsv(covariates, file.path(exportFolder, "covariate.csv"), incremental = incremental, covariateId = covariates$covariateId)
      
      if (!exists("counts")) {
        counts <- readr::read_csv(file = file.path(exportFolder, "cohort_count.csv"), col_types = readr::cols())
        names(counts) <- SqlRender::snakeCaseToCamelCase(names(counts))
      }
      
      data$covariateName <- NULL
      data$analysisId <- NULL
      if (nrow(data) > 0) {
        data$databaseId <- databaseId
        data <- merge(data, counts[, c("cohortId", "cohortEntries")])
        data <- enforceMinCellValue(data, "mean", minCellCount/data$cohortEntries)
        data$sd[data$mean < 0] <- NA
        data$cohortEntries <- NULL
        data$mean <- round(data$mean, 3)
        data$sd <- round(data$sd, 3)
      }
      writeToCsv(data, file.path(exportFolder, "covariate_value.csv"), incremental = incremental, cohortId = subset$cohortId)
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

getConceptSets <- function(cohorts) {
  getConceptSetDetails <- function(conceptSet) {
    return(tibble::tibble(conceptSetId = conceptSet$id,
                          conceptSetName = conceptSet$name,
                          expression = RJSONIO::toJSON(conceptSet$expression[[1]])))
  }
  
  getConceptSetsFromCohortDef <- function(cohort) {
    cohortDefinition <- RJSONIO::fromJSON(cohort$json)
    if (length(cohortDefinition$ConceptSets) == 0) {
      return(NULL)
    }
    conceptSets <- lapply(cohortDefinition$ConceptSets, getConceptSetDetails)
    conceptSets <- do.call(rbind, conceptSets)
    
    sqlParts <- SqlRender::splitSql( gsub("with primary_events.*", "", cohort$sql))
    sqlParts <- sqlParts[-1]
    conceptSetIds <- stringr::str_extract_all(string = sqlParts, 
                                              pattern = stringr::regex(pattern = "SELECT [0-9]+ as codeset_id", 
                                                                       ignore_case = T), 
                                              simplify = TRUE) %>%
                    stringr::str_replace_all(string = .,
                                             pattern = stringr::regex(pattern = "SELECT ([0-9]+) as codeset_id", ignore_case = T), 
                                             replacement = "\\1") %>%
                     utils::type.convert()
    if (any(!(conceptSetIds %in% conceptSets$conceptSetId)) ||
        any(!(conceptSets$conceptSetId %in% conceptSetIds))) {
      stop("Mismatch in concept set IDs between SQL and JSON for cohort ", cohort$cohortFullName)
    }
    conceptSets <- merge(conceptSets, 
                         tibble::tibble(conceptSetId = conceptSetIds,
                                        sql = sqlParts))
    conceptSets$cohortId <- cohort$cohortId
    return(conceptSets)
  }
  conceptSets <- lapply(split(cohorts, 1:nrow(cohorts)), getConceptSetsFromCohortDef)
  conceptSets <- do.call(rbind, conceptSets)
  uniqueConceptSets <- unique(conceptSets$expression)
  uniqueConceptSets <- tibble::tibble(expression = uniqueConceptSets,
                                      uniqueConceptSetId = 1:length(uniqueConceptSets))
  conceptSets <- merge(conceptSets, uniqueConceptSets)
  return(conceptSets)
}

instantiateUniqueConceptSets <- function(cohorts, uniqueConceptSets, connection, cdmDatabaseSchema, oracleTempSchema) {
  ParallelLogger::logInfo("Instantiating concept sets")
  sql <- gsub("with primary_events.*", "", cohorts$sql[1])
  createTempTableSql <- SqlRender::splitSql(sql)[1]
  sql <- sapply(split(uniqueConceptSets, 1:nrow(uniqueConceptSets)), 
                function(x) {
                  sub("SELECT [0-9]+ as codeset_id", sprintf("SELECT %s as codeset_id", x$uniqueConceptSetId), x$sql)
                })
  sql <- paste(c(createTempTableSql, sql), collapse = ";\n")
  sql <- SqlRender::render(sql, vocabulary_database_schema = cdmDatabaseSchema)
  sql <- SqlRender::translate(sql,
                              targetDialect = connection@dbms,
                              oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(connection, sql)
}

runConceptSetDiagnostics <- function(connection, 
                                     oracleTempSchema, 
                                     cdmDatabaseSchema,
                                     databaseId,
                                     cohorts, 
                                     runIncludedSourceConcepts, 
                                     runOrphanConcepts,
                                     exportFolder,
                                     minCellCount,
                                     conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                     conceptCountsTable = "concept_counts",
                                     conceptCountsTableIsTemp = FALSE,
                                     useExternalConceptCountsTable = FALSE,
                                     incremental = FALSE,
                                     recordKeepingFile) {
  startConceptSetDiagnostics <- Sys.time()
  ParallelLogger::logInfo("Starting concept set diagnostics at ", Sys.time())
  
  
  subset <- tibble::tibble()
  if (runIncludedSourceConcepts) {
    subsetIncluded <- subsetToRequiredCohorts(cohorts = cohorts, 
                                              task = "runIncludedSourceConcepts", 
                                              incremental = incremental, 
                                              recordKeepingFile = recordKeepingFile)
    subset <- dplyr::bind_rows(subset, subsetIncluded)
  }
  if (runOrphanConcepts) {
    subsetOrphans <- subsetToRequiredCohorts(cohorts = cohorts, 
                                             task = "runOrphanConcepts", 
                                             incremental = incremental, 
                                             recordKeepingFile = recordKeepingFile)
    subset <- dplyr::bind_rows(subset, subsetOrphans)
  }
  subset <- unique(subset)
  
  if (nrow(subset) == 0) {
    return()
  }
  
  conceptSets <- getConceptSets(subset)
  uniqueConceptSets <- conceptSets[!duplicated(conceptSets$uniqueConceptSetId), ]
  instantiateUniqueConceptSets(cohorts = subset,
                               uniqueConceptSets = uniqueConceptSets,
                               connection = connection,
                               cdmDatabaseSchema = cdmDatabaseSchema,
                               oracleTempSchema = oracleTempSchema)
  
  if (runIncludedSourceConcepts) {
    # Included concepts ------------------------------------------------------------------
    ParallelLogger::logInfo("Fetching included source concepts - Started at ",Sys.time() )
    if (nrow(subsetIncluded) > 0) {
      start <- Sys.time()
      ParallelLogger::logInfo("Counting codes in concept sets")
      if (useExternalConceptCountsTable) {
        sql <- SqlRender::loadRenderTranslateSql("CohortSourceConceptsFromCcTable.sql",
                                                 packageName = "CohortDiagnostics",
                                                 dbms = connection@dbms,
                                                 oracleTempSchema = oracleTempSchema,
                                                 cdm_database_schema = cdmDatabaseSchema,
                                                 concept_counts_database_schema = conceptCountsDatabaseSchema,
                                                 concept_counts_table = conceptCountsTable,
                                                 concept_counts_table_is_temp = conceptCountsTableIsTemp)
        sourceCounts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
        
        sql <- SqlRender::loadRenderTranslateSql("CohortStandardConceptsFromCcTable.sql",
                                                 packageName = "CohortDiagnostics",
                                                 dbms = connection@dbms,
                                                 oracleTempSchema = oracleTempSchema,
                                                 cdm_database_schema = cdmDatabaseSchema,
                                                 concept_counts_database_schema = conceptCountsDatabaseSchema,
                                                 concept_counts_table = conceptCountsTable,
                                                 concept_counts_table_is_temp = conceptCountsTableIsTemp)
        standardCounts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
        
        # To avoid double counting, subtract standard concept counts included in source counts.
        # Note: this can create negative counts, because a source concept can be double counted itself
        # if it maps to more than one standard concept, but it will show correctly in the viewer app, 
        # where the counts will be added back in.
        dupCounts <- aggregate(conceptCount ~ conceptId, sourceCounts, sum)
        colnames(dupCounts)[2] <- "dupCount"
        dupSubjects <- aggregate(conceptSubjects ~ conceptId, sourceCounts, sum)
        colnames(dupSubjects)[2] <- "dupSubjects"
        standardCounts <- merge(standardCounts, dupCounts, all.x = TRUE)
        standardCounts <- merge(standardCounts, dupSubjects, all.x = TRUE)
        standardCounts$dupCount[is.na(standardCounts$dupCount)] <- 0
        standardCounts$dupSubjects[is.na(standardCounts$dupSubjects)] <- 0
        standardCounts$conceptCount <- standardCounts$conceptCount - standardCounts$dupCount
        standardCounts$conceptSubjects <- standardCounts$conceptSubjects - standardCounts$dupSubjects
        standardCounts$dupCount <- NULL
        standardCounts$dupSubjects <- NULL
        
        counts <- dplyr::bind_rows(sourceCounts, standardCounts)
      } else {
        sql <- SqlRender::loadRenderTranslateSql("CohortSourceCodes.sql",
                                                 packageName = "CohortDiagnostics",
                                                 dbms = connection@dbms,
                                                 oracleTempSchema = oracleTempSchema,
                                                 cdm_database_schema = cdmDatabaseSchema,
                                                 by_month = FALSE,
                                                 use_source_values = FALSE)
        counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
      }
      
      colnames(counts)[colnames(counts) == "conceptSetId"] <- "uniqueConceptSetId"
      counts <- merge(conceptSets[, c("cohortId", "conceptSetId", "conceptSetName", "uniqueConceptSetId")], counts)
      counts$uniqueConceptSetId <- NULL
      counts <- counts[order(counts$cohortId,
                             counts$conceptSetId,
                             counts$conceptId,
                             counts$sourceConceptName,
                             counts$sourceVocabularyId), ]
      counts <- counts[counts$cohortId %in% subsetIncluded$cohortId, ]
      if (nrow(counts) > 0) {
        counts$databaseId <- databaseId
        counts <- enforceMinCellValue(counts, "conceptSubjects", minCellCount)
        counts <- enforceMinCellValue(counts, "conceptCount", minCellCount)
      }
      writeToCsv(counts, file.path(exportFolder, "included_source_concept.csv"), incremental = incremental, cohortId = subsetIncluded$cohortId)
      recordTasksDone(cohortId = subsetIncluded$cohortId,
                      task = "runIncludedSourceConcepts",
                      checksum = subsetIncluded$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
      delta <- Sys.time() - start
      ParallelLogger::logInfo(paste("Finding source codes took",
                                    signif(delta, 3),
                                    attr(delta, "units")))
    }
  }
  
  if (runOrphanConcepts) {
    # Orphan concepts ---------------------------------------------------------
    ParallelLogger::logInfo("Finding orphan concepts - started at ", Sys.time())
    if (nrow(subsetOrphans > 0)) {
      start <- Sys.time()
      if (!useExternalConceptCountsTable) {
        createConceptCountsTable(connection = connection,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 oracleTempSchema = oracleTempSchema,
                                 conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                                 conceptCountsTable = conceptCountsTable,
                                 conceptCountsTableIsTemp = conceptCountsTableIsTemp)
      }
      runOrphanConcepts <- function(conceptSet) {
        ParallelLogger::logInfo("- Finding orphan concepts for concept set ", conceptSet$conceptSetName)
        orphanConcepts <- .findOrphanConcepts(connection = connection,
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              oracleTempSchema = oracleTempSchema,
                                              useCodesetTable = TRUE,
                                              codesetId = conceptSet$uniqueConceptSetId,
                                              conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                                              conceptCountsTable = conceptCountsTable,
                                              conceptCountsTableIsTemp = conceptCountsTableIsTemp)
        orphanConcepts$uniqueConceptSetId <- rep(conceptSet$uniqueConceptSetId, nrow(orphanConcepts))
        return(orphanConcepts)
      }
      
      data <- lapply(split(uniqueConceptSets, uniqueConceptSets$uniqueConceptSetId), runOrphanConcepts)
      data <- do.call(rbind, data)
      data <- merge(conceptSets[, c("cohortId", "conceptSetId", "conceptSetName", "uniqueConceptSetId")], data)
      data$uniqueConceptSetId <- NULL
      data$databaseId <- rep(databaseId, nrow(data))
      data <- data[data$cohortId %in% subsetOrphans$cohortId, ]
      if (nrow(data) > 0) {
        data <- enforceMinCellValue(data, "conceptCount", minCellCount)
      }
      writeToCsv(data, file.path(exportFolder, "orphan_concept.csv"), incremental = incremental, cohortId = subsetOrphans$cohortId)
      recordTasksDone(cohortId = subsetOrphans$cohortId,
                      task = "runOrphanConcepts",
                      checksum = subsetOrphans$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
      
      if (!useExternalConceptCountsTable) {
        ParallelLogger::logTrace("Dropping temp concept counts")
        sql <- "TRUNCATE TABLE #concept_counts; DROP TABLE #concept_counts;"
        DatabaseConnector::renderTranslateExecuteSql(connection,
                                                     sql,
                                                     progressBar = FALSE,
                                                     reportOverallTime = FALSE)
      }
      delta <- Sys.time() - start
      ParallelLogger::logInfo(paste("Finding orphan concepts took",
                                    signif(delta, 3),
                                    attr(delta, "units")))
    }
  }
  ParallelLogger::logTrace("Dropping temp concept set tables")
  sql <- "TRUNCATE TABLE #Codesets; DROP TABLE #Codesets;"
  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               sql,
                                               oracleTempSchema = oracleTempSchema,
                                               progressBar = FALSE,
                                               reportOverallTime = FALSE)
  
  delta <- Sys.time() - startConceptSetDiagnostics
  ParallelLogger::logInfo(paste("Running concept set diagnostics",
                                signif(delta, 3),
                                attr(delta, "units")))
}


