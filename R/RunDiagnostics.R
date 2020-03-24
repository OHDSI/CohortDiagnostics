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
                                 minCellCount = 5,
                                 incremental = FALSE,
                                 incrementalFolder = exportFolder) {
  start <- Sys.time()
  if (!file.exists(exportFolder)) {
    dir.create(exportFolder, recursive = TRUE)
  }
  
  if (incremental) {
    if (is.null(incrementalFolder)) {
      stop("Must specify incrementalFolder when incremental = TRUE")
    }
    if (!file.exists(incrementalFolder)) {
      dir.create(incrementalFolder, recursive = TRUE)
    }
  }
  
  if ((runTimeDistributions || runCohortCharacterization) && !is.null(getOption("fftempdir")) && !file.exists(getOption("fftempdir"))) {
    warning("fftempdir '", getOption("fftempdir"), "' not found. Attempting to create folder")
    dir.create(getOption("fftempdir"), recursive = TRUE)
  }
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (is.null(packageName)) {
    cohorts <- loadCohortsFromWebApi(baseUrl = baseUrl,
                                     cohortSetReference = cohortSetReference,
                                     cohortIds = cohortIds)
  } else {
    cohorts <- loadCohortsFromPackage(packageName = packageName,
                                      cohortToCreateFile = cohortToCreateFile,
                                      cohortIds = cohortIds)
  }
  
  writeToCsv(cohorts, file.path(exportFolder, "cohort.csv"))
  
  if (incremental) {
    cohorts$checksum <- computeChecksum(cohorts$sql)
    recordKeepingFile <- file.path(incrementalFolder, "CreatedDiagnostics.csv")
  }
  
  ParallelLogger::logInfo("Saving database metadata")
  database <- data.frame(databaseId = databaseId,
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
    ParallelLogger::logInfo("Fetching inclusion rule statistics")
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
  }
  
  if (runBreakdownIndexEvents) {
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
  }
  
  if (runIncidenceRate) {
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
  }
  
  if (runCohortOverlap) {
    # Cohort overlap ---------------------------------------------------------------------------------
    ParallelLogger::logInfo("Computing cohort overlap")
    combis <- expand.grid(targetCohortId = cohorts$cohortId, comparatorCohortId = cohorts$cohortId)
    combis <- combis[combis$targetCohortId < combis$comparatorCohortId, ]
    if (incremental) {
      combis <- merge(combis, data.frame(targetCohortId = cohorts$cohortId, targetChecksum = cohorts$checksum))
      combis <- merge(combis, data.frame(comparatorCohortId = cohorts$cohortId, comparatorChecksum = cohorts$checksum))
      combis$checksum <- paste(combis$targetChecksum, combis$comparatorChecksum)
    }
    subset <- subsetToRequiredCombis(combis = combis, 
                                     task = "runCohortOverlap", 
                                     incremental = incremental, 
                                     recordKeepingFile = recordKeepingFile)
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
    if (nrow(subset) == 0) {
      data <- data.frame()
    } else {
      data <- lapply(split(subset, 1:nrow(subset)), runCohortOverlap)
      data <- do.call(rbind, data)
    }
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
  
  if (runCohortCharacterization) {
    # Cohort characterization ---------------------------------------------------------------
    ParallelLogger::logInfo("Creating cohort characterizations")
    subset <- subsetToRequiredCohorts(cohorts = cohorts, 
                                      task = "runCohortCharacterization", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      runCohortCharacterization <- function(row) {
        ParallelLogger::logInfo("- Creating characterization for cohort ", row$cohortName)
        data <- getCohortCharacteristics(connection = connection,
                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                         cohortDatabaseSchema = cohortDatabaseSchema,
                                         cohortTable = cohortTable,
                                         cohortId = row$cohortId)
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
  }
  
  # Add all to zip file -------------------------------------------------------------------------------
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <- file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at:", zipName)
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing all diagnostics took",
                                signif(delta, 3),
                                attr(delta, "units")))
}

writeToCsv <- function(data, fileName, incremental = FALSE, ...) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  if (incremental) {
    params <- list(...)
    names(params) <- SqlRender::camelCaseToSnakeCase(names(params))
    params$data = data
    params$fileName = fileName
    do.call(saveIncremental, params)
  } else {
    readr::write_csv(data, fileName)
  }
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

getUniqueConceptSets <- function(cohorts) {
  getConceptSetDetails <- function(conceptSet) {
    return(tibble::tibble(conceptSetId = conceptSet$id,
                          conceptSetName = conceptSet$name,
                          expression = RJSONIO::toJSON(conceptSet$expression[[1]])))
  }
  
  getConceptSetsFromCohortDef <- function(cohort) {
    cohortDefinition <- RJSONIO::fromJSON(cohort$json)
    conceptSets <- lapply(cohortDefinition$ConceptSets, getConceptSetDetails)
    conceptSets <- do.call(rbind, conceptSets)
    sqlParts <- SqlRender::splitSql( gsub("with primary_events.*", "", cohort$sql))
    sqlParts <- sqlParts[-1]
    conceptSetIds <- as.numeric(gsub("^.*SELECT ([0-9]+) as codeset_id.*$", "\\1", sqlParts, ignore.case = TRUE))
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
  uniqueConceptSets <- conceptSets[!duplicated(conceptSets$uniqueConceptSetId), ]
  return(uniqueConceptSets)
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
  
  uniqueConceptSets <- getUniqueConceptSets(cohorts)
  instantiateUniqueConceptSets(cohorts = cohorts,
                               uniqueConceptSets = uniqueConceptSets,
                               connection = connection,
                               cdmDatabaseSchema = cdmDatabaseSchema,
                               oracleTempSchema = oracleTempSchema)
  
  if (runIncludedSourceConcepts) {
    # Included concepts ------------------------------------------------------------------
    ParallelLogger::logInfo("Fetching included source concepts")
    if (nrow(subsetIncluded > 0)) {
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
      counts <- merge(uniqueConceptSets[, c("cohortId", "conceptSetId", "conceptSetName", "uniqueConceptSetId")], counts)
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
    ParallelLogger::logInfo("Finding orphan concepts")
    if (nrow(subsetOrphans > 0)) {
      start <- Sys.time()
      if (!useExternalConceptCountsTable) {
        createConceptCountsTable(connection = connection,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
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
      data <- merge(uniqueConceptSets[, c("cohortId", "conceptSetId", "conceptSetName", "uniqueConceptSetId")], data)
      data$uniqueConceptSetId <- NULL
      data$databaseId <- rep(databaseId, nrow(data))
      data <- data[data$cohortId %in% subsetOrphans$cohortId, ]
      if (nrow(data) > 0) {
        data <- enforceMinCellValue(data, "conceptCount", minCellCount)
      }
      writeToCsv(data, file.path(exportFolder, "orphan_concept.csv"), incremental = incremental, cohortId = subsetOrphans$cohortId)
      recordTasksDone(cohortId = subsetOrphans$cohortId,
                      task = "runIncludedSourceConcepts",
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
                                               progressBar = FALSE,
                                               reportOverallTime = FALSE)
}

subsetToRequiredCohorts <- function(cohorts, task, incremental, recordKeepingFile) {
  if (incremental) {
    tasks <- getRequiredTasks(cohortId = cohorts$cohortId,
                              task = task,
                              checksum = cohorts$checksum,
                              recordKeepingFile = recordKeepingFile)
    return(cohorts[cohorts$cohortId %in% tasks$cohortId, ])
  } else {
    return(cohorts)
  }
}

subsetToRequiredCombis <- function(combis, task, incremental, recordKeepingFile) {
  if (incremental) {
    tasks <- getRequiredTasks(cohortId = combis$targetCohortId,
                              comparatorId = combis$comparatorCohortId,
                              task = task,
                              checksum = combis$checksum,
                              recordKeepingFile = recordKeepingFile)
    return(merge(combis, tasks))
  } else {
    return(combis)
  }
}

loadCohortsFromPackage <- function(packageName, cohortToCreateFile, cohortIds) {
  pathToCsv <- system.file(cohortToCreateFile, package = packageName)
  cohorts <- readr::read_csv(pathToCsv, col_types = readr::cols())
  cohorts$atlasId <- NULL
  if (!is.null(cohortIds)) {
    cohorts <- cohorts[cohorts$cohortId %in% cohortIds, ]
  }
  if ("atlasName" %in% colnames(cohorts)) {
    cohorts <- dplyr::rename(cohorts, cohortName = "name", cohortFullName = "atlasName")
  } else {
    cohorts <- dplyr::rename(cohorts, cohortName = "name", cohortFullName = "fullName")
  }
  
  getSql <- function(name) {
    pathToSql <- system.file("sql", "sql_server", paste0(name, ".sql"), package = packageName)
    sql <- readChar(pathToSql, file.info(pathToSql)$size)
    return(sql)
  }
  cohorts$sql <- sapply(cohorts$cohortName, getSql)
  getJson <- function(name) {
    pathToJson <- system.file("cohorts", paste0(name, ".json"), package = packageName)
    json <- readChar(pathToJson, file.info(pathToJson)$size)
    return(json)
  }
  cohorts$json <- sapply(cohorts$cohortName, getJson)
  return(cohorts)
}

loadCohortsFromWebApi <- function(baseUrl,
                                  cohortSetReference,
                                  cohortIds = NULL,
                                  generateStats = TRUE) {
  cohorts <- cohortSetReference
  if (!is.null(cohortIds)) {
    cohorts <- cohorts[cohorts$cohortId %in% cohortIds, ]
  }
  cohorts <- dplyr::rename(cohorts, cohortName = "name", cohortFullName = "atlasName")
  ParallelLogger::logInfo("Retrieving cohort definitions from WebAPI")
  for (i in 1:nrow(cohorts)) {
    ParallelLogger::logInfo("- Retrieving definitions for cohort ", cohorts$cohortFullName[i])
    cohortExpression <-  ROhdsiWebApi::getCohortDefinitionExpression(definitionId = cohorts$atlasId[i],
                                                                     baseUrl = baseUrl)
    cohorts$json[i] <- cohortExpression$expression
    cohorts$sql[i] <- ROhdsiWebApi::getCohortDefinitionSql(definitionId = cohorts$atlasId[i],
                                                           baseUrl = baseUrl,
                                                           generateStats = generateStats)
  }
  cohorts$atlasId <- NULL
  return(cohorts)
}
