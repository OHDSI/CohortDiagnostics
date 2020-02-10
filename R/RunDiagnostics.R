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
#' \code{ROhdsiWebApi::insertCohortDefinitionSetInPackage} function.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template OracleTempSchema
#'
#' @template CohortTable
#'
#' @param packageName                 The name of the package containing the cohort definitions
#' @param cohortToCreateFile          The location of the cohortToCreate file within the package.
#' @param inclusionStatisticsFolder   The folder where the inclusion rule statistics are stored. Can be
#'                                    left NULL if \code{runInclusionStatistics = FALSE}.
#' @param exportFolder                The folder where the output will be exported to. If this folder
#'                                    does not exist it will be created.
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    diagnostics to.
#' @param databaseId                  A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName                The full name of the database.
#' @param databaseDescription         A short description (several sentences) of the database.
#' @param runInclusionStatistics      Generate and export statistic on the cohort incusion rules?
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runTimeDistributions        Generate and export cohort time distributions?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#' @param runIncidenceProportion      Generate and export the cohort incidence proportions?
#' @param runCohortOverlap            Generate and export the cohort overlap?
#' @param runCohortCharacterization   Generate and export the cohort characterization?
#' @param minCellCount                The minimum cell count for fields contains person counts or fractions.
#'
#' @export
runCohortDiagnostics <- function(packageName,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
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
                                 databaseName,
                                 databaseDescription,
                                 runInclusionStatistics = TRUE,
                                 runIncludedSourceConcepts = TRUE,
                                 runOrphanConcepts = TRUE,
                                 runTimeDistributions = TRUE,
                                 runBreakdownIndexEvents = TRUE,
                                 runIncidenceProportion = TRUE,
                                 runCohortOverlap = TRUE,
                                 runCohortCharacterization = TRUE,
                                 minCellCount = 5) {
  start <- Sys.time()
  if (!file.exists(exportFolder)) {
    dir.create(exportFolder)
  }
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  # Load created cohorts
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
  writeToCsv(cohorts, file.path(exportFolder, "cohort.csv"))
  
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
  
  ParallelLogger::logInfo("Saving database metadata")
  database <- data.frame(databaseId = databaseId,
                         databaseName = databaseName,
                         description = databaseDescription,
                         isMetaAnalysis = 0)
  writeToCsv(database, file.path(exportFolder, "database.csv"))
  
  ParallelLogger::logInfo("Counting cohorts")
  counts <- getCohortCounts(connection = connection,
                            cohortDatabaseSchema = cohortDatabaseSchema,
                            cohortTable = cohortTable,
                            cohortIds = cohorts$cohortId)
  if (nrow(counts) > 0) {
    counts$databaseId <- databaseId
    counts <- enforceMinCellValue(counts, "cohortEntries", minCellCount)
    counts <- enforceMinCellValue(counts, "cohortSubjects", minCellCount)
  }
  writeToCsv(counts, file.path(exportFolder, "cohort_count.csv"))
  
  if (runInclusionStatistics) {
    ParallelLogger::logInfo("Fetching inclusion rule statistics")
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
    stats <- lapply(split(cohorts, cohorts$cohortId), runInclusionStatistics)
    stats <- do.call(rbind, stats)
    if (nrow(stats) > 0) {
      stats$databaseId <- databaseId
      stats <- enforceMinCellValue(stats, "meetSubjects", minCellCount)
      stats <- enforceMinCellValue(stats, "gainSubjects", minCellCount)
      stats <- enforceMinCellValue(stats, "totalSubjects", minCellCount)
      stats <- enforceMinCellValue(stats, "remainSubjects", minCellCount)
    }
    writeToCsv(stats, file.path(exportFolder, "inclusion_rule_stats.csv"))
  }
  
  if (runIncludedSourceConcepts || runOrphanConcepts) {
    runConceptSetDiagnostics(connection = connection,
                             oracleTempSchema = oracleTempSchema,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             cohortDatabaseSchema = cohortDatabaseSchema,
                             cohorts = cohorts,
                             runIncludedSourceConcepts = runIncludedSourceConcepts,
                             runOrphanConcepts = runOrphanConcepts,
                             exportFolder = exportFolder,
                             minCellCount = minCellCount)
  }
  
  if (runTimeDistributions) {
    ParallelLogger::logInfo("Creating time distributions")

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
    data <- lapply(split(cohorts, cohorts$cohortId), runTimeDist)
    data <- do.call(rbind, data)
    if (nrow(data) > 0) {
      data$databaseId <- databaseId
    }
    writeToCsv(data, file.path(exportFolder, "time_distribution.csv"))
  }
  
  if (runBreakdownIndexEvents) {
    ParallelLogger::logInfo("Breaking down index events")
    
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
    data <- lapply(split(cohorts, cohorts$cohortId), runBreakdownIndexEvents)
    data <- do.call(rbind, data)
    if (nrow(data) > 0) {
      data$databaseId <- databaseId
      data <- enforceMinCellValue(data, "conceptCount", minCellCount)
    }
    writeToCsv(data, file.path(exportFolder, "index_event_breakdown.csv"))
  }
  
  if (runIncidenceProportion) {
    ParallelLogger::logInfo("Computing incidence proportion")
    
    runIncidenceProportion <- function(row) {
      ParallelLogger::logInfo("- Computing incidence proportion for cohort ", row$cohortName)
      data <- getIncidenceProportion(connection = connection,
                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                     cohortTable = cohortTable,
                                     cohortId = row$cohortId)
      if (nrow(data) > 0) {
        data$cohortId <- row$cohortId
      }
      return(data)
    }
    data <- lapply(split(cohorts, cohorts$cohortId), runIncidenceProportion)
    data <- do.call(rbind, data)
    if (nrow(data) > 0) {
      data$databaseId <- databaseId
      data <- enforceMinCellValue(data, "cohortSubjects", minCellCount)
      data <- enforceMinCellValue(data, "backgroundSubjects", minCellCount)
      data <- enforceMinCellValue(data, "incidenceProportion", 1000*minCellCount/data$backgroundSubjects)
      
    }
    writeToCsv(data, file.path(exportFolder, "incidence_proportion.csv"))
  }
  
  if (runCohortOverlap) {
    ParallelLogger::logInfo("Computing cohort overlap")
    combis <- expand.grid(targetCohortId = cohorts$cohortId, comparatorCohortId = cohorts$cohortId)
    combis <- combis[combis$targetCohortId < combis$comparatorCohortId, ]
    
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
    data <- lapply(split(combis, 1:nrow(combis)), runCohortOverlap)
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
    writeToCsv(data, file.path(exportFolder, "cohort_overlap.csv"))
  }
  
  if (runCohortCharacterization) {
    ParallelLogger::logInfo("Creating cohort characterizations")
    
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
    data <- lapply(split(cohorts, cohorts$cohortId), runCohortCharacterization)
    data <- do.call(rbind, data)
    # Drop covariates with mean = 0 after rounding to 3 digits:
    data <- data[round(data$mean, 3) != 0, ]
    covariates <- unique(data[, c("covariateId", "covariateName", "analysisId")])
    colnames(covariates)[[3]] <- "covariateAnalysisId"
    writeToCsv(covariates, file.path(exportFolder, "covariate.csv"))
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
    writeToCsv(data, file.path(exportFolder, "covariate_value.csv"))
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

writeToCsv <- function(data, fileName) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  readr::write_csv(data, fileName)
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

runConceptSetDiagnostics <- function(connection, 
                                     oracleTempSchema, 
                                     cdmDatabaseSchema,
                                     cohortDatabaseSchema,
                                     cohorts, 
                                     runIncludedSourceConcepts, 
                                     runOrphanConcepts,
                                     exportFolder,
                                     minCellCount) {
  # Find all unique concept sets:
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
  
  if (runIncludedSourceConcepts) {
    ParallelLogger::logInfo("Fetching included source concepts")
    start <- Sys.time()
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
    
    ParallelLogger::logInfo("Counting codes in concept sets")
    sql <- SqlRender::loadRenderTranslateSql("CohortSourceCodes.sql",
                                             packageName = "CohortDiagnostics",
                                             dbms = connection@dbms,
                                             oracleTempSchema = oracleTempSchema,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             by_month = FALSE,
                                             use_source_values = FALSE)
    counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
    colnames(counts)[colnames(counts) == "conceptSetId"] <- "uniqueConceptSetId"
    counts <- merge(conceptSets[, c("cohortId", "conceptSetId", "conceptSetName", "uniqueConceptSetId")], counts)
    counts$uniqueConceptSetId <- NULL
    counts <- counts[order(counts$cohortId,
                           counts$conceptSetId,
                           counts$conceptId,
                           counts$sourceConceptName,
                           counts$sourceVocabularyId), ]
    if (nrow(counts) > 0) {
      counts$databaseId <- databaseId
      counts <- enforceMinCellValue(counts, "conceptSubjects", minCellCount)
    }
    writeToCsv(counts, file.path(exportFolder, "included_source_concept.csv"))
    
    
    sql <- "TRUNCATE TABLE #Codesets; DROP TABLE #Codesets;"
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 progressBar = FALSE,
                                                 reportOverallTime = FALSE)
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Finding source codes took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  }
  
  if (runOrphanConcepts) {
    ParallelLogger::logInfo("Finding orphan concepts")
    start <- Sys.time()
    createConceptCountsTable(connection = connection,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             conceptCountsDatabaseSchema = cohortDatabaseSchema)
    
    getConceptIdFromItem <- function(item) {
      if (item$isExcluded) {
        return(NULL)
      } else {
        return(item$concept$CONCEPT_ID)
      }
    }
    
    runOrphanConcepts <- function(conceptSet) {
      ParallelLogger::logInfo("- Finding orphan concepts for concept set ", conceptSet$conceptSetName)
      conceptIds <- lapply(RJSONIO::fromJSON(conceptSet$expression), getConceptIdFromItem)
      conceptIds <- do.call(c, conceptIds)
      orphanConcepts <- findOrphanConcepts(connection = connection,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           oracleTempSchema = oracleTempSchema,
                                           conceptIds = conceptIds,
                                           conceptCountsDatabaseSchema = cohortDatabaseSchema)
      if (nrow(orphanConcepts) > 0) {
        orphanConcepts$uniqueConceptSetId <- conceptSet$uniqueConceptSetId
      }
      return(orphanConcepts)
    }
    
    data <- lapply(split(uniqueConceptSets, uniqueConceptSets$uniqueConceptSetId), runOrphanConcepts)
    data <- do.call(rbind, data)
    if (nrow(data) > 0) {
      data <- merge(conceptSets[, c("cohortId", "conceptSetId", "conceptSetName", "uniqueConceptSetId")], data)
      data$uniqueConceptSetId <- NULL
      data$databaseId <- databaseId
      data <- enforceMinCellValue(data, "conceptCount", minCellCount)
    }
    writeToCsv(data, file.path(exportFolder, "orphan_concept.csv"))
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Finding orphan concepts took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
  }
}
