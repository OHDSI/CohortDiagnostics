# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of StudyDiagnostics
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

#' Run study diagnostics
#' 
#' @description 
#' Runs the study diagnostics on all (or a subset of) the cohorts instantiated using the \code{ROhdsiWebApi::insertCohortDefinitionSetInPackage}
#' function.
#' 
#' @template Connection
#' 
#' @template CohortTable
#' 
#' @param packageName          The name of the package containing the cohort definitions
#' @param cohortToCreateFile   The location of the cohortToCreate file witing the package.
#' @param inclusionStatisticsFolder  The folder where the inclusion rule statistics are stored. Can be left NULL if 
#'                                   \code{runInclusionStatistics = FALSE}. 
#' @param exportFolder         The folder where the output will be exported to. If this folder does not exist it will be created.
#' @param cohortIds            Optionally, provide a subset of cohort IDs to restrict the diagnostics to.
#' @param runInclusionStatistics    Generate and export statistic on the cohort incusion rules?
#' @param runIncludedSourceConcepts Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts         Generate and export potential orphan concepts?
#' @param runBreakdownIndexEvents   Generate and export the breakdown of index events?
#' 
#'
#' @export
runStudyDiagnostics <- function(packageName,
                                cohortToCreateFile = "settings/CohortsToCreate.csv",
                                connectionDetails = NULL,
                                connection = NULL,
                                cohortDatabaseSchema,
                                cohortTable = "cohort",
                                cohortIds = NULL,
                                inclusionStatisticsFolder = NULL,
                                exportFolder,
                                runInclusionStatistics = TRUE,
                                runIncludedSourceConcepts = TRUE,
                                runOrphanConcepts = TRUE,
                                runBreakdownIndexEvents = TRUE) {
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
  cohorts <- dplyr::rename(cohorts, cohortName = "name", cohortFullName = "fullName")
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
  
  if (runInclusionStatistics) {
    ParallelLogger::logInfo("Fetching inclusion rule statistics")
    runInclusionStatistics <- function(cohortId) {
      ParallelLogger::logDebug("- Fetching inclusion rule statistics for cohort ", cohortId)
      stats <- getInclusionStatisticsFromFiles(cohortId = cohortId,
                                               folder = inclusionStatisticsFolder,
                                               simplify = TRUE)
      if (nrow(stats) > 0) {
        stats$cohortId <- cohortId
      }
      return(stats)
    }
    stats <- lapply(cohorts$cohortId, runInclusionStatistics)
    stats <- do.call(rbind, stats)
    if (nrow(stats) > 0) {
      stats$databaseId <- databaseId
    }
    writeToCsv(stats, file.path(exportFolder, "inclusion_rule_stats.csv"))
  }
  
  if (runIncludedSourceConcepts) {
    ParallelLogger::logInfo("Fetching included source concepts")
    runIncludedSourceConcepts <- function(row) {
      ParallelLogger::logDebug("- Fetching included source concepts for cohort ", cohortId)
      data <- findCohortIncludedSourceConcepts(connection = connection,
                                               cdmDatabaseSchema = cdmDatabaseSchema,
                                               oracleTempSchema = oracleTempSchema,
                                               cohortJson = row$json,
                                               cohortSql = row$sql,
                                               byMonth = FALSE,
                                               useSourceValues = FALSE)
      if (nrow(data) > 0) {
        data$cohortId <- row$cohortId
      }
      return(data)
    }
    data <- lapply(split(cohorts, cohorts$cohortId), runIncludedSourceConcepts)
    data <- do.call(rbind, data)
    if (nrow(data) > 0) {
      data$databaseId <- databaseId
    }
    writeToCsv(data, file.path(exportFolder, "included_source_concept.csv"))
  }
  
  if (runOrphanConcepts) {
    ParallelLogger::logInfo("Finding orphan concepts")
    createConceptCountsTable(connection = connection,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             conceptCountsDatabaseSchema = cohortDatabaseSchema)
    
    runOrphanConcepts <- function(row) {
      ParallelLogger::logDebug("- Finding orphan concepts for cohort ", cohortId)
      data <- findCohortOrphanConcepts(connection = connection,
                                       cdmDatabaseSchema = cdmDatabaseSchema,
                                       oracleTempSchema = oracleTempSchema,
                                       conceptCountsDatabaseSchema = cohortDatabaseSchema,
                                       cohortJson = row$json)
      if (nrow(data) > 0) {
        data$cohortId <- row$cohortId
      }
      return(data)
    }
    data <- lapply(split(cohorts, cohorts$cohortId), runOrphanConcepts)
    data <- do.call(rbind, data)
    if (nrow(data) > 0) {
      data$databaseId <- databaseId
    }
    writeToCsv(data, file.path(exportFolder, "orphan_concept.csv"))
  }
  
  if (runBreakdownIndexEvents) {
    ParallelLogger::logInfo("Breaking down index events")

    runBreakdownIndexEvents <- function(row) {
      ParallelLogger::logDebug("- Breaking down index events for cohort ", cohortId)
      data <- breakDownIndexEvents(connection = connection,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   oracleTempSchema = oracleTempSchema,
                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                   cohortTable = cohortTable,
                                   cohortId = row@cohortId,
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
    }
    writeToCsv(data, file.path(exportFolder, "index_event_breakdown.csv"))
  }
  
}

writeToCsv <- function(data, fileName) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))    
  write.csv(data, fileName, row.names = FALSE)
}
