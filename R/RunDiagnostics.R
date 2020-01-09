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
#' @template CdmDatabaseSchema
#' 
#' @template OracleTempSchema
#' 
#' @template CohortTable
#' 
#' @param packageName          The name of the package containing the cohort definitions
#' @param cohortToCreateFile   The location of the cohortToCreate file within the package.
#' @param inclusionStatisticsFolder  The folder where the inclusion rule statistics are stored. Can be left NULL if 
#'                                   \code{runInclusionStatistics = FALSE}. 
#' @param exportFolder         The folder where the output will be exported to. If this folder does not exist it will be created.
#' @param cohortIds            Optionally, provide a subset of cohort IDs to restrict the diagnostics to.
#' @param databaseId            A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName          The full name of the database.
#' @param databaseDescription   A short description (several sentences) of the database.
#' @param runInclusionStatistics    Generate and export statistic on the cohort incusion rules?
#' @param runIncludedSourceConcepts Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts         Generate and export potential orphan concepts?
#' @param runBreakdownIndexEvents   Generate and export the breakdown of index events?
#' @param runIncidenceProportion    Generate and export the cohort incidence proportions?
#' @param runCohortOverlap          Generate and export the cohort overlap?
#' @param runCohortCharacterization Generate and export the cohort characterization?
#'
#' @export
runStudyDiagnostics <- function(packageName,
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
                                runBreakdownIndexEvents = TRUE,
                                runIncidenceProportion = TRUE,
                                runCohortOverlap = TRUE,
                                runCohortCharacterization = TRUE) {
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
  
  ParallelLogger::logInfo("Saving database metadata")
  database <- data.frame(databaseId = databaseId,
                         databaseName = databaseName,
                         description = databaseDescription,
                         isMetaAnalysis = 0)
  writeToCsv(database, file.path(exportFolder, "database.csv"))
  
  if (runInclusionStatistics) {
    ParallelLogger::logInfo("Fetching inclusion rule statistics")
    runInclusionStatistics <- function(row) {
      ParallelLogger::logInfo("- Fetching inclusion rule statistics for cohort ",  row$cohortName)
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
    }
    writeToCsv(stats, file.path(exportFolder, "inclusion_rule_stats.csv"))
  }
  
  if (runIncludedSourceConcepts) {
    ParallelLogger::logInfo("Fetching included source concepts")
    runIncludedSourceConcepts <- function(row) {
      ParallelLogger::logInfo("- Fetching included source concepts for cohort ", row$cohortName)
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
      ParallelLogger::logInfo("- Finding orphan concepts for cohort ", row$cohortName)
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
    }
    writeToCsv(data, file.path(exportFolder, "index_event_breakdown.csv"))
  }
  
  if (runIncidenceProportion) {
    ParallelLogger::logInfo("Breaking down index events")
    
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
    }
    writeToCsv(data, file.path(exportFolder, "incidence_proportion.csv"))
  }

  if (runCohortOverlap) {
    ParallelLogger::logInfo("Computing cohort overlap")
    combis <- expand.grid(targetCohortId = cohorts$cohortId, comparatorCohortId = cohorts$cohortId)
    combis <- combis[combis$targetCohortId < combis$comparatorCohortId, ]
    
    runCohortOverlap <- function(row) {
      ParallelLogger::logInfo("- Computing overlap for cohorts ", row$targetCohortId, " and ", row$comparatorCohortId)
      data <- computeCohortOverlap(connection = connection,
                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                   cohortTable = cohortTable,
                                   targetCohortId = row$targetCohortId,
                                   comparatorCohortId = row$comparatorCohortId)
      if (nrow(data) > 0) {
        data$targetCohortId = row$targetCohortId
        data$comparatorCohortId = row$comparatorCohortId
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
      data <- rbind(data, revData)
      data$databaseId <- databaseId
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
    covariates <- unique(data[, c("covariateId", "covariateName", "analysisId")])
    colnames(covariates)[[3]] <- "covariateAnalysisId"
    writeToCsv(covariates, file.path(exportFolder, "covariates.csv"))
    data$covariateName <- NULL
    data$analysisId <- NULL
    if (nrow(data) > 0) {
      data$databaseId <- databaseId
    }
    writeToCsv(data, file.path(exportFolder, "covariate_value.csv"))
  }
  
  # Add all to zip file -------------------------------------------------------------------------------
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <- file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd))
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at:", zipName)
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing all diagnostics took", signif(delta, 3), attr(delta, "units")))
}

writeToCsv <- function(data, fileName) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))    
  write.csv(data, fileName, row.names = FALSE)
}

swapColumnContents <- function(df, column1 = "targetId", column2 = "comparatorId") {
  temp <- df[, column1]
  df[, column1] <- df[, column2]
  df[, column2] <- temp
  return(df)
}
