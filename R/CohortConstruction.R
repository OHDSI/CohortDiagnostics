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

#' Create cohort table(s)
#' 
#' @description 
#' This function creates an empty cohort table. Optionally, additional empty tables are created 
#' to store statistics on the various inclusion criteria.
#' 
#' @template Connection
#' 
#' @template CohortTable
#' 
#' @param createInclusionStatsTables Create the four additional tables for storing inclusion rule statistics?
#' @param resultsDatabaseSchema      Schema name where the statistics tables reside.
#'                                   Note that for SQL Server, this should include both the database and
#'                                   schema name, for example 'scratch.dbo'.
#' @param cohortInclusionTable       Name of the inclusion table, one of the tables for storing inclusion rule statistics.
#' @param cohortInclusionResultTable Name of the inclusion result table, one of the tables for storing inclusion rule statistics.
#' @param cohortInclusionStatsTable  Name of the inclusion stats table, one of the tables for storing inclusion rule statistics.
#' @param cohortSummaryStatsTable    Name of the summary stats table, one of the tables for storing inclusion rule statistics.
#'
#' @export
createCohortTable <- function(connectionDetails = NULL,
                              connection = NULL,
                              cohortDatabaseSchema,
                              cohortTable = "cohort",
                              createInclusionStatsTables = FALSE,
                              resultsDatabaseSchema = cohortDatabaseSchema,
                              cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                              cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                              cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                              cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats"))  {
  start <- Sys.time()
  ParallelLogger::logInfo("Creating cohort table")
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails) 
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql("CreateCohortTable.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connectionDetails$dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortTable)
  
  if (createInclusionStatsTables) {
    ParallelLogger::logInfo("Creating inclusion rule statistics tables")
    sql <- SqlRender::loadRenderTranslateSql("CreateInclusionStatsTables.sql",
                                             packageName = "StudyDiagnostics",
                                             dbms = connectionDetails$dbms,
                                             cohort_database_schema = resultsDatabaseSchema,
                                             cohort_inclusion_table = cohortInclusionTable,
                                             cohort_inclusion_result_table = cohortInclusionResultTable,
                                             cohort_inclusion_stats_table = cohortInclusionStatsTable,
                                             cohort_summary_stats_table = cohortSummaryStatsTable)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortInclusionTable)
    ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortInclusionResultTable)
    ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortInclusionStatsTable)
    ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortSummaryStatsTable)
  }
  delta <- Sys.time() - start
  writeLines(paste("Creating cohort table took", signif(delta, 3), attr(delta, "units")))
}



#' Instantiate a cohort
#' 
#' @description 
#' This function instantiates the cohort in the cohort table. Optionally, the inclusion rule statistics are computed and stored in
#' the inclusion rule statistics tables described in \code{\link{createCohortTable}}).
#' 
#' @template Connection
#' 
#' @template CohortTable
#' 
#' @template CohortDef
#' 
#' @template OracleTempSchema
#' 
#' @template CdmDatabaseSchema
#' 
#' @param cohortId       The cohort definition ID used to reference the cohort in the cohort table.
#' 
#' @param generateInclusionStats     Compute and store inclusion rule statistics?
#' @param resultsDatabaseSchema      Schema name where the statistics tables reside.
#'                                   Note that for SQL Server, this should include both the database and
#'                                   schema name, for example 'scratch.dbo'.
#' @param cohortInclusionTable       Name of the inclusion table, one of the tables for storing inclusion rule statistics.
#' @param cohortInclusionResultTable Name of the inclusion result table, one of the tables for storing inclusion rule statistics.
#' @param cohortInclusionStatsTable  Name of the inclusion stats table, one of the tables for storing inclusion rule statistics.
#' @param cohortSummaryStatsTable    Name of the summary stats table, one of the tables for storing inclusion rule statistics.
#'
#' @export
instantiateCohort <- function(connectionDetails = NULL,
                              connection = NULL,
                              cdmDatabaseSchema,
                              oracleTempSchema = NULL,
                              cohortDatabaseSchema = cdmDatabaseSchema,
                              cohortTable = "cohort",
                              baseUrl = NULL,
                              webApiCohortId = NULL,
                              cohortJson = NULL,
                              cohortSql = NULL,
                              cohortId = webApiCohortId,
                              generateInclusionStats = FALSE,
                              resultsDatabaseSchema = cohortDatabaseSchema,
                              cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                              cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                              cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                              cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats")) {
  if (is.null(baseUrl) && is.null(cohortJson)) {
    stop("Must provide either baseUrl and webApiCohortId, or cohortJson and cohortSql")
  }
  if (!is.null(cohortJson) && !is.character(cohortJson)) {
    stop("cohortJson should be character (a JSON string).") 
  }
  start <- Sys.time()
  if (is.null(cohortJson)) {
    ParallelLogger::logInfo("Retrieving cohort definition from WebAPI")
    cohortExpression <- ROhdsiWebApi::getCohortDefinitionExpression(definitionId = webApiCohortId, baseUrl = baseUrl)
    cohortJson <- cohortExpression$expression
    cohortSql <- ROhdsiWebApi::getCohortDefinitionSql(definitionId = webApiCohortId, 
                                                      baseUrl = baseUrl, 
                                                      generateStats = generateInclusionStats)
  }
  cohortDefinition <- RJSONIO::fromJSON(cohortJson)
  if (generateInclusionStats) {
    inclusionRules <- data.frame()
    if (!is.null(cohortDefinition$InclusionRules)) {
      nrOfRules <- length(cohortDefinition$InclusionRules)
      if (nrOfRules > 0) {
        for (i in 1:nrOfRules) {
          inclusionRules <- rbind(inclusionRules, data.frame(cohortDefinitionId = cohortId,
                                                             ruleSequence = i - 1,
                                                             name = cohortDefinition$InclusionRules[[i]]$name))
        }
      }
    }
  }
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails) 
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  ParallelLogger::logInfo("Instantiation cohort with cohort_definition_id = ", cohortId)
  sql <- cohortSql
  if (generateInclusionStats) {
    sql <- SqlRender::render(sql, 
                             cdm_database_schema = cdmDatabaseSchema,
                             vocabulary_database_schema = cdmDatabaseSchema,
                             target_database_schema = cohortDatabaseSchema,
                             target_cohort_table = cohortTable,
                             target_cohort_id = cohortId,
                             results_database_schema.cohort_inclusion = paste(resultsDatabaseSchema, cohortInclusionTable, sep = "."),  
                             results_database_schema.cohort_inclusion_result = paste(resultsDatabaseSchema, cohortInclusionResultTable, sep = "."),  
                             results_database_schema.cohort_inclusion_stats = paste(resultsDatabaseSchema, cohortInclusionStatsTable, sep = "."),  
                             results_database_schema.cohort_summary_stats = paste(resultsDatabaseSchema, cohortSummaryStatsTable, sep = "."))
  } else {
    sql <- SqlRender::render(sql, 
                             cdm_database_schema = cdmDatabaseSchema,
                             vocabulary_database_schema = cdmDatabaseSchema,
                             target_database_schema = cohortDatabaseSchema,
                             target_cohort_table = cohortTable,
                             target_cohort_id = cohortId)
  }
  sql <- SqlRender::translate(sql,
                              targetDialect = connectionDetails$dbms,
                              oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(connection, sql)
  
  if (generateInclusionStats && nrow(inclusionRules) > 0) {
    DatabaseConnector::insertTable(connection = connection,
                                   tableName = paste(resultsDatabaseSchema, cohortInclusionTable, sep = "."),
                                   data = inclusionRules,
                                   dropTableIfExists = FALSE,
                                   createTable = FALSE,
                                   tempTable = FALSE,
                                   camelCaseToSnakeCase = TRUE)
  }
  delta <- Sys.time() - start
  writeLines(paste("Instantiating cohort took", signif(delta, 3), attr(delta, "units")))
  
}

#' Get statistics on cohort inclusion criteria
#' 
#' @template Connection
#'
#' @param cohortTable                Name of the cohort table. Used only to conveniently derive names of the four rule statistics tables.
#' @param cohortId       The cohort definition ID used to reference the cohort in the cohort table.
#' @param simplify                   Simply output the attrition table?
#' @param resultsDatabaseSchema      Schema name where the statistics tables reside.
#'                                   Note that for SQL Server, this should include both the database and
#'                                   schema name, for example 'scratch.dbo'.
#' @param cohortInclusionTable       Name of the inclusion table, one of the tables for storing inclusion rule statistics.
#' @param cohortInclusionResultTable Name of the inclusion result table, one of the tables for storing inclusion rule statistics.
#' @param cohortInclusionStatsTable  Name of the inclusion stats table, one of the tables for storing inclusion rule statistics.
#' @param cohortSummaryStatsTable    Name of the summary stats table, one of the tables for storing inclusion rule statistics.
#'
#' @return 
#' If `simplify = TRUE`, this function returns a single data frame. Else a list of data frames is returned.
#' 
#' @export
getInclusionStatistics <- function(connectionDetails = NULL,
                                   connection = NULL,
                                   resultsDatabaseSchema,
                                   cohortId,
                                   simplify = TRUE,
                                   cohortTable = "cohort",
                                   cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                                   cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                                   cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                                   cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats")) {
  start <- Sys.time()
  ParallelLogger::logInfo("Fetching inclusion statistics for cohort with cohort_definition_id = ", cohortId)
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails) 
    on.exit(DatabaseConnector::disconnect(connection))
  }
  fetchStats <- function(table) {
    ParallelLogger::logDebug("- Fetching data from ", table)
    sql <- "SELECT * FROM @database_schema.@table WHERE cohort_definition_id = @cohort_id"
    DatabaseConnector::renderTranslateQuerySql(sql = sql,
                                               connection = connection,
                                               snakeCaseToCamelCase = TRUE,
                                               database_schema = resultsDatabaseSchema,
                                               table = table,
                                               cohort_id = cohortId) 
  }
  inclusion <- fetchStats(cohortInclusionTable)
  summaryStats <- fetchStats(cohortSummaryStatsTable)
  inclusionStats <- fetchStats(cohortInclusionStatsTable)
  inclusionResults <- fetchStats(cohortInclusionResultTable)
  result <- processInclusionStats(inclusion = inclusion,
                                  inclusionResults = inclusionResults,
                                  inclusionStats = inclusionStats,
                                  summaryStats = summaryStats,
                                  simplify = simplify)
  delta <- Sys.time() - start
  writeLines(paste("Fetching inclusion statistics took", signif(delta, 3), attr(delta, "units")))
  return(result)
}

#' Get inclusion criteria statistics from files
#' 
#' @description 
#' Gets inclusion criteria statistics from files, as stored when using the \code{ROhdsiWebApi::insertCohortDefinitionSetInPackage} function
#' with \code{generateStats = TRUE}.
#' 
#' @param cohortId       The cohort definition ID used to reference the cohort in the cohort table.
#' @param simplify                   Simply output the attrition table?
#' @param folder                    The path to the folder where the inclusion statistics are stored.
#' @param cohortInclusionFile       Name of the inclusion table, one of the tables for storing inclusion rule statistics.
#' @param cohortInclusionResultFile Name of the inclusion result table, one of the tables for storing inclusion rule statistics.
#' @param cohortInclusionStatsFile  Name of the inclusion stats table, one of the tables for storing inclusion rule statistics.
#' @param cohortSummaryStatsFile    Name of the summary stats table, one of the tables for storing inclusion rule statistics.
#'
#' @return 
#' If `simplify = TRUE`, this function returns a single data frame. Else a list of data frames is returned.
#' 
#' @export
getInclusionStatisticsFromFiles <- function(cohortId,
                                            folder,
                                            cohortInclusionFile = file.path(folder, "cohortInclusion.csv"),
                                            cohortInclusionResultFile = file.path(folder, "cohortIncResult.csv"),
                                            cohortInclusionStatsFile = file.path(folder, "cohortIncStats.csv"),
                                            cohortSummaryStatsFile = file.path(folder, "cohortSummaryStats.csv"),
                                            simplify = TRUE) {
  start <- Sys.time()
  ParallelLogger::logInfo("Fetching inclusion statistics for cohort with cohort_definition_id = ", cohortId)
  
  fetchStats <- function(file) {
    ParallelLogger::logDebug("- Fetching data from ", file)
    stats <- readr::read_csv(file, col_types = readr::cols())
    stats <- stats[stats$cohortDefinitionId == cohortId, ]
    return(stats)                                             
  }
  inclusion <- fetchStats(cohortInclusionFile)
  summaryStats <- fetchStats(cohortSummaryStatsFile)
  inclusionStats <- fetchStats(cohortInclusionStatsFile)
  inclusionResults <- fetchStats(cohortInclusionResultFile)
  result <- processInclusionStats(inclusion = inclusion,
                                  inclusionResults = inclusionResults,
                                  inclusionStats = inclusionStats,
                                  summaryStats = summaryStats,
                                  simplify = simplify)
  delta <- Sys.time() - start
  writeLines(paste("Fetching inclusion statistics took", signif(delta, 3), attr(delta, "units")))
  return(result)
}

processInclusionStats <- function(inclusion,
                                  inclusionResults,
                                  inclusionStats,
                                  summaryStats,
                                  simplify) {
  if (simplify) {
    if (nrow(inclusion) == 0 || nrow(inclusionStats) == 0) {
      return(data.frame())
    }
    result <- merge(unique(inclusion[, c("ruleSequence", "name")]),
                    inclusionStats[inclusionStats$modeId == 0, c("ruleSequence", 
                                                                 "personCount", 
                                                                 "gainCount", 
                                                                 "personTotal")],)
    
    result$remain <- rep(0, nrow(result))
    inclusionResults <- inclusionResults[inclusionResults$modeId == 0, ]
    mask <- 0
    for (ruleId in 0:(nrow(result) - 1)) {
      mask <- bitwOr(mask, 2^ruleId)  
      idx <- bitwAnd(inclusionResults$inclusionRuleMask, mask) == mask
      result$remain[result$ruleSequence == ruleId] <- sum(inclusionResults$personCount[idx])
    }
    colnames(result) <- c("ruleSequenceId", "ruleName", "meetSubjects", "gainSubjects", "totalSubjects", "remainSubjects")
  } else {
    if (nrow(inclusion) == 0) {
      return(list())
    }
    result <- list(inclusion = inclusion,
                   inclusionResults = inclusionResults,
                   inclusionStats = inclusionStats,
                   summaryStats = summaryStats)
  }
  return(result)
}
