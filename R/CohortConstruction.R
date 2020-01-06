# Copyright 2019 Observational Health Data Sciences and Informatics
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
#' This function first extracts all concept sets used in a cohort definition. Then, for each concept set
#' the concept found in the CDM database the contributing source codes are identified.
#' 
#' 
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cohortDatabaseSchema Schema name where your cohort table will be created.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'scratch.dbo'.
#' @param cohortTable          Name of the cohort table.
#'
#' @export
createCohortTable <- function(connectionDetails,
                              cohortDatabaseSchema,
                              cohortTable = "cohort",
                              createInclusionStatsTables = FALSE,
                              resultsDatabaseSchema = cohortDatabaseSchema,
                              cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                              cohortInclusionResultTable = paste0(cohortTable, "_inc_result"),
                              cohortInclusionStatsTable = paste0(cohortTable, "_inc_stats"),
                              cohortSummaryStatsTable = paste0(cohortTable, "_sum_stats"))  {
  start <- Sys.time()
  ParallelLogger::logInfo("Creating cohort table")
  connection <- DatabaseConnector::connect(connectionDetails) 
  on.exit(DatabaseConnector::disconnect(connection))
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
  writeLines(paste("Creating cohort taBLE took", signif(delta, 3), attr(delta, "units")))
}



#' Check source codes used in a cohort definition
#' 
#' @description 
#' This function first extracts all concept sets used in a cohort definition. Then, for each concept set
#' the concept found in the CDM database the contributing source codes are identified.
#' 
#' There are two ways to call this function:
#' \itemize{
#'   \item \code{findIncludedSourceCodes(connectionDetails, cdmDatabaseSchema, oracleTempSchema, baseUrl, cohortId)}
#'   \item \code{findIncludedSourceCodes(connectionDetails, cdmDatabaseSchema, oracleTempSchema, cohortJson, cohortSql)}
#' }
#' 
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param oracleTempSchema     Should be used in Oracle to specify a schema where the user has write
#'                             priviliges for storing temporary tables.
#' @param baseUrl              The base URL for the WebApi instance, for example: "http://server.org:80/WebAPI".
#' @param cohortId             The ID of the cohort in the WebAPI instance.
#' @param cohortJson           A characteric string containing the JSON of a cohort definition.
#' @param cohortSql            The OHDSI SQL representation of the same cohort definition.
#' @param byMonth              Compute counts by month? If FALSE, only overall counts are computed.
#' @param createCohortTable    A logical value indicating whether a new cohort table should be created. If it
#'                             already exists it will be deleted first. If \code{createCohortTable = FALSE}, the
#'                             cohort table is assumed to already exits.
#' @param instantiateCohort    A logical value indicating whether the cohort should be instantiated in the cohort
#'                             table. If not, the cohort is assumed to already exists.
#' @param instantiatedCohortId The cohort definition ID used in the cohort table. 
#'
#' @return 
#' A data frame with concepts, and per concept the count of how often the concept was encountered at the index date.
#' 
#' @export
instantiateCohort <- function(connectionDetails,
                              cdmDatabaseSchema,
                              oracleTempSchema = NULL,
                              cohortDatabaseSchema = cdmDatabaseSchema,
                              cohortTable = "cohort",
                              baseUrl = NULL,
                              cohortId = NULL,
                              cohortJson = NULL,
                              cohortSql = NULL,
                              instantiatedCohortId = cohortId,
                              generateInclusionStats = FALSE,
                              resultsDatabaseSchema = cohortDatabaseSchema,
                              cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                              cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                              cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                              cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats")) {
  if (is.null(baseUrl) && is.null(cohortJson)) {
    stop("Must provide either baseUrl and cohortId, or cohortJson and cohortSql")
  }
  if (!is.null(cohortJson) && !is.character(cohortJson)) {
    stop("cohortJson should be character (a JSON string).") 
  }
  start <- Sys.time()
  if (is.null(cohortJson)) {
    ParallelLogger::logInfo("Retrieving cohort definition from WebAPI")
    cohortExpression <- ROhdsiWebApi::getCohortDefinitionExpression(definitionId = cohortId, baseUrl = baseUrl)
    cohortJson <- cohortExpression$expression
    cohortSql <- ROhdsiWebApi::getCohortDefinitionSql(definitionId = cohortId, 
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
          inclusionRules <- rbind(inclusionRules, data.frame(cohortDefinitionId = instantiatedCohortId,
                                                             ruleSequence = i - 1,
                                                             name = cohortDefinition$InclusionRules[[i]]$name))
        }
      }
    }
  }
  
  connection <- DatabaseConnector::connect(connectionDetails) 
  on.exit(DatabaseConnector::disconnect(connection))
  
  ParallelLogger::logInfo("Instantiation cohort with cohort_definition_id = ", instantiatedCohortId)
  sql <- cohortSql
  if (generateInclusionStats) {
    sql <- SqlRender::render(sql, 
                             cdm_database_schema = cdmDatabaseSchema,
                             vocabulary_database_schema = cdmDatabaseSchema,
                             target_database_schema = cohortDatabaseSchema,
                             target_cohort_table = cohortTable,
                             target_cohort_id = instantiatedCohortId,
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
                             target_cohort_id = instantiatedCohortId)
  }
  sql <- SqlRender::translate(sql,
                              targetDialect = connectionDetails$dbms,
                              oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(connection, sql)
  
  if (generateInclusionStats) {
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

#' Title
#'
#' @param connectionDetails 
#' @param resultsDatabaseSchema 
#' @param instantiatedCohortId 
#' @param cohortTable 
#' @param cohortInclusionTable 
#' @param cohortInclusionResultTable 
#' @param cohortInclusionStatsTable 
#' @param cohortSummaryStatsTable 
#'
#' @return A list of data frames.
#' 
#' @export
getInclusionStatistics <- function(connectionDetails,
                                   resultsDatabaseSchema,
                                   instantiatedCohortId,
                                   cohortTable = "cohort",
                                   cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                                   cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                                   cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                                   cohortSummaryStatsTable = paste0(cohortTable, "_inclusion_stats")) {
  start <- Sys.time()
  ParallelLogger::logInfo("Fetching inclusion statistics for cohort with cohort_definition_id = ", instantiatedCohortId)
  connection <- DatabaseConnector::connect(connectionDetails) 
  on.exit(DatabaseConnector::disconnect(connection))
  fetchStats <- function(table) {
    ParallelLogger::logDebug("- Fetching data from ", table)
    sql <- "SELECT * FROM @database_schema.@table WHERE cohort_definition_id = @cohort_id"
    DatabaseConnector::renderTranslateQuerySql(sql = sql,
                                               connection = connection,
                                               snakeCaseToCamelCase = TRUE,
                                               database_schema = resultsDatabaseSchema,
                                               table = table,
                                               cohort_id = instantiatedCohortId) 
  }
  inclusion <- fetchStats(cohortInclusionTable)
  inclusionResults <- fetchStats(cohortInclusionResultTable)
  inclusionStats <- fetchStats(cohortInclusionStatsTable)
  summaryStats <- fetchStats(cohortSummaryStatsTable)
  delta <- Sys.time() - start
  writeLines(paste("Fetching inclusion statistics took", signif(delta, 3), attr(delta, "units")))
  return(list(inclusion = inclusion,
              inclusionResults = inclusionResults,
              inclusionStats = inclusionStats,
              summaryStats = summaryStats))
}