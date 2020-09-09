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

checkCohortReference <- function(cohortReference, errorMessage = NULL) {
  if (is.null(errorMessage) | !class(errorMessage) == 'AssertColection') {
    errorMessage <- checkmate::makeAssertCollection()
  }
  checkmate::assertDataFrame(x = cohortReference, 
                             types = c("integer", "character","numeric"),
                             min.rows = 1,
                             min.cols = 6,
                             null.ok = FALSE,
                             col.names = "named",
                             add = errorMessage)
  checkmate::assertIntegerish(x = cohortReference$referentConceptId, 
                              lower = 0, 
                              any.missing = FALSE, 
                              unique = FALSE, 
                              null.ok = FALSE, 
                              add = errorMessage)
  checkmate::assertIntegerish(x = cohortReference$cohortId, 
                              lower = 0, 
                              any.missing = FALSE, 
                              unique = TRUE, 
                              null.ok = FALSE, 
                              add = errorMessage)
  checkmate::assertIntegerish(x = cohortReference$webApiCohortId, 
                              lower = 0, 
                              any.missing = FALSE, 
                              unique = TRUE, 
                              null.ok = FALSE, 
                              add = errorMessage)
  checkmate::assertNames(x = names(cohortReference),subset.of =  c("referentConceptId","cohortId",
                                                                   "webApiCohortId","cohortName",
                                                                   "logicDescription","clinicalRationale" ),
                         add = errorMessage)
  invisible(errorMessage)
}


getCohortsJsonAndSqlFromPackage <- function(packageName = packageName,
                                            cohortToCreateFile = cohortToCreateFile,
                                            cohortIds = NULL,
                                            errorMessage = NULL) {
  ParallelLogger::logInfo("Executing on cohorts specified in package - ", packageName)
  
  if (is.null(errorMessage) | !class(errorMessage) == 'AssertColection') {
    errorMessage <- checkmate::makeAssertCollection()
  }
  checkmate::assertCharacter(x = packageName, min.len = 1, max.len = 1, add = errorMessage)
  pathToCsv <- system.file(cohortToCreateFile, package = packageName)
  checkmate::assertFileExists(x = system.file(cohortToCreateFile, package = packageName), 
                              access = "r", 
                              extension = "csv", 
                              add = errorMessage)
  
  cohorts <- readr::read_csv(pathToCsv, col_types = readr::cols())
  if (!is.null(cohortIds)) {
    cohorts <- cohorts %>% dplyr::filter(.data$cohortId %in% cohortIds)
  }
  checkCohortReference(cohortReference = cohorts, errorMessage = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  getSql <- function(name) {
    pathToSql <- system.file("sql", "sql_server", paste0(name, ".sql"), package = packageName)
    checkmate::assertFile(x = pathToSql, access = "r", extension = ".sql", add = errorMessage)
    sql <- readChar(pathToSql, file.info(pathToSql)$size)
    return(sql)
  }
  cohorts$sql <- sapply(cohorts$cohortId, getSql)
  getJson <- function(name) {
    pathToJson <- system.file("cohorts", paste0(name, ".json"), package = packageName)
    checkmate::assertFile(x = pathToJson, access = "r", extension = ".sql", add = errorMessage)
    json <- readChar(pathToJson, file.info(pathToJson)$size)
    return(json)
  }
  cohorts$json <- sapply(cohorts$cohortId, getJson)
  return(cohorts)
}


getCohortsJsonAndSqlFromWebApi <- function(baseUrl = baseUrl,
                                           cohortSetReference = cohortSetReference,
                                           cohortIds = NULL,
                                           errorMessage = NULL) {
  ParallelLogger::logInfo("[WebApi mode] Running Cohort Diagnostics on cohort specified in WebApi - ", baseUrl)
  
  if (is.null(errorMessage) | !class(errorMessage) == 'AssertColection') {
    errorMessage <- checkmate::makeAssertCollection()
  }
  checkmate::assertCharacter(x = baseUrl, min.chars = 1, add = errorMessage)
  webApiVersion <- ROhdsiWebApi::getWebApiVersion(baseUrl)
  ParallelLogger::logInfo("WebApi of version ", webApiVersion, " found at ", baseUrl)
  checkmate::assertCharacter(x = webApiVersion, min.chars = 1, add = errorMessage)
  cohorts <- cohortSetReference
  errorMessage <- checkCohortReference(cohortReference = cohorts, errorMessage = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  if (!is.null(cohortIds)) {
    cohorts <- cohorts %>% dplyr::filter(.data$cohortId %in% cohortIds)
  }

  if ("name" %in% names(cohorts)) {
    cohorts <- dplyr::rename(cohorts, cohortName = "name")
  }
  cohorts <- dplyr::rename(cohorts, cohortFullName = "atlasName")
  ParallelLogger::logInfo("Retrieving cohort definitions from WebAPI")
  cohort <- list()
  for (i in 1:nrow(cohorts)) {
    cohort[[i]] <- cohorts %>% dplyr::slice(i)
    ParallelLogger::logInfo("- Retrieving definitions for cohort ", cohort[[i]]$cohortFullName)
    cohortDefinition <-  ROhdsiWebApi::getCohortDefinition(cohortId = cohort[[i]]$atlasId,
                                                           baseUrl = baseUrl)
    cohort[[i]]$json <- RJSONIO::toJSON(x = cohortDefinition$expression, digits = 23)
    cohort[[i]]$sql <- ROhdsiWebApi::getCohortSql(cohortDefinition = cohortDefinition,
                                                 baseUrl = baseUrl,
                                                 generateStats = TRUE)
  }
  checkmate::reportAssertions(collection = errorMessage)
  return(dplyr::bind_rows(cohort))
}

#' Get cohorts JSON and parameterized OHDSI SQL
#'
#' @description
#' This function may be used to collect a cohorts JSON and OHDSI SQL. Based on whether a
#' baseUrl is available, the function will collect the specifications from either from
#' WebApi or a Package. 
#'
#' @template CohortSetSpecs
#' 
#' @template CohortSetReference
#' 
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    diagnostics to.
#' @return 
#' The function will return a R list object with cohort information including specifications 
#' such as JSON and SQL.
#'
#' @examples
#' \dontrun{
#' cohorts <- getCohortsJsonAndSql(packageName = 'cohortDiagnostics',
#'                                 baseUrl = "http://server.org:80/WebAPI")
#' }
#' 
#' @export
getCohortsJsonAndSql <- function(packageName = NULL,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
                                 baseUrl = NULL,
                                 cohortSetReference = NULL,
                                 cohortIds = NULL) {
  # Input parameters check
  ParallelLogger::logInfo("Beginning cohort input parameter checks")
  errorMessage <- checkmate::makeAssertCollection()
  
  if (!is.null(cohortIds)) {
    checkmate::assertIntegerish(x = cohortIds, 
                                any.missing = FALSE, 
                                min.len = 1, 
                                unique = TRUE, 
                                add = errorMessage)
  }
  
  if (!is.null(packageName)) {
    cohorts <- getCohortsJsonAndSqlFromPackage(packageName = packageName, 
                                               cohortToCreateFile = cohortToCreateFile,
                                               cohortIds = cohortIds)
    if (!is.null(baseUrl)) {
      baseUrl <- NULL
      ParallelLogger::logInfo("[Package mode] Ignoring parameter baseUrl because packageName is provided.\n",
                              "Overiding user parameter baseUrl - setting to NULL")
    }
    if (!is.null(cohortSetReference)) {
      cohortSetReference <- NULL
      ParallelLogger::logInfo("Ignoring parameter cohortSetReference because packageName is provided.\n",
                              "Overiding user parameter cohortSetReference - setting to NULL")
    }
  } else {
    cohorts <- getCohortsJsonAndSqlFromWebApi(baseUrl = baseUrl,
                                              cohortSetReference = cohortSetReference,
                                              cohortIds = cohortIds)
  }
  checkmate::reportAssertions(collection = errorMessage)
  return(cohorts)
}


#' Create cohort table(s)
#'
#' @description
#' This function creates an empty cohort table. Optionally, additional empty tables are created to
#' store statistics on the various inclusion criteria.
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @param createInclusionStatsTables   Create the four additional tables for storing inclusion rule
#'                                     statistics?
#' @param resultsDatabaseSchema        Schema name where the statistics tables reside. Note that for
#'                                     SQL Server, this should include both the database and schema
#'                                     name, for example 'scratch.dbo'.
#' @param cohortInclusionTable         Name of the inclusion table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortInclusionResultTable   Name of the inclusion result table, one of the tables for
#'                                     storing inclusion rule statistics.
#' @param cohortInclusionStatsTable    Name of the inclusion stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortSummaryStatsTable      Name of the summary stats table, one of the tables for storing
#'                                     inclusion rule statistics.
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
                              cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats")) {
  start <- Sys.time()
  ParallelLogger::logInfo("Creating cohort table")
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql("CreateCohortTable.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortTable)
  
  if (createInclusionStatsTables) {
    ParallelLogger::logInfo("Creating inclusion rule statistics tables")
    sql <- SqlRender::loadRenderTranslateSql("CreateInclusionStatsTables.sql",
                                             packageName = "CohortDiagnostics",
                                             dbms = connection@dbms,
                                             cohort_database_schema = resultsDatabaseSchema,
                                             cohort_inclusion_table = cohortInclusionTable,
                                             cohort_inclusion_result_table = cohortInclusionResultTable,
                                             cohort_inclusion_stats_table = cohortInclusionStatsTable,
                                             cohort_summary_stats_table = cohortSummaryStatsTable)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortInclusionTable)
    ParallelLogger::logDebug("- Created table ",
                             cohortDatabaseSchema,
                             ".",
                             cohortInclusionResultTable)
    ParallelLogger::logDebug("- Created table ",
                             cohortDatabaseSchema,
                             ".",
                             cohortInclusionStatsTable)
    ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortSummaryStatsTable)
  }
  delta <- Sys.time() - start
  writeLines(paste("Creating cohort table took", signif(delta, 3), attr(delta, "units")))
}


#' Instantiate a cohort
#'
#' @description
#' This function instantiates the cohort in the cohort table. Optionally, the inclusion rule
#' statistics are computed and stored in the inclusion rule statistics tables described in
#' \code{\link{createCohortTable}}).
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
#' @param cohortId                     The cohort definition ID used to reference the cohort in the
#'                                     cohort table.
#'
#' @param generateInclusionStats       Compute and store inclusion rule statistics?
#' @param resultsDatabaseSchema        Schema name where the statistics tables reside. Note that for
#'                                     SQL Server, this should include both the database and schema
#'                                     name, for example 'scratch.dbo'.
#' @param cohortInclusionTable         Name of the inclusion table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortInclusionResultTable   Name of the inclusion result table, one of the tables for
#'                                     storing inclusion rule statistics.
#' @param cohortInclusionStatsTable    Name of the inclusion stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortSummaryStatsTable      Name of the summary stats table, one of the tables for storing
#'                                     inclusion rule statistics.
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
                              cohortId = NULL,
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
    cohortDefinition <- ROhdsiWebApi::getCohortDefinition(cohortId = cohortId,
                                                          baseUrl = baseUrl)
    cohortDefinition <- cohortDefinition$expression
    cohortSql <- ROhdsiWebApi::getCohortSql(cohortDefinition = cohortDefinition,
                                            baseUrl = baseUrl,
                                            generateStats = generateInclusionStats)
  } else {
    cohortDefinition <- RJSONIO::fromJSON(content = cohortJson, digits = 23)
  }
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
  .warnMismatchSqlInclusionStats(sql, generateInclusionStats = generateInclusionStats)
  if (generateInclusionStats) {
    sql <- SqlRender::render(sql,
                             cdm_database_schema = cdmDatabaseSchema,
                             vocabulary_database_schema = cdmDatabaseSchema,
                             target_database_schema = cohortDatabaseSchema,
                             target_cohort_table = cohortTable,
                             target_cohort_id = cohortId,
                             results_database_schema.cohort_inclusion = paste(resultsDatabaseSchema,
                                                                              cohortInclusionTable,
                                                                              sep = "."),
                             results_database_schema.cohort_inclusion_result = paste(resultsDatabaseSchema,
                                                                                     cohortInclusionResultTable,
                                                                                     sep = "."),
                             results_database_schema.cohort_inclusion_stats = paste(resultsDatabaseSchema,
                                                                                    cohortInclusionStatsTable,
                                                                                    sep = "."),
                             results_database_schema.cohort_summary_stats = paste(resultsDatabaseSchema,
                                                                                  cohortSummaryStatsTable,
                                                                                  sep = "."))
  } else {
    sql <- SqlRender::render(sql,
                             cdm_database_schema = cdmDatabaseSchema,
                             vocabulary_database_schema = cdmDatabaseSchema,
                             target_database_schema = cohortDatabaseSchema,
                             target_cohort_table = cohortTable,
                             target_cohort_id = cohortId)
  }
  sql <- SqlRender::translate(sql,
                              targetDialect = connection@dbms,
                              oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(connection, sql)
  
  if (generateInclusionStats && nrow(inclusionRules) > 0) {
    DatabaseConnector::insertTable(connection = connection,
                                   tableName = paste(resultsDatabaseSchema,
                                                     cohortInclusionTable,
                                                     sep = "."),
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
#' @param cohortTable                  Name of the cohort table. Used only to conveniently derive names
#'                                     of the four rule statistics tables.
#' @param cohortId                     The cohort definition ID used to reference the cohort in the
#'                                     cohort table.
#' @param simplify                     Simply output the attrition table?
#' @param resultsDatabaseSchema        Schema name where the statistics tables reside. Note that for
#'                                     SQL Server, this should include both the database and schema
#'                                     name, for example 'scratch.dbo'.
#' @param cohortInclusionTable         Name of the inclusion table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortInclusionResultTable   Name of the inclusion result table, one of the tables for
#'                                     storing inclusion rule statistics.
#' @param cohortInclusionStatsTable    Name of the inclusion stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortSummaryStatsTable      Name of the summary stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#'
#' @return
#' If `simplify = TRUE`, this function returns a single data frame. Else a list of data frames is
#' returned.
#'
#' @export
getInclusionStatistics <- function(connectionDetails = NULL,
                                   connection = NULL,
                                   resultsDatabaseSchema,
                                   cohortId,
                                   simplify = TRUE,
                                   cohortTable = "cohort",
                                   cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                                   cohortInclusionResultTable = paste0(cohortTable,
                                                                       "_inclusion_result"),
                                   cohortInclusionStatsTable = paste0(cohortTable,
                                                                      "_inclusion_stats"),
                                   cohortSummaryStatsTable = paste0(cohortTable,
                                                                    "_summary_stats")) {
  start <- Sys.time()
  ParallelLogger::logInfo("Fetching inclusion statistics for cohort with cohort_definition_id = ",
                          cohortId)
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
#' Gets inclusion criteria statistics from files, as stored when using the
#' \code{ROhdsiWebApi::insertCohortDefinitionSetInPackage} function with \code{generateStats = TRUE}.
#'
#' @param cohortId                    The cohort definition ID used to reference the cohort in the
#'                                    cohort table.
#' @param simplify                    Simply output the attrition table?
#' @param folder                      The path to the folder where the inclusion statistics are stored.
#' @param cohortInclusionFile         Name of the inclusion table, one of the tables for storing
#'                                    inclusion rule statistics.
#' @param cohortInclusionResultFile   Name of the inclusion result table, one of the tables for storing
#'                                    inclusion rule statistics.
#' @param cohortInclusionStatsFile    Name of the inclusion stats table, one of the tables for storing
#'                                    inclusion rule statistics.
#' @param cohortSummaryStatsFile      Name of the summary stats table, one of the tables for storing
#'                                    inclusion rule statistics.
#'
#' @return
#' If `simplify = TRUE`, this function returns a single data frame. Else a list of data frames is
#' returned.
#'
#' @export
getInclusionStatisticsFromFiles <- function(cohortId,
                                            folder,
                                            cohortInclusionFile = file.path(folder,
                                                                            "cohortInclusion.csv"),
                                            cohortInclusionResultFile = file.path(folder,
                                                                                  "cohortIncResult.csv"),
                                            cohortInclusionStatsFile = file.path(folder,
                                                                                 "cohortIncStats.csv"),
                                            cohortSummaryStatsFile = file.path(folder,
                                                                               "cohortSummaryStats.csv"),
                                            simplify = TRUE) {
  start <- Sys.time()
  ParallelLogger::logInfo("Fetching inclusion statistics for cohort with cohort_definition_id = ",
                          cohortId)
  
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
    # result <- inclusion %>% 
    #   dplyr::inner_join(inclusionStats) %>% 
    #   dplyr::inner_join(inclusionResults %>% dplyr::select(-.data$personCount)) %>% 
    #   dplyr::mutate(description = tidyr::replace_na(data = .data$description, replace = ''),
    #                 modeName = dplyr::case_when(modeId == 0 ~ 'persons',
    #                                             TRUE ~ 'events')) %>% 
    #   dplyr::rename_all(stringr::str_replace, replacement = "", pattern = "Count")
    
    result <- merge(unique(inclusion[, c("ruleSequence", "name")]),
                    inclusionStats[inclusionStats$modeId ==
                                     0, c("ruleSequence", "personCount", "gainCount", "personTotal")], )
    
    result$remain <- rep(0, nrow(result))
    inclusionResults <- inclusionResults[inclusionResults$modeId == 0, ]
    mask <- 0
    for (ruleId in 0:(nrow(result) - 1)) {
      if (nrow(inclusionResults) > 0) {
        mask <- bitwOr(mask, 2^ruleId)
        idx <- bitwAnd(inclusionResults$inclusionRuleMask, mask) == mask
        result$remain[result$ruleSequence == ruleId] <- sum(inclusionResults$personCount[idx])
      }
    }
    colnames(result) <- c("ruleSequenceId",
                          "ruleName",
                          "meetSubjects",
                          "gainSubjects",
                          "totalSubjects",
                          "remainSubjects")
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

#' Instantiate a set of cohort
#'
#' @description
#' This function instantiates a set of cohort in the cohort table, using definitions that are fetched from a WebApi interface.
#' Optionally, the inclusion rule statistics are computed and stored in the \code{inclusionStatisticsFolder}.
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @template OracleTempSchema
#'
#' @template CdmDatabaseSchema
#' 
#' @template CohortSetSpecs
#' 
#' @template CohortSetReference
#'
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    construction to.
#' @param generateInclusionStats      Compute and store inclusion rule statistics?
#' @param inclusionStatisticsFolder   The folder where the inclusion rule statistics are stored. Can be
#'                                    left NULL if \code{generateInclusionStats = FALSE}.
#' @param createCohortTable           Create the cohort table? If \code{incremental = TRUE} and the table
#'                                    already exists this will be skipped.
#' @param incremental                 Create only cohorts that haven't been created before?
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are kept
#'                                    of which definition has been executed.
#' @return
#' A data frame with cohort counts                                    
#'
#' @export
instantiateCohortSet <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 oracleTempSchema = NULL,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortIds = NULL,
                                 packageName = NULL,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
                                 baseUrl = NULL,
                                 cohortSetReference = NULL,
                                 generateInclusionStats = FALSE,
                                 inclusionStatisticsFolder = NULL,
                                 createCohortTable = FALSE,
                                 incremental = FALSE,
                                 incrementalFolder = NULL) {
  if (generateInclusionStats) {
    if (is.null(inclusionStatisticsFolder)) {
      stop("Must specify inclusionStatisticsFolder when generateInclusionStats = TRUE")
    }
    if (!file.exists(inclusionStatisticsFolder)) {
      dir.create(inclusionStatisticsFolder, recursive = TRUE)
    }
  }
  if (incremental) {
    if (is.null(incrementalFolder)) {
      stop("Must specify incrementalFolder when incremental = TRUE")
    }
    if (!file.exists(incrementalFolder)) {
      dir.create(incrementalFolder, recursive = TRUE)
    }
  }
  
  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  if (createCohortTable) {
    needToCreate <- TRUE
    if (incremental) {
      tables <- DatabaseConnector::getTableNames(connection, cohortDatabaseSchema)
      if (toupper(cohortTable) %in% toupper(tables)) {
        ParallelLogger::logInfo("Cohort table already exists and in incremental mode, so not recreating table.")
        needToCreate <- FALSE
      }
    }
    if (needToCreate) {
      createCohortTable(connection = connection,
                        cohortDatabaseSchema = cohortDatabaseSchema,
                        cohortTable = cohortTable,
                        createInclusionStatsTables = FALSE)
    }
  }
  
  cohorts <- getCohortsJsonAndSql(packageName = packageName,
                                  cohortToCreateFile = cohortToCreateFile,
                                  baseUrl = baseUrl,
                                  cohortSetReference = cohortSetReference, 
                                  cohortIds = cohortIds)
  
  if (incremental) {
    cohorts$checksum <- computeChecksum(cohorts$sql)
    recordKeepingFile <- file.path(incrementalFolder, "InstantiatedCohorts.csv")
  }
  
  if (generateInclusionStats) {
    createTempInclusionStatsTables(connection, oracleTempSchema, cohorts) 
  }
  
  instantiatedCohortIds <- c() 
  for (i in 1:nrow(cohorts)) {
    if (!incremental || isTaskRequired(cohortId = cohorts$cohortId[i],
                                       checksum = cohorts$checksum[i],
                                       recordKeepingFile = recordKeepingFile)) {
      ParallelLogger::logInfo("Instantiation cohort ", cohorts$cohortName[i], " (Cohort id: ", cohorts$cohortId[i], ")")
      sql <- cohorts$sql[i]
      .warnMismatchSqlInclusionStats(sql, generateInclusionStats = generateInclusionStats)
      if (generateInclusionStats) {
        sql <- SqlRender::render(sql,
                                 cdm_database_schema = cdmDatabaseSchema,
                                 vocabulary_database_schema = cdmDatabaseSchema,
                                 target_database_schema = cohortDatabaseSchema,
                                 target_cohort_table = cohortTable,
                                 target_cohort_id = cohorts$cohortId[i],
                                 results_database_schema.cohort_inclusion = "#cohort_inclusion",
                                 results_database_schema.cohort_inclusion_result = "#cohort_inc_result",
                                 results_database_schema.cohort_inclusion_stats = "#cohort_inc_stats",
                                 results_database_schema.cohort_summary_stats = "#cohort_summary_stats")
      } else {
        sql <- SqlRender::render(sql,
                                 cdm_database_schema = cdmDatabaseSchema,
                                 vocabulary_database_schema = cdmDatabaseSchema,
                                 target_database_schema = cohortDatabaseSchema,
                                 target_cohort_table = cohortTable,
                                 target_cohort_id = cohorts$cohortId[i])
      }
      sql <- SqlRender::translate(sql,
                                  targetDialect = connection@dbms,
                                  oracleTempSchema = oracleTempSchema)
      DatabaseConnector::executeSql(connection, sql)
      instantiatedCohortIds <- c(instantiatedCohortIds, cohorts$cohortId[i])
    }
  }
  
  if (generateInclusionStats) {
    saveAndDropTempInclusionStatsTables(connection = connection, 
                                        oracleTempSchema = oracleTempSchema, 
                                        inclusionStatisticsFolder = inclusionStatisticsFolder, 
                                        incremental = incremental, 
                                        cohortIds = instantiatedCohortIds)
  }
  if (incremental) {
    recordTasksDone(cohortId = cohorts$cohortId, checksum = cohorts$checksum, recordKeepingFile = recordKeepingFile)
  }
  
  delta <- Sys.time() - start
  writeLines(paste("Instantiating cohort set took", signif(delta, 3), attr(delta, "units")))
}

createTempInclusionStatsTables <- function(connection, oracleTempSchema, cohorts) {
  ParallelLogger::logInfo("Creating temporary inclusion statistics tables")
  pathToSql <- system.file( "inclusionStatsTables.sql", package = "ROhdsiWebApi")
  sql <- SqlRender::readSql(pathToSql)
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms, oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(connection, sql)
  
  inclusionRules <- tidyr::tibble()
  for (i in 1:nrow(cohorts)) {
    cohortDefinition <- RJSONIO::fromJSON(content = cohorts$json[i], digits = 23)
    if (!is.null(cohortDefinition$InclusionRules)) {
      nrOfRules <- length(cohortDefinition$InclusionRules)
      if (nrOfRules > 0) {
        for (j in 1:nrOfRules) {
          inclusionRules <- dplyr::bind_rows(inclusionRules, 
                                             tidyr::tibble(cohortId = cohorts$cohortId[i],
                                                           ruleSequence = j - 1,
                                                           ruleName = cohortDefinition$InclusionRules[[j]]$name)) %>% 
            dplyr::distinct()
        }
      }
    }
  }
  
  if (nrow(inclusionRules) > 0) {
    inclusionRules <- inclusionRules %>% 
      dplyr::inner_join(cohorts %>% dplyr::select(.data$cohortId, .data$cohortName)) %>% 
      dplyr::rename(cohort_definition_id = .data$cohortId, 
                    rule_sequence = .data$ruleSequence, 
                    name = .data$ruleName) %>% 
      dplyr::select(.data$cohort_definition_id, .data$rule_sequence, .data$name)
    
    inclusionRules <- data.frame(inclusionRules) # temporary solution till DatabaseConnector supports tibble
    
    DatabaseConnector::insertTable(connection = connection,
                                   tableName = "#cohort_inclusion",
                                   data = inclusionRules,
                                   dropTableIfExists = FALSE,
                                   createTable = FALSE,
                                   tempTable = TRUE,
                                   oracleTempSchema = oracleTempSchema)
  }
}

saveAndDropTempInclusionStatsTables <- function(connection, 
                                                oracleTempSchema, 
                                                inclusionStatisticsFolder, 
                                                incremental,
                                                cohortIds) {
  fetchStats <- function(table, fileName) {
    ParallelLogger::logDebug("- Fetching data from ", table)
    sql <- "SELECT * FROM @table"
    data <- DatabaseConnector::renderTranslateQuerySql(sql = sql,
                                                       connection = connection,
                                                       oracleTempSchema = oracleTempSchema,
                                                       snakeCaseToCamelCase = TRUE,
                                                       table = table)
    data <- tidyr::as_tibble(data)
    fullFileName <- file.path(inclusionStatisticsFolder, fileName)
    if (incremental) {
      saveIncremental(data, fullFileName, cohortDefinitionId = cohortIds)
    } else {
      readr::write_csv(data, fullFileName)
    }
  }
  fetchStats("#cohort_inclusion", "cohortInclusion.csv")
  fetchStats("#cohort_inc_result", "cohortIncResult.csv")
  fetchStats("#cohort_inc_stats", "cohortIncStats.csv")
  fetchStats("#cohort_summary_stats", "cohortSummaryStats.csv")
  
  sql <- "TRUNCATE TABLE #cohort_inclusion; 
    DROP TABLE #cohort_inclusion;
    
    TRUNCATE TABLE #cohort_inc_result; 
    DROP TABLE #cohort_inc_result;
    
    TRUNCATE TABLE #cohort_inc_stats; 
    DROP TABLE #cohort_inc_stats;
    
    TRUNCATE TABLE #cohort_summary_stats; 
    DROP TABLE #cohort_summary_stats;"
  DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                               sql = sql,
                                               progressBar = FALSE,
                                               reportOverallTime = FALSE,
                                               oracleTempSchema = oracleTempSchema)
}


.warnMismatchSqlInclusionStats <- function(sql, generateInclusionStats) {
  if (any(stringr::str_detect(string = sql, pattern = "_inclusion_result"), 
          stringr::str_detect(string = sql, pattern = "_inclusion_stats"), 
          stringr::str_detect(string = sql, pattern = "_summary_stats")
  )
  ) {
    if (isFALSE(generateInclusionStats)) {
      warning("The SQL template used to instantiate cohort was designed to output cohort inclusion statistics. 
              But, generateInclusionStats is set to False while instantiating cohort. 
              This may cause error and terminate cohort diagnositcs.")
    }
  } else {
    if (isTRUE(generateInclusionStats)) {
      warning("The SQL template used to instantiate cohort was designed to NOT output cohort inclusion statistics. 
              But, generateInclusionStats is set to TRUE while instantiating cohort. 
              This may cause error and terminate cohort diagnositcs.")
    }
  }
}




#' Get record counts for a set of cohort
#'
#' @description
#' This function get record count for a set of cohorts in the cohort table. Optionally, the inclusion rule statistics
#' are also counted.
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @template OracleTempSchema
#'
#' @template CdmDatabaseSchema
#' 
#' @template CohortSetSpecs
#' 
#' @template CohortSetReference
#'
#' @param cohortIds                   Provide a list of cohort IDs to get records for
#' @param includeInclusionStatsTables Should record count from inclusion stats table results be returned.
#' @return
#' A list with four tibble objects (cohort, inclusionTable, inclusionResult, inclusionStats, inclusionSummaryStats)                                 
#'
#' @export
recordCountOfInstantiatedCohorts <- function(connection,
                                             cdmDatabaseSchema,
                                             cohortDatabaseSchema,
                                             cohortTable,
                                             cohortIds,
                                             includeInclusionStatsTables = FALSE,
                                             cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                                             cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                                             cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                                             cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats")) {
  
  if (is.vector(x = cohortIds) && length(cohortIds) > 1) {
    cohortIds <- paste0(cohortIds, collapse = ",")
  }
  
  tables <- DatabaseConnector::getTableNames(connection = connection,
                                             databaseSchema = cdmDatabaseSchema
  )
  
  sql <- "SELECT cohort_definition_id, COUNT(*) COUNT 
    FROM @cohort_database_schema.@cohort_table 
    WHERE cohort_definition_id in (@cohort_id)
    group by cohort_definition_id ;"
  
  if (tolower(cohortTable) %in% tolower(tables)) {
  cohortTableRecords <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                                   sql = sql,
                                                                   cohort_database_schema = cohortDatabaseSchema,
                                                                   cohort_table = cohortTable,
                                                                   cohort_id = cohortIds,
                                                                   snakeCaseToCamelCase = TRUE) %>% 
    tidyr::tibble()
  } else {
    ParallelLogger::logWarn(cohortTable, ' not found while retrieving record counts.')
  }
  if (includeInclusionStatsTables) {
    
    if (tolower(cohortInclusionTable) %in% tolower(tables)) {
      cohortInclusionTableRecords <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                                                sql = sql,
                                                                                cohort_database_schema = cohortDatabaseSchema,
                                                                                cohort_table = cohortInclusionTable,
                                                                                cohort_id = cohortIds) %>% 
        tidyr::tibble()
    } else {
      ParallelLogger::logWarn(cohortInclusionTable, ' not found while retrieving record counts.')
      cohortInclusionTableRecords <- tidyr::tibble()
    }
    
    if (tolower(cohortInclusionResultTable) %in% tolower(tables)) {
      cohortInclusionResultTableRecords <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                                                      sql = sql,
                                                                                      cohort_database_schema = cohortDatabaseSchema,
                                                                                      cohort_table = cohortInclusionResultTable,
                                                                                      cohort_id = cohortIds) %>% 
        tidyr::tibble()
    } else {
      ParallelLogger::logWarn(cohortInclusionResultTable, ' not found while retrieving record counts.')
      cohortInclusionResultTableRecords <- tidyr::tibble()
    }
    
    if (tolower(cohortInclusionStatsTable) %in% tolower(tables)) {
      cohortInclusionStatsTableRecords <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                                                     sql = sql,
                                                                                     cohort_database_schema = cohortDatabaseSchema,
                                                                                     cohort_table = cohortInclusionStatsTable,
                                                                                     cohort_id = cohortIds) %>% 
        tidyr::tibble()
    } else {
      ParallelLogger::logWarn(cohortInclusionStatsTable, ' not found while retrieving record counts.')
      cohortInclusionStatsTableRecords <- tidyr::tibble()
    }
    
    if (tolower(cohortSummaryStatsTable) %in% tolower(tables)) {
      cohortSummaryStatsTableRecords <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                                                   sql = sql,
                                                                                   cohort_database_schema = cohortDatabaseSchema,
                                                                                   cohort_table = cohortSummaryStatsTable,
                                                                                   cohort_id = cohortIds) %>% 
        tidyr::tibble()
    } else {
      ParallelLogger::logWarn(cohortSummaryStatsTable, ' not found while retrieving record counts.')
      cohortSummaryStatsTableRecords <- tidyr::tibble()
    }
  } else {
    cohortInclusionTableRecords <- tidyr::tibble()
    cohortInclusionResultTableRecords <- tidyr::tibble()
    cohortInclusionStatsTableRecords <- tidyr::tibble()
    cohortSummaryStatsTableRecords <- tidyr::tibble()
  }
  
  count <- list(cohort = cohortTableRecords,
             inclusionTable = cohortInclusionTableRecords,
             inclusionResult = cohortInclusionResultTableRecords,
             inclusionStats = cohortInclusionStatsTableRecords,
             inclusionSummaryStats = cohortSummaryStatsTableRecords)
  return(count)
}
