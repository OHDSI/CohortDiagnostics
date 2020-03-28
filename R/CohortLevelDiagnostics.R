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

#' Count the cohort(s)
#'
#' @description
#' Computes the subject and entry count per cohort
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @param cohortIds            The cohort definition ID(s0 used to reference the cohort in the cohort
#'                             table. If left empty, all cohorts in the table will be included.
#'
#' @return
#' A data frame with cohort counts
#'
#' @export
getCohortCounts <- function(connectionDetails = NULL,
                            connection = NULL,
                            cohortDatabaseSchema,
                            cohortTable = "cohort",
                            cohortIds = c()) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CohortCounts.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cohort_ids = cohortIds)
  counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Counting cohorts took",
                                signif(delta, 3),
                                attr(delta, "units")))
  return(counts)
  
}

#' Break down index events
#'
#' @description
#' For the concepts included in the index event definition, count how often they are encountered at
#' the cohort index date.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template OracleTempSchema
#'
#' @template CohortTable
#'
#' @template CohortDef
#'
#' @param cohortId   The cohort definition ID used to reference the cohort in the cohort table.
#'
#' @return
#' A data frame with concepts, and per concept the count of how often the concept was encountered at
#' the index date.
#'
#' @export
breakDownIndexEvents <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 oracleTempSchema = NULL,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 baseUrl = NULL,
                                 webApiCohortId = NULL,
                                 cohortJson = NULL,
                                 cohortSql = NULL,
                                 cohortId = cohortId) {
  if (is.null(baseUrl) && is.null(cohortJson)) {
    stop("Must provide either baseUrl and webApiCohortId, or cohortJson and cohortSql")
  }
  if (!is.null(cohortJson) && !is.character(cohortJson)) {
    stop("cohortJson should be character (a JSON string).")
  }
  start <- Sys.time()
  if (is.null(cohortJson)) {
    ParallelLogger::logInfo("Retrieving cohort definition from WebAPI")
    cohortExpression <- ROhdsiWebApi::getCohortDefinitionExpression(definitionId = webApiCohortId,
                                                                    baseUrl = baseUrl)
    cohortJson <- cohortExpression$expression
    cohortSql <- ROhdsiWebApi::getCohortDefinitionSql(definitionId = webApiCohortId,
                                                      baseUrl = baseUrl,
                                                      generateStats = FALSE)
  }
  cohortDefinition <- RJSONIO::fromJSON(cohortJson)

  getCodeSetId <- function(criterion) {
    if (is.list(criterion)) {
      criterion$CodesetId
    } else if (is.vector(criterion)) {
      return(criterion["CodesetId"])
    } else {
      return(NULL)
    }
  }
  getCodeSetIds <- function(criterionList) {
    codeSetIds <- lapply(criterionList, getCodeSetId)
    codeSetIds <- do.call(c, codeSetIds)
    if (is.null(codeSetIds)) {
      return(NULL)
    } else {
      return(tibble::tibble(domain = names(criterionList), codeSetIds = codeSetIds))
    }
  }
  primaryCodesetIds <- lapply(cohortDefinition$PrimaryCriteria$CriteriaList, getCodeSetIds)
  primaryCodesetIds <- do.call(rbind, primaryCodesetIds)
  primaryCodesetIds <- primaryCodesetIds[!is.na(primaryCodesetIds$codeSetIds), ]
  if (is.null(primaryCodesetIds) || nrow(primaryCodesetIds) == 0) {
    warning("No primary event criteria concept sets found")
    return(data.frame())
  }
  pasteIds <- function(row) {
    return(tibble::tibble(domain = row$domain[1],
                          codeSetIds = paste(row$codeSetIds, collapse = ", ")))
  }
  primaryCodesetIds <- lapply(split(primaryCodesetIds, primaryCodesetIds$domain), pasteIds)
  primaryCodesetIds <- dplyr::bind_rows(primaryCodesetIds)
  # primaryCodesetIds <- aggregate(codeSetIds ~ domain, primaryCodesetIds, c)

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (!checkIfCohortInstantiated(connection = connection,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 cohortId = cohortId)) {
    warning("Cohort with ID ", cohortId, " appears to be empty. Was it instantiated?")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Breaking down index events took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    return(data.frame())
  }
  ParallelLogger::logInfo("Instantiating concept sets")
  instantiateConceptSets(connection, cdmDatabaseSchema, oracleTempSchema, cohortSql)

  ParallelLogger::logInfo("Computing counts")
  domains <- readr::read_csv(system.file("csv", "domains.csv", package = "CohortDiagnostics"),
                             col_types = readr::cols())
  getCounts <- function(row) {
    domain <- domains[domains$domain == row$domain, ]
    sql <- SqlRender::loadRenderTranslateSql("CohortEntryBreakdown.sql",
                                             packageName = "CohortDiagnostics",
                                             dbms = connection@dbms,
                                             oracleTempSchema = oracleTempSchema,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             cohort_database_schema = cohortDatabaseSchema,
                                             cohort_table = cohortTable,
                                             cohort_id = cohortId,
                                             domain_table = domain$domainTable,
                                             domain_start_date = domain$domainStartDate,
                                             domain_concept_id = domain$domainConceptId,
                                             primary_codeset_ids = row$codeSetIds)
    counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
    return(counts)
  }
  counts <- lapply(split(primaryCodesetIds, 1:nrow(primaryCodesetIds)), getCounts)
  counts <- do.call(rbind, counts)
  rownames(counts) <- NULL
  counts <- counts[order(-counts$conceptCount), ]

  ParallelLogger::logInfo("Cleaning up concept sets")
  sql <- "TRUNCATE TABLE #Codesets; DROP TABLE #Codesets;"
  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               sql,
                                               progressBar = FALSE,
                                               reportOverallTime = FALSE)

  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Breaking down index events took",
                                signif(delta, 3),
                                attr(delta, "units")))
  return(counts)
}

checkIfCohortInstantiated <- function(connection, cohortDatabaseSchema, cohortTable, cohortId) {
  sql <- "SELECT COUNT(*) FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id = @cohort_id;"
  count <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                      sql,
                                                      cohort_database_schema = cohortDatabaseSchema,
                                                      cohort_table = cohortTable,
                                                      cohort_id = cohortId)
  return(count > 0)
}
