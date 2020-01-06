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
breakDownIndexEvents <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 oracleTempSchema = NULL,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 baseUrl = NULL,
                                 cohortId = NULL,
                                 cohortJson = NULL,
                                 cohortSql = NULL,
                                 instantiatedCohortId = cohortId) {
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
  primaryCodesetIds <- aggregate(codeSetIds ~ domain, primaryCodesetIds, c)
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails) 
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (!checkIfCohortInstantiated(connection = connection,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 instantiatedCohortId = instantiatedCohortId)) {
    warning("Cohort with ID ", instantiatedCohortId, " appears to be empty. Was it instantiated?")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Breaking down index events took", signif(delta, 3), attr(delta, "units")))
    return(data.frame())
  } 
  ParallelLogger::logInfo("Instantiating concept sets")
  StudyDiagnostics:::instantiateConceptSets(connection, cdmDatabaseSchema, oracleTempSchema, cohortSql)
  
  ParallelLogger::logInfo("Computing counts")
  domains <- readr::read_csv(system.file("csv", "domains.csv", package = "StudyDiagnostics"), col_types = readr::cols())
  getCounts <- function(row) {
    domain <- domains[domains$domain == row$domain, ]
    sql <- SqlRender::loadRenderTranslateSql("CohortEntryBreakdown.sql",
                                             packageName = "StudyDiagnostics",
                                             dbms = connectionDetails$dbms,
                                             oracleTempSchema = oracleTempSchema,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             cohort_database_schema = cohortDatabaseSchema,
                                             cohort_table = cohortTable,
                                             cohort_id = instantiatedCohortId,
                                             domain_table = domain$domainTable,
                                             domain_start_date = domain$domainStartDate,
                                             domain_concept_id = domain$domainConceptId,
                                             primary_codeset_ids = row$codeSetIds)
    counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  }
  counts <- lapply(split(primaryCodesetIds, 1:nrow(primaryCodesetIds)), getCounts)
  counts <- do.call(rbind, counts)
  rownames(counts) <- NULL
  counts <- counts[order(-counts$conceptCount), ]
 
  ParallelLogger::logInfo("Cleaning up concept sets")
  sql <- "TRUNCATE TABLE #Codesets; DROP TABLE #Codesets;"
  DatabaseConnector::renderTranslateExecuteSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Breaking down index events took", signif(delta, 3), attr(delta, "units")))
  return(counts)
}

checkIfCohortInstantiated <- function(connection, cohortDatabaseSchema, cohortTable, instantiatedCohortId) {
  sql <- "SELECT COUNT(*) FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id = @cohort_id;"
  count <- DatabaseConnector::renderTranslateQuerySql(connection = connection, 
                                                      sql,
                                                      cohort_database_schema = cohortDatabaseSchema,
                                                      cohort_table = cohortTable,
                                                      cohort_id = instantiatedCohortId)
  return(count > 0)
}
