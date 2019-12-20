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

#' Find source codes that do not roll up to their ancestor
#' 
#' @description
#' \code{findOrphanConcepts} searches for concepts that should belong to the concept sets in a cohort definition but don't, for
#' example because of missing source-to-standard concept maps, or erroneous hierachical relationships.
#' 
#' There are two ways to call this function:
#' \itemize{
#'   \item \code{findOrphanConcepts(connectionDetails, cdmDatabaseSchema, oracleTempSchema, baseUrl, cohortId)}
#'   \item \code{findOrphanConcepts(connectionDetails, cdmDatabaseSchema, oracleTempSchema, cohortJson, cohortSql)}
#' }
#'
#' @usage
#' NULL
#' 
#' @details 
#' Logically, this function performs the following steps for each concept set expression in the cohort definition:
#' 
#' \itemize{
#' \item  Given the concept set expression, find all included concepts.
#' \item  Find all names of those concepts, including synonyms, and the names of source concepts that map to them.
#' \item  Search for concepts (standard and source) that contain any of those names as substring.
#' \item  Filter those concepts to those that are not in the original set of concepts (i.e. orphans).
#' \item  Restrict the set of orphan concepts to those that appear in the CDM database and across network concept prevalence (as either source concept or standard concept).
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
#'
#' @return 
#' A data frame with orhan concepts, with counts per domain how often the code was encountered
#' in the CDM.
#' 
#' @export
findOrphanConcepts <- function(connectionDetails,
                               cdmDatabaseSchema,
                               oracleTempSchema = NULL,
                               baseUrl = NULL,
                               cohortId = NULL,
                               cohortJson = NULL,
                               cohortSql = NULL) {
  
  
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
#' @param useSourceValues      Use the source_value fields to find the codesa used in the data? If not,
#'                             this analysis will rely entirely on the source_concept_id fields instead. 
#'                             Note that, depending on the source data and ETL, it might be possible for the
#'                             source_value fields to contain patient-identifiable information by accident.
#'
#' @return 
#' A data frame with source codes, with counts per domain how often the code was encountered
#' in the CDM.
#' 
#' @export
findIncludedSourceCodes <- function(connectionDetails,
                                    cdmDatabaseSchema,
                                    oracleTempSchema = NULL,
                                    baseUrl = NULL,
                                    cohortId = NULL,
                                    cohortJson = NULL,
                                    cohortSql = NULL,
                                    byMonth = FALSE,
                                    useSourceValues = FALSE) {
  if (is.null(baseUrl) && is.null(cohortJson)) {
    stop("Must provide either baseUrl and cohortId, or cohortJson and cohortSql")
  }
  if (!is.null(cohortJson) && !is.character(cohortJson)) {
    stop("cohortJson should be character (a JSON string).") 
  }
  start <- Sys.time()
  if (is.null(cohortJson)) {
    cohortExpression <- ROhdsiWebApi::getCohortDefinitionExpression(definitionId = cohortId, baseUrl = baseUrl)
    cohortJson <- cohortExpression$expression
    cohortSql <- ROhdsiWebApi::getCohortDefinitionSql(definitionId = cohortId, 
                                                      baseUrl = baseUrl,
                                                      generateStats = FALSE)
  }

  # outputFile <- "c:/temp/report.html"
  
  cohortDefinition <- RJSONIO::fromJSON(cohortJson)
  
  connection <- DatabaseConnector::connect(connectionDetails) 
  on.exit(DatabaseConnector::disconnect(connection))
  
  ParallelLogger::logInfo("Instantiating concept sets")
  instantiateConceptSets(connection, cdmDatabaseSchema, oracleTempSchema, cohortSql)
  
  ParallelLogger::logInfo("Counting codes in concept sets")
  sql <- SqlRender::loadRenderTranslateSql("CohortSourceCodes.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connectionDetails$dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           by_month = byMonth,
                                           use_source_values = useSourceValues)
  counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  
  getConceptSetName <- function(conceptSet) {
    return(data.frame(conceptSetId = conceptSet$id,
                      conceptSetName = conceptSet$name))
  }
  conceptSetNames <- lapply(cohortDefinition$ConceptSets, getConceptSetName)
  conceptSetNames <- do.call(rbind, conceptSetNames)
  counts <- merge(conceptSetNames, counts)
  
  if (byMonth) {
    sql <- SqlRender::loadRenderTranslateSql("ObservedPerCalendarMonth.sql",
                                             packageName = "StudyDiagnostics",
                                             dbms = connectionDetails$dbms,
                                             oracleTempSchema = oracleTempSchema,
                                             cdm_database_schema = cdmDatabaseSchema)
    backgroundCounts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
    
    sql <- "TRUNCATE TABLE #Codesets; DROP TABLE #Codesets;"
    DatabaseConnector::renderTranslateExecuteSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    
    backgroundCounts$startCount[is.na(backgroundCounts$startCount)] <- 0
    backgroundCounts$endCount[is.na(backgroundCounts$endCount)] <- 0
    backgroundCounts$net <- backgroundCounts$startCount - backgroundCounts$endCount
    backgroundCounts$time <- backgroundCounts$eventYear + (backgroundCounts$eventMonth - 1) / 12
    backgroundCounts <- backgroundCounts[order(backgroundCounts$time), ]
    backgroundCounts$backgroundCount <- cumsum(backgroundCounts$net)
    backgroundCounts$backgroundCount <- backgroundCounts$backgroundCount + backgroundCounts$endCount
    counts <- merge(counts, backgroundCounts[, c("eventYear", "eventMonth", "backgroundCount")], all.x = TRUE)
    
    if (any(is.na(counts$backgroundCount))) {
      stop("code counts in calendar months without observation period starts or ends. Need to do some lookup here") 
    }
    
    counts$proportion <- counts$personCount / counts$backgroundCount
    counts <- counts[order(counts$conceptSetId, 
                           counts$conceptId, 
                           counts$sourceConceptName, 
                           counts$sourceVocabularyId,
                           counts$eventYear,
                           counts$eventMonth), ]
  } else {
    counts <- counts[order(counts$conceptSetId, 
                           counts$conceptId, 
                           counts$sourceConceptName, 
                           counts$sourceVocabularyId), ]
  }
  delta <- Sys.time() - start
  writeLines(paste("Finding source codes took", signif(delta, 3), attr(delta, "units")))
  return(counts)
}

instantiateConceptSets <- function(connection, cdmDatabaseSchema, oracleTempSchema, cohortSql) {
  sql <- gsub("with primary_events.*", "", cohortSql)   
  sql <- SqlRender::render(sql, vocabulary_database_schema = cdmDatabaseSchema)
  sql <- SqlRender::translate(sql,
                              targetDialect = connection@dbms,
                              oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(connection, sql)
}


