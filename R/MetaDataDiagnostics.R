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

#' Find (source) concepts that do not roll up to their ancestor(s)
#'
#' @description
#' Searches for concepts that should belong to the set of concepts but don't, for example because of
#' missing source-to-standard concept maps, or erroneous hierarchical relationships.
#'
#' @details
#' Logically, this function performs the following steps for the input set of concept IDs:
#' \itemize{
#'   \item Find all names of the input concepts, including synonyms, and the names of source concepts
#'         that map to them.
#'   \item Search for concepts (standard and source) that contain any of those names as substring.
#'   \item Filter those concepts to those that are not in the original set of concepts (i.e. orphans).
#'   \item Restrict the set of orphan concepts to those that appear in the CDM database and across
#'         network concept prevalence (as either source concept or standard concept).
#' }
#'
#'
#' @template Connection
#'
#' @template OracleTempSchema
#'
#' @template ConceptCounts
#'
#' @template CdmDatabaseSchema
#'
#' @param conceptIds   A vector of concept IDs for which we want to find orphans.
#'
#' @return
#' A data frame with orphan concepts, with counts how often the code was encountered in the CDM.
#'
#' @export
findOrphanConcepts <- function(connectionDetails = NULL,
                               connection = NULL,
                               cdmDatabaseSchema,
                               oracleTempSchema = NULL,
                               conceptIds,
                               conceptCountsDatabaseSchema = cdmDatabaseSchema,
                               conceptCountsTable = "concept_counts",
                               conceptCountsTableIsTemp = FALSE) {
  return(.findOrphanConcepts(connectionDetails = connectionDetails,
                             connection = connection,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             oracleTempSchema = oracleTempSchema,
                             conceptIds = conceptIds,
                             conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                             conceptCountsTable = conceptCountsTable,
                             conceptCountsTableIsTemp = conceptCountsTableIsTemp,
                             orphanConceptTable = '#recommended_concepts'))
}

.findOrphanConcepts <- function(connectionDetails = NULL,
                                connection = NULL,
                                cdmDatabaseSchema,
                                oracleTempSchema = NULL,
                                conceptIds = c(),
                                useCodesetTable = FALSE,
                                codesetId = 1,
                                conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                conceptCountsTable = "concept_counts",
                                conceptCountsTableIsTemp = FALSE,
                                instantiatedCodeSets = "#InstConceptSets",
                                orphanConceptTable = '#recommended_concepts') {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql("OrphanCodes.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           work_database_schema = conceptCountsDatabaseSchema,
                                           concept_counts_table = conceptCountsTable,
                                           concept_counts_table_is_temp = conceptCountsTableIsTemp,
                                           concept_ids = conceptIds,
                                           use_codesets_table = useCodesetTable,
                                           orphan_concept_table = orphanConceptTable,
                                           instantiated_code_sets = instantiatedCodeSets,
                                           codeset_id = codesetId)
  DatabaseConnector::executeSql(connection, sql)
  ParallelLogger::logTrace("- Fetching orphan concepts from server")
  sql <- "SELECT * FROM @orphan_concept_table;"
  orphanConcepts <- DatabaseConnector::renderTranslateQuerySql(sql = sql,
                                                               connection = connection,
                                                               oracleTempSchema = oracleTempSchema,
                                                               orphan_concept_table = orphanConceptTable,
                                                               snakeCaseToCamelCase = TRUE) %>% 
    tidyr::tibble()
  
  # For debugging:
  # x <- querySql(connection, "SELECT * FROM #starting_concepts;")
  # View(x)
  # 
  # x <- querySql(connection, "SELECT * FROM #concept_synonyms;")
  # View(x)
  # 
  # x <- querySql(connection, "SELECT * FROM #search_strings;")
  # View(x)
  # 
  # x <- querySql(connection, "SELECT * FROM #search_str_top1000;")
  # View(x)
  # 
  # x <- querySql(connection, "SELECT * FROM #search_string_subset;")
  # View(x)
  # 
  # x <- querySql(connection, "SELECT * FROM #recommended_concepts;")
  # View(x)
  
  ParallelLogger::logTrace("- Dropping orphan temp tables")
  sql <- SqlRender::loadRenderTranslateSql("DropOrphanConceptTempTables.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(connection = connection, 
                                sql = sql, 
                                progressBar = FALSE, 
                                reportOverallTime = FALSE)
  return(orphanConcepts)
}

#' Find orphan concepts for all concept sets in a cohort
#'
#' @description
#' Searches for concepts that should belong to the concept sets in a cohort definition but don't, for
#' example because of missing source-to-standard concept maps, or erroneous hierarchical
#' relationships.
#'
#' @details
#' Logically, this function performs the following steps for each concept set expression in the cohort
#' definition:
#' \itemize{
#'   \item Given the concept set expression, find all included concepts.
#'   \item Find all names of the input concepts, including synonyms, and the names of source concepts
#'         that map to them.
#'   \item Search for concepts (standard and source) that contain any of those names as substring.
#'   \item Filter those concepts to those that are not in the original set of concepts (i.e. orphans).
#'   \item Restrict the set of orphan concepts to those that appear in the CDM database and across
#'         network concept prevalence (as either source concept or standard concept).
#' }
#'
#'
#' @template Connection
#'
#' @template OracleTempSchema
#'
#' @template ConceptCounts
#'
#' @template CdmDatabaseSchema
#'
#' @param baseUrl          The base URL for the WebApi instance, for example:
#'                         "http://server.org:80/WebAPI". Needn't be provided if \code{cohortJson} is
#'                         provided.
#' @param webApiCohortId   The ID of the cohort in the WebAPI instance. Needn't be provided if
#'                         \code{cohortJson} is provided.
#' @param cohortJson       A character string containing the JSON of a cohort definition. Needn't be
#'                         provided if \code{baseUrl} and \code{webApiCohortId} are provided.
#'
#' @return
#' A data frame with orphan concepts, with counts how often the code was encountered in the CDM.
#'
#' @export
findCohortOrphanConcepts <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     oracleTempSchema = NULL,
                                     baseUrl = NULL,
                                     webApiCohortId = NULL,
                                     cohortJson = NULL,
                                     conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                     conceptCountsTable = "concept_counts",
                                     conceptCountsTableIsTemp = FALSE) {
  
  if (is.null(baseUrl) && is.null(cohortJson)) {
    stop("Must provide either baseUrl and webApiCohortId, or cohortJson and cohortSql")
  }
  if (!is.null(cohortJson) && !is.character(cohortJson)) {
    stop("cohortJson should be character (a JSON string).")
  }
  start <- Sys.time()
  if (is.null(cohortJson)) {
    cohortDefinition <- ROhdsiWebApi::getCohortDefinition(cohortId = webApiCohortId,
                                                          baseUrl = baseUrl)
    cohortDefinition <- cohortDefinition$expression
  } else {
    cohortDefinition <- RJSONIO::fromJSON(cohortJson)
  }
  getConceptIdFromItem <- function(item) {
    if (item$isExcluded) {
      return(NULL)
    } else {
      return(item$concept$CONCEPT_ID)
    }
  }
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  conceptSets <- cohortDefinition$ConceptSets
  allOrphanConcepts <- data.frame()
  for (conceptSet in conceptSets) {
    ParallelLogger::logInfo("Finding orphan concepts for concept set '", conceptSet$name, "'")
    conceptIds <- lapply(conceptSet$expression$items, getConceptIdFromItem)
    conceptIds <- do.call(c, conceptIds)
    orphanConcepts <- findOrphanConcepts(connection = connection,
                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                         oracleTempSchema = oracleTempSchema,
                                         conceptIds = conceptIds,
                                         conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                                         conceptCountsTable = conceptCountsTable,
                                         conceptCountsTableIsTemp = conceptCountsTableIsTemp)
    if (nrow(orphanConcepts) > 0) {
      orphanConcepts$conceptSetId <- conceptSet$id
      orphanConcepts$conceptSetName <- conceptSet$name
      allOrphanConcepts <- rbind(allOrphanConcepts, orphanConcepts)
    }
  }
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Finding orphan concepts took",
                                signif(delta, 3),
                                attr(delta, "units")))
  return(allOrphanConcepts)
}

#' Create concept counts table
#'
#' @description
#' Create a table with counts of how often each concept ID occurs in the CDM.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template ConceptCounts
#' 
#' @template OracleTempSchema
#' 
#' @return
#' The function will by default return \code{DatabaseConnector::executeSql} output, by 
#' attempting to create a table on dbms. In addition, if \code{getConceptCountsTable = TRUE}
#' then a tibble data frame copy of the created table will be retuned back to R.
#' @export
createConceptCountsTable <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     oracleTempSchema = NULL,
                                     conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                     conceptCountsTable = "concept_counts",
                                     conceptCountsTableIsTemp = FALSE) {
  ParallelLogger::logInfo("Creating internal concept counts table")
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql("CreateConceptCountTable.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           work_database_schema = conceptCountsDatabaseSchema,
                                           concept_counts_table = conceptCountsTable,
                                           table_is_temp = conceptCountsTableIsTemp)
  DatabaseConnector::executeSql(connection, sql)
}

#' Check source codes used in a cohort definition
#'
#' @description
#' This function first extracts all concept sets used in a cohort definition. Then, for each concept
#' set the concept found in the CDM database the contributing source codes are identified.
#'
#' @template Connection
#' @template CdmDatabaseSchema
#' @template OracleTempSchema
#' @template CohortDef
#' @template WebApiCohortId
#' @param byMonth           Compute counts by month? If FALSE, only overall counts are computed.
#' @param useSourceValues   Use the source_value fields to find the codes used in the data? If not,
#'                          this analysis will rely entirely on the source_concept_id fields instead.
#'                          Note that, depending on the source data and ETL, it might be possible for
#'                          the source_value fields to contain patient-identifiable information by
#'                          accident.
#'
#' @return
#' A data frame with source codes, with counts per domain how often the code was encountered in the
#' CDM.
#'
#' @export
findCohortIncludedSourceConcepts <- function(connectionDetails = NULL,
                                             connection = NULL,
                                             cdmDatabaseSchema,
                                             oracleTempSchema = NULL,
                                             baseUrl = NULL,
                                             webApiCohortId = NULL,
                                             cohortJson = NULL,
                                             cohortSql = NULL,
                                             byMonth = FALSE,
                                             useSourceValues = FALSE) {
  if (is.null(baseUrl) && is.null(cohortJson)) {
    stop("Must provide either baseUrl and webApiCohortId, or cohortJson and cohortSql")
  }
  if (!is.null(cohortJson) && !is.character(cohortJson)) {
    stop("cohortJson should be character (a JSON string).")
  }
  start <- Sys.time()
  if (is.null(cohortJson)) {
    cohortDefinition <- ROhdsiWebApi::getCohortDefinition(cohortId = webApiCohortId,
                                                          baseUrl = baseUrl)
    cohortDefinition <- cohortDefinition$expression
    cohortSql <- ROhdsiWebApi::getCohortSql(cohortDefinition = cohortDefinition,
                                            baseUrl = baseUrl,
                                            generateStats = FALSE)
  } else {
    cohortDefinition <- RJSONIO::fromJSON(cohortJson)
  }
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  ParallelLogger::logInfo("Instantiating concept sets")
  instantiateConceptSets(connection, cdmDatabaseSchema, oracleTempSchema, cohortSql)
  
  ParallelLogger::logInfo("Counting codes in concept sets")
  sql <- SqlRender::loadRenderTranslateSql("CohortSourceCodes.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           by_month = byMonth,
                                           use_source_values = useSourceValues)
  counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  
  getConceptSetName <- function(conceptSet) {
    return(data.frame(conceptSetId = conceptSet$id, conceptSetName = conceptSet$name))
  }
  conceptSetNames <- lapply(cohortDefinition$ConceptSets, getConceptSetName)
  conceptSetNames <- do.call(rbind, conceptSetNames)
  counts <- merge(conceptSetNames, counts)
  
  if (byMonth) {
    sql <- SqlRender::loadRenderTranslateSql("ObservedPerCalendarMonth.sql",
                                             packageName = "CohortDiagnostics",
                                             dbms = connection@dbms,
                                             oracleTempSchema = oracleTempSchema,
                                             cdm_database_schema = cdmDatabaseSchema)
    backgroundCounts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
    backgroundCounts$startCount[is.na(backgroundCounts$startCount)] <- 0
    backgroundCounts$endCount[is.na(backgroundCounts$endCount)] <- 0
    backgroundCounts$net <- backgroundCounts$startCount - backgroundCounts$endCount
    backgroundCounts$time <- backgroundCounts$eventYear + (backgroundCounts$eventMonth - 1)/12
    backgroundCounts <- backgroundCounts[order(backgroundCounts$time), ]
    backgroundCounts$backgroundSubjects <- cumsum(backgroundCounts$net)
    backgroundCounts$backgroundSubjects <- backgroundCounts$backgroundSubjects +
      backgroundCounts$endCount
    counts <- merge(counts, backgroundCounts[, c("eventYear",
                                                 "eventMonth",
                                                 "backgroundSubjects")], all.x = TRUE)
    
    if (any(is.na(counts$backgroundCount))) {
      stop("code counts in calendar months without observation period starts or ends. Need to do some lookup here")
    }
    
    counts$proportion <- counts$personCount/counts$backgroundCount
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
  sql <- "TRUNCATE TABLE #Codesets; DROP TABLE #Codesets;"
  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               sql,
                                               oracleTempSchema = oracleTempSchema,
                                               progressBar = FALSE,
                                               reportOverallTime = FALSE)
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Finding source codes took",
                                signif(delta, 3),
                                attr(delta, "units")))
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
