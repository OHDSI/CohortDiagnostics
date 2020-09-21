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


getOmopVocabularyTables <-
  function(connection = NULL,
           cdmDatabaseSchema,
           cohortDatabaseSchema,
           uniqueConceptIdsTable = '#unique_concept_ids',
           vocabularyTableNames = c('concept',
                                    'conceptAncestor',
                                    'conceptClass',
                                    'conceptRelationship',
                                    'conceptSynonym',
                                    'domain',
                                    'relationship',
                                    'vocabulary'),
           exportFolder) {
    if (!is.null(connectionDetails)) {
      if (is.null(connection)) {
        connection <- DatabaseConnector::connect(connectionDetails)
        on.exit(DatabaseConnector::disconnect(connection))
      }
    }
    if (is.null(connection)) {
      ParallelLogger::logWarn('no connection provided')
    }
    
    vocabularyTableNames <-
      tidyr::tibble(vocabularyTableNames = vocabularyTableNames) %>%
      dplyr::mutate(serverTableNames =
                      SqlRender::camelCaseToSnakeCase(vocabularyTableNames) %>%
                      tolower())
    
    vocabularyTablesInCdmDatabaseSchema <-
      tidyr::tibble(serverTableNames = DatabaseConnector::getTableNames(connection,
                                                                        cdmDatabaseSchema) %>%
                      tolower()) %>%
      dplyr::filter(.data$serverTableNames %in%
                      (SqlRender::camelCaseToSnakeCase(string = vocabularyTableNames$serverTableNames))) %>%
      dplyr::left_join(vocabularyTableNames)
    
    if (nrow(vocabularyTablesInCdmDatabaseSchema) == 0) {
      ParallelLogger::logWarn("Vocabulary tables not found in ", cdmDatabaseSchema)
      stop("No vocabulary retrieved")
      return()
    }
    
    for (i in (1:nrow(vocabularyTablesInCdmDatabaseSchema))) {
      print(vocabularyTablesInCdmDatabaseSchema$vocabularyTableNames[[i]] )
      if (vocabularyTablesInCdmDatabaseSchema$vocabularyTableNames[[i]] %in% c('concept', 'conceptSynonym')) {
        sql <- "select a.* from @cdm_database_schema.@table a
        inner join (select distinct concept_id from @cohortDatabaseSchema.@uniqueConceptIdsTable) b
        on a.concept_id = b.concept_id"
      } else if (vocabularyTablesInCdmDatabaseSchema$vocabularyTableNames[[i]] %in% c('conceptAncestor')) {
        sql <- "select a.* from @cdm_database_schema.@table a
        left join (select distinct concept_id from @cohortDatabaseSchema.@uniqueConceptIdsTable) b
        on a.ancestor_concept_id = b.concept_id or
        a.descendant_concept_id = b.concept_id"
      } else if (vocabularyTablesInCdmDatabaseSchema$vocabularyTableNames[[i]] %in% c('conceptRelationship')) {
        sql <- "select a.* from @cdm_database_schema.@table a
        left join (select distinct concept_id from @cohortDatabaseSchema.@uniqueConceptIdsTable) b
        on a.concept_id_1 = b.concept_id or
        a.concept_id_2 = b.concept_id"
      } 
      if (vocabularyTablesInCdmDatabaseSchema$vocabularyTableNames[[i]] %in% c('concept'
                                                                               , 'conceptSynonym'
                                                                               , 'conceptAncestor'
                                                                               , 'conceptRelationship')) {
        assign(
          x = vocabularyTablesInCdmDatabaseSchema[i,]$serverTableNames,
          DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                     sql = sql,
                                                     cdm_database_schema = cdmDatabaseSchema,
                                                     uniqueConceptIdsTable = uniqueConceptIdsTable,
                                                     table = vocabularyTablesInCdmDatabaseSchema$serverTableNames[[i]]) %>%
            tidyr::tibble())
      } else if (vocabularyTablesInCdmDatabaseSchema$vocabularyTableNames[[i]] %in% c('domain',
                                                                                      'relationship',
                                                                                      'vocabulary',
                                                                                      'conceptClass')) {
        sql <- "select a.* from @cdm_database_schema.@table a"
        assign(
          x = vocabularyTablesInCdmDatabaseSchema[i,]$serverTableNames,
          DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                     sql = sql,
                                                     cdm_database_schema = cdmDatabaseSchema,
                                                     table = vocabularyTablesInCdmDatabaseSchema$serverTableNames[[i]]) %>%
            tidyr::tibble())
      }
      
      if (nrow(get(vocabularyTablesInCdmDatabaseSchema[i,]$serverTableNames)) > 0) {
        readr::write_excel_csv(
          x = get(vocabularyTablesInCdmDatabaseSchema[i,]$serverTableNames),
          path = file.path(
            exportFolder,
            paste0(vocabularyTablesInCdmDatabaseSchema[i,]$serverTableNames,
                   ".csv"
            ) %>% tolower()
          )
        )
      }
      
      ParallelLogger::logInfo('  Wrote ',
                              basename(file.path(
                                exportFolder,
                                paste0(
                                  vocabularyTablesInCdmDatabaseSchema[i,]$serverTableNames,
                                  ".csv"
                                ) %>% tolower()
                              )),
                              ' with ',
                              nrow(
                                get(
                                  vocabularyTablesInCdmDatabaseSchema[i,]$serverTableNames
                                )
                              ),
                              " records.")
    }
  }



#' @title Resolves cohort sql to concept_ids
#' 
#' @description
#' Resolves cohort sql to concept_ids
#'
#' @template Connection                  
#' @param cdmDatabaseSchema         DatabaseSchema where the omop vocabulary files are located.
#' @param cohort                    A tibble data frame object with atleast two columns. cohortId refering to the integer
#'                                  id that identifies a cohort definition. And sql, which is the cohort definition
#'                                  in OHDSI SQL dialect. It may contain parameters designed to be replaced 
#'                                  by SqlRender. The standard form the cohort definition SQL is generated is using 
#'                                  circe-be by WebApi and Atlas. The 'cohort' table in Cohort Diagnostics results
#'                                  data model satisfies this requirement.
#' @param databaseId                A text string corresponding to the id of the database.
#' @param conceptSetsConceptId      Table to store the output on the database system. This is a temporary table.
#' @template oracleTempSchema
#' @return
#' Tibble Data Frame object
#'
#' @export
#'
resolveCohortSqlToConceptIds <- function(connection = NULL,
                                         connectionDetails = NULL,
                                         cdmDatabaseSchema,
                                         databaseId,
                                         conceptSetsConceptId = "#resolved_concept_set",
                                         oracleTempSchema = NULL,
                                         cohort) {
  if (!is.null(connectionDetails)) {
    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
  }
  if (is.null(connection)) {
    ParallelLogger::logWarn('no connection provided')
  }
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertNames(x = colnames(cohort), 
                         must.include = c('cohortId', 'sql'),
                         add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  checkmate::assertTibble(x = cohort %>% 
                            dplyr::select(.data$cohortId, .data$sql), 
                          types = c('double', 'character'), 
                          any.missing = FALSE, 
                          min.rows = 1, 
                          min.cols = 2,
                          add = errorMessage)
  checkmate::assertCharacter(x = databaseId, 
                             any.missing = FALSE, 
                             min.len = 1,  
                             max.len = 1,
                             null.ok = FALSE,
                             add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  conceptSetSql <- list()
  for (i in (1:nrow(cohort))) {
    conceptSetSql[[i]] <- CohortDiagnostics::extractConceptSetsSqlFromCohortSql(cohortSql = cohort$sql[[i]]) %>% 
      dplyr::select(.data$conceptSetSql)
  }
  conceptSetSql <- dplyr::bind_rows(conceptSetSql) %>% dplyr::distinct()
  
  uniqueConceptSetSql <- conceptSetSql %>%
    dplyr::select(.data$conceptSetSql) %>%
    dplyr::distinct() %>%
    dplyr::mutate(uniqueConceptSetId = dplyr::row_number())
  
  CohortDiagnostics:::instantiateUniqueConceptSets(uniqueConceptSets = uniqueConceptSetSql, 
                                                   connection = connection,
                                                   cdmDatabaseSchema = cdmDatabaseSchema, 
                                                   oracleTempSchema = oracleTempSchema)
  
  cohortCodeSets <- list()
  for (i in (1:nrow(cohort))) {
    codeSetSql <- 
      CohortDiagnostics::extractConceptSetsSqlFromCohortSql(cohortSql = 
                                                              cohort$sql[[i]])
    for (j in (1:nrow(codeSetSql))) {
      if (i == 1 && j == 1) {
        DatabaseConnector::insertTable(connection = connection,
                                       tableName = conceptSetsConceptId,
                                       data = DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                                                         sql = codeSetSql$conceptSetSql[[j]],
                                                                                         vocabulary_database_schema = cdmDatabaseSchema, 
                                                                                         snakeCaseToCamelCase = TRUE) %>% 
                                         dplyr::mutate(cohortId = cohort$cohortId[[i]],
                                                       databaseId = !!databaseId) %>% 
                                         dplyr::select(.data$databaseId, .data$cohortId, .data$codesetId, .data$conceptId),
                                       dropTableIfExists = TRUE,
                                       createTable = TRUE,
                                       tempTable = TRUE,
                                       oracleTempSchema = oracleTempSchema,
                                       camelCaseToSnakeCase = TRUE)
      } else {
        sql <- paste0("insert into @concept_sets_conceptId (database_id, cohort_id, codeset_id, concept_id) 
                       SELECT '@database_id' as database_id, 
                               @cohort_id as cohort_id, 
                               codeset_id, concept_id FROM (",
                      codeSetSql$conceptSetSql[[j]],
                      ") fin;"
        )
        DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                     sql = sql,
                                                     vocabulary_database_schema = cdmDatabaseSchema,
                                                     database_id = databaseId,
                                                     concept_sets_conceptId = conceptSetsConceptId,
                                                     cohort_id = cohort$cohortId[[i]])
      }
    }
  }
  cohortCodeSets <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                               sql = "select * from @concept_sets_conceptId",
                                                               snakeCaseToCamelCase = TRUE,
                                                               concept_sets_conceptId = conceptSetsConceptId
  ) %>% 
    tidyr::tibble()
  return(cohortCodeSets)
}
