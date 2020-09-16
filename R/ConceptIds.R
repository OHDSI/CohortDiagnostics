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


#' @title
#' Get all unique concept id's referenced in the cohort diagnostics
#'
#' @description
#' Get all unique concept id's referenced in the cohort diagnostics
#'
#' @param exportFolder 	The folder where the output is exported by Cohort Diagnostics.
#'                      If this folder does not exist, or does not have the searched file
#'                      the function will return an error.
#' @return
#' Returns a vector unique conceptId's from various objects in the export folder.
#'
#' @export
#'
getUniqueConceptIds <-
  function(exportFolder) {
    databaseTables <- c('covariate_ref',
                        'included_source_concept',
                        'index_event_breakdown',
                        'orphan_concept',
                        'temporal_covariate'
    )
    tablesWithConceptIds <- list()
    
    for (i in (1:length(databaseTables))) {
      if (file.exists(file.path(exportFolder, paste0(databaseTables[[i]], ".csv")))) {
        path <- file.path(exportFolder, paste0(databaseTables[[i]], ".csv"))
        assign(x = databaseTables[[i]],
               value = readr::read_csv(file = path,
                                       col_types = readr::cols(),
                                       guess_max = min(1e7))
        )
      } else {
        assign(x = databaseTables[[i]],
               value = tidyr::tibble())
      }
      if ('concept_id' %in% colnames(get(databaseTables[[i]]))) {
        tablesWithConceptIds[[i]] <- get(databaseTables[[i]]) %>%
          dplyr::select(.data$concept_id) %>%
          dplyr::distinct()
      }
      if ('concept_id' %in% colnames(get(databaseTables[[i]]))) {
        tablesWithConceptIds[[i]] <- get(databaseTables[[i]]) %>%
          dplyr::select(.data$concept_id) %>%
          dplyr::distinct()
      }
    }
    if (length(tablesWithConceptIds) > 0) {
      conceptIds <-
        dplyr::bind_rows(tablesWithConceptIds) %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$concept_id) %>%
        dplyr::pull(.data$concept_id)
    } else {
      conceptIds <- tidyr::tibble()
    }
    return(conceptIds)
  }



#' @title
#' Get a copy of omop vocabulary as csv
#'
#' @description
#' For a given list of conceptId's get a subset of omop vocabulary
#' of these conceptIds. These are written as csv in the export folder
#'
#' @template Connection
#'
#' @param exportFolder 	            The folder where the output is exported by Cohort Diagnostics.
#'                                  If this folder does not exist, or does not have the searched file
#'                                  the function will return an error.
#' @param conceptIds                (optional) A vector of conceptIds to filter the extract of omop 
#'                                  vocabulary files. If NULL, all conceptIds are extracted.                      
#' @param cdmDatabaseSchema  databaseSchema where the omop vocabulary files are located.
#'                                  These are most commonly the same as cdmDatabaseSchema.
#' @param vocabularyTableNames      (optional) A vector of omop vocabulary table names to download.
#' @return
#' NULL. The function writes the vocabulary tables into the export folder as csv.
#'
#' @export
#'
writeOmopvocabularyTables <-
  function(connectionDetails = NULL,
           connection = NULL,
           cdmDatabaseSchema,
           conceptIds = NULL,
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
      dplyr::mutate(serverTableNames = SqlRender::camelCaseToSnakeCase(vocabularyTableNames) %>%
                      tolower())
    
    vocabularyTablesInCohortDatabaseSchema <-
      tidyr::tibble(serverTableNames = DatabaseConnector::getTableNames(connection, 
                                                                        cdmDatabaseSchema) %>% 
                      tolower()) %>%
      dplyr::filter(.data$serverTableNames %in% (SqlRender::camelCaseToSnakeCase(string =
                                                                                   vocabularyTableNames$serverTableNames))) %>%
      dplyr::left_join(vocabularyTableNames)
    
    if (nrow(vocabularyTablesInCohortDatabaseSchema) == 0) {
      ParallelLogger::logWarn("Vocabulary tables not found in ", cdmDatabaseSchema)
      stop("No vocabulary retrieved")
      return()
    }
    
    for (i in (1:nrow(vocabularyTablesInCohortDatabaseSchema))) {
      if (!is.null(conceptIds) & is.vector(conceptIds)) {
        conceptIdsToQuery <-
          conceptIds %>% unique() %>% paste(collapse = ",")
        ParallelLogger::logInfo('Found ',
                                length(conceptIds %>% unique()),
                                ' unique concept ids.')
        sqlFileName <- paste0("VocabularyQuery",
                              SqlRender::camelCaseToTitleCase(string = 
                                                                vocabularyTablesInCohortDatabaseSchema %>%
                                                                dplyr::slice(i) %>%
                                                                dplyr::pull(.data$vocabularyTableNames)),
                              ".sql")
        sql <-
          SqlRender::loadRenderTranslateSql(
            sqlFilename = stringr::str_squish(sqlFileName),
            packageName = "CohortDiagnostics",
            dbms = connection@dbms,
            vocabulary_database_schema = cdmDatabaseSchema,
            conceptIds = conceptIdsToQuery,
            warnOnMissingParameters = FALSE
          )
      } else {
        sql <-
          SqlRender::render(
            sql = "select * from @cdm_database_schema.@vocabulary_table_name",
            cdm_database_schema = cdmDatabaseSchema,
            vocabulary_table_name = vocabularyTablesInCdmDatabaseSchema %>%
              dplyr::slice(i) %>%
              dplyr::pull(.data$vocabularyTableNames),
            warnOnMissingParameters = FALSE
          )
        sql <- SqlRender::translate(sql = sql, targetDialect = connection@dbms)
      }
      
      assign(
        x = vocabularyTablesInCdmDatabaseSchema %>% 
          dplyr::slice(i) %>% 
          dplyr::pull(.data$serverTableNames),
        value = DatabaseConnector::QuerySql(sql = sql) %>% 
          tidyr::tibble() %>% 
          dplyr::rename_all(.tbl = .data, .funs = tolower)
      )
      
      if (nrow(get(vocabularyTablesInCdmDatabaseSchema %>% 
                   dplyr::slice(i) %>% 
                   dplyr::pull(.data$serverTableNames))) > 0) {
        readr::write_csv(
          x = get(vocabularyTablesInCdmDatabaseSchema %>% 
                    dplyr::slice(i) %>% 
                    dplyr::pull(.data$serverTableNames)),
          path = file.path(
            exportFolder,
            paste0(vocabularyTablesInCdmDatabaseSchema %>% 
                     dplyr::slice(i) %>% 
                     dplyr::pull(.data$serverTableNames),
                   ".csv"
            ) %>% tolower()
          )
        )
      }
      
      ParallelLogger::logInfo('  Wrote ',
                              basename(file.path(
                                exportFolder,
                                paste0(
                                  vocabularyTablesInCdmDatabaseSchema %>% 
                                    dplyr::slice(i) %>% 
                                    dplyr::pull(.data$serverTableNames),
                                  ".csv"
                                ) %>% tolower()
                              )),
                              ' with ',
                              nrow(
                                get(
                                  vocabularyTablesInCdmDatabaseSchema %>% 
                                    dplyr::slice(i) %>% 
                                    dplyr::pull(.data$serverTableNames)
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
    codesets <- list()
    for (j in (1:nrow(codeSetSql))) {
      if (i == 1 && j == 1) {
        DatabaseConnector::insertTable(connection = connection,
                                       tableName = 'CdResolvedCodeSet',
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
        sql <- paste0("insert into #CdResolvedCodeSet (database_id, cohort_id, codeset_id, concept_id) 
                       SELECT '@databaseId' as database_id, 
                               @cohortId as cohort_id, 
                               codeset_id, concept_id FROM (",
                      codeSetSql$conceptSetSql[[j]],
                      ") fin;"
        )
        DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                     sql = sql,
                                                     vocabulary_database_schema = cdmDatabaseSchema,
                                                     databaseId = databaseId,
                                                     cohortId = cohort$cohortId[[i]])
      }
    }
  }
  cohortCodeSets <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                             sql = "select * from #CdResolvedCodeSet",
                                             snakeCaseToCamelCase = TRUE
                                            ) %>% 
    tidyr::tibble()
  return(cohortCodeSets)
}