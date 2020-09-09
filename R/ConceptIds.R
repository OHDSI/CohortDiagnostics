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
                                       col_types = readr::cols())
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
#' @param vocabularyDatabaseSchema  databaseSchema where the omop vocabulary files are located.
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
           vocabularyDatabaseSchema = NULL,
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
                                                                        vocabularyDatabaseSchema) %>% 
                      tolower()) %>%
      dplyr::filter(.data$serverTableNames %in% (vocabularyTableNames$serverTableNames %>% 
                                                   SqlRender::camelCaseToSnakeCase(string = .))) %>%
      dplyr::left_join(vocabularyTableNames)
    
    if (nrow(vocabularyTablesInCohortDatabaseSchema) == 0) {
      ParallelLogger::logWarn("Vocabulary tables not found in ", vocabularyDatabaseSchema)
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
        sql <-
          SqlRender::loadRenderTranslateSql(
            sqlFilename = paste0("VocabularyQuery",
                                 vocabularyTablesInCohortDatabaseSchema %>%
                                   dplyr::slice(i) %>%
                                   dplyr::pull(.data$vocabularyTableNames) %>%
                                   SqlRender::camelCaseToTitleCase(string = .) %>%
                                   stringr::str_replace_all(
                                     string = .,
                                     pattern = " ",
                                     replacement = ""
                                   ),
                                 ".sql"
            ),
            packageName = "CohortDiagnostics",
            dbms = connection@dbms,
            vocabulary_database_schema = vocabularyDatabaseSchema,
            conceptIds = conceptIdsToQuery,
            warnOnMissingParameters = FALSE
          )
      } else {
        sql <-
          SqlRender::render(
            sql = "select * from @vocabulary_database_schema.@vocabulary_table_name",
            vocabulary_database_schema = vocabularyDatabaseSchema,
            vocabulary_table_name = vocabularyTablesInCohortDatabaseSchema %>%
              dplyr::slice(i) %>%
              dplyr::pull(.data$vocabularyTableNames),
            warnOnMissingParameters = FALSE
          )
        sql <- SqlRender::translate(sql = sql, targetDialect = connection@dbms)
      }
      
      assign(
        x = vocabularyTablesInCohortDatabaseSchema %>% 
          dplyr::slice(i) %>% 
          dplyr::pull(.data$serverTableNames),
        value = DatabaseConnector::QuerySql(sql = sql) %>% 
          tidyr::tibble() %>% 
          dplyr::rename_all(.tbl = ., .funs = tolower)
      )
      
      if (nrow(get(vocabularyTablesInCohortDatabaseSchema %>% 
                   dplyr::slice(i) %>% 
                   dplyr::pull(.data$serverTableNames))) > 0) {
        readr::write_csv(
          x = get(vocabularyTablesInCohortDatabaseSchema %>% 
                    dplyr::slice(i) %>% 
                    dplyr::pull(.data$serverTableNames)),
          path = file.path(
            exportFolder,
            paste0(vocabularyTablesInCohortDatabaseSchema %>% 
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
                                  vocabularyTablesInCohortDatabaseSchema %>% 
                                    dplyr::slice(i) %>% 
                                    dplyr::pull(.data$serverTableNames),
                                  ".csv"
                                ) %>% tolower()
                              )),
                              ' with ',
                              nrow(
                                get(
                                  vocabularyTablesInCohortDatabaseSchema %>% 
                                    dplyr::slice(i) %>% 
                                    dplyr::pull(.data$serverTableNames)
                                )
                              ),
                              " records.")
    }
  }
