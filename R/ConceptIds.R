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
                        'temporal_covariate_ref',
                        'concept_sets_concept_id',
                        'phenotype_description'
    )
    tablesWithConceptIds <- list()
    
    for (i in (1:length(databaseTables))) {
      if (file.exists(file.path(exportFolder, paste0(databaseTables[[i]], ".csv")))) {
        ParallelLogger::logInfo("working on ", databaseTables[[i]])
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
        ParallelLogger::logInfo("    Found concept_id")
        tablesWithConceptIds[[i]] <- get(databaseTables[[i]]) %>%
          dplyr::select(.data$concept_id) %>%
          dplyr::distinct()
      }
      if ('referent_concept_id' %in% colnames(get(databaseTables[[i]]))) {
        ParallelLogger::logInfo("    Found referent_concept_id")
        tablesWithConceptIds[[i]] <- get(databaseTables[[i]]) %>%
          dplyr::select(.data$referent_concept_id) %>%
          dplyr::distinct()
      }
      if ('source_concept_id' %in% colnames(get(databaseTables[[i]]))) {
        ParallelLogger::logInfo("    Found source_concept_id")
        tablesWithConceptIds[[i]] <- get(databaseTables[[i]]) %>%
          dplyr::select(.data$source_concept_id) %>%
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
#' @param conceptIdTable            (optional) A table with one column called concept_id (integer) that
#'                                  contains all the concept_ids to limit the data pull. In the absence 
#'                                  of this table, the entire vocabulary is pulled down (slow)
#'                                  vocabulary files. If NULL, all conceptIds are extracted. Please
#'                                  provide the full name of the vocabulary table e.g.
#'                                  databaseSchema.conceptIdTable. If it is a temporary table
#'                                  please use '#conceptIdTable'. Remember, for temporary table
#'                                  the connection object has to be active.                     
#' @param cdmDatabaseSchema         databaseSchema where the omop vocabulary files are located.
#'                                  These are most commonly the same as cdmDatabaseSchema.
#' @param vocabularyTableNames      (optional) A vector of omop vocabulary table names to download.
#' @return
#' NULL. The function writes the vocabulary tables into the export folder as csv.
#'
#' @export
#'
getOmopVocabularyTables <-
  function(connectionDetails = NULL,
           connection = NULL,
           cdmDatabaseSchema,
           conceptIdTable = NULL,
           vocabularyTableNames = c('concept',
                                    'conceptAncestor',
                                    'conceptClass',
                                    'conceptRelationship',
                                    'conceptSynonym',
                                    'domain',
                                    'relationship',
                                    'vocabulary'),
           exportFolder) {
    if (!is.null(connection)) {
      connectionDetails <- NULL
      ParallelLogger::logInfo('Connection provided')
    }
    if (!is.null(connectionDetails)) {
      if (is.null(connection)) {
        connection <- DatabaseConnector::connect(connectionDetails)
        on.exit(DatabaseConnector::disconnect(connection))
      }
    }
    if (is.null(connection)) {
      ParallelLogger::logWarn('no connection provided')
    }
    
    if (!is.null(conceptIdTable)) {
      DatabaseConnector::dbExistsTable(conn = connection, )
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
                      (SqlRender::camelCaseToSnakeCase(string =                                                                                   vocabularyTableNames$serverTableNames))) %>%
      dplyr::left_join(vocabularyTableNames)
    
    if (nrow(vocabularyTablesInCdmDatabaseSchema) == 0) {
      ParallelLogger::logWarn("Vocabulary tables not found in ", cdmDatabaseSchema)
      stop("No vocabulary retrieved")
      return()
    }
    
    if (!is.null(conceptIds)) {
      concept <- tidyr::tibble(concept_id = conceptIds) %>% 
        dplyr::filter(.data$concept_id > 0) %>% 
        dplyr::distinct()
      ParallelLogger::logInfo('Found conceptIds count = ', length(conceptIds))
      ParallelLogger::logInfo('    Uploading temp table')
      DatabaseConnector::insertTable(connection = connection,
                                     data = concept,
                                     dropTableIfExists = TRUE,
                                     tableName = '#conceptsToExtract', 
                                     progressBar = TRUE,
                                     tempTable = TRUE)
    } else {
      ParallelLogger::logInfo('No conceptIds found, downloading for all conceptIds')
      ParallelLogger::logInfo('    Might take a very long time....')
      sql <- "select distinct concept_id
              into #conceptsToExtract
              from @cdm_database_schema.concept"
      DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                   sql = sql, 
                                                   cdm_database_schema = cdmDatabaseSchema
      )
    }
    
    for (i in (1:nrow(vocabularyTablesInCdmDatabaseSchema))) {
      if (vocabularyTablesInCdmDatabaseSchema[[i]] %in% c('domain',
                                                          'relationship',
                                                          'vocabulary',
                                                          'concept_class')) {
        sql <- "select * from @cdm_database_schema.@table"

      } else if (vocabularyTablesInCdmDatabaseSchema[[i]] %in% c('concept', 'conceptSynonym')) {
        sql <- "select * from @cdm_database_schema.@table a
        inner join #conceptsToExtract b
        on a.concept_id = b.concept_id"
      } else if (vocabularyTablesInCdmDatabaseSchema[[i]] %in% c('conceptAncestor')) {
        sql <- "select * from @cdm_database_schema.@table a
        left join #conceptsToExtract b
        on a.ancestor_concept_id = b.concept_id or
        a.descendant_concept_id = b.concept_id"
      } else if (vocabularyTablesInCdmDatabaseSchema[[i]] %in% c('conceptRelationship')) {
        sql <- "select * from @cdm_database_schema.@table a
        left join #conceptsToExtract b
        on a.concept_id_1 = b.concept_id or
        a.a.concept_id_2 = b.concept_id"
      }
      assign(
        x = vocabularyTablesInCdmDatabaseSchema[i,]$serverTableNames,
        DatabaseConnector::renderTranslateQuerySql(connection = connection, 
                                                   sql = sql, 
                                                   cdm_database_schema = cdmDatabaseSchema,
                                                   table = vocabularyTablesInCdmDatabaseSchema[[i]]) %>% 
          tidyr::tibble())
      
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
                                       tableName = '#CdResolvedCodeSet',
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
