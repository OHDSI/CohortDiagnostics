# Copyright 2022 Observational Health Data Sciences and Informatics
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

#' createConceptCountsTable
#' 
#' @description Create a table containing concept counts. 
#' CohortDiagnostics performs this task in every run and takes a significant amount of time. 
#' However, with this function, the user can create this table beforehand and 
#' save it in the writing schema for further use.
#'  
#' @inheritParams executeDiagnostics
#' @param conceptCountsDatabaseSchema schema name for the concept counts table
#' @param conceptCountsTableIsTemp boolean to indicate if it should be a temporary table
#' @param removeCurrentTable Should the current table should be removed? TRUE (default) or FALSE
#'
#' @export
createConceptCountsTable <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     tempEmulationSchema = NULL,
                                     conceptCountsTable = "concept_counts",
                                     conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                     conceptCountsTableIsTemp = FALSE,
                                     removeCurrentTable = TRUE) {
  ParallelLogger::logInfo("Creating concept counts table")
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <-
    SqlRender::loadRenderTranslateSql(
      "CreateConceptCountTable.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      cdm_database_schema = cdmDatabaseSchema,
      work_database_schema = conceptCountsDatabaseSchema,
      concept_counts_table = conceptCountsTable,
      table_is_temp = conceptCountsTableIsTemp,
      remove_current_table = removeCurrentTable
    )
  executeSql(connection, sql)
}

#' getConceptCountsTableName
#' 
#' @description Get a concept counts table name that is unique for the current database version.
#' We need to make sure the table is only used if the counts are for the current database.
#' 
#' @param connection database connection
#' @param cdmDatabaseSchema CDM schema
#' 
#' @return the concepts count table name
#' @export
getConceptCountsTableName <- function(connection, cdmDatabaseSchema) {
  result <- "concept_counts"
  sql <- paste("SELECT vocabulary_version as version",
               "FROM @cdmDatabaseSchema.VOCABULARY",
               "WHERE vocabulary_id = 'None'")
  dbVersion <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                          sql = sql,
                                                          cdmDatabaseSchema = cdmDatabaseSchema) |> 
    dplyr::pull(1)
  if (!identical(dbVersion, character(0))) {
    result <- paste(gsub(" |\\.|-", "_", dbVersion), result, sep = "_")
  }
  return(result)
}