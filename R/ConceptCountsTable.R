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
#' @description Create a table containing the concept counts.
#' 
#' @param connectionDetails database connection details
#' @param connection database connection
#' @param cdmDatabaseSchema CDM schema
#' @param tempEmulationSchema schema to emulate temp table
#' @param conceptCountsDatabaseSchema schema name for the concept counts table
#' @param conceptCountsTable table name
#' @param conceptCountsTableIsTemp boolean to indicate if it should be a temporary table
#' @param removeCurrentTable if the current table should be removed
#'
#' @export
createConceptCountsTable <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     tempEmulationSchema = NULL,
                                     conceptCountsDatabaseSchema,
                                     conceptCountsTable = "concept_counts",
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
      packageName = utils::packageName(),
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      cdm_database_schema = cdmDatabaseSchema,
      work_database_schema = conceptCountsDatabaseSchema,
      concept_counts_table = conceptCountsTable,
      table_is_temp = conceptCountsTableIsTemp,
      remove_current_table = removeCurrentTable
    )
  DatabaseConnector::executeSql(connection, sql)
}