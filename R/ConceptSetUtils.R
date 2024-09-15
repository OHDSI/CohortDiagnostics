# Copyright 2024 Observational Health Data Sciences and Informatics
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

.findOrphanConcepts <- function(connectionDetails = NULL,
                                connection = NULL,
                                cdmDatabaseSchema,
                                vocabularyDatabaseSchema = cdmDatabaseSchema,
                                tempEmulationSchema = NULL,
                                conceptIds = c(),
                                useCodesetTable = FALSE,
                                codesetId = 1,
                                conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                conceptCountsTable = "concept_counts",
                                conceptCountsTableIsTemp = FALSE,
                                instantiatedCodeSets = "#InstConceptSets",
                                orphanConceptTable = "#recommended_concepts") {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql(
    "OrphanCodes.sql",
    packageName = utils::packageName(),
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    vocabulary_database_schema = vocabularyDatabaseSchema,
    work_database_schema = conceptCountsDatabaseSchema,
    concept_counts_table = conceptCountsTable,
    concept_counts_table_is_temp = conceptCountsTableIsTemp,
    concept_ids = conceptIds,
    use_codesets_table = useCodesetTable,
    orphan_concept_table = orphanConceptTable,
    instantiated_code_sets = instantiatedCodeSets,
    codeset_id = codesetId
  )
  DatabaseConnector::executeSql(connection, sql)
  ParallelLogger::logTrace("- Fetching orphan concepts from server")
  sql <- "SELECT * FROM @orphan_concept_table;"
  orphanConcepts <-
    DatabaseConnector::renderTranslateQuerySql(
      sql = sql,
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      orphan_concept_table = orphanConceptTable,
      snakeCaseToCamelCase = TRUE
    ) %>%
    tidyr::tibble()

  ParallelLogger::logTrace("- Dropping orphan temp tables")
  sql <-
    SqlRender::loadRenderTranslateSql(
      "DropOrphanConceptTempTables.sql",
      packageName = utils::packageName(),
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema
    )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  return(orphanConcepts)
}

createConceptCountsTable <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     tempEmulationSchema = NULL,
                                     conceptCountsDatabaseSchema = NULL,
                                     conceptCountsTable = "concept_counts",
                                     conceptCountsTableIsTemp = FALSE,
                                     useAchilles = FALSE,
                                     achillesDatabaseSchema = NULL) {
  ParallelLogger::logInfo("Creating internal concept counts table")
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (isTRUE(useAchilles)) {
    checkmate::assert_character(resultsDatabaseSchema, min.chars = 1, len = 1, any.missing = FALSE)
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
      use_achilles = useAchilles,
      achilles_database_schema = achillesDatabaseSchema
    )
  DatabaseConnector::executeSql(connection, sql)
}
