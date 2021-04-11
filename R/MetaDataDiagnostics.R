# Copyright 2021 Observational Health Data Sciences and Informatics
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
