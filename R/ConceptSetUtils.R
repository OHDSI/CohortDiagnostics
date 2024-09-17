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

#' Create the concept counts table used by CohortDiagnostics
#' 
#' CohortDiagnostics requires the record and person counts for all concepts in the CDM.
#' createConceptCountsTable will create this table. To speed up execution pre-computed 
#' Achilles data can be used in the creation of the table. This table can also be persisted 
#' across executions of CohortDiagnostics and does not need to be recreated with each run.
#'
#' @template Connection 
#' @template CdmDatabaseSchema 
#' @template TempEmulationSchema 
#' @param conceptCountsDatabaseSchema character. The schema where the concept counts table should be created.
#' @param conceptCountsTable character. The name of concept counts table. If the name starts with # then a 
#'                           temporary table will be created.
#' @param achillesDatabaseSchema character. Schema where Achilles tables are stored in the database. 
#'                               If provided and the correct analysis IDs exist
#'                               then the Achilles results will be used to create the concept counts table.
#' @param overwrite logical. Should the table be overwritten if it already exists? TRUE or FALSE (default)
#'
#' @return
#' @export
#'
#' @examples
#' \dontrun{
#' connectionDetails <- Eunomia::getEunomiaConnectionDetails()
#' connection <- DatabaseConnector::connect(connectionDetails)
#' 
#' createConceptCountsTable(connection = connection,
#'                          cdmDatabaseSchema = "main",
#'                          conceptCountsDatabaseSchema = "main",
#'                          conceptCountsTable = "concept_counts",
#'                          achillesDatabaseSchema = NULL,
#'                          overwrite = FALSE)  
#' 
#' DatabaseConnector::disconnect(connection)
#' }
createConceptCountsTable <- function(connection = NULL,
                                     cdmDatabaseSchema,
                                     tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                     conceptCountsDatabaseSchema = NULL,
                                     conceptCountsTable = "concept_counts",
                                     achillesDatabaseSchema = NULL,
                                     overwrite = FALSE) {browser()
  
  dbRegex <- "^[a-zA-Z][a-zA-Z0-9_]*$" # regex for letters, numbers, and _ but must start with a letter
  checkmate::assertClass(connection, "DatabaseConnectorConnection")
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, min.chars = 1, any.missing = FALSE, pattern = dbRegex)
  checkmate::assertCharacter(tempEmulationSchema, len = 1, min.chars = 1, any.missing = FALSE, pattern = dbRegex, null.ok = TRUE)
  checkmate::assertCharacter(achillesDatabaseSchema, len = 1, min.chars = 1, any.missing = FALSE, pattern = dbRegex, null.ok = TRUE)
  checkmate::assertLogical(overwrite, any.missing = FALSE, len = 1)
  checkmate::assertCharacter(conceptCountsTable, len = 1, min.chars = 1, any.missing = FALSE)
  
  # check if table exists, has expected column names, and at least a few rows
  
  conceptCountsTableIsTemp <- (substr(conceptCountsTable, 1, 1) == "#")
  if (conceptCountsTableIsTemp) {
    checkmate::assertCharacter(conceptCountsDatabaseSchema, len = 1, min.chars = 1, any.missing = FALSE, pattern = dbRegex)
  }
  
  if (isFALSE(conceptCountsTableIsTemp) && isFALSE(overwrite)) {
    
    tablesInSchema <- DatabaseConnector::getTableNames(connection, databaseSchema = conceptCountsDatabaseSchema, cast = "lower")
    
    if (tolower(conceptCountsTable) %in% tablesInSchema) {
      # check that the table has the correct column names and at least a few rows
      result <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "select top 5 * from @schema.@concept_counts_table;",
        tempEmulationSchema = tempEmulationSchema,
        schema = conceptCountsDatabaseSchema,
        concept_counts_table = conceptCountsTable
      )
      
      columnsExist <- all(c("concept_id", "concept_count", "concept_subjects") %in% tolower(colnames(result)))
      
      if (columnsExist && (nrow(result) > 3)) {
        ParallelLogger::logInfo("Concept counts table already exists and is not being overwritten")
        return(invisible(NULL))
      }
    }
  }
  
  ParallelLogger::logInfo("Creating concept counts table")
  
  useAchilles <- FALSE
  if (!is.null(achillesDatabaseSchema)) {
    # check that achilles tables exist
    tablesInAchillesSchema <- DatabaseConnector::getTableNames(connection, databaseSchema = achillesDatabaseSchema, cast = "lower")
    if ("achilles_results" %in% tablesInAchillesSchema) {
      ParallelLogger::logInfo("Using achilles_results table to create the concept_counts table")
      useAchilles <- TRUE
    } else {
      ParallelLogger::logInfo("achilles_results table was not found in the achilles schema")
    }
  }
  
  sql <- SqlRender::readSql(system.file("sql", "sql_server", "CreateConceptCountTable.sql", package = "CohortDiagnostics"))
  sql <- SqlRender::render(sql, 
                           tempEmulationSchema = tempEmulationSchema,
                           cdm_database_schema = cdmDatabaseSchema,
                           work_database_schema = conceptCountsDatabaseSchema,
                           concept_counts_table = conceptCountsTable,
                           table_is_temp = conceptCountsTableIsTemp,
                           use_achilles = useAchilles,
                           achilles_database_schema = achillesDatabaseSchema)
  sql <- SqlRender::translate(sql, DatabaseConnector::dbms(connection))
  DatabaseConnector::executeSql(connection = connection, sql = sql)
  
  n <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "select count(*) as n from @schema.@table;",
    schema = conceptCountsDatabaseSchema,
    table = conceptCountsTable
  )
  
  ParallelLogger::logInfo(paste("Concept counts table created with", n[[1]], "rows"))
  if (n == 0) ParallelLogger::logWarn("concept_counts table has zeros rows!")
  return(invisible(NULL))
}

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


