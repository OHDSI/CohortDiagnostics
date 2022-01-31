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
                                orphanConceptTable = '#recommended_concepts') {
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
                                     conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                     conceptCountsTable = "concept_counts",
                                     conceptCountsTableIsTemp = FALSE) {
  ParallelLogger::logInfo("Creating internal concept counts table")
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
      table_is_temp = conceptCountsTableIsTemp
    )
  DatabaseConnector::executeSql(connection, sql)
}

saveDatabaseMetaData <- function(databaseId,
                                 databaseName,
                                 databaseDescription,
                                 exportFolder,
                                 vocabularyVersionCdm,
                                 vocabularyVersion) {
  ParallelLogger::logInfo("Saving database metadata")
  startMetaData <- Sys.time()
  database <- dplyr::tibble(
    databaseId = databaseId,
    databaseName = dplyr::coalesce(databaseName, databaseId),
    description = dplyr::coalesce(databaseDescription, databaseId),
    vocabularyVersionCdm = !!vocabularyVersionCdm,
    vocabularyVersion = !!vocabularyVersion,
    isMetaAnalysis = 0
  )
  writeToCsv(data = database,
             fileName = file.path(exportFolder, "database.csv"))
  delta <- Sys.time() - startMetaData
  writeLines(paste(
    "Saving database metadata took",
    signif(delta, 3),
    attr(delta, "units")
  ))
}

getCdmVocabularyVersion <- function(connection, cdmDatabaseSchema) {
  vocabularyVersionCdm <- NULL
  tryCatch({
    vocabularyVersionCdm <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "select * from @cdm_database_schema.cdm_source;",
        cdm_database_schema = cdmDatabaseSchema,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble()
  }, error = function(...) {
    warning("Problem getting vocabulary version. cdm_source table not found in the database.")
    if (connection@dbms == "postgresql") { #this is for test that automated testing purpose
      DatabaseConnector::dbExecute(connection, "ABORT;")
    }
  })

  if (all(!is.null(vocabularyVersionCdm),
          nrow(vocabularyVersionCdm) > 0,
          'vocabularyVersion' %in% colnames(vocabularyVersionCdm))) {
    if (nrow(vocabularyVersionCdm) > 1) {
      warning('Please check ETL convention for OMOP cdm_source table. It appears that there is more than one row while only one is expected.')
    }
    vocabularyVersionCdm <- vocabularyVersionCdm %>%
      dplyr::rename(vocabularyVersionCdm = .data$vocabularyVersion) %>%
      dplyr::pull(vocabularyVersionCdm) %>%
      max() %>%
      unique()
  } else {
    warning("Problem getting vocabulary version. cdm_source table either does not have data, or does not have the field vocabulary_version.")
    vocabularyVersionCdm <- "Unknown"
  }

  return(vocabularyVersionCdm)
}


getVocabularyVersion <- function(connection, vocabularyDatabaseSchema) {
  DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "select * from @vocabulary_database_schema.vocabulary where vocabulary_id = 'None';",
    vocabulary_database_schema = vocabularyDatabaseSchema,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble() %>%
    dplyr::rename(vocabularyVersion = .data$vocabularyVersion) %>%
    dplyr::pull(.data$vocabularyVersion) %>%
    unique()
}
