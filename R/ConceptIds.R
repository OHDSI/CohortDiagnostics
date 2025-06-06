# Copyright 2025 Observational Health Data Sciences and Informatics
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

createConceptTable <- function(connection, tempEmulationSchema) {
  ParallelLogger::logTrace("Creating concept ID table for tracking concepts used in diagnostics")
  sql <-
    SqlRender::loadRenderTranslateSql(
      "CreateConceptIdTable.sql",
      packageName = utils::packageName(),
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      table_name = "#concept_ids"
    )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
}

exportConceptInformation <- function(connection = NULL,
                                     vocabularyDatabaseSchema,
                                     tempEmulationSchema,
                                     conceptIdTable,
                                     vocabularyTableNames = getDefaultVocabularyTableNames(),
                                     incremental,
                                     exportFolder) {
  ParallelLogger::logInfo("Retrieving concept information")
  start <- Sys.time()
  if (is.null(connection)) {
    warning("No connection provided")
  }
  vocabularyTableNames <-
    tolower(SqlRender::camelCaseToSnakeCase(vocabularyTableNames))
  tablesInCdmDatabaseSchema <-
    tolower(DatabaseConnector::getTableNames(connection, vocabularyDatabaseSchema))
  vocabularyTablesInCdmDatabaseSchema <-
    tablesInCdmDatabaseSchema[tablesInCdmDatabaseSchema %in% vocabularyTableNames]

  if (length(vocabularyTablesInCdmDatabaseSchema) == 0) {
    stop("Vocabulary tables not found in ", vocabularyDatabaseSchema)
  }
  sql <- "SELECT DISTINCT concept_id FROM @unique_concept_id_table;"
  uniqueConceptIds <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      unique_concept_id_table = conceptIdTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema
    )[, 1]
  if (length(uniqueConceptIds) == 0) {
    if (!incremental) {
      warning("No concept IDs in cohorts. No concept information exported.")
    } else {
      ParallelLogger::logInfo("Incremental mode, no additional concept information to export.")
    }
    return(NULL)
  }

  for (vocabularyTable in vocabularyTablesInCdmDatabaseSchema) {
    ParallelLogger::logInfo(
      "- Retrieving concept information from vocabulary table '",
      vocabularyTable,
      "'"
    )
    if (vocabularyTable %in% c("concept", "concept_synonym")) {
      sql <- "SELECT a.* FROM @cdm_database_schema.@table a
        INNER JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b
          ON a.concept_id = b.concept_id;"
    } else if (vocabularyTable %in% c("concept_ancestor")) {
      sql <- "SELECT a.* FROM @cdm_database_schema.@table a
        LEFT JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b1
          ON a.ancestor_concept_id = b1.concept_id
        LEFT JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b2
          ON a.descendant_concept_id = b2.concept_id
        WHERE b1.concept_id IS NOT NULL or b2.concept_id IS NOT NULL;"
    } else if (vocabularyTable %in% c("concept_relationship")) {
      sql <- "SELECT a.* FROM @cdm_database_schema.@table a
        INNER JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b1
          ON a.concept_id_1 = b1.concept_id
        INNER JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b2
          ON a.concept_id_2 = b2.concept_id
        WHERE b1.concept_id IS NOT NULL or b2.concept_id IS NOT NULL;"
    }
    if (vocabularyTable %in% c(
      "concept",
      "concept_synonym",
      "concept_ancestor",
      "concept_relationship"
    )) {
      data <-
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          cdm_database_schema = vocabularyDatabaseSchema,
          unique_concept_id_table = conceptIdTable,
          table = vocabularyTable,
          snakeCaseToCamelCase = TRUE
        )
      if (nrow(data) > 0) {
        data <- makeDataExportable(
          x = data,
          tableName = vocabularyTable
        )

        writeToCsv(
          data = data,
          fileName = file.path(exportFolder, paste(vocabularyTable, "csv", sep = ".")),
          incremental = incremental,
          conceptId = uniqueConceptIds
        )
      }
    } else if (vocabularyTable %in% c(
      "domain",
      "relationship",
      "vocabulary",
      "conceptClass"
    )) {
      sql <- "SELECT * FROM @cdm_database_schema.@table;"
      data <-
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          cdm_database_schema = vocabularyDatabaseSchema,
          table = vocabularyTable,
          snakeCaseToCamelCase = TRUE
        )
      if (nrow(data) > 0) {
        data <- makeDataExportable(
          x = data,
          tableName = vocabularyTable
        )

        # Workaround for sqlite imports that add decimal precision to fields
        if (vocabularyTable == "relationship" && DatabaseConnector::dbms(connection) == "sqlite") {
          data$isHierarchical <- as.character(as.numeric(data$isHierarchical))
          data$definesAncestry <- as.character(as.numeric(data$definesAncestry))
        }

        writeToCsv(
          data = data,
          fileName = file.path(exportFolder, paste(vocabularyTable, "csv", sep = ".")),
          incremental = FALSE
        )
      }
    }
  }
  delta <- Sys.time() - start
  ParallelLogger::logInfo(
    "Retrieving concept information took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
}
