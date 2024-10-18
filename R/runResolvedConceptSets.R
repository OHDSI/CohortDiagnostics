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

getResolvedConceptSets <- function(connection,
                                   cohortDefinitionSet,
                                   vocabularyDatabaseSchema,
                                   tempEmulationSchema) {
  
  ParallelLogger::logInfo("Instantiating concept sets")
  
  assertCohortDefinitionSetContainsAllParents(cohortDefinitionSet)
  
  # We need to get concept sets from all cohorts in case subsets are present and
  # Added incrementally after cohort generation
  conceptSets <- combineConceptSetsFromCohorts(cohortDefinitionSet)
  
  if (is.null(conceptSets)) {
    ParallelLogger::logInfo("No concept sets found - skipping")
    return(NULL)
  }
  
  uniqueConceptSets <-
    conceptSets[!duplicated(conceptSets$uniqueConceptSetId), ] %>%
    dplyr::select(-"cohortId", -"conceptSetId")
  
  sql <- sapply(
    split(uniqueConceptSets, 1:nrow(uniqueConceptSets)),
    function(x) {
      sub(
        "SELECT [0-9]+ as codeset_id",
        sprintf("SELECT %s as codeset_id", x$uniqueConceptSetId),
        x$conceptSetSql
      )
    }
  )
  
  batchSize <- 100
  tempTables <- c()
  pb <- utils::txtProgressBar(style = 3)
  for (start in seq(1, length(sql), by = batchSize)) {
    utils::setTxtProgressBar(pb, start / length(sql))
    tempTable <-
      paste("#", paste(sample(letters, 20, replace = TRUE), collapse = ""), sep = "")
    tempTables <- c(tempTables, tempTable)
    end <- min(start + batchSize - 1, length(sql))
    sqlSubset <- sql[start:end]
    sqlSubset <- paste(sqlSubset, collapse = "\n\n  UNION ALL\n\n")
    sqlSubset <-
      sprintf(
        "SELECT *\nINTO %s\nFROM (\n %s\n) tmp;",
        tempTable,
        sqlSubset
      )
    sqlSubset <-
      SqlRender::render(sqlSubset, vocabulary_database_schema = vocabularyDatabaseSchema)
    sqlSubset <- SqlRender::translate(sqlSubset,
                                      targetDialect = connection@dbms,
                                      tempEmulationSchema = tempEmulationSchema
    )
    DatabaseConnector::executeSql(connection,
                                  sqlSubset,
                                  progressBar = FALSE,
                                  reportOverallTime = FALSE
    )
  }
  utils::setTxtProgressBar(pb, 1)
  close(pb)
  
  # always recreate the inst_concept_sets table
  if (tempTableExists(connection, "inst_concept_sets")) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "TRUNCATE TABLE #inst_concept_sets; DROP TABLE #inst_concept_sets;",
      tempEmulationSchema = tempEmulationSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  mergeTempTables(
    connection = connection,
    tableName = "#inst_concept_sets",
    tempTables = tempTables,
    tempEmulationSchema = tempEmulationSchema
  )
  
  addConceptIdsToConceptTempTable(
    connection = connection,
    copyFromTempTable = "#inst_concept_sets",
    tempEmulationSchema = tempEmulationSchema
  )
  
  resolvedConceptIds <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM #inst_concept_sets;",
      tempEmulationSchema = tempEmulationSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble() %>%
    dplyr::rename("uniqueConceptSetId" = "codesetId") %>%
    dplyr::inner_join(dplyr::distinct(conceptSets),
                      by = "uniqueConceptSetId",
                      relationship = "many-to-many") %>%
    dplyr::select(
      "cohortId",
      "conceptSetId",
      "conceptId"
    ) %>%
    dplyr::distinct()
  
  return(resolvedConceptIds)
}


runResolvedConceptSets <- function(connection,
                                   cohortDefinitionSet,
                                   databaseId,
                                   exportFolder,
                                   minCellCount,
                                   vocabularyDatabaseSchema,
                                   tempEmulationSchema) {
  
  errorMessage <- checkmate::makeAssertCollection()
  checkArg(connection, add = errorMessage)
  checkArg(cohortDefinitionSet, add = errorMessage)
  checkArg(databaseId, add = errorMessage)
  checkArg(exportFolder, add = errorMessage)
  checkArg(minCellCount, add = errorMessage)
  checkArg(vocabularyDatabaseSchema, add = errorMessage)
  checkArg(tempEmulationSchema, add = errorMessage)
  checkmate::reportAssertions(errorMessage)
  
  # TODO: how should this work in incremental mode
  
  timeExecution(
    exportFolder,
    taskName = "getResolvedConceptSets",
    cohortIds = NULL,
    parent = "runResolvedConceptSets",
    expr = {
      resolvedConceptIds <- getResolvedConceptSets(
        connection = connection,
        cohortDefinitionSet = cohortDefinitionSet,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema
      )
    }
  )
  
  exportDataToCsv(
    data = resolvedConceptIds,
    tableName = "resolved_concepts",
    fileName = file.path(exportFolder, "resolved_concepts.csv"),
    minCellCount = minCellCount,
    databaseId = databaseId,
    incremental = FALSE
  )
}

