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
  database <- makeDataExportable(
    x = database,
    tableName = "database",
    databaseId = databaseId,
    minCellCount = minCellCount
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
