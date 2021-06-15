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

getVisitContext <- function(connectionDetails = NULL,
                            connection = NULL,
                            cdmDatabaseSchema,
                            tempEmulationSchema = NULL,
                            cohortDatabaseSchema = cdmDatabaseSchema,
                            vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                            cohortTable = "cohort",
                            cohortIds,
                            cdmVersion = 5) {
  if (!cdmVersion == 5) {
    warning('Only OMOP CDM v5.x.x is supported. Continuing execution.')
  }
  
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  sql <- SqlRender::loadRenderTranslateSql(
    "VisitContext.sql",
    packageName = "CohortDiagnostics",
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    visit_context_table = "#visit_context",
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    vocabulary_database_schema = vocabularyDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds
  )
  DatabaseConnector::executeSql(connection, sql)
  sql <- "SELECT * FROM @visit_context_table;"
  visitContext <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      visit_context_table = "#visit_context",
      snakeCaseToCamelCase = TRUE
    )
  
  sql <-
    "TRUNCATE TABLE @visit_context_table;\nDROP TABLE @visit_context_table;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    visit_context_table = "#visit_context",
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo("Retrieving visit context took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
  return(visitContext)
}
