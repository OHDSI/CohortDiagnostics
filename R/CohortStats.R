# Copyright 2023 Observational Health Data Sciences and Informatics
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

#' Get stats data
#'
#' @param connection db connection
#' @param cohortDatabaseSchema  cohort db schema
#' @param table  table name
#' @param snakeCaseToCamelCase snake case to camel case?
#' @param databaseId database identifier
#' @param includeDatabaseId if db id should be included
#'
#' @return the stats table
#'
getStatsTable <- function(connection,
                          cohortDatabaseSchema,
                          table,
                          snakeCaseToCamelCase = FALSE,
                          databaseId = NULL,
                          includeDatabaseId = TRUE) {
  # Force databaseId to NULL when includeDatabaseId is FALSE
  if (!includeDatabaseId) {
    databaseId <- NULL
  }
  
  ParallelLogger::logInfo("- Fetching data from ", table)
  sql <- "SELECT {@database_id != ''}?{CAST('@database_id' as VARCHAR(255)) as database_id,} * FROM @cohort_database_schema.@table"
  data <- renderTranslateQuerySql(
    sql = sql,
    connection = connection,
    snakeCaseToCamelCase = snakeCaseToCamelCase,
    table = table,
    cohort_database_schema = cohortDatabaseSchema,
    database_id = ifelse(test = is.null(databaseId),
                         yes = "",
                         no = databaseId
    )
  )
  
  if (!snakeCaseToCamelCase) {
    colnames(data) <- tolower(colnames(data))
  }
  
  return(data)
}

#' Get Cohort Inclusion Stats Table Data
#' @description
#' This function returns a data frame of the data in the Cohort Inclusion Tables.
#' Results are organized in to a list with 5 different data frames:
#'  * cohortInclusionTable
#'  * cohortInclusionResultTable
#'  * cohortInclusionStatsTable
#'  * cohortSummaryStatsTable
#'  * cohortCensorStatsTable
#'
#'
#' These can be optionally specified with the `outputTables`.
#' @param connection                  db connection
#' @param cohortDatabaseSchema        db scheme for cohort
#' @param databaseId                  database identifier
#' @param snakeCaseToCamelCase        Convert column names from snake case to camel case.
#' @param outputTables                Character vector. One or more of "cohortInclusionTable", "cohortInclusionResultTable",
#'                                    "cohortInclusionStatsTable", "cohortInclusionStatsTable", "cohortSummaryStatsTable"
#'                                    or "cohortCensorStatsTable". Output is limited to these tables. Cannot export, for,
#'                                    example, the cohort table. Defaults to all stats tables.
#' @param cohortTableNames            cohort table names
getCohortStats <- function(connection,
                           cohortDatabaseSchema,
                           databaseId = NULL,
                           snakeCaseToCamelCase = TRUE,
                           outputTables = c(
                             "cohortInclusionTable",
                             "cohortInclusionResultTable",
                             "cohortInclusionStatsTable",
                             "cohortInclusionStatsTable",
                             "cohortSummaryStatsTable",
                             "cohortCensorStatsTable"
                           ),
                           cohortTableNames = getCohortTableNames()) {
  # Names of cohort table names must include output tables
  checkmate::assertNames(names(cohortTableNames), must.include = outputTables)
  # ouput tables strictly the set of allowed tables
  checkmate::assertNames(outputTables,
                         subset.of = c(
                           "cohortInclusionTable",
                           "cohortInclusionResultTable",
                           "cohortInclusionStatsTable",
                           "cohortInclusionStatsTable",
                           "cohortSummaryStatsTable",
                           "cohortCensorStatsTable"
                         )
  )
  results <- list()
  for (table in outputTables) {
    # The cohortInclusionTable does not hold database
    # specific information so the databaseId
    # should NOT be included.
    includeDatabaseId <- ifelse(test = table != "cohortInclusionTable",
                                yes = TRUE,
                                no = FALSE
    )
    results[[table]] <- getStatsTable(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      table = cohortTableNames[[table]],
      snakeCaseToCamelCase = snakeCaseToCamelCase,
      includeDatabaseId = includeDatabaseId
    )
  }
  return(results)
}