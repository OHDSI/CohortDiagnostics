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

#' Count the cohort(s)
#'
#' @description
#' Computes the subject and entry count per cohort
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @param cohortIds            The cohort Id(s) used to reference the cohort in the cohort
#'                             table. If left empty, all cohorts in the table will be included.
#'
#' @return
#' A tibble with cohort counts
#'
#' @export
getCohortCounts <- function(connectionDetails = NULL,
                            connection = NULL,
                            cohortDatabaseSchema,
                            cohortTable = "cohort",
                            cohortIds = c()) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  sql <-
    SqlRender::loadRenderTranslateSql(
      sqlFilename = "CohortCounts.sql",
      packageName = utils::packageName(),
      dbms = connection@dbms,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      cohort_ids = cohortIds
    )
  tablesInServer <-
    tolower(DatabaseConnector::dbListTables(conn = connection, schema = cohortDatabaseSchema))
  if (tolower(cohortTable) %in% tablesInServer) {
    counts <-
      DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE) %>%
      tidyr::tibble()
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste(
      "Counting cohorts took",
      signif(delta, 3),
      attr(delta, "units")
    ))
    return(counts)
  } else {
    warning('Cohort table was not found. Was it created?')
    return(NULL)
  }
}

checkIfCohortInstantiated <- function(connection,
                                      cohortDatabaseSchema,
                                      cohortTable,
                                      cohortId) {
  sql <-
    "SELECT COUNT(*) COUNT FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id = @cohort_id;"
  count <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      cohort_id = cohortId
    )
  count <- count %>% dplyr::pull(1)
  return(count > 0)
}

computeCohortCounts <- function(connection,
                                cohortDatabaseSchema,
                                cohortTable,
                                cohorts,
                                exportFolder,
                                minCellCount,
                                databaseId) {
  ParallelLogger::logInfo("Counting cohort records and subjects")
  cohortCounts <- getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohorts$cohortId
  )

  if (is.null(cohortCounts)) {
    stop("Cohort table is empty")
  }

  cohortCounts <- cohortCounts %>%
    dplyr::mutate(databaseId = !!databaseId)
  if (nrow(cohortCounts) > 0) {
    cohortCounts <-
      enforceMinCellValue(data = cohortCounts,
                          fieldName = "cohortEntries",
                          minValues = minCellCount)
    cohortCounts <-
      enforceMinCellValue(data = cohortCounts,
                          fieldName = "cohortSubjects",
                          minValues = minCellCount)
  }
  writeToCsv(
    data = cohortCounts,
    fileName = file.path(exportFolder, "cohort_count.csv"),
    incremental = FALSE,
    cohortId = cohorts$cohortId
  )
  return(cohortCounts)
}
