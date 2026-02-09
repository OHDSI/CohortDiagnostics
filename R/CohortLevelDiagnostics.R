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

# re-export getCohortCounts from CohortGenerator

#' @importFrom CohortGenerator getCohortCounts
#' @export
CohortGenerator::getCohortCounts

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
                                databaseId,
                                writeResult = TRUE) {
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

  cohortCounts <- makeDataExportable(
    x = cohortCounts,
    tableName = "cohort_count",
    minCellCount = minCellCount,
    databaseId = databaseId
  )

  if (writeResult) {
    writeToCsv(
      data = cohortCounts,
      fileName = file.path(exportFolder, "cohort_count.csv"),
      incremental = FALSE,
      cohortId = cohorts$cohortId
    )
  }
  return(cohortCounts)
}
