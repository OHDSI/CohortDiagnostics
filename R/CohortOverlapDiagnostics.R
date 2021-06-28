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



#' Given two sets of cohorts get overlap between the cohorts.
#'
#' @description
#' Given two sets of cohorts, get data on overlap between the cohorts.
#' Note: only the first occurrence of subject_id in the cohort is used.
#'
#' @template Connection
#'
#' @template CohortDatabaseSchema
#'
#' @template CohortTable
#'
#' @template CohortIds
#'
#' @param batchSize                   {Optional, default set to 50} If running diagnostics on large set
#'                                    of cohorts, this function allows you to batch them into chunks that
#'                                    by default run over 50 target cohorts (and all comparator cohorts).
#'
#' @export
computeCohortOverlap <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cohortDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortIds,
                                 batchSize = 50) {
  startTime <- Sys.time()
  
  if (length(cohortIds) == 0) {
    return(NULL)
  }
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  results <- Andromeda::andromeda()
  for (start in seq(1, length(cohortIds), by = batchSize)) {
    end <- min(start + batchSize - 1, length(cohortIds))
    if (length(cohortIds) > batchSize) {
      ParallelLogger::logInfo(sprintf(
        "Batch Cohort Overlap Processing cohorts %s through %s",
        start,
        end
      ))
    }
    sql <- SqlRender::loadRenderTranslateSql(
      "CohortOverlap.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      target_cohort_ids = cohortIds
    )
    DatabaseConnector::executeSql(connection = connection,
                                  sql = sql)
    
    overlap <- renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM #cohort_overlap_long;",
      snakeCaseToCamelCase = TRUE
    )
    
    DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                 sql = "IF OBJECT_ID('tempdb..#cohort_overlap_long', 'U') IS NOT NULL DROP TABLE #cohort_overlap_long;",
                                                 progressBar = TRUE)
    
    if ("overlap" %in% names(results)) {
      Andromeda::appendToTable(results$overlap, overlap)
    } else {
      results$overlap <- overlap
    }
  }
  overlapAll <- results$overlap %>% dplyr::collect()
  delta <- Sys.time() - startTime
  ParallelLogger::logInfo(paste(
    "Computing overlap took",
    signif(delta, 3),
    attr(delta, "units")
  ))
  return(overlapAll)
}
