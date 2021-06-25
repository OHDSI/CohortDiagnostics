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



#' Given two sets of cohorts get relationships between the cohorts.
#'
#' @description
#' Given two cohorts, get data to compare relationships between the cohorts - such as overlap, temporal 
#' relationships between cohort start date(s). 
#' Note: only the first occurrence of subject_id in the cohort is used.
#'
#' @template Connection
#' 
#' @template CohortDatabaseSchema
#'
#' @template CohortTable
#'                                    
#' @param targetCohortIds             A list of cohort ids to be used as target cohorts.
#' 
#' @param comparatorCohortIds         A list of cohort ids to be used as comparator cohorts.
#'                                    
#' @param batchSize                   {Optional, default set to 200} If running diagnostics on larget set
#'                                    of cohorts, this function allows you to batch them into chunks that run 
#'                                    as a batch.
#'                                    
#' @export
computeCohortOverlap <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cohortDatabaseSchema,
                                 cohortTable = "cohort",
                                 targetCohortIds,
                                 comparatorCohortIds,
                                 batchSize = 200) {
  startTime <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  cohortIds <- c(targetCohortIds, comparatorCohortIds) %>% unique() %>% sort()
  
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
      target_cohort_ids = targetCohortIds,
      comparator_cohort_ids = comparatorCohortIds
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql
    )
    
    overlap <- renderTranslateQuerySql(connection = connection, 
                                       sql = "SELECT * FROM #cohort_overlap_long;", 
                                       snakeCaseToCamelCase = TRUE)
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "IF OBJECT_ID('tempdb..#cohort_overlap_long', 'U') IS NOT NULL DROP TABLE #cohort_overlap_long;",
      progressBar = TRUE
    )
    
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
