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



#' Given a set of cohorts get relationships between the cohorts.
#'
#' @description
#' Given a set of cohorts, get temporal relationships between the 
#' cohort_start_date of the cohorts.
#'
#' @template Connection
#' 
#' @template CohortDatabaseSchema
#'
#' @template CohortTable
#'                                    
#' @template CohortIds
#'                                    
#' @param batchSize                   {Optional, default set to 200} If running diagnostics on larget set
#'                                    of cohorts, this function allows you to batch them into chunks that run 
#'                                    as a batch.
#'                                    
#' @export
computeCohortTemporalRelationship <- function(connectionDetails = NULL,
                                              connection = NULL,
                                              cohortDatabaseSchema,
                                              cohortTable = "cohort",
                                              cohortIds,
                                              batchSize = 200) {
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
        "Batch Cohort Temporal Relationship Processing cohorts %s through %s",
        start,
        end
      ))
    }
    sql <- SqlRender::loadRenderTranslateSql(
      "CohortRelationship.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      target_cohort_ids = cohortIds[[start:end]]
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql
    )
    temporalRelationship <- renderTranslateQuerySql(connection = connection, 
                                       sql = "SELECT * FROM #cohort_rel_long;", 
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
