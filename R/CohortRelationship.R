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
#' @param targetCohortIds              A vector of one or more Cohort Ids for use as target cohorts.
#' 
#' @param comparatorCohortIds          A vector of one or more Cohort Ids for use as feature/comparator cohorts.
#'
#' @export
computeCohortTemporalRelationship <-
  function(connectionDetails = NULL,
           connection = NULL,
           cohortDatabaseSchema,
           cohortTable = "cohort",
           targetCohortIds,
           fetureCohortIds) {
    startTime <- Sys.time()
    
    if (length(cohortIds) == 0) {
      return(NULL)
    }
    
    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
    
    sql <- SqlRender::loadRenderTranslateSql(
      "CohortRelationship.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      target_cohort_ids = cohortIds,
      feture_ohort_ids = fetureCohortIds
    )
    DatabaseConnector::executeSql(connection = connection,
                                  sql = sql)
    temporalRelationship <-
      renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT * FROM #cohort_rel_long;",
        snakeCaseToCamelCase = TRUE
      )
    
    dropSql = "IF OBJECT_ID('tempdb..#cohort_rel_long', 'U') IS NOT NULL DROP TABLE #cohort_rel_long;"
    DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                 sql = dropSql,
                                                 progressBar = TRUE)
    delta <- Sys.time() - startTime
    ParallelLogger::logInfo(paste(
      "Computing cohort temporal relationship took",
      signif(delta, 3),
      attr(delta, "units")
    ))
    return(temporalRelationship)
  }
