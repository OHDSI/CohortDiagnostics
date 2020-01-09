# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of StudyDiagnostics
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

#' Compute overlap between two cohorts
#'
#' @description
#' Computes the overlap between a target and a comparator cohort.
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @param targetCohortId       The cohort definition ID used to reference the target cohort in the
#'                             cohort table.
#' @param comparatorCohortId   The cohort definition ID used to reference the comparator cohort in the
#'                             cohort table.
#'
#' @return
#' A data frame with overlap statistics.
#'
#' @export
computeCohortOverlap <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cohortDatabaseSchema,
                                 cohortTable = "cohort",
                                 targetCohortId,
                                 comparatorCohortId) {
  start <- Sys.time()

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (!checkIfCohortInstantiated(connection = connection,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 cohortId = targetCohortId)) {
    warning("Target cohort with ID ", targetCohortId, " appears to be empty. Was it instantiated?")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Computing overlap took", signif(delta, 3), attr(delta, "units")))
    return(data.frame())
  }
  if (!checkIfCohortInstantiated(connection = connection,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 cohortId = comparatorCohortId)) {
    warning("Comparator cohort with ID ",
            comparatorCohortId,
            " appears to be empty. Was it instantiated?")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Computing overlap took", signif(delta, 3), attr(delta, "units")))
    return(data.frame())
  }
  ParallelLogger::logInfo("Computing overlap")
  sql <- SqlRender::loadRenderTranslateSql("CohortOverlap.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connection@dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           target_cohort_id = targetCohortId,
                                           comparator_cohort_id = comparatorCohortId)
  overlap <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing overlap took", signif(delta, 3), attr(delta, "units")))
  return(overlap)
}
