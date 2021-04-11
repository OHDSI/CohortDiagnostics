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
    warning("- Target cohort with ID ", 
                            targetCohortId, 
                            " appears to be empty. Was it instantiated? Skipping overlap computation.")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Computing overlap took", signif(delta, 3), attr(delta, "units")))
    return(tidyr::tibble())
  }
  
  if (!checkIfCohortInstantiated(connection = connection,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 cohortId = comparatorCohortId)) {
    warning("- Comparator cohort with ID ", 
                            comparatorCohortId, 
                            " appears to be empty. Was it instantiated? Skipping overlap computation.")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Computing overlap took", signif(delta, 3), attr(delta, "units")))
    return(tidyr::tibble())
  }
  
  sql <- SqlRender::loadRenderTranslateSql("CohortOverlap.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           target_cohort_id = targetCohortId,
                                           comparator_cohort_id = comparatorCohortId)
  overlap <- DatabaseConnector::querySql(connection = connection, 
                                         sql = sql, 
                                         snakeCaseToCamelCase = TRUE) %>% 
    tidyr::tibble()
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing overlap took", signif(delta, 3), attr(delta, "units")))
  return(overlap)
}
