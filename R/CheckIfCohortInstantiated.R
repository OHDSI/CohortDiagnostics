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



#' Checks if a set of cohortId(s) are instantiated in the cohort table
#'
#' @description
#' Given a set of one or more cohortIds and a single cohort table, checks if 
#' all cohortIds in the set are instantiated.
#'
#' @template Connection
#' 
#' @template CohortDatabaseSchema
#' 
#' @template CohortTable
#' 
#' @param cohortIds                   Provide a set of cohort IDs to check if instantiated.
#' 
#' @return
#' Returns TRUE if all cohortIds are instantiated.
checkIfCohortInstantiated <-
  function(connectionDetails = NULL,
           connection = NULL,
           cohortDatabaseSchema,
           cohortTable,
           cohortIds) {
    
    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
    
    sql <-
      "SELECT COUNT(*) COUNT FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id = @cohort_id;"
    count <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql,
        cohort_database_schema = cohortDatabaseSchema,
        cohort_table = cohortTable,
        cohort_id = cohortIds
      )
    count <- count %>% dplyr::pull(1)
    if (count > 0) {
      return(TRUE)
    } else {
      return(FALSE)
    }
  }
