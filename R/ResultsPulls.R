# Copyright 2020 Observational Health Data Sciences and Informatics
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



#' Get time distribution data for plotting
#'
#' @description
#' Get time distribution data for plotting from data stored in cohort diagnostics results data
#' model. The output of this function may be used to create plots or tables
#' related to time distribution diagnostics. This function will look for time_distribution table
#' in results data model.
#' 
#' Note: this function relies on data available in Cohort Diagnostics results data model. There are
#' two methods to connect to the resutls data model, database mode and in-memory mode.
#' 
#' Database mode: If either \code{connectionDetails} or \code{\link[DatabaseConnector]{connect}} is 
#' provided in the function call, the query is set to database mode. In the absence of both 
#' \code{connectionDetails} and \code{\link[DatabaseConnector]{connect}}, the query will be in-memory 
#' mode. In in-memory mode, R will expect the data in results data model to be available in R's memory.
#' In database mode, R will perform a database query. Objects in R's memory are expected to
#' follow camelCase naming conventions, while objects in dbms are expected to follow snake-case naming
#' conventions. In database mode, vocabulary tables (if needed) are used from vocabularySchema (which
#' defaults to resultsDatabaseSchema.). In in-memory mode, vocabulary tables are assumed to in R's 
#' memory.
#'
#' @param cohortId       Cohort Id to retrieve the data. This is one of the integer (bigint) value from
#'                       cohortId field in cohort table of the results data model.
#' @param databaseId     The database to retrieve the results for. This is the character field value from
#'                       the databaseId field in the database table of the results data model.
#' @template Connection
#' @param resultsDatabaseSchema (optional) The databaseSchema where the results data model of cohort diagnostics
#'                              is stored. This is only required when \code{connectionDetails} or 
#'                              \code{\link[DatabaseConnector]{connect}} is provided.                       
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' timeDistribution <- getTimeDistribution(cohortId = 343242,
#'                                         databaseId = 'eunomia')
#' }
#'
#' @export

getTimeDistribution <- function(connection = NULL,
                                connect = NULL,
                                cohortId,
                                databaseId,
                                resultsDatabaseSchema = NULL) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertInt(x = cohortId, 
                       na.ok = FALSE, 
                       null.ok = FALSE,
                       lower = 0,
                       upper = 2^53,
                       add = errorMessage)
  checkmate::assertCharacter(x = databaseId,
                             min.len = 1,
                             max.len = 1,
                             any.missing = FALSE,
                             add = errorMessage)
  
  if (!NULL(connect) || !NULL(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logInfo(" \n - No connection or connectionDetails provided.")
      ParallelLogger::logInfo("  Checking if required objects existsin R memory.")
    }
  } else {
    if (exists('timeDistribution')) {
      ParallelLogger::logInfo("  timeDistribution object found in R memory. Continuing.")
    } else {
      ParallelLogger::logWarn("  timeDistribution object not found in R memory. Exiting.")
      return(NULL)
    }
  }
  
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.time_distribution
              WHERE cohort_definition_id = @cohortId
            	AND database_id = '@databaseId';"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       cohortId = cohortId,
                                                       database_id = databaseId, 
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- timeDistribution %>% 
      dplyr::filter(.data$cohortDefinitionId == selectedCohort &
                      .data$databaseId %in% selectedDatabaseIds) %>% 
      dplyr::select(-.data$cohortDefinitionId, .data$databaseid) %>% 
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved from time distribution.")
    return(NULL)
  }
  return(data)
}
