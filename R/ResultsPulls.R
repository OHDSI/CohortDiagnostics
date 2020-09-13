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
#' two methods to connect to the results data model, database mode and in-memory mode.
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
#' @template Connection
#' @param cohortId       Cohort Id to retrieve the data. This is one of the integer (bigint) value from
#'                       cohortId field in cohort table of the results data model.
#' @param databaseId     The database to retrieve the results for. This is the character field value from
#'                       the databaseId field in the database table of the results data model.
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
                                connectionDetails = NULL,
                                cohortId,
                                databaseId,
                                resultsDatabaseSchema = NULL) {
  table = "timeDistribution"
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertInt(x = cohortId, 
                       na.ok = FALSE, 
                       null.ok = FALSE,
                       lower = 1,
                       upper = 2^53,
                       add = errorMessage)
  checkmate::assertCharacter(x = databaseId,
                             min.len = 1,
                             max.len = 5,
                             any.missing = FALSE,
                             unique = TRUE,
                             add = errorMessage)
  
  if (!is.null(connectionDetails) || !is.null(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  # route query
  route <- routeDataQuery(connection = connection,
                          connectionDetails = connectionDetails,
                          table = table, 
                          databaseSchema = resultsDatabaseSchema)
  
  if (route == 'quit') {
    ParallelLogger::logWarn("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table
              WHERE cohort_definition_id = @cohortId
            	AND database_id = '@databaseId';"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       cohortId = cohortId,
                                                       database_id = databaseId, 
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table) %>% 
      dplyr::filter(.data$cohortDefinitionId == cohortId &
                      .data$databaseId %in% !!databaseId) %>% 
      dplyr::select(-.data$cohortDefinitionId, .data$databaseId) %>% 
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved for ", SqlRender::camelCaseToTitleCase(table), ".")
    return(NULL)
  }
  
  data <- data %>% 
    dplyr::select(.data$databaseId,
                  .data$timeMetric, 
                  .data$averageValue, 
                  .data$standardDeviation, 
                  .data$minValue, 
                  .data$p10Value, 
                  .data$p25Value, 
                  .data$medianValue, 
                  .data$p75Value, 
                  .data$p90Value, 
                  .data$maxValue) %>% 
    dplyr::rename(Database = "databaseId",
                  TimeMeasure = "timeMetric", 
                  Average = "averageValue", 
                  SD = "standardDeviation", 
                  Min = "minValue", 
                  P10 = "p10Value", 
                  P25 = "p25Value", 
                  Median = "medianValue", 
                  P75 = "p75Value", 
                  P90 = "p90Value", 
                  Max = "maxValue")
  return(data)
}


#' Get incidence rate data for plotting
#'
#' @description
#' Get incidence rate data for plotting from data stored in cohort diagnostics results data
#' model. The output of this function may be used to create plots or tables
#' related to incidence rate diagnostics. This function will look for incidence_rate table
#' in results data model.
#' 
#' Note: this function relies on data available in Cohort Diagnostics results data model. There are
#' two methods to connect to the results data model, database mode and in-memory mode.
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
#' @template Connection
#' @param cohortId       Cohort Id to retrieve the data. This is one of the integer (bigint) value from
#'                       cohortId field in cohort table of the results data model.
#' @param databaseIds    A vector one or more databaseIds to retrieve the results for. This is a character 
#'                       field values from the databaseId field in the database table of the results data model.
#' @param stratifyByGender       (optional) Do you want to stratify by gender.
#' @param stratifyByAgeGroup     (optional) Do you want to stratify by age group.
#' @param stratifyByCalendarYear (optional) Do you want to stratify by calendar year.
#' @param resultsDatabaseSchema  (optional) The databaseSchema where the results data model of cohort diagnostics
#'                               is stored. This is only required when \code{connectionDetails} or 
#'                               \code{\link[DatabaseConnector]{connect}} is provided.
#' @param minPersonYears (optional) Default value = 1000. Minimum person years needed to create plot.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' incidenceRate <- getIncidenceRate(cohortId = 343242,
#'                                   databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
getIncidenceRate <- function(connection = NULL,
                             connectionDetails = NULL,
                             cohortId,
                             databaseIds,
                             stratifyByGender = TRUE,
                             stratifyByAgeGroup = TRUE,
                             stratifyByCalendarYear = TRUE,
                             minPersonYears = 1000,
                             resultsDatabaseSchema = NULL) {
  table = "incidenceRate"
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertInt(x = cohortId, 
                       na.ok = FALSE, 
                       null.ok = FALSE,
                       lower = 1,
                       upper = 2^53,
                       add = errorMessage)
  checkmate::assertVector(x = databaseIds,
                          min.len = 1,
                          any.missing = FALSE,
                          unique = TRUE,
                          add = errorMessage)
  checkmate::assertCharacter(x = databaseIds,
                             min.len = 1,
                             any.missing = FALSE,
                             unique = TRUE,
                             add = errorMessage)
  
  if (!is.null(connectionDetails) || !is.null(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::assertLogical(x = stratifyByGender,
                           add = errorMessage)
  checkmate::assertLogical(x = stratifyByAgeGroup,
                           add = errorMessage)
  checkmate::assertLogical(x = stratifyByCalendarYear,
                           add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  # route query
  route <- routeDataQuery(connection = connection,
                          connectionDetails = connectionDetails,
                          table = table)
  
  if (route == 'quit') {
    ParallelLogger::logWarn("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table
              WHERE cohort_id = @cohortId
            	AND database_id in c('@databaseIds')
              {@gender == TRUE} ? {AND gender ISNULL} : {AND gender NOT ISNULL}
              {@age_group == TRUE} ? {AND age_group ISNULL} : {AND age_group NOT ISNULL}
            	{@calendar_year == TRUE} ? {AND calendar_year ISNULL} : {AND calendar_year NOT ISNULL}
              AND person_years > @personYears;"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       cohortId = cohortId,
                                                       databaseIds = databaseIds,
                                                       gender = stratifyByGender,
                                                       age_group = stratifyByAgeGroup,
                                                       calendar_year = stratifyByCalendarYear,
                                                       personYears = minPersonYears,
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table) %>% 
      dplyr::filter(.data$cohortId == cohortId &
                      .data$databaseId %in% databaseIds &
                      is.na(.data$gender) == stratifyByGender &
                      is.na(.data$ageGroup) == stratifyByAgeGroup &
                      is.na(.data$calendarYear) == stratifyByCalendarYear) %>% 
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved for 'incidence rate'.")
    return(NULL)
  }
  return(data)
}



#' Get cohort counts
#'
#' @description
#' Get cohort counts data from data stored in cohort diagnostics results data
#' model. The output of this function may be used to create tables
#' related to cohort counts diagnostics. This function will look for cohort_count table
#' in results data model.
#' 
#' Note: this function relies on data available in Cohort Diagnostics results data model. There are
#' two methods to connect to the results data model, database mode and in-memory mode.
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
#' @template Connection
#' @param databaseIds    A vector one or more databaseIds to retrieve the results for. This is a character 
#'                       field value from the databaseId field in the database table of the results data model.
#' @param resultsDatabaseSchema (optional) The databaseSchema where the results data model of cohort diagnostics
#'                              is stored. This is only required when \code{connectionDetails} or 
#'                              \code{\link[DatabaseConnector]{connect}} is provided.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' cohortCounts <- getCohortCounts(resultsDatabaseSchema = resultsDatabaseSchema,
#'                                 databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
getCohortCounts <- function(connection = NULL,
                            connectionDetails = NULL,
                            databaseIds,
                            resultsDatabaseSchema = NULL) {
  table = "cohortCount"
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  if (!is.null(connectionDetails) || !is.null(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  # route query
  route <- routeDataQuery(connection = connection,
                          connectionDetails = connectionDetails,
                          table = table)
  
  if (route == 'quit') {
    ParallelLogger::logWarn("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table
            	WHERE database_id in c('@databaseIds');;"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema, 
                                                       databaseIds = databaseIds,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table) %>% 
      dplyr::filter(.data$databaseId %in% databaseIds) %>% 
      tidyr::tibble()
  }
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved for '", SqlRender::camelCaseToTitleCase(table), "'")
    return(NULL)
  }
  data <- data %>% 
    dplyr::relocate(.data$cohortId, .data$cohortName)
  return(data)
}


#' Get cohort overlap
#'
#' @description
#' Get cohort overlap data from data stored in cohort diagnostics results data
#' model. The output of this function may be used to create tables/plots to compare
#' two cohorts (target and comparator) related to cohort overlap diagnostics. 
#' This function will look for cohort_overlap table in results data model.
#' 
#' Note: this function relies on data available in Cohort Diagnostics results data model. There are
#' two methods to connect to the results data model, database mode and in-memory mode.
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
#' @template Connection
#' @param databaseIds    A vector one or more databaseIds to retrieve the results for. This is a character 
#'                       field value from the databaseId field in the database table of the results data model.
#' @param targetCohortId        Cohort Id to retrieve the data. This is one of the integer (bigint) value from
#'                              cohortId field in cohort table of the results data model.
#' @param comparatorCohortId    Cohort Id to retrieve the data. This is one of the integer (bigint) value from
#'                              cohortId field in cohort table of the results data model.
#' @param resultsDatabaseSchema (optional) The databaseSchema where the results data model of cohort diagnostics
#'                              is stored. This is only required when \code{connectionDetails} or 
#'                              \code{\link[DatabaseConnector]{connect}} is provided.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' cohortOverlap <- getCohortOverLap(targetCohortId = 342432,
#'                                   comparatorCohortId = 432423,
#'                                   databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
getCohortOverLap <- function(connection = NULL,
                             connectionDetails = NULL,
                             targetCohortId,
                             comparatorCohortId,
                             databaseId,
                             resultsDatabaseSchema = NULL) {
  table = "cohortOverlap"
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertInt(x = targetCohortId, 
                       na.ok = FALSE, 
                       null.ok = FALSE,
                       lower = 1,
                       upper = 2^53,
                       add = errorMessage)
  checkmate::assertInt(x = comparatorCohortId, 
                       na.ok = FALSE, 
                       null.ok = FALSE,
                       lower = 1,
                       upper = 2^53,
                       add = errorMessage)
  checkmate::assertVector(x = databaseId,
                          min.len = 1,
                          any.missing = FALSE,
                          unique = TRUE,
                          add = errorMessage)
  checkmate::assertCharacter(x = databaseId,
                             min.len = 1,
                             any.missing = FALSE,
                             unique = TRUE,
                             add = errorMessage)
  
  if (!is.null(connectionDetails) || !is.null(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  # route query
  route <- routeDataQuery(connection = connection,
                          connectionDetails = connectionDetails,
                          table = table)
  
  if (route == 'quit') {
    ParallelLogger::logWarn("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table
              WHERE target_cohort_id = @targetCohortId
              AND comparator_cohort_id = @comparatorCohortId
            	AND database_id in c('@databaseId');"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       targetCohortId = targetCohortId,
                                                       comparatorCohortId = comparatorCohortId,
                                                       databaseId = databaseId, 
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table) %>% 
      dplyr::filter(.data$targetCohortId == !!targetCohortId &
                      .data$comparatorCohortId == !!comparatorCohortId &
                      .data$databaseId == !!databaseId) %>% 
      tidyr::tibble()
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  data <- data %>% 
    dplyr::relocate(.data$databaseId, .data$targetCohortId, .data$comparatorCohortId)
  return(data)
}


#' Get cohort covariate reference (including temporal)
#'
#' @description
#' Get covariate reference data from data stored in cohort diagnostics results data
#' model. The output of this function may be used, together with covariate value
#' to create tables/plots regarding a cohorts characteristics diagnostics. 
#' 
#' Note: this function relies on data available in Cohort Diagnostics results data model. There are
#' two methods to connect to the results data model, database mode and in-memory mode.
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
#' @template Connection
#' @param isTemporal     Get temporal covariate references?
#' @param resultsDatabaseSchema (optional) The databaseSchema where the results data model of cohort diagnostics
#'                              is stored. This is only required when \code{connectionDetails} or 
#'                              \code{\link[DatabaseConnector]{connect}} is provided.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' covariateReference <- getCovariateReference()
#' }
#'
#' @export
getCovariateReference <- function(connection = NULL,
                                  connectionDetails = NULL,
                                  isTemporal = TRUE,
                                  resultsDatabaseSchema = NULL) {
  table <- 'covariateReference'
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(x = isTemporal, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           max.len = 1)
  
  if (!is.null(connectionDetails) || !is.null(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  # route query
  route <- routeDataQuery(connection = connection,
                          connectionDetails = connectionDetails,
                          table = table)
  
  if (route == 'quit') {
    ParallelLogger::logWarn("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table;"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table)
  }
  
  data <- data %>% #occassionally we may have more than one covariateName per covariateId
    # because of change in concept_name. See https://github.com/OHDSI/CohortDiagnostics/issues/162
    dplyr::group_by(.data$covariateId) %>% 
    dplyr::slice(1)
  return(data)
}



#' Get time reference for temporal covariates
#'
#' @description
#' Get time reference data from data stored in cohort diagnostics results data
#' model. The output of this function may be used, together with temporal covariate value
#' to create tables/plots related to temporal cohorts characteristics diagnostics. 
#' 
#' Note: this function relies on data available in Cohort Diagnostics results data model. There are
#' two methods to connect to the results data model, database mode and in-memory mode.
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
#' timeReference <- getTimeReference()
#' }
#'
#' @export
getTimeReference <- function(connection = NULL,
                             connectionDetails = NULL,
                             resultsDatabaseSchema = NULL){
  table <- 'timeRef'
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  
  if (!is.null(connectionDetails) || !is.null(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  # route query
  route <- routeDataQuery(connection = connection,
                          connectionDetails = connectionDetails,
                          table = table)
  
  if (route == 'quit') {
    ParallelLogger::logWarn("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table;"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table)
  }
  return(data)
}

#' Get cohort covariate (including temporal)
#'
#' @description
#' Get cohort covariate value data from data stored in cohort diagnostics results data
#' model. The output of this function may be used, together with covariate ref
#' to create tables/plots regarding a cohorts characteristics diagnostics. 
#' Because of the large volume of covariates, this function allows to filter range of 
#' covariate_value by providing the minimum and maximum proportion. Its important to note 
#' that all covariates are expected to output proportions that are between the values 0.0 to 1.0
#' 
#' Note: this function relies on data available in Cohort Diagnostics results data model. There are
#' two methods to connect to the results data model, database mode and in-memory mode.
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
#' @template Connection
#' @param databaseIds    A vector one or more databaseIds to retrieve the results for. This is a character 
#'                       field value from the databaseId field in the database table of the results data model.
#' @param cohortIds      Cohort Ids to retrieve the characterization data. 
#' @param isTemporal     (optional) Get temporal covariate values?
#' @param timeIds        (optional) Used only if isTemporal = TRUE. Do you want to limit to certain
#'                        'time ids'. By default all time ids are returned. 
#' @param resultsDatabaseSchema (optional) The databaseSchema where the results data model of cohort diagnostics
#'                              is stored. This is only required when \code{connectionDetails} or 
#'                              \code{\link[DatabaseConnector]{connect}} is provided.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' covariateValue <- getPreComputedCovariateValues(cohortId = c(342432,432423),
#'                                           databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
getPreComputedCovariateValues <- function(connection = NULL,
                                          connectionDetails = NULL,
                                          cohortIds,
                                          databaseIds,
                                          minProportion = 0.01,
                                          maxProportion = 1,
                                          isTemporal = TRUE,
                                          timeIds = NULL,
                                          resultsDatabaseSchema = NULL) {
  if (isTemporal) {
    table <- 'temporalCovariateValue'
  } else {
    table <- 'covariateValue'
  }
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(x = cohortIds, 
                              lower = 1,
                              upper = 2^53,
                              any.missing = FALSE,
                              min.len = 1,
                              unique = TRUE,
                              null.ok = FALSE,
                              add = errorMessage)
  checkmate::assertVector(x = cohortIds,
                          min.len = 1,
                          any.missing = FALSE,
                          unique = TRUE,
                          add = errorMessage)
  checkmate::assertVector(x = databaseIds,
                          min.len = 1,
                          any.missing = FALSE,
                          unique = TRUE,
                          add = errorMessage)
  checkmate::assertCharacter(x = databaseIds,
                             min.len = 1,
                             any.missing = FALSE,
                             unique = TRUE,
                             add = errorMessage)
  checkmate::assertLogical(x = isTemporal, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           max.len = 1)
  checkmate::reportAssertions(collection = errorMessage)
  
  if (isTemporal) {
    checkmate::assertInteger(x = timeIds, 
                             lower = 0, 
                             any.missing = FALSE, 
                             unique = TRUE, 
                             null.ok = FALSE, 
                             add = errorMessage)
  }
  
  if (!is.null(connectionDetails) || !is.null(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  # route query
  route <- routeDataQuery(connection = connection,
                          connectionDetails = connectionDetails,
                          table = table)
  
  if (route == 'quit') {
    ParallelLogger::logWarn("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table
              WHERE cohort_id in (@cohortId)
            	AND database_id in ('@databaseIds')
              {@isTemporal == TRUE} ? {AND time_id in ('@timeIds')}
              AND mean >= @minProportion
              AND mean <= @maxProportion;"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       cohortId = cohortId,
                                                       databaseIds = databaseIds, 
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       isTemporal = isTemporal,
                                                       timeIds = timeIds,
                                                       minProportion = minProportion,
                                                       maxProportion = maxProportion,
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table) %>% 
      dplyr::filter(.data$cohortId %in% cohortId,
                    .data$databaseId %in% databaseIds,
                    .data$mean >= minProportion,
                    .data$mean <= maxProportion)
    if (isTemporal && !is.null(timeIds)) {
      data <- data %>% 
        dplyr::filter(.data$timeId %in% timeIds)
    }
  }
  return(data)
}



#' Get cohort covariate comparison (including temporal)
#'
#' @description
#' Get cohort covariate value comparison data for cohorts stored in cohort diagnostics results data
#' model. The output of this function may be used, together with covariate ref
#' to create tables/plots regarding a cohort characteristics comparison diagnostics. 
#' Because of the large volume of covariates, this function allows to filter range of 
#' covariate_value by providing the minimum and maximum proportion. Its important to note 
#' that all covariates are expected to output proportions that are between the values 0.0 to 1.0
#' 
#' Note: this function relies on data available in Cohort Diagnostics results data model. There are
#' two methods to connect to the results data model, database mode and in-memory mode.
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
#' @template Connection
#' @param databaseIds    A vector one or more databaseIds to retrieve the results for. This is a character 
#'                       field value from the databaseId field in the database table of the results data model.
#' @param targetCohortIds      Cohort Ids to retrieve and serve as target cohorts for comparison.
#' @param comparatorCohortIds  Cohort Ids to retrieve the serve as comparator cohorts for comparison.
#' @param isTemporal     (optional) Get temporal covariate values?
#' @param timeIds        (optional) Used only if isTemporal = TRUE. Do you want to limit to certain
#'                        'time ids'. By default all time ids are returned. 
#' @param resultsDatabaseSchema (optional) The databaseSchema where the results data model of cohort diagnostics
#'                              is stored. This is only required when \code{connectionDetails} or 
#'                              \code{\link[DatabaseConnector]{connect}} is provided.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' covariateValue <- compareCovariateValue(targetCohortId = c(342432,432423),
#'                                         comparatorCohortId = c(34243, 342432),
#'                                         databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
compareCovariateValue <- function(connection = NULL,
                                  connectionDetails = NULL,
                                  targetCohortIds,
                                  comparatorCohortIds,
                                  databaseIds,
                                  minProportion = 0.01,
                                  maxProportion = 1,
                                  isTemporal = TRUE,
                                  timeIds = NULL,
                                  resultsDatabaseSchema = NULL) {
  if (isTemporal) {
    table <- 'temporalCovariateValue'
  } else {
    table <- 'covariateValue'
  }
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(x = targetCohortIds, 
                              lower = 1,
                              upper = 2^53,
                              any.missing = FALSE,
                              min.len = 1,
                              unique = TRUE,
                              null.ok = FALSE,
                              add = errorMessage)
  checkmate::assertIntegerish(x = comparatorCohortIds, 
                              lower = 1,
                              upper = 2^53,
                              any.missing = FALSE,
                              min.len = 1,
                              unique = TRUE,
                              null.ok = FALSE,
                              add = errorMessage)
  checkmate::assertVector(x = targetCohortIds,
                          min.len = 1,
                          any.missing = FALSE,
                          unique = TRUE,
                          add = errorMessage)
  checkmate::assertVector(x = comparatorCohortIds,
                          min.len = 1,
                          any.missing = FALSE,
                          unique = TRUE,
                          add = errorMessage)
  checkmate::assertVector(x = databaseIds,
                          min.len = 1,
                          any.missing = FALSE,
                          unique = TRUE,
                          add = errorMessage)
  checkmate::assertCharacter(x = databaseIds,
                             min.len = 1,
                             any.missing = FALSE,
                             unique = TRUE,
                             add = errorMessage)
  checkmate::assertLogical(x = isTemporal, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           max.len = 1)
  checkmate::reportAssertions(collection = errorMessage)
  
  if (isTemporal) {
    checkmate::assertInteger(x = timeIds, 
                             lower = 0, 
                             any.missing = FALSE, 
                             unique = TRUE, 
                             null.ok = FALSE, 
                             add = errorMessage)
  }
  
  if (!is.null(connectionDetails) || !is.null(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  cohortsIds <- c(targetCohortIds, comparatorCohortIds) %>% unique() %>% sort()
  
  covariateValue <- getPreComputedCovariateValues(connection = connection, 
                                                  connectionDetails = connectionDetails, 
                                                  cohortIds = cohortIds, 
                                                  databaseIds = databaseIds, 
                                                  minProportion = minProportion, 
                                                  maxProportion = maxProportion, 
                                                  isTemporal = isTemporal, 
                                                  timeIds = timeIds, 
                                                  resultsDatabaseSchema = resultsDatabaseSchema)
  
  targetCovariateValue = covariateValue %>% 
    dplyr::filter(.data$cohortIds %in% targetCohortIds) %>% 
    dplyr::rename(targetCohortId = cohortId)
  comparatorCovariateValue = covariateValue %>% 
    dplyr::filter(.data$cohortIds %in% comparatorCovariateValue) %>% 
    dplyr::rename(comparatorCohortId = cohortId)
  
  data <- dplyr::full_join(x = targetCovariateValue,
                        y = comparatorCovariateValue,
                        suffix = c("1", "2")) %>%
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = 0)),
                  sd = sqrt(.data$sd1^2 + .data$sd2^2),
                  stdDiff = (.data$mean2 - .data$mean1)/.data$sd) %>%
    dplyr::arrange(-abs(.data$stdDiff)) %>%
    dplyr::mutate(absStdDiff = abs(.data$stdDiff))
  
  return(data)
}

routeDataQuery <- function(connection = NULL,
                           connectionDetails = NULL,
                           table,
                           checkInDbms = FALSE,
                           checkInRMemory = TRUE,
                           databaseSchema = NULL,
                           silent = TRUE
) {
  if (is.null(connection)) {
    if (is.null(connectionDetails)) {
      if (!silent) {
        ParallelLogger::logInfo("\n- No connection or connectionDetails provided.")
        ParallelLogger::logInfo("  Checking if required objects exists in R memory.")
      }
    } else {
      if (!silent) {
        ParallelLogger::logInfo("\n- No existing connection to dbms provided. But connection details found.")
        ParallelLogger::logInfo("  Attempting to establish connection to dbms using connection details.")
      }
      connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    }
  }
  
  if (!checkInDbms) {
    tableExistsInDbms <- TRUE
  }
  
  if (!is.null(connection) && isTRUE(checkInDbms)) {
    tableExistsInDbms <- DatabaseConnector::dbExistsTable(conn = connection,
                                                          name = paste0(databaseSchema, ".", table)
    )
    if (!tableExistsInDbms) {
      if (!silent) {
        ParallelLogger::logWarn("  '", table, "' not found in ", databaseSchema)
      }
    }
  }
  
  if (checkInRMemory) {
    if (exists(table)) {
      tableExistsInRMemory <- TRUE
      if (!silent) {
        ParallelLogger::logInfo("  '", SqlRender::camelCaseToTitleCase(table), "' data object found in R memory.")
      }
    } else {
      tableExistsInRMemory <- FALSE
      if (is.null(connection)) {
        if (!silent) {
          ParallelLogger::logWarn("  '", SqlRender::camelCaseToTitleCase(table), "' data object not found in R memory.")
        }
      } else {
        if (!silent) {
          ParallelLogger::logInfo("  '", SqlRender::camelCaseToTitleCase(table), "' data object not found in R memory.")
        }
      }
    }
  }
  if (!is.null(connection) & isTRUE(tableExistsInDbms)) {
    return(connection)
  } else if (!is.null(connection) & !isTRUE(tableExistsInDbms) & isTRUE(tableExistsInRMemory)) {
    ParallelLogger::logWarn(SqlRender::camelCaseToTitleCase(table), " was not found in dbms but was found in R memory. Using the data loaded in R memory.")
    return("memory")
  } else if (is.null(connection) & isTRUE(tableExistsInRMemory)) {
    return("memory")
  } else if (is.null(connection) & !isTRUE(tableExistsInRMemory)) {
    ParallelLogger::logWarn(SqlRender::camelCaseToTitleCase(table), " not found.")
    return("quit")
  }
}
