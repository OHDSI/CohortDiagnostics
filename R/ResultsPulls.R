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
                             max.len = 1,
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
  
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logInfo(" \n - No connection or connectionDetails provided.")
      ParallelLogger::logInfo("  Checking if required objects existsin R memory.")
      if (exists('timeDistribution')) {
        ParallelLogger::logInfo("  'time distribution' data object found in R memory. Continuing.")
      } else {
        ParallelLogger::logWarn("  'time distribution' data object not found in R memory. Exiting.")
        return(NULL)
      }
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
      dplyr::filter(.data$cohortDefinitionId == cohortId &
                      .data$databaseId %in% databaseId) %>% 
      dplyr::select(-.data$cohortDefinitionId, .data$databaseId) %>% 
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved for 'time distribution'.")
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
  
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logInfo(" \n - No connection or connectionDetails provided.")
      ParallelLogger::logInfo("  Checking if required objects existsin R memory.")
      if (exists('incidenceRate')) {
        ParallelLogger::logInfo("  'incidence rate' data object found in R memory. Continuing.")
      } else {
        ParallelLogger::logWarn("  'incidence rate' data object not found in R memory. Exiting.")
        return(NULL)
      }
    }
  }
  
  if (!is.null(connection)) {
    sql <-   "SELECT database_id, incidence_rate, person_years, cohort_count
              FROM  @resultsDatabaseSchema.incidence_rate
              WHERE cohort_id = @cohortId
            	AND database_id in c('@databaseIds')
              {@gender == TRUE} ? {AND gender ISNULL} : {AND gender NOT ISNULL}
              {@age_group == TRUE} ? {AND age_group ISNULL} : {AND age_group NOT ISNULL}
            	{@calendar_year == TRUE} ? {AND calendar_year ISNULL} : {AND calendar_year NOT ISNULL}
              AND person_years > @personYears;"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       cohortId = cohortId,
                                                       databaseIds = databaseIds,
                                                       gender = stratifyByGender,
                                                       age_group = stratifyByAgeGroup,
                                                       calendar_year = stratifyByCalendarYear,
                                                       personYears = minPersonYears,
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- incidenceRate %>% 
      dplyr::filter(.data$cohortId == cohortId &
                      .data$databaseId %in% databaseIds &
                      is.na(.data$gender) == stratifyByGender &
                      is.na(.data$ageGroup) == stratifyByAgeGroup &
                      is.na(.data$calendarYear) == stratifyByCalendarYear) %>% 
      dplyr::select(.data$databaseId, .data$incidenceRate, .data$personYears, .data$cohortCount) %>% 
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
  
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logInfo(" \n - No connection or connectionDetails provided.")
      ParallelLogger::logInfo("  Checking if required objects existsin R memory.")
      if (exists('cohortCount') && exists('cohort')) {
        ParallelLogger::logInfo("  'cohort count' & 'cohort' data object found in R memory. Continuing.")
      } else {
        ParallelLogger::logWarn("  'cohort count' or 'cohort' data object not found in R memory. Exiting.")
        return(NULL)
      }
    }
  }
  
  if (!is.null(connection)) {
    sql <-   "SELECT cc.*, c.cohort_name
              FROM  @resultsDatabaseSchema.cohort_count cc
              INNER JOIN @resulsDatabaseSchema.cohort c
              ON cc.cohort_id = c.cohort_id
            	WHERE database_id in c('@databaseIds');;"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema, 
                                                       databaseIds = databaseIds,
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- cohortCount %>% 
      dplyr::filter(.data$databaseId %in% databaseIds) %>% 
      dplyr::inner_join(cohort %>% dplyr::select(.data$cohortId, .data$cohortName)) %>% 
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved for 'cohort count'.")
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
                             databaseIds,
                             resultsDatabaseSchema = NULL) {
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
  checkmate::reportAssertions(collection = errorMessage)
  
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logInfo(" \n - No connection or connectionDetails provided.")
      ParallelLogger::logInfo("  Checking if required objects existsin R memory.")
      if (exists('cohortOverlap')) {
        ParallelLogger::logInfo("  'cohort overlap' data object found in R memory. Continuing.")
      } else {
        ParallelLogger::logWarn("  'cohort overlap' data object not found in R memory. Exiting.")
        return(NULL)
      }
    }
  }
  
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.cohort_overlap
              WHERE target_cohort_id = @targetCohortId
              AND comparator_cohort_id = @comparatorCohortId
            	AND database_id in c('@databaseIds');"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       targetCohortId = targetCohortId,
                                                       comparatorCohortId = comparatorCohortId,
                                                       databaseIds = databaseIds, 
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- cohortOverlap %>% 
      dplyr::filter(.data$targetCohortId == targetCohortId &
                      .data$comparatorCohortId == comparatorCohortId &
                      .data$databaseId %in% databaseIds) %>% 
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    return(NULL)
  }
  data <- data %>% 
    dplyr::relocate(.data$databaseId, .data$targetCohortId, .data$comparatorCohortId)
  
  return(data)
}




#' Get cohort covariate
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
#' @param isTemporal     Get temporal covariate values?
#' @param resultsDatabaseSchema (optional) The databaseSchema where the results data model of cohort diagnostics
#'                              is stored. This is only required when \code{connectionDetails} or 
#'                              \code{\link[DatabaseConnector]{connect}} is provided.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' covariateValue <- getCohortCovariateValue(cohortId = c(342432,432423),
#'                                           databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
getCohortCovariateValue <- function(connection = NULL,
                                    connectionDetails = NULL,
                                    cohortIds,
                                    databaseIds,
                                    minProportion = 0.01,
                                    maxProportion = 1,
                                    isTemporal = FALSE,
                                    resultsDatabaseSchema = NULL){
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
  
  if (!is.null(connectionDetails) || !is.null(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  if (isTemporal) {
    value <- 'temporalCovariateValue'
  } else {
    value <- 'covariateValue'
  }
  
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logInfo(" \n - No connection or connectionDetails provided.")
      ParallelLogger::logInfo("  Checking if required objects existsin R memory.")
      if (exists(value)) {
        ParallelLogger::logInfo("  '", SqlRender::camelCaseToTitleCase(value), "' data object found in R memory. Continuing.")
      } else {
        ParallelLogger::logWarn("  '", SqlRender::camelCaseToTitleCase(value), "' data object found in R memory. Exiting.")
        return(NULL)
      }
    }
  }
  
  if (!is.null(connection)) {
    sql <-   "SELECT cohort_id, database_id, covariate_id, mean, sd
              FROM  @resultsDatabaseSchema.@value
              WHERE cohort_id in (@cohortId)
            	AND database_id in ('@databaseIds')
              AND mean >= @minProportion
              AND mean <= @maxProportion;"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       cohortId = cohortId,
                                                       databaseIds = databaseIds, 
                                                       value = SqlRender::camelCaseToSnakeCase(value),
                                                       minProportion = minProportion,
                                                       maxProportion = maxProportion,
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(value) %>% 
      dplyr::filter(.data$cohortId %in% cohortId,
                    .data$databaseId %in% databaseIds,
                    .data$mean >= minProportion,
                    .data$mean <= maxProportion) %>%
      dplyr::select(.data$cohortId,
                    .data$databaseId, 
                    .data$covariateId, 
                    .data$mean, .data$sd)
  }
  return(data)
}


  
  
  
  covariate <- covariateRef %>% #occassionally we may have more than one covariateName per covariateId
    # because of change in concept_name. See https://github.com/OHDSI/CohortDiagnostics/issues/162
    dplyr::group_by(.data$covariateId) %>% 
    dplyr::slice(1)
  
  data <- data %>% 
    dplyr::inner_join(covariate) %>% 
    dplyr::select(.data$databaseId, .data$covariateId, .data$conceptId, .data$covariateName,
                  .data$mean, .data$sd) %>% 
    dplyr::arrange(.data$conceptId, .data$covariateId, .data$covariateName)
  
  return(data)
}



covs1 <- covariateValue %>% 
  dplyr::filter(.data$cohortId == cohortId,
                .data$databaseId == databaseIds)
covs2 <- covariateValue %>% 
  dplyr::filter(.data$cohortId == comparatorCohortId,
                .data$databaseId == databaseIds)
}


if (cohortId == comparatorCohortId) {
  return(tidyr::tibble())
}

covs1 <- dplyr::left_join(x = covs1, y = covariate)
covs2 <- dplyr::left_join(x = covs2, y = covariate)

m <- dplyr::full_join(x = covs1 %>% dplyr::distinct(), 
                      y = covs2 %>% dplyr::distinct(), 
                      by = c("covariateId", "conceptId", "databaseId", "covariateName", "covariateAnalysisId"),
                      suffix = c("1", "2")) %>%
  dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = 0)),
                sd = sqrt(.data$sd1^2 + .data$sd2^2),
                stdDiff = (.data$mean2 - .data$mean1)/.data$sd) %>% 
  dplyr::arrange(-abs(.data$stdDiff))

data <- m %>%
  dplyr::mutate(absStdDiff = abs(.data$stdDiff))

return(data)

}

