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



#' Get time distribution results
#'
#' @description
#' Get time distribution results
#'
#' @template ModeAndDetails
#' @template CohortIds
#' @template DatabaseIds
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' timeDistribution <- getTimeDistributionResult(cohortIds = 343242,
#'                                               databaseIds = 'eunomia')
#' }
#'
#' @export
getTimeDistributionResult <- function(connection = NULL,
                                      connectionDetails = NULL,
                                      cohortIds,
                                      databaseIds,
                                      resultsDatabaseSchema = NULL) {
  table = "timeDistribution"
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = cohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
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
              WHERE cohort_id in (@cohortId)
            	AND database_id in (@databaseIds);"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       cohortId = cohortIds,
                                                       database_id = databaseIds, 
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table) %>% 
      dplyr::filter(.data$cohortId %in% !!cohortIds &
                      .data$databaseId %in% !!databaseIds) %>% 
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved for ", SqlRender::camelCaseToTitleCase(table), ".")
    return(NULL)
  }
  
  data <- data %>% 
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
                  Max = "maxValue") %>% 
    dplyr::relocate(.data$cohortId, .data$Database, .data$TimeMeasure) %>% 
    dplyr::arrange(.data$cohortId, .data$Database, .data$TimeMeasure)
  
  # data <- data %>% 
  #   dplyr::mutate(dplyr::across(.cols = c(.data$Database, 
  #                                         .data$TimeMeasure), 
  #                               .fns = dplyr::case_when(stringr::str_detect(string = .,
  #                                                                           pattern = "_") ~ 
  #                                                         SqlRender::snakeCaseToCamelCase()
  #                                                       )))
  return(data)
}



#' Get incidence rate results
#'
#' @description
#' Get incidence rate results.
#' 
#' @template ModeAndDetails
#' @template CohortIds
#' @template DatabaseIds
#' @param stratifyByGender       (optional) Do you want to stratify by gender.
#' @param stratifyByAgeGroup     (optional) Do you want to stratify by age group.
#' @param stratifyByCalendarYear (optional) Do you want to stratify by calendar year.
#' @param minPersonYears         (optional) Default value = 1000. Minimum person years needed to create plot.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' incidenceRate <- getIncidenceRateResult(cohortIds = 343242,
#'                                         databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
getIncidenceRateResult <- function(connection = NULL,
                                   connectionDetails = NULL,
                                   cohortIds,
                                   databaseIds,
                                   stratifyByGender = c(TRUE,FALSE),
                                   stratifyByAgeGroup = c(TRUE,FALSE),
                                   stratifyByCalendarYear = c(TRUE,FALSE),
                                   minPersonYears = 1000,
                                   resultsDatabaseSchema = NULL) {
  table = "incidenceRate"
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = cohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
  checkmate::assertLogical(x = stratifyByGender,
                           add = errorMessage,
                           min.len = 1,
                           max.len = 2,
                           unique = TRUE)
  checkmate::assertLogical(x = stratifyByAgeGroup,
                           add = errorMessage,
                           min.len = 1,
                           max.len = 2,
                           unique = TRUE)
  checkmate::assertLogical(x = stratifyByCalendarYear,
                           add = errorMessage,
                           min.len = 1,
                           max.len = 2,
                           unique = TRUE)
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
              WHERE cohort_id in (@cohortIds)
            	AND database_id in c('@databaseIds')
              {@gender == TRUE} ? {AND gender ISNULL} : {AND gender NOT ISNULL}
              {@age_group == TRUE} ? {AND age_group ISNULL} : {AND age_group NOT ISNULL}
            	{@calendar_year == TRUE} ? {AND calendar_year ISNULL} : {AND calendar_year NOT ISNULL}
              AND person_years > @personYears;"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       cohortId = cohortIds,
                                                       databaseIds = databaseIds,
                                                       gender = stratifyByGender,
                                                       age_group = stratifyByAgeGroup,
                                                       calendar_year = stratifyByCalendarYear,
                                                       personYears = minPersonYears,
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table) %>% 
      dplyr::mutate(strataGender = !is.na(.data$gender),
                    strataAgeGroup = !is.na(.data$ageGroup),
                    strataCalendarYear = !is.na(.data$calendarYear)) %>% 
      dplyr::filter(.data$cohortId %in% !!cohortIds &
                      .data$databaseId %in% !!databaseIds &
                      .data$strataGender %in% !!stratifyByGender &
                      .data$strataAgeGroup %in% !!stratifyByAgeGroup &
                      .data$strataCalendarYear %in% !!stratifyByCalendarYear &
                      .data$personYears > !!minPersonYears) %>% 
      dplyr::select(-tidyselect::starts_with('strata')) %>% 
      tidyr::tibble()
  }
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved for 'incidence rate'.")
  }
  return(data %>% 
           dplyr::arrange(.data$cohortId, .data$databaseId))
}



#' Get cohort counts
#'
#' @description
#' Get cohort counts
#' 
#' @template ModeAndDetails
#' @template DatabaseIds
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' cohortCounts <- getCohortCountResult(databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
getCohortCountResult <- function(connection = NULL,
                                 connectionDetails = NULL,
                                 databaseIds = NULL,
                                 resultsDatabaseSchema = NULL) {
  table = "cohortCount"
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
  checkmate::assertCharacter(x = databaseIds,
                             any.missing = FALSE, 
                             min.len = 1,
                             null.ok = TRUE,
                             add = errorMessage)
  
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
            	WHERE database_id in c('@databaseIds');"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema, 
                                                       databaseIds = databaseIds,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table) 
    if (!is.null(databaseIds)) { 
      data <- data %>% 
        dplyr::filter(.data$databaseId %in% databaseIds)
    }
  }
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved for '", SqlRender::camelCaseToTitleCase(table), "'")
    return(NULL)
  }
  data <- data %>% 
    dplyr::relocate(.data$cohortId, .data$databaseId) %>% 
    dplyr::arrange(.data$cohortId, .data$databaseId)
  return(data)
}



#' Get cohort overlap
#'
#' @description
#' Get cohort overlap data
#' 
#' @template ModeAndDetails
#' @template DatabaseIds
#' @param targetCohortIds        A vector of one or more Cohort Ids.
#' @param comparatorCohortIds    A vector of one or more Cohort Ids.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' cohortOverlap <- getCohortOverlapResult(targetCohortIds = 342432,
#'                                         comparatorCohortIds = c(432423,34234),
#'                                         databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
getCohortOverlapResult <- function(connection = NULL,
                                   connectionDetails = NULL,
                                   targetCohortIds,
                                   comparatorCohortIds,
                                   databaseIds,
                                   resultsDatabaseSchema = NULL) {
  table = "cohortOverlap"
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = targetCohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = comparatorCohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
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
              WHERE target_cohort_id in (@targetCohortIds)
              AND comparator_cohort_id in (@comparatorCohortIds)
            	AND database_id in c('@databaseIds');"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       targetCohortId = targetCohortIds,
                                                       comparatorCohortId = comparatorCohortIds,
                                                       databaseId = databaseIds, 
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table) %>% 
      dplyr::filter(.data$targetCohortId %in% !!targetCohortIds &
                      .data$comparatorCohortId %in% !!comparatorCohortIds &
                      .data$databaseId %in% !!databaseIds) %>% 
      tidyr::tibble()
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  data <- data %>% 
    dplyr::relocate(.data$databaseId, .data$targetCohortId, .data$comparatorCohortId) %>% 
    dplyr::arrange(.data$databaseId, .data$targetCohortId, .data$comparatorCohortId)
  return(data)
}



#' Get cohort covariate reference (including temporal)
#'
#' @description
#' Get cohort covariate reference (including temporal). 
#'
#' @template ModeAndDetails
#' @param isTemporal     Get temporal covariate references?
#' @param covariateIds   (optional) A vector of covariateIds to subset the results
#'  
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' covariateReference <- getCovariateReference(isTemporal = FALSE)
#' }
#'
#' @export
getCovariateReference <- function(connection = NULL,
                                  connectionDetails = NULL,
                                  covariateIds = NULL,
                                  isTemporal = TRUE,
                                  resultsDatabaseSchema = NULL) {
  if (isTemporal) {
    table <- 'temporalCovariateRef'
  } else {
    table <- 'covariateRef'
  }
  
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(x = isTemporal, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           max.len = 1)
  checkmate::assertDouble(x = covariateIds, 
                          lower = 0,
                          upper = 2^53, 
                          any.missing = FALSE,
                          unique = TRUE,
                          null.ok = TRUE)
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
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
              {covariateIds == }? {WHERE covariate_id in c(@covariateIds)};"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       covariateIds = covariateIds,
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table)
    if (!is.null(covariateIds)) {
      data <- data %>% 
        dplyr::filter(.data$covariateId %in% covariateIds)
    }
  }
  data <- data %>% #occassionally we may have more than one covariateName per covariateId
    # because of change in concept_name. See https://github.com/OHDSI/CohortDiagnostics/issues/162
    dplyr::group_by(.data$covariateId) %>% 
    dplyr::slice(1)
  return(data %>% dplyr::arrange(.data$covariateId))
}



#' Get time reference for temporal covariates
#'
#' @description
#' Get time reference for temporal covariates
#'
#' @template ModeAndDetails
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
  table <- 'temporalTimeRef'
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
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
  return(data %>% dplyr::arrange(x = .data$timeId))
}



#' Get cohort covariate (including temporal)
#'
#' @description
#' Get cohort covariate value data from data stored in cohort diagnostics results data
#' model. The output of this function may be used, together with covariate ref (temporal
#' covariate ref and time ref if temporal) for cohorts characteristization diagnostics. 
#' Because of the large volume of covariates, this function allows to filter range of 
#' covariate_value by providing the minimum and maximum proportion. Its important to note 
#' that all covariates are expected to output proportions that are between the values 0.0 to 1.0
#'
#' @template ModeAndDetails
#' @template CohortIds
#' @template DatabaseIds
#' @param isTemporal     (optional) Get temporal covariate values?
#' @param timeIds        (optional) Will only be used if isTemporal = TRUE. Do you want to limit to certain
#'                        'time ids'. By default timeId = c(1,2,3,4,5) are returned. These correspond to
#'                        -365 to -31, -30 to -1, 0 to 0, 1 to 30, 31 to 365.
#'                        If any of timeId value = 0, all timeIds are returned.
#'                        If any of timeId value = -1, will return all timeIds > 5 (for time series analysis)
#' @param minProportion   Do you want to limit the data returned by a lower threshold. Enter a number
#'                        between 0.00 to 1.00. Be default the value is 0.01.
#' @param maxProportion   Do you want to limit the data returned by a upper threshold. Enter a number
#'                        between 0.00 to 1.00. Be default the value is 1. 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' covariateValue <- getCovariateValueResult(cohortIds = c(342432,432423),
#'                                           databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
getCovariateValueResult <- function(connection = NULL,
                                    connectionDetails = NULL,
                                    cohortIds,
                                    databaseIds,
                                    minProportion = 0.01,
                                    maxProportion = 1,
                                    isTemporal = TRUE,
                                    timeIds = c(1,2,3,4,5),
                                    resultsDatabaseSchema = NULL) {
  if (isTemporal) {
    table <- 'temporalCovariateValue'
  } else {
    table <- 'covariateValue'
  }
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(x = isTemporal, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           max.len = 1)
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
  if (isTemporal) {
    checkmate::assertIntegerish(x = timeIds, 
                                lower = -1, 
                                any.missing = FALSE, 
                                unique = TRUE, 
                                null.ok = TRUE,
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
              WHERE cohort_id in (@cohortIds)
            	AND database_id in ('@databaseIds')
              {@isTemporal == TRUE} ? {AND time_id in ('@timeIds')}
              AND mean >= @minProportion
              AND mean <= @maxProportion;"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       cohortId = cohortIds,
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
      dplyr::filter(.data$cohortId %in% cohortIds,
                    .data$databaseId %in% databaseIds,
                    .data$mean >= minProportion,
                    .data$mean <= maxProportion)
    if (isTemporal && (any(timeIds != 0))) {
      if (any(timeIds == -1)) {
        data <- data %>% 
          dplyr::filter(.data$timeId > 5)
      } else {
        data <- data %>% 
          dplyr::filter(.data$timeId %in% timeIds)
      }
    }
  }
  if (isTemporal) {
    data <- data %>% 
      dplyr::relocate(.data$cohortId, .data$databaseId, .data$timeId, .data$covariateId) %>% 
      dplyr::arrange(.data$cohortId, .data$databaseId, .data$timeId, .data$covariateId)
  } else {
    data <- data %>% 
      dplyr::relocate(.data$cohortId, .data$databaseId, .data$covariateId) %>% 
      dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
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
#' @template ModeAndDetails
#' @template DatabaseIds
#' @param targetCohortIds        A vector of one or more Cohort Ids.
#' @param comparatorCohortIds    A vector of one or more Cohort Ids.
#' @param isTemporal             (optional) Get temporal covariate values?
#' @param timeIds                (optional) Used only if isTemporal = TRUE. Do you want to limit to certain
#'                               'time ids'. By default all time ids are returned. 
#' @param minProportion          Do you want to limit the data returned by a lower threshold. Enter a number
#'                               between 0.00 to 1.00. Be default the value is 0.01.
#' @param maxProportion          Do you want to limit the data returned by a upper threshold. Enter a number
#'                               between 0.00 to 1.00. Be default the value is 1.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' covariateValue <- compareCovariateValueResult(targetCohortIds = c(342432,432423),
#'                                               comparatorCohortIds = c(34243, 342432),
#'                                               databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
compareCovariateValueResult <- function(connection = NULL,
                                        connectionDetails = NULL,
                                        targetCohortIds,
                                        comparatorCohortIds,
                                        databaseIds,
                                        minProportion = 0.01,
                                        maxProportion = 1,
                                        isTemporal = TRUE,
                                        timeIds = NULL,
                                        resultsDatabaseSchema = NULL) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(x = isTemporal, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           max.len = 1,
                           add = errorMessage)
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = targetCohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = comparatorCohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  if (isTemporal) {
    if (!is.null(timeIds)) {
      checkmate::assertIntegerish(x = timeIds, 
                                  lower = 0, 
                                  any.missing = FALSE, 
                                  unique = TRUE, 
                                  null.ok = FALSE,
                                  add = errorMessage)
    }
  }
  checkmate::reportAssertions(collection = errorMessage)
  cohortIds <- c(targetCohortIds, comparatorCohortIds) %>% unique() %>% sort()
  covariateValue <- getCovariateValueResult(connection = connection, 
                                            connectionDetails = connectionDetails, 
                                            cohortIds = cohortIds, 
                                            databaseIds = databaseIds, 
                                            minProportion = minProportion, 
                                            maxProportion = maxProportion, 
                                            isTemporal = isTemporal, 
                                            timeIds = timeIds, 
                                            resultsDatabaseSchema = resultsDatabaseSchema)
  
  targetCovariateValue = covariateValue %>% 
    dplyr::filter(.data$cohortId %in% targetCohortIds) %>% 
    dplyr::rename(targetCohortId = .data$cohortId,
                  mean1 = .data$mean,
                  sd1 = .data$sd)
  comparatorCovariateValue = covariateValue %>% 
    dplyr::filter(.data$cohortId %in% comparatorCohortIds) %>% 
    dplyr::rename(comparatorCohortId = .data$cohortId,
                  mean2 = .data$mean,
                  sd2 = .data$sd)
  
  data <- dplyr::full_join(x = targetCovariateValue,
                           y = comparatorCovariateValue) %>%
    dplyr::relocate(.data$databaseId,
                    .data$targetCohortId,
                    .data$comparatorCohortId) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = 0)),
                  sd = sqrt(.data$sd1^2 + .data$sd2^2),
                  stdDiff = (.data$mean2 - .data$mean1)/.data$sd) %>%
    dplyr::arrange(-abs(.data$stdDiff)) %>%
    dplyr::mutate(absStdDiff = abs(.data$stdDiff)) %>% 
    dplyr::arrange(.data$databaseId,
                   .data$targetCohortId,
                   .data$comparatorCohortId, 
                   .data$covariateId)
  return(data)
}




#' Get cohort information
#'
#' @description
#' Get cohort information 
#'
#' @template ModeAndDetails
#' @template CohortIds
#' @param getJson  Do you want the JSON expression of cohort?
#' @param getSql  Do you want the Sql expression of cohort?
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' cohortReference <- getCohortReference()
#' }
#'
#' @export
getCohortReference <- function(connection = NULL,
                               connectionDetails = NULL,
                               cohortIds = NULL,
                               resultsDatabaseSchema = NULL,
                               getJson = FALSE,
                               getSql = FALSE) {
  table <- 'cohort'
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
  checkmate::assertDouble(x = cohortIds,
                          min.len = 1, 
                          null.ok = TRUE,
                          add = errorMessage)
  checkmate::assertLogical(x = getJson, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           max.len = 1)
  checkmate::assertLogical(x = getSql, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           max.len = 1)
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
              {@cohortIds == } ? {}:{where cohort_id in ('@cohortIds')};"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       cohortIds = cohortIds,
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table)
    if (!is.null(cohortIds)) {
      data <- data %>% 
        dplyr::filter(.data$cohortId %in% cohortIds)
    }
  }
  if (!getSql) {
    data <- data %>% 
      dplyr::select(-.data$sql)
  }
  if (!getJson) {
    data <- data %>% 
      dplyr::select(-.data$json)
  }
  data <- data %>% 
    dplyr::mutate(phenotypeId = .data$referentConceptId * 1000) %>% 
    dplyr::relocate(.data$phenotypeId, 
                    .data$cohortId,
                    .data$cohortName,
                    .data$logicDescription) %>% 
    dplyr::arrange(.data$phenotypeId, .data$cohortId)
  return(data )
}




#' Get database information
#'
#' @description
#' Get database information 
#'
#' @template ModeAndDetails
#' @param databaseIds      (optional) A vector of character string to identify database.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' databaseReference <- getDatabaseReference()
#' }
#'
#' @export
getDatabaseReference <- function(connection = NULL,
                                 connectionDetails = NULL,
                                 databaseIds = NULL,
                                 resultsDatabaseSchema = NULL) {
  table <- 'database'
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
  checkmate::assertCharacter(x = databaseIds,
                             min.len = 1, 
                             null.ok = TRUE,
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
              {@databaseIds == } ? {}:{where databaseId in ('@databaseIds')};"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table)
    if (!is.null(databaseIds)) {
      data <- data %>% 
        dplyr::filter(.data$databaseId %in% databaseIds)
    }
  }
  return(data %>% dplyr::arrange(.data$databaseId))
}




#' Get concept information
#'
#' @description
#' Get concept information 
#'
#' @template ModeAndDetails
#' @param conceptIds      (optional) A vector of integers corresponding to conceptids of interest.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' conceptReference <- getConceptReference()
#' }
#'
#' @export
getConceptReference <- function(connection = NULL,
                                connectionDetails = NULL,
                                conceptIds = NULL,
                                resultsDatabaseSchema = NULL) {
  table <- 'concept'
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
  checkmate::assertIntegerish(x = conceptIds,
                              min.len = 1, 
                              null.ok = TRUE,
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
              WHERE invalid_reason IS NULL 
              {@conceptIds == } ? {}:{AND concept_id IN ('@conceptIds')};"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       table = SqlRender::camelCaseToSnakeCase(table),
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table)
    if (!is.null(conceptIds)) {
      data <- data %>% 
        dplyr::filter(.data$conceptId %in% conceptIds,
                      is.na(.data$invalidReason)) %>% 
        dplyr::select(-.data$invalidReason)
    }
  }
  return(data %>% dplyr::arrange(.data$conceptId))
}




#' Get concept set data diagnostics
#'
#' @description
#' Get concept set data diagnostics data
#'
#' @template ModeAndDetails
#' @template CohortIds
#' @template DatabaseIds
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' conceptSetDataDiagnostics <- getConceptSetDiagnosticsResults()
#' }
#'
#' @export
getConceptSetDiagnosticsResults <- function(connection = NULL,
                                         connectionDetails = NULL,
                                         cohortIds = NULL,
                                         databaseIds = NULL,
                                         resultsDatabaseSchema = NULL) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorResultsDatabaseSchema(connection = connection,
                                                  connectionDetails = connectionDetails,
                                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                                  errorMessage = errorMessage)
  checkmate::assertDouble(x = cohortIds,
                          min.len = 1, 
                          null.ok = TRUE,
                          add = errorMessage)
  checkmate::assertCharacter(x = databaseIds,
                             min.len = 1, 
                             null.ok = TRUE,
                             add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  
  # included source concepts
  table <- 'includedSourceConcept'
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
              WHERE conceptId > 0
              {@cohortIds == } ? {}:{AND cohort_id IN ('@cohortIds')}
              {@databaseIds == } ? {}:{AND database_id IN ('@databaseIds')};"
    dataIncludedSourceConcept <- 
      DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                 sql = sql,
                                                 resultsDatabaseSchema = resultsDatabaseSchema,
                                                 table = SqlRender::camelCaseToSnakeCase(table),
                                                 snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    dataIncludedSourceConcept <- get(table)
    if (!is.null(cohortIds)) {
      dataIncludedSourceConcept <- dataIncludedSourceConcept %>% 
        dplyr::filter(.data$cohortId %in% !!cohortIds)
    }
    if (!is.null(databaseIds)) {
      dataIncludedSourceConcept <- dataIncludedSourceConcept %>% 
        dplyr::filter(.data$databaseId %in% !!databaseIds)
    }
  }
  
  # orphan concept
  table <-  'orphanConcept'
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
              WHERE conceptId > 0
              {@cohortIds == } ? {}:{AND cohort_id IN ('@cohortIds')}
              {@databaseIds == } ? {}:{AND database_id IN ('@databaseIds')};"
    dataOrphanConcept <- 
      DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                 sql = sql,
                                                 resultsDatabaseSchema = resultsDatabaseSchema,
                                                 table = SqlRender::camelCaseToSnakeCase(table),
                                                 snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    dataOrphanConcept <- get(table)
    if (!is.null(cohortIds)) {
      dataOrphanConcept <- dataOrphanConcept %>% 
        dplyr::filter(.data$cohortId %in% !!cohortIds)
    }
    if (!is.null(databaseIds)) {
      dataOrphanConcept <- dataOrphanConcept %>% 
        dplyr::filter(.data$databaseId %in% !!databaseIds)
    }
  }

  data <- dplyr::bind_rows(
    dataIncludedSourceConcept %>% 
      dplyr::select(.data$databaseId, 
                    .data$cohortId, 
                    .data$conceptSetId, 
                    .data$conceptId, 
                    .data$conceptSubjects,
                    .data$conceptCount
                    ) %>% 
      dplyr::mutate(type = 'included',
                    query = 'S'),
    dataIncludedSourceConcept %>% 
      dplyr::select(.data$databaseId, 
                    .data$cohortId, 
                    .data$sourceConceptId,
                    .data$conceptSubjects,
                    .data$conceptCount
      ) %>% 
      dplyr::rename(conceptId = .data$sourceConceptId) %>% 
      dplyr::mutate(type = 'included',
                    query = 'N'),
    dataOrphanConcept %>% 
      dplyr::select(.data$databaseId,
                    .data$cohortId,
                    .data$conceptSetId, 
                    .data$conceptId,
                    .data$conceptCount) %>% 
      dplyr::mutate(conceptSubjects = 0) %>% 
      dplyr::mutate(type = 'orphan',
                    query = 'U')
  )
  
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
        ParallelLogger::logWarn("  '", 
                                table, 
                                "' not found in ", 
                                databaseSchema)
      }
    }
  }
  
  if (checkInRMemory) {
    if (exists(table)) {
      tableExistsInRMemory <- TRUE
      if (!silent) {
        ParallelLogger::logInfo("  '", 
                                SqlRender::camelCaseToTitleCase(table), 
                                "' data object found in R memory.")
      }
    } else {
      tableExistsInRMemory <- FALSE
      if (is.null(connection)) {
        if (!silent) {
          ParallelLogger::logWarn("  '", 
                                  SqlRender::camelCaseToTitleCase(table), 
                                  "' data object not found in R memory.")
        }
      } else {
        if (!silent) {
          ParallelLogger::logInfo("  '", 
                                  SqlRender::camelCaseToTitleCase(table), 
                                  "' data object not found in R memory.")
        }
      }
    }
  }
  if (!is.null(connection) & isTRUE(tableExistsInDbms)) {
    return(connection)
  } else if (!is.null(connection) & !isTRUE(tableExistsInDbms) & 
             isTRUE(tableExistsInRMemory)) {
    ParallelLogger::logWarn(SqlRender::camelCaseToTitleCase(table), 
                            " was not found in dbms but was found in R memory. 
                            Using the data loaded in R memory.")
    return("memory")
  } else if (is.null(connection) & isTRUE(tableExistsInRMemory)) {
    return("memory")
  } else if (is.null(connection) & !isTRUE(tableExistsInRMemory)) {
    ParallelLogger::logWarn(SqlRender::camelCaseToTitleCase(table), 
                            " not found.")
    return("quit")
  }
}


checkErrorResultsDatabaseSchema <- function(errorMessage,
                                            resultsDatabaseSchema,
                                            connectionDetails,
                                            connection) {
  if (!is.null(connectionDetails) || !is.null(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  return(errorMessage)
}


checkErrorCohortIdsDatabaseIds <- function(errorMessage,
                                           cohortIds,
                                           databaseIds) {
  checkmate::assertDouble(x = cohortIds,
                          null.ok = FALSE,
                          lower = 1,
                          upper = 2^53,
                          any.missing = FALSE,
                          add = errorMessage)
  checkmate::assertCharacter(x = databaseIds,
                             min.len = 1,
                             any.missing = FALSE,
                             unique = TRUE,
                             add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  return(errorMessage)
}
