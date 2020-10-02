createDatabaseDataSource <- function(connection, resultsDatabaseSchema, vocabularyDatabaseSchema = resultsDatabaseSchema) {
  return(list(connection = connectionPool,
              resultsDatabaseSchema = resultsDatabaseSchema,
              vocabularyDatabaseSchema = vocabularyDatabaseSchema))
}

createFileDataSource <- function(premergedDataFile, envir = new.env()) {
  load(premergedDataFile, envir = envir)
  return(envir)
}


renderTranslateQuerySql <- function(connection, sql, ..., snakeCaseToCamelCase = FALSE) {
  if (is(connection, "Pool")) {
    # Connection pool is used by Shiny app, which always uses PostgreSQL:
    sql <- SqlRender::render(sql, ...)
    sql <- SqlRender::translate(sql, targetDialect = "postgresql")
    
    # Just for development purposes. Remove when done:
    writeLines(sql)
    
    tryCatch({
      data <- DatabaseConnector::dbGetQuery(connection, sql)
    }, error = function(err) {
      writeLines(sql)
      stop(err)
    })
    if (snakeCaseToCamelCase) {
      colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
    }
    return(data)
  } else {
    return(DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                      sql = sql,
                                                      ...,
                                                      snakeCaseToCamelCase = snakeCaseToCamelCase))
  }
}

quoteLiterals <- function(x) {
  if (is.null(x)) {
    return("")
  } else {
    return(paste0("'", paste(x, collapse = "', '"), "'")) 
  }
}

getCohortCounts <- function(dataSource = .GlobalEnv,
                            cohortIds = NULL,
                            databaseIds) {
  if (is(dataSource, "environment")) {
    data <- get("cohortCount", envir = dataSource) %>% 
      dplyr::filter(.data$databaseId %in% !!databaseIds) 
    if (!is.null(cohortIds)) {
      data <- data %>% 
        dplyr::filter(.data$cohortId %in% !!cohortIds) 
    }
  } else {
    sql <- "SELECT *
            FROM  @resultsDatabaseSchema.cohort_count
            WHERE database_id in (@database_id)
            {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)}
            ;"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    resultsDatabaseSchema = dataSource$resultsDatabaseSchema,
                                    cohort_ids = cohortIds,
                                    database_id = quoteLiterals(databaseIds), 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  }
  return(data)
}

getTimeDistributionResult <- function(dataSource = .GlobalEnv,
                                      cohortIds,
                                      databaseIds,
                                      resultsDatabaseSchema = NULL) {
  if (is(dataSource, "environment")) {
    data <- get("timeDistribution") %>% 
      dplyr::filter(.data$cohortId %in% !!cohortIds &
                      .data$databaseId %in% !!databaseIds) %>% 
      tidyr::tibble()
  } else {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.time_distribution
              WHERE cohort_id in (@cohortId)
            	AND database_id in (@database_ids);"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    resultsDatabaseSchema = dataSource$resultsDatabaseSchema,
                                    cohortId = cohortIds,
                                    database_ids = quoteLiterals(databaseIds), 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } 
  
  if (nrow(data) == 0) {
    warning("No records retrieved for ", SqlRender::camelCaseToTitleCase(table), ".")
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
  return(data)
}

getIncidenceRateResult <- function(dataSource = .GlobalEnv,
                                   cohortIds,
                                   databaseIds,
                                   stratifyByGender = c(TRUE,FALSE),
                                   stratifyByAgeGroup = c(TRUE,FALSE),
                                   stratifyByCalendarYear = c(TRUE,FALSE),
                                   minPersonYears = 1000) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
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
  
  if (is(dataSource, "environment")) {
    data <- get("incidenceRate", envir = dataSource) %>% 
      dplyr::mutate(strataGender = !is.na(.data$gender),
                    strataAgeGroup = !is.na(.data$ageGroup),
                    strataCalendarYear = !is.na(.data$calendarYear)) %>% 
      dplyr::filter(.data$cohortId %in% !!cohortIds &
                      .data$databaseId %in% !!databaseIds &
                      .data$strataGender %in% !!stratifyByGender &
                      .data$strataAgeGroup %in% !!stratifyByAgeGroup &
                      .data$strataCalendarYear %in% !!stratifyByCalendarYear &
                      .data$personYears > !!minPersonYears) %>% 
      dplyr::select(-tidyselect::starts_with('strata'))
  } else {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.incidence_rate
              WHERE cohort_id in (@cohort_ids)
            	AND database_id in (@database_ids)
              {@gender == TRUE} ? {AND gender != ''} : {AND gender = ''}
              {@age_group == TRUE} ? {AND age_group != ''} : {AND age_group = ''}
            	{@calendar_year == TRUE} ? {AND calendar_year != ''} : {AND calendar_year = ''}
              AND person_years > @personYears;"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    resultsDatabaseSchema = dataSource$resultsDatabaseSchema,
                                    cohort_ids = cohortIds,
                                    database_ids = quoteLiterals(databaseIds),
                                    gender = stratifyByGender,
                                    age_group = stratifyByAgeGroup,
                                    calendar_year = stratifyByCalendarYear,
                                    personYears = minPersonYears,
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
    data <- data %>%
      dplyr::mutate(gender = dplyr::na_if(.data$gender, ""),
                    ageGroup = dplyr::na_if(.data$ageGroup, ""),
                    calendarYear = dplyr::na_if(.data$calendarYear, ""))
  } 
 
  if (nrow(data) == 0) {
    warning("No records retrieved for 'incidence rate'.")
  }
  return(data %>% 
           dplyr::mutate(calendarYear = as.integer(.data$calendarYear)) %>%
           dplyr::arrange(.data$cohortId, .data$databaseId))
}


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
    warning("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table
            	WHERE database_id in c('@databaseIds');"
    data <- renderTranslateQuerySql(connection = connection,
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
    warning("No records retrieved for '", SqlRender::camelCaseToTitleCase(table), "'")
    return(NULL)
  }
  data <- data %>% 
    dplyr::relocate(.data$cohortId, .data$databaseId) %>% 
    dplyr::arrange(.data$cohortId, .data$databaseId)
  return(data)
}

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
    warning("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
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
    data <- renderTranslateQuerySql(connection = connection,
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
    warning("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table
              {covariateIds == }? {WHERE covariate_id in c(@covariateIds)};"
    data <- renderTranslateQuerySql(connection = connection,
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
  data <- data[!duplicated(data$covariateId),]
  return(data %>% dplyr::arrange(.data$covariateId))
}


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
    warning("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table;"
    data <- renderTranslateQuerySql(connection = connection,
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
    warning("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
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
    data <- renderTranslateQuerySql(connection = connection,
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


compareCovariateValueResult <- function(connection = NULL,
                                        connectionDetails = NULL,
                                        targetCohortIds,
                                        comparatorCohortIds,
                                        databaseIds,
                                        minProportion = 0.01,
                                        maxProportion = 1,
                                        isTemporal = TRUE,
                                        timeIds = NULL,
                                        resultsDatabaseSchema = NULL,
                                        domain = NULL) {
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
  
  data <- dplyr::inner_join(x = targetCovariateValue,
                            y = comparatorCovariateValue,
                            by = c('covariateId', 'databaseId')) %>%
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
                   .data$covariateId) %>% 
    dplyr::inner_join(covariateRef)
  
  if (domain != "" && !is.null(domain) && domain != "all") {
    data <- data %>% dplyr::filter(grepl(paste0("^",domain), .data$covariateName))
  }
  return(data)
}

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
    warning("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table
              {@cohortIds == } ? {}:{where cohort_id in ('@cohortIds')};"
    data <- renderTranslateQuerySql(connection = connection,
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
  return(data )
}

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
    warning("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.@table
              {@databaseIds == } ? {}:{where databaseId in ('@databaseIds')};"
    data <- renderTranslateQuerySql(connection = connection,
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
    warning("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
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
    data <- renderTranslateQuerySql(connection = connection,
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
    warning("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
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
      renderTranslateQuerySql(connection = connection,
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
    warning("  Cannot query '", SqlRender::camelCaseToTitleCase(table), '. Exiting.')
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
      renderTranslateQuerySql(connection = connection,
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
                           silent = TRUE) {
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
                                                          name = table,
                                                          schema = databaseSchema)
    if (!tableExistsInDbms) {
      if (!silent) {
        warning("  '", 
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
          warning("  '", 
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
    return("database")
  } else if (!is.null(connection) & !isTRUE(tableExistsInDbms) & 
             isTRUE(tableExistsInRMemory)) {
    warning(SqlRender::camelCaseToTitleCase(table), 
            " was not found in dbms but was found in R memory. 
                            Using the data loaded in R memory.")
    return("memory")
  } else if (is.null(connection) & isTRUE(tableExistsInRMemory)) {
    return("memory")
  } else if (is.null(connection) & !isTRUE(tableExistsInRMemory)) {
    warning(SqlRender::camelCaseToTitleCase(table), 
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
