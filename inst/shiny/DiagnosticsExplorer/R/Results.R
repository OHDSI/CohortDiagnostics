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

getCohortCountResult <- function(dataSource = .GlobalEnv,
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
            FROM  @results_database_schema.cohort_count
            WHERE database_id in (@database_id)
            {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)}
            ;"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    results_database_schema = dataSource$resultsDatabaseSchema,
                                    cohort_ids = cohortIds,
                                    database_id = quoteLiterals(databaseIds), 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  }
  return(data)
}

getTimeDistributionResult <- function(dataSource = .GlobalEnv,
                                      cohortIds,
                                      databaseIds) {
  if (is(dataSource, "environment")) {
    data <- get("timeDistribution", envir = dataSource) %>% 
      dplyr::filter(.data$cohortId %in% !!cohortIds &
                      .data$databaseId %in% !!databaseIds)
  } else {
    sql <-   "SELECT *
              FROM  @results_database_schema.time_distribution
              WHERE cohort_id in (@cohort_ids)
            	AND database_id in (@database_ids);"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    results_database_schema = dataSource$resultsDatabaseSchema,
                                    cohort_ids = cohortIds,
                                    database_ids = quoteLiterals(databaseIds), 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
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
    sql <- "SELECT *
            FROM  @results_database_schema.incidence_rate
            WHERE cohort_id in (@cohort_ids)
           	  AND database_id in (@database_ids)
            {@gender == TRUE} ? {AND gender != ''} : {  AND gender = ''}
            {@age_group == TRUE} ? {AND age_group != ''} : {  AND age_group = ''}
            {@calendar_year == TRUE} ? {AND calendar_year != ''} : {  AND calendar_year = ''}
              AND person_years > @personYears;"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    results_database_schema = dataSource$resultsDatabaseSchema,
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
  
  return(data %>% 
           dplyr::mutate(calendarYear = as.integer(.data$calendarYear)) %>%
           dplyr::arrange(.data$cohortId, .data$databaseId))
}

getInclusionRuleStats <- function(dataSource = .GlobalEnv,
                                  cohortIds = NULL,
                                  databaseIds) {
  if (is(dataSource, "environment")) {
    data <- get("inclusionRuleStats", envir = dataSource) %>% 
      dplyr::filter(.data$databaseId %in% !!databaseIds) 
    if (!is.null(cohortIds)) {
      data <- data %>% 
        dplyr::filter(.data$cohortId %in% !!cohortIds) 
    }
  } else {
    sql <- "SELECT *
    FROM  @resultsDatabaseSchema.inclusion_rule_stats
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
  data <- data %>% 
    dplyr::select(.data$ruleSequenceId, .data$ruleName, 
                  .data$meetSubjects, .data$gainSubjects, 
                  .data$remainSubjects, .data$totalSubjects, .data$databaseId) %>% 
    dplyr::arrange(.data$ruleSequenceId)
  return(data)
}


getIndexEventBreakdown <- function(dataSource = .GlobalEnv,
                                   cohortIds,
                                   databaseIds) {
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = cohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  if (is(dataSource, "environment")) {
    data <- get("indexEventBreakdown", envir = dataSource) %>% 
      dplyr::filter(.data$databaseId %in% !!databaseIds) 
    if (!is.null(cohortIds)) {
      data <- data %>% 
        dplyr::filter(.data$cohortId %in% !!cohortIds) 
    }
    data <- data %>%
      dplyr::inner_join(dplyr::select(get("concept", envir = dataSource),
                                      .data$conceptId,
                                      .data$conceptName),
                        by = c("conceptId"))
  } else {
    sql <- "SELECT index_event_breakdown.*,
              standard_concept.concept_name AS concept_name
            FROM  @results_database_schema.index_event_breakdown
            INNER JOIN  @vocabulary_database_schema.concept standard_concept
              ON index_event_breakdown.concept_id = standard_concept.concept_id
            WHERE database_id in (@database_id)
              AND cohort_id in (@cohort_ids);"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    results_database_schema = dataSource$resultsDatabaseSchema,
                                    vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
                                    cohort_ids = cohortIds,
                                    database_id = quoteLiterals(databaseIds), 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  }
  return(data)
}

getVisitContextResults <- function(dataSource = .GlobalEnv,
                                   cohortIds,
                                   databaseIds) {
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = cohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  if (is(dataSource, "environment")) {
    data <- get("visitContext", envir = dataSource) %>% 
      dplyr::filter(.data$databaseId %in% !!databaseIds) 
    if (!is.null(cohortIds)) {
      data <- data %>% 
        dplyr::filter(.data$cohortId %in% !!cohortIds) 
    }
    data <- data %>%
      dplyr::inner_join(dplyr::select(get("concept", envir = dataSource),
                                      visitConceptId = .data$conceptId,
                                      visitConceptName = .data$conceptName),
                        by = c("visitConceptId"))
  } else {
    sql <- "SELECT visit_context.*,
              standard_concept.concept_name AS visit_concept_name
            FROM  @results_database_schema.visit_context
            INNER JOIN  @vocabulary_database_schema.concept standard_concept
              ON visit_context.visit_concept_id = standard_concept.concept_id
            WHERE database_id in (@database_id)
              AND cohort_id in (@cohort_ids);"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    results_database_schema = dataSource$resultsDatabaseSchema,
                                    vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
                                    cohort_ids = cohortIds,
                                    database_id = quoteLiterals(databaseIds), 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  }
  return(data)
}

getIncludedConceptResult <- function(dataSource = .GlobalEnv,
                                     cohortId,
                                     databaseIds) {
  if (is(dataSource, "environment")) {
    data <- get("includedSourceConcept", envir = dataSource) %>% 
      dplyr::filter(.data$cohortId == !!cohortId &
                      .data$databaseId %in% !!databaseIds) %>% 
      dplyr::inner_join(dplyr::select(get("conceptSets", envir = dataSource),
                                      .data$cohortId,
                                      .data$conceptSetId,
                                      .data$conceptSetName), 
                        by = c("cohortId", "conceptSetId")) %>%
      dplyr::inner_join(dplyr::select(get("concept", envir = dataSource),
                                      sourceConceptId = .data$conceptId,
                                      sourceConceptName = .data$conceptName,
                                      sourceVocabularyId = .data$vocabularyId,
                                      sourceConceptCode = .data$conceptCode),
                        by = c("sourceConceptId")) %>%
      dplyr::inner_join(dplyr::select(get("concept", envir = dataSource),
                                      .data$conceptId,
                                      .data$conceptName,
                                      .data$vocabularyId),
                        by = c("conceptId"))
  } else {
    sql <- "SELECT included_source_concept.*,
              concept_set_name,
              source_concept.concept_name AS source_concept_name,
              source_concept.vocabulary_id AS source_vocabulary_id,
              source_concept.concept_code AS source_concept_code,
              standard_concept.concept_name AS concept_name,
              standard_concept.vocabulary_id AS vocabulary_id
            FROM  @results_database_schema.included_source_concept
            INNER JOIN  @results_database_schema.concept_sets
              ON included_source_concept.cohort_id = concept_sets.cohort_id
            INNER JOIN  @vocabulary_database_schema.concept source_concept
              ON included_source_concept.source_concept_id = source_concept.concept_id
            INNER JOIN  @vocabulary_database_schema.concept standard_concept
              ON included_source_concept.concept_id = standard_concept.concept_id
            WHERE included_source_concept.cohort_id = @cohort_id
             AND database_id in (@database_ids);"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    results_database_schema = dataSource$resultsDatabaseSchema,
                                    vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
                                    cohort_id = cohortId,
                                    database_ids = quoteLiterals(databaseIds), 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } 
  
  return(data)
}

getOrphanConceptResult <- function(dataSource = .GlobalEnv,
                                   cohortId,
                                   databaseIds) {
  if (is(dataSource, "environment")) {
    data <- get("orphanConcept", envir = dataSource) %>% 
      dplyr::filter(.data$cohortId == !!cohortId &
                      .data$databaseId %in% !!databaseIds) %>% 
      dplyr::inner_join(dplyr::select(get("conceptSets", envir = dataSource),
                                      .data$cohortId,
                                      .data$conceptSetId,
                                      .data$conceptSetName), 
                        by = c("cohortId", "conceptSetId")) %>%
      dplyr::inner_join(dplyr::select(get("concept", envir = dataSource),
                                      .data$conceptId,
                                      .data$conceptName,
                                      .data$vocabularyId,
                                      .data$conceptCode),
                        by = c("conceptId"))
  } else {
    sql <- "SELECT orphan_concept.*,
              concept_set_name,
              standard_concept.concept_name AS concept_name,
              standard_concept.vocabulary_id AS vocabulary_id,
              standard_concept.concept_code AS concept_code
            FROM  @results_database_schema.orphan_concept
            INNER JOIN  @results_database_schema.concept_sets
              ON orphan_concept.cohort_id = concept_sets.cohort_id
            INNER JOIN  @vocabulary_database_schema.concept standard_concept
              ON orphan_concept.concept_id = standard_concept.concept_id
            WHERE orphan_concept.cohort_id = @cohort_id
             AND database_id in (@database_ids);"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    results_database_schema = dataSource$resultsDatabaseSchema,
                                    vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
                                    cohort_id = cohortId,
                                    database_ids = quoteLiterals(databaseIds), 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } 
  
  return(data)
}


getCohortOverlapResult <- function(dataSource = .GlobalEnv,
                                   targetCohortIds,
                                   comparatorCohortIds,
                                   databaseIds) {
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = targetCohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = comparatorCohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
  
  if (is(dataSource, "environment")) {
    data <- get("cohortOverlap", envir = dataSource) %>% 
      dplyr::filter(.data$targetCohortId %in% !!targetCohortIds &
                      .data$comparatorCohortId %in% !!comparatorCohortIds &
                      .data$databaseId %in% !!databaseIds) %>% 
      tidyr::tibble()
  } else {
    sql <-   "SELECT *
              FROM  @results_database_schema.cohort_overlap
              WHERE target_cohort_id in (@targetCohortId)
              AND comparator_cohort_id in (@comparatorCohortId)
            	AND database_id in (@databaseId);"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    results_database_schema = dataSource$resultsDatabaseSchema,
                                    targetCohortId = targetCohortIds,
                                    comparatorCohortId = comparatorCohortIds,
                                    databaseId = quoteLiterals(databaseIds), 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } 
  
  if (nrow(data) == 0) {
    return(tidyr::tibble())
  }
  data <- data %>% 
    dplyr::relocate(.data$databaseId, .data$targetCohortId, .data$comparatorCohortId) %>% 
    dplyr::arrange(.data$databaseId, .data$targetCohortId, .data$comparatorCohortId)
  return(data)
}


getCovariateReference <- function(dataSource = .GlobalEnv,
                                  covariateIds,
                                  isTemporal = TRUE) {
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
                           max.len = 1,
                           add = errorMessage)
  checkmate::assertDouble(x = covariateIds, 
                          lower = 0,
                          upper = 2^53, 
                          any.missing = FALSE,
                          unique = TRUE,
                          null.ok = TRUE)
  checkmate::reportAssertions(collection = errorMessage)
  
  if (is(dataSource, "environment")) {
    data <- get(table, envir = dataSource) %>% 
      dplyr::filter(.data$covariateIds %in% !!covariateIds)
    data <- data[!duplicated(data$covariateId),]
    # we dont expect the full vocabulary tables to be loaded in R's memory.
    # The logic below will approximate the domain_id
    data <- data %>% 
      dplyr::mutate(domainId = tolower(stringr::word(.data$covariateName))) %>% 
      dplyr::mutate(domainId = dplyr::case_when(stringr::str_detect(string = domainId, pattern = "drug") ~ "Drug",
                                                stringr::str_detect(string = domainId, pattern = "observation") ~ "Observation",
                                                stringr::str_detect(string = domainId, pattern = "condition") ~ "Condition",
                                                stringr::str_detect(string = domainId, pattern = "procedure") ~ "Procedure",
                                                stringr::str_detect(string = domainId, pattern = "measurement") ~ "Measurement",
                                                TRUE ~ "Other"))
  } else {
    sql <-   "SELECT cv.*, c.domain_id
              FROM  @results_database_schema.@table cv
              LEFT JOIN @results_database_schema.concept c
              ON cv.concept_id = c.concept_id
              WHERE covariate_id in (@covariateIds);"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    results_database_schema = dataSource$resultsDatabaseSchema,
                                    covariateIds = covariateIds,
                                    table = SqlRender::camelCaseToSnakeCase(table), 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble() %>% 
      dplyr::mutate(domainId = tidyr::replace_na(.data$domainId, replace = "Other"),
                    domainId = stringr::str_replace(string = .data$domainId, pattern = 'Metadata', replacement = 'Other'))
  }
  return(data %>% dplyr::arrange(.data$covariateId))
}


getTimeReference <- function(connection = NULL,
                             connectionDetails = NULL,
                             resultsDatabaseSchema = NULL){
  table <- 'temporalTimeRef'
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  # route query
  route <- routeDataQuery(connection = connection,
                          connectionDetails = connectionDetails,
                          table = table)
  
  if (route == 'quit') {
    warning("  Cannot query '", camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @results_database_schema.@table;"
    data <- renderTranslateQuerySql(connection = connection,
                                    sql = sql,
                                    results_database_schema = resultsDatabaseSchema,
                                    table = SqlRender::camelCaseToSnakeCase(table),
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- get(table)
  }
  return(data %>% dplyr::arrange(x = .data$timeId))
}


getCovariateValueResult <- function(dataSource = .GlobalEnv,
                                    cohortIds,
                                    analysisIds = NULL,
                                    databaseIds,
                                    timeIds = NULL,
                                    isTemporal = FALSE) {
  
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(x = isTemporal, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           max.len = 1, 
                           add = errorMessage)
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = cohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
  if (isTemporal) {
    checkmate::assertIntegerish(x = timeIds, 
                                lower = 0, 
                                any.missing = FALSE, 
                                unique = TRUE, 
                                null.ok = TRUE,
                                add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  

  if (isTemporal) {
    table <- "temporalCovariateValue"
    refTable <- "temporalCovariateRef"
    timeRefTable <- "temporalTimeRef"
  } else {
    table <- "covariateValue"
    refTable <- "covariateRef"
    timeRefTable <- ""
  }
  
  if (is(dataSource, "environment")) {
    data <- get(table, envir = dataSource) %>%
      dplyr::filter(.data$cohortId %in% !!cohortIds,
                    .data$databaseId %in% !!databaseIds) %>%
      dplyr::inner_join(get(refTable, envir = dataSource), by = "covariateId")
    if (!is.null(analysisIds)) {
      data <- data %>%
        dplyr::filter(.data$analysisId %in% analysisIds)
    }
    if (isTemporal) {
      data <- data %>%
        dplyr::inner_join(get(timeRefTable, envir = dataSource), by = "timeId")
      if (!is.null(timeIds)) {
        data <- data %>%
          dplyr::filter(.data$timeId %in% timeIds)
      }
    }
  } else {
    sql <- "SELECT covariate.*,
              covariate_name,
            {@time_ref_table != \"\"} ? {
              start_day,
              end_day,
            }
              concept_id,
              analysis_id
            FROM  @results_database_schema.@table covariate
            INNER JOIN @results_database_schema.@ref_table covariate_ref
              ON covariate.covariate_id = covariate_ref.covariate_id
            {@time_ref_table != \"\"} ? {
            INNER JOIN @results_database_schema.@time_ref_table time_ref
              ON covariate.time_id = time_ref.time_id
            }
            WHERE cohort_id in (@cohort_ids)
            {@time_ref_table != \"\" & @time_ids != \"\"} ? {  AND covariate.time_id IN (@time_ids)}
            {@analysis_ids != \"\"} ? {  AND analysis_id IN (@analysis_ids)}
            	AND database_id in (@databaseIds);"
    if (is.null(timeIds)) {
      timeIds <- ""
    }
    if (is.null(analysisIds)) {
      analysisIds <- ""
    }
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    table = SqlRender::camelCaseToSnakeCase(table),
                                    ref_table = SqlRender::camelCaseToSnakeCase(refTable),
                                    time_ref_table = SqlRender::camelCaseToSnakeCase(timeRefTable),
                                    results_database_schema = dataSource$resultsDatabaseSchema,
                                    cohort_ids = cohortIds,
                                    analysis_ids = analysisIds,
                                    databaseIds = quoteLiterals(databaseIds),
                                    time_ids = timeIds,
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  }
  if (isTemporal) {
    data <- data %>% 
      dplyr::relocate(.data$cohortId, 
                      .data$databaseId, 
                      .data$timeId, 
                      .data$startDay, 
                      .data$endDay,
                      .data$analysisId,
                      .data$covariateId, 
                      .data$covariateName) %>% 
      dplyr::arrange(.data$cohortId, .data$databaseId, .data$timeId, .data$covariateId, .data$covariateName)
  } else {
    data <- data %>% 
      dplyr::relocate(.data$cohortId, 
                      .data$databaseId, 
                      .data$analysisId,
                      .data$covariateId, 
                      .data$covariateName) %>% 
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
                                        resultsDatabaseSchema = NULL) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(x = isTemporal, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           max.len = 1,
                           add = errorMessage)
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
                   .data$covariateId)
  return(data)
}

getCohortReference <- function(cohortIds = NULL,
                               getJson = FALSE,
                               getSql = FALSE) {
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertDouble(x = cohortIds,
                          min.len = 1, 
                          null.ok = TRUE,
                          add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  table <- 'cohort'
  data <- get(table)
  if (!is.null(cohortIds)) {
    data <- data %>% 
      dplyr::filter(.data$cohortId %in% cohortIds)
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

getDatabaseReference <- function(databaseIds = NULL) {
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(x = databaseIds,
                             min.len = 1, 
                             null.ok = TRUE,
                             add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  table <- 'database'
  data <- get(table)
  if (!is.null(databaseIds)) {
    data <- data %>% 
      dplyr::filter(.data$databaseId %in% databaseIds)
  }
  return(data %>% dplyr::arrange(.data$databaseId))
}

getConceptReference <- function(dataSource = .GlobalEnv,
                                conceptIds) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(x = conceptIds,
                              min.len = 1, 
                              null.ok = TRUE,
                              add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  if (is(dataSource, "environment")) {
    data <- get("cohort", envir = dataSource) %>% 
      dplyr::filter(!is.na(.data$invalidReason)) %>% 
      dplyr::filter(.data$conceptId %in% conceptIds)
  } else {
    sql <- "SELECT *
              FROM  @results_database_schema.concept
              WHERE invalid_reason IS NULL 
              {@conceptIds == } ? {}:{AND concept_id IN (@conceptIds)};"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    results_database_schema = dataSource$resultsDatabaseSchema,
                                    conceptIds = conceptIds, 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
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
    warning("  Cannot query '", camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @results_database_schema.@table
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
    warning("  Cannot query '", camelCaseToTitleCase(table), '. Exiting.')
    return(NULL)
  } else if (route == 'memory') {
    connection <- NULL
  }
  # perform query
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @results_database_schema.@table
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
                                camelCaseToTitleCase(table), 
                                "' data object found in R memory.")
      }
    } else {
      tableExistsInRMemory <- FALSE
      if (is.null(connection)) {
        if (!silent) {
          warning("  '", 
                  camelCaseToTitleCase(table), 
                  "' data object not found in R memory.")
        }
      } else {
        if (!silent) {
          ParallelLogger::logInfo("  '", 
                                  camelCaseToTitleCase(table), 
                                  "' data object not found in R memory.")
        }
      }
    }
  }
  if (!is.null(connection) & isTRUE(tableExistsInDbms)) {
    return("database")
  } else if (!is.null(connection) & !isTRUE(tableExistsInDbms) & 
             isTRUE(tableExistsInRMemory)) {
    warning(camelCaseToTitleCase(table), 
            " was not found in dbms but was found in R memory. 
                            Using the data loaded in R memory.")
    return("memory")
  } else if (is.null(connection) & isTRUE(tableExistsInRMemory)) {
    return("memory")
  } else if (is.null(connection) & !isTRUE(tableExistsInRMemory)) {
    warning(camelCaseToTitleCase(table), 
            " not found.")
    return("quit")
  }
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
