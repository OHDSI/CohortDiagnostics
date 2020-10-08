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
  shortNames <- data %>%
    dplyr::inner_join(cohort) %>% 
    dplyr::distinct(.data$cohortId, .data$cohortName) %>%
    dplyr::arrange(.data$cohortName) %>%
    dplyr::mutate(shortName = paste0('C', dplyr::row_number()))
  
  
  data <- data %>% 
    dplyr::inner_join(shortNames, by = "cohortId")
  
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
  shortNames <- data %>%
    dplyr::inner_join(cohort) %>% 
    dplyr::distinct(.data$cohortId, .data$cohortName) %>%
    dplyr::arrange(.data$cohortName) %>%
    dplyr::mutate(shortName = paste0('C', dplyr::row_number()))

  
  data <- data %>% 
    dplyr::inner_join(shortNames, by = "cohortId")
  
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
                AND included_source_concept.concept_set_id = concept_sets.concept_set_id
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
                AND orphan_concept.concept_set_id = concept_sets.concept_set_id
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
      dplyr::inner_join(dplyr::select(get("cohort", envir = dataSource), 
                                      targetCohortId = .data$cohortId,
                                      targetCohortName = .data$cohortName),
                        by = "targetCohortId") %>% 
      dplyr::inner_join(dplyr::select(get("cohort", envir = dataSource), 
                                      comparatorCohortId = .data$cohortId,
                                     comparatorCohortName = .data$cohortName),
                        by = "comparatorCohortId")
  } else {
    sql <-   "SELECT cohort_overlap.*,
                target_cohort.cohort_name AS target_cohort_name,
                comparator_cohort.cohort_name AS comparator_cohort_name
              FROM  @results_database_schema.cohort_overlap
              INNER JOIN @results_database_schema.cohort target_cohort
                ON cohort_overlap.target_cohort_id = target_cohort.cohort_id
              INNER JOIN @results_database_schema.cohort comparator_cohort
                ON cohort_overlap.comparator_cohort_id = comparator_cohort.cohort_id
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
  targetShortNames <- data %>%
    dplyr::distinct(.data$targetCohortId, .data$targetCohortName) %>%
    dplyr::arrange(.data$targetCohortName) %>%
    dplyr::select(-.data$targetCohortName) %>%
    dplyr::mutate(targetShortName = paste0('C', dplyr::row_number()))
  
  comparatorShortNames <- data %>%
    dplyr::distinct(.data$comparatorCohortId, .data$comparatorCohortName) %>%
    dplyr::arrange(.data$comparatorCohortName) %>%
    dplyr::select(-.data$comparatorCohortName) %>%
    dplyr::mutate(comparatorShortName = paste0('C', dplyr::row_number()))
  
  data <- data %>% 
    dplyr::inner_join(targetShortNames, by = "targetCohortId") %>%
    dplyr::inner_join(comparatorShortNames, by = "comparatorCohortId") 
  return(data)
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
