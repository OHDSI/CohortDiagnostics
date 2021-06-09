getCohortCountResult <- function(dataSource = .GlobalEnv,
                                 cohortIds = NULL,
                                 databaseIds) {
  data <-  getResultsFromCohortCount(dataSource = dataSource,
                                     cohortIds = cohortIds,
                                     databaseIds = databaseIds)
  return(data)
}



getTimeDistributionResult <- function(dataSource = .GlobalEnv,
                                      cohortIds,
                                      databaseIds) {
  table <- 'timeDistribution'
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>% 
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
  if (nrow(data) == 0) {
    return(NULL)
  }  
  return(data)
}


getIncidenceRateResult <- function(dataSource = .GlobalEnv,
                                   cohortIds,
                                   databaseIds,
                                   stratifyByGender = c(TRUE,FALSE),
                                   stratifyByAgeGroup = c(TRUE,FALSE),
                                   stratifyByCalendarYear = c(TRUE,FALSE),
                                   minPersonYears = 1000,
                                   minSubjectCount = NA) {
  table <- 'incidenceRate'
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>% 
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
    if (nrow(data) == 0) {
      return(NULL)
    }
    data <- data %>%
      dplyr::mutate(gender = dplyr::na_if(.data$gender, ""),
                    ageGroup = dplyr::na_if(.data$ageGroup, ""),
                    calendarYear = dplyr::na_if(.data$calendarYear, ""))
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  data <- data %>% 
    dplyr::inner_join(cohortCount, 
                      by = c("cohortId", "databaseId")) %>% 
    dplyr::mutate(calendarYear = as.integer(.data$calendarYear)) %>%
    dplyr::arrange(.data$cohortId, .data$databaseId)
  
  if (!is.na(minSubjectCount)) {
    data <- data %>% 
      dplyr::filter(.data$cohortSubjects > !!minSubjectCount)
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}

getInclusionRuleStats <- function(dataSource = .GlobalEnv,
                                  cohortIds = NULL,
                                  databaseIds) {
  table <- 'inclusionRuleStats'
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>% 
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
  if (nrow(data) == 0) {
    return(NULL)
  }
  data <- data %>% 
    dplyr::select(.data$cohortId,
                  .data$ruleSequenceId, 
                  .data$ruleName, 
                  .data$meetSubjects, 
                  .data$gainSubjects, 
                  .data$remainSubjects, 
                  .data$totalSubjects, 
                  .data$databaseId) %>% 
    dplyr::arrange(.data$cohortId, .data$ruleSequenceId)
  return(data)
}


getIndexEventBreakdown <- function(dataSource = .GlobalEnv,
                                   cohortIds,
                                   databaseIds) {
  table <- 'indexEventBreakdown'
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>% 
      dplyr::filter(.data$databaseId %in% !!databaseIds) 
    if (!is.null(cohortIds)) {
      data <- data %>% 
        dplyr::filter(.data$cohortId %in% !!cohortIds) 
    }
    data <- data %>%
      dplyr::inner_join(dplyr::select(get("concept", envir = dataSource),
                                      .data$conceptId,
                                      .data$conceptName,
                                      .data$domainId,
                                      .data$vocabularyId,
                                      .data$standardConcept),
                        by = c("conceptId"))
  } else {
    sql <- "SELECT index_event_breakdown.*,
              concept.concept_name,
              concept.domain_id,
              concept.vocabulary_id,
              concept.standard_concept
            FROM  @results_database_schema.index_event_breakdown
            INNER JOIN  @vocabulary_database_schema.concept
              ON index_event_breakdown.concept_id = concept.concept_id
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
  if (nrow(data) == 0) {
    return(NULL)
  }  
  data <- data %>% 
    dplyr::inner_join(cohortCount, 
                      by = c('databaseId', 'cohortId')) %>% 
    dplyr::mutate(subjectPercent = .data$subjectCount/.data$cohortSubjects,
                  conceptPercent = .data$conceptCount/.data$cohortEntries)
  return(data)
}

getVisitContextResults <- function(dataSource = .GlobalEnv,
                                   cohortIds,
                                   databaseIds) {
  table <- 'visitContext'
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>% 
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
  if (nrow(data) == 0) {
    return(NULL)
  }
  data <- data %>%
    dplyr::inner_join(cohortCount,
                      by = c("cohortId", "databaseId")) %>% 
    dplyr::mutate(subjectPercent = .data$subjects/.data$cohortSubjects)
  return(data)
}

getIncludedConceptResult <- function(dataSource = .GlobalEnv,
                                     cohortId,
                                     databaseIds) {
  table <- 'includedSourceConcept'
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>% 
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
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}

getOrphanConceptResult <- function(dataSource = .GlobalEnv,
                                   cohortId,
                                   databaseIds) {
  table <- 'orphanConcept'
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>% 
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
                                      .data$conceptCode,
                                      .data$standardConcept),
                        by = c("conceptId"))
  } else {
    sql <- "SELECT orphan_concept.*,
              concept_set_name,
              standard_concept.concept_name AS concept_name,
              standard_concept.vocabulary_id AS vocabulary_id,
              standard_concept.concept_code AS concept_code,
              standard_concept.standard_concept AS standard_concept
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
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}


getCohortOverlapResult <- function(dataSource = .GlobalEnv,
                                   targetCohortIds,
                                   comparatorCohortIds,
                                   databaseIds) {
  table <- 'cohortOverlap'
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>% 
      dplyr::filter(.data$targetCohortId %in% !!targetCohortIds &
                      .data$comparatorCohortId %in% !!comparatorCohortIds &
                      .data$databaseId %in% !!databaseIds) %>% 
      dplyr::inner_join(dplyr::select(get("cohort", envir = dataSource), 
                                      targetCohortId = .data$cohortId,
                                      targetCohortName = .data$cohortName,
                                      cohortName = .data$cohortName),
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
    return(NULL)
  }
  return(data)
}

getCovariateValueResult <- function(dataSource = .GlobalEnv,
                                    cohortIds,
                                    analysisIds = NULL,
                                    databaseIds,
                                    timeIds = NULL,
                                    isTemporal = FALSE) {

  if (isTemporal) {
    table <- "temporalCovariateValue"
    covariateRefTable <- "temporalCovariateRef"
    analysisRefTable <- "temporalAnalysisRef"
    timeRefTable <- "temporalTimeRef"
  } else {
    table <- "covariateValue"
    covariateRefTable <- "covariateRef"
    analysisRefTable <- "analysisRef"
    timeRefTable <- ""
  }
  
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>%
      dplyr::filter(.data$cohortId %in% !!cohortIds) %>% 
      dplyr::filter(.data$databaseId %in% !!databaseIds) %>%
      dplyr::inner_join(get(covariateRefTable, envir = dataSource), by = "covariateId") %>% 
      dplyr::inner_join(get(analysisRefTable, envir = dataSource), by = "analysisId")
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
    } else {
      data <- data %>%
        dplyr::select(-.data$startDay, -.data$endDay)
    }
  } else {
    sql <- "SELECT covariate.*,
              covariate_name,
            {@time_ref_table != \"\"} ? {
              start_day,
              end_day,
            }
              concept_id,
              covariate_ref.analysis_id,
              analysis_ref.is_binary,
              analysis_ref.analysis_name,
              analysis_ref.domain_id
            FROM  @results_database_schema.@table covariate
            INNER JOIN @results_database_schema.@covariate_ref_table covariate_ref
              ON covariate.covariate_id = covariate_ref.covariate_id
            INNER JOIN @results_database_schema.@analysis_ref_table analysis_ref
              ON covariate_ref.analysis_id = analysis_ref.analysis_id
            {@time_ref_table != \"\"} ? {
            INNER JOIN @results_database_schema.@time_ref_table time_ref
              ON covariate.time_id = time_ref.time_id
            }
            WHERE cohort_id in (@cohort_ids)
            {@time_ref_table != \"\" & @time_ids != \"\"} ? {  AND covariate.time_id IN (@time_ids)}
            {@analysis_ids != \"\"} ? {  AND covariate_ref.analysis_id IN (@analysis_ids)}
            	AND database_id in (@databaseIds);"
    if (is.null(timeIds)) {
      timeIds <- ""
    }
    if (is.null(analysisIds)) {
      analysisIds <- ""
    }
    # bringing down a lot of covariateName is probably slowing the return.
    # An alternative is to create two temp tables - one of it has distinct values of covariateId, covariateName
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    table = SqlRender::camelCaseToSnakeCase(table),
                                    covariate_ref_table = SqlRender::camelCaseToSnakeCase(covariateRefTable),
                                    analysis_ref_table = SqlRender::camelCaseToSnakeCase(analysisRefTable),
                                    time_ref_table = SqlRender::camelCaseToSnakeCase(timeRefTable),
                                    results_database_schema = dataSource$resultsDatabaseSchema,
                                    cohort_ids = cohortIds,
                                    analysis_ids = analysisIds,
                                    databaseIds = quoteLiterals(databaseIds),
                                    time_ids = timeIds,
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  if (isTemporal) {
    data <- data %>%
      dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>% 
      dplyr::relocate(.data$cohortId, 
                      .data$databaseId, 
                      .data$timeId, 
                      .data$startDay, 
                      .data$endDay,
                      .data$analysisId,
                      .data$covariateId, 
                      .data$covariateName,
                      .data$isBinary) %>% 
      dplyr::arrange(.data$cohortId, .data$databaseId, .data$timeId, .data$covariateId, .data$covariateName)
  } else {
    data <- data %>% 
      dplyr::left_join(analysisRef %>% 
                         dplyr::select(.data$analysisId, .data$startDay, .data$endDay),
                       by = "analysisId") %>% 
      dplyr::mutate(analysisNameLong = paste0(.data$analysisName, " (", as.character(.data$startDay), " to ", as.character(.data$endDay), ")")) %>% 
      dplyr::select(-.data$startDay, -.data$endDay) %>% 
      dplyr::relocate(.data$cohortId, 
                      .data$databaseId, 
                      .data$analysisId,
                      .data$covariateId, 
                      .data$covariateName,
                      .data$isBinary) %>% 
      dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
  }
  if ('missingMeansZero' %in% colnames(data)) {
    data <- data %>% 
      dplyr::mutate(mean = dplyr::if_else(is.na(.data$mean) &
                                                    !is.na(.data$missingMeansZero) &
                                                    .data$missingMeansZero  == 'Y',
                                          0,
                                          .data$mean)) %>% 
      dplyr::select(-.data$missingMeansZero)
  }
  return(data)
}

getConceptDetails <- function(dataSource = .GlobalEnv,
                              conceptIds) {
  table <- 'concept'
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>% 
      dplyr::filter(.data$conceptId %in% conceptIds)
  } else {
    sql <- "SELECT *
            FROM @vocabulary_database_schema.concept
            WHERE concept_id IN (@concept_ids);"
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                                    sql = sql,
                                    vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
                                    concept_ids = conceptIds, 
                                    snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}


resolveMappedConceptSetFromVocabularyDatabaseSchema <- function(dataSource = .GlobalEnv, 
                                                                conceptSets,
                                                                vocabularyDatabaseSchema = 'vocabulary') {
  if (is(dataSource, "environment")) {
    stop("Cannot resolve concept sets without a database connection")
  } else {
    sqlBase <- paste("SELECT DISTINCT codeset_id AS concept_set_id, concept.*",
                     "FROM (",
                     paste(conceptSets$conceptSetSql, collapse = ("\nUNION ALL\n")),
                     ") concept_sets",
                     sep = "\n")
    sqlResolved <- paste(sqlBase,
                         "INNER JOIN @vocabulary_database_schema.concept",
                         "  ON concept_sets.concept_id = concept.concept_id;",
                         sep = "\n")
    
    sqlBaseMapped <- paste("SELECT DISTINCT codeset_id AS concept_set_id, 
                           concept_sets.concept_id AS resolved_concept_id,
                           concept.*",
                           "FROM (",
                           paste(conceptSets$conceptSetSql, collapse = ("\nUNION ALL\n")),
                           ") concept_sets",
                           sep = "\n")
    sqlMapped <- paste(sqlBaseMapped,
                       "INNER JOIN @vocabulary_database_schema.concept_relationship",
                       "  ON concept_sets.concept_id = concept_relationship.concept_id_2",
                       "INNER JOIN @vocabulary_database_schema.concept",
                       "  ON concept_relationship.concept_id_1 = concept.concept_id",
                       "WHERE relationship_id = 'Maps to'",
                       "  AND standard_concept IS NULL;",
                       sep = "\n")
    
    resolved <- renderTranslateQuerySql(connection = dataSource$connection,
                                        sql = sqlResolved,
                                        vocabulary_database_schema = vocabularyDatabaseSchema,
                                        snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble() %>% 
      dplyr::select(.data$conceptSetId, .data$conceptId, .data$conceptName,
                    .data$domainId, .data$vocabularyId, .data$conceptClassId,
                    .data$standardConcept, .data$conceptCode, .data$invalidReason) %>% 
      dplyr::arrange(.data$conceptId)
    mapped <- renderTranslateQuerySql(connection = dataSource$connection,
                                      sql = sqlMapped,
                                      vocabulary_database_schema = vocabularyDatabaseSchema,
                                      snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble() %>% 
      dplyr::select(.data$resolvedConceptId, .data$conceptId,
                    .data$conceptName, .data$domainId,
                    .data$vocabularyId, .data$conceptClassId, 
                    .data$standardConcept, .data$conceptCode,
                    .data$conceptSetId) %>% 
      dplyr::distinct() %>% 
      dplyr::arrange(.data$resolvedConceptId, .data$conceptId)
  }
  data <- list(resolved = resolved, mapped = mapped)
  return(data)
}


resolveMappedConceptSet <- function(dataSource = .GlobalEnv, 
                                    databaseIds,
                                    cohortId) {
  table <- 'resolvedConcepts'
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    resolved <- get(table, envir = dataSource) %>% 
      dplyr::filter(.data$databaseId %in% !!databaseIds) %>% 
      dplyr::filter(.data$cohortId == !!cohortId) %>% 
      dplyr::inner_join(get("concept"), by = "conceptId") %>% 
      dplyr::distinct() %>% 
      dplyr::arrange(.data$conceptId)
    if (exists("conceptRelationship")) {
    mapped <- resolved %>% 
      dplyr::select(.data$conceptId, .data$databaseId, .data$cohortId, .data$conceptSetId) %>% 
      dplyr::distinct() %>% 
      dplyr::inner_join(get("conceptRelationship"), by = c("conceptId" = "conceptId2")) %>%
      dplyr::filter(.data$relationshipId == 'Maps to') %>%
      dplyr::filter(is.na(.data$invalidReason)) %>% 
      dplyr::select(.data$conceptId, .data$conceptId1, .data$databaseId, .data$cohortId, .data$conceptSetId) %>% 
      dplyr::rename(resolvedConceptId = .data$conceptId) %>% 
      dplyr::inner_join(get("concept"), by = c("conceptId1" = "conceptId")) %>% 
      dplyr::filter(is.na(.data$invalidReason)) %>% 
      dplyr::rename(conceptId = .data$conceptId1) %>% 
      dplyr::select(.data$resolvedConceptId, .data$conceptId,
                    .data$conceptName, .data$domainId,
                    .data$vocabularyId, .data$conceptClassId, 
                    .data$standardConcept, .data$conceptCode,
                    .data$databaseId, .data$conceptSetId) %>% 
      dplyr::distinct() %>% 
      dplyr::arrange(.data$resolvedConceptId, .data$conceptId)
    } else {
      mapped <- NULL
    }
  } else {
    sqlResolved <- "SELECT DISTINCT resolved_concepts.cohort_id,
                    	resolved_concepts.concept_set_id,
                    	concept.concept_id,
                    	concept.concept_name,
                    	concept.domain_id,
                    	concept.vocabulary_id,
                    	concept.concept_class_id,
                    	concept.standard_concept,
                    	concept.concept_code,
                    	resolved_concepts.database_id
                    FROM @results_database_schema.resolved_concepts
                    INNER JOIN @results_database_schema.concept
                    ON resolved_concepts.concept_id = concept.concept_id
                    WHERE database_id IN (@databaseIds)
                    	AND cohort_id = @cohortId
                    ORDER BY concept.concept_id;"
    resolved <- renderTranslateQuerySql(connection = dataSource$connection,
                                        sql = sqlResolved,
                                        results_database_schema = dataSource$resultsDatabaseSchema,
                                        databaseIds = quoteLiterals(databaseIds),
                                        cohortId = cohortId,
                                        snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble() %>% 
      dplyr::arrange(.data$conceptId)
    sqlMapped <- "SELECT DISTINCT concept_sets.concept_id AS resolved_concept_id,
                  	concept.concept_id,
                  	concept.concept_name,
                  	concept.domain_id,
                  	concept.vocabulary_id,
                  	concept.concept_class_id,
                  	concept.standard_concept,
                  	concept.concept_code,
                  	concept_sets.database_id,
                  	concept_sets.concept_set_id
                  FROM (
                  	SELECT DISTINCT concept_id, database_id, concept_set_id
                  	FROM @results_database_schema.resolved_concepts
                  	WHERE database_id IN (@databaseIds)
                  		AND cohort_id = @cohortId
                  	) concept_sets
                  INNER JOIN @results_database_schema.concept_relationship ON concept_sets.concept_id = concept_relationship.concept_id_2
                  INNER JOIN @results_database_schema.concept ON concept_relationship.concept_id_1 = concept.concept_id
                  WHERE relationship_id = 'Maps to'
                  	AND standard_concept IS NULL
                  ORDER BY concept.concept_id;"
    mapped <- renderTranslateQuerySql(connection = dataSource$connection,
                                      sql = sqlMapped,
                                      results_database_schema = dataSource$resultsDatabaseSchema,
                                      databaseIds = quoteLiterals(databaseIds),
                                      cohortId = cohortId,
                                      snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble() %>% 
      dplyr::arrange(.data$resolvedConceptId)
  }
  data <- list(resolved = resolved, 
               mapped = mapped)
  return(data)
}
