createDatabaseDataSource <-
  function(connection,
           resultsDatabaseSchema,
           vocabularyDatabaseSchema = resultsDatabaseSchema) {
    return(
      list(
        connection = connection,
        resultsDatabaseSchema = resultsDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema
      )
    )
  }

createFileDataSource <-
  function(premergedDataFile, envir = new.env()) {
    load(premergedDataFile, envir = envir)
    return(envir)
  }


renderTranslateQuerySql <-
  function(connection,
           sql,
           ...,
           snakeCaseToCamelCase = FALSE) {
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
      return(data %>% dplyr::tibble())
    } else {
      return(
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          ...,
          snakeCaseToCamelCase = snakeCaseToCamelCase
        ) %>% dplyr::tibble()
      )
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
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        cohort_ids = cohortIds,
        database_id = quoteLiterals(databaseIds),
        snakeCaseToCamelCase = TRUE
      )
  }
  return(data)
}

getTimeDistributionResult <- function(dataSource = .GlobalEnv,
                                      cohortIds) {
  if (is(dataSource, "environment")) {
    data <- get("timeDistribution", envir = dataSource) %>%
      dplyr::filter(.data$cohortId %in% !!cohortIds)
  } else {
    sql <-   "SELECT *
              FROM  @results_database_schema.time_distribution
              WHERE cohort_id in (@cohort_ids);"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        cohort_ids = cohortIds,
        snakeCaseToCamelCase = TRUE
      )
  }
  data <- data %>%
    dplyr::mutate(timeMetric = as.factor(.data$timeMetric))
  return(data)
}


getIncidenceRateResult <- function(dataSource = .GlobalEnv,
                                   cohortIds,
                                   minPersonYears = 0) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertDouble(
    x = cohortIds,
    null.ok = FALSE,
    lower = 1,
    upper = 2 ^ 53,
    any.missing = FALSE,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  
  if (is(dataSource, "environment")) {
    data <- get("incidenceRate", envir = dataSource) %>%
      dplyr::filter(.data$cohortId %in% !!cohortIds &
                      .data$personYears > !!minPersonYears)
  } else {
    sql <- "SELECT *
            FROM  @results_database_schema.incidence_rate
            WHERE cohort_id in (@cohort_ids)
              AND person_years > @personYears;"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        cohort_ids = cohortIds,
        personYears = minPersonYears,
        snakeCaseToCamelCase = TRUE
      )
  }
  data <- data %>% 
    dplyr::mutate(gender = dplyr::case_when(.data$gender == "" ~ "All", TRUE ~ .data$gender),
                  ageGroup = dplyr::case_when(.data$ageGroup == "" ~ "All", TRUE ~ .data$ageGroup),
                  calendarYear = dplyr::case_when(.data$calendarYear == "" ~ "All", TRUE ~ .data$calendarYear))
  return(data)
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
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        resultsDatabaseSchema = dataSource$resultsDatabaseSchema,
        cohort_ids = cohortIds,
        database_id = quoteLiterals(databaseIds),
        snakeCaseToCamelCase = TRUE
      )
  }
  data <- data %>%
    dplyr::select(
      .data$cohortId,
      .data$ruleSequenceId,
      .data$ruleName,
      .data$meetSubjects,
      .data$gainSubjects,
      .data$remainSubjects,
      .data$totalSubjects,
      .data$databaseId
    ) %>%
    dplyr::arrange(.data$cohortId, .data$ruleSequenceId)
  return(data)
}


getIndexEventBreakdown <- function(dataSource = .GlobalEnv,
                                   cohortIds,
                                   databaseIds) {
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <-
    checkErrorCohortIdsDatabaseIds(
      cohortIds = cohortIds,
      databaseIds = databaseIds,
      errorMessage = errorMessage
    )
  checkmate::reportAssertions(collection = errorMessage)
  
  if (is(dataSource, "environment")) {
    data <- get("indexEventBreakdown", envir = dataSource) %>%
      dplyr::filter(.data$databaseId %in% !!databaseIds)
    if (!is.null(cohortIds)) {
      data <- data %>%
        dplyr::filter(.data$cohortId %in% !!cohortIds)
    }
    data <- data %>%
      dplyr::inner_join(dplyr::select(
        get("concept", envir = dataSource),
        .data$conceptId,
        .data$conceptName
      ),
      by = c("conceptId"))
  } else {
    sql <- "SELECT index_event_breakdown.*,
              standard_concept.concept_name AS concept_name
            FROM  @results_database_schema.index_event_breakdown
            INNER JOIN  @vocabulary_database_schema.concept standard_concept
              ON index_event_breakdown.concept_id = standard_concept.concept_id
            WHERE database_id in (@database_id)
              AND cohort_id in (@cohort_ids);"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
        cohort_ids = cohortIds,
        database_id = quoteLiterals(databaseIds),
        snakeCaseToCamelCase = TRUE
      )
  }
  data <-
    data %>% dplyr::relocate(
      .data$databaseId,
      .data$cohortId,
      .data$conceptId,
      .data$conceptName,
      .data$conceptCount
    )
  return(data)
}

getVisitContextResults <- function(dataSource = .GlobalEnv,
                                   cohortIds,
                                   databaseIds) {
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <-
    checkErrorCohortIdsDatabaseIds(
      cohortIds = cohortIds,
      databaseIds = databaseIds,
      errorMessage = errorMessage
    )
  checkmate::reportAssertions(collection = errorMessage)
  
  if (is(dataSource, "environment")) {
    data <- get("visitContext", envir = dataSource) %>%
      dplyr::filter(.data$databaseId %in% !!databaseIds)
    if (!is.null(cohortIds)) {
      data <- data %>%
        dplyr::filter(.data$cohortId %in% !!cohortIds)
    }
    data <- data %>%
      dplyr::inner_join(
        dplyr::select(
          get("concept", envir = dataSource),
          visitConceptId = .data$conceptId,
          visitConceptName = .data$conceptName
        ),
        by = c("visitConceptId")
      )
  } else {
    sql <- "SELECT visit_context.*,
              standard_concept.concept_name AS visit_concept_name
            FROM  @results_database_schema.visit_context
            INNER JOIN  @vocabulary_database_schema.concept standard_concept
              ON visit_context.visit_concept_id = standard_concept.concept_id
            WHERE database_id in (@database_id)
              AND cohort_id in (@cohort_ids);"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
        cohort_ids = cohortIds,
        database_id = quoteLiterals(databaseIds),
        snakeCaseToCamelCase = TRUE
      )
  }
  data <- data %>%
    tidyr::pivot_wider(
      id_cols = c(
        .data$databaseId,
        .data$cohortId,
        .data$visitConceptId,
        .data$visitConceptName
      ),
      names_from = .data$visitContext,
      values_from = .data$subjects
    ) %>% dplyr::relocate(.data$databaseId,
                          .data$cohortId,
                          .data$visitConceptId,
                          .data$visitConceptName) %>%
    dplyr::mutate(
      visitConceptName = as.factor(.data$visitConceptName),
      visitConceptId = as.factor(.data$visitConceptId)
    )
  return(data)
}

getIncludedConceptResult <- function(dataSource = .GlobalEnv,
                                     cohortId,
                                     databaseIds) {
  if (is(dataSource, "environment")) {
    data <- get("includedSourceConcept", envir = dataSource) %>%
      dplyr::filter(.data$cohortId == !!cohortId &
                      .data$databaseId %in% !!databaseIds) %>%
      dplyr::inner_join(
        dplyr::select(
          get("conceptSets", envir = dataSource),
          .data$cohortId,
          .data$conceptSetId,
          .data$conceptSetName
        ),
        by = c("cohortId", "conceptSetId")
      ) %>%
      dplyr::inner_join(
        dplyr::select(
          get("concept", envir = dataSource),
          sourceConceptId = .data$conceptId,
          sourceConceptName = .data$conceptName,
          sourceVocabularyId = .data$vocabularyId,
          sourceConceptCode = .data$conceptCode
        ),
        by = c("sourceConceptId")
      ) %>%
      dplyr::inner_join(
        dplyr::select(
          get("concept", envir = dataSource),
          .data$conceptId,
          .data$conceptName,
          .data$vocabularyId
        ),
        by = c("conceptId")
      )
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
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
        cohort_id = cohortId,
        database_ids = quoteLiterals(databaseIds),
        snakeCaseToCamelCase = TRUE
      )
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
      dplyr::inner_join(
        dplyr::select(
          get("conceptSets", envir = dataSource),
          .data$cohortId,
          .data$conceptSetId,
          .data$conceptSetName
        ),
        by = c("cohortId", "conceptSetId")
      ) %>%
      dplyr::inner_join(
        dplyr::select(
          get("concept", envir = dataSource),
          .data$conceptId,
          .data$conceptName,
          .data$vocabularyId,
          .data$conceptCode
        ),
        by = c("conceptId")
      )
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
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
        cohort_id = cohortId,
        database_ids = quoteLiterals(databaseIds),
        snakeCaseToCamelCase = TRUE
      )
  }
  
  return(data)
}


getCohortOverlapResult <- function(dataSource = .GlobalEnv,
                                   targetCohortIds,
                                   comparatorCohortIds) {
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertDouble(
    x = targetCohortIds,
    null.ok = FALSE,
    lower = 1,
    upper = 2 ^ 53,
    any.missing = FALSE,
    add = errorMessage
  )  
  checkmate::assertDouble(
    x = comparatorCohortIds,
    null.ok = FALSE,
    lower = 1,
    upper = 2 ^ 53,
    any.missing = FALSE,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  
  if (is(dataSource, "environment")) {
    data <- get("cohortOverlap", envir = dataSource) %>%
      dplyr::filter(
        .data$targetCohortId %in% !!targetCohortIds &
          .data$comparatorCohortId %in% !!comparatorCohortIds
      ) %>%
      dplyr::inner_join(
        dplyr::select(
          get("cohort", envir = dataSource),
          targetCohortId = .data$cohortId,
          targetCohortName = .data$cohortName
        ),
        by = "targetCohortId"
      ) %>%
      dplyr::inner_join(
        dplyr::select(
          get("cohort", envir = dataSource),
          comparatorCohortId = .data$cohortId,
          comparatorCohortName = .data$cohortName
        ),
        by = "comparatorCohortId"
      )
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
              AND comparator_cohort_id in (@comparatorCohortId);"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        targetCohortId = targetCohortIds,
        comparatorCohortId = comparatorCohortIds,
        snakeCaseToCamelCase = TRUE
      )
  }
  
  if (nrow(data) == 0) {
    return(dplyr::tibble())
  }
  return(data)
}

getCovariateValueResult <- function(dataSource = .GlobalEnv,
                                    table = "covariate_value",
                                    cohortIds) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertDouble(
    x = cohortIds,
    null.ok = FALSE,
    lower = 1,
    upper = 2 ^ 53,
    any.missing = FALSE,
    add = errorMessage
  )
  checkmate::assertCharacter(x = table, add = errorMessage)
  checkmate::assertChoice(x = table, 
                          choices = c("covariateValue",
                                      "temporalCovariateValue")
                          , add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  if (is(dataSource, "environment")) {
    data <- get(table, envir = dataSource) %>%
      dplyr::filter(.data$cohortId %in% !!cohortIds)
  } else {
    sql <- "
    SELECT *
    FROM @results_database_schema.@table
    WHERE cohort_id IN (@cohort_ids);
    "
    data <- renderTranslateQuerySql(connection = dataSource$connection,
                            sql = sql,
                            table = camelCaseToSnakeCase(table),
                            results_database_schema = dataSource$resultsDatabaseSchema,
                            cohort_ids = cohortIds,
                            snakeCaseToCamelCase = TRUE)
  }
  return(data)
}

getConceptDetails <- function(dataSource = .GlobalEnv,
                              conceptIds) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(
    x = conceptIds,
    min.len = 1,
    null.ok = TRUE,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  if (is(dataSource, "environment")) {
    data <- get("concept", envir = dataSource) %>%
      dplyr::filter(.data$conceptId %in% conceptIds)
  } else {
    sql <- "SELECT *
            FROM @vocabulary_database_schema.concept
            WHERE concept_id IN (@concept_ids);"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
        concept_ids = conceptIds,
        snakeCaseToCamelCase = TRUE
      )
  }
  return(data)
}

resolveConceptSet <-
  function(dataSource = .GlobalEnv,
           conceptSets,
           source = FALSE) {
    if (is(dataSource, "environment")) {
      stop("Cannot resolve concept sets without a database connection")
    } else {
      sql <-
        paste(
          "SELECT DISTINCT codeset_id AS concept_set_id, concept.*",
          "FROM (",
          paste(conceptSets$conceptSetSql, collapse = ("\nUNION ALL\n")),
          ") concept_sets",
          sep = "\n"
        )
      
      if (source) {
        sql <- paste(
          sql,
          "INNER JOIN @vocabulary_database_schema.concept_relationship",
          "  ON concept_sets.concept_id = concept_relationship.concept_id_2",
          "INNER JOIN @vocabulary_database_schema.concept",
          "  ON concept_relationship.concept_id_1 = concept.concept_id",
          "WHERE relationship_id = 'Maps to'",
          "  AND standard_concept IS NULL;",
          sep = "\n"
        )
      } else {
        sql <- paste(
          sql,
          "INNER JOIN @vocabulary_database_schema.concept",
          "  ON concept_sets.concept_id = concept.concept_id;",
          sep = "\n"
        )
      }
      
      data <-
        renderTranslateQuerySql(
          connection = dataSource$connection,
          sql = sql,
          vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
          snakeCaseToCamelCase = TRUE
        )
    }
    return(data %>% dplyr::arrange(.data$conceptId))
  }

checkErrorCohortIdsDatabaseIds <- function(errorMessage,
                                           cohortIds,
                                           databaseIds) {
  checkmate::assertDouble(
    x = cohortIds,
    null.ok = FALSE,
    lower = 1,
    upper = 2 ^ 53,
    any.missing = FALSE,
    add = errorMessage
  )
  checkmate::assertCharacter(
    x = databaseIds,
    min.len = 1,
    any.missing = FALSE,
    unique = TRUE,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  return(errorMessage)
}

getSearchTerms <- function(dataSource, includeDescendants = FALSE) {
  if (is(dataSource, "environment")) {
    warning("Search terms not implemented when using in-memory objects")
    return(dplyr::tibble(phenotypeId = -1, term = ""))
  } else {
    if (includeDescendants) {
      sql <- "SELECT DISTINCT phenotype_id,
              LOWER(term) AS term
              FROM (
                SELECT phenotype_id,
                  concept_synonym_name AS term
                FROM @results_database_schema.phenotype_description
                INNER JOIN @vocabulary_database_schema.concept_ancestor
                  ON (phenotype_description.phenotype_id/1000) = ancestor_concept_id
                INNER JOIN @vocabulary_database_schema.concept_synonym
                  ON descendant_concept_id = concept_synonym.concept_id
                WHERE language_concept_id = 4180186 -- English

                UNION

                SELECT phenotype_id,
                  concept_synonym_name AS term
                FROM @results_database_schema.phenotype_description
                INNER JOIN @vocabulary_database_schema.concept_ancestor
                  ON (phenotype_description.phenotype_id/1000) = ancestor_concept_id
                INNER JOIN @vocabulary_database_schema.concept_relationship
                  ON descendant_concept_id = concept_id_2
                INNER JOIN @vocabulary_database_schema.concept_synonym
                  ON concept_id_1 = concept_synonym.concept_id
                WHERE relationship_id = 'Maps to'
                  AND language_concept_id = 4180186 -- English
              ) tmp;"
    } else {
      sql <- "SELECT DISTINCT phenotype_id,
                LOWER(term) AS term
              FROM (
                SELECT phenotype_id,
                  concept_synonym_name AS term
                FROM @results_database_schema.phenotype_description
                INNER JOIN @vocabulary_database_schema.concept_synonym
                  ON (phenotype_description.phenotype_id/1000) = concept_synonym.concept_id
                WHERE language_concept_id = 4180186 -- English

                UNION

                SELECT phenotype_id,
                  concept_synonym_name AS term
                FROM @results_database_schema.phenotype_description
                INNER JOIN @vocabulary_database_schema.concept_relationship
                  ON (phenotype_description.phenotype_id/1000) = concept_id_2
                INNER JOIN @vocabulary_database_schema.concept_synonym
                  ON concept_id_1 = concept_synonym.concept_id
                WHERE relationship_id = 'Maps to'
                  AND language_concept_id = 4180186 -- English
              ) tmp;"
    }
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
        snakeCaseToCamelCase = TRUE
      )
    return(data)
  }
}
