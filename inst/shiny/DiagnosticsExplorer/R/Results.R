createDatabaseDataSource <- function(connection, 
                                     resultsDatabaseSchema, 
                                     vocabularyDatabaseSchema = resultsDatabaseSchema,
                                     dbms) {
  return(list(connection = connectionPool,
              resultsDatabaseSchema = resultsDatabaseSchema,
              vocabularyDatabaseSchema = vocabularyDatabaseSchema,
              dbms = dbms))
}


renderTranslateExecuteSql <- function(connection, sql, ...) {
  if (is(connection, "Pool")) {
    sql <- SqlRender::render(sql, ...)
    sqlFinal <- SqlRender::translate(sql, targetDialect = dbms)
    DatabaseConnector::dbExecute(connection, sqlFinal)
  } else {
    DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                 sql = sql,
                                                 ...)
  }
}

getCohortCountResult <- function(dataSource,
                                 cohortIds = NULL,
                                 databaseIds = NULL) {
  sql <- "SELECT *
            FROM  @results_database_schema.cohort_count
            WHERE cohort_id IS NOT NULL 
            {@database_ids != ''} ? { AND database_id in (@database_id)}
            {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)}
            ;"
  data <- renderTranslateQuerySql(connection = dataSource$connection,
                                  dbms = dataSource$dbms,
                                  sql = sql,
                                  results_database_schema = dataSource$resultsDatabaseSchema,
                                  cohort_ids = cohortIds,
                                  database_id = if (!is.null(databaseIds)) {quoteLiterals(databaseIds)} else {''},
                                  snakeCaseToCamelCase = TRUE) %>%
    tidyr::tibble()

  return(data)
}



getIncidenceRateResult <- function(dataSource,
                                   cohortIds,
                                   databaseIds,
                                   stratifyByGender = c(TRUE, FALSE),
                                   stratifyByAgeGroup = c(TRUE, FALSE),
                                   stratifyByCalendarYear = c(TRUE, FALSE),
                                   minPersonYears = 1000,
                                   minSubjectCount = NA) {
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

  sql <- "SELECT *
            FROM  @results_database_schema.incidence_rate
            WHERE cohort_id in (@cohort_ids)
           	  AND database_id in (@database_ids)
            {@gender == TRUE} ? {AND gender != ''} : {  AND gender = ''}
            {@age_group == TRUE} ? {AND age_group != ''} : {  AND age_group = ''}
            {@calendar_year == TRUE} ? {AND calendar_year != ''} : {  AND calendar_year = ''}
              AND person_years > @personYears;"
  data <- renderTranslateQuerySql(connection = dataSource$connection,
                                  dbms = dataSource$dbms,
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

  data <- data %>%
    dplyr::inner_join(cohortCount,
                      by = c("cohortId", "databaseId")) %>%
    dplyr::mutate(calendarYear = as.integer(.data$calendarYear)) %>%
    dplyr::arrange(.data$cohortId, .data$databaseId)

  if (!is.na(minSubjectCount)) {
    data <- data %>%
      dplyr::filter(.data$cohortSubjects > !!minSubjectCount)
  }

  return(data)
}

getInclusionRuleStats <- function(dataSource,
                                  cohortIds = NULL,
                                  databaseIds) {

  sql <- "SELECT *
    FROM  @resultsDatabaseSchema.inclusion_rule_stats
    WHERE database_id in (@database_id)
    {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)}
    ;"
  data <- renderTranslateQuerySql(connection = dataSource$connection,
                                  dbms = dataSource$dbms,
                                  sql = sql,
                                  resultsDatabaseSchema = dataSource$resultsDatabaseSchema,
                                  cohort_ids = cohortIds,
                                  database_id = quoteLiterals(databaseIds),
                                  snakeCaseToCamelCase = TRUE) %>%
    tidyr::tibble()

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


getIndexEventBreakdown <- function(dataSource,
                                   cohortIds,
                                   databaseIds) {
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = cohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)

  sql <- "SELECT index_event_breakdown.*,
              concept.concept_name,
              concept.domain_id,
              concept.vocabulary_id,
              concept.standard_concept,
              concept.concept_code
            FROM  @results_database_schema.index_event_breakdown
            INNER JOIN  @vocabulary_database_schema.concept
              ON index_event_breakdown.concept_id = concept.concept_id
            WHERE database_id in (@database_id)
              AND cohort_id in (@cohort_ids);"
  data <- renderTranslateQuerySql(connection = dataSource$connection,
                                  dbms = dataSource$dbms,
                                  sql = sql,
                                  results_database_schema = dataSource$resultsDatabaseSchema,
                                  vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
                                  cohort_ids = cohortIds,
                                  database_id = quoteLiterals(databaseIds),
                                  snakeCaseToCamelCase = TRUE) %>%
    tidyr::tibble()


  data <- data %>%
    dplyr::inner_join(cohortCount,
                      by = c('databaseId', 'cohortId')) %>%
    dplyr::mutate(subjectPercent = .data$subjectCount / .data$cohortSubjects,
                  conceptPercent = .data$conceptCount / .data$cohortEntries)

  return(data)
}

getVisitContextResults <- function(dataSource,
                                   cohortIds,
                                   databaseIds) {
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <- checkErrorCohortIdsDatabaseIds(cohortIds = cohortIds,
                                                 databaseIds = databaseIds,
                                                 errorMessage = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)

  sql <- "SELECT visit_context.*,
              standard_concept.concept_name AS visit_concept_name
            FROM  @results_database_schema.visit_context
            INNER JOIN  @vocabulary_database_schema.concept standard_concept
              ON visit_context.visit_concept_id = standard_concept.concept_id
            WHERE database_id in (@database_id)
              AND cohort_id in (@cohort_ids);"
  data <- renderTranslateQuerySql(connection = dataSource$connection,
                                  dbms = dataSource$dbms,
                                  sql = sql,
                                  results_database_schema = dataSource$resultsDatabaseSchema,
                                  vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
                                  cohort_ids = cohortIds,
                                  database_id = quoteLiterals(databaseIds),
                                  snakeCaseToCamelCase = TRUE) %>%
    tidyr::tibble()

  data <- data %>%
    dplyr::inner_join(cohortCount,
                      by = c("cohortId", "databaseId")) %>%
    dplyr::mutate(subjectPercent = .data$subjects / .data$cohortSubjects)
  return(data)
}

getConceptsInCohort <-
  function(dataSource,
           cohortId,
           databaseIds) {
    sql <- "SELECT concepts.*,
            	c.concept_name,
            	c.vocabulary_id,
            	c.domain_id,
            	c.standard_concept,
            	c.concept_code
            FROM (
            	SELECT database_id,
            		cohort_id,
            		concept_id,
            		0 source_concept_id,
            		max(concept_subjects) concept_subjects,
            		sum(concept_count) concept_count
            	FROM @results_database_schema.included_source_concept
            	WHERE included_source_concept.cohort_id = @cohort_id
            		AND database_id IN (@database_ids)
            	GROUP BY database_id,
            		cohort_id,
            		concept_id
            	
            	UNION
            	
            	SELECT c.database_id,
            		c.cohort_id,
            		c.source_concept_id concept_id,
            		1 source_concept_id,
            		max(c.concept_subjects) concept_subjects,
            		sum(c.concept_count) concept_count
            	FROM @results_database_schema.included_source_concept c
            	WHERE c.cohort_id = @cohort_id
            		AND c.database_id IN (@database_ids)
            	) concepts
            INNER JOIN @results_database_schema.concept c ON concepts.concept_id = c.concept_id
            WHERE c.invalid_reason IS NULL;"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        dbms = dataSource$dbms,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        cohort_id = cohortId,
        database_ids = quoteLiterals(databaseIds),
        snakeCaseToCamelCase = TRUE
      ) %>%
      tidyr::tibble()
    return(data)
  }


getCountForConceptIdInCohort <-
  function(dataSource,
           cohortId,
           databaseIds) {
    sql <- "SELECT included_source_concept.*
            FROM  @results_database_schema.included_source_concept
            WHERE included_source_concept.cohort_id = @cohort_id
             AND database_id in (@database_ids);"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        dbms = dataSource$dbms,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        cohort_id = cohortId,
        database_ids = quoteLiterals(databaseIds),
        snakeCaseToCamelCase = TRUE
      ) %>%
      tidyr::tibble()
    
    standardConceptId <- data %>%
      dplyr::select(.data$databaseId,
                    .data$conceptId,
                    .data$conceptSubjects,
                    .data$conceptCount) %>%
      dplyr::group_by(.data$databaseId,
                      .data$conceptId) %>% 
    dplyr::summarise(
      conceptSubjects = max(.data$conceptSubjects),
      conceptCount = sum(.data$conceptCount), 
      .groups = "keep"
    ) %>%
      dplyr::ungroup()
    
    
    sourceConceptId <- data %>%
      dplyr::select(
        .data$databaseId,
        .data$sourceConceptId,
        .data$conceptSubjects,
        .data$conceptCount
      ) %>%
      dplyr::rename(conceptId = .data$sourceConceptId) %>%
      dplyr::group_by(.data$databaseId,
                      .data$conceptId) %>% 
      dplyr::summarise(
        conceptSubjects = max(.data$conceptSubjects),
        conceptCount = sum(.data$conceptCount), 
        .groups = "keep"
      ) %>%
      dplyr::ungroup()
    
    data <- dplyr::bind_rows(
      standardConceptId,
      sourceConceptId %>%
        dplyr::anti_join(
          y = standardConceptId %>%
            dplyr::select(.data$databaseId, .data$conceptId),
          by = c("databaseId", "conceptId")
        )
    ) %>%
      dplyr::distinct() %>%
      dplyr::arrange(.data$databaseId, .data$conceptId)
    
    return(data)
  }

getOrphanConceptResult <- function(dataSource,
                                   cohortId,
                                   databaseIds) {
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
                                  dbms = dataSource$dbms,
                                  sql = sql,
                                  results_database_schema = dataSource$resultsDatabaseSchema,
                                  vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
                                  cohort_id = cohortId,
                                  database_ids = quoteLiterals(databaseIds),
                                  snakeCaseToCamelCase = TRUE) %>%
    tidyr::tibble()
  return(data)
}


getCohortOverlapResult <- function(dataSource,
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
  sql <- "SELECT cohort_overlap.*,
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
                                  dbms = dataSource$dbms,
                                  sql = sql,
                                  results_database_schema = dataSource$resultsDatabaseSchema,
                                  targetCohortId = targetCohortIds,
                                  comparatorCohortId = comparatorCohortIds,
                                  databaseId = quoteLiterals(databaseIds),
                                  snakeCaseToCamelCase = TRUE) %>%
    tidyr::tibble()


  if (nrow(data) == 0) {
    return(tidyr::tibble())
  }
  return(data)
}




getConceptDetails <- function(dataSource,
                              conceptIds) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(x = conceptIds,
                              min.len = 1,
                              null.ok = TRUE,
                              add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)

  sql <- "SELECT *
            FROM @vocabulary_database_schema.concept
            WHERE concept_id IN (@concept_ids);"
  data <- renderTranslateQuerySql(connection = dataSource$connection,
                                  dbms = dataSource$dbms,
                                  sql = sql,
                                  vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
                                  concept_ids = conceptIds,
                                  snakeCaseToCamelCase = TRUE) %>%
    tidyr::tibble()

  return(data)
}

resolveMappedConceptSetFromVocabularyDatabaseSchema <- function(dataSource,
                                                                conceptSets,
                                                                vocabularyDatabaseSchema = 'vocabulary') {

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
                                      dbms = dataSource$dbms,
                                      sql = sqlResolved,
                                      vocabulary_database_schema = vocabularyDatabaseSchema,
                                      snakeCaseToCamelCase = TRUE) %>%
    tidyr::tibble() %>%
    dplyr::select(.data$conceptSetId, .data$conceptId, .data$conceptName,
                  .data$domainId, .data$vocabularyId, .data$conceptClassId,
                  .data$standardConcept, .data$conceptCode, .data$invalidReason) %>%
    dplyr::arrange(.data$conceptId)
  mapped <- renderTranslateQuerySql(connection = dataSource$connection,
                                    dbms = dataSource$dbms,
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

  data <- list(resolved = resolved, mapped = mapped)
  return(data)
}


resolvedConceptSet <- function(dataSource,
                              databaseIds,
                              cohortId) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(
    x = cohortId,
    min.len = 1,
    max.len = 1,
    null.ok = TRUE,
    add = errorMessage
  )
  checkmate::assertCharacter(
    x = databaseIds,
    min.len = 1,
    min.chars = 1,
    null.ok = TRUE,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  
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
  resolved <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sqlResolved,
      results_database_schema = dataSource$resultsDatabaseSchema,
      databaseIds = quoteLiterals(databaseIds),
      cohortId = cohortId,
      snakeCaseToCamelCase = TRUE
    ) %>%
    tidyr::tibble() %>%
    dplyr::arrange(.data$conceptId)
  
  return(resolved)
}


mappedConceptSet <- function(dataSource,
                             databaseIds,
                             cohortId) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertIntegerish(
    x = cohortId,
    min.len = 1,
    max.len = 1,
    null.ok = TRUE,
    add = errorMessage
  )
  checkmate::assertCharacter(
    x = databaseIds,
    min.len = 1,
    min.chars = 1,
    null.ok = TRUE,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  sqlMapped <-
    "WITH resolved_concepts_mapped
    AS (
    	SELECT concept_sets.concept_id AS resolved_concept_id,
    		concept.concept_id,
    		concept.concept_name,
    		concept.domain_id,
    		concept.vocabulary_id,
    		concept.concept_class_id,
    		concept.standard_concept,
    		concept.concept_code
    	FROM (
    		SELECT DISTINCT concept_id
    		FROM @results_database_schema.resolved_concepts
    		WHERE database_id IN (@databaseIds)
    			AND cohort_id = @cohortId
    		) concept_sets
    	INNER JOIN @results_database_schema.concept_relationship ON concept_sets.concept_id = concept_relationship.concept_id_2
    	INNER JOIN @results_database_schema.concept ON concept_relationship.concept_id_1 = concept.concept_id
    	WHERE relationship_id = 'Maps to'
    		AND standard_concept IS NULL
    	)
    SELECT c.database_id,
    	c.cohort_id,
    	c.concept_set_id,
    	mapped.*
    FROM (SELECT DISTINCT concept_id, database_id, cohort_id, concept_set_id FROM @results_database_schema.resolved_concepts) c
    INNER JOIN resolved_concepts_mapped mapped ON c.concept_id = mapped.resolved_concept_id;"
  mapped <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sqlMapped,
      results_database_schema = dataSource$resultsDatabaseSchema,
      databaseIds = quoteLiterals(databaseIds),
      cohortId = cohortId,
      snakeCaseToCamelCase = TRUE
    ) %>%
    tidyr::tibble() %>%
    dplyr::arrange(.data$resolvedConceptId)
  return(mapped)
}


getDatabaseCounts <- function(dataSource,
                              databaseIds) {
  sql <- "SELECT *
              FROM  @results_database_schema.database
              WHERE database_id in (@database_ids);"
  data <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sql,
      results_database_schema = dataSource$resultsDatabaseSchema,
      database_ids = quoteLiterals(databaseIds),
      snakeCaseToCamelCase = TRUE
    ) %>%
    tidyr::tibble()
  
  return(data)
}
