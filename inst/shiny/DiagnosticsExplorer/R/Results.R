renderTranslateExecuteSql <- function(dataSource, sql, ...) {
  if (is(dataSource$connection, "Pool")) {
    sql <- SqlRender::render(sql, ...)
    sqlFinal <- SqlRender::translate(sql, targetDialect = dataSource$dbms)
    DatabaseConnector::dbExecute(dataSource$connection, sqlFinal)
  } else {
    DatabaseConnector::renderTranslateExecuteSql(
      connection = dataSource$connection,
      sql = sql,
      ...
    )
  }
}

getResultsCohortCounts <- function(dataSource,
                                   cohortIds = NULL,
                                   databaseIds = NULL) {
  sql <- "SELECT cc.*, db.database_name
            FROM  @results_database_schema.@table_name cc
            INNER JOIN @results_database_schema.@database_table db ON db.database_id = cc.database_id
            WHERE cc.cohort_id IS NOT NULL
            {@use_database_ids} ? { AND cc.database_id in (@database_ids)}
            {@cohort_ids != ''} ? {  AND cc.cohort_id in (@cohort_ids)}
            ;"
  data <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sql,
      results_database_schema = dataSource$resultsDatabaseSchema,
      cohort_ids = cohortIds,
      use_database_ids = !is.null(databaseIds),
      database_ids = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("cohort_count"),
      database_table = dataSource$databaseTableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()

  return(data)
}

#' Global ranges for IR values
getIncidenceRateRanges <- function(dataSource, minPersonYears = 0) {
  sql <- "SELECT DISTINCT age_group FROM @results_database_schema.@ir_table WHERE person_years >= @person_years"

  ageGroups <- renderTranslateQuerySql(
    connection = dataSource$connection,
    dbms = dataSource$dbms,
    sql = sql,
    results_database_schema = dataSource$resultsDatabaseSchema,
    ir_table = dataSource$prefixTable("incidence_rate"),
    person_years = minPersonYears,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::mutate(ageGroup = dplyr::na_if(.data$ageGroup, ""))

  sql <- "SELECT DISTINCT calendar_year FROM @results_database_schema.@ir_table WHERE person_years >= @person_years"

  calendarYear <- renderTranslateQuerySql(
    connection = dataSource$connection,
    dbms = dataSource$dbms,
    sql = sql,
    results_database_schema = dataSource$resultsDatabaseSchema,
    ir_table = dataSource$prefixTable("incidence_rate"),
    person_years = minPersonYears,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::mutate(
      calendarYear = dplyr::na_if(.data$calendarYear, "")
    ) %>%
    dplyr::mutate(calendarYear = as.integer(.data$calendarYear))

  sql <- "SELECT DISTINCT gender FROM @results_database_schema.@ir_table WHERE person_years >= @person_years"

  gender <- renderTranslateQuerySql(
    connection = dataSource$connection,
    dbms = dataSource$dbms,
    sql = sql,
    results_database_schema = dataSource$resultsDatabaseSchema,
    ir_table = dataSource$prefixTable("incidence_rate"),
    person_years = minPersonYears,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::mutate(gender = dplyr::na_if(.data$gender, ""))


  sql <- "SELECT
    min(incidence_rate) as min_ir,
    max(incidence_rate) as max_ir
   FROM @results_database_schema.@ir_table
   WHERE person_years >= @person_years
   AND incidence_rate > 0.0
   "

  incidenceRate <- renderTranslateQuerySql(
    connection = dataSource$connection,
    dbms = dataSource$dbms,
    sql = sql,
    results_database_schema = dataSource$resultsDatabaseSchema,
    ir_table = dataSource$prefixTable("incidence_rate"),
    person_years = minPersonYears,
    snakeCaseToCamelCase = TRUE
  )

  return(list(gender = gender,
              incidenceRate = incidenceRate,
              calendarYear = calendarYear,
              ageGroups = ageGroups))
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
  errorMessage <-
    checkErrorCohortIdsDatabaseIds(
      cohortIds = cohortIds,
      databaseIds = databaseIds,
      errorMessage = errorMessage
    )
  checkmate::assertLogical(
    x = stratifyByGender,
    add = errorMessage,
    min.len = 1,
    max.len = 2,
    unique = TRUE
  )
  checkmate::assertLogical(
    x = stratifyByAgeGroup,
    add = errorMessage,
    min.len = 1,
    max.len = 2,
    unique = TRUE
  )
  checkmate::assertLogical(
    x = stratifyByCalendarYear,
    add = errorMessage,
    min.len = 1,
    max.len = 2,
    unique = TRUE
  )
  checkmate::reportAssertions(collection = errorMessage)

  sql <- "SELECT ir.*, dt.database_name, cc.cohort_subjects
            FROM  @results_database_schema.@ir_table ir
            INNER JOIN @results_database_schema.@database_table dt ON ir.database_id = dt.database_id
            INNER JOIN @results_database_schema.@cc_table cc ON (
              ir.database_id = cc.database_id AND ir.cohort_id = cc.cohort_id
            )
            WHERE ir.cohort_id in (@cohort_ids)
           	  AND ir.database_id in (@database_ids)
            {@gender == TRUE} ? {AND ir.gender != ''} : {  AND ir.gender = ''}
            {@age_group == TRUE} ? {AND ir.age_group != ''} : {  AND ir.age_group = ''}
            {@calendar_year == TRUE} ? {AND ir.calendar_year != ''} : {  AND ir.calendar_year = ''}
              AND ir.person_years > @personYears;"
  data <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sql,
      results_database_schema = dataSource$resultsDatabaseSchema,
      cohort_ids = cohortIds,
      database_ids = quoteLiterals(databaseIds),
      gender = stratifyByGender,
      age_group = stratifyByAgeGroup,
      calendar_year = stratifyByCalendarYear,
      personYears = minPersonYears,
      ir_table = dataSource$prefixTable("incidence_rate"),
      cc_table = dataSource$prefixTable("cohort_count"),
      database_table = dataSource$databaseTableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()

  data <- data %>%
    dplyr::mutate(
      gender = dplyr::na_if(.data$gender, ""),
      ageGroup = dplyr::na_if(.data$ageGroup, ""),
      calendarYear = dplyr::na_if(.data$calendarYear, "")
    ) %>%
    dplyr::mutate(calendarYear = as.integer(.data$calendarYear)) %>%
    dplyr::arrange(.data$cohortId, .data$databaseId)


  if (!is.na(minSubjectCount)) {
    data <- data %>%
      dplyr::filter(.data$cohortSubjects > !!minSubjectCount)
  }

  return(data)
}

# modeId = 0 -- Events
# modeId = 1 -- Persons
getInclusionRuleStats <- function(dataSource,
                                  cohortIds = NULL,
                                  databaseIds,
                                  modeId = 1) {
  sql <- "SELECT *
    FROM  @resultsDatabaseSchema.@table_name
    WHERE database_id in (@database_id)
    {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)}
    ;"

  inclusion <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sql,
      resultsDatabaseSchema = dataSource$resultsDatabaseSchema,
      cohort_ids = cohortIds,
      database_id = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("cohort_inclusion"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()

  inclusionResults <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sql,
      resultsDatabaseSchema = dataSource$resultsDatabaseSchema,
      cohort_ids = cohortIds,
      database_id = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("cohort_inc_result"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()

  inclusionStats <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sql,
      resultsDatabaseSchema = dataSource$resultsDatabaseSchema,
      cohort_ids = cohortIds,
      database_id = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("cohort_inc_stats"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()


  if (!hasData(inclusion) || !hasData(inclusionStats)) {
    return(NULL)
  }

  result <- inclusion %>%
    dplyr::select(.data$cohortId, .data$databaseId, .data$ruleSequence, .data$name) %>%
    dplyr::distinct() %>%
    dplyr::left_join(
      inclusionStats %>%
        dplyr::filter(.data$modeId == !!modeId) %>%
        dplyr::select(
          .data$cohortId,
          .data$databaseId,
          .data$ruleSequence,
          .data$personCount,
          .data$gainCount,
          .data$personTotal
        ),
      by = c("cohortId", "databaseId", "ruleSequence")
    ) %>%
    dplyr::arrange(.data$cohortId,
                   .data$databaseId,
                   .data$ruleSequence) %>%
    dplyr::mutate(remain = 0)

  inclusionResults <- inclusionResults %>%
    dplyr::filter(.data$modeId == !!modeId)

  combis <- result %>%
    dplyr::select(.data$cohortId,
                  .data$databaseId) %>%
    dplyr::distinct()

  resultFinal <- c()
  for (j in (1:nrow(combis))) {
    combi <- combis[j,]
    data <- result %>%
      dplyr::inner_join(combi,
                        by = c("cohortId", "databaseId"))

    inclusionResult <- inclusionResults %>%
      dplyr::inner_join(combi,
                        by = c("cohortId", "databaseId"))
    mask <- 0
    for (ruleId in (0:(nrow(data) - 1))) {
      mask <- bitwOr(mask, 2^ruleId) #bitwise OR operation: if both are 0, then 0; else 1
      idx <-
        bitwAnd(inclusionResult$inclusionRuleMask, mask) == mask
      data$remain[data$ruleSequence == ruleId] <-
        sum(inclusionResult$personCount[idx])
    }
    resultFinal[[j]] <- data
  }
  resultFinal <- dplyr::bind_rows(resultFinal) %>%
    dplyr::rename(
      "meetSubjects" = .data$personCount,
      "gainSubjects" = .data$gainCount,
      "remainSubjects" = .data$remain,
      "totalSubjects" = .data$personTotal,
      "ruleName" = .data$name,
      "ruleSequenceId" = .data$ruleSequence
    ) %>%
    dplyr::select(
      .data$cohortId,
      .data$ruleSequenceId,
      .data$ruleName,
      .data$meetSubjects,
      .data$gainSubjects,
      .data$remainSubjects,
      .data$totalSubjects,
      .data$databaseId
    )
  return(resultFinal)
}


getIndexEventBreakdown <- function(dataSource,
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

  sql <- "SELECT index_event_breakdown.*,
              concept.concept_name,
              concept.domain_id,
              concept.vocabulary_id,
              concept.standard_concept,
              concept.concept_code
            FROM  @results_database_schema.@table_name index_event_breakdown
            INNER JOIN  @vocabulary_database_schema.@concept_table concept
              ON index_event_breakdown.concept_id = concept.concept_id
            WHERE database_id in (@database_id)
              AND cohort_id in (@cohort_ids);"
  data <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sql,
      results_database_schema = dataSource$resultsDatabaseSchema,
      vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
      cohort_ids = cohortIds,
      database_id = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("index_event_breakdown"),
      concept_table = dataSource$prefixVocabTable("concept"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()


  data <- data %>%
    dplyr::inner_join(cohortCount,
                      by = c("databaseId", "cohortId")
    ) %>%
    dplyr::mutate(
      subjectPercent = .data$subjectCount / .data$cohortSubjects,
      conceptPercent = .data$conceptCount / .data$cohortEntries
    )

  return(data)
}

getVisitContextResults <- function(dataSource,
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

  sql <- "SELECT visit_context.*,
              standard_concept.concept_name AS visit_concept_name
            FROM  @results_database_schema.@table_name visit_context
            INNER JOIN  @vocabulary_database_schema.@concept_table standard_concept
              ON visit_context.visit_concept_id = standard_concept.concept_id
            WHERE database_id in (@database_id)
              AND cohort_id in (@cohort_ids);"
  data <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sql,
      results_database_schema = dataSource$resultsDatabaseSchema,
      vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
      cohort_ids = cohortIds,
      database_id = quoteLiterals(databaseIds),
      table_name = dataSource$prefixTable("visit_context"),
      concept_table = dataSource$prefixVocabTable("concept"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()

  data <- data %>%
    dplyr::inner_join(cohortCount,
                      by = c("cohortId", "databaseId")
    ) %>%
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
            	SELECT isc.database_id,
            		isc.cohort_id,
            		isc.concept_id,
            		0 source_concept_id,
            		max(concept_subjects) concept_subjects,
            		sum(concept_count) concept_count
            	FROM @results_database_schema.@table_name isc
            	WHERE isc.cohort_id = @cohort_id
            		AND isc.database_id IN (@database_ids)
            	GROUP BY isc.database_id,
            		isc.cohort_id,
            		isc.concept_id

            	UNION

            	SELECT c.database_id,
            		c.cohort_id,
            		c.source_concept_id as concept_id,
            		1 source_concept_id,
            		max(c.concept_subjects) concept_subjects,
            		sum(c.concept_count) concept_count
            	FROM @results_database_schema.@table_name c
            	WHERE c.cohort_id = @cohort_id
            		AND c.database_id IN (@database_ids)
            	GROUP BY
            	    c.database_id,
            		c.cohort_id,
            		c.source_concept_id
            	) concepts
            INNER JOIN @results_database_schema.@concept_table c ON concepts.concept_id = c.concept_id
            WHERE c.invalid_reason IS NULL;"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        dbms = dataSource$dbms,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        cohort_id = cohortId,
        database_ids = quoteLiterals(databaseIds),
        table_name = dataSource$prefixTable("included_source_concept"),
        concept_table = dataSource$prefixTable("concept"),
        snakeCaseToCamelCase = TRUE
      ) %>%
        tidyr::tibble()
    return(data)
  }


getCountForConceptIdInCohort <-
  function(dataSource,
           cohortId,
           databaseIds) {
    sql <- "SELECT ics.*
            FROM  @results_database_schema.@table_name ics
            WHERE ics.cohort_id = @cohort_id
             AND database_id in (@database_ids);"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        dbms = dataSource$dbms,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        cohort_id = cohortId,
        database_ids = quoteLiterals(databaseIds),
        table_name = dataSource$prefixTable("included_source_concept"),
        snakeCaseToCamelCase = TRUE
      ) %>%
        tidyr::tibble()

    standardConceptId <- data %>%
      dplyr::select(
        .data$databaseId,
        .data$conceptId,
        .data$conceptSubjects,
        .data$conceptCount
      ) %>%
      dplyr::group_by(
        .data$databaseId,
        .data$conceptId
      ) %>%
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
      dplyr::group_by(
        .data$databaseId,
        .data$conceptId
      ) %>%
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
                                   databaseIds,
                                   cohortId,
                                   conceptSetId = NULL) {
  sql <- "SELECT oc.*,
              cs.concept_set_name,
              c.concept_name,
              c.vocabulary_id,
              c.concept_code,
              c.standard_concept
            FROM  @results_database_schema.@orphan_table_name oc
            INNER JOIN  @results_database_schema.@cs_table_name cs
              ON oc.cohort_id = cs.cohort_id
                AND oc.concept_set_id = cs.concept_set_id
            INNER JOIN  @vocabulary_database_schema.@concept_table c
              ON oc.concept_id = c.concept_id
            WHERE oc.cohort_id = @cohort_id
              AND database_id in (@database_ids)
              {@concept_set_id != \"\"} ? { AND oc.concept_set_id IN (@concept_set_id)};"
  data <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sql,
      results_database_schema = dataSource$resultsDatabaseSchema,
      vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
      cohort_id = cohortId,
      database_ids = quoteLiterals(databaseIds),
      orphan_table_name = dataSource$prefixTable("orphan_concept"),
      cs_table_name = dataSource$prefixTable("concept_sets"),
      concept_table = dataSource$prefixVocabTable("concept"),
      concept_set_id = conceptSetId,
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()
  return(data)
}

resolveMappedConceptSetFromVocabularyDatabaseSchema <-
  function(dataSource,
           conceptSets,
           vocabularyDatabaseSchema = "vocabulary") {
    sqlBase <-
      paste(
        "SELECT DISTINCT codeset_id AS concept_set_id, concept.*",
        "FROM (",
        paste(conceptSets$conceptSetSql, collapse = ("\nUNION ALL\n")),
        ") concept_sets",
        sep = "\n"
      )
    sqlResolved <- paste(
      sqlBase,
      "INNER JOIN @vocabulary_database_schema.@concept",
      "  ON concept_sets.concept_id = concept.concept_id;",
      sep = "\n"
    )

    sqlBaseMapped <-
      paste(
        "SELECT DISTINCT codeset_id AS concept_set_id,
                           concept_sets.concept_id AS resolved_concept_id,
                           concept.*",
        "FROM (",
        paste(conceptSets$conceptSetSql, collapse = ("\nUNION ALL\n")),
        ") concept_sets",
        sep = "\n"
      )
    sqlMapped <- paste(
      sqlBaseMapped,
      "INNER JOIN @vocabulary_database_schema.@concept_relationship",
      "  ON concept_sets.concept_id = concept_relationship.concept_id_2",
      "INNER JOIN @vocabulary_database_schema.@concept",
      "  ON concept_relationship.concept_id_1 = concept.concept_id",
      "WHERE relationship_id = 'Maps to'",
      "  AND standard_concept IS NULL;",
      sep = "\n"
    )

    resolved <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        dbms = dataSource$dbms,
        sql = sqlResolved,
        vocabulary_database_schema = vocabularyDatabaseSchema,
        concept = dataSource$prefixVocabTable("concept"),
        snakeCaseToCamelCase = TRUE
      ) %>%
        tidyr::tibble() %>%
        dplyr::select(
          .data$conceptSetId,
          .data$conceptId,
          .data$conceptName,
          .data$domainId,
          .data$vocabularyId,
          .data$conceptClassId,
          .data$standardConcept,
          .data$conceptCode,
          .data$invalidReason
        ) %>%
        dplyr::arrange(.data$conceptId)
    mapped <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        dbms = dataSource$dbms,
        sql = sqlMapped,
        vocabulary_database_schema = vocabularyDatabaseSchema,
        concept = dataSource$prefixVocabTable("concept"),
        concept_relationship = dataSource$prefixVocabTable("concept_relationship"),
        snakeCaseToCamelCase = TRUE
      ) %>%
        tidyr::tibble() %>%
        dplyr::select(
          .data$resolvedConceptId,
          .data$conceptId,
          .data$conceptName,
          .data$domainId,
          .data$vocabularyId,
          .data$conceptClassId,
          .data$standardConcept,
          .data$conceptCode,
          .data$conceptSetId
        ) %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$resolvedConceptId, .data$conceptId)

    data <- list(resolved = resolved, mapped = mapped)
    return(data)
  }


resolvedConceptSet <- function(dataSource,
                               databaseIds,
                               cohortId,
                               conceptSetId = NULL) {
  sqlResolved <- "SELECT DISTINCT rc.cohort_id,
                    	rc.concept_set_id,
                    	c.concept_id,
                    	c.concept_name,
                    	c.domain_id,
                    	c.vocabulary_id,
                    	c.concept_class_id,
                    	c.standard_concept,
                    	c.concept_code,
                    	rc.database_id
                    FROM @results_database_schema.@resolved_concepts_table rc
                    LEFT JOIN @results_database_schema.@concept_table c
                    ON rc.concept_id = c.concept_id
                    WHERE rc.database_id IN (@database_ids)
                    	AND rc.cohort_id = @cohortId
                      {@concept_set_id != \"\"} ? { AND rc.concept_set_id IN (@concept_set_id)}
                    ORDER BY c.concept_id;"
  resolved <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sqlResolved,
      results_database_schema = dataSource$resultsDatabaseSchema,
      database_ids = quoteLiterals(databaseIds),
      cohortId = cohortId,
      concept_set_id = conceptSetId,
      resolved_concepts_table = dataSource$prefixTable("resolved_concepts"),
      concept_table = dataSource$prefixTable("concept"),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble() %>%
      dplyr::arrange(.data$conceptId)

  return(resolved)
}

getMappedStandardConcepts <-
  function(dataSource,
           conceptIds) {
    sql <-
      "SELECT cr.CONCEPT_ID_2 AS SEARCHED_CONCEPT_ID,
          c.*
        FROM @results_database_schema.@concept_relationship cr
        JOIN @results_database_schema.@concept c ON c.concept_id = cr.concept_id_1
        WHERE cr.concept_id_2 IN (@concept_ids)
        	AND cr.INVALID_REASON IS NULL
        	AND relationship_id IN ('Mapped from');"

    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        dbms = dataSource$dbms,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        concept_ids = conceptIds,
        concept = dataSource$prefixTable("concept"),
        concept_relationship = dataSource$prefixTable("concept_relationship"),
        snakeCaseToCamelCase = TRUE
      ) %>%
        tidyr::tibble()

    return(data)
  }


getMappedSourceConcepts <-
  function(dataSource,
           conceptIds) {
    sql <-
      "
      SELECT cr.CONCEPT_ID_2 AS SEARCHED_CONCEPT_ID,
        c.*
      FROM @results_database_schema.@concept_relationship cr
      JOIN @results_database_schema.@concept c ON c.concept_id = cr.concept_id_1
      WHERE cr.concept_id_2 IN (@concept_ids)
      	AND cr.INVALID_REASON IS NULL
      	AND relationship_id IN ('Maps to');"

    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        dbms = dataSource$dbms,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        concept_ids = conceptIds,
        concept = dataSource$prefixTable("concept"),
        concept_relationship = dataSource$prefixTable("concept_relationship"),
        snakeCaseToCamelCase = TRUE
      ) %>%
        tidyr::tibble()

    return(data)
  }


mappedConceptSet <- function(dataSource,
                             databaseIds,
                             cohortId) {
  sqlMapped <-
    "WITH resolved_concepts_mapped
    AS (
    	SELECT concept_sets.concept_id AS resolved_concept_id,
    		c1.concept_id,
    		c1.concept_name,
    		c1.domain_id,
    		c1.vocabulary_id,
    		c1.concept_class_id,
    		c1.standard_concept,
    		c1.concept_code
    	FROM (
    		SELECT DISTINCT concept_id
    		FROM @results_database_schema.@resolved_concepts
    		WHERE database_id IN (@databaseIds)
    			AND cohort_id = @cohort_id
    		) concept_sets
    	INNER JOIN @results_database_schema.@concept_relationship cr ON concept_sets.concept_id = cr.concept_id_2
    	INNER JOIN @results_database_schema.@concept c1 ON cr.concept_id_1 = c1.concept_id
    	WHERE relationship_id = 'Maps to'
    		AND standard_concept IS NULL
    	)
    SELECT
        c.database_id,
    	c.cohort_id,
    	c.concept_set_id,
    	mapped.*
    FROM (SELECT DISTINCT concept_id, database_id, cohort_id, concept_set_id FROM @results_database_schema.@resolved_concepts) c
    INNER JOIN resolved_concepts_mapped mapped ON c.concept_id = mapped.resolved_concept_id
    {@cohort_id != ''} ? { WHERE c.cohort_id = @cohort_id};
    "
  mapped <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sqlMapped,
      results_database_schema = dataSource$resultsDatabaseSchema,
      databaseIds = quoteLiterals(databaseIds),
      concept = dataSource$prefixTable("concept"),
      concept_relationship = dataSource$prefixTable("concept_relationship"),
      resolved_concepts = dataSource$prefixTable("resolved_concepts"),
      cohort_id = cohortId,
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble() %>%
      dplyr::arrange(.data$resolvedConceptId)
  return(mapped)
}


getDatabaseCounts <- function(dataSource,
                              databaseIds) {
  sql <- "SELECT *
              FROM  @results_database_schema.@database_table
              WHERE database_id in (@database_ids);"
  data <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sql,
      results_database_schema = dataSource$resultsDatabaseSchema,
      database_ids = quoteLiterals(databaseIds),
      database_table = dataSource$databaseTableName,
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()

  return(data)
}

getMetaDataResults <- function(dataSource, databaseId) {
  sql <- "SELECT *
              FROM  @results_database_schema.@metadata
              WHERE database_id = @database_id;"

  data <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = sql,
      metadata = dataSource$prefixTable("metadata"),
      results_database_schema = dataSource$resultsDatabaseSchema,
      database_id = quoteLiterals(databaseId),
      snakeCaseToCamelCase = TRUE
    ) %>%
      tidyr::tibble()

  return(data)
}


getExecutionMetadata <- function(dataSource, databaseId) {
  databaseMetadata <-
    getMetaDataResults(dataSource, databaseId)

  if (!hasData(databaseMetadata)) {
    return(NULL)
  }
  columnNames <-
    databaseMetadata$variableField %>%
      unique() %>%
      sort()
  columnNamesNoJson <-
    columnNames[stringr::str_detect(
      string = tolower(columnNames),
      pattern = "json",
      negate = TRUE
    )]
  columnNamesJson <-
    columnNames[stringr::str_detect(
      string = tolower(columnNames),
      pattern = "json",
      negate = FALSE
    )]

  transposeNonJsons <- databaseMetadata %>%
    dplyr::filter(.data$variableField %in% c(columnNamesNoJson)) %>%
    dplyr::rename(name = "variableField") %>%
    dplyr::group_by(.data$databaseId, .data$startTime, .data$name) %>%
    dplyr::summarise(
      valueField = max(.data$valueField),
      .groups = "keep"
    ) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(
      names_from = .data$name,
      values_from = .data$valueField
    ) %>%
    dplyr::mutate(startTime = stringr::str_replace(
      string = .data$startTime,
      pattern = "TM_",
      replacement = ""
    ))

  transposeNonJsons$startTime <-
    transposeNonJsons$startTime %>% lubridate::as_datetime()

  transposeJsons <- databaseMetadata %>%
    dplyr::filter(.data$variableField %in% c(columnNamesJson)) %>%
    dplyr::rename(name = "variableField") %>%
    dplyr::group_by(.data$databaseId, .data$startTime, .data$name) %>%
    dplyr::summarise(
      valueField = max(.data$valueField),
      .groups = "keep"
    ) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(
      names_from = .data$name,
      values_from = .data$valueField
    ) %>%
    dplyr::mutate(startTime = stringr::str_replace(
      string = .data$startTime,
      pattern = "TM_",
      replacement = ""
    ))

  transposeJsons$startTime <-
    transposeJsons$startTime %>% lubridate::as_datetime()

  transposeJsonsTemp <- list()
  for (i in (1:nrow(transposeJsons))) {
    transposeJsonsTemp[[i]] <- transposeJsons[i,]
    for (j in (1:length(columnNamesJson))) {
      transposeJsonsTemp[[i]][[columnNamesJson[[j]]]] <-
        transposeJsonsTemp[[i]][[columnNamesJson[[j]]]] %>%
          RJSONIO::fromJSON(digits = 23) %>%
          RJSONIO::toJSON(digits = 23, pretty = TRUE)
    }
  }
  transposeJsons <- dplyr::bind_rows(transposeJsonsTemp)
  data <- transposeNonJsons %>%
    dplyr::left_join(transposeJsons,
                     by = c("databaseId", "startTime")
    )
  if ("observationPeriodMaxDate" %in% colnames(data)) {
    data$observationPeriodMaxDate <-
      tryCatch(
        expr = lubridate::as_date(data$observationPeriodMaxDate),
        error = data$observationPeriodMaxDate
      )
  }
  if ("observationPeriodMinDate" %in% colnames(data)) {
    data$observationPeriodMinDate <-
      tryCatch(
        expr = lubridate::as_date(data$observationPeriodMinDate),
        error = data$observationPeriodMinDate
      )
  }
  if ("sourceReleaseDate" %in% colnames(data)) {
    data$sourceReleaseDate <-
      tryCatch(
        expr = lubridate::as_date(data$sourceReleaseDate),
        error = data$sourceReleaseDate
      )
  }
  if ("personDaysInDatasource" %in% colnames(data)) {
    data$personDaysInDatasource <-
      tryCatch(
        expr = as.numeric(data$personDaysInDatasource),
        error = data$personDaysInDatasource
      )
  }
  if ("recordsInDatasource" %in% colnames(data)) {
    data$recordsInDatasource <-
      tryCatch(
        expr = as.numeric(data$recordsInDatasource),
        error = data$recordsInDatasource
      )
  }
  if ("personDaysInDatasource" %in% colnames(data)) {
    data$personDaysInDatasource <-
      tryCatch(
        expr = as.numeric(data$personDaysInDatasource),
        error = data$personDaysInDatasource
      )
  }
  if ("runTime" %in% colnames(data)) {
    data$runTime <-
      tryCatch(
        expr = round(as.numeric(data$runTime), digits = 1),
        error = data$runTime
      )
  }
  return(data)
}
