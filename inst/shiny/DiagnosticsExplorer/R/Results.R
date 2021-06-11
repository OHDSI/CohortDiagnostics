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
