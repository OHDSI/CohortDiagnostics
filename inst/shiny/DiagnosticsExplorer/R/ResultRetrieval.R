

renderTranslateQuerySql <-
  function(connection,
           sql,
           dbms,
           ...,
           snakeCaseToCamelCase = FALSE) {
    if (is(connection, "Pool")) {
      sql <- SqlRender::render(sql, ...)
      sql <- SqlRender::translate(sql, targetDialect = dbms)
      
      tryCatch(
        {
          data <- DatabaseConnector::dbGetQuery(connection, sql)
        },
        error = function(err) {
          writeLines(sql)
          stop(err)
        }
      )
      if (snakeCaseToCamelCase) {
        colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
      }
      return(data)
    } else {
      return(
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          ...,
          snakeCaseToCamelCase = snakeCaseToCamelCase
        )
      )
    }
  }


queryResultCovariateValue <- function(dataSource,
                                      cohortIds,
                                      analysisIds = NULL,
                                      databaseIds,
                                      startDay = NULL,
                                      endDay = NULL,
                                      temporalCovariateValue = TRUE,
                                      temporalCovariateValueDist = TRUE) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  errorMessage <-
    checkErrorCohortIdsDatabaseIds(
      cohortIds = cohortIds,
      databaseIds = databaseIds,
      errorMessage = errorMessage
    )
  checkmate::assertIntegerish(
    x = startDay,
    any.missing = TRUE,
    unique = FALSE,
    null.ok = TRUE,
    add = errorMessage
  )
  checkmate::assertIntegerish(
    x = endDay,
    any.missing = TRUE,
    unique = FALSE,
    null.ok = TRUE,
    add = errorMessage
  )
  
  temporalTimeRefData <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = "SELECT *
             FROM @results_database_schema.temporal_time_ref
             WHERE time_id IS NOT NULL
              {@start_day != \"\"} ? { AND start_day IN (@start_day)}
              {@end_day != \"\"} ? { AND AND end_day IN (@end_day)};",
      snakeCaseToCamelCase = TRUE,
      results_database_schema = dataSource$resultsDatabaseSchema,
      start_day = startDay,
      end_day = endDay
    ) %>%
    dplyr::tibble()
  
  temporalAnalysisRefData <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = "SELECT *
             FROM @results_database_schema.temporal_analysis_ref
              WHERE analysis_id IS NOT NULL
                {@analysis_ids != \"\"} ? { AND analysis_id IN (@analysis_ids)}
              ;",
      analysis_ids = analysisIds,
      snakeCaseToCamelCase = TRUE,
      results_database_schema = dataSource$resultsDatabaseSchema
    ) %>%
    dplyr::tibble()
  
  temporalCovariateRefData <-
    renderTranslateQuerySql(
      connection = dataSource$connection,
      dbms = dataSource$dbms,
      sql = "SELECT *
             FROM @results_database_schema.temporal_covariate_ref
              WHERE covariate_id IS NOT NULL
                {@analysis_ids != \"\"} ? { AND analysis_id IN (@analysis_ids)};",
      snakeCaseToCamelCase = TRUE,
      analysis_ids = analysisIds,
      results_database_schema = dataSource$resultsDatabaseSchema
    ) %>%
    dplyr::tibble()
  
  temporalCovariateValueData <- NULL
  if (temporalCovariateValue) {
    temporalCovariateValueData <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        dbms = dataSource$dbms,
        sql = "SELECT *
                FROM @results_database_schema.temporal_covariate_value
                WHERE covariate_id IN (
                                        SELECT DISTINCT covariate_id
                                        FROM @results_database_schema.temporal_covariate_ref
                                        WHERE covariate_id IS NOT NULL
                                          {@analysis_ids != \"\"} ? { AND analysis_id IN (@analysis_ids)}
                                      )
                {@cohort_id != \"\"} ? { AND cohort_id IN (@cohort_id)}
                {@time_id != \"\"} ? { AND (time_id IN (@time_id) OR time_id IS NULL)}
                {@database_id != \"\"} ? { AND database_id IN (@database_id)};",
        snakeCaseToCamelCase = TRUE,
        analysis_ids = analysisIds,
        time_id = temporalTimeRefData$timeId %>% unique(),
        database_id = quoteLiterals(databaseIds),
        cohort_id = cohortIds,
        results_database_schema = dataSource$resultsDatabaseSchema
      ) %>%
      dplyr::tibble()
  }
  
  temporalCovariateValueDistData <- NULL
  if (temporalCovariateValueDist) {
    temporalCovariateValueDistData <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        dbms = dataSource$dbms,
        sql = "SELECT *
             FROM @results_database_schema.temporal_covariate_value_dist
              WHERE covariate_id IS NOT NULL
        {@covariate_id != \"\"} ? { AND covariate_id IN (@covariate_id)}
                {@cohort_id != \"\"} ? { AND cohort_id IN (@cohort_id)}
                {@time_id != \"\"} ? { AND (time_id IN (@time_id) OR time_id IS NULL)}
                {@database_id != \"\"} ? { AND database_id IN (@database_id)};",
        snakeCaseToCamelCase = TRUE,
        covariate_id = temporalCovariateRefData$covariateId %>% unique(),
        time_id = temporalTimeRefData$timeId %>% unique(),
        database_id = quoteLiterals(databaseIds),
        cohort_id = cohortIds,
        results_database_schema = dataSource$resultsDatabaseSchema
      ) %>%
      dplyr::tibble()
  }
  
  data <- list(
    temporalTimeRef = temporalTimeRefData,
    temporalAnalysisRef = temporalAnalysisRefData,
    temporalCovariateRef = temporalCovariateRefData,
    temporalCovariateValue = temporalCovariateValueData,
    temporalCovariateValueDist = temporalCovariateValueDistData
  )
  return(data)
}


getCovariateValueResult <- function(dataSource,
                                    cohortIds,
                                    analysisIds = NULL,
                                    databaseIds,
                                    startDay = NULL,
                                    endDay = NULL,
                                    temporalCovariateValue = TRUE,
                                    temporalCovariateValueDist = TRUE) {
  data <- queryResultCovariateValue(
    dataSource = dataSource,
    cohortIds = cohortIds,
    analysisIds = analysisIds,
    databaseIds = databaseIds,
    startDay = startDay,
    endDay = endDay,
    temporalCovariateValue = temporalCovariateValue,
    temporalCovariateValueDist = temporalCovariateValueDist
  )
  
  resultCovariateValue <- NULL
  if ("temporalCovariateValue" %in% names(data) &&
      hasData(data$temporalCovariateValue)) {
    resultCovariateValue <- data$temporalCovariateValue %>%
      dplyr::arrange(
        .data$cohortId,
        .data$databaseId,
        .data$timeId,
        .data$covariateId
      ) %>%
      dplyr::left_join(data$temporalTimeRef,
                       by = "timeId"
      ) %>%
      # dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
      dplyr::inner_join(data$temporalCovariateRef,
                        by = "covariateId"
      ) %>%
      dplyr::inner_join(data$temporalAnalysisRef,
                        by = "analysisId"
      ) %>%
      dplyr::relocate(
        .data$cohortId,
        .data$databaseId,
        .data$timeId,
        .data$startDay,
        .data$endDay,
        # .data$choices,
        .data$analysisId,
        .data$covariateId,
        .data$covariateName,
        .data$isBinary
      )
    
    if ("missingMeansZero" %in% colnames(resultCovariateValue)) {
      resultCovariateValue <- resultCovariateValue %>%
        dplyr::mutate(mean = dplyr::if_else(
          is.na(.data$mean) &
            !is.na(.data$missingMeansZero) &
            .data$missingMeansZero == "Y",
          0,
          .data$mean
        )) %>%
        dplyr::select(-.data$missingMeansZero)
    }
    
    resultCovariateValue <- resultCovariateValue %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("condition_occurrence: "),
          replacement = ""
        )
      ) %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("condition_era: "),
          replacement = ""
        )
      ) %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("condition_era group: "),
          replacement = ""
        )
      ) %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("drug_era: "),
          replacement = ""
        )
      ) %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("drug_era group: "),
          replacement = ""
        )
      ) %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("drug_occurrence: "),
          replacement = ""
        )
      ) %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("procedure_occurrence: "),
          replacement = ""
        )
      )
  }
  
  resultCovariateValueDist <- NULL
  if ("temporalCovariateValueDist" %in% names(data) &&
      hasData(data$temporalCovariateValueDist)) {
    resultCovariateValueDist <- data$temporalCovariateValueDist %>%
      dplyr::arrange(
        .data$cohortId,
        .data$databaseId,
        .data$timeId,
        .data$covariateId
      ) %>%
      dplyr::left_join(data$temporalTimeRef,
                       by = "timeId"
      ) %>%
      # dplyr::mutate(choices = paste0("Start ", .data$startDay, " to end ", .data$endDay)) %>%
      dplyr::inner_join(data$temporalCovariateRef,
                        by = "covariateId"
      ) %>%
      dplyr::inner_join(data$temporalAnalysisRef,
                        by = "analysisId"
      ) %>%
      dplyr::relocate(
        .data$cohortId,
        .data$databaseId,
        .data$timeId,
        .data$startDay,
        .data$endDay,
        .data$analysisId,
        .data$covariateId,
        .data$covariateName,
        .data$isBinary
      )
    if ("missingMeansZero" %in% colnames(resultCovariateValueDist)) {
      resultCovariateValueDist <- resultCovariateValueDist %>%
        dplyr::mutate(mean = dplyr::if_else(
          is.na(.data$mean) &
            !is.na(.data$missingMeansZero) &
            .data$missingMeansZero == "Y",
          0,
          .data$mean
        )) %>%
        dplyr::select(-.data$missingMeansZero)
    }
    resultCovariateValueDist <- resultCovariateValueDist %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("condition_occurrence: "),
          replacement = ""
        )
      ) %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("condition_era: "),
          replacement = ""
        )
      ) %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("drug_era: "),
          replacement = ""
        )
      ) %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("drug_occurrence: "),
          replacement = ""
        )
      ) %>%
      dplyr::mutate(
        covariateName = stringr::str_replace(
          string = .data$covariateName,
          pattern = stringr::fixed("prodcedure_occurrence: "),
          replacement = ""
        )
      )
  }
  return(
    list(
      covariateValue = resultCovariateValue,
      covariateValueDist = resultCovariateValueDist
    )
  )
}

getTimeDistributionResult <- function(dataSource,
                                      cohortIds,
                                      databaseIds) {
  data <- getCovariateValueResult(
    dataSource = dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    analysisIds = c(8, 9, 10),
    temporalCovariateValue = FALSE,
    temporalCovariateValueDist = TRUE
  )
  if (!hasData(data)) {
    return(NULL)
  }
  data <- data$covariateValueDist
  if (!hasData(data)) {
    return(NULL)
  }
  data <- data %>%
    dplyr::rename(
      "timeMetric" = .data$covariateName,
      "averageValue" = .data$mean,
      "standardDeviation" = .data$sd
    ) %>%
    dplyr::select(
      "cohortId",
      "databaseId",
      "timeMetric",
      "averageValue",
      "standardDeviation",
      "minValue",
      "p10Value",
      "p25Value",
      "medianValue",
      "p75Value",
      "p90Value",
      "maxValue"
    )
  return(data)
}
