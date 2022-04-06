# Copyright 2022 Observational Health Data Sciences and Informatics
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

hasData <- function(data) {
  if (is.null(data)) {
    return(FALSE)
  }
  if (is.data.frame(data)) {
    if (nrow(data) == 0) {
      return(FALSE)
    }
  }
  if (!is.data.frame(data)) {
    if (length(data) == 0) {
      return(FALSE)
    }
    if (length(data) == 1) {
      if (is.na(data)) {
        return(FALSE)
      }
      if (data == "") {
        return(FALSE)
      }
    }
  }
  return(TRUE)
}

#' Returns list with circe generated documentation
#'
#' @description
#' Returns list with circe generated documentation
#'
#' @param cohortDefinition An R object (list) with a list representation of the cohort definition expression,
#'                          that may be converted to a cohort expression JSON using
#'                          RJSONIO::toJSON(x = cohortDefinition, digits = 23, pretty = TRUE)
#'
#' @param cohortName Name for the cohort definition
#'
#' @param includeConceptSets Do you want to inclued concept set in the documentation
#'
#' @return list object
#'
#' @export
getCirceRenderedExpression <- function(cohortDefinition,
                                       cohortName = "Cohort Definition",
                                       includeConceptSets = FALSE) {
  cohortJson <-
    RJSONIO::toJSON(
      x = cohortDefinition,
      digits = 23,
      pretty = TRUE
    )
  circeExpression <-
    CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
  circeExpressionMarkdown <-
    CirceR::cohortPrintFriendly(circeExpression)
  circeConceptSetListmarkdown <-
    CirceR::conceptSetListPrintFriendly(circeExpression$conceptSets)

  circeExpressionMarkdown <-
    paste0(
      "## Human Readable Cohort Definition",
      "\r\n\r\n",
      circeExpressionMarkdown
    )

  circeExpressionMarkdown <-
    paste0(
      "# ",
      cohortName,
      "\r\n\r\n",
      circeExpressionMarkdown
    )

  if (includeConceptSets) {
    circeExpressionMarkdown <-
      paste0(
        circeExpressionMarkdown,
        "\r\n\r\n",
        "\r\n\r\n",
        "## Concept Sets:",
        "\r\n\r\n",
        circeConceptSetListmarkdown
      )
  }

  htmlExpressionCohort <-
    convertMdToHtml(circeExpressionMarkdown)
  htmlExpressionConceptSetExpression <-
    convertMdToHtml(circeConceptSetListmarkdown)
  return(
    list(
      cohortJson = cohortJson,
      cohortMarkdown = circeExpressionMarkdown,
      conceptSetMarkdown = circeConceptSetListmarkdown,
      cohortHtmlExpression = htmlExpressionCohort,
      conceptSetHtmlExpression = htmlExpressionConceptSetExpression
    )
  )
}


getConceptSetDataFrameFromConceptSetExpression <-
  function(conceptSetExpression) {
    if ("items" %in% names(conceptSetExpression)) {
      items <- conceptSetExpression$items
    } else {
      items <- conceptSetExpression
    }
    conceptSetExpressionDetails <- items %>%
      purrr::map_df(.f = purrr::flatten)
    if ("CONCEPT_ID" %in% colnames(conceptSetExpressionDetails)) {
      if ("isExcluded" %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(IS_EXCLUDED = .data$isExcluded)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(IS_EXCLUDED = FALSE)
      }
      if ("includeDescendants" %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(INCLUDE_DESCENDANTS = .data$includeDescendants)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(INCLUDE_DESCENDANTS = FALSE)
      }
      if ("includeMapped" %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(INCLUDE_MAPPED = .data$includeMapped)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(INCLUDE_MAPPED = FALSE)
      }
      conceptSetExpressionDetails <-
        conceptSetExpressionDetails %>%
        tidyr::replace_na(list(
          IS_EXCLUDED = FALSE,
          INCLUDE_DESCENDANTS = FALSE,
          INCLUDE_MAPPED = FALSE
        ))
      colnames(conceptSetExpressionDetails) <-
        SqlRender::snakeCaseToCamelCase(colnames(conceptSetExpressionDetails))
    }
    return(conceptSetExpressionDetails)
  }


getConceptSetDetailsFromCohortDefinition <-
  function(cohortDefinitionExpression) {
    if ("expression" %in% names(cohortDefinitionExpression)) {
      expression <- cohortDefinitionExpression$expression
    } else {
      expression <- cohortDefinitionExpression
    }

    if (is.null(expression$ConceptSets)) {
      return(NULL)
    }

    conceptSetExpression <- expression$ConceptSets %>%
      dplyr::bind_rows() %>%
      dplyr::mutate(json = RJSONIO::toJSON(
        x = .data$expression,
        pretty = TRUE
      ))

    conceptSetExpressionDetails <- list()
    i <- 0
    for (id in conceptSetExpression$id) {
      i <- i + 1
      conceptSetExpressionDetails[[i]] <-
        getConceptSetDataFrameFromConceptSetExpression(
          conceptSetExpression =
            conceptSetExpression[i, ]$expression$items
        ) %>%
        dplyr::mutate(id = conceptSetExpression[i, ]$id) %>%
        dplyr::relocate(.data$id) %>%
        dplyr::arrange(.data$id)
    }
    conceptSetExpressionDetails <-
      dplyr::bind_rows(conceptSetExpressionDetails)
    output <- list(
      conceptSetExpression = conceptSetExpression,
      conceptSetExpressionDetails = conceptSetExpressionDetails
    )
    return(output)
  }


quoteLiterals <- function(x) {
  if (is.null(x)) {
    return("")
  } else {
    return(paste0("'", paste(x, collapse = "', '"), "'"))
  }
}

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


checkIfObjectIsTrue <- function(object) {
  if (is.null(object)) {
    return(FALSE)
  }
  if (!isTRUE(object)) {
    return(FALSE)
  }
  return(TRUE)
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

convertMdToHtml <- function(markdown) {
  markdown <- gsub("'", "%sq%", markdown)
  mdFile <- tempfile(fileext = ".md")
  htmlFile <- tempfile(fileext = ".html")
  SqlRender::writeSql(markdown, mdFile)
  rmarkdown::render(
    input = mdFile,
    output_format = "html_fragment",
    output_file = htmlFile,
    clean = TRUE,
    quiet = TRUE
  )
  html <- SqlRender::readSql(htmlFile)
  unlink(mdFile)
  unlink(htmlFile)
  html <- gsub("%sq%", "'", html)

  return(html)
}

checkErrorCohortIdsDatabaseIds <- function(errorMessage,
                                           cohortIds,
                                           databaseIds) {
  checkmate::assertDouble(
    x = cohortIds,
    null.ok = FALSE,
    lower = 1,
    upper = 2^53,
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
