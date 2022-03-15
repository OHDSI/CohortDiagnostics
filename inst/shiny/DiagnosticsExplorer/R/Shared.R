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

camelCaseToSnakeCase <- function(string) {
  string <- gsub("([A-Z])", "_\\1", string)
  string <- tolower(string)
  string <- gsub("([a-z])([0-9])", "\\1_\\2", string)
  return(string)
}

snakeCaseToCamelCase <- function(string) {
  string <- tolower(string)
  for (letter in letters) {
    string <-
      gsub(paste("_", letter, sep = ""), toupper(letter), string)
  }
  string <- gsub("_([0-9])", "\\1", string)
  return(string)
}

# private function - not exported
camelCaseToSnakeCase <- function(string) {
  string <- gsub("([A-Z])", "_\\1", string)
  string <- tolower(string)
  string <- gsub("([a-z])([0-9])", "\\1_\\2", string)
  return(string)
}

is.date <- function(x) {
  inherits(x, c("Date", "POSIXt"))
}

# private function - not exported
titleCaseToCamelCase <- function(string) {
  string <- stringr::str_replace_all(string = string,
                                     pattern = ' ',
                                     replacement = '')
  substr(string, 1, 1) <- tolower(substr(string, 1, 1))
  return(string)
}

# private function - not exported
quoteLiterals <- function(x) {
  if (is.null(x)) {
    return("")
  } else {
    return(paste0("'", paste(x, collapse = "', '"), "'"))
  }
}

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
#' @param cohortDefinition An R object (list) with a list representation of the cohort definition expression.
#'
#' @param cohortName Name for the cohort definition
#'
#' @param embedText Any additional text to embed in the top of circe generated content.
#'
#' @param includeConceptSets Do you want to inclued concept set in the documentation
#'
#' @return list object
#'
#' @export
getCirceRenderedExpression <- function(cohortDefinition,
                                       cohortName = NULL,
                                       embedText = NULL,
                                       includeConceptSets = FALSE) {
  cohortJson <-
    RJSONIO::toJSON(x = cohortDefinition,
                    digits = 23,
                    pretty = TRUE)
  circeExpression <-
    CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
  circeExpressionMarkdown <-
    CirceR::cohortPrintFriendly(circeExpression)
  circeConceptSetListmarkdown <-
    CirceR::conceptSetListPrintFriendly(circeExpression$conceptSets)
  
  if (hasData(embedText)) {
    circeExpressionMarkdown <-
      paste0(
        "##### ",
        embedText,
        "\r\n\r\n",
        "# Cohort Definition:",
        "\r\n\r\n",
        "### ",
        cohortName,
        "\r\n\r\n",
        circeExpressionMarkdown
      )
  }
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
    if ('CONCEPT_ID' %in% colnames(conceptSetExpressionDetails)) {
      if ('isExcluded' %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(IS_EXCLUDED = .data$isExcluded)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(IS_EXCLUDED = FALSE)
      }
      if ('includeDescendants' %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(INCLUDE_DESCENDANTS = .data$includeDescendants)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(INCLUDE_DESCENDANTS = FALSE)
      }
      if ('includeMapped' %in% colnames(conceptSetExpressionDetails)) {
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
      dplyr::mutate(json = RJSONIO::toJSON(x = .data$expression,
                                           pretty = TRUE))
    
    conceptSetExpressionDetails <- list()
    i <- 0
    for (id in conceptSetExpression$id) {
      i <- i + 1
      conceptSetExpressionDetails[[i]] <-
        getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression =
                                                         conceptSetExpression[i,]$expression$items) %>%
        dplyr::mutate(id = conceptSetExpression[i, ]$id) %>%
        dplyr::relocate(.data$id) %>%
        dplyr::arrange(.data$id)
    }
    conceptSetExpressionDetails <-
      dplyr::bind_rows(conceptSetExpressionDetails)
    output <- list(conceptSetExpression = conceptSetExpression,
                   conceptSetExpressionDetails = conceptSetExpressionDetails)
    return(output)
  }