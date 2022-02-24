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





#' Get domain information
#'
#' @param packageName e.g. 'CohortDiagnostics'
#'
#' @return
#' A list with two tibble data frame objects with domain information represented in wide and long format respectively.
getDomainInformation <- function() {
  ParallelLogger::logTrace("  - Reading domains.csv")
  pathToCsv <-
    system.file("csv",
                "domains.csv",
                package = utils::packageName())
  if (!pathToCsv == "") {
    domains <-
      readr::read_csv(
        file = pathToCsv,
        guess_max = min(1e7),
        col_types = readr::cols()
      )
  } else {
    stop(paste0("domains.csv was not found in installed package: ",
                packageName))
  }
  
  domains <- domains %>%
    .replaceNaInDataFrameWithEmptyString() %>%
    dplyr::mutate(domainTableShort = stringr::str_sub(
      string = toupper(.data$domain),
      start = 1,
      end = 2
    )) %>%
    dplyr::mutate(
      domainTableShort = dplyr::case_when(
        stringr::str_detect(string = tolower(.data$domain), pattern = 'era') ~ paste0(.data$domainTableShort, 'E'),
        TRUE ~ .data$domainTableShort
      )
    )
  
  domains$domainConceptIdShort <-
    stringr::str_replace_all(
      string = sapply(
        stringr::str_extract_all(
          string = SqlRender::camelCaseToTitleCase(SqlRender::snakeCaseToCamelCase(domains$domainConceptId)),
          pattern = '[A-Z]'
        ),
        paste,
        collapse = ' '
      ),
      pattern = " ",
      replacement = ""
    )
  domains$domainSourceConceptIdShort <-
    stringr::str_replace_all(
      string = sapply(
        stringr::str_extract_all(
          string = SqlRender::camelCaseToTitleCase(SqlRender::snakeCaseToCamelCase(domains$domainSourceConceptId)),
          pattern = '[A-Z]'
        ),
        paste,
        collapse = ' '
      ),
      pattern = " ",
      replacement = ""
    )
  domains <- domains  %>%
    dplyr::mutate(isEraTable = stringr::str_detect(string = .data$domainTable,
                                                   pattern = 'era'))
  data <- list()
  data$wide <- domains
  data$long <- dplyr::bind_rows(
    data$wide %>%
      dplyr::select(
        .data$domainTableShort,
        .data$domainTable,
        .data$domainConceptIdShort,
        .data$domainConceptId
      ) %>%
      dplyr::rename(
        domainFieldShort = .data$domainConceptIdShort,
        domainField = .data$domainConceptId
      ),
    data$wide %>%
      dplyr::select(
        .data$domainTableShort,
        .data$domainSourceConceptIdShort,
        .data$domainTable,
        .data$domainSourceConceptId
      ) %>%
      dplyr::rename(
        domainFieldShort = .data$domainSourceConceptIdShort,
        domainField = .data$domainSourceConceptId
      )
  ) %>%
    dplyr::distinct() %>%
    dplyr::filter(.data$domainFieldShort != "") %>%
    dplyr::mutate(eraTable = stringr::str_detect(string = .data$domainTable,
                                                 pattern = 'era')) %>%
    dplyr::mutate(isSourceField = stringr::str_detect(string = .data$domainField,
                                                      pattern = 'source'))
  return(data)
}

.replaceNaInDataFrameWithEmptyString <- function(data) {
  #https://github.com/r-lib/tidyselect/issues/201
  data <- 
  data %>%
    dplyr::collect() %>%
    dplyr::mutate(dplyr::across(
      tidyselect:::where(is.character),
      ~ tidyr::replace_na(.x, as.character(''))
    )) %>%
    dplyr::mutate(dplyr::across(
      tidyselect:::where(is.logical),
      ~ tidyr::replace_na(.x, as.character(''))
    )) %>%
    dplyr::mutate(dplyr::across(
      tidyselect:::where(is.numeric),
      ~ tidyr::replace_na(.x, as.numeric(''))
    ))
  return(data)
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

