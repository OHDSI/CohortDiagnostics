# Copyright 2021 Observational Health Data Sciences and Informatics
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

createIfNotExist <-
  function(type,
           name,
           recursive = TRUE,
           errorMessage = NULL) {
    if (is.null(errorMessage) |
        !class(errorMessage) == 'AssertColection') {
      errorMessage <- checkmate::makeAssertCollection()
    }
    if (!is.null(type)) {
      if (length(name) == 0) {
        stop(ParallelLogger::logError("  - Must specify ", name))
      }
      if (type %in% c("folder")) {
        if (!file.exists(gsub("/$", "", name))) {
          dir.create(name, recursive = recursive)
          ParallelLogger::logInfo("  - Created ", type, " at ", name)
        } else {
          # ParallelLogger::logInfo(type, " already exists at ", name)
        }
      }
      checkmate::assertDirectory(x = name,
                                 access = 'x',
                                 add = errorMessage)
    }
    invisible(errorMessage)
  }

enforceMinCellValue <-
  function(data, fieldName, minValues, silent = FALSE) {
    if (nrow(data) == 0) {
      return(data)
    }
    toCensor <-
      !is.na(data[, fieldName]) &
      data[, fieldName] < minValues & data[, fieldName] != 0
    if (!silent) {
      percent <- round(100 * sum(toCensor) / nrow(data), 1)
      ParallelLogger::logTrace(
        "  - Censoring ",
        sum(toCensor),
        " values (",
        percent,
        "%) from ",
        fieldName,
        " because value below minimum"
      )
    }
    if (length(minValues) == 1) {
      data[toCensor, fieldName] <- -minValues
    } else {
      data[toCensor, fieldName] <- -minValues[toCensor]
    }
    return(data)
  }

enforceMinCellValueInDataframe <- function(data,
                                           columnNames = colnames(data),
                                           minCellCount = 5) {
  for (i in (1:length(columnNames))) {
    if (columnNames[[i]] %in% colnames(data)) {
      data <-
        enforceMinCellValue(data = data,
                            fieldName = columnNames[[i]],
                            minValues = minCellCount)
    }
  }
  return(data)
}

naToEmpty <- function(x) {
  x[is.na(x)] <- ""
  return(x)
}

naToZero <- function(x) {
  x[is.na(x)] <- 0
  return(x)
}

nullToEmpty <- function(x) {
  x[is.null(x)] <- ""
  return(x)
}


.replaceNaInDataFrameWithEmptyString <- function(data) {
  data %>%
    dplyr::mutate(dplyr::across(where(is.character), ~ tidyr::replace_na(.x, as.character('')))) %>%
    dplyr::mutate(dplyr::across(where(is.logical), ~ tidyr::replace_na(.x, as.character('')))) %>%
    dplyr::mutate(dplyr::across(where(is.numeric), ~ tidyr::replace_na(.x, as.numeric(''))))
}


getDomainInformation <- function(package = "CohortDiagnostics") {
  ParallelLogger::logTrace("  - Reading domains.csv")
  domains <- readr::read_csv(
    system.file("csv", "domains.csv", package = package),
    col_types = readr::cols(),
    guess_max = min(1e7),
    na = "NA"
  ) %>%
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
          string = camelCaseToTitleCase(snakeCaseToCamelCase(domains$domainConceptId)),
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
          string = camelCaseToTitleCase(snakeCaseToCamelCase(domains$domainSourceConceptId)),
          pattern = '[A-Z]'
        ),
        paste,
        collapse = ' '
      ),
      pattern = " ",
      replacement = ""
    )
  return(domains)
}




writeToAllOutputToCsv <- function(object,
                                  exportFolder,
                                  incremental,
                                  minCellCount,
                                  databaseId) {
  resultsDataModel <-
    getResultsDataModelSpecifications(packageName = 'CohortDiagnostics')
  tablesOfInterest = resultsDataModel %>%
    dplyr::pull(.data$tableName) %>%
    unique()
  
  columnsToApplyMinCellValue <-
    c(
      "baseCount",
      "cohortCount",
      "cohortEntries.",
      "finalCount",
      "gainCount",
      "gainSubjects",
      "meetSubjects",
      "personCount",
      "personDays",
      "personTotal",
      "records",
      "recordsEnd",
      "recordsStart",
      "remainSubjects",
      "subjectCount",
      "subjects",
      "subjectsEnd",
      "subjectsStart",
      "totalSubjects"
    )
  vocabularyTables <- c(
    "concept",
    "concept_ancestor",
    "concept_class",
    "concept_relationship",
    "concept_synonym",
    "domain",
    "relationship",
    "vocabulary"
  )
  vocabularyTablesNoIncremental <- c("concept_class",
                                     "domain",
                                     "relationship",
                                     "vocabulary")
  
  
  # write vocabulary tables
  for (i in (1:length(tablesOfInterest))) {
    if (tablesOfInterest[[i]] %in% names(object)) {
      ParallelLogger::logTrace(paste0(" - Writing data to file: ", tablesOfInterest[[i]], ".csv"))
      columns <- resultsDataModel %>%
        dplyr::filter(.data$tableName %in% tablesOfInterest[[i]]) %>%
        dplyr::pull(.data$fieldName)
      data <-
        object[[tablesOfInterest[[i]]]]
      if (!tablesOfInterest[[i]] %in% vocabularyTables) {
        data <- data %>%
          dplyr::mutate(databaseId = !!databaseId)
      }
      data <- data %>%
        dplyr::select(snakeCaseToCamelCase(columns))
      data <- data %>%
        enforceMinCellValueInDataframe(columnNames = columnsToApplyMinCellValue,
                                       minCellCount = minCellCount)
      if (tablesOfInterest[[i]] %in% vocabularyTablesNoIncremental) {
        # these tables are never incremental, always full replace
        writeToCsv(
          data = data,
          fileName = file.path(exportFolder,
                               paste0(tablesOfInterest[[i]], ".csv")),
          incremental = FALSE
        )
      } else {
        writeToCsv(
          data = data,
          fileName = file.path(exportFolder,
                               paste0(tablesOfInterest[[i]], ".csv")),
          incremental = incremental
        )
      }
    }
  }
}
