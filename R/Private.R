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
    if (!silent) {
      censoredRecords <- data %>%
        dplyr::filter(.data[[fieldName]] <= !!minValues) %>% 
        dplyr::filter(.data[[fieldName]] != 0) %>%
        dplyr::summarize(n = dplyr::n()) %>%
        dplyr::pull(.data$n)
      
      allRecords <- data %>%
        dplyr::summarize(n = dplyr::n()) %>%
        dplyr::pull(.data$n)
      
      percent <- round(100 * censoredRecords / allRecords, 1)
      ParallelLogger::logTrace(
        "  - Censoring ",
        censoredRecords,
        " values (",
        percent,
        "%) from ",
        fieldName,
        " because value below minimum"
      )
    }
    data <- data %>%
      dplyr::mutate(
        !!fieldName := dplyr::case_when(
          .data[[fieldName]] <= !!minValues &
            .data[[fieldName]] > 0 ~ !!minValues * -1,
          TRUE ~ .data[[fieldName]]
        )
      )
    return(data)
  }



enforceMinCellValueInDataframe <- function(data,
                                           columnNames,
                                           minCellCount = 5) {
  if (is.null(columnNames)) {
    return(data)
  }
  presentInBoth <- intersect(columnNames, colnames(data))
  if (length(presentInBoth) == 0) {
    return(data)
  }
  for (i in (1:length(presentInBoth))) {
    if (presentInBoth[[i]] %in% colnames(data)) {
      data <-
        enforceMinCellValue(data = data,
                            fieldName = presentInBoth[[i]],
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

.convertDateToString <- function(data) {
  data %>%
    dplyr::collect() %>%
    dplyr::mutate(dplyr::across(where(is.date),
                                as.character))
}


writeToAllOutputToCsv <- function(object,
                                  exportFolder,
                                  incremental,
                                  minCellCount,
                                  databaseId) {
  # ObjectIsAndromeda <- Andromeda::isAndromeda(object)
  
  resultsDataModel <-
    getResultsDataModelSpecifications(packageName = 'CohortDiagnostics')
  tablesOfInterest = resultsDataModel %>%
    dplyr::pull(.data$tableName) %>%
    unique()
  
  columnsToApplyMinCellValue <-
    c(
      "baseCount",
      "cohortCount",
      "cohortEntries",
      "cohortSubjects",
      "conceptCount",
      "finalCount",
      "gainCount",
      "gainSubjects",
      "meetSubjects",
      "personCount",
      "personDays",
      "personDaysIn",
      "personTotal",
      "records",
      "recordsEnd",
      "recordsStart",
      "remainSubjects",
      "subjectCount",
      "subjects",
      "subjectsEnd",
      "subjectsEndIn",
      "subjectsStart",
      "subjectsStartIn",
      "subCsBeforeTs",
      "recCsBeforeTs",
      "subCsOnTs",
      "recCsOnTs",
      "subCsAfterTs",
      "recCsAfterTs",
      "subCsBeforeTe",
      "recCsBeforeTe",
      "subCsOnTe",
      "recCsOnTe",
      "subCsAfterTe",
      "recCsAfterTe",
      "subCsWindowT",
      "recCsWindowT",
      "subCeWindowT",
      "recCeWindowT",
      "subCsWindowTs",
      "recCsWindowTs",
      "subCsWindowTe",
      "recCsWindowTe",
      "subCeWindowTs",
      "recCeWindowTs",
      "subCeWindowTe",
      "recCeWindowTe",
      "subC_withinT",
      "recC_withinT",
      "cDaysBeforeTs",
      "cDaysBeforeTe",
      "cDays_withinTDays",
      "cDaysAfterTs",
      "cDaysAfterTe",
      "tDays",
      "cDays",
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
  
  ParallelLogger::logTrace(paste0("  - Found ", paste0(names(object), collapse = ", ")))
  presentInBoth <-
    intersect(tablesOfInterest, camelCaseToSnakeCase(names(object)))
  presentInObjectOnly <-
    setdiff(camelCaseToSnakeCase(names(object)), tablesOfInterest)
  if (!setequal(presentInBoth, camelCaseToSnakeCase((names(object))))) {
    warning(
      " - Unexpected objects found ",
      paste(presentInObjectOnly, collapse = ", "),
      ". Please contact developer."
    )
  }
  
  # write vocabulary tables
  for (i in (1:length(tablesOfInterest))) {
    if (tablesOfInterest[[i]] %in% camelCaseToSnakeCase(names(object))) {
      ParallelLogger::logTrace(paste0(" - Writing data to file: ", tablesOfInterest[[i]], ".csv"))
      columns <- resultsDataModel %>%
        dplyr::filter(.data$tableName %in% tablesOfInterest[[i]]) %>%
        dplyr::pull(.data$fieldName) %>%
        snakeCaseToCamelCase()
      ## because Andromeda is not handling date consistently -
      ## temporary solution is to collect data into R memory using dplyr::collect()
      data <-
        object[[snakeCaseToCamelCase(tablesOfInterest[[i]])]] %>%
        dplyr::collect()
      if (!tablesOfInterest[[i]] %in% vocabularyTables) {
        # object[[snakeCaseToCamelCase(tablesOfInterest[[i]])]] <-
        #   object[[snakeCaseToCamelCase(tablesOfInterest[[i]])]] %>%
        #   dplyr::mutate(databaseId = !!databaseId)
        data <- data %>%
          dplyr::mutate(databaseId = !!databaseId)
      }
      # select columns as required in data model
      # !!!!!!!!!!commenting out this section because of
      # https://github.com/OHDSI/Andromeda/issues/28
      # object[[snakeCaseToCamelCase(tablesOfInterest[[i]])]] <-
      #   object[[snakeCaseToCamelCase(tablesOfInterest[[i]])]] %>%
      #   dplyr::select(columns)
      
      data <-
        data %>%
        dplyr::select(columns)
      
      # enforce minimum cell count value
      # object[[snakeCaseToCamelCase(tablesOfInterest[[i]])]] <-
      #   object[[snakeCaseToCamelCase(tablesOfInterest[[i]])]] %>%
      #   enforceMinCellValueInDataframe(columnNames = columnsToApplyMinCellValue,
      #                                  minCellCount = minCellCount)
      data <-
        data %>%
        enforceMinCellValueInDataframe(columnNames = columnsToApplyMinCellValue,
                                       minCellCount = minCellCount)
      if (tablesOfInterest[[i]] %in% vocabularyTablesNoIncremental) {
        # these tables are never incremental, always full replace
        writeToCsv(
          data = data,
          #object[[snakeCaseToCamelCase(tablesOfInterest[[i]])]],
          fileName = file.path(exportFolder,
                               paste0(tablesOfInterest[[i]], ".csv")),
          incremental = FALSE
        )
      } else {
        writeToCsv(
          data = data,
          #object[[snakeCaseToCamelCase(tablesOfInterest[[i]])]],
          fileName = file.path(exportFolder,
                               paste0(tablesOfInterest[[i]], ".csv")),
          incremental = incremental
        )
      }
    }
  }
}
