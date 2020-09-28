# Copyright 2020 Observational Health Data Sciences and Informatics
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
# 

#' Get specifications for Cohort Diagnostics results data model
#' 
#' @return 
#' A tibble data frame object with specifications
#' 
#' @export
getResultsDataModelSpecifications <- function() {
  pathToCsv <- system.file("settings", "resultsDataModelSpecification.csv", package = "CohortDiagnostics")
  resultsDataModelSpecifications <- readr::read_csv(file = pathToCsv, col_types = readr::cols())
  return(resultsDataModelSpecifications)
}

checkColumnNames <- function(table, tableName, zipFileName, specifications = getResultsDataModelSpecifications()) {
  observeredNames <- colnames(table)[order(colnames(table))]
  
  tableSpecs <- specifications %>%
    dplyr::filter(.data$tableName == !!tableName)
  
  optionalNames <- tableSpecs %>%
    dplyr::filter(.data$optional == "Yes") %>%
    dplyr::select(.data$fieldName)
  
  expectedNames <- tableSpecs %>%
    dplyr::select(.data$fieldName) %>%
    dplyr::anti_join(dplyr::filter(optionalNames, !.data$fieldName %in% observeredNames), by = "fieldName") %>%
    dplyr::arrange(.data$fieldName) %>%
    dplyr::pull()
  
  if (!isTRUE(all.equal(expectedNames, observeredNames))) {
    stop(sprintf("Column names of table %s in zip file %s do not match specifications.\n- Observed columns: %s\n- Expected columns: %s",
                 tableName,
                 zipFileName,
                 paste(observeredNames, collapse = ", "),
                 paste(expectedNames, collapse = ", ")))
  }
}

checkAndFixDataTypes <- function(table, tableName, zipFileName, specifications = getResultsDataModelSpecifications()) {
  tableSpecs <- specifications %>%
    filter(.data$tableName == !!tableName)
  
  observedTypes <- sapply(table, class)
  for (i in 1:length(observedTypes)) {
    fieldName <- names(observedTypes)[i]
    expectedType <- gsub("\\(.*\\)", "", tolower(tableSpecs$type[tableSpecs$fieldName == fieldName]))
    if (expectedType == "bigint" || expectedType == "float") {
      if (observedTypes[i] != "numeric" && observedTypes[i] != "double") {
        ParallelLogger::logDebug(sprintf("Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
                                         fieldName,
                                         tableName,
                                         zipFileName,
                                         observedTypes[i], 
                                         expectedType))
        table <- mutate_at(table, i, as.numeric)
      }
    } else if (expectedType == "int") {
      if (observedTypes[i] != "integer") {
        ParallelLogger::logDebug(sprintf("Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
                                         fieldName,
                                         tableName,
                                         zipFileName,
                                         observedTypes[i], 
                                         expectedType))
        table <- mutate_at(table, i, as.integer)
      }
    } else if (expectedType == "varchar") {
      if (observedTypes[i] != "character") {
        ParallelLogger::logDebug(sprintf("Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
                                         fieldName,
                                         tableName,
                                         zipFileName,
                                         observedTypes[i], 
                                         expectedType))
        table <- mutate_at(table, i, as.character)
      }
    } else if (expectedType == "date") {
      if (observedTypes[i] != "Date") {
        ParallelLogger::logDebug(sprintf("Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
                                         fieldName,
                                         tableName,
                                         zipFileName,
                                         observedTypes[i], 
                                         expectedType))
        table <- mutate_at(table, i, as.Date)
      }
    }
  } 
  return(table)
}

checkAndFixDuplicateRows <- function(table, tableName, zipFileName, specifications = getResultsDataModelSpecifications()) {
  primaryKeys <- specifications %>%
    dplyr::filter(.data$tableName == !!tableName & .data$primaryKey == "Yes") %>%
    dplyr::select(.data$fieldName) %>%
    dplyr::pull()
  duplicated <- duplicated(table[, primaryKeys])
  if (any(duplicated)) {
    warning(sprintf("Table %s in zip file %s has duplicate rows. Removing %s records.",
                    tableName,
                    zipFileName,
<<<<<<< HEAD
                    sum(duplicated)))
=======
                    sum(duplicated)
    ))
>>>>>>> 31d936f122b1b1d3b9c63a6fbdd474d71ef1c0ce
    return(table[!duplicated, ])
  } else {
    return(table)
  }
}

appendNewRows <- function(data, newData, tableName, specifications = getResultsDataModelSpecifications()) {
  if (nrow(data) > 0) {
    primaryKeys <- specifications %>%
      dplyr::filter(.data$tableName == !!tableName & .data$primaryKey == "Yes") %>%
      dplyr::select(.data$fieldName) %>%
      dplyr::pull()
    newData <- newData %>%
      dplyr::anti_join(data, by = primaryKeys)
  }
  return(dplyr::bind_rows(data, newData))  
}
