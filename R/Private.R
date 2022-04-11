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

createIfNotExist <-
  function(type,
           name,
           recursive = TRUE,
           errorMessage = NULL) {
    if (is.null(errorMessage) |
      !class(errorMessage) == "AssertColection") {
      errorMessage <- checkmate::makeAssertCollection()
    }
    if (!is.null(type)) {
      if (length(name) == 0) {
        stop(ParallelLogger::logError("Must specify ", name))
      }
      if (type %in% c("folder")) {
        if (!file.exists(gsub("/$", "", name))) {
          dir.create(name, recursive = recursive)
          ParallelLogger::logInfo("Created ", type, " at ", name)
        } else {
          # ParallelLogger::logInfo(type, " already exists at ", name)
        }
      }
      checkmate::assertDirectory(
        x = name,
        access = "x",
        add = errorMessage
      )
    }
    invisible(errorMessage)
  }

swapColumnContents <-
  function(df,
           column1 = "targetId",
           column2 = "comparatorId") {
    temp <- df[, column1]
    df[, column1] <- df[, column2]
    df[, column2] <- temp
    return(df)
  }

enforceMinCellValue <-
  function(data, fieldName, minValues, silent = FALSE) {
    toCensor <-
      !is.na(data[, fieldName]) &
        data[, fieldName] < minValues & data[, fieldName] != 0
    if (!silent) {
      percent <- round(100 * sum(toCensor) / nrow(data), 1)
      ParallelLogger::logInfo(
        "- Censoring ",
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


#' Check character encoding of input file
#'
#' @description
#' For its input files, CohortDiagnostics only accepts UTF-8 or ASCII character encoding. This
#' function can be used to check whether a file meets these criteria.
#'
#' @param fileName  The path to the file to check
#'
#' @return
#' Throws an error if the input file does not have the correct encoding.
#'
checkInputFileEncoding <- function(fileName) {
  encoding <- readr::guess_encoding(file = fileName, n_max = min(1e7))

  if (!encoding$encoding[1] %in% c("UTF-8", "ASCII")) {
    stop(
      "Illegal encoding found in file ",
      basename(fileName),
      ". Should be 'ASCII' or 'UTF-8', found:",
      paste(
        paste0(encoding$encoding, " (", encoding$confidence, ")"),
        collapse = ", "
      )
    )
  }
  invisible(TRUE)
}

naToZero <- function(x) {
  x[is.na(x)] <- 0
  return(x)
}

nullToEmpty <- function(x) {
  x[is.null(x)] <- ""
  return(x)
}



makeDataExportable <- function(x,
                               tableName,
                               minCellCount = 5,
                               databaseId = NULL) {
  ParallelLogger::logTrace(paste0(" - Ensuring data is exportable: ", tableName))
  if (hasData(x)) {
    ParallelLogger::logTrace("  - Object has no data.")
  }

  if ("cohortDefinitionId" %in% colnames(x)) {
    x <- x %>% 
      dplyr::rename(cohortId = .data$cohortDefinitionId)
  }
  
  resultsDataModel <- getResultsDataModelSpecifications()

  if (!is.null(databaseId)) {
    x <- x %>%
      dplyr::mutate(databaseId = !!databaseId)
  }

  fieldsInDataModel <- resultsDataModel %>%
    dplyr::filter(.data$tableName == !!tableName) %>%
    dplyr::pull(.data$fieldName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    unique()

  requiredFieldsInDataModel <- resultsDataModel %>%
    dplyr::filter(.data$tableName == !!tableName) %>%
    dplyr::filter(.data$isRequired == "Yes") %>%
    dplyr::pull(.data$fieldName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    unique()

  primaryKeyInDataModel <- resultsDataModel %>%
    dplyr::filter(.data$tableName == !!tableName) %>%
    dplyr::filter(.data$primaryKey == "Yes") %>%
    dplyr::pull(.data$fieldName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    unique()

  columnsToApplyMinCellValue <- resultsDataModel %>%
    dplyr::filter(.data$tableName == !!tableName) %>%
    dplyr::filter(.data$minCellCount == "Yes") %>%
    dplyr::pull(.data$fieldName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    unique()

  ParallelLogger::logTrace(paste0(
    "  - Found in table ",
    tableName,
    " the following fields: ",
    paste0(names(x), collapse = ", ")
  ))

  presentInBoth <-
    intersect(fieldsInDataModel, names(x))
  presentInDataOnly <-
    setdiff(names(x), fieldsInDataModel)
  missingRequiredFields <-
    setdiff(requiredFieldsInDataModel, presentInBoth)

  if (length(presentInDataOnly) > 0) {
    ParallelLogger::logInfo(
      " - Unexpected fields found in table ",
      tableName,
      " - ",
      paste(presentInDataOnly, collapse = ", "),
      ". These fields will be ignored."
    )
  }

  if (length(missingRequiredFields) > 0) {
    stop(
      " - Cannot find required field ",
      tableName,
      " - ",
      paste(missingRequiredFields, collapse = ", "),
      "."
    )
  }

  # check to see if there are primary key collision in tables that have this unique constraint
  if (length(primaryKeyInDataModel) > 0) {
    distinctRows <- x %>%
      dplyr::select(dplyr::all_of(primaryKeyInDataModel)) %>%
      dplyr::distinct() %>%
      nrow()
    if (nrow(x) > distinctRows) {
      stop(
        " - duplicates found in primary key for table ",
        tableName,
        ". The primary keys are: ",
        paste0(primaryKeyInDataModel, collapse = ", ")
      )
    }
  }
  ## because Andromeda is not handling date consistently -
  # https://github.com/OHDSI/Andromeda/issues/28
  ## temporary solution is to collect data into R memory using dplyr::collect()

  x <- x %>%
    dplyr::collect()

  # limit to fields in data model
  x <- x %>%
    dplyr::select(dplyr::all_of(presentInBoth))

  # enforce minimum cell count value
  if (hasData(x)) {
    x <- x %>%
      enforceMinCellValueInDataframe(columnNames = columnsToApplyMinCellValue,
                                     minCellCount = minCellCount)
  }
  return(x)
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
        enforceMinCellValue(
          data = data,
          fieldName = presentInBoth[[i]],
          minValues = minCellCount
        )
    }
  }
  return(data)
}


# private function - not exported
titleCaseToCamelCase <- function(string) {
  string <- stringr::str_replace_all(
    string = string,
    pattern = " ",
    replacement = ""
  )
  substr(string, 1, 1) <- tolower(substr(string, 1, 1))
  return(string)
}

getTimeAsInteger <- function(time = Sys.time(),
                             tz = "UTC") {
  return(as.numeric(as.POSIXlt(time, tz = tz)))
}
