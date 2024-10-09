# Copyright 2024 Observational Health Data Sciences and Informatics
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
  } else {
    if (length(data) == 0) {
      return(FALSE)
    }
    if (length(data) == 1 && is.na(data)) {
        return(FALSE)
    }
  }
  return(TRUE)
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
  function(data, columnName, minValues, silent = FALSE) {
    data <- as.data.frame(data)
    toCensor <-
      !is.na(data[, columnName]) &
        data[, columnName] < minValues & data[, columnName] > 0

    if (!silent) {
      percent <- round(100 * sum(toCensor) / nrow(data), 1)
      ParallelLogger::logInfo(
        "- Censoring ",
        sum(toCensor),
        " values (",
        percent,
        "%) from ",
        columnName,
        " because value below minimum"
      )
    }

    if (length(minValues) == 1) {
      data[toCensor, columnName] <- -minValues
    } else {
      data[toCensor, columnName] <- -minValues[toCensor]
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
  readr::local_edition(1)
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

# makeDataExportable is used to validate that the results conform to the output data model
# and suppress cell counts
makeDataExportable <- function(x,
                               tableName,
                               minCellCount = 5,
                               databaseId = NULL) {
  
  # x can be a dataframe or an Andromeda table (sqlite dplyr::tbl table reference)
  ## because Andromeda is not handling date consistently -
  # https://github.com/OHDSI/Andromeda/issues/28
  ## temporary solution is to collect data into R memory using dplyr::collect()
  # Note: this means that all data processed ends up fully in memory
  # This could be changed with batch operations on andromeda objects
  # If x is an andromeda object dplyr::collect will bring it into R as a dataframe
  # If x is a dataframe then dplyr::collect has no effect
  x <- dplyr::collect(x)
  
  checkmate::assertClass(x, "data.frame")
  
  ParallelLogger::logTrace(paste0(" - Ensuring data is exportable: ", tableName))
  
  if (nrow(x) == 0) {
    ParallelLogger::logTrace("  - Object has no data.")
    return(x)
  }
  
  resultsDataModel <- getResultsDataModelSpecifications(tableName = tableName)
  
  checkmate::assertIntegerish(minCellCount, len = 1, lower = 0, any.missing = FALSE)
  checkmate::assertCharacter(databaseId, min.chars = 1, len = 1, any.missing = FALSE, null.ok = TRUE)

  if ("cohortDefinitionId" %in% colnames(x)) {
    x <- dplyr::rename(x, "cohortId" = "cohortDefinitionId")
  }

  if (!is.null(databaseId)) {
    x <- dplyr::mutate(x, databaseId = .env$databaseId)
  }

  # column names in results datamodel specification are in snake case but the columns in R are camel case
  # writeToCsv converts the camel case column names in R to snake case
  fieldsInDataModel <- SqlRender::snakeCaseToCamelCase(resultsDataModel$columnName)

  requiredFieldsInDataModel <- resultsDataModel %>%
    dplyr::filter(.data$isRequired == "Yes") %>%
    dplyr::pull(.data$columnName) %>% 
    SqlRender::snakeCaseToCamelCase() 

  primaryKeyInDataModel <- resultsDataModel %>%
    dplyr::filter(.data$primaryKey == "Yes") %>%
    dplyr::pull(.data$columnName) %>% 
    SqlRender::snakeCaseToCamelCase() 

  columnsToApplyMinCellValue <- resultsDataModel %>%
    dplyr::filter(.data$minCellCount == "Yes") %>%
    dplyr::pull(.data$columnName) %>% 
    SqlRender::snakeCaseToCamelCase()

  ParallelLogger::logTrace(paste0(
    "  - Found in table ",
    tableName,
    " the following fields: ",
    paste0(names(x), collapse = ", ")
  ))

  presentInBoth <- intersect(fieldsInDataModel, names(x))
  
  presentInDataOnly <- setdiff(names(x), fieldsInDataModel)
  
  missingRequiredFields <- setdiff(requiredFieldsInDataModel, presentInBoth)

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
      dplyr::count() %>%
      dplyr::pull()

    rowCount <- x %>%
      dplyr::count() %>%
      dplyr::pull()

    if (nrow(x) > distinctRows) {
      stop(
        " - duplicates found in primary key for table ",
        tableName,
        ". The primary keys are: ",
        paste0(primaryKeyInDataModel, collapse = ", ")
      )
    }
  }

  # limit to fields in data model
  x <- dplyr::select(x, dplyr::all_of(presentInBoth))

  # enforce minimum cell count value
    for (column in columnsToApplyMinCellValue) {
      if (column %in% colnames(data)) {
        data <-
          enforceMinCellValue(
            data = data,
            columnName = column,
            minValues = minCellCount
          )
      }
    }

  # Ensure that timeId is never NA
  if ("timeId" %in% colnames(x)) {
    if (any(is.na(x$timeId))) {
      x[is.na(x$timeId), "timeId"] <- 0
    }
  }
  return(x)
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


getPrefixedTableNames <- function(tablePrefix) {
  if (is.null(tablePrefix)) {
    tablePrefix <- ""
  }

  if (grepl(" ", tablePrefix)) {
    stop("Table prefix cannot include spaces")
  }

  dataModel <- getResultsDataModelSpecifications()
  tableNames <- dataModel$tableName %>% unique()
  resultList <- list()

  for (tableName in tableNames) {
    resultList[tableName] <- paste0(tablePrefix, tableName)
  }

  return(resultList)
}

# Internal utility function for logging execution of variables
timeExecution <- function(exportFolder,
                          taskName,
                          cohortIds = NULL,
                          parent = NULL,
                          start = NA,
                          execTime = NA,
                          expr = NULL) {
  readr::local_edition(1)
  executionTimePath <- file.path(exportFolder, "executionTimes.csv")
  if (is.na(start)) {
    start <- Sys.time()
    eval(expr)
    execTime <- Sys.time() - start
  }
  executionTimes <- data.frame(
    task = taskName,
    startTime = start,
    cohortIds = paste(cohortIds, collapse = ";"),
    executionTime = execTime,
    parent = paste(parent, collapse = "")
  )

  readr::write_csv(executionTimes, file = executionTimePath, append = file.exists(executionTimePath))
  return(executionTimes)
}

# check if a temp table already exists
tempTableExists <- function(connection, tempTableName) {
  tryCatch(
    is.data.frame(
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection, 
        sql = "select top 1 * from  #@tempTableName;",
        tempTableName = tempTableName)
    ),
    error = function(e) {
      if (DatabaseConnector::dbms(connection) %in% c("postgresql", "redshift") &&
          methods::is(connection, "DatabaseConnectorJdbcConnection")) {
        DatabaseConnector::executeSql(connection, "rollback;", reportOverallTime = F, progressBar = F) 
      }
      return(FALSE)
    }
  )
}

exportDataToCsv <- function(data, tableName, fileName, minCellCount = 5, databaseId = NULL, 
                            incremental = FALSE, enforceMinCellValueFunc = NULL,  ...) {
  data <- makeDataExportable(
    x = data,
    tableName = tableName,
    minCellCount = minCellCount,
    databaseId = databaseId
  )

  if (!is.null(enforceMinCellValueFunc) && nrow(data) > 0) {
    data <- enforceMinCellValueFunc
  }
  
  writeToCsv(
    data = data,
    fileName = fileName,
    incremental = incremental,
    ...
  )
  return(data)
}
