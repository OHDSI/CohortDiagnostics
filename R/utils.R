# Copyright 2025 Observational Health Data Sciences and Informatics
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

#' Maximum value without warning on all-missing input
#'
#' Returns the maximum of the input values, or \code{NA} when there are no
#' values or all values are missing (no warning).
#'
#' @param x A numeric, Date, or character vector (or \code{...} when \code{x} is omitted).
#' @param ... Additional values to include (when \code{x} is a single value).
#' @param na.rm Logical: remove \code{NA} before computing max (default \code{TRUE}).
#' @return The maximum value, or \code{NA} if \code{length(x)} is 0 or all values are \code{NA}.
#' @export
safeMax <- function(x, ..., na.rm = TRUE) {
  if (missing(x)) {
    x <- c(...)
  } else {
    x <- c(x, ...)
  }
  if (length(x) == 0L || all(is.na(x))) {
    return(NA)
  }
  max(x, na.rm = TRUE)
}

#' Check that all elements are covariateSettings objects
#'
#' @param x A single \code{covariateSettings} object or a list of such objects.
#' @return \code{TRUE} if \code{x} is a single covariateSettings object or a list of covariateSettings objects.
#' @keywords internal
allCovariateSettings <- function(x) {
  if (is(x, "covariateSettings")) return(TRUE)
  if (!is.list(x)) return(FALSE)
  all(vapply(x, function(s) "covariateSettings" %in% class(s), logical(1)))
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
    }
  }
  return(TRUE)
}

createIfNotExist <-
  function(type,
           name,
           recursive = TRUE,
           errorMessage = NULL) {
    if (is.null(errorMessage) |
      !is(errorMessage, "AssertionCollection")) {
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
    if (column %in% colnames(x)) {
      x <-
        enforceMinCellValue(
          data = x,
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

#' Internal utility function for logging execution of variables
#' @noRd
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
  checkmate::assertClass(execTime, "difftime")
  checkmate::assertClass(start, "POSIXct")
  executionTimes <- data.frame(
    task = taskName,
    startTime = start,
    cohortIds = paste(cohortIds, collapse = ";"),
    executionTime = round(as.numeric(execTime, units = "secs")/60, 4),
    parent = paste(parent, collapse = "")
  )

  readr::write_csv(executionTimes, file = executionTimePath, append = file.exists(executionTimePath))
  return(executionTimes)
}

# check if a temp table already exists
tempTableExists <- function(connection, tempTableName) {
  stopifnot(methods::is(connection, "DatabaseConnectorConnection"), 
            is.character(tempTableName),
            length(tempTableName) == 1)
  tryCatch(
    is.data.frame(
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection, 
        sql = "select top 1 * from  #@tempTableName;",
        tempTableName = tempTableName)
    ),
    error = function(e) {
      if (methods::is(connection, "DatabaseConnectorJdbcConnection") &&
          DatabaseConnector::dbms(connection) %in% c("postgresql", "redshift")) {
        DatabaseConnector::executeSql(connection, "rollback;", reportOverallTime = FALSE, progressBar = FALSE) 
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
    data <- enforceMinCellValueFunc(data)
  }
  
  writeToCsv(
    data = data,
    fileName = fileName,
    incremental = incremental,
    ...
  )
  return(data)
}

assertCohortDefinitionSetContainsAllParents <- function(cohortDefinitionSet) {
  stopifnot(CohortGenerator::isCohortDefinitionSet(cohortDefinitionSet))
  if ("subsetParent" %in% names(cohortDefinitionSet)) {
    
    parentCohortIds <- cohortDefinitionSet %>% 
      dplyr::filter(!is.na(.data$subsetParent), .data$cohortId != .data$subsetParent) %>% 
      dplyr::pull(.data$subsetParent) %>% 
      unique()
    
    parentCohortIdsNotInCohortSet <- dplyr::setdiff(parentCohortIds, cohortDefinitionSet$cohortId)
    
    if (length(parentCohortIdsNotInCohortSet) > 0) {
      stop(paste0("The CohortDefinitionSet contains parent cohort IDs (", 
                 paste(parentCohortIdsNotInCohortSet, collapse = ", "),
                 ") that are not in the cohortDefinitionSet!"))
    }
  }
  invisible(NULL)
}

# returns an empty result dataframe from the result data model
emptyResult <- function(tableName = NULL) {
  allSpecs <- getResultsDataModelSpecifications()
  checkmate::assertChoice(tableName, unique(allSpecs$tableName))
  
  unique(allSpecs$dataType)
  unique(allSpecs$tableName)
  
  spec <- dplyr::filter(allSpecs, .data$tableName == .env$tableName) %>% 
    dplyr::select(columnName, dataType) %>% 
    dplyr::mutate(rDataType = dplyr::case_when(
      grepl("varchar", dataType) ~ "character()",
      dataType == "float" ~ "double()",
      dataType == "int" ~ "integer()",
      dataType == "bigint" ~ "integer()",
      dataType == "Date" ~ "as.Date(integer())",
      TRUE ~ "character()")
    )
  
  result <- dplyr::tibble()
  for (row in split(spec, seq_len(nrow(spec)))) {
    result[[row$columnName]] <- eval(parse(text = row$rDataType))
  }
  return(result)
}


processTemporalCovariateSettings <- function(temporalCovariateSettings) {
  
  stopifnot(methods::is(temporalCovariateSettings[[1]], "covariateSettings"))
  
  # Adding required temporal windows required in results viewer
  requiredTemporalPairs <-
    list(
      c(-365, 0),
      c(-30, 0),
      c(-365, -31),
      c(-30, -1),
      c(0, 0),
      c(1, 30),
      c(31, 365),
      c(-9999, 9999)
    )
  for (p1 in requiredTemporalPairs) {
    found <- FALSE
    for (i in seq_along(temporalCovariateSettings[[1]]$temporalStartDays)) {
      p2 <- c(
        temporalCovariateSettings[[1]]$temporalStartDays[i],
        temporalCovariateSettings[[1]]$temporalEndDays[i]
      )
      
      if (p2[1] == p1[1] & p2[2] == p1[2]) {
        found <- TRUE
        break
      }
    }
    
    if (!found) {
      temporalCovariateSettings[[1]]$temporalStartDays <-
        c(temporalCovariateSettings[[1]]$temporalStartDays, p1[1])
      temporalCovariateSettings[[1]]$temporalEndDays <-
        c(temporalCovariateSettings[[1]]$temporalEndDays, p1[2])
    }
  }
}


