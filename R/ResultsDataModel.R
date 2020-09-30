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
                    sum(duplicated)))
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


#' Create the results data model tables on a database server.
#'
#' @template Connection 
#' @param schema         The schema on the postgres server where the tables will be created.
#'
#' @export
createResultsDataModel <- function(connection = NULL,
                                   connectionDetails = NULL,
                                   schema) {
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  } 
  executeSql(connection, sprintf("SET search_path TO %s;", schema), progressBar = FALSE, reportOverallTime = FALSE)
  pathToSql <- system.file("sql", "postgresql", "CreateResultsDataModel.sql", package = "CohortDiagnostics")
  sql <- SqlRender::readSql(pathToSql)
  # sql <- SqlRender::render(sql, results_schema = schema)
  executeSql(connection, sql)
}

naToEmpty <- function(x) {
  x[is.na(x)] <- ""
  return(x)
}

#' Upload results to the database server.
#' 
#' @description 
#' Requires the results data model tables have been created using the \code{\link{createResultsDataModel}} function.
#'
#' @template Connection 
#' @param schema         The schema on the postgres server where the tables will be created.
#' @param zipFileName    The name of the zip file.
#' @param tempFolder     A folder on the local file system where the zip files are extracted to. Will be cleaned
#'                       up when the function is finished. Can be used to specify a temp folder on a drive that
#'                       has sufficent space if the default system temp space is too limited.
#'
#' @export
uploadResults <- function(connection = NULL,
                          connectionDetails = NULL,
                          schema,
                          zipFileName, 
                          tempFolder = tempdir()) {
  start <- Sys.time()
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  } 

  unzipFolder <- tempfile("unzipTempFolder", tmpdir = tempFolder)
  dir.create(path = unzipFolder, recursive = TRUE)
  on.exit(unlink(unzipFolder, recursive = TRUE), add = TRUE)
  
  ParallelLogger::logInfo("Unzipping ", zipFileName)
  zip::unzip(zipFileName, exdir = unzipFolder)

  specifications = getResultsDataModelSpecifications()
  
  uploadTable <- function(tableName, env) {
    ParallelLogger::logInfo("Uploading table ", tableName)
    start <- Sys.time()
    csvFileName <- paste0(tableName, ".csv")
    if (csvFileName %in% list.files(unzipFolder)) {
      env <- new.env()
      env$schema <- schema
      env$tableName <- tableName
      uploadChunk <- function(chunk, pos) {
        ParallelLogger::logInfo("- Uploading rows ", pos, " through ", pos + nrow(chunk) - 1)
        checkColumnNames(table = chunk, 
                         tableName = env$tableName, 
                         zipFileName = zipFileName,
                         specifications = specifications)
        chunk <- checkAndFixDataTypes(table = chunk, 
                                        tableName = env$tableName, 
                                        zipFileName = zipFileName,
                                        specifications = specifications)
        chunk <- checkAndFixDuplicateRows(table = chunk, 
                                            tableName = env$tableName, 
                                            zipFileName = zipFileName,
                                            specifications = specifications) 
        
        # Primary key fields cannot be NULL, so for some tables convert NAs to empty:
        toEmpty <- specifications %>%
          filter(.data$tableName == env$tableName & .data$emptyIsNa == "No") %>%
          select(.data$fieldName) %>%
          pull()
        if (length(toEmpty) > 0) {
          chunk <- chunk %>% 
            dplyr::mutate_at(toEmpty, naToEmpty)
        }
        
        #TODO: Check if primary key already exists in database
        
          
        DatabaseConnector::insertTable(connection = connection,
                                       tableName = paste(env$schema, env$tableName, sep = "."),
                                       data = as.data.frame(chunk),
                                       dropTableIfExists = FALSE,
                                       createTable = FALSE,
                                       tempTable = FALSE,
                                       progressBar = TRUE)
      }
      readr::read_csv_chunked(file = file.path(unzipFolder, csvFileName),
                              callback = uploadChunk,
                              chunk_size = 1e7,
                              col_types = readr::cols(),
                              guess_max = 1e6,
                              progress = FALSE)
      
      # chunk <- readr::read_csv(file = file.path(unzipFolder, csvFileName),
      # col_types = readr::cols(),
      # guess_max = 1e6)
      
    }
  }
  invisible(lapply(unique(specifications$tableName), uploadTable))
  delta <- Sys.time() - start
  writeLines(paste("Uploading data took", signif(delta, 3), attr(delta, "units")))
}

