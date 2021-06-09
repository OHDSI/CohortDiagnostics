# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of ConceptSetDiagnostics
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


# this script is shared between Cohort Diagnostics and Diagnostics Explorer

# private function - not exported
camelCaseToTitleCase <- function(string) {
  string <- gsub("([A-Z])", " \\1", string)
  string <- gsub("([a-z])([0-9])", "\\1 \\2", string)
  substr(string, 1, 1) <- toupper(substr(string, 1, 1))
  return(string)
}

# private function - not exported
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

# private function - not exported
quoteLiterals <- function(x) {
  if (is.null(x)) {
    return("")
  } else {
    return(paste0("'", paste(x, collapse = "', '"), "'"))
  }
}

#' Return a database data source object
#'
#' @description
#' Collects a list of objects needed to connect to a database datsource. This includes one of
#' \code{DatabaseConnector::createConnectionDetails} object, or a DBI database connection created
#' using either \code{DatabaseConnector::connection} or \code{pool::dbPool}, and a names of
#' resultsDatabaseSchema and vocabularyDatabaseSchema
#'
#' @template Connection
#'
#' @template VocabularyDatabaseSchema
#' 
#' @template resultsDatabaseSchema
#'
#' @return
#' Returns a list with information on database data source
#' @export
createDatabaseDataSource <- function(connection = NULL,
                                     connectionDetails = NULL,
                                     resultsDatabaseSchema,
                                     vocabularyDatabaseSchema = resultsDatabaseSchema) {
  return(
    list(
      connection = connection,
      connectionDetails = connectionDetails,
      resultsDatabaseSchema = resultsDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema
    )
  )
}


#' Return a file data source object
#'
#' @description
#' Given a premerged file (an .RData/rds object the output
#' of \code{CohortDiagnostics::preMergeDiagnosticsFiles} reads the object into
#' memory and makes it available for query.
#'
#' @param premergedDataFile  an .RData/rds object the output
#'                           of \code{CohortDiagnostics::preMergeDiagnosticsFiles}
#'                           
#' @param envir             (optional) R-environment to read premerged data. By default this is the 
#'                          global environment.
#'
#' @return
#' R environment containing data conforming to Cohort Diagnostics results data model specifications.
#' @export
createFileDataSource <-
  function(premergedDataFile, envir = .GlobalEnv) {
    load(premergedDataFile, envir = envir)
    return(envir)
  }


# private function - not exported
# this function supports query with connection that is either pool or not pool. It can also establish a connection
# when connection details is provided
renderTranslateQuerySql <-
  function(sql,
           connection = NULL,
           connectionDetails = NULL,
           ...,
           snakeCaseToCamelCase = FALSE) {
    # Set up connection to server ----------------------------------------------------
    if (is.null(connection)) {
      if (!is.null(connectionDetails)) {
        writeLines("Connecting to database using provided connection details.")
        connection <- DatabaseConnector::connect(connectionDetails)
        on.exit(DatabaseConnector::disconnect(connection))
      } else {
        stop("No connection or connectionDetails provided.")
      }
    }
    
    if (methods::is(connection, "Pool")) {
      # Connection pool is used by Shiny app, which always uses PostgreSQL:
      sql <- SqlRender::render(sql, ...)
      sql <- SqlRender::translate(sql, targetDialect = "postgresql")
      
      tryCatch({
        data <- DatabaseConnector::dbGetQuery(connection, sql)
      }, error = function(err) {
        writeLines(sql)
        stop(err)
      })
      if (snakeCaseToCamelCase) {
        colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
      }
      return(data %>% dplyr::tibble())
    } else {
      if (is.null(attr(connection, "dbms"))) {
        stop("dbms not provided. Unable to translate query.")
      }
      return(
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          ...,
          snakeCaseToCamelCase = snakeCaseToCamelCase
        ) %>% dplyr::tibble()
      )
    }
  }

# private function - not exported
getDataForDatabaseIdsCohortIds <- function(dataSource,
                                           cohortIds = NULL,
                                           databaseIds = NULL,
                                           dataTableName) {
  if (any(is.null(databaseIds), length(databaseIds) == 0)) {
    stop("Database Ids not correctly specified.")
  }
  
  if (any(!exists('cohortIds'), length(cohortIds) == 0)) {
    stop("Cohort Ids not correctly specified.")
  }
  
  if (is(dataSource, "environment")) {
    if (!exists(get(dataTableName, envir = dataSource))) {
      stop(paste0(
        'Please check if ',
        dataTableName,
        " is present in environment."
      ))
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      warning(paste0(dataTableName, " in environment was found to have o rows."))
    }
    data <- get(dataTableName, envir = dataSource)
    if (!is.null(cohortIds)) {
      data <- data %>%
        dplyr::filter(.data$cohortId %in% !!cohortIds)
    }
    if (!is.null(databaseIds)) {
      data <- data %>%
        dplyr::filter(.data$databaseId %in% !!databaseIds)
    }
  } else {
    if (!DatabaseConnector::dbIsValid(dataSource$connection)) {
      stop('Connection to database seems to be closed.')
    }
    sql <- "SELECT *
            FROM  @results_database_schema.@data_table
            WHERE database_id in (@database_id)
            {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)}
            ;"
    
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        cohort_ids = cohortIds,
        data_table = camelCaseToSnakeCase(dataTableName),
        database_id = quoteLiterals(databaseIds),
        snakeCaseToCamelCase = TRUE
      ) %>%
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    return(NULL)
  }
  
  resultsDataModelSpecifications <-
    CohortDiagnostics::getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == camelCaseToSnakeCase(!!dataTableName))
  
  requiredColumns <- resultsDataModelSpecifications %>%
    dplyr::filter(.data$isRequired == 'Yes') %>%
    dplyr::pull(.data$fieldName) %>%
    snakeCaseToCamelCase()
  
  missingColumns <- setdiff(requiredColumns, colnames(data))
  
  if (length(missingColumns) > 0) {
    stop(paste0(
      "Missing the following columns in ",
      dataTableName,
      ": ",
      paste0(missingColumns, collapse = ", ")
    ))
  }
  return(data)
}


#' Returns data from cohort_count table in cohort diagnostics
#'
#' @description
#' Returns data from cohort_count table in cohort diagnostics
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to cohort counts
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromCohortCount <- function(dataSource,
                                      cohortIds,
                                      databaseIds) {
  data <- getDataForDatabaseIdsCohortIds(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = 'cohortCount'
  )
  return(data)
}

#' Returns data from time_series table in cohort diagnostics
#'
#' @description
#' Returns data from time_series table in cohort diagnostics
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to time series
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromTimeSeries <- function(dataSource,
                                     cohortIds,
                                     databaseIds) {
  data <- getDataForDatabaseIdsCohortIds(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = 'timeSeries'
  )
  return(data)
}
