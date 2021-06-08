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
getDataForDatabaseIdsCohortIds <- function(dataSource = NULL,
                                           connectionDetails = NULL,
                                           connection = NULL,
                                           resultsDatabaseSchema = NULL,
                                           premergedFile = NULL,
                                           csvFile = NULL,
                                           cohortIds,
                                           databaseIds,
                                           dataTableName) {
  if (any(is.null(databaseIds), length(databaseIds) == 0)) {
    stop("Database Ids not correctly specified.")
  }
  
  if (any(!exists('cohortIds'), length(cohortIds) == 0)) {
    stop("Cohort Ids not correctly specified.")
  }
  
  if (!is.null(dataSource)) {
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
      source <- 'premerged'
    } else {
      if (!DatabaseConnector::dbIsValid(dataSource$connection)) {
        stop('Connection to database seems to be closed.')
      }
      source <- 'database'
    }
  } else if (!is.null(premergedFile)) {
    if (file.exists(premergedFile)) {
      writeLines(paste0('Loading premerged file: ', premergedFile))
      load(premergedFile)
      if (!exists(dataTableName)) {
        stop(paste0(
          dataTableName,
          " not found in provided premergedFile ",
          premergedFile
        ))
      }
      if (nrow(get(dataTableName)) == 0) {
        warning(paste0(
          dataTableName,
          " loaded from premerged file has 0 row records"
        ))
      }
      source <- 'premerged'
    } else {
      message <-
        (paste0("Premerged file ", basename(premergedFile), " not found"))
      if (dirname(premergedFile) != '.') {
        message <- paste0(message, " at ", dirname(premergedFile))
      }
      stop(message)
    }
  } else if (!is.null(connection)) {
    if (!DatabaseConnector::dbIsValid(connection)) {
      stop('Connection to database seems to be closed.')
    }
    source <- 'database'
  } else if (!is.null(connectionDetails)) {
    writeLines("Connecting to database using provided connection details.")
    if (is.null(resultsDatabaseSchema)) {
      stop('resultsDatabaseSchema not provided.')
    }
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
    source <- 'database'
  } else if (!is.null(csvFile)) {
    if (file.exists(csvFile)) {
      writeLines(paste0("Reading csv file: '", csvFile, "'"))
      # csv files output by cohort diagnostics always have snakeCase
      data <- readr::read_csv(file = csvFile, col_types = readr::cols())
      colnames(data) <- colnames(data) %>% snakeCaseToCamelCase()
      assign(
        x = dataTableName,
        value = data
      )
      if (nrow(get(dataTableName)) == 0) {
        warning(paste0(dataTableName, " loaded from csv file has 0 row records"))
      }
      source <- 'csvFile'
    } else {
      message <- (paste0("Csv file ", basename(csvFile), " not found"))
      if (dirname(csvFile) != '.') {
        message <- paste0(message, " at ", dirname(csvFile))
      }
      stop(message)
    }
  } else {
    stop('Unable to retrieve data')
  }
  
  resultsDataModelSpecifications <-
    CohortDiagnostics::getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == camelCaseToSnakeCase(!!dataTableName))
  
  requiredColumns <- resultsDataModelSpecifications %>%
    dplyr::filter(.data$isRequired == 'Yes') %>%
    dplyr::pull(.data$fieldName) %>%
    snakeCaseToCamelCase()
  
  if (source != 'database') {
    if (!is.null(dataSource)) {
      data <- get(dataTableName, envir = dataSource)
    } else {
      data <- get(dataTableName)
    }
    missingColumns <- setdiff(requiredColumns, colnames(data))
    if (length(missingColumns) > 0) {
      stop(paste0(
        "Missing the following columns in ",
        dataTableName,
        ": ",
        paste0(missingColumns, collapse = ", ")
      ))
    }
    if (!is.null(cohortIds)) {
      data <- data %>%
        dplyr::filter(.data$cohortId %in% !!cohortIds)
    }
    if (!is.null(databaseIds)) {
      data <- data %>%
        dplyr::filter(.data$databaseId %in% !!databaseIds)
    }
  } else {
    sql <- "SELECT *
            FROM  @results_database_schema.@data_table
            WHERE database_id in (@database_id)
            {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)}
            ;"
    if (!is.null(dataSource)) {
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
    } else {
      data <-
        renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          results_database_schema = resultsDatabaseSchema,
          cohort_ids = cohortIds,
          database_id = quoteLiterals(databaseIds),
          snakeCaseToCamelCase = TRUE
        ) %>%
        tidyr::tibble() 
    }
    if (nrow(data) == 0) {
      return(NULL)
    }
  }
  missingColumns <- setdiff(requiredColumns, colnames(data))
  if (length(missingColumns) > 0) {
    warning(paste0(
      "Missing the following columns in ",
      dataTableName,
      ": ",
      paste0(missingColumns, collapse = ", ")
    ))
  }
  return(data %>% dplyr::tibble())
}


###################################################################################
###################################################################################
################# Get cohort count from results data model ########################
###################################################################################
###################################################################################

# private function - not exported
getResultsCohortCountFromAny <- function(dataSource = NULL,
                                         connectionDetails = NULL,
                                         connection = NULL,
                                         premergedFile = NULL,
                                         csvFile = NULL,
                                         cohortIds,
                                         databaseIds) {
  cohortCountResult <-
    getDataForDatabaseIdsCohortIds(
      dataSource = dataSource,
      connectionDetails = connectionDetails,
      connection = connection,
      premergedFile = premergedFile,
      csvFile = csvFile,
      cohortIds = cohortIds,
      databaseIds = databaseIds,
      dataTableName = 'cohortCount'
    )
  return(cohortCountResult)
}

# private function - not exported
# this function is for use with R shiny
getResultsCohortCountFromEnvironment <-
  function(dataSource,
           cohortIds,
           databaseIds) {
    cohortCountResult <-
      getResultsCohortCountFromAny(dataSource = dataSource,
                                   cohortIds = cohortIds,
                                   databaseIds = databaseIds)
    return(cohortCountResult)
  }

#' Get cohort counts from Database with data in results data model format
#'
#' @description
#' Given a set of one or more cohortIds and one or more databaseIds,
#' return data from the cohort_count table in results data model
#' with data filtered to the cohortIds and databaseIds.
#'
#' @template Connection
#'
#' @param cohortIds  One or more cohort ids to returns cohort counts
#'
#' @param databaseIds  One or more database ids to returns cohort counts
#'
#' @return
#' Returns a tibble dataframe
#' @export
getResultsCohortCountFromDatabase <-
  function(connectionDetails = NULL,
           connection = NULL,
           cohortIds,
           databaseIds) {
    cohortCountResult <-
      getResultsCohortCountFromAny(
        connectionDetails = connectionDetails,
        connection = connection,
        cohortIds = cohortIds,
        databaseIds = databaseIds
      )
    return(cohortCountResult)
  }



#' Get cohort counts from csv file with data in results data model format
#'
#' @description
#' Given a set of one or more cohortIds and one or more databaseIds,
#' return data from the cohort_count table in results data model
#' with data filtered to the cohortIds and databaseIds.
#'
#' @param csvFile  The full path to read the csv file.
#'
#' @param cohortIds  One or more cohort ids to returns cohort counts
#'
#' @param databaseIds  One or more database ids to returns cohort counts
#'
#' @return
#' Returns a tibble dataframe
#' @export
getResultsCohortCountFromCsv <- function(csvFile,
                                         cohortIds,
                                         databaseIds) {
  cohortCountResult <- getResultsCohortCountFromAny(
    csvFile = csvFile,
    cohortIds = cohortIds,
    databaseIds = databaseIds
  )
  return(cohortCountResult)
}


#' Get cohort counts from premerged file with data in results data model format
#'
#' @description
#' Given a set of one or more cohortIds and one or more databaseIds,
#' return data from the cohort_count table in results data model
#' with data filtered to the cohortIds and databaseIds.
#'
#' @param premergedFile  The full path to read the premerged file.
#'
#' @param cohortIds  One or more cohort ids to returns cohort counts
#'
#' @param databaseIds  One or more database ids to returns cohort counts
#'
#' @return
#' Returns a tibble dataframe
#' @export
getResultsCohortCountFromPremergedFile <- function(premergedFile,
                                                   cohortIds,
                                                   databaseIds) {
  cohortCountResult <-
    getResultsCohortCountFromAny(
      premergedFile = premergedFile,
      cohortIds = cohortIds,
      databaseIds = databaseIds
    )
  return(cohortCountResult)
}


###################################################################################
###################################################################################
################# Get Time Series from results data model ########################
###################################################################################
###################################################################################

# private function - not exported
getResultsTimeSeriesFromAny <- function(dataSource = NULL,
                                        connectionDetails = NULL,
                                        connection = NULL,
                                        premergedFile = NULL,
                                        csvFile = NULL,
                                        cohortIds,
                                        databaseIds) {
  timeSeriesResult <-
    getDataForDatabaseIdsCohortIds(
      dataSource = dataSource,
      connectionDetails = connectionDetails,
      connection = connection,
      premergedFile = premergedFile,
      csvFile = csvFile,
      cohortIds = cohortIds,
      databaseIds = databaseIds,
      dataTableName = 'timeSeries'
    )
  timeSeriesResult <- timeSeriesResult %>% 
    dplyr::arrange(.data$cohortId,.data$databaseId, .data$calendarInterval, .data$seriesType, .data$periodBegin) %>% 
    tsibble::as_tsibble(key = c(.data$cohortId,.data$databaseId, .data$seriesType, .data$calendarInterval), 
                        index = .data$periodBegin)
  return(timeSeriesResult)
}

# private function - not exported
# this function is for use with R shiny
getResultsTimeSeriesFromEnvironment <-
  function(dataSource,
           cohortIds,
           databaseIds) {
    timeSeriesResult <-
      getResultsTimeSeriesFromAny(dataSource = dataSource,
                                  cohortIds = cohortIds,
                                  databaseIds = databaseIds)
    return(timeSeriesResult)
  }

#' Get cohort counts from Database with data in results data model format
#'
#' @description
#' Given a set of one or more cohortIds and one or more databaseIds,
#' return data from the cohort_count table in results data model
#' with data filtered to the cohortIds and databaseIds.
#'
#' @template Connection
#'
#' @param cohortIds  One or more cohort ids to returns cohort counts
#'
#' @param databaseIds  One or more database ids to returns cohort counts
#'
#' @return
#' Returns a tibble dataframe
#' @export
getResultsTimeSeriesFromDatabase <-
  function(connectionDetails = NULL,
           connection = NULL,
           cohortIds,
           databaseIds) {
    timeSeriesResult <-
      getResultsTimeSeriesFromAny(
        connectionDetails = connectionDetails,
        connection = connection,
        cohortIds = cohortIds,
        databaseIds = databaseIds
      )
    return(timeSeriesResult)
  }



#' Get cohort counts from csv file with data in results data model format
#'
#' @description
#' Given a set of one or more cohortIds and one or more databaseIds,
#' return data from the cohort_count table in results data model
#' with data filtered to the cohortIds and databaseIds.
#'
#' @param csvFile  The full path to read the csv file.
#'
#' @param cohortIds  One or more cohort ids to returns cohort counts
#'
#' @param databaseIds  One or more database ids to returns cohort counts
#'
#' @return
#' Returns a tibble dataframe
#' @export
getResultsTimeSeriesFromCsv <- function(csvFile,
                                        cohortIds,
                                        databaseIds) {
  timeSeriesResult <- getResultsTimeSeriesFromAny(
    csvFile = csvFile,
    cohortIds = cohortIds,
    databaseIds = databaseIds
  )
  return(timeSeriesResult)
}


#' Get cohort counts from premerged file with data in results data model format
#'
#' @description
#' Given a set of one or more cohortIds and one or more databaseIds,
#' return data from the cohort_count table in results data model
#' with data filtered to the cohortIds and databaseIds.
#'
#' @param premergedFile  The full path to read the premerged file.
#'
#' @param cohortIds  One or more cohort ids to returns cohort counts
#'
#' @param databaseIds  One or more database ids to returns cohort counts
#'
#' @return
#' Returns a tibble dataframe
#' @export
getResultsTimeSeriesFromPremergedFile <- function(premergedFile,
                                                  cohortIds,
                                                  databaseIds) {
  timeSeriesResult <-
    getResultsTimeSeriesFromAny(
      premergedFile = premergedFile,
      cohortIds = cohortIds,
      databaseIds = databaseIds
    )
  return(timeSeriesResult)
}