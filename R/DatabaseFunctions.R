# Copyright 2023 Observational Health Data Sciences and Informatics
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

#' renderTranslateExecuteSql
#'
#' @param connection 
#' @param sql 
#' @param errorReportFile 
#' @param snakeCaseToCamelCase 
#' @param oracleTempSchema 
#' @param tempEmulationSchema 
#' @param integerAsNumeric 
#' @param integer64AsNumeric 
#' @param ... 
#'
#' @return NONE
renderTranslateExecuteSql <- function(connection,
                                      sql,
                                      errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                      snakeCaseToCamelCase = FALSE,
                                      oracleTempSchema = NULL,
                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                      integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                      integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE),
                                      ...) {
  # renderSQL
  sql <- SqlRender::render(sql = sql, ...)
  # translate
  sql <- SqlRender::translate(sql = sql, targetDialect = CDMConnector::dbms(connection))
  # execute
  DBI::dbExecute(conn = connection, statement = sql)
}

#' renderTranslateQuerySql
#'
#' @param connection 
#' @param sql 
#' @param errorReportFile 
#' @param snakeCaseToCamelCase 
#' @param oracleTempSchema 
#' @param tempEmulationSchema 
#' @param integerAsNumeric 
#' @param integer64AsNumeric 
#' @param ... 
#'
#' @return the query results
renderTranslateQuerySql <- function(connection,
                                    sql,
                                    errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                    snakeCaseToCamelCase = FALSE,
                                    oracleTempSchema = NULL,
                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                    integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                    integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE),
                                    ...) {
  # renderSQL
  sql <- SqlRender::render(sql = sql, ...)
  # translate
  sql <- SqlRender::translate(sql = sql, targetDialect = CDMConnector::dbms(connection))
  # get results
  result <- DBI::dbGetQuery(conn = connection, statement = sql)
  if (snakeCaseToCamelCase) {
    colnames(result) <- SqlRender::snakeCaseToCamelCase(colnames(result))
  }
  return(result)
}

#' querySql
#'
#' @param connection 
#' @param sql 
#' @param errorReportFile 
#' @param snakeCaseToCamelCase 
#' @param integerAsNumeric 
#' @param integer64AsNumeric 
#'
#' @return
#' @export
#'
#' @examples
querySql <- function(connection,
                     sql,
                     errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                     snakeCaseToCamelCase = FALSE,
                     integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                     integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE)) {
  result <- DBI::dbGetQuery(conn = connection, statement = sql)
  if (snakeCaseToCamelCase) {
    colnames(result) <- SqlRender::snakeCaseToCamelCase(colnames(result))
  }
  return(result)
}

#' executeSql
#'
#' @param connection 
#' @param sql 
#' @param profile 
#' @param progressBar 
#' @param reportOverallTime 
#' @param errorReportFile 
#' @param runAsBatch 
#'
#' @return
#' @export
#'
#' @examples
executeSql <- function(connection,
                       sql,
                       profile = FALSE,
                       progressBar = !as.logical(Sys.getenv("TESTTHAT", unset = FALSE)),
                       reportOverallTime = TRUE,
                       errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                       runAsBatch = FALSE) {
  # execute
  DBI::dbExecute(conn = connection, statement = sql)
}

#' getDbms
#'
#' @param connection 
#'
#' @return
#' @export
#'
#' @examples
getDbms <- function(connection) {
  result <- NULL
  if ("dbms" %in% names(attributes(connection))) {
    result <- connection@dbms
  } else {
    result <- CDMConnector::dbms(connection)
  }
  return(result)
}

#' getTableNames
#'
#' @param connection 
#' @param cdmDatabaseSchema 
#'
#' @return
#' @export
#'
#' @examples
getTableNames <- function(connection, cdmDatabaseSchema) {
  result <- NULL
  if ("dbms" %in% names(attributes(connection))) {
    result <- DatabaseConnector::getTableNames(connection, cdmDatabaseSchema)
  } else {
    result <- CDMConnector::listTables(connection, cdmDatabaseSchema)
  }
  return(result)
}
