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

getDbms <- function(connection) {
  result <- NULL
  if ("dbms" %in% names(attributes(connection))) {
    result <- connection@dbms
  } else {
    result <- CDMConnector::dbms(connection)
  }
  return(result)
}

getTableNames <- function(connection, cdmDatabaseSchema) {
  result <- NULL
  if ("dbms" %in% names(attributes(connection))) {
    result <- DatabaseConnector::getTableNames(connection, cdmDatabaseSchema)
  } else {
    result <- CDMConnector::listTables(connection, cdmDatabaseSchema)
  }
  return(result)
}
