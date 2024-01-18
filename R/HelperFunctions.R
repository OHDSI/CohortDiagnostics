#' Title
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
#' @return
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
  sql <- SqlRender::render(sql = sql)
  # translate
  sql <- SqlRender::translate(sql = sql)
  # execute
  DBI::dbExecute(conn = connection, statement = sql)
}

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
  sql <- SqlRender::render(sql = sql)
  # translate
  sql <- SqlRender::translate(sql = sql)
  # get results
  DBI::dbGetQuery(conn = connection, statement = sql)
}
