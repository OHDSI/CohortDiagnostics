#' utility function to make sure connection is closed after usage
with_dbc_connection <- function(connection, code) {
  on.exit({
    DatabaseConnector::disconnect(connection)
  })
  eval(substitute(code), envir = connection, enclos = parent.frame())
}

tableExists <- function(connection, schema, tableName) {
  tableNames <- DatabaseConnector::getTableNames(connection, schema)
  return(tableName %in% tableNames)
}
