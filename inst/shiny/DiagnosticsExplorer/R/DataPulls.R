queryRenderedSqlFromDatabase <- function(connectionDetails = NULL,
                                         connection = NULL, 
                                         sql) {
  
  # Set up connection to server ----------------------------------------------------
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }
  
  data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                     sql = sql,
                                                     snakeCaseToCamelCase = TRUE) %>% 
    dplyr::tibble()
  return(data)
}