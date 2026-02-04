
#' @title
#' Get Migrations
#' @description
#' Checks to see if migrations are present in the database for a given table prefix
#' @family Utils
#' @noRd
getMigrations <- function(connectionHandler, resultDatabaseSettings, tablePrefix) {
  migrations <- data.frame()
  # Handle case where no migrations are present
  tryCatch({
    migrations <- connectionHandler$queryDb("SELECT * FROM @schema.@table_prefixmigration ORDER BY migration_order",
                                            snakeCaseToCamelCase = TRUE,
                                            schema = resultDatabaseSettings$schema,
                                            table_prefix = tablePrefix)
  }, error = function(err) {
    warning("Schema does not contain migrations table.")
  })

  return(migrations)
}

#' Migration present
#' @description
#' Given a data.frame of migrations check if a migration number is present
#' @family Utils
#' @noRd
migrationPresent <- function(migrations, migrationId) {
  if (nrow(migrations) == 0) {
    return(FALSE)
  }
  return(migrationId %in% migrations$migrationOrder)
}
