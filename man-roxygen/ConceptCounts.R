#' @param conceptCountsDatabaseSchema   Schema name where your concept counts table resides. Note that
#'                                      for SQL Server, this should include both the database and
#'                                      schema name, for example 'scratch.dbo'. Ignored if
#'                                      \code{conceptCountsTableIsTemp = TRUE}.
#' @param conceptCountsTable            Name of the concept counts table. This table can be created
#'                                      using the \code{\link{createConceptCountsTable}}.
#' @param conceptCountsTableIsTemp      Is the concept counts table a temp table?                                     
