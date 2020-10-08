#' @param connectionDetails     (optional) An object of type \code{connectionDetails} as created using the
#'                              \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                              DatabaseConnector package. Can be left NULL if \code{connection} is
#'                              provided.
#' @param connection            (optional) An object of type \code{connection} as created using the
#'                              \code{\link[DatabaseConnector]{connect}} function in the
#'                              DatabaseConnector package. Can be left NULL if \code{connectionDetails}
#'                              is provided, in which case a new connection will be opened at the start
#'                              of the function, and closed when the function finishes.
#' @param resultsDatabaseSchema (optional) The databaseSchema where the results data model 
#'                               of cohort diagnostics is stored. This is only required when 
#'                               \code{connectionDetails} or \code{\link[DatabaseConnector]{connect}} 
#'                               is provided.
#' @details 
#' Note: The output of this function may be used to create plots or tables. This function relies 
#' on data available in Cohort Diagnostics results data model. The function will use one of the two
#' methods to connect to the results data model, 1) database mode, and 2) in-memory mode.
#' 
#' Database mode: In this mode, R will look for the results data model in a remote relational dbms. The 
#' database system should be one of the databases supported by DatabaseConnector package. If 
#' either \code{connectionDetails} or \code{\link[DatabaseConnector]{connect}} parameters are
#' populated with an argument during a function call, the connection gets set to database mode. 
#' 
#' In-memory mode: If both \code{connectionDetails} and \code{\link[DatabaseConnector]{connect}} are 
#' parameters have a NULL argument then query will be in-memory mode. R will now expect and check
#' for a data frame object available in R's memory, as required for the function. The object should be
#' an object specificed in Cohort Diagnostics results data model.
#' 
#' Objects in R's memory are expected to follow camelCase naming conventions (see HADES), while objects 
#' in a relational database system are expected to follow snake-case naming conventions.
