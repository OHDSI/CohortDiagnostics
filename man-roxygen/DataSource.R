#' @param dataSource           A list object that is the output of \code{createDatabaseDataSource} or \code{createFileDataSource} function. 
#'                             This object helps direct the function to query data from the database (created by \code{createDatabaseDataSource})
#'                             or a local premerged file (created by \code{createFileDataSource}). Premerged files are output of cohortDiagnostics
#'                             compiled into RData using \code{preMergeDiagnosticsFiles}. Database DataSources are data inserted into a remote
#'                             database (only a postgres database is supported) with tables created with DDL function \code{createResultsDataModel}
#'                             and uploaded using \code{uploadResults}
