#' @param incremental `TRUE` or `FALSE` (default). If TRUE diagnostics for cohorts in the 
#' cohort definition set that have not changed will be skipped and existing results 
#' csv files will be updated. If FALSE then diagnostics for all cohorts in the cohort 
#' definition set will be executed and pre-existing results files will be deleted.
#' @param incrementalFolder If \code{incremental = TRUE}, specify a folder where records are kept
#'                          of which cohort diagnostics has been executed.
