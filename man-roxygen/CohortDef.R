#' @param baseUrl          The base URL for the WebApi instance, for example:
#'                         "http://server.org:80/WebAPI". Needn't be provided if \code{cohortJson} and
#'                         \code{cohortSql} are provided.
#' @param webApiCohortId   The ID of the cohort in the WebAPI instance. Needn't be provided if
#'                         \code{cohortJson} and \code{cohortSql} are provided.
#' @param cohortJson       A character string containing the JSON of a cohort definition. Needn't be
#'                         provided if \code{baseUrl} and \code{cohortId} are provided.
#' @param cohortSql        The OHDSI SQL representation of the same cohort definition. Needn't be
#'                         provided if \code{baseUrl} and \code{cohortId} are provided.
