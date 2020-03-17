#' @param packageName         The name of the package containing the cohort definitions. Can be left NULL if 
#'                            \code{baseUrl} and \code{cohortSetReference} have been specified.
#' @param cohortToCreateFile  The location of the cohortToCreate file within the package. Is ignored if 
#'                            \code{baseUrl} and \code{cohortSetReference} have been specified.
#' @param baseUrl             The base URL for the WebApi instance, for example:
#'                            "http://server.org:80/WebAPI". Can be left NULL if 
#'                            \code{packageName} and \code{cohortToCreateFile} have been specified.      
#' @param cohortSetReference  A data frame with four columns, as described in the details. Can be left NULL if 
#'                            \code{packageName} and \code{cohortToCreateFile} have been specified.  
#' 
#' @details 
#' Currently two ways of executing this function are supported, either (1) embedded in a study package, assuming 
#' the cohort definitions are stored in that package using the \code{ROhdsiWebApi::insertCohortDefinitionSetInPackage},
#' or (2) by using a WebApi interface to retrieve the cohort definitions.
#' 
#' When using this function from within a study package, use the \code{packageName} and \code{cohortToCreateFile} to specify
#' the name of the study package, and the name of the cohortToCreate file within that package, respectively
#' 
#' When using this function using a WebApi interface, use the \code{baseUrl} and \code{cohortSetReference} to specify how to 
#' connect to the WebApi, and which cohorts to fetch, respectively. 
