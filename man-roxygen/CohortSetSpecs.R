#' @param packageName         The name of the package containing the cohort definitions. Can be left NULL if 
#'                            \code{baseUrl} and \code{cohortSetReference} have been specified.
#' @param cohortToCreateFile  The location of the cohortToCreate file within the package. Is ignored if 
#'                            \code{baseUrl} and \code{cohortSetReference} have been specified (i.e. webapi mode 
#'                            takes precedence).
#' @param baseUrl             The base URL for the WebApi instance, for example:
#'                            "http://server.org:80/WebAPI". Can be left NULL if 
#'                            \code{packageName} and \code{cohortToCreateFile} have been specified.      
#' @param cohortSetReference  A data frame with four columns, as described in the details. Can be left NULL if 
#'                            \code{packageName} and \code{cohortToCreateFile} have been specified.  
#' 
#' @details 
#' Currently two ways of executing this function are supported, either 
#' (1) [Package Mode] embedded in a study package, e.g. by hydrating a the \code{SkeletonCohortDiagnosticsStudy} package using \code{Hydra::hydrate} or 
#'      inserting cohort specifications using \code{ROhdsiWebApi::insertCohortDefinitionSetInPackage}, or 
#' (2) [WebApi Mode] By using a WebApi interface to retrieve the cohort definitions. Note: WebAPi mode takes precedence over package mode.
#' 
#' Structure of \code{cohortSetReference} or \code{cohortToCreateFile}
#' \describe{
#' \item{cohortId}{(required) The cohort Id in Atlas for the cohort you want to diagnose.}
#' \item{cohortName}{(optional) The full name of the cohort. This will be shown in the Shiny app. If not provided, the 
#'  name used in Atlas will be displayed.}
#' }
#' 
#' In addition - \code{cohortToCreateFile} is able to accept additional optional columns
#' \describe{
#' \item{cohortId}{(required) The cohort Id in Atlas for the cohort you want to diagnose.}
#' \item{cohortName}{(optional) The full name of the cohort. This will be shown in the Shiny app. If not provided, the 
#'  name used in Atlas will be displayed.}
#' \item{metaData}{(optional) A JSON with metadata of the cohort that you would like to provided. 
#' Logic description may be a metadata object. Other types of metadata objects may include project code, author, version, key words etc.}
#' }
#'  
#' When using this function in Package Mode: Use the \code{packageName} and \code{cohortToCreateFile} to specify
#' the name of the study package, and the name of the cohortToCreate file within that package, respectively
#' 
#' When using this function in WebApi Mode: use the \code{baseUrl} and \code{cohortSetReference} to specify how to 
#' connect to the WebApi, and which cohorts to fetch, respectively.
