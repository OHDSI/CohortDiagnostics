#' @details 
#' The \code{cohortSetReference} argument must be a data frame with  the following columns:
#' \describe{
#' \item{cohortId}{(required) The cohort Id is the id used to identify  a cohort definition. This is required to be unique. It will be used to create file names. It is recommended to be (referentConceptId * 1000) + a number between 3 to 999}
#' \item{cohortName}{(required) The full name of the cohort. This will be shown in the Shiny app.}
#' \item{logicDescription}{(optional) A human understandable brief description of the cohort definition. This logic does not have to a 
#' fully specified description of the cohort definition, but should provide enough context to help user understand the
#' meaning of the cohort definition}
#' \item{metaData}{(optional) A JSON with metadata of the cohort that you would like to provided. At this time these pairs are used: referentConceptId, tags, projectCode}
#' }
