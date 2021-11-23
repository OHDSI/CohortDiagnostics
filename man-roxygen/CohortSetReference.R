#' @details 
#' The \code{cohortSetReference} argument must be a data frame with  the following columns:
#' \describe{
#' \item{cohortId}{The cohort Id is the id used to identify  a cohort definition. This is required to be unique. It will be used to create file names. It is recommended to be (referrentConceptId * 1000) + a number between 3 to 999}
#' \item{atlasId}{Cohort Id in the webApi/atlas instance. It is a required field to run Cohort Diagnostics in WebApi mode. It is discarded in package mode.}
#' \item{cohortName}{The full name of the cohort. This will be shown in the Shiny app.}
#' \item{logicDescription}{A human understandable brief description of the cohort definition. This logic does not have to a 
#' fully specified description of the cohort definition, but should provide enough context to help user understand the
#' meaning of the cohort definition}
#' \item{referentConceptId}{A standard omop concept id that serves as the referent phenotype definition for the cohort Id (optional)}
#' }
