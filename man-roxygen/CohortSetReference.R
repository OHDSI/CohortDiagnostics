#' @details 
#' The \code{cohortSetReference} argument must be a data frame with at least the following columns.These fields will be exported as is to the cohort table that is part of Cohort Diagnostics results data model. Any additional fields found will be stored as JSON object in the metadata field of the cohort table:
#' \describe{
#' \item{cohortId}{The cohort Id is the id used to identify  a cohort definition. This is required to be unique. It will be used to create file names.}
#' \item{cohortName}{The full name of the cohort. This will be shown in the Shiny app.}
#' \item{json}{The JSON cohort definition for the cohort.}
#' \item{sql}{The SQL of the cohort definition rendered from the cohort json.}
#' }
