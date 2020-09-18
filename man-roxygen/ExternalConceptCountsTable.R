#' @param useExternalConceptCountsTableOnly      (Optional) Logical (TRUE/FALSE, with default = FALSE). 
#'                                                Do you want to only use external Concept Counts table?
#'                                                i.e. no data diagnostics will be performed on source database.
#'                                                If this is selected true, but
#'                                                 \code{externalConceptCountTableDatabaseSchema} is NULL or 
#'                                                  \code{externalConceptCountTableName} is NULL - it will lead
#'                                                to an error. 
#'                                                Only used if concept set diagnostics (orphan code or 
#'                                                included source concept) is being performed.
#' @param externalConceptCountTableDatabaseSchema (Optional) Only used if useExternalConceptCountsTable = TRUE. The 
#'                                                databaseSchema that has the external concept counts table.
#'                                                Only used if concept set diagnostics (orphan code or 
#'                                                included source concept) is being performed.
#' @param externalConceptCountTableName           (Optional) The table name that contains the external concept
#'                                                counts. 
#'                                                Only used if concept set diagnostics (orphan code or 
#'                                                included source concept) is being performed.
#'                                                The table should have the following fields:
