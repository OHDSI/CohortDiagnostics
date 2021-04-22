library(shiny)
library(ggplot2)
library(DT)
library(plotly)
library(magrittr)

# Set up connection to server ----------------------------------------------------
if (is.null(shinySettings$connection)) {
  if (!is.null(shinySettings$connectionDetails)) {
    connection <- DatabaseConnector::connect(shinySettings$connectionDetails)
    on.exit(DatabaseConnector::disconnect(shinySettings$connection))
  } else {
    stop("No connection or connectionDetails provided.")
  }
}

# take a random sample
if (is.null(shinySettings$subjectIds)) {
  sql <- "SELECT TOP @sample_size subject_id
          FROM (
          	SELECT DISTINCT subject_id
          	FROM @cohort_database_schema.@cohort_table
          	WHERE cohort_definition_id = @cohort_definition_id
          	) all_ids
          ORDER BY NEWID();"

  subjectIds <- DatabaseConnector::renderTranslateQuerySql(connection = connection, 
                                                           sql = sql, 
                                                           sample_size = shinySettings$sampleSize,
                                                           cohort_database_schema = shinySettings$cohortDatabaseSchema,
                                                           cohort_table = shinySettings$cohortTable,
                                                           cohort_definition_id = shinySettings$cohortDefinitionId,
                                                           tempEmulationSchema = shinySettings$tempEmulationSchema)[, 1]
} else {
  subjectIds <- shinySettings$subjectIds
}

if (length(subjectIds) == 0) {
  stop("No subjects found in cohort ",
       shinySettings$cohortDefinitionId)
}

sql <- SqlRender::readSql("sql/GetCohort.sql")
cohort <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                     sql = sql,
                                                     cohort_database_schema = shinySettings$cohortDatabaseSchema,
                                                     cohort_table = shinySettings$cohortTable,
                                                     cdm_database_schema = shinySettings$cdmDatabaseSchema,
                                                     cohort_definition_id = shinySettings$cohortDefinitionId,
                                                     subject_ids = subjectIds,
                                                     tempEmulationSchema = shinySettings$tempEmulationSchema,
                                                     snakeCaseToCamelCase = TRUE) %>% 
  dplyr::tibble() %>% 
  dplyr::arrange(.data$subjectId, .data$cohortStartDate)
subjectIds <- unique(cohort$subjectId)

if (nrow(cohort) == 0) {
  stop("Cohort is empty")
}
eventSql <- SqlRender::readSql("sql/GetEvents.sql")
