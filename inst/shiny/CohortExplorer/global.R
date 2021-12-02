library(shiny)
library(ggplot2)
library(DT)
library(plotly)
library(magrittr)

cohortTable <- shinySettings$cohortTable
cohortDatabaseSchema <- shinySettings$cohortDatabaseSchema
cdmDatabaseSchema <- shinySettings$cdmDatabaseSchema
cohortDefinitionId <- shinySettings$cohortDefinitionId

if (!is.null(shinySettings$vocabularyDatabaseSchema)) {
  vocabularyDatabaseSchema <- shinySettings$vocabularyDatabaseSchema
} else {
  vocabularyDatabaseSchema <- shinySettings$cdmDatabaseSchema
}

if (!is.null(shinySettings$tempEmulationSchema)) {
  tempEmulationSchema <- shinySettings$tempEmulationSchema
}

if (!is.null(shinySettings$subjectIds)) {
  subjectIds <- shinySettings$subjectIds
} else {
  subjectIds <- NULL
}

if (!is.null(shinySettings$sampleSize)) {
  sampleSize <- shinySettings$sampleSize
}


connection <- DatabaseConnector::connect(connectionDetails)
onStop(function() {
  if (DBI::dbIsValid(connection)) {
    DatabaseConnector::disconnect(connection = connection)
  }
})


# take a random sample
if (is.null(subjectIds)) {
  sql <- "SELECT TOP @sample_size subject_id
          FROM (
          	SELECT DISTINCT subject_id
          	FROM @cohort_database_schema.@cohort_table
          	WHERE cohort_definition_id = @cohort_definition_id
          	) all_ids
          ORDER BY NEWID();"
  
  writeLines("Attempting to find subjects in cohort table.")
  subjectIds <- DatabaseConnector::renderTranslateQuerySql(connection = connection, 
                                                           sql = sql, 
                                                           sample_size = sampleSize,
                                                           cohort_database_schema = cohortDatabaseSchema,
                                                           cohort_table = cohortTable,
                                                           cohort_definition_id = cohortDefinitionId)[, 1]
}

if (length(subjectIds) == 0) {
  stop("No subjects found in cohort ",
       cohortDefinitionId)
}

sql <- SqlRender::readSql("sql/GetCohort.sql")
cohort <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                     sql = sql,
                                                     cohort_database_schema = cohortDatabaseSchema,
                                                     cohort_table = cohortTable,
                                                     cdm_database_schema = cdmDatabaseSchema,
                                                     cohort_definition_id = cohortDefinitionId,
                                                     subject_ids = subjectIds,
                                                     snakeCaseToCamelCase = TRUE) %>% 
  dplyr::tibble() %>% 
  dplyr::arrange(.data$subjectId, .data$cohortStartDate)
subjectIds <- unique(cohort$subjectId)

if (nrow(cohort) == 0) {
  stop("Cohort is empty")
}
eventSql <- SqlRender::readSql("sql/GetEvents.sql")
