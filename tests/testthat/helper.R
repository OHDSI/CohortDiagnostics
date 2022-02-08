#' utility function to make sure connection is closed after usage
with_dbc_connection <- function(connection, code) {
  on.exit({
    DatabaseConnector::disconnect(connection)
  })
  eval(substitute(code), envir = connection, enclos = parent.frame())
}

#' Only works with postgres > 9.4
.pgTableExists <- function(connection, schema, tableName) {
  return(!is.na(
    DatabaseConnector::renderTranslateQuerySql(
      connection,
      "SELECT to_regclass('@schema.@table');",
      table = tableName,
      schema = schema
    )
  )[[1]])
}

# Create a cohort definition set from test cohorts
loadTestCohorts <- function(cohortIds = NULL) {
  creationFile <- file.path("cohorts", "CohortsToCreate.csv")
  cohortDefinitionSet <- read.csv(creationFile)

  if (!is.null(cohortIds)) {
    cohortDefinitionSet <- cohortDefinitionSet %>% dplyr::filter(.data$cohortId %in% cohortIds)
  }

  cohortDefinitionSet$sql <- mapply(function(cohortId) {
    fp <- file.path("cohorts", paste0(cohortId, ".sql"))
    SqlRender::readSql(fp)
  }, cohortDefinitionSet$cohortId)

  cohortDefinitionSet$json <- mapply(function(cohortId) {
    fp <- file.path("cohorts", paste0(cohortId, ".json"))
    SqlRender::readSql(fp)
  }, cohortDefinitionSet$cohortId)

  cohortDefinitionSet
}