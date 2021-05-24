cohortDefinitionId <- 14907
subjectIds <- NULL
sampleSize <- 10
# source(Sys.getenv("startUpScriptLocation")) # this sources information for cdmSources and dataSourceInformation.

library(shiny)
library(ggplot2)
library(DT)
library(plotly)
library(magrittr)

packageName <- 'SkeletonCohortDiagnosticsStudy'
databaseId <- 'truven_ccae'
tempEmulationSchema <- NULL

if (exists("cdmSources")) {
  connectionSpecifications <- cdmSources %>%
    dplyr::filter(sequence == 1) %>%
    dplyr::filter(database == databaseId)
}  

userNameService = "OHDSI_USER" # example: "this is key ring service that securely stores credentials"
passwordService = "OHDSI_PASSWORD" # example: "this is key ring service that securely stores credentials"

defaultSampleSize <- 10
defaultCohortDatabaseSchema <- paste0('scratch_', keyring::key_get(service = userNameService))
# scratch - usually something like 'scratch_grao'
defaultCohortTable <- # example: 'cohort'
  paste0("s", connectionSpecifications$sourceId, "_", packageName)
defaultServer <- connectionSpecifications$server # example: 'fdsfd.yourdatabase.yourserver.com"
defaultCdmDatabaseSchema <- connectionSpecifications$cdmDatabaseSchema # example: "cdm"
defaultPort <- connectionSpecifications$port
defaultVocabularySchema <- connectionSpecifications$vocabDatabaseSchema # example: "vocabulary"
defaultDbms <- connectionSpecifications$dbms # example: 'redshift' please change


if (exists("shinySettings")) {
  writeLines("Using user provided settings")
  if (!is.null(shinySettings$connectionDetails)) {
    stop("No connection details provided.")
  }
    
  cohortTable <- shinySettings$cohortTable
  cohortDatabaseSchema <- shinySettings$cohortDatabaseSchema
  cdmDatabaseSchema <- shinySettings$cdmDatabaseSchema
  
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
  }
  
  if (!is.null(shinySetting$sampleSize)) {
    sampleSize <- shinySettings$sampleSize
  }
  
} else {
  if (!exists("cdmSources") ||
      !exists("connectionSpecifications")) {
    writeLines("Default connection settings not available.")
  } else {
    writeLines("Using default connection settings")
    connectionDetails <- DatabaseConnector::createConnectionDetails(
      dbms = defaultDbms,
      user = keyring::key_get(service = userNameService),
      password = keyring::key_get(service = passwordService),
      port = defaultPort,
      server = defaultServer
    )
    cohortTable <- defaultCohortTable
    vocabularyDatabaseSchema <- defaultVocabularySchema
    cohortDatabaseSchema <-   defaultCohortDatabaseSchema
    cdmDatabaseSchema <- defaultCdmDatabaseSchema
  }
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
                                                           cohort_definition_id = cohortDefinitionId,
                                                           tempEmulationSchema = tempEmulationSchema)[, 1]
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
                                                     tempEmulationSchema = tempEmulationSchema,
                                                     snakeCaseToCamelCase = TRUE) %>% 
  dplyr::tibble() %>% 
  dplyr::arrange(.data$subjectId, .data$cohortStartDate)
subjectIds <- unique(cohort$subjectId)

if (nrow(cohort) == 0) {
  stop("Cohort is empty")
}
eventSql <- SqlRender::readSql("sql/GetEvents.sql")
