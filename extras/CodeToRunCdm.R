# Run CohortDiagnostics using CDMConnector

library(Eunomia)
library(CohortDiagnostics)
library(CohortGenerator)
library(CDMConnector) # needs 1.3.1

useCDMConnection <- FALSE
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cohortTable <- "mycohort"
conceptCountsTable <- "concept_counts"
outputFolder <- "export"
databaseId <- "Eunomia"
minCellCount <- 5

cohortDefinitionSet <-
  CohortGenerator::getCohortDefinitionSet(
    settingsFileName = "settings/CohortsToCreate.csv",
    jsonFolder = "cohorts",
    sqlFolder = "sql/sql_server",
    packageName = "SkeletonCohortDiagnosticsStudy",
    cohortFileNameValue = "cohortId"
  ) %>%  dplyr::tibble()

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
cdm <- cdmFromCon(con, cdmSchema = cdmDatabaseSchema, writeSchema = cohortDatabaseSchema, cdmName = databaseId)
cdm <- generateCohortSet(cdm, cohortDefinitionSet, name = cohortTable)

CohortDiagnostics::executeDiagnosticsCdm(cdm = cdm,
                                         cohortDefinitionSet = cohortDefinitionSet,
                                         cohortTable = cohortTable,
                                         exportFolder = outputFolder,
                                         minCellCount = minCellCount,
                                         runInclusionStatistics = T,
                                         runIncludedSourceConcepts = T,
                                         runOrphanConcepts = T,
                                         runTimeSeries = T,
                                         runVisitContext = T,
                                         runBreakdownIndexEvents = T,
                                         runIncidenceRate = T,
                                         runCohortRelationship = T,
                                         runTemporalCohortCharacterization = T)

# package results ----
CohortDiagnostics::createMergedResultsFile(dataFolder = outputFolder, overwrite = TRUE)
# Launch diagnostics explorer shiny app ----
CohortDiagnostics::launchDiagnosticsExplorer()
