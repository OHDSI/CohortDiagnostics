# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of epi808CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Execute the cohort diagnostics
#'
#' @details
#' This function executes the cohort diagnostics.
#'
#' @param connectionDetails                   An object of type \code{connectionDetails} as created
#'                                            using the
#'                                            \code{\link[DatabaseConnector]{createConnectionDetails}}
#'                                            function in the DatabaseConnector package.
#' @param cdmDatabaseSchema                   Schema name where your patient-level data in OMOP CDM
#'                                            format resides. Note that for SQL Server, this should
#'                                            include both the database and schema name, for example
#'                                            'cdm_data.dbo'.
#' @param cohortDatabaseSchema                Schema name where intermediate data can be stored. You
#'                                            will need to have write privileges in this schema. Note
#'                                            that for SQL Server, this should include both the
#'                                            database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable                         The name of the table that will be created in the work
#'                                            database schema. This table will hold the exposure and
#'                                            outcome cohorts used in this study.
#' @param oracleTempSchema                    Should be used in Oracle to specify a schema where the
#'                                            user has write privileges for storing temporary tables.
#' @param outputFolder                        Name of local folder to place results; make sure to use
#'                                            forward slashes (/). Do not use a folder on a network
#'                                            drive since this greatly impacts performance.
#' @param databaseId                          A short string for identifying the database (e.g.
#'                                            'Synpuf').
#' @param databaseName                        The full name of the database (e.g. 'Medicare Claims
#'                                            Synthetic Public Use Files (SynPUFs)').
#' @param databaseDescription                 A short description (several sentences) of the database.
#' @param createCohorts                       Create the cohortTable table with the exposure and
#'                                            outcome cohorts?
#' @param runInclusionStatistics              Generate and export statistic on the cohort inclusion
#'                                            rules?
#' @param runIncludedSourceConcepts           Generate and export the source concepts included in the
#'                                            cohorts?
#' @param runOrphanConcepts                   Generate and export potential orphan concepts?
#' @param runTimeDistributions                Generate and export cohort time distributions?
#' @param runBreakdownIndexEvents             Generate and export the breakdown of index events?
#' @param runIncidenceRates                   Generate and export the cohort incidence rates?
#' @param runCohortOverlap                    Generate and export the cohort overlap?
#' @param runCohortCharacterization           Generate and export the cohort characterization?
#' @param runTemporalCohortCharacterization   Generate and export the temporal cohort characterization?
#' @param minCellCount                        The minimum number of subjects contributing to a count
#'                                            before it can be included in packaged results.
#' @param packageName                         The package to look for study specifications
#' @param cohortIds                           Optionally, provide a subset of cohort IDs to restrict the diagnostics to.
#'
#' @export
runCohortDiagnostics <- function(connectionDetails,
                                 cdmDatabaseSchema,
                                 packageName,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 oracleTempSchema = cohortDatabaseSchema,
                                 outputFolder,
                                 incrementalFolder = file.path(outputFolder, 'incrementalFolder'),
                                 databaseId = "Unknown",
                                 databaseName = "Unknown",
                                 databaseDescription = "Unknown",
                                 createCohorts = TRUE,
                                 runInclusionStatistics = TRUE,
                                 runIncludedSourceConcepts = TRUE,
                                 runOrphanConcepts = TRUE,
                                 runTimeDistributions = TRUE,
                                 runBreakdownIndexEvents = TRUE,
                                 runIncidenceRates = TRUE,
                                 runCohortOverlap = TRUE,
                                 runVisitContext = TRUE,
                                 cohortIds = NULL,
                                 runCohortCharacterization = TRUE,
                                 runTemporalCohortCharacterization = TRUE,
                                 minCellCount = 5) {
  if (!file.exists(outputFolder))
    dir.create(outputFolder, recursive = TRUE)
  
  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)
  
  if (createCohorts) {
    ParallelLogger::logInfo("Creating cohorts")
    CohortDiagnostics::instantiateCohortSet(connectionDetails = connectionDetails,
                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                            cohortDatabaseSchema = cohortDatabaseSchema,
                                            cohortTable = cohortTable,
                                            oracleTempSchema = oracleTempSchema,
                                            packageName = packageName,
                                            cohortToCreateFile = "settings/CohortsToCreate.csv",
                                            createCohortTable = TRUE,
                                            generateInclusionStats = TRUE,
                                            inclusionStatisticsFolder = outputFolder,
                                            cohortIds = cohortIds,
                                            incremental = TRUE,
                                            incrementalFolder = incrementalFolder)
    
  }
  
  if (file.exists("settings/PhenotypeDescription.csv")) {
    phenotypeDescriptionFile <- "settings/PhenotypeDescription.csv"
  } else {
    phenotypeDescriptionFile <- NULL
  }
  
  ParallelLogger::logInfo("Running study diagnostics")
  CohortDiagnostics::runCohortDiagnostics(packageName = packageName,
                                          phenotypeDescriptionFile = phenotypeDescriptionFile,
                                          connectionDetails = connectionDetails,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          oracleTempSchema = oracleTempSchema,
                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                          cohortTable = cohortTable,
                                          inclusionStatisticsFolder = outputFolder,
                                          exportFolder = file.path(outputFolder,
                                                                   "diagnosticsExport"),
                                          cohortIds = cohortIds,
                                          databaseId = databaseId,
                                          databaseName = databaseName,
                                          databaseDescription = databaseDescription,
                                          runInclusionStatistics = runInclusionStatistics,
                                          runIncludedSourceConcepts = runIncludedSourceConcepts,
                                          runOrphanConcepts = runOrphanConcepts,
                                          runTimeDistributions = runTimeDistributions,
                                          runBreakdownIndexEvents = runBreakdownIndexEvents,
                                          runIncidenceRate = runIncidenceRates,
                                          runCohortOverlap = runCohortOverlap,
                                          runVisitContext = runVisitContext,
                                          runCohortCharacterization = runCohortCharacterization,
                                          runTemporalCohortCharacterization = runTemporalCohortCharacterization,
                                          minCellCount = minCellCount,
                                          incremental = TRUE,
                                          incrementalFolder = incrementalFolder)
}
