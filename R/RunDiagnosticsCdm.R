# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
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

#' executeDiagnosticsCdm
#'
#' @param cdm                         Cdm reference object
#' @param cohortDefinitionSet         Data.frame of cohorts must include columns cohortId, cohortName, json, sql 
#' @param cohortTable                 Cohort table name
#' @param conceptCountsTable          Concept counts table name
#' @param exportFolder                The folder where the output will be exported to. If this folder does not exist it will be created.
#' @param minCellCount                The minimum cell count for fields contains person counts or fractions.
#' @param runInclusionStatistics      Generate and export statistic on the cohort inclusion rules?
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runTimeSeries               Generate and export the time series diagnostics?
#' @param runVisitContext             Generate and export index-date visit context?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#' @param runIncidenceRate            Generate and export the cohort incidence  rates?
#' @param runCohortRelationship       Generate and export the cohort relationship? Cohort relationship checks the temporal
#'                                    relationship between two or more cohorts.
#' @param runTemporalCohortCharacterization   Generate and export the temporal cohort characterization?
#'                                            Only records with values greater than 0.001 are returned.
#' @param useExternalConceptCountsTable if external concept counts table should be used
#' 
#' @examples
#' \dontrun{
#' cohortTable <- "mycohort"
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
#' cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")
#' cohortDefinitionSet <- CDMConnector::readCohortSet(system.file("cohorts", package = "CohortDiagnostics"))
#' cdm <- generateCohortSet(cdm, cohortDefinitionSet, name = cohortTable)
#' executeDiagnosticsCdm(cdm = cdm,
#'  cohortDefinitionSet = cohortDefinitionSet,
#'  cohortTable = cohortTable,
#'  exportFolder = "output",
#'  minCellCount = 5,
#'  runInclusionStatistics = T,
#'  runIncludedSourceConcepts = T,
#'  runOrphanConcepts = T,
#'  runTimeSeries = T,
#'  runVisitContext = T,
#'  runBreakdownIndexEvents = T,
#'  runIncidenceRate = T,
#'  runCohortRelationship = T,
#'  runTemporalCohortCharacterization = T,
#'  useExternalConceptCountsTable = F)
#' }
#' 
#' @export
executeDiagnosticsCdm <- function(cdm,
                                  cohortDefinitionSet,
                                  cohortTable = "cohort",
                                  conceptCountsTable = "#concept_counts",
                                  exportFolder,
                                  minCellCount = 5,
                                  runInclusionStatistics = TRUE,
                                  runIncludedSourceConcepts = TRUE,
                                  runOrphanConcepts = TRUE,
                                  runTimeSeries = TRUE,
                                  runVisitContext = TRUE,
                                  runBreakdownIndexEvents = TRUE,
                                  runIncidenceRate = TRUE,
                                  runCohortRelationship = TRUE,
                                  runTemporalCohortCharacterization = TRUE,
                                  useExternalConceptCountsTable = TRUE) {
  
  executeDiagnostics(cohortDefinitionSet,
                     connectionDetails = NULL,
                     connection = attr(cdm, "dbcon"),
                     cdmVersion = floor(as.numeric(CDMConnector::version(cdm))),
                     cohortTable = cohortTable,
                     conceptCountsTable = conceptCountsTable,
                     cohortDatabaseSchema = attr(cdm, "write_schema"),
                     cdmDatabaseSchema = attr(cdm, "cdm_schema"),
                     exportFolder = exportFolder,
                     databaseId = attr(cdm, "cdm_name"),
                     minCellCount = minCellCount,
                     runInclusionStatistics = runInclusionStatistics,
                     runIncludedSourceConcepts = runIncludedSourceConcepts,
                     runOrphanConcepts = runIncludedSourceConcepts,
                     runTimeSeries = runIncludedSourceConcepts,
                     runVisitContext = runVisitContext,
                     runBreakdownIndexEvents = runBreakdownIndexEvents,
                     runIncidenceRate = runIncidenceRate,
                     runCohortRelationship = runCohortRelationship,
                     runTemporalCohortCharacterization = runTemporalCohortCharacterization,
                     useExternalConceptCountsTable = useExternalConceptCountsTable)
}
