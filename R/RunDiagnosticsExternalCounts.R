# Copyright 2020 Observational Health Data Sciences and Informatics
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

#' Run cohort diagnostics using external concept counts
#'
#' @description
#' Runs the cohort diagnostics on all (or a subset of) the cohorts instantiated using the
#' \code{ROhdsiWebApi::insertCohortDefinitionSetInPackage} function, but using external concept counts.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template OracleTempSchema
#'
#' @template CohortTable
#' 
#' @template ConceptCounts
#'
#' @param packageName                 The name of the package containing the cohort definitions
#' @param cohortToCreateFile          The location of the cohortToCreate file within the package.
#' @param exportFolder                The folder where the output will be exported to. If this folder
#'                                    does not exist it will be created.
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    diagnostics to.
#' @param databaseId                  A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName                The full name of the database.
#' @param databaseDescription         A short description (several sentences) of the database.
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param minCellCount                The minimum cell count for fields contains person counts or fractions.
#'
#' @export
runCohortDiagnosticsExternalCounts <- function(packageName,
                                               cohortToCreateFile = "settings/CohortsToCreate.csv",
                                               connectionDetails = NULL,
                                               connection = NULL,
                                               cdmDatabaseSchema,
                                               oracleTempSchema = NULL,
                                               cohortDatabaseSchema,
                                               cohortTable = "cohort",
                                               cohortIds = NULL,
                                               conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                               conceptCountsTable = "concept_counts",
                                               conceptCountsTableIsTemp = FALSE,
                                               exportFolder,
                                               databaseId,
                                               databaseName,
                                               databaseDescription,
                                               runIncludedSourceConcepts = TRUE,
                                               runOrphanConcepts = TRUE,
                                               minCellCount = 5) {
  start <- Sys.time()
  if (!file.exists(exportFolder)) {
    dir.create(exportFolder)
  }
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  cohorts <- loadCohortsFromPackage(packageName = packageName,
                                    cohortToCreateFile = cohortToCreateFile,
                                    cohortIds = cohortIds)
  
  ParallelLogger::logInfo("Saving database metadata")
  database <- data.frame(databaseId = databaseId,
                         databaseName = databaseName,
                         description = databaseDescription,
                         isMetaAnalysis = 0)
  writeToCsv(database, file.path(exportFolder, "database.csv"))
  if (runIncludedSourceConcepts || runOrphanConcepts) {
    runConceptSetDiagnostics(connection = connection,
                             oracleTempSchema = oracleTempSchema,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             cohortDatabaseSchema = cohortDatabaseSchema,
                             cohorts = cohorts,
                             runIncludedSourceConcepts = runIncludedSourceConcepts,
                             runOrphanConcepts = runOrphanConcepts,
                             exportFolder = exportFolder,
                             minCellCount = minCellCount,
                             conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                             conceptCountsTable = conceptCountsTable,
                             conceptCountsTableIsTemp = conceptCountsTableIsTemp,
                             useExternalConceptCountsTable = TRUE)
  }

  # Add all to zip file -------------------------------------------------------------------------------
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <- file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at:", zipName)
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing all diagnostics took",
                                signif(delta, 3),
                                attr(delta, "units")))
}

