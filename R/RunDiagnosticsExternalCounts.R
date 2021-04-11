# Copyright 2021 Observational Health Data Sciences and Informatics
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
#' Runs  cohort diagnostics on all (or a subset of) the cohorts, but using external concept counts. The external counts 
#' must have the following columns:
#' 
#' \describe{
#' \item{concept_id}{The source or target concept ID.}
#' \item{concept_count}{The number of records having the concept.}
#' \item{concept_subjects}{The number of unique persons having the concept.}
#' }
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template OracleTempSchema
#'
#' @template CohortSetSpecs
#' 
#' @template CohortSetReference
#' 
#' @param conceptCountsDatabaseSchema Schema name where your concept counts table resides. Note that
#'                                    for SQL Server, this should include both the database and
#'                                    schema name, for example 'scratch.dbo'. Ignored if
#'                                    \code{conceptCountsTableIsTemp = TRUE}.
#' @param conceptCountsTable          Name of the concept counts table. 
#' @param conceptCountsTableIsTemp    Is the concept counts table a temp table?   
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
# Disabling because currently doesn't work
# runCohortDiagnosticsUsingExternalCounts <- function(packageName = NULL,
#                                                     cohortToCreateFile = "settings/CohortsToCreate.csv",
#                                                     baseUrl = NULL,
#                                                     cohortSetReference = NULL,
#                                                     connectionDetails = NULL,
#                                                     connection = NULL,
#                                                     cdmDatabaseSchema,
#                                                     oracleTempSchema = NULL,
#                                                     cohortIds = NULL,
#                                                     conceptCountsDatabaseSchema = cdmDatabaseSchema,
#                                                     conceptCountsTable = "concept_counts",
#                                                     conceptCountsTableIsTemp = FALSE,
#                                                     exportFolder,
#                                                     databaseId,
#                                                     databaseName = databaseId,
#                                                     databaseDescription = "",
#                                                     runIncludedSourceConcepts = TRUE,
#                                                     runOrphanConcepts = TRUE,
#                                                     minCellCount = 5) {
#   if (is.null(packageName) && is.null(baseUrl)) {
#     stop("Must provide either packageName and cohortToCreateFile, or baseUrl and cohortSetReference")
#   }
#   if (!is.null(cohortSetReference)) {
#     if (is.null(cohortSetReference$atlasId))
#       stop("cohortSetReference must contain atlasId field")
#     if (is.null(cohortSetReference$atlasName))
#       stop("cohortSetReference must contain atlasName field")
#     if (is.null(cohortSetReference$cohortId))
#       stop("cohortSetReference must contain cohortId field")
#     if (is.null(cohortSetReference$name))
#       stop("cohortSetReference must contain name field")
#   }
#   
#   start <- Sys.time()
#   if (!file.exists(exportFolder)) {
#     dir.create(exportFolder)
#   }
#   
#   if (is.null(connection)) {
#     connection <- DatabaseConnector::connect(connectionDetails)
#     on.exit(DatabaseConnector::disconnect(connection))
#   }
#   
#   cohorts <- getCohortsJsonAndSql(packageName = packageName,
#                                   cohortToCreateFile = cohortToCreateFile,
#                                   baseUrl = baseUrl,
#                                   cohortSetReference = cohortSetReference,
#                                   cohortIds = cohortIds)
#   
#   writeToCsv(cohorts, file.path(exportFolder, "cohort.csv"))
#   
#   ParallelLogger::logInfo("Saving database metadata")
#   database <- data.frame(databaseId = databaseId,
#                          databaseName = databaseName,
#                          description = databaseDescription,
#                          isMetaAnalysis = 0)
#   writeToCsv(database, file.path(exportFolder, "database.csv"))
#   if (runIncludedSourceConcepts || runOrphanConcepts) {
#     runConceptSetDiagnostics(connection = connection,
#                              oracleTempSchema = oracleTempSchema,
#                              cdmDatabaseSchema = cdmDatabaseSchema,
#                              databaseId = databaseId,
#                              cohorts = cohorts,
#                              runIncludedSourceConcepts = runIncludedSourceConcepts,
#                              runOrphanConcepts = runOrphanConcepts,
#                              exportFolder = exportFolder,
#                              minCellCount = minCellCount,
#                              conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
#                              conceptCountsTable = conceptCountsTable,
#                              conceptCountsTableIsTemp = conceptCountsTableIsTemp,
#                              useExternalConceptCountsTable = TRUE)
#   }
#   
#   # Add all to zip file -------------------------------------------------------------------------------
#   ParallelLogger::logInfo("Adding results to zip file")
#   zipName <- file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
#   files <- list.files(exportFolder, pattern = ".*\\.csv$")
#   oldWd <- setwd(exportFolder)
#   on.exit(setwd(oldWd), add = TRUE)
#   DatabaseConnector::createZipFile(zipFile = zipName, files = files)
#   ParallelLogger::logInfo("Results are ready for sharing at:", zipName)
#   
#   delta <- Sys.time() - start
#   ParallelLogger::logInfo(paste("Computing all diagnostics took",
#                                 signif(delta, 3),
#                                 attr(delta, "units")))
# }

