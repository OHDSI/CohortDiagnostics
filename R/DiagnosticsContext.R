# Copyright 2025 Observational Health Data Sciences and Informatics
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

#' Create a diagnostics context
#'
#' @description
#' Creates a context object that holds shared resources for running diagnostics.
#' This object can be passed to individual diagnostic functions.
#'
#' @param connectionDetails        An object of type \code{connectionDetails} as created using the
#'                                 \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                                 DatabaseConnector package.
#' @param connection               An object of type \code{connection} as created using the
#'                                 \code{\link[DatabaseConnector]{connect}} function in the
#'                                 DatabaseConnector package. Can be used instead of \code{connectionDetails}.
#' @param cdmDatabaseSchema        Schema name where your patient-level data in OMOP CDM format resides.
#'                                 Note that for SQL Server, this should include both the database and
#'                                 schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema     Schema name where intermediate data can be stored. Will be used to read
#'                                 cohorts from. For SQL Server, this should include both the database
#'                                 and schema name, for example 'scratch.dbo'.
#' @param cohortTable              The name of the table that contains the cohort definitions.
#' @param cohortTableNames         An optional list of cohort table names used by CohortGenerator.
#' @param vocabularyDatabaseSchema Schema name where your OMOP vocabulary resides. This is commonly the
#'                                 same as cdmDatabaseSchema. Note that for SQL Server, this should
#'                                 include both the database and schema name, for example
#'                                 'vocabulary.dbo'.
#' @param tempEmulationSchema      Some database platforms like Oracle and Impala do not truly support
#'                                 temp tables. To emulate temp tables, provide a schema with write
#'                                 privileges where temp tables can be created.
#' @param databaseId               A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName             The full name of the database.
#' @param databaseDescription      A short description of the database.
#' @param exportFolder             The folder where the output will be exported to.
#' @param minCellCount             The minimum cell count for fields containing person counts or fractions.
#' @param incremental              Create only cohort diagnostics that haven't been created before?
#' @param incrementalFolder        If \code{incremental = TRUE}, specify a folder where records are kept.
#' @param cdmVersion               The version of the CDM (default is 5).
#'
#' @return A DiagnosticsContext object (S3 list)
#' @export
createDiagnosticsContext <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     cohortDatabaseSchema,
                                     cohortTable = "cohort",
                                     cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = cohortTable),
                                     vocabularyDatabaseSchema = cdmDatabaseSchema,
                                     tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                     databaseId,
                                     databaseName = NULL,
                                     databaseDescription = NULL,
                                     exportFolder,
                                     minCellCount = 5,
                                     incremental = FALSE,
                                     incrementalFolder = file.path(exportFolder, "incremental"),
                                     cdmVersion = 5) {
  # Argument validation
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertList(cohortTableNames, null.ok = FALSE, types = "character", add = errorMessage, names = "named")
  checkmate::assertCharacter(cdmDatabaseSchema, min.len = 1, add = errorMessage)
  checkmate::assertCharacter(cohortDatabaseSchema, min.len = 1, add = errorMessage)
  checkmate::assertCharacter(databaseId, min.len = 1, add = errorMessage)
  checkmate::assertInt(cdmVersion, lower = 5, upper = 5, add = errorMessage)
  checkmate::assertInt(minCellCount, lower = 0, add = errorMessage)
  checkmate::assertLogical(incremental, add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)

  # Normalize paths
  exportFolder <- normalizePath(exportFolder, mustWork = FALSE)
  incrementalFolder <- normalizePath(incrementalFolder, mustWork = FALSE)

  context <- list(
    connectionDetails = connectionDetails,
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortTableNames = cohortTableNames,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    databaseId = as.character(databaseId),
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    exportFolder = exportFolder,
    minCellCount = minCellCount,
    incremental = incremental,
    incrementalFolder = incrementalFolder,
    cdmVersion = cdmVersion,
    # Internal state tracking
    isInitialized = FALSE,
    startTime = NULL,
    metadata = list()
  )

  class(context) <- "DiagnosticsContext"
  return(context)
}
