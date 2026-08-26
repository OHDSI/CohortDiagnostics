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

#' Finalize diagnostics
#'
#' @description
#' Performs cleanup and exports metadata for cohort diagnostics.
#'
#' @param context A DiagnosticsContext object.
#'
#' @return The DiagnosticsContext object (invisible)
#' @export
finalizeDiagnostics <- function(context) {
  checkmate::assertClass(context, "DiagnosticsContext")

  if (!context$isInitialized) {
    stop("Diagnostics context not initialized. Call initializeDiagnostics() first.")
  }

  # Export concept information
  timeExecution(
    context$exportFolder,
    "exportConceptInformation",
    parent = "finalizeDiagnostics",
    expr = {
      exportConceptInformation(
        connection = context$connection,
        vocabularyDatabaseSchema = context$vocabularyDatabaseSchema,
        tempEmulationSchema = context$tempEmulationSchema,
        conceptIdTable = "#concept_ids",
        incremental = context$incremental,
        exportFolder = context$exportFolder
      )
    }
  )

  # Delete concept ID table
  ParallelLogger::logTrace("Deleting concept ID table")
  timeExecution(
    context$exportFolder,
    "DeleteConceptIdTable",
    parent = "finalizeDiagnostics",
    expr = {
      sql <- "TRUNCATE TABLE @table;\nDROP TABLE @table;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection = context$connection,
        sql = sql,
        tempEmulationSchema = context$tempEmulationSchema,
        table = "#concept_ids",
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
  )

  # Write metadata file
  ParallelLogger::logInfo("Retrieving metadata information and writing metadata")

  packageName <- utils::packageName()
  packageVersion <- if (!methods::getPackageName() == ".GlobalEnv") {
    as.character(utils::packageVersion(packageName))
  } else {
    ""
  }

  delta <- Sys.time() - context$startTime

  variableField <- c(
    "timeZone", "runTime", "runTimeUnits", "packageDependencySnapShotJson",
    "argumentsAtDiagnosticsInitiationJson", "rversion", "currentPackage",
    "currentPackageVersion", "sourceDescription", "cdmSourceName",
    "sourceReleaseDate", "cdmVersion", "cdmReleaseDate", "vocabularyVersion",
    "datasourceName", "datasourceDescription", "vocabularyVersionCdm",
    "observationPeriodMinDate", "observationPeriodMaxDate", "personsInDatasource",
    "recordsInDatasource", "personDaysInDatasource"
  )

  valueField <- c(
    as.character(Sys.timezone()),
    as.character(as.numeric(delta, units = attr(delta, "units"))),
    as.character(attr(delta, "units")),
    "{}",
    as.character(if (!is.null(context$callingArgsJson)) context$callingArgsJson else "{}"),
    as.character(R.Version()$version.string),
    as.character(nullToEmpty(packageName)),
    as.character(nullToEmpty(packageVersion)),
    as.character(nullToEmpty(context$cdmSourceInformation$sourceDescription)),
    as.character(nullToEmpty(context$cdmSourceInformation$cdmSourceName)),
    as.character(nullToEmpty(context$cdmSourceInformation$sourceReleaseDate)),
    as.character(nullToEmpty(context$cdmSourceInformation$cdmVersion)),
    as.character(nullToEmpty(context$cdmSourceInformation$cdmReleaseDate)),
    as.character(nullToEmpty(context$vocabularyVersion)),
    as.character(context$databaseName),
    as.character(context$databaseDescription),
    as.character(nullToEmpty(context$vocabularyVersion)),
    as.character(context$observationPeriodDateRange$observationPeriodMinDate),
    as.character(context$observationPeriodDateRange$observationPeriodMaxDate),
    as.character(context$observationPeriodDateRange$persons),
    as.character(context$observationPeriodDateRange$records),
    as.character(context$observationPeriodDateRange$personDays)
  )

  metadata <- dplyr::tibble(
    databaseId = as.character(context$databaseId),
    startTime = paste0("TM_", as.character(context$startTime)),
    variableField = variableField,
    valueField = valueField
  )

  metadata <- makeDataExportable(
    x = metadata,
    tableName = "metadata",
    minCellCount = context$minCellCount,
    databaseId = context$databaseId
  )

  writeToCsv(
    data = metadata,
    fileName = file.path(context$exportFolder, "metadata.csv"),
    incremental = TRUE,
    start_time = as.character(context$startTime)
  )

  # Zip results
  timeExecution(
    context$exportFolder,
    "writeResultsZip",
    NULL,
    parent = "finalizeDiagnostics",
    expr = {
      writeResultsZip(context$exportFolder, context$databaseId)
    }
  )

  ParallelLogger::logInfo(
    "Computing all diagnostics took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )

  # Disconnect if we created the connection
  if (context$createdConnection) {
    DatabaseConnector::disconnect(context$connection)
  }

  return(invisible(context))
}
