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

#' Initialize diagnostics
#'
#' @description
#' Initializes shared resources and validates inputs for cohort diagnostics.
#'
#' @param context              A DiagnosticsContext object.
#' @param cohortDefinitionSet  Data.frame of cohorts must include columns cohortId, cohortName, json, sql
#' @param cohortIds            Optionally, provide a subset of cohort IDs to restrict the diagnostics to.
#'
#' @return The modified DiagnosticsContext object.
#' @export
initializeDiagnostics <- function(context,
                                  cohortDefinitionSet,
                                  cohortIds = NULL) {
  checkmate::assertClass(context, "DiagnosticsContext")

  start <- Sys.time()
  context$startTime <- start
  ParallelLogger::logInfo("Run Cohort Diagnostics started at ", start)

  # Set up loggers if not already present
  ParallelLogger::unregisterLogger("CD_LOGGER", silent = TRUE)
  ParallelLogger::addDefaultFileLogger(file.path(context$exportFolder, "log.txt"), name = "CD_LOGGER")

  ParallelLogger::unregisterLogger("CD_ERROR_LOGGER", silent = TRUE)
  ParallelLogger::addDefaultErrorReportLogger(file.path(context$exportFolder, "errorReportR.txt"), name = "CD_ERROR_LOGGER")

  # Validate cohortDefinitionSet
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertDataFrame(cohortDefinitionSet, add = errorMessage)
  checkmate::assertNames(names(cohortDefinitionSet),
    must.include = c("json", "cohortId", "cohortName", "sql"),
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)

  if (!"isSubset" %in% colnames(cohortDefinitionSet)) {
    cohortDefinitionSet$isSubset <- FALSE
  }

  # Filter by cohortIds
  if (!is.null(cohortIds)) {
    cohortDefinitionSet <- cohortDefinitionSet %>% dplyr::filter(.data$cohortId %in% cohortIds)
  }

  if (nrow(cohortDefinitionSet) == 0) {
    stop("No cohorts specified")
  }

  # Further validation of cohortDefinitionSet columns
  cohortTableColumnNamesObserved <- colnames(cohortDefinitionSet) %>% sort()
  cohortTableColumnNamesExpected <- getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == "cohort") %>%
    dplyr::pull(.data$columnName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    sort()
  cohortTableColumnNamesRequired <- getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == "cohort") %>%
    dplyr::filter(.data$isRequired == "Yes") %>%
    dplyr::pull(.data$columnName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    sort()

  expectedButNotObsevered <- setdiff(x = cohortTableColumnNamesExpected, y = cohortTableColumnNamesObserved)
  if (length(expectedButNotObsevered) > 0) {
    requiredButNotObsevered <- setdiff(x = cohortTableColumnNamesRequired, y = cohortTableColumnNamesObserved)
    if (length(requiredButNotObsevered) > 0) {
      stop(paste("The following required fields not found in cohort table:", paste0(requiredButNotObsevered, collapse = ", ")))
    }
  }

  obseveredButNotExpected <- setdiff(x = cohortTableColumnNamesObserved, y = cohortTableColumnNamesExpected)
  if (length(obseveredButNotExpected) > 0) {
    ParallelLogger::logInfo(paste0(
      "The following fields found in the cohortDefinitionSet will be exported in JSON format as part of metadata field of cohort table:\n    ",
      paste0(obseveredButNotExpected, collapse = ",\n    ")
    ))
  }

  # Prepare folders
  createIfNotExist(type = "folder", name = context$exportFolder)
  if (context$incremental) {
    createIfNotExist(type = "folder", name = context$incrementalFolder)
  }

  # Export cohort metadata
  cohortDefinitionSetForExport <- makeDataExportable(
    x = cohortDefinitionSet,
    tableName = "cohort",
    minCellCount = context$minCellCount,
    databaseId = NULL
  )
  writeToCsv(data = cohortDefinitionSetForExport, fileName = file.path(context$exportFolder, "cohort.csv"))

  subsets <- CohortGenerator::getSubsetDefinitions(cohortDefinitionSet)
  if (length(subsets)) {
    dfs <- lapply(subsets, function(x) {
      data.frame(subsetDefinitionId = x$definitionId, json = as.character(x$toJSON()))
    })
    subsetDefinitions <- dplyr::bind_rows(dfs)
    writeToCsv(data = subsetDefinitions, fileName = file.path(context$exportFolder, "subset_definition.csv"))
  }

  # Connection Management
  if (is.null(context$connection)) {
    if (!is.null(context$connectionDetails)) {
      context$connection <- DatabaseConnector::connect(context$connectionDetails)
      context$createdConnection <- TRUE
    } else {
      stop("No connection or connectionDetails provided.")
    }
  } else {
    context$createdConnection <- FALSE
  }

  # CDM Source Information
  timeExecution(
    context$exportFolder,
    taskName = "getCdmDataSourceInformation",
    cohortIds = NULL,
    parent = "initializeDiagnostics",
    expr = {
      cdmSourceInformation <- getCdmDataSourceInformation(
        connection = context$connection,
        cdmDatabaseSchema = context$cdmDatabaseSchema
      )

      if (!is.null(cdmSourceInformation)) {
        if (any(is.null(context$databaseName), is.na(context$databaseName))) {
          context$databaseName <- cdmSourceInformation$cdmSourceName
        }
        if (any(is.null(context$databaseDescription), is.na(context$databaseDescription))) {
          context$databaseDescription <- cdmSourceInformation$sourceDescription
        }
      } else {
        if (any(is.null(context$databaseName), is.na(context$databaseName))) {
          context$databaseName <- context$databaseId
        }
        if (any(is.null(context$databaseDescription), is.na(context$databaseDescription))) {
          context$databaseDescription <- context$databaseName
        }
      }
      vocabularyVersion <- getVocabularyVersion(context$connection, context$vocabularyDatabaseSchema)
    }
  )

  # Checksums for incremental
  cohortDefinitionSet$checksum <- computeChecksum(cohortDefinitionSet$sql)

  # Observation Period Date Range
  ParallelLogger::logTrace(" - Collecting date range from Observational period table.")
  timeExecution(
    context$exportFolder,
    taskName = "observationPeriodDateRange",
    cohortIds = NULL,
    parent = "initializeDiagnostics",
    expr = {
      observationPeriodDateRange <- renderTranslateQuerySql(
        connection = context$connection,
        sql = "SELECT MIN(observation_period_start_date) observation_period_min_date,
             MAX(observation_period_end_date) observation_period_max_date,
             COUNT(distinct person_id) persons,
             COUNT(person_id) records,
             SUM(CAST(DATEDIFF(dd, observation_period_start_date, observation_period_end_date) AS BIGINT)) person_days
             FROM @cdm_database_schema.observation_period;",
        cdm_database_schema = context$cdmDatabaseSchema,
        snakeCaseToCamelCase = TRUE,
        tempEmulationSchema = context$tempEmulationSchema
      )
    }
  )

  # Save database metadata
  saveDatabaseMetaData(
    databaseId = context$databaseId,
    databaseName = context$databaseName,
    databaseDescription = context$databaseDescription,
    exportFolder = context$exportFolder,
    minCellCount = context$minCellCount,
    vocabularyVersionCdm = cdmSourceInformation$vocabularyVersion,
    vocabularyVersion = vocabularyVersion
  )

  # Create concept table
  createConceptTable(context$connection, context$tempEmulationSchema)

  # Store needed values in context for finalization
  context$cdmSourceInformation <- cdmSourceInformation
  context$observationPeriodDateRange <- observationPeriodDateRange
  context$vocabularyVersion <- vocabularyVersion

  # Cohort counts (essential for deciding which cohorts to run diagnostics on)
  timeExecution(
    context$exportFolder,
    taskName = "computeCohortCounts",
    cohortIds = cohortIds,
    parent = "initializeDiagnostics",
    expr = {
      cohortCounts <- computeCohortCounts(
        connection = context$connection,
        cohortDatabaseSchema = context$cohortDatabaseSchema,
        cohortTable = context$cohortTable,
        cohorts = cohortDefinitionSet,
        exportFolder = context$exportFolder,
        minCellCount = context$minCellCount,
        databaseId = context$databaseId
      )
    }
  )

  if (nrow(cohortCounts) > 0) {
    instantiatedCohorts <- cohortCounts %>%
      dplyr::filter(.data$cohortEntries > 0) %>%
      dplyr::pull(.data$cohortId)
  } else {
    stop("All cohorts were either not instantiated or all have 0 records.")
  }

  context$cohortDefinitionSet <- cohortDefinitionSet %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts)
  context$instantiatedCohorts <- instantiatedCohorts
  context$cohortCounts <- cohortCounts
  context$isInitialized <- TRUE

  return(context)
}
