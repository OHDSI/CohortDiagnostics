# Copyright 2022 Observational Health Data Sciences and Informatics
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

#' Run cohort diagnostics
#'
#' @description
#' Runs the cohort diagnostics on all (or a subset of) the cohorts instantiated using the
#' \code{ROhdsiWebApi::insertCohortDefinitionSetInPackage} function. Assumes the cohorts have already been instantiated.
#'
#' Characterization:
#' If runTemporalCohortCharacterization argument is TRUE, then the following default covariateSettings object will be created
#' using \code{RFeatureExtraction::createTemporalCovariateSettings}
#' Alternatively, a covariate setting object may be created using the above as an example.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#' @template VocabularyDatabaseSchema
#' @template CohortDatabaseSchema
#' @template TempEmulationSchema
#' @template OracleTempSchema
#'
#' @template CohortTable
#'
#' @template CohortSetSpecs
#'
#' @template CohortSetReference
#' @param inclusionStatisticsFolder   The folder where the inclusion rule statistics are stored. Can be
#'                                    left NULL if \code{runInclusionStatistics = FALSE}.
#' @param exportFolder                The folder where the output will be exported to. If this folder
#'                                    does not exist it will be created.
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    diagnostics to.
#' @param cohortDefinitionSet         Data.frame of cohorts must include columns cohortId, cohortName, json, sql
#' @param cohortTableNames            Cohort Table names used by CohortGenerator package
#' @param databaseId                  A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName                The full name of the database. If NULL, defaults to databaseId.
#' @param databaseDescription         A short description (several sentences) of the database. If NULL, defaults to databaseId.
#' @template cdmVersion
#' @param runInclusionStatistics      Generate and export statistic on the cohort inclusion rules?
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runTimeDistributions        Generate and export cohort time distributions?
#' @param runVisitContext             Generate and export index-date visit context?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#' @param runIncidenceRate            Generate and export the cohort incidence  rates?
#' @param runTimeSeries               Generate and export the cohort prevalence  rates?
#' @param runCohortOverlap            Generate and export the cohort overlap? Overlaps are checked within cohortIds
#'                                    that have the same phenotype ID sourced from the CohortSetReference or
#'                                    cohortToCreateFile.
#' @param runCohortCharacterization   Generate and export the cohort characterization?
#'                                    Only records with values greater than 0.0001 are returned.
#' @param covariateSettings           Either an object of type \code{covariateSettings} as created using one of
#'                                    the createCovariateSettings function in the FeatureExtraction package, or a list
#'                                    of such objects.
#' @param runTemporalCohortCharacterization   Generate and export the temporal cohort characterization?
#'                                    Only records with values greater than 0.001 are returned.
#' @param temporalCovariateSettings   Either an object of type \code{covariateSettings} as created using one of
#'                                    the createTemporalCovariateSettings function in the FeatureExtraction package, or a list
#'                                    of such objects.
#' @param minCellCount                The minimum cell count for fields contains person counts or fractions.
#' @param incremental                 Create only cohort diagnostics that haven't been created before?
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are kept
#'                                    of which cohort diagnostics has been executed.
#'
#' @importFrom CohortGenerator getCohortTableNames
#' @export
runCohortDiagnostics <- function(packageName = NULL,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
                                 cohortDefinitionSet = NULL,
                                 baseUrl = NULL,
                                 cohortSetReference = NULL,
                                 connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 oracleTempSchema = NULL,
                                 tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                 cohortDatabaseSchema,
                                 vocabularyDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = cohortTable),
                                 cohortIds = NULL,
                                 inclusionStatisticsFolder = NULL,
                                 exportFolder,
                                 databaseId,
                                 databaseName = databaseId,
                                 databaseDescription = databaseId,
                                 cdmVersion = 5,
                                 runInclusionStatistics = TRUE,
                                 runIncludedSourceConcepts = TRUE,
                                 runOrphanConcepts = TRUE,
                                 runTimeDistributions = TRUE,
                                 runVisitContext = TRUE,
                                 runBreakdownIndexEvents = TRUE,
                                 runIncidenceRate = TRUE,
                                 runTimeSeries = FALSE,
                                 runCohortOverlap = TRUE,
                                 runCohortCharacterization = TRUE,
                                 covariateSettings = createDefaultCovariateSettings(),
                                 runTemporalCohortCharacterization = TRUE,
                                 temporalCovariateSettings = createTemporalCovariateSettings(
                                   useConditionOccurrence = TRUE,
                                   useDrugEraStart = TRUE,
                                   useProcedureOccurrence = TRUE,
                                   useMeasurement = TRUE,
                                   temporalStartDays = c(-365, -30, 0, 1, 31),
                                   temporalEndDays = c(-31, -1, 0, 30, 365)
                                 ),
                                 minCellCount = 5,
                                 incremental = FALSE,
                                 incrementalFolder = file.path(exportFolder, "incremental")) {

  exportFolder <- normalizePath(exportFolder, mustWork = FALSE)
  incrementalFolder <- normalizePath(incrementalFolder, mustWork = FALSE)

  if (!is.null(inclusionStatisticsFolder))
    inclusionStatisticsFolder <- normalizePath(inclusionStatisticsFolder, mustWork = FALSE)

  if (!is.null(cohortSetReference)) {
    ParallelLogger::logInfo("Found cohortSetReference. Cohort Diagnostics is running in WebApi mode.")
    cohortToCreateFile <- NULL
  }
  
  start <- Sys.time()
  ParallelLogger::logInfo("Run Cohort Diagnostics started at ", start)
  
  if (!is.null(oracleTempSchema) & is.null(tempEmulationSchema)) {
    tempEmulationSchema <- oracleTempSchema
    warning('OracleTempSchema has been deprecated by DatabaseConnector')
  }
  
  if (any(is.null(databaseName), is.na(databaseName))) {
    databaseName <- databaseId
  }
  if (any(is.null(databaseDescription), is.na(databaseDescription))) {
    databaseDescription <- databaseId
  }

  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertList(cohortTableNames, null.ok = FALSE, types = "character", add = errorMessage, names = "named")
  checkmate::assertNames(names(cohortTableNames),
                         must.include = c("cohortTable",
                                          "cohortInclusionTable",
                                          "cohortInclusionResultTable",
                                          "cohortInclusionStatsTable",
                                          "cohortSummaryStatsTable",
                                          "cohortCensorStatsTable"))
  cohortTable <- cohortTableNames$cohortTable
  checkmate::assertLogical(runInclusionStatistics, add = errorMessage)
  checkmate::assertLogical(runIncludedSourceConcepts, add = errorMessage)
  checkmate::assertLogical(runOrphanConcepts, add = errorMessage)
  checkmate::assertLogical(runTimeDistributions, add = errorMessage)
  checkmate::assertLogical(runBreakdownIndexEvents, add = errorMessage)
  checkmate::assertLogical(runIncidenceRate, add = errorMessage)
  checkmate::assertLogical(runCohortOverlap, add = errorMessage)
  checkmate::assertLogical(runCohortCharacterization, add = errorMessage)
  checkmate::assertInt(
    x = cdmVersion,
    na.ok = FALSE,
    lower = 5,
    upper = 5,
    null.ok = FALSE,
    add = errorMessage
  )
  minCellCount <- utils::type.convert(minCellCount, as.is = TRUE)
  checkmate::assertInteger(x = minCellCount, lower = 0, add = errorMessage)
  checkmate::assertLogical(incremental, add = errorMessage)

  if (any(
    runInclusionStatistics,
    runIncludedSourceConcepts,
    runOrphanConcepts,
    runTimeDistributions,
    runBreakdownIndexEvents,
    runIncidenceRate,
    runCohortOverlap,
    runCohortCharacterization
  )) {
    checkmate::assertCharacter(x = cdmDatabaseSchema,
                               min.len = 1,
                               add = errorMessage)
    checkmate::assertCharacter(x = vocabularyDatabaseSchema,
                               min.len = 1,
                               add = errorMessage)
    checkmate::assertCharacter(x = cohortDatabaseSchema,
                               min.len = 1,
                               add = errorMessage)
    checkmate::assertCharacter(x = cohortTable,
                               min.len = 1,
                               add = errorMessage)
    checkmate::assertCharacter(x = databaseId,
                               min.len = 1,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  errorMessage <-
    createIfNotExist(type = "folder",
                     name = exportFolder,
                     errorMessage = errorMessage)
  if (incremental) {
    errorMessage <-
      createIfNotExist(type = "folder",
                       name = incrementalFolder,
                       errorMessage = errorMessage)
  }
  if (isTRUE(runInclusionStatistics) & !is.null(inclusionStatisticsFolder)) {
    errorMessage <-
      createIfNotExist(type = "folder",
                       name = inclusionStatisticsFolder,
                       errorMessage = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)

  if (is.null(cohortDefinitionSet)) {
    warning("Loading cohorts directly in runCohortDiagnostics will be removed in a future version. See executeDiagnostics")
    cohortDefinitionSet <- getCohortsJsonAndSql(
      packageName = packageName,
      cohortToCreateFile = cohortToCreateFile,
      baseUrl = baseUrl,
      cohortSetReference = cohortSetReference,
      cohortIds = cohortIds
    )
  } else if (!is.null(cohortIds)) {
    cohortDefinitionSet <- cohortDefinitionSet %>% dplyr::filter(.data$cohortId %in% cohortIds)
  }

  if (nrow(cohortDefinitionSet) == 0) {
    stop("No cohorts specified")
  }
  if ('name' %in% colnames(cohortDefinitionSet)) {
    cohortDefinitionSet <- cohortDefinitionSet %>%
      dplyr::select(-.data$name)
  }
  cohortTableColumnNamesObserved <- colnames(cohortDefinitionSet) %>%
    sort()
  cohortTableColumnNamesExpected <-
    getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == 'cohort') %>%
    dplyr::pull(.data$fieldName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    sort()
  cohortTableColumnNamesRequired <-
    getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == 'cohort') %>%
    dplyr::filter(.data$isRequired == 'Yes') %>%
    dplyr::pull(.data$fieldName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    sort()
  
  expectedButNotObsevered <-
    setdiff(x = cohortTableColumnNamesExpected, y = cohortTableColumnNamesObserved)
  if (length(expectedButNotObsevered) > 0) {
    requiredButNotObsevered <-
      setdiff(x = cohortTableColumnNamesRequired, y = cohortTableColumnNamesObserved)
  }
  obseveredButNotExpected <-
    setdiff(x = cohortTableColumnNamesObserved, y = cohortTableColumnNamesExpected)
  
  if (length(requiredButNotObsevered) > 0) {
    stop(paste(
      "The following required fields not found in cohort table:",
      paste0(requiredButNotObsevered, collapse = ", ")
    ))
  }
  
  if ('logicDescription' %in% expectedButNotObsevered) {
    cohortDefinitionSet$logicDescription <- cohortDefinitionSet$cohortName
  }
  if ('phenotypeId' %in% expectedButNotObsevered) {
    cohortDefinitionSet$phenotypeId <-
      0  # phenotypeId is assigned = 0 when no phenotypeId is provided.
    # This is required for cohort overlap
  }
  if ('metadata' %in% expectedButNotObsevered) {
    if (length(obseveredButNotExpected) > 0) {
      writeLines(
        paste(
          "The following columns were observed in the cohort table, \n
        that are not expected and will be available as part of json object \n
        in a newly created 'metadata' column.",
          paste0(obseveredButNotExpected, collapse = ", ")
        )
      )
    }
    columnsToAddToJson <-
      setdiff(x = cohortTableColumnNamesObserved,
              y = c('json', 'sql')) %>%
      unique() %>%
      sort()
    cohortDefinitionSet <- cohortDefinitionSet %>%
      dplyr::mutate(metadata = as.list(columnsToAddToJson) %>% RJSONIO::toJSON(digits = 23))
  } else {
    if (length(obseveredButNotExpected) > 0) {
      writeLines(
        paste(
          "The following columns were observed in the cohort table, \n
          that are not expected. If you would like to retain them please \n
          them as JSON objects in the 'metadata' column.",
          paste0(obseveredButNotExpected, collapse = ", ")
        )
      )
      stop(paste0(
        "Terminating - please update the metadata column to include: ",
        paste0(obseveredButNotExpected, collapse = ", ")
      ))
    }
  }
  
  cohortDefinitionSet <- cohortDefinitionSet %>%
    dplyr::select(cohortTableColumnNamesExpected)
  writeToCsv(data = cohortDefinitionSet,
             fileName = file.path(exportFolder, "cohort.csv"))
  
  if (!"phenotypeId" %in% colnames(cohortDefinitionSet)) {
    cohortDefinitionSet$phenotypeId <- NA
  }
  
  # Set up connection to server ----------------------------------------------------
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }
  
  if (all(runTimeSeries,
          connection@dbms %in% c('bigquery'))) {
    warning('TimeSeries is not supported for bigquery at this time. TimeSeries will not run.')
    runTimeSeries <- FALSE
  }
  
  vocabularyVersionCdm <- getCdmVocabularyVersion(connection, cdmDatabaseSchema)
  vocabularyVersion <- getVocabularyVersion(connection, vocabularyDatabaseSchema)
  
  if (incremental) {
    ParallelLogger::logDebug("Working in incremental mode.")
    cohortDefinitionSet$checksum <- computeChecksum(cohortDefinitionSet$sql)
    recordKeepingFile <-
      file.path(incrementalFolder, "CreatedDiagnostics.csv")
    if (file.exists(path = recordKeepingFile)) {
      ParallelLogger::logInfo(
        "Found existing record keeping file in incremental folder - CreatedDiagnostics.csv"
      )
    }
  }
  
  # Database metadata ---------------------------------------------
  saveDatabaseMetaData(databaseId,
                       databaseName,
                       databaseDescription,
                       exportFolder,
                       vocabularyVersionCdm,
                       vocabularyVersion)
  # Create concept table ------------------------------------------
  createConceptTable(connection, tempEmulationSchema, cohortDefinitionSet)
  
  # Counting cohorts -----------------------------------------------------------------------
  cohortCounts <- computeCohortCounts(connection,
                                      cohortDatabaseSchema,
                                      cohortTable,
                                      cohortDefinitionSet,
                                      exportFolder,
                                      minCellCount,
                                      databaseId)

  if (nrow(cohortCounts) > 0) {
    instantiatedCohorts <- cohortCounts %>%
      dplyr::pull(.data$cohortId)
    ParallelLogger::logInfo(
      sprintf(
        "Found %s of %s (%1.2f%%) submitted cohorts instantiated. ",
        length(instantiatedCohorts),
        nrow(cohortDefinitionSet),
        100 * (length(instantiatedCohorts) / nrow(cohortDefinitionSet))
      ),
      "Beginning cohort diagnostics for instantiated cohorts. "
    )
  } else {
    stop("All cohorts were either not instantiated or all have 0 records.")
  }

  # Inclusion statistics -----------------------------------------------------------------------
  if (runInclusionStatistics) {
    getInclusionStats(connection,
                      exportFolder,
                      databaseId,
                      cohortDefinitionSet,
                      cohortDatabaseSchema,
                      cohortTableNames,
                      incremental,
                      instantiatedCohorts,
                      inclusionStatisticsFolder,
                      minCellCount,
                      recordKeepingFile)
  }
  
  # Concept set diagnostics -----------------------------------------------
  if (runIncludedSourceConcepts ||
      runOrphanConcepts || runBreakdownIndexEvents) {
    runConceptSetDiagnostics(
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      databaseId = databaseId,
      cohorts = cohortDefinitionSet,
      runIncludedSourceConcepts = runIncludedSourceConcepts,
      runOrphanConcepts = runOrphanConcepts,
      runBreakdownIndexEvents = runBreakdownIndexEvents,
      exportFolder = exportFolder,
      minCellCount = minCellCount,
      conceptCountsDatabaseSchema = NULL,
      conceptCountsTable = "#concept_counts",
      conceptCountsTableIsTemp = TRUE,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      useExternalConceptCountsTable = FALSE,
      incremental = incremental,
      conceptIdTable = "#concept_ids",
      recordKeepingFile = recordKeepingFile
    )
  }
  
  # Time distributions ----------------------------------------------------------------------
  if (runTimeDistributions) {
    executeTimeDistributionDiagnostics(
      connection,
      tempEmulationSchema,
      cdmDatabaseSchema,
      cohortDatabaseSchema,
      cohortTable,
      cdmVersion,
      databaseId,
      exportFolder,
      cohortDefinitionSet,
      instantiatedCohorts,
      incremental,
      recordKeepingFile
    )
  }
  
  # Visit context ----------------------------------------------------------------------------
  if (runVisitContext) {
    executeVisitContextDiagnostics(
      connection,
      tempEmulationSchema,
      cdmDatabaseSchema,
      cohortDatabaseSchema,
      cohortTable,
      cdmVersion,
      databaseId,
      exportFolder,
      minCellCount,
      cohortDefinitionSet,
      instantiatedCohorts,
      recordKeepingFile,
      incremental
    )
  }
  
  # Incidence rates --------------------------------------------------------------------------------------
  if (runIncidenceRate) {
    computeIncidenceRates(
      connection,
      tempEmulationSchema,
      cdmDatabaseSchema,
      cohortDatabaseSchema,
      cohortTable,
      databaseId,
      exportFolder,
      minCellCount,
      cohortDefinitionSet,
      instantiatedCohorts,
      recordKeepingFile,
      incremental
    )
  }

  # Cohort time series -----------------------------------------------------------------------
  if (runTimeSeries) {
    executeTimeSeriesDiagnostics(
      connection,
      cohortDatabaseSchema,
      tempEmulationSchema,
      cohortTable,
      cohortDefinitionSet,
      exportFolder,
      incremental,
      recordKeepingFile,
      instantiatedCohorts,
      databaseId,
      minCellCount
    )
  }
  
  # Cohort overlap ---------------------------------------------------------------------------------
  if (runCohortOverlap) {
    executeCohortComparisonDiagnostics(
      connection,
      databaseId,
      exportFolder,
      cohortDatabaseSchema,
      cohortTable,
      cohortDefinitionSet,
      minCellCount,
      recordKeepingFile,
      incremental
    )
  }
  
  # Cohort characterization ---------------------------------------------------------------
  if (runCohortCharacterization) {
    executeCohortCharacterization(
      connection,
      databaseId,
      exportFolder,
      cdmDatabaseSchema,
      cohortDatabaseSchema,
      cohortTable,
      covariateSettings,
      tempEmulationSchema,
      cdmVersion,
      cohortDefinitionSet,
      cohortCounts,
      minCellCount,
      instantiatedCohorts,
      incremental,
      recordKeepingFile
    )
  }
  
  # Temporal Cohort characterization ---------------------------------------------------------------
  if (runTemporalCohortCharacterization) {
    executeCohortCharacterization(
      connection,
      databaseId,
      exportFolder,
      cdmDatabaseSchema,
      cohortDatabaseSchema,
      cohortTable,
      temporalCovariateSettings,
      tempEmulationSchema,
      cdmVersion,
      cohortDefinitionSet,
      cohortCounts,
      minCellCount,
      instantiatedCohorts,
      incremental,
      recordKeepingFile,
      jobName = "Temporal Cohort characterization",
      task = "runTemporalCohortCharacterization",
      covariateValueFileName = file.path(exportFolder, "temporal_covariate_value.csv"),
      covariateRefFileName = file.path(exportFolder, "temporal_covariate_ref.csv"),
      analysisRefFileName = file.path(exportFolder, "temporal_analysis_ref.csv"),
      timeRefFileName = file.path(exportFolder, "temporal_time_ref.csv")
    )
  }
  
  # Store information from the vocabulary on the concepts used -------------------------
  exportConceptInformation(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    conceptIdTable = "#concept_ids",
    incremental = incremental,
    exportFolder = exportFolder
  )
  
  # Delete unique concept ID table ---------------------------------
  ParallelLogger::logTrace("Deleting concept ID table")
  sql <- "TRUNCATE TABLE @table;\nDROP TABLE @table;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    table = "#concept_ids",
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  # Add all to zip file -------------------------------------------------------------------------------
  writeResultsZip(exportFolder, databaseId, vocabularyVersion, vocabularyVersionCdm)
  delta <- Sys.time() - start
  ParallelLogger::logInfo("Computing all diagnostics took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
}


writeResultsZip <- function(exportFolder, databaseId, vocabularyVersion, vocabularyVersionCdm) {
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <- file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  wd <- getwd()
  on.exit(setwd(wd), add = TRUE)
  setwd(exportFolder)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at: ", zipName)

  metaData <- dplyr::tibble(
    databaseId = databaseId,
    variableField = c('vocabularyVersionCdm', 'vocabularyVersion'),
    valueField = c(vocabularyVersionCdm, vocabularyVersion)
  )
  writeToCsv(data = metaData,
             fileName = "metaData.csv")

}

#' Execute cohort diagnostics functions
#' @description
#' Execute cohort diagnostics on a set of cohorts.
#' Assumes that cohorts have already been instantiated in advance.
#'
#' Note that none of the references passed to this function are used, they are required for results.
#' @inheritParams runCohortDiagnostics
#' @examples
#'
#' \dontrun{
#' # Load cohorts (assumes that they have already been instantiated)
#' cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "cohort")
#' cohorts <- loadCohortsFromPackage(packageName = "MyGreatPackage")
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              server = "ohdsi.com",
#'                                              port = 5432,
#'                                              user = "me",
#'                                              password = "secure")
#'
#' executeDiagnostics(cohorts = cohorts,
#'                    exportFolder = "export",
#'                    cohortTableNames = cohortTableNames,
#'                    cohortDatabaseSchema = "results",
#'                    cdmDatabaseSchema = "cdm",
#'                    databaseId = "mySpecialCdm",
#'                    connectionDetails = connectionDetails)
#'
#' # Use a custom set of cohorts defined in a data.frame
#' cohorts <- data.frame(
#'   cohortId = c(100),
#'   cohortName = c("Cohort Name"),
#'   logicDescription = c("My Cohort"),
#'   sql = c(readLines("path_to.sql")),
#'   json = c(readLines("path_to.json"))
#' )
#' executeDiagnostics(cohorts = cohorts,
#'                    exportFolder = "export",
#'                    cohortTable = "cohort",
#'                    cohortDatabaseSchema = "results",
#'                    cdmDatabaseSchema = "cdm",
#'                    databaseId = "mySpecialCdm",
#'                    connectionDetails = connectionDetails)
#' }
#'
#' @importFrom CohortGenerator getCohortTableNames
#' @export
executeDiagnostics <- function(cohortDefinitionSet,
                               exportFolder,
                               databaseId,
                               connectionDetails = NULL,
                               connection = NULL,
                               cdmDatabaseSchema,
                               tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                               cohortDatabaseSchema,
                               cohortTable = "cohort",
                               cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = cohortTable),
                               vocabularyDatabaseSchema = cdmDatabaseSchema,
                               cohortIds = NULL,
                               inclusionStatisticsFolder = NULL,
                               databaseName = databaseId,
                               databaseDescription = databaseId,
                               cdmVersion = 5,
                               runInclusionStatistics = TRUE,
                               runIncludedSourceConcepts = TRUE,
                               runOrphanConcepts = TRUE,
                               runTimeDistributions = TRUE,
                               runVisitContext = TRUE,
                               runBreakdownIndexEvents = TRUE,
                               runIncidenceRate = TRUE,
                               runTimeSeries = FALSE,
                               runCohortOverlap = TRUE,
                               runCohortCharacterization = TRUE,
                               covariateSettings = createDefaultCovariateSettings(),
                               runTemporalCohortCharacterization = TRUE,
                               temporalCovariateSettings = createTemporalCovariateSettings(
                                 useConditionOccurrence = TRUE,
                                 useDrugEraStart = TRUE,
                                 useProcedureOccurrence = TRUE,
                                 useMeasurement = TRUE,
                                 temporalStartDays = c(-365, -30, 0, 1, 31),
                                 temporalEndDays = c(-31, -1, 0, 30, 365)
                               ),
                               minCellCount = 5,
                               incremental = FALSE,
                               incrementalFolder = file.path(exportFolder, "incremental")) {

  checkCohortReference(cohortReference = cohortDefinitionSet)

  runCohortDiagnostics(cohortDefinitionSet = cohortDefinitionSet,
                       exportFolder = exportFolder,
                       databaseId = databaseId,
                       connectionDetails = connectionDetails,
                       connection = connection,
                       cdmDatabaseSchema = cdmDatabaseSchema,
                       tempEmulationSchema = tempEmulationSchema,
                       cohortDatabaseSchema = cohortDatabaseSchema,
                       vocabularyDatabaseSchema = cdmDatabaseSchema,
                       cohortTableNames = cohortTableNames,
                       cohortIds = cohortIds,
                       inclusionStatisticsFolder = inclusionStatisticsFolder,
                       databaseName = databaseName,
                       databaseDescription = databaseDescription,
                       cdmVersion = cdmVersion,
                       runInclusionStatistics = runInclusionStatistics,
                       runIncludedSourceConcepts = runIncludedSourceConcepts,
                       runOrphanConcepts = runOrphanConcepts,
                       runTimeDistributions = runTimeDistributions,
                       runVisitContext = runTimeDistributions,
                       runBreakdownIndexEvents = runBreakdownIndexEvents,
                       runIncidenceRate = runIncidenceRate,
                       runTimeSeries = runTimeSeries,
                       runCohortOverlap = runCohortOverlap,
                       runCohortCharacterization = runCohortCharacterization,
                       covariateSettings = covariateSettings,
                       runTemporalCohortCharacterization = runTemporalCohortCharacterization,
                       temporalCovariateSettings = temporalCovariateSettings,
                       minCellCount = minCellCount,
                       incremental = incremental,
                       incrementalFolder = incrementalFolder)
}
