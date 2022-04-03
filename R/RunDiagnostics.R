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

#' Execute cohort diagnostics
#'
#' @description
#' Runs the cohort diagnostics on all (or a subset of) the cohorts instantiated using the
#' Assumes the cohorts have already been instantiated. with the CohortGenerator package
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
#'
#' @template CohortTable
#'
#'
#' @template CohortSetReference
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
#' @param runTimeDistributions        Generate and export cohort time distributions?
#' @param runTimeSeries               Generate and export the time series diagnostics?
#' @param runVisitContext             Generate and export index-date visit context?
#' @param runIncidenceRate            Generate and export the cohort incidence  rates?
#' @param runCohortOverlap            Generate and export the cohort overlap? Overlaps are checked within cohortIds
#'                                    that have the same phenotype ID sourced from the CohortSetReference or
#'                                    cohortToCreateFile.
#' @param runCohortRelationship       Generate and export the cohort relationship? Cohort relationship checks the temporal
#'                                    relationship between two or more cohorts.
#' @param runConceptSetDiagnostics       Do you want to run concept set diagnostics?
#' @param runConceptCountByCalendarPeriod  Do you want to stratify the counts by calendar period like calendar year, calendar month?
#' @param runIndexDateConceptCoOccurrence Generate and export co-occurrence of concepts on index date. This diagnostics will
#'                                    compute if any two concept-id co-occur on the same day. Computation will be performed
#'                                    for co-occurrence of standard-standard, standard-source, and source-source
#'                                    concept ids. This may take time. It is set to FALSE by default.
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
#' @examples
#' \dontrun{
#' # Load cohorts (assumes that they have already been instantiated)
#' cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "cohort")
#' cohorts <- CohortGenerator::getCohortDefinitionSet(packageName = "MyGreatPackage")
#' connectionDetails <- createConnectionDetails(
#'   dbms = "postgresql",
#'   server = "ohdsi.com",
#'   port = 5432,
#'   user = "me",
#'   password = "secure"
#' )
#'
#' executeDiagnostics(
#'   cohorts = cohorts,
#'   exportFolder = "export",
#'   cohortTableNames = cohortTableNames,
#'   cohortDatabaseSchema = "results",
#'   cdmDatabaseSchema = "cdm",
#'   databaseId = "mySpecialCdm",
#'   connectionDetails = connectionDetails
#' )
#'
#' # Use a custom set of cohorts defined in a data.frame
#' cohorts <- data.frame(
#'   cohortId = c(100),
#'   cohortName = c("Cohort Name"),
#'   logicDescription = c("My Cohort"),
#'   sql = c(readLines("path_to.sql")),
#'   json = c(readLines("path_to.json"))
#' )
#' executeDiagnostics(
#'   cohorts = cohorts,
#'   exportFolder = "export",
#'   cohortTable = "cohort",
#'   cohortDatabaseSchema = "results",
#'   cdmDatabaseSchema = "cdm",
#'   databaseId = "mySpecialCdm",
#'   connectionDetails = connectionDetails
#' )
#' }
#'
#' @importFrom CohortGenerator getCohortTableNames
#' @importFrom tidyr any_of
#' @export
executeDiagnostics <- function(cohortDefinitionSet,
                               exportFolder,
                               databaseId,
                               cohortDatabaseSchema,
                               databaseName = databaseId,
                               databaseDescription = databaseId,
                               connectionDetails = NULL,
                               connection = NULL,
                               cdmDatabaseSchema,
                               tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                               cohortTable = "cohort",
                               cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = cohortTable),
                               vocabularyDatabaseSchema = cdmDatabaseSchema,
                               cohortIds = NULL,
                               cdmVersion = 5,
                               runInclusionStatistics = TRUE,
                               runConceptSetDiagnostics = TRUE,
                               runIndexDateConceptCoOccurrence = FALSE,
                               runConceptCountByCalendarPeriod = FALSE,
                               runTimeDistributions = TRUE,
                               runTimeSeries = FALSE,
                               runVisitContext = TRUE,
                               runIncidenceRate = TRUE,
                               runCohortOverlap = TRUE,
                               runCohortRelationship = FALSE,
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

  # collect arguments that were passed to cohort diagnostics at initiation
  argumentsAtDiagnosticsInitiation <- formals(executeDiagnostics)
  argumentsAtDiagnosticsInitiationJson <-
    list(
      runInclusionStatistics = argumentsAtDiagnosticsInitiation$runInclusionStatistics,
      runConceptSetDiagnostics = argumentsAtDiagnosticsInitiation$runConceptSetDiagnostics,
      runIndexDateConceptCoOccurrence = argumentsAtDiagnosticsInitiation$runIndexDateConceptCoOccurrence,
      runConceptCountByCalendarPeriod = argumentsAtDiagnosticsInitiation$runConceptCountByCalendarPeriod,
      runTimeDistributions = argumentsAtDiagnosticsInitiation$runTimeDistributions,
      runTimeSeries = argumentsAtDiagnosticsInitiation$runTimeSeries,
      runVisitContext = argumentsAtDiagnosticsInitiation$runVisitContext,
      runIncidenceRate = argumentsAtDiagnosticsInitiation$runIncidenceRate,
      runCohortOverlap = argumentsAtDiagnosticsInitiation$runCohortOverlap,
      runCohortCharacterization = argumentsAtDiagnosticsInitiation$runCohortCharacterization,
      runTemporalCohortCharacterization = argumentsAtDiagnosticsInitiation$runTemporalCohortCharacterization,
      minCellCount = argumentsAtDiagnosticsInitiation$minCellCount,
      incremental = argumentsAtDiagnosticsInitiation$incremental,
      covariateSettings = argumentsAtDiagnosticsInitiation$covariateSettings,
      temporalCovariateSettings = argumentsAtDiagnosticsInitiation$temporalCovariateSettings
    ) %>%
    RJSONIO::toJSON(digits = 23, pretty = TRUE)

  # take package dependency snapshot
  packageDependencySnapShotJson <-
    takepackageDependencySnapshot() %>%
    RJSONIO::toJSON(digits = 23, pretty = TRUE)

  exportFolder <- normalizePath(exportFolder, mustWork = FALSE)
  incrementalFolder <- normalizePath(incrementalFolder, mustWork = FALSE)

  start <- Sys.time()
  ParallelLogger::logInfo("Run Cohort Diagnostics started at ", start)

  databaseId <- as.character(databaseId)

  if (any(is.null(databaseName), is.na(databaseName))) {
    databaseName <- databaseId
    ParallelLogger::logTrace(" - Databasename was not provided.")
  }
  if (any(is.null(databaseDescription), is.na(databaseDescription))) {
    databaseDescription <- databaseId
    ParallelLogger::logTrace(" - Databasedescription was not provided.")
  }

  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertList(cohortTableNames, null.ok = FALSE, types = "character", add = errorMessage, names = "named")
  checkmate::assertNames(names(cohortTableNames),
    must.include = c(
      "cohortTable",
      "cohortInclusionTable",
      "cohortInclusionResultTable",
      "cohortInclusionStatsTable",
      "cohortSummaryStatsTable",
      "cohortCensorStatsTable"
    ),
    add = errorMessage
  )
  checkmate::assertDataFrame(cohortDefinitionSet, add = errorMessage)
  checkmate::assertNames(names(cohortDefinitionSet),
    must.include = c(
      "json",
      "cohortId",
      "cohortName",
      "sql"
    ),
    add = errorMessage
  )

  cohortTable <- cohortTableNames$cohortTable
  checkmate::assertLogical(runInclusionStatistics, add = errorMessage)
  checkmate::assertLogical(runConceptSetDiagnostics, add = errorMessage)
  checkmate::assertLogical(runIndexDateConceptCoOccurrence, add = errorMessage)
  checkmate::assertLogical(runConceptCountByCalendarPeriod, add = errorMessage)
  checkmate::assertLogical(runTimeDistributions, add = errorMessage)
  checkmate::assertLogical(runTimeSeries, add = errorMessage)
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
    runConceptSetDiagnostics,
    runIndexDateConceptCoOccurrence,
    runConceptCountByCalendarPeriod,
    runTimeDistributions,
    runIncidenceRate,
    runCohortOverlap,
    runCohortCharacterization
  )) {
    checkmate::assertCharacter(
      x = cdmDatabaseSchema,
      min.len = 1,
      add = errorMessage
    )
    checkmate::assertCharacter(
      x = vocabularyDatabaseSchema,
      min.len = 1,
      add = errorMessage
    )
    checkmate::assertCharacter(
      x = cohortDatabaseSchema,
      min.len = 1,
      add = errorMessage
    )
    checkmate::assertCharacter(
      x = cohortTable,
      min.len = 1,
      add = errorMessage
    )
    checkmate::assertCharacter(
      x = databaseId,
      min.len = 1,
      add = errorMessage
    )
  }
  checkmate::reportAssertions(collection = errorMessage)

  errorMessage <-
    createIfNotExist(
      type = "folder",
      name = exportFolder,
      errorMessage = errorMessage
    )
  if (incremental) {
    errorMessage <-
      createIfNotExist(
        type = "folder",
        name = incrementalFolder,
        errorMessage = errorMessage
      )
  }

  checkmate::reportAssertions(collection = errorMessage)
  if (!is.null(cohortIds)) {
    cohortDefinitionSet <- cohortDefinitionSet %>% dplyr::filter(.data$cohortId %in% cohortIds)
  }

  if (nrow(cohortDefinitionSet) == 0) {
    stop("No cohorts specified")
  }
  cohortTableColumnNamesObserved <- colnames(cohortDefinitionSet) %>%
    sort()
  cohortTableColumnNamesExpected <-
    getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == "cohort") %>%
    dplyr::pull(.data$fieldName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    sort()
  cohortTableColumnNamesRequired <-
    getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == "cohort") %>%
    dplyr::filter(.data$isRequired == "Yes") %>%
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

  if (length(obseveredButNotExpected) > 0) {
    ParallelLogger::logInfo(
      paste0(
        "The following fields found in the cohortDefinitionSet will be exported in JSON format as part of metadata field of cohort table:\n    ",
        paste0(obseveredButNotExpected, collapse = ",\n    ")
      )
    )
  }

  cohortDefinitionSet <- makeDataExportable(
    x = cohortDefinitionSet,
    tableName = "cohort",
    minCellCount = minCellCount,
    databaseId = NULL
  )

  writeToCsv(
    data = cohortDefinitionSet,
    fileName = file.path(exportFolder, "cohort.csv")
  )

  # Set up connection to server ----------------------------------------------------
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }

  ## CDM source information----
  cdmSourceInformation <-
    getCdmDataSourceInformation(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema
    )

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

  ## Observation period----
  ParallelLogger::logTrace(" - Collecting date range from Observational period table.")
  observationPeriodDateRange <- renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT MIN(observation_period_start_date) observation_period_min_date,
             MAX(observation_period_end_date) observation_period_max_date,
             COUNT(distinct person_id) persons,
             COUNT(person_id) records,
             SUM(DATEDIFF(dd, observation_period_start_date, observation_period_end_date)) person_days
             FROM @cdm_database_schema.observation_period;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE,
    tempEmulationSchema = tempEmulationSchema
  )

  # Database metadata ---------------------------------------------
  saveDatabaseMetaData(
    databaseId,
    databaseName,
    databaseDescription,
    exportFolder,
    minCellCount,
    cdmSourceInformation$vocabularyVersion,
    vocabularyVersion
  )
  # Create concept table ------------------------------------------
  createConceptTable(connection, tempEmulationSchema, cohortDefinitionSet)

  # Counting cohorts -----------------------------------------------------------------------
  cohortCounts <- computeCohortCounts(
    connection,
    cohortDatabaseSchema,
    cohortTable,
    cohortDefinitionSet,
    exportFolder,
    minCellCount,
    databaseId
  )

  if (nrow(cohortCounts) > 0) {
    instantiatedCohorts <- cohortCounts %>%
      dplyr::filter(.data$cohortEntries > 0) %>%
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
    getInclusionStats(
      connection,
      exportFolder,
      databaseId,
      cohortDefinitionSet,
      cohortDatabaseSchema,
      cohortTableNames,
      incremental,
      instantiatedCohorts,
      minCellCount,
      recordKeepingFile
    )
  }

  # Concept set diagnostics -----------------------------------------------
  if (runConceptSetDiagnostics) {
    executeConceptSetDiagnostics(
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      databaseId = databaseId,
      cohorts = cohortDefinitionSet,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      runConceptSetDiagnostics = runConceptSetDiagnostics,
      runIndexDateConceptCoOccurrence = runIndexDateConceptCoOccurrence,
      runConceptCountByCalendarPeriod = runConceptCountByCalendarPeriod,
      exportFolder = exportFolder,
      minCellCount = minCellCount,
      keep2BillionConceptId = FALSE,
      # by default all concept id >= 2 billion are removed. to overide set this parameter to TRUE
      incremental = incremental,
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
      minCellCount,
      cohortDefinitionSet,
      instantiatedCohorts,
      incremental,
      recordKeepingFile
    )
  }

  # Time series ----------------------------------------------------------------------
  if (runTimeSeries) {
    executeTimeSeriesDiagnostics(
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortDefinitionSet = cohortDefinitionSet,
      databaseId = databaseId,
      exportFolder = exportFolder,
      minCellCount = minCellCount,
      instantiatedCohorts = instantiatedCohorts,
      incremental = incremental,
      recordKeepingFile = recordKeepingFile,
      observationPeriodDateRange = observationPeriodDateRange
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


  # Cohort relationship ---------------------------------------------------------------------------------
  if (runCohortRelationship) {
    executeCohortRelationshipDiagnostics(
      connection,
      databaseId,
      exportFolder,
      cohortDatabaseSchema,
      cdmDatabaseSchema,
      tempEmulationSchema,
      cohortTable,
      cohortDefinitionSet,
      temporalCovariateSettings,
      minCellCount,
      recordKeepingFile,
      incremental,
      incrementalFolder
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

  # Writing metadata file
  ParallelLogger::logInfo("Retrieving metadata information and writing metadata")

  packageName <- utils::packageName()
  packageVersion <- if (!methods::getPackageName() == ".GlobalEnv") {
    as.character(utils::packageVersion(packageName))
  } else {
    ""
  }
  delta <- Sys.time() - start
  variableField <- c(
    "timeZone",
    # 1
    "runTime",
    # 2
    "runTimeUnits",
    # 3
    "packageDependencySnapShotJson",
    # 4
    "argumentsAtDiagnosticsInitiationJson",
    # 5
    "rversion",
    # 6
    "currentPackage",
    # 7
    "currentPackageVersion",
    # 8
    "sourceDescription",
    # 9
    "cdmSourceName",
    # 10
    "sourceReleaseDate",
    # 11
    "cdmVersion",
    # 12
    "cdmReleaseDate",
    # 13
    "vocabularyVersion",
    # 14
    "datasourceName",
    # 15
    "datasourceDescription",
    # 16
    "vocabularyVersionCdm",
    # 17
    "observationPeriodMinDate",
    # 18
    "observationPeriodMaxDate",
    # 19
    "personsInDatasource",
    # 20
    "recordsInDatasource",
    # 21
    "personDaysInDatasource" # 22
  )
  valueField <- c(
    as.character(Sys.timezone()),
    # 1
    as.character(as.numeric(
      x = delta, units = attr(delta, "units")
    )),
    # 2
    as.character(attr(delta, "units")),
    # 3
    packageDependencySnapShotJson,
    # 4
    argumentsAtDiagnosticsInitiationJson,
    # 5
    as.character(R.Version()$version.string),
    # 6
    as.character(nullToEmpty(packageName)),
    # 7
    as.character(nullToEmpty(packageVersion)),
    # 8
    as.character(nullToEmpty(
      cdmSourceInformation$sourceDescription
    )),
    # 9
    as.character(nullToEmpty(cdmSourceInformation$cdmSourceName)),
    # 10
    as.character(nullToEmpty(
      cdmSourceInformation$sourceReleaseDate
    )),
    # 11
    as.character(nullToEmpty(cdmSourceInformation$cdmVersion)),
    # 12
    as.character(nullToEmpty(cdmSourceInformation$cdmReleaseDate)),
    # 13
    as.character(nullToEmpty(
      cdmSourceInformation$vocabularyVersion
    )),
    # 14
    as.character(databaseName),
    # 15
    as.character(databaseDescription),
    # 16
    as.character(nullToEmpty(cdmSourceInformation$vocabularyVersion)),
    # 17
    as.character(observationPeriodDateRange$observationPeriodMinDate),
    # 18
    as.character(observationPeriodDateRange$observationPeriodMaxDate),
    # 19
    as.character(observationPeriodDateRange$persons),
    # 20
    as.character(observationPeriodDateRange$records),
    # 21
    as.character(observationPeriodDateRange$personDays) # 22
  )
  metadata <- dplyr::tibble(
    databaseId = as.character(!!databaseId),
    startTime = paste0("TM_", as.character(start)),
    variableField = variableField,
    valueField = valueField
  )
  metadata <- makeDataExportable(
    x = metadata,
    tableName = "metadata",
    minCellCount = minCellCount,
    databaseId = databaseId
  )
  writeToCsv(
    data = metadata,
    fileName = file.path(exportFolder, "metadata.csv"),
    incremental = TRUE,
    start_time = as.character(start)
  )

  # Add all to zip file -------------------------------------------------------------------------------
  writeResultsZip(exportFolder, databaseId)
  ParallelLogger::logInfo(
    "Computing all diagnostics took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
}


writeResultsZip <- function(exportFolder, databaseId) {
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <- file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  wd <- getwd()
  on.exit(setwd(wd), add = TRUE)
  setwd(exportFolder)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at: ", zipName)
}
