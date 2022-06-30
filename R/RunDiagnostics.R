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
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runTimeSeries               Generate and export the time series diagnostics?
#' @param runVisitContext             Generate and export index-date visit context?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#' @param runIncidenceRate            Generate and export the cohort incidence  rates?
#' @param runCohortRelationship       Generate and export the cohort relationship? Cohort relationship checks the temporal
#'                                    relationship between two or more cohorts.
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
                               runIncludedSourceConcepts = TRUE,
                               runOrphanConcepts = TRUE,
                               runTimeSeries = FALSE,
                               runVisitContext = TRUE,
                               runBreakdownIndexEvents = TRUE,
                               runIncidenceRate = TRUE,
                               runCohortRelationship = TRUE,
                               runTemporalCohortCharacterization = TRUE,
                               temporalCovariateSettings = FeatureExtraction::createTemporalCovariateSettings(
                                 useDemographicsGender = TRUE,
                                 useDemographicsAge = TRUE,
                                 useDemographicsAgeGroup = TRUE,
                                 useDemographicsRace = TRUE,
                                 useDemographicsEthnicity = TRUE,
                                 useDemographicsIndexYear = TRUE,
                                 useDemographicsIndexMonth = TRUE,
                                 useDemographicsIndexYearMonth = TRUE,
                                 useDemographicsPriorObservationTime = TRUE,
                                 useDemographicsPostObservationTime = TRUE,
                                 useDemographicsTimeInCohort = TRUE,
                                 useConditionOccurrence = TRUE,
                                 useProcedureOccurrence = TRUE,
                                 useDrugEraStart = TRUE,
                                 useMeasurement = TRUE,
                                 useConditionEraStart = TRUE,
                                 useConditionEraOverlap = TRUE,
                                 useConditionEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
                                 useConditionEraGroupOverlap = TRUE,
                                 useDrugExposure = FALSE, # leads to too many concept id
                                 useDrugEraOverlap = FALSE,
                                 useDrugEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
                                 useDrugEraGroupOverlap = TRUE,
                                 useObservation = TRUE,
                                 useDeviceExposure = TRUE,
                                 useCharlsonIndex = TRUE,
                                 useDcsi = TRUE,
                                 useChads2 = TRUE,
                                 useChads2Vasc = TRUE,
                                 useHfrs = FALSE,
                                 temporalStartDays = c(
                                   # components displayed in cohort characterization
                                   -9999, # anytime prior
                                   -365, # long term prior
                                   -180, # medium term prior
                                   -30, # short term prior
                                   
                                   # components displayed in temporal characterization
                                   -365, # one year prior to -31
                                   -30, # 30 day prior not including day 0
                                   0, # index date only
                                   1, # 1 day after to day 30
                                   31,
                                   -9999 # Any time prior to any time future
                                 ),
                                 temporalEndDays = c(
                                   0, # anytime prior
                                   0, # long term prior
                                   0, # medium term prior
                                   0, # short term prior
                                   
                                   # components displayed in temporal characterization
                                   -31, # one year prior to -31
                                   -1, # 30 day prior not including day 0
                                   0, # index date only
                                   30, # 1 day after to day 30
                                   365,
                                   9999 # Any time prior to any time future
                                 )
                               ),
                               minCellCount = 5,
                               incremental = FALSE,
                               incrementalFolder = file.path(exportFolder, "incremental")) {
  
  # collect arguments that were passed to cohort diagnostics at initiation
  argumentsAtDiagnosticsInitiation <- formals(executeDiagnostics)
  argumentsAtDiagnosticsInitiationJson <-
    list(
      runInclusionStatistics = argumentsAtDiagnosticsInitiation$runInclusionStatistics,
      runIncludedSourceConcepts = argumentsAtDiagnosticsInitiation$runIncludedSourceConcepts,
      runOrphanConcepts = argumentsAtDiagnosticsInitiation$runOrphanConcepts,
      runTimeSeries = argumentsAtDiagnosticsInitiation$runTimeSeries,
      runVisitContext = argumentsAtDiagnosticsInitiation$runVisitContext,
      runBreakdownIndexEvents = argumentsAtDiagnosticsInitiation$runBreakdownIndexEvents,
      runIncidenceRate = argumentsAtDiagnosticsInitiation$runIncidenceRate,
      runTemporalCohortCharacterization = argumentsAtDiagnosticsInitiation$runTemporalCohortCharacterization,
      minCellCount = argumentsAtDiagnosticsInitiation$minCellCount,
      incremental = argumentsAtDiagnosticsInitiation$incremental,
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
  checkmate::assertLogical(runIncludedSourceConcepts, add = errorMessage)
  checkmate::assertLogical(runOrphanConcepts, add = errorMessage)
  checkmate::assertLogical(runTimeSeries, add = errorMessage)
  checkmate::assertLogical(runBreakdownIndexEvents, add = errorMessage)
  checkmate::assertLogical(runIncidenceRate, add = errorMessage)
  checkmate::assertLogical(runTemporalCohortCharacterization, add = errorMessage)
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
    runBreakdownIndexEvents,
    runIncidenceRate
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
  if (runTemporalCohortCharacterization) {
    checkmate::assert_class(x = temporalCovariateSettings,
                            classes = c("covariateSettings"))
    # forcefully set ConditionEraGroupStart and drugEraGroupStart to NULL
    # because of known bug in FeatureExtraction. https://github.com/OHDSI/FeatureExtraction/issues/144
    temporalCovariateSettings$ConditionEraGroupStart <- NULL
    temporalCovariateSettings$DrugEraGroupStart <- NULL

    checkmate::assert_integerish(x = temporalCovariateSettings$temporalStartDays,
                              any.missing = FALSE,
                              min.len = 1,
                              add = errorMessage)
    checkmate::assert_integerish(x = temporalCovariateSettings$temporalEndDays,
                              any.missing = FALSE,
                              min.len = 1,
                              add = errorMessage)
    checkmate::reportAssertions(collection = errorMessage)
    
    # Adding required temporal windows required in results viewer
    requiredTemporalPairs <-
      list(c(-365, 0),
           c(-30, 0),
           c(-365,-31),
           c(-30,-1),
           c(0, 0),
           c(1, 30),
           c(31, 365),
           c(-9999, 9999))
    for (p1 in requiredTemporalPairs) {
      found <- FALSE
      for (i in 1:length(temporalCovariateSettings$temporalStartDays)) {
        p2 <- c(
          temporalCovariateSettings$temporalStartDays[i],
          temporalCovariateSettings$temporalEndDays[i]
        )
        
        if (p2[1] == p1[1] & p2[2] == p1[2]) {
          found <- TRUE
          break
        }
      }
      
      if (!found) {
        temporalCovariateSettings$temporalStartDays <-
          c(temporalCovariateSettings$temporalStartDays, p1[1])
        temporalCovariateSettings$temporalEndDays <-
          c(temporalCovariateSettings$temporalEndDays, p1[2])
      }
    }
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
             SUM(CAST(DATEDIFF(dd, observation_period_start_date, observation_period_end_date) AS BIGINT)) person_days
             FROM @cdm_database_schema.observation_period;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE,
    tempEmulationSchema = tempEmulationSchema
  )
  
  # Database metadata ---------------------------------------------
  saveDatabaseMetaData(
    databaseId = databaseId,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    exportFolder = exportFolder,
    minCellCount = minCellCount,
    vocabularyVersionCdm = cdmSourceInformation$vocabularyVersion,
    vocabularyVersion = vocabularyVersion
  )
  # Create concept table ------------------------------------------
  createConceptTable(connection, tempEmulationSchema)
  
  # Counting cohorts -----------------------------------------------------------------------
  cohortCounts <- computeCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohorts = cohortDefinitionSet,
    exportFolder = exportFolder,
    minCellCount = minCellCount,
    databaseId = databaseId
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
      connection = connection,
      exportFolder = exportFolder,
      databaseId = databaseId,
      cohortDefinitionSet = cohortDefinitionSet,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      incremental = incremental,
      instantiatedCohorts = instantiatedCohorts,
      minCellCount = minCellCount,
      recordKeepingFile = recordKeepingFile
    )
  }
  
  # Concept set diagnostics -----------------------------------------------
  if (runIncludedSourceConcepts ||
      runOrphanConcepts ||
      runBreakdownIndexEvents) {
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
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cdmVersion = cdmVersion,
      databaseId = databaseId,
      exportFolder = exportFolder,
      minCellCount = minCellCount,
      cohorts = cohortDefinitionSet,
      instantiatedCohorts = instantiatedCohorts,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
  }
  
  # Incidence rates --------------------------------------------------------------------------------------
  if (runIncidenceRate) {
    computeIncidenceRates(
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      databaseId = databaseId,
      exportFolder = exportFolder,
      minCellCount = minCellCount,
      cohorts = cohortDefinitionSet,
      instantiatedCohorts = instantiatedCohorts,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
  }
  
  # Cohort relationship ---------------------------------------------------------------------------------
  if (runCohortRelationship) {
    executeCohortRelationshipDiagnostics(
      connection = connection,
      databaseId = databaseId,
      exportFolder = exportFolder,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortTable = cohortTable,
      cohortDefinitionSet = cohortDefinitionSet,
      temporalCovariateSettings = temporalCovariateSettings,
      minCellCount = minCellCount,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
  }
  
  # Temporal Cohort characterization ---------------------------------------------------------------
  if (runTemporalCohortCharacterization) {
    executeCohortCharacterization(
      connection = connection,
      databaseId = databaseId,
      exportFolder = exportFolder,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      covariateSettings = temporalCovariateSettings,
      tempEmulationSchema = tempEmulationSchema,
      cdmVersion = cdmVersion,
      cohorts = cohortDefinitionSet,
      cohortCounts = cohortCounts,
      minCellCount = minCellCount,
      instantiatedCohorts = instantiatedCohorts,
      incremental = incremental,
      recordKeepingFile = recordKeepingFile,
      task = "runTemporalCohortCharacterization",
      jobName = "Temporal Cohort characterization",
      covariateValueFileName = file.path(exportFolder, "temporal_covariate_value.csv"),
      covariateValueContFileName = file.path(exportFolder, "temporal_covariate_value_dist.csv"),
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
