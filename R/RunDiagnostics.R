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

#' Run cohort diagnostics
#'
#' @description
#' Runs cohort diagnostics on cohorts specified in cohortToCreateFile (file) or cohortSetReference. The function
#' checks if the specified cohorts are instantiated, and only runs diagnostics on the instantiated
#' cohorts (i.e. > 0 rows in cohort table).
#'
#' Characterization:
#' If runTemporalCohortCharacterization argument is TRUE, then \code{RFeatureExtraction::createTemporalCovariateSettings}
#' is used as default.
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
#' @param databaseId                  A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName                The full name of the database. If NULL, defaults to databaseId.
#' @param databaseDescription         A short description (several sentences) of the database. If NULL, defaults to databaseId.
#' @template cdmVersion
#' @param runInclusionStatistics      Generate and export statistic on the cohort inclusion rules?
#' @param runConceptSetDiagnostics    Concept Set Diagnostics includes concept counts, concepts in data source,
#'                                    index event breakdown, concept cooccurrence, excluded concepts,
#'                                    resolved concepts. This function call now supersedes runIncludedSourceConcepts,
#'                                    runOrphanConcepts, runBreakdownIndexEvents.
#' @param runIncludedSourceConcepts   (Deprecated) Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           (Deprecated) Generate and export potential orphan concepts?
#' @param runVisitContext             Generate and export index-date visit context?
#' @param runBreakdownIndexEvents     (Deprecated) Generate and export the breakdown of index events?
#' @param runIncidenceRate            Generate and export the cohort incidence  rates?
#' @param runCohortTimeSeries         Generate and export the cohort level time series?
#' @param runDataSourceTimeSeries     Generate and export the Data source level time series? i.e.
#'                                    using all persons found in observation period table.
#' @param runCohortRelationship       Do you want to compute temporal relationship between the cohorts being diagnosed. This
#'                                    diagnostics is needed for cohort as feature characterization.
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
#' @template ExportDetailedVocabulary
#' @param minCellCount                The minimum cell count for fields contains person counts or fractions.
#' @param incremental                 Create only cohort diagnostics that haven't been created before?
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are kept
#'                                    of which cohort diagnostics has been executed.
#' @export
runCohortDiagnostics <- function(packageName = NULL,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
                                 baseUrl = NULL,
                                 cohortSetReference = NULL,
                                 connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 oracleTempSchema = NULL,
                                 tempEmulationSchema = NULL,
                                 cohortDatabaseSchema,
                                 vocabularyDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortIds = NULL,
                                 inclusionStatisticsFolder = file.path(exportFolder, "inclusionStatistics"),
                                 exportFolder,
                                 databaseId,
                                 databaseName = databaseId,
                                 databaseDescription = databaseId,
                                 cdmVersion = 5,
                                 runInclusionStatistics = TRUE,
                                 runConceptSetDiagnostics = TRUE,
                                 runIncludedSourceConcepts = FALSE,
                                 runOrphanConcepts = FALSE,
                                 runVisitContext = TRUE,
                                 runBreakdownIndexEvents = FALSE,
                                 runIncidenceRate = TRUE,
                                 runCohortTimeSeries = TRUE,
                                 runDataSourceTimeSeries = TRUE,
                                 runCohortRelationship = TRUE,
                                 runCohortCharacterization = TRUE,
                                 covariateSettings = list(
                                   FeatureExtraction::createDefaultCovariateSettings(),
                                   FeatureExtraction::createCovariateSettings(
                                     useVisitCountLongTerm = TRUE,
                                     useVisitCountShortTerm = TRUE,
                                     useVisitConceptCountLongTerm = TRUE,
                                     useVisitConceptCountShortTerm = TRUE,
                                     useDemographicsPriorObservationTime = TRUE,
                                     useDemographicsPostObservationTime = TRUE,
                                     useDemographicsTimeInCohort = TRUE,
                                     useDemographicsIndexYearMonth = TRUE,
                                   )
                                 ),
                                 runTemporalCohortCharacterization = TRUE,
                                 temporalCovariateSettings = FeatureExtraction::createTemporalCovariateSettings(
                                   useConditionOccurrence = TRUE,
                                   useDrugEraStart = TRUE,
                                   useProcedureOccurrence = TRUE,
                                   useMeasurement = TRUE,
                                   temporalStartDays = c(
                                     -365,-30,
                                     0,
                                     1,
                                     31,
                                     seq(from = -421, to = -31, by = 30),
                                     seq(from = 0, to = 390, by = 30)
                                   ),
                                   temporalEndDays = c(
                                     -31,-1,
                                     0,
                                     30,
                                     365,
                                     seq(from = -391, to = -1, by = 30),
                                     seq(from = 30, to = 420, by = 30)
                                   )
                                 ),
                                 exportDetailedVocabulary = TRUE,
                                 minCellCount = 5,
                                 incremental = FALSE,
                                 incrementalFolder = file.path(exportFolder, "incremental")) {
  start <- Sys.time()
  
  if (all(is.null(connectionDetails),
          is.null(connection))) {
    stop('Please provide either connection or connectionDetails to connect to database.')
  }
  # Set up connection to server----
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
  }
  tables <-
    DatabaseConnector::getTableNames(connection, cohortDatabaseSchema)
  if (!toupper(cohortTable) %in% toupper(tables)) {
    stop(
      "Cannot find cohort table. Did you instantiate cohorts? \n Try running function instantiateCohortSet with option createCohortTable = TRUE, or \n use function Try functions createCohortTable if you only want to create the cohort table without instantiating cohorts."
    )
  }
  
  if (runIncludedSourceConcepts) {
    warning(
      "runIncludedSourceConcepts is deprecated. Running runConceptSetDiagnostics instead."
    )
    runConceptSetDiagnostics <- TRUE
  }
  if (runOrphanConcepts) {
    warning("runOrphanConcepts is deprecated. Running runConceptSetDiagnostics instead.")
    runConceptSetDiagnostics <- TRUE
  }
  if (runBreakdownIndexEvents) {
    warning(
      "runBreakdownIndexEvents is deprecated. Running runConceptSetDiagnostics instead."
    )
    runConceptSetDiagnostics <- TRUE
  }
  
  ParallelLogger::logInfo("Run Cohort Diagnostics started at ", start, '. Initiating...')
  
  # collect arguments that were passed to cohort diagnostics at initiation
  argumentsAtDiagnosticsInitiation <- formals(runCohortDiagnostics)
  argumentsAtDiagnosticsInitiationJson <-
    list(
      runInclusionStatistics = argumentsAtDiagnosticsInitiation$runInclusionStatistics,
      runConceptSetDiagnostics = argumentsAtDiagnosticsInitiation$runConceptSetDiagnostics,
      runVisitContext = argumentsAtDiagnosticsInitiation$runVisitContext,
      runIncidenceRate = argumentsAtDiagnosticsInitiation$runIncidenceRate,
      runCohortTimeSeries = argumentsAtDiagnosticsInitiation$runCohortTimeSeries,
      runDataSourceTimeSeries = argumentsAtDiagnosticsInitiation$runDataSourceTimeSeries,
      runCohortRelationship = argumentsAtDiagnosticsInitiation$runCohortRelationship,
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
  
  # Execution mode determination----
  if (!is.null(cohortSetReference)) {
    ParallelLogger::logInfo(" - Found cohortSetReference. Cohort Diagnostics will run in WebApi mode.")
    cohortToCreateFile <- NULL
  }
  
  if (all(!is.null(oracleTempSchema), is.null(tempEmulationSchema))) {
    tempEmulationSchema <- oracleTempSchema
    warning(
      ' - OracleTempSchema has been deprecated by DatabaseConnector. Please use tempEmulationSchema instead.'
    )
  }
  
  if (any(is.null(databaseName), is.na(databaseName))) {
    databaseName <- databaseId
    ParallelLogger::logTrace(' - Databasename was not provided.')
  }
  if (any(is.null(databaseDescription), is.na(databaseDescription))) {
    databaseDescription <- databaseId
    ParallelLogger::logTrace(' - Databasedescription was not provided.')
  }
  
  # Assert checks----
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(runInclusionStatistics, add = errorMessage)
  checkmate::assertLogical(runConceptSetDiagnostics, add = errorMessage)
  checkmate::assertLogical(runIncidenceRate, add = errorMessage)
  checkmate::assertLogical(runCohortCharacterization, add = errorMessage)
  checkmate::assertInt(
    x = cdmVersion,
    na.ok = FALSE,
    lower = 5,
    upper = 5,
    null.ok = FALSE,
    add = errorMessage
  )
  minCellCount <- utils::type.convert(minCellCount)
  checkmate::assertInteger(x = minCellCount, lower = 0, add = errorMessage)
  checkmate::assertLogical(incremental, add = errorMessage)
  
  if (any(
    runInclusionStatistics,
    runConceptSetDiagnostics,
    runIncidenceRate,
    runCohortTimeSeries,
    runCohortRelationship,
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
  if (isTRUE(runInclusionStatistics)) {
    errorMessage <-
      createIfNotExist(type = "folder",
                       name = inclusionStatisticsFolder,
                       errorMessage = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  # Cohort Info checks----
  cohorts <- getCohortsJsonAndSql(
    # get cohort json/sql from webapi or package
    packageName = packageName,
    cohortToCreateFile = cohortToCreateFile,
    baseUrl = baseUrl,
    cohortSetReference = cohortSetReference,
    cohortIds = cohortIds
  )
  
  if (nrow(cohorts) == 0) {
    stop("No cohorts specified, or no matching cohorts found. Aborting.")
  }
  if ('name' %in% colnames(cohorts)) {
    cohorts <- cohorts %>%
      dplyr::select(-.data$name)
  }
  cohortTableColumnNamesObserved <- colnames(cohorts) %>%
    sort()
  cohortTableColumnNamesExpected <-
    getResultsDataModelSpecifications(packageName = 'CohortDiagnostics') %>%
    dplyr::filter(.data$tableName == 'cohort') %>%
    dplyr::pull(.data$fieldName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    sort()
  cohortTableColumnNamesRequired <-
    getResultsDataModelSpecifications(packageName = 'CohortDiagnostics') %>%
    dplyr::filter(.data$tableName == 'cohort') %>%
    dplyr::filter(.data$isRequired == 'Yes') %>%
    dplyr::pull(.data$fieldName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    sort()
  
  requiredButNotObsevered <-
    setdiff(x = cohortTableColumnNamesRequired, y = cohortTableColumnNamesObserved)
  if (length(requiredButNotObsevered) > 0) {
    stop(paste(
      "- The following required fields not found in cohort table:",
      paste0(" '", requiredButNotObsevered, "'", collapse = ", ")
    ))
  }
  
  obseveredButNotExpected <-
    setdiff(x = cohortTableColumnNamesObserved,
            y = cohortTableColumnNamesExpected)
  if (length(obseveredButNotExpected) > 0) {
    ParallelLogger::logTrace(
      paste0(
        " - The following columns were found in cohort table, but are not expected - they will be removed:",
        paste0(" '", obseveredButNotExpected, "'", collapse = ",")
      )
    )
  }
  
  for (i in (1:length(cohortTableColumnNamesExpected))) {
    if (!cohortTableColumnNamesExpected[[i]] %in% colnames(cohorts)) {
      cohorts[[cohortTableColumnNamesExpected[[i]]]] <- NA
    }
  }
  
  cohorts <- cohorts %>%
    dplyr::select(cohortTableColumnNamesExpected)
  cohorts <- .replaceNaInDataFrameWithEmptyString(cohorts)
  writeToCsv(data = cohorts,
             fileName = file.path(exportFolder, "cohort.csv"))
  
  # Metadata----
  startMetaData <- Sys.time()
  ## CDM source information----
  cdmSourceInformation <-
    getCdmDataSourceInformation(connection = connection,
                                cdmDatabaseSchema = cdmDatabaseSchema)
  
  ## Vocabulary information----
  vocabularyVersion <-
    renderTranslateQuerySql(
      connection = connection,
      sql = "select * from @vocabulary_database_schema.vocabulary where vocabulary_id = 'None';",
      vocabulary_database_schema = vocabularyDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::pull(.data$vocabularyVersion) %>%
    unique()
  
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
  
  database <- dplyr::tibble(
    databaseId = databaseId,
    databaseName = dplyr::coalesce(databaseName, databaseId),
    description = dplyr::coalesce(databaseDescription, databaseId),
    vocabularyVersionCdm = cdmSourceInformation$vocabularyVersion,
    vocabularyVersion = !!vocabularyVersion,
    isMetaAnalysis = 0,
    observationPeriodMinDate = observationPeriodDateRange$observationPeriodMinDate,
    observationPeriodMaxDate = observationPeriodDateRange$observationPeriodMaxDate,
    persons = observationPeriodDateRange$persons,
    records = observationPeriodDateRange$records,
    personDays = observationPeriodDateRange$personDays
  )
  database <- .replaceNaInDataFrameWithEmptyString(database)
  writeToCsv(data = database,
             fileName = file.path(exportFolder, "database.csv"))
  delta <- Sys.time() - startMetaData
  ParallelLogger::logTrace(paste(
    " - Saving database metadata took",
    signif(delta, 3),
    attr(delta, "units")
  ))
  
  # Incremental mode----
  if (incremental) {
    ParallelLogger::logInfo(" - Working in incremental mode.")
    cohorts$checksum <- computeChecksum(cohorts$sql)
    recordKeepingFile <-
      file.path(incrementalFolder, "CreatedDiagnostics.csv")
    if (file.exists(path = recordKeepingFile)) {
      ParallelLogger::logTrace(
        "  - Found existing record keeping file in incremental folder - CreatedDiagnostics.csv."
      )
    } else {
      ParallelLogger::logTrace(
        "  - Did not find existing record keeping file in incremental folder. Creating file CreatedDiagnostics.csv."
      )
    }
  }
  
  # Counting cohorts----
  ParallelLogger::logTrace(" - Counting records & subjects for instantiated cohorts.")
  cohortCounts <- getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohorts$cohortId
  ) # cohortCounts is reused
  output <- list()
  output$cohort_count <- cohortCounts
  # Get instantiated cohorts ----
  instantiatedCohorts <-
    as.double(c(-1)) # set by default to non instantiated
  if (!is.null(cohortCounts)) {
    if (nrow(output$cohort_count) > 0) {
      writeToAllOutputToCsv(
        object = output,
        exportFolder = exportFolder,
        databaseId = databaseId,
        incremental = incremental,
        minCellCount = minCellCount
      )
      instantiatedCohorts <- output$cohort_count %>%
        dplyr::pull(.data$cohortId)
      ParallelLogger::logInfo(
        paste0(
          " - Found ",
          scales::comma(length(instantiatedCohorts)),
          " of ",
          scales::comma(length(instantiatedCohorts)),
          " (",
          scales::percent(length(instantiatedCohorts) / nrow(cohorts),
                          accuracy = 0.1),
          ") submitted cohorts instantiated. Beginning cohort diagnostics on instantiated cohorts. "
        )
      )
      output <- NULL
    } else {
      warning(
        " - All cohorts were either not instantiated or all have 0 records. All diagnostics will be empty."
      )
    }
  }
  
  # Inclusion statistics----
  ParallelLogger::logInfo(" - Retrieving inclusion rules from file.")
  if (runInclusionStatistics) {
    startInclusionStatistics <- Sys.time()
    if (any(is.null(instantiatedCohorts),-1 %in% instantiatedCohorts)) {
      ParallelLogger::logTrace("  - Skipping inclusion statistics from files because no cohorts were instantiated.")
    } else {
      subset <- subsetToRequiredCohorts(
        cohorts = cohorts %>%
          dplyr::filter(.data$cohortId %in% instantiatedCohorts),
        task = "runInclusionStatistics",
        incremental = incremental,
        recordKeepingFile = recordKeepingFile
      )
      if (nrow(subset) > 0) {
        if (incremental &&
            (length(instantiatedCohorts) - nrow(subset)) > 0) {
          ParallelLogger::logInfo(sprintf(
            "  - Skipping %s cohorts in incremental mode.",
            length(instantiatedCohorts) - nrow(subset)
          ))
        }
        stats <-
          getInclusionStatisticsFromFiles(cohortIds = subset$cohortId,
                                          folder = inclusionStatisticsFolder)
        writeToAllOutputToCsv(
          object = stats,
          exportFolder = exportFolder,
          databaseId = databaseId,
          incremental = incremental,
          minCellCount = minCellCount
        )
        recordTasksDone(
          cohortId = subset$cohortId,
          task = "runInclusionStatistics",
          checksum = subset$checksum,
          recordKeepingFile = recordKeepingFile,
          incremental = incremental
        )
      } else {
        ParallelLogger::logInfo("  - Skipping in incremental mode.")
      }
    }
    
    delta <- Sys.time() - startInclusionStatistics
    ParallelLogger::logTrace(" - Running Inclusion Statistics took ",
                             signif(delta, 3),
                             " ",
                             attr(delta, "units"))
  }
  
  # Concept set diagnostics----
  if (runConceptSetDiagnostics) {
    # running together because share common process of needing to resolve concept sets
    ParallelLogger::logInfo(" - Beginning concept set diagnostics.")
    # note for incremental mode - if a cohort id is eligible for computation for any diagnostics,
    # all diagnostics are computed for that cohort
    startConceptSetDiagnostics <- Sys.time()
    subset <- subsetToRequiredCohorts(
      cohorts = cohorts,
      task = "runConceptSetDiagnostics",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    if (nrow(subset) > 0) {
      if (nrow(cohorts) - nrow(subset) > 0) {
        ParallelLogger::logInfo(sprintf(
          "  - Skipping %s cohorts in incremental mode.",
          nrow(cohorts) - nrow(subset)
        ))
      }
      conceptSetDiagnostics <- runConceptSetDiagnostics(
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        cohorts = cohorts,
        cohortIds = subset$cohortId,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        exportDetailedVocabulary = exportDetailedVocabulary
      )
      writeToAllOutputToCsv(
        object = conceptSetDiagnostics,
        exportFolder = exportFolder,
        databaseId = databaseId,
        incremental = incremental,
        minCellCount = minCellCount
      )
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runConceptSetDiagnostics",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    } else {
      ParallelLogger::logInfo("  - Skipping in incremental mode.")
    }
    delta <- Sys.time() - startConceptSetDiagnostics
    ParallelLogger::logInfo(
      " - Running Concept Set Diagnostics and saving files took ",
      signif(delta, 3),
      " ",
      attr(delta, "units")
    )
  }
  
  # Visit context----
  if (runVisitContext) {
    ParallelLogger::logInfo("Retrieving visit context for index dates")
    startVisitContext <- Sys.time()
    subset <- subsetToRequiredCohorts(
      cohorts = cohorts %>%
        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
      task = "runVisitContext",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    if (nrow(subset) > 0) {
      if (incremental &&
          (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo(sprintf(
          " - Skipping %s cohorts in incremental mode.",
          length(instantiatedCohorts) - nrow(subset)
        ))
      }
      output <- list()
      output$data <- runVisitContextDiagnostics(
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        cohortTable = cohortTable,
        cdmVersion = cdmVersion,
        cohortIds = subset$cohortId
      )
      writeToAllOutputToCsv(
        object = output,
        exportFolder = exportFolder,
        databaseId = databaseId,
        incremental = incremental,
        minCellCount = minCellCount
      )
      output <- NULL
    } else {
      ParallelLogger::logInfo("  - Skipping in incremental mode.")
    }
    delta <- Sys.time() - startVisitContext
    ParallelLogger::logInfo(" - Running Visit Context and saving files took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  # Incidence rates----
  if (runIncidenceRate) {
    ParallelLogger::logInfo("Computing incidence rates")
    startIncidenceRate <- Sys.time()
    subset <- subsetToRequiredCohorts(
      cohorts = cohorts %>%
        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
      task = "runIncidenceRate",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    if (nrow(subset) > 0) {
      if (incremental &&
          (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo(sprintf(
          " - Skipping %s cohorts in incremental mode.",
          length(instantiatedCohorts) - nrow(subset)
        ))
      }
      runIncidenceRate <- function(row) {
        ParallelLogger::logInfo(" - '",
                                row$cohortName,
                                "'")
        cohortExpression <- RJSONIO::fromJSON(row$json, digits = 23)
        washoutPeriod <- tryCatch({
          cohortExpression$PrimaryCriteria$ObservationWindow$PriorDays
        }, error = function(e) {
          0
        })
        data <- runIncidenceRateDiagnostics(
          connection = connection,
          cdmDatabaseSchema = cdmDatabaseSchema,
          tempEmulationSchema = tempEmulationSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          cohortId = row$cohortId,
          firstOccurrenceOnly = TRUE,
          washoutPeriod = washoutPeriod
        )
        if (is.null(data)) {
          return(NULL)
        }
        if (nrow(data) == 0) {
          data <- NULL
        }
        if (nrow(data) > 0) {
          data <- data %>%
            dplyr::mutate(cohortId = row$cohortId)
        }
        return(data)
      }
      data <-
        lapply(split(subset, subset$cohortId), runIncidenceRate)
      output <- list()
      output$data <- dplyr::bind_rows(data)
      data <- NULL
      writeToAllOutputToCsv(
        object = output,
        exportFolder = exportFolder,
        databaseId = databaseId,
        incremental = incremental,
        minCellCount = minCellCount
      )
      output <- NULL
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runIncidenceRate",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    } else {
      ParallelLogger::logInfo("  - Skipping in incremental mode.")
    }
    delta <- Sys.time() - startIncidenceRate
    ParallelLogger::logInfo(" - Running Incidence Rate took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  # Time Series----
  if (any(runCohortTimeSeries, runDataSourceTimeSeries)) {
    ParallelLogger::logInfo("Computing Time Series")
    startTimeSeries <- Sys.time()
    cohortIds <- NULL
    if (runCohortTimeSeries) {
      subset <- subsetToRequiredCohorts(
        cohorts = cohorts %>%
          dplyr::filter(.data$cohortId %in% instantiatedCohorts),
        task = "runCohortTimeSeries",
        incremental = incremental,
        recordKeepingFile = recordKeepingFile
      )
      cohortIds <- subset$cohortId
      
      if (nrow(subset) > 0) {
        if (incremental &&
            (length(instantiatedCohorts) - nrow(subset)) > 0) {
          ParallelLogger::logInfo(sprintf(
            " - Skipping %s cohorts in incremental mode.",
            length(instantiatedCohorts) - nrow(subset)
          ))
        }
        output <- list()
        output$timeSeries <-
          runCohortTimeSeriesDiagnostics(
            connection = connection,
            tempEmulationSchema = tempEmulationSchema,
            cohortDatabaseSchema = cohortDatabaseSchema,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortTable = cohortTable,
            runDataSourceTimeSeries = runDataSourceTimeSeries,
            runCohortTimeSeries = runCohortTimeSeries,
            timeSeriesMinDate = observationPeriodDateRange$observationPeriodMinDate,
            timeSeriesMaxDate = observationPeriodDateRange$observationPeriodMaxDate,
            cohortIds = cohortIds
          )
        writeToAllOutputToCsv(
          object = output,
          exportFolder = exportFolder,
          databaseId = databaseId,
          incremental = incremental,
          minCellCount = minCellCount
        )
        output <- NULL
        recordTasksDone(
          cohortId = subset$cohortId,
          task = "runCohortTimeSeries",
          checksum = subset$checksum,
          recordKeepingFile = recordKeepingFile,
          incremental = incremental
        )
      } else {
        ParallelLogger::logInfo("  - Skipping in incremental mode.")
      }
    }
    delta <- Sys.time() - startTimeSeries
    ParallelLogger::logInfo(" - Computing time series took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  # Cohort Relationship ----
  if (runCohortRelationship) {
    ParallelLogger::logInfo("Computing Cohort Relationship")
    startCohortRelationship <- Sys.time()
    
    # no point in incremental for cohort relationship, because we have
    # to compute the relationship between combinations of target and
    # comparators. What do we increment on - target? Then we
    # will still have to read all the comparator cohorts which
    # is the full cohort table
    # The only purpose of incremental is to prevent re-run
    # Its all or none - Even if one cohort need computation, everything is run
    
    subset <- subsetToRequiredCohorts(
      cohorts = cohorts %>%
        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
      task = "runCohortRelationship",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    if (nrow(subset) > 0) {
      if (incremental &&
          (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo(sprintf(
          " - Skipping %s cohort combinations in incremental mode.",
          nrow(cohorts) - nrow(subset)
        ))
      }
      ParallelLogger::logTrace(" - Beginning Cohort Relationship SQL")
      cohortRelationship <-
        runCohortRelationshipDiagnostics(
          connection = connection,
          cohortDatabaseSchema = cohortDatabaseSchema,
          tempEmulationSchema = tempEmulationSchema,
          cohortTable = cohortTable,
          targetCohortIds = subset$cohortId,
          comparatorCohortIds = cohorts$cohortId
        )
      
      if (nrow(cohortRelationship) > 0) {
        cohortRelationship <- cohortRelationship %>%
          dplyr::mutate(databaseId = !!databaseId)
        columnsInCohortRelationship <- c(
          'bothSubjects',
          'cBeforeTSubjects',
          'tBeforeCSubjects',
          'sameDaySubjects',
          'cPersonDays',
          'cSubjectsStart',
          'cSubjectsEnd',
          'cInTSubjects'
        )
        for (i in (1:length(columnsInCohortRelationship))) {
          cohortRelationship <-
            enforceMinCellValue(cohortRelationship,
                                columnsInCohortRelationship[[i]],
                                minCellCount)
        }
        cohortRelationship <-
          .replaceNaInDataFrameWithEmptyString(cohortRelationship)
        writeToCsv(
          data = cohortRelationship,
          fileName = file.path(exportFolder, "cohort_relationships.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      } else {
        warning('No cohort relationship data')
      }
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runCohortRelationship",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    } else {
      ParallelLogger::logInfo("  - Skipping in incremental mode.")
    }
    delta <- Sys.time() - startCohortRelationship
    ParallelLogger::logInfo(" - Computing cohort relationships took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  # Characterization----
  ## Cohort characterization----
  if (runCohortCharacterization) {
    ParallelLogger::logInfo("Characterizing cohorts")
    startCohortCharacterization <- Sys.time()
    subset <- subsetToRequiredCohorts(
      cohorts = cohorts %>%
        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
      task = "runCohortCharacterization",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    
    if (nrow(subset) > 0) {
      if (incremental &&
          (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo(sprintf(
          " - Skipping %s cohorts in incremental mode.",
          length(instantiatedCohorts) - nrow(subset)
        ))
      }
      characteristics <-
        runCohortCharacterizationDiagnostics(
          connection = connection,
          cdmDatabaseSchema = cdmDatabaseSchema,
          tempEmulationSchema = tempEmulationSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          cohortIds = subset$cohortId,
          covariateSettings = covariateSettings,
          cdmVersion = cdmVersion
        )
      
      exportFeatureExtractionOutput(
        featureExtractionDbCovariateData = characteristics,
        databaseId = databaseId,
        incremental = incremental,
        covariateValueFileName = file.path(exportFolder, "covariate_value.csv"),
        covariateValueContFileName = file.path(exportFolder, "covariate_value_dist.csv"),
        covariateRefFileName = file.path(exportFolder, "covariate_ref.csv"),
        analysisRefFileName = file.path(exportFolder, "analysis_ref.csv"),
        timeDistributionFileName = file.path(exportFolder, "time_distribution.csv"),
        cohortCounts = cohortCounts,
        minCellCount = minCellCount
      )
    } else {
      ParallelLogger::logInfo("  - Skipping in incremental mode.")
    }
    recordTasksDone(
      cohortId = subset$cohortId,
      task = "runCohortCharacterization",
      checksum = subset$checksum,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
    delta <- Sys.time() - startCohortCharacterization
    ParallelLogger::logInfo(" - Running Characterization took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  ## Temporal Cohort characterization----
  if (runTemporalCohortCharacterization) {
    ParallelLogger::logInfo("Temporal Cohort characterization")
    startTemporalCohortCharacterization <- Sys.time()
    subset <- subsetToRequiredCohorts(
      cohorts = cohorts %>%
        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
      task = "runTemporalCohortCharacterization",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    
    if (nrow(subset) > 0) {
      if (incremental &&
          (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo(sprintf(
          " - Skipping %s cohorts in incremental mode.",
          length(instantiatedCohorts) - nrow(subset)
        ))
      }
      characteristics <-
        runCohortCharacterizationDiagnostics(
          connection = connection,
          cdmDatabaseSchema = cdmDatabaseSchema,
          tempEmulationSchema = tempEmulationSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          cohortIds = subset$cohortId,
          covariateSettings = temporalCovariateSettings,
          cdmVersion = cdmVersion
        )
      exportFeatureExtractionOutput(
        featureExtractionDbCovariateData = characteristics,
        databaseId = databaseId,
        incremental = incremental,
        covariateValueFileName = file.path(exportFolder, "temporal_covariate_value.csv"),
        covariateRefFileName = file.path(exportFolder, "temporal_covariate_ref.csv"),
        analysisRefFileName = file.path(exportFolder, "temporal_analysis_ref.csv"),
        timeRefFileName = file.path(exportFolder, "temporal_time_ref.csv"),
        # add temporal_covariate_value_dist.csv, but timeId does not seem to be correctly returned
        cohortCounts = cohortCounts,
        minCellCount = minCellCount
      )
    } else {
      ParallelLogger::logInfo("  - Skipping in incremental mode.")
    }
    recordTasksDone(
      cohortId = subset$cohortId,
      task = "runTemporalCohortCharacterization",
      checksum = subset$checksum,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
    delta <- Sys.time() - startTemporalCohortCharacterization
    ParallelLogger::logInfo(
      " - Running Temporal Characterization took ",
      signif(delta, 3),
      " ",
      attr(delta, "units")
    )
  }
  
  # Writing metadata file
  ParallelLogger::logInfo("Retrieving metadata information and writing metadata")
  metadata <-
    dplyr::tibble(
      databaseId = as.character(!!databaseId),
      startTime = paste0("DT-", as.character(start)),
      variableField = c(
        "runTime",
        "runTimeUnits",
        "packageDependencySnapShotJson",
        "argumentsAtDiagnosticsInitiationJson",
        "Rversion",
        "CurrentPackage",
        "CurrentPackageVersion",
        "sourceDescription",
        "cdmSourceName",
        "sourceReleaseDate",
        "cdmVersion",
        "cdmReleaseDate",
        "vocabularyVersion"
      ),
      valueField =  c(
        as.character(as.numeric(
          x = delta, units = attr(delta, "units")
        )),
        as.character(attr(delta, "units")),
        packageDependencySnapShotJson,
        argumentsAtDiagnosticsInitiationJson,
        as.character(R.Version()$version.string),
        as.character(nullToEmpty(packageName())),
        as.character(if (!getPackageName() == ".GlobalEnv") {
          packageVersion(packageName())
        } else {
          ''
        }),
        as.character(nullToEmpty(
          cdmSourceInformation$sourceDescription
        )),
        as.character(nullToEmpty(cdmSourceInformation$cdmSourceName)),
        as.character(nullToEmpty(
          cdmSourceInformation$sourceReleaseDate
        )),
        as.character(nullToEmpty(cdmSourceInformation$cdmVersion)),
        as.character(nullToEmpty(cdmSourceInformation$cdmReleaseDate)),
        as.character(nullToEmpty(
          cdmSourceInformation$vocabularyVersion
        ))
      )
    )
  metadata <- .replaceNaInDataFrameWithEmptyString(metadata)
  writeToCsv(
    data = metadata,
    fileName = file.path(exportFolder, "metadata.csv"),
    incremental = TRUE,
    start_time = as.character(start)
  )
  
  # Add all to zip file----
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <-
    file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo(" - Results are ready for sharing at: ", zipName)
  
  delta <- Sys.time() - start
  
  ParallelLogger::logInfo("Computing all diagnostics took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
}
