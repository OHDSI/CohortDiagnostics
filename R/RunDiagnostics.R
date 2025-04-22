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

#' Get default covariate settings
#' @description
#' Default covariate settings for cohort diagnostics execution
#'
#' Overriding this behaviour is possible, however, certain time windows are requirement of other diagnostics.
#' For this reason, the time windows will be included, regardless of user specifications:
#'
#' (-365, 0),
#' (-30, 0),
#' (-365, -31),
#' (-30, -1),
#' (0, 0),
#' (1, 30),
#' (31, 365),
#' (-9999, 9999)
#' @export
getDefaultCovariateSettings <- function() {
  FeatureExtraction::createTemporalCovariateSettings(
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
    useMeasurementValueAsConcept = TRUE,
    useMeasurementRangeGroup = TRUE,
    useConditionEraStart = TRUE,
    useConditionEraOverlap = TRUE,
    useConditionEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
    useConditionEraGroupOverlap = TRUE,
    useDrugExposure = FALSE, # leads to too many concept id
    useDrugEraOverlap = FALSE,
    useDrugEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
    useDrugEraGroupOverlap = TRUE,
    useObservation = TRUE,
    useObservationValueAsConcept = TRUE,
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
  )
}

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

#' @template cdmVersion
#' @param runInclusionStatistics      Generate and export statistic on the cohort inclusion rules?
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runTimeSeries               Generate and export the time series diagnostics?
#' @param runVisitContext             Generate and export index-date visit context?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#' @param runIncidenceRate            Generate and export the cohort incidence  rates?
#' @inheritParams createCohortDiagnosticsSettings
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
executeDiagnostics <- function(...,
                               runInclusionStatistics = TRUE,
                               runIncludedSourceConcepts = TRUE,
                               runOrphanConcepts = TRUE,
                               runTimeSeries = FALSE,
                               runVisitContext = TRUE,
                               runBreakdownIndexEvents = TRUE,
                               runIncidenceRate = TRUE,
                               runCohortRelationship = TRUE,
                               runTemporalCohortCharacterization = TRUE) {

  cdSettings <- createCohortDiagnosticsSettings(...)
  start <- Sys.time()
  ParallelLogger::logInfo("Run Cohort Diagnostics started at ", start)
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(runInclusionStatistics, add = errorMessage)
  checkmate::assertLogical(runIncludedSourceConcepts, add = errorMessage)
  checkmate::assertLogical(runOrphanConcepts, add = errorMessage)
  checkmate::assertLogical(runTimeSeries, add = errorMessage)
  checkmate::assertLogical(runBreakdownIndexEvents, add = errorMessage)
  checkmate::assertLogical(runIncidenceRate, add = errorMessage)
  checkmate::assertLogical(runTemporalCohortCharacterization, add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)

  cdSettings$temporalCovariateSettings <- cdSettings$checkDefaultTemporalCovariateSettings()

  cohortDefinitionSet <- makeDataExportable(
    x = cdSettings$cohortDefinitionSet,
    tableName = "cohort",
    minCellCount = cdSettings$minCellCount,
    databaseId = NULL
  )

  writeToCsv(
    data = cdSettings$cohortDefinitionSet,
    fileName = file.path(cdSettings$exportFolder, "cohort.csv")
  )

  subsets <- CohortGenerator::getSubsetDefinitions(cdSettings$cohortDefinitionSet)
  if (length(subsets)) {
    dfs <- lapply(subsets, function(x) {
      data.frame(subsetDefinitionId = x$definitionId, json = as.character(x$toJSON()))
    })
    subsetDefinitions <- data.frame()
    for (subsetDef in dfs) {
      subsetDefinitions <- rbind(subsetDefinitions, subsetDef)
    }

    writeToCsv(
      data = subsetDefinitions,
      fileName = file.path(cdSettings$exportFolder, "subset_definition.csv")
    )
  }

  connection <- cdSettings$getConnection()
  ## CDM source information----
  timeExecution(
    cdSettings$exportFolder,
    taskName = "getCdmDataSourceInformation",
    cohortIds = NULL,
    parent = "executeDiagnostics",
    expr = {
      cdmSourceInformation <-
        getCdmDataSourceInformation(
          connection = connection,
          cdmDatabaseSchema = cdSettings$cdmDatabaseSchema
        )
      ## Use CDM source table as default name/description
      if (!is.null(cdmSourceInformation)) {
        if (any(is.null(cdSettings$databaseName), is.na(cdSettings$databaseName))) {
          databaseName <- cdmSourceInformation$cdmSourceName
        }

        if (any(is.null(cdSettings$databaseDescription), is.na(cdSettings$databaseDescription))) {
          databaseDescription <- cdmSourceInformation$sourceDescription
        }
      } else {
        if (any(is.null(cdSettings$databaseName), is.na(cdSettings$databaseName))) {
          databaseName <- databaseId
        }

        if (any(is.null(cdSettings$databaseDescription), is.na(cdSettings$databaseDescription))) {
          databaseDescription <- databaseName
        }
      }
      vocabularyVersion <- getVocabularyVersion(connection, vocabularyDatabaseSchema)
    }
  )

  cdSettings$cohortDefinitionSet$checksum <- computeChecksum(cdSettings$cohortDefinitionSet$sql)

  if (cdSettings$incremental) {
    ParallelLogger::logDebug("Working in incremental mode.")
    recordKeepingFile <-
      file.path(cdSettings$incrementalFolder, "CreatedDiagnostics.csv")
    if (file.exists(path = recordKeepingFile)) {
      ParallelLogger::logInfo(
        "Found existing record keeping file in incremental folder - CreatedDiagnostics.csv"
      )
    }
  }

  ## Observation period----
  ParallelLogger::logTrace(" - Collecting date range from Observational period table.")
  timeExecution(
    cdSettings$exportFolder,
    taskName = "observationPeriodDateRange",
    cohortIds = NULL,
    parent = "executeDiagnostics",
    expr = {
      observationPeriodDateRange <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT MIN(observation_period_start_date) observation_period_min_date,
             MAX(observation_period_end_date) observation_period_max_date,
             COUNT(distinct person_id) persons,
             COUNT(person_id) records,
             SUM(CAST(DATEDIFF(dd, observation_period_start_date, observation_period_end_date) AS BIGINT)) person_days
             FROM @cdm_database_schema.observation_period;",
        cdm_database_schema = cdSettings$cdmDatabaseSchema,
        snakeCaseToCamelCase = TRUE,
        tempEmulationSchema = cdSettings$tempEmulationSchema
      )
    }
  )
  # Database metadata ---------------------------------------------
  saveDatabaseMetaData(
    databaseId = cdSettings$databaseId,
    databaseName = cdSettings$databaseName,
    databaseDescription = cdSettings$databaseDescription,
    exportFolder = cdSettings$exportFolder,
    minCellCount = cdSettings$minCellCount,
    vocabularyVersionCdm = cdmSourceInformation$vocabularyVersion,
    vocabularyVersion = cdSettings$vocabularyVersion
  )
  # Create concept table ------------------------------------------
  createConceptTable(connection, cdSettings$tempEmulationSchema)

  # Counting cohorts -----------------------------------------------------------------------
  timeExecution(
    cdSettings$exportFolder,
    taskName = "getInclusionStats",
    cohortIds = cdSettings$cohortIds,
    parent = "executeDiagnostics",
    expr = {
      cohortCounts <- computeCohortCounts(
        connection = connection,
        cohortDatabaseSchema = cdSettings$cohortDatabaseSchema,
        cohortTable = cdSettings$cohortTable,
        cohorts = cdSettings$cohortDefinitionSet,
        exportFolder = cdSettings$exportFolder,
        minCellCount = cdSettings$minCellCount,
        databaseId = cdSettings$databaseId
      )
    }
  )

  if (nrow(cohortCounts) > 0) {
    instantiatedCohorts <- cohortCounts |>
      dplyr::filter(.data$cohortEntries > 0) |>
      dplyr::pull(.data$cohortId)
    ParallelLogger::logInfo(
      sprintf(
        "Found %s of %s (%1.2f%%) submitted cohorts instantiated. ",
        length(instantiatedCohorts),
        nrow(cdSettings$cohortDefinitionSet),
        100 * (length(instantiatedCohorts) / nrow(cdSettings$cohortDefinitionSet))
      ),
      "Beginning cohort diagnostics for instantiated cohorts. "
    )
  } else {
    stop("All cohorts were either not instantiated or all have 0 records.")
  }

  cdSettings$cohortDefinitionSet <- cdSettings$cohortDefinitionSet |>
    dplyr::filter(.data$cohortId %in% instantiatedCohorts)

  # Inclusion statistics -----------------------------------------------------------------------
  if (runInclusionStatistics) {
    timeExecution(
      cdSettings$exportFolder,
      "getInclusionStats",
      cdSettings$cohortIds,
      parent = "executeDiagnostics",
      expr = {
        getInclusionStats(
          connection = connection,
          exportFolder = cdSettings$exportFolder,
          databaseId = cdSettings$databaseId,
          cohortDefinitionSet = cdSettings$cohortDefinitionSet,
          cohortDatabaseSchema = cdSettings$cohortDatabaseSchema,
          cohortTableNames = cdSettings$cohortTableNames,
          incremental = cdSettings$incremental,
          instantiatedCohorts = cdSettings$instantiatedCohorts,
          minCellCount = cdSettings$minCellCount,
          recordKeepingFile = recordKeepingFile
        )
      }
    )
  }

  # Always export concept sets to csv
  exportConceptSets(
    cohortDefinitionSet = cdSettings$cohortDefinitionSet,
    exportFolder = cdSettings$exportFolder,
    minCellCount = cdSettings$minCellCount,
    databaseId = cdSettings$databaseId
  )

  # Concept set diagnostics -----------------------------------------------
  if (runIncludedSourceConcepts ||
    runOrphanConcepts ||
    runBreakdownIndexEvents) {
    timeExecution(
      cdSettings$exportFolder,
      taskName = "runConceptSetDiagnostics",
      cdSettings$cohortIds,
      parent = "executeDiagnostics",
      expr = {
        runConceptSetDiagnostics(
          connection = connection,
          tempEmulationSchema = cdSettings$tempEmulationSchema,
          cdmDatabaseSchema = cdSettings$cdmDatabaseSchema,
          vocabularyDatabaseSchema = cdSettings$vocabularyDatabaseSchema,
          databaseId = cdSettings$databaseId,
          cohorts = cdSettings$cohortDefinitionSet,
          runIncludedSourceConcepts = runIncludedSourceConcepts,
          runOrphanConcepts = runOrphanConcepts,
          runBreakdownIndexEvents = runBreakdownIndexEvents,
          exportFolder = cdSettings$exportFolder,
          minCellCount = cdSettings$minCellCount,
          conceptCountsDatabaseSchema = NULL,
          conceptCountsTable = "#concept_counts",
          conceptCountsTableIsTemp = TRUE,
          cohortDatabaseSchema = cdSettings$cohortDatabaseSchema,
          cohortTable = cdSettings$cohortTable,
          useExternalConceptCountsTable = FALSE,
          incremental = cdSettings$incremental,
          conceptIdTable = "#concept_ids",
          recordKeepingFile = recordKeepingFile
        )
      }
    )
  }

  # Time series ----------------------------------------------------------------------
  if (runTimeSeries) {
    timeExecution(
      cdSettings$exportFolder,
      "executeTimeSeriesDiagnostics",
      cdSettings$cohortIds,
      parent = "executeDiagnostics",
      expr = {
        executeTimeSeriesDiagnostics(
          connection = connection,
          tempEmulationSchema = cdSettings$tempEmulationSchema,
          cdmDatabaseSchema = cdSettings$cdmDatabaseSchema,
          cohortDatabaseSchema = ccdSettings$ohortDatabaseSchema,
          cohortTable = cdSettings$cohortTable,
          cohortDefinitionSet = cdSettings$cohortDefinitionSet,
          databaseId = cdSettings$databaseId,
          exportFolder = cdSettings$exportFolder,
          minCellCount = cdSettings$minCellCount,
          instantiatedCohorts = cdSettings$instantiatedCohorts,
          incremental = cdSettings$incremental,
          recordKeepingFile = recordKeepingFile,
          observationPeriodDateRange = observationPeriodDateRange
        )
      }
    )
  }


  # Visit context ----------------------------------------------------------------------------
  if (runVisitContext) {
    timeExecution(
      cdSettings$exportFolder,
      "executeVisitContextDiagnostics",
      cdSettings$cohortIds,
      parent = "executeDiagnostics",
      expr = {
        executeVisitContextDiagnostics(
          connection = connection,
          tempEmulationSchema = cdSettings$tempEmulationSchema,
          cdmDatabaseSchema = cdSettings$cdmDatabaseSchema,
          cohortDatabaseSchema = cdSettings$cohortDatabaseSchema,
          cohortTable = cdSettings$cohortTable,
          cdmVersion = cdSettings$cdmVersion,
          databaseId = cdSettings$databaseId,
          exportFolder = cdSettings$exportFolder,
          minCellCount = cdSettings$minCellCount,
          cohorts = cdSettings$cohortDefinitionSet,
          instantiatedCohorts = instantiatedCohorts,
          recordKeepingFile = recordKeepingFile,
          incremental = cdSettings$incremental
        )
      }
    )
  }

  # Incidence rates --------------------------------------------------------------------------------------
  if (runIncidenceRate) {
    timeExecution(
      cdSettings$exportFolder,
      "computeIncidenceRates",
      cdSettings$cohortIds,
      parent = "executeDiagnostics",
      expr = {
        computeIncidenceRates(
          connection = connection,
          tempEmulationSchema = cdSettings$tempEmulationSchema,
          cdmDatabaseSchema = cdSettings$cdmDatabaseSchema,
          cohortDatabaseSchema = cdSettings$cohortDatabaseSchema,
          cohortTable = cdSettings$cohortTable,
          databaseId = cdSettings$databaseId,
          exportFolder = cdSettings$exportFolder,
          minCellCount = cdSettings$minCellCount,
          cohorts = cdSettings$cohortDefinitionSet,
          washoutPeriod = cdSettings$irWashoutPeriod,
          instantiatedCohorts = cdSettings$instantiatedCohorts,
          recordKeepingFile = recordKeepingFile,
          incremental = cdSettings$incremental
        )
      }
    )
  }

  # Cohort relationship ---------------------------------------------------------------------------------
  if (runCohortRelationship && nrow(cdSettings$cohortDefinitionSet) > 1) {
    covariateCohorts <- cdSettings$cohortDefinitionSet |> dplyr::select("cohortId", "cohortName")
    analysisId <- as.integer(Sys.getenv("OHDSI_CD_CF_ANALYSIS_ID", unset = 173))

    cohortFeSettings <-
      FeatureExtraction::createCohortBasedTemporalCovariateSettings(
        analysisId = analysisId, # problem - how to assign this uniquely?
        covariateCohortDatabaseSchema = cdSettings$cohortDatabaseSchema,
        covariateCohortTable = cdSettings$cohortTableNames$cohortTable,
        covariateCohorts = covariateCohorts,
        valueType = "binary",
        temporalStartDays = cdSettings$temporalCovariateSettings[[1]]$temporalStartDays,
        temporalEndDays = cdSettings$temporalCovariateSettings[[1]]$temporalEndDays
      )
    # Add feature set
    cdSettings$temporalCovariateSettings[[length(cdSettings$temporalCovariateSettings$temporalCovariateSettings) + 1]] <- cohortFeSettings
  }


  feCohortDefinitionSet <- cdSettings$cohortDefinitionSet
  feCohortTable <- cdSettings$cohortTable
  feCohortCounts <- cdSettings$cohortCounts
  # Temporal Cohort characterization ---------------------------------------------------------------
  if (runTemporalCohortCharacterization) {
    if (cdSettings$runFeatureExtractionOnSample & !isTRUE(attr(cdSettings$cohortDefinitionSet, "isSampledCohortDefinition"))) {
      cdSettings$cohortTableNames$cohortSampleTable <- paste0(cdSettings$cohortTableNames$cohortTable, "_cd_sample")
      CohortGenerator::createCohortTables(
        connection = connection,
        cohortTableNames = cdSettings$cohortTableNames,
        cohortDatabaseSchema = cdSettings$cohortDatabaseSchema,
        incremental = TRUE
      )

      feCohortTable <- cdSettings$cohortTableNames$cohortSampleTable
      feCohortDefinitionSet <-
        CohortGenerator::sampleCohortDefinitionSet(
          connection = connection,
          cohortDefinitionSet = cdSettings$cohortDefinitionSet,
          tempEmulationSchema = cdSettings$tempEmulationSchema,
          cohortDatabaseSchema = cdSettings$cohortDatabaseSchema,
          cohortTableNames = cdSettings$cohortTableNames,
          n = cdSettings$sampleN,
          seed = cdSettings$seed,
          seedArgs = cdSettings$seedArgs,
          identifierExpression = "cohortId",
          incremental = cdSettings$incremental,
          incrementalFolder = cdSettings$incrementalFolder
        )

      feCohortCounts <- computeCohortCounts(
        connection = connection,
        cohortDatabaseSchema = cdSettings$cohortDatabaseSchema,
        cohortTable = cdSettings$cohortTableNames$cohortSampleTable,
        cohorts = feCohortDefinitionSet,
        exportFolder = cdSettings$exportFolder,
        minCellCount = cdSettings$minCellCount,
        databaseId = cdSettings$databaseId,
        writeResult = FALSE
      )
    }
  } else {
    cdSettings$temporalCovariateSettings <- cdSettings$temporalCovariateSettings[-1]
  }

  if (length(cdSettings$temporalCovariateSettings)) {
    timeExecution(
      cdSettings$exportFolder,
      "executeCohortCharacterization",
      cdSettings$cohortIds,
      parent = "executeDiagnostics",
      expr = {
        executeCohortCharacterization(
          connection = connection,
          databaseId = cdSettings$databaseId,
          exportFolder = cdSettings$exportFolder,
          cdmDatabaseSchema = cdSettings$cdmDatabaseSchema,
          cohortDatabaseSchema = cdSettings$cohortDatabaseSchema,
          cohortTable = feCohortTable,
          covariateSettings = cdSettings$temporalCovariateSettings,
          tempEmulationSchema = cdSettings$tempEmulationSchema,
          cdmVersion = cdSettings$cdmVersion,
          cohorts = feCohortDefinitionSet,
          cohortCounts = feCohortCounts,
          minCellCount = cdSettings$minCellCount,
          instantiatedCohorts = instantiatedCohorts,
          incremental = cdSettings$incremental,
          recordKeepingFile = recordKeepingFile,
          task = "runTemporalCohortCharacterization",
          jobName = "Temporal Cohort characterization",
          covariateValueFileName = file.path(cdSettings$exportFolder, "temporal_covariate_value.csv"),
          covariateValueContFileName = file.path(cdSettings$exportFolder, "temporal_covariate_value_dist.csv"),
          covariateRefFileName = file.path(cdSettings$exportFolder, "temporal_covariate_ref.csv"),
          analysisRefFileName = file.path(cdSettings$exportFolder, "temporal_analysis_ref.csv"),
          timeRefFileName = file.path(cdSettings$exportFolder, "temporal_time_ref.csv"),
          minCharacterizationMean = cdSettings$minCharacterizationMean
        )
      }
    )
  }

  # Store information from the vocabulary on the concepts used -------------------------
  timeExecution(
    cdSettings$exportFolder,
    "exportConceptInformation",
    parent = "executeDiagnostics",
    expr = {
      exportConceptInformation(
        connection = connection,
        vocabularyDatabaseSchema = cdSettings$vocabularyDatabaseSchema,
        tempEmulationSchema = cdSettings$tempEmulationSchema,
        conceptIdTable = "#concept_ids",
        incremental = cdSettings$incremental,
        exportFolder = cdSettings$exportFolder
      )
    }
  )
  # Delete unique concept ID table ---------------------------------
  ParallelLogger::logTrace("Deleting concept ID table")
  timeExecution(
    cdSettings$exportFolder,
    "DeleteConceptIdTable",
    parent = "executeDiagnostics",
    expr = {
      sql <- "TRUNCATE TABLE @table;\nDROP TABLE @table;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        tempEmulationSchema = cdSettings$tempEmulationSchema,
        table = "#concept_ids",
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
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

  timeExecution(
    exportFolder = cdSettings$exportFolder,
    taskName = "executeDiagnostics",
    parent = NULL,
    cohortIds = NULL,
    start = start,
    execTime = delta
  )

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
    "{}",
    # 4
    cdSettings$toJson(),
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
    databaseId = as.character(!!cdSettings$databaseId),
    startTime = paste0("TM_", as.character(start)),
    variableField = variableField,
    valueField = valueField
  )
  metadata <- makeDataExportable(
    x = metadata,
    tableName = "metadata",
    minCellCount = cdSettings$minCellCount,
    databaseId = cdSettings$databaseId
  )
  writeToCsv(
    data = metadata,
    fileName = file.path(cdSettings$exportFolder, "metadata.csv"),
    incremental = TRUE,
    start_time = as.character(start)
  )

  # Add all to zip file -------------------------------------------------------------------------------
  timeExecution(
    cdSettings$exportFolder,
    "writeResultsZip",
    NULL,
    parent = "executeDiagnostics",
    expr = {
      writeResultsZip(cdSettings$exportFolder, cdSettings$databaseId)
    }
  )

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
