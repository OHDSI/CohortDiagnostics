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
    useConditionEraGroupStart = TRUE,
    useConditionEraGroupOverlap = TRUE,
    useDrugExposure = FALSE, # leads to too many concept id
    useDrugEraOverlap = FALSE,
    useDrugEraGroupStart = TRUE,
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
#' @param exportFolder                The folder where the output will be exported to. If this folder
#'                                    does not exist it will be created.
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    diagnostics to.
#' @param cohortDefinitionSet         Data.frame of cohorts must include columns cohortId, cohortName, json, sql
#' @param cohortTableNames            Cohort Table names used by CohortGenerator package
#' @param databaseId                  A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName                The full name of the database. If NULL, defaults to value in cdm_source table
#' @param databaseDescription         A short description (several sentences) of the database. If NULL, defaults to value in cdm_source table
#' @template cdmVersion
#' @param runInclusionStatistics      Generate and export statistic on the cohort inclusion rules?
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runTimeSeries               Generate and export the time series diagnostics?
#' @param runVisitContext             Generate and export index-date visit context?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#' @param runIncidenceRate            Generate and export the cohort incidence  rates?
#' @param runCohortRelationship       Compute cohort relationships. Overlap is now computed with FeaturExtraction, time paramters are derived from temporalCovariateSettings
#'                                    relationship between two or more cohorts.
#' @param runTemporalCohortCharacterization   Generate and export the temporal cohort characterization?
#'                                            Only records with values greater than 0.001 are returned.
#' @param temporalCovariateSettings   Either an object of type \code{covariateSettings} as created using one of
#'                                    the createTemporalCovariateSettings function in the FeatureExtraction package, or a list
#'                                    of such objects. This can be anythin accepted by FeatureExtraction (including
#'                                    custom covariates). However, it should be noted that certain time windows will be
#'                                    included by default. @seealso[getDefaultCovariateSettings]
#' @param minCellCount                The minimum cell count for fields contains person counts or fractions.
#' @param minCharacterizationMean     The minimum mean value for characterization output. Values below this will be cut off from output. This
#'                                    will help reduce the file size of the characterization output, but will remove information
#'                                    on covariates that have very low values. The default is 0.001 (i.e. 0.1 percent)
#' @param irWashoutPeriod             Number of days washout to include in calculation of incidence rates - default is 0
#' @param incremental                 Create only cohort diagnostics that haven't been created before?
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are kept
#'                                    of which cohort diagnostics has been executed.
#' @param runFeatureExtractionOnSample Logical. If TRUE, the function will operate on a sample of the data.
#'                                    Default is FALSE, meaning the function will operate on the full data set.
#'
#' @param sampleN                     Integer. The number of records to include in the sample if runFeatureExtractionOnSample is TRUE.
#'                                    Default is 1000. Ignored if runFeatureExtractionOnSample is FALSE.
#'
#' @param seed                        Integer. The seed for the random number generator used to create the sample.
#'                                    This ensures that the same sample can be drawn again in future runs. Default is 64374.
#'
#' @param seedArgs                    List. Additional arguments to pass to the sampling function.
#'                                    This can be used to control aspects of the sampling process beyond the seed and sample size.
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
                               databaseName = NULL,
                               databaseDescription = NULL,
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
                               temporalCovariateSettings = getDefaultCovariateSettings(),
                               minCellCount = 5,
                               minCharacterizationMean = 0.01,
                               irWashoutPeriod = 0,
                               incremental = FALSE,
                               incrementalFolder = file.path(exportFolder, "incremental"),
                               runFeatureExtractionOnSample = FALSE,
                               sampleN = 1000,
                               seed = 64374,
                               seedArgs = NULL) {
  # collect arguments that were passed to cohort diagnostics at initiation
  callingArgsJson <-
    list(
      runInclusionStatistics = runInclusionStatistics,
      runIncludedSourceConcepts = runIncludedSourceConcepts,
      runOrphanConcepts = runOrphanConcepts,
      runTimeSeries = runTimeSeries,
      runVisitContext = runVisitContext,
      runBreakdownIndexEvents = runBreakdownIndexEvents,
      runIncidenceRate = runIncidenceRate,
      runTemporalCohortCharacterization = runTemporalCohortCharacterization,
      minCellCount = minCellCount,
      minCharacterizationMean = minCharacterizationMean,
      incremental = incremental,
      temporalCovariateSettings = temporalCovariateSettings
    ) %>%
    ParallelLogger::convertSettingsToJson()

  # Create context
  context <- createDiagnosticsContext(
    connectionDetails = connectionDetails,
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortTableNames = cohortTableNames,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    databaseId = databaseId,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    exportFolder = exportFolder,
    minCellCount = minCellCount,
    incremental = incremental,
    incrementalFolder = incrementalFolder,
    cdmVersion = cdmVersion
  )

  # Store JSON args
  context$callingArgsJson <- callingArgsJson

  # Initialize
  context <- initializeDiagnostics(context, cohortDefinitionSet, cohortIds)

  # Run Diagnostics
  if (runInclusionStatistics) {
    runInclusionStatisticsDiagnostic(context)
  }

  if (runIncludedSourceConcepts || runOrphanConcepts || runBreakdownIndexEvents) {
    runConceptSetDiagnostic(context,
      runIncludedSourceConcepts = runIncludedSourceConcepts,
      runOrphanConcepts = runOrphanConcepts,
      runBreakdownIndexEvents = runBreakdownIndexEvents
    )
  }

  if (runTimeSeries) {
    runTimeSeriesDiagnostic(context)
  }

  if (runVisitContext) {
    runVisitContextDiagnostic(context)
  }

  if (runIncidenceRate) {
    runIncidenceRateDiagnostic(context, irWashoutPeriod = irWashoutPeriod)
  }

  if (runTemporalCohortCharacterization) {
    runTemporalCharacterizationDiagnostic(context,
      temporalCovariateSettings = temporalCovariateSettings,
      minCharacterizationMean = minCharacterizationMean,
      runFeatureExtractionOnSample = runFeatureExtractionOnSample,
      sampleN = sampleN,
      seed = seed,
      seedArgs = seedArgs,
      runCohortRelationship = runCohortRelationship
    )
  }

  # Finalize
  finalizeDiagnostics(context)
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
