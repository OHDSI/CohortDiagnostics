#' Cohort Diagnostics Settings
#' @description R6Class Generator for working within the CohortDiagnostics package.
#' Exposes results model and icremental exectution API.
#' Should be used to execute all diagnostics functions within cohort diagnostics
#' @noRd
CohortDiagnosticsSettings <- R6::R6Class(
  classname = "CohortDiagnosticsSettings",
  public = list(
    errorMessage = NULL,
    cohortTable = NULL,
    # Initalize
    initialize = function(settings, connection = NULL, connectionDetails = NULL) {
      #ParallelLogger::addDefaultFileLogger(file.path(exportFolder, "log.txt"), name = "CD_LOGGER")
      #ParallelLogger::addDefaultErrorReportLogger(file.path(exportFolder, "errorReportR.txt"), name = "CD_ERROR_LOGGER")
      if (is.null(connection) && is.null(connectionDetails))
        stop("Connection or ConnectionDetails for a CDM must be provided")

      self$errorMessage <- checkmate::makeAssertCollection()

      for (name in names(settings)) {
        self[[name]] <- settings[[name]]
      }

      checkmate::reportAssertions(collection = self$errorMessage)
      private$.executionTimePath <- file.path(self$exportFolder, "taskExecutionTimes.csv")
      private$setConnection(connection, connectionDetails)
    },

    # to list
    toList = function() {
      idList <- list()
      for (name in names(CohortDiagnosticsSettings)) {
        idList[[name]] <- self[[name]]
      }
      return(idList)
    },

    # to json
    toJson = function() {
      self$toList() |> ParallelLogger::convertSettingsToJson()
    },

    # get database connection.
    # return an active connection object
    getConnection = function() {
      if (!is.null(private$.connection) && DatabaseConnector::dbIsValid(private$.connection)) {
        return(private$.connection)
      }
      stop("Connection is no longer valid")
    },

    # Check a set of default temporal covariate settings
    # This is required for default behaviour in shiny apps and if all reporting is required
    checkDefaultTemporalCovariateSettings = function(temporalCovariateSettings = self$temporalCovariateSettings, errorMessage = self$errorMessage) {
      if (is(temporalCovariateSettings, "covariateSettings")) {
        temporalCovariateSettings <- list(temporalCovariateSettings)
      }
      # All temporal covariate settings objects must be covariateSettings
      checkmate::assert_true(all(lapply(temporalCovariateSettings, class) == "covariateSettings"), add = errorMessage)


      requiredCharacterisationSettings <- c(
        "DemographicsGender", "DemographicsAgeGroup", "DemographicsRace",
        "DemographicsEthnicity", "DemographicsIndexYear", "DemographicsIndexMonth",
        "ConditionEraGroupOverlap", "DrugEraGroupOverlap", "CharlsonIndex",
        "Chads2", "Chads2Vasc"
      )
      presentSettings <- temporalCovariateSettings[[1]][requiredCharacterisationSettings]
      if (!all(unlist(presentSettings))) {
        warning(
          "For cohort charcterization to display standardized results the following covariates must be present in your temporalCovariateSettings: \n\n",
          paste(requiredCharacterisationSettings, collapse = ", ")
        )
      }

      requiredTimeDistributionSettings <- c(
        "DemographicsPriorObservationTime",
        "DemographicsPostObservationTime",
        "DemographicsTimeInCohort"
      )

      presentSettings <- temporalCovariateSettings[[1]][requiredTimeDistributionSettings]
      if (!all(unlist(presentSettings))) {
        warning(
          "For time distributions diagnostics to display standardized results the following covariates must be present in your temporalCovariateSettings: \n\n",
          paste(requiredTimeDistributionSettings, collapse = ", ")
        )
      }

      # forcefully set ConditionEraGroupStart and drugEraGroupStart to NULL
      # because of known bug in FeatureExtraction. https://github.com/OHDSI/FeatureExtraction/issues/144
      temporalCovariateSettings[[1]]$ConditionEraGroupStart <- NULL
      temporalCovariateSettings[[1]]$DrugEraGroupStart <- NULL

      checkmate::assert_integerish(
        x = temporalCovariateSettings[[1]]$temporalStartDays,
        any.missing = FALSE,
        min.len = 1,
        add = errorMessage
      )
      checkmate::assert_integerish(
        x = temporalCovariateSettings[[1]]$temporalEndDays,
        any.missing = FALSE,
        min.len = 1,
        add = errorMessage
      )
      checkmate::reportAssertions(collection = errorMessage)

      # Adding required temporal windows required in results viewer
      requiredTemporalPairs <-
        list(
          c(-365, 0),
          c(-30, 0),
          c(-365, -31),
          c(-30, -1),
          c(0, 0),
          c(1, 30),
          c(31, 365),
          c(-9999, 9999)
        )
      for (p1 in requiredTemporalPairs) {
        found <- FALSE
        for (i in seq_len(length(temporalCovariateSettings[[1]]$temporalStartDays))) {
          p2 <- c(
            temporalCovariateSettings[[1]]$temporalStartDays[i],
            temporalCovariateSettings[[1]]$temporalEndDays[i]
          )

          if (p2[1] == p1[1] & p2[2] == p1[2]) {
            found <- TRUE
            break
          }
        }

        if (!found) {
          temporalCovariateSettings[[1]]$temporalStartDays <-
            c(temporalCovariateSettings[[1]]$temporalStartDays, p1[1])
          temporalCovariateSettings[[1]]$temporalEndDays <-
            c(temporalCovariateSettings[[1]]$temporalEndDays, p1[2])
        }
      }

      return(temporalCovariateSettings)
    }

  ),
  active = list(
    # @field cohortDefinitionSet
    cohortDefinitionSet = function(cohortDefinitionSet) {
      if (missing(cohortDefinitionSet)) return(private$.cohortDefinitionSet)
      # Assert data frame
      checkmate::assertDataFrame(cohortDefinitionSet, add = self$errorMessage)
      checkmate::assertNames(names(cohortDefinitionSet),
                             must.include = c(
                               "json",
                               "cohortId",
                               "cohortName",
                               "sql"
                             ),
                             add = self$errorMessage)

      if (!"isSubset" %in% colnames(cohortDefinitionSet)) {
        cohortDefinitionSet$isSubset <- FALSE
      }
      private$.cohortDefinitionSet <- cohortDefinitionSet
    },

    # @field exportFolder folder to export results to
    exportFolder = function(exportFolder) {
      if (missing(exportFolder)) return(private$.exportFolder)
      # Assert string
      checkmate::assertCharacter(exportFolder, add = self$errorMessage)
      dir.create(exportFolder, showWarnings = FALSE, recursive = TRUE)
      private$.exportFolder <- exportFolder
    },

    # @field databaseId database identifier to use in export of results and storage of incremental work
    databaseId = function(databaseId) {
      if (missing(databaseId)) return(private$.databaseId)
      # Assert string
      databaseId <- as.character(databaseId)
      checkmate::assertCharacter(databaseId, min.len = 1, add = self$errorMessage)
      private$.databaseId <- databaseId
    },

    # @field databaseId database identifier to use in export of results and storage of incremental work
    databaseName = function(databaseName) {
      if (missing(databaseName)) return(private$.databaseName)
      databaseName <- as.character(databaseName)
      if (length(databaseName) == 0) {
        ParallelLogger::logTrace(" - Database description was not provided. Using CDM source table")
        databaseName <- NULL
      }
      checkmate::assertCharacter(databaseName, null.ok = TRUE, add = self$errorMessage)
      private$.databaseName <- databaseName
    },

    # @field databaseId database identifier to use in export of results and storage of incremental work
    databaseDescription = function(databaseDescription) {
      if (missing(databaseDescription)) return(private$.databaseDescription)
      databaseDescription <- as.character(databaseDescription)
      if (length(databaseDescription) == 0) {
        ParallelLogger::logTrace(" - Database description was not provided. Using CDM source table")
        databaseDescription <- NULL
      }
      checkmate::assertCharacter(databaseDescription, null.ok = TRUE, add = self$errorMessage)
      private$.databaseDescription <- databaseDescription
    },

    # @field connectionDetails DatabaseConnector connectionDetails instance for a a CDM
    connectionDetails = function(connectionDetails) {
      if (missing(connectionDetails)) {
        return(private$.connectionDetails)
      }

      checkmate::assertClass(connectionDetails, classes = "ConnectionDetails", null.ok = TRUE)
      private$.connectionDetails <- connectionDetails
    },

    # @field cdmDatabaseSchema database schema that cdm is on
    cdmDatabaseSchema = function(cdmDatabaseSchema) {
      if (missing(cdmDatabaseSchema)) return(private$.cdmDatabaseSchema)
      checkmate::assertCharacter(cdmDatabaseSchema)
      private$.cdmDatabaseSchema <- cdmDatabaseSchema
    },

    # @field cdmDatabaseSchema database schema that cdm is on
    cohortDatabaseSchema = function(cohortDatabaseSchema) {
      if (missing(cohortDatabaseSchema)) return(private$.cohortDatabaseSchema)
      checkmate::assertCharacter(cohortDatabaseSchema)
      private$.cohortDatabaseSchema <- cohortDatabaseSchema
    },

    # @field tempEmulationSchema temp schema on platforms that do not use temp schemas
    tempEmulationSchema = function(tempEmulationSchema) {
      if (missing(tempEmulationSchema)) return(private$.tempEmulationSchema)
      checkmate::assertCharacter(tempEmulationSchema, null.ok = TRUE)
      private$.tempEmulationSchema <- tempEmulationSchema
    },

    # @field cohortTableNames list of table names created with CohortGenerator::getTableNames
    cohortTableNames = function(cohortTableNames) {
      if (missing(cohortTableNames)) return(private$.cohortTableNames)
      checkmate::assertList(cohortTableNames, null.ok = FALSE, types = "character", add = self$errorMessage, names = "named")
      checkmate::assertNames(names(cohortTableNames),
                             must.include = c(
                               "cohortTable",
                               "cohortInclusionTable",
                               "cohortInclusionResultTable",
                               "cohortInclusionStatsTable",
                               "cohortSummaryStatsTable",
                               "cohortCensorStatsTable"
                             ),
                             add = self$errorMessage
      )
      private$.cohortTableNames <- cohortTableNames
      self$cohortTable <- cohortTableNames$cohortTable
    },

    # @field  vocabularyDatabaseSchema generally the same as the cdm schema but where the vocabulary is set
    vocabularyDatabaseSchema = function(vocabularyDatabaseSchema) {
      if (missing(vocabularyDatabaseSchema)) return(private$.vocabularyDatabaseSchema)
      checkmate::assertCharacter(vocabularyDatabaseSchema)
      private$.vocabularyDatabaseSchema <- vocabularyDatabaseSchema
    },
    # @field  cohortIds, if specified execution will only happen on this subset of cohort ids
    cohortIds = function(cohortIds) {
      if (missing(cohortIds)) return(private$.cohortIds)
      checkmate::assertNumeric(cohortIds, add = self$errorMessage, null.ok = TRUE)
      private$.cohortIds <- cohortIds
    },
    # @field  cdmVersion      The version of the OMOP CDM. Default 5. (Note: only 5.x lineage is supported.)
    cdmVersion = function(cdmVersion) {
      if (missing(cdmVersion)) return(private$.cdmVersion)
      checkmate::assertNumeric(
        x = cdmVersion,
        lower = 5,
        upper = 5.4,
        null.ok = FALSE,
        add = self$errorMessage
      )
      private$.cdmVersion <- cdmVersion
    },
    # @field temporalCovariateSettings   Either an object of type \code{covariateSettings} as created using one of
    #                                    the createTemporalCovariateSettings function in the FeatureExtraction package, or a list
    #                                    of such objects. This can be anythin accepted by FeatureExtraction (including
    #                                    custom covariates). However, it should be noted that certain time windows will be
    #                                    included by default. @seealso[getDefaultCovariateSettings]
    temporalCovariateSettings = function(temporalCovariateSettings) {
      if (missing(temporalCovariateSettings)) return(private$.temporalCovariateSettings)
      checkmate::assertList(temporalCovariateSettings, add = self$errorMessage)
      private$.temporalCovariateSettings <- temporalCovariateSettings
    },
    # @field minCellCount                The minimum cell count for fields contains person counts or fractions.
    minCellCount = function(minCellCount) {
      if (missing(minCellCount)) return(private$.minCellCount)
      minCellCount <- utils::type.convert(minCellCount, as.is = TRUE)
      checkmate::assertNumeric(minCellCount, , len = 1, add = self$errorMessage)
      private$.minCellCount <- minCellCount
    },
    # @field minCharacterizationMean     The minimum mean value for characterization output. Values below this will be cut off from output. This
    #                                    will help reduce the file size of the characterization output, but will remove information
    #                                    on covariates that have very low values. The default is 0.001 (i.e. 0.1 percent)
    minCharacterizationMean = function(minCharacterizationMean) {
      if (missing(minCharacterizationMean)) return(private$.minCharacterizationMean)
      val <- utils::type.convert(minCharacterizationMean, as.is = TRUE)
      checkmate::assertNumeric(x = val, len = 1, lower = 0, upper = 1, add = self$errorMessage)
      private$.minCharacterizationMean <- minCharacterizationMean
    },

    # @field irWashoutPeriod             Number of days washout to include in calculation of incidence rates - default is 0
    irWashoutPeriod = function(irWashoutPeriod) {
      if (missing(irWashoutPeriod)) return(private$.irWashoutPeriod)
      checkmate::assertNumeric(irWashoutPeriod, add = self$errorMessage)
      private$.irWashoutPeriod <- irWashoutPeriod
    },
    # @field incremental                 Create only cohort diagnostics that haven't been created before?
    incremental = function(incremental) {
      if (missing(incremental)) return(private$.incremental)
      checkmate::assertLogical(incremental, add = self$errorMessage)
      private$.incremental <- incremental
    },

    # @field incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are kept
    #                                    of which cohort diagnostics has been executed.
    incrementalFolder = function(incrementalFolder) {
      if (missing(incrementalFolder)) return(private$.incrementalFolder)
      checkmate::assertCharacter(incrementalFolder, add = self$errorMessage)
      dir.create(incrementalFolder, showWarnings = FALSE, recursive = TRUE)
      private$.incrementalFolder <- incrementalFolder
    },
    # @field runFeatureExtractionOnSample Logical. If TRUE, the function will operate on a sample of the data.
    #                                    Default is FALSE, meaning the function will operate on the full data set.
    runFeatureExtractionOnSample = function(runFeatureExtractionOnSample) {
      if (missing(runFeatureExtractionOnSample)) return(private$.runFeatureExtractionOnSample)
      checkmate::assertLogical(runFeatureExtractionOnSample, add = self$errorMessage)
      private$.runFeatureExtractionOnSample <- runFeatureExtractionOnSample
    },

    # @field sampleN                     Integer. The number of records to include in the sample if runFeatureExtractionOnSample is TRUE.
    #                                    Default is 1000. Ignored if runFeatureExtractionOnSample is FALSE.
    sampleN = function(sampleN) {
      if (missing(sampleN)) return(private$.sampleN)
      checkmate::assertNumeric(sampleN, add = self$errorMessage)
      private$.sampleN <- sampleN
    },
    # @field seed                        Integer. The seed for the random number generator used to create the sample.
    #                                    This ensures that the same sample can be drawn again in future runs. Default is 64374.
    seed = function(seed) {
      if (missing(seed)) return(private$.seed)
      checkmate::assertNumeric(seed, add = self$errorMessage)
      private$.seed <- seed
    },
    # @field seedArgs                    List. Additional arguments to pass to the sampling function.
    #                                    This can be used to control aspects of the sampling process beyond the seed and sample size.
    seedArgs = function(seedArgs) {
      if (missing(seedArgs)) return(private$.seedArgs)
      checkmate::assertList(seedArgs, null.ok = TRUE, add = self$errorMessage)
      private$.seedArgs <- seedArgs
    }
  ),
  private = list(
    .cohortDefinitionSet = NULL,
    .exportFolder = NULL,
    .databaseId = NULL,
    .databaseName = NULL,
    .databaseDescription = NULL,
    .connectionDetails = NULL,
    .connection = NULL,
    .cdmDatabaseSchema = NULL,
    .tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    .cohortDatabaseSchema = NULL,
    .cohortTable = "cohort",
    .cohortTableNames = NULL,
    .vocabularyDatabaseSchema = NULL,
    .cohortIds = NULL,
    .cdmVersion = 5,
    .temporalCovariateSettings = NULL,
    .minCellCount = 5,
    .minCharacterizationMean = 0.01,
    .irWashoutPeriod = 0,
    .incremental = FALSE,
    .incrementalFolder = NULL,
    .runFeatureExtractionOnSample = FALSE,
    .sampleN = 1000,
    .seed = 64374,
    .seedArgs = NULL,
    .executionTimePath = NULL,
    setConnection = function(connection, connectionDetails) {
      if (is.null(connection))
        private$.connection <- DatabaseConnector::connect(connectionDetails)
      else if (DatabaseConnector::dbIsValid(connection))
        private$.connection <- connection
      else
        stop("Connection is not valid")
    }
  )
)

#' Create CohortDiagnostics Settings
#' @description
#'
#' Create a global object that contains settings for execution with cohort diagnostics
#' This returns an R6 Class instance that can be used throughout all cohort diagnostics
#'
#' @export
#'
#' @template CdmDatabaseSchema
#' @template VocabularyDatabaseSchema
#' @template CohortDatabaseSchema
#' @template TempEmulationSchema
#'
#' @template CohortTable
#'
#' @template cdmVersion
#' @param exportFolder                The folder where the output will be exported to. If this folder
#'                                    does not exist it will be created.
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    diagnostics to.
#' @param cohortDefinitionSet         Data.frame of cohorts must include columns cohortId, cohortName, json, sql
#' @param cohortTableNames            Cohort Table names used by CohortGenerator package
#' @param databaseId                  A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName                The full name of the database. If NULL, defaults to value in cdm_source table
#' @param databaseDescription         A short description (several sentences) of the database. If NULL, defaults to value in cdm_source table
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
createCohortDiagnosticsSettings <- function(cohortDefinitionSet,
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
  # Validate and encapsulate parameters in a list
  settings <- list(
    cohortDefinitionSet = cohortDefinitionSet,
    exportFolder = exportFolder,
    databaseId = databaseId,
    cohortDatabaseSchema = cohortDatabaseSchema,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortTableNames = cohortTableNames,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cohortIds = cohortIds,
    cdmVersion = cdmVersion,
    temporalCovariateSettings = temporalCovariateSettings,
    minCellCount = minCellCount,
    minCharacterizationMean = minCharacterizationMean,
    irWashoutPeriod = irWashoutPeriod,
    incremental = incremental,
    incrementalFolder = incrementalFolder,
    runFeatureExtractionOnSample = runFeatureExtractionOnSample,
    sampleN = sampleN,
    seed = seed,
    seedArgs = seedArgs
  )

  # Create an instance of the R6 class
  return(CohortDiagnosticsSettings$new(settings, connection = connection, connectionDetails = connectionDetails))
}