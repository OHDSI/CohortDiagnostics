# Copyright 2024 Observational Health Data Sciences and Informatics
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
#' Runs the cohort diagnostics on all (or a subset of) the cohorts.
#' Assumes the cohorts have already been instantiated with the CohortGenerator package.
#'
#' Characterization:
#' If runTemporalCohortCharacterization argument is TRUE, then the following default covariateSettings object will be created
#' using \code{RFeatureExtraction::createTemporalCovariateSettings}
#' Alternatively, a covariate setting object may be created using the above as an example.
#'
#' @template Connection
#' @template CdmDatabaseSchema
#' @template VocabularyDatabaseSchema
#' @template CohortDatabaseSchema
#' @template TempEmulationSchema
#' @template CohortTable
#' @template CohortSetReference
#' @template exportFolder              
#' @template cohortIds
#' @template cohortDefinitionSet
#' @template MinCellCount
#' @template Incremental
#' @template cdmVersion
#' @template databaseId
#' @template minCharacterizationMean     
#' 
#' @param cohortTableNames            Cohort Table names used by CohortGenerator package
#' @param conceptCountsTable          Concepts count table name. The default is "#concept_counts" to create a temporal concept counts table.
#'                                    If an external concept counts table is used, provide the name in character, e.g. "concept_counts" without a hash
#' @param databaseName                The full name of the database. If NULL, defaults to value in cdm_source table
#' @param databaseDescription         A short description (several sentences) of the database. If NULL, defaults to value in cdm_source table
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
#'                                            Only records with values greater than 0.001 are returned.
#' @param temporalCovariateSettings   Either an object of type \code{covariateSettings} as created using one of
#'                                    the createTemporalCovariateSettings function in the FeatureExtraction package, or a list
#'                                    of such objects.
#' @param irWashoutPeriod             Number of days washout to include in calculation of incidence rates - default is 0
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are kept
#'                                    of which cohort diagnostics has been executed.
#' @param useExternalConceptCountsTable If TRUE an external table for the cohort concept counts will be used.
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
#' @param sampleIdentifierExpression Character. An expression that generates unique identifiers for each sample.
#'                                   This expression can use the variables 'cohortId' and 'seed'.
#'                                   Default is "cohortId * 1000 + seed", which ensures unique identifiers
#'                                   as long as there are fewer than 1000 cohorts.
#'                                   
#' @param useAchilles                Logical. Should the pre-computed Achilles analyses be used to get concept counts? TRUE or FALSE (default)
#'
#' @param achillesDatabaseSchema     Character. The name of the schema where the Achilles results tables are located. 
#'                                   Require if `useAchilles` is TRUE and ignored otherwise.
#'                                   
#' @param workDatabaseSchema         Character. The name of a schema where the user has write access. Intermediate tables for concept counts 
#'                                   and orphan concepts will be created in this schema if supplied. If NULL (default) intermediate tables will
#'                                   be created as temporary tables.                        
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
                               conceptCountsTable = "concept_counts",
                               runFeatureExtractionOnSample = FALSE,
                               sampleN = 1000,
                               seed = 64374,
                               seedArgs = NULL,
                               sampleIdentifierExpression = "cohortId * 1000 + seed",
                               useAchilles = FALSE, 
                               achillesDatabaseSchema = NULL,
                               workDatabaseSchema = NULL) {
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
    RJSONIO::toJSON(digits = 23, pretty = TRUE)

  exportFolder <- normalizePath(exportFolder, mustWork = FALSE)
  incrementalFolder <- normalizePath(incrementalFolder, mustWork = FALSE)
  executionTimePath <- file.path(exportFolder, "taskExecutionTimes.csv")

  ParallelLogger::addDefaultFileLogger(file.path(exportFolder, "log.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(exportFolder, "errorReportR.txt"))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(
    ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE),
    add = TRUE
  )

  start <- Sys.time()
  ParallelLogger::logInfo("Run Cohort Diagnostics started at ", start)

  databaseId <- as.character(databaseId)

  if (any(is.null(databaseName), is.na(databaseName))) {
    ParallelLogger::logTrace(" - Databasename was not provided. Using CDM source table")
  }
  if (any(is.null(databaseDescription), is.na(databaseDescription))) {
    ParallelLogger::logTrace(" - Databasedescription was not provided. Using CDM source table")
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
  cohortDefinitionSet <- dplyr::tibble(cohortDefinitionSet) # for better printing
  
  checkmate::assertIntegerish(cohortIds, lower = 0, any.missing = FALSE, null.ok = TRUE, add = errorMessage)
  checkmate::assertSubset(cohortIds, cohortDefinitionSet$cohortId, add = errorMessage)

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
  checkmate::assertInteger(x = minCellCount, len = 1, lower = 0, add = errorMessage)
  minCharacterizationMean <- utils::type.convert(minCharacterizationMean, as.is = TRUE)
  checkmate::assertNumeric(x = minCharacterizationMean, lower = 0, add = errorMessage)
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
  
  checkmate::assertLogical(useAchilles, len = 1, any.missing = FALSE, add = errorMessage)
  
  if (isTRUE(useAchilles)) {
    checkmate::assertCharacter(achillesDatabaseSchema, len = 1, any.missing = FALSE, add = errorMessage)
  }
  
  # Create output and incremental folders. check that we have write access.
  if (!file.exists(gsub("/$", "", exportFolder))) {
    dir.create(name, recursive = TRUE)
    ParallelLogger::logInfo("Created export folder", exportFolder)
  }
  checkmate::assertDirectory(exportFolder, access = "w", add = errorMessage)

  if (incremental) {
    if (!file.exists(gsub("/$", "", exportFolder))) {
      dir.create(name, recursive = TRUE)
      ParallelLogger::logInfo("Created incremental folder", incrementalFolder)
    }
    checkmate::assertDirectory(incrementalFolder, access = "w", add = errorMessage)
  }

  if (is(temporalCovariateSettings, "covariateSettings")) {
    temporalCovariateSettings <- list(temporalCovariateSettings)
  }
  
  checkmate::reportAssertions(collection = errorMessage)
  
  # All temporal covariate settings objects must be covariateSettings
  checkmate::assert_true(all(lapply(temporalCovariateSettings, class) == c("covariateSettings")), add = errorMessage)

  if (runTemporalCohortCharacterization) {
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
      for (i in seq_along(temporalCovariateSettings[[1]]$temporalStartDays)) {
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
  }

  checkmate::reportAssertions(collection = errorMessage)
  if (!is.null(cohortIds)) {
    cohortDefinitionSet <- cohortDefinitionSet %>% dplyr::filter(.data$cohortId %in% cohortIds)
  }

  if (nrow(cohortDefinitionSet) == 0) {
    stop("No cohorts specified")
  }
  
  cohortTableColumnNamesObserved <- sort(colnames(cohortDefinitionSet))
  
  cohortTableColumnNamesExpected <-
    getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == "cohort") %>%
    dplyr::pull(.data$columnName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    sort()
  
  cohortTableColumnNamesRequired <-
    getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == "cohort") %>%
    dplyr::filter(.data$isRequired == "Yes") %>%
    dplyr::pull(.data$columnName) %>%
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

  subsets <- CohortGenerator::getSubsetDefinitions(cohortDefinitionSet)
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
      fileName = file.path(exportFolder, "subset_definition.csv")
    )
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

  ## CDM source information----
  timeExecution(
    exportFolder,
    taskName = "getCdmDataSourceInformation",
    cohortIds = NULL,
    parent = "executeDiagnostics",
    expr = {
      cdmSourceInformation <-
        getCdmDataSourceInformation(
          connection = connection,
          cdmDatabaseSchema = cdmDatabaseSchema
        )
      ## Use CDM source table as default name/description
      if (!is.null(cdmSourceInformation)) {
        if (any(is.null(databaseName), is.na(databaseName))) {
          databaseName <- cdmSourceInformation$cdmSourceName
        }

        if (any(is.null(databaseDescription), is.na(databaseDescription))) {
          databaseDescription <- cdmSourceInformation$sourceDescription
        }
      } else {
        if (any(is.null(databaseName), is.na(databaseName))) {
          databaseName <- databaseId
        }

        if (any(is.null(databaseDescription), is.na(databaseDescription))) {
          databaseDescription <- databaseName
        }
      }
      vocabularyVersion <- getVocabularyVersion(connection, vocabularyDatabaseSchema)
    }
  )

  cohortDefinitionSet$checksum <- computeChecksum(cohortDefinitionSet$sql)

  if (incremental) {
    ParallelLogger::logDebug("Working in incremental mode.")
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
  timeExecution(
    exportFolder,
    taskName = "observationPeriodDateRange",
    cohortIds = NULL,
    parent = "executeDiagnostics",
    expr = {
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
    }
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
  ParallelLogger::logTrace("Creating concept ID table for tracking concepts used in diagnostics")
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "DROP TABLE IF EXISTS #concept_ids; CREATE TABLE #concept_ids (concept_id BIGINT);",
    progressBar = FALSE,
    reportOverallTime = FALSE,
    tempEmulationSchema = tempEmulationSchema
  )

  # Counting cohorts -----------------------------------------------------------------------
  timeExecution(
    exportFolder,
    taskName = "getCohortCounts",
    cohortIds = cohortIds,
    parent = "executeDiagnostics",
    expr = {
      ParallelLogger::logInfo("Counting cohort records and subjects")
      cohortCounts <- CohortGenerator::getCohortCounts(
        connection = connection,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cohortIds = cohortIds,
        cohortDefinitionSet = cohortDefinitionSet,
        databaseId = databaseId
      )
      
      if (sum(cohortCounts$cohortEntries) == 0) {
        stop("Cohort table is empty")
      }
      
      cohortCounts <- exportDataToCsv(
        data = cohortCounts,
        tableName = "cohort_count",
        fileName = file.path(exportFolder, "cohort_count.csv"),
        minCellCount = minCellCount,
        databaseId = databaseId,
        incremental = FALSE,
        cohortId = cohorts$cohortId
      )
    }
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

  # subset the cohortDefinitionSet to only cohorts with entries for the CDM
  # The remainder of the analyses will only run on cohorts with counts
  cohortDefinitionSet <- cohortDefinitionSet %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts)

  # Inclusion statistics -----------------------------------------------------------------------
  if (runInclusionStatistics) {
    timeExecution(
      exportFolder,
      "getInclusionStats",
      cohortIds,
      parent = "executeDiagnostics",
      expr = {
        runInclusionStatistics(
          connection = connection,
          exportFolder = exportFolder,
          databaseId = databaseId,
          cohortDefinitionSet = cohortDefinitionSet,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTableNames = cohortTableNames,
          incremental = incremental,
          minCellCount = minCellCount,
          recordKeepingFile = recordKeepingFile
        )
      }
    )
  }
  
  # Defines variables and checks version of external concept counts table -----
  checkConceptCountsTableExists <- DatabaseConnector::dbExistsTable(connection,
                                                                    name = conceptCountsTable,
                                                                    databaseSchema = cdmDatabaseSchema)
  
  
  if (substr(conceptCountsTable, 1, 1) == "#") {
    conceptCountsTableIsTemp <- TRUE
  } else {
      conceptCountsTableIsTemp <- FALSE
      conceptCountsTable <- conceptCountsTable
      dataSourceInfo <- getCdmDataSourceInformation(connection = connection, 
                                                    cdmDatabaseSchema = cdmDatabaseSchema)
      vocabVersion <- dataSourceInfo$vocabularyVersion
      vocabVersionExternalConceptCountsTable <- renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT DISTINCT vocabulary_version FROM @work_database_schema.@concept_counts_table;",
        work_database_schema = cohortDatabaseSchema,
        concept_counts_table = conceptCountsTable,
        snakeCaseToCamelCase = TRUE,
        tempEmulationSchema = getOption("sqlRenderTempEmulationSchena")
      )
      if (!identical(vocabVersion, vocabVersionExternalConceptCountsTable[1,1])) {
        stop(paste0("External concept counts table (", 
                    vocabVersionExternalConceptCountsTable, 
                    ") does not match database (", 
                    vocabVersion, 
                    "). Update concept_counts with createConceptCountsTable()"))
      }
  }

  # Always export concept sets to csv
  exportConceptSets(
    cohortDefinitionSet = cohortDefinitionSet,
    exportFolder = exportFolder,
    minCellCount = minCellCount,
    databaseId = databaseId
  )

  # Concept set diagnostics -----------------------------------------------
  if (runIncludedSourceConcepts || runOrphanConcepts || runBreakdownIndexEvents) {
    timeExecution(
      exportFolder,
      taskName = "runIncludedSourceConcepts",
      cohortIds,
      parent = "executeDiagnostics",
      expr = {
        runIncludedSourceConcepts(
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
          conceptCountsTable = conceptCountsTable,
          conceptCountsTableIsTemp = conceptCountsTableIsTemp,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          useExternalConceptCountsTable = useExternalConceptCountsTable,
          incremental = incremental,
          conceptIdTable = "#concept_ids",
          recordKeepingFile = recordKeepingFile,
          useAchilles = useAchilles,
          resultsDatabaseSchema = resultsDatabaseSchema
        )
      }
    )
  }

  # Time series ----------------------------------------------------------------------
  if (runTimeSeries) {
    timeExecution(
      exportFolder,
      "runTimeSeries",
      cohortIds,
      parent = "executeDiagnostics",
      expr = {
        runTimeSeries(
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
    )
  }


  # Visit context ----------------------------------------------------------------------------
  if (runVisitContext) {
    timeExecution(
      exportFolder,
      "runVisitContext",
      cohortIds,
      parent = "executeDiagnostics",
      expr = {
        runVisitContext(
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
    )
  }

  # Incidence rates --------------------------------------------------------------------------------------
  if (runIncidenceRate) {
    timeExecution(
      exportFolder,
      "runIncidenceRate",
      cohortIds,
      parent = "executeDiagnostics",
      expr = {
        runIncidenceRate(
          connection = connection,
          tempEmulationSchema = tempEmulationSchema,
          cdmDatabaseSchema = cdmDatabaseSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          databaseId = databaseId,
          exportFolder = exportFolder,
          minCellCount = minCellCount,
          cohorts = cohortDefinitionSet,
          washoutPeriod = irWashoutPeriod,
          instantiatedCohorts = instantiatedCohorts,
          recordKeepingFile = recordKeepingFile,
          incremental = incremental
        )
      }
    )
  }

  # Cohort relationship ---------------------------------------------------------------------------------
  if (runCohortRelationship) {
    timeExecution(
      exportFolder,
      "runCohortRelationship",
      cohortIds,
      parent = "executeDiagnostics",
      expr = {
        runCohortRelationship(
          connection = connection,
          databaseId = databaseId,
          exportFolder = exportFolder,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cdmDatabaseSchema = cdmDatabaseSchema,
          tempEmulationSchema = tempEmulationSchema,
          cohortTable = cohortTable,
          cohortDefinitionSet = cohortDefinitionSet,
          temporalCovariateSettings = temporalCovariateSettings[[1]],
          minCellCount = minCellCount,
          recordKeepingFile = recordKeepingFile,
          incremental = incremental
        )
      }
    )
  }

  # Temporal Cohort characterization ---------------------------------------------------------------
  if (runTemporalCohortCharacterization) {
    timeExecution(
      exportFolder,
      "executeCohortCharacterization",
      cohortIds,
      parent = "executeDiagnostics",
      expr = {
        feCohortDefinitionSet <- cohortDefinitionSet
        feCohortTable <- cohortTable
        feCohortCounts <- cohortCounts

        if (runFeatureExtractionOnSample & !isTRUE(attr(cohortDefinitionSet, "isSampledCohortDefinition"))) {
          cohortTableNames$cohortSampleTable <- paste0(cohortTableNames$cohortTable, "_cd_sample")
          CohortGenerator::createCohortTables(
            connection = connection,
            cohortTableNames = cohortTableNames,
            cohortDatabaseSchema = cohortDatabaseSchema,
            incremental = TRUE
          )

          feCohortTable <- cohortTableNames$cohortSampleTable
          feCohortDefinitionSet <-
            CohortGenerator::sampleCohortDefinitionSet(
              connection = connection,
              cohortDefinitionSet = cohortDefinitionSet,
              tempEmulationSchema = tempEmulationSchema,
              cohortDatabaseSchema = cohortDatabaseSchema,
              cohortTableNames = cohortTableNames,
              n = sampleN,
              seed = seed,
              seedArgs = seedArgs,
              identifierExpression = "cohortId",
              incremental = incremental,
              incrementalFolder = incrementalFolder
            )

          feCohortCounts <- computeCohortCounts(
            connection = connection,
            cohortDatabaseSchema = cohortDatabaseSchema,
            cohortTable = cohortTableNames$cohortSampleTable,
            cohorts = feCohortDefinitionSet,
            exportFolder = exportFolder,
            minCellCount = minCellCount,
            databaseId = databaseId,
            writeResult = FALSE
          )
        }

        runCohortCharacterization(
          connection = connection,
          databaseId = databaseId,
          exportFolder = exportFolder,
          cdmDatabaseSchema = cdmDatabaseSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = feCohortTable,
          covariateSettings = temporalCovariateSettings,
          tempEmulationSchema = tempEmulationSchema,
          cdmVersion = cdmVersion,
          cohorts = feCohortDefinitionSet,
          cohortCounts = feCohortCounts,
          minCellCount = minCellCount,
          instantiatedCohorts = instantiatedCohorts,
          incremental = incremental,
          recordKeepingFile = recordKeepingFile,
          covariateValueFileName = file.path(exportFolder, "temporal_covariate_value.csv"),
          covariateValueContFileName = file.path(exportFolder, "temporal_covariate_value_dist.csv"),
          covariateRefFileName = file.path(exportFolder, "temporal_covariate_ref.csv"),
          analysisRefFileName = file.path(exportFolder, "temporal_analysis_ref.csv"),
          timeRefFileName = file.path(exportFolder, "temporal_time_ref.csv"),
          minCharacterizationMean = minCharacterizationMean
        )
      }
    )
  }

  # Store information from the vocabulary on the concepts used -------------------------
  timeExecution(
    exportFolder,
    "exportConceptInformation",
    parent = "executeDiagnostics",
    expr = {
      exportConceptInformation(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        conceptIdTable = "#concept_ids",
        incremental = incremental,
        exportFolder = exportFolder
      )
    }
  )
  # Delete unique concept ID table ---------------------------------
  ParallelLogger::logTrace("Deleting concept ID table")
  timeExecution(
    exportFolder,
    "DeleteConceptIdTable",
    parent = "executeDiagnostics",
    expr = {
      sql <- "TRUNCATE TABLE @table;\nDROP TABLE @table;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        tempEmulationSchema = tempEmulationSchema,
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
    exportFolder = exportFolder,
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
    callingArgsJson,
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
  timeExecution(
    exportFolder,
    "writeResultsZip",
    NULL,
    parent = "executeDiagnostics",
    expr = {
      writeResultsZip(exportFolder, databaseId)
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
