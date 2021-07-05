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
#' @param databaseId                  A short string for identifying the database (e.g. 'Synpuf').
#' @param databaseName                The full name of the database. If NULL, defaults to databaseId.
#' @param databaseDescription         A short description (several sentences) of the database. If NULL, defaults to databaseId.
#' @template cdmVersion
#' @param runInclusionStatistics      Generate and export statistic on the cohort inclusion rules?
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runVisitContext             Generate and export index-date visit context?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#' @param runIncidenceRate            Generate and export the cohort incidence  rates?
#' @param runTimeSeries               Generate and export the cohort prevalence  rates?
#' @param runCohortOverlap            Generate and export the cohort overlap? Overlaps are checked within cohortIds
#'                                    that have the same phenotype ID sourced from the CohortSetReference or
#'                                    cohortToCreateFile.
#' @param runCohortTemporalRelationship       Do you want to compute temporal relationship between the cohorts being diagnosed. This
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
                                 runIncludedSourceConcepts = TRUE,
                                 runOrphanConcepts = TRUE,
                                 runVisitContext = TRUE,
                                 runBreakdownIndexEvents = TRUE,
                                 runIncidenceRate = TRUE,
                                 runTimeSeries = TRUE,
                                 runCohortOverlap = TRUE,
                                 runCohortTemporalRelationship = TRUE,
                                 runCohortCharacterization = TRUE,
                                 covariateSettings = list(
                                   FeatureExtraction::createDefaultCovariateSettings(),
                                   FeatureExtraction::createCovariateSettings(
                                     useVisitCountLongTerm = TRUE,
                                     useVisitCountMediumTerm = TRUE,
                                     useVisitCountShortTerm = TRUE,
                                     useVisitConceptCountLongTerm = TRUE,
                                     useVisitConceptCountMediumTerm = TRUE,
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
                                   useConditionEraOverlap = TRUE,
                                   useConditionEraGroupStart = TRUE,
                                   useDrugExposure = TRUE,
                                   useDrugEraStart = TRUE,
                                   useDrugEraGroupStart = TRUE,
                                   useDrugEraOverlap = TRUE,
                                   useDrugEraGroupOverlap = TRUE,
                                   useVisitCount = TRUE,
                                   useVisitConceptCount = TRUE,
                                   useProcedureOccurrence = TRUE,
                                   useMeasurement = TRUE,
                                   temporalStartDays = c(
                                     -365,
                                     -30,
                                     0,
                                     1,
                                     31,
                                     seq(from = -421, to = -31, by = 30),
                                     seq(from = 0, to = 390, by = 30)
                                   ),
                                   temporalEndDays = c(
                                     -31,
                                     -1,
                                     0,
                                     30,
                                     365,
                                     seq(from = -391, to = -1, by = 30),
                                     seq(from = 30, to = 420, by = 30)
                                   )
                                 ),
                                 minCellCount = 5,
                                 incremental = FALSE,
                                 incrementalFolder = file.path(exportFolder, "incremental")) {
  # Execution mode determination----
  if (!is.null(cohortSetReference)) {
    ParallelLogger::logInfo("Found cohortSetReference. Cohort Diagnostics is running in WebApi mode.")
    cohortToCreateFile <- NULL
  }
  
  start <- Sys.time()
  ParallelLogger::logInfo("Run Cohort Diagnostics started at ", start)
  
  if (all(!is.null(oracleTempSchema), is.null(tempEmulationSchema))) {
    tempEmulationSchema <- oracleTempSchema
    warning('OracleTempSchema has been deprecated by DatabaseConnector')
  }
  
  if (any(is.null(databaseName), is.na(databaseName))) {
    databaseName <- databaseId
    ParallelLogger::logTrace('Databasename was not provided.')
  }
  if (any(is.null(databaseDescription), is.na(databaseDescription))) {
    databaseDescription <- databaseId
    ParallelLogger::logTrace('Databasedescription was not provided.')
  }
  
  # Assert checks----
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(runInclusionStatistics, add = errorMessage)
  checkmate::assertLogical(runIncludedSourceConcepts, add = errorMessage)
  checkmate::assertLogical(runOrphanConcepts, add = errorMessage)
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
  minCellCount <- utils::type.convert(minCellCount)
  checkmate::assertInteger(x = minCellCount, lower = 0, add = errorMessage)
  checkmate::assertLogical(incremental, add = errorMessage)
  
  if (any(
    runInclusionStatistics,
    runIncludedSourceConcepts,
    runOrphanConcepts,
    runBreakdownIndexEvents,
    runIncidenceRate,
    runCohortOverlap,
    runTimeSeries,
    runCohortTemporalRelationship,
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
    stop("No cohorts specified")
  }
  if ('name' %in% colnames(cohorts)) {
    cohorts <- cohorts %>%
      dplyr::select(-.data$name)
  }
  cohortTableColumnNamesObserved <- colnames(cohorts) %>%
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
  
  requiredButNotObsevered <-
    setdiff(x = cohortTableColumnNamesRequired, y = cohortTableColumnNamesObserved)
  if (length(requiredButNotObsevered) > 0) {
    stop(paste(
      "The following required fields not found in cohort table:",
      paste0(requiredButNotObsevered, collapse = ", ")
    ))
  }
  obseveredButNotExpected <-
    setdiff(x = cohortTableColumnNamesObserved, y = cohortTableColumnNamesExpected)
  if (length(obseveredButNotExpected) > 0) {
    ParallelLogger::logTrace(
      paste0(
        "The following columns were found in cohort table, but are not expected - they will be removed:",
        obseveredButNotExpected
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
  writeToCsv(data = cohorts,
             fileName = file.path(exportFolder, "cohort.csv"))
  
  # Set up connection to server----
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }
  
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
  ParallelLogger::logInfo("Saving database metadata")
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
  writeToCsv(data = database,
             fileName = file.path(exportFolder, "database.csv"))
  delta <- Sys.time() - startMetaData
  writeLines(paste(
    "Saving database metadata took",
    signif(delta, 3),
    attr(delta, "units")
  ))
  
  # Incremental mode----
  if (incremental) {
    ParallelLogger::logTrace("Working in incremental mode.")
    cohorts$checksum <- computeChecksum(cohorts$sql)
    recordKeepingFile <-
      file.path(incrementalFolder, "CreatedDiagnostics.csv")
    if (file.exists(path = recordKeepingFile)) {
      ParallelLogger::logInfo(
        "Found existing record keeping file in incremental folder - CreatedDiagnostics.csv"
      )
    }
  }
  
  # Counting cohorts----
  # this is required step, no condition
  ParallelLogger::logInfo("Counting cohort records and subjects")
  cohortCounts <- getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohorts$cohortId
  )
  if (!is.null(cohortCounts)) {
    cohortCounts <- cohortCounts %>%
      dplyr::mutate(databaseId = !!databaseId)
    if (nrow(cohortCounts) > 0) {
      cohortCounts <-
        enforceMinCellValue(data = cohortCounts,
                            fieldName = "cohortEntries",
                            minValues = minCellCount)
      cohortCounts <-
        enforceMinCellValue(data = cohortCounts,
                            fieldName = "cohortSubjects",
                            minValues = minCellCount)
    }
    writeToCsv(
      data = cohortCounts,
      fileName = file.path(exportFolder, "cohort_count.csv"),
      incremental = FALSE,
      cohortId = subset$cohortId
    )
    if (nrow(cohortCounts) > 0) {
      instantiatedCohorts <- cohortCounts %>%
        dplyr::pull(.data$cohortId)
      ParallelLogger::logInfo(
        sprintf(
          "Found %s of %s (%1.2f%%) submitted cohorts instantiated. ",
          length(instantiatedCohorts),
          nrow(cohorts),
          100 * (length(instantiatedCohorts) /
                   nrow(cohorts))
        ),
        "Beginning cohort diagnostics for instantiated cohorts. "
      )
    } else {
      instantiatedCohorts <- NULL
      ParallelLogger::logWarn(
        "All cohorts were either not instantiated or all have 0 records. All diagnostics will be empty."
      )
    }
  }
  
  # Inclusion statistics----
  if (runInclusionStatistics) {
    startInclusionStatistics <- Sys.time()
    if (is.null(instantiatedCohorts)) {
      ParallelLogger::logInfo(" - Skipping inclusion statistics from files because no cohorts were instantiated.")
    } else {
      ParallelLogger::logInfo("Fetching inclusion statistics from files")
      subset <- subsetToRequiredCohorts(
        cohorts = cohorts %>%
          dplyr::filter(.data$cohortId %in% instantiatedCohorts),
        task = "runInclusionStatistics",
        incremental = incremental,
        recordKeepingFile = recordKeepingFile
      )
      if (incremental &&
          (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo(sprintf(
          " - Skipping %s cohorts in incremental mode.",
          length(instantiatedCohorts) - nrow(subset)
        ))
      }
      if (nrow(subset) > 0) {
        stats <-
          getInclusionStatisticsFromFiles(
            cohortIds = subset$cohortId,
            folder = inclusionStatisticsFolder,
            simplify = TRUE
          )
        if (!is.null(stats$simplifiedOutput)) {
          stats$simplifiedOutput <- stats$simplifiedOutput %>%
            dplyr::mutate(databaseId = !!databaseId)
          if (nrow(stats$simplifiedOutput) > 0) {
            stats$simplifiedOutput <-
              enforceMinCellValue(
                data = stats$simplifiedOutput,
                fieldName = "meetSubjects",
                minValues = minCellCount
              )
            stats$simplifiedOutput <-
              enforceMinCellValue(
                data = stats$simplifiedOutput,
                fieldName = "gainSubjects",
                minValues = minCellCount
              )
            stats$simplifiedOutput <-
              enforceMinCellValue(
                data = stats$simplifiedOutput,
                fieldName = "totalSubjects",
                minValues = minCellCount
              )
            stats$simplifiedOutput <-
              enforceMinCellValue(
                data = stats$simplifiedOutput,
                fieldName = "remainSubjects",
                minValues = minCellCount
              )
          }
          
          if ("cohortDefinitionId" %in% (colnames(stats$simplifiedOutput))) {
            stats$simplifiedOutput <- stats$simplifiedOutput %>%
              dplyr::rename(cohortId = .data$cohortDefinitionId)
          }
          colnames(stats$simplifiedOutput) <-
            SqlRender::camelCaseToSnakeCase(colnames(stats$simplifiedOutput))
          writeToCsv(
            data = stats$simplifiedOutput,
            fileName = file.path(exportFolder, "inclusion_rule_stats.csv"),
            incremental = incremental,
            cohortId = subset$cohortId
          )
          
          listOfInclusionTables <- c(
            'cohortInclusion',
            'cohortInclusionResult',
            'cohortInclusionStats',
            'cohortSummaryStats'
          )
          
          for (k in (1:length(listOfInclusionTables))) {
            data <- stats[[listOfInclusionTables[[k]]]]
            if ('personCount' %in% colnames(data)) {
              data <- enforceMinCellValue(
                data = data,
                fieldName = "personCount",
                minValues = minCellCount
              )
            }
            if ('gainCount' %in% colnames(data)) {
              data <- enforceMinCellValue(
                data = data,
                fieldName = "gainCount",
                minValues = minCellCount
              )
            }
            if ('personTotal' %in% colnames(data)) {
              data <- enforceMinCellValue(
                data = data,
                fieldName = "personTotal",
                minValues = minCellCount
              )
            }
            if ('baseCount' %in% colnames(data)) {
              data <- enforceMinCellValue(
                data = data,
                fieldName = "baseCount",
                minValues = minCellCount
              )
            }
            if ('finalCount' %in% colnames(data)) {
              data <- enforceMinCellValue(
                data = data,
                fieldName = "finalCount",
                minValues = minCellCount
              )
            }
            if ("cohortDefinitionId" %in% (colnames(data))) {
              data <- data %>%
                dplyr::rename(cohortId = .data$cohortDefinitionId)
            }
            if (!"databaseId" %in% (colnames(data))) {
              data <- data %>%
                dplyr::mutate(databaseId = !!databaseId)
            }
            
            colnames(data) <-
              SqlRender::camelCaseToSnakeCase(colnames(data))
            
            writeToCsv(
              data = data,
              fileName = file.path(
                exportFolder,
                paste0(
                  camelCaseToSnakeCase(listOfInclusionTables[[k]]),
                  ".csv"
                )
              ),
              incremental = incremental,
              cohortId = subset$cohortId
            )
          }
          recordTasksDone(
            cohortId = subset$cohortId,
            task = "runInclusionStatistics",
            checksum = subset$checksum,
            recordKeepingFile = recordKeepingFile,
            incremental = incremental
          )
        } else {
          ParallelLogger::logWarn(
            paste0(
              "Cohort Inclusion statistics files not found.\n",
              "    This might mean that the instantiated cohort(s) did not have\n",
              "    any inclusion statistics or the inclusion statistics files\n",
              "    that were exported as part of cohort instantiation\n",
              "    were not found."
            )
          )
        }
      }
    }
    delta <- Sys.time() - startInclusionStatistics
    ParallelLogger::logInfo("Running Inclusion Statistics took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  # Concept set diagnostics----
  if (runIncludedSourceConcepts ||
      runOrphanConcepts || runBreakdownIndexEvents) {
    # running together because share common process of needing to resolve concept sets
    ParallelLogger::logInfo("Beginning concept set diagnostics.")
    # note for incremental mode - if a cohort id is eligible for computation for any diagnostics,
    # all diagnostics are computed for that cohort
    startConceptSetDiagnostics <- Sys.time()
    
    subset <- dplyr::tibble()
    if (runIncludedSourceConcepts) {
      subsetIncluded <- subsetToRequiredCohorts(
        cohorts = cohorts,
        task = "runIncludedSourceConcepts",
        incremental = incremental,
        recordKeepingFile = recordKeepingFile
      )
      subset <- dplyr::bind_rows(subset, subsetIncluded)
    }
    if (runBreakdownIndexEvents) {
      subsetBreakdown <- subsetToRequiredCohorts(
        cohorts = cohorts,
        task = "runBreakdownIndexEvents",
        incremental = incremental,
        recordKeepingFile = recordKeepingFile
      )
      subset <- dplyr::bind_rows(subset, subsetBreakdown)
    }
    if (runOrphanConcepts) {
      subsetOrphans <- subsetToRequiredCohorts(
        cohorts = cohorts,
        task = "runOrphanConcepts",
        incremental = incremental,
        recordKeepingFile = recordKeepingFile
      )
      subset <- dplyr::bind_rows(subset, subsetOrphans)
    }
    if (runCohortCharacterization) {
      subsetCharacterization <- subsetToRequiredCohorts(
        cohorts = cohorts,
        task = "runCohortCharacterization",
        incremental = incremental,
        recordKeepingFile = recordKeepingFile
      )
      subset <- dplyr::bind_rows(subset, subsetCharacterization)
    }
    subset <- dplyr::distinct(subset)
    ParallelLogger::logInfo(sprintf(
      " - Skipping %s cohorts in incremental mode.",
      nrow(cohorts) - nrow(subset)
    ))
    
    if (nrow(subset) > 0) {
      conceptSetDiagnostics <- runConceptSetDiagnostics(
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        cohorts = cohorts,
        cohortIds = subset$cohortId,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        runIncludedSourceConcepts = runIncludedSourceConcepts,
        runOrphanConcepts = runOrphanConcepts,
        runBreakdownIndexEvents = runBreakdownIndexEvents
      )
      
      # write vocabulary tables
      vocabularyTableNames = c(
        "concept",
        "concept_ancestor",
        "concept_class",
        "concept_relationship",
        "concept_synonym",
        "domain",
        "relationship",
        "vocabulary"
      )
      
      for (i in (1:length(vocabularyTableNames))) {
        if (vocabularyTableNames[[i]] %in% names(conceptSetDiagnostics)) {
          ParallelLogger::logInfo(
            paste0(
              "- Writing extracted vocabulary data to file - ",
              vocabularyTableNames[[i]]
            ),
            ".csv"
          )
          if (vocabularyTableNames[[i]] %in% c("domain",
                                               "relationship",
                                               "vocabulary",
                                               "concept_class")) {
            writeToCsv(
              data = conceptSetDiagnostics[[vocabularyTableNames[[i]]]],
              fileName = file.path(
                exportFolder,
                paste(vocabularyTableNames[[i]], "csv", sep = ".")
              ),
              incremental = FALSE
            )
          } else {
            writeToCsv(
              data = conceptSetDiagnostics[[vocabularyTableNames[[i]]]],
              fileName = file.path(
                exportFolder,
                paste(vocabularyTableNames[[i]], "csv", sep = ".")
              ),
              incremental = incremental
            )
          }
          conceptSetDiagnostics[[vocabularyTableNames[[i]]]] <- NULL
        }
      }
      if ('conceptSets' %in% names(conceptSetDiagnostics)) {
        ParallelLogger::logInfo("- Writing concept_sets.csv")
        writeToCsv(
          data = conceptSetDiagnostics$conceptSets %>%
            dplyr::select(
              .data$cohortId,
              .data$conceptSetExpression,
              .data$conceptSetId,
              .data$conceptSetName,
              .data$conceptSetSql
            ) %>%
            dplyr::distinct(),
          fileName = file.path(exportFolder, "concept_sets.csv"),
          incremental = incremental,
          cohortId = conceptSetDiagnostics$conceptSets$cohortId
        )
        conceptSetDiagnostics$conceptSets <- NULL
      }
      
      if ('resolvedConceptIds' %in% names(conceptSetDiagnostics)) {
        ParallelLogger::logInfo("- Writing resolved_concepts.csv")
        writeToCsv(
          data = conceptSetDiagnostics$resolvedConceptIds %>%
            dplyr::mutate(databaseId = !!databaseId),
          fileName = file.path(exportFolder, "resolved_concepts.csv"),
          incremental = incremental,
          cohortId = conceptSetDiagnostics$resolvedConceptIds$cohortId
        )
        conceptSetDiagnostics$resolvedConceptIds <- NULL
      }
      
      if ('includedSourceCodes' %in% names(conceptSetDiagnostics) &&
          runIncludedSourceConcepts) {
        if (nrow(conceptSetDiagnostics$includedSourceCodes) > 0) {
          ParallelLogger::logInfo("- Writing included_source_concept.csv")
          conceptSetDiagnostics$includedSourceCodes$databaseId <-
            databaseId
          conceptSetDiagnostics$includedSourceCodes <-
            enforceMinCellValue(
              conceptSetDiagnostics$includedSourceCodes,
              "conceptSubjects",
              minCellCount
            )
          conceptSetDiagnostics$includedSourceCodes <-
            enforceMinCellValue(
              conceptSetDiagnostics$includedSourceCodes,
              "conceptCount",
              minCellCount
            )
          writeToCsv(
            data = conceptSetDiagnostics$includedSourceCodes,
            fileName = file.path(exportFolder, "included_source_concept.csv"),
            incremental = incremental,
            cohortId = conceptSetDiagnostics$includedSourceCodes$cohortId
          )
        }
        recordTasksDone(
          cohortId = subset$cohortId,
          task = "runIncludedSourceConcepts",
          checksum = subset$checksum,
          recordKeepingFile = recordKeepingFile,
          incremental = incremental
        )
        conceptSetDiagnostics$includedSourceCodes <- NULL
      }
      
      if ('indexEventBreakdown' %in% names(conceptSetDiagnostics) &&
          runBreakdownIndexEvents) {
        if (nrow(conceptSetDiagnostics$indexEventBreakdown) > 0) {
          ParallelLogger::logInfo("- Writing index_event_breakdown.csv")
          conceptSetDiagnostics$indexEventBreakdown$databaseId <-
            databaseId
          conceptSetDiagnostics$indexEventBreakdown <-
            enforceMinCellValue(
              conceptSetDiagnostics$indexEventBreakdown,
              "subjectCount",
              minCellCount
            )
          conceptSetDiagnostics$indexEventBreakdown <-
            enforceMinCellValue(
              conceptSetDiagnostics$indexEventBreakdown,
              "conceptCount",
              minCellCount
            )
          writeToCsv(
            data = conceptSetDiagnostics$indexEventBreakdown,
            fileName = file.path(exportFolder, "index_event_breakdown.csv"),
            incremental = incremental,
            cohortId = conceptSetDiagnostics$indexEventBreakdown$cohortId
          )
        }
        recordTasksDone(
          cohortId = subset$cohortId,
          task = "runBreakdownIndexEvents",
          checksum = subset$checksum,
          recordKeepingFile = recordKeepingFile,
          incremental = incremental
        )
        conceptSetDiagnostics$indexEventBreakdown <- NULL
      }
      
      if ('orphanCodes' %in% names(conceptSetDiagnostics) &&
          runOrphanConcepts) {
        if (nrow(conceptSetDiagnostics$orphanCodes) > 0) {
          ParallelLogger::logInfo("- Writing orphan_concept.csv")
          conceptSetDiagnostics$orphanCodes$databaseId <- databaseId
          conceptSetDiagnostics$orphanCodes <-
            enforceMinCellValue(conceptSetDiagnostics$orphanCodes,
                                "conceptSubjects",
                                minCellCount)
          conceptSetDiagnostics$orphanCodes <-
            enforceMinCellValue(conceptSetDiagnostics$orphanCodes,
                                "conceptCount",
                                minCellCount)
          writeToCsv(
            data = conceptSetDiagnostics$orphanCodes,
            fileName = file.path(exportFolder, "orphan_concept.csv"),
            incremental = incremental,
            cohortId = conceptSetDiagnostics$orphanCodes$cohortId %>% unique()
          )
        }
        recordTasksDone(
          cohortId = subset$cohortId,
          task = "runOrphanConcepts",
          checksum = subset$checksum,
          recordKeepingFile = recordKeepingFile,
          incremental = incremental
        )
        conceptSetDiagnostics$orphanCodes <- NULL
      }
    }
    delta <- Sys.time() - startConceptSetDiagnostics
    ParallelLogger::logInfo("Running Concept Set Diagnostics took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
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
    if (incremental &&
        (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf(
        " - Skipping %s cohorts in incremental mode.",
        length(instantiatedCohorts) - nrow(subset)
      ))
    }
    if (nrow(subset) > 0) {
      data <- runVisitContextDiagnostics(
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        cohortTable = cohortTable,
        cdmVersion = cdmVersion,
        cohortIds = subset$cohortId
      )
      if (nrow(data) > 0) {
        data <- data %>%
          dplyr::mutate(databaseId = !!databaseId)
        data <- enforceMinCellValue(data, "subjects", minCellCount)
        writeToCsv(
          data = data,
          fileName = file.path(exportFolder, "visit_context.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
        recordTasksDone(
          cohortId = subset$cohortId,
          task = "runVisitContext",
          checksum = subset$checksum,
          recordKeepingFile = recordKeepingFile,
          incremental = incremental
        )
      }
    }
    delta <- Sys.time() - startVisitContext
    ParallelLogger::logInfo("Running Visit Context took ",
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
    if (incremental &&
        (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf(
        " - Skipping %s cohorts in incremental mode.",
        length(instantiatedCohorts) - nrow(subset)
      ))
    }
    if (nrow(subset) > 0) {
      runIncidenceRate <- function(row) {
        ParallelLogger::logInfo("  Computing incidence rate for cohort '",
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
        if (nrow(data) > 0) {
          data <- data %>% dplyr::mutate(cohortId = row$cohortId)
        }
        return(data)
      }
      data <-
        lapply(split(subset, subset$cohortId), runIncidenceRate)
      data <- dplyr::bind_rows(data)
      if (nrow(data) > 0) {
        data <- data %>% dplyr::mutate(databaseId = !!databaseId)
        data <-
          enforceMinCellValue(data, "cohortCount", minCellCount)
        data <-
          enforceMinCellValue(data,
                              "incidenceRate",
                              1000 * minCellCount / data$personYears)
      }
      writeToCsv(
        data = data,
        fileName = file.path(exportFolder, "incidence_rate.csv"),
        incremental = incremental,
        cohortId = subset$cohortId
      )
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runIncidenceRate",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    }
    delta <- Sys.time() - startIncidenceRate
    ParallelLogger::logInfo("Running Incidence Rate took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  # Cohort overlap----
  if (runCohortOverlap) {
    ParallelLogger::logInfo("Computing cohort overlap")
    startCohortOverlap <- Sys.time()
    subset <- subsetToRequiredCohorts(
      cohorts = cohorts %>%
        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
      task = "runCohortOverlap",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    if (incremental &&
        (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf(
        " - Skipping %s cohorts in incremental mode.",
        nrow(cohorts) - nrow(subset)
      ))
    }
    if (nrow(subset) > 0) {
      ParallelLogger::logTrace("Beginning Cohort overlap SQL")
      cohortOverlap <- runCohortOverlapDiagnostics(
        connection = connection,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        targetCohortIds = subset$cohortId,
        comparatorCohortIds = cohorts$cohortId
      )
      if (nrow(cohortOverlap) > 0) {
        cohortOverlap <-
          enforceMinCellValue(cohortOverlap, "countValue", minCellCount)
        
        cohortOverlap <- cohortOverlap %>%
          dplyr::mutate(
            attributeName = dplyr::case_when(
              .data$attributeName == 'es' ~ 'eitherSubjects',
              .data$attributeName == 'bs' ~ 'bothSubjects',
              .data$attributeName == 'ts' ~ 'tOnlySubjects',
              .data$attributeName == 'cs' ~ 'cOnlySubjects',
              .data$attributeName == 'tb' ~ 'tBeforeCSubjects',
              .data$attributeName == 'cb' ~ 'cBeforeTSubjects',
              .data$attributeName == 'sd' ~ 'sameDaySubjects',
              .data$attributeName == 'tc' ~ 'tInCSubjects',
              .data$attributeName == 'ct' ~ 'cInTSubjects'
            )
          ) %>%
          dplyr::rename(targetCohortId = .data$cohortId) %>%
          dplyr::select(
            .data$targetCohortId,
            .data$comparatorCohortId,
            .data$attributeName,
            .data$countValue
          ) %>%
          tidyr::pivot_wider(
            id_cols = c("targetCohortId", "comparatorCohortId"),
            values_from = "countValue",
            values_fill = 0,
            names_from = "attributeName"
          ) %>%
          dplyr::mutate(databaseId = !!databaseId) %>%
          dplyr::select(
            .data$eitherSubjects,
            .data$bothSubjects,
            .data$tOnlySubjects,
            .data$cOnlySubjects,
            .data$tBeforeCSubjects,
            .data$cBeforeTSubjects,
            .data$sameDaySubjects,
            .data$tInCSubjects,
            .data$cInTSubjects,
            .data$targetCohortId,
            .data$comparatorCohortId,
            .data$databaseId
          )
        writeToCsv(
          data = cohortOverlap,
          fileName = file.path(exportFolder, "cohort_overlap.csv"),
          incremental = incremental,
          targetCohortId = subset$cohortId
        )
      } else {
        warning('No cohort overlap data')
      }
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runCohortOverlap",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    }
    delta <- Sys.time() - startCohortOverlap
    ParallelLogger::logInfo("Computing cohort overlap took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  # Time Series----
  if (runTimeSeries) {
    ParallelLogger::logInfo("Computing Time Series")
    startTimeSeries <- Sys.time()
    subset <- subsetToRequiredCohorts(
      cohorts = cohorts %>%
        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
      task = "runTimeSeries",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    if (incremental &&
        (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf(
        " - Skipping %s cohorts in incremental mode.",
        length(instantiatedCohorts) - nrow(subset)
      ))
    }
    
    if (nrow(subset) > 0) {
      timeSeries <-
        runCohortTimeSeriesDiagnostics(
          connection = connection,
          tempEmulationSchema = tempEmulationSchema,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cdmDatabaseSchema = cdmDatabaseSchema,
          cohortTable = cohortTable,
          timeSeriesMinDate = observationPeriodDateRange$observationPeriodMinDate,
          timeSeriesMaxDate = observationPeriodDateRange$observationPeriodMaxDate,
          cohortIds = subset$cohortId
        )
      
      if (!is.null(timeSeries) && nrow(timeSeries) > 0) {
        timeSeries <- timeSeries %>%
          dplyr::mutate(databaseId = !!databaseId)
        timeSeries <-
          enforceMinCellValue(timeSeries, "records", minCellCount)
        timeSeries <-
          enforceMinCellValue(timeSeries, "subjects", minCellCount)
        timeSeries <-
          enforceMinCellValue(timeSeries, "personDays", minCellCount)
        timeSeries <-
          enforceMinCellValue(timeSeries, "recordsIncidence", minCellCount)
        timeSeries <-
          enforceMinCellValue(timeSeries, "subjectsIncidence", minCellCount)
        timeSeries <-
          enforceMinCellValue(timeSeries, "recordsTerminate", minCellCount)
        timeSeries <-
          enforceMinCellValue(timeSeries, "subjectsTerminate", minCellCount)
        writeToCsv(
          data = timeSeries,
          fileName = file.path(exportFolder, "time_series.csv"),
          incremental = incremental,
          cohortId = c(subset$cohortId) %>% unique()
        )
      }
      else {
        warning('No time series data')
      }
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runTimeSeries",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    }
    delta <- Sys.time() - startTimeSeries
    ParallelLogger::logInfo("Computing time series took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  # Cohort Temporal Relationship ----
  if (runCohortTemporalRelationship) {
    ParallelLogger::logInfo("Computing Cohort Temporal Relationship")
    startCohortRelationship <- Sys.time()
    
    subset <- subsetToRequiredCohorts(
      cohorts = cohorts %>%
        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
      task = "runCohortTemporalRelationship",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    if (incremental &&
        (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf(
        " - Skipping %s cohort combinations in incremental mode.",
        nrow(cohorts) - nrow(subset)
      ))
    }
    
    if (nrow(subset) > 0) {
      ParallelLogger::logTrace("Beginning Cohort Relationship SQL")
      cohortTemporalRelationship <-
        runCohortTemporalRelationshipDiagnostics(
          connection = connection,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          targetCohortIds = subset$cohortId,
          comparatorCohortIds = cohorts$cohortId
        )
      
      if (nrow(cohortTemporalRelationship) > 0) {
        cohortTemporalRelationship <- cohortTemporalRelationship %>%
          dplyr::mutate(databaseId = !!databaseId)
        writeToCsv(
          data = cohortTemporalRelationship,
          fileName = file.path(exportFolder, "cohort_relationships.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      } else {
        warning('No cohort relationship data')
      }
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runCohortTemporalRelationship",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    }
    delta <- Sys.time() - startCohortRelationship
    ParallelLogger::logInfo("Computing cohort relationships took ",
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
    
    if (incremental &&
        (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf(
        " - Skipping %s cohorts in incremental mode.",
        length(instantiatedCohorts) - nrow(subset)
      ))
    }
    if (nrow(subset) > 0) {
      ParallelLogger::logInfo(sprintf(
        "Starting large scale characterization of %s cohort(s)",
        nrow(subset)
      ))
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
      
      exportCharacterization(
        characteristics = characteristics,
        databaseId = databaseId,
        incremental = incremental,
        covariateValueFileName = file.path(exportFolder, "covariate_value.csv"),
        covariateValueContFileName = file.path(exportFolder, "covariate_value_dist.csv"),
        covariateRefFileName = file.path(exportFolder, "covariate_ref.csv"),
        analysisRefFileName = file.path(exportFolder, "analysis_ref.csv"),
        timeDistributionFileName = file.path(exportFolder, 'time_distribution.csv'),
        counts = cohortCounts,
        minCellCount = minCellCount
      )
    }
    recordTasksDone(
      cohortId = subset$cohortId,
      task = "runCohortCharacterization",
      checksum = subset$checksum,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
    delta <- Sys.time() - startCohortCharacterization
    ParallelLogger::logInfo("Running Characterization took ",
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
    
    if (incremental &&
        (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf(
        " - Skipping %s cohorts in incremental mode.",
        length(instantiatedCohorts) - nrow(subset)
      ))
    }
    if (nrow(subset) > 0) {
      ParallelLogger::logInfo(sprintf(
        "Starting large scale temporal characterization of %s cohort(s)",
        nrow(subset)
      ))
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
      exportCharacterization(
        characteristics = characteristics,
        databaseId = databaseId,
        incremental = incremental,
        covariateValueFileName = file.path(exportFolder, "temporal_covariate_value.csv"),
        covariateRefFileName = file.path(exportFolder, "temporal_covariate_ref.csv"),
        analysisRefFileName = file.path(exportFolder, "temporal_analysis_ref.csv"),
        timeRefFileName = file.path(exportFolder, "temporal_time_ref.csv"),
        # add temporal_covariate_value_dist.csv, but timeId does not seem to be correctly returned
        counts = cohortCounts,
        minCellCount = minCellCount
      )
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
      "Running Temporal Characterization took ",
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
      variableField = c(
        "CohortDiagnosticsVersion",
        "DatabaseConnectorVersion",
        "FeatureExtractionVersion",
        "SqlRenderVersion",
        "AndromedaVersion",
        "dplyrVersion",
        "tidyrVersion",
        "Rversion",
        "CurrentPackage",
        "CurrentPackageVersion",
        "runTime",
        "runTimeUnits",
        "sourceDescription",
        "cdmSourceName",
        "sourceReleaseDate",
        "cdmVersion",
        "cdmReleaseDate",
        "vocabularyVersion",
        "covariateSettingsJson",
        "temporalCovariateSettingsJson"
      ),
      valueField =  c(
        as.character(packageVersion("CohortDiagnostics")),
        as.character(packageVersion("DatabaseConnector")),
        as.character(packageVersion("FeatureExtraction")),
        as.character(packageVersion("SqlRender")),
        as.character(packageVersion("Andromeda")),
        as.character(packageVersion("dplyr")),
        as.character(packageVersion("tidyr")),
        as.character(R.Version()$version.string),
        as.character(nullToEmpty(packageName())),
        as.character(if (!getPackageName() == ".GlobalEnv") {
          packageVersion(packageName())
        } else {
          ''
        }),
        as.character(as.numeric(
          x = delta, units = attr(delta, "units")
        )),
        as.character(attr(delta, "units")),
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
        )),
        as.character(RJSONIO::toJSON(
          covariateSettings, digits = 23, pretty = TRUE
        )),
        as.character(
          RJSONIO::toJSON(
            temporalCovariateSettings,
            digits = 23,
            pretty = TRUE
          )
        )
      )
    )
  writeToCsv(data = metadata,
             fileName = file.path(exportFolder, "metadata.csv"))
  
  
  # Add all to zip file----
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <-
    file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at: ", zipName)
  
  delta <- Sys.time() - start
  
  ParallelLogger::logInfo("Computing all diagnostics took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
}
