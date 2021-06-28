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
#' @param runTimeDistributions        Generate and export cohort time distributions?
#' @param runVisitContext             Generate and export index-date visit context?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#' @param runIncidenceRate            Generate and export the cohort incidence  rates?
#' @param runTimeSeries               Generate and export the cohort prevalence  rates?
#' @param runCohortOverlap            Generate and export the cohort overlap? Overlaps are checked within cohortIds
#'                                    that have the same phenotype ID sourced from the CohortSetReference or
#'                                    cohortToCreateFile.
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
                                 runTimeDistributions = TRUE,
                                 runVisitContext = TRUE,
                                 runBreakdownIndexEvents = TRUE,
                                 runIncidenceRate = TRUE,
                                 runTimeSeries = TRUE,
                                 runCohortOverlap = TRUE,
                                 runCohortRelationship = TRUE,
                                 runCohortCharacterization = TRUE,
                                 covariateSettings = createDefaultCovariateSettings(),
                                 runTemporalCohortCharacterization = TRUE,
                                 temporalCovariateSettings = createTemporalCovariateSettings(
                                   useConditionOccurrence = TRUE,
                                   useDrugEraStart = TRUE,
                                   useDrugEraOverlap = TRUE,
                                   useVisitCount = TRUE,
                                   useVisitConceptCount = TRUE,
                                   useProcedureOccurrence = TRUE,
                                   useMeasurement = TRUE,
                                   temporalStartDays = c(-365, -30, 0, 1, 31, 
                                                         seq(from = -421, to = -31, by = 30), 
                                                         seq(from = 0, to = 390, by = 30)),
                                   temporalEndDays = c(-31, -1, 0, 30, 365, 
                                                       seq(from = -391, to = -1, by = 30),  
                                                       seq(from = 30, to = 420, by = 30))
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
  minCellCount <- utils::type.convert(minCellCount)
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
    runTimeSeries,
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
    cohorts$logicDescription <- cohorts$cohortName
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
    cohorts <- cohorts %>%
      dplyr::mutate(metadata = as.list(columnsToAddToJson) %>% RJSONIO::toJSON(digits = 23))
  } else {
    if (length(obseveredButNotExpected) > 0) {
      writeLines(
        paste(
          "The following columns were observed in the cohort table, \n
          that are not expected. If you would like to retain them please add \n
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
    getCdmDataSourceInformation(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema
    )
  
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
      ParallelLogger::logWarn("All cohorts were either not instantiated or all have 0 records. All diagnostics will be empty.")
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
        if (!is.null(stats)) {
          stats <- stats %>%
            dplyr::mutate(databaseId = !!databaseId)
          if (nrow(stats) > 0) {
            stats <-
              enforceMinCellValue(data = stats,
                                  fieldName = "meetSubjects",
                                  minValues = minCellCount)
            stats <-
              enforceMinCellValue(data = stats,
                                  fieldName = "gainSubjects",
                                  minValues = minCellCount)
            stats <-
              enforceMinCellValue(data = stats,
                                  fieldName = "totalSubjects",
                                  minValues = minCellCount)
            stats <-
              enforceMinCellValue(data = stats,
                                  fieldName = "remainSubjects",
                                  minValues = minCellCount)
          }
          if ("cohortDefinitionId" %in% (colnames(stats))) {
            stats <- stats %>%
              dplyr::rename(cohortId = .data$cohortDefinitionId)
          }
          colnames(stats) <-
            SqlRender::camelCaseToSnakeCase(colnames(stats))
          writeToCsv(
            data = stats,
            fileName = file.path(exportFolder, "inclusion_rule_stats.csv"),
            incremental = incremental,
            cohortId = subset$cohortId
          )
          recordTasksDone(
            cohortId = subset$cohortId,
            task = "runInclusionStatistics",
            checksum = subset$checksum,
            recordKeepingFile = recordKeepingFile,
            incremental = incremental
          )
        } else {
          ParallelLogger::logWarn(paste0("Cohort Inclusion statistics files not found.\n",
                                         "    This might mean that the instantiated cohort(s) did not have\n",
                                         "    any inclusion statistics or the inclusion statistics files\n",
                                         "    that were exported as part of cohort instantiation\n",
                                         "    were not found."))
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
          ParallelLogger::logInfo(paste0("- Writing extracted vocabulary data to file - ", vocabularyTableNames[[i]]), ".csv")
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
          data = conceptSetDiagnostics$conceptSets,
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
        ParallelLogger::logInfo("- Writing included_source_concept.csv")
        if (nrow(conceptSetDiagnostics$includedSourceCodes) > 0) {
          conceptSetDiagnostics$includedSourceCodes$databaseId <- databaseId
          conceptSetDiagnostics$includedSourceCodes <-
            enforceMinCellValue(conceptSetDiagnostics$includedSourceCodes,
                                "conceptSubjects",
                                minCellCount)
          conceptSetDiagnostics$includedSourceCodes <-
            enforceMinCellValue(conceptSetDiagnostics$includedSourceCodes,
                                "conceptCount",
                                minCellCount)
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
        ParallelLogger::logInfo("- Writing index_event_breakdown.csv")
        if (nrow(conceptSetDiagnostics$indexEventBreakdown) > 0) {
          conceptSetDiagnostics$indexEventBreakdown$databaseId <- databaseId
          conceptSetDiagnostics$indexEventBreakdown <-
            enforceMinCellValue(conceptSetDiagnostics$indexEventBreakdown,
                                "subjectCount",
                                minCellCount)
          conceptSetDiagnostics$indexEventBreakdown <-
            enforceMinCellValue(conceptSetDiagnostics$indexEventBreakdown,
                                "conceptCount",
                                minCellCount)
          writeToCsv(
            data = conceptSetDiagnostics$indexEventBreakdown,
            fileName = file.path(exportFolder, "index_event_breakdown.csv"),
            incremental = incremental,
            cohortId = conceptSetDiagnostics$includedSourceCodes$cohortId
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
        ParallelLogger::logInfo("- Writing orphan_concept.csv")
        if (nrow(conceptSetDiagnostics$orphanCodes) > 0) {
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
  
  # Time distributions----
  if (runTimeDistributions) {
    ParallelLogger::logInfo("Creating time distributions")
    startTimeDistribution <- Sys.time()
    subset <- subsetToRequiredCohorts(
      cohorts = cohorts %>%
        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
      task = "runTimeDistributions",
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
      data <- getTimeDistributions(
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cdmVersion = cdmVersion,
        cohortIds = subset$cohortId
      )
      if (nrow(data) > 0) {
        data <- data %>%
          dplyr::mutate(databaseId = !!databaseId)
        writeToCsv(
          data = data,
          fileName = file.path(exportFolder, "time_distribution.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      }
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runTimeDistributions",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    }
    delta <- Sys.time() - startTimeDistribution
    ParallelLogger::logInfo("Running Time Distribution took ",
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
      data <- getVisitContext(
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
        data <- getIncidenceRate(
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
        " - Skipping %s cohort combinations in incremental mode.",
        nrow(combis) - nrow(subset)
      ))
    }
    if (nrow(subset) > 0) {
      ParallelLogger::logTrace("Beginning Cohort overlap SQL")
      cohortOverlap <- computeCohortOverlap(
        connection = connection,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cohortIds = subset$cohortId
      )
      if (nrow(cohortOverlap) > 0) {
        cohortOverlap <-
          enforceMinCellValue(cohortOverlap, "valueCount", minCellCount)
        
        cohortOverlap <- cohortOverlap %>% 
          dplyr::mutate(attributeName = dplyr::case_when(.data$attributeName == 'es' ~ 'eitherSubjects',
                                                         .data$attributeName == 'bs' ~ 'bothSubjects',
                                                         .data$attributeName == 'ts' ~ 'tOnlySubjects',
                                                         .data$attributeName == 'cs' ~ 'cOnlySubjects',
                                                         .data$attributeName == 'tb' ~ 'tBeforeCSubjects',
                                                         .data$attributeName == 'cb' ~ 'cBeforeTSubjects',
                                                         .data$attributeName == 'sd' ~ 'sameDaySubjects',
                                                         .data$attributeName == 'tc' ~ 'tInCSubjects',
                                                         .data$attributeName == 'ct' ~ 'cInTSubjects')
          ) %>% 
          dplyr::rename(targetCohortId = .data$cohortId) %>% 
          dplyr::select(.data$targetCohortId, 
                        .data$comparatorCohortId,
                        .data$attributeName,
                        .data$valueCount) %>%
          tidyr::pivot_wider(id_cols = c("targetCohortId", "comparatorCohortId"),
                             values_from = "valueCount",
                             values_fill = 0,
                             names_from = "attributeName") %>%
          dplyr::mutate(databaseId = !!databaseId) %>% 
          dplyr::select(.data$eitherSubjects, .data$bothSubjects, .data$tOnlySubjects,
                        .data$cOnlySubjects, .data$tBeforeCSubjects, .data$cBeforeTSubjects,
                        .data$sameDaySubjects, .data$tInCSubjects, .data$cInTSubjects,
                        .data$targetCohortId, .data$comparatorCohortId, .data$databaseId)
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
        cohortId = subset$targetCohortId,
        comparatorId = subset$comparatorCohortId,
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
  browser()
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
      ## Calendar period----
      ParallelLogger::logTrace("Preparing calendar table for time series computation.")
      # note calendar span is created based on all dates in observation period table, with 1980 cut off/left censor (arbitary choice)
      minYear <- (max(clock::get_year(observationPeriodDateRange$observationPeriodMinDate), 1980) %>% as.integer())
      maxYear <- clock::get_year(observationPeriodDateRange$observationPeriodMaxDate) %>% as.integer()
      
      calendarQuarter <- dplyr::tibble(periodBegin = clock::date_seq(from = clock::date_build(year = minYear), 
                                                                     to = clock::date_build(year = maxYear + 1), 
                                                                     by = clock::duration_months(3))) %>% 
        dplyr::mutate(periodEnd = clock::add_months(x = .data$periodBegin, n = 3) - 1) %>% 
        dplyr::mutate(calendarInterval = 'q')
      
      calendarMonth <- dplyr::tibble(periodBegin = clock::date_seq(from = clock::date_build(year = minYear), 
                                                                   to = clock::date_build(year = maxYear + 1), 
                                                                   by = clock::duration_months(1))) %>% 
        dplyr::mutate(periodEnd = clock::add_months(x = .data$periodBegin, n = 1) - 1) %>% 
        dplyr::mutate(calendarInterval = 'm')
      
      calendarYear <- dplyr::tibble(periodBegin = clock::date_seq(from = clock::date_build(year = minYear), 
                                                                  to = clock::date_build(year = maxYear + 1 + 1), 
                                                                  by = clock::duration_years(1))) %>% 
        dplyr::mutate(periodEnd = clock::add_years(x = .data$periodBegin, n = 1) - 1) %>% 
        dplyr::mutate(calendarInterval = 'y')
      
      # calendarWeek <- dplyr::tibble(periodBegin = clock::date_seq(from = (clock::year_month_weekday(year = cohortDateRange$minYear %>% as.integer(),
      #                                                                                               month = clock::clock_months$january,
      #                                                                                               day = clock::clock_weekdays$monday,
      #                                                                                               index = 1) %>% 
      #                                                                       clock::as_date() %>% 
      #                                                                       clock::add_weeks(n = -1)),
      #                                                             to = (clock::year_month_weekday(year = (cohortDateRange$maxYear  %>% as.integer()) + 1,
      #                                                                                             month = clock::clock_months$january,
      #                                                                                             day = clock::clock_weekdays$sunday,
      #                                                                                             index = 1) %>% 
      #                                                                     clock::as_date()), 
      #                                                             by = clock::duration_weeks(n = 1))) %>% 
      #   dplyr::mutate(periodEnd = clock::add_days(x = .data$periodBegin, n = 6))
      
      calendarPeriods <- dplyr::bind_rows(calendarMonth, calendarQuarter, calendarYear) %>%  #calendarWeek
        # dplyr::filter(.data$periodBegin >= as.Date('1999-12-25')) %>% 
        # dplyr::filter(.data$periodEnd <= clock::date_today("")) %>% 
        dplyr::distinct()
      
      ParallelLogger::logTrace("Inserting calendar periods")
      DatabaseConnector::insertTable(
        connection = connection,
        tableName = "#calendar_periods",
        data = calendarPeriods,
        dropTableIfExists = TRUE,
        createTable = TRUE,
        progressBar = TRUE,
        tempTable = TRUE,
        tempEmulationSchema = tempEmulationSchema,
        camelCaseToSnakeCase = TRUE
      )
      ParallelLogger::logTrace("Done inserting calendar periods")
      
      ParallelLogger::logTrace("Beginning time series SQL")
      sql <-
        SqlRender::loadRenderTranslateSql(
          "ComputeTimeSeries.sql",
          packageName = "CohortDiagnostics",
          dbms = connection@dbms
        )
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        cohort_database_schema = cohortDatabaseSchema,
        cdm_database_schema = cdmDatabaseSchema,
        cohort_table = cohortTable, 
        tempEmulationSchema = tempEmulationSchema,
        cohort_ids = subset$cohortId
      ) 
      timeSeries <- renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT * FROM #time_series;", 
        tempEmulationSchema = tempEmulationSchema,
        snakeCaseToCamelCase = TRUE)
      
      timeSeries <- timeSeries %>% 
        dplyr::mutate(databaseId = !!databaseId) %>% 
        dplyr::select(.data$cohortId, .data$databaseId, 
                      .data$periodBegin, .data$calendarInterval,
                      .data$seriesType,
                      .data$records, .data$subjects,
                      .data$personDays, .data$recordsIncidence,
                      .data$subjectsIncidence, .data$recordsTerminate,
                      .data$subjectsTerminate)
      
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = "IF OBJECT_ID('tempdb..#calendar_periods', 'U') IS NOT NULL DROP TABLE #calendar_periods;",
        progressBar = TRUE, 
        tempEmulationSchema = tempEmulationSchema
      )
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = "IF OBJECT_ID('tempdb..#time_series', 'U') IS NOT NULL DROP TABLE #time_series;",
        progressBar = TRUE, 
        tempEmulationSchema = tempEmulationSchema
      )
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
      
      if (nrow(timeSeries) > 0) {
        writeToCsv(
          data = timeSeries,
          fileName = file.path(exportFolder, "time_series.csv"),
          incremental = incremental,
          cohortId = c(subset$cohortId) %>% unique()
        )
      } else {
        warning('No time series data')
      }
      recordTasksDone(
        cohortId = subset$targetCohortId,
        comparatorId = subset$comparatorCohortId,
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
  
  browser()
  # Cohort Relationship ----
  if (runCohortRelationship) {
    ParallelLogger::logInfo("Computing Cohort Relationship")
    startCohortRelationship <- Sys.time()
    
    combis <- cohorts  %>%
      dplyr::filter(.data$cohortId %in% instantiatedCohorts) %>%
      dplyr::select(.data$cohortId) %>%
      dplyr::distinct()
    combis <- tidyr::crossing(combis %>% dplyr::rename("targetCohortId" = .data$cohortId),
                              combis %>% dplyr::rename("comparatorCohortId" = .data$cohortId)) %>%
      dplyr::filter(.data$targetCohortId != .data$comparatorCohortId) %>%
      dplyr::select(.data$targetCohortId, .data$comparatorCohortId) %>%
      dplyr::distinct()
    
    subset <- subsetToRequiredCombis(
      combis = combis,
      task = "runCohortRelationship",
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
      ParallelLogger::logTrace("Beginning Cohort Relationship SQL")
      cohortOverlap <- computeCohortTemporalRelationship(
        connection = connection,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        targetCohortIds = subset$targetCohortId,
        comparatorCohortIds = subset$comparatorCohortId
      )
      cohortRelationships <- cohortRelationships %>% 
        dplyr::rename(countValue = .data$value) %>% 
        dplyr::mutate(startDay = as.numeric(.data$attributeName)*30) %>% 
        dplyr::mutate(endDay = (as.numeric(.data$attributeName)*30) + 29)  %>%
        dplyr::mutate(databaseId = !!databaseId) %>% 
        dplyr::select(.data$databaseId,
                      .data$cohortId, 
                      .data$comparatorCohortId,
                      .data$startDay,
                      .data$endDay,
                      .data$countValue) %>% 
        dplyr::arrange(.data$cohortId, 
                       .data$comparatorCohortId,
                       .data$startDay,
                       .data$endDay,
                       .data$countValue)
    }
    if (nrow(cohortRelationships) > 0) {
      writeToCsv(
        data = cohortRelationships,
        fileName = file.path(exportFolder, "cohort_relationships.csv"),
        incremental = incremental,
        cohortId = subset$targetCohortId,
        comparatorCohortId = subset$comparatorCohortId
      )
    } else {
      warning('No cohort relationship data')
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
        getCohortCharacteristics(
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
        getCohortCharacteristics(
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
        "vocabularyVersion"
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
        as.character(if (!getPackageName() == ".GlobalEnv") {packageVersion(packageName())} else {''}),
        as.character(as.numeric(x = delta, units = attr(delta, "units"))),
        as.character(attr(delta, "units")),
        as.character(nullToEmpty(cdmSourceInformation$sourceDescription)),
        as.character(nullToEmpty(cdmSourceInformation$cdmSourceName)),
        as.character(nullToEmpty(cdmSourceInformation$sourceReleaseDate)),
        as.character(nullToEmpty(cdmSourceInformation$cdmVersion)),
        as.character(nullToEmpty(cdmSourceInformation$cdmReleaseDate)),
        as.character(nullToEmpty(cdmSourceInformation$vocabularyVersion))
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
