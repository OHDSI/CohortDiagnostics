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
#' @param phenotypeDescriptionFile    (Optional) The location of the phenotype description file within the package.
#'                                    The file must be .csv file and have the following columns that may be read into following
#'                                    data types: phenotypeId (double), phenotypeName (character),
#'                                    referentConceptId (double), clinicalDescription (character),
#'                                    literatureReview (character), phenotypeNotes (character). Note: the field
#'                                    names are in snake_case. Also, character fields should not have 'NA' in it.
#'                                    'NA's are commonly added by R when R functions exports data from dataframe
#'                                    into CSV. Instead please use '' (empty string) to represent absence of data.
#'                                    The literature_review field is expected to be a html link to page that contains
#'                                    resources related to literature review for the phenotype, and will be used
#'                                    to create link-out object. This csv file is expected to be encoded in either
#'                                    ASCII or UTF-8, if not, an error message will be displayed and process stopped.
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
#' @export
runCohortDiagnostics <- function(packageName = NULL,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
                                 baseUrl = NULL,
                                 cohortSetReference = NULL,
                                 phenotypeDescriptionFile = NULL,
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
  }
  if (any(is.null(databaseDescription), is.na(databaseDescription))) {
    databaseDescription <- databaseId
  }
  
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
  
  cohorts <- getCohortsJsonAndSql(
    packageName = packageName,
    cohortToCreateFile = cohortToCreateFile,
    baseUrl = baseUrl,
    cohortSetReference = cohortSetReference,
    cohortIds = cohortIds
  )
  
  if (!is.null(phenotypeDescriptionFile)) {
    phenotypeDescription <-
      loadAndExportPhenotypeDescription(
        packageName = packageName,
        phenotypeDescriptionFile = phenotypeDescriptionFile,
        exportFolder = exportFolder,
        cohorts = cohorts,
        errorMessage = errorMessage
      )
  } else {
    phenotypeDescription <- NULL
  }
  
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
  if ('phenotypeId' %in% expectedButNotObsevered) {
    cohorts$phenotypeId <-
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
    cohorts <- cohorts %>%
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
  
  cohorts <- cohorts %>%
    dplyr::select(cohortTableColumnNamesExpected)
  writeToCsv(data = cohorts,
             fileName = file.path(exportFolder, "cohort.csv"))
  
  if (!"phenotypeId" %in% colnames(cohorts)) {
    cohorts$phenotypeId <- NA
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
  
  vocabularyVersionCdm <- ""
  tryCatch({
    vocabularyVersionCdm <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "select * from @cdm_database_schema.cdm_source;",
        cdm_database_schema = cdmDatabaseSchema,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble()
  }, error = function(...) {
    warning("Problem getting vocabulary version")
    vocabularyVersionCdm <- dplyr::tibble()
    if (connection@dbms == "postgresql") { #this is for test that automated testing purpose
      DatabaseConnector::dbExecute(connection, "ABORT;")
    }
  })
  
  if (all(nrow(vocabularyVersionCdm) > 0,
      'vocabularyVersion' %in% colnames(vocabularyVersionCdm))) {
    vocabularyVersionCdm <- vocabularyVersionCdm %>% 
      dplyr::rename(vocabularyVersionCdm = .data$vocabularyVersion) %>%
      dplyr::pull(vocabularyVersionCdm) %>%
      unique()
  } else {
    vocabularyVersionCdm <<- paste0("v", cdmVersion, ".0 -")
  }
  
  vocabularyVersion <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "select * from @vocabulary_database_schema.vocabulary where vocabulary_id = 'None';",
      vocabulary_database_schema = vocabularyDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble() %>%
    dplyr::rename(vocabularyVersion = .data$vocabularyVersion) %>%
    dplyr::pull(.data$vocabularyVersion) %>%
    unique()
  
  if (incremental) {
    ParallelLogger::logDebug("Working in incremental mode.")
    cohorts$checksum <- computeChecksum(cohorts$sql)
    recordKeepingFile <-
      file.path(incrementalFolder, "CreatedDiagnostics.csv")
    if (file.exists(path = recordKeepingFile)) {
      ParallelLogger::logInfo(
        "Found existing record keeping file in incremental folder - CreatedDiagnostics.csv"
      )
    }
  }
  
  # Database metadata ---------------------------------------------
  ParallelLogger::logInfo("Saving database metadata")
  startMetaData <- Sys.time()
  database <- dplyr::tibble(
    databaseId = databaseId,
    databaseName = dplyr::coalesce(databaseName, databaseId),
    description = dplyr::coalesce(databaseDescription, databaseId),
    vocabularyVersionCdm = !!vocabularyVersionCdm,
    vocabularyVersion = !!vocabularyVersion,
    isMetaAnalysis = 0
  )
  writeToCsv(data = database,
             fileName = file.path(exportFolder, "database.csv"))
  delta <- Sys.time() - startMetaData
  writeLines(paste(
    "Saving database metadata took",
    signif(delta, 3),
    attr(delta, "units")
  ))
  
  # Create concept table ------------------------------------------
  ParallelLogger::logTrace("Creating concept ID table for tracking concepts used in diagnostics")
  sql <-
    SqlRender::loadRenderTranslateSql(
      "CreateConceptIdTable.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      table_name = "#concept_ids"
    )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  referentConceptIdToInsert <- dplyr::tibble()
  if ('referentConceptId' %in% colnames(cohorts)) {
    referentConceptIdToInsert <-
      dplyr::bind_rows(referentConceptIdToInsert,
                       cohorts %>%
                         dplyr::transmute(conceptId = as.double(.data$referentConceptId))) %>%
      dplyr::distinct()
  }
  if (nrow(referentConceptIdToInsert) > 0) {
    ParallelLogger::logInfo(
      sprintf(
        "Inserting %s referent concept IDs into the concept ID table. This may take a while.",
        nrow(referentConceptIdToInsert)
      )
    )
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = "#concept_ids",
      data = referentConceptIdToInsert,
      dropTableIfExists = FALSE,
      createTable = FALSE,
      progressBar = TRUE,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = TRUE
    )
    ParallelLogger::logTrace("Done inserting")
  }
  
  # Counting cohorts -----------------------------------------------------------------------
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
      stop("All cohorts were either not instantiated or all have 0 records.")
    }
  } else {
    stop(
      "Cohort table should be instantiated. If there is no cohort table, there are no cohorts to diagnose."
    )
  }
  
  # Inclusion statistics -----------------------------------------------------------------------
  if (runInclusionStatistics) {
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
        "Skipping %s cohorts in incremental mode.",
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
        if (nrow(stats) > 0) {
          stats <- stats %>%
            dplyr::mutate(databaseId = !!databaseId)
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
        warning("Cohort Inclusion statistics file not found. Inclusion Statistis not run.")
      }
    }
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
      cohorts = cohorts,
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
    ParallelLogger::logInfo("Creating time distributions")
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
        "Skipping %s cohorts in incremental mode.",
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
  }
  
  # Visit context ----------------------------------------------------------------------------
  if (runVisitContext) {
    ParallelLogger::logInfo("Retrieving visit context for index dates")
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
        "Skipping %s cohorts in incremental mode.",
        length(instantiatedCohorts) - nrow(subset)
      ))
    }
    if (nrow(subset) > 0) {
      data <- getVisitContext(
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cdmVersion = cdmVersion,
        cohortIds = subset$cohortId,
        conceptIdTable = "#concept_ids"
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
      }
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runVisitContext",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    }
  }
  
  # Incidence rates --------------------------------------------------------------------------------------
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
        "Skipping %s cohorts in incremental mode.",
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
    }
    recordTasksDone(
      cohortId = subset$cohortId,
      task = "runIncidenceRate",
      checksum = subset$checksum,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
    delta <- Sys.time() - startIncidenceRate
    ParallelLogger::logInfo("Running Incidence Rate took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  # Cohort overlap ---------------------------------------------------------------------------------
  if (runCohortOverlap) {
    ParallelLogger::logInfo("Computing cohort overlap")
    startCohortOverlap <- Sys.time()
    
    combis <- cohorts %>%
      dplyr::select(.data$phenotypeId, .data$cohortId) %>%
      dplyr::distinct()
    
    combis <- combis %>%
      dplyr::rename(targetCohortId = .data$cohortId) %>%
      dplyr::inner_join(combis %>%
                          dplyr::rename(comparatorCohortId = .data$cohortId),
                        by = "phenotypeId") %>%
      dplyr::filter(.data$targetCohortId < .data$comparatorCohortId) %>%
      dplyr::select(.data$targetCohortId, .data$comparatorCohortId) %>%
      dplyr::distinct()
    
    if (incremental) {
      combis <- combis %>%
        dplyr::inner_join(
          dplyr::tibble(
            targetCohortId = cohorts$cohortId,
            targetChecksum = cohorts$checksum
          ),
          by = "targetCohortId"
        ) %>%
        dplyr::inner_join(
          dplyr::tibble(
            comparatorCohortId = cohorts$cohortId,
            comparatorChecksum = cohorts$checksum
          ),
          by = "comparatorCohortId"
        ) %>%
        dplyr::mutate(checksum = paste(.data$targetChecksum, .data$comparatorChecksum))
    }
    subset <- subsetToRequiredCombis(
      combis = combis,
      task = "runCohortOverlap",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    
    if (incremental && (nrow(combis) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf(
        "Skipping %s cohort combinations in incremental mode.",
        nrow(combis) - nrow(subset)
      ))
    }
    if (nrow(subset) > 0) {
      runCohortOverlap <- function(row) {
        ParallelLogger::logInfo(
          "- Computing overlap for cohorts ",
          row$targetCohortId,
          " and ",
          row$comparatorCohortId
        )
        data <- computeCohortOverlap(
          connection = connection,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortTable = cohortTable,
          targetCohortId = row$targetCohortId,
          comparatorCohortId = row$comparatorCohortId
        )
        if (nrow(data) > 0) {
          data <- data %>%
            dplyr::mutate(
              targetCohortId = row$targetCohortId,
              comparatorCohortId = row$comparatorCohortId
            )
        }
        return(data)
      }
      data <-
        lapply(split(subset, 1:nrow(subset)), runCohortOverlap)
      data <- dplyr::bind_rows(data)
      if (nrow(data) > 0) {
        revData <- data
        revData <-
          swapColumnContents(revData, "targetCohortId", "comparatorCohortId")
        revData <-
          swapColumnContents(revData, "tOnlySubjects", "cOnlySubjects")
        revData <-
          swapColumnContents(revData, "tBeforeCSubjects", "cBeforeTSubjects")
        revData <-
          swapColumnContents(revData, "tInCSubjects", "cInTSubjects")
        data <- dplyr::bind_rows(data, revData) %>%
          dplyr::mutate(databaseId = !!databaseId)
        data <-
          enforceMinCellValue(data, "eitherSubjects", minCellCount)
        data <-
          enforceMinCellValue(data, "bothSubjects", minCellCount)
        data <-
          enforceMinCellValue(data, "tOnlySubjects", minCellCount)
        data <-
          enforceMinCellValue(data, "cOnlySubjects", minCellCount)
        data <-
          enforceMinCellValue(data, "tBeforeCSubjects", minCellCount)
        data <-
          enforceMinCellValue(data, "cBeforeTSubjects", minCellCount)
        data <-
          enforceMinCellValue(data, "sameDaySubjects", minCellCount)
        data <-
          enforceMinCellValue(data, "tInCSubjects", minCellCount)
        data <-
          enforceMinCellValue(data, "cInTSubjects", minCellCount)
        data <- data %>%
          dplyr::mutate(dplyr::across(.cols = everything(), ~ tidyr::replace_na(
            data = ., replace = 0
          )))
        
        writeToCsv(
          data = data,
          fileName = file.path(exportFolder, "cohort_overlap.csv"),
          incremental = incremental,
          targetCohortId = subset$targetCohortId,
          comparatorCohortId = subset$comparatorCohortId
        )
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
    ParallelLogger::logInfo("Running Cohort Overlap took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  # Cohort characterization ---------------------------------------------------------------
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
        "Skipping %s cohorts in incremental mode.",
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
  
  # Temporal Cohort characterization ---------------------------------------------------------------
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
        "Skipping %s cohorts in incremental mode.",
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
  
  # Store information from the vocabulary on the concepts used -------------------------
  ParallelLogger::logInfo("Retrieving concept information")
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
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <-
    file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at: ", zipName)
  
  delta <- Sys.time() - start
  metaData <- dplyr::tibble(
    databaseId = databaseId,
    variableField = c('vocabularyVersionCdm', 'vocabularyVersion'),
    valueField = c(vocabularyVersionCdm, vocabularyVersion)
  )
  writeToCsv(data = metaData,
             fileName = "metaData.csv")
  
  ParallelLogger::logInfo("Computing all diagnostics took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
}
