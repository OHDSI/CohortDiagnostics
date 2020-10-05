# Copyright 2020 Observational Health Data Sciences and Informatics
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
#' @template CohortDatabaseSchema
#' @template OracleTempSchema
#'
#' @template CohortTable
#'
#' @template CohortSetSpecs
#' 
#' @template CohortSetReference
#' @param phenotypeDescriptionFile    (Optional) The location of the phenotype descriptioon file within the package. 
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
#' @param databaseName                The full name of the database.
#' @param databaseDescription         A short description (several sentences) of the database.
#' @template cdmVersion
#' @param runInclusionStatistics      Generate and export statistic on the cohort inclusion rules?
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runTimeDistributions        Generate and export cohort time distributions?
#' @param runVisitContext             Generate and export index-date visit context?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#' @param runIncidenceRate            Generate and export the cohort incidence  rates?
#' @param runCohortOverlap            Generate and export the cohort overlap? Overlaps are checked within cohortIds
#'                                    that have the same referrent conceptId sourced from the CohortSetReference or 
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
                                 cohortDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortIds = NULL,
                                 inclusionStatisticsFolder = file.path(exportFolder, "inclusionStatistics"),
                                 exportFolder,
                                 databaseId,
                                 databaseName = NULL,
                                 databaseDescription = NULL,
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
                                     temporalStartDays = c(-365,-30,0,1,31), 
                                     temporalEndDays = c(-31,-1,0,30,365)),
                                 minCellCount = 5,
                                 incremental = FALSE,
                                 incrementalFolder = file.path(exportFolder, "incremental")) {
  
  start <- Sys.time()
  ParallelLogger::logInfo("Run Cohort Diagnostics started at ", start)
  
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(runInclusionStatistics, add = errorMessage)
  checkmate::assertLogical(runIncludedSourceConcepts, add = errorMessage)
  checkmate::assertLogical(runOrphanConcepts, add = errorMessage)
  checkmate::assertLogical(runTimeDistributions, add = errorMessage)
  checkmate::assertLogical(runBreakdownIndexEvents, add = errorMessage)
  checkmate::assertLogical(runIncidenceRate, add = errorMessage)
  checkmate::assertLogical(runCohortOverlap, add = errorMessage)
  checkmate::assertLogical(runCohortCharacterization, add = errorMessage)
  checkmate::assertInt(x = cdmVersion, na.ok = FALSE, lower = 5, upper = 5, null.ok = FALSE, add = errorMessage)
  minCellCount <- utils::type.convert(minCellCount)
  checkmate::assertInteger(x = minCellCount, lower = 0, add = errorMessage)
  checkmate::assertLogical(incremental, add = errorMessage)
  
  if (any(runInclusionStatistics, runIncludedSourceConcepts, runOrphanConcepts, 
          runTimeDistributions, runBreakdownIndexEvents, runIncidenceRate,
          runCohortOverlap, runCohortCharacterization)) {
    checkmate::assertCharacter(x = cdmDatabaseSchema, min.len = 1, add = errorMessage)
    checkmate::assertCharacter(x = cohortDatabaseSchema, min.len = 1, add = errorMessage)
    checkmate::assertCharacter(x = cohortTable, min.len = 1, add = errorMessage)
    checkmate::assertCharacter(x = databaseId, min.len = 1, add = errorMessage)
    checkmate::assertCharacter(x = databaseDescription, min.len = 1, add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  errorMessage <- createIfNotExist(type = "folder", name = exportFolder, errorMessage = errorMessage)
  if (incremental) {
    errorMessage <- createIfNotExist(type = "folder", name = incrementalFolder, errorMessage = errorMessage)
  }
  if (isTRUE(runInclusionStatistics)) {
    errorMessage <- createIfNotExist(type = "folder", name = inclusionStatisticsFolder, errorMessage = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  cohorts <- getCohortsJsonAndSql(packageName = packageName,
                                  cohortToCreateFile = cohortToCreateFile,
                                  baseUrl = baseUrl,
                                  cohortSetReference = cohortSetReference,
                                  cohortIds = cohortIds)
  
  if (!is.null(phenotypeDescriptionFile)) {
    phenotypeDescription <- loadAndExportPhenotypeDescription(packageName = packageName,
                                                              phenotypeDescriptionFile = phenotypeDescriptionFile,
                                                              exportFolder = exportFolder,
                                                              cohorts = cohorts,
                                                              errorMessage = errorMessage)
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
  writeToCsv(data = cohorts, fileName = file.path(exportFolder, "cohort.csv"))
  if (!"phenotypeId" %in% colnames(cohorts)) {
    cohorts$phenotypeId <- NA
  }
  
  ##############################
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }
  
  ##############################
  ParallelLogger::logInfo("Saving database metadata")
  startMetaData <- Sys.time()
  database <- dplyr::tibble(databaseId = databaseId,
                            databaseName = dplyr::coalesce(databaseName,databaseId),
                            description = dplyr::coalesce(databaseDescription,databaseId),
                            isMetaAnalysis = 0)
  writeToCsv(data = database, 
             fileName = file.path(exportFolder, "database.csv"))
  delta <- Sys.time() - startMetaData
  writeLines(paste("Saving database metadata took", signif(delta, 3), attr(delta, "units")))
  
  ##############################
  ParallelLogger::logTrace("Creating unique concept ID table")
  sql <- SqlRender::loadRenderTranslateSql("CreateConceptIdTable.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           table_name = "#concept_ids")
  DatabaseConnector::executeSql(connection = connection, sql = sql, progressBar = FALSE, reportOverallTime = FALSE)
  if (!is.null(phenotypeDescription)) {
    data <- phenotypeDescription %>% 
      dplyr::filter(!is.na(.data$referentConceptId)) %>%
      dplyr::transmute(conceptId = as.integer(.data$referentConceptId)) %>% 
      dplyr::distinct() %>%
      as.data.frame() #DatabaseConnector currently does not support tibble
    if (nrow(data) > 0) {
      ParallelLogger::logInfo(sprintf("Inserting %s referent concept IDs into the concept ID table. This may take a while.",
                                      nrow(data)))
      DatabaseConnector::insertTable(connection = connection, 
                                     tableName = "#concept_ids",
                                     data = data,
                                     dropTableIfExists = FALSE,
                                     createTable = FALSE, 
                                     progressBar = TRUE,
                                     tempTable = TRUE,
                                     oracleTempSchema = oracleTempSchema,
                                     camelCaseToSnakeCase = TRUE)
      ParallelLogger::logTrace("Done inserting")
    }
  }
  
  ##############################
  recordCountOfInstantiatedCohorts <- getRecordCountOfInstantiatedCohorts(connection = connection,
                                                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                                                          cohortTable = cohortTable, 
                                                                          cohortIds = cohorts$cohortId)
  if (nrow(recordCountOfInstantiatedCohorts %>% 
           dplyr::filter(.data$count > 0)) > 0) {
    instantiatedCohorts <- recordCountOfInstantiatedCohorts %>% 
      dplyr::filter(.data$count > 0) %>% 
      dplyr::pull(.data$cohortId)
    ParallelLogger::logInfo(sprintf("Found %s of %s (%1.2f%%) submitted cohorts instantiated. ", 
                                    length(instantiatedCohorts), 
                                    nrow(cohorts),
                                    100*(length(instantiatedCohorts)/nrow(cohorts))),
                            "Beginning cohort diagnostics for instantiated cohorts. ")
  } else {
    stop("All cohorts were either not instantiated or all have 0 records.")
  }
  
  ##############################
  if (incremental) {
    ParallelLogger::logDebug("Working in incremental mode.")
    cohorts$checksum <- computeChecksum(cohorts$sql)
    recordKeepingFile <- file.path(incrementalFolder, "CreatedDiagnostics.csv")
    if (file.exists(path = recordKeepingFile)) {
      ParallelLogger::logInfo("Found existing record keeping file in incremental folder - CreatedDiagnostics.csv")
    }
  }
  ##############################
  # Counting cohorts -----------------------------------------------------------------------
  ParallelLogger::logInfo("Counting cohort records and subjects")
  subset <- subsetToRequiredCohorts(cohorts = cohorts %>% 
                                      dplyr::filter(.data$cohortId %in% instantiatedCohorts), 
                                    task = "getCohortCounts", 
                                    incremental = incremental, 
                                    recordKeepingFile = recordKeepingFile)
  if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
    ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", 
                                    length(instantiatedCohorts) - nrow(subset)))
  }
  if (nrow(subset) > 0) {
    counts <- getCohortCounts(connection = connection,
                              cohortDatabaseSchema = cohortDatabaseSchema,
                              cohortTable = cohortTable,
                              cohortIds = subset$cohortId)
    
    if (nrow(counts) > 0) {
      counts <- counts %>% dplyr::mutate(databaseId = !!databaseId)
      counts <- enforceMinCellValue(data = counts, fieldName = "cohortEntries", minValues = minCellCount)
      counts <- enforceMinCellValue(data = counts, fieldName = "cohortSubjects", minValues = minCellCount)
    }
    writeToCsv(data = counts, 
               fileName = file.path(exportFolder, "cohort_count.csv"), 
               incremental = incremental, 
               cohortId = subset$cohortId)
    recordTasksDone(cohortId = subset$cohortId,
                    task = "getCohortCounts",
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
  }
  # Get all counts, not just those we didn't count before:
  counts <- readr::read_csv(file = file.path(exportFolder, "cohort_count.csv"), 
                            col_types = readr::cols(),
                            guess_max = min(1e7))
  names(counts) <- SqlRender::snakeCaseToCamelCase(names(counts))

  # Inclusion statistics -----------------------------------------------------------------------
  if (runInclusionStatistics) {
    ParallelLogger::logInfo("Fetching inclusion statistics from files")
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts), 
                                      task = "runInclusionStatistics", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    
    if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", 
                                      length(instantiatedCohorts) - nrow(subset)))
    }
    if (nrow(subset) > 0) {
      stats <- getInclusionStatisticsFromFiles(cohortIds = subset$cohortId,
                                               folder = inclusionStatisticsFolder,
                                               simplify = TRUE)
      if (nrow(stats) > 0) {
        stats <- stats %>% 
          dplyr::mutate(databaseId = !!databaseId)
        stats <- enforceMinCellValue(data = stats, fieldName = "meetSubjects", minValues = minCellCount)
        stats <- enforceMinCellValue(data = stats, fieldName = "gainSubjects", minValues = minCellCount)
        stats <- enforceMinCellValue(data = stats, fieldName = "totalSubjects", minValues = minCellCount)
        stats <- enforceMinCellValue(data = stats, fieldName = "remainSubjects", minValues = minCellCount)
      }
      if ("cohortDefinitionId" %in% (colnames(stats))) {
        stats <- stats %>% 
          dplyr::rename(cohortId = .data$cohortDefinitionId)
      }
      colnames(stats) <- SqlRender::camelCaseToSnakeCase(colnames(stats))
      writeToCsv(data = stats, 
                 fileName = file.path(exportFolder, "inclusion_rule_stats.csv"), 
                 incremental = incremental, 
                 cohortId = subset$cohortId)
      recordTasksDone(cohortId = subset$cohortId,
                      task = "runInclusionStatistics",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
  }
  
  # Concept set diagnostics -----------------------------------------------
  if (runIncludedSourceConcepts || runOrphanConcepts || runBreakdownIndexEvents) {
    runConceptSetDiagnostics(connection = connection,
                             oracleTempSchema = oracleTempSchema,
                             cdmDatabaseSchema = cdmDatabaseSchema,
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
                             recordKeepingFile = recordKeepingFile)
  }
  
  # Time distributions ----------------------------------------------------------------------
  if (runTimeDistributions) {
    ParallelLogger::logInfo("Creating time distributions")
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
                                      task = "runTimeDistributions",
                                      incremental = incremental,
                                      recordKeepingFile = recordKeepingFile)
    
    if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", 
                                      length(instantiatedCohorts) - nrow(subset)))
    }
    if (nrow(subset) > 0) {
      data <- getTimeDistributions(connection = connection,
                                   oracleTempSchema = oracleTempSchema,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                   cohortTable = cohortTable,
                                   cdmVersion = cdmVersion,
                                   cohortIds = subset$cohortId)
      if (nrow(data) > 0) {
        data <- data %>%
          dplyr::mutate(databaseId = !!databaseId)
        writeToCsv(data = data,
                   fileName = file.path(exportFolder, "time_distribution.csv"),
                   incremental = incremental,
                   cohortId = subset$cohortId)
      }
      recordTasksDone(cohortId = subset$cohortId,
                      task = "runTimeDistributions",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
  }
  
  # Visit context ----------------------------------------------------------------------------
  if (runVisitContext) {
    ParallelLogger::logInfo("Retrieving visit context for index dates")
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
                                      task = "runVisitContext",
                                      incremental = incremental,
                                      recordKeepingFile = recordKeepingFile)
    
    if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", 
                                      length(instantiatedCohorts) - nrow(subset)))
    }
    if (nrow(subset) > 0) {
      data <- getVisitContext(connection = connection,
                              oracleTempSchema = oracleTempSchema,
                              cdmDatabaseSchema = cdmDatabaseSchema,
                              cohortDatabaseSchema = cohortDatabaseSchema,
                              cohortTable = cohortTable,
                              cdmVersion = cdmVersion,
                              cohortIds = subset$cohortId,
                              conceptIdTable = "#concept_ids")
      if (nrow(data) > 0) {
        data <- data %>%
          dplyr::mutate(databaseId = !!databaseId)
        data <- enforceMinCellValue(data, "subjects", minCellCount)
        writeToCsv(data = data,
                   fileName = file.path(exportFolder, "visit_context.csv"),
                   incremental = incremental,
                   cohortId = subset$cohortId)
      }
      recordTasksDone(cohortId = subset$cohortId,
                      task = "runVisitContext",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
  }
  
  # Incidence rates --------------------------------------------------------------------------------------
  if (runIncidenceRate) {
    ParallelLogger::logInfo("Computing incidence rates")
    startIncidenceRate <- Sys.time()
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts), 
                                      task = "runIncidenceRate", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    
    if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", 
                                      length(instantiatedCohorts) - nrow(subset)))
    }
    if (nrow(subset) > 0) {
      runIncidenceRate <- function(row) {
        ParallelLogger::logInfo("  Computing incidence rate for cohort '", row$cohortName, "'")
        cohortExpression <- RJSONIO::fromJSON(row$json)
        washoutPeriod <- tryCatch({
          cohortExpression$PrimaryCriteria$ObservationWindow$PriorDays
        }, error = function(e) {
          0
        })
        data <- getIncidenceRate(connection = connection,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 oracleTempSchema = oracleTempSchema,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 cohortId = row$cohortId,
                                 firstOccurrenceOnly = TRUE,
                                 washoutPeriod = washoutPeriod)
        if (nrow(data) > 0) {
          data <- data %>% dplyr::mutate(cohortId = row$cohortId)
        }
        return(data)
      }
      data <- lapply(split(subset, subset$cohortId), runIncidenceRate)
      data <- dplyr::bind_rows(data)
      if (nrow(data) > 0) {
        data <- data %>% dplyr::mutate(databaseId = !!databaseId)
        data <- enforceMinCellValue(data, "cohortCount", minCellCount)
        data <- enforceMinCellValue(data, "incidenceRate", 1000*minCellCount/data$personYears)
      }
      writeToCsv(data = data,
                 fileName = file.path(exportFolder, "incidence_rate.csv"), 
                 incremental = incremental, 
                 cohortId = subset$cohortId)
    }
    recordTasksDone(cohortId = subset$cohortId,
                    task = "runIncidenceRate",
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
    delta <- Sys.time() - startIncidenceRate
    ParallelLogger::logInfo("Running Incidence Rate took ", signif(delta, 3), " ", attr(delta, "units"))
  }
  
  # Cohort overlap ---------------------------------------------------------------------------------
  if (runCohortOverlap) {
    ParallelLogger::logInfo("Computing cohort overlap")
    startCohortOverlap <- Sys.time()
    
    combis <- cohorts %>% 
      dplyr::select(.data$phenotypeId, .data$cohortId) %>% 
      dplyr::rename(targetCohortId = .data$cohortId) %>% 
      dplyr::inner_join(cohorts %>% 
                          dplyr::select(.data$phenotypeId, .data$cohortId) %>% 
                          dplyr::rename(comparatorCohortId = .data$cohortId),
                        by = "phenotypeId") %>% 
      dplyr::filter(.data$targetCohortId < .data$comparatorCohortId) %>% 
      dplyr::select(.data$targetCohortId, .data$comparatorCohortId) %>% 
      dplyr::distinct()
    
    if (incremental) {
      combis <- combis %>% 
        dplyr::inner_join(dplyr::tibble(targetCohortId = cohorts$cohortId, 
                                        targetChecksum = cohorts$checksum),
                          by = "targetCohortId") %>% 
        dplyr::inner_join(dplyr::tibble(comparatorCohortId = cohorts$cohortId, 
                                        comparatorChecksum = cohorts$checksum),
                          by = "comparatorCohortId") %>% 
        dplyr::mutate(checksum = paste(.data$targetChecksum, .data$comparatorChecksum))
    }
    subset <- subsetToRequiredCombis(combis = combis, 
                                     task = "runCohortOverlap", 
                                     incremental = incremental, 
                                     recordKeepingFile = recordKeepingFile)
    
    if (incremental && (length(combis) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohort combinations in incremental mode.", 
                                      length(combis) - nrow(subset)))
    }
    if (nrow(subset) > 0) {
      runCohortOverlap <- function(row) {
        ParallelLogger::logInfo("- Computing overlap for cohorts ",
                                row$targetCohortId,
                                " and ",
                                row$comparatorCohortId)
        data <- computeCohortOverlap(connection = connection,
                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                     cohortTable = cohortTable,
                                     targetCohortId = row$targetCohortId,
                                     comparatorCohortId = row$comparatorCohortId)
        if (nrow(data) > 0) {
          data <- data %>% 
            dplyr::mutate(targetCohortId = row$targetCohortId,
                          comparatorCohortId = row$comparatorCohortId)
        }
        return(data)
      }
      data <- lapply(split(subset, 1:nrow(subset)), runCohortOverlap)
      data <- dplyr::bind_rows(data)
      if (nrow(data) > 0) {
        revData <- data
        revData <- swapColumnContents(revData, "targetCohortId", "comparatorCohortId")
        revData <- swapColumnContents(revData, "tOnlySubjects", "cOnlySubjects")
        revData <- swapColumnContents(revData, "tBeforeCSubjects", "cBeforeTSubjects")
        revData <- swapColumnContents(revData, "tInCSubjects", "cInTSubjects")
        data <- dplyr::bind_rows(data, revData) %>% 
          dplyr::mutate(databaseId = !!databaseId)
        data <- enforceMinCellValue(data, "eitherSubjects", minCellCount)
        data <- enforceMinCellValue(data, "bothSubjects", minCellCount)
        data <- enforceMinCellValue(data, "tOnlySubjects", minCellCount)
        data <- enforceMinCellValue(data, "cOnlySubjects", minCellCount)
        data <- enforceMinCellValue(data, "tBeforeCSubjects", minCellCount)
        data <- enforceMinCellValue(data, "cBeforeTSubjects", minCellCount)
        data <- enforceMinCellValue(data, "sameDaySubjects", minCellCount)
        data <- enforceMinCellValue(data, "tInCSubjects", minCellCount)
        data <- enforceMinCellValue(data, "cInTSubjects", minCellCount)
        data <- data %>% 
          dplyr::mutate(dplyr::across(.cols = everything(), ~tidyr::replace_na(data = ., replace = 0)))
        
        writeToCsv(data = data, 
                   fileName = file.path(exportFolder, "cohort_overlap.csv"), 
                   incremental = incremental, 
                   targetCohortId = subset$targetCohortId, 
                   comparatorCohortId = subset$comparatorCohortId)
      }
      recordTasksDone(cohortId = subset$targetCohortId,
                      comparatorId = subset$comparatorCohortId,
                      task = "runCohortOverlap",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
    
    delta <- Sys.time() - startCohortOverlap
    ParallelLogger::logInfo("Running Cohort Overlap took ", signif(delta, 3), " ", attr(delta, "units"))
  }
  
  # Cohort characterization ---------------------------------------------------------------
  if (runCohortCharacterization) {
    ParallelLogger::logInfo("Characterizing cohorts")
    startCohortCharacterization <- Sys.time()
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts), 
                                      task = "runCohortCharacterization", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    
    if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", 
                                      length(instantiatedCohorts) - nrow(subset)))
    }
    if (nrow(subset) > 0) {  
      ParallelLogger::logInfo(sprintf("Starting large scale characterization of %s cohort(s)", nrow(subset)))
      characteristics <- getCohortCharacteristics(connection = connection,
                                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                                  oracleTempSchema = oracleTempSchema,
                                                  cohortDatabaseSchema = cohortDatabaseSchema,
                                                  cohortTable = cohortTable,
                                                  cohortIds = subset$cohortId,
                                                  covariateSettings = covariateSettings,
                                                  cdmVersion = cdmVersion)
      exportCharacterization(characteristics = characteristics,
                             databaseId = databaseId,
                             incremental = incremental,
                             covariateValueFileName = file.path(exportFolder, "covariate_value.csv"),
                             covariateRefFileName = file.path(exportFolder, "covariate_ref.csv"),
                             analysisRefFileName = file.path(exportFolder, "analysis_ref.csv"),
                             counts = counts,
                             minCellCount = minCellCount)
    } 
    recordTasksDone(cohortId = subset$cohortId,
                    task = "runCohortCharacterization",
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
    delta <- Sys.time() - startCohortCharacterization
    ParallelLogger::logInfo("Running Characterization took ", signif(delta, 3), " ", attr(delta, "units"))
  }
  
  # Temporal Cohort characterization ---------------------------------------------------------------
  if (runTemporalCohortCharacterization) {
    ParallelLogger::logInfo("Temporal Cohort characterization")
    startTemporalCohortCharacterization <- Sys.time()
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts), 
                                      task = "runTemporalCohortCharacterization", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    
    if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.", 
                                      length(instantiatedCohorts) - nrow(subset)))
    }
    if (nrow(subset) > 0) {  
      ParallelLogger::logInfo(sprintf("Starting large scale temporal characterization of %s cohort(s)", nrow(subset)) )
      characteristics <- getCohortCharacteristics(connection = connection,
                                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                                  oracleTempSchema = oracleTempSchema,
                                                  cohortDatabaseSchema = cohortDatabaseSchema,
                                                  cohortTable = cohortTable,
                                                  cohortIds = subset$cohortId,
                                                  covariateSettings = temporalCovariateSettings,
                                                  cdmVersion = cdmVersion)
      exportCharacterization(characteristics = characteristics,
                             databaseId = databaseId,
                             incremental = incremental,
                             covariateValueFileName = file.path(exportFolder, "temporal_covariate_value.csv"),
                             covariateRefFileName = file.path(exportFolder, "temporal_covariate_ref.csv"),
                             analysisRefFileName = file.path(exportFolder, "temporal_analysis_ref.csv"),
                             timeRefFileName = file.path(exportFolder, "temporal_time_ref.csv"),
                             counts = counts,
                             minCellCount = minCellCount)
    } 
    recordTasksDone(cohortId = subset$cohortId,
                    task = "runTemporalCohortCharacterization",
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
    delta <- Sys.time() - startTemporalCohortCharacterization
    ParallelLogger::logInfo("Running Temporal Characterization took ", signif(delta, 3), " ", attr(delta, "units"))
  }
  
  # Store information from the vocabulary on the concepts used -------------------------
  ParallelLogger::logInfo("Retrieving concept information")
  exportConceptInformation(connection = connection,
                           cdmDatabaseSchema = cdmDatabaseSchema,
                           oracleTempSchema = oracleTempSchema,
                           conceptIdTable = "#concept_ids",
                           incremental = incremental,
                           exportFolder = exportFolder)
  
  # Delete unique concept ID table ---------------------------------
  ParallelLogger::logTrace("Deleting concept ID table")
  sql <- "TRUNCATE TABLE @table;\nDROP TABLE @table;"
  DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                               sql = sql,
                                               oracleTempSchema = oracleTempSchema,
                                               table = "#concept_ids",
                                               progressBar = FALSE,
                                               reportOverallTime = FALSE)
  
  # Add all to zip file -------------------------------------------------------------------------------
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <- file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at: ", zipName)
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo("Computing all diagnostics took ", signif(delta, 3), " ", attr(delta, "units"))
}


loadAndExportPhenotypeDescription <- function(packageName,
                                              phenotypeDescriptionFile,
                                              exportFolder,
                                              cohorts, 
                                              errorMessage = NULL) {
  if (is.null(errorMessage)) {
    errorMessage <- checkmate::makeAssertCollection(errorMessage)
  }
  pathToCsv <- system.file(phenotypeDescriptionFile, package = packageName)
  if (file.exists(pathToCsv)) {
    ParallelLogger::logInfo("Found phenotype description file. Loading.")
    
    guessedEncoding <- readr::guess_encoding(file = pathToCsv, 
                                             n_max = min(1e7)) %>% 
      dplyr::mutate(message = paste0(.data$encoding, " (", scales::percent(.data$confidence), ")"))
    
    encodingMessage <- paste0("Please check the encoding of the cohorts to create file at:", 
                              pathToCsv,
                              ".\nExpecting either 'ASCII' or 'UTF-8'. Found\n  ",
                              paste0(guessedEncoding$message, collapse = "\n  "))
    
    if (nrow(guessedEncoding %>% 
             dplyr::filter(.data$confidence == 1,
                           .data$encoding %in% c('ASCII', 'UTF-8'))) == 0) {
      ParallelLogger::logError(encodingMessage)
      stop()
    }
    
    phenotypeDescription <- readr::read_csv(file = pathToCsv, 
                                            col_types = readr::cols(),
                                            na = 'NA',
                                            guess_max = min(1e7)) %>% 
      dplyr::arrange(.data$phenotypeName, .data$phenotypeId)
    
    checkmate::assertTibble(x = phenotypeDescription, 
                            any.missing = TRUE, 
                            min.rows = 1, 
                            min.cols = 6, 
                            add = errorMessage)
    checkmate::assertNames(x = colnames(phenotypeDescription),
                           must.include = c("phenotypeId", "phenotypeName",
                                            "referentConceptId", "clinicalDescription",
                                            "literatureReview", "phenotypeNotes"),
                           add = errorMessage)
    checkmate::reportAssertions(collection = errorMessage)
    
    phenotypeDescription <- phenotypeDescription %>% 
      dplyr::mutate(phenotypeName = dplyr::coalesce(as.character(.data$phenotypeName),""),
                    clinicalDescription = dplyr::coalesce(as.character(.data$clinicalDescription),""),
                    literatureReview = dplyr::coalesce(as.character(.data$literatureReview),""),
                    phenotypeNotes = dplyr::coalesce(as.character(.data$phenotypeNotes),"")
      )  
    checkmate::assertTibble(x = phenotypeDescription,
                            types = c("double", "character"))
    checkmate::reportAssertions(collection = errorMessage)
    
    ParallelLogger::logInfo(sprintf("Phenotype description file has %s rows. Matching with submitted cohorts", 
                                    nrow(phenotypeDescription)))
    
    phenotypeDescription <- phenotypeDescription %>% 
      dplyr::filter(.data$phenotypeId %in% unique(cohorts$phenotypeId))

    ParallelLogger::logInfo(sprintf("%s rows matched", nrow(phenotypeDescription)))
    
    if (nrow(phenotypeDescription) > 0) {
      writeToCsv(phenotypeDescription, file.path(exportFolder, "phenotype_description.csv"))
    } else {
      warning("Phentoype description csv file found, but records dont match the referent concept ids of the cohorts being diagnosed.")
    }
    return(phenotypeDescription)
  } else {
    warning("Phentoype description file not found")
    return(NULL)
  }
}

exportCharacterization <- function(characteristics,
                                   databaseId,
                                   incremental,
                                   covariateValueFileName,
                                   covariateRefFileName,
                                   analysisRefFileName,
                                   timeRefFileName = NULL,
                                   counts,
                                   minCellCount) {
    if (!"covariates" %in% names(characteristics)) {
    warning("No characterization output for submitted cohorts")
  } else if (dplyr::pull(dplyr::count(characteristics$covariateRef)) > 0) {
    characteristics$filteredCovariates <- characteristics$covariates %>% 
      dplyr::filter(mean >= 0.0001) %>% 
      dplyr::mutate(databaseId = !!databaseId) %>% 
      dplyr::left_join(counts, by = c("cohortId", "databaseId"), copy = TRUE) %>%
      dplyr::mutate(mean = dplyr::case_when(.data$mean != 0 & .data$mean < minCellCount / .data$cohortEntries ~ -minCellCount / .data$cohortEntries, 
                                            TRUE ~ .data$mean)) %>%
      dplyr::mutate(sd = dplyr::case_when(.data$mean >= 0 ~ sd)) %>% 
      dplyr::mutate(mean = round(.data$mean, digits = 4),
                    sd = round(.data$sd, digits = 4)) %>%
      dplyr::select(-.data$cohortEntries, -.data$cohortSubjects)
    
    if (dplyr::pull(dplyr::count(characteristics$filteredCovariates)) > 0) {
      covariateRef <- dplyr::collect(characteristics$covariateRef)
      writeToCsv(data = covariateRef,
                 fileName = covariateRefFileName,
                 incremental = incremental,
                 covariateId = covariateRef$covariateId)
      analysisRef <- dplyr::collect(characteristics$analysisRef)
      writeToCsv(data = analysisRef,
                 fileName = analysisRefFileName,
                 incremental = incremental,
                 analysisId = analysisRef$analysisId)
      if (!is.null(timeRefFileName)) {
        timeRef <- dplyr::collect(characteristics$timeRef)
        writeToCsv(data = timeRef,
                   fileName = timeRefFileName,
                   incremental = incremental,
                   analysisId = timeRef$timeId)
      }
      writeCovariateDataAndromedaToCsv(data = characteristics$filteredCovariates, 
                                       fileName = covariateValueFileName, 
                                       incremental = incremental)
    }
  } 
}
