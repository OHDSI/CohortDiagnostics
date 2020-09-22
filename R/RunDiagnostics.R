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
#' @template VocabularyDatabaseSchema
#' @template CohortDatabaseSchema
#' @template OracleTempSchema
#'
#' @template CohortTable
#'
#' @template CohortSetSpecs
#' 
#' @template CohortSetReference
#' @param    phenotypeDescriptionCsv  (Optional) Path for the location of the phenotype_description.csv file.
#'                                    This file should have the following columns that may be read into following
#'                                    data types: phenotype_id (double), phenotype_name (character),
#'                                    referent_concept_id (double), clinical_description (character),
#'                                    literature_review (character), phenotype_notes (character). Note: the field
#'                                    names are in snake_case. Also, character fields should not have 'NA' in it.
#'                                    'NA's are commonly added by R when R functions exports data from dataframe 
#'                                    into CSV. Instead please use '' (empty string) to represent absence of data. 
#'                                    The literature_review field is expected to be a html link to page that contains
#'                                    resources related to literature review for the phenotype, and will be used
#'                                    to create link-out object.
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
#' @param exportConceptCountTableForDatabase Do you want to export concept count table for the database?
#' @param runTimeDistributions        Generate and export cohort time distributions?
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
#' @param runSubsetOmopVocabularyTables Do you want to retrieve a copy of the OMOP vocabulary, but subset to the unique conceptId
#'                                    used in the diagnosis?
#' @param uniqueConceptIdTable        Table to store unique concept_ids.
#' @param minCellCount                The minimum cell count for fields contains person counts or fractions.
#' @param incremental                 Create only cohort diagnostics that haven't been created before?
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are kept
#'                                    of which cohort diagnostics has been executed.
#' @export
runCohortDiagnostics <- function(packageName = NULL,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
                                 baseUrl = NULL,
                                 cohortSetReference = NULL,
                                 phenotypeDescriptionCsv = NULL,
                                 connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 oracleTempSchema = NULL,
                                 cohortDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortIds = NULL,
                                 inclusionStatisticsFolder = file.path(exportFolder, 'inclusionStatistics'),
                                 exportFolder,
                                 databaseId,
                                 databaseName = databaseId,
                                 databaseDescription = "",
                                 cdmVersion = 5,
                                 runInclusionStatistics = TRUE,
                                 runIncludedSourceConcepts = TRUE,
                                 runOrphanConcepts = TRUE,
                                 exportConceptCountTableForDatabase = TRUE,
                                 runTimeDistributions = TRUE,
                                 runBreakdownIndexEvents = TRUE,
                                 runIncidenceRate = TRUE,
                                 runCohortOverlap = TRUE,
                                 runCohortCharacterization = TRUE,
                                 covariateSettings = 
                                   FeatureExtraction::createDefaultCovariateSettings(),
                                 runTemporalCohortCharacterization = TRUE,
                                 temporalCovariateSettings = 
                                   FeatureExtraction::createTemporalCovariateSettings(
                                     useConditionOccurrence = TRUE, 
                                     useDrugEraStart = TRUE, 
                                     useProcedureOccurrence = TRUE, 
                                     useMeasurement = TRUE,                                          
                                     temporalStartDays = c(-365,-30,0,1,31, 
                                                           seq(from = -30, to = -420, by = -30), 
                                                           seq(from = 1, to = 390, by = 30)), 
                                     temporalEndDays = c(-31,-1,0,30,365,
                                                         seq(from = 0, to = -390, by = -30),
                                                         seq(from = 31, to = 420, by = 30))),
                                 runSubsetOmopVocabularyTables = TRUE,
                                 uniqueConceptIdTable = "unique_concept_id_table",
                                 minCellCount = 5,
                                 incremental = FALSE,
                                 incrementalFolder = file.path(exportFolder, 'incremental')) {
  
  start <- Sys.time()
  ParallelLogger::logInfo("\n- Run Cohort Diagnostics started at ", start)
  
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertLogical(runInclusionStatistics, add = errorMessage)
  checkmate::assertLogical(runIncludedSourceConcepts, add = errorMessage)
  checkmate::assertLogical(runOrphanConcepts, add = errorMessage)
  checkmate::assertLogical(runTimeDistributions, add = errorMessage)
  checkmate::assertLogical(runBreakdownIndexEvents, add = errorMessage)
  checkmate::assertLogical(runIncidenceRate, add = errorMessage)
  checkmate::assertLogical(runCohortOverlap, add = errorMessage)
  checkmate::assertLogical(runCohortCharacterization, add = errorMessage)
  checkmate::assertLogical(runSubsetOmopVocabularyTables, add = errorMessage)
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
  if (runSubsetOmopVocabularyTables) {
    checkmate::assertCharacter(x = uniqueConceptIdTable, min.len = 1, add = errorMessage)
  } else {
    uniqueConceptIdTable <- NULL
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  errorMessage <- createIfNotExist(type = 'folder', name = exportFolder, errorMessage = errorMessage)
  errorMessage <- createIfNotExist(type = 'folder', name = incrementalFolder, errorMessage = errorMessage)
  if (isTRUE(runInclusionStatistics)) {
    errorMessage <- createIfNotExist(type = 'folder', name = inclusionStatisticsFolder, errorMessage = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  cohorts <- getCohortsJsonAndSql(packageName = packageName,
                                  cohortToCreateFile = cohortToCreateFile,
                                  baseUrl = baseUrl,
                                  cohortSetReference = cohortSetReference,
                                  cohortIds = cohortIds)
  
  if (!is.null(phenotypeDescriptionCsv)) {
    writePhenotypeDescriptionCsvToResults(phenotypeDescriptionCsvPath = phenotypeDescriptionCsv,
                                          exportFolder = exportFolder,
                                          cohorts = cohorts,
                                          errorMessage)
  }
  
  
  if (nrow(cohorts) == 0) {
    ParallelLogger::logWarn("Terminating cohort diagnostics. No cohorts specified")
    return(NULL)
  }
  writeToCsv(data = cohorts, 
             fileName = file.path(exportFolder, "cohort.csv"))
  
  ##############################
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logWarn("No connection or connectionDetails provided. Diagnostics not run.")
      return(NULL)
    }
  }
  
  ##############################
  ParallelLogger::logInfo("\n- Saving database metadata to results data model")
  database <- tibble::tibble(databaseId = databaseId,
                             databaseName = databaseName,
                             description = databaseDescription,
                             isMetaAnalysis = 0)
  writeToCsv(data = database, 
             fileName = file.path(exportFolder, "database.csv"))
  
  ##############################
  if (runSubsetOmopVocabularyTables) {
    ParallelLogger::logInfo("  Export of OMOP vocabulary tables has been requested. Collecting all unique concept ids in diagnosis.")
    uniqueConceptIdTableExists <- DatabaseConnector::dbExistsTable(conn = connection, 
                                                                   name = uniqueConceptIdTable)
    if (uniqueConceptIdTableExists) {
      ParallelLogger::logInfo("  Unique concept id table: ", uniqueConceptIdTable, " already exists. Table will not be recreated.")
    } else {
      ParallelLogger::logInfo("  Creating unique concept id table: '", uniqueConceptIdTable, "'")
      sql <- SqlRender::loadRenderTranslateSql(
        "CreateUniqueConceptIdTable.sql",
        packageName = "CohortDiagnostics",
        dbms = connection@dbms,
        oracleTempSchema = oracleTempSchema,
        cohort_database_schema = cohortDatabaseSchema,
        unique_concept_id_table = uniqueConceptIdTable
      )
      DatabaseConnector::executeSql(connection = connection, 
                                    sql = sql)
    }
  }
  ##############################
  if (runSubsetOmopVocabularyTables) {
    ParallelLogger::logInfo("  Storing referent_concept_id as concept_id in ", uniqueConceptIdTable)
    data <- cohorts %>% 
      dplyr::select(.data$cohortId, .data$referentConceptId) %>% 
      dplyr::mutate(concept_id = .data$referentConceptId) %>% 
      dplyr::distinct()
    ParallelLogger::logInfo("  Inserting into ", 
                            uniqueConceptIdTable, " ",
                            scales::comma(nrow(data)), 
                            " records. This may take time.")
    DatabaseConnector::insertTable(connection = connection, 
                                   tableName = '#cohort_ref_conc_id',
                                   data = data,
                                   dropTableIfExists = TRUE,
                                   createTable = TRUE, 
                                   progressBar = TRUE,
                                   tempTable = TRUE,
                                   camelCaseToSnakeCase = TRUE)
    ParallelLogger::logInfo("  Done.")
    sql <- "DELETE FROM @cohort_database_schema.@unique_concept_id_table
            WHERE database_id = '@database_id'
            AND cohort_id in (@cohort_ids)
            AND task = '@task';
    
            INSERT INTO @cohort_database_schema.@unique_concept_id_table 
            (database_id, cohort_id, task, concept_id)
            SELECT DISTINCT '@database_id' as database_id,
                   cohort_id,
                   '@task' as task,
                   concept_id
            FROM #cohort_ref_conc_id;
    
            DROP TABLE #cohort_ref_conc_id;"
    DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                 sql = sql,
                                                 cohort_database_schema = cohortDatabaseSchema,
                                                 unique_concept_id_table = uniqueConceptIdTable,
                                                 database_id = databaseId,
                                                 cohort_ids = paste0(data$cohortId %>% unique(), collapse = ","),
                                                 task = 'referentCohortConcpetId'
    )
  }
  
  
  ##############################
  recordCountOfInstantiatedCohorts <- 
    getRecordCountOfInstantiatedCohorts(connection = connection,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTable = cohortTable, 
                                        cohortIds = cohorts$cohortId
    )
  if (nrow(recordCountOfInstantiatedCohorts %>% 
           dplyr::filter(.data$count > 0)) > 0) {
    instantiatedCohorts <- recordCountOfInstantiatedCohorts %>% 
      dplyr::filter(.data$count > 0) %>% 
      dplyr::pull(.data$cohortId)
    ParallelLogger::logInfo("\n- Found ", 
                            scales::comma(length(instantiatedCohorts)), 
                            " of the ",
                            scales::comma(nrow(cohorts), accuracy = 1),
                            " (", 
                            scales::percent(length(instantiatedCohorts)/nrow(cohorts)),
                            ") submitted cohorts instantiated. \n",
                            "  Beginning Cohort Data Diagnostics for instantiated cohorts.")
  } else {
    ParallelLogger::logWarn("\n- All cohorts were either not instantiated or all have 0 records.\n",
                            "  Exiting Cohort Data Diagnostics.")
    return(NULL)
  }
  
  ##############################
  if (incremental) {
    ParallelLogger::logInfo("\n- Working in incremental mode.")
    cohorts$checksum <- computeChecksum(cohorts$sql)
    recordKeepingFile <- file.path(incrementalFolder, "CreatedDiagnostics.csv")
    if (file.exists(path = recordKeepingFile)) {
      ParallelLogger::logInfo("  Found existing record keeping file in incremental folder - CreatedDiagnostics.csv")
    }
  }
  ##############################
  # Counting cohorts -----------------------------------------------------------------------
  ParallelLogger::logInfo("------------------------------------")
  ParallelLogger::logInfo("\n- Getting record and subject counts for instantiated cohorts")
  subset <- subsetToRequiredCohorts(cohorts = cohorts %>% 
                                      dplyr::filter(.data$cohortId %in% instantiatedCohorts), 
                                    task = "getCohortCounts", 
                                    incremental = incremental, 
                                    recordKeepingFile = recordKeepingFile)
  
  if (nrow(subset) > 0) {
    if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo("  Skipping ", 
                              scales::comma(length(instantiatedCohorts) - length(subset), accuracy = 1), 
                              " cohorts in incremental mode.")
    }
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
    ParallelLogger::logInfo("\n")
  }
  
  # Inclusion statistics -----------------------------------------------------------------------
  if (runInclusionStatistics) {
    ParallelLogger::logInfo("------------------------------------")
    ParallelLogger::logInfo("- Fetching inclusion rule statistics. Started at ", Sys.time())
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts), 
                                      task = "runInclusionStatistics", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo("  Skipping ", 
                                scales::comma(length(instantiatedCohorts) - length(subset), accuracy = 1), 
                                " cohorts in incremental mode.")
      }
      runInclusionStatistics <- function(row) {
        #  [OPTIMIZATION idea]  convert to bulk mode, why is reading from file so slow? we can read all cohorts in one read
        ParallelLogger::logInfo("  Fetching inclusion rule statistics for cohort \n     '", row$cohortName, "'")
        stats <- getInclusionStatisticsFromFiles(cohortId = row$cohortId,
                                                 folder = inclusionStatisticsFolder,
                                                 simplify = TRUE)
        if (nrow(stats) > 0) {
          stats$cohortDefinitionId <- row$cohortId
        }
        return(stats)
      }
      stats <- lapply(split(subset, subset$cohortId), runInclusionStatistics)
      stats <- dplyr::bind_rows(stats)
      if (nrow(stats) > 0) {
        stats <- stats %>% 
          dplyr::mutate(databaseId = !!databaseId)
        stats <- enforceMinCellValue(data = stats, fieldName = "personCount", minValues = minCellCount)
        stats <- enforceMinCellValue(data = stats, fieldName = "gainCount", minValues = minCellCount)
        stats <- enforceMinCellValue(data = stats, fieldName = "personTotal", minValues = minCellCount)
        # stats <- enforceMinCellValue(data = stats, fieldName = "remain_subjects", minValues = minCellCount)
      }
      if ('cohortDefinitionId' %in% (colnames(stats))) {
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
    ParallelLogger::logInfo("\n")
  }
  
  if (runIncludedSourceConcepts || runOrphanConcepts) {
    ParallelLogger::logInfo("------------------------------------")
    ParallelLogger::logInfo("- Data diagnostics included source concepts/orphan concepts. Started at ", Sys.time())
    # Concept set diagnostics -----------------------------------------------
    runConceptSetDiagnostics(connection = connection,
                             oracleTempSchema = oracleTempSchema,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             databaseId = databaseId,
                             cohorts = cohorts,
                             runIncludedSourceConcepts = runIncludedSourceConcepts,
                             runOrphanConcepts = runOrphanConcepts,
                             includeSourceConceptTable = '#inc_src_con',
                             orphanConceptTable = '#orphan_concept',
                             exportConceptCountTableForDatabase = exportConceptCountTableForDatabase,
                             exportFolder = exportFolder,
                             minCellCount = minCellCount,
                             conceptCountsDatabaseSchema = NULL,
                             conceptCountsTable = "#concept_counts",
                             conceptCountsTableIsTemp = TRUE,
                             useExternalConceptCountsTable = FALSE,
                             incremental = incremental,
                             uniqueConceptIdTable = uniqueConceptIdTable,
                             recordKeepingFile = recordKeepingFile)
    ParallelLogger::logInfo("\n")
  }
  
  if (runTimeDistributions) {
    startTimeDistribution <- Sys.time()
    # Time distributions ----------------------------------------------------------------------
    ParallelLogger::logInfo("\n- Creating time distributions")
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
                                      task = "runTimeDistributions",
                                      incremental = incremental,
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo("  Skipping ",
                                scales::comma(length(instantiatedCohorts) - length(subset), accuracy = 1),
                                " cohorts in incremental mode.")
      }
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
    delta <- Sys.time() - startTimeDistribution
    ParallelLogger::logInfo(paste("Running time distribution took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    ParallelLogger::logInfo("\n")
  }
  
  # Index event breakdown ---------------------------------------------------------------------
  if (runBreakdownIndexEvents) {
    ParallelLogger::logInfo("------------------------------------")
    startRunBreakdownIndexEvents <- Sys.time()
    ParallelLogger::logInfo("- Breaking down index events")
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts), 
                                      task = "runBreakdownIndexEvents", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo("  Skipping ", 
                                scales::comma(length(instantiatedCohorts) - length(subset), accuracy = 1), 
                                " cohorts in incremental mode.")
      }
      runBreakdownIndexEvents <- function(row) {
        ParallelLogger::logInfo("  Breaking down index events for cohort ", row$cohortName)
        # convert to bulk mode
        data <- breakDownIndexEvents(connection = connection,
                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                     oracleTempSchema = oracleTempSchema,
                                     cohortDatabaseSchema = cohortDatabaseSchema,
                                     cohortTable = cohortTable,
                                     cohortId = row$cohortId,
                                     cohortJson = row$json,
                                     cohortSql = row$sql)
        if (nrow(data) > 0) {
          data$cohortId <- row$cohortId
        }
        return(data)
      }
      data <- lapply(split(subset, subset$cohortId), runBreakdownIndexEvents)
      data <- dplyr::bind_rows(data)
      if (nrow(data) > 0) {
        data <- data %>% 
          dplyr::mutate(databaseId = !!databaseId)
        data <- enforceMinCellValue(data, "conceptCount", minCellCount)
      }
      writeToCsv(data = data, 
                 fileName = file.path(exportFolder, "index_event_breakdown.csv"), 
                 incremental = incremental, 
                 cohortId = subset$cohortId)
      recordTasksDone(cohortId = subset$cohortId,
                      task = "runBreakdownIndexEvents",
                      checksum = subset$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
    }
    
    delta <- Sys.time() - startRunBreakdownIndexEvents
    ParallelLogger::logInfo(paste("  Running index event breakdown took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    ParallelLogger::logInfo("\n")
  }
  
  # Incidence rates --------------------------------------------------------------------------------------
  if (runIncidenceRate) {
    ParallelLogger::logInfo("------------------------------------")
    startIncidenceRate <- Sys.time()
    ParallelLogger::logInfo("- Computing incidence rate")
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts), 
                                      task = "runIncidenceRate", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo("  Skipping ", 
                                scales::comma(length(instantiatedCohorts) - length(subset), accuracy = 1), 
                                " cohorts in incremental mode.")
      }
      runIncidenceRate <- function(row) {
        ParallelLogger::logInfo("  Computing incidence rate for cohort \n    ", row$cohortName)
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
    ParallelLogger::logInfo(paste("Running Incidence Rate took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    ParallelLogger::logInfo("\n")
  }
  
  # Cohort overlap ---------------------------------------------------------------------------------
  if (runCohortOverlap) {
    ParallelLogger::logInfo("------------------------------------")
    startCohortOverlap <- Sys.time()
    ParallelLogger::logInfo("- Computing cohort overlap at ", startCohortOverlap)
    combis <- cohorts %>% 
      dplyr::select(.data$referentConceptId, .data$cohortId) %>% 
      dplyr::rename(targetCohortId = .data$cohortId) %>% 
      dplyr::inner_join(cohorts %>% 
                          dplyr::select(.data$referentConceptId, .data$cohortId) %>% 
                          dplyr::rename(comparatorCohortId = .data$cohortId)) %>% 
      dplyr::filter(.data$targetCohortId < .data$comparatorCohortId,
                    .data$referentConceptId > 0) %>% 
      dplyr::select(.data$targetCohortId, .data$comparatorCohortId) %>% 
      dplyr::distinct()
    
    if (incremental) {
      combis <- combis %>% 
        dplyr::inner_join(tibble::tibble(targetCohortId = cohorts$cohortId, 
                                         targetChecksum = cohorts$checksum)) %>% 
        dplyr::inner_join(tibble::tibble(comparatorCohortId = cohorts$cohortId, 
                                         comparatorChecksum = cohorts$checksum)) %>% 
        dplyr::mutate(checksum = paste(.data$targetChecksum, .data$comparatorChecksum))
    }
    subset <- subsetToRequiredCombis(combis = combis, 
                                     task = "runCohortOverlap", 
                                     incremental = incremental, 
                                     recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo("  Skipping ", 
                                scales::comma(length(instantiatedCohorts) - length(subset), accuracy = 1), 
                                " cohorts in incremental mode.")
      }
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
    ParallelLogger::logInfo(paste("Running Cohort Overlap took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    ParallelLogger::logInfo("\n")
  }
  
  # Cohort characterization ---------------------------------------------------------------
  if (runCohortCharacterization) {
    ParallelLogger::logInfo("------------------------------------")
    startCohortCharacterization <- Sys.time()
    ParallelLogger::logInfo("- Cohort characterizations - started at ", Sys.time())
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts), 
                                      task = "runCohortCharacterization", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo("  Skipping ", 
                                scales::comma(length(instantiatedCohorts) - length(subset), accuracy = 1), 
                                " cohorts in incremental mode.")
      }
      
      ParallelLogger::logInfo(paste0('Starting large scale characterization of ', 
                                     scales::comma(nrow(subset)), 
                                     " cohorts"))
      cohortCharacteristicsOutput <- getCohortCharacteristics(connection = connection,
                                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                                              oracleTempSchema = oracleTempSchema,
                                                              cohortDatabaseSchema = cohortDatabaseSchema,
                                                              cohortTable = cohortTable,
                                                              cohortIds = subset$cohortId,
                                                              covariateSettings = covariateSettings,
                                                              cdmVersion = cdmVersion)
      if (length(cohortCharacteristicsOutput) == 0) {
        ParallelLogger::logWarn("\n   No characterization output for submitted cohorts")
      }
      
      message <- "\n  Characterization results summary:\n"
      message <- c(message, paste0("Number of cohorts submitted for characterization = ", length(subset$cohortId), '\n'))
      message <- c(message, "Following cohorts were submitted: \n")
      message <- c(message, paste0(subset %>% 
                                     dplyr::mutate(message = paste0("      ", subset$cohortName, 
                                                                    " (Cohort Id: ", 
                                                                    subset$cohortId, 
                                                                    ')\n')) %>% 
                                     dplyr::pull(.data$message), collapse = ""))
      message <- c(message, paste0("Total number of records returned for all cohorts characterized = ", 
                                   nrow(cohortCharacteristicsOutput$result) %>% 
                                     scales::comma(accuracy = 1), 
                                   '\n'))
      
      if (nrow(cohortCharacteristicsOutput$result) > 0) {
        message <- c(message, paste0("     ", 
                                     subset %>% 
                                       dplyr::select(.data$cohortId) %>% 
                                       dplyr::left_join(cohortCharacteristicsOutput$result %>% 
                                                          dplyr::group_by(.data$cohortId) %>% 
                                                          dplyr::summarise(n = dplyr::n()) %>% 
                                                          dplyr::select(.data$cohortId, .data$n)
                                       ) %>% 
                                       dplyr::mutate(n = tidyr::replace_na(data = .data$n, replace = 0)) %>% 
                                       dplyr::mutate(message = paste0('Cohort Id:', 
                                                                      .data$cohortId, 
                                                                      " Feature Count -> ", 
                                                                      scales::comma(.data$n, accuracy = 1), "\n")) %>% 
                                       dplyr::pull(message)))
        
        characteristicsResultFiltered <- cohortCharacteristicsOutput$result %>% 
          dplyr::mutate(mean = round(x = mean, digits = 4)) %>% 
          dplyr::filter(mean != 0) # Drop covariates with mean = 0 after rounding to 4 digits
        
        message <- c(message, paste0("The number of cohorts with atleast one covariate with mean > 0.0001 is ",
                                     length(characteristicsResultFiltered$cohortId %>% unique()) %>% scales::comma(accuracy = 1)))
        message <- c(message, paste0("\n    Total number of records returned for all cohorts characterized with mean > 0.0001 = ",
                                     nrow(characteristicsResultFiltered) %>% scales::comma(accuracy = 1)))
        message <- c(message, paste0("      ", 
                                     subset %>% 
                                       dplyr::select(.data$cohortId) %>% 
                                       dplyr::left_join(characteristicsResultFiltered %>% 
                                                          dplyr::group_by(.data$cohortId) %>% 
                                                          dplyr::summarise(n = dplyr::n()) %>% 
                                                          dplyr::select(.data$cohortId, .data$n)
                                       ) %>% 
                                       dplyr::mutate(n = tidyr::replace_na(data = .data$n, replace = 0)) %>% 
                                       dplyr::mutate(message = paste0('Cohort Id: ', 
                                                                      .data$cohortId, 
                                                                      " Feature Count -> ", 
                                                                      scales::comma(.data$n, accuracy = 1), "\n")) %>% 
                                       dplyr::pull(message)))
        if (nrow(characteristicsResultFiltered) > 0) {
          writeToCsv(
            data = cohortCharacteristicsOutput$covariateRef %>% 
              dplyr::rename(covariateAnalysisId = .data$analysisId),
            fileName = file.path(exportFolder, "covariate_ref.csv"),
            incremental = incremental,
            covariateId = cohortCharacteristicsOutput$covariateRef$covariateId
          )
          # sql <- "DELETE FROM @cohort_database_schema.@unique_concept_id_table
          #         WHERE database_id = '@database_id'
          #         AND cohort_id in (0)
          #         AND task = '@task';
          # 
          #         INSERT INTO @cohort_database_schema.@unique_concept_id_table
          #         (database_id, cohort_id, task, concept_id)
          #         SELECT DISTINCT '@database_id' as database_id,
          #                0 as cohort_id,
          #                '@task' as task,
          #                covariate.concept_id
          #         FROM #covariate_ref covariate;"
          # DatabaseConnector::renderTranslateExecuteSql(connection = connection,
          #                                              sql = sql,
          #                                              cohort_database_schema = cohortDatabaseSchema,
          #                                              unique_concept_id_table = uniqueConceptIdTable,
          #                                              database_id = databaseId,
          #                                              task = 'runCohortCharacterization')
          writeToCsv(
            data = cohortCharacteristicsOutput$analysisRef,
            fileName = file.path(exportFolder, "analysis_ref.csv"),
            incremental = incremental,
            analysisId = cohortCharacteristicsOutput$analysisRef$analysisId
          )
          if (!exists("counts")) {
            counts <- readr::read_csv(file = file.path(exportFolder, "cohort_count.csv"), 
                                      col_types = readr::cols(),
                                      guess_max = min(1e7))
            names(counts) <- SqlRender::snakeCaseToCamelCase(names(counts))
          }
          characteristicsResultFiltered <- characteristicsResultFiltered %>% 
            dplyr::mutate(databaseId = !!databaseId) %>% 
            dplyr::left_join(y = counts)
          characteristicsResultFiltered <- enforceMinCellValue(data = characteristicsResultFiltered, 
                                                               fieldName = "mean", 
                                                               minValues = minCellCount/characteristicsResultFiltered$cohortEntries)
          characteristicsResultFiltered <- characteristicsResultFiltered %>% 
            dplyr::mutate(sd = dplyr::case_when(mean >= 0 ~ sd)) %>% 
            dplyr::mutate(mean = round(.data$mean, digits = 4),
                          sd = round(.data$sd, digits = 4)) %>% 
            dplyr::select(-.data$cohortEntries, -.data$cohortSubjects)
          writeToCsv(data = characteristicsResultFiltered, 
                     fileName = file.path(exportFolder, "covariate_value.csv"), 
                     incremental = incremental, 
                     cohortId = characteristicsResultFiltered$cohortId %>% unique())
        }
      } else {
        message <- c(message, "- WARNING: No characterization output received for any of the submitted cohorts.\n")
        message <- c(message, "           No characterization data for any of the submitted cohorts.\n")
      }
    } else {
      message <- "Skipping Cohort Characterization. All submitted cohorts were previously characterized."
    }
    ParallelLogger::logInfo(message)
    recordTasksDone(cohortId = subset$cohortId,
                    task = "runCohortCharacterization",
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
    delta <- Sys.time() - startCohortCharacterization
    ParallelLogger::logInfo(paste("Running Characterization took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    ParallelLogger::logInfo("\n")
  }
  
  # Temporal Cohort characterization ---------------------------------------------------------------
  if (runTemporalCohortCharacterization) {
    ParallelLogger::logInfo("------------------------------------")
    startTemporalCohortCharacterization <- Sys.time()
    ParallelLogger::logInfo("- Temporal Cohort characterizations - started at ", Sys.time())
    subset <- subsetToRequiredCohorts(cohorts = cohorts %>%
                                        dplyr::filter(.data$cohortId %in% instantiatedCohorts), 
                                      task = "runTemporalCohortCharacterization", 
                                      incremental = incremental, 
                                      recordKeepingFile = recordKeepingFile)
    if (nrow(subset) > 0) {
      if (incremental && (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo("  Skipping ", 
                                scales::comma(length(instantiatedCohorts) - length(subset), accuracy = 1), 
                                " cohorts in incremental mode.")
      }
      
      ParallelLogger::logInfo(paste0('Starting large scale temporal characterization of ', 
                                     scales::comma(nrow(subset), accuracy = 1), 
                                     " cohorts"))
      cohortCharacteristicsOutput <- getCohortCharacteristics(connection = connection,
                                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                                              oracleTempSchema = oracleTempSchema,
                                                              cohortDatabaseSchema = cohortDatabaseSchema,
                                                              cohortTable = cohortTable,
                                                              cohortIds = subset$cohortId,
                                                              covariateSettings = temporalCovariateSettings,
                                                              cdmVersion = cdmVersion)
      
      if (length(cohortCharacteristicsOutput) == 0) {
        ParallelLogger::logWarn("\n   No temporal characterization output for submitted cohorts")
      }
      
      message <- "\n  Characterization results summary:\n"
      message <- c(message, paste0("    Number of cohorts submitted for characterization = ", length(subset$cohortId), '\n'))
      message <- c(message, "    Following cohorts were submitted: \n")
      message <- c(message, paste0(subset %>% 
                                     dplyr::mutate(message = paste0("      ", subset$cohortName, 
                                                                    " (Cohort Id: ", 
                                                                    subset$cohortId, 
                                                                    ')\n')) %>% 
                                     dplyr::pull(.data$message), collapse = ""))
      message <- c(message, paste0("    Total number of records returned for all cohorts characterized = ", 
                                   nrow(cohortCharacteristicsOutput$result) %>% 
                                     scales::comma(accuracy = 1), 
                                   '\n'))
      
      if (nrow(cohortCharacteristicsOutput$result) > 0) {
        message <- c(message, paste0("     ", 
                                     subset %>% 
                                       dplyr::select(.data$cohortId) %>% 
                                       dplyr::left_join(cohortCharacteristicsOutput$result %>% 
                                                          dplyr::group_by(.data$cohortId) %>% 
                                                          dplyr::summarise(n = dplyr::n()) %>% 
                                                          dplyr::select(.data$cohortId, .data$n)
                                       ) %>% 
                                       dplyr::mutate(n = tidyr::replace_na(data = .data$n, replace = 0)) %>% 
                                       dplyr::mutate(message = paste0('Cohort Id:', 
                                                                      .data$cohortId, 
                                                                      " Feature Count -> ", 
                                                                      scales::comma(.data$n, accuracy = 1), "\n")) %>% 
                                       dplyr::pull(message)))
        
        characteristicsResultFiltered <- cohortCharacteristicsOutput$result %>% 
          dplyr::mutate(mean = round(x = mean, digits = 4)) %>% 
          dplyr::filter(mean != 0) # Drop covariates with mean = 0 after rounding to 4 digits
        
        message <- c(message, paste0("    The number of cohorts with atleast one covariate with mean > 0.0001 is ",
                                     length(characteristicsResultFiltered$cohortId %>% unique()) %>% scales::comma(accuracy = 1)))
        message <- c(message, paste0("\n    Total number of records returned for all cohorts characterized with mean > 0.0001 = ",
                                     nrow(characteristicsResultFiltered) %>% scales::comma(accuracy = 1)))
        message <- c(message, paste0("      ", 
                                     subset %>% 
                                       dplyr::select(.data$cohortId) %>% 
                                       dplyr::left_join(characteristicsResultFiltered %>% 
                                                          dplyr::group_by(.data$cohortId) %>% 
                                                          dplyr::summarise(n = dplyr::n()) %>% 
                                                          dplyr::select(.data$cohortId, .data$n)
                                       ) %>% 
                                       dplyr::mutate(n = tidyr::replace_na(data = .data$n, replace = 0)) %>% 
                                       dplyr::mutate(message = paste0('Cohort Id: ', 
                                                                      .data$cohortId, 
                                                                      " Feature Count -> ", 
                                                                      scales::comma(.data$n, accuracy = 1), "\n")) %>% 
                                       dplyr::pull(message)))
        if (nrow(characteristicsResultFiltered) > 0) {
          writeToCsv(
            data = cohortCharacteristicsOutput$covariateRef %>% 
              dplyr::rename(covariateAnalysisId = .data$analysisId),
            fileName = file.path(exportFolder, "temporal_covariate_ref.csv"),
            incremental = incremental,
            covariateId = cohortCharacteristicsOutput$covariateRef$covariateId
          )
          # sql <- "DELETE FROM @cohort_database_schema.@unique_concept_id_table
          #         WHERE database_id = '@database_id'
          #         AND cohort_id in (0)
          #         AND task = '@task';
          # 
          #         INSERT INTO @cohort_database_schema.@unique_concept_id_table
          #         (database_id, cohort_id, task, concept_id)
          #         SELECT DISTINCT '@database_id' as database_id,
          #                0 as cohort_id,
          #                '@task' as task,
          #                covariate.concept_id
          #         FROM #covariate_ref covariate;"
          # DatabaseConnector::renderTranslateExecuteSql(connection = connection,
          #                                              sql = sql,
          #                                              cohort_database_schema = cohortDatabaseSchema,
          #                                              unique_concept_id_table = uniqueConceptIdTable,
          #                                              database_id = databaseId,
          #                                              task = 'runTemporalCohortCharacterization')
          writeToCsv(
            data = cohortCharacteristicsOutput$analysisRef,
            fileName = file.path(exportFolder, "temporal_analysis_ref.csv"),
            incremental = incremental,
            analysisId = cohortCharacteristicsOutput$analysisRef$analysisId
          )
          writeToCsv(
            data = cohortCharacteristicsOutput$timeRef,
            fileName = file.path(exportFolder, "temporal_time_ref.csv"),
            incremental = incremental,
            timeId = cohortCharacteristicsOutput$timeRef$timeId
          )
          if (!exists("counts")) {
            counts <- readr::read_csv(file = file.path(exportFolder, "cohort_count.csv"), 
                                      col_types = readr::cols(),
                                      guess_max = min(1e7))
            names(counts) <- SqlRender::snakeCaseToCamelCase(names(counts))
          }
          characteristicsResultFiltered <- characteristicsResultFiltered %>% 
            dplyr::mutate(databaseId = !!databaseId) %>% 
            dplyr::left_join(y = counts)
          characteristicsResultFiltered <- enforceMinCellValue(data = characteristicsResultFiltered, 
                                                               fieldName = "mean", 
                                                               minValues = minCellCount/characteristicsResultFiltered$cohortEntries)
          characteristicsResultFiltered <- characteristicsResultFiltered %>% 
            dplyr::mutate(sd = dplyr::case_when(mean >= 0 ~ sd)) %>% 
            dplyr::mutate(mean = round(.data$mean, digits = 4),
                          sd = round(.data$sd, digits = 4)) %>% 
            dplyr::select(-.data$cohortEntries, -.data$cohortSubjects)
          writeToCsv(data = characteristicsResultFiltered, 
                     fileName = file.path(exportFolder, "temporal_covariate_value.csv"), 
                     incremental = incremental, 
                     cohortId = characteristicsResultFiltered$cohortId %>% unique())
        }
      } else {
        message <- c(message, "- WARNING: No temporal characterization output received for any of the submitted cohorts.\n")
        message <- c(message, "           No temporal characterization data for any of the submitted cohorts.\n")
      }
    } else {
      message <- "Skipping Tempoarl Characterization. All submitted cohorts were previously characterized."
    }
    ParallelLogger::logInfo(message)
    recordTasksDone(cohortId = subset$cohortId,
                    task = "runTemporalCohortCharacterization",
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
    delta <- Sys.time() - startTemporalCohortCharacterization
    ParallelLogger::logInfo(paste("Running Temporal Characterization took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    ParallelLogger::logInfo("\n")
  }
  
  # get a copy of the omop vocabulary but subset to the concept id of cohorts diagnosed -------------------------
  if (runSubsetOmopVocabularyTables) {
    ParallelLogger::logInfo("------------------------------------")
    startSubsetOmopVocabularyTables <- Sys.time()
    getOmopVocabularyTables(connection = connection,
                            cdmDatabaseSchema = cdmDatabaseSchema,
                            cohortDatabaseSchema = cohortDatabaseSchema,
                            uniqueConceptIdTable = uniqueConceptIdTable,
                            exportFolder = exportFolder)
    recordTasksDone(cohortId = subset$cohortId,
                    task = "runSubsetOmopVocabularyTables",
                    checksum = subset$checksum,
                    recordKeepingFile = recordKeepingFile,
                    incremental = incremental)
    delta <- Sys.time() - startSubsetOmopVocabularyTables
    ParallelLogger::logInfo(paste("Subsetting and extracting OMOP vocabulary tables took ",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    ParallelLogger::logInfo("\n")
  }
  
  # Add all to zip file -------------------------------------------------------------------------------
  ParallelLogger::logInfo("Adding results to zip file")
  zipName <- file.path(exportFolder, paste0("Results_", databaseId, ".zip"))
  files <- list.files(exportFolder, pattern = ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  ParallelLogger::logInfo("Results are ready for sharing at: ", zipName)
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing all diagnostics took",
                                signif(delta, 3),
                                attr(delta, "units")))
}


writePhenotypeDescriptionCsvToResults <- function(phenotypeDescriptionCsvPath,
                                                  exportFolder,
                                                  cohorts, 
                                                  errorMessage = NULL) {
  if (is.null(errorMessage)) {
    errorMessage <- checkmate::makeAssertCollection(errorMessage)
  }
  
  if (file.exists(phenotypeDescriptionCsvPath)) {
    ParallelLogger::logInfo('  Found phenotype description csv file. Loading.')
    phenotypeDescription <- readr::read_csv(file = phenotypeDescriptionCsvPath, 
                                            col_types = readr::cols(),
                                            na = 'NA',
                                            guess_max = min(1e7)) %>% 
      dplyr::arrange(.data$phenotype_name, .data$phenotype_id)
    
    checkmate::assertTibble(x = phenotypeDescription, 
                            any.missing = TRUE, 
                            min.rows = 1, 
                            min.cols = 6, 
                            add = errorMessage)
    checkmate::assertNames(x = colnames(phenotypeDescription),
                           must.include = c('phenotype_id', 'phenotype_name',
                                            'referent_concept_id', 'clinical_description',
                                            'literature_review', 'phenotype_notes'),
                           add = errorMessage)
    checkmate::reportAssertions(collection = errorMessage)
    
    phenotypeDescription <- phenotypeDescription %>% 
      dplyr::mutate(phenotype_name = dplyr::coalesce(as.character(.data$phenotype_name),''),
                    clinical_description = dplyr::coalesce(as.character(.data$clinical_description),''),
                    literature_review = dplyr::coalesce(as.character(.data$literature_review),''),
                    phenotype_notes = dplyr::coalesce(as.character(.data$phenotype_notes),'')
      )  
    checkmate::assertTibble(x = phenotypeDescription,
                            types = c('double', 'character'))
    checkmate::reportAssertions(collection = errorMessage)
    
    ParallelLogger::logInfo(' Phenotype description file has ', 
                            scales::comma(nrow(phenotypeDescription)), 
                            ' rows. Matching with submitted cohorts')
    
    phenotypeDescription <- phenotypeDescription %>% 
      dplyr::inner_join(cohorts %>% 
                          dplyr::select(.data$referentConceptId) %>% 
                          dplyr::mutate(referent_concept_id = .data$referentConceptId))
    
    ParallelLogger::logInfo(scales::comma(nrow(phenotypeDescription)), 
                            ' rows matched')
    
    if (nrow(phenotypeDescription) > 0) {
      phenotypeDescription %>% 
        readr::write_excel_csv(path = file.path(exportFolder, "phenotype_descripton.csv"), 
                               na = '')
    } else {
      ParallelLogger::logWarn(" phentoype description csv file found, but records dont match the referent concept ids of the cohorts being diagnosed.")
    }
  } else {
    ParallelLogger::logWarn(" phentoype description csv file not found")
  }
}
