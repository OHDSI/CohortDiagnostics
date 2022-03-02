# Copyright 2022 Observational Health Data Sciences and Informatics
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

checkCohortReference <- function(cohortReference, errorMessage = NULL) {
  if (is.null(errorMessage) | !class(errorMessage) == 'AssertColection') {
    errorMessage <- checkmate::makeAssertCollection()
  }
  checkmate::assertDataFrame(
    x = cohortReference,
    types = c("integer", "character", "numeric"),
    min.rows = 1,
    min.cols = 5,
    null.ok = FALSE,
    col.names = "named",
    add = errorMessage
  )
  if ("referentConceptId" %in% names(cohortReference)) {
    checkmate::assertIntegerish(
      x = cohortReference$referentConceptId,
      lower = 0,
      any.missing = FALSE,
      unique = FALSE,
      null.ok = FALSE,
      add = errorMessage
    )
  }
  checkmate::assertNames(
    x = names(cohortReference),
    must.include = c(
      "cohortId",
      "cohortName",
      "logicDescription",
      "sql",
      "json"
    ),
    add = errorMessage
  )
  invisible(errorMessage)
}

makeBackwardsCompatible <- function(cohorts) {
  if (!"name" %in% colnames(cohorts)) {
    if ('cohortId' %in% colnames(cohorts)) {
      cohorts <- cohorts %>%
        dplyr::mutate(name = as.character(.data$cohortId))
    } else if ('id' %in% colnames(cohorts)) {
      cohorts <- cohorts %>%
        dplyr::mutate(name = as.character(.data$id)) %>%
        dplyr::mutate(cohortId = .data$id)
    }
  }
  if (!"webApiCohortId" %in% colnames(cohorts) &&
      "atlasId" %in% colnames(cohorts)) {
    cohorts <- cohorts %>%
      dplyr::mutate(webApiCohortId = .data$atlasId)
  }
  if (!"cohortName" %in% colnames(cohorts) &&
      "atlasName" %in% colnames(cohorts)) {
    cohorts <- cohorts %>%
      dplyr::mutate(cohortName = .data$atlasName)
  }
  return(cohorts)
}

#' Load Cohort Definitions From A Study Package
#' @description
#' Load cohort references for usage in executeDiagnostics.
#' @inheritParams runCohortDiagnostics
#' @param errorMessage      checkmate assert collection, used internally for error checks
#' @export
loadCohortsFromPackage <- function(packageName,
                                   cohortToCreateFile = "settings/cohortsToCreate.csv",
                                   cohortIds = NULL,
                                   errorMessage = NULL) {
  ParallelLogger::logDebug("Executing on cohorts specified in package - ", packageName)

  displayErrors <- FALSE
  if (is.null(errorMessage) |
    !class(errorMessage) == 'AssertColection') {
    displayErrors <- TRUE
    errorMessage <- checkmate::makeAssertCollection()
  }
  checkmate::assertCharacter(
    x = packageName,
    min.len = 1,
    max.len = 1,
    add = errorMessage
  )
  pathToCsv <- system.file(cohortToCreateFile, package = packageName)
  checkmate::assertFileExists(
    x = system.file(cohortToCreateFile, package = packageName),
    access = "r",
    extension = "csv",
    add = errorMessage
  )

  checkInputFileEncoding(pathToCsv)

  cohorts <- readr::read_csv(pathToCsv,
                             col_types = readr::cols(),
                             guess_max = min(1e7))
  cohorts <- makeBackwardsCompatible(cohorts)
  if (!is.null(cohortIds)) {
    cohorts <- cohorts %>%
      dplyr::filter(.data$cohortId %in% cohortIds)
  }

  checkCohortReference(cohortReference = cohorts, errorMessage = errorMessage)

  getSql <- function(name) {
    pathToSql <-
      system.file("sql", "sql_server", paste0(name, ".sql"), package = packageName)
    checkmate::assertFile(
      x = pathToSql,
      access = "r",
      extension = "sql",
      add = errorMessage
    )
    sql <- readChar(pathToSql, file.info(pathToSql)$size)
    return(sql)
  }

  cohorts$sql <- sapply(cohorts$name, getSql)

  getJson <- function(name) {
    pathToJson <-
      system.file("cohorts", paste0(name, ".json"), package = packageName)
    checkmate::assertFile(
      x = pathToJson,
      access = "r",
      extension = "json",
      add = errorMessage
    )
    json <- readChar(pathToJson, file.info(pathToJson)$size)
    return(json)
  }

  cohorts$json <- sapply(cohorts$name, getJson)
  if (displayErrors) {
    checkmate::reportAssertions(collection = errorMessage)
  }
  return(selectColumnAccordingToResultsModel(cohorts))
}


getCohortsJsonAndSqlFromWebApi <- function(baseUrl = baseUrl,
                                           cohortSetReference = cohortSetReference,
                                           cohortIds = NULL,
                                           errorMessage = NULL,
                                           generateStats = TRUE) {
  ParallelLogger::logDebug("Running Cohort Diagnostics on cohort specified in WebApi - ",
                           baseUrl)
  
  if (is.null(errorMessage) |
      !class(errorMessage) == 'AssertColection') {
    errorMessage <- checkmate::makeAssertCollection()
  }
  checkmate::assertCharacter(x = baseUrl,
                             min.chars = 1,
                             add = errorMessage)
  webApiVersion <- ROhdsiWebApi::getWebApiVersion(baseUrl)
  ParallelLogger::logInfo("WebApi of version ", webApiVersion, " found at ", baseUrl)
  checkmate::assertCharacter(x = webApiVersion,
                             min.chars = 1,
                             add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  if (!is.null(cohortIds)) {
    cohortSetReference <- cohortSetReference %>%
      dplyr::filter(.data$cohortId %in% cohortIds)
  }
  
  if ("name" %in% names(cohortSetReference)) {
    cohortSetReference <-
      dplyr::rename(cohortSetReference, cohortName = "name")
  }

  if ("webApiCohortId" %in% names(cohortSetReference)) {
    cohortSetReference <- cohortSetReference %>% 
      dplyr::rename(atlasId = .data$webApiCohortId)
  }

  if (!"atlasId" %in% names(cohortSetReference)) {
    cohortSetReference <- cohortSetReference %>% 
      dplyr::mutate(atlasId = .data$cohortId)
  }

  cohortSetReference$json <- ""
  cohortSetReference$sql <- ""
  
  ParallelLogger::logInfo("Retrieving cohort definitions from WebAPI")
  for (i in 1:nrow(cohortSetReference)) {
    ParallelLogger::logInfo("- Retrieving definitions for cohort ",
                            cohortSetReference$cohortName[i])
    cohortDefinition <-
      ROhdsiWebApi::getCohortDefinition(cohortId = cohortSetReference$atlasId[i],
                                        baseUrl = baseUrl)
    cohortSetReference$json[i] <-
      RJSONIO::toJSON(x = cohortDefinition$expression, digits = 23)
    cohortSetReference$sql[i] <-
      ROhdsiWebApi::getCohortSql(
        cohortDefinition = cohortDefinition,
        baseUrl = baseUrl,
        generateStats = generateStats
      )
  }
  return(selectColumnAccordingToResultsModel(cohortSetReference))
}

selectColumnAccordingToResultsModel <- function(data) {
  columsToInclude <- c()
  if ("phenotypeId" %in% colnames(data)) {
    columsToInclude <- c(columsToInclude, "phenotypeId")
  }
  columsToInclude <- c(columsToInclude, "cohortId", "cohortName")
  if ("logicDescription" %in% colnames(data)) {
    columsToInclude <- c(columsToInclude, "logicDescription")
  }
  if ("referentConceptId" %in% colnames(data)) {
    columsToInclude <- c(columsToInclude, "referentConceptId")
  }
  if ("cohortType" %in% colnames(data)) {
    columsToInclude <- c(columsToInclude, "cohortType")
  }
  if ("atlasId" %in% colnames(data)) {
    columsToInclude <- c(columsToInclude, "atlasId")
  }
  columsToInclude <-
    c(columsToInclude, "json" , "sql")
  return(data[, columsToInclude])
}

getCohortsJsonAndSql <- function(packageName = NULL,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
                                 baseUrl = NULL,
                                 cohortSetReference = NULL,
                                 cohortIds = NULL,
                                 generateStats = TRUE) {
  if (!is.null(packageName)) {
    cohorts <-
      loadCohortsFromPackage(
        packageName = packageName,
        cohortToCreateFile = cohortToCreateFile,
        cohortIds = cohortIds
      )
    if (!is.null(baseUrl)) {
      baseUrl <- NULL
      ParallelLogger::logInfo(
        "Ignoring parameter baseUrl because packageName is provided. Overiding user parameter baseUrl - setting to NULL"
      )
    }
    if (!is.null(cohortSetReference)) {
      cohortSetReference <- NULL
      ParallelLogger::logInfo(
        "Ignoring parameter cohortSetReference because packageName is provided. Overiding user parameter cohortSetReference - setting to NULL"
      )
    }
  } else {
    warning("Use of WebApi mode is to be removed in a future version")
    cohorts <- getCohortsJsonAndSqlFromWebApi(
      baseUrl = baseUrl,
      cohortSetReference = cohortSetReference,
      cohortIds = cohortIds,
      generateStats = generateStats
    )
  }
  ParallelLogger::logInfo("Number of cohorts ", nrow(cohorts))
  if (nrow(cohorts) == 0) {
    warning("No cohorts founds")
  }
  if (nrow(cohorts) != length(cohorts$cohortId %>% unique())) {
    warning(
      "Please check input cohort specification. Is there duplication of cohortId? Returning empty cohort table."
    )
    return(cohorts[0, ])
  }
  return(cohorts)
}

createCohortTable <- function(connectionDetails = NULL,
                              connection = NULL,
                              cohortDatabaseSchema,
                              cohortTable = "cohort") {
  start <- Sys.time()
  ParallelLogger::logInfo("Creating cohort table")
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql(
    "CreateCohortTable.sql",
    packageName = utils::packageName(),
    dbms = connection@dbms,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable
  )
  DatabaseConnector::executeSql(connection,
                                sql,
                                progressBar = FALSE,
                                reportOverallTime = FALSE)
  ParallelLogger::logDebug("Created table ", cohortDatabaseSchema, ".", cohortTable)
  
  delta <- Sys.time() - start
  writeLines(paste(
    "Creating cohort table took",
    signif(delta, 3),
    attr(delta, "units")
  ))
}

getInclusionStatisticsFromFiles <- function(cohortIds = NULL,
                                            folder,
                                            cohortInclusionFile = file.path(folder,
                                                                            "cohortInclusion.csv"),
                                            cohortInclusionResultFile = file.path(folder,
                                                                                  "cohortIncResult.csv"),
                                            cohortInclusionStatsFile = file.path(folder,
                                                                                 "cohortIncStats.csv"),
                                            cohortSummaryStatsFile = file.path(folder,
                                                                               "cohortSummaryStats.csv"),
                                            simplify = TRUE) {
  start <- Sys.time()
  
  if (!file.exists(cohortInclusionFile)) {
    return(NULL)
  }
  
  fetchStats <- function(file) {
    ParallelLogger::logDebug("- Fetching data from ", file)
    stats <- readr::read_csv(file,
                             col_types = readr::cols(),
                             guess_max = min(1e7))
    if (!is.null(cohortIds)) {
      stats <- stats %>%
        dplyr::filter(.data$cohortDefinitionId %in% cohortIds)
    }
    return(stats)
  }

  inclusion <- fetchStats(cohortInclusionFile)
  if ('description' %in% names(inclusion)) {
    inclusion$description <- as.character(inclusion$description)
    inclusion$description[is.na(inclusion$description)] <- ""
  } else {
    inclusion$description <- ""
  }

  summaryStats <- fetchStats(cohortSummaryStatsFile)
  inclusionStats <- fetchStats(cohortInclusionStatsFile)
  inclusionResults <- fetchStats(cohortInclusionResultFile)
  result <- dplyr::tibble()
  for (cohortId in unique(inclusion$cohortDefinitionId)) {
    cohortResult <-
      processInclusionStats(
        inclusion = filter(inclusion, .data$cohortDefinitionId == cohortId),
        inclusionResults = filter(inclusionResults, .data$cohortDefinitionId == cohortId),
        inclusionStats = filter(inclusionStats, .data$cohortDefinitionId == cohortId),
        summaryStats = filter(summaryStats, .data$cohortDefinitionId == cohortId),
        simplify = simplify
      )
    cohortResult$cohortDefinitionId <- cohortId
    result <- dplyr::bind_rows(result, cohortResult)
  }
  delta <- Sys.time() - start
  writeLines(paste(
    "Fetching inclusion statistics took",
    signif(delta, 3),
    attr(delta, "units")
  ))
  return(result)
}

processInclusionStats <- function(inclusion,
                                  inclusionResults,
                                  simplify,
                                  inclusionStats,
                                  summaryStats) {
  if (simplify) {
    if (nrow(inclusion) == 0 || nrow(inclusionStats) == 0) {
      return(tidyr::tibble())
    }
    
    result <- inclusion %>%
      dplyr::select(.data$ruleSequence, .data$name) %>%
      dplyr::distinct() %>%
      dplyr::inner_join(
        inclusionStats %>%
          dplyr::filter(.data$modeId == 0) %>%
          dplyr::select(
            .data$ruleSequence,
            .data$personCount,
            .data$gainCount,
            .data$personTotal
          ),
        by = "ruleSequence"
      ) %>%
      dplyr::mutate(remain = 0)
    
    inclusionResults <- inclusionResults %>%
      dplyr::filter(.data$modeId == 0)
    mask <- 0
    for (ruleId in 0:(nrow(result) - 1)) {
      mask <- bitwOr(mask, 2 ^ ruleId)
      idx <-
        bitwAnd(inclusionResults$inclusionRuleMask, mask) == mask
      result$remain[result$ruleSequence == ruleId] <-
        sum(inclusionResults$personCount[idx])
    }
    colnames(result) <- c(
      "ruleSequenceId",
      "ruleName",
      "meetSubjects",
      "gainSubjects",
      "totalSubjects",
      "remainSubjects"
    )
  } else {
    if (nrow(inclusion) == 0) {
      return(list())
    }
    result <- list(
      inclusion = inclusion,
      inclusionResults = inclusionResults,
      inclusionStats = inclusionStats,
      summaryStats = summaryStats
    )
  }
  return(result)
}

getInclusionStats <- function(connection,
                              exportFolder,
                              databaseId,
                              cohortDefinitionSet,
                              cohortDatabaseSchema,
                              cohortTableNames,
                              incremental,
                              instantiatedCohorts,
                              inclusionStatisticsFolder,
                              minCellCount,
                              recordKeepingFile) {
  ParallelLogger::logInfo("Fetching inclusion statistics from files")
  subset <- subsetToRequiredCohorts(
    cohorts = cohortDefinitionSet %>%
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
    if (is.null(inclusionStatisticsFolder)) {
      ParallelLogger::logInfo("Exporting inclusion rules with CohortGenerator")
      CohortGenerator::insertInclusionRuleNames(connection = connection,
                                                cohortDefinitionSet = subset,
                                                cohortDatabaseSchema = cohortDatabaseSchema,
                                                cohortInclusionTable = cohortTableNames$cohortInclusionTable)
      # This part will change in future version, with a patch to CohortGenerator that
      # supports the usage of exporting tables without writing to disk
      inclusionStatisticsFolder <- tempfile("CdCohortStatisticsFolder")
      on.exit(unlink(inclusionStatisticsFolder), add = TRUE)
      CohortGenerator::exportCohortStatsTables(connection = connection,
                                               cohortDatabaseSchema = cohortDatabaseSchema,
                                               cohortTableNames = cohortTableNames,
                                               cohortStatisticsFolder = inclusionStatisticsFolder,
                                               incremental = FALSE) # Note use of FALSE to always genrate stats here
    }

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
      warning("Cohort Inclusion statistics file not found. Inclusion Statistics not run.")
    }
  }
}

#' Instantiate a set of cohort
#'
#' @description
#' This function instantiates a set of cohort in the cohort table, using definitions that are fetched from a WebApi interface.
#' Optionally, the inclusion rule statistics are computed and stored in the \code{inclusionStatisticsFolder}.
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @template TempEmulationSchema
#'
#' @template OracleTempSchema
#'
#' @template CdmDatabaseSchema
#'
#' @template VocabularyDatabaseSchema
#'
#' @template CohortSetSpecs
#'
#' @template CohortSetReference
#'
#' @inheritParams runCohortDiagnostics
#' @param generateInclusionStats      Compute and store inclusion rule statistics?
#' @param createCohortTable           Create/Overwrite a cohort table?
#' @return
#' A data frame with cohort counts
#'
#' @export
instantiateCohortSet <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 vocabularyDatabaseSchema = cdmDatabaseSchema,
                                 tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                 oracleTempSchema = NULL,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortIds = NULL,
                                 cohortDefinitionSet = NULL,
                                 packageName = NULL,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
                                 baseUrl = NULL,
                                 cohortSetReference = NULL,
                                 generateInclusionStats = TRUE,
                                 inclusionStatisticsFolder = NULL,
                                 createCohortTable = TRUE,
                                 incremental = FALSE,
                                 incrementalFolder = NULL) {
  message("***** This function will be removed in a future version. Please see https://github.com/ohdsi/CohortGenerator for future generation of cohorts ****")

  if (!is.null(cohortSetReference)) {
    ParallelLogger::logInfo("Found cohortSetReference. Cohort Diagnostics is running in WebApi mode.")
    cohortToCreateFile <- NULL
  }
  
  if (!is.null(oracleTempSchema) && is.null(tempEmulationSchema)) {
    tempEmulationSchema <- oracleTempSchema
    warning('OracleTempSchema has been deprecated by DatabaseConnector')
  }
  
  if (generateInclusionStats) {
    if (is.null(inclusionStatisticsFolder)) {
      stop("Must specify inclusionStatisticsFolder when generateInclusionStats = TRUE")
    }
    if (!file.exists(inclusionStatisticsFolder)) {
      dir.create(inclusionStatisticsFolder, recursive = TRUE)
    }
  }
  if (incremental) {
    if (is.null(incrementalFolder)) {
      stop("Must specify incrementalFolder when incremental = TRUE")
    }
    if (!file.exists(incrementalFolder)) {
      dir.create(incrementalFolder, recursive = TRUE)
    }
  }
  
  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  if (createCohortTable) {
    needToCreate <- TRUE
    if (incremental) {
      tables <-
        DatabaseConnector::getTableNames(connection, cohortDatabaseSchema)
      if (toupper(cohortTable) %in% toupper(tables)) {
        ParallelLogger::logInfo("Cohort table already exists and in incremental mode, so not recreating table.")
        needToCreate <- FALSE
      }
    }
    if (needToCreate) {
      createCohortTable(
        connection = connection,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable
      )
    }
  }


  if (is.null(cohortDefinitionSet)) {
    cohortDefinitionSet <- getCohortsJsonAndSql(
      packageName = packageName,
      cohortToCreateFile = cohortToCreateFile,
      baseUrl = baseUrl,
      cohortSetReference = cohortSetReference,
      cohortIds = cohortIds,
      generateStats = generateInclusionStats
    )
  } else {
    checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
    checkmate::assertNames(colnames(cohortDefinitionSet),
                           must.include = c("cohortId",
                                            "cohortName",
                                            "logicDescription",
                                            "json",
                                            "sql"))
    # Filter to cohort subset
    if (!is.null(cohortIds)) {
      cohortDefinitionSet <- cohortDefinitionSet %>%
        dplyr::filter(.data$cohortId %in% cohortIds)
    }
  }

  if (incremental) {
    cohortDefinitionSet$checksum <- computeChecksum(cohortDefinitionSet$sql)
    recordKeepingFile <-
      file.path(incrementalFolder, "InstantiatedCohorts.csv")
  }
  
  if (generateInclusionStats) {
    createTempInclusionStatsTables(connection, tempEmulationSchema, cohortDefinitionSet)
  }
  
  instantiatedCohortIds <- c()
  for (i in 1:nrow(cohortDefinitionSet)) {
    if (!incremental || isTaskRequired(
      cohortId = cohortDefinitionSet$cohortId[i],
      checksum = cohortDefinitionSet$checksum[i],
      recordKeepingFile = recordKeepingFile
    )) {
      ParallelLogger::logInfo(
        "Instantiation cohort ",
        cohortDefinitionSet$cohortName[i],
        " (Cohort id: ",
        cohortDefinitionSet$cohortId[i],
        ")"
      )
      sql <- cohortDefinitionSet$sql[i]
      .warnMismatchSqlInclusionStats(sql, generateInclusionStats = generateInclusionStats)
      sql <- SqlRender::render(
        sql,
        cdm_database_schema = cdmDatabaseSchema,
        target_database_schema = cohortDatabaseSchema,
        target_cohort_table = cohortTable,
        target_cohort_id = cohortDefinitionSet$cohortId[i]
      )
      if (stringr::str_detect(string = sql,
                              pattern = 'vocabulary_database_schema')) {
        sql <- SqlRender::render(sql,
                                 vocabulary_database_schema = vocabularyDatabaseSchema)
      } else {
        ParallelLogger::logDebug('Cohort id ', cohortDefinitionSet$cohortId[i], " SQL does not have vocabularyDatabaseSchema.")
      }
      if (stringr::str_detect(string = sql,
                              pattern = 'results_database_schema')) {
        ParallelLogger::logDebug('Cohort id ', cohortDefinitionSet$cohortId[i], " SQL has inclusion rule statistics tables.")
      }
      if (generateInclusionStats) {
        if (stringr::str_detect(string = sql,
                                pattern = 'cohort_inclusion')) {
          sql <- SqlRender::render(
            sql,
            results_database_schema.cohort_inclusion = "#cohort_inclusion",
            results_database_schema.cohort_inclusion_result = "#cohort_inc_result",
            results_database_schema.cohort_inclusion_stats = "#cohort_inc_stats",
            results_database_schema.cohort_summary_stats = "#cohort_summary_stats"
          )
        } else {
          ParallelLogger::logDebug('Cohort id ', cohortDefinitionSet$cohortId[i], " SQL does not have inclusion rule statistics tables.")
        }
        # added for compatibility for 2.8.1
        # https://github.com/OHDSI/CohortDiagnostics/issues/387
        # this table was introduced in v2.8.1, and does not exist in prior version of webapi
        if (stringr::str_detect(string = sql,
                                pattern = 'cohort_censor_stats')) {
          sql <- SqlRender::render(sql = sql,
                                   results_database_schema.cohort_censor_stats = "#cohort_censor_stats"
                                   )
        }
      } else {
        ParallelLogger::logDebug("Skipping inclusion rules for cohort id ",
                                 cohortDefinitionSet$cohortId[i],
                                 " because this diagnostics is set to FALSE.")
      }
      sql <- SqlRender::translate(sql,
                                  targetDialect = connection@dbms,
                                  tempEmulationSchema = tempEmulationSchema)
      DatabaseConnector::executeSql(connection, sql)
      instantiatedCohortIds <-
        c(instantiatedCohortIds, cohortDefinitionSet$cohortId[i])
    }
  }
  
  if (generateInclusionStats) {
    saveAndDropTempInclusionStatsTables(
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      inclusionStatisticsFolder = inclusionStatisticsFolder,
      incremental = incremental,
      cohortIds = instantiatedCohortIds
    )
  }
  if (incremental) {
    recordTasksDone(
      cohortId = cohortDefinitionSet$cohortId,
      checksum = cohortDefinitionSet$checksum,
      recordKeepingFile = recordKeepingFile
    )
  }
  
  delta <- Sys.time() - start
  writeLines(paste(
    "Instantiating cohort set took",
    signif(delta, 3),
    attr(delta, "units")
  ))
}

createTempInclusionStatsTables <-
  function(connection, tempEmulationSchema, cohorts) {
    ParallelLogger::logInfo("Creating temporary inclusion statistics tables")
    sql <-
      SqlRender::loadRenderTranslateSql(
        "inclusionStatsTables.sql",
        packageName = utils::packageName(),
        dbms = connection@dbms,
        tempEmulationSchema = tempEmulationSchema
      )
    DatabaseConnector::executeSql(connection, sql)
    
    inclusionRules <- tidyr::tibble()
    for (i in 1:nrow(cohorts)) {
      cohortDefinition <-
        RJSONIO::fromJSON(content = cohorts$json[i], digits = 23)
      if (!is.null(cohortDefinition$InclusionRules)) {
        nrOfRules <- length(cohortDefinition$InclusionRules)
        if (nrOfRules > 0) {
          for (j in 1:nrOfRules) {
            ruleName <- cohortDefinition$InclusionRules[[j]]$name
            if (length(ruleName) == 0) {
              ruleName <- paste0("Unamed rule (Sequence ", j - 1, ")")
            }
            inclusionRules <- dplyr::bind_rows(
              inclusionRules,
              tidyr::tibble(
                cohortId = cohorts$cohortId[i],
                ruleSequence = j - 1,
                ruleName = !!ruleName
              )
            ) %>%
              dplyr::distinct()
          }
        }
      }
    }
    
    if (nrow(inclusionRules) > 0) {
      inclusionRules <- inclusionRules %>%
        dplyr::inner_join(cohorts %>% dplyr::select(.data$cohortId, .data$cohortName),
                          by = "cohortId") %>%
        dplyr::rename(name = .data$ruleName,
                      cohortDefinitionId = .data$cohortId) %>%
        dplyr::mutate(cohortDefinitionId = as.integer(.data$cohortDefinitionId),
                      ruleSequence = as.integer(.data$ruleSequence)) %>%
        dplyr::select(.data$cohortDefinitionId, .data$ruleSequence, .data$name)
      
      DatabaseConnector::insertTable(
        connection = connection,
        tableName = "#cohort_inclusion",
        data = inclusionRules,
        dropTableIfExists = TRUE,
        createTable = TRUE,
        tempTable = TRUE,
        tempEmulationSchema = tempEmulationSchema,
        camelCaseToSnakeCase = TRUE
      )
    } else {
      inclusionRules <- dplyr::tibble(
        cohortDefinitionId = as.double(),
        ruleSequence = as.integer(),
        name = as.character()
      )
      DatabaseConnector::insertTable(
        connection = connection,
        tableName = "#cohort_inclusion",
        data = inclusionRules,
        dropTableIfExists = TRUE,
        createTable = TRUE,
        tempTable = TRUE,
        tempEmulationSchema = tempEmulationSchema,
        camelCaseToSnakeCase = TRUE
      )
    }
  }

saveAndDropTempInclusionStatsTables <- function(connection,
                                                tempEmulationSchema,
                                                inclusionStatisticsFolder,
                                                incremental,
                                                cohortIds) {
  fetchStats <- function(table, fileName) {
    ParallelLogger::logDebug("- Fetching data from ", table)
    sql <- "SELECT * FROM @table"
    data <- DatabaseConnector::renderTranslateQuerySql(
      sql = sql,
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      snakeCaseToCamelCase = TRUE,
      table = table
    ) %>%
      tidyr::tibble()
    fullFileName <- file.path(inclusionStatisticsFolder, fileName)
    if (incremental) {
      saveIncremental(data, fullFileName, cohortId = cohortIds)
    } else {
      readr::write_csv(x = data, file = fullFileName)
    }
  }
  fetchStats("#cohort_inclusion", "cohortInclusion.csv")
  fetchStats("#cohort_inc_result", "cohortIncResult.csv")
  fetchStats("#cohort_inc_stats", "cohortIncStats.csv")
  fetchStats("#cohort_summary_stats", "cohortSummaryStats.csv")
  
  sql <- "TRUNCATE TABLE #cohort_inclusion;
    DROP TABLE #cohort_inclusion;

    TRUNCATE TABLE #cohort_inc_result;
    DROP TABLE #cohort_inc_result;

    TRUNCATE TABLE #cohort_inc_stats;
    DROP TABLE #cohort_inc_stats;

    TRUNCATE TABLE #cohort_summary_stats;
    DROP TABLE #cohort_summary_stats;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    tempEmulationSchema = tempEmulationSchema
  )
}

.warnMismatchSqlInclusionStats <-
  function(sql, generateInclusionStats) {
    if (any(
      stringr::str_detect(string = sql, pattern = "_inclusion_result"),
      stringr::str_detect(string = sql, pattern = "_inclusion_stats"),
      stringr::str_detect(string = sql, pattern = "_summary_stats"),
      stringr::str_detect(string = sql, pattern = "_censor_stats")
    )) {
      if (isFALSE(generateInclusionStats)) {
        warning(
          "The SQL template used to instantiate cohort was designed to output cohort inclusion statistics.
              But, generateInclusionStats is set to False while instantiating cohort.
              This may cause error and terminate cohort diagnositcs."
        )
      }
    } else {
      if (isTRUE(generateInclusionStats)) {
        warning(
          "The SQL template used to instantiate cohort was designed to NOT output cohort inclusion statistics.
              But, generateInclusionStats is set to TRUE while instantiating cohort.
              This may cause error and terminate cohort diagnositcs."
        )
      }
    }
  }
