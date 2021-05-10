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

checkCohortReference <-
  function(cohortReference, errorMessage = NULL) {
    if (is.null(errorMessage) |
        !class(errorMessage) == 'AssertColection') {
      errorMessage <- checkmate::makeAssertCollection()
    }
    checkmate::assertDataFrame(
      x = cohortReference,
      types = c("integer", "character", "numeric"),
      min.rows = 1,
      min.cols = 4,
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
      subset.of =  c(
        "referentConceptId",
        "cohortId",
        "webApiCohortId",
        "cohortName",
        "logicDescription",
        "clinicalRationale"
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

getCohortsJsonAndSqlFromPackage <-
  function(packageName = packageName,
           cohortToCreateFile = cohortToCreateFile,
           cohortIds = NULL,
           errorMessage = NULL) {
    ParallelLogger::logDebug("Executing on cohorts specified in package - ", packageName)
    
    if (is.null(errorMessage) |
        !class(errorMessage) == 'AssertColection') {
      errorMessage <- checkmate::makeAssertCollection()
    }
    checkmate::assertCharacter(
      x = packageName,
      min.len = 1,
      max.len = 1,
      add = errorMessage
    )
    pathToCsv <-
      system.file(cohortToCreateFile, package = packageName)
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
    checkmate::reportAssertions(collection = errorMessage)
    
    getSql <- function(name) {
      pathToSql <-
        system.file("sql", "sql_server", paste0(name, ".sql"), package = packageName)
      checkmate::assertFile(
        x = pathToSql,
        access = "r",
        extension = ".sql",
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
        extension = ".sql",
        add = errorMessage
      )
      json <- readChar(pathToJson, file.info(pathToJson)$size)
      return(json)
    }
    cohorts$json <- sapply(cohorts$name, getJson)
    return(selectColumnAccordingToResultsModel(cohorts))
  }


getCohortsJsonAndSqlFromWebApi <- function(baseUrl = baseUrl,
                                           cohortSetReference = cohortSetReference,
                                           cohortIds = NULL,
                                           errorMessage = NULL) {
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
  cohortSetReference <- makeBackwardsCompatible(cohortSetReference)
  cohortSetReference$json <- ""
  cohortSetReference$sql <- ""
  
  ParallelLogger::logInfo("Retrieving cohort definitions from WebAPI")
  for (i in 1:nrow(cohortSetReference)) {
    ParallelLogger::logInfo("- Retrieving definitions for cohort ",
                            cohortSetReference$cohortName[i])
    cohortDefinition <-
      ROhdsiWebApi::getCohortDefinition(cohortId = cohortSetReference$webApiCohortId[i],
                                        baseUrl = baseUrl)
    cohortSetReference$json[i] <-
      RJSONIO::toJSON(x = cohortDefinition$expression, digits = 23)
    cohortSetReference$sql[i] <-
      ROhdsiWebApi::getCohortSql(
        cohortDefinition = cohortDefinition,
        baseUrl = baseUrl,
        generateStats = TRUE
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
  columsToInclude <-
    c(columsToInclude, "json" , "sql", "webApiCohortId")
  return(data[, columsToInclude])
}

getCohortsJsonAndSql <- function(packageName = NULL,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
                                 baseUrl = NULL,
                                 cohortSetReference = NULL,
                                 cohortIds = NULL) {
  if (!is.null(packageName)) {
    cohorts <-
      getCohortsJsonAndSqlFromPackage(
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
    cohorts <- getCohortsJsonAndSqlFromWebApi(
      baseUrl = baseUrl,
      cohortSetReference = cohortSetReference,
      cohortIds = cohortIds
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
    packageName = "CohortDiagnostics",
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
  inclusion <- fetchStats(cohortInclusionFile) %>%
    tidyr::replace_na(replace = list(description = ''))
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
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    construction to.
#' @param generateInclusionStats      Compute and store inclusion rule statistics?
#' @param inclusionStatisticsFolder   The folder where the inclusion rule statistics are stored. Can be
#'                                    left NULL if \code{generateInclusionStats = FALSE}.
#' @param createCohortTable           Create the cohort table? If \code{incremental = TRUE} and the table
#'                                    already exists this will be skipped.
#' @param incremental                 Create only cohorts that haven't been created before?
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are kept
#'                                    of which definition has been executed.
#' @return
#' A data frame with cohort counts
#'
#' @export
instantiateCohortSet <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 vocabularyDatabaseSchema = cdmDatabaseSchema,
                                 tempEmulationSchema = NULL,
                                 oracleTempSchema = NULL,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortIds = NULL,
                                 packageName = NULL,
                                 cohortToCreateFile = "settings/CohortsToCreate.csv",
                                 baseUrl = NULL,
                                 cohortSetReference = NULL,
                                 generateInclusionStats = FALSE,
                                 inclusionStatisticsFolder = NULL,
                                 createCohortTable = TRUE,
                                 incremental = FALSE,
                                 incrementalFolder = NULL) {
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
  
  cohorts <- getCohortsJsonAndSql(
    packageName = packageName,
    cohortToCreateFile = cohortToCreateFile,
    baseUrl = baseUrl,
    cohortSetReference = cohortSetReference,
    cohortIds = cohortIds
  )
  
  if (incremental) {
    cohorts$checksum <- computeChecksum(cohorts$sql)
    recordKeepingFile <-
      file.path(incrementalFolder, "InstantiatedCohorts.csv")
  }
  
  if (generateInclusionStats) {
    createTempInclusionStatsTables(connection, tempEmulationSchema, cohorts)
  }
  
  instantiatedCohortIds <- c()
  for (i in 1:nrow(cohorts)) {
    if (!incremental || isTaskRequired(
      cohortId = cohorts$cohortId[i],
      checksum = cohorts$checksum[i],
      recordKeepingFile = recordKeepingFile
    )) {
      ParallelLogger::logInfo(
        "Instantiation cohort ",
        cohorts$cohortName[i],
        " (Cohort id: ",
        cohorts$cohortId[i],
        ")"
      )
      sql <- cohorts$sql[i]
      .warnMismatchSqlInclusionStats(sql, generateInclusionStats = generateInclusionStats)
      if (generateInclusionStats) {
        sql <- SqlRender::render(
          sql,
          cdm_database_schema = cdmDatabaseSchema,
          vocabulary_database_schema = vocabularyDatabaseSchema,
          target_database_schema = cohortDatabaseSchema,
          target_cohort_table = cohortTable,
          target_cohort_id = cohorts$cohortId[i],
          results_database_schema.cohort_inclusion = "#cohort_inclusion",
          results_database_schema.cohort_inclusion_result = "#cohort_inc_result",
          results_database_schema.cohort_inclusion_stats = "#cohort_inc_stats",
          results_database_schema.cohort_summary_stats = "#cohort_summary_stats"
        )
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
        sql <- SqlRender::render(
          sql,
          cdm_database_schema = cdmDatabaseSchema,
          vocabulary_database_schema = vocabularyDatabaseSchema,
          target_database_schema = cohortDatabaseSchema,
          target_cohort_table = cohortTable,
          target_cohort_id = cohorts$cohortId[i]
        )
      }
      sql <- SqlRender::translate(sql,
                                  targetDialect = connection@dbms,
                                  tempEmulationSchema = tempEmulationSchema)
      DatabaseConnector::executeSql(connection, sql)
      instantiatedCohortIds <-
        c(instantiatedCohortIds, cohorts$cohortId[i])
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
      cohortId = cohorts$cohortId,
      checksum = cohorts$checksum,
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
        packageName = "CohortDiagnostics",
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
