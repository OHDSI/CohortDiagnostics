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


#' Run concept set diagnostics
#'
#' @description
#' Runs concept set diagnostics on a set of cohorts. For index event breakdown,
#' the cohorts need to be instantiated.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template VocabularyDatabaseSchema
#'
#' @template CohortDatabaseSchema
#'
#' @template TempEmulationSchema
#'
#' @param    cohorts                 A dataframe object with required fields cohortId, sql, json, cohortName
#'
#' @template CohortTable
#'
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    diagnostics to.
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events? This is executed on
#'                                    instantiated cohorts only.
#' @export
runConceptSetDiagnostics <- function(connection = NULL,
                                     connectionDetails = NULL,
                                     tempEmulationSchema = NULL,
                                     cdmDatabaseSchema,
                                     vocabularyDatabaseSchema = cdmDatabaseSchema,
                                     cohorts,
                                     cohortIds = NULL,
                                     cohortDatabaseSchema = NULL,
                                     cohortTable = NULL,
                                     runIncludedSourceConcepts,
                                     runOrphanConcepts,
                                     runBreakdownIndexEvents) {
  ParallelLogger::logInfo(" - Starting concept set diagnostics")
  startConceptSetDiagnostics <- Sys.time()
  if (length(cohortIds) == 0) {
    return(NULL)
  }
  
  if (all(is.null(connectionDetails),
          is.null(connection))) {
    stop('Please provide either connection or connectionDetails to connect to database.')
  }
  
  # Set up connection to server----
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
  }
  
  # Create concept table----
  ParallelLogger::logTrace(" - Creating concept ID table for tracking concepts used in diagnostics")
  sqlConceptTable <-
    "IF OBJECT_ID('tempdb..#concept_ids', 'U') IS NOT NULL
                      	DROP TABLE #concept_ids;
                      CREATE TABLE #concept_ids (concept_id INT);"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlConceptTable,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  # Cohorts to run the concept set diagnostics----
  if (!is.null(cohortIds)) {
    subset <- cohorts %>%
      dplyr::filter(.data$cohortId %in% cohortIds)
  }
  if (nrow(subset) == 0) {
    ParallelLogger::logInfo(" - No cohorts to run concept set diagnostics. Exiting concept set diagnostics.")
    return(NULL)
  }
  
  # Get concept sets metadata----
  conceptSets <- combineConceptSetsFromCohorts(subset)
  if (is.null(conceptSets)) {
    ParallelLogger::logInfo(
      " - Cohorts being diagnosed does not have concept ids. Exiting concept set diagnostics."
    )
    return(NULL)
  }
  
  uniqueConceptSets <-
    conceptSets[!duplicated(conceptSets$uniqueConceptSetId),] %>%
    dplyr::select(-.data$cohortId,-.data$conceptSetId)
  
  # andromeda object ----
  conceptSetDiagnosticsResults <- Andromeda::andromeda()
  
  # Instantiate (resolve) unique concept sets----
  ParallelLogger::logInfo(" - Resolving concept sets found in cohorts.")
  resolveConceptSetsToTable(
    uniqueConceptSets = uniqueConceptSets,
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    conceptSetsTable = "#inst_concept_sets"
  )
  conceptSetDiagnosticsResults$resolvedConceptIds <-
    renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT DISTINCT * FROM #inst_concept_sets;",
      tempEmulationSchema = tempEmulationSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::rename(uniqueConceptSetId = .data$codesetId) %>%
    dplyr::inner_join(conceptSets %>% dplyr::distinct(),
                      by = "uniqueConceptSetId") %>%
    dplyr::select(.data$cohortId,
                  .data$conceptSetId,
                  .data$conceptId) %>%
    dplyr::distinct()
  # keeping track
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql =  "INSERT INTO #concept_ids
            SELECT DISTINCT concept_id
            FROM #inst_concept_sets;",
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  # Excluded concepts ----
  ParallelLogger::logInfo(" - Identifying concept ids that were excluded in concept set expression.")
  conceptSetWithExclude <- list()
  for (i in (1:nrow(cohorts))) {
    cohortExpression <- cohorts$json[[i]] %>%
      RJSONIO::fromJSON(digits = 23)
    conceptSetDetailsFromCohortDefinition <-
      getConceptSetDetailsFromCohortDefinition(cohortExpression)
    conceptSetWithExclude[[i]] <-
      conceptSetDetailsFromCohortDefinition$conceptSetExpressionDetails %>%
      dplyr::mutate(cohortId = cohorts$cohortId[[i]]) %>%
      dplyr::rename(conceptSetId = .data$id)
  }
  conceptSetWithExclude <-
    dplyr::bind_rows(conceptSetWithExclude) %>%
    dplyr::inner_join(
      conceptSets %>%
        dplyr::select(
          .data$cohortId,
          .data$conceptSetId,
          .data$uniqueConceptSetId
        ),
      by = c("cohortId", "conceptSetId")
    )
  conceptSetWithExclude <-
    conceptSetWithExclude[!duplicated(conceptSetWithExclude$uniqueConceptSetId),] %>%
    dplyr::filter(.data$isExcluded == TRUE) %>%
    dplyr::select(.data$uniqueConceptSetId,
                  .data$conceptId,
                  .data$includeDescendants)
  conceptIdsWithOutDescendantsInExclude <-
    conceptSetWithExclude %>%
    dplyr::filter(.data$includeDescendants != TRUE) %>%
    dplyr::select(.data$conceptId) %>%
    dplyr::distinct() %>%
    dplyr::pull()
  conceptIdWithDescendants <- dplyr::tibble()
  if (length(conceptIdsWithOutDescendantsInExclude) > 0) {
    sql <- "INSERT INTO #concept_ids
          SELECT DISTINCT CONCEPT_ID
          FROM @vocabulary_database_schema.concept
          WHERE CONCEPT_ID IN (@concept_ids);"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      concept_ids = conceptIdsWithOutDescendantsInExclude,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema
    )
    conceptIdWithDescendants <-
      dplyr::tibble(conceptId = conceptIdsWithOutDescendantsInExclude,
                    descendantConceptId = conceptIdsWithOutDescendantsInExclude)
  }
  conceptIdsWithDescendantsInExclude <- conceptSetWithExclude %>%
    dplyr::filter(.data$includeDescendants == TRUE) %>%
    dplyr::select(.data$conceptId) %>%
    dplyr::distinct() %>%
    dplyr::pull()
  if (length(conceptIdsWithDescendantsInExclude) > 0) {
    sqlConceptAncestor <-
      "IF OBJECT_ID('tempdb..#excluded_descendants', 'U') IS NOT NULL
                      	        DROP TABLE #excluded_descendants;
                          SELECT DISTINCT ANCESTOR_CONCEPT_ID CONCEPT_ID,
                          	DESCENDANT_CONCEPT_ID
                          INTO #excluded_descendants
                          FROM @vocabulary_database_schema.concept_ancestor
                          WHERE ANCESTOR_CONCEPT_ID IN (@concept_ids);

                          INSERT INTO #concept_ids
                          SELECT DISTINCT DESCENDANT_CONCEPT_ID
                          FROM #excluded_descendants;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sqlConceptAncestor,
      concept_ids = conceptIdsWithDescendantsInExclude,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema
    )
    data <-
      renderTranslateQuerySql(
        sql = "SELECT * FROM #excluded_descendants;",
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        snakeCaseToCamelCase = TRUE
      )
    conceptIdWithDescendants <-
      dplyr::bind_rows(conceptIdWithDescendants,
                       data) %>%
      dplyr::distinct()
  }
  conceptSetDiagnosticsResults$excludedConceptsInConceptSet <- NULL
  if (nrow(conceptIdWithDescendants) > 0) {
    conceptSetDiagnosticsResults$excludedConceptsInConceptSet <-
      conceptIdWithDescendants %>%
      dplyr::inner_join(conceptSetWithExclude, by = "conceptId") %>%
      dplyr::select(
        .data$cohortId,
        .data$conceptSetId,
        .data$conceptId,
        .data$descendantConceptId
      ) %>%
      dplyr::distinct()
  }
  
  if (runBreakdownIndexEvents) {
    # Index event breakdown ----
    ParallelLogger::logInfo(" - Breaking down index events.")
    startBreakdownEvents <- Sys.time()
    domains <-
      readr::read_csv(
        system.file("csv", "domains.csv", package = "CohortDiagnostics"),
        col_types = readr::cols(),
        guess_max = min(1e7)
      )
    ParallelLogger::logInfo("  - Starting index event breakdown")
    conceptSetDiagnosticsResults$indexEventBreakdown <-
      getBreakdownIndexEvents(
        cohortIds = subset$cohortId,
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        conceptIdUniverse = "#concept_ids"
      )
    ParallelLogger::logInfo("  - Starting concept co-occurrence.")
    conceptSetDiagnosticsResults$indexDateConceptCooccurrence <-
      getIndexDateConceptCooccurrence(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        cohortIds = subset$cohortId,
        conceptIdUniverse = "#concept_ids"
      )
  }
  
  if (runOrphanConcepts) {
    # Orphan concepts ----
    ParallelLogger::logInfo("Finding orphan concepts")
    startOrphanCodes <- Sys.time()
    orphanConcepts <- findOrphanConcepts(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      instantiatedCodeSets = "#inst_concept_sets",
      orphanConceptTable = "#orphan_concepts",
      conceptIdUniverse = "#concept_ids"
    )
    
    orphanCodes <- orphanConcepts %>%
      dplyr::inner_join(conceptSetDiagnosticsResults$resolvedConceptIds %>%
                          dplyr::collect(),
                        by = "conceptId") %>%
      dplyr::select(.data$cohortId,
                    .data$conceptSetId,
                    .data$orphanConceptId) %>%
      dplyr::distinct() %>%
      dplyr::anti_join(
        conceptSetDiagnosticsResults$resolvedConceptIds %>%
          dplyr::collect() %>%
          dplyr::rename(orphanConceptId = .data$conceptId),
        by = c("cohortId", "conceptSetId", "orphanConceptId")
      ) %>%
      dplyr::rename(conceptId = .data$orphanConceptId) %>%
      dplyr::select(.data$cohortId, .data$conceptSetId, .data$conceptId)
    
    delta <- Sys.time() - startOrphanCodes
    ParallelLogger::logInfo("Finding orphan concepts took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }

  # get concept count----
  conceptSetDiagnosticsResults$conceptCount <- getConceptsInDataSource(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    conceptIdUniverse = "#concept_ids"
  )
  
  ParallelLogger::logInfo("Retrieving concept information")
  exportedVocablary <- exportConceptInformation(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    conceptIdTable = "#concept_ids"
  )
  
  # Drop temporary tables
  ParallelLogger::logTrace("Dropping temp concept set table")
  sql <-
    "TRUNCATE TABLE #inst_concept_sets; DROP TABLE #inst_concept_sets;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  if (runIncludedSourceConcepts  || runOrphanConcepts) {
    ParallelLogger::logTrace("Dropping temp concept count table")
    countTable <- "#concept_counts"
    
    sql <- "TRUNCATE TABLE @count_table; DROP TABLE @count_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      tempEmulationSchema = tempEmulationSchema,
      count_table = countTable,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  if (is.null(exportedVocablary)) {
    exportedVocablary <- list()
  }
  if (!is.null(conceptSetDiagnosticsResults$resolvedConceptIds)) {
    exportedVocablary$resolvedConceptIds = conceptSetDiagnosticsResults$resolvedConceptIds
  }
  if (!is.null(includedSourceCodes)) {
    exportedVocablary$includedSourceCodes = includedSourceCodes
  }
  if (!is.null(conceptSetDiagnosticsResults$indexEventBreakdown)) {
    exportedVocablary$indexEventBreakdown = conceptSetDiagnosticsResults$indexEventBreakdown
  }
  if (!is.null(orphanCodes)) {
    exportedVocablary$orphanCodes = orphanCodes
  }
  if (!is.null(conceptSets)) {
    exportedVocablary$conceptSets = conceptSets
  }
  if (!is.null(conceptSetDiagnosticsResults$indexDateConceptCooccurrence)) {
    exportedVocablary$indexDateConceptCooccurrence = conceptSetDiagnosticsResults$indexDateConceptCooccurrence
  }
  
  delta <- Sys.time() - startConceptSetDiagnostics
  ParallelLogger::logInfo("Running concept set diagnostics took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
  
  return(exportedVocablary)
}


##################### private function #########################

extractConceptSetsSqlFromCohortSql <- function(cohortSql) {
  if (length(cohortSql) > 1) {
    stop("Please check if more than one cohort SQL was provided.")
  }
  sql <- gsub("with primary_events.*", "", cohortSql)
  
  # Find opening and closing parentheses:
  starts <- stringr::str_locate_all(sql, "\\(")[[1]][, 1]
  ends <- stringr::str_locate_all(sql, "\\)")[[1]][, 1]
  
  x <- rep(0, nchar(sql))
  x[starts] <- 1
  x[ends] <- -1
  level <- cumsum(x)
  level0 <- which(level == 0)
  
  subQueryLocations <-
    stringr::str_locate_all(sql, "SELECT [0-9]+ as codeset_id")[[1]]
  subQueryCount <- nrow(subQueryLocations)
  conceptsetSqls <- vector("character", subQueryCount)
  conceptSetIds <- vector("integer", subQueryCount)
  
  temp <- list()
  if (subQueryCount > 0) {
    for (i in 1:subQueryCount) {
      startForSubQuery <- min(starts[starts > subQueryLocations[i, 2]])
      endForSubQuery <- min(level0[level0 > startForSubQuery])
      subQuery <-
        paste(stringr::str_sub(sql, subQueryLocations[i, 1], endForSubQuery),
              "C")
      conceptsetSqls[i] <- subQuery
      conceptSetIds[i] <- stringr::str_replace(
        subQuery,
        pattern = stringr::regex(
          pattern = "SELECT ([0-9]+) as codeset_id.*",
          ignore_case = TRUE,
          multiline = TRUE,
          dotall = TRUE
        ),
        replacement = "\\1"
      ) %>%
        utils::type.convert()
      temp[[i]] <- tidyr::tibble(conceptSetId = conceptSetIds[i],
                                 conceptSetSql = conceptsetSqls[i])
    }
  } else {
    temp <- dplyr::tibble()
  }
  return(dplyr::bind_rows(temp))
}


extractConceptSetsJsonFromCohortJson <- function(cohortJson) {
  cohortDefinition <-
    RJSONIO::fromJSON(content = cohortJson, digits = 23)
  if ("expression" %in% names(cohortDefinition)) {
    expression <- cohortDefinition$expression
  } else {
    expression <- cohortDefinition
  }
  conceptSetExpression <- list()
  if (length(expression$ConceptSets) > 0) {
    for (i in (1:length(expression$ConceptSets))) {
      conceptSetExpression[[i]] <-
        tidyr::tibble(
          conceptSetId = expression$ConceptSets[[i]]$id,
          conceptSetName = expression$ConceptSets[[i]]$name,
          conceptSetExpression = expression$ConceptSets[[i]]$expression$items %>% RJSONIO::toJSON(digits = 23)
        )
    }
  } else {
    conceptSetExpression <- dplyr::tibble()
  }
  return(dplyr::bind_rows(conceptSetExpression))
}

combineConceptSetsFromCohorts <- function(cohorts) {
  #cohorts should be a dataframe with at least cohortId, sql and json
  
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertDataFrame(x = cohorts,
                             min.cols = 4,
                             add = errorMessage)
  checkmate::assertNames(
    x = colnames(cohorts),
    must.include = c('cohortId', 'sql', 'json', 'cohortName')
  )
  checkmate::reportAssertions(errorMessage)
  checkmate::assertDataFrame(
    x = cohorts %>% dplyr::select(.data$cohortId,
                                  .data$sql,
                                  .data$json,
                                  .data$cohortName),
    any.missing = FALSE,
    min.cols = 4,
    add = errorMessage
  )
  checkmate::reportAssertions(errorMessage)
  
  conceptSets <- list()
  conceptSetCounter <- 0
  
  for (i in (1:nrow(cohorts))) {
    cohort <- cohorts[i,]
    sql <-
      extractConceptSetsSqlFromCohortSql(cohortSql = cohort$sql)
    json <-
      extractConceptSetsJsonFromCohortJson(cohortJson = cohort$json)
    
    if (nrow(sql) == 0 || nrow(json) == 0) {
      ParallelLogger::logInfo(
        "Cohort Definition expression does not have a concept set expression. ",
        "Skipping Cohort: ",
        cohort$cohortName
      )
    } else {
      if (!length(sql$conceptSetId %>% unique()) == length(json$conceptSetId %>% unique())) {
        stop(
          "Mismatch in concept set IDs between SQL and JSON for cohort ",
          cohort$cohortFullName
        )
      }
      if (length(sql) > 0 && length(json) > 0) {
        conceptSetCounter <- conceptSetCounter + 1
        conceptSets[[conceptSetCounter]] <-
          tidyr::tibble(cohortId = cohort$cohortId,
                        dplyr::inner_join(x = sql, y = json, by = "conceptSetId"))
      }
    }
  }
  if (length(conceptSets) == 0) {
    return(NULL)
  }
  conceptSets <- dplyr::bind_rows(conceptSets) %>%
    dplyr::arrange(.data$cohortId, .data$conceptSetId)
  
  uniqueConceptSets <- conceptSets %>%
    dplyr::select(.data$conceptSetExpression) %>%
    dplyr::distinct() %>%
    dplyr::mutate(uniqueConceptSetId = dplyr::row_number())
  
  conceptSets <- conceptSets %>%
    dplyr::inner_join(uniqueConceptSets, by = "conceptSetExpression") %>%
    dplyr::distinct() %>%
    dplyr::relocate(.data$uniqueConceptSetId,
                    .data$cohortId,
                    .data$conceptSetId) %>%
    dplyr::arrange(.data$uniqueConceptSetId,
                   .data$cohortId,
                   .data$conceptSetId)
  return(conceptSets)
}


mergeTempTables <-
  function(connection,
           tableName,
           tempTables,
           tempEmulationSchema) {
    valueString <-
      paste(tempTables, collapse = "\n\n  UNION ALL\n\n  SELECT *\n  FROM ")
    sql <-
      sprintf("SELECT *\nINTO %s\nFROM (\n  SELECT *\n  FROM %s\n) tmp;",
              tableName,
              valueString)
    sql <-
      SqlRender::translate(sql,
                           targetDialect = connection@dbms,
                           tempEmulationSchema = tempEmulationSchema)
    DatabaseConnector::executeSql(connection,
                                  sql,
                                  progressBar = FALSE,
                                  reportOverallTime = FALSE)
    
    # Drop temp tables:
    for (tempTable in tempTables) {
      sql <-
        sprintf("TRUNCATE TABLE %s;\nDROP TABLE %s;", tempTable, tempTable)
      sql <-
        SqlRender::translate(sql,
                             targetDialect = connection@dbms,
                             tempEmulationSchema = tempEmulationSchema)
      DatabaseConnector::executeSql(connection,
                                    sql,
                                    progressBar = FALSE,
                                    reportOverallTime = FALSE)
    }
  }

# function: resolveConceptSetsToTable ----
resolveConceptSetsToTable <- function(uniqueConceptSets,
                                      connection,
                                      cdmDatabaseSchema,
                                      vocabularyDatabaseSchema = cdmDatabaseSchema,
                                      tempEmulationSchema,
                                      conceptSetsTable = '#inst_concept_sets') {
  ParallelLogger::logInfo(" - Instantiating concept sets")
  sql <- sapply(split(uniqueConceptSets, 1:nrow(uniqueConceptSets)),
                function(x) {
                  sub(
                    "SELECT [0-9]+ as codeset_id",
                    sprintf("SELECT %s as codeset_id", x$uniqueConceptSetId),
                    x$conceptSetSql
                  )
                })
  
  batchSize <- 100
  tempTables <- c()
  pb <- utils::txtProgressBar(style = 3)
  for (start in seq(1, length(sql), by = batchSize)) {
    utils::setTxtProgressBar(pb, start / length(sql))
    tempTable <-
      paste("#", paste(sample(letters, 20, replace = TRUE), collapse = ""), sep = "")
    tempTables <- c(tempTables, tempTable)
    end <- min(start + batchSize - 1, length(sql))
    sqlSubset <- sql[start:end]
    sqlSubset <- paste(sqlSubset, collapse = "\n\n  UNION ALL\n\n")
    sqlSubset <-
      sprintf("SELECT *\nINTO %s\nFROM (\n %s\n) tmp;",
              tempTable,
              sqlSubset)
    sqlSubset <-
      SqlRender::render(sqlSubset, vocabulary_database_schema = vocabularyDatabaseSchema)
    sqlSubset <- SqlRender::translate(sqlSubset,
                                      targetDialect = connection@dbms,
                                      tempEmulationSchema = tempEmulationSchema)
    DatabaseConnector::executeSql(connection,
                                  sqlSubset,
                                  progressBar = FALSE,
                                  reportOverallTime = FALSE)
  }
  utils::setTxtProgressBar(pb, 1)
  close(pb)
  
  mergeTempTables(
    connection = connection,
    tableName = conceptSetsTable,
    tempTables = tempTables,
    tempEmulationSchema = tempEmulationSchema
  )
}

getCodeSetId <- function(criterion) {
  if (is.list(criterion)) {
    criterion$CodesetId
  } else if (is.vector(criterion)) {
    return(criterion["CodesetId"])
  } else {
    return(NULL)
  }
}

getCodeSetIds <- function(criterionList) {
  codeSetIds <- lapply(criterionList, getCodeSetId)
  codeSetIds <- do.call(c, codeSetIds)
  if (is.null(codeSetIds)) {
    return(NULL)
  } else {
    return(dplyr::tibble(domain = names(criterionList), codeSetIds = codeSetIds)
           %>% filter(!is.na(codeSetIds)))
  }
}

# function: exportConceptInformation ----
exportConceptInformation <- function(connection = NULL,
                                     cdmDatabaseSchema,
                                     tempEmulationSchema,
                                     conceptIdTable = "#concept_ids",
                                     vocabularyTableNames = c(
                                       "concept",
                                       "concept_ancestor",
                                       "concept_class",
                                       "concept_relationship",
                                       "concept_synonym",
                                       "domain",
                                       "relationship",
                                       "vocabulary"
                                     )) {
  start <- Sys.time()
  if (is.null(connection)) {
    warning('No connection provided')
  }
  
  tablesInCdmDatabaseSchema <-
    tolower(DatabaseConnector::getTableNames(connection, cdmDatabaseSchema))
  vocabularyTablesInCdmDatabaseSchema <-
    tablesInCdmDatabaseSchema[tablesInCdmDatabaseSchema %in% vocabularyTableNames]
  
  if (length(vocabularyTablesInCdmDatabaseSchema) == 0) {
    stop("Vocabulary tables not found in ", cdmDatabaseSchema)
  }
  sql <- "SELECT DISTINCT concept_id FROM @unique_concept_id_table;"
  uniqueConceptIds <-
    renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      unique_concept_id_table = conceptIdTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema
    )[, 1]
  if (length(uniqueConceptIds) == 0) {
    ParallelLogger::logInfo("No concept IDs in cohorts. No concept information exported.")
    return(NULL)
  }
  
  vocabularyTablesData <- list()
  for (vocabularyTable in vocabularyTablesInCdmDatabaseSchema) {
    ParallelLogger::logInfo("- Retrieving concept information from vocabulary table '",
                            vocabularyTable,
                            "'")
    if (vocabularyTable %in% c("concept", "concept_synonym")) {
      sql <- "SELECT a.* FROM @cdm_database_schema.@table a
        INNER JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b
          ON a.concept_id = b.concept_id;"
    } else if (vocabularyTable %in% c("concept_ancestor")) {
      sql <- "SELECT a.* FROM @cdm_database_schema.@table a
        LEFT JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b1
          ON a.ancestor_concept_id = b1.concept_id
        LEFT JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b2
          ON a.descendant_concept_id = b2.concept_id
        WHERE b1.concept_id IS NOT NULL or b2.concept_id IS NOT NULL;"
    } else if (vocabularyTable %in% c("concept_relationship")) {
      sql <- "SELECT a.* FROM @cdm_database_schema.@table a
        INNER JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b1
          ON a.concept_id_1 = b1.concept_id
        INNER JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b2
          ON a.concept_id_2 = b2.concept_id
        WHERE b1.concept_id IS NOT NULL or b2.concept_id IS NOT NULL;"
    }
    if (vocabularyTable %in% c("concept",
                               "concept_synonym",
                               "concept_ancestor",
                               "concept_relationship")) {
      data <-
        renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          cdm_database_schema = cdmDatabaseSchema,
          unique_concept_id_table = conceptIdTable,
          table = vocabularyTable,
          snakeCaseToCamelCase = TRUE
        )
    } else if (vocabularyTable %in% c("domain",
                                      "relationship",
                                      "vocabulary",
                                      "concept_class")) {
      sql <- "SELECT * FROM @cdm_database_schema.@table;"
      data <-
        renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          cdm_database_schema = cdmDatabaseSchema,
          table = vocabularyTable,
          snakeCaseToCamelCase = TRUE
        ) %>%
        dplyr::tibble()
    }
    data <-
      CohortDiagnostics:::.replaceNaInDataFrameWithEmptyString(data)
    vocabularyTablesData[[vocabularyTable]] <- data
  }
  delta <- Sys.time() - start
  ParallelLogger::logInfo("Retrieving concept information took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
  return(vocabularyTablesData)
}

# function: findOrphanConcepts ----
findOrphanConcepts <- function(connectionDetails = NULL,
                               connection = NULL,
                               cdmDatabaseSchema,
                               vocabularyDatabaseSchema = cdmDatabaseSchema,
                               tempEmulationSchema = NULL,
                               instantiatedCodeSets = "#inst_concept_sets",
                               orphanConceptTable = '#recommended_concepts',
                               conceptIdUniverse = "#concept_ids") {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql(
    "OrphanCodes.sql",
    packageName = "CohortDiagnostics",
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    vocabulary_database_schema = vocabularyDatabaseSchema,
    orphan_concept_table = orphanConceptTable,
    instantiated_code_sets = instantiatedCodeSets,
    concept_id_universe = conceptIdUniverse
  )
  DatabaseConnector::executeSql(connection, sql)
  ParallelLogger::logTrace("- Fetching orphan concepts from server")
  sql <- "SELECT * FROM @orphan_concept_table;"
  orphanConcepts <-
    renderTranslateQuerySql(
      sql = sql,
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      orphan_concept_table = orphanConceptTable,
      snakeCaseToCamelCase = TRUE
    )
  ParallelLogger::logTrace("- Dropping orphan temp tables")
  sql <-
    "IF OBJECT_ID('tempdb..@orphan_concept_table', 'U') IS NOT NULL
    	DROP TABLE @orphan_concept_table;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    orphan_concept_table = orphanConceptTable,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  return(orphanConcepts)
}

# function: getConceptsInDataSource ----
### Concepts in data source -----
getConceptsInDataSource <- function(connection,
                                    cdmDatabaseSchema,
                                    tempEmulationSchema,
                                    conceptIdUniverse = "#concept_ids") {
  ParallelLogger::logInfo(" - Starting counts of concepts in datasource.")
  ParallelLogger::logTrace(" - Reading domains.csv")
  domains <-
    readr::read_csv(
      system.file("csv", "domains.csv", package = "CohortDiagnostics"),
      col_types = readr::cols(),
      guess_max = min(1e7)
    )
  sql <- "SELECT @domain_concept_id concept_id,
        {@domain_source_concept_id != ''} ? {
                	@domain_source_concept_id source_concept_id,
        }
                	YEAR(@domain_start_date) event_year,
                	MONTH(@domain_start_date) event_month,
                	COUNT_BIG(DISTINCT person_id) concept_subjects,
                	COUNT_BIG(*) concept_count
                FROM @cdm_database_schema.@domain_table
                INNER JOIN (
                	SELECT DISTINCT concept_id
                	FROM @concept_id_universe
                	) c
                	ON @domain_concept_id = concept_id
        {@domain_source_concept_id != ''} ? {
                		OR @domain_source_concept_id = concept_id
        }
                GROUP BY @domain_concept_id,
        {@domain_source_concept_id != ''} ? {
                	@domain_source_concept_id,
        }
                	YEAR(@domain_start_date),
                	MONTH(@domain_start_date);"
  
  conceptsInDataSource <- list()
  for (i in (1:nrow(domains))) {
    rowData <- domains[i, ]    
    ParallelLogger::logTrace(paste0(
      "   - Counting concepts in ",
      rowData$domainTable
    ))
    conceptsInDataSource[[i]] <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        tempEmulationSchema = tempEmulationSchema,
        domain_concept_id = rowData$domainConceptId,
        domain_source_concept_id = rowData$domainSourceConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        domain_table = rowData$domainTable,
        domain_start_date = rowData$domainStartDate,
        concept_id_universe = conceptIdUniverse
      )
  }
  conceptsInDataSource <- dplyr::bind_rows(conceptsInDataSource)
  return(conceptsInDataSource)
}

# function: getBreakdownIndexEvents ----
getBreakdownIndexEvents <- function(cohortIds,
                                    connection,
                                    tempEmulationSchema,
                                    conceptIdUniverse = "#concept_ids") {
  ParallelLogger::logTrace("  - Reading domains.csv")
  domains <-
    readr::read_csv(
      system.file("csv", "domains.csv", package = "CohortDiagnostics"),
      col_types = readr::cols(),
      guess_max = min(1e7)
    )
  sql <- "SELECT cohort_definition_id cohort_id,
              	'@domain_table' AS domain_table,
              	'@domain_concept_id' AS domain_field,
              	@domain_concept_id AS concept_id,
              	COUNT(*) AS concept_count,
              	COUNT(DISTINCT subject_id) AS subject_count
              FROM @cohort_database_schema.@cohort_table
              INNER JOIN @cdm_database_schema.@domain_table
              	ON subject_id = person_id
              		AND cohort_start_date = @domain_start_date
              INNER JOIN (select distinct concept_id from @concept_id_universe) a
              	ON @domain_concept_id = concept_id
              WHERE cohort_definition_id IN (@cohort_id)
              GROUP BY @domain_concept_id,
                  cohort_definition_id;"
  
  breakdownDataStandard <- list()
  for (i in (1:nrow(domains))) {
    rowData <- domains[i,]
    ParallelLogger::logTrace(paste0(
      "  - Working on ",
      rowData$domainTable,
      ".",
      rowData$domainConceptId
    ))
    breakdownDataStandard[[i]] <- renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      domain_table = rowData$domainTable,
      domain_concept_id = rowData$domainConceptId,
      cdm_database_schema = cdmDatabaseSchema,
      cohort_database_schema = cohortDatabaseSchema,
      domain_start_date = rowData$domainStartDate,
      concept_id_universe = conceptIdUniverse,
      cohort_id = cohortIds,
      cohort_table = cohortTable,
      snakeCaseToCamelCase = TRUE
    )
  }
  breakdownDataStandard <- dplyr::bind_rows(breakdownDataStandard)
  
  breakdownDataNonStandard <- list()
  for (i in (1:nrow(domains))) {
    rowData <- domains[i,]
    if (all(
      nchar(rowData$domainSourceConceptId) > 4,!is.na(rowData$domainSourceConceptId)
    )) {
      ParallelLogger::logTrace(
        paste0(
          "  - Working on ",
          rowData$domainTable,
          ".",
          rowData$domainSourceConceptId
        )
      )
      breakdownDataNonStandard[[i]] <- renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        tempEmulationSchema = tempEmulationSchema,
        domain_table = rowData$domainTable,
        domain_concept_id = rowData$domainSourceConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        cohort_database_schema = cohortDatabaseSchema,
        domain_start_date = rowData$domainStartDate,
        concept_id_universe = conceptIdUniverse,
        cohort_id = cohortIds,
        cohort_table = cohortTable,
        snakeCaseToCamelCase = TRUE
      )
    }
  }
  breakdownDataNonStandard <-
    dplyr::bind_rows(breakdownDataNonStandard)
  return(
    dplyr::bind_rows(breakdownDataNonStandard,
                     breakdownDataStandard) %>% dplyr::distinct()
  )
}


# function: getIndexDateConceptCooccurrence ----
### indexDateConceptCooccurrence -----
getIndexDateConceptCooccurrence <- function(connection,
                                            cdmDatabaseSchema,
                                            tempEmulationSchema,
                                            cohortIds,
                                            conceptIdUniverse = "#concept_ids") {
  domains <-
    readr::read_csv(
      system.file("csv", "domains.csv", package = "CohortDiagnostics"),
      col_types = readr::cols(),
      guess_max = min(1e7)
    )
  sqlDdlDrop <-
    "IF OBJECT_ID('tempdb..#concept_cooccurrence', 'U') IS NOT NULL
                	      DROP TABLE #concept_cooccurrence;"
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdlDrop,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  sqlDdl <- "CREATE TABLE #concept_cooccurrence (
                                                    	cohort_id BIGINT,
                                                    	concept_id INT,
                                                    	person_id BIGINT
                                                    	);"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdl,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  sql <- "	INSERT INTO #concept_cooccurrence
                SELECT DISTINCT cohort_definition_id cohort_id,
                	@domain_concept_id concept_id,
                	person_id
                FROM @cohort_database_schema.@cohort_table
                INNER JOIN @cdm_database_schema.@domain_table
                	ON subject_id = person_id
                		AND cohort_start_date = @domain_start_date
                INNER JOIN (select distinct concept_id from @concept_id_universe) u
                	ON @domain_concept_id = concept_id
                WHERE cohort_definition_id IN (@cohortIds);"
  
  for (i in (1:nrow(domains))) {
    rowData <- domains[i, ]
    ParallelLogger::logTrace(paste0(
      "  - Working on ",
      rowData$domainTable,
      ".",
      rowData$domainConceptId
    ))
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      domain_concept_id = rowData$domainConceptId,
      cdm_database_schema = cdmDatabaseSchema,
      cohort_database_schema = cohortDatabaseSchema,
      domain_table = rowData$domainTable,
      domain_start_date = rowData$domainStartDate,
      concept_id_universe = conceptIdUniverse,
      cohortIds = cohortIds,
      cohort_table = cohortTable,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    if (all(
      !is.na(rowData$domainSourceConceptId),
      length(rowData$domainSourceConceptId) > 0
    )) {
      ParallelLogger::logTrace(
        paste0(
          "  - Working on ",
          rowData$domainTable,
          ".",
          rowData$domainSourceConceptId
        )
      )
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        tempEmulationSchema = tempEmulationSchema,
        domain_concept_id = rowData$domainSourceConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        cohort_database_schema = cohortDatabaseSchema,
        domain_table = rowData$domainTable,
        domain_start_date = rowData$domainStartDate,
        concept_id_universe = conceptIdUniverse,
        cohortIds = cohortIds,
        cohort_table = cohortTable,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
  }
  sqlCooccurrence <- "WITH cooccurrence
                          AS (
                          	SELECT DISTINCT *
                          	FROM #concept_cooccurrence
                          	)
                          SELECT a.cohort_id,
                          	a.concept_id,
                          	b.concept_id co_concept_id,
                          	count(*) count_value
                          FROM cooccurrence a
                          INNER JOIN cooccurrence b ON a.cohort_id = b.cohort_id
                          	AND a.person_id = b.person_id
                          	AND b.concept_id > a.concept_id
                          GROUP BY a.cohort_id,
                          	a.concept_id,
                          	b.concept_id;"
  
  indexDateConceptCooccurrence <-
    renderTranslateQuerySql(
      sql = sqlCooccurrence,
      connection = connection,
      snakeCaseToCamelCase = TRUE
    )
  
  sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM #concept_cooccurrence;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    concept_id_table = "#concept_ids",
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdlDrop,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  return(indexDateConceptCooccurrence)
}
