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
#' @param keepCustomConceptId         (Optional) Default FALSE. Do you want to keep concept id above 2 billion.
#'                                    Per OMOP conventions any conceptId >= 2 billion are considered site specific
#'                                    custom value that are not shipped as part of default OMOP vocabulary tables.
#'
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    diagnostics to.
#' @export
runConceptSetDiagnostics <- function(connection = NULL,
                                     connectionDetails = NULL,
                                     tempEmulationSchema = NULL,
                                     cdmDatabaseSchema,
                                     vocabularyDatabaseSchema = cdmDatabaseSchema,
                                     cohorts,
                                     cohortIds = NULL,
                                     cohortDatabaseSchema = NULL,
                                     keepCustomConceptId = FALSE,
                                     cohortTable = NULL) {
  ParallelLogger::logTrace(" - Running concept set diagnostics")
  startConceptSetDiagnostics <- Sys.time()
  if (length(cohortIds) == 0) {
    return(NULL)
  }
  if (all(is.null(connectionDetails),
          is.null(connection))) {
    stop('Please provide either connection or connectionDetails to connect to database.')
  }
  # Set up connection to server----
  ParallelLogger::logTrace(" - Setting up connection")
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
  }
  
  # Create concept tracking table----
  #For some domains (e.g. Vist download all vocabulary - as it is used in visit context etc)
  ParallelLogger::logTrace(" - Creating concept ID table for tracking concepts used in diagnostics")
  sql <-
    "IF OBJECT_ID('tempdb..#concept_tracking', 'U') IS NOT NULL
                      	DROP TABLE #concept_tracking;
                      CREATE TABLE #concept_tracking (concept_id INT);
                      SELECT DISTINCT CONCEPT_ID
                      FROM @vocabulary_database_schema.concept
                      WHERE domain_id IN ('Visit', 'Ethnicity', 'Race', 'Gender', 'Place of Service', 'Type Concept');
  "
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    vocabulary_database_schema = vocabularyDatabaseSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  # Cohorts to run the concept set diagnostics----
  if (!is.null(cohortIds)) {
    subset <- cohorts %>%
      dplyr::filter(.data$cohortId %in% cohortIds)
  }
  if (nrow(subset) == 0) {
    ParallelLogger::logInfo("  - No cohorts to run concept set diagnostics. Exiting concept set diagnostics.")
    return(NULL)
  }
  
  # andromeda object ----
  conceptSetDiagnosticsResults <- Andromeda::andromeda()
  
  # Get concept sets metadata----
  conceptSetDiagnosticsResults$conceptSets <-
    combineConceptSetsFromCohorts(subset)
  if (is.null(conceptSetDiagnosticsResults$conceptSets)) {
    ParallelLogger::logInfo(
      "  - Cohorts being diagnosed does not have concept ids. Exiting concept set diagnostics."
    )
    return(NULL)
  }
  conceptSets <-
    conceptSetDiagnosticsResults$conceptSets %>% dplyr::collect()
  uniqueConceptSets <-
    conceptSets[!duplicated(conceptSets$uniqueConceptSetId), ] %>%
    dplyr::select(-.data$cohortId, -.data$conceptSetId)
 
  ParallelLogger::logTrace(
    paste0(
      " - Note: There are ",
      scales::comma(length(uniqueConceptSets)),
      " unique concept set ids in ",
      scales::comma(
        conceptSetDiagnosticsResults$conceptSets %>% dplyr::summarise(n = dplyr::n()) %>% dplyr::pull(.data$n)
      ),
      " concept sets in all cohort definitions."
    )
  )
  
  # Optimize unique concept set----
  ParallelLogger::logInfo("  - Optimizing concept sets found in cohorts.")
  optimizedConceptSet <- list()
  for (i in (1:nrow(uniqueConceptSets))) {
    uniqueConceptSet <- uniqueConceptSets[i,]
    conceptSetExpression <- RJSONIO::fromJSON(uniqueConceptSet$conceptSetExpression)
    optimizationRecommendation <- 
      getOptimizationRecommendationForConceptSetExpression(conceptSetExpression = conceptSetExpression, 
                                                           connection = connection, 
                                                           vocabularyDatabaseSchema = vocabularyDatabaseSchema, 
                                                           tempEmulationSchema = tempEmulationSchema)
    if (!is.null(optimizationRecommendation)) {
      optimizationRecommendation <- optimizationRecommendation %>% 
        dplyr::mutate(uniqueConceptSetId = uniqueConceptSet$uniqueConceptSetId) %>% 
        dplyr::inner_join(conceptSets %>% 
                            dplyr::select(.data$uniqueConceptSetId,
                                          .data$cohortId,
                                          .data$conceptSetId),
                          by = "uniqueConceptSetId")
      optimizedConceptSet[[i]] <- optimizationRecommendation %>% 
        dplyr::select(.data$cohortId,
                      .data$conceptSetId,
                      .data$conceptId,
                      .data$excluded,
                      .data$removed
                      ) %>% 
        dplyr::distinct()
    }
  }
  conceptSetDiagnosticsResults$conceptSetsOptimized <- dplyr::bind_rows(optimizedConceptSet) %>% 
    dplyr::distinct() %>% 
    dplyr::tibble()
  rm("conceptSets")
  
  # Instantiate (resolve) unique concept sets----
  ParallelLogger::logInfo("  - Resolving concept sets found in cohorts.")
  conceptSetDiagnosticsResults$conceptResolved <-
    resolveConceptSets(
      uniqueConceptSets = uniqueConceptSets,
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      conceptSetsTable = "#resolved_concept_set",
      conceptTrackingTable = "#concept_tracking",
      dropConceptSetsTable = FALSE
    )
  if (!is.null(conceptSetDiagnosticsResults$conceptResolved)) {
    if (!keepCustomConceptId) {
      conceptSetDiagnosticsResults$conceptResolved <-
        conceptSetDiagnosticsResults$conceptResolved %>%
        dplyr::filter(.data$conceptId < 200000000)
    }
    conceptSetDiagnosticsResults$conceptResolved <-
      conceptSetDiagnosticsResults$conceptResolved %>%
      dplyr::inner_join(conceptSetDiagnosticsResults$conceptSets %>% dplyr::distinct(),
                        by = "uniqueConceptSetId") %>%
      dplyr::select(.data$cohortId,
                    .data$conceptSetId,
                    .data$conceptId) %>%
      dplyr::distinct()
  }
  
  # Excluded concepts ----
  ParallelLogger::logInfo("  - Collecting excluded concepts.")
  conceptSetDiagnosticsResults$conceptExcluded <-
    getExcludedConceptSets(
      connection = connection,
      uniqueConceptSets = uniqueConceptSets,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      conceptTrackingTable = "#concept_tracking"
    )
  if (!is.null(conceptSetDiagnosticsResults$conceptExcluded)) {
    if (!keepCustomConceptId) {
      conceptSetDiagnosticsResults$conceptExcluded <-
        conceptSetDiagnosticsResults$conceptExcluded %>%
        dplyr::filter(.data$conceptId < 200000000)
    }
    conceptSetDiagnosticsResults$conceptExcluded <-
      conceptSetDiagnosticsResults$conceptExcluded %>%
      dplyr::inner_join(conceptSetDiagnosticsResults$conceptSets %>% dplyr::distinct(),
                        by = "uniqueConceptSetId") %>%
      dplyr::select(.data$cohortId,
                    .data$conceptSetId,
                    .data$conceptId) %>%
      dplyr::distinct()
  }
  
  # Index event breakdown ----
  startBreakdownEvents <- Sys.time()
  ParallelLogger::logInfo("  - Learning about the breakdown in index events.")
  conceptSetDiagnosticsResults$indexEventBreakdown <-
    getBreakdownIndexEvents(
      cohortIds = subset$cohortId,
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      tempEmulationSchema = tempEmulationSchema,
      conceptIdUniverse = "#concept_tracking"
    )
  if (!keepCustomConceptId) {
    conceptSetDiagnosticsResults$indexEventBreakdown <-
      conceptSetDiagnosticsResults$indexEventBreakdown %>%
      dplyr::filter(.data$conceptId < 200000000)
  }
  ParallelLogger::logInfo("  - Looking for concept co-occurrence on index date.")
  conceptSetDiagnosticsResults$conceptCooccurrence <-
    getIndexDateConceptCooccurrence(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = subset$cohortId,
      conceptIdUniverse = "#concept_tracking"
    )
  if (!keepCustomConceptId) {
    conceptSetDiagnosticsResults$conceptCooccurrence <-
      conceptSetDiagnosticsResults$conceptCooccurrence %>%
      dplyr::filter(.data$conceptId < 200000000)
  }
  
  delta <- Sys.time() - startBreakdownEvents
  ParallelLogger::logTrace("  - Index event breakdown took ",
                           signif(delta, 3),
                           " ",
                           attr(delta, "units"))
  
  # Orphan concepts ----
  ParallelLogger::logInfo("  - Searching for concepts that may have been orphaned.")
  startOrphanCodes <- Sys.time()
  conceptSetDiagnosticsResults$orphanConcept <- getOrphanConcepts(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    instantiatedCodeSets = "#resolved_concept_set",
    conceptIdUniverse = '#concept_tracking'
  )
  if (!keepCustomConceptId) {
    conceptSetDiagnosticsResults$orphanConcept <-
      conceptSetDiagnosticsResults$orphanConcept %>%
      dplyr::filter(.data$conceptId < 200000000)
  }
  conceptSetDiagnosticsResults$orphanConcept <-
    conceptSetDiagnosticsResults$orphanConcept %>%
    dplyr::rename(uniqueConceptSetId = .data$codesetId) %>%
    dplyr::inner_join(
      conceptSetDiagnosticsResults$conceptSets %>%
        dplyr::select(
          .data$uniqueConceptSetId,
          .data$cohortId,
          .data$conceptSetId
        ),
      by = "uniqueConceptSetId"
    ) %>%
    dplyr::select(.data$cohortId,
                  .data$conceptSetId,
                  .data$conceptId) %>%
    dplyr::arrange(.data$cohortId, .data$conceptSetId, .data$conceptId)
  
  delta <- Sys.time() - startOrphanCodes
  ParallelLogger::logTrace("  - Finding orphan concepts took ",
                           signif(delta, 3),
                           " ",
                           attr(delta, "units"))

  # get concept record count----
  ParallelLogger::logInfo("  - Counting concepts in data source.")
  conceptSetDiagnosticsResults$conceptCount <-
    getConceptRecordCount(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      conceptIdUniverse = "#concept_tracking"
    )
  if (!keepCustomConceptId) {
    conceptSetDiagnosticsResults$conceptCount <-
      conceptSetDiagnosticsResults$conceptCount %>%
      dplyr::filter(.data$conceptId < 200000000)
  }

  # get concept mapping----
  ParallelLogger::logInfo("  - Mapping concepts.")
  conceptSetDiagnosticsResults$conceptMapping <-
    getConceptSourceStandardMapping(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      conceptIdUniverse = "#concept_tracking"
    )
  if (!keepCustomConceptId) {
    conceptSetDiagnosticsResults$conceptMapping <-
      conceptSetDiagnosticsResults$conceptMapping %>%
      dplyr::filter(.data$conceptId < 200000000) %>%
      dplyr::filter(is.na(.data$sourceConceptId) ||
                      .data$sourceConceptId < 200000000)
  }
  
  conceptSetDiagnosticsResults$conceptSets <-
    conceptSetDiagnosticsResults$conceptSets %>%
    dplyr::select(
      .data$cohortId,
      .data$conceptSetId,
      .data$conceptSetSql,
      .data$conceptSetName,
      .data$conceptSetExpression
    )
  
  #get vocabulary details----
  ParallelLogger::logInfo("  - Retrieving vocabulary details.")
  #get full data -----
  vocabularyTables1 <- c("domain",
                         "relationship",
                         "vocabulary",
                         "conceptClass")
  for (i in (1:length(vocabularyTables1))) {
    ParallelLogger::logInfo(paste0(
      "   - Retrieving '",
      camelCaseToTitleCase(vocabularyTables1[[i]])
    ), "'")
    sql <- "SELECT * FROM @vocabulary_database_schema.@table;"
    conceptSetDiagnosticsResults[[vocabularyTables1[[i]]]] <-
      renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        vocabulary_database_schema = vocabularyDatabaseSchema,
        table = camelCaseToSnakeCase(vocabularyTables1[[i]]),
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble()
  }
  vocabularyTables2 <- c('concept', "conceptSynonym")
  for (i in (1:length(vocabularyTables2))) {
    ParallelLogger::logInfo(paste0(
      "   - Retrieving '",
      camelCaseToTitleCase(vocabularyTables2[[i]])
    ), "'")
    sql <- "SELECT a.* FROM @vocabulary_database_schema.@table a
            INNER JOIN
              (SELECT distinct concept_id FROM @unique_concept_id_table) b
            ON a.concept_id = b.concept_id;"
    conceptSetDiagnosticsResults[[vocabularyTables2[[i]]]] <-
      renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        vocabulary_database_schema = vocabularyDatabaseSchema,
        table = camelCaseToSnakeCase(vocabularyTables2[[i]]),
        unique_concept_id_table = "#concept_tracking",
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble()
  }
  vocabularyTables3 <- c("conceptRelationship")
  for (i in (1:length(vocabularyTables3))) {
    ParallelLogger::logInfo(paste0(
      "   - Retrieving '",
      camelCaseToTitleCase(vocabularyTables3[[i]])
    ), "'")
    sql <-
      "SELECT DISTINCT a.* FROM @vocabulary_database_schema.@table a
            LEFT JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b1
              ON a.concept_id_1 = b1.concept_id
            LEFT JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b2
              ON a.concept_id_2 = b2.concept_id
            WHERE b1.concept_id IS NOT NULL or b2.concept_id IS NOT NULL;"
    conceptSetDiagnosticsResults[[vocabularyTables3[[i]]]] <-
      renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        vocabulary_database_schema = vocabularyDatabaseSchema,
        table = camelCaseToSnakeCase(vocabularyTables3[[i]]),
        unique_concept_id_table = "#concept_tracking",
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble()
  }
  vocabularyTables4 <- c("conceptAncestor")
  for (i in (1:length(vocabularyTables4))) {
    ParallelLogger::logInfo(paste0(
      "   - Retrieving '",
      camelCaseToTitleCase(vocabularyTables4[[i]])
    ), "'")
    sql <-
      "SELECT DISTINCT a.* FROM @vocabulary_database_schema.@table a
            LEFT JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b1
              ON a.ancestor_concept_id = b1.concept_id
            LEFT JOIN (SELECT distinct concept_id FROM @unique_concept_id_table) b2
              ON a.descendant_concept_id = b2.concept_id
            WHERE b1.concept_id IS NOT NULL or b2.concept_id IS NOT NULL;"
    conceptSetDiagnosticsResults[[vocabularyTables4[[i]]]] <-
      renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        vocabulary_database_schema = vocabularyDatabaseSchema,
        table = camelCaseToSnakeCase(vocabularyTables4[[i]]),
        unique_concept_id_table = "#concept_tracking",
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble()
  }
  
  # Drop temporary tables
  ParallelLogger::logTrace(" - Dropping temporary tables")
  sql <-
    "IF OBJECT_ID('tempdb..#resolved_concept_set', 'U') IS NOT NULL
                      	        DROP TABLE #resolved_concept_set;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  sql <-
    "IF OBJECT_ID('tempdb..#concept_tracking', 'U') IS NOT NULL
                      	        DROP TABLE #concept_tracking;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  delta <- Sys.time() - startConceptSetDiagnostics
  ParallelLogger::logTrace(" - Running concept set diagnostics took ",
                           signif(delta, 3),
                           " ",
                           attr(delta, "units"))
  
  return(conceptSetDiagnosticsResults)
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
    cohort <- cohorts[i, ]
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

# function: resolveConceptSets ----
resolveConceptSets <- function(uniqueConceptSets,
                               connection,
                               cdmDatabaseSchema,
                               vocabularyDatabaseSchema = cdmDatabaseSchema,
                               tempEmulationSchema,
                               conceptSetsTable = "#resolved_concept_set",
                               conceptTrackingTable = NULL,
                               dropConceptSetsTable = TRUE) {
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
  for (start in seq(1, length(sql), by = batchSize)) {
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
  mergeTempTables(
    connection = connection,
    tableName = conceptSetsTable,
    tempTables = tempTables,
    tempEmulationSchema = tempEmulationSchema
  )
  
  if (!is.null(conceptTrackingTable)) {
    # keeping track
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql =  "INSERT INTO @concept_tracking_table
            SELECT DISTINCT concept_id
            FROM @concept_sets_table;",
      tempEmulationSchema = tempEmulationSchema,
      concept_tracking_table = conceptTrackingTable,
      concept_sets_table = conceptSetsTable,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  resolvedConcepts <-
    renderTranslateQuerySql(
      sql = "Select * from @concept_sets_table;",
      connection = connection,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      concept_sets_table = conceptSetsTable
    ) %>%
    dplyr::rename(uniqueConceptSetId = .data$codesetId)
  
  if (dropConceptSetsTable) {
    # Drop temporary tables
    ParallelLogger::logTrace(" - Dropping temporary table: resolved concept set")
    sql <-
      "IF OBJECT_ID('tempdb..#resolved_concept_set', 'U') IS NOT NULL
                      	        DROP TABLE #resolved_concept_set;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  return(resolvedConcepts)
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

# function: getOrphanConcepts ----
getOrphanConcepts <- function(connectionDetails = NULL,
                              connection = NULL,
                              cdmDatabaseSchema,
                              vocabularyDatabaseSchema = cdmDatabaseSchema,
                              tempEmulationSchema = NULL,
                              instantiatedCodeSets = "#resolved_concept_set",
                              conceptIdUniverse = NULL) {
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
    instantiated_code_sets = instantiatedCodeSets,
    concept_id_universe = conceptIdUniverse
  )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  sql <- "SELECT * FROM #orphan_concept_table;"
  orphanCodes <- renderTranslateQuerySql(
    sql = sql,
    connection = connection,
    snakeCaseToCamelCase = TRUE
  )
  sql <-
    "IF OBJECT_ID('tempdb..#orphan_concept_table', 'U') IS NOT NULL
	              DROP TABLE #orphan_concept_table;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  return(orphanCodes)
}

# function: getConceptRecordCount ----
getConceptRecordCount <- function(connection,
                                         cdmDatabaseSchema,
                                         tempEmulationSchema,
                                         conceptIdUniverse = "#concept_tracking") {
  ParallelLogger::logTrace(" - Counting concepts by person id, calendar month and year")
  domains <- getDomainInformation(packageName = 'CohortDiagnostics')
  domains <- domains$wide
  nonEraTables <- domains %>% 
    dplyr::filter(.data$isEraTable == FALSE) %>% 
    dplyr::pull(.data$domainTableShort) %>% 
    unique()
  sql1 <- "SELECT @domain_concept_id concept_id,
          	YEAR(@domain_start_date) event_year,
          	MONTH(@domain_start_date) event_month,
          	COUNT_BIG(*) concept_count,
          	COUNT_BIG(DISTINCT person_id) subject_count
          FROM @cdm_database_schema.@domain_table
          INNER JOIN (
          	SELECT DISTINCT concept_id
          	FROM @concept_id_universe
          	) c
          	ON @domain_concept_id = concept_id
          WHERE YEAR(@domain_start_date) > 0
          GROUP BY @domain_concept_id,
          	YEAR(@domain_start_date),
          	MONTH(@domain_start_date);"
  sql2 <- "SELECT @domain_concept_id concept_id,
          	YEAR(@domain_start_date) event_year,
          	0 as event_month,
          	COUNT_BIG(*) concept_count,
          	COUNT_BIG(DISTINCT person_id) subject_count
          FROM @cdm_database_schema.@domain_table
          INNER JOIN (
          	SELECT DISTINCT concept_id
          	FROM @concept_id_universe
          	) c
          	ON @domain_concept_id = concept_id
          WHERE YEAR(@domain_start_date) > 0
          GROUP BY @domain_concept_id,
          	YEAR(@domain_start_date);"
  sql3 <- "SELECT @domain_concept_id concept_id,
          	0 as event_year,
          	0 as event_month,
          	COUNT_BIG(*) concept_count,
          	COUNT_BIG(DISTINCT person_id) subject_count
          FROM @cdm_database_schema.@domain_table
          INNER JOIN (
          	SELECT DISTINCT concept_id
          	FROM @concept_id_universe
          	) c
          	ON @domain_concept_id = concept_id
          WHERE YEAR(@domain_start_date) > 0
          GROUP BY @domain_concept_id;"
  
  standardConcepts <- list()
  for (i in (1:nrow(domains))) {
    rowData <- domains[i, ]
    ParallelLogger::logTrace(paste0(
      "   - Working on ",
      rowData$domainTable,
      ".",
      rowData$domainConceptId
    ))
    data1 <- renderTranslateQuerySql(
      connection = connection,
      sql = sql1,
      tempEmulationSchema = tempEmulationSchema,
      domain_table = rowData$domainTable,
      domain_concept_id = rowData$domainConceptId,
      cdm_database_schema = cdmDatabaseSchema,
      domain_start_date = rowData$domainStartDate,
      concept_id_universe = conceptIdUniverse,
      snakeCaseToCamelCase = TRUE
    )
    if (!rowData$isEraTable) {
      data2 <- renderTranslateQuerySql(
        connection = connection,
        sql = sql2,
        tempEmulationSchema = tempEmulationSchema,
        domain_table = rowData$domainTable,
        domain_concept_id = rowData$domainConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        domain_start_date = rowData$domainStartDate,
        concept_id_universe = conceptIdUniverse,
        snakeCaseToCamelCase = TRUE
      )
      data3 <- renderTranslateQuerySql(
        connection = connection,
        sql = sql3,
        tempEmulationSchema = tempEmulationSchema,
        domain_table = rowData$domainTable,
        domain_concept_id = rowData$domainConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        domain_start_date = rowData$domainStartDate,
        concept_id_universe = conceptIdUniverse,
        snakeCaseToCamelCase = TRUE
      )
    } else {
      data2 <- dplyr::tibble()
      data3 <- dplyr::tibble()
    }
    standardConcepts[[i]] <- dplyr::bind_rows(data1,
                                              data2,
                                              data3) %>%
      dplyr::mutate(domainTable = rowData$domainTableShort) %>%
      dplyr::mutate(domainField = rowData$domainConceptIdShort)
  }
  standardConcepts <- dplyr::bind_rows(standardConcepts) %>%
    dplyr::distinct()
  
  nonStandardConcepts <- list()
  for (i in (1:nrow(domains))) {
    rowData <- domains[i, ]
    if (nchar(rowData$domainSourceConceptId) > 4) {
      ParallelLogger::logTrace(paste0(
        "   - Working on ",
        rowData$domainTable,
        ".",
        rowData$domainSourceConceptId
      ))
      nsData1 <- renderTranslateQuerySql(
        connection = connection,
        sql = sql1,
        tempEmulationSchema = tempEmulationSchema,
        domain_table = rowData$domainTable,
        domain_concept_id = rowData$domainSourceConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        domain_start_date = rowData$domainStartDate,
        concept_id_universe = conceptIdUniverse,
        snakeCaseToCamelCase = TRUE
      ) %>%
        # conceptIds - only keep concept id that were never found in standard fields
        dplyr::anti_join(
          y = standardConcepts %>%
            dplyr::select(.data$conceptId) %>%
            dplyr::distinct(),
          by = 'conceptId'
        )
      if (!rowData$isEraTable) {
        nsData2 <- renderTranslateQuerySql(
          connection = connection,
          sql = sql2,
          tempEmulationSchema = tempEmulationSchema,
          domain_table = rowData$domainTable,
          domain_concept_id = rowData$domainSourceConceptId,
          cdm_database_schema = cdmDatabaseSchema,
          domain_start_date = rowData$domainStartDate,
          concept_id_universe = conceptIdUniverse,
          snakeCaseToCamelCase = TRUE
        ) %>%
          # conceptIds - only keep concept id that were never found in standard fields
          dplyr::anti_join(
            y = standardConcepts %>%
              dplyr::select(.data$conceptId) %>%
              dplyr::distinct(),
            by = 'conceptId'
          )
        nsData3 <- renderTranslateQuerySql(
          connection = connection,
          sql = sql3,
          tempEmulationSchema = tempEmulationSchema,
          domain_table = rowData$domainTable,
          domain_concept_id = rowData$domainSourceConceptId,
          cdm_database_schema = cdmDatabaseSchema,
          domain_start_date = rowData$domainStartDate,
          concept_id_universe = conceptIdUniverse,
          snakeCaseToCamelCase = TRUE
        ) %>%
          # conceptIds - only keep concept id that were never found in standard fields
          dplyr::anti_join(
            y = standardConcepts %>%
              dplyr::select(.data$conceptId) %>%
              dplyr::distinct(),
            by = 'conceptId'
          )
      } else {
        data2 <- dplyr::tibble()
        data3 <- dplyr::tibble()
      }
      nonStandardConcepts[[i]] <- dplyr::bind_rows(nsData1,
                                                   nsData2,
                                                   nsData3) %>%
        dplyr::mutate(domainTable = rowData$domainTableShort) %>%
        dplyr::mutate(domainField = rowData$domainConceptIdShort)
    }
  }
  nonStandardConcepts <-
    dplyr::bind_rows(nonStandardConcepts)
  data <-
    dplyr::bind_rows(standardConcepts, nonStandardConcepts) %>%
    dplyr::select(
      .data$domainTable,
      .data$domainField,
      .data$conceptId,
      .data$eventYear,
      .data$eventMonth,
      .data$conceptCount,
      .data$subjectCount
    ) %>%
    dplyr::distinct() %>%
    dplyr::arrange(
      .data$domainTable,
      .data$domainField,
      .data$conceptId,
      .data$eventYear,
      .data$eventMonth
    )
  data <- dplyr::bind_rows(data,
                   data %>% 
                     dplyr::group_by(.data$conceptId,
                                     .data$eventYear,
                                     .data$eventMonth) %>% 
                     dplyr::summarise(conceptCount = sum(.data$conceptCount),
                                      subjectCount = max(.data$subjectCount), 
                                      .groups = "keep") %>% 
                     dplyr::mutate(domainField = "All",
                                   domainTable = "All"),
                   data %>%
                     dplyr::group_by(.data$conceptId,
                                     .data$eventYear,
                                     .data$eventMonth,
                                     .data$domainTable
                     ) %>% 
                     dplyr::summarise(conceptCount = sum(.data$conceptCount),
                                      subjectCount = max(.data$subjectCount), 
                                      .groups = "keep") %>% 
                     dplyr::mutate(domainField = "All")
  ) %>% 
    dplyr::distinct()
  return(data)
}


# function: getBreakdownIndexEvents ----
getBreakdownIndexEvents <- function(cohortIds,
                                    connection,
                                    cdmDatabaseSchema,
                                    cohortDatabaseSchema,
                                    cohortTable,
                                    tempEmulationSchema,
                                    conceptIdUniverse = "#concept_tracking") {
  domains <- getDomainInformation(packageName = 'CohortDiagnostics')
  domains <- domains$wide
  nonEraTables <- domains %>% 
    dplyr::filter(.data$isEraTable == FALSE) %>% 
    dplyr::pull(.data$domainTableShort) %>% 
    unique()
  sql <- "SELECT cohort_definition_id cohort_id,
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
    ) %>%
      dplyr::mutate(
        domainTable = rowData$domainTableShort,
        domainField = rowData$domainConceptIdShort
      )
  }
  breakdownDataStandard <- dplyr::bind_rows(breakdownDataStandard)
  
  breakdownDataNonStandard <- list()
  for (i in (1:nrow(domains))) {
    rowData <- domains[i,]
    if (all(!is.na(rowData$domainSourceConceptId),
        nchar(rowData$domainSourceConceptId) > 4)) {
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
      ) %>%
        dplyr::mutate(
          domainTable = rowData$domainTableShort,
          domainField = rowData$domainSourceConceptIdShort
        )
    }
  }
  breakdownDataNonStandard <-
    dplyr::bind_rows(breakdownDataNonStandard)
  data <- 
    dplyr::bind_rows(breakdownDataNonStandard,
                     breakdownDataStandard) %>% 
    dplyr::distinct()
  data <- dplyr::bind_rows(data,
                           data %>% 
                             dplyr::filter(.data$domainTable %in% c(nonEraTables)) %>% 
                             dplyr::group_by(.data$cohortId,
                                             .data$conceptId
                                             ) %>% 
                             dplyr::summarise(conceptCount = sum(.data$conceptCount),
                                              subjectCount = max(.data$subjectCount), 
                                              .groups = "keep") %>% 
                             dplyr::mutate(domainField = "All",
                                           domainTable = "All"),
                           data %>%
                             dplyr::filter(.data$domainTable %in% c(nonEraTables)) %>% 
                             dplyr::group_by(.data$cohortId,
                                             .data$conceptId,
                                             .data$domainTable
                             ) %>% 
                             dplyr::summarise(conceptCount = sum(.data$conceptCount),
                                              subjectCount = max(.data$subjectCount), 
                                              .groups = "keep") %>% 
                             dplyr::mutate(domainField = "All")
                             ) %>% 
    dplyr::distinct()
  return(data)
}


# function: getIndexDateConceptCooccurrence ----
### indexDateConceptCooccurrence -----
getIndexDateConceptCooccurrence <- function(connection,
                                            cdmDatabaseSchema,
                                            tempEmulationSchema,
                                            cohortTable,
                                            cohortDatabaseSchema,
                                            cohortIds,
                                            conceptIdUniverse = "#concept_tracking") {
  domains <- getDomainInformation(packageName = 'CohortDiagnostics')
  domains <- domains$wide
  sqlDdlDrop <-
    "IF OBJECT_ID('tempdb..#concept_cooccurrence', 'U') IS NOT NULL
                	      DROP TABLE #concept_cooccurrence;
  CREATE TABLE #concept_cooccurrence (
                                    	cohort_id BIGINT,
                                    	concept_id INT,
                                    	person_id BIGINT
                                    	);"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdlDrop,
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
    rowData <- domains[i,]
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
    if (all(!is.na((rowData$domainSourceConceptId)),
            nchar(rowData$domainSourceConceptId) > 4)) {
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
                          	count(*) subject_count
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
    concept_id_table = "#concept_tracking",
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



# function: getConceptSourceStandardMapping ----
getConceptSourceStandardMapping <- function(connection,
                                            cdmDatabaseSchema,
                                            tempEmulationSchema,
                                            sourceValue = FALSE,
                                            conceptIdUniverse = "#concept_tracking") {
  domains <- getDomainInformation(packageName = 'CohortDiagnostics')
  domains <- domains$wide %>%
    dplyr::filter(nchar(.data$domainSourceConceptId) > 1)
  
  sql <- "WITH concept_id_universe
          AS (
          	SELECT DISTINCT concept_id
          	FROM @concept_id_universe
          	)
          SELECT @domain_concept_id concept_id,
          	{@domain_source_concept_id != '' } ? { @domain_source_concept_id source_concept_id,
          	} {@sourceValue} ? { @domain_source_value source_value,
          	} COUNT(*) AS concept_count,
          	COUNT(DISTINCT person_id) AS subject_count
          FROM @cdm_database_schema.@domain_table
          LEFT JOIN concept_id_universe a
          	ON @domain_concept_id = a.concept_id {@domain_source_concept_id != '' } ? {
          LEFT JOIN concept_id_universe b
          	ON @domain_source_concept_id = b.concept_id}
          WHERE (
          		@domain_concept_id IS NOT NULL
          		AND @domain_concept_id > 0 {@domain_source_concept_id != '' } ? {
          		AND @domain_source_concept_id IS NOT NULL
          		AND @domain_source_concept_id > 0 }
          		)
          	AND (
          		a.concept_id IS NOT NULL {@domain_source_concept_id != '' } ? {
          		OR b.concept_id IS NOT NULL}
          		)
          GROUP BY @domain_concept_id {@domain_source_concept_id != '' } ? {,
          	@domain_source_concept_id } {@sourceValue} ? {,
          	@domain_source_value }
          ORDER BY @domain_concept_id {@domain_source_concept_id != '' } ? {,
          	@domain_source_concept_id } {@sourceValue} ? {,
          	@domain_source_value };"
  
  conceptMapping <- list()
  for (i in (1:nrow(domains))) {
    rowData <- domains[i,]
    ParallelLogger::logTrace(paste0(
      "  - Working on ",
      rowData$domainTable,
      ".",
      rowData$domainConceptId
    ))
    conceptMapping[[i]] <- renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      domain_table = rowData$domainTable,
      domain_concept_id = rowData$domainConceptId,
      domain_source_concept_id = rowData$domainSourceConceptId,
      domain_source_value = rowData$domainSourceValue,
      cdm_database_schema = cdmDatabaseSchema,
      concept_id_universe = conceptIdUniverse,
      sourceValue = sourceValue,
      snakeCaseToCamelCase = TRUE
    )
    conceptMapping[[i]]$domainTable <- rowData$domainTableShort
  }
  conceptMapping <- dplyr::bind_rows(conceptMapping) %>%
    dplyr::distinct() %>% 
    dplyr::arrange(
      .data$domainTable,
      .data$conceptId,
      .data$sourceConceptId,
      .data$conceptCount,
      .data$subjectCount
    )
  conceptMapping <- dplyr::bind_rows(conceptMapping,
                                     conceptMapping %>% 
                             dplyr::group_by(.data$conceptId,
                                             .data$sourceConceptId
                             ) %>% 
                             dplyr::summarise(conceptCount = sum(.data$conceptCount),
                                              subjectCount = max(.data$subjectCount), 
                                              .groups = "keep") %>% 
                             dplyr::mutate(domainTable = "All")
  ) %>% 
    dplyr::distinct()
  return(conceptMapping)
}


# function:getExcludedConceptSets ----
getExcludedConceptSets <- function(connection,
                                   uniqueConceptSets,
                                   vocabularyDatabaseSchema,
                                   tempEmulationSchema,
                                   conceptTrackingTable = NULL) {
  conceptSetWithExclude <- list()
  for (i in (1:nrow(uniqueConceptSets))) {
    conceptSetExpression <-
      uniqueConceptSets$conceptSetExpression[[i]] %>%
      RJSONIO::fromJSON(digits = 23)
    conceptSetWithExclude[[i]] <-
      getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression)
    if ('isExcluded' %in% colnames(conceptSetWithExclude[[i]])) {
      conceptSetWithExclude[[i]] <- conceptSetWithExclude[[i]] %>%
        dplyr::filter(.data$isExcluded == TRUE) %>%
        dplyr::mutate(uniqueConceptSetId = uniqueConceptSets$uniqueConceptSetId[[i]]) %>%
        dplyr::select(.data$uniqueConceptSetId,
                      .data$conceptId,
                      .data$includeDescendants) %>%
        dplyr::distinct()
    } else {
      conceptSetWithExclude[[i]] <- conceptSetWithExclude[[i]][0,]
    }
  }
  conceptSetWithExclude <-
    dplyr::bind_rows(conceptSetWithExclude)
  
  # with descendants = FALSE
  conceptIdsWithOutDescendantsInExclude <-
    conceptSetWithExclude %>%
    dplyr::filter(.data$includeDescendants != TRUE) %>%
    dplyr::select(.data$conceptId) %>%
    dplyr::distinct() %>%
    dplyr::pull()
  # with descendants = TRUE
  conceptIdsWithDescendantsInExclude <-
    conceptSetWithExclude %>%
    dplyr::filter(.data$includeDescendants == TRUE) %>%
    dplyr::select(.data$conceptId) %>%
    dplyr::distinct() %>%
    dplyr::pull()
  
  if (length(c(
    conceptIdsWithOutDescendantsInExclude,
    conceptIdsWithDescendantsInExclude
  )) == 0) {
    ParallelLogger::logTrace(
      "   - None of the concept sets had excluded concepts. Exiting excluded concept diagnostics."
    )
    return(NULL)
  }
  
  if (!is.null(conceptTrackingTable)) {
    # tracking table
    sql <- "INSERT INTO @concept_tracking_table
            SELECT DISTINCT concept_id
            FROM @vocabulary_database_schema.concept
            WHERE concept_id IN (@noDescendants)

            UNION

            SELECT DISTINCT descendant_concept_id concept_id
            FROM @vocabulary_database_schema.concept_ancestor
            WHERE ancestor_concept_id IN (@descendants)
    ;"
    if (length(conceptIdsWithOutDescendantsInExclude) == 0) {
      noDescendants <- -1
    } else {
      noDescendants <- conceptIdsWithOutDescendantsInExclude
    }
    if (length(conceptIdsWithDescendantsInExclude) == 0) {
      descendants <- -1
    } else {
      descendants <- conceptIdsWithDescendantsInExclude
    }
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      noDescendants = noDescendants,
      descendants = descendants,
      vocabulary_database_schema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      concept_tracking_table = conceptTrackingTable,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "#exclude_no_des",
    createTable = TRUE,
    dropTableIfExists = TRUE,
    tempTable = TRUE,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    camelCaseToSnakeCase = TRUE,
    data = conceptSetWithExclude %>%
      dplyr::filter(.data$includeDescendants != TRUE) %>%
      dplyr::select(.data$uniqueConceptSetId,
                    .data$conceptId)
  )
  
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "#exclude_des",
    createTable = TRUE,
    dropTableIfExists = TRUE,
    tempTable = TRUE,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    camelCaseToSnakeCase = TRUE,
    data = conceptSetWithExclude %>%
      dplyr::filter(.data$includeDescendants == TRUE) %>%
      dplyr::select(.data$uniqueConceptSetId,
                    .data$conceptId)
  )
  
  # resolve excluded concepts
  sql <-
    "SELECT DISTINCT unique_concept_set_id,
    	concept_id
    FROM (
    	SELECT DISTINCT unique_concept_set_id,
    		descendant_concept_id concept_id
    	FROM @vocabulary_database_schema.concept_ancestor
    	INNER JOIN #exclude_des
    		ON ancestor_concept_id = concept_id

    	UNION

    	SELECT DISTINCT e.unique_concept_set_id,
    		c.concept_id
    	FROM @vocabulary_database_schema.concept c
    	INNER JOIN #exclude_no_des e
    		ON c.concept_id = e.concept_id
    	) f;"
  excludedConcepts <- renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    vocabulary_database_schema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  )
  sql <-
    "IF OBJECT_ID('tempdb..#exclude_no_des', 'U') IS NOT NULL
                      	DROP TABLE #exclude_no_des;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  sql <-
    "IF OBJECT_ID('tempdb..#exclude_des', 'U') IS NOT NULL
                      	DROP TABLE #exclude_des;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  return(excludedConcepts)
}



#' given a concept set table, get optimization recommendation
#'
#' @template Connection
#'
#' @template VocabularyDatabaseSchema
#' 
#' @template TempEmulationSchema
#'
#' @param conceptSetExpression   An R Object (list) with concept set expression. This maybe generated
#'                               by first getting the JSON representation of concept set expression and
#'                               converting it to a list using RJSONIO::fromJson(digits = 23)
#'
#' @export
getOptimizationRecommendationForConceptSetExpression <-
  function(conceptSetExpression,
           vocabularyDatabaseSchema = 'vocabulary',
           tempEmulationSchema = tempEmulationSchema,
           connectionDetails = NULL,
           connection = NULL) {
    conceptSetExpressionDataFrame <-
      getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression)
    if (nrow(conceptSetExpressionDataFrame) <= 1) {
      # no optimization necessary
      return(
        conceptSetExpressionDataFrame %>%
          dplyr::mutate(excluded = as.integer(.data$isExcluded),
                        removed = 0) %>%
          dplyr::select(.data$conceptId, .data$excluded, .data$removed)
      )
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
    
    conceptSetConceptIdsExcluded <-
      conceptSetExpressionDataFrame %>%
      dplyr::filter(.data$isExcluded == TRUE) %>%
      dplyr::pull(.data$conceptId)
    
    conceptSetConceptIdsDescendantsExcluded <-
      conceptSetExpressionDataFrame %>%
      dplyr::filter(.data$isExcluded == TRUE) %>%
      dplyr::filter(.data$includeDescendants == TRUE) %>%
      dplyr::pull(.data$conceptId)
    
    conceptSetConceptIdsNotExcluded <-
      conceptSetExpressionDataFrame %>%
      dplyr::filter(!.data$isExcluded == TRUE) %>%
      dplyr::pull(.data$conceptId)
    
    conceptSetConceptIdsDescendantsNotExcluded <-
      conceptSetExpressionDataFrame %>%
      dplyr::filter(!.data$isExcluded == TRUE) %>%
      dplyr::filter(.data$includeDescendants == TRUE) %>%
      dplyr::pull(.data$conceptId)
    
    if (!doesObjectHaveData(conceptSetConceptIdsExcluded)) {
      conceptSetConceptIdsExcluded <- 0
    }
    if (!doesObjectHaveData(conceptSetConceptIdsDescendantsExcluded)) {
      conceptSetConceptIdsDescendantsExcluded <- 0
    }
    if (!doesObjectHaveData(conceptSetConceptIdsNotExcluded)) {
      conceptSetConceptIdsNotExcluded <- 0
    }
    if (!doesObjectHaveData(conceptSetConceptIdsDescendantsNotExcluded)) {
      conceptSetConceptIdsDescendantsNotExcluded <- 0
    }
    
    sql <-
      SqlRender::readSql(
        sourceFile = system.file("sql",
                                 "sql_server",
                                 'OptimizeConceptSet.sql',
                                 package = "CohortDiagnostics")
      )
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      reportOverallTime = FALSE,
      progressBar = FALSE,
      vocabulary_database_schema = vocabularyDatabaseSchema,
      conceptSetConceptIdsExcluded = conceptSetConceptIdsExcluded,
      conceptSetConceptIdsDescendantsExcluded = conceptSetConceptIdsDescendantsExcluded,
      conceptSetConceptIdsNotExcluded = conceptSetConceptIdsNotExcluded,
      conceptSetConceptIdsDescendantsNotExcluded = conceptSetConceptIdsDescendantsNotExcluded
    )
    
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = "SELECT * FROM #optimized_set;",
                                                       snakeCaseToCamelCase = TRUE)
    
    sqlCleanUp <-
      "IF OBJECT_ID('tempdb..#optimized_set', 'U') IS NOT NULL
	        DROP TABLE #optimized_set;"
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sqlCleanUp,
      reportOverallTime = FALSE,
      progressBar = FALSE
    )
    data <- data %>% 
      dplyr::filter(.data$conceptId != 0)
    
    if (nrow(data) == 0) {
      return(NULL)
    }
    return(data)
  }
