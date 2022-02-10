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
#' @template IndexDateDiagnosticsRelativeDays
#'
#' @param minCellCount                The minimum cell count for fields contains person counts or fractions.
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
                                     indexDateDiagnosticsRelativeDays = c(0),
                                     minCellCount = 5,
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
  
  # Set up----
  ## Set up connection to server----
  ParallelLogger::logTrace(" - Setting up connection")
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
  }
  ## Create concept tracking table----
  #For some domains (e.g. Visit download all vocabulary - as it is used in visit context etc)
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
  
  ## Create andromeda object ----
  conceptSetDiagnosticsResults <- Andromeda::andromeda()
  
  ## Cohorts to run----
  if (!is.null(cohortIds)) {
    subset <- cohorts %>%
      dplyr::filter(.data$cohortId %in% cohortIds)
  }
  if (nrow(subset) == 0) {
    ParallelLogger::logInfo("  - No cohorts to run concept set diagnostics. Exiting concept set diagnostics.")
    return(NULL)
  }
  
  ## Get concept sets----
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
  
  ## Unique concept sets----
  uniqueConceptSets <-
    conceptSets[!duplicated(conceptSets$uniqueConceptSetId),] %>%
    dplyr::select(-.data$cohortId,-.data$conceptSetId)
  ParallelLogger::logTrace(
    paste0(
      " - Note: There are ",
      scales::comma(nrow(uniqueConceptSets)),
      " unique concept set ids in ",
      scales::comma(
        conceptSetDiagnosticsResults$conceptSets %>% dplyr::summarise(n = dplyr::n()) %>% dplyr::pull(.data$n)
      ),
      " concept sets in all cohort definitions."
    )
  )
  
  ## Optimize unique concept set----
  ParallelLogger::logInfo("  - Optimizing concept sets found in cohorts.")
  optimizedConceptSet <- list()
  for (i in (1:nrow(uniqueConceptSets))) {
    ParallelLogger::logTrace(
      paste0(
        "Optimizing concept sets: ",
        scales::comma(i),
        " of ",
        scales::comma(nrow(uniqueConceptSets)),
        " unique concept sets."
      )
    )
    uniqueConceptSet <- uniqueConceptSets[i, ]
    conceptSetExpression <-
      RJSONIO::fromJSON(uniqueConceptSet$conceptSetExpression)
    optimizationRecommendation <-
      getOptimizationRecommendationForConceptSetExpression(
        conceptSetExpression = conceptSetExpression,
        connection = connection,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema
      )
    if (!is.null(optimizationRecommendation)) {
      optimizationRecommendation <- optimizationRecommendation %>%
        dplyr::mutate(uniqueConceptSetId = uniqueConceptSet$uniqueConceptSetId) %>%
        dplyr::inner_join(
          conceptSets %>%
            dplyr::select(
              .data$uniqueConceptSetId,
              .data$cohortId,
              .data$conceptSetId
            ),
          by = "uniqueConceptSetId"
        )
      optimizedConceptSet[[i]] <- optimizationRecommendation %>%
        dplyr::select(
          .data$cohortId,
          .data$conceptSetId,
          .data$conceptId,
          .data$excluded,
          .data$removed
        ) %>%
        dplyr::distinct()
    }
  }
  conceptSetDiagnosticsResults$conceptSetsOptimized <-
    dplyr::bind_rows(optimizedConceptSet) %>%
    dplyr::distinct() %>%
    dplyr::tibble()
  rm("conceptSets")
  
  ## Instantiate (resolve) unique concept sets----
  ParallelLogger::logInfo("  - Resolving concept sets found in cohorts.")
  startInstantiateConceptSet <- Sys.time()
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
  delta <- Sys.time() - startInstantiateConceptSet
  ParallelLogger::logTrace("  - Instantiating concept sets took ",
                           signif(delta, 3),
                           " ",
                           attr(delta, "units"))
  
  ## Excluded concepts ----
  ParallelLogger::logInfo("  - Collecting excluded concepts.")
  excludedConceptsStart <- Sys.time()
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
  delta <- Sys.time() - excludedConceptsStart
  ParallelLogger::logTrace("  - Collecting excluded concepts took ",
                           signif(delta, 3),
                           " ",
                           attr(delta, "units"))
  browser()
  
  ## Orphan concepts ----
  ParallelLogger::logInfo("  - Searching for concepts that may have been orphaned.")
  startOrphanCodes <- Sys.time()
  conceptSetDiagnosticsResults$orphanConcept <- getOrphanConcepts(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    instantiatedCodeSets = "#resolved_concept_set"
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
  ## Concept Source to Standard mapping----
  ParallelLogger::logInfo("  - Mapping concepts.")
  startConceptMapping <- Sys.time()
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
  delta <- Sys.time() - startConceptMapping
  ParallelLogger::logTrace("  - Counting concept mapping took ",
                           signif(delta, 3),
                           " ",
                           attr(delta, "units"))
  
  ## Get all the conceptIds of interest, per cohort
  ParallelLogger::logInfo("  - Tracking cohort level concepts.")
  startCohortConceptTrack <- Sys.time()
  conceptsCohort <- dplyr::bind_rows(
    conceptSetDiagnosticsResults$conceptResolved %>%
      dplyr::select(.data$cohortId,
                    .data$conceptId) %>%
      dplyr::collect(),
    conceptSetDiagnosticsResults$conceptExcluded %>%
      dplyr::select(.data$cohortId,
                    .data$conceptId) %>%
      dplyr::collect(),
    conceptSetDiagnosticsResults$orphanConcept %>%
      dplyr::select(.data$cohortId,
                    .data$conceptId) %>%
      dplyr::collect(),
    conceptSetDiagnosticsResults$conceptSetsOptimized %>%
      dplyr::select(.data$cohortId,
                    .data$conceptId) %>%
      dplyr::collect()
  ) %>%
    dplyr::distinct()
  conceptsCohortMapped1 <- conceptsCohort %>%
    dplyr::inner_join(
      conceptSetDiagnosticsResults$conceptMapping %>%
        dplyr::select(.data$conceptId, .data$sourceConceptId) %>%
        dplyr::collect(),
      by = c("conceptId")
    ) %>%
    dplyr::select(-.data$conceptId) %>%
    dplyr::rename("conceptId" = .data$sourceConceptId)
  conceptsCohortMapped2 <- conceptsCohort %>%
    dplyr::inner_join(
      conceptSetDiagnosticsResults$conceptMapping %>%
        dplyr::select(.data$conceptId, .data$sourceConceptId) %>%
        dplyr::rename(
          "sourceConceptId" = .data$conceptId,
          "conceptId" = .data$sourceConceptId
        ) %>%
        dplyr::collect(),
      by = c("conceptId")
    ) %>%
    dplyr::select(-.data$conceptId) %>%
    dplyr::rename("conceptId" = .data$sourceConceptId)
  #getConceptId from 'Visit' and 'Place of Service' domains
  sqlVisitPlaceOfService <- "SELECT DISTINCT CONCEPT_ID
                      FROM @vocabulary_database_schema.concept
                      WHERE domain_id IN ('Visit', 'Place of Service');"
  conceptIdVisit <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sqlVisitPlaceOfService,
      vocabulary_database_schema = vocabularyDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::distinct() %>%
    tidyr::crossing(dplyr::tibble(cohortId = cohorts$cohortId %>% unique())) %>%
    dplyr::select(.data$cohortId, .data$conceptId) %>%
    dplyr::distinct()
  conceptsCohort <- dplyr::bind_rows(conceptsCohort,
                                     conceptsCohortMapped1,
                                     conceptsCohortMapped2,
                                     conceptIdVisit) %>%
    dplyr::distinct()
  randomStringTableName <-
    tolower(paste0("tmp_",
                   paste0(
                     sample(
                       x = c(LETTERS, 0:9),
                       size = 12,
                       replace = TRUE
                     ), collapse = ""
                   )))
  
  # insert into server concept ids in cohort
  ParallelLogger::logTrace(paste0("    - Uploading to ", randomStringTableName))
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = paste0(cohortDatabaseSchema, ".", randomStringTableName),
    createTable = TRUE,
    dropTableIfExists = TRUE,
    tempTable = FALSE,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    bulkLoad = (Sys.getenv("bulkLoad") == TRUE),
    camelCaseToSnakeCase = TRUE,
    data = conceptsCohort
  )
  delta <- Sys.time() - startCohortConceptTrack
  ParallelLogger::logTrace(
    "    - Creating and loading concept_id's for use in index event breakdown took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
  if (Sys.getenv("bulkLoad") == TRUE) {
    ParallelLogger::logTrace("      - Bulkload was used.")
  }
  
  ## Index event breakdown ----
  ParallelLogger::logInfo("  - Learning about the breakdown in index events.")
  startBreakdownEvents <- Sys.time()
  conceptSetDiagnosticsResults$indexEventBreakdown <-
    getConceptOccurrenceRelativeToIndexDay(
      cohortIds = subset$cohortId,
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      minCellCount = minCellCount,
      tempEmulationSchema = tempEmulationSchema,
      conceptIdUniverse = "#concept_tracking",
      indexDateDiagnosticsRelativeDays = indexDateDiagnosticsRelativeDays,
      conceptIdToFilterIndexEvent = paste0(cohortDatabaseSchema, ".", randomStringTableName)
    )
  if (!keepCustomConceptId) {
    conceptSetDiagnosticsResults$indexEventBreakdown <-
      conceptSetDiagnosticsResults$indexEventBreakdown %>%
      dplyr::filter(.data$conceptId < 200000000)
  }
  delta <- (Sys.time() - startBreakdownEvents)
  ParallelLogger::logTrace("  - Index event breakdown took ",
                           signif(delta, 3),
                           " ",
                           attr(delta, "units"))
  
  
  ParallelLogger::logInfo(paste0(
    "  - Dropping table ",
    paste0(cohortDatabaseSchema, ".", randomStringTableName)
  ))
  sqlDrop <- paste0("DROP TABLE ",
                    paste0(cohortDatabaseSchema, ".", randomStringTableName),
                    ";")
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDrop,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  ParallelLogger::logInfo("    - Success")
  
  ## Concept count----
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
  
  ## Finalize----
  conceptSetDiagnosticsResults$conceptSets <-
    conceptSetDiagnosticsResults$conceptSets %>%
    dplyr::select(
      .data$cohortId,
      .data$conceptSetId,
      .data$conceptSetSql,
      .data$conceptSetName,
      .data$conceptSetExpression
    )
  
  # Vocabulary tables----
  ParallelLogger::logInfo("  - Retrieving vocabulary details.")
  ## All data -----
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
  ## Partial data -----
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
      " SELECT a.*
        FROM @vocabulary_database_schema.@table a
        INNER JOIN (
        	SELECT DISTINCT concept_id
        	FROM @unique_concept_id_table
        	) b1 ON a.concept_id_1 = b1.concept_id

        UNION

        SELECT a.*
        FROM @vocabulary_database_schema.@table a
        INNER JOIN (
        	SELECT DISTINCT concept_id
        	FROM @unique_concept_id_table
        	) b2 ON a.concept_id_2 = b2.concept_id;"
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
  
  # Clean up----
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
        utils::type.convert(as.is = TRUE)
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
            FROM @concept_sets_table
            WHERE concept_id != 0;",
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
                              conceptTrackingTable = NULL,
                              useDirectConceptsOnly = FALSE,
                              concept_counts_table_is_temp = TRUE
                              ) {
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
    use_direct_concepts_only = useDirectConceptsOnly,
    concept_counts_table_is_temp = TRUE
  )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    profile = FALSE,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  if (!is.null(conceptTrackingTable)) {
    # tracking table
    sql <- "INSERT INTO @concept_tracking_table (concept_id)
                SELECT DISTINCT concept_id
                FROM @orphan_concept_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      concept_tracking_table = conceptTrackingTable,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
    
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
  domains <- getDomainInformation(packageName = 'CohortDiagnostics')
  domains <- domains$wide %>%
    dplyr::filter(.data$isEraTable == FALSE)
  #filtering out ERA tables because they are supposed to be derived tables, and counting them is double counting
  sqlDdlDrop <-
    "IF OBJECT_ID('tempdb..#concept_count_temp', 'U') IS NOT NULL
                	      DROP TABLE #concept_count_temp;"
  sqlDdlCreate <- "
  CREATE TABLE #concept_count_temp (
                                    	concept_id INT,
                                    	event_year INT,
                                    	event_month INT,
                                    	concept_is_standard VARCHAR(1),
                                    	concept_count BIGINT,
                                    	subject_count BIGINT
                                    	);"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdlDrop,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdlCreate,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  #REASON for many SQL --DISTINCT subject_count cannot be computed from aggregation query of calendar month level data
  sql1 <- "INSERT INTO #concept_count_temp
          	SELECT @domain_concept_id concept_id,
          		YEAR(@domain_start_date) event_year,
          		MONTH(@domain_start_date) event_month,
          		'Y' concept_is_standard,
          		COUNT_BIG(*) concept_count,
          		COUNT_BIG(DISTINCT person_id) subject_count
          	FROM @cdm_database_schema.@domain_table
          	INNER JOIN (
          		SELECT DISTINCT concept_id
          		FROM @concept_id_universe
          		) c ON @domain_concept_id = concept_id
          	WHERE YEAR(@domain_start_date) > 0
          		AND @domain_concept_id > 0
          	GROUP BY @domain_concept_id,
          		YEAR(@domain_start_date),
          		MONTH(@domain_start_date);"
  sql2 <- " INSERT INTO #concept_count_temp
            SELECT @domain_concept_id concept_id,
            	YEAR(@domain_start_date) event_year,
            	0 AS event_month,
            	'Y' concept_is_standard,
            	COUNT_BIG(*) concept_count,
            	COUNT_BIG(DISTINCT person_id) subject_count
            FROM @cdm_database_schema.@domain_table
            INNER JOIN (
            	SELECT DISTINCT concept_id
            	FROM @concept_id_universe
            	) c ON @domain_concept_id = concept_id
            WHERE YEAR(@domain_start_date) > 0
            	AND @domain_concept_id > 0
            GROUP BY @domain_concept_id,
            	YEAR(@domain_start_date);"
  sql3 <- "INSERT INTO #concept_count_temp
            SELECT @domain_concept_id concept_id,
            	0 as event_year,
            	0 as event_month,
          		'Y' concept_is_standard,
            	COUNT_BIG(*) concept_count,
            	COUNT_BIG(DISTINCT person_id) subject_count
            FROM @cdm_database_schema.@domain_table
            INNER JOIN (
            	SELECT DISTINCT concept_id
            	FROM @concept_id_universe
            	) c
            	ON @domain_concept_id = concept_id
            WHERE YEAR(@domain_start_date) > 0
            AND @domain_concept_id > 0
            GROUP BY @domain_concept_id;"
  
  
  sql4 <- "INSERT INTO #concept_count_temp
          	SELECT @domain_concept_id concept_id,
          		YEAR(@domain_start_date) event_year,
          		MONTH(@domain_start_date) event_month,
          		'N' concept_is_standard,
          		COUNT_BIG(*) concept_count,
          		COUNT_BIG(DISTINCT person_id) subject_count
          	FROM @cdm_database_schema.@domain_table dt
          	INNER JOIN (
          		SELECT DISTINCT concept_id
          		FROM @concept_id_universe
          		) c ON @domain_concept_id = concept_id
          	LEFT JOIN (
          	  SELECT DISTINCT concept_id
          	  FROM #concept_count_temp
          	  WHERE concept_is_standard = 'Y'
          	) std
          	ON @domain_concept_id = std.concept_id
          	WHERE YEAR(@domain_start_date) > 0
          		AND @domain_concept_id > 0
          		AND std.concept_id IS NULL
          	GROUP BY @domain_concept_id,
          		YEAR(@domain_start_date),
          		MONTH(@domain_start_date);"
  sql5 <- " INSERT INTO #concept_count_temp
            SELECT @domain_concept_id concept_id,
            	YEAR(@domain_start_date) event_year,
            	0 AS event_month,
            	'N' concept_is_standard,
            	COUNT_BIG(*) concept_count,
            	COUNT_BIG(DISTINCT person_id) subject_count
            FROM @cdm_database_schema.@domain_table
            INNER JOIN (
            	SELECT DISTINCT concept_id
            	FROM @concept_id_universe
            	) c ON @domain_concept_id = concept_id
            LEFT JOIN (
            	SELECT DISTINCT concept_id
            	FROM #concept_count_temp
          	  WHERE concept_is_standard = 'Y'
            	) std ON @domain_concept_id = std.concept_id
            WHERE YEAR(@domain_start_date) > 0
            	AND @domain_concept_id > 0
            	AND std.concept_id IS NULL
            GROUP BY @domain_concept_id,
            	YEAR(@domain_start_date);"
  sql6 <- " INSERT INTO #concept_count_temp
            SELECT @domain_concept_id concept_id,
            	0 AS event_year,
            	0 AS event_month,
            	'N' concept_is_standard,
            	COUNT_BIG(*) concept_count,
            	COUNT_BIG(DISTINCT person_id) subject_count
            FROM @cdm_database_schema.@domain_table
            INNER JOIN (
            	SELECT DISTINCT concept_id
            	FROM @concept_id_universe
            	) c ON @domain_concept_id = concept_id
            LEFT JOIN (
            	SELECT DISTINCT concept_id
            	FROM #concept_count_temp
          	  WHERE concept_is_standard = 'Y'
            	) std ON @domain_concept_id = std.concept_id
            WHERE YEAR(@domain_start_date) > 0
            	AND @domain_concept_id > 0
            	AND std.concept_id IS NULL
            GROUP BY @domain_concept_id;"
  
  for (i in (1:nrow(domains))) {
    rowData <- domains[i,]
    ParallelLogger::logTrace(paste0(
      "   - Working on ",
      rowData$domainTable,
      ".",
      rowData$domainConceptId
    ))
    ParallelLogger::logTrace("    - Counting concepts by calendar month and year")
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql1,
      tempEmulationSchema = tempEmulationSchema,
      domain_table = rowData$domainTable,
      domain_concept_id = rowData$domainConceptId,
      cdm_database_schema = cdmDatabaseSchema,
      domain_start_date = rowData$domainStartDate,
      concept_id_universe = conceptIdUniverse,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    ParallelLogger::logTrace("    - Counting concepts by calendar year")
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql2,
      tempEmulationSchema = tempEmulationSchema,
      domain_table = rowData$domainTable,
      domain_concept_id = rowData$domainConceptId,
      cdm_database_schema = cdmDatabaseSchema,
      domain_start_date = rowData$domainStartDate,
      concept_id_universe = conceptIdUniverse,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    ParallelLogger::logTrace("    - Counting concepts without calendar period")
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql3,
      tempEmulationSchema = tempEmulationSchema,
      domain_table = rowData$domainTable,
      domain_concept_id = rowData$domainConceptId,
      cdm_database_schema = cdmDatabaseSchema,
      domain_start_date = rowData$domainStartDate,
      concept_id_universe = conceptIdUniverse,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  for (i in (1:nrow(domains))) {
    rowData <- domains[i, ]
    if (nchar(rowData$domainSourceConceptId) > 4) {
      ParallelLogger::logTrace(
        paste0(
          "   - Working on ",
          rowData$domainTable,
          ".",
          rowData$domainSourceConceptId
        )
      )
      ParallelLogger::logTrace("    - Counting concepts by calendar month and year")
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql4,
        tempEmulationSchema = tempEmulationSchema,
        domain_table = rowData$domainTable,
        domain_concept_id = rowData$domainSourceConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        domain_start_date = rowData$domainStartDate,
        concept_id_universe = conceptIdUniverse,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
      ParallelLogger::logTrace("    - Counting concepts by calendar year")
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql5,
        tempEmulationSchema = tempEmulationSchema,
        domain_table = rowData$domainTable,
        domain_concept_id = rowData$domainSourceConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        domain_start_date = rowData$domainStartDate,
        concept_id_universe = conceptIdUniverse,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
      ParallelLogger::logTrace("    - Counting concepts - no calendar stratification")
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql6,
        tempEmulationSchema = tempEmulationSchema,
        domain_table = rowData$domainTable,
        domain_concept_id = rowData$domainSourceConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        domain_start_date = rowData$domainStartDate,
        concept_id_universe = conceptIdUniverse,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
  }
  retrieveSql <- "SELECT concept_id, event_year, event_month,
                    sum(concept_count) concept_count,
                    max(subject_count) subject_count
                  FROM #concept_count_temp
                  GROUP BY concept_id, event_year, event_month
                  ORDER By concept_id, event_year, event_month;"
  data <- renderTranslateQuerySql(
    connection = connection,
    sql = retrieveSql,
    snakeCaseToCamelCase = TRUE
  ) %>%
    dplyr::tibble()
  # i was thinking of keeping counts at the table level - but the file size became too big
  # so i decided to not include them
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdlDrop,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  return(data)
}


# function: getConceptOccurrenceRelativeToIndexDay ----
getConceptOccurrenceRelativeToIndexDay <- function(cohortIds,
                                                   connection,
                                                   cdmDatabaseSchema,
                                                   cohortDatabaseSchema,
                                                   cohortTable,
                                                   tempEmulationSchema,
                                                   indexDateDiagnosticsRelativeDays,
                                                   minCellCount,
                                                   conceptIdUniverse,
                                                   conceptIdToFilterIndexEvent) {
  if (is.null(minCellCount)) {
    minCellCount <- 0
  }
  if (minCellCount < 0) {
    minCellCount <- 0
  }
  
  IndexDateDiagnosticsRelativeDays <-
    indexDateDiagnosticsRelativeDays %>% sort() %>% unique()
  
  sqlVocabulary <-
    "IF OBJECT_ID('tempdb..#indx_concepts', 'U') IS NOT NULL
                	      DROP TABLE #indx_concepts;

                	  WITH c_ancestor
                    AS (
                    	SELECT DISTINCT -- cohort_id,
                    		descendant_concept_id concept_id
                    	FROM @cdm_database_schema.concept_ancestor ca
                    	INNER JOIN @concept_id_universe cu ON ancestor_concept_id = cu.concept_id
                    	),
                    all_concepts
                    AS (
                    	SELECT -- cohort_id,
                    		concept_id_2 concept_id
                    	FROM @cdm_database_schema.concept_relationship cr
                    	INNER JOIN c_ancestor ca ON concept_id_1 = ca.concept_id

                    	UNION

                    	SELECT -- cohort_id,
                    		concept_id
                    	FROM c_ancestor
                    	)
                    SELECT DISTINCT -- cohort_id,
                    	concept_id
                    INTO #indx_concepts
                    FROM all_concepts;"
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlVocabulary,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    concept_id_universe = conceptIdToFilterIndexEvent,
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema
  )
  
  if (!is.null(conceptIdUniverse)) {
    # keeping track
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql =  "INSERT INTO @concept_tracking_table
            SELECT DISTINCT concept_id
            FROM #indx_concepts
            WHERE concept_id != 0;",
      tempEmulationSchema = tempEmulationSchema,
      concept_tracking_table = conceptIdUniverse,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  domains <- getDomainInformation(packageName = 'CohortDiagnostics')
  domains <- domains$wide
  nonEraTables <- domains %>%
    #filtering out ERA tables because they are supposed to be derived tables, and counting them is double counting
    dplyr::filter(.data$isEraTable == FALSE) %>%
    dplyr::pull(.data$domainTableShort) %>%
    unique()
  domains <- domains %>%
    dplyr::filter(.data$domainTableShort %in% c(nonEraTables))
  
  sqlDdlDrop <-
    "IF OBJECT_ID('tempdb..#indx_breakdown', 'U') IS NOT NULL
                	      DROP TABLE #indx_breakdown;"
  sqlDdlCreate <- "
  CREATE TABLE #indx_breakdown (
                              	cohort_id BIGINT NOT NULL,
                              	days_relative_index BIGINT NOT NULL,
                              	concept_id INT NOT NULL,
                              	co_concept_id INT,
                              	subject_count BIGINT NOT NULL,
                              	concept_count BIGINT NOT NULL
                              	);"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdlDrop,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdlCreate,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  sqlConceptIdCount <- "INSERT INTO #indx_breakdown
                        SELECT cohort_definition_id cohort_id,
                        	@days_relative_index days_relative_index,
                        	d1.@domain_concept_id concept_id,
                        	0 co_concept_id,
                        	COUNT(DISTINCT c.subject_id) subject_count,
                        	COUNT(*) concept_count
                        FROM @cohort_database_schema.@cohort_table c
                        INNER JOIN @cdm_database_schema.@domain_table d1 ON c.subject_id = d1.person_id
                        	AND DATEADD('d', @days_relative_index, c.cohort_start_date) = d1.@domain_start_date
                        INNER JOIN #indx_concepts cu ON d1.@domain_concept_id = cu.concept_id
                        WHERE c.cohort_definition_id IN (@cohortIds)
                        	AND d1.@domain_concept_id != 0
                        	AND d1.@domain_concept_id IS NOT NULL
                        GROUP BY cohort_definition_id,
                        	d1.@domain_concept_id
                        HAVING count(DISTINCT c.subject_id) > @min_subject_count;"
  
  #conceptId is from _concept_id field of domain table and coConceptId is also from _concept_id field of same domain table
  # i.e. same day co-occurrence of two standard concept ids relative to index date
  
  sqlConceptIdCoConceptIdSameCount <- " INSERT INTO #indx_breakdown
                                        SELECT cohort_definition_id cohort_id,
                                        	@days_relative_index days_relative_index,
                                        	d1.@domain_concept_id concept_id,
                                        	d2.@domain_concept_id co_concept_id,
                                        	COUNT(DISTINCT c.subject_id) subject_count,
                                        	COUNT(DISTINCT CONCAT (
                                        			cast(d1.@domain_concept_id AS VARCHAR(30)),
                                        			'_',
                                        			cast(d2.@domain_concept_id AS VARCHAR(30)),
                                        			'_',
                                        			cast(c.subject_id AS VARCHAR(30))
                                        			)) concept_count
                                        FROM (
                                        	SELECT *
                                        	FROM @cohort_database_schema.@cohort_table
                                        	WHERE cohort_definition_id IN (@cohortIds)
                                        	) c
                                        INNER JOIN @cdm_database_schema.@domain_table d1 ON c.subject_id = d1.person_id
                                        	AND DATEADD('d', @days_relative_index, c.cohort_start_date) = d1.@domain_start_date
                                        INNER JOIN @cdm_database_schema.@domain_table d2 ON c.subject_id = d2.person_id
                                        	AND DATEADD('d', @days_relative_index, c.cohort_start_date) = d2.@domain_start_date
                                        -- AND d1.@domain_start_date = d2.@domain_start_date
                                        -- AND d1.person_id = d2.person_id
                                        INNER JOIN #indx_concepts cu1
                                        ON d1.@domain_concept_id = cu1.concept_id
                                        INNER JOIN #indx_concepts cu2
                                        ON d2.@domain_concept_id = cu2.concept_id
                                        WHERE d1.@domain_concept_id != d2.@domain_concept_id
                                        GROUP BY cohort_definition_id,
                                        	d1.@domain_concept_id,
                                        	d2.@domain_concept_id
                                        HAVING count(DISTINCT c.subject_id) > @min_subject_count
                                    ;"
  
  #conceptId is from _concept_id field of domain table and coConceptId is also from _source_concept_id field of same domain table
  # i.e. same day co-occurrence of concept ids where second (coConceptId) maybe non-standard relative to index date
  # the inner join to conceptIdUnivese to _concep_id limits to standard concepts in conceptIdUniverse - because only standard concept should be in _concept_id
  sqlConceptIdCoConceptIdOppositeCount <-
    " INSERT INTO #indx_breakdown
                                            SELECT cohort_definition_id cohort_id,
                                            	@days_relative_index days_relative_index,
                                            	d1.@domain_concept_id concept_id,
                                            	d2.@domain_source_concept_id co_concept_id,
                                            	COUNT(DISTINCT c.subject_id) subject_count,
                                            	COUNT(DISTINCT CONCAT (
                                            			cast(d1.@domain_concept_id AS VARCHAR(30)),
                                            			'_',
                                            			cast(d2.@domain_source_concept_id AS VARCHAR(30)),
                                            			'_',
                                            			cast(c.subject_id AS VARCHAR(30))
                                            			)) concept_count
                                            FROM (
                                            	SELECT *
                                            	FROM @cohort_database_schema.@cohort_table
                                            	WHERE cohort_definition_id IN (@cohortIds)
                                            	) c
                                            INNER JOIN @cdm_database_schema.@domain_table d1 ON c.subject_id = d1.person_id
                                            	AND DATEADD('d', @days_relative_index, c.cohort_start_date) = d1.@domain_start_date
                                            INNER JOIN @cdm_database_schema.@domain_table d2 ON c.subject_id = d2.person_id
                                            	AND DATEADD('d', @days_relative_index, c.cohort_start_date) = d2.@domain_start_date
                                            	-- AND d1.@domain_start_date = d2.@domain_start_date
                                            	-- AND d1.person_id = d2.person_id
                                            INNER JOIN #indx_concepts cu1
                                            ON d1.@domain_concept_id = cu1.concept_id
                                            INNER JOIN #indx_concepts cu2
                                            ON d1.@domain_source_concept_id = cu2.concept_id
                                            WHERE d1.@domain_concept_id != d2.@domain_source_concept_id
                                            GROUP BY cohort_definition_id,
                                            	d1.@domain_concept_id,
                                            	d2.@domain_source_concept_id
                                            HAVING count(DISTINCT c.subject_id) > @min_subject_count;"
  
  for (j in (1:length(IndexDateDiagnosticsRelativeDays))) {
    ParallelLogger::logTrace(
      paste0(
        "  - Working on ",
        scales::comma(x = IndexDateDiagnosticsRelativeDays[[j]]),
        " days relative to index date. ",
        scales::comma(j),
        " of ",
        scales::comma(length(IndexDateDiagnosticsRelativeDays)),
        "."
      )
    )
    for (i in (1:nrow(domains))) {
      rowData <- domains[i,]
      ParallelLogger::logTrace(paste0(
        "   - Working on ",
        rowData$domainTable,
        ".",
        rowData$domainConceptId
      ))
      ParallelLogger::logTrace(
        paste0(
          "      - Performing concept count - ",
          rowData$domainConceptId,
          " field of ",
          rowData$domainTable,
          " table"
        )
      )
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sqlConceptIdCount,
        tempEmulationSchema = tempEmulationSchema,
        domain_table = rowData$domainTable,
        domain_concept_id = rowData$domainConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        cohort_database_schema = cohortDatabaseSchema,
        domain_start_date = rowData$domainStartDate,
        cohortIds = cohortIds,
        cohort_table = cohortTable,
        days_relative_index = IndexDateDiagnosticsRelativeDays[[j]],
        min_subject_count = minCellCount,
        reportOverallTime = FALSE,
        progressBar = FALSE
      )
      
      ParallelLogger::logTrace(
        paste0(
          "      - Performing co-concept count - ",
          rowData$domainConceptId,
          " field of ",
          rowData$domainTable,
          " table"
        )
      )
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sqlConceptIdCoConceptIdSameCount,
        tempEmulationSchema = tempEmulationSchema,
        domain_table = rowData$domainTable,
        domain_concept_id = rowData$domainConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        cohort_database_schema = cohortDatabaseSchema,
        domain_start_date = rowData$domainStartDate,
        cohortIds = cohortIds,
        cohort_table = cohortTable,
        days_relative_index = IndexDateDiagnosticsRelativeDays[[j]],
        min_subject_count = minCellCount,
        reportOverallTime = FALSE,
        progressBar = FALSE
      )
      
      if (all(
        !is.na(rowData$domainSourceConceptId),
        nchar(rowData$domainSourceConceptId) > 4
      )) {
        ParallelLogger::logTrace(
          paste0(
            "   - Performing concept count - ",
            rowData$domainSourceConceptId,
            " field of ",
            rowData$domainTable,
            " table"
          )
        )
        DatabaseConnector::renderTranslateExecuteSql(
          connection = connection,
          sql = sqlConceptIdCount,
          tempEmulationSchema = tempEmulationSchema,
          domain_table = rowData$domainTable,
          domain_concept_id = rowData$domainSourceConceptId,
          cdm_database_schema = cdmDatabaseSchema,
          cohort_database_schema = cohortDatabaseSchema,
          domain_start_date = rowData$domainStartDate,
          cohortIds = cohortIds,
          cohort_table = cohortTable,
          days_relative_index = IndexDateDiagnosticsRelativeDays[[j]],
          min_subject_count = minCellCount,
          reportOverallTime = FALSE,
          progressBar = FALSE
        )
        ParallelLogger::logTrace(
          paste0(
            "      - Performing co-concept count - ",
            rowData$domainSourceConceptId,
            " field of ",
            rowData$domainTable,
            " table"
          )
        )
        DatabaseConnector::renderTranslateExecuteSql(
          connection = connection,
          sql = sqlConceptIdCoConceptIdSameCount,
          tempEmulationSchema = tempEmulationSchema,
          domain_table = rowData$domainTable,
          domain_concept_id = rowData$domainSourceConceptId,
          cdm_database_schema = cdmDatabaseSchema,
          cohort_database_schema = cohortDatabaseSchema,
          domain_start_date = rowData$domainStartDate,
          cohortIds = cohortIds,
          cohort_table = cohortTable,
          days_relative_index = IndexDateDiagnosticsRelativeDays[[j]],
          min_subject_count = minCellCount,
          reportOverallTime = FALSE,
          progressBar = FALSE
        )
        
        ParallelLogger::logTrace(
          paste0(
            "      - Performing co-concept count - ",
            rowData$domainConceptId,
            " field and ",
            rowData$domainSourceConceptId,
            " of ",
            rowData$domainTable,
            " table"
          )
        )
        DatabaseConnector::renderTranslateExecuteSql(
          connection = connection,
          sql = sqlConceptIdCoConceptIdOppositeCount,
          tempEmulationSchema = tempEmulationSchema,
          domain_table = rowData$domainTable,
          domain_concept_id = rowData$domainConceptId,
          domain_source_concept_id = rowData$domainSourceConceptId,
          cdm_database_schema = cdmDatabaseSchema,
          cohort_database_schema = cohortDatabaseSchema,
          domain_start_date = rowData$domainStartDate,
          cohortIds = cohortIds,
          cohort_table = cohortTable,
          days_relative_index = IndexDateDiagnosticsRelativeDays[[j]],
          min_subject_count = minCellCount,
          reportOverallTime = FALSE,
          progressBar = FALSE
        )
      }
    }
  }
  sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM #indx_breakdown;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    concept_id_table = "#concept_tracking",
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  #avoid any potential duplication
  #removes domain table - counts are retained from the domain table that has the most prevalence concept id -
  #assumption here is that a conceptId should not be in more than one domain table
  data <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT DISTINCT cohort_id,
                days_relative_index,
                concept_id,
                co_concept_id,
                max(subject_count) subject_count,
                max(concept_count) concept_count
              FROM #indx_breakdown
              group by cohort_id,
                days_relative_index,
                concept_id,
                co_concept_id
              order by cohort_id,
                concept_id,
                co_concept_id,
                days_relative_index;",
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()
  # i was thinking of keeping counts at the table level - but the file size became too big
  # so i decided to not include them - as conceptId's are expected to be in their own domains
  # keeping the max of subject_count or concept_count for any domain table, discarding the rest
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdlDrop,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  return(data)
}


# function: getConceptSourceStandardMapping ----
getConceptSourceStandardMapping <- function(connection,
                                            cdmDatabaseSchema,
                                            tempEmulationSchema,
                                            conceptIdUniverse = "#concept_tracking") {
  domains <- getDomainInformation(packageName = 'CohortDiagnostics')
  domains <- domains$wide %>%
    dplyr::filter(nchar(.data$domainSourceConceptId) > 1)
  
  sqlConceptMapping <-
    "IF OBJECT_ID('tempdb..#concept_mapping', 'U') IS NOT NULL
                      	DROP TABLE #concept_mapping;
                      CREATE TABLE #concept_mapping (concept_id INT,
                                                    source_concept_id INT,
                                                    domain_table VARCHAR(20),
                                                    concept_count BIGINT,
                                                    subject_count BIGINT);"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlConceptMapping,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  sqlMapping <- "WITH concept_id_universe
          AS (
          	SELECT DISTINCT concept_id
          	FROM @concept_id_universe
          	)
          INSERT INTO #concept_mapping
          SELECT @domain_concept_id concept_id,
          	@domain_source_concept_id source_concept_id,
          	'@domainTableShort' domain_table,
          	COUNT(*) AS concept_count,
          	COUNT(DISTINCT person_id) AS subject_count
          FROM @cdm_database_schema.@domain_table
          INNER JOIN concept_id_universe a ON @domain_concept_id = a.concept_id
          WHERE (
          		@domain_source_concept_id IS NOT NULL
          		AND @domain_source_concept_id > 0
          		)
          GROUP BY @domain_concept_id,
          	@domain_source_concept_id
          ORDER BY @domain_concept_id,
          	@domain_source_concept_id;"
  
  conceptMapping <- list()
  for (i in (1:nrow(domains))) {
    rowData <- domains[i,]
    ParallelLogger::logTrace(paste0(
      "  - Working on ",
      rowData$domainTable,
      ".",
      rowData$domainConceptId
    ))
    renderTranslateExecuteSql(
      connection = connection,
      sql = sqlMapping,
      tempEmulationSchema = tempEmulationSchema,
      domain_table = rowData$domainTable,
      domain_concept_id = rowData$domainConceptId,
      domain_source_concept_id = rowData$domainSourceConceptId,
      cdm_database_schema = cdmDatabaseSchema,
      concept_id_universe = conceptIdUniverse,
      domainTableShort = rowData$domainTableShort,
      reportOverallTime = FALSE,
      progressBar = FALSE
    )
  }
  sql <- "SELECT * FROM #concept_mapping;"
  conceptMapping <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema
    ) %>%
    dplyr::distinct() %>%
    dplyr::arrange(
      .data$domainTable,
      .data$conceptId,
      .data$sourceConceptId,
      .data$conceptCount,
      .data$subjectCount
    )
  conceptMapping <- dplyr::bind_rows(
    conceptMapping,
    conceptMapping %>%
      dplyr::group_by(.data$conceptId,
                      .data$sourceConceptId) %>%
      dplyr::summarise(
        conceptCount = sum(.data$conceptCount),
        subjectCount = max(.data$subjectCount),
        .groups = "keep"
      ) %>%
      dplyr::mutate(domainTable = "All")
  ) %>%
    dplyr::distinct()
  
  sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT source_concept_id concept_id
                  FROM #concept_mapping;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    concept_id_table = "#concept_tracking",
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  sqlDdlDrop <-
    "IF OBJECT_ID('tempdb..#concept_mapping', 'U') IS NOT NULL
                	      DROP TABLE #concept_mapping;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdlDrop,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
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
      conceptSetWithExclude[[i]] <- conceptSetWithExclude[[i]][0, ]
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
            and concept_id != 0

            UNION

            SELECT DISTINCT descendant_concept_id concept_id
            FROM @vocabulary_database_schema.concept_ancestor
            WHERE ancestor_concept_id IN (@descendants)
            and ancestor_concept_id != 0
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
          dplyr::mutate(
            excluded = as.integer(.data$isExcluded),
            removed = 0
          ) %>%
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
    
    if (!hasData(conceptSetConceptIdsExcluded)) {
      conceptSetConceptIdsExcluded <- 0
    }
    if (!hasData(conceptSetConceptIdsDescendantsExcluded)) {
      conceptSetConceptIdsDescendantsExcluded <- 0
    }
    if (!hasData(conceptSetConceptIdsNotExcluded)) {
      conceptSetConceptIdsNotExcluded <- 0
    }
    if (!hasData(conceptSetConceptIdsDescendantsNotExcluded)) {
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
    
    data <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT * FROM #optimized_set;",
        snakeCaseToCamelCase = TRUE
      )
    
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




executeConceptSetDiagnostics <- function(connection,
                                         tempEmulationSchema,
                                         cdmDatabaseSchema,
                                         vocabularyDatabaseSchema,
                                         databaseId,
                                         cohorts,
                                         runIncludedSourceConcepts,
                                         runOrphanConcepts,
                                         runBreakdownIndexEvents,
                                         indexDateDiagnosticsRelativeDays,
                                         exportFolder,
                                         minCellCount,
                                         keepCustomConceptId,
                                         conceptCountsDatabaseSchema,
                                         conceptCountsTable,
                                         conceptCountsTableIsTemp,
                                         cohortDatabaseSchema,
                                         cohortTable,
                                         useExternalConceptCountsTable,
                                         incremental,
                                         conceptIdTable,
                                         recordKeepingFile) {
  ParallelLogger::logInfo(" - Beginning concept set diagnostics.")
  startConceptSetDiagnostics <- Sys.time()
  subset <- subsetToRequiredCohorts(
    cohorts = cohorts,
    task = "runConceptSetDiagnostics",
    incremental = incremental,
    recordKeepingFile = recordKeepingFile
  )
  if (nrow(subset) > 0) {
    if (nrow(cohorts) - nrow(subset) > 0) {
      ParallelLogger::logInfo(sprintf(
        "  - Skipping %s cohorts in incremental mode.",
        nrow(cohorts) - nrow(subset)
      ))
    }
    output <- runConceptSetDiagnostics(
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      cohorts = cohorts,
      cohortIds = subset$cohortId,
      cohortDatabaseSchema = cohortDatabaseSchema,
      indexDateDiagnosticsRelativeDays = indexDateDiagnosticsRelativeDays,
      cohortTable = cohortTable,
      minCellCount = minCellCount
    )
    writeToAllOutputToCsv(
      object = output,
      exportFolder = exportFolder,
      databaseId = databaseId,
      incremental = incremental,
      minCellCount = minCellCount
    )
    Andromeda::close(output)
    rm("output")
    recordTasksDone(
      cohortId = subset$cohortId,
      task = "runConceptSetDiagnostics",
      checksum = subset$checksum,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
  } else {
    ParallelLogger::logInfo("  - Skipping in incremental mode.")
  }
  delta <- Sys.time() - startConceptSetDiagnostics
  ParallelLogger::logInfo(
    " - Running Concept Set Diagnostics and saving files took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
}





getConceptSetDetailsFromCohortDefinition <-
  function(cohortDefinitionExpression) {
    if ("expression" %in% names(cohortDefinitionExpression)) {
      expression <- cohortDefinitionExpression$expression
    } else {
      expression <- cohortDefinitionExpression
    }
    
    if (is.null(expression$ConceptSets)) {
      return(NULL)
    }
    
    conceptSetExpression <- expression$ConceptSets %>%
      dplyr::bind_rows() %>%
      dplyr::mutate(json = RJSONIO::toJSON(x = .data$expression,
                                           pretty = TRUE))
    
    conceptSetExpressionDetails <- list()
    i <- 0
    for (id in conceptSetExpression$id) {
      i <- i + 1
      conceptSetExpressionDetails[[i]] <-
        getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression =
                                                         conceptSetExpression[i, ]$expression$items) %>%
        dplyr::mutate(id = conceptSetExpression[i,]$id) %>%
        dplyr::relocate(.data$id) %>%
        dplyr::arrange(.data$id)
    }
    conceptSetExpressionDetails <-
      dplyr::bind_rows(conceptSetExpressionDetails)
    output <- list(conceptSetExpression = conceptSetExpression,
                   conceptSetExpressionDetails = conceptSetExpressionDetails)
    return(output)
  }


getConceptSetDataFrameFromConceptSetExpression <-
  function(conceptSetExpression) {
    if ("items" %in% names(conceptSetExpression)) {
      items <- conceptSetExpression$items
    } else {
      items <- conceptSetExpression
    }
    conceptSetExpressionDetails <- items %>%
      purrr::map_df(.f = purrr::flatten)
    if ('CONCEPT_ID' %in% colnames(conceptSetExpressionDetails)) {
      if ('isExcluded' %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(IS_EXCLUDED = .data$isExcluded)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(IS_EXCLUDED = FALSE)
      }
      if ('includeDescendants' %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(INCLUDE_DESCENDANTS = .data$includeDescendants)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(INCLUDE_DESCENDANTS = FALSE)
      }
      if ('includeMapped' %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(INCLUDE_MAPPED = .data$includeMapped)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(INCLUDE_MAPPED = FALSE)
      }
      conceptSetExpressionDetails <-
        conceptSetExpressionDetails %>%
        tidyr::replace_na(list(
          IS_EXCLUDED = FALSE,
          INCLUDE_DESCENDANTS = FALSE,
          INCLUDE_MAPPED = FALSE
        ))
      colnames(conceptSetExpressionDetails) <-
        snakeCaseToCamelCase(colnames(conceptSetExpressionDetails))
    }
    return(conceptSetExpressionDetails)
  }
