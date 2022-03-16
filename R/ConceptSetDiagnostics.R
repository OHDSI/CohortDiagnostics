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
#' @param runConceptSetOptimization   Generate and export optimized concept set expression?
#' 
#' @param runExcludedConceptSet       Generate and export excluded concept id in concept set expression?
#' 
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' 
#' @param runBreakdownIndexEvents     Generate and export breakdown of index events?
#' 
#' @param runBreakdownIndexEventRelativeDays (optional) array of days to offset breakdown of index events. 
#'                                    default value is 0 i.e. no offsets. Options include c(-5:5) to calculate a 
#'                                    range of days starting -5 of cohort start to +5 of cohort start.
#'                                    
#' @param runStandardToSourceMappingCount Generate and export counts for mapping between standard to non standard as observed in data.
#' 
#' @param runIndexDateConceptCoOccurrence      Generate concept co-occurrence on index date
#' 
#' @param runConceptCount             Generate and export count of concept id
#' 
#' @param runConceptCountByCalendarPeriod  Do you want to stratify the counts by calendar period like calendar year, calendar month?
#'
#' @param minCellCount                The minimum cell count for fields contains person counts or fractions.
#'
#' @param keep2BillionConceptId       (Optional) Default FALSE. Do you want to keep concept id above 2 billion.
#'                                    Per OMOP conventions any conceptId >= 2 billion are considered site specific
#'                                    custom value that are not shipped as part of default OMOP vocabulary tables.
#'
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    diagnostics to.
#' 
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
                                     keep2BillionConceptId = FALSE,
                                     runConceptSetOptimization = TRUE,
                                     runExcludedConceptSet = TRUE,
                                     runOrphanConcepts = TRUE,
                                     runBreakdownIndexEvents = TRUE,
                                     runBreakdownIndexEventRelativeDays = c(0),
                                     runIndexDateConceptCoOccurrence = FALSE,
                                     runStandardToSourceMappingCount = TRUE,
                                     runConceptCount = TRUE,
                                     runConceptCountByCalendarPeriod = FALSE,
                                     minCellCount = 5) {
  ParallelLogger::logTrace(" - Running concept set diagnostics")
  startConceptSetDiagnostics <- Sys.time()
  if (length(cohortIds) == 0) {
    ParallelLogger::logTrace("  - Running concept set diagnostics for all cohorts.")
    cohortIds <- cohorts$cohortId %>% unique()
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
  #upload crosswalk of unique concept set and cohort concept
  conceptSetsXWalk <-
    conceptSetDiagnosticsResults$conceptSets %>% 
    dplyr::collect() %>% 
    dplyr::select(.data$uniqueConceptSetId,
                  .data$cohortId,
                  .data$conceptSetId)
  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "#concept_sets_x_walk", 
                                 data = conceptSetsXWalk,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 tempEmulationSchema = tempEmulationSchema, 
                                 camelCaseToSnakeCase = TRUE)
  
  conceptTrackingTable <- "#concept_tracking"
  ## Create concept tracking table----
  ParallelLogger::logTrace(" - Creating concept ID table for tracking concepts used in diagnostics")
  sql <-
    "DROP TABLE IF EXISTS @concept_tracking_table;
     CREATE TABLE @concept_tracking_table (unique_concept_set_id INT, concept_id INT);
  "
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    concept_tracking_table = conceptTrackingTable,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  ## Unique concept sets----
  conceptSetsT <- conceptSetDiagnosticsResults$conceptSets %>% 
    dplyr::collect()
  uniqueConceptSets <-
    conceptSetsT[!duplicated(conceptSetsT$uniqueConceptSetId),] %>%
    dplyr::select(-.data$cohortId,-.data$conceptSetId)
  rm("conceptSetsT")
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
  
  if (runConceptSetOptimization) {
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
      uniqueConceptSet <- uniqueConceptSets[i,]
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
            conceptSetsXWalk %>%
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
  }
  

  ## Instantiate (resolve) unique concept sets----
  ### resolving concept set is required (not optional) and common to all diagnostics
  ParallelLogger::logInfo("  - Resolving concept sets found in cohorts.")
  startInstantiateConceptSet <- Sys.time()
  conceptSetDiagnosticsResults$conceptResolved <-
    resolveConceptSets(
      uniqueConceptSets = uniqueConceptSets,
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      resolvedConceptsTable = "#resolved_concept_set", #ensures the temp table is usable
      conceptTrackingTable = conceptTrackingTable,
      conceptSetsXWalk = "#concept_sets_x_walk",
      keep2BillionConceptId = keep2BillionConceptId
    )
  delta <- Sys.time() - startInstantiateConceptSet
  ParallelLogger::logTrace("  - Instantiating concept sets took ",
                           signif(delta, 3),
                           " ",
                           attr(delta, "units"))
  
  
  if (runExcludedConceptSet) {
    ## Excluded concepts ----
    ParallelLogger::logInfo("  - Collecting excluded concepts.")
    excludedConceptsStart <- Sys.time()
    conceptSetDiagnosticsResults$conceptExcluded <-
      getExcludedConceptSets(
        connection = connection,
        uniqueConceptSets = uniqueConceptSets,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        conceptSetsXWalk = "#concept_sets_x_walk",
        conceptTrackingTable = conceptTrackingTable,
        keep2BillionConceptId = keep2BillionConceptId
      )
    delta <- Sys.time() - excludedConceptsStart
    ParallelLogger::logTrace("  - Collecting excluded concepts took ",
                             signif(delta, 3),
                             " ",
                             attr(delta, "units"))
  }

  if (runOrphanConcepts) {
    ## Orphan concepts ----
    ParallelLogger::logInfo("  - Searching for concepts that may have been orphaned.")
    startOrphanCodes <- Sys.time()
    # conceptSetDiagnosticsResults$orphanConcept <- getOrphanConcepts(
    #   connection = connection,
    #   cdmDatabaseSchema = cdmDatabaseSchema,
    #   vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    #   tempEmulationSchema = tempEmulationSchema,
    #   conceptTrackingTable = conceptTrackingTable,
    #   conceptSetsXWalk = "#concept_sets_x_walk",
    #   resolvedConceptSets = "#resolved_concept_set",
    #   keep2BillionConceptId = keep2BillionConceptId
    # )
    delta <- Sys.time() - startOrphanCodes
    ParallelLogger::logTrace("  - Finding orphan concepts took ",
                             signif(delta, 3),
                             " ",
                             attr(delta, "units"))
  }
  
  if (runStandardToSourceMappingCount) {
    ## Counting standard to source concept mapping----
    ParallelLogger::logInfo("  - Counting standard to source concept mapping.")
    startconceptStdSrcCnt <- Sys.time()
    conceptSetDiagnosticsResults$conceptStdSrcCnt <-
      getConceptStandardSourceMappingCount(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        conceptTrackingTable = conceptTrackingTable,
        keep2BillionConceptId = keep2BillionConceptId
      )
    delta <- Sys.time() - startconceptStdSrcCnt
    ParallelLogger::logTrace(
      "  - Counting standard to source concept mapping mapping took ",
      signif(delta, 3),
      " ",
      attr(delta, "units")
    )
  }
  
  if (runBreakdownIndexEvents) {
    ## Index event breakdown ----
    ParallelLogger::logInfo("  - Learning about the breakdown in index events.")
    startBreakdownEvents <- Sys.time()
    ableToRunBreakdownEvents <- TRUE
    
    if (is.null(cohortTable)) {
      ParallelLogger::logInfo("   - Skipping because no cohort table provided.")
      ableToRunBreakdownEvents <- FALSE
    }
    if (ableToRunBreakdownEvents) {
      cohortCounts <- getCohortCounts(
        connection = connection,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cohortIds = cohorts$cohortId
      )
      if (is.null(cohortCounts)) {
        ParallelLogger::logInfo("   - Skipping because no instantiated cohorts found.")
        ableToRunBreakdownEvents <- FALSE
      }
    }
    
    if (ableToRunBreakdownEvents) {
      instantiatedCohorts <- cohortCounts %>%
        dplyr::filter(.data$cohortEntries > 0) %>%
        dplyr::pull(.data$cohortId)
      if (length(instantiatedCohorts) == 0) {
        ableToRunBreakdownEvents <- FALSE
        ParallelLogger::logInfo("   - Skipping because no instantiated cohorts found.")
      }
    }
    
    if (ableToRunBreakdownEvents) {
      if (length(instantiatedCohorts) > 0) {
        conceptSetDiagnosticsResults$indexEventBreakdown <-
          getConceptOccurrenceRelativeToIndexDay(
            cohortIds = instantiatedCohorts,
            connection = connection,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortDatabaseSchema = cohortDatabaseSchema,
            cohortTable = cohortTable,
            minCellCount = minCellCount,
            tempEmulationSchema = tempEmulationSchema,
            conceptSetsXWalk = "#concept_sets_x_walk",
            conceptTrackingTable = conceptTrackingTable,
            runBreakdownIndexEventRelativeDays = runBreakdownIndexEventRelativeDays,
            runIndexDateConceptCoOccurrence = runIndexDateConceptCoOccurrence
          )
      }
    }
    delta <- (Sys.time() - startBreakdownEvents)
    ParallelLogger::logTrace("  - Index event breakdown took ",
                             signif(delta, 3),
                             " ",
                             attr(delta, "units"))
  }
  
  #For some domains (e.g. Visit download all vocabulary - as it is used in visit context etc)
  #Track those concept_id as -1 codeset_id
  #these are essential for visit_context
  ParallelLogger::logTrace(" - Creating concept ID table for tracking concepts used in diagnostics")
  sql <-
    "INSERT INTO @concept_tracking_table
    SELECT DISTINCT -1 unique_concept_set_id, CONCEPT_ID
    FROM @vocabulary_database_schema.concept
    WHERE domain_id IN ('Visit', 'Ethnicity', 'Race', 'Gender', 'Place of Service', 'Type Concept');
  "
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    vocabulary_database_schema = vocabularyDatabaseSchema,
    concept_tracking_table = conceptTrackingTable,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  if (runConceptCount) {
    ## Concept count----
    ParallelLogger::logInfo("  - Counting concepts in data source.")
    
    conceptSetDiagnosticsResults$conceptCount <-
      getConceptRecordCount(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        conceptTrackingTable = conceptTrackingTable,
        runConceptCountByCalendarPeriod = runConceptCountByCalendarPeriod,
        minCellCount = minCellCount
      )
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
      SqlRender::camelCaseToTitleCase(vocabularyTables1[[i]])
    ),
    "'")
    sql <- "SELECT * FROM @vocabulary_database_schema.@table;"
    conceptSetDiagnosticsResults[[vocabularyTables1[[i]]]] <-
      renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        vocabulary_database_schema = vocabularyDatabaseSchema,
        table = SqlRender::camelCaseToSnakeCase(vocabularyTables1[[i]]),
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble()
  }
  
  ## Partial data -----
  vocabularyTables2 <- c('concept', "conceptSynonym")
  for (i in (1:length(vocabularyTables2))) {
    ParallelLogger::logInfo(paste0(
      "   - Retrieving '",
      SqlRender::camelCaseToTitleCase(vocabularyTables2[[i]])
    ),
    "'")
    sql <- "SELECT DISTINCT a.* FROM @vocabulary_database_schema.@table a
          INNER JOIN
            (SELECT distinct concept_id FROM @concept_tracking_table) b
          ON a.concept_id = b.concept_id;"
    conceptSetDiagnosticsResults[[vocabularyTables2[[i]]]] <-
      renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        vocabulary_database_schema = vocabularyDatabaseSchema,
        table = SqlRender::camelCaseToSnakeCase(vocabularyTables2[[i]]),
        concept_tracking_table = conceptTrackingTable,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::tibble()
  }
  
  ParallelLogger::logInfo("   - Retrieving 'Concept Relationship'")
  sql <-
    " WITH concept_id_table (concept_id)
      AS (
      	SELECT DISTINCT concept_id
      	FROM @concept_tracking_table
      	)
      SELECT DISTINCT f.*
      FROM (
      	SELECT a.*
      	FROM @vocabulary_database_schema.concept_relationship a
      	INNER JOIN concept_id_table b1 ON a.concept_id_1 = b1.concept_id
			  WHERE a.invalid_reason IS NULL
      	
      	UNION ALL
      	
      	SELECT b.*
      	FROM @vocabulary_database_schema.concept_relationship b
      	INNER JOIN concept_id_table b2 ON b.concept_id_2 = b2.concept_id
			  WHERE b.invalid_reason IS NULL
      	) f
      ORDER BY concept_id_1,
      	concept_id_2;"
  conceptSetDiagnosticsResults$conceptRelationship <-
    renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      vocabulary_database_schema = vocabularyDatabaseSchema,
      concept_tracking_table = conceptTrackingTable,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()
  

  ParallelLogger::logInfo("   - Retrieving 'Concept Ancestor'")
  sql <-
    " WITH concept_id_table (concept_id)
      AS (
      	SELECT DISTINCT concept_id
      	FROM @concept_tracking_table
      	)
      SELECT DISTINCT f.*
      FROM (
      	SELECT a.*
      	FROM @vocabulary_database_schema.concept_ancestor a
      	INNER JOIN concept_id_table b1 ON a.ancestor_concept_id = b1.concept_id

      	UNION ALL

      	SELECT b.*
      	FROM @vocabulary_database_schema.concept_ancestor b
      	INNER JOIN concept_id_table b2 ON b.descendant_concept_id = b2.concept_id
      	) f
      ORDER BY ancestor_concept_id, descendant_concept_id;"
  conceptSetDiagnosticsResults$conceptAncestor <-
    renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      vocabulary_database_schema = vocabularyDatabaseSchema,
      concept_tracking_table = conceptTrackingTable,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()
  
  
  # Clean up----
  # Drop temporary tables
  ParallelLogger::logTrace(" - Dropping temporary tables")
  sql <-
    " DROP TABLE IF EXISTS #resolved_concept_set;
      DROP TABLE IF EXISTS @concept_tracking_table;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    concept_tracking_table = conceptTrackingTable,
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
                               resolvedConceptsTable = NULL,
                               conceptSetsXWalk = "#concept_sets_x_walk",
                               conceptTrackingTable = NULL,
                               keep2BillionConceptId = FALSE) {
  if (is.null(resolvedConceptsTable)) {
    dropResolvedConceptsTable <- TRUE
  } else {
    dropResolvedConceptsTable <- FALSE
  }
  # to optimize compute and to treat repeated concept set as unique, replace codeset_id with unique_concept_set_id
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
    tableName = resolvedConceptsTable,
    tempTables = tempTables,
    tempEmulationSchema = tempEmulationSchema
  )
  
  if (!is.null(conceptTrackingTable)) {
    # keeping track of concept_id by unique_concept_set_id
    #rename codeset_id as unique_concept_set_id
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql =  "INSERT INTO @concept_tracking_table
            SELECT DISTINCT codeset_id unique_concept_set_id, concept_id
            FROM @resolved_concept_set_table
            WHERE concept_id != 0
            {@keep_custom_concept_id} ? {} : {AND concept_id < 200000000};",
      tempEmulationSchema = tempEmulationSchema,
      concept_tracking_table = conceptTrackingTable,
      resolved_concept_set_table = resolvedConceptsTable,
      keep_custom_concept_id = keep2BillionConceptId,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  # note usage of codeset_id instead of concept_set_id - they are the same
  # unique_code_set_id is crosswalked back to cohort level codeset_id
  if (is.null(conceptSetsXWalk)) {
    sql = " Select DISTINCT 0 cohort_id, codeset_id concept_set_id, concept_id
              FROM @resolved_concept_set_table cs
              {@keep_custom_concept_id} ? {} : {WHERE cs.concept_id < 200000000};"
    resolvedConcepts <-
      renderTranslateQuerySql(
        sql = sql,
        connection = connection,
        snakeCaseToCamelCase = TRUE,
        tempEmulationSchema = tempEmulationSchema,
        resolved_concept_set_table = resolvedConceptsTable,
        keep_custom_concept_id = keep2BillionConceptId
      )
  } else {
    sql = " Select DISTINCT unq.cohort_id, unq.concept_set_id concept_set_id, cs.concept_id
              FROM @resolved_concept_set_table cs
              INNER JOIN @concept_sets_x_walk unq
              ON cs.codeset_id = unq.unique_concept_set_id
              {@keep_custom_concept_id} ? {} : {WHERE cs.concept_id < 200000000};"
    resolvedConcepts <-
      renderTranslateQuerySql(
        sql = sql,
        connection = connection,
        snakeCaseToCamelCase = TRUE,
        tempEmulationSchema = tempEmulationSchema,
        resolved_concept_set_table = resolvedConceptsTable,
        concept_sets_x_walk = conceptSetsXWalk,
        keep_custom_concept_id = keep2BillionConceptId
      )
  }
  
  if (dropResolvedConceptsTable) {
    # Drop temporary tables
    ParallelLogger::logTrace(" - Dropping temporary table: resolved concept set")
    sql <- "DROP TABLE IF EXISTS #resolved_concept_set;"
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
                              resolvedConceptSets = "#resolved_concept_set",
                              conceptSetsXWalk = NULL,
                              conceptTrackingTable = NULL,
                              keep2BillionConceptId = FALSE
) {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql(
    "OrphanCodes.sql",
    packageName = utils::packageName(),
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    vocabulary_database_schema = vocabularyDatabaseSchema,
    resolved_concept_sets = resolvedConceptSets
  )
  ParallelLogger::logInfo("Starting Orphan concept string search. This might take some time.")
  browser()
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    profile = TRUE,
    progressBar = TRUE,
    reportOverallTime = TRUE
  )
  if (!is.null(conceptTrackingTable)) {
    # tracking table
    sql <-
      "INSERT INTO @concept_tracking_table (unique_concept_set_id, concept_id)
            SELECT DISTINCT codeset_id unique_concept_set_id, concept_id
            FROM #orphan_concept_table
            WHERE concept_id != 0
            {@keep_custom_concept_id} ? {} : {AND concept_id < 200000000};"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      concept_tracking_table = conceptTrackingTable,
      keep_custom_concept_id = keep2BillionConceptId,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  if (!is.null(conceptSetsXWalk)) {
    orphanCodes <- renderTranslateQuerySql(
      sql = " Select DISTINCT unq.cohort_id, unq.concept_set_id concept_set_id, cs.concept_id
              FROM #orphan_concept_table cs
              INNER JOIN @concept_sets_x_walk unq
              ON cs.codeset_id = unq.unique_concept_set_id
              {@keep_custom_concept_id} ? {} : {WHERE cs.concept_id < 200000000};",
      connection = connection,
      keep_custom_concept_id = keep2BillionConceptId,
      concept_sets_x_walk = conceptSetsXWalk,
      snakeCaseToCamelCase = TRUE
    )   
  } else {
    orphanCodes <- renderTranslateQuerySql(
      sql = " Select DISTINCT 0 cohort_id, codeset_id concept_set_id, concept_id
              FROM #orphan_concept_table cs
              {@keep_custom_concept_id} ? {} : {WHERE cs.concept_id < 200000000};",
      connection = connection,
      keep_custom_concept_id = keep2BillionConceptId,
      snakeCaseToCamelCase = TRUE
    )
  }

  sql <- "DROP TABLE IF EXISTS #orphan_concept_table;"
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
                                  conceptTrackingTable,
                                  runConceptCountByCalendarPeriod,
                                  minCellCount = 5) {
  domains <- getDomainInformation()
  domains <- domains$wide %>%
    dplyr::filter(.data$isEraTable == FALSE)
  #filtering out ERA tables because they are supposed to be derived tables, and counting them is double counting
  sqlDdlDrop <- "DROP TABLE IF EXISTS #concept_count_temp;"
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
          		FROM @concept_tracking_table
          		) c ON @domain_concept_id = concept_id
          	WHERE YEAR(@domain_start_date) > 0
          		AND @domain_concept_id > 0
          	GROUP BY @domain_concept_id,
          		YEAR(@domain_start_date),
          		MONTH(@domain_start_date)
            HAVING COUNT_BIG(DISTINCT person_id) > @min_cell_count;"
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
            	FROM @concept_tracking_table
            	) c ON @domain_concept_id = concept_id
            WHERE YEAR(@domain_start_date) > 0
            	AND @domain_concept_id > 0
            GROUP BY @domain_concept_id,
            	YEAR(@domain_start_date)
            HAVING COUNT_BIG(DISTINCT person_id) > @min_cell_count;"
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
            	FROM @concept_tracking_table
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
          		FROM @concept_tracking_table
          		) c ON @domain_concept_id = c.concept_id
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
          		MONTH(@domain_start_date)
            HAVING COUNT_BIG(DISTINCT person_id) > @min_cell_count;"
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
            	FROM @concept_tracking_table
            	) c ON @domain_concept_id = c.concept_id
            LEFT JOIN (
            	SELECT DISTINCT concept_id
            	FROM #concept_count_temp
          	  WHERE concept_is_standard = 'Y'
            	) std ON @domain_concept_id = std.concept_id
            WHERE YEAR(@domain_start_date) > 0
            	AND @domain_concept_id > 0
            	AND std.concept_id IS NULL
            GROUP BY @domain_concept_id,
            	YEAR(@domain_start_date)
            HAVING COUNT_BIG(DISTINCT person_id) > @min_cell_count;"
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
            	FROM @concept_tracking_table
            	) c ON @domain_concept_id = c.concept_id
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
    if (runConceptCountByCalendarPeriod) {
      ParallelLogger::logTrace("    - Counting concepts by calendar month and year")
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql1,
        tempEmulationSchema = tempEmulationSchema,
        domain_table = rowData$domainTable,
        domain_concept_id = rowData$domainConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        domain_start_date = rowData$domainStartDate,
        concept_tracking_table = conceptTrackingTable,
        min_cell_count = minCellCount,
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
        concept_tracking_table = conceptTrackingTable,
        min_cell_count = minCellCount,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
    ParallelLogger::logTrace("    - Counting concepts without calendar period")
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql3,
      tempEmulationSchema = tempEmulationSchema,
      domain_table = rowData$domainTable,
      domain_concept_id = rowData$domainConceptId,
      cdm_database_schema = cdmDatabaseSchema,
      domain_start_date = rowData$domainStartDate,
      concept_tracking_table = conceptTrackingTable,
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
      if (runConceptCountByCalendarPeriod) {
        ParallelLogger::logTrace("    - Counting concepts by calendar month and year")
        DatabaseConnector::renderTranslateExecuteSql(
          connection = connection,
          sql = sql4,
          tempEmulationSchema = tempEmulationSchema,
          domain_table = rowData$domainTable,
          domain_concept_id = rowData$domainSourceConceptId,
          cdm_database_schema = cdmDatabaseSchema,
          domain_start_date = rowData$domainStartDate,
          concept_tracking_table = conceptTrackingTable,
          min_cell_count = minCellCount,
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
          concept_tracking_table = conceptTrackingTable,
          min_cell_count = minCellCount,
          progressBar = FALSE,
          reportOverallTime = FALSE
        )
      }
      ParallelLogger::logTrace("    - Counting concepts - no calendar stratification")
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql6,
        tempEmulationSchema = tempEmulationSchema,
        domain_table = rowData$domainTable,
        domain_concept_id = rowData$domainSourceConceptId,
        cdm_database_schema = cdmDatabaseSchema,
        domain_start_date = rowData$domainStartDate,
        concept_tracking_table = conceptTrackingTable,
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
  # i was thinking of keeping counts at the omop table level - but the file size became too big
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
                                                   conceptSetsXWalk = "#concept_sets_x_walk",
                                                   conceptTrackingTable,
                                                   runBreakdownIndexEventRelativeDays = c(0),
                                                   runIndexDateConceptCoOccurrence = FALSE,
                                                   minCellCount) {
  if (is.null(minCellCount)) {
    minCellCount <- 0
  }
  if (minCellCount < 0) {
    minCellCount <- 0
  }
  
  runBreakdownIndexEventRelativeDays <-
    runBreakdownIndexEventRelativeDays %>% sort() %>% unique()
  
  sqlVocabulary <-
    "DROP TABLE IF EXISTS #indx_concepts;

  	  WITH c_ancestor
      AS (
      	SELECT DISTINCT unq.cohort_id,
      		ca.descendant_concept_id concept_id
      	FROM @cdm_database_schema.concept_ancestor ca
      	INNER JOIN @concept_tracking_table cu ON ancestor_concept_id = cu.concept_id
      	INNER JOIN @concept_sets_x_walk unq ON cu.unique_concept_set_id = unq.unique_concept_set_id
      	),
      all_concepts
      AS (
      	SELECT cohort_id,
      		concept_id_2 concept_id
      	FROM @cdm_database_schema.concept_relationship cr
      	INNER JOIN c_ancestor ca ON concept_id_1 = ca.concept_id

      	UNION

      	SELECT cohort_id,
      		concept_id
      	FROM c_ancestor
      	)
      SELECT DISTINCT cohort_id,
      	concept_id
      INTO #indx_concepts
      FROM all_concepts;"
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlVocabulary,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    concept_tracking_table = conceptTrackingTable,
    concept_sets_x_walk = conceptSetsXWalk,
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema
  )
  
  if (!is.null(conceptTrackingTable)) {
    # keeping track
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql =  "INSERT INTO @concept_tracking_table
            SELECT DISTINCT cohort_id, concept_id
            FROM #indx_concepts
            WHERE concept_id != 0;",
      tempEmulationSchema = tempEmulationSchema,
      concept_tracking_table = conceptTrackingTable,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  domains <- getDomainInformation()
  domains <- domains$wide
  nonEraTables <- domains %>%
    #filtering out ERA tables because they are supposed to be derived tables, and counting them is double counting
    dplyr::filter(.data$isEraTable == FALSE) %>%
    dplyr::pull(.data$domainTableShort) %>%
    unique()
  domains <- domains %>%
    dplyr::filter(.data$domainTableShort %in% c(nonEraTables))
  
  sqlDdlDrop <-
    "DROP TABLE IF EXISTS #indx_breakdown;"
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
                        	AND DATEADD(dd, @days_relative_index, c.cohort_start_date) = d1.@domain_start_date
                        INNER JOIN #indx_concepts cu ON d1.@domain_concept_id = cu.concept_id
                          AND cu.cohort_id = c.cohort_definition_id
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
                                        FROM @cohort_database_schema.@cohort_table c
                                        INNER JOIN @cdm_database_schema.@domain_table d1 ON c.subject_id = d1.person_id
                                        	AND DATEADD(dd, @days_relative_index, c.cohort_start_date) = d1.@domain_start_date
                                        INNER JOIN @cdm_database_schema.@domain_table d2 ON c.subject_id = d2.person_id
                                        	AND DATEADD(dd, @days_relative_index, c.cohort_start_date) = d2.@domain_start_date
                                          -- AND d1.@domain_start_date = d2.@domain_start_date
                                          -- AND d1.person_id = d2.person_id
                                        INNER JOIN #indx_concepts cu1
                                        ON d1.@domain_concept_id = cu1.concept_id
                                          AND cu1.cohort_id = c.cohort_definition_id
                                        INNER JOIN #indx_concepts cu2
                                        ON d2.@domain_concept_id = cu2.concept_id
                                          AND cu2.cohort_id = c.cohort_definition_id
                                        WHERE d1.@domain_concept_id != d2.@domain_concept_id
                                          AND c.cohort_definition_id IN (@cohortIds)
                                        GROUP BY cohort_definition_id,
                                        	d1.@domain_concept_id,
                                        	d2.@domain_concept_id
                                        HAVING count(DISTINCT c.subject_id) > @min_subject_count
                                    ;"
  
  #conceptId is from _concept_id field of domain table and coConceptId is also from _source_concept_id field of same domain table
  # i.e. same day co-occurrence of concept ids where second (coConceptId) maybe non-standard relative to index date
  # the inner join to conceptIdUnivese to _concep_id limits to standard concepts in conceptTrackingTable - because only standard concept should be in _concept_id
  sqlConceptIdCoConceptIdOppositeCount <- " INSERT INTO #indx_breakdown
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
                                            FROM @cohort_database_schema.@cohort_table c
                                            INNER JOIN @cdm_database_schema.@domain_table d1 ON c.subject_id = d1.person_id
                                            	AND DATEADD(dd, @days_relative_index, c.cohort_start_date) = d1.@domain_start_date
                                            INNER JOIN @cdm_database_schema.@domain_table d2 ON c.subject_id = d2.person_id
                                            	AND DATEADD(dd, @days_relative_index, c.cohort_start_date) = d2.@domain_start_date
                                            	-- AND d1.@domain_start_date = d2.@domain_start_date
                                            	-- AND d1.person_id = d2.person_id
                                            INNER JOIN #indx_concepts cu1
                                            ON d1.@domain_concept_id = cu1.concept_id
                                              AND cu1.cohort_id = c.cohort_definition_id
                                            INNER JOIN #indx_concepts cu2
                                            ON d1.@domain_source_concept_id = cu2.concept_id
                                              AND cu2.cohort_id = c.cohort_definition_id
                                            WHERE d1.@domain_concept_id != d2.@domain_source_concept_id
                                              AND c.cohort_definition_id IN (@cohortIds)
                                            GROUP BY cohort_definition_id,
                                            	d1.@domain_concept_id,
                                            	d2.@domain_source_concept_id
                                            HAVING count(DISTINCT c.subject_id) > @min_subject_count;"
  
  if (runIndexDateConceptCoOccurrence) {
    ParallelLogger::logInfo(
      "      - concept Co-occurrence matrix computation has been requested. This may take time"
    )
  }
  
  for (j in (1:length(runBreakdownIndexEventRelativeDays))) {
    if (length(runBreakdownIndexEventRelativeDays) > 1) {
      ParallelLogger::logInfo(
        paste0(
          "   - Working on ",
          scales::comma(x = runBreakdownIndexEventRelativeDays[[j]]),
          " days relative to index date. ",
          scales::comma(j),
          " of ",
          scales::comma(length(runBreakdownIndexEventRelativeDays)),
          "."
        )
      )
    }
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
        days_relative_index = runBreakdownIndexEventRelativeDays[[j]],
        min_subject_count = minCellCount,
        reportOverallTime = FALSE,
        progressBar = FALSE
      )
      
      if (runIndexDateConceptCoOccurrence) {
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
          days_relative_index = runBreakdownIndexEventRelativeDays[[j]],
          min_subject_count = minCellCount,
          reportOverallTime = FALSE,
          progressBar = FALSE
        )
      }
      
      if (all(!is.na(rowData$domainSourceConceptId),
              nchar(rowData$domainSourceConceptId) > 4)) {
        ParallelLogger::logTrace(
          paste0(
            "   - Performing concept count - ",
            rowData$domainSourceConceptId,
            " field of ",
            rowData$domainTable,
            " table"
          )
        )
        # counting SqlConceptIdCount for domainSourceConceptId
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
          days_relative_index = runBreakdownIndexEventRelativeDays[[j]],
          min_subject_count = minCellCount,
          reportOverallTime = FALSE,
          progressBar = FALSE
        )
        
        if (runIndexDateConceptCoOccurrence) {
          ParallelLogger::logTrace(
            paste0(
              "      - Performing co-concept count - ",
              rowData$domainSourceConceptId,
              " field of ",
              rowData$domainTable,
              " table"
            )
          )
          # counting sqlConceptIdCoConceptIdSameCount for domainSourceConceptId
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
            days_relative_index = runBreakdownIndexEventRelativeDays[[j]],
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
          # counting sqlConceptIdCoConceptIdOppositeCount
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
            days_relative_index = runBreakdownIndexEventRelativeDays[[j]],
            min_subject_count = minCellCount,
            reportOverallTime = FALSE,
            progressBar = FALSE
          )
        }
      }
    }
  }
  
  #avoid any potential duplication
  #removes domain table - counts are retained from the domain table that has the most prevalent concept id -
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

# function: getConceptStandardSourceMappingCount ----
getConceptStandardSourceMappingCount <- function(connection,
                                                 cdmDatabaseSchema,
                                                 tempEmulationSchema,
                                                 conceptTrackingTable,
                                                 keep2BillionConceptId = FALSE) {
  domains <- getDomainInformation()
  domains <- domains$wide %>%
    dplyr::filter(nchar(.data$domainSourceConceptId) > 1)
  
  sqlconceptStdSrcCnt <-
    "DROP TABLE IF EXISTS #concept_std_src_cnt;
      CREATE TABLE #concept_std_src_cnt ( concept_id INT,
                                          source_concept_id INT,
                                          domain_table VARCHAR(20),
                                          concept_count BIGINT,
                                          subject_count BIGINT);"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlconceptStdSrcCnt,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  sqlMapping <- "WITH concept_tracking_table
          AS (
          	SELECT DISTINCT concept_id
          	FROM @concept_tracking_table
          	)
          INSERT INTO #concept_std_src_cnt
          SELECT @domain_concept_id concept_id,
          	@domain_source_concept_id source_concept_id,
          	'@domainTableShort' domain_table,
          	COUNT(*) AS concept_count,
          	COUNT(DISTINCT person_id) AS subject_count
          FROM @cdm_database_schema.@domain_table
          INNER JOIN concept_tracking_table a ON @domain_concept_id = a.concept_id
          WHERE (
          		@domain_source_concept_id IS NOT NULL
          		AND @domain_source_concept_id > 0
          		)
              {@keep_custom_concept_id} ? {} : {AND @domain_source_concept_id < 200000000
                                                AND @domain_concept_id < 200000000}
          GROUP BY @domain_concept_id,
          	@domain_source_concept_id
          ORDER BY @domain_concept_id,
          	@domain_source_concept_id;"
  
  conceptStdSrcCnt <- list()
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
      concept_tracking_table = conceptTrackingTable,
      domainTableShort = rowData$domainTableShort,
      keep_custom_concept_id = keep2BillionConceptId,
      reportOverallTime = FALSE,
      progressBar = FALSE
    )
  }
  sql <- "SELECT * FROM #concept_std_src_cnt;"
  conceptStdSrcCnt <-
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
  conceptStdSrcCnt <- dplyr::bind_rows(
    conceptStdSrcCnt,
    conceptStdSrcCnt %>%
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
  
  sql <-
    " INSERT INTO @concept_tracking_table (unique_concept_set_id, concept_id)
      SELECT DISTINCT ct.unique_concept_set_id, cm.source_concept_id concept_id
      FROM #concept_std_src_cnt cm
      INNER JOIN @concept_tracking_table ct
      ON cm.concept_id = ct.concept_id;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    concept_tracking_table = conceptTrackingTable,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  sqlDdlDrop <-
    "DROP TABLE IF EXISTS #concept_std_src_cnt;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlDdlDrop,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  return(conceptStdSrcCnt)
}


# function:getExcludedConceptSets ----
getExcludedConceptSets <- function(connection,
                                   uniqueConceptSets,
                                   vocabularyDatabaseSchema,
                                   tempEmulationSchema,
                                   conceptSetsXWalk = "#concept_sets_x_walk",
                                   conceptTrackingTable = NULL,
                                   keep2BillionConceptId = FALSE) {
  conceptSetWithEx <- list()
  for (i in (1:nrow(uniqueConceptSets))) {
    conceptSetExpression <-
      uniqueConceptSets$conceptSetExpression[[i]] %>%
      RJSONIO::fromJSON(digits = 23)
    conceptSetWithEx[[i]] <-
      getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression)
    if ('isExcluded' %in% colnames(conceptSetWithEx[[i]])) {
      conceptSetWithEx[[i]] <- conceptSetWithEx[[i]] %>%
        dplyr::filter(.data$isExcluded == TRUE) %>%
        dplyr::mutate(uniqueConceptSetId = uniqueConceptSets$uniqueConceptSetId[[i]]) %>%
        dplyr::select(.data$uniqueConceptSetId,
                      .data$conceptId,
                      .data$includeDescendants) %>%
        dplyr::distinct()
    } else {
      conceptSetWithEx[[i]] <- conceptSetWithEx[[i]][0,]
    }
  }
  conceptSetWithEx <-
    dplyr::bind_rows(conceptSetWithEx) %>% 
    dplyr::mutate(includeDescendants = as.numeric(.data$includeDescendants))
  
  if (nrow(conceptSetWithEx) == 0) {
    return(NULL)
  }
  
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "#concept_set_with_ex",
    data = conceptSetWithEx,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE,
    progressBar = FALSE,
    camelCaseToSnakeCase = TRUE
  )
  if (is.null(conceptSetsXWalk)) {
    sql <-
      "
      DROP TABLE IF EXISTS #excluded_concepts;

      SELECT DISTINCT unique_concept_set_id codeset_id,
      	concept_id
      INTO #excluded_concepts
      FROM (
      	SELECT DISTINCT ex.unique_concept_set_id,
      		ex.concept_id
      	FROM #concept_set_with_ex ex

      	UNION

      	SELECT DISTINCT ex.unique_concept_set_id,
      		anc.descendant_concept_id concept_id
      	FROM @vocabulary_database_schema.concept_ancestor anc
      	INNER JOIN #concept_set_with_ex ex ON anc.ancestor_concept_id = ex.concept_id
      	WHERE ex.include_descendants = 1
      	) cs
        {@keep_custom_concept_id} ? {} : {WHERE concept_id < 200000000 };"
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      vocabulary_database_schema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      keep_custom_concept_id = keep2BillionConceptId,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  } else {
    sql <-
      "
      DROP TABLE IF EXISTS #excluded_concepts;

      SELECT DISTINCT unq.cohort_id,
      	unq.concept_set_id,
      	cs.concept_id
      INTO #excluded_concepts
      FROM (
      	SELECT DISTINCT ex.unique_concept_set_id,
      		ex.concept_id
      	FROM #concept_set_with_ex ex

      	UNION

      	SELECT DISTINCT ex.unique_concept_set_id,
      		anc.descendant_concept_id concept_id
      	FROM @vocabulary_database_schema.concept_ancestor anc
      	INNER JOIN #concept_set_with_ex ex ON anc.ancestor_concept_id = ex.concept_id
      	WHERE ex.include_descendants = 1
      	) cs
      INNER JOIN @concept_sets_x_walk unq ON cs.unique_concept_set_id = unq.unique_concept_set_id
        {@keep_custom_concept_id} ? {} : {WHERE cs.concept_id < 200000000 };"
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      vocabulary_database_schema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      keep_custom_concept_id = keep2BillionConceptId,
      concept_sets_x_walk = conceptSetsXWalk,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  if (!is.null(conceptTrackingTable)) {
    sql <- "INSERT INTO @concept_tracking_table
            SELECT DISTINCT xw.unique_concept_set_id, concept_id
            FROM #excluded_concepts ex
            INNER JOIN @concept_sets_x_walk xw
            ON ex.cohort_id = xw.cohort_id AND
            ex.concept_set_id = xw.concept_set_id;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      concept_tracking_table = conceptTrackingTable,
      concept_sets_x_walk = conceptSetsXWalk,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  excludedConcepts <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM #excluded_concepts;",
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema
    )
  
  sql <-
    "DROP TABLE IF EXISTS #excluded_concepts;
      DROP TABLE IF EXISTS #concept_set_with_ex;"
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
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
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
        sourceFile = system.file(
          "sql",
          "sql_server",
          "OptimizeConceptSet.sql",
          package = utils::packageName()
        )
      )
    sql <- SqlRender::loadRenderTranslateSql(
      "OptimizeConceptSet.sql",
      packageName = utils::packageName(),
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      vocabulary_database_schema = vocabularyDatabaseSchema,
      conceptSetConceptIdsExcluded = conceptSetConceptIdsExcluded,
      conceptSetConceptIdsDescendantsExcluded = conceptSetConceptIdsDescendantsExcluded,
      conceptSetConceptIdsNotExcluded = conceptSetConceptIdsNotExcluded,
      conceptSetConceptIdsDescendantsNotExcluded = conceptSetConceptIdsDescendantsNotExcluded
    )
    
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      reportOverallTime = FALSE,
      progressBar = FALSE
    )
    
    data <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT * FROM #optimized_set;",
        snakeCaseToCamelCase = TRUE
      )
    
    sqlCleanUp <- "DROP TABLE IF EXISTS #optimized_set;"
    
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
                                         cohortDatabaseSchema,
                                         cohortTable,
                                         runConceptSetDiagnostics = TRUE,
                                         runBreakdownIndexEventRelativeDays = c(0),
                                         runIndexDateConceptCoOccurrence = FALSE,
                                         runConceptCountByCalendarPeriod = FALSE,
                                         exportFolder,
                                         minCellCount,
                                         keep2BillionConceptId,
                                         incremental,
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
    
    outputAndromeda <- runConceptSetDiagnostics(
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      cohorts = cohorts,
      cohortIds = subset$cohortId,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      runBreakdownIndexEventRelativeDays = runBreakdownIndexEventRelativeDays,
      runIndexDateConceptCoOccurrence = runIndexDateConceptCoOccurrence,
      runConceptCountByCalendarPeriod = runConceptCountByCalendarPeriod,
      minCellCount = minCellCount,
      keep2BillionConceptId = keep2BillionConceptId
    )
    outputConceptTable <-
      function(andromedObject,
               tableNameCamelCase,
               minCellCount,
               databaseId,
               exportFolder,
               incremental) {
        data <- makeDataExportable(
          x = andromedObject[[tableNameCamelCase]] %>% dplyr::collect(),
          tableName = SqlRender::camelCaseToSnakeCase(tableNameCamelCase),
          minCellCount = minCellCount,
          databaseId = databaseId
        )
        ParallelLogger::logInfo("  - writing ",
                                paste0(SqlRender::camelCaseToSnakeCase(tableNameCamelCase),
                                       ".csv"))
        writeToCsv(
          data = data,
          fileName = file.path(exportFolder, paste0(
            SqlRender::camelCaseToSnakeCase(tableNameCamelCase),
            ".csv"
          )),
          incremental = incremental
        )
      }
    
    vectorOfTablesToOutput <- names(outputAndromeda)
    for (i in (1:length(vectorOfTablesToOutput))) {
      outputConceptTable(
        andromedObject = outputAndromeda,
        tableNameCamelCase = vectorOfTablesToOutput[[i]],
        minCellCount = minCellCount,
        databaseId = databaseId,
        exportFolder = exportFolder,
        incremental = incremental
      )
    }
    
    Andromeda::close(outputAndromeda)
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
                                                         conceptSetExpression[i,]$expression$items) %>%
        dplyr::mutate(id = conceptSetExpression[i, ]$id) %>%
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
        SqlRender::snakeCaseToCamelCase(colnames(conceptSetExpressionDetails))
    }
    return(conceptSetExpressionDetails)
  }
