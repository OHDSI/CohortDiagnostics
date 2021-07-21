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
  ParallelLogger::logInfo("Starting concept set diagnostics")
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
  
  # Cohorts to run the concept set diagnostics
  if (!is.null(cohortIds)) {
    subset <- cohorts %>%
      dplyr::filter(.data$cohortId %in% cohortIds)
  }
  
  # Get concept sets metadata----
  conceptSets <- combineConceptSetsFromCohorts(subset)
  if (is.null(conceptSets)) {
    ParallelLogger::logInfo(
      "Cohorts being diagnosed does not have concept ids. Skipping concept set diagnostics."
    )
    return(NULL)
  }
  
  # Instantiate (resolve) unique concept sets----
  uniqueConceptSets <-
    conceptSets[!duplicated(conceptSets$uniqueConceptSetId), ] %>%
    dplyr::select(-.data$cohortId, -.data$conceptSetId)
  instantiateUniqueConceptSets(
    uniqueConceptSets = uniqueConceptSets,
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    conceptSetsTable = "#inst_concept_sets"
  )
  
  resolvedConceptIds <-
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
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql =  "INSERT INTO #concept_ids
            SELECT DISTINCT concept_id
            FROM #inst_concept_sets;",
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  if (runIncludedSourceConcepts || runOrphanConcepts) {
    # Concept counts computation----
    createConceptCountsTable(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      conceptCountsTable = "#concept_counts"
    )
  }
  
  includedSourceCodes <- NULL
  if (runIncludedSourceConcepts) {
    # Included concepts----
    ParallelLogger::logInfo("Fetching included source concepts")
    startIncludedSourceConcepts <- Sys.time()
    
    ParallelLogger::logTrace("Included codes SQL")
    sql <- SqlRender::loadRenderTranslateSql(
      "CohortSourceCodes.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      cdm_database_schema = cdmDatabaseSchema,
      instantiated_concept_sets = "#inst_concept_sets",
      include_source_concept_table = "#inc_src_concepts",
      by_month = FALSE
    )
    DatabaseConnector::executeSql(connection = connection, sql = sql)
    includedSourceCodes <-
      renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT * FROM @include_source_concept_table;",
        include_source_concept_table = "#inc_src_concepts",
        tempEmulationSchema = tempEmulationSchema,
        snakeCaseToCamelCase = TRUE
      )
    ParallelLogger::logTrace("Included codes SQL complete")
    includedSourceCodes <- includedSourceCodes  %>%
      dplyr::rename(uniqueConceptSetId = .data$conceptSetId) %>%
      dplyr::inner_join(
        conceptSets %>% dplyr::select(
          .data$uniqueConceptSetId,
          .data$cohortId,
          .data$conceptSetId
        ),
        by = "uniqueConceptSetId"
      ) %>%
      dplyr::select(-.data$uniqueConceptSetId) %>%
      dplyr::relocate(.data$cohortId,
                      .data$conceptSetId,
                      .data$conceptId) %>%
      dplyr::distinct()
    
    sqlInsertIncludedConcepts <-
      "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @include_source_concept_table;

                  INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT source_concept_id
                  FROM @include_source_concept_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sqlInsertIncludedConcepts,
      tempEmulationSchema = tempEmulationSchema,
      concept_id_table = '#concept_ids',
      include_source_concept_table = "#inc_src_concepts",
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    
    sql <-
      "TRUNCATE TABLE @include_source_concept_table;\nDROP TABLE @include_source_concept_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      include_source_concept_table = "#inc_src_concepts",
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    
    delta <- Sys.time() - startIncludedSourceConcepts
    ParallelLogger::logInfo(paste(
      "----Finding source codes took",
      signif(delta, 3),
      attr(delta, "units")
    ))
  }
  
  if (runBreakdownIndexEvents) {
    # Index event breakdown ----
    ParallelLogger::logInfo("Breaking down index events")
    startBreakdownEvents <- Sys.time()
    domains <-
      readr::read_csv(
        system.file("csv", "domains.csv", package = "CohortDiagnostics"),
        col_types = readr::cols(),
        guess_max = min(1e7)
      )
    
    runBreakdownIndexEvents <- function(cohort,
                                        connection,
                                        tempEmulationSchema) {
      ParallelLogger::logInfo("- Breaking down index events for cohort '",
                              cohort$cohortName,
                              "'")
      cohortDefinition <-
        RJSONIO::fromJSON(cohort$json, digits = 23)
      primaryCodesetIds <-
        lapply(cohortDefinition$PrimaryCriteria$CriteriaList,
               getCodeSetIds) %>%
        dplyr::bind_rows()
      if (nrow(primaryCodesetIds) == 0) {
        warning("No primary event criteria concept sets found for cohort id: ",
                cohort$cohortId)
        return(tidyr::tibble())
      }
      primaryCodesetIds <-
        primaryCodesetIds %>% dplyr::filter(.data$domain %in%
                                              c(domains$domain %>% unique()))
      if (nrow(primaryCodesetIds) == 0) {
        warning(
          "Primary event criteria concept sets found for cohort id: ",
          cohort$cohortId,
          " but,",
          "\nnone of the concept sets belong to the supported domains.",
          "\nThe supported domains are:\n",
          paste(domains$domain,
                collapse = ", ")
        )
        return(dplyr::tibble())
      }
      primaryCodesetIds <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% cohort$cohortId) %>%
        dplyr::select(codeSetIds = .data$conceptSetId, .data$uniqueConceptSetId) %>%
        dplyr::inner_join(primaryCodesetIds, by = "codeSetIds")
      
      pasteIds <- function(row) {
        return(dplyr::tibble(
          domain = row$domain[1],
          uniqueConceptSetId = paste(row$uniqueConceptSetId, collapse = ", ")
        ))
      }
      primaryCodesetIds <-
        lapply(split(primaryCodesetIds, primaryCodesetIds$domain),
               pasteIds)
      primaryCodesetIds <- dplyr::bind_rows(primaryCodesetIds)
      
      getCounts <- function(row,
                            connection,
                            tempEmulationSchema) {
        domain <- domains[domains$domain == row$domain,]
        sql <-
          SqlRender::loadRenderTranslateSql(
            "CohortEntryBreakdown.sql",
            packageName = "CohortDiagnostics",
            dbms = connection@dbms,
            tempEmulationSchema = tempEmulationSchema,
            cdm_database_schema = cdmDatabaseSchema,
            vocabulary_database_schema = vocabularyDatabaseSchema,
            cohort_database_schema = cohortDatabaseSchema,
            cohort_table = cohortTable,
            cohort_id = cohort$cohortId,
            domain_table = domain$domainTable,
            domain_start_date = domain$domainStartDate,
            domain_concept_id = domain$domainConceptId,
            domain_source_concept_id = domain$domainSourceConceptId,
            use_source_concept_id = !is.null(domain$domainSourceConceptId),
            primary_codeset_ids = row$uniqueConceptSetId,
            concept_set_table = "#inst_concept_sets",
            store = TRUE,
            store_table = "#breakdown"
          )
        
        DatabaseConnector::executeSql(
          connection = connection,
          sql = sql,
          progressBar = FALSE,
          reportOverallTime = FALSE
        )
        sql <- "SELECT * FROM @store_table;"
        counts <-
          renderTranslateQuerySql(
            connection = connection,
            sql = sql,
            tempEmulationSchema = tempEmulationSchema,
            store_table = "#breakdown",
            snakeCaseToCamelCase = TRUE
          )
        sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @store_table;"
        renderTranslateExecuteSql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          concept_id_table = "#concept_ids",
          store_table = "#breakdown",
          progressBar = FALSE,
          reportOverallTime = FALSE
        )
        sql <-
          "TRUNCATE TABLE @store_table;\nDROP TABLE @store_table;"
        renderTranslateExecuteSql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          store_table = "#breakdown",
          progressBar = FALSE,
          reportOverallTime = FALSE
        )
        return(counts)
      }
      ParallelLogger::logTrace("Index event breakdown SQL")
      counts <- list()
      for (i in (1:nrow(primaryCodesetIds))) {
        counts[[i]] <- getCounts(
          row = primaryCodesetIds[i,],
          connection = connection,
          tempEmulationSchema = tempEmulationSchema
        )
      }
      counts <- dplyr::bind_rows(counts) %>%
        dplyr::arrange(.data$conceptCount)
      ParallelLogger::logTrace("End Index event breakdown SQL")
      
      if (nrow(counts) > 0) {
        counts$cohortId <- cohort$cohortId
      } else {
        ParallelLogger::logInfo("-- Index event breakdown results were not returned for: ",
                                cohort$cohortId)
        return(dplyr::tibble())
      }
      return(counts)
    }
    
    data <- list()
    for (i in (1:nrow(subset))) {
      data[[i]] <- runBreakdownIndexEvents(
        cohort = subset[i,],
        connection = connection,
        tempEmulationSchema = tempEmulationSchema
      )
    }
    indexEventBreakdown <- dplyr::bind_rows(data)
    
    ### indexDateConceptCooccurrence -----
    getIndexDateConceptCooccurrence <- function(connection,
                                                cdmDatabaseSchema,
                                                tempEmulationSchema,
                                                cohortIds) {
      sqlDdlDrop <-
        "IF OBJECT_ID('tempdb..#concept_cooccurrence', 'U') IS NOT NULL
                	      DROP TABLE #concept_cooccurrence;"
      
      renderTranslateExecuteSql(
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
      renderTranslateExecuteSql(
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
                INNER JOIN @concept_set_table
                	ON @domain_concept_id = concept_id
                WHERE cohort_definition_id IN (@cohortIds);"
      
      for (i in (1:nrow(domains))) {
        rowData <- domains[i,]
        renderTranslateExecuteSql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          domain_concept_id = rowData$domainConceptId,
          cdm_database_schema = cdmDatabaseSchema,
          cohort_database_schema = cohortDatabaseSchema,
          domain_table = rowData$domainTable,
          domain_start_date = rowData$domainStartDate,
          concept_set_table = "#inst_concept_sets",
          cohortIds = cohortIds,
          cohort_table = cohortTable,
          progressBar = FALSE,
          reportOverallTime = FALSE
        )
        if (all(
          !is.na(rowData$domainSourceConceptId),
          length(rowData$domainSourceConceptId) > 0
        )) {
          renderTranslateExecuteSql(
            connection = connection,
            sql = sql,
            tempEmulationSchema = tempEmulationSchema,
            domain_concept_id = rowData$domainSourceConceptId,
            cdm_database_schema = cdmDatabaseSchema,
            cohort_database_schema = cohortDatabaseSchema,
            domain_table = rowData$domainTable,
            domain_start_date = rowData$domainStartDate,
            concept_set_table = "#inst_concept_sets",
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
	  renderTranslateExecuteSql(
        connection = connection,
        sql = sqlDdlDrop,
        tempEmulationSchema = tempEmulationSchema,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )	  
      return(indexDateConceptCooccurrence)
    }
    indexDateConceptCooccurrence <-
      getIndexDateConceptCooccurrence(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        cohortIds = subset$cohortId
      )
    
    delta <- Sys.time() - startBreakdownEvents
    ParallelLogger::logInfo(paste(
      "Breaking down index event took",
      signif(delta, 3),
      attr(delta, "units")
    ))
  }
  
  if (runOrphanConcepts) {
    # Orphan concepts ----
    ParallelLogger::logInfo("Finding orphan concepts")
    startOrphanCodes <- Sys.time()
    data <- list()
    ParallelLogger::logTrace("Orphan concept SQL")
    for (i in (1:nrow(uniqueConceptSets))) {
      conceptSet <- uniqueConceptSets[i, ]
      ParallelLogger::logInfo("- Finding orphan concepts for concept set '",
                              conceptSet$conceptSetName,
                              "'")
      data[[i]] <- .findOrphanConcepts(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        useCodesetTable = TRUE,
        codesetId = conceptSet$uniqueConceptSetId,
        conceptCountsTable = "#concept_counts",
        instantiatedCodeSets = "#inst_concept_sets",
        orphanConceptTable = "#orphan_concepts"
      )
      
      sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @orphan_concept_table;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        tempEmulationSchema = tempEmulationSchema,
        concept_id_table = "#concept_ids",
        orphan_concept_table = "#orphan_concepts",
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
      sql <-
        "TRUNCATE TABLE @orphan_concept_table;\nDROP TABLE @orphan_concept_table;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        tempEmulationSchema = tempEmulationSchema,
        orphan_concept_table = "#orphan_concepts",
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
    ParallelLogger::logTrace("End Orphan concept SQL")
    orphanCodes <- dplyr::bind_rows(data) %>%
      dplyr::distinct() %>%
      dplyr::rename(uniqueConceptSetId = .data$codesetId) %>%
      dplyr::inner_join(
        conceptSets %>%
          dplyr::select(
            .data$uniqueConceptSetId,
            .data$cohortId,
            .data$conceptSetId
          ),
        by = "uniqueConceptSetId"
      ) %>%
      dplyr::select(-.data$uniqueConceptSetId) %>%
      dplyr::relocate(.data$cohortId, .data$conceptSetId)
    
    delta <- Sys.time() - startOrphanCodes
    ParallelLogger::logInfo("Finding orphan concepts took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
  
  ParallelLogger::logInfo("Retrieving concept information")
  exportedVocablary <- exportConceptInformation(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    conceptIdTable = "#concept_ids"
  )
  
  # Drop temporay tables
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
  if (!is.null(resolvedConceptIds)) {
    exportedVocablary$resolvedConceptIds = resolvedConceptIds
  }
  if (!is.null(includedSourceCodes)) {
    exportedVocablary$includedSourceCodes = includedSourceCodes
  }
  if (!is.null(indexEventBreakdown)) {
    exportedVocablary$indexEventBreakdown = indexEventBreakdown
  }
  if (!is.null(orphanCodes)) {
    exportedVocablary$orphanCodes = orphanCodes
  }
  if (!is.null(conceptSets)) {
    exportedVocablary$conceptSets = conceptSets
  }
  if (!is.null(indexDateConceptCooccurrence)) {
    exportedVocablary$indexDateConceptCooccurrence = indexDateConceptCooccurrence
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

instantiateUniqueConceptSets <- function(uniqueConceptSets,
                                         connection,
                                         cdmDatabaseSchema,
                                         vocabularyDatabaseSchema = cdmDatabaseSchema,
                                         tempEmulationSchema,
                                         conceptSetsTable = '#inst_concept_sets') {
  ParallelLogger::logInfo("Instantiating concept sets")
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


createConceptCountsTable <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     tempEmulationSchema = NULL,
                                     conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                     conceptCountsTable = "concept_counts") {
  ParallelLogger::logInfo("Creating internal concept counts table")
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <-
    SqlRender::loadRenderTranslateSql(
      "CreateConceptCountTable.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      cdm_database_schema = cdmDatabaseSchema,
      work_database_schema = conceptCountsDatabaseSchema,
      concept_counts_table = conceptCountsTable,
      table_is_temp = TRUE
    )
  DatabaseConnector::executeSql(connection, sql)
}


.findOrphanConcepts <- function(connectionDetails = NULL,
                                connection = NULL,
                                cdmDatabaseSchema,
                                vocabularyDatabaseSchema = cdmDatabaseSchema,
                                tempEmulationSchema = NULL,
                                conceptIds = c(),
                                useCodesetTable = FALSE,
                                codesetId = 1,
                                conceptCountsDatabaseSchema = NULL,
                                conceptCountsTable = "concept_counts",
                                conceptCountsTableIsTemp = TRUE,
                                instantiatedCodeSets = "#InstConceptSets",
                                orphanConceptTable = '#recommended_concepts') {
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
    work_database_schema = conceptCountsDatabaseSchema,
    concept_counts_table = conceptCountsTable,
    concept_counts_table_is_temp = conceptCountsTableIsTemp,
    concept_ids = conceptIds,
    use_codesets_table = useCodesetTable,
    orphan_concept_table = orphanConceptTable,
    instantiated_code_sets = instantiatedCodeSets,
    codeset_id = codesetId
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
    SqlRender::loadRenderTranslateSql(
      "DropOrphanConceptTempTables.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema
    )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  return(orphanConcepts)
}
