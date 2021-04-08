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
    temp <- tidyr::tibble()
  }
  return(dplyr::bind_rows(temp))
}


extractConceptSetsJsonFromCohortJson <- function(cohortJson) {
  cohortDefinition <- RJSONIO::fromJSON(content = cohortJson, digits = 23)
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
    conceptSetExpression <- tidyr::tibble()
  }
  return(dplyr::bind_rows(conceptSetExpression))
}

combineConceptSetsFromCohorts <- function(cohorts) {
  #cohorts should be a dataframe with at least cohortId, sql and json
  
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertDataFrame(x = cohorts,
                             min.cols = 4,
                             add = errorMessage)
  checkmate::assertNames(x = colnames(cohorts),
                         must.include = c('cohortId', 'sql', 'json', 'cohortName'))
  checkmate::reportAssertions(errorMessage)
  checkmate::assertDataFrame(x = cohorts %>% dplyr::select(.data$cohortId, 
                                                           .data$sql,
                                                           .data$json, 
                                                           .data$cohortName),
                             any.missing = FALSE,
                             min.cols = 4,
                             add = errorMessage)
  checkmate::reportAssertions(errorMessage)
  
  conceptSets <- list()
  conceptSetCounter <- 0
  
  for (i in (1:nrow(cohorts))) {
    cohort <- cohorts[i,]
    sql <- extractConceptSetsSqlFromCohortSql(cohortSql = cohort$sql)
    json <- extractConceptSetsJsonFromCohortJson(cohortJson = cohort$json)
    
    if (nrow(sql) == 0 || nrow(json) == 0) {
      ParallelLogger::logInfo("Cohort Definition expression does not have a concept set expression. ",
                              "Skipping Cohort: ", 
                              cohort$cohortName )
    } else {
      if (!length(sql$conceptSetId %>% unique()) == length(json$conceptSetId %>% unique())) {
        stop("Mismatch in concept set IDs between SQL and JSON for cohort ",
             cohort$cohortFullName)
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
    dplyr::relocate(.data$uniqueConceptSetId, .data$cohortId, .data$conceptSetId) %>% 
    dplyr::arrange(.data$uniqueConceptSetId, .data$cohortId, .data$conceptSetId)
  return(conceptSets)
}


mergeTempTables <- function(connection, tableName, tempTables, oracleTempSchema) {
  valueString <- paste(tempTables, collapse = "\n\n  UNION ALL\n\n  SELECT *\n  FROM ")
  sql <- sprintf("SELECT *\nINTO %s\nFROM (\n  SELECT *\n  FROM %s\n) tmp;", tableName, valueString)
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms, oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  # Drop temp tables:
  for (tempTable in tempTables) {
    sql <- sprintf("TRUNCATE TABLE %s;\nDROP TABLE %s;", tempTable, tempTable)
    sql <- SqlRender::translate(sql, targetDialect = connection@dbms, oracleTempSchema = oracleTempSchema)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
}

instantiateUniqueConceptSets <- function(uniqueConceptSets,
                                         connection,
                                         cdmDatabaseSchema,
                                         oracleTempSchema,
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
    utils::setTxtProgressBar(pb, start/length(sql))
    tempTable <- paste("#", paste(sample(letters, 20, replace = TRUE), collapse = ""), sep = "")
    tempTables <- c(tempTables, tempTable)
    end <- min(start + batchSize - 1, length(sql))
    sqlSubset <- sql[start:end]
    sqlSubset <- paste(sqlSubset, collapse = "\n\n  UNION ALL\n\n")
    sqlSubset <- sprintf("SELECT *\nINTO %s\nFROM (\n %s\n) tmp;", tempTable, sqlSubset)
    sqlSubset <- SqlRender::render(sqlSubset, vocabulary_database_schema = cdmDatabaseSchema)
    sqlSubset <- SqlRender::translate(sqlSubset,
                                      targetDialect = connection@dbms,
                                      oracleTempSchema = oracleTempSchema)
    DatabaseConnector::executeSql(connection, sqlSubset, progressBar = FALSE, reportOverallTime = FALSE)
  }
  utils::setTxtProgressBar(pb, 1)
  close(pb)
  
  mergeTempTables(connection = connection,
                  tableName = conceptSetsTable,
                  tempTables = tempTables, 
                  oracleTempSchema = oracleTempSchema)
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


runConceptSetDiagnostics <- function(connection,
                                     oracleTempSchema,
                                     cdmDatabaseSchema,
                                     databaseId,
                                     cohorts,
                                     runIncludedSourceConcepts,
                                     runOrphanConcepts,
                                     runBreakdownIndexEvents,
                                     exportFolder,
                                     minCellCount,
                                     conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                     conceptCountsTable = "concept_counts",
                                     conceptCountsTableIsTemp = FALSE,
                                     cohortDatabaseSchema,
                                     cohortTable,
                                     useExternalConceptCountsTable = FALSE,
                                     incremental = FALSE,
                                     conceptIdTable = NULL,
                                     recordKeepingFile) {
  ParallelLogger::logInfo("Starting concept set diagnostics")
  startConceptSetDiagnostics <- Sys.time()
  
  subset <- dplyr::tibble()
  if (runIncludedSourceConcepts) {
    subsetIncluded <- subsetToRequiredCohorts(cohorts = cohorts,
                                              task = "runIncludedSourceConcepts",
                                              incremental = incremental,
                                              recordKeepingFile = recordKeepingFile)
    subset <- dplyr::bind_rows(subset, subsetIncluded)
  }
  if (runBreakdownIndexEvents) {
    subsetBreakdown <- subsetToRequiredCohorts(cohorts = cohorts,
                                               task = "runBreakdownIndexEvents",
                                               incremental = incremental,
                                               recordKeepingFile = recordKeepingFile)
    subset <- dplyr::bind_rows(subset, subsetBreakdown)
  }
  
  if (runOrphanConcepts) {
    subsetOrphans <- subsetToRequiredCohorts(cohorts = cohorts,
                                             task = "runOrphanConcepts",
                                             incremental = incremental,
                                             recordKeepingFile = recordKeepingFile)
    subset <- dplyr::bind_rows(subset, subsetOrphans)
  }
  subset <- dplyr::distinct(subset)
  
  if (nrow(subset) == 0) {
    return()
  }
  
  conceptSets <- combineConceptSetsFromCohorts(subset)
  
  if (is.null(conceptSets)) {
    ParallelLogger::logInfo("Cohorts being diagnosed does not have concept ids. Skipping concept set diagnostics.")
    return(NULL)
  }
  
  # Save concept set metadata ---------------------------------------
  writeToCsv(data = conceptSets %>%
               dplyr::select(-.data$uniqueConceptSetId),
             fileName = file.path(exportFolder, "concept_sets.csv"),
             incremental = incremental,
             cohortId = conceptSets$cohortId)
  
  uniqueConceptSets <- conceptSets[!duplicated(conceptSets$uniqueConceptSetId),] %>% 
    dplyr::select(-.data$cohortId, -.data$conceptSetId)
  
  instantiateUniqueConceptSets(uniqueConceptSets = uniqueConceptSets,
                               connection = connection,
                               cdmDatabaseSchema = cdmDatabaseSchema,
                               oracleTempSchema = oracleTempSchema,
                               conceptSetsTable = "#inst_concept_sets")
  
  # Export concept IDs per concept set ------------------------------------------
  # Disabling for now, since we don't have a diagnostic for it. 
  # If we do enable this, it should be a separate option (e.g. runConceptSetExpansion),
  # and it should be tracked for incremental mode.
  # sql <- "SELECT DISTINCT codeset_id AS unique_concept_set_id,
  #             concept_id
  #         FROM @concept_sets_table;"
  # conceptSetConceptIds <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
  #                                                                    sql = sql,
  #                                                                    concept_sets_table = "#inst_concept_sets",
  #                                                                    snakeCaseToCamelCase = TRUE) %>% 
  #   tidyr::tibble()
  # 
  # conceptSetConceptIds <- conceptSetConceptIds %>%
  #   dplyr::inner_join(conceptSets, by = "uniqueConceptSetId") %>%
  #   dplyr::select(.data$cohortId, .data$conceptSetId, .data$conceptId)
  # 
  # writeToCsv(data = conceptSetConceptIds, 
  #            fileName = file.path(exportFolder, "concept_sets_concept_id.csv"), 
  #            incremental = incremental,
  #            cohortId = conceptSetConceptIds$cohortId)
  # 
  # 
  # if (!is.null(conceptIdTable)) {
  #   sql <- "INSERT INTO @concept_id_table (concept_id)
  #           SELECT DISTINCT concept_id
  #           FROM @concept_sets_table;"
  #   DatabaseConnector::renderTranslateExecuteSql(connection = connection,
  #                                                sql = sql,
  #                                                oracleTempSchema = oracleTempSchema,
  #                                                concept_id_table = conceptIdTable,
  #                                                concept_sets_table = "#inst_concept_sets",
  #                                                progressBar = FALSE,
  #                                                reportOverallTime = FALSE)
  # }
  
  if ((runIncludedSourceConcepts && nrow(subsetIncluded) > 0) || 
      (runOrphanConcepts && nrow(subsetOrphans) > 0)) {
    createConceptCountsTable(connection = connection,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             oracleTempSchema = oracleTempSchema,
                             conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                             conceptCountsTable = conceptCountsTable,
                             conceptCountsTableIsTemp = conceptCountsTableIsTemp)
  }
  if (runIncludedSourceConcepts) {
    # Included concepts ------------------------------------------------------------------
    ParallelLogger::logInfo("Fetching included source concepts")
    #TODO: Disregard empty cohorts in tally:
    if (incremental && (nrow(cohorts) - nrow(subsetIncluded)) > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.",
                                      nrow(cohorts) - nrow(subsetIncluded)))
    }
    if (nrow(subsetIncluded) > 0) {
      start <- Sys.time()
      
      if (useExternalConceptCountsTable) {
        stop("Use of external concept count table is not supported")
        
        # sql <-
        #   SqlRender::loadRenderTranslateSql(
        #     "CohortSourceConceptsFromCcTable.sql",
        #     packageName = "CohortDiagnostics",
        #     dbms = connection@dbms,
        #     oracleTempSchema = oracleTempSchema,
        #     cdm_database_schema = cdmDatabaseSchema,
        #     concept_counts_database_schema = conceptCountsDatabaseSchema,
        #     concept_counts_table = conceptCountsTable,
        #     concept_counts_table_is_temp = conceptCountsTableIsTemp
        #   )
        # 
        # sourceCounts <-
        #   DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
        # 
        # sql <-
        #   SqlRender::loadRenderTranslateSql(
        #     "CohortStandardConceptsFromCcTable.sql",
        #     packageName = "CohortDiagnostics",
        #     dbms = connection@dbms,
        #     oracleTempSchema = oracleTempSchema,
        #     cdm_database_schema = cdmDatabaseSchema,
        #     concept_counts_database_schema = conceptCountsDatabaseSchema,
        #     concept_counts_table = conceptCountsTable,
        #     concept_counts_table_is_temp = conceptCountsTableIsTemp
        #   )
        # standardCounts <-
        #   DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
        # 
        # # To avoid double counting, subtract standard concept counts included in source counts.
        # # Note: this can create negative counts, because a source concept can be double counted itself
        # # if it maps to more than one standard concept, but it will show correctly in the viewer app,
        # # where the counts will be added back in.
        # dupCounts <-
        #   aggregate(conceptCount ~ conceptId, sourceCounts, sum)
        # colnames(dupCounts)[2] <- "dupCount"
        # dupSubjects <-
        #   aggregate(conceptSubjects ~ conceptId, sourceCounts, sum)
        # colnames(dupSubjects)[2] <- "dupSubjects"
        # standardCounts <-
        #   merge(standardCounts, dupCounts, all.x = TRUE)
        # standardCounts <-
        #   merge(standardCounts, dupSubjects, all.x = TRUE)
        # standardCounts$dupCount[is.na(standardCounts$dupCount)] <- 0
        # standardCounts$dupSubjects[is.na(standardCounts$dupSubjects)] <-
        #   0
        # standardCounts$conceptCount <-
        #   standardCounts$conceptCount - standardCounts$dupCount
        # standardCounts$conceptSubjects <-
        #   standardCounts$conceptSubjects - standardCounts$dupSubjects
        # standardCounts$dupCount <- NULL
        # standardCounts$dupSubjects <- NULL
        # 
        # counts <- dplyr::bind_rows(sourceCounts, standardCounts)
      } else {
        sql <- SqlRender::loadRenderTranslateSql( "CohortSourceCodes.sql",
                                                  packageName = "CohortDiagnostics",
                                                  dbms = connection@dbms,
                                                  oracleTempSchema = oracleTempSchema,
                                                  cdm_database_schema = cdmDatabaseSchema,
                                                  instantiated_concept_sets = "#inst_concept_sets",
                                                  include_source_concept_table = "#inc_src_concepts",
                                                  by_month = FALSE )
        DatabaseConnector::executeSql(connection = connection, sql = sql)
        counts <- DatabaseConnector::renderTranslateQuerySql(connection = connection, 
                                                             sql = "SELECT * FROM @include_source_concept_table;",
                                                             include_source_concept_table = "#inc_src_concepts",
                                                             oracleTempSchema = oracleTempSchema,
                                                             snakeCaseToCamelCase = TRUE) %>% 
          tidyr::tibble()
        
        counts <- counts  %>% 
          dplyr::rename(uniqueConceptSetId = .data$conceptSetId) %>% 
          dplyr::inner_join(conceptSets %>% dplyr::select(.data$uniqueConceptSetId, 
                                                          .data$cohortId, 
                                                          .data$conceptSetId),
                            by = "uniqueConceptSetId") %>% 
          dplyr::select(-.data$uniqueConceptSetId) %>% 
          dplyr::mutate(databaseId = !!databaseId) %>% 
          dplyr::relocate(.data$databaseId, 
                          .data$cohortId, 
                          .data$conceptSetId, 
                          .data$conceptId) %>% 
          dplyr::distinct()
        
        if (nrow(counts) > 0) {
          counts$databaseId <- databaseId
          counts <- enforceMinCellValue(counts, "conceptSubjects", minCellCount)
          counts <- enforceMinCellValue(counts, "conceptCount", minCellCount)
        }
        writeToCsv(counts,
                   file.path(exportFolder, "included_source_concept.csv"),
                   incremental = incremental,
                   cohortId = subsetIncluded$cohortId)
        recordTasksDone(cohortId = subsetIncluded$cohortId,
                        task = "runIncludedSourceConcepts",
                        checksum = subsetIncluded$checksum,
                        recordKeepingFile = recordKeepingFile,
                        incremental = incremental)
        
        if (!is.null(conceptIdTable)) {
          sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @include_source_concept_table;
                  
                  INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT source_concept_id
                  FROM @include_source_concept_table;"
          DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                       sql = sql,
                                                       oracleTempSchema = oracleTempSchema,
                                                       concept_id_table = conceptIdTable,
                                                       include_source_concept_table = "#inc_src_concepts",
                                                       progressBar = FALSE,
                                                       reportOverallTime = FALSE)
        }
        sql <- "TRUNCATE TABLE @include_source_concept_table;\nDROP TABLE @include_source_concept_table;"
        DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                     sql = sql,
                                                     oracleTempSchema = oracleTempSchema,
                                                     include_source_concept_table = "#inc_src_concepts",
                                                     progressBar = FALSE,
                                                     reportOverallTime = FALSE)
        
        delta <- Sys.time() - start
        ParallelLogger::logInfo(paste("Finding source codes took", signif(delta, 3), attr(delta, "units")))
      }
    }
  }
  
  if (runBreakdownIndexEvents) {
    # Index event breakdown --------------------------------------------------------------------------
    ParallelLogger::logInfo("Breaking down index events")
    if (incremental && (nrow(cohorts) - nrow(subsetBreakdown)) > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.",
                                      nrow(cohorts) - nrow(subsetBreakdown)))
    }
    if (nrow(subsetBreakdown) > 0) {
      start <- Sys.time()
      domains <- readr::read_csv(system.file("csv", "domains.csv", package = "CohortDiagnostics"),
                                 col_types = readr::cols(),
                                 guess_max = min(1e7))
      
      runBreakdownIndexEvents <- function(cohort) {
        ParallelLogger::logInfo("- Breaking down index events for cohort '", cohort$cohortName, "'")
        
        cohortDefinition <- RJSONIO::fromJSON(cohort$json, digits = 23)
        primaryCodesetIds <- lapply(cohortDefinition$PrimaryCriteria$CriteriaList, getCodeSetIds) %>% 
          dplyr::bind_rows() 
        if (nrow(primaryCodesetIds) == 0) {
          warning("No primary event criteria concept sets found for cohort id: ", cohort$cohortId)
          return(tidyr::tibble())
        }
        primaryCodesetIds <- conceptSets %>%
          dplyr::filter(.data$cohortId %in% cohort$cohortId) %>%
          dplyr::select(codeSetIds = .data$conceptSetId, .data$uniqueConceptSetId) %>%
          dplyr::inner_join(primaryCodesetIds, by = "codeSetIds")
        
        pasteIds <- function(row) {
          return(dplyr::tibble(domain = row$domain[1],
                               uniqueConceptSetId = paste(row$uniqueConceptSetId, collapse = ", ")))
        }
        primaryCodesetIds <- lapply(split(primaryCodesetIds, primaryCodesetIds$domain), pasteIds)
        primaryCodesetIds <- dplyr::bind_rows(primaryCodesetIds)
        
        getCounts <- function(row) {
          domain <- domains[domains$domain == row$domain, ]
          sql <- SqlRender::loadRenderTranslateSql("CohortEntryBreakdown.sql",
                                                   packageName = "CohortDiagnostics",
                                                   dbms = connection@dbms,
                                                   oracleTempSchema = oracleTempSchema,
                                                   cdm_database_schema = cdmDatabaseSchema,
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
                                                   store_table = "#breakdown")
          DatabaseConnector::executeSql(connection = connection, 
                                        sql = sql, 
                                        progressBar = FALSE, 
                                        reportOverallTime = FALSE)
          sql <- "SELECT * FROM @store_table;"
          counts <- DatabaseConnector::renderTranslateQuerySql(connection = connection, 
                                                               sql = sql,
                                                               oracleTempSchema = oracleTempSchema,
                                                               store_table = "#breakdown",
                                                               snakeCaseToCamelCase = TRUE) %>% 
            tidyr::tibble()
          if (!is.null(conceptIdTable)) {
            sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @store_table;"
            DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                         sql = sql,
                                                         oracleTempSchema = oracleTempSchema,
                                                         concept_id_table = conceptIdTable,
                                                         store_table = "#breakdown",
                                                         progressBar = FALSE,
                                                         reportOverallTime = FALSE)
          }
          sql <- "TRUNCATE TABLE @store_table;\nDROP TABLE @store_table;"
          DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                       sql = sql,
                                                       oracleTempSchema = oracleTempSchema,
                                                       store_table = "#breakdown",
                                                       progressBar = FALSE,
                                                       reportOverallTime = FALSE)
          return(counts)
        }
        counts <- lapply(split(primaryCodesetIds, 1:nrow(primaryCodesetIds)), getCounts) %>% 
          dplyr::bind_rows() %>% 
          dplyr::arrange(.data$conceptCount)
        
        if (nrow(counts) > 0) {
          counts$cohortId <- cohort$cohortId
        } else {
          ParallelLogger::logInfo("Index event breakdown results were not returned for: ", cohort$cohortId)
          return(dplyr::tibble())
        }
        return(counts)
      }
      data <- lapply(split(subsetBreakdown, subsetBreakdown$cohortId), runBreakdownIndexEvents)
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
      delta <- Sys.time() - start
      ParallelLogger::logInfo(paste("Breaking down index event took", signif(delta, 3), attr(delta, "units")))
    }
  }
  
  if (runOrphanConcepts) {
    # Orphan concepts ---------------------------------------------------------
    ParallelLogger::logInfo("Finding orphan concepts")
    if (incremental && (nrow(cohorts) - nrow(subsetOrphans)) > 0) {
      ParallelLogger::logInfo(sprintf("Skipping %s cohorts in incremental mode.",
                                      nrow(cohorts) - nrow(subsetOrphans)))
    }
    if (nrow(subsetOrphans > 0)) {
      start <- Sys.time()
      if (!useExternalConceptCountsTable) {
        ParallelLogger::logTrace("Using internal concept count table.")
      } else {
        stop("Use of external concept count table is not supported")
      }
      
      # [OPTIMIZATION idea] can we modify the sql to do this for all uniqueConceptSetId in one query using group by?
      data <- list()
      for (i in (1:nrow(uniqueConceptSets))) {
        conceptSet <- uniqueConceptSets[i,]
        ParallelLogger::logInfo("- Finding orphan concepts for concept set '", conceptSet$conceptSetName, "'")
        data[[i]] <- .findOrphanConcepts(connection = connection,
                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                         oracleTempSchema = oracleTempSchema,
                                         useCodesetTable = TRUE,
                                         codesetId = conceptSet$uniqueConceptSetId,
                                         conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
                                         conceptCountsTable = conceptCountsTable,
                                         conceptCountsTableIsTemp = conceptCountsTableIsTemp,
                                         instantiatedCodeSets = "#inst_concept_sets",
                                         orphanConceptTable = "#orphan_concepts")
        
        if (!is.null(conceptIdTable)) {
          sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @orphan_concept_table;"
          DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                       sql = sql,
                                                       oracleTempSchema = oracleTempSchema,
                                                       concept_id_table = conceptIdTable,
                                                       orphan_concept_table = "#orphan_concepts",
                                                       progressBar = FALSE,
                                                       reportOverallTime = FALSE)
        }
        sql <- "TRUNCATE TABLE @orphan_concept_table;\nDROP TABLE @orphan_concept_table;"
        DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                     sql = sql,
                                                     oracleTempSchema = oracleTempSchema,
                                                     orphan_concept_table = "#orphan_concepts",
                                                     progressBar = FALSE,
                                                     reportOverallTime = FALSE)
      }
      data <- dplyr::bind_rows(data) %>% 
        dplyr::distinct() %>% 
        dplyr::rename(uniqueConceptSetId = .data$codesetId) %>% 
        dplyr::inner_join(conceptSets %>% 
                            dplyr::select(.data$uniqueConceptSetId, 
                                          .data$cohortId, 
                                          .data$conceptSetId),
                          by = "uniqueConceptSetId") %>% 
        dplyr::select(-.data$uniqueConceptSetId) %>% 
        dplyr::mutate(databaseId = !!databaseId) %>% 
        dplyr::relocate(.data$cohortId, .data$conceptSetId, .data$databaseId)
      
      if (nrow(data) > 0) {
        data <- enforceMinCellValue(data, "conceptCount", minCellCount)
        data <- enforceMinCellValue(data, "conceptSubjects", minCellCount)
      }
      
      writeToCsv(data,
                 file.path(exportFolder, "orphan_concept.csv"),
                 incremental = incremental,
                 cohortId = subsetOrphans$cohortId)
      
      recordTasksDone(cohortId = subsetOrphans$cohortId,
                      task = "runOrphanConcepts",
                      checksum = subsetOrphans$checksum,
                      recordKeepingFile = recordKeepingFile,
                      incremental = incremental)
      
      delta <- Sys.time() - start
      ParallelLogger::logInfo("Finding orphan concepts took ", signif(delta, 3), " ", attr(delta, "units"))
    }
  }
  ParallelLogger::logTrace("Dropping temp concept set table")
  sql <- "TRUNCATE TABLE #inst_concept_sets; DROP TABLE #inst_concept_sets;"
  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               sql,
                                               oracleTempSchema = oracleTempSchema,
                                               progressBar = FALSE,
                                               reportOverallTime = FALSE)
  
  if ((runIncludedSourceConcepts && nrow(subsetIncluded) > 0) || 
      (runOrphanConcepts && nrow(subsetOrphans) > 0)) {
    ParallelLogger::logTrace("Dropping temp concept count table")
    if (conceptCountsTableIsTemp) {
      countTable <- conceptCountsTable
    } else {
      countTable <- paste(conceptCountsDatabaseSchema, conceptCountsTable, sep = ".")
    }
    
    sql <- "TRUNCATE TABLE @count_table; DROP TABLE @count_table;"
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 oracleTempSchema = oracleTempSchema,
                                                 count_table = countTable,
                                                 progressBar = FALSE,
                                                 reportOverallTime = FALSE)
  }
  
  delta <- Sys.time() - startConceptSetDiagnostics
  ParallelLogger::logInfo("Running concept set diagnostics took ", signif(delta, 3), " ", attr(delta, "units"))
}
