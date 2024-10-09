
# Copyright 2024 Observational Health Data Sciences and Informatics
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


#' Title
#'
#' @param connection 
#' @param tempEmulationSchema 
#' @param cdmDatabaseSchema 
#' @param vocabularyDatabaseSchema 
#' @param databaseId 
#' @param cohorts 
#' @param runIncludedSourceConcepts 
#' @param runOrphanConcepts 
#' @param runBreakdownIndexEvents 
#' @param exportFolder 
#' @param minCellCount 
#' @param conceptCountsDatabaseSchema 
#' @param conceptCountsTable 
#' @param conceptCountsTableIsTemp 
#' @param cohortDatabaseSchema 
#' @param cohortTable 
#' @param useExternalConceptCountsTable 
#' @param incremental 
#' @param conceptIdTable 
#' @param recordKeepingFile 
#' @param useAchilles 
#' @param resultsDatabaseSchema 
#'
#' @return
#' @export
#'
#' @examples
runBreakdownIndexEvents <- function(connection,
                                    tempEmulationSchema,
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchema = cdmDatabaseSchema,
                                    databaseId,
                                    cohorts,
                                    exportFolder,
                                    minCellCount,
                                    conceptCountsDatabaseSchema = NULL,
                                    conceptCountsTable = "concept_counts",
                                    conceptCountsTableIsTemp = FALSE,
                                    cohortDatabaseSchema,
                                    cohortTable,
                                    useExternalConceptCountsTable = FALSE,
                                    incremental = FALSE,
                                    conceptIdTable = NULL,
                                    recordKeepingFile,
                                    useAchilles,
                                    resultsDatabaseSchema) {
  ParallelLogger::logInfo("Starting concept set diagnostics")
  startConceptSetDiagnostics <- Sys.time()
  subset <- dplyr::tibble()
  
  if (runIncludedSourceConcepts) {
    subsetIncluded <- subsetToRequiredCohorts(
      cohorts = cohorts,
      task = "runIncludedSourceConcepts",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    subset <- dplyr::bind_rows(subset, subsetIncluded)
  }
  if (runBreakdownIndexEvents) {
    subsetBreakdown <- subsetToRequiredCohorts(
      cohorts = cohorts,
      task = "runBreakdownIndexEvents",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    subset <- dplyr::bind_rows(subset, subsetBreakdown)
  }
  
  if (runOrphanConcepts) {
    subsetOrphans <- subsetToRequiredCohorts(
      cohorts = cohorts,
      task = "runOrphanConcepts",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    subset <- dplyr::bind_rows(subset, subsetOrphans)
  }
  subset <- dplyr::distinct(subset)
  
  if (nrow(subset) == 0) {
    return(NULL)
  }
  
  # We need to get concept sets from all cohorts in case subsets are present and
  # Added incrementally after cohort generation
  conceptSets <- combineConceptSetsFromCohorts(cohorts)
  conceptSets <- conceptSets %>% dplyr::filter(.data$cohortId %in% subset$cohortId)
  
  if (is.null(conceptSets)) {
    ParallelLogger::logInfo(
      "Cohorts being diagnosed does not have concept ids. Skipping concept set diagnostics."
    )
    return(NULL)
  }
  
  uniqueConceptSets <-
    conceptSets[!duplicated(conceptSets$uniqueConceptSetId), ] %>%
    dplyr::select(-"cohortId", -"conceptSetId")
  
  if (nrow(uniqueConceptSets) == 0) {
    ParallelLogger::logInfo("No concept sets found - skipping")
    return(NULL)
  }
  
  timeExecution(
    exportFolder,
    taskName = "instantiateUniqueConceptSets",
    cohortIds = NULL,
    parent = "runConceptSetDiagnostics",
    expr = {
      instantiateUniqueConceptSets(
        uniqueConceptSets = uniqueConceptSets,
        connection = connection,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        conceptSetsTable = "#inst_concept_sets"
      )
    }
  )
  
  if ((runIncludedSourceConcepts && nrow(subsetIncluded) > 0) ||
      (runOrphanConcepts && nrow(subsetOrphans) > 0)) {
    timeExecution(
      exportFolder,
      taskName = "createConceptCountsTable",
      cohortIds = NULL,
      parent = "runConceptSetDiagnostics",
      expr = {
        createConceptCountsTable(
          connection = connection,
          cdmDatabaseSchema = cdmDatabaseSchema,
          tempEmulationSchema = tempEmulationSchema,
          conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
          conceptCountsTable = conceptCountsTable,
          conceptCountsTableIsTemp = conceptCountsTableIsTemp
        )
      }
    )
  }
  if (runIncludedSourceConcepts) {
    timeExecution(
      exportFolder,
      taskName = "runIncludedSourceConcepts",
      cohortIds = NULL,
      parent = "runConceptSetDiagnostics",
      expr = {
        # Included concepts ------------------------------------------------------------------
        ParallelLogger::logInfo("Fetching included source concepts")
        # TODO: Disregard empty cohorts in tally:
        if (incremental && (nrow(cohorts) - nrow(subsetIncluded)) > 0) {
          ParallelLogger::logInfo(sprintf(
            "Skipping %s cohorts in incremental mode.",
            nrow(cohorts) - nrow(subsetIncluded)
          ))
        }
        if (nrow(subsetIncluded) > 0) {
          start <- Sys.time()
          if (useExternalConceptCountsTable) {
            stop("Use of external concept count table is not supported")
          } else {
            sql <- SqlRender::loadRenderTranslateSql(
              "CohortSourceCodes.sql",
              packageName = utils::packageName(),
              dbms = connection@dbms,
              tempEmulationSchema = tempEmulationSchema,
              cdm_database_schema = cdmDatabaseSchema,
              instantiated_concept_sets = "#inst_concept_sets",
              include_source_concept_table = "#inc_src_concepts",
              by_month = FALSE
            )
            DatabaseConnector::executeSql(connection = connection, sql = sql)
            counts <-
              DatabaseConnector::renderTranslateQuerySql(
                connection = connection,
                sql = "SELECT * FROM @include_source_concept_table;",
                include_source_concept_table = "#inc_src_concepts",
                tempEmulationSchema = tempEmulationSchema,
                snakeCaseToCamelCase = TRUE
              ) %>%
              tidyr::tibble()
            
            counts <- counts %>%
              dplyr::distinct() %>%
              dplyr::rename("uniqueConceptSetId" = "conceptSetId") %>%
              dplyr::inner_join(
                conceptSets %>% dplyr::select(
                  "uniqueConceptSetId",
                  "cohortId",
                  "conceptSetId"
                ) %>% dplyr::distinct(),
                by = "uniqueConceptSetId",
                relationship = "many-to-many"
              ) %>%
              dplyr::select(-"uniqueConceptSetId") %>%
              dplyr::mutate(databaseId = !!databaseId) %>%
              dplyr::relocate(
                "databaseId",
                "cohortId",
                "conceptSetId",
                "conceptId"
              ) %>%
              dplyr::distinct()
            
            counts <- counts %>%
              dplyr::group_by(
                .data$databaseId,
                .data$cohortId,
                .data$conceptSetId,
                .data$conceptId,
                .data$sourceConceptId
              ) %>%
              dplyr::summarise(
                conceptCount = max(.data$conceptCount),
                conceptSubjects = max(.data$conceptSubjects)
              ) %>%
              dplyr::ungroup()
            
            exportDataToCsv(
              data = counts,
              tableName = "included_source_concept",
              fileName = file.path(exportFolder, "included_source_concept.csv"),
              minCellCount = minCellCount,
              databaseId = databaseId,
              incremental = incremental,
              cohortId = subsetIncluded$cohortId
            )
            
            recordTasksDone(
              cohortId = subsetIncluded$cohortId,
              task = "runIncludedSourceConcepts",
              checksum = subsetIncluded$checksum,
              recordKeepingFile = recordKeepingFile,
              incremental = incremental
            )
            
            if (!is.null(conceptIdTable)) {
              sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @include_source_concept_table;

                  INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT source_concept_id
                  FROM @include_source_concept_table;"
              DatabaseConnector::renderTranslateExecuteSql(
                connection = connection,
                sql = sql,
                tempEmulationSchema = tempEmulationSchema,
                concept_id_table = conceptIdTable,
                include_source_concept_table = "#inc_src_concepts",
                progressBar = FALSE,
                reportOverallTime = FALSE
              )
            }
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
            
            delta <- Sys.time() - start
            ParallelLogger::logInfo(paste(
              "Finding source codes took",
              signif(delta, 3),
              attr(delta, "units")
            ))
          }
        }
      }
    )
  }
  
  if (runBreakdownIndexEvents) {
    # Index event breakdown --------------------------------------------------------------------------
    ParallelLogger::logInfo("Breaking down index events")
    if (incremental &&
        (nrow(cohorts) - nrow(subsetBreakdown)) > 0) {
      ParallelLogger::logInfo(sprintf(
        "Skipping %s cohorts in incremental mode.",
        nrow(cohorts) - nrow(subsetBreakdown)
      ))
    }
    if (nrow(subsetBreakdown) > 0) {
      start <- Sys.time()
      readr::local_edition(1)
      domains <-
        readr::read_csv(
          system.file("csv", "domains.csv", package = utils::packageName()),
          col_types = readr::cols(),
          guess_max = min(1e7)
        )
      
      getCohortIndexEventBreakdown <- function(cohort) {
        ParallelLogger::logInfo(
          "- Breaking down index events for cohort '",
          cohort$cohortName,
          "'"
        )
        
        timeExecution(
          exportFolder,
          taskName = "getBreakdownIndexEvents",
          cohortIds = cohort$cohortId,
          parent = "runConceptSetDiagnostics",
          expr = {
            if (isTRUE(cohort$isSubset)) {
              parent <- getParentCohort(cohort, cohorts)
              jsonDef <- parent$json
            } else {
              jsonDef <- cohort$json
            }
            
            cohortDefinition <-
              RJSONIO::fromJSON(jsonDef, digits = 23)
            
            primaryCodesetIds <-
              lapply(
                cohortDefinition$PrimaryCriteria$CriteriaList,
                getCodeSetIds
              )
            
            if (length(primaryCodesetIds)) {
              primaryCodesetIds <- dplyr::bind_rows(primaryCodesetIds)
            } else {
              primaryCodesetIds <- data.frame()
            }
            
            if (nrow(primaryCodesetIds) == 0) {
              warning(
                "No primary event criteria concept sets found for cohort id: ",
                cohort$cohortId
              )
              return(tidyr::tibble())
            }
            primaryCodesetIds <- primaryCodesetIds %>% dplyr::filter(.data$domain %in%
                                                                       c(domains$domain %>% unique()))
            if (nrow(primaryCodesetIds) == 0) {
              warning(
                "Primary event criteria concept sets found for cohort id: ",
                cohort$cohortId, " but,", "\nnone of the concept sets belong to the supported domains.",
                "\nThe supported domains are:\n", paste(domains$domain,
                                                        collapse = ", "
                )
              )
              return(tidyr::tibble())
            }
            primaryCodesetIds <- conceptSets %>%
              dplyr::filter(.data$cohortId %in% cohort$cohortId) %>%
              dplyr::select(
                codeSetIds = "conceptSetId",
                "uniqueConceptSetId"
              ) %>%
              dplyr::distinct() %>%
              dplyr::inner_join(primaryCodesetIds %>% dplyr::distinct(), by = "codeSetIds")
            
            pasteIds <- function(row) {
              return(dplyr::tibble(
                domain = row$domain[1],
                uniqueConceptSetId = paste(row$uniqueConceptSetId, collapse = ", ")
              ))
            }
            
            primaryCodesetIds <-
              lapply(
                split(primaryCodesetIds, primaryCodesetIds$domain),
                pasteIds
              )
            
            if (length(primaryCodesetIds) == 0) {
              primaryCodesetIds <- data.frame()
            } else {
              primaryCodesetIds <- dplyr::bind_rows(primaryCodesetIds)
            }
            
            getCounts <- function(row) {
              domain <- domains %>% dplyr::filter(.data$domain == row$domain)
              sql <-
                SqlRender::loadRenderTranslateSql(
                  "CohortEntryBreakdown.sql",
                  packageName = utils::packageName(),
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
                  use_source_concept_id = !(is.na(domain$domainSourceConceptId) | is.null(domain$domainSourceConceptId)),
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
                DatabaseConnector::renderTranslateQuerySql(
                  connection = connection,
                  sql = sql,
                  tempEmulationSchema = tempEmulationSchema,
                  store_table = "#breakdown",
                  snakeCaseToCamelCase = TRUE
                ) %>%
                tidyr::tibble()
              if (!is.null(conceptIdTable)) {
                sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @store_table;"
                DatabaseConnector::renderTranslateExecuteSql(
                  connection = connection,
                  sql = sql,
                  tempEmulationSchema = tempEmulationSchema,
                  concept_id_table = conceptIdTable,
                  store_table = "#breakdown",
                  progressBar = FALSE,
                  reportOverallTime = FALSE
                )
              }
              sql <-
                "TRUNCATE TABLE @store_table;\nDROP TABLE @store_table;"
              DatabaseConnector::renderTranslateExecuteSql(
                connection = connection,
                sql = sql,
                tempEmulationSchema = tempEmulationSchema,
                store_table = "#breakdown",
                progressBar = FALSE,
                reportOverallTime = FALSE
              )
              return(counts)
            }
            
            
            if (nrow(primaryCodesetIds) > 0) {
              counts <-
                lapply(split(primaryCodesetIds, 1:nrow(primaryCodesetIds)), getCounts) %>%
                dplyr::bind_rows() %>%
                dplyr::arrange(.data$conceptCount)
            } else {
              counts <- data.frame()
            }
            
            
            if (nrow(counts) > 0) {
              counts$cohortId <- cohort$cohortId
            } else {
              ParallelLogger::logInfo(
                "Index event breakdown results were not returned for: ",
                cohort$cohortId
              )
              return(dplyr::tibble())
            }
            return(counts)
          }
        )
      }
      
      data <-
        lapply(
          split(subsetBreakdown, subsetBreakdown$cohortId),
          getCohortIndexEventBreakdown
        )
      
      data <- dplyr::bind_rows(data)
      
      exportDataToCsv(
        data = data,
        tableName = "index_event_breakdown",
        fileName = file.path(exportFolder, "index_event_breakdown.csv"),
        minCellCount = minCellCount,
        databaseId = databaseId,
        incremental = incremental,
        cohortId = subset$cohortId
      )
      
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runBreakdownIndexEvents",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
      delta <- Sys.time() - start
      ParallelLogger::logInfo(paste(
        "Breaking down index event took",
        signif(delta, 3),
        attr(delta, "units")
      ))
    }
  }
  
  # put all instantiated concepts into #concept_ids table
  # this is extracted with vocabulary tables
  # this will have more codes than included source concepts
  # included source concepts is limited to resolved concept ids in source data
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "INSERT INTO #concept_ids (concept_id)
            SELECT DISTINCT concept_id
            FROM #inst_concept_sets;",
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  resolvedConceptIds <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM #inst_concept_sets;",
      tempEmulationSchema = tempEmulationSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble() %>%
    dplyr::rename("uniqueConceptSetId" = "codesetId") %>%
    dplyr::inner_join(conceptSets %>% dplyr::distinct(),
                      by = "uniqueConceptSetId",
                      relationship = "many-to-many"
    ) %>%
    dplyr::select(
      "cohortId",
      "conceptSetId",
      "conceptId"
    ) %>%
    dplyr::distinct()
  
  exportDataToCsv(
    data = resolvedConceptIds,
    tableName = "resolved_concepts",
    fileName = file.path(exportFolder, "resolved_concepts.csv"),
    minCellCount = minCellCount,
    databaseId = databaseId,
    incremental = TRUE
  )
  
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
  
  if ((runIncludedSourceConcepts && nrow(subsetIncluded) > 0) ||
      (runOrphanConcepts && nrow(subsetOrphans) > 0)) {
    ParallelLogger::logTrace("Dropping temp concept count table")
    if (conceptCountsTableIsTemp) {
      countTable <- conceptCountsTable
    } else {
      countTable <-
        paste(conceptCountsDatabaseSchema, conceptCountsTable, sep = ".")
    }
    
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
  
  delta <- Sys.time() - startConceptSetDiagnostics
  ParallelLogger::logInfo(
    "Running concept set diagnostics took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
}
