
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


getBreakdownIndexEvents <- function(connection,
                                    cohort,
                                    conceptSets, 
                                    tempEmulationSchema,
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchema,
                                    cohortDatabaseSchema,
                                    cohortTable) {
  
  if (!CohortGenerator::isCohortDefinitionSet(cohort) || nrow(cohort) != 1) {
    stop("cohortDefinitionSet must have one row")
  }
  
  if(!tempTableExists(connection, "inst_concept_sets")) {
    stop("Execute the function runResolvedConceptSets() first.")
  }
  
  
  
  domains <- 
    readr::read_csv(
      system.file("csv", "domains.csv", package = "CohortDiagnostics", mustWork = T),
      show_col_types = FALSE
    )
  
  emptyResult <- dplyr::tibble(
    domainTable = character(),        
    domainField = character(),                
    conceptId = double(),
    conceptCount = double(),
    subjectCount = double(),
    cohortId = double()
  )
  
  if (isTRUE(cohort$isSubset)) {
    checkmate::assert_character(cohort$parentJson, len = 1, any.missing = F)
    jsonDef <- cohort$parentJson
  } else {
    jsonDef <- cohort$json
  }

  cohortDefinition <- RJSONIO::fromJSON(jsonDef, digits = 23)

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
    return(emptyResult)
  }
  
  primaryCodesetIds <- primaryCodesetIds %>% 
    dplyr::filter(.data$domain %in% c(domains$domain %>% unique()))

  if (nrow(primaryCodesetIds) == 0) {
    warning(
      "Primary event criteria concept sets found for cohort id: ",
      cohort$cohortId, " but,", 
      "\nnone of the concept sets belong to the supported domains.",
      "\nThe supported domains are:\n", 
      paste(domains$domain, collapse = ", ")
    )
    return(emptyResult)
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
    return(emptyResult)
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
    
    counts <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT * FROM #breakdown;",
        tempEmulationSchema = tempEmulationSchema,
        snakeCaseToCamelCase = TRUE
      ) %>%
      tidyr::tibble()
    
    sql <- "INSERT INTO #concept_ids (concept_id)
            SELECT DISTINCT a.concept_id
            FROM #breakdown a
            LEFT JOIN #concept_ids b ON a.concept_id = b.concept_id
            WHERE a.concept_id is not NULL AND b.concept_id is NULL;"
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "TRUNCATE TABLE #breakdown;\nDROP TABLE #breakdown;",
      tempEmulationSchema = tempEmulationSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    return(counts)
  }

  # optimization idea - can this loop be removed and done all in one sql statement?
  counts <-
    lapply(split(primaryCodesetIds, 1:nrow(primaryCodesetIds)), getCounts) %>%
    dplyr::bind_rows() %>%
    dplyr::arrange(.data$conceptCount)

  if (nrow(counts) > 0) {
    counts$cohortId <- cohort$cohortId
  } else {
    ParallelLogger::logInfo(
      "Index event breakdown results were not returned for: ",
      cohort$cohortId
    )
    return(emptyResult)
  }
  return(counts)
}



#' runBreakdownIndexEvents
#'
#' @template connection 
#' @template cohortDefinitionSet 
#' @template tempEmulationSchema 
#' @template cdmDatabaseSchema 
#' @template vocabularyDatabaseSchema 
#' @template cohortDatabaseSchema 
#' @template databaseId 
#' @template exportFolder 
#' @template minCellCount 
#' @template cohortTable 
#' @template Incremental 
#'
#' @return NULL
#' @export
runBreakdownIndexEvents <- function(connection,
                                    cohortDefinitionSet,
                                    tempEmulationSchema,
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchema,
                                    cohortDatabaseSchema,
                                    databaseId,
                                    exportFolder,
                                    minCellCount,
                                    cohortTable,
                                    incremental = FALSE,
                                    incrementalFolder) {
  
  ParallelLogger::logInfo("Breaking down index events")
  start <- Sys.time()
  
  cohortDefinitionSet$checksum <- computeChecksum(cohortDefinitionSet$sql)
  
  # for each row in the cohort definition set add a column with parent json if it is a subset
  # this is so that all data needed for running one cohort is in one row of the cohort definition set
  cohortDefinitionSet$parentJson <- vapply(
    split(cohortDefinitionSet, cohortDefinitionSet$cohortId),
    FUN = function(.) getParentCohort(., cohortDefinitionSet)$json,
    FUN.VALUE = character(1L)
  )
  
  subset <- subsetToRequiredCohorts(
    cohorts = cohortDefinitionSet,
    task = "runBreakdownIndexEvents",
    incremental = incremental,
    recordKeepingFile = file.path(incrementalFolder, "CreatedDiagnostics.csv")
  ) %>% dplyr::distinct()
  
  if (nrow(subset) == 0) {
    return(NULL)
  }
  
  if (incremental && (nrow(cohorts) - nrow(subset)) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Skipping %s cohorts in incremental mode.",
      nrow(cohorts) - nrow(subset)
    ))
  }
  
  conceptSets <- combineConceptSetsFromCohorts(cohortDefinitionSet) %>% 
    dplyr::filter(.data$cohortId %in% subset$cohortId)
     
  data <- lapply(split(subset, subset$cohortId),
    function(cohort) {
      ParallelLogger::logInfo(
        "- Breaking down index events for cohort '", cohort$cohortName, "'"
      )
      
      timeExecution(
        exportFolder,
        taskName = "getBreakdownIndexEvents",
        cohortIds = cohort$cohortId,
        parent = "runConceptSetDiagnostics",
        expr = {
          result <- getBreakdownIndexEvents(
            connection = connection,
            cohort = cohort,
            conceptSets = conceptSets,
            tempEmulationSchema = tempEmulationSchema,
            cdmDatabaseSchema = cdmDatabaseSchema,
            vocabularyDatabaseSchema = vocabularyDatabaseSchema,
            cohortDatabaseSchema = cohortDatabaseSchema,
            cohortTable = cohortTable
          ) 
        }
      )
      return(result)
    }
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
    recordKeepingFile = file.path(incrementalFolder, "CreatedDiagnostics.csv"),
    incremental = incremental
  )
    
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste(
    "Breaking down index event took",
    signif(delta, 3),
    attr(delta, "units")
  ))
}
