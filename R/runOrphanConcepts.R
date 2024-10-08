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

.findOrphanConcepts <- function(connectionDetails = NULL,
                                connection = NULL,
                                cdmDatabaseSchema,
                                vocabularyDatabaseSchema = cdmDatabaseSchema,
                                tempEmulationSchema = NULL,
                                conceptIds = c(),
                                useCodesetTable = FALSE,
                                codesetId = 1,
                                conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                conceptCountsTable = "concept_counts",
                                conceptCountsTableIsTemp = FALSE,
                                instantiatedCodeSets = "#InstConceptSets",
                                orphanConceptTable = "#recommended_concepts") {
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
    DatabaseConnector::renderTranslateQuerySql(
      sql = sql,
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      orphan_concept_table = orphanConceptTable,
      snakeCaseToCamelCase = TRUE
    ) %>%
    tidyr::tibble()
  
  ParallelLogger::logTrace("- Dropping orphan temp tables")
  sql <-
    SqlRender::loadRenderTranslateSql(
      "DropOrphanConceptTempTables.sql",
      packageName = utils::packageName(),
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

#' Title
#'
#' @param connection 
#' @param tempEmulationSchema 
#' @param cdmDatabaseSchema 
#' @param vocabularyDatabaseSchema 
#' @param databaseId 
#' @param cohorts 
#' @param exportFolder 
#' @param minCellCount 
#' @param conceptCountsDatabaseSchema 
#' @param conceptCountsTable 
#' @param cohortDatabaseSchema 
#' @param cohortTable 
#' @param incremental 
#' @param conceptIdTable 
#' @param recordKeepingFile 
#' @param resultsDatabaseSchema 
#'
#' @return
#' @export
#'
#' @examples
runOrphanConcepts <- function(connection,
                              tempEmulationSchema,
                              cdmDatabaseSchema,
                              vocabularyDatabaseSchema = cdmDatabaseSchema,
                              databaseId,
                              cohorts,
                              exportFolder,
                              minCellCount,
                              conceptCountsDatabaseSchema = NULL,
                              conceptCountsTable = "concept_counts",
                              cohortDatabaseSchema,
                              cohortTable,
                              incremental = FALSE,
                              conceptIdTable = NULL,
                              recordKeepingFile,
                              resultsDatabaseSchema) {
  ParallelLogger::logInfo("Starting concept set diagnostics")
  startTime <- Sys.time()
  subset <- dplyr::tibble()
  
  subset <- subsetToRequiredCohorts(
      cohorts = cohorts,
      task = "runOrphanConcepts",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    ) %>%
    dplyr::distinct()
  
  # Make sure an empty result file is written to the export folder
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
  
  ParallelLogger::logInfo("Finding orphan concepts")
  if (incremental && (nrow(cohorts) - nrow(subsetOrphans)) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Skipping %s cohorts in incremental mode.",
      nrow(cohorts) - nrow(subsetOrphans)
    ))
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
      conceptSet <- uniqueConceptSets[i, ]
      ParallelLogger::logInfo(
        "- Finding orphan concepts for concept set '",
        conceptSet$conceptSetName,
        "'"
      )
      
      timeExecution(
        exportFolder,
        taskName = "orphanConcepts",
        parent = "runConceptSetDiagnostics",
        cohortIds = paste("concept_set-", conceptSet$conceptSetName),
        expr = {
          data[[i]] <- .findOrphanConcepts(
            connection = connection,
            cdmDatabaseSchema = cdmDatabaseSchema,
            tempEmulationSchema = tempEmulationSchema,
            useCodesetTable = TRUE,
            codesetId = conceptSet$uniqueConceptSetId,
            conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
            conceptCountsTable = conceptCountsTable,
            conceptCountsTableIsTemp = conceptCountsTableIsTemp,
            instantiatedCodeSets = "#inst_concept_sets",
            orphanConceptTable = "#orphan_concepts"
          )
          
          if (!is.null(conceptIdTable)) {
            sql <- "INSERT INTO @concept_id_table (concept_id)
                SELECT DISTINCT concept_id
                FROM @orphan_concept_table;"
            DatabaseConnector::renderTranslateExecuteSql(
              connection = connection,
              sql = sql,
              tempEmulationSchema = tempEmulationSchema,
              concept_id_table = conceptIdTable,
              orphan_concept_table = "#orphan_concepts",
              progressBar = FALSE,
              reportOverallTime = FALSE
            )
          }
        }
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
    
    data <- dplyr::bind_rows(data) %>%
      dplyr::distinct() %>%
      dplyr::rename("uniqueConceptSetId" = "codesetId") %>%
      dplyr::inner_join(
        conceptSets %>%
          dplyr::select(
            "uniqueConceptSetId",
            "cohortId",
            "conceptSetId"
          ) %>% dplyr::distinct(),
        by = "uniqueConceptSetId",
        relationship = "many-to-many"
      ) %>%
      dplyr::select(-"uniqueConceptSetId") %>%
      dplyr::select(
        "cohortId",
        "conceptSetId",
        "conceptId",
        "conceptCount",
        "conceptSubjects"
      ) %>%
      dplyr::group_by(
        .data$cohortId,
        .data$conceptSetId,
        .data$conceptId
      ) %>%
      dplyr::summarise(
        conceptCount = max(.data$conceptCount),
        conceptSubjects = max(.data$conceptSubjects)
      ) %>%
      dplyr::ungroup()
    data <- makeDataExportable(
      x = data,
      tableName = "orphan_concept",
      minCellCount = minCellCount,
      databaseId = databaseId
    )
    
    writeToCsv(
      data,
      file.path(exportFolder, "orphan_concept.csv"),
      incremental = incremental,
      cohortId = subsetOrphans$cohortId
    )
    
    recordTasksDone(
      cohortId = subsetOrphans$cohortId,
      task = "runOrphanConcepts",
      checksum = subsetOrphans$checksum,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
    
    delta <- Sys.time() - start
    
    timeExecution(
      exportFolder,
      taskName = "allOrphanConcepts",
      parent = "runConceptSetDiagnostics",
      start = start,
      execTime = delta
    )
    
    ParallelLogger::logInfo(
      "Finding orphan concepts took ",
      signif(delta, 3),
      " ",
      attr(delta, "units")
    )
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
  
  resolvedConceptIds <- makeDataExportable(
    x = resolvedConceptIds,
    tableName = "resolved_concepts",
    minCellCount = minCellCount,
    databaseId = databaseId
  )
  
  writeToCsv(
    resolvedConceptIds,
    file.path(exportFolder, "resolved_concepts.csv"),
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
  
  delta <- Sys.time() - startTime
  ParallelLogger::logInfo(
    "Running concept set diagnostics took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
}

