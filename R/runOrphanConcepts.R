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
                                instantiatedCodeSets = "#inst_concept_sets",
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

#' Generate and export potential orphan concepts. 
#' 
#' @description
#' Runs the required code to find orphan codes, codes that should be, but are not included in a particular concept set.
#' 
#' @template Connection
#' @template TempEmulationSchema
#' @template CdmDatabaseSchema
#' @template VocabularyDatabaseSchema
#' @template DatabaseId
#' @template ExportFolder
#' @template MinCellCount
#' @template CohortTable
#' @template Incremental
#' @template CohortDatabaseSchema
#' 
#' @param cohorts  The cohorts for which to find the orphan concepts
#' @param instantiatedCodeSets Table created by runResolvedConcepts, contains all unique conceptSetIds from all cohorts and has the following columns: codesetId conceptId. 
#' @param conceptCountsDatabaseSchema Schema where the concept_counts table is located.
#' @param conceptCountsTable Name of the concept_counts table.
#' @param conceptIdTable Table where the orphan concepts will be written.
#'
#' @return None, it will write the results to a csv file
#' @export
runOrphanConcepts <- function(connection,
                              tempEmulationSchema,
                              cdmDatabaseSchema,
                              vocabularyDatabaseSchema = cdmDatabaseSchema,
                              databaseId,
                              cohorts,
                              exportFolder,
                              minCellCount,
                              conceptCountsDatabaseSchema = NULL,
                              conceptCountsTable = "#concept_counts",
                              instantiatedCodeSets = "#inst_concept_sets",
                              cohortDatabaseSchema,
                              cohortTable,
                              conceptIdTable = NULL,
                              incremental = FALSE,
                              incrementalFolder = exportFolder
                              ) {
  
  errorMessage <- checkmate::makeAssertCollection()
  checkArg(connection, add = errorMessage)
  checkArg(tempEmulationSchema, add = errorMessage)
  checkArg(cdmDatabaseSchema, add = errorMessage)
  checkArg(vocabularyDatabaseSchema, add = errorMessage)
  checkArg(databaseId, add = errorMessage)
  # checkArg(cohorts, add = errorMessage) # no argument check currently available
  checkArg(exportFolder, add = errorMessage)
  checkArg(minCellCount, add = errorMessage)
  # checkArg(conceptCountsDatabaseSchema, add = errorMessage) # no argument check currently available
  # checkArg(conceptCountsTable, add = errorMessage) # no argument check currently available
  # checkArg(instantiatedCodeSets, add = errorMessage) # no argument check currently available
  checkArg(cohortDatabaseSchema, add = errorMessage)
  checkArg(cohortTable, add = errorMessage)
  # checkArg(conceptIdTable, add = errorMessage) # no argument check currently available
  checkArg(incremental, add = errorMessage)
  checkArg(incrementalFolder, add = errorMessage)
  checkmate::reportAssertions(errorMessage)

  recordKeepingFile <- file.path(incrementalFolder, "CreatedDiagnostics.csv")
  
  ParallelLogger::logInfo("Starting concept set diagnostics")
  startTime <- Sys.time()
  subsetOrphans <- dplyr::tibble()
  
  subsetOrphans <- subsetToRequiredCohorts(
      cohorts = cohorts,
      task = "runOrphanConcepts",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    ) %>%
    dplyr::distinct()
  
  # Make sure an empty result file is written to the export folder
  if (nrow(subsetOrphans) == 0) {
    return(NULL)
  }
  
  # We need to get concept sets from all cohorts in case subsetOrphans are present and
  # Added incrementally after cohort generation
  conceptSets <- combineConceptSetsFromCohorts(cohorts)
  conceptSets <- conceptSets %>% dplyr::filter(.data$cohortId %in% subsetOrphans$cohortId)
  
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

  # Defines variables and checks version of external concept counts table -----
  checkConceptCountsTableExists <- DatabaseConnector::dbExistsTable(connection,
                                                                    name = conceptCountsTable,
                                                                    databaseSchema = conceptCountsDatabaseSchema)
  
  conceptCountsTableIsTemp <- (substr(conceptCountsTable, 1, 1) == "#") 
  
  # Create conceptCountsTable if name has # or doesn´t exists ------------
  if (conceptCountsTableIsTemp || !checkConceptCountsTableExists) {
    
    ParallelLogger::logInfo(paste(
      "Creating", ifelse(conceptCountsTableIsTemp, "temp", "") ,"concept counts table"
    ))
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
    
  } else {
    conceptCountsTableIsTemp <- FALSE
    conceptCountsTable <- conceptCountsTable
    dataSourceInfo <- getCdmDataSourceInformation(connection = connection, 
                                                  cdmDatabaseSchema = cdmDatabaseSchema)
    
    # If conceptCountsTable exists, check the vocabVersion
    
    vocabVersion <- dataSourceInfo$vocabularyVersion
    vocabVersionExternalConceptCountsTable <- renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT DISTINCT vocabulary_version FROM @work_database_schema.@concept_counts_table;",
      work_database_schema = cohortDatabaseSchema,
      concept_counts_table = conceptCountsTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = getOption("sqlRenderTempEmulationSchena")
    )
    if (!identical(vocabVersion, vocabVersionExternalConceptCountsTable[1,1])) {
      stop(paste0("External concept counts table (", 
                  vocabVersionExternalConceptCountsTable, 
                  ") does not match database (", 
                  vocabVersion, 
                  "). Update concept_counts with createConceptCountsTable()"))
    }
  }
  
  ParallelLogger::logInfo("Finding orphan concepts")
  if (incremental && (nrow(cohorts) - nrow(subsetOrphans)) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Skipping %s cohorts in incremental mode.",
      nrow(cohorts) - nrow(subsetOrphans)
    ))
  }
  if (nrow(subsetOrphans > 0)) {
    start <- Sys.time()
    
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
            instantiatedCodeSets = instantiatedCodeSets,
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
    
    exportDataToCsv(
      data = data,
      tableName = "orphan_concept",,
      fileName = file.path(exportFolder, "orphan_concept.csv"),
      minCellCount = minCellCount,
      databaseId = databaseId,
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
  
  exportDataToCsv(
    data = resolvedConceptIds,
    tableName = "resolved_concepts",
    fileName = file.path(exportFolder, "resolved_concepts.csv"),
    minCellCount = minCellCount,
    databaseId = databaseId,
    incremental = TRUE
  )
  
  # TODO eliminate inst_concept_sets after three analysis complete in executeDiagnostics
  # if (tempTableExists(connection, "inst_concept_sets")) {
  #   ParallelLogger::logTrace("Dropping Unique ConceptSets table")
  #   sql <-
  #     "TRUNCATE TABLE #inst_concept_sets; DROP TABLE #inst_concept_sets;"
  #   DatabaseConnector::renderTranslateExecuteSql(
  #     connection,
  #     sql,
  #     tempEmulationSchema = tempEmulationSchema,
  #     progressBar = FALSE,
  #     reportOverallTime = FALSE
  #   )
  # }
  
  if (conceptCountsTableIsTemp) {
    sql <- "TRUNCATE TABLE @count_table; DROP TABLE @count_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      tempEmulationSchema = tempEmulationSchema,
      count_table = conceptCountsTable,
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

