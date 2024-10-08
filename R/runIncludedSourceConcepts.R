
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
#' @param conceptCountsTableIsTemp 
#' @param cohortDatabaseSchema 
#' @param cohortTable 
#' @param useExternalConceptCountsTable 
#' @param incremental 
#' @param conceptIdTable 
#' @param recordKeepingFile 
#' @param resultsDatabaseSchema 
#'
#' @return
#' @export
#'
#' @examples
runIncludedSourceConcepts <- function(connection,
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

  subset <- dplyr::distinct(subset)
  
  if (nrow(subset) == 0) {
    # TODO write/append an empty result
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
    parent = "runIncludedSourceConcepts",
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
    parent = "runIncludedSourceConcepts",
    expr = {
      createConceptCountsTable(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
        conceptCountsTable = conceptCountsTable,
        conceptCountsTableIsTemp = conceptCountsTableIsTemp,
        useAchilles = useAchilles,
        resultsDatabaseSchema = resultsDatabaseSchema
      )
    }
  )

  timeExecution(
    exportFolder,
    taskName = "runIncludedSourceConcepts",
    cohortIds = NULL,
    parent = "runIncludedSourceConcepts",
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