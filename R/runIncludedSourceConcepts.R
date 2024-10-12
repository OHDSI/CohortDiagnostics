

getIncludedSourceConcepts <- function(connection,
                                      cohortDefinitionSet,
                                      cdmDatabaseSchema,
                                      tempEmulationSchema) {
  
  if (!tempTableExists(connection, "inst_concept_sets")) {
    stop("Execute the function runResolvedConceptSets() first.")
  }
  
  if (nrow(cohortDefinitionSet) == 0) {
    return(
      dplyr::tibble(
        cohortId = double(), 
        conceptSetId = double(),
        conceptId = double(),
        sourceConceptId = double(),
        conceptCount = double(),
        conceptSubjects = double()
      )
    )
  }
  
  conceptSets <- combineConceptSetsFromCohorts(cohortDefinitionSet)
  
  sql <- SqlRender::loadRenderTranslateSql(
    "includedSourceConcepts.sql",
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
      sql = "SELECT * FROM #inc_src_concepts",
      tempEmulationSchema = tempEmulationSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble()
    
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
    dplyr::relocate(
      "cohortId",
      "conceptSetId",
      "conceptId"
    ) %>%
    dplyr::distinct() %>%
    dplyr::group_by(
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
    
    addConceptIdsToConceptTempTable(
      connection = connection,
      copyFromTempTable = "#inc_src_concepts",
      conceptIdFieldName = "concept_id",
      tempEmulationSchema = tempEmulationSchema
    )
    
    addConceptIdsToConceptTempTable(
      connection = connection,
      copyFromTempTable = "#inc_src_concepts",
      conceptIdFieldName = "source_concept_id",
      tempEmulationSchema = tempEmulationSchema
    )
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "TRUNCATE TABLE #inc_src_concepts; DROP TABLE #inc_src_concepts;",
      tempEmulationSchema = tempEmulationSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    
  return(counts)
}


#' Title
#'
#' @param connection 
#' @template cohortDefinitionSet
#' @param tempEmulationSchema 
#' @param cdmDatabaseSchema 
#' @param vocabularyDatabaseSchema 
#' @param databaseId 
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
                                     cohortDefinitionSet,
                                     tempEmulationSchema,
                                     cdmDatabaseSchema,
                                     databaseId,
                                     exportFolder,
                                     minCellCount,
                                     incremental = FALSE,
                                     recordKeepingFile) {
  
  ParallelLogger::logInfo("Starting concept set diagnostics")
  start <- Sys.time()
  subset <- dplyr::tibble()
  
  ParallelLogger::logInfo("Fetching included source concepts")
  
  subset <- subsetToRequiredCohorts(
    cohorts = cohortDefinitionSet,
    task = "runIncludedSourceConcepts",
    incremental = incremental,
    recordKeepingFile = recordKeepingFile
  )
  
  # TODO: Disregard empty cohorts in tally:
  if (incremental && (nrow(cohortDefinitionSet) - nrow(subsetIncluded)) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Skipping %s cohorts in incremental mode.",
      nrow(cohortDefinitionSet) - nrow(subsetIncluded)
    ))
  }
  
  if (nrow(subset) == 0) {
    # TODO write/append an empty result
    return(NULL)
  }
  
  # We need to get concept sets from all cohorts in case subsets are present and
  # Added incrementally after cohort generation
  conceptSets <- combineConceptSetsFromCohorts(cohortDefinitionSet)
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
    taskName = "runIncludedSourceConcepts",
    cohortIds = NULL,
    parent = "executeDiagnostics",
    expr = {
      data <- getIncludedSourceConcepts(
        connection,
        cohortDefinitionSet,
        cdmDatabaseSchema,
        tempEmulationSchema
      )
    }
  )
  
  exportDataToCsv(
    data = data,
    tableName = "included_source_concept",
    fileName = file.path(exportFolder, "included_source_concept.csv"),
    minCellCount = minCellCount,
    databaseId = databaseId,
    incremental = incremental,
    cohortId = subset$cohortId
  )
  
  recordTasksDone(
    cohortId = subset$cohortId,
    task = "runIncludedSourceConcepts",
    checksum = subset$checksum,
    recordKeepingFile = recordKeepingFile,
    incremental = incremental
  )
  
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste(
    "Finding source codes took",
    signif(delta, 3),
    attr(delta, "units")
  ))
}