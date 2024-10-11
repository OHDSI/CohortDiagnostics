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

getVisitContext <- function(connection = NULL,
                            cdmDatabaseSchema,
                            tempEmulationSchema = NULL,
                            cohortDatabaseSchema = cdmDatabaseSchema,
                            cohortTable = "cohort",
                            cohortIds,
                            conceptIdTable = NULL,
                            cdmVersion = 5) {
  if (!cdmVersion == 5) {
    warning("Only OMOP CDM v5.x.x is supported. Continuing execution.")
  }

  start <- Sys.time()

  if (is.null(connection)) {
    stop("Connection cannot be null.")
  }

  sql <- SqlRender::loadRenderTranslateSql(
    "VisitContext.sql",
    packageName = utils::packageName(),
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    visit_context_table = "#visit_context",
    cdm_database_schema = cdmDatabaseSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds
  )
  
  DatabaseConnector::executeSql(connection, sql)
  sql <- "SELECT * FROM @visit_context_table;"
  visitContext <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      visit_context_table = "#visit_context",
      snakeCaseToCamelCase = TRUE
    )

  if (!is.null(conceptIdTable)) {
    
    createTablesql <- "IF OBJECT_ID('@unique_concept_id_table', 'U') IS NULL CREATE TABLE @unique_concept_id_table (concept_id BIGINT);"
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = createTablesql,
      tempEmulationSchema = tempEmulationSchema,
      unique_concept_id_table = conceptIdTable,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    
    sql <- "INSERT INTO @unique_concept_id_table (concept_id)
            SELECT DISTINCT visit_concept_id
            FROM @visit_context_table
            LEFT JOIN @unique_concept_id_table ON @unique_concept_id_table.concept_id = @visit_context_table.visit_concept_id
            WHERE @unique_concept_id_table.concept_id is NULL;"
            
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      unique_concept_id_table = conceptIdTable,
      visit_context_table = "#visit_context",
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  sql <-
    "TRUNCATE TABLE @visit_context_table;\nDROP TABLE @visit_context_table;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    visit_context_table = "#visit_context",
    progressBar = FALSE,
    reportOverallTime = FALSE
  )

  delta <- Sys.time() - start
  ParallelLogger::logInfo(
    "Retrieving visit context took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
  return(visitContext)
}

runVisitContext <- function(connection,
                            cohortDefinitionSet,
                            exportFolder,
                            databaseId,
                            cohortDatabaseSchema,
                            cdmDatabaseSchema,
                            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                            cohortTable  = "cohort",
                            cdmVersion = 5,
                            minCellCount,
                            incremental,
                            incrementalFolder = file.path(exportFolder, "incremental")){

  
  if (incremental && !file.exists(incrementalFolder)) {
    # Create the file if it doesn't exist
    file.create(incrementalFolder)
    ParallelLogger::logInfo(
      sprintf(
        "Created record keeping file %s.",
        incrementalFolder
      )
    )
  } 
  
  cohortCounts <- CohortGenerator::getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohortDefinitionSet$cohortId,
    cohortDefinitionSet = cohortDefinitionSet,
    databaseId = databaseId
  )
  
  if (nrow(cohortCounts) > 0) {
    instantiatedCohorts <- cohortCounts %>%
      dplyr::filter(.data$cohortEntries > 0) %>%
      dplyr::pull(.data$cohortId)
    ParallelLogger::logInfo(
      sprintf(
        "Found %s of %s (%1.2f%%) submitted cohorts instantiated. ",
        length(instantiatedCohorts),
        nrow(cohortDefinitionSet),
        100 * (length(instantiatedCohorts) / nrow(cohortDefinitionSet))
      ),
      "Beginning cohort diagnostics for instantiated cohorts. "
    )
  } else {
    stop("All cohorts were either not instantiated or all have 0 records.")
  }
  
  ParallelLogger::logInfo("Retrieving visit context for index dates")
  subset <- subsetToRequiredCohorts(
    cohorts = cohortDefinitionSet %>%
      dplyr::filter(.data$cohortId %in% instantiatedCohorts),
    task = "runVisitContext",
    incremental = incremental,
    recordKeepingFile = incrementalFolder
  )
  
  if (incremental &&
    (length(instantiatedCohorts) - nrow(subset)) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Skipping %s cohorts in incremental mode.",
      length(instantiatedCohorts) - nrow(subset)
    ))
  }
  
  if (nrow(subset) > 0) {
    data <- getVisitContext(
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cdmVersion = cdmVersion,
      cohortIds = subset$cohortId,
      conceptIdTable = "#concept_ids"
    )
  } else {
    getResultsDataModelSpecifications("visit_context")
    data <- dplyr::tibble(
      cohortId = integer(),
      visitConceptId = integer(),
      visitContext = character(),
      subjects = double()
    )
  }
  
  exportDataToCsv(
    data = data,
    tableName = "visit_context",
    fileName = file.path(exportFolder, "visit_context.csv"),
    minCellCount = minCellCount,
    databaseId = databaseId,
    incremental = incremental,
    cohortId = subset$cohortId
  )
    
  recordTasksDone(
    cohortId = subset$cohortId,
    task = "runVisitContext",
    checksum = subset$checksum,
    recordKeepingFile = incrementalFolder,
    incremental = incremental
  )
}
