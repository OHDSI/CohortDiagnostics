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



#' Given a set of cohorts get relationships between the cohorts.
#'
#' @description
#' Given a set of cohorts, get temporal relationships between the
#' cohort_start_date of the cohorts.
#'
#' @template Connection
#'
#' @template CohortDatabaseSchema
#'
#' @template CdmDatabaseSchema
#'
#' @template TempEmulationSchema
#'
#' @template CohortTable
#'
#' @param targetCohortIds              A vector of one or more Cohort Ids for use as target cohorts.
#'
#' @param comparatorCohortIds          A vector of one or more Cohort Ids for use as feature/comparator cohorts.
#'
#' @param relationshipDays             A dataframe with two columns startDay and endDay representing periods of time to compute relationship
#'
#' @param incremental                 Create only cohort diagnostics that haven't been created before?
#'
#' @param incrementalFolder           If \code{incremental = TRUE}, specify a folder where records are kept
#'                                    of which cohort diagnostics has been executed.
#'
#' @export
runCohortRelationshipDiagnostics <-
  function(connectionDetails = NULL,
           connection = NULL,
           cohortDatabaseSchema,
           cdmDatabaseSchema,
           tempEmulationSchema = NULL,
           cohortTable = "cohort",
           targetCohortIds,
           comparatorCohortIds,
           relationshipDays,
           incremental = FALSE,
           incrementalFolder = NULL) {
    startTime <- Sys.time()
    
    if (any(is.null(relationshipDays),!is.data.frame(relationshipDays))) {
      stop("relationshipDays is not specified")
    }
    if (!all(
      "startDay" %in% colnames(relationshipDays),
      "endDay" %in% colnames(relationshipDays)
    )) {
      difference <- setdiff(x = c("startDay", "endDay"),
                            y = colnames(relationshipDays))
      stop(paste0(
        "Required fields not found in relationship table: ",
        difference
      ))
    }
    
    if (length(targetCohortIds) == 0) {
      warning("No target cohort ids specified")
      return(NULL)
    }
    if (length(comparatorCohortIds) == 0) {
      warning("No comparator cohort ids specified")
      return(NULL)
    }
    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
    
    sqlCount <-
      "SELECT COUNT(*) count FROM @cohort_database_schema.@cohort_table where cohort_definition_id IN (@cohort_ids);"
    targetCohortCount <-
      renderTranslateQuerySql(
        connection = connection,
        sql = sqlCount,
        cohort_database_schema = cohortDatabaseSchema,
        cohort_table = cohortTable,
        cohort_ids = targetCohortIds, 
        snakeCaseToCamelCase = TRUE
      )
    if (targetCohortCount$count == 0) {
      ParallelLogger::logInfo("Please check if target cohorts are instantiated. Exiting cohort relationship.")
      return(NULL)
    }
    comparatorCohortCount <-
      renderTranslateQuerySql(
        connection = connection,
        sql = sqlCount,
        cohort_database_schema = cohortDatabaseSchema,
        cohort_table = cohortTable,
        cohort_ids = comparatorCohortIds, 
        snakeCaseToCamelCase = TRUE
      )
    if (comparatorCohortCount$count == 0) {
      ParallelLogger::logInfo("Please check if comparator cohorts are instantiated. Exiting cohort relationship.")
      return(NULL)
    }
    
    ParallelLogger::logTrace("  - Creating cohort table subsets")
    cohortSubsetSqlTargetDrop <-
      " IF OBJECT_ID('tempdb..#target_subset', 'U') IS NOT NULL
      	DROP TABLE #target_subset;
      "
    
    cohortSubsetSqlTarget <-
      "--HINT DISTRIBUTE_ON_KEY(subject_id)
      SELECT cohort_definition_id,
      	subject_id,
      	min(cohort_start_date) cohort_start_date,
      	min(cohort_end_date) cohort_end_date
      INTO #target_subset
      FROM @cohort_database_schema.@cohort_table
      WHERE cohort_definition_id IN (@cohort_ids)
      GROUP BY cohort_definition_id,
      	subject_id;"
    
    cohortSubsetSqlComparatorDrop <-
      "IF OBJECT_ID('tempdb..#comparator_subset', 'U') IS NOT NULL
      	DROP TABLE #comparator_subset;"
    
    cohortSubsetSqlComparator <-
      "--HINT DISTRIBUTE_ON_KEY(subject_id)
      	SELECT *
      	INTO #comparator_subset
      	FROM @cohort_database_schema.@cohort_table
      	WHERE cohort_definition_id IN (@cohort_ids);

      INSERT INTO #comparator_subset
      SELECT -1 cohort_definition_id,
            person_id subject_id,
            observation_period_start_date cohort_start_date,
            observation_period_end_date cohort_end_date
      FROM @cdm_database_schema.observation_period;"
    
    
    ParallelLogger::logTrace("   - Target subset")
    ParallelLogger::logTrace("    - dropping temporary table")
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = cohortSubsetSqlTargetDrop,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    ParallelLogger::logTrace("    - creating temporary table")
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = cohortSubsetSqlTarget,
      cohort_database_schema = cohortDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohort_table = cohortTable,
      cohort_ids = targetCohortIds,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    
    ParallelLogger::logTrace("   - Comparator subset")
    ParallelLogger::logTrace("    - dropping temporary table")
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = cohortSubsetSqlComparatorDrop,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    ParallelLogger::logTrace("    - creating temporary table")
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = cohortSubsetSqlComparator,
      cohort_database_schema = cohortDatabaseSchema,
      cdm_database_schema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohort_table = cohortTable,
      cohort_ids = comparatorCohortIds,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    
    timePeriods <- relationshipDays %>%
      dplyr::distinct() %>%
      dplyr::arrange(.data$startDay, .data$endDay) %>%
      dplyr::mutate(timeId = dplyr::row_number())
    
    ParallelLogger::logTrace("   - Creating Andromeda object to collect results")
    
    ParallelLogger::logTrace(paste0("   - Working with ", scales::comma(nrow(timePeriods)), " time ids."))
    resultsInAndromeda <- Andromeda::andromeda()
    
    if (incremental) {
      ParallelLogger::logTrace("  - Running in Incremental mode.")
      ParallelLogger::logTrace(
        "    - Looking for results from previous run in incremental mode for time periods that have already executed."
      )
      if (file.exists(file.path(
        incrementalFolder,
        "CreatedDiagnosticsCohortRelationship.csv"
      ))) {
        ParallelLogger::logTrace(
          "    - Found results from previous execution. Subsetting to required time periods before computing cohort relationship diagnostic."
        )
        previousRunResults <-
          readr::read_csv(
            file = file.path(
              incrementalFolder,
              "CreatedDiagnosticsCohortRelationship.csv"
            ),
            col_types = readr::cols(),
            lazy = FALSE,
            guess_max = 1e7
          ) %>%
          dplyr::inner_join(timePeriods,
                            by = c('startDay', 'endDay'))
        requiredColNames <- setdiff(
          x = c(
            "cohortId",
            "comparatorCohortId",
            "startDay",
            "endDay",
            "timeId"
          ),
          y = colnames(previousRunResults)
        )
        if (length(requiredColNames) > 0) {
          ParallelLogger::logWarn(
            " - Attempted to run cohort relationship diagnostics in incremental mode,\n
          by limiting to only the time periods that were not previously\n
          executed. But the diagnostic file does not have the required fields. \n
          Please check CreatedDiagnosticsCohortRelationship.csv in your incremental\n
          folder. Ignoring the file and continuing diagnostic. File may be overwritten."
          )
          unlink(
            file.path(
              incrementalFolder,
              "CreatedDiagnosticsCohortRelationship.csv"
            ),
            force = TRUE
          )
        } else {
          timePeriodsPreviouslyExecuted <- previousRunResults %>%
            dplyr::select("startDay",
                          "endDay",
                          "timeId") %>%
            dplyr::distinct()
          timePeriodsPreviouslyExecuted <-
            timePeriodsPreviouslyExecuted %>%
            dplyr::inner_join(timePeriods,
                              by = c("startDay",
                                     "endDay",
                                     "timeId")) %>%
            dplyr::select(.data$timeId) %>%
            dplyr::distinct()
          
          ParallelLogger::logTrace(
            paste0(
              "    - Found previous execution to have ",
              scales::comma(nrow(timePeriodsPreviouslyExecuted)),
              " records. Removing time_periods corresponding to those executions and reusing previous results."
            )
          )
          timePeriods <- timePeriods %>%
            dplyr::anti_join(timePeriodsPreviouslyExecuted,
                             by = c("timeId"))
          
          ParallelLogger::logTrace(paste0(
            "    - Executing over ",
            scales::comma(nrow(timePeriods)),
            " time_periods."
          ))
          
          resultsInAndromeda$cohortRelationships <-
            previousRunResults %>%
            dplyr::select(-.data$startDay,-.data$endDay)
        }
      } else {
        ParallelLogger::logTrace("    - Not found, running cohort relationship diagnostics for all time periods.")
      }
    }
    for (i in (1:nrow(timePeriods))) {
      ParallelLogger::logTrace(
        paste0(
          "    - Working on ",
          scales::comma(timePeriods[i,]$startDay),
          " to ",
          scales::comma(timePeriods[i,]$endDay),
          " days (",
          scales::comma(i),
          " of ",
          scales::comma(nrow(timePeriods)),
          ")"
        )
      )
      sql <- SqlRender::loadRenderTranslateSql(
        "CohortRelationship.sql",
        packageName = "CohortDiagnostics",
        dbms = connection@dbms,
        time_id = timePeriods[i, ]$timeId,
        start_day_offset = timePeriods[i, ]$startDay,
        end_day_offset = timePeriods[i, ]$endDay
      )
      DatabaseConnector::querySqlToAndromeda(
        connection = connection,
        sql = sql,
        snakeCaseToCamelCase = TRUE,
        andromeda = resultsInAndromeda,
        andromedaTableName = 'temp'
      )
      
      if (!"cohortRelationships" %in% names(resultsInAndromeda)) {
        resultsInAndromeda$cohortRelationships <- resultsInAndromeda$temp
      } else {
        Andromeda::appendToTable(resultsInAndromeda$cohortRelationships,
                                 resultsInAndromeda$temp)
      }
      readr::write_excel_csv(
        x = resultsInAndromeda$cohortRelationships %>%
          dplyr::collect() %>%
          dplyr::inner_join(timePeriods,
                            by = "timeId") %>%
          dplyr::select(-.data$timeId),
        file = file.path(
          incrementalFolder,
          "CreatedDiagnosticsCohortRelationship.csv"
        ),
        na = "",
        append = FALSE,
        delim = ","
      )
    }
    
    resultsInAndromeda$timePeriods <- timePeriods
    resultsInAndromeda$temp <- NULL
    
    resultsInAndromeda$cohortRelationships <-
      resultsInAndromeda$cohortRelationships %>%
      dplyr::inner_join(resultsInAndromeda$timePeriods, by = 'timeId') %>%
      dplyr::select(-.data$timeId) %>% 
      dplyr::arrange(.data$cohortId,
                     .data$comparatorCohortId,
                     .data$startDay,
                     .data$endDay)
    resultsInAndromeda$timePeriods <- NULL
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = cohortSubsetSqlTargetDrop,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = cohortSubsetSqlComparatorDrop,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    unlink(
      x = file.path(
        incrementalFolder,
        "CreatedDiagnosticsCohortRelationship.csv"
      ),
      force = TRUE
    )
    delta <- Sys.time() - startTime
    ParallelLogger::logTrace(paste(
      " - Computing cohort relationship took",
      signif(delta, 3),
      attr(delta, "units")
    ))
    # changing the output from Andromeda object to in R-memory
    # in next version explore making the output an Andromeda object
    data <-
      resultsInAndromeda$cohortRelationships %>% dplyr::collect()
    return(data)
  }





executeCohortRelationshipDiagnostics <- function(connection,
                                                 databaseId,
                                                 exportFolder,
                                                 cohortDatabaseSchema,
                                                 cdmDatabaseSchema,
                                                 tempEmulationSchema,
                                                 cohortTable,
                                                 cohortDefinitionSet,
                                                 temporalCovariateSettings,
                                                 minCellCount,
                                                 recordKeepingFile,
                                                 incremental,
                                                 incrementalFolder) {
  ParallelLogger::logInfo("Computing Cohort Relationship")
  startCohortRelationship <- Sys.time()
  
  subset <- subsetToRequiredCohorts(
    cohorts = cohortDefinitionSet,
    task = "runCohortRelationship",
    incremental = incremental,
    recordKeepingFile = recordKeepingFile
  )
  
  if (nrow(subset) > 0) {
    if (incremental &&
        (nrow(cohortDefinitionSet) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf(
        " - Skipping %s cohort combinations in incremental mode.",
        nrow(cohortDefinitionSet) - nrow(subset)
      ))
    }
    ParallelLogger::logTrace(" - Beginning Cohort Relationship SQL")
    if (all(exists("temporalCovariateSettings"),!is.null(temporalCovariateSettings))) {
      temporalStartDays <- temporalCovariateSettings$temporalStartDays
      temporalEndDays <- temporalCovariateSettings$temporalEndDays
    } else {
      temporalStartDays = c(
        -365,-30,
        0,
        1,
        31,-9999,-365,-180,-30,-9999,-365,-180,-30,-9999,
        seq(
          from = -421,
          to = -31,
          by = 30
        ),
        seq(
          from = 0,
          to = 390,
          by = 30
        ),
        seq(
          from = -5,
          to = 5,
          by = 1
        )
      )
      temporalEndDays = c(
        -31,-1,
        0,
        30,
        365,
        0,
        0,
        0,
        0,-1,-1,-1,-1,
        9999,
        seq(
          from = -391,
          to = -1,
          by = 30
        ),
        seq(
          from = 30,
          to = 420,
          by = 30
        ),
        seq(
          from = -5,
          to = 5,
          by = 1
        )
      )
    }
    
    output <-
      runCohortRelationshipDiagnostics(
        connection = connection,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        cohortTable = cohortTable,
        targetCohortIds = subset$cohortId,
        comparatorCohortIds = cohortDefinitionSet$cohortId,
        relationshipDays = dplyr::tibble(startDay = temporalStartDays,
                                         endDay = temporalEndDays),
        incremental = incremental,
        incrementalFolder = incrementalFolder
      )
    
    data <- makeDataExportable(
      x = output,
      tableName = "cohort_relationships",
      minCellCount = minCellCount,
      databaseId = databaseId
    )
    
    writeToCsv(
      data = data,
      fileName = file.path(exportFolder, "cohort_relationships.csv"),
      incremental = incremental
    )
    
    recordTasksDone(
      cohortId = subset$cohortId,
      task = "runCohortRelationship",
      checksum = subset$checksum,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
  } else {
    ParallelLogger::logInfo("  - Skipping in incremental mode.")
  }
  delta <- Sys.time() - startCohortRelationship
  ParallelLogger::logInfo(" - Computing cohort relationships took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
}