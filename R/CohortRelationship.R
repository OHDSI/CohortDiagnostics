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
#' @template TempEmulationSchema
#'
#' @template CohortTable
#'
#' @param targetCohortIds              A vector of one or more Cohort Ids for use as target cohorts.
#' 
#' @param comparatorCohortIds          A vector of one or more Cohort Ids for use as feature/comparator cohorts.
#'
#' @export
runCohortTemporalRelationshipDiagnostics <-
  function(connectionDetails = NULL,
           connection = NULL,
           cohortDatabaseSchema,
           tempEmulationSchema = NULL,
           cohortTable = "cohort",
           targetCohortIds,
           comparatorCohortIds) {
    startTime <- Sys.time()
    
    if (length(targetCohortIds) == 0) {
      return(NULL)
    }
    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
    
    dateRangeSql <- "SELECT DATEDIFF(day, min_date, max_date) days_diff,
                            min_date,
                            max_date
                    FROM 
                    (SELECT min(cohort_start_date) min_date,
	                           max(cohort_end_date) max_date
                     FROM @cohort_database_schema.@cohort_table
                     WHERE cohort_definition_id IN (@target_cohort_ids,@comparator_cohort_ids)) f;"
    
    dateRange <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                            sql = dateRangeSql, 
                                                            cohort_database_schema = cohortDatabaseSchema,
                                                            cohort_table = cohortTable,
                                                            target_cohort_ids = targetCohortIds,
                                                            comparator_cohort_ids = comparatorCohortIds,
                                                            snakeCaseToCamelCase = TRUE)
    
    
    dateRange$daysDiff <- min(421, dateRange$daysDiff)
    
    # every 30 days
    daysDiff30 <- dateRange$daysDiff +
      (30 - (dateRange$daysDiff %% 30))
    
    seqStart30 <- seq(daysDiff30 * -1, daysDiff30, by = 30)
    seqEnd30 <- seqStart30 + 30
    
    # every 180 days
    daysDiff180 <- dateRange$daysDiff +
      (180 - (dateRange$daysDiff %% 180))
    seqStart180 <- seq(daysDiff180 * -1, daysDiff180, by = 180)
    seqEnd180 <- seqStart180 + 180  
    
    # every 365 days
    daysDiff365 <- dateRange$daysDiff +
      (365 - (dateRange$daysDiff %% 365))
    seqStart365 <- seq(daysDiff365 * -1, daysDiff365, by = 365)
    seqEnd365 <- seqStart365 + 365  
    
    seqStart <- c(0, -99999, 0, 1, seqStart30, seqStart180, seqStart365)
    seqEnd <- c(0, 0, 99999, 99999, seqEnd30, seqEnd180, seqEnd365)
    
    timePeriods <- dplyr::tibble(startDay = seqStart,
                                 endDay = seqEnd) %>% 
      dplyr::arrange(.data$startDay, .data$endDay) %>% 
      dplyr::mutate(timeId = dplyr::row_number())
    
    ParallelLogger::logTrace("Inserting time periods")
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = "#time_periods",
      data = timePeriods,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = TRUE,
      progressBar = FALSE
    )
    ParallelLogger::logTrace("Done inserting time periods")
    
    sql <- SqlRender::loadRenderTranslateSql(
      "CohortTemporalRelationship.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      target_cohort_ids = targetCohortIds,
      comparator_cohort_ids = comparatorCohortIds
    )
    DatabaseConnector::executeSql(connection = connection,
                                  sql = sql)
    temporalRelationship <-
      renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT * FROM #cohort_rel_long;",
        snakeCaseToCamelCase = TRUE
      )
    
    dropSql = "IF OBJECT_ID('tempdb..#cohort_rel_long', 'U') IS NOT NULL DROP TABLE #cohort_rel_long;"
    DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                 sql = dropSql,
                                                 progressBar = TRUE)
    
    temporalRelationship <- temporalRelationship %>% 
      dplyr::inner_join(timePeriods, by = 'timeId') %>%
      dplyr::mutate(relationship_type == 'T1') %>% 
      dplyr::select(.data$cohortId, 
                    .data$comparatorCohortId,
                    .data$relationshipType,
                    .data$startDay,
                    .data$endDay,
                    .data$subjects,
                    .data$records,
                    .data$personDays,
                    .data$recordsIncidence,
                    .data$subjectsIncidence,
                    .data$eraIncidence,
                    .data$recordsTerminate,
                    .data$subjectsTerminate) %>% 
      dplyr::arrange(.data$cohortId, 
                     .data$comparatorCohortId,
                     .data$relationshipType,
                     .data$startDay,
                     .data$endDay,
                     .data$subjects,
                     .data$records)
    
    delta <- Sys.time() - startTime
    ParallelLogger::logInfo(paste(
      "Computing cohort temporal relationship took",
      signif(delta, 3),
      attr(delta, "units")
    ))
    return(temporalRelationship)
  }
