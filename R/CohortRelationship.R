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
runCohortRelationshipDiagnostics <-
  function(connectionDetails = NULL,
           connection = NULL,
           cohortDatabaseSchema,
           tempEmulationSchema = NULL,
           cohortTable = "cohort",
           targetCohortIds,
           comparatorCohortIds) {
    startTime <- Sys.time()
    
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
    
    
    sqlCount <- "SELECT COUNT(*) FROM @cohort_database_schema.@cohort_table where cohort_definition_id IN (@cohort_ids);"
    targetCohortCount <- CohortDiagnostics:::renderTranslateQuerySql(connection = connection,
                                                                     sql = sqlCount,
                                                                     cohort_database_schema = cohortDatabaseSchema,
                                                                     cohort_table = cohortTable,
                                                                     cohort_ids = targetCohortIds)
    if (targetCohortCount$COUNT == 0) {
      warning("Please check if target cohorts are instantiated. Exiting cohort relationship.")
    }
    comparatorCohortCount <- CohortDiagnostics:::renderTranslateQuerySql(connection = connection,
                                                                         sql = sqlCount,
                                                                         cohort_database_schema = cohortDatabaseSchema,
                                                                         cohort_table = cohortTable,
                                                                         cohort_ids = comparatorCohortIds)
    if (comparatorCohortCount$COUNT == 0) {
      warning("Please check if target cohorts are instantiated. Exiting cohort relationship.")
    }
    
    ParallelLogger::logTrace(" - Creating cohort table subsets")
    cohortSubsetSql <- "IF OBJECT_ID('tempdb..@subset_cohort_table', 'U') IS NOT NULL
	                      DROP TABLE @subset_cohort_table;
	                      
	                      --HINT DISTRIBUTE_ON_KEY(subject_id)
                        SELECT *
                        INTO @subset_cohort_table
                        FROM @cohort_database_schema.@cohort_table
                        WHERE cohort_definition_id IN (@cohort_ids);"
    
    DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                 sql = cohortSubsetSql, 
                                                 cohort_database_schema = cohortDatabaseSchema,
                                                 cohort_table = cohortTable,
                                                 subset_cohort_table = '#target_subset',
                                                 cohort_ids = targetCohortIds, 
                                                 progressBar = FALSE, 
                                                 reportOverallTime = FALSE)
    
    DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                 sql = cohortSubsetSql, 
                                                 cohort_database_schema = cohortDatabaseSchema,
                                                 cohort_table = cohortTable,
                                                 subset_cohort_table = '#comparator_subset',
                                                 cohort_ids = comparatorCohortIds, 
                                                 progressBar = FALSE, 
                                                 reportOverallTime = FALSE)
    
    ParallelLogger::logTrace(" - Computing date range in target cohorts")
    dateRangeSql <- "SELECT DATEDIFF(day, min_date, max_date) days_diff,
                            min_date,
                            max_date
                    FROM 
                    (SELECT min(cohort_start_date) min_date,
	                           max(cohort_end_date) max_date
                     FROM #target_subset) f;"
    dateRange <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                            sql = dateRangeSql,
                                                            snakeCaseToCamelCase = TRUE)
    if (is.na(dateRange$daysDiff)) {
      warning("Please check if the cohorts are instantiated. Exiting Cohort relationship.")
    }
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
    
    # custom sequence 1 - for temporal characterization
    seqStartCustom1 <- c(-365,-30, 0,1,31)
    seqEndCustom1 <- c(-31, -1, 0, 30, 365)
    
    # custom sequence 2 - all time prior to day before index (not including index date)
    seqStartCustom2 <- c(-99999)
    seqEndCustom2 <- c(-1)
    
    # custom sequence 3 - all time prior to index (including index date)
    seqStartCustom3 <- c(-99999)
    seqEndCustom3 <- c(0)
    
    # custom sequence 4 - index date to all time into future
    seqStartCustom4 <- c(0)
    seqEndCustom4 <- c(99999)
    
    # custom sequence 5 - day 1 to all time into future
    seqStartCustom5 <- c(1)
    seqEndCustom5 <- c(99999)
    
    # custom sequence 6 - all time prior to all time future
    seqStartCustom6 <- c(-99999)
    seqEndCustom6 <- c(99999)
    
    seqStart <- c(seqStartCustom1, seqStartCustom2, seqStartCustom3, seqStartCustom4, 
                  seqStartCustom5, seqStartCustom6, seqStart30, seqStart180, seqStart365)
    seqEnd <- c(seqEndCustom1, seqEndCustom2, seqEndCustom3, seqEndCustom4, 
                seqEndCustom5, seqEndCustom6, seqEnd30, seqEnd180, seqEnd365)
    
    timePeriods <- dplyr::tibble(startDay = seqStart,
                                 endDay = seqEnd) %>% 
      dplyr::distinct() %>% 
      dplyr::arrange(.data$startDay, .data$endDay) %>% 
      dplyr::mutate(timeId = dplyr::row_number())
    
    
    ParallelLogger::logTrace(" --- Creating Andromeda object to collect results")
    resultsInAndromeda <- Andromeda::andromeda()
    pb <- utils::txtProgressBar(style = 3)
    
    for (i in (1:nrow(timePeriods))) {
      ParallelLogger::logTrace(paste0(" ---- Working on Time id:",
                                      timePeriods[i,]$timeId,
                                      " start day: ",
                                      timePeriods[i,]$startDay,
                                      " to end day:",
                                      timePeriods[i,]$endDay))
      sql <- SqlRender::loadRenderTranslateSql(
        "CohortRelationship.sql",
        packageName = "CohortDiagnostics",
        dbms = connection@dbms,
        time_id = timePeriods[i,]$timeId,
        start_day_offset = timePeriods[i,]$startDay,
        end_day_offset = timePeriods[i,]$endDay
      )
      DatabaseConnector::querySqlToAndromeda(connection = connection,
                                             sql = sql, 
                                             snakeCaseToCamelCase = TRUE,
                                             andromeda = resultsInAndromeda, 
                                             andromedaTableName = 'temp')
      
      if (!"cohortRelationship" %in% names(resultsInAndromeda)) {
        resultsInAndromeda$cohortRelationship <- resultsInAndromeda$temp
      } else {
        Andromeda::appendToTable(
          resultsInAndromeda$cohortRelationship,
          resultsInAndromeda$temp
        )
      }
      utils::setTxtProgressBar(pb, i/nrow(timePeriods))
    }
    
    results <- resultsInAndromeda$cohortRelationship %>% 
      dplyr::collect() %>% 
      dplyr::inner_join(timePeriods, by = 'timeId') %>%
      dplyr::select(.data$cohortId, 
                    .data$comparatorCohortId,
                    .data$startDay,
                    .data$endDay,
                    .data$bothSubjects,
                    .data$cBeforeTSubjects,
                    .data$tBeforeCSubjects,
                    .data$sameDaySubjects,
                    .data$cPersonDays,
                    .data$cSubjectsStart,
                    .data$cSubjectsEnd,
                    .data$cInTSubjects) %>% 
      dplyr::arrange(.data$cohortId, 
                     .data$comparatorCohortId,
                     .data$startDay,
                     .data$endDay,
                     .data$bothSubjects)
    
    delta <- Sys.time() - startTime
    ParallelLogger::logInfo(paste(
      "Computing cohort temporal relationship took",
      signif(delta, 3),
      attr(delta, "units")
    ))
    return(results)
  }
