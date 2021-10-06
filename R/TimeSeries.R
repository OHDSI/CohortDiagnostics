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


#' Given a set of instantiated cohorts get time series for the cohorts.
#'
#' @description
#' This function first generates a calendar period table, that has
#' calendar intervals between the \code{timeSeriesMinDate} and \code{timeSeriesMaxDate}.
#' Calendar Month, Quarter and year are supported.
#' For each of the calendar interval, time series data are computed. The returned
#' object is a R dataframe that will need to be converted to a time series object
#' to perform time series analysis.
#'
#' Data Source time series: computes time series at the data source level i.e. observation
#' period table. This output is NOT limited to individuals in the cohort table
#' but is for ALL people in the datasource (i.e. present in observation period table)
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
#' @param timeSeriesMinDate      (optional) Minimum date for time series. Default value January 1st 1980.
#'
#' @param timeSeriesMaxDate      (optional) Maximum date for time series. Default value System date.
#'
#' @param cohortIds              A vector of one or more Cohort Ids to compute time distribution for.
#'
#' @param runCohortTimeSeries         Generate and export the cohort level time series?
#'
#' @param runDataSourceTimeSeries     Generate and export the Data source level time series? i.e.
#'                                    using all persons found in observation period table.
#'
#' @export
runCohortTimeSeriesDiagnostics <- function(connectionDetails = NULL,
                                           connection = NULL,
                                           tempEmulationSchema = NULL,
                                           cdmDatabaseSchema,
                                           cohortDatabaseSchema = cdmDatabaseSchema,
                                           cohortTable = "cohort",
                                           runCohortTimeSeries = TRUE,
                                           runDataSourceTimeSeries = TRUE,
                                           timeSeriesMinDate = as.Date('1980-01-01'),
                                           timeSeriesMaxDate = as.Date(Sys.Date()),
                                           cohortIds = NULL) {
  if (all(!runCohortTimeSeries, !runDataSourceTimeSeries)) {
    warning(
      ' - Both Cohort Time Series and Data Source Time Series are set to FALSE. Exiting time series diagnostics.'
    )
    return(NULL)
  }
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  ParallelLogger::logTrace(" - Creating Andromeda object to collect results")
  resultsInAndromeda <- Andromeda::andromeda()
  
  sqlCount <-
    "SELECT cohort_definition_id, COUNT(*) count FROM @cohort_database_schema.@cohort_table
               {@cohort_ids != ''} ? { where cohort_definition_id IN (@cohort_ids)}
               GROUP BY cohort_definition_id;"
  resultsInAndromeda$cohortCount <- renderTranslateQuerySql(
    connection = connection,
    sql = sqlCount,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_ids = cohortIds,
    cohort_table = cohortTable
  )
  if (resultsInAndromeda$cohortCount %>% dplyr::summarise(n = dplyr::n()) %>% dplyr::pull(.data$n) == 0) {
    warning("Please check if cohorts are instantiated. Exiting cohort time series.")
    return(NULL)
  }
  
  ## Calendar period----
  ParallelLogger::logTrace(" - Preparing calendar table for time series computation.")
  # note calendar span is created based on all dates in observation period table,
  # with 1980 cut off/left censor (arbitrary choice)
  minYear <-
    (max(clock::get_year(timeSeriesMinDate),
         1980) %>% as.integer())
  maxYear <-
    clock::get_year(timeSeriesMaxDate) %>% as.integer()
  
  calendarQuarter <-
    dplyr::tibble(
      periodBegin = clock::date_seq(
        from = clock::date_build(year = minYear),
        to = clock::date_build(year = maxYear + 1),
        by = clock::duration_months(3)
      )
    ) %>%
    dplyr::mutate(periodEnd = clock::add_months(x = .data$periodBegin, n = 3) - 1) %>%
    dplyr::mutate(calendarInterval = 'q')
  
  calendarMonth <-
    dplyr::tibble(
      periodBegin = clock::date_seq(
        from = clock::date_build(year = minYear),
        to = clock::date_build(year = maxYear + 1),
        by = clock::duration_months(1)
      )
    ) %>%
    dplyr::mutate(periodEnd = clock::add_months(x = .data$periodBegin, n = 1) - 1) %>%
    dplyr::mutate(calendarInterval = 'm')
  
  calendarYear <-
    dplyr::tibble(
      periodBegin = clock::date_seq(
        from = clock::date_build(year = minYear),
        to = clock::date_build(year = maxYear + 1 + 1),
        by = clock::duration_years(1)
      )
    ) %>%
    dplyr::mutate(periodEnd = clock::add_years(x = .data$periodBegin, n = 1) - 1) %>%
    dplyr::mutate(calendarInterval = 'y')
  
  # calendarWeek <- dplyr::tibble(periodBegin = clock::date_seq(from = (clock::year_month_weekday(year = cohortDateRange$minYear %>% as.integer(),
  #                                                                                               month = clock::clock_months$january,
  #                                                                                               day = clock::clock_weekdays$monday,
  #                                                                                               index = 1) %>%
  #                                                                       clock::as_date() %>%
  #                                                                       clock::add_weeks(n = -1)),
  #                                                             to = (clock::year_month_weekday(year = (cohortDateRange$maxYear  %>% as.integer()) + 1,
  #                                                                                             month = clock::clock_months$january,
  #                                                                                             day = clock::clock_weekdays$sunday,
  #                                                                                             index = 1) %>%
  #                                                                     clock::as_date()),
  #                                                             by = clock::duration_weeks(n = 1))) %>%
  #   dplyr::mutate(periodEnd = clock::add_days(x = .data$periodBegin, n = 6))
  
  calendarPeriods <-
    dplyr::bind_rows(calendarMonth, calendarQuarter, calendarYear) %>%  #calendarWeek
    dplyr::distinct() %>%
    dplyr::arrange(.data$periodBegin, .data$periodEnd, .data$calendarInterval) %>%
    dplyr::mutate(timeId = dplyr::row_number())
  
  ParallelLogger::logTrace(" - Inserting calendar periods")
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "#calendar_periods",
    data = calendarPeriods,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    progressBar = FALSE,
    tempTable = TRUE,
    tempEmulationSchema = tempEmulationSchema,
    camelCaseToSnakeCase = TRUE
  )
  
  tsSetUpSql <- "-- #time_series
                IF OBJECT_ID('tempdb..#time_series', 'U') IS NOT NULL
                	DROP TABLE #time_series;

                IF OBJECT_ID('tempdb..#c_time_series1', 'U') IS NOT NULL
                	DROP TABLE #c_time_series1;

                IF OBJECT_ID('tempdb..#c_time_series2', 'U') IS NOT NULL
                	DROP TABLE #c_time_series2;

                IF OBJECT_ID('tempdb..#d_time_series3', 'U') IS NOT NULL
                	DROP TABLE #d_time_series3;
  "
                # IF OBJECT_ID('tempdb..#c_time_series4', 'U') IS NOT NULL
                # 	DROP TABLE #c_time_series4;
                # 
                # IF OBJECT_ID('tempdb..#c_time_series5', 'U') IS NOT NULL
                # 	DROP TABLE #c_time_series5;
                # 
                # IF OBJECT_ID('tempdb..#d_time_series6', 'U') IS NOT NULL
                # 	DROP TABLE #d_time_series6;

  ParallelLogger::logTrace(" - Dropping any time_series temporary tables that maybe present at start up.")
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = tsSetUpSql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  seriesToRun <- NULL
  if (runCohortTimeSeries) {
    seriesToRun <- c(
      seriesToRun,
      'ComputeTimeSeries1.sql',
      'ComputeTimeSeries2.sql'
    )
  }
  # ,
  # 'ComputeTimeSeries4.sql',
  # 'ComputeTimeSeries5.sql'
  if (runDataSourceTimeSeries) {
    seriesToRun <- c(seriesToRun,
                     'ComputeTimeSeries3.sql')
    # ,
    # 'ComputeTimeSeries6.sql'
  }
  seriesToRun <- seriesToRun %>% sort()
  ParallelLogger::logTrace(" - Beginning time series SQL")
  
  sqlCohortDrop <- "IF OBJECT_ID('tempdb..#cohort_ts', 'U') IS NOT NULL
      	DROP TABLE #cohort_ts;"
  
  sqlCohort <- "--HINT DISTRIBUTE_ON_KEY(subject_id)
      WITH cohort
      AS (
      	SELECT *
      	FROM @cohort_database_schema.@cohort_table
      	WHERE cohort_definition_id IN (@cohort_ids)
      	),
      cohort_first
      AS (
      	SELECT cohort_definition_id,
      		subject_id,
      		min(cohort_start_date) cohort_start_date,
      		min(cohort_end_date) cohort_end_date
      	FROM cohort
      	GROUP BY cohort_definition_id,
      		subject_id
      	)
      SELECT c.*,
      	CASE 
      		WHEN c.cohort_start_date = cf.cohort_start_date
      			THEN 'Y'
      		ELSE 'N'
      		END first_occurrence
      INTO #cohort_ts
      FROM cohort c
      INNER JOIN cohort_first cf ON c.cohort_definition_id = cf.cohort_definition_id
      	AND c.subject_id = cf.subject_id;"
  
  ParallelLogger::logTrace("   - Dropping any time series temporary tables")
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlCohortDrop,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  ParallelLogger::logTrace("   - Creating cohort table copy for time series")
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlCohort,
    cohort_database_schema = cohortDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  for (i in (1:length(seriesToRun))) {
    ParallelLogger::logTrace(paste0(" - Running ", seriesToRun[[i]]))
    if (seriesToRun[[i]] == 'ComputeTimeSeries1.sql') {
      ParallelLogger::logInfo(
        paste0(
          "  - (",
          scales::percent(i / length(seriesToRun)),
          ") Running cohort time series T1: subjects in the cohort who have atleast one cohort day in calendar period."
        )
      )
    }
    if (seriesToRun[[i]] == 'ComputeTimeSeries2.sql') {
      ParallelLogger::logInfo(
        paste0(
          "  - (",
          scales::percent(i / length(seriesToRun)),
          ") Running cohort time series T2: subjects in the cohort who have atleast one observation day in calendar period."
        )
      )
    }
    if (seriesToRun[[i]] == 'ComputeTimeSeries3.sql') {
      ParallelLogger::logInfo(
        paste0(
          "  - (",
          scales::percent(i / length(seriesToRun)),
          ") Running database time series T3: persons in the data source who have atleast one observation day in calendar period."
        )
      )
    }
    # commenting out time series 4/5/6 as it does not seem have value
    # if (seriesToRun[[i]] == 'ComputeTimeSeries4.sql') {
    #   ParallelLogger::logInfo(
    #     paste0(
    #       "  - (",
    #       scales::percent(i / length(seriesToRun)),
    #       ") Running cohort time series T4: subjects in the cohorts whose cohort period are embedded within calendar period."
    #     )
    #   )
    # }
    # if (seriesToRun[[i]] == 'ComputeTimeSeries5.sql') {
    #   ParallelLogger::logInfo(
    #     paste0(
    #       "  - (",
    #       scales::percent(i / length(seriesToRun)),
    #       ") Running cohort time series T5: subjects in the cohorts whose observation period is embedded within calendar period."
    #     )
    #   )
    # }
    # if (seriesToRun[[i]] == 'ComputeTimeSeries6.sql') {
    #   ParallelLogger::logInfo(
    #     paste0(
    #       "  - (",
    #       scales::percent(i / length(seriesToRun)),
    #       ") Running datasource time series T6: persons in the observation table whose observation period is embedded within calendar period."
    #     )
    #   )
    # }
    
    sql <- SqlRender::readSql(system.file("sql/sql_server",
                                          seriesToRun[[i]],
                                          package = 'CohortDiagnostics'))
    
    seriesId <-  stringr::str_replace(string = seriesToRun[[i]],
                                      pattern = 'ComputeTimeSeries',
                                      replacement = '') %>%
      stringr::str_replace(pattern = '.sql',
                           replacement = '')
    seriesId <- paste0('T', as.character(seriesId))
    
    sql <- SqlRender::render(
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      warnOnMissingParameters = FALSE
    )
    # ,
    # 'ComputeTimeSeries5.sql',
    # 'ComputeTimeSeries6.sql'
    if (seriesToRun[[i]] %in% c(
      'ComputeTimeSeries2.sql',
      'ComputeTimeSeries3.sql'
    )) {
      sql <- SqlRender::render(
        sql = sql,
        cdm_database_schema = cdmDatabaseSchema,
        warnOnMissingParameters = FALSE
      )
    }
    # ,
    # 'ComputeTimeSeries4.sql',
    # 'ComputeTimeSeries5.sql',
    # 'ComputeTimeSeries6.sql'
    if (seriesToRun[[i]] %in% c(
      'ComputeTimeSeries1.sql',
      'ComputeTimeSeries2.sql'
    ))  {
      sql <- SqlRender::render(
        sql = sql,
        cohort_database_schema = cohortDatabaseSchema,
        warnOnMissingParameters = FALSE
      )
    }
    sql <- SqlRender::translate(sql = sql,
                                targetDialect = connection@dbms)
    
    DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      snakeCaseToCamelCase = TRUE,
      andromeda = resultsInAndromeda,
      andromedaTableName = 'temp'
    )
    
    resultsInAndromeda$temp <- resultsInAndromeda$temp %>%
      dplyr::mutate(seriesType = !!seriesId)
    
    if (!"timeSeries" %in% names(resultsInAndromeda)) {
      resultsInAndromeda$timeSeries <- resultsInAndromeda$temp
    } else {
      Andromeda::appendToTable(resultsInAndromeda$timeSeries,
                               resultsInAndromeda$temp)
    }
    ParallelLogger::logTrace('     Completed.')
  }
  resultsInAndromeda$calendarPeriods <- calendarPeriods
  resultsInAndromeda$timeSeries <- resultsInAndromeda$timeSeries %>%
    dplyr::collect() %>% #temporal solution till fix of bug in andromeda on handling dates
    # periodBegin gets converted to integer
    dplyr::inner_join(resultsInAndromeda$calendarPeriods %>% dplyr::collect(), by = c('timeId')) %>%
    dplyr::arrange(
      .data$cohortId,
      .data$periodBegin,
      .data$calendarInterval,
      .data$seriesType,
      .data$periodBegin
    )
  resultsInAndromeda$calendarPeriods <- NULL
  resultsInAndromeda$temp <- NULL
  resultsInAndromeda$cohortCount <- NULL
  ParallelLogger::logTrace(" - Dropping any time_series temporary tables at clean up")
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "IF OBJECT_ID('tempdb..#calendar_periods', 'U') IS NOT NULL DROP TABLE #calendar_periods;",
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  ParallelLogger::logTrace(" - Dropping any time_series temporary tables that maybe present at clean up.")
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = tsSetUpSql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  ParallelLogger::logTrace("   - Dropping any time series temporary tables at clean up")
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlCohortDrop,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  delta <- Sys.time() - start
  ParallelLogger::logTrace(" - Retrieving Time Series data took ",
                           signif(delta, 3),
                           " ",
                           attr(delta, "units"))
  return(resultsInAndromeda)
}
