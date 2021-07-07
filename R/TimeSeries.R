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
#' @export
runCohortTimeSeriesDiagnostics <- function(connectionDetails = NULL,
                                           connection = NULL,
                                           tempEmulationSchema = NULL,
                                           cdmDatabaseSchema,
                                           cohortDatabaseSchema = cdmDatabaseSchema,
                                           cohortTable = "cohort",
                                           timeSeriesMinDate = as.Date('1980-01-01'),
                                           timeSeriesMaxDate = as.Date(Sys.Date()),
                                           cohortIds) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  ## Calendar period----
  ParallelLogger::logTrace("Preparing calendar table for time series computation.")
  # note calendar span is created based on all dates in observation period table, with 1980 cut off/left censor (arbitary choice)
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
    # dplyr::filter(.data$periodBegin >= as.Date('1999-12-25')) %>%
    # dplyr::filter(.data$periodEnd <= clock::date_today("")) %>%
    dplyr::distinct()
  
  ParallelLogger::logTrace("Inserting calendar periods")
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
  ParallelLogger::logTrace("Done inserting calendar periods")
  
  ParallelLogger::logTrace("Beginning time series SQL")
  sql <-
    SqlRender::loadRenderTranslateSql("ComputeTimeSeries.sql",
                                      packageName = "CohortDiagnostics",
                                      dbms = connection@dbms,
                                      cohort_database_schema = cohortDatabaseSchema,
                                      cdm_database_schema = cdmDatabaseSchema,
                                      cohort_table = cohortTable,
                                      tempEmulationSchema = tempEmulationSchema,
                                      cohort_ids = cohortIds)
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql
  )
  timeSeries <- renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT * FROM #time_series;",
    tempEmulationSchema = tempEmulationSchema,
    snakeCaseToCamelCase = TRUE
  )
  
  timeSeries <- timeSeries %>%
    dplyr::select(
      .data$cohortId,
      .data$periodBegin,
      .data$calendarInterval,
      .data$seriesType,
      .data$records,
      .data$subjects,
      .data$personDays,
      .data$recordsStart,
      .data$subjectsStart,
      .data$recordsEnd,
      .data$subjectsEnd
    )
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "IF OBJECT_ID('tempdb..#calendar_periods', 'U') IS NOT NULL DROP TABLE #calendar_periods;",
    progressBar = TRUE,
    tempEmulationSchema = tempEmulationSchema
  )
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "IF OBJECT_ID('tempdb..#time_series', 'U') IS NOT NULL DROP TABLE #time_series;",
    progressBar = TRUE,
    tempEmulationSchema = tempEmulationSchema
  )
  delta <- Sys.time() - start
  ParallelLogger::logInfo("Retrieving Time Series data took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
  return(timeSeries)
}
