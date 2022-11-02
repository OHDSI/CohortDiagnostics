# Copyright 2022 Observational Health Data Sciences and Informatics
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
#' @param stratifyByGender       Do you want to stratify by Gender
#'
#' @param stratifyByAgeGroup     Do you want to stratify by Age group
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
                                           runDataSourceTimeSeries = FALSE,
                                           timeSeriesMinDate = as.Date("1980-01-01"),
                                           timeSeriesMaxDate = as.Date(Sys.Date()),
                                           stratifyByGender = TRUE,
                                           stratifyByAgeGroup = TRUE,
                                           cohortIds = NULL) {
  if (all(!runCohortTimeSeries, !runDataSourceTimeSeries)) {
    warning(
      " - Both Cohort Time Series and Data Source Time Series are set to FALSE. Exiting time series diagnostics."
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

  if (runCohortTimeSeries) {
    sqlCount <-
      " SELECT cohort_definition_id, COUNT(*) count
      FROM @cohort_database_schema.@cohort_table
      {@cohort_ids != ''} ? { where cohort_definition_id IN (@cohort_ids)}
      GROUP BY cohort_definition_id;"
    resultsInAndromeda$cohortCount <- renderTranslateQuerySql(
      connection = connection,
      sql = sqlCount,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_ids = cohortIds,
      cohort_table = cohortTable
    )
    if (resultsInAndromeda$cohortCount %>%
      dplyr::summarise(n = dplyr::n()) %>%
      dplyr::pull(.data$n) == 0) {
      warning("Please check if cohorts are instantiated. Exiting cohort time series.")
      return(NULL)
    }
  }

  ## Calendar period----
  ParallelLogger::logTrace(" - Preparing calendar table for time series computation.")
  # note calendar span is created based on all dates in observation period table,
  # with 1980 cut off/left censor (arbitrary choice)
  minYear <-
    (max(
      clock::get_year(timeSeriesMinDate),
      1980
    ) %>% as.integer())
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
      dplyr::mutate(calendarInterval = "q")

  calendarMonth <-
    dplyr::tibble(
      periodBegin = clock::date_seq(
        from = clock::date_build(year = minYear),
        to = clock::date_build(year = maxYear + 1),
        by = clock::duration_months(1)
      )
    ) %>%
      dplyr::mutate(periodEnd = clock::add_months(x = .data$periodBegin, n = 1) - 1) %>%
      dplyr::mutate(calendarInterval = "m")

  calendarYear <-
    dplyr::tibble(
      periodBegin = clock::date_seq(
        from = clock::date_build(year = minYear),
        to = clock::date_build(year = maxYear + 1 + 1),
        by = clock::duration_years(1)
      )
    ) %>%
      dplyr::mutate(periodEnd = clock::add_years(x = .data$periodBegin, n = 1) - 1) %>%
      dplyr::mutate(calendarInterval = "y")

  timeSeriesDateRange <- dplyr::tibble(
    periodBegin = timeSeriesMinDate,
    periodEnd = timeSeriesMaxDate,
    calendarInterval = "c"
  )

  calendarPeriods <-
    dplyr::bind_rows(
      calendarMonth,
      calendarQuarter,
      calendarYear,
      timeSeriesDateRange
    ) %>% # calendarWeek
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
                DROP TABLE IF EXISTS #time_series;
                DROP TABLE IF EXISTS #c_time_series1;
                DROP TABLE IF EXISTS #c_time_series2;
                DROP TABLE IF EXISTS #c_time_series3;"

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
      "ComputeTimeSeries1.sql",
      "ComputeTimeSeries2.sql"
    )
  }

  if (runDataSourceTimeSeries) {
    seriesToRun <- c(
      seriesToRun,
      "ComputeTimeSeries3.sql"
    )
  }
  seriesToRun <- seriesToRun %>% sort()
  ParallelLogger::logTrace(" - Beginning time series SQL")

  sqlCohortDrop <-
    "DROP TABLE IF EXISTS #cohort_ts;"
  ParallelLogger::logTrace("   - Dropping any cohort temporary tables used by time series")
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sqlCohortDrop,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  sqlCohort <- "DROP TABLE IF EXISTS #ts_cohort;
                DROP TABLE IF EXISTS #ts_cohort_first;
                DROP TABLE IF EXISTS #ts_output;

                --HINT DISTRIBUTE_ON_KEY(subject_id)
                SELECT *
                INTO #ts_cohort
                FROM @cohort_database_schema.@cohort_table {@cohort_ids != '' } ? {
                WHERE cohort_definition_id IN (@cohort_ids) };

                --HINT DISTRIBUTE_ON_KEY(subject_id)
                SELECT cohort_definition_id,
                	subject_id,
                	min(cohort_start_date) cohort_start_date,
                	min(cohort_end_date) cohort_end_date
                INTO #ts_cohort_first
                FROM #ts_cohort
                GROUP BY cohort_definition_id,
                	subject_id;


                SELECT c.*,
                	CASE
                		WHEN c.cohort_start_date = cf.cohort_start_date
                			THEN 'Y'
                		ELSE 'N'
                		END first_occurrence {@stratify_by_gender} ? {,
                	concept.concept_name gender} {@stratify_by_age_group} ? {,
                	p.year_of_birth}
                INTO #cohort_ts
                FROM #ts_cohort c
                INNER JOIN #ts_cohort_first cf
                	ON c.cohort_definition_id = cf.cohort_definition_id
                		AND c.subject_id = cf.subject_id {@stratify_by_gender | @stratify_by_age_group} ? {
                INNER JOIN @cdm_database_schema.person p
                	ON c.subject_id = p.person_id} {@stratify_by_gender} ? {
                INNER JOIN @cdm_database_schema.concept
                	ON p.gender_concept_id = concept.concept_id};

                DROP TABLE IF EXISTS #ts_cohort;
                DROP TABLE IF EXISTS #ts_cohort_first;"

  if (runCohortTimeSeries) {
    ParallelLogger::logTrace("   - Creating cohort table copy for time series")
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sqlCohort,
      cohort_database_schema = cohortDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohort_table = cohortTable,
      cohort_ids = cohortIds,
      stratify_by_gender = stratifyByGender,
      stratify_by_age_group = stratifyByAgeGroup,
      cdm_database_schema = cdmDatabaseSchema,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }

  for (i in (1:length(seriesToRun))) {
    ParallelLogger::logTrace(paste0(" - Running ", seriesToRun[[i]]))
    if (seriesToRun[[i]] == "ComputeTimeSeries1.sql") {
      ParallelLogger::logInfo(
        paste0(
          "  - (",
          scales::percent(i / length(seriesToRun)),
          ") Running cohort time series T1: subjects in the cohort who have atleast one cohort day in calendar period."
        )
      )
    }
    if (seriesToRun[[i]] == "ComputeTimeSeries2.sql") {
      ParallelLogger::logInfo(
        paste0(
          "  - (",
          scales::percent(i / length(seriesToRun)),
          ") Running cohort time series T2: subjects in the cohort who have atleast one observation day in calendar period."
        )
      )
    }
    if (seriesToRun[[i]] == "ComputeTimeSeries3.sql") {
      ParallelLogger::logInfo(
        paste0(
          "  - (",
          scales::percent(i / length(seriesToRun)),
          ") Running database time series T3: persons in the data source who have atleast one observation day in calendar period."
        )
      )
    }
    seriesId <- stringr::str_replace(
      string = seriesToRun[[i]],
      pattern = "ComputeTimeSeries",
      replacement = ""
    ) %>%
      stringr::str_replace(
        pattern = ".sql",
        replacement = ""
      )
    seriesId <- paste0("T", as.character(seriesId))

    sqlAll <- SqlRender::loadRenderTranslateSql(
      sqlFilename = seriesToRun[[i]],
      packageName = utils::packageName(),
      stratify_by_gender = FALSE,
      stratify_by_age_group = FALSE,
      dbms = connection@dbms
    )
    sqlGender <- SqlRender::loadRenderTranslateSql(
      sqlFilename = seriesToRun[[i]],
      packageName = utils::packageName(),
      stratify_by_gender = TRUE,
      stratify_by_age_group = FALSE,
      dbms = connection@dbms
    )
    sqlAgeGroup <- SqlRender::loadRenderTranslateSql(
      sqlFilename = seriesToRun[[i]],
      packageName = utils::packageName(),
      stratify_by_gender = FALSE,
      stratify_by_age_group = TRUE,
      dbms = connection@dbms
    )
    sqlAgeGroupGender <- SqlRender::loadRenderTranslateSql(
      sqlFilename = seriesToRun[[i]],
      packageName = utils::packageName(),
      stratify_by_gender = TRUE,
      stratify_by_age_group = TRUE,
      dbms = connection@dbms
    )

    if (seriesToRun[[i]] %in% c(
      "ComputeTimeSeries2.sql",
      "ComputeTimeSeries3.sql"
    )) {
      sqlAll <- SqlRender::render(
        sql = sqlAll,
        cdm_database_schema = cdmDatabaseSchema,
        warnOnMissingParameters = FALSE
      )
      sqlGender <- SqlRender::render(
        sql = sqlGender,
        cdm_database_schema = cdmDatabaseSchema,
        warnOnMissingParameters = FALSE
      )
      sqlAgeGroup <- SqlRender::render(
        sql = sqlAgeGroup,
        cdm_database_schema = cdmDatabaseSchema,
        warnOnMissingParameters = FALSE
      )
      sqlAgeGroupGender <- SqlRender::render(
        sql = sqlAgeGroupGender,
        cdm_database_schema = cdmDatabaseSchema,
        warnOnMissingParameters = FALSE
      )
    }

    DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sqlAll,
      snakeCaseToCamelCase = TRUE,
      andromeda = resultsInAndromeda,
      andromedaTableName = "allData"
    )
    resultsInAndromeda$allData <- resultsInAndromeda$allData %>%
      dplyr::mutate(seriesType = !!seriesId)
    ParallelLogger::logInfo("       Time series without stratification - completed.")

    if (stratifyByGender) {
      DatabaseConnector::querySqlToAndromeda(
        connection = connection,
        sql = sqlGender,
        snakeCaseToCamelCase = TRUE,
        andromeda = resultsInAndromeda,
        andromedaTableName = "gender"
      )
      resultsInAndromeda$gender <- resultsInAndromeda$gender %>%
        dplyr::mutate(seriesType = !!seriesId)
      Andromeda::appendToTable(
        resultsInAndromeda$allData,
        resultsInAndromeda$gender
      )
    }
    ParallelLogger::logInfo("       Time series stratified by gender - completed.")

    if (stratifyByAgeGroup) {
      DatabaseConnector::querySqlToAndromeda(
        connection = connection,
        sql = sqlAgeGroup,
        snakeCaseToCamelCase = TRUE,
        andromeda = resultsInAndromeda,
        andromedaTableName = "ageGroup"
      )
      resultsInAndromeda$ageGroup <- resultsInAndromeda$ageGroup %>%
        dplyr::mutate(seriesType = !!seriesId)
      Andromeda::appendToTable(
        resultsInAndromeda$allData,
        resultsInAndromeda$ageGroup
      )
    }
    ParallelLogger::logInfo("       Time series stratified by age group - completed.")

    if (stratifyByGender && stratifyByAgeGroup) {
      DatabaseConnector::querySqlToAndromeda(
        connection = connection,
        sql = sqlAgeGroupGender,
        snakeCaseToCamelCase = TRUE,
        andromeda = resultsInAndromeda,
        andromedaTableName = "ageGroupGender"
      )
      resultsInAndromeda$ageGroupGender <-
        resultsInAndromeda$ageGroupGender %>%
          dplyr::mutate(seriesType = !!seriesId)
      Andromeda::appendToTable(
        resultsInAndromeda$allData,
        resultsInAndromeda$ageGroupGender
      )
    }
    ParallelLogger::logInfo("       Time series stratified by age group and gender - completed.")

    if (!"timeSeries" %in% names(resultsInAndromeda)) {
      resultsInAndromeda$timeSeries <-
        resultsInAndromeda$allData
    } else {
      Andromeda::appendToTable(
        resultsInAndromeda$timeSeries,
        resultsInAndromeda$allData
      )
    }
    ParallelLogger::logTrace("     Completed.")
  }
  resultsInAndromeda$calendarPeriods <- calendarPeriods
  resultsInAndromeda$timeSeries <- resultsInAndromeda$timeSeries %>%
    dplyr::collect() %>% # temporal solution till fix of bug in andromeda on handling dates
    # periodBegin gets converted to integer
    dplyr::inner_join(resultsInAndromeda$calendarPeriods %>% dplyr::collect(),
                      by = c("timeId")
    ) %>%
    dplyr::arrange(
      .data$cohortId,
      .data$periodBegin,
      .data$calendarInterval,
      .data$seriesType,
      .data$periodBegin
    ) %>%
    dplyr::select(-.data$timeId) %>%
    dplyr::mutate(ageGroup = dplyr::if_else(
      condition = is.na(.data$ageGroup),
      true = as.character(.data$ageGroup),
      false = paste(10 * .data$ageGroup, 10 * .data$ageGroup + 9, sep = "-")
    ))

  resultsInAndromeda$calendarPeriods <- NULL
  resultsInAndromeda$temp <- NULL
  resultsInAndromeda$cohortCount <- NULL
  ParallelLogger::logTrace(" - Dropping any time_series temporary tables at clean up")
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "DROP TABLE IF EXISTS #calendar_periods;",
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
  ParallelLogger::logTrace(
    " - Retrieving Time Series data took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
  return(resultsInAndromeda$timeSeries %>% dplyr::collect())
}


executeTimeSeriesDiagnostics <- function(connection,
                                         tempEmulationSchema,
                                         cdmDatabaseSchema,
                                         cohortDatabaseSchema,
                                         cohortTable,
                                         cohortDefinitionSet,
                                         runCohortTimeSeries = TRUE,
                                         runDataSourceTimeSeries = FALSE,
                                         databaseId,
                                         exportFolder,
                                         minCellCount,
                                         instantiatedCohorts,
                                         incremental,
                                         recordKeepingFile,
                                         observationPeriodDateRange,
                                         batchSize = getOption("CohortDiagnostics-TimeSeries-batch-size", default = 20)) {

  if (all(!runCohortTimeSeries, !runDataSourceTimeSeries)) {
    warning(
      "Both Datasource time series and cohort time series are set to FALSE. Skippping executeTimeSeriesDiagnostics."
    )
  }

  if (runCohortTimeSeries & nrow(cohortDefinitionSet) > 0) {
    subset <- subsetToRequiredCohorts(
      cohorts = cohortDefinitionSet %>%
        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
      task = "runCohortTimeSeries",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    ) %>%
      dplyr::arrange(.data$cohortId)

    if (nrow(subset) > 0) {
      if (incremental &&
        (length(instantiatedCohorts) - nrow(subset)) > 0) {
        ParallelLogger::logInfo(sprintf(
          " - Skipping %s cohorts in incremental mode.",
          length(instantiatedCohorts) - nrow(subset)
        ))
      }

      outputFile <- file.path(exportFolder, "time_series.csv")
      if (!incremental & file.exists(outputFile)) {
        ParallelLogger::logInfo("Time series file exists, removing before batch operations")
        unlink(outputFile)
      }

      for (start in seq(1, nrow(subset), by = batchSize)) {
        end <- min(start + batchSize - 1, nrow(subset))

        if (nrow(subset) > batchSize) {
          ParallelLogger::logInfo(
            sprintf(
              "  - Batch cohort time series. Processing cohorts %s through %s combinations of %s total combinations",
              start,
              end,
              nrow(subset)
            )
          )
        }

        cohortIds <- subset[start:end,]$cohortId %>% unique()
        timeExecution(
          exportFolder,
          "runCohortTimeSeriesDiagnostics",
          cohortIds,
          parent = "executeTimeSeriesDiagnostics",
          expr = {
            data <-
              runCohortTimeSeriesDiagnostics(
                connection = connection,
                tempEmulationSchema = tempEmulationSchema,
                cohortDatabaseSchema = cohortDatabaseSchema,
                cdmDatabaseSchema = cdmDatabaseSchema,
                cohortTable = cohortTable,
                runCohortTimeSeries = runCohortTimeSeries,
                runDataSourceTimeSeries = FALSE,
                timeSeriesMinDate = observationPeriodDateRange$observationPeriodMinDate,
                timeSeriesMaxDate = observationPeriodDateRange$observationPeriodMaxDate,
                cohortIds = cohortIds
              )
          }
        )
        data <- makeDataExportable(
          x = data,
          tableName = "time_series",
          minCellCount = minCellCount,
          databaseId = databaseId
        )
        writeToCsv(
          data = data,
          fileName = outputFile,
          incremental = TRUE,
          cohortId = subset[start:end,]$cohortId %>% unique()
        )
        recordTasksDone(
          cohortId = subset[start:end,]$cohortId %>% unique(),
          task = "runCohortTimeSeries",
          checksum = subset[start:end,]$checksum,
          recordKeepingFile = recordKeepingFile,
          incremental = incremental
        )
      }
    }
  }

  # separating out data source time series
  if (runDataSourceTimeSeries) {
    subset <- subsetToRequiredCohorts(
      cohorts = dplyr::tibble(
        cohortId = -44819062, # cohort id is identified by an omop concept id https://athena.ohdsi.org/search-terms/terms/44819062
        checksum = computeChecksum(column = "data source time series")
      ),
      task = "runDataSourceTimeSeries",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )

    if (all(nrow(subset) == 0,
            incremental)) {
      ParallelLogger::logInfo("Skipping Data Source Time Series in Incremental mode.")
      return(NULL)
    }

    timeExecution(
      exportFolder,
      "runCohortTimeSeriesDiagnostics",
      -44819062,
      parent = "executeTimeSeriesDiagnostics",
      expr = {
        data <-
          runCohortTimeSeriesDiagnostics(
            connection = connection,
            tempEmulationSchema = tempEmulationSchema,
            cdmDatabaseSchema = cdmDatabaseSchema,
            runCohortTimeSeries = FALSE,
            runDataSourceTimeSeries = runDataSourceTimeSeries,
            timeSeriesMinDate = observationPeriodDateRange$observationPeriodMinDate,
            timeSeriesMaxDate = observationPeriodDateRange$observationPeriodMaxDate
          )
      }
    )
    data <- makeDataExportable(
      x = data,
      tableName = "time_series",
      minCellCount = minCellCount,
      databaseId = databaseId
    )
    writeToCsv(
      data = data,
      fileName = file.path(exportFolder, "time_series.csv"),
      incremental = incremental,
      cohortId = -44819062
    )
    recordTasksDone(
      cohortId = -44819062,
      task = "runDataSourceTimeSeries",
      checksum = subset$checksum,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
  }
}
