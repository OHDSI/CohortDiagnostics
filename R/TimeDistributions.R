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

getTimeDistributions <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 tempEmulationSchema = NULL,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortIds,
                                 cdmVersion = 5) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  covariateSettings <-
    FeatureExtraction::createCovariateSettings(
      useDemographicsPriorObservationTime = TRUE,
      useDemographicsPostObservationTime = TRUE,
      useDemographicsTimeInCohort = TRUE
    )
  
  data <-
    FeatureExtraction::getDbCovariateData(
      connection = connection,
      oracleTempSchema = tempEmulationSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortId = cohortIds,
      covariateSettings = covariateSettings,
      cdmVersion = cdmVersion,
      aggregated = TRUE
    )
  
  if (is.null(data$covariatesContinuous)) {
    result <- tidyr::tibble()
  } else {
    result <- data$covariatesContinuous %>%
      dplyr::inner_join(data$covariateRef, by = "covariateId") %>%
      dplyr::select(
        -.data$conceptId,
        -.data$analysisId,
        -.data$covariateId
      ) %>%
      dplyr::rename(timeMetric = .data$covariateName,
                    cohortId = .data$cohortDefinitionId) %>%
      dplyr::collect()
  }
  attr(result, "cohortSize") <- data$metaData$populationSize
  delta <- Sys.time() - start
  ParallelLogger::logInfo("Computing time distributions took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
  return(result)
}

executeTimeDistributionDiagnostics <- function(connection,
                                               tempEmulationSchema,
                                               cdmDatabaseSchema,
                                               cohortDatabaseSchema,
                                               cohortTable,
                                               cdmVersion,
                                               databaseId,
                                               exportFolder,
                                               cohorts,
                                               instantiatedCohorts,
                                               incremental,
                                               recordKeepingFile) {
  ParallelLogger::logInfo("Creating time distributions")
    subset <- subsetToRequiredCohorts(
      cohorts = cohorts %>%
        dplyr::filter(.data$cohortId %in% instantiatedCohorts),
      task = "runTimeDistributions",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )

    if (incremental &&
        (length(instantiatedCohorts) - nrow(subset)) > 0) {
      ParallelLogger::logInfo(sprintf(
        "Skipping %s cohorts in incremental mode.",
        length(instantiatedCohorts) - nrow(subset)
      ))
    }
    if (nrow(subset) > 0) {
      data <- getTimeDistributions(
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cdmVersion = cdmVersion,
        cohortIds = subset$cohortId
      )
      if (nrow(data) > 0) {
        data <- data %>%
          dplyr::mutate(databaseId = !!databaseId)
        writeToCsv(
          data = data,
          fileName = file.path(exportFolder, "time_distribution.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      }
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runTimeDistributions",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    }
}

executeTimeSeriesDiagnostics <- function(connection,
                                         cohortDatabaseSchema,
                                         tempEmulationSchema,
                                         cohortTable,
                                         cohorts,
                                         exportFolder,
                                         incremental,
                                         recordKeepingFile,
                                         instantiatedCohorts,
                                         databaseId,
                                         minCellCount) {
  ParallelLogger::logInfo("Calculating time series of subjects and records.")
  startPrevalenceRate <- Sys.time()

  subset <- subsetToRequiredCohorts(
    cohorts = cohorts %>%
      dplyr::filter(.data$cohortId %in% instantiatedCohorts),
    task = "runTimeSeries",
    incremental = incremental,
    recordKeepingFile = recordKeepingFile
  )

  if (incremental &&
    (length(instantiatedCohorts) - nrow(subset)) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Skipping %s cohorts in incremental mode.",
      length(instantiatedCohorts) - nrow(subset)
    ))
  }
  if (nrow(subset) > 0) {
    cohortDateRange <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT MIN(year(cohort_start_date)) MIN_YEAR,
             MAX(year(cohort_end_date)) MAX_YEAR
             FROM @cohort_database_schema.@cohort_table;",
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema
    )

    calendarQuarter <- dplyr::tibble(periodBegin = clock::date_seq(from = clock::date_build(year = max(2000,
                                                                                                       cohortDateRange$minYear %>% as.integer())),
                                                                   to = clock::date_build(year = min(clock::get_year(clock::date_today("")),
                                                                                                     (cohortDateRange$maxYear %>% as.integer()))
                                                                     + 1),
                                                                   by = clock::duration_months(3))) %>%
      dplyr::mutate(periodEnd = clock::add_months(x = .data$periodBegin, n = 3) - 1) %>%
      dplyr::mutate(calendarInterval = 'q')

    calendarMonth <- dplyr::tibble(periodBegin = clock::date_seq(from = clock::date_build(year = max(2000,
                                                                                                     cohortDateRange$minYear %>% as.integer())),
                                                                 to = clock::date_build(year = min(clock::get_year(clock::date_today("")),
                                                                                                   (cohortDateRange$maxYear %>% as.integer()))
                                                                   + 1),
                                                                 by = clock::duration_months(1))) %>%
      dplyr::mutate(periodEnd = clock::add_months(x = .data$periodBegin, n = 1) - 1) %>%
      dplyr::mutate(calendarInterval = 'm')

    calendarYear <- dplyr::tibble(periodBegin = clock::date_seq(from = clock::date_build(year = cohortDateRange$minYear %>% as.integer()),
                                                                to = clock::date_build(year = (cohortDateRange$maxYear %>% as.integer()) + 1),
                                                                by = clock::duration_years(1))) %>%
      dplyr::mutate(periodEnd = clock::add_years(x = .data$periodBegin, n = 1) - 1) %>%
      dplyr::mutate(calendarInterval = 'y')


    calendarPeriods <- dplyr::bind_rows(calendarMonth, calendarQuarter, calendarYear) %>%  #calendarWeek
      dplyr::filter(.data$periodBegin >= as.Date('1999-12-25')) %>%
      dplyr::filter(.data$periodEnd <= clock::date_today("")) %>%
      dplyr::distinct()

    ParallelLogger::logTrace("Inserting calendar periods into temporay table. This might take time.")
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = "#calendar_periods",
      data = calendarPeriods,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      progressBar = TRUE,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = TRUE
    )
    ParallelLogger::logTrace("Done inserting calendar periods")

    sql <-
      SqlRender::loadRenderTranslateSql(
        "ComputeTimeSeries.sql",
        packageName = utils::packageName(),
        dbms = connection@dbms
      )
    
    data <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      cohort_ids = subset$cohortId
    ) 
    
    data <- data %>%
      dplyr::tibble() %>%
      dplyr::mutate(databaseId = !!databaseId) %>%
      dplyr::select(.data$cohortId, .data$databaseId,
                    .data$periodBegin, .data$calendarInterval,
                    .data$records, .data$subjects,
                    .data$personDays, .data$recordsIncidence,
                    .data$subjectsIncidence)

    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "IF OBJECT_ID('tempdb..#calendar_periods', 'U') IS NOT NULL DROP TABLE #calendar_periods;",
      progressBar = TRUE,
      tempEmulationSchema = tempEmulationSchema
    )
    data <-
      enforceMinCellValue(data, "records", minCellCount)
    data <-
      enforceMinCellValue(data, "subjects", minCellCount)
    data <-
      enforceMinCellValue(data, "personDays", minCellCount)
    data <-
      enforceMinCellValue(data, "recordsIncidence", minCellCount)
    data <-
      enforceMinCellValue(data, "subjectsIncidence", minCellCount)

    if (nrow(data) > 0) {
      writeToCsv(
        data = data,
        fileName = file.path(exportFolder, "time_series.csv"),
        incremental = incremental,
        cohortId = subset$cohortId
      )
    }

    recordTasksDone(
      cohortId = subset$cohortId,
      task = "runTimeSeries",
      checksum = subset$checksum,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
    delta <- Sys.time() - startPrevalenceRate
    ParallelLogger::logInfo("Running Prevalence Rate took ",
                            signif(delta, 3),
                            " ",
                            attr(delta, "units"))
  }
}
