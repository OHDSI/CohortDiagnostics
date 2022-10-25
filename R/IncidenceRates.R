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

#' Get Incidence Rate for Cohort Diagnostics
#' 
#' @description generates incidence rates for a single cohort 
#' 
#' @template ConnectionDetails
#'
#' @template CdmDatabaseSchema
#' 
#' @template VocabularyDatabaseSchema
#' 
#' @template TempEmulationSchema
#' 
#' @template CohortTable
#' 
#' @template cdmVersion
#' 
#' @param firstOccurrenceOnly a indicator stating whether the incidence rate should be based on the first occurrence
#'
#' @param washoutPeriod the period of time in days where the subject is not exposed to the medical event. 
#' The default value is 365 days
#' 
#' @param cohortId a cohort id to identify the cohort definition to use to get incidence rate
#' 
#' @export
getIncidenceRate <- function(connectionDetails = NULL,
                             connection = NULL,
                             cdmDatabaseSchema,
                             vocabularyDatabaseSchema = cdmDatabaseSchema,
                             tempEmulationSchema = tempEmulationSchema,
                             cohortDatabaseSchema,
                             cohortTable,
                             cdmVersion = 5,
                             firstOccurrenceOnly = TRUE,
                             washoutPeriod = 365,
                             cohortId) {
  start <- Sys.time()
  if (!cdmVersion == 5) {
    stop("Only CDM version 5 is supported. Terminating.")
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (!checkIfCohortInstantiated(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = cohortId
  )) {
    warning(
      "Cohort with ID ",
      cohortId,
      " appears to be empty. Was it instantiated? Skipping incidence rate computation."
    )
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste(
      "Computing incidence rates took",
      signif(delta, 3),
      attr(delta, "units")
    ))
    return(tidyr::tibble())
  }

  ParallelLogger::logInfo("Calculating incidence rate per year by age and gender")
  sql <-
    SqlRender::loadRenderTranslateSql(
      sqlFilename = "GetCalendarYearRange.sql",
      packageName = utils::packageName(),
      dbms = connection@dbms,
      cdm_database_schema = cdmDatabaseSchema
    )
  yearRange <-
    DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)

  calendarYears <-
    dplyr::tibble(calendarYear = as.integer(seq(yearRange$startYear, yearRange$endYear, by = 1)))
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "#calendar_years",
    data = calendarYears,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE,
    tempEmulationSchema = tempEmulationSchema,
    camelCaseToSnakeCase = TRUE
  )

  sql <-
    SqlRender::loadRenderTranslateSql(
      sqlFilename = "ComputeIncidenceRates.sql",
      packageName = utils::packageName(),
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      cdm_database_schema = cdmDatabaseSchema,
      vocabulary_database_schema = vocabularyDatabaseSchema,
      first_occurrence_only = firstOccurrenceOnly,
      washout_period = washoutPeriod,
      cohort_id = cohortId
    )
  DatabaseConnector::executeSql(connection, sql)

  sql <- "SELECT * FROM #rates_summary;"
  ratesSummary <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
    tidyr::tibble()

  sql <- "TRUNCATE TABLE #rates_summary; DROP TABLE #rates_summary;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    tempEmulationSchema = tempEmulationSchema
  )

  irYearAgeGender <- recode(ratesSummary)
  irOverall <-
    tidyr::tibble(
      cohortCount = sum(irYearAgeGender$cohortCount),
      personYears = sum(irYearAgeGender$personYears)
    )
  irGender <-
    aggregateIr(irYearAgeGender, list(gender = irYearAgeGender$gender))
  irAge <-
    aggregateIr(irYearAgeGender, list(ageGroup = irYearAgeGender$ageGroup))
  irAgeGender <-
    aggregateIr(
      irYearAgeGender,
      list(
        ageGroup = irYearAgeGender$ageGroup,
        gender = irYearAgeGender$gender
      )
    )
  irYear <-
    aggregateIr(
      irYearAgeGender,
      list(calendarYear = irYearAgeGender$calendarYear)
    )
  irYearAge <-
    aggregateIr(
      irYearAgeGender,
      list(
        calendarYear = irYearAgeGender$calendarYear,
        ageGroup = irYearAgeGender$ageGroup
      )
    )
  irYearGender <-
    aggregateIr(
      irYearAgeGender,
      list(
        calendarYear = irYearAgeGender$calendarYear,
        gender = irYearAgeGender$gender
      )
    )
  result <- dplyr::bind_rows(
    irOverall,
    irGender,
    irAge,
    irAgeGender,
    irYear,
    irYearAge,
    irYearGender,
    irYearAgeGender
  )
  result$incidenceRate <-
    1000 * result$cohortCount / result$personYears
  result$incidenceRate[is.nan(result$incidenceRate)] <- 0
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste(
    "Computing incidence rates took",
    signif(delta, 3),
    attr(delta, "units")
  ))
  return(result)
}

recode <- function(ratesSummary) {
  ratesSummary$ageGroup <-
    paste(10 * ratesSummary$ageGroup, 10 * ratesSummary$ageGroup + 9, sep = "-")
  ratesSummary$gender <- tolower(ratesSummary$gender)
  substr(ratesSummary$gender, 1, 1) <-
    toupper(substr(ratesSummary$gender, 1, 1))
  return(tidyr::tibble(ratesSummary))
}

aggregateIr <- function(ratesSummary, aggregateList) {
  if (nrow(ratesSummary) > 0) {
    return(aggregate(
      cbind(
        cohortCount = ratesSummary$cohortCount,
        personYears = ratesSummary$personYears
      ),
      by = aggregateList,
      FUN = sum
    ))
  } else {
    return(tidyr::tibble())
  }
}



#' Batch Incidence Rates
#' 
#' @description Batch generation of incidence rate for multiple cohorts in Cohort Diagnostics
#' 
#' @template Connection
#'
#' @template CdmDatabaseSchema
#' 
#' @template TempEmulationSchema
#' 
#' @template CohortTable
#' 
#' @template CohortDefinitionSet
#' 
#' @template DataExport
#' 
#' @template BatchOptions
#' 
#' @export
batchIncidenceRates <- function(connection,
                                cdmDatabaseSchema,
                                tempEmulationSchema,
                                cohortDatabaseSchema,
                                cohortTable,
                                cohortDefinitionSet,
                                databaseId,
                                exportFolder,
                                minCellCount,
                                instantiatedCohorts,
                                recordKeepingFile,
                                incremental) {
  ParallelLogger::logInfo("Computing incidence rates")
  startIncidenceRate <- Sys.time()
  subset <- subsetToRequiredCohorts(
    cohorts = cohortDefinitionSet %>% #replace cohorts to cohort definition set consistency
      dplyr::filter(.data$cohortId %in% instantiatedCohorts),
    task = "runIncidenceRate",
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
    runIncidenceRate <- function(row) {
      ParallelLogger::logInfo(
        "  Computing incidence rate for cohort '",
        row$cohortName,
        "'"
      )

      # TODO: do we really want to get this from the cohort definition?
      cohortExpression <- RJSONIO::fromJSON(row$json, digits = 23)
      washoutPeriod <- cohortExpression$PrimaryCriteria$ObservationWindow[["PriorDays"]]
      if (is.null(washoutPeriod)) {
        washoutPeriod <- 0
      }

      timeExecution(
        exportFolder,
        taskName = "getIncidenceRate",
        parent = "computeIncidenceRates",
        cohortIds = row$cohortId,
        expr = {
          data <- getIncidenceRate(
            connection = connection,
            cdmDatabaseSchema = cdmDatabaseSchema,
            tempEmulationSchema = tempEmulationSchema,
            cohortDatabaseSchema = cohortDatabaseSchema,
            cohortTable = cohortTable,
            cohortId = row$cohortId,
            firstOccurrenceOnly = TRUE,
            washoutPeriod = washoutPeriod
          )
        }
      )
      if (nrow(data) > 0) {
        data <- data %>% dplyr::mutate(cohortId = row$cohortId)
      }
      return(data)
    }

    data <-
      lapply(split(subset, subset$cohortId), runIncidenceRate)
    data <- dplyr::bind_rows(data)
    data <- makeDataExportable(
      x = data,
      tableName = "incidence_rate",
      minCellCount = minCellCount,
      databaseId = databaseId
    )

    # incidenceRate field is a calculated field that does not follow the same pattern as others for minCellValue
    if (nrow(data) > 0) {
      data <-
        enforceMinCellValue(
          data,
          "incidenceRate",
          1000 * minCellCount / data$personYears
        )
    }

    writeToCsv(
      data = data,
      fileName = file.path(exportFolder, "incidence_rate.csv"),
      incremental = incremental,
      cohortId = subset$cohortId
    )
  }
  recordTasksDone(
    cohortId = subset$cohortId,
    task = "runIncidenceRate",
    checksum = subset$checksum,
    recordKeepingFile = recordKeepingFile,
    incremental = incremental
  )
  delta <- Sys.time() - startIncidenceRate
  ParallelLogger::logInfo(
    "Running Incidence Rate took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
}
