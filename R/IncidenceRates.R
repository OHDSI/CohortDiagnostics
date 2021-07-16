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

#' Given a set of instantiated cohorts get Incidence Rate for the cohorts.
#'
#' @description
#' This function computes incidence rate, one cohort at a time.
#'
#' @template Connection
#'
#' @template CohortDatabaseSchema
#' 
#' @template CdmDatabaseSchema
#'
#' @template VocabularyDatabaseSchema
#' 
#' @template TempEmulationSchema
#'
#' @template CohortTable
#'
#' @param cohortId               A Cohort Id to compute time distribution for.
#' 
#' @param cdmVersion              Only CDM version 5 is supported.
#' 
#' @param firstOccurrenceOnly    Compute for first occurrence of subject in the cohort.
#' 
#' @param washoutPeriod          (Optional) Washout period to use. The default value is either
#'                               365 days or minimum prior observation period requirement
#'                               specified in cohort definition.
#'
#' @export
runIncidenceRateDiagnostics <- function(connectionDetails = NULL,
                             connection = NULL,
                             cohortDatabaseSchema,
                             cohortTable,
                             cdmDatabaseSchema,
                             vocabularyDatabaseSchema = cdmDatabaseSchema,
                             cdmVersion = 5,
                             tempEmulationSchema = tempEmulationSchema,
                             firstOccurrenceOnly = TRUE,
                             washoutPeriod = 365,
                             cohortId) {
  start <- Sys.time()
  
  if (!cdmVersion == 5) {
    stop("Only CDM version 5 is supported. Terminating.")
  }
  
  connection <- .setUpConnection(connection = connection,
                                 connectionDetails = connectionDetails)
  
  instantiatedCohortsCheck <- checkIfCohortInstantiated(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohortId
  )
  
  if (!isTRUE(instantiatedCohortsCheck)) {
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
    return(dplyr::tibble())
  }
  
  ParallelLogger::logInfo("Calculating incidence rate per year by age and gender")
  sql <-
    SqlRender::loadRenderTranslateSql(
      sqlFilename = "GetCalendarYearRange.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      cdm_database_schema = cdmDatabaseSchema
    )
  yearRange <-
    DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  
  calendarYears <-
    dplyr::tibble(calendarYear = seq(yearRange$startYear, yearRange$endYear, by = 1))
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
      packageName = "CohortDiagnostics",
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
    renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      snakeCaseToCamelCase = TRUE
    )
  
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
      list(ageGroup = irYearAgeGender$ageGroup,
           gender = irYearAgeGender$gender)
    )
  irYear <-
    aggregateIr(irYearAgeGender,
                list(calendarYear = irYearAgeGender$calendarYear))
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
    return(dplyr::tibble())
  }
}
