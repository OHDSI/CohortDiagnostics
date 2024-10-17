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

getIncidenceRate <- function(connection = NULL,
                             cohortDatabaseSchema,
                             cohortTable,
                             cdmDatabaseSchema,
                             vocabularyDatabaseSchema = cdmDatabaseSchema,
                             tempEmulationSchema = tempEmulationSchema,
                             firstOccurrenceOnly = TRUE,
                             washoutPeriod = 365,
                             cohortId) {
  start <- Sys.time()
  
  # check that cohort is instantiated
  cohortCount <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      "SELECT COUNT(*) AS COUNT FROM @cohort_database_schema.@cohort_table WHERE cohort_definition_id = @cohort_id;",
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      cohort_id = cohortId
    ) %>% dplyr::pull(1)
  
  if (!(cohortCount > 0)) {
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
  # optimization idea - only run this sql once since the result does not depend on the cohort
  # Also look into adding this into the sql file an not using insertTable
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
    data.frame(calendarYear = as.integer(seq(yearRange$startYear, yearRange$endYear, by = 1)))
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
      cohortCount = as.numeric(sum(irYearAgeGender$cohortCount)),
      personYears = as.numeric(sum(irYearAgeGender$personYears))
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
    1000 * as.numeric(result$cohortCount) / as.numeric(result$personYears)
  result$incidenceRate[is.nan(result$incidenceRate) |
    is.infinite(result$incidenceRate) |
    is.null(result$incidenceRate)] <- 0
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
        cohortCount = as.numeric(ratesSummary$cohortCount),
        personYears = as.numeric(ratesSummary$personYears)
      ),
      by = aggregateList,
      FUN = sum
    ))
  } else {
    return(tidyr::tibble())
  }
}

#' Run the incidence rate cohort diagnostic
#' 
#' runIncidenceRate computes incidence rates for cohorts in the CDM population stratified
#' by age, sex, and calendar year.
#'
#' @template connection 
#' @template cohortDefinitionSet 
#' @param washoutPeriod Then minimum number of required observation days prior to 
#'                      cohort index to be included in the numerator of the incidence rate
#' @template tempEmulationSchema 
#' @template cdmDatabaseSchema 
#' @template CohortTable 
#' @template databaseId 
#' @template exportFolder 
#' @template minCellCount 
#' @template Incremental 
#'
#' @return
#' @export
runIncidenceRate <- function(connection,
                             cohortDefinitionSet,
                             tempEmulationSchema,
                             cdmDatabaseSchema,
                             cohortDatabaseSchema,
                             cohortTable,
                             databaseId,
                             exportFolder,
                             minCellCount,
                             washoutPeriod = 0,
                             incremental,
                             incrementalFolder = exportFolder) {
  
  errorMessage <- checkmate::makeAssertCollection()
  checkArg(connection, add = errorMessage)
  checkArg(cohortDefinitionSet, add = errorMessage)
  checkArg(tempEmulationSchema, add = errorMessage)
  checkArg(cdmDatabaseSchema, add = errorMessage)
  checkArg(cohortDatabaseSchema, add = errorMessage)
  checkArg(cohortTable, add = errorMessage)
  checkArg(databaseId, add = errorMessage)
  checkArg(exportFolder, add = errorMessage)
  checkArg(minCellCount, add = errorMessage)
  # checkArg(washoutPeriod, add = errorMessage) # check not yet implemented
  checkArg(incremental, add = errorMessage)
  checkArg(incrementalFolder, add = errorMessage)
  checkmate::reportAssertions(errorMessage)
  
  recordKeepingFile <- file.path(incrementalFolder, "incremental")
  
  checkmate::assertIntegerish(washoutPeriod, len = 1, lower = 0)
  
  ParallelLogger::logInfo("Computing incidence rates")
  startIncidenceRate <- Sys.time()
  subset <- subsetToRequiredCohorts(
    cohorts = cohortDefinitionSet,
    task = "runIncidenceRate",
    incremental = incremental,
    recordKeepingFile = recordKeepingFile
  )

  if (incremental && (nrow(subset) > 0)) {
    ParallelLogger::logInfo(sprintf(
      "Skipping %s cohorts in incremental mode.",
      nrow(cohortDefinitionSet) - nrow(subset)
    ))
  }
  
  if (nrow(subset) > 0) {
    runOneIncidenceRate <- function(row) {
      ParallelLogger::logInfo(
        "  Computing incidence rate for cohort '",
        row$cohortName,
        "'"
      )

      timeExecution(
        exportFolder,
        taskName = "getIncidenceRate",
        parent = "runIncidenceRate",
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

    data <- lapply(split(subset, subset$cohortId), runOneIncidenceRate)
    data <- dplyr::bind_rows(data)
    
    exportDataToCsv(
      data = data,
      tableName = "incidence_rate",
      fileName = file.path(exportFolder, "incidence_rate.csv"),
      minCellCount = minCellCount,
      databaseId = databaseId,
      incremental = incremental,
      # incidenceRate field is a calculated field that does not follow the same pattern as others for minCellValue
      enforceMinCellValueFunc = enforceMinCellValue(
        data,
        "incidenceRate",
        1000 * minCellCount / as.numeric(data$personYears)
      ),
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
