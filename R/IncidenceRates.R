# Copyright 2020 Observational Health Data Sciences and Informatics
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

#' @title
#' Compute incidence rate for a cohort
#'
#' @description
#' Returns yearly incidence rate stratified by age and gender
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @template CdmDatabaseSchema
#' 
#' @template OracleTempSchema
#'
#' @param firstOccurrenceOnly   Use only the first occurrence of the cohort per person?
#'
#' @param washoutPeriod         The minimum amount of observation time required before the occurrence
#'                              of a cohort entry. This is also used to eliminate immortal time from
#'                              the denominator.
#' @param cohortId              The cohort definition ID used to reference the cohort in the cohort
#'                              table.
#'
#' @return
#' Returns a data frame of cohort count, person year count, and incidence rate per 1000 persons
#' years with the following stratifications: 1) no stratification, 2) gender stratification,
#' 3) age (10-year) stratification, 4) calendar year and age (10-year) stratification, 5) calendar
#' year and gender stratification, 6) calendar year, age (10-year), and gender stratification with
#' option to save dataframes.
#'
#' @export
getIncidenceRate <- function(connectionDetails = NULL,
                             connection = NULL,
                             cohortDatabaseSchema,
                             cohortTable,
                             cdmDatabaseSchema,
                             oracleTempSchema = oracleTempSchema,
                             firstOccurrenceOnly = TRUE,
                             washoutPeriod = 365,
                             cohortId) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (!checkIfCohortInstantiated(connection = connection,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 cohortId = cohortId)) {
    warning("Cohort with ID ", cohortId, " appears to be empty. Was it instantiated?")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Computing incidence rates took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    return(data.frame())
  }
  
  ParallelLogger::logInfo("Calculating incidence rate per year by age and gender")
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "GetCalendarYearRange.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           cdm_database_schema = cdmDatabaseSchema)
  yearRange <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  calendarYears <- data.frame(calendarYear = seq(yearRange$startYear, yearRange$endYear, by = 1))
  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "#calendar_years",
                                 data = calendarYears,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 oracleTempSchema = oracleTempSchema,
                                 camelCaseToSnakeCase = TRUE)
  
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "ComputeIncidenceRates.sql",
                                           packageName = "CohortDiagnostics",
                                           dbms = connection@dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           first_occurrence_only = firstOccurrenceOnly,
                                           washout_period = washoutPeriod,
                                           cohort_id = cohortId)
  DatabaseConnector::executeSql(connection, sql)
  
  sql <- "SELECT * FROM #rates_summary;"
  ratesSummary <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                             sql = sql,
                                                             oracleTempSchema = oracleTempSchema,
                                                             snakeCaseToCamelCase = TRUE)
  
  sql <- "TRUNCATE TABLE #rates_summary; DROP TABLE #rates_summary;"
  DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                               sql = sql,
                                               progressBar = FALSE,
                                               reportOverallTime = FALSE,
                                               oracleTempSchema = oracleTempSchema)
  
  irYearAgeGender <- recode(ratesSummary)
  irOverall <- data.frame(cohortCount = sum(irYearAgeGender$cohortCount),
                          personYears = sum(irYearAgeGender$personYears))
  irGender <- aggregateIr(irYearAgeGender, list(gender = irYearAgeGender$gender))
  irAge <- aggregateIr(irYearAgeGender, list(ageGroup = irYearAgeGender$ageGroup))
  irAgeGender <- aggregateIr(irYearAgeGender, list(ageGroup = irYearAgeGender$ageGroup,
                                                   gender = irYearAgeGender$gender))
  irYear <- aggregateIr(irYearAgeGender, list(calendarYear = irYearAgeGender$calendarYear))
  irYearAge <- aggregateIr(irYearAgeGender, list(calendarYear = irYearAgeGender$calendarYear,
                                                 ageGroup = irYearAgeGender$ageGroup))
  irYearGender <- aggregateIr(irYearAgeGender, list(calendarYear = irYearAgeGender$calendarYear,
                                                    gender = irYearAgeGender$gender))
  result <- dplyr::bind_rows(irOverall,
                             irGender,
                             irAge,
                             irAgeGender,
                             irYear,
                             irYearAge,
                             irYearGender,
                             irYearAgeGender)
  result$incidenceRate <- 1000 * result$cohortCount/result$personYears
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Computing incidence rates took",
                                signif(delta, 3),
                                attr(delta, "units")))
  return(result)
}

recode <- function(ratesSummary) {
  ratesSummary$ageGroup <- paste(10 * ratesSummary$ageGroup, 10 * ratesSummary$ageGroup + 9, sep = "-")
  ratesSummary$gender <- tolower(ratesSummary$gender)
  substr(ratesSummary$gender, 1, 1) <- toupper(substr(ratesSummary$gender, 1, 1) ) 
  return(ratesSummary)
}

aggregateIr <- function(ratesSummary, aggregateList) {
  return(aggregate(cbind(cohortCount = ratesSummary$cohortCount,
                         personYears = ratesSummary$personYears), 
                   by = aggregateList, 
                   FUN = sum))
}

filterIncidenceRateData <- function(incidenceRate, stratifyByAge, stratifyByGender, stratifyByCalendarYear, minPersonYears) {
  idx <- rep(TRUE, nrow(incidenceRate))
  if (stratifyByAge) {
    idx <- idx & !is.na(incidenceRate$ageGroup)
  } else {
    idx <- idx & is.na(incidenceRate$ageGroup)
  }
  if (stratifyByGender) {
    idx <- idx & !is.na(incidenceRate$gender)
  } else {
    idx <- idx & is.na(incidenceRate$gender)
  }
  if (stratifyByCalendarYear) {
    idx <- idx & !is.na(incidenceRate$calendarYear)
  } else {
    idx <- idx & is.na(incidenceRate$calendarYear)
  }
  data <- incidenceRate[idx, ]
  data <- data[data$cohortCount > 0, ]
  data <- data[data$personYears > minPersonYears, ]
  data$gender <- as.factor(data$gender)
  data$calendarYear <- as.numeric(as.character(data$calendarYear))
  
  # Sort ageGroup numerically, so 100-109 > 20-29:
  ageGroups <- unique(data$ageGroup)
  ageGroups <- ageGroups[order(as.numeric(gsub("-.*", "", ageGroups)))]
  data$ageGroup <- factor(data$ageGroup, levels = ageGroups)
  return(data)
}
  
#' Plot incidence rate by year, age, and/or gender
#'
#' @description
#' Characterizes the incidence rate of a cohort definition.
#'
#' @details
#' Generates time series plots of the incidence rate per 1000 person years of cohort entry by
#' year, age, and/or gender.
#'
#' @param data                    Incidence rate time series data for plotting generated using
#'                                \code{\link{getIncidenceRate}} function.
#' @param minPersonYears          Estimates get very unstable with low background counts, so removing them
#'                                makes for cleaner plots.
#' @param stratifyByAge           Should the plot be stratified by age?
#' @param stratifyByGender        Should the plot be stratified by gender?
#' @param stratifyByCalendarYear  Should the plot be stratified by calendar year?
#' @param fileName                Optional: name of the file where the plot should be saved, for
#'                                example 'plot.png'. See the function \code{ggsave} in the ggplot2
#'                                package for supported file formats.
#'
#' @return
#' A ggplot object. Use the \code{\link[ggplot2]{ggsave}} function to save to file in a different
#' format.
#'
#' @export
plotincidenceRate <- function(incidenceRate,
                              minPersonYears = 1000,
                              stratifyByAge = TRUE,
                              stratifyByGender = TRUE,
                              stratifyByCalendarYear = TRUE,
                              fileName = NULL) {
  
  data <- filterIncidenceRateData(incidenceRate = incidenceRate,
                                  stratifyByAge = stratifyByAge,
                                  stratifyByGender = stratifyByGender,
                                  stratifyByCalendarYear = stratifyByCalendarYear,
                                  minPersonYears = minPersonYears)
  
  idx <- rep(TRUE, nrow(incidenceRate))
  if (stratifyByAge) {
    idx <- idx & !is.na(incidenceRate$ageGroup)
  } else {
    idx <- idx & is.na(incidenceRate$ageGroup)
  }
  if (stratifyByGender) {
    idx <- idx & !is.na(incidenceRate$gender)
  } else {
    idx <- idx & is.na(incidenceRate$gender)
  }
  if (stratifyByCalendarYear) {
    idx <- idx & !is.na(incidenceRate$calendarYear)
  } else {
    idx <- idx & is.na(incidenceRate$calendarYear)
  }
  data <- incidenceRate[idx, ]
  
  aesthetics <- list(y = "incidenceRate")
  if (stratifyByCalendarYear) {
    aesthetics$x <- "calendarYear"
    xLabel <- "Calender year"
    showX <- TRUE
    if (stratifyByGender) {
      aesthetics$group <- "gender"
      aesthetics$color <- "gender"
    }
    plotType <- "line"
  } else {
    xLabel <- ""
    if (stratifyByGender) {
      aesthetics$x <- "gender"
      aesthetics$color <- "gender"
      aesthetics$fill <- "gender"
      showX <- TRUE
    } else {
      aesthetics$x <- "dummy"
      showX <- FALSE
    }
    plotType <- "bar"
  }
  
  plot <- ggplot2::ggplot(data = data, do.call(ggplot2::aes_string, aesthetics)) +
    ggplot2::xlab(xLabel) +
    ggplot2::ylab("Incidence Rate (/1,000 person years)") +
    ggplot2::theme(legend.position = "top",
                   legend.title = ggplot2::element_blank(),
                   axis.text.x = if (showX) ggplot2::element_text(angle = 90, vjust = 0.5) else ggplot2::element_blank() )
  
  if (plotType == "line") {
    plot <- plot + ggplot2::geom_line(size = 1.25, alpha = 0.6) +
      ggplot2::geom_point(size = 1.25, alpha = 0.6)
  } else {
    plot <- plot + ggplot2::geom_bar(stat = "identity", alpha = 0.6)
  }
  
  # databaseId field only present when called in Shiny app:
  if (!is.null(data$databaseId) && length(data$databaseId) > 1) {
    if (stratifyByAge) {
      plot <- plot + ggplot2::facet_grid(databaseId~ageGroup, scales = "free_y")
    } else {
      plot <- plot + ggplot2::facet_grid(databaseId~., scales = "free_y") 
    }
  } else {
    if (stratifyByAge) {
      plot <- plot + ggplot2::facet_grid(~ageGroup) 
    }
  }
  if (!is.null(fileName))
    ggplot2::ggsave(fileName, plot, width = 5, height = 3.5, dpi = 400)
  return(plot)
}