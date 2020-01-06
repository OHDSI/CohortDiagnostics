# Copyright 2019 Observational Health Data Sciences and Informatics
#
# This file is part of StudyDiagnostics
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
#' Get incidence proportion data
#' 
#' @description
#' Returns yearly incidence proportion time series data stratified by age and gender
#'
#' @details
#' Returns a list of 7 dataframes of cohort count, database count, and 
#' incidence proportion per 1000 persons of cohort entry with the following
#' stratifications: 1) no stratification, 2) gender stratification, 3) age (10-year)
#' stratification, 4) calendar year and age (10-year) stratification, 5) calendar year and
#' gender stratification, 6) calendar year, age (10-year), and gender stratification
#' with option to save dataframes.
#'
#' @return
#' A list of 7 dataframe objects with option to save as an *.rds file.
#'
#' @param connectionDetails      The connection details to your database server
#' @param cohortDatabaseSchema   The database name where your phenotype is instantiated as a standard cohort table.
#' @param cohortTable            The table name where your phenotype is instantiated as a standard cohort table.
#' @param cdmDatabaseSchema      The name of your CDM source
#' @param instantiatedCohortId     Cohort ID
#'
#' @export
getIncidenceProportion <- function(connectionDetails = NULL,
                                   connection = NULL,
                                   cohortDatabaseSchema,
                                   cohortTable,
                                   cdmDatabaseSchema,
                                   firstOccurrenceOnly = TRUE,
                                   minObservationTime = 365,
                                   instantiatedCohortId) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails) 
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (!checkIfCohortInstantiated(connection = connection,
                                 cohortDatabaseSchema = cohortDatabaseSchema,
                                 cohortTable = cohortTable,
                                 instantiatedCohortId = instantiatedCohortId)) {
    warning("Cohort with ID ", instantiatedCohortId, " appears to be empty. Was it instantiated?")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste("Getting incidence proportion data took", signif(delta, 3), attr(delta, "units")))
    return(data.frame())
  } 
  
  ParallelLogger::logInfo("Calculating incidence proportion per year by age and gender...")
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "IncidenceProportionYearAgeGenderStratified.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connectionDetails$dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           first_occurrence_only = firstOccurrenceOnly,
                                           min_observation_time = minObservationTime,
                                           cohort_definition_id = instantiatedCohortId)
  DatabaseConnector::executeSql(connection, sql)
  
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "GetIncidenceProportionData.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connectionDetails$dbms)
  ipYearAgeGenderData <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  ipYearAgeGenderData <- recode(ipYearAgeGenderData)

  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "RemoveTempTables.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connectionDetails$dbms)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

  ipData <- data.frame(numCount = sum(ipYearAgeGenderData$numCount),
                       denomCount = sum(ipYearAgeGenderData$denomCount),
                       ip1000P = 1000 * sum(ipYearAgeGenderData$numCount) / sum(ipYearAgeGenderData$denomCount))
  ipGender <- aggregateGetIp(ipYearAgeGenderData, list(gender = ipYearAgeGenderData$gender))
  ipGender <- ipGender[order(ipGender$gender), ]
  ipAge <- aggregateGetIp(ipYearAgeGenderData, list(ageGroup10y = ipYearAgeGenderData$ageGroup10y))
  ipAge <- ipAge[order(ipAge$ageGroup10y), ]
  ipYearData <- aggregateGetIp(ipYearAgeGenderData, list(indexYear = ipYearAgeGenderData$indexYear))
  ipYearData <- ipYearData[order(ipYearData$indexYear), ]
  ipYearAgeData <- aggregateGetIp(ipYearAgeGenderData, list(indexYear = ipYearAgeGenderData$indexYear, ageGroup10y = ipYearAgeGenderData$ageGroup10y))
  ipYearAgeData <- ipYearAgeData[order(ipYearAgeData$indexYear, ipYearAgeData$ageGroup10y), ]
  ipYearGenderData <- aggregateGetIp(ipYearAgeGenderData, list(indexYear = ipYearAgeGenderData$indexYear, gender = ipYearAgeGenderData$gender))
  ipYearGenderData <- ipYearGenderData[order(ipYearGenderData$indexYear, ipYearGenderData$gender), ]
  ipYearAgeGenderData <- ipYearAgeGenderData[order(ipYearAgeGenderData$indexYear, ipYearAgeGenderData$ageGroup10y, ipYearAgeGenderData$gender), ]
  result <- dplyr::bind_rows(ipData, ipGender, ipAge, ipYearData, ipYearAgeData, ipYearGenderData, ipYearAgeGenderData)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Getting incidence proportion data took", signif(delta, 3), attr(delta, "units")))
  return(result)
}

recode <- function(ipData) {
  ageGroups <- unique(ipData$ageGroup10y)
  ageGroups <- min(ageGroups):max(ageGroups)
  for (i in ageGroups) {
    ipData$ageGroup10y[ipData$ageGroup10y == i] <- paste(10*i, 10*i + 9, sep = "-")
  }
  ipData$ageGroup10y <- factor(ipData$ageGroup10y,
                                 levels = paste(10*ageGroups, 10*ageGroups + 9, sep = "-"),
                                 ordered = TRUE)
  ipData$gender[ipData$gender == "FEMALE"] <- "Female"
  ipData$gender[ipData$gender == "MALE"] <- "Male"
  ipData$indexYear <- as.factor(ipData$indexYear)
  ipData <- ipData[!is.na(ipData$numCount), ]
  return(ipData)
}

aggregateGetIp <- function(ipData, aggregateList) {
  ipData <- aggregate(cbind(numCount = ipData$numCount,
                            denomCount = ipData$denomCount),
                      by = aggregateList,
                      FUN = sum)
  ipData$ip1000P <- 1000 * ipData$numCount / ipData$denomCount
  return(ipData)
}


#' @title
#' Generate stability plots
#' 
#' @description
#' Characterizes the incidence proportion of a phenotype as a time series visualization
#'
#' @details
#' Generates time series plots of the incidence proportion per 1000 persons of phenotype
#' entry by year, by year and 10-year age group, by year and gender, and by year and
#' 10-year age group and gender
#'
#' @return
#' A list of 4 gg objects generated by the ggplot2 package with option to save as 1 *.rds
#' file and 4 png files.
#'
#' @param incidenceProportion   Incidence proportion time series data for plotting generated using
#'                              \code{\link{getIncidenceProportion}} function.
#' @param panel    Create trellis panels by gender or by age? select "age" or "gender", defaults to "age"
#' @param fileName Optional: directory and filename of plot to be saved
#'
#' @export
plotIncidenceProportion <- function(incidenceProportion,
                                    panel = "age",
                                    restrictToFullAgeData = FALSE,
                                    fileName = NULL) {
  # To do: rewrite for new format. One function per plot
  
  # stratified by year
  ipYearData <- incidenceProportion[incidenceProportion]
  ipYearPlot <- ggplot2::ggplot(data = ipYearData,
                                ggplot2::aes(x = INDEX_YEAR, y = IP_1000P, group = 1)) +
    ggplot2::geom_line(ggplot2::aes(color = "red"), size = 1.25) +
    ggplot2::xlab("Year") +
    ggplot2::ylab("Incidence proportion (/1000 persons)") +
    ggplot2::theme(legend.position = "none",
                   axis.text.x = ggplot2::element_text(angle = 90))
  
  # stratified by year, age
  ipYearAgeData <- ipData$ipYearAgeData
  if (restrictToFullAgeData) {
    ipYearAgeData <- useFullData(ipYearAgeData)
  }
  if (panel == "age") {
    width <- 12
    ipYearAgePlot <- ggplot2::ggplot(data = ipYearAgeData,
                                     ggplot2::aes(x = INDEX_YEAR, y = IP_1000P, group = 1)) +
      ggplot2::geom_line(ggplot2::aes(color = "red"), size = 1.25) +
      ggplot2::facet_grid(. ~ AGE_GROUP_10Y) +
      ggplot2::xlab("Year") +
      ggplot2::ylab("Incidence proportion (/1000 persons)") +
      ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 90, size = 6),
                     legend.position = "none")
  }
  if (panel == "gender") {
    width <- 6
    ipYearAgePlot <- ggplot2::ggplot(data = ipYearAgeData,
                                     ggplot2::aes(x = INDEX_YEAR, y = IP_1000P, group = AGE_GROUP_10Y)) +
      ggplot2::geom_line(ggplot2::aes(color = AGE_GROUP_10Y), size = 1.25) +
      ggplot2::xlab("Year") +
      ggplot2::ylab("Incidence proportion (/1000 persons)") +
      ggplot2::labs(color = "Age (years)") +
      ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 90))
  }
  
  # stratified by year, gender
  ipYearGenderData <- ipData$ipYearGenderData
  ipYearGenderPlot <- ggplot2::ggplot(data = ipYearGenderData,
                                      ggplot2::aes(x = INDEX_YEAR, y = IP_1000P, group = GENDER)) +
    ggplot2::geom_line(ggplot2::aes(color = GENDER), size = 1.25) +
    ggplot2::xlab("Year") +
    ggplot2::ylab("Incidence proportion (/1000 persons)") +
    ggplot2::labs(color = "Gender") +
    ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 90))
  
  # stratified by year, age, gender
  ipYearAgeGenderData <- ipData$ipYearAgeGenderData
  if (restrictToFullAgeData) {
    ipYearAgeGenderData <- useFullData(ipYearAgeGenderData)
  }
  if (panel == "age") {
    ipYearAgeGenderPlot <- ggplot2::ggplot(data = ipYearAgeGenderData,
                                           ggplot2::aes(x = INDEX_YEAR, y = IP_1000P, group = GENDER)) +
      ggplot2::geom_line(ggplot2::aes(color = GENDER),  size = 1.25) +
      ggplot2::facet_grid(. ~ AGE_GROUP_10Y) +
      ggplot2::xlab("Year") +
      ggplot2::ylab("Incidence proportion (/1000 persons)") +
      ggplot2::labs(color = "Gender") +
      ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 90, size = 6))
  }
  if (panel == "gender") {
    ipYearAgeGenderPlot <- ggplot2::ggplot(data = ipYearAgeGenderData,
                                           ggplot2::aes(x = INDEX_YEAR, y = IP_1000P, group = AGE_GROUP_10Y)) +
      ggplot2::geom_line(ggplot2::aes(color = AGE_GROUP_10Y),  size = 1.25) +
      ggplot2::facet_grid(. ~ GENDER) +
      ggplot2::xlab("Year") +
      ggplot2::ylab("Incidence proportion (/1000 persons)") +
      ggplot2::labs(color = "Age (years)") +
      ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 90))
  }
  
  ipPlotList <- list(ipYearPlot = ipYearPlot,
                     ipYearAgePlot = ipYearAgePlot,
                     ipYearGenderPlot = ipYearGenderPlot,
                     ipYearAgeGenderPlot = ipYearAgeGenderPlot)
  if (!is.null(workFolder)) {
    if (!file.exists(workFolder)) {
      dir.create(workFolder, recursive = TRUE)
    }
    saveRDS(ipPlotList, file.path(workFolder, "ipPlots.rds"))
    ggplot2::ggsave(file.path(workFolder, "ipYearPlot.png"), ipYearPlot, width = 6, height = 4.5, dpi = 400)
    ggplot2::ggsave(file.path(workFolder, "ipYearAgePlot.png"), ipYearAgePlot, width = width, height = 4.5, dpi = 400)
    ggplot2::ggsave(file.path(workFolder, "ipYearGenderPlot.png"), ipYearGenderPlot, width = 6, height = 4.5, dpi = 400)
    ggplot2::ggsave(file.path(workFolder, "ipYearAgeGenderPlot.png"), ipYearAgeGenderPlot, width = 12, height = 4.5, dpi = 400)
  }
  return(ipPlotList)
}

useFullData <- function(df) {
  yearList <- list()
  for (year in unique(df$INDEX_YEAR)) {
    yearList[[length(yearList) + 1]] <- unique(df$AGE_GROUP_10Y[df$INDEX_YEAR == year])
  }
  ageGroups <- Reduce(intersect, yearList)
  df <- df[df$AGE_GROUP_10Y %in% ageGroups, ]
  return(df)
}
