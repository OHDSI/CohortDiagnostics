# Copyright 2020 Observational Health Data Sciences and Informatics
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
#' Compute incidence proportion for a cohort
#'
#' @description
#' Returns yearly incidence proportion time series data stratified by age and gender
#'
#' @details
#' Returns a data frame of cohort count, background count, and incidence proportion per 1000 persons
#' of cohort entry with the following stratifications: 1) no stratification, 2) gender stratification,
#' 3) age (10-year) stratification, 4) calendar year and age (10-year) stratification, 5) calendar
#' year and gender stratification, 6) calendar year, age (10-year), and gender stratification with
#' option to save dataframes.
#'
#' @template Connection
#'
#' @template CohortTable
#'
#' @template CdmDatabaseSchema
#'
#' @param firstOccurrenceOnly   Use only the first occurrence of the cohort per person?
#'
#' @param minObservationTime    The minimum amount of observation time required before the occurrence
#'                              of a cohort entry. This is also used to eliminate immortal time from
#'                              the denominator.
#' @param cohortId              The cohort definition ID used to reference the cohort in the cohort
#'                              table.
#'
#' @return
#' A data frame
#'
#' @export
getIncidenceProportion <- function(connectionDetails = NULL,
                                   connection = NULL,
                                   cohortDatabaseSchema,
                                   cohortTable,
                                   cdmDatabaseSchema,
                                   firstOccurrenceOnly = TRUE,
                                   minObservationTime = 365,
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
    ParallelLogger::logInfo(paste("Getting incidence proportion data took",
                                  signif(delta, 3),
                                  attr(delta, "units")))
    return(data.frame())
  }

  ParallelLogger::logInfo("Calculating incidence proportion per year by age and gender...")
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "IncidenceProportionYearAgeGenderStratified.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connection@dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           first_occurrence_only = firstOccurrenceOnly,
                                           min_observation_time = minObservationTime,
                                           cohort_definition_id = cohortId)
  DatabaseConnector::executeSql(connection, sql)

  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "GetIncidenceProportionData.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connection@dbms)
  ipYearAgeGenderData <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  ipYearAgeGenderData <- recode(ipYearAgeGenderData)
  ipYearAgeGenderData$incidenceProportion <- 1000 * ipYearAgeGenderData$cohortSubjects/ipYearAgeGenderData$backgroundSubjects

  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "RemoveTempTables.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connection@dbms)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

  ipData <- data.frame(cohortSubjects = sum(ipYearAgeGenderData$cohortSubjects),
                       backgroundSubjects = sum(ipYearAgeGenderData$backgroundSubjects),
                       incidenceProportion = 1000 * sum(ipYearAgeGenderData$cohortSubjects)/sum(ipYearAgeGenderData$backgroundSubjects))
  ipGender <- aggregateGetIp(ipYearAgeGenderData, list(gender = ipYearAgeGenderData$gender))
  ipGender <- ipGender[order(ipGender$gender), ]
  ipAge <- aggregateGetIp(ipYearAgeGenderData, list(ageGroup = ipYearAgeGenderData$ageGroup))
  ipAge <- ipAge[order(ipAge$ageGroup), ]
  ipYearData <- aggregateGetIp(ipYearAgeGenderData, list(indexYear = ipYearAgeGenderData$indexYear))
  ipYearData <- ipYearData[order(ipYearData$indexYear), ]
  ipYearAgeData <- aggregateGetIp(ipYearAgeGenderData,
                                  list(indexYear = ipYearAgeGenderData$indexYear,
                                                            ageGroup = ipYearAgeGenderData$ageGroup))
  ipYearAgeData <- ipYearAgeData[order(ipYearAgeData$indexYear, ipYearAgeData$ageGroup), ]
  ipYearGenderData <- aggregateGetIp(ipYearAgeGenderData,
                                     list(indexYear = ipYearAgeGenderData$indexYear,
                                                               gender = ipYearAgeGenderData$gender))
  ipYearGenderData <- ipYearGenderData[order(ipYearGenderData$indexYear, ipYearGenderData$gender), ]
  ipYearAgeGenderData <- ipYearAgeGenderData[order(ipYearAgeGenderData$indexYear,
                                                   ipYearAgeGenderData$ageGroup,
                                                   ipYearAgeGenderData$gender), ]
  result <- dplyr::bind_rows(ipData,
                             ipGender,
                             ipAge,
                             ipYearData,
                             ipYearAgeData,
                             ipYearGenderData,
                             ipYearAgeGenderData)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste("Getting incidence proportion data took",
                                signif(delta, 3),
                                attr(delta, "units")))
  return(result)
}

recode <- function(ipData) {
  ageGroups <- unique(ipData$ageGroup)
  ageGroups <- min(ageGroups):max(ageGroups)
  for (i in ageGroups) {
    ipData$ageGroup[ipData$ageGroup == i] <- paste(10 * i, 10 * i + 9, sep = "-")
  }
  ipData$ageGroup <- factor(ipData$ageGroup,
                            levels = paste(10 * ageGroups, 10 * ageGroups + 9, sep = "-"),
                            ordered = TRUE)
  ipData$gender[ipData$gender == "FEMALE"] <- "Female"
  ipData$gender[ipData$gender == "MALE"] <- "Male"
  ipData$indexYear <- as.factor(ipData$indexYear)
  ipData <- ipData[!is.na(ipData$cohortSubjects), ]
  return(ipData)
}

aggregateGetIp <- function(ipData, aggregateList) {
  ipData <- aggregate(cbind(cohortSubjects = ipData$cohortSubjects,
                            backgroundSubjects = ipData$backgroundSubjects), 
                      by = aggregateList, 
                      FUN = sum)
  ipData$incidenceProportion <- 1000 * ipData$cohortSubjects/ipData$backgroundSubjects
  return(ipData)
}

#' Plot incidence proportion by year
#'
#' @description
#' Characterizes the incidence proportion of a phenotype as a time series visualization
#'
#' @details
#' Generates time series plots of the incidence proportion per 1000 persons of phenotype entry by
#' year.
#'
#' @param incidenceProportion   Incidence proportion time series data for plotting generated using
#'                              \code{\link{getIncidenceProportion}} function.
#' @param fileName              Optional: name of the file where the plot should be saved, for example
#'                              'plot.png'. See the function \code{ggsave} in the ggplot2 package for
#'                              supported file formats.
#'
#' @return
#' A ggplot object. Use the \code{\link[ggplot2]{ggsave}} function to save to file in a different
#' format.
#'
#' @export
plotIncidenceProportionByYear <- function(incidenceProportion, fileName = NULL) {
  data <- incidenceProportion[is.na(incidenceProportion$gender) & is.na(incidenceProportion$ageGroup) &
    !is.na(incidenceProportion$indexYear), ]
  data$indexYear <- as.numeric(as.character(data$indexYear))
  plot <- ggplot2::ggplot(data = data,
                          ggplot2::aes(x = indexYear, y = incidenceProportion, group = 1)) +
          ggplot2::geom_line(color = rgb(0, 0, 0.8), size = 1.25, alpha = 0.6) +
          ggplot2::xlab("Year") +
          ggplot2::ylab("Incidence proportion (/1000 persons)") +
          ggplot2::theme(legend.position = "none",
                         axis.text.x = ggplot2::element_text(angle = 90, vjust = 0.5))
  if (!is.null(fileName))
    ggplot2::ggsave(fileName, plot, width = 5, height = 3.5, dpi = 400)
  return(plot)
}

#' Plot incidence proportion by year, age, and gender
#'
#' @description
#' Characterizes the incidence proportion of a phenotype as a time series visualization
#'
#' @details
#' Generates time series plots of the incidence proportion per 1000 persons of phenotype entry by
#' year, age, and gender.
#'
#' @param incidenceProportion     Incidence proportion time series data for plotting generated using
#'                                \code{\link{getIncidenceProportion}} function.
#' @param restrictToFullAgeData   Restrict to panels having data on all ages?
#' @param fileName                Optional: name of the file where the plot should be saved, for
#'                                example 'plot.png'. See the function \code{ggsave} in the ggplot2
#'                                package for supported file formats.
#'
#' @return
#' A ggplot object. Use the \code{\link[ggplot2]{ggsave}} function to save to file in a different
#' format.
#'
#' @export
plotIncidenceProportion <- function(incidenceProportion,
                                    restrictToFullAgeData = FALSE,
                                    fileName = NULL) {
  data <- incidenceProportion[!is.na(incidenceProportion$gender) & !is.na(incidenceProportion$ageGroup) &
    !is.na(incidenceProportion$indexYear), ]
  data$gender <- as.factor(data$gender)
  data$indexYear <- as.numeric(as.character(data$indexYear))
  if (restrictToFullAgeData) {
    data <- useFullData(data)
  }
  plot <- ggplot2::ggplot(data = data, ggplot2::aes(x = indexYear,
                                                    y = incidenceProportion,
                                                    group = gender,
                                                    color = gender)) +
          ggplot2::geom_line(size = 1.25, alpha = 0.6) +
          ggplot2::xlab("Year") +
          ggplot2::ylab("Incidence proportion (/1000 persons)") +
          ggplot2::facet_grid(~ageGroup) +
          ggplot2::theme(legend.position = "top",
                         legend.title = ggplot2::element_blank(),
                         axis.text.x = ggplot2::element_text(angle = 90, vjust = 0.5))
  if (!is.null(fileName))
    ggplot2::ggsave(fileName, plot, width = 5, height = 3.5, dpi = 400)
  return(plot)
}

useFullData <- function(df) {
  yearList <- list()
  for (year in unique(df$indexYear)) {
    yearList[[length(yearList) + 1]] <- unique(df$ageGroup[df$indexYear == year])
  }
  ageGroups <- Reduce(intersect, yearList)
  df <- df[df$ageGroup %in% ageGroups, ]
  return(df)
}
