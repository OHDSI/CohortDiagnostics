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
#' @param cohortDefinitionId     Cohort ID
#' @param workFolder             Optional: directory where time series data to be saved as ipData.rds
#'
#' @export
getIncidenceProportionData <- function(connectionDetails,
                                       cohortDatabaseSchema,
                                       cohortTable,
                                       cdmDatabaseSchema,
                                       firstOccurrenceOnly = TRUE,
                                       minObservationTime = 365,
                                       cohortDefinitionId,
                                       workFolder = NULL) {

  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "IncidenceProportionYearAgeGenderStratified.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connectionDetails$dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           first_occurrence_only = firstOccurrenceOnly,
                                           min_observation_time = minObservationTime,
                                           cohort_definition_id = cohortDefinitionId)
  connection <- DatabaseConnector::connect(connectionDetails)
  writeLines("Calculating incidence proportion per year by age and gender...")
  DatabaseConnector::executeSql(connection, sql)
  
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "GetIncidenceProportionData.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connectionDetails$dbms)
  ipYearAgeGenderData <- DatabaseConnector::querySql(connection, sql)
  ipYearAgeGenderData <- recodes(ipYearAgeGenderData)

  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "RemoveTempTables.sql",
                                           packageName = "StudyDiagnostics",
                                           dbms = connectionDetails$dbms)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  DatabaseConnector::disconnect(connection)

  ipData <- data.frame(NUM_COUNT = sum(ipYearAgeGenderData$NUM_COUNT),
                       DENOM_COUNT = sum(ipYearAgeGenderData$DENOM_COUNT),
                       IP_1000P = 1000 * sum(ipYearAgeGenderData$NUM_COUNT) / sum(ipYearAgeGenderData$DENOM_COUNT))
  ipGender <- aggregateGetIp(ipYearAgeGenderData, list(GENDER = ipYearAgeGenderData$GENDER))
  ipGender <- ipGender[order(ipGender$GENDER), ]
  ipAge <- aggregateGetIp(ipYearAgeGenderData, list(AGE_GROUP_10Y = ipYearAgeGenderData$AGE_GROUP_10Y))
  ipAge <- ipAge[order(ipAge$AGE_GROUP_10Y), ]
  ipYearData <- aggregateGetIp(ipYearAgeGenderData, list(INDEX_YEAR = ipYearAgeGenderData$INDEX_YEAR))
  ipYearData <- ipYearData[order(ipYearData$INDEX_YEAR), ]
  ipYearAgeData <- aggregateGetIp(ipYearAgeGenderData, list(INDEX_YEAR = ipYearAgeGenderData$INDEX_YEAR, AGE_GROUP_10Y = ipYearAgeGenderData$AGE_GROUP_10Y))
  ipYearAgeData <- ipYearAgeData[order(ipYearAgeData$INDEX_YEAR, ipYearAgeData$AGE_GROUP_10Y), ]
  ipYearGenderData <- aggregateGetIp(ipYearAgeGenderData, list(INDEX_YEAR = ipYearAgeGenderData$INDEX_YEAR, GENDER = ipYearAgeGenderData$GENDER))
  ipYearGenderData <- ipYearGenderData[order(ipYearGenderData$INDEX_YEAR, ipYearGenderData$GENDER), ]
  ipYearAgeGenderData <- ipYearAgeGenderData[order(ipYearAgeGenderData$INDEX_YEAR, ipYearAgeGenderData$AGE_GROUP_10Y, ipYearAgeGenderData$GENDER), ]
  ipDataList <- list(ipData = ipData,
                     ipGender = ipGender,
                     ipAge = ipAge,
                     ipYearData = ipYearData,
                     ipYearAgeData = ipYearAgeData,
                     ipYearGenderData = ipYearGenderData,
                     ipYearAgeGenderData = ipYearAgeGenderData)

  if (!is.null(workFolder)) {
    if (!file.exists(workFolder)) {
      dir.create(workFolder, recursive = TRUE)
    }
    fileName <- file.path(workFolder, "ipData.rds")
    saveRDS(ipDataList, fileName)
  }
  return(ipDataList)
}

recodes <- function(ipData) {
  ageGroups <- unique(ipData$AGE_GROUP_10Y)
  ageGroups <- min(ageGroups):max(ageGroups)
  for (i in ageGroups) {
    ipData$AGE_GROUP_10Y[ipData$AGE_GROUP_10Y == i] <- paste(10*i, 10*i + 9, sep = "-")
  }
  ipData$AGE_GROUP_10Y <- factor(ipData$AGE_GROUP_10Y,
                                 levels = paste(10*ageGroups, 10*ageGroups + 9, sep = "-"),
                                 ordered = TRUE)
  ipData$GENDER[ipData$GENDER == "FEMALE"] <- "Female"
  ipData$GENDER[ipData$GENDER == "MALE"] <- "Male"
  ipData$INDEX_YEAR <- as.factor(ipData$INDEX_YEAR)
  ipData <- ipData[!is.na(ipData$NUM_COUNT), ]
  return(ipData)
}

aggregateGetIp <- function(ipData, aggregateList) {
  ipData <- aggregate(cbind(NUM_COUNT = ipData$NUM_COUNT,
                            DENOM_COUNT = ipData$DENOM_COUNT),
                      by = aggregateList,
                      FUN = sum)
  ipData$IP_1000P <- 1000 * ipData$NUM_COUNT / ipData$DENOM_COUNT
  return(ipData)
}
