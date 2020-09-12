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



#' Get time distribution data for plotting
#'
#' @description
#' Get time distribution data for plotting from data stored in cohort diagnostics results data
#' model. The output of this function may be used to create plots or tables
#' related to time distribution diagnostics. This function will look for time_distribution table
#' in results data model.
#' 
#' Note: this function relies on data available in Cohort Diagnostics results data model. There are
#' two methods to connect to the resutls data model, database mode and in-memory mode.
#' 
#' Database mode: If either \code{connectionDetails} or \code{\link[DatabaseConnector]{connect}} is 
#' provided in the function call, the query is set to database mode. In the absence of both 
#' \code{connectionDetails} and \code{\link[DatabaseConnector]{connect}}, the query will be in-memory 
#' mode. In in-memory mode, R will expect the data in results data model to be available in R's memory.
#' In database mode, R will perform a database query. Objects in R's memory are expected to
#' follow camelCase naming conventions, while objects in dbms are expected to follow snake-case naming
#' conventions. In database mode, vocabulary tables (if needed) are used from vocabularySchema (which
#' defaults to resultsDatabaseSchema.). In in-memory mode, vocabulary tables are assumed to in R's 
#' memory.
#'
#' @template Connection
#' @param cohortId       Cohort Id to retrieve the data. This is one of the integer (bigint) value from
#'                       cohortId field in cohort table of the results data model.
#' @param databaseId     The database to retrieve the results for. This is the character field value from
#'                       the databaseId field in the database table of the results data model.
#' @param resultsDatabaseSchema (optional) The databaseSchema where the results data model of cohort diagnostics
#'                              is stored. This is only required when \code{connectionDetails} or 
#'                              \code{\link[DatabaseConnector]{connect}} is provided.                       
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' timeDistribution <- getTimeDistribution(cohortId = 343242,
#'                                         databaseId = 'eunomia')
#' }
#'
#' @export
getTimeDistribution <- function(connection = NULL,
                                connect = NULL,
                                cohortId,
                                databaseId,
                                resultsDatabaseSchema = NULL) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertInt(x = cohortId, 
                       na.ok = FALSE, 
                       null.ok = FALSE,
                       lower = 0,
                       upper = 2^53,
                       add = errorMessage)
  checkmate::assertCharacter(x = databaseId,
                             min.len = 1,
                             max.len = 1,
                             any.missing = FALSE,
                             unique = TRUE,
                             add = errorMessage)
  
  if (!NULL(connect) || !NULL(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logInfo(" \n - No connection or connectionDetails provided.")
      ParallelLogger::logInfo("  Checking if required objects existsin R memory.")
    }
  } else {
    if (exists('timeDistribution')) {
      ParallelLogger::logInfo("  timeDistribution data object found in R memory. Continuing.")
    } else {
      ParallelLogger::logWarn("  timeDistribution data object not found in R memory. Exiting.")
      return(NULL)
    }
  }
  
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.time_distribution
              WHERE cohort_definition_id = @cohortId
            	AND database_id = '@databaseId';"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       cohortId = cohortId,
                                                       database_id = databaseId, 
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- timeDistribution %>% 
      dplyr::filter(.data$cohortDefinitionId == cohortId &
                      .data$databaseId %in% databaseId) %>% 
      dplyr::select(-.data$cohortDefinitionId, .data$databaseid) %>% 
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved for 'time distribution'.")
    return(NULL)
  }
  
  data <- data %>% 
    dplyr::select(.data$databaseId,
                  .data$timeMetric, 
                  .data$averageValue, 
                  .data$standardDeviation, 
                  .data$minValue, 
                  .data$p10Value, 
                  .data$p25Value, 
                  .data$medianValue, 
                  .data$p75Value, 
                  .data$p90Value, 
                  .data$maxValue) %>% 
    dplyr::rename(Database = "databaseId",
                  TimeMeasure = "timeMetric", 
                  Average = "averageValue", 
                  SD = "standardDeviation", 
                  Min = "minValue", 
                  P10 = "p10Value", 
                  P25 = "p25Value", 
                  Median = "medianValue", 
                  P75 = "p75Value", 
                  P90 = "p90Value", 
                  Max = "maxValue")
  
  return(data)
}

#' Get incidence rate data for plotting
#'
#' @description
#' Get incidence rate data for plotting from data stored in cohort diagnostics results data
#' model. The output of this function may be used to create plots or tables
#' related to incidence rate diagnostics. This function will look for incidence_rate table
#' in results data model.
#' 
#' Note: this function relies on data available in Cohort Diagnostics results data model. There are
#' two methods to connect to the resutls data model, database mode and in-memory mode.
#' 
#' Database mode: If either \code{connectionDetails} or \code{\link[DatabaseConnector]{connect}} is 
#' provided in the function call, the query is set to database mode. In the absence of both 
#' \code{connectionDetails} and \code{\link[DatabaseConnector]{connect}}, the query will be in-memory 
#' mode. In in-memory mode, R will expect the data in results data model to be available in R's memory.
#' In database mode, R will perform a database query. Objects in R's memory are expected to
#' follow camelCase naming conventions, while objects in dbms are expected to follow snake-case naming
#' conventions. In database mode, vocabulary tables (if needed) are used from vocabularySchema (which
#' defaults to resultsDatabaseSchema.). In in-memory mode, vocabulary tables are assumed to in R's 
#' memory.
#'
#' @template Connection
#' @param cohortId       Cohort Id to retrieve the data. This is one of the integer (bigint) value from
#'                       cohortId field in cohort table of the results data model.
#' @param databaseIds    A vector one or more databaseIds to retrieve the results for. This is a character 
#'                       field value from the databaseId field in the database table of the results data model.
#' @param resultsDatabaseSchema (optional) The databaseSchema where the results data model of cohort diagnostics
#'                              is stored. This is only required when \code{connectionDetails} or 
#'                              \code{\link[DatabaseConnector]{connect}} is provided.
#' @param minPersonYears (optional) Default value = 1000. Minimum person years needed to create plot.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' incidenceRate <- getIncidenceRate(cohortId = 343242,
#'                                   databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
getIncidenceRate <- function(connection = NULL,
                             connect = NULL,
                             cohortId,
                             databaseIds,
                             stratification,
                             minPersonYears = 1000,
                             resultsDatabaseSchema = NULL) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertInt(x = cohortId, 
                       na.ok = FALSE, 
                       null.ok = FALSE,
                       lower = 0,
                       upper = 2^53,
                       add = errorMessage)
  checkmate::assertCharacter(x = databaseIds,
                             min.len = 1,
                             any.missing = FALSE,
                             unique = TRUE,
                             add = errorMessage)
  
  if (!NULL(connect) || !NULL(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logInfo(" \n - No connection or connectionDetails provided.")
      ParallelLogger::logInfo("  Checking if required objects existsin R memory.")
    }
  } else {
    if (exists('incidenceRate')) {
      ParallelLogger::logInfo("  'incidence rate' data object found in R memory. Continuing.")
    } else {
      ParallelLogger::logWarn("  'incidence rate' data object not found in R memory. Exiting.")
      return(NULL)
    }
  }
  
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.incidence_rate
              WHERE cohort_definition_id = @cohortId
            	AND database_id in c('@databaseIds');"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       cohortId = cohortId,
                                                       databaseIds = databaseIds, 
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- incidenceRate %>% 
      dplyr::filter(.data$cohortDefinitionId == cohortId &
                      .data$databaseId %in% databaseIds) %>% 
      dplyr::select(-.data$cohortDefinitionId) %>% 
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved for 'incidence rate'.")
    return(NULL)
  }
  
  stratifyByAge <- "Age" %in% input$irStratification
  stratifyByGender <- "Gender" %in% input$irStratification
  stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
  
  
  idx <- rep(TRUE, nrow(data))
  if (stratifyByAge) {
    idx <- idx & !is.na(data$ageGroup)
  } else {
    idx <- idx & is.na(data$ageGroup)
  }
  if (stratifyByGender) {
    idx <- idx & !is.na(data$gender)
  } else {
    idx <- idx & is.na(data$gender)
  }
  if (stratifyByCalendarYear) {
    idx <- idx & !is.na(data$calendarYear)
  } else {
    idx <- idx & is.na(data$calendarYear)
  }
  data <- data[idx, ]
  data <- data[data$cohortCount > 0, ]
  data <- data[data$personYears > minPersonYears, ]
  data$gender <- as.factor(data$gender)
  data$calendarYear <- as.numeric(as.character(data$calendarYear))
  ageGroups <- unique(data$ageGroup)
  ageGroups <- ageGroups[order(as.numeric(gsub("-.*", "", ageGroups)))]
  data$ageGroup <- factor(data$ageGroup, levels = ageGroups)
  data <- data[data$incidenceRate > 0, ]
  data$dummy <- 0
  if (nrow(data) == 0) {
    return(NULL)
  } else {
    return(data)
  }
}



#' Get cohort counts
#'
#' @description
#' Get cohort counts data from data stored in cohort diagnostics results data
#' model. The output of this function may be used to create tables
#' related to cohort counts diagnostics. This function will look for cohort_count table
#' in results data model.
#' 
#' Note: this function relies on data available in Cohort Diagnostics results data model. There are
#' two methods to connect to the resutls data model, database mode and in-memory mode.
#' 
#' Database mode: If either \code{connectionDetails} or \code{\link[DatabaseConnector]{connect}} is 
#' provided in the function call, the query is set to database mode. In the absence of both 
#' \code{connectionDetails} and \code{\link[DatabaseConnector]{connect}}, the query will be in-memory 
#' mode. In in-memory mode, R will expect the data in results data model to be available in R's memory.
#' In database mode, R will perform a database query. Objects in R's memory are expected to
#' follow camelCase naming conventions, while objects in dbms are expected to follow snake-case naming
#' conventions. In database mode, vocabulary tables (if needed) are used from vocabularySchema (which
#' defaults to resultsDatabaseSchema.). In in-memory mode, vocabulary tables are assumed to in R's 
#' memory.
#'
#' @template Connection
#' @param databaseIds    A vector one or more databaseIds to retrieve the results for. This is a character 
#'                       field value from the databaseId field in the database table of the results data model.
#' @param resultsDatabaseSchema (optional) The databaseSchema where the results data model of cohort diagnostics
#'                              is stored. This is only required when \code{connectionDetails} or 
#'                              \code{\link[DatabaseConnector]{connect}} is provided.
#' 
#' @return
#' The function will return a tibble data frame object.
#'
#' @examples
#' \dontrun{
#' cohortCounts <- getCohortCounts(resultsDatabaseSchema = resultsDatabaseSchema,
#'                                 databaseIds = c('eunomia', 'hcup'))
#' }
#'
#' @export
getCohortCounts <- function(connection = NULL,
                            connect = NULL,
                            databaseIds,
                            resultsDatabaseSchema = NULL) {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  if (!NULL(connect) || !NULL(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logInfo(" \n - No connection or connectionDetails provided.")
      ParallelLogger::logInfo("  Checking if required objects existsin R memory.")
    }
  } else {
    if (exists('cohortCount')) {
      ParallelLogger::logInfo("  'cohort count' data object found in R memory. Continuing.")
    } else {
      ParallelLogger::logWarn("  'cohort count' data object not found in R memory. Exiting.")
      return(NULL)
    }
  }
  
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.cohort_count
            	WHERE database_id in c('@databaseIds');;"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema, 
                                                       databaseIds = databaseIds,
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    data <- cohortCount %>% 
      dplyr::filter(.data$databaseId %in% databaseIds) %>% 
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("No records retrieved for 'cohort count'.")
    return(NULL)
  }
  
databaseIds <- unique(data$databaseId) %>% sort()
table <- data[data$databaseId == databaseIds[1], c("cohortId", "cohortEntries", "cohortSubjects")]
colnames(table)[2:3] <- paste(colnames(table)[2:3], databaseIds[1], sep = "_")
if (length(databaseIds) > 1) {
  for (i in 2:length(databaseIds)) {
    temp <- data[data$databaseId == databaseIds[i], c("cohortId", "cohortEntries", "cohortSubjects")]
    colnames(temp)[2:3] <- paste(colnames(temp)[2:3], databaseIds[i], sep = "_")
    table <- merge(table, temp, all = TRUE)
  }
}
table <- merge(cohort, table, all.x = TRUE)
table$url <- paste0(cohortBaseUrl2(), table$cohortId)
table$cohortName <- paste0("<a href='", table$url, "' target='_blank'>", table$cohortName, "</a>")
table$cohortId <- NULL
table$url <- NULL
table <- table %>% 
  dplyr::arrange(.data$cohortName)
return(table)
}

#' @export
getCompareCohortCharacterization <- function(connection = NULL,
                                             connect = NULL,
                                             cohortId,
                                             comparatorCohortId,
                                             databaseIds,
                                             resultsDatabaseSchema = NULL){
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertInt(x = cohortId, 
                       na.ok = FALSE, 
                       null.ok = FALSE,
                       lower = 0,
                       upper = 2^53,
                       add = errorMessage)
  checkmate::assertInt(x = comparatorCohortId, 
                       na.ok = FALSE, 
                       null.ok = FALSE,
                       lower = 0,
                       upper = 2^53,
                       add = errorMessage)
  checkmate::assertCharacter(x = databaseIds,
                             min.len = 1,
                             any.missing = FALSE,
                             unique = TRUE,
                             add = errorMessage)
  
  if (!NULL(connect) || !NULL(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logInfo(" \n - No connection or connectionDetails provided.")
      ParallelLogger::logInfo("  Checking if required objects existsin R memory.")
    }
  } else {
    if (exists('covariateRef')) {
      ParallelLogger::logInfo("  'Covariate ref' data object found in R memory. Continuing.")
    } else {
      ParallelLogger::logWarn("  'covariate ref' data object not found in R memory. Exiting.")
      return(NULL)
    }
  }
  
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.covariate_value
              WHERE cohort_id = @cohortId
              AND cohort_id = @comparatorCohortId
            	AND database_id = '@databaseIds';"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       cohortId = cohortId,
                                                       databaseIds = databaseIds, 
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    covariate <- covariateRef %>% 
      dplyr::group_by(.data$covariateId) %>% 
      dplyr::slice(1) %>% 
      dplyr::distinct() %>% 
      dplyr::arrange(.data$covariateName) %>% 
      tidyr::tibble()
    
    covs1 <- covariateValue %>% 
      dplyr::filter(.data$cohortId == cohortId,
                    .data$databaseId == databaseIds)
    covs2 <- covariateValue %>% 
      dplyr::filter(.data$cohortId == comparatorCohortId,
                    .data$databaseId == databaseIds)
  }
  
  
  if (cohortId == comparatorCohortId) {
    return(tidyr::tibble())
  }
  
  covs1 <- dplyr::left_join(x = covs1, y = covariate)
  covs2 <- dplyr::left_join(x = covs2, y = covariate)
  
  m <- dplyr::full_join(x = covs1 %>% dplyr::distinct(), 
                        y = covs2 %>% dplyr::distinct(), 
                        by = c("covariateId", "conceptId", "databaseId", "covariateName", "covariateAnalysisId"),
                        suffix = c("1", "2")) %>%
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = 0)),
                  sd = sqrt(.data$sd1^2 + .data$sd2^2),
                  stdDiff = (.data$mean2 - .data$mean1)/.data$sd) %>% 
    dplyr::arrange(-abs(.data$stdDiff))
  
  data <- m %>%
    dplyr::mutate(absStdDiff = abs(.data$stdDiff))
  
  return(data)
  
}


#' @export

getCohortOverLap <- function(connection = NULL,
                             connect,
                             cohortId,
                             comparatorCohortId,
                             databaseIds,
                             resultsDatabaseSchema = NULL){
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertInt(x = cohortId, 
                       na.ok = FALSE, 
                       null.ok = FALSE,
                       lower = 0,
                       upper = 2^53,
                       add = errorMessage)
  checkmate::assertInt(x = comparatorCohortId, 
                       na.ok = FALSE, 
                       null.ok = FALSE,
                       lower = 0,
                       upper = 2^53,
                       add = errorMessage)
  checkmate::assertCharacter(x = databaseIds,
                             min.len = 1,
                             any.missing = FALSE,
                             unique = TRUE,
                             add = errorMessage)
  
  if (!NULL(connect) || !NULL(connection)) {
    checkmate::assertCharacter(x = resultsDatabaseSchema,
                               min.len = 1,
                               max.len = 1,
                               any.missing = FALSE,
                               add = errorMessage)
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  ## set up connection to server
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      ParallelLogger::logInfo(" \n - No connection or connectionDetails provided.")
      ParallelLogger::logInfo("  Checking if required objects existsin R memory.")
    }
  } else {
    if (exists('cohortOverlap')) {
      ParallelLogger::logInfo("  'Cohort overlap' data object found in R memory. Continuing.")
    } else {
      ParallelLogger::logWarn("  'Cohort overlap' data object not found in R memory. Exiting.")
      return(NULL)
    }
  }
  
  if (!is.null(connection)) {
    sql <-   "SELECT *
              FROM  @resultsDatabaseSchema.cohort_overlap
              WHERE target_cohort_id = @cohortId
              AND comparator_cohort_id = @comparatorCohortId
            	AND database_id = '@databaseIds';"
    data <- DatabaseConnector::renderTranslateQuerySql(connection = connection,
                                                       sql = sql,
                                                       resultsDatabaseSchema = resultsDatabaseSchema,
                                                       targetCohortId = cohortId,
                                                       comparatorCohortId = comparatorCohortId,
                                                       databaseIds = databaseIds, 
                                                       snakeCaseToCamelCase = TRUE) %>% 
      tidyr::tibble()
  } else {
    
    data <- cohortOverlap %>% 
      dplyr::filter(.data$targetCohortId == cohortId &
                      .data$comparatorCohortId == comparatorCohortId &
                      .data$databaseId == databaseIds) %>% 
      tidyr::tibble()
  }
  
  if (nrow(data) == 0) {
    return(NULL)
  }else{
    return(data)
  }
}
  