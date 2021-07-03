# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of ConceptSetDiagnostics
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
#


# this script is shared between Cohort Diagnostics and Diagnostics Explorer

# private function - not exported
camelCaseToTitleCase <- function(string) {
  string <- gsub("([A-Z])", " \\1", string)
  string <- gsub("([a-z])([0-9])", "\\1 \\2", string)
  substr(string, 1, 1) <- toupper(substr(string, 1, 1))
  return(string)
}

# private function - not exported
snakeCaseToCamelCase <- function(string) {
  string <- tolower(string)
  for (letter in letters) {
    string <-
      gsub(paste("_", letter, sep = ""), toupper(letter), string)
  }
  string <- gsub("_([0-9])", "\\1", string)
  return(string)
}

# private function - not exported
camelCaseToSnakeCase <- function(string) {
  string <- gsub("([A-Z])", "_\\1", string)
  string <- tolower(string)
  string <- gsub("([a-z])([0-9])", "\\1_\\2", string)
  return(string)
}

# private function - not exported
quoteLiterals <- function(x) {
  if (is.null(x)) {
    return("")
  } else {
    return(paste0("'", paste(x, collapse = "', '"), "'"))
  }
}

#' Return a database data source object
#'
#' @description
#' Collects a list of objects needed to connect to a database datsource. This includes one of
#' \code{DatabaseConnector::createConnectionDetails} object, or a DBI database connection created
#' using either \code{DatabaseConnector::connection} or \code{pool::dbPool}, and a names of
#' resultsDatabaseSchema and vocabularyDatabaseSchema
#'
#' @template Connection
#'
#' @template VocabularyDatabaseSchema
#'
#' @template resultsDatabaseSchema
#'
#' @return
#' Returns a list with information on database data source
#' @export
createDatabaseDataSource <- function(connection = NULL,
                                     connectionDetails = NULL,
                                     resultsDatabaseSchema,
                                     vocabularyDatabaseSchema = resultsDatabaseSchema) {
  return(
    list(
      connection = connection,
      connectionDetails = connectionDetails,
      resultsDatabaseSchema = resultsDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema
    )
  )
}


#' Return a file data source object
#'
#' @description
#' Given a premerged file (an .RData/rds object the output
#' of \code{CohortDiagnostics::preMergeDiagnosticsFiles} reads the object into
#' memory and makes it available for query.
#'
#' @param premergedDataFile  an .RData/rds object the output
#'                           of \code{CohortDiagnostics::preMergeDiagnosticsFiles}
#'
#' @param envir             (optional) R-environment to read premerged data. By default this is the
#'                          global environment.
#'
#' @return
#' R environment containing data conforming to Cohort Diagnostics results data model specifications.
#' @export
createFileDataSource <-
  function(premergedDataFile, envir = .GlobalEnv) {
    load(premergedDataFile, envir = envir)
    return(envir)
  }


# private function - not exported
# this function supports query with connection that is either pool or not pool. It can also establish a connection
# when connection details is provided
renderTranslateQuerySql <-
  function(sql,
           connection = NULL,
           connectionDetails = NULL,
           ...,
           snakeCaseToCamelCase = FALSE) {
    ## Set up connection to server ----------------------------------------------------
    if (is.null(connection)) {
      if (!is.null(connectionDetails)) {
        writeLines("Connecting to database using provided connection details.")
        connection <- DatabaseConnector::connect(connectionDetails)
        on.exit(DatabaseConnector::disconnect(connection))
      } else {
        stop("No connection or connectionDetails provided.")
      }
    }
    
    if (methods::is(connection, "Pool")) {
      # Connection pool is used by Shiny app, which always uses PostgreSQL:
      sql <- SqlRender::render(sql, ...)
      sql <- SqlRender::translate(sql, targetDialect = "postgresql")
      
      tryCatch({
        data <- DatabaseConnector::dbGetQuery(connection, sql)
      }, error = function(err) {
        writeLines(sql)
        stop(err)
      })
      if (snakeCaseToCamelCase) {
        colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
      }
      return(data %>% dplyr::tibble())
    } else {
      if (is.null(attr(connection, "dbms"))) {
        stop("dbms not provided. Unable to translate query.")
      }
      return(
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          ...,
          snakeCaseToCamelCase = snakeCaseToCamelCase
        ) %>% dplyr::tibble()
      )
    }
  }

# private function - not exported
getDataFromResultsDatabaseSchema <- function(dataSource,
                                             cohortIds = NULL,
                                             databaseIds = NULL,
                                             dataTableName) {
  if (is(dataSource, "environment")) {
    if (!exists(dataTableName, envir = dataSource)) {
      stop(paste0(
        "Please check if ",
        dataTableName,
        " is present in environment."
      ))
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      warning(paste0(dataTableName, " in environment was found to have o rows."))
    }
    data <- get(dataTableName, envir = dataSource)
    if (!is.null(cohortIds)) {
      data <- data %>%
        dplyr::filter(.data$cohortId %in% !!cohortIds)
    }
    if (!is.null(databaseIds)) {
      data <- data %>%
        dplyr::filter(.data$databaseId %in% !!databaseIds)
    }
  } else {
    if (is.null(dataSource$connection)) {
      stop("No connection provided. Unable to query database.")
    }
    
    if (!DatabaseConnector::dbIsValid(dataSource$connection)) {
      stop("Connection to database seems to be closed.")
    }
    
    sql <- "SELECT *
            FROM  @results_database_schema.@data_table
            {@cohort_ids == '' & @database_id !=''} ? { WHERE database_id in (@database_id)}
            {@cohort_ids != '' & @database_id !=''} ? {  WHERE database_id in (@database_id) AND cohort_id in (@cohort_ids)}
            {@cohort_ids != '' & @database_id ==''} ? {  WHERE cohort_id in (@cohort_ids)}
            ;"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        cohort_ids = cohortIds,
        data_table = camelCaseToSnakeCase(dataTableName),
        database_id = quoteLiterals(databaseIds),
        snakeCaseToCamelCase = TRUE
      )
  }
  
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}


#' Returns data from cohort_count table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from cohort_count table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to cohort counts
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromCohortCount <- function(dataSource,
                                      cohortIds = NULL,
                                      databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "cohortCount"
  )
  return(data)
}


#' Returns data from time_series table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from time_series table of Cohort Diagnostics results data model.
#' The returned object is a tsibble, but to use in time series analysis, gaps
#' need to be filled. Only absolute values are returned i.e. negative values are
#' converted to positives.
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a list of tsibble (time series) objects with results that conform to time series
#' table in Cohort Diagnostics results data model. There are three list objects, labelled
#' m for monthly, q for quarterly and y for yearly. The periodBegin variable is in the
#' format of tsibble::yearmonth for monthly, tsibble::yearquarter for quarter and integer
#' for year.
#'
#' @export
getResultsFromTimeSeries <- function(dataSource,
                                     cohortIds = NULL,
                                     databaseIds = NULL) {
  # cohortId = 0, represent all persons in observation_period
  if (!is.null(cohortIds)) {
    cohortIds <- c(cohortIds, 0) %>% unique()
  }
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "timeSeries"
  )
  
  if (nrow(data) > 0) {
    data <- data %>%
      dplyr::mutate(periodBeginRaw = .data$periodBegin)
    
    intervals <- data$calendarInterval %>% unique()
    dataList <- list()
    for (i in (1:length(intervals))) {
      intervalData <- data %>%
        dplyr::filter(.data$calendarInterval == intervals[[i]]) %>%
        dplyr::select(-.data$calendarInterval)
      if (intervals[[i]] == 'y') {
        intervalData <- intervalData %>%
          dplyr::mutate(periodBegin = clock::get_year(.data$periodBegin))
      }
      if (intervals[[i]] == 'q') {
        intervalData <- intervalData %>%
          dplyr::mutate(periodBegin = tsibble::yearquarter(.data$periodBegin))
      }
      if (intervals[[i]] == 'm') {
        intervalData <- intervalData %>%
          dplyr::mutate(periodBegin = tsibble::yearmonth(.data$periodBegin))
      }
      intervalData <- intervalData %>%
        dplyr::relocate(.data$databaseId, .data$cohortId, .data$seriesType) %>%
        dplyr::mutate(
          records = abs(.data$records),
          subjects = abs(.data$subjects),
          personDays = abs(.data$personDays),
          recordsIncidence = abs(.data$recordsIncidence),
          subjectsIncidence = abs(.data$subjectsIncidence),
          recordsTerminate = abs(.data$recordsTerminate),
          subjectsTerminate = abs(.data$subjectsTerminate)
        ) %>%
        tsibble::as_tsibble(
          key = c(.data$databaseId, .data$cohortId, .data$seriesType),
          index = .data$periodBegin
        ) %>%
        dplyr::arrange(.data$databaseId, .data$cohortId, .data$seriesType)
      dataList[[intervals[[i]]]] <- intervalData
    }
  } else {
    return(NULL)
  }
  return(dataList)
}

#' Returns data from time_distribution table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from time_distribution table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to time_distribution
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromTimeDistribution <- function(dataSource,
                                           cohortIds = NULL,
                                           databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "timeDistribution"
  )
  return(data)
}


#' Returns data from incidence_rate table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from incidence_rate table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to incidence_rate
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromIncidenceRate <- function(dataSource,
                                        cohortIds = NULL,
                                        databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "incidenceRate"
  )
  return(data)
}

#' Returns data from calendar_incidence table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from calendar_incidence table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to incidence_rate
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromCalendarIncidence <- function(dataSource,
                                            cohortIds = NULL,
                                            databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "calendarIncidence"
  )
  return(data)
}

#' Returns data from inclusion_rule_stats table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from inclusion_rule_stats table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to inclusion_rule_stats
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromInclusionRuleStatistics <- function(dataSource,
                                                  cohortIds = NULL,
                                                  databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "inclusionRuleStats"
  )
  return(data)
}


#' Returns data from index_event_breakdown table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from index_event_breakdown table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to index_event_breakdown
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromIndexEventBreakdown <- function(dataSource,
                                              cohortIds = NULL,
                                              databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "indexEventBreakdown"
  )
  return(data)
}


#' Returns data from concept table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from concept table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template ConceptIds
#'
#' @template VocabularyDatabaseSchema
#'
#' @return
#' Returns a data frame (tibble) with results that conform to concept
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromConcept <- function(dataSource = .GlobalEnv,
                                  vocabularyDatabaseSchema = NULL,
                                  conceptIds = NULL) {
  table <- "concept"
  if (!is.null(vocabularyDatabaseSchema) &&
      is(dataSource, "environment")) {
    warning(
      "vocabularyDatabaseSchema provided for function getResultsFromConcept in non database mode. This will be ignored."
    )
  }
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    if (nrow(get(table, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(table, envir = dataSource)
    if (!is.null(conceptIds)) {
      data <- data %>%
        dplyr::filter(.data$conceptId %in% conceptIds)
    }
  } else {
    sql <- "SELECT *
            FROM @vocabulary_database_schema.concept
            {@concept_ids == ''} ? { WHERE concept_id IN (@concept_ids)};"
    if (!is.null(vocabularyDatabaseSchema)) {
      sql <-
        SqlRender::render(
          sql = sql,
          vocabulary_database_schema = !!vocabularyDatabaseSchema
        )
    }
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
        concept_ids = conceptIds,
        snakeCaseToCamelCase = TRUE
      )
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}


#' Returns data from visit_context table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from visit_context table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to visit_context
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromVisitContext <- function(dataSource,
                                       cohortIds = NULL,
                                       databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "visitContext"
  )
  return(data)
}


#' Returns data from included_concept table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from included_concept table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to included_concept
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromIncludedConcept <- function(dataSource,
                                          cohortIds = NULL,
                                          databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "includedSourceConcept"
  )
  return(data)
}


#' Returns data from orphan_concept table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from orphan_concept table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to orphan_concept
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromOrphanConcept <- function(dataSource,
                                        cohortIds = NULL,
                                        databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "orphanConcept"
  )
  return(data)
}

#' Returns data from cohort_relationships table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from cohort_relationships table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template cohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to cohort_relationships
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromCohortRelationships <- function(dataSource,
                                              cohortIds = NULL,
                                              databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "cohortRelationships"
  )
  return(data)
}


# not exported
getResultsFromCovariateValue <- function(dataSource,
                                         cohortIds,
                                         databaseIds) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "covariateValue"
  )
  return(data)
}

# not exported
getResultsFromCovariateValueDist <- function(dataSource,
                                             cohortIds,
                                             databaseIds) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "covariateValueDist"
  )
  return(data)
}

# not exported
getResultsFromTemporalCovariateValue <- function(dataSource,
                                                 cohortIds,
                                                 databaseIds) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "temporalCovariateValue"
  )
  return(data)
}

# not exported
getResultsFromTemporalCovariateValueDist <- function(dataSource,
                                                     cohortIds,
                                                     databaseIds) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "temporalCovariateValueDist"
  )
  return(data)
}

# not exported
getResultsFromResolvedConcepts <- function(dataSource,
                                           cohortIds,
                                           databaseIds) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "resolvedConcepts"
  )
  return(data)
}


#' Returns resolved and mapped concepts for concept set expression in a cohort
#'
#' @description
#' Returns a list object with resolved and mapped concepts for all concept sets in one
#' cohort from one or more data sources. This is being returned from the results data model
#' of Cohort Diagnostics and is precomputed.
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a list object with resolved and mapped concepts for all concept sets in one
#' cohort from one or more data sources. There will be two objects in the returned
#' list object resolved, mapped - each will be tibble.
#'
#' @export
getResultsResolveMappedConceptSet <- function(dataSource,
                                              databaseIds = NULL,
                                              cohortIds = NULL) {
  table <- "resolvedConcepts"
  if (is(dataSource, "environment")) {
    if (!exists(table)) {
      return(NULL)
    }
    if (length(table) == 0) {
      return(NULL)
    }
    resolved <- get(table, envir = dataSource)
    if (any(is.null(resolved), nrow(resolved) == 0)) {
      return(NULL)
    }
    if (!is.null(databaseIds)) {
      resolved <- resolved %>%
        dplyr::filter(.data$databaseId %in% !!databaseIds)
    }
    if (!is.null(cohortIds)) {
      resolved <- resolved %>%
        dplyr::filter(.data$cohortId == !!cohortIds)
    }
    if (any(is.null(resolved), nrow(resolved) == 0)) {
      return(NULL)
    }
    resolved <- resolved %>%
      dplyr::inner_join(get("concept"), by = "conceptId") %>%
      dplyr::distinct() %>%
      dplyr::arrange(.data$conceptId)
    if (exists("conceptRelationship")) {
      mapped <- resolved %>%
        dplyr::select(.data$conceptId,
                      .data$databaseId,
                      .data$cohortId,
                      .data$conceptSetId) %>%
        dplyr::distinct() %>%
        dplyr::inner_join(get("conceptRelationship"),
                          by = c("conceptId" = "conceptId2")) %>%
        dplyr::filter(.data$relationshipId == "Maps to") %>%
        dplyr::filter(is.na(.data$invalidReason)) %>%
        dplyr::select(
          .data$conceptId,
          .data$conceptId1,
          .data$databaseId,
          .data$cohortId,
          .data$conceptSetId
        ) %>%
        dplyr::rename(resolvedConceptId = .data$conceptId) %>%
        dplyr::inner_join(get("concept"), by = c("conceptId1" = "conceptId")) %>%
        dplyr::filter(is.na(.data$invalidReason)) %>%
        dplyr::rename(conceptId = .data$conceptId1) %>%
        dplyr::select(
          .data$resolvedConceptId,
          .data$conceptId,
          .data$conceptName,
          .data$domainId,
          .data$vocabularyId,
          .data$conceptClassId,
          .data$standardConcept,
          .data$conceptCode,
          .data$databaseId,
          .data$conceptSetId
        ) %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$resolvedConceptId, .data$conceptId)
      
      if (nrow(mapped) == 0) {
        mapped <- NULL
      }
    } else {
      mapped <- NULL
    }
  } else {
    sqlResolved <- "SELECT DISTINCT resolved_concepts.cohort_id,
                    	resolved_concepts.concept_set_id,
                    	concept.concept_id,
                    	concept.concept_name,
                    	concept.domain_id,
                    	concept.vocabulary_id,
                    	concept.concept_class_id,
                    	concept.standard_concept,
                    	concept.concept_code,
                    	resolved_concepts.database_id
                    FROM @results_database_schema.resolved_concepts
                    INNER JOIN @results_database_schema.concept
                    ON resolved_concepts.concept_id = concept.concept_id
                    {@cohort_id == '' & @database_id !=''} ? { WHERE database_id in (@database_id)}
                    {@cohort_id != '' & @database_id !=''} ? { WHERE database_id in (@database_id) AND cohort_id in (@cohort_id)}
                    {@cohort_id != '' & @database_id ==''} ? { WHERE cohort_id in (@cohort_id)}
                    ORDER BY concept.concept_id;"
    
    
    
    resolved <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sqlResolved,
        results_database_schema = dataSource$resultsDatabaseSchema,
        database_id = quoteLiterals(databaseIds),
        cohort_id = cohortIds,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::arrange(.data$conceptId)
    sqlMapped <-
      "SELECT DISTINCT concept_sets.concept_id AS resolved_concept_id,
                  	concept.concept_id,
                  	concept.concept_name,
                  	concept.domain_id,
                  	concept.vocabulary_id,
                  	concept.concept_class_id,
                  	concept.standard_concept,
                  	concept.concept_code,
                  	concept_sets.database_id,
                  	concept_sets.concept_set_id
                  FROM (
                  	SELECT DISTINCT concept_id, database_id, concept_set_id
                  	FROM @results_database_schema.resolved_concepts
                    {@cohort_id == '' & @database_id !=''} ? { WHERE database_id in (@database_id)}
                    {@cohort_id != '' & @database_id !=''} ? { WHERE database_id in (@database_id) AND cohort_id in (@cohort_id)}
                    {@cohort_id != '' & @database_id ==''} ? { WHERE cohort_id in (@cohort_id)}
                  	) concept_sets
                  INNER JOIN @results_database_schema.concept_relationship ON concept_sets.concept_id = concept_relationship.concept_id_2
                  INNER JOIN @results_database_schema.concept ON concept_relationship.concept_id_1 = concept.concept_id
                  WHERE relationship_id = 'Maps to'
                  	AND standard_concept IS NULL
                  ORDER BY concept.concept_id;"
    mapped <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sqlMapped,
        results_database_schema = dataSource$resultsDatabaseSchema,
        database_id = quoteLiterals(databaseIds),
        cohort_id = cohortIds,
        snakeCaseToCamelCase = TRUE
      ) %>%
      dplyr::arrange(.data$resolvedConceptId)
  }
  data <- list(resolved = resolved,
               mapped = mapped)
  return(data)
}


#' Returns cohort characterization output of feature extraction
#'
#' @description
#' Returns a list object with covariateValue, covariateValueDist,
#' covariateRef, analysisRef output of feature extraction.
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a list object with covariateValue, covariateValueDist,
#' covariateRef, analysisRef output of feature extraction along with
#' concept information.
#'
#' @export
getCohortCharacterizationResults <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL) {
    analysisRef <- getResultsAnalysisRef(dataSource = dataSource)
    covariateRef <- getResultsCovariateRef(dataSource = dataSource)
    concept <- getResultsFromConcept(dataSource = dataSource,
                                     conceptIds = covariateRef$conceptId %>% unique())
    covariateValue <-
      getResultsFromCovariateValue(dataSource = dataSource,
                                   cohortIds = cohortIds,
                                   databaseIds = databaseIds)
    covariateValueDist <-
      getResultsFromCovariateValueDist(dataSource = dataSource,
                                       cohortIds = cohortIds,
                                       databaseIds = databaseIds)
    return(
      list(
        analysisRef = analysisRef,
        covariateRef = covariateRef,
        covariateValue = covariateValue,
        covariateValueDist = covariateValueDist,
        concept = concept
      )
    )
  }



#' Returns temporal cohort characterization output of feature extraction
#'
#' @description
#' Returns a list object with temporalCovariateValue, temporalCovariateValueDist,
#' temporalCovariateRef, temporalAnalysisRef, temporalRef output of feature 
#' extraction along with concept information.
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a list object with temporalCovariateValue, temporalCovariateValueDist,
#' temporalCovariateRef, temporalAnalysisRef, temporalTimeRef, Concept output of feature extraction.
#'
#' @export
getTemporalCohortCharacterizationResults <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL) {
    temporalAnalysisRef <-
      getResultsTemporalAnalysisRef(dataSource = dataSource)
    temporalCovariateRef <-
      getResultsTemporalCovariateRef(dataSource = dataSource)
    temporalTimeRef <- getResultsTemporalTimeRef(dataSource = dataSource)
    concept <- getResultsFromConcept(dataSource = dataSource,
                                     conceptIds = temporalCovariateRef$conceptId %>% unique())
    temporalCovariateValue <-
      getResultsFromTemporalCovariateValue(dataSource = dataSource,
                                           cohortIds = cohortIds,
                                           databaseIds = databaseIds)
    # temporary till https://github.com/OHDSI/FeatureExtraction/issues/127
    # temporalCovariateValueDist <- getResultsFromTemporalValueDist(dataSource = dataSource,
    #                                                               cohortIds = cohortIds,
    #                                                               databaseIds = databaseIds)
    
    temporalCovariateValueDist <- NULL
    
    return(
      list(
        temporalAnalysisRef = temporalAnalysisRef,
        temporalCovariateRef = temporalCovariateRef,
        temporalTimeRef = temporalTimeRef,
        temporalCovariateValue = temporalCovariateValue,
        temporalCovariateValueDist = temporalCovariateValueDist,
        concept = concept
      )
    )
  }


#' Returns cohort as feature characterization
#'
#' @description
#' Returns a list object with covariateValue,
#' covariateRef, analysisRef output of cohort as features.
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a list object with covariateValue,
#' covariateRef, analysisRef output of cohort as features. To avoid clash
#' with covaraiteId and conceptId returned from Feature Extraction
#' the output is a negative integer.
#'
#' @export
getCohortAsFeatureCharacterizationResults <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL) {
    # meta information
    cohortCounts <-
      getResultsFromCohortCount(dataSource = dataSource,
                                cohortIds = cohortIds,
                                databaseIds = databaseIds)
    cohort <- getResultsCohort(dataSource = dataSource)
    
    cohortRelationships <-
      getResultsFromCohortRelationships(dataSource = dataSource,
                                        cohortIds = cohortIds,
                                        databaseIds = databaseIds)

    # comparator cohort was on or after target cohort
    summarizeCohortRelationship <- function(data,
                                            startDay = NULL,
                                            endDay = NULL,
                                            incidentTarget = FALSE,
                                            incidentComparator = FALSE,
                                            analysisId,
                                            cohortCounts) {
      
      if (is.null(data) || nrow(data) == 0) {return(NULL)}
      if (incidentTarget) {
        data <- data %>% 
          dplyr::filter(.data$relationshipType == '1A') %>% 
          dplyr::select(-.data$relationshipType)
      } else {
        data <- data %>% 
          dplyr::filter(.data$relationshipType == 'AA') %>% 
          dplyr::select(-.data$relationshipType)
      }
      
      data <- data %>% 
        dplyr::filter(.data$startDay >= 0)
      
      if (is.null(data) || nrow(data) == 0) {return(NULL)}
      
      if (!is.null(startDay) && !is.na(startDay) && length(startDay) > 0) {
        data <- data %>%
          dplyr::filter(.data$startDay <= !!startDay)
      }
      
      if (!is.null(endDay) && !is.na(endDay) && length(endDay) > 0) {
        data <- data %>%
          dplyr::filter(.data$endDay >= !!endDay)
      }
      
      data <- data %>%
        dplyr::select(-.data$startDay, -.data$endDay) %>%
        dplyr::group_by(.data$databaseId,
                        .data$cohortId,
                        .data$comparatorCohortId) %>%
        dplyr::summarise(sumValue = sum(.data$countValue),
                         .groups = 'keep') %>%
        dplyr::ungroup()
      
      if (nrow(cohortCounts) > 0) {
        if (incidentTarget) {
          cohortCounts <- cohortCounts %>%
            dplyr::select(.data$cohortId,
                          .data$databaseId,
                          .data$cohortSubjects)
          data <- suppressWarnings(data %>%
                                     dplyr::inner_join(cohortCounts, by = c('databaseId', 'cohortId')) %>%
                                     dplyr::mutate(mean = .data$sumValue / .data$cohortSubjects) %>%
                                     dplyr::mutate(sd = sqrt(.data$mean * (1 - .data$mean))))
        } else {
          cohortCounts <- cohortCounts %>%
            dplyr::select(.data$cohortId,
                          .data$databaseId,
                          .data$cohortEntries)
          data <- suppressWarnings(data %>%
                                     dplyr::inner_join(cohortCounts, by = c('databaseId', 'cohortId')) %>%
                                     dplyr::mutate(mean = .data$sumValue / .data$cohortEntries) %>%
                                     dplyr::mutate(sd = sqrt(.data$mean * (1 - .data$mean))))
        }
      }
      
      data <- data %>%
        dplyr::mutate(analysisId = !!analysisId) %>%
        dplyr::mutate(covariateId = (.data$comparatorCohortId*-1000)-999+!!analysisId) %>%
        dplyr::select(
          .data$cohortId,
          .data$covariateId,
          .data$sumValue,
          .data$mean,
          .data$sd,
          .data$databaseId
        )
      return(data)
    }
    
    analysisId <- c(-1,-2,-3,-4,-5,-6,-7,-8,-9,-10,-11,-12,-13,-14,-15,-16)
    analysisName <- c("ComparatorCohortStartAnyTimePrior","ComparatorCohortStartLongTerm","ComparatorCohortStartMediumTerm","ComparatorCohortStartShortTerm","ComparatorIncidentCohortStartAnyTimePrior","ComparatorIncidentCohortStartLongTerm","ComparatorIncidentCohortStartMediumTerm","ComparatorIncidentCohortStartShortTerm","ComparatorCohortStartAnyTimePriorIncidentTarget","ComparatorCohortStartLongTermIncidentTarget","ComparatorCohortStartMediumTermIncidentTarget","ComparatorCohortStartShortTermIncidentTarget","ComparatorIncidentCohortStartAnyTimePriorIncidentTarget","ComparatorIncidentCohortStartLongTermIncidentTarget","ComparatorIncidentCohortStartMediumTermIncidentTarget","ComparatorIncidentCohortStartShortTermIncidentTarget")
    startDay <- c(NA,365,180,30,NA,365,180,30,NA,365,180,30,NA,365,180,30)
    endDay <- c(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
    description <- c("Count of distinct comparator cohort start dates any time prior to index.",
                     "Count of distinct comparator cohort start dates in long term period prior to index.",
                     "Count of distinct comparator cohort start dates in medium term period prior to index.",
                     "Count of distinct comparator cohort start dates in short term period prior to index.",
                     "Count of distinct incident comparator cohort start dates any time prior to index.",
                     "Count of distinct incident comparator cohort start dates in long term period prior to index.",
                     "Count of distinct incident comparator cohort start dates in medium term period prior to index.",
                     "Count of distinct incident comparator cohort start dates in short term period prior to index.",
                     "Count of distinct comparator cohort start dates any time prior to index date of incident target.",
                     "Count of distinct comparator cohort start dates in long term period prior to index date of incident target.",
                     "Count of distinct comparator cohort start dates in medium term period prior to index date of incident target.",
                     "Count of distinct comparator cohort start dates in short term period prior to index date of incident target.",
                     "Count of distinct incident comparator cohort start dates any time prior to index date of incident target.",
                     "Count of distinct incident comparator cohort start dates in long term period prior to index date of incident target.",
                     "Count of distinct incident comparator cohort start dates in medium term period prior to index date of incident target.",
                     "Count of distinct incident comparator cohort start dates in short term period prior to index date of incident target.")
    incidentTarget <- c(FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,FALSE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE)
    incidentComparator <- c(FALSE,FALSE,FALSE,FALSE,FALSE,TRUE,TRUE,TRUE,FALSE,FALSE,FALSE,FALSE,TRUE,TRUE,TRUE,TRUE)
    
    analysisRef <- dplyr::tibble(analysisId, analysisName, startDay, endDay, description, incidentTarget, incidentComparator) %>% 
      dplyr::mutate(isBinary = 'Y',
                    missingMeansZero = 'Y') %>% 
      dplyr::arrange(.data$analysisId)
    
    result <- list()
    for (j in (1:nrow(analysisRef))) {
      result[[j]] <- summarizeCohortRelationship(data = cohortRelationships,
                                                 startDay = analysisRef[j,]$startDay,
                                                 endDay = analysisRef[j,]$endDay,
                                                 incidentTarget = analysisRef[j,]$incidentTarget,
                                                 incidentComparator = analysisRef[j,]$incidentComparator,
                                                 analysisId = analysisRef[j,]$analysisId,
                                                 cohortCounts = cohortCounts)
    }
    result <- dplyr::bind_rows(result)
    
    if (nrow(result) == 0) {
      result <- NULL
    }
    
    covariateRef <- tidyr::crossing(cohort,
                                    analysisRef %>% 
                                      dplyr::select(.data$analysisId, 
                                                    .data$description)) %>% 
      dplyr::mutate(covariateId = (.data$cohortId*-1000)-999+!!analysisId) %>% 
      dplyr::mutate(covariateName = paste0(.data$cohortName, "(", .data$covariateId, ")")) %>% 
      # dplyr::mutate(covariateName = paste0(.data$description, .data$cohortName)) %>% 
      dplyr::mutate(conceptId = .data$cohortId * -1) %>% 
      dplyr::select(-.data$description) %>% 
      dplyr::arrange(.data$covariateId) %>% 
      dplyr::select(.data$covariateId,
                    .data$covariateName,
                    .data$analysisId,
                    .data$conceptId)
    
    concept <- cohort %>% 
      dplyr::filter(.data$cohortId %in% abs(covariateRef$conceptId) %>% unique()) %>% 
      dplyr::mutate(conceptId = .data$cohortId * -1,
                    conceptName = .data$cohortName,
                    domainId = 'Cohort',
                    vocabularyId = 'Cohort',
                    conceptClassId = 'Cohort',
                    standardConcept = 'S',
                    conceptCode = as.character(.data$cohortId),
                    validStartDate = as.Date('2002-01-31'),
                    validEndDate = as.Date('2099-12-31'),
                    invalidReason = as.character(NA)) %>% 
      dplyr::select(.data$conceptId, .data$conceptName, .data$domainId,
                    .data$vocabularyId, .data$conceptClassId, .data$standardConcept,
                    .data$conceptCode, .data$validStartDate, .data$validEndDate) %>% 
      dplyr::arrange(.data$conceptId)
    
    return(
      list(
        covariateRef = covariateRef,
        covariateValue = result,
        covariateValueDist = NULL,
        analysisRef = analysisRef,
        concept = concept
      )
    )
  }

#' Returns cohort temporal feature characterization
#'
#' @description
#' Returns a list object with temporalCovariateValue,
#' temporalCovariateRef, temporalAnalysisRef, temporalRef output of cohort as features.
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a list object with temporalCovariateValue,
#' temporalCovariateRef, temporalAnalysisRef, temporalRef output of cohort as features.
#'
#' @export
getCohortAsFeatureTemporalCharacterizationResults <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL) {
    # meta information
    cohortCounts <-
      getResultsFromCohortCount(dataSource = dataSource,
                                cohortIds = cohortIds,
                                databaseIds = databaseIds)
    cohort <- getResultsCohort(dataSource = dataSource)
    cohortCovariateRef <- cohort %>%
      dplyr::mutate(
        covariateId = .data$cohortId,
        covariateName = .data$cohortName,
        analysisId = 0,
        conceptId = 0,
        typeCovariate = 3
      ) %>%
      dplyr::select(
        .data$covariateId,
        .data$covariateName,
        .data$analysisId,
        .data$conceptId,
        .data$covariateType
      )
    
  }


#' Returns multiple characterization output
#'
#' @description
#' Returns multiple characterization output
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns multiple characterization output
#'
#' @export
getMultipleCharacterizationResults <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL) {
    
    addCharacterizationSource <- function(x, characterizationSourceValue) {
      exepectedDataTables <- c('analysisRef', 'covariateRef', 'covariateValue', 'covariateValueDist', 'concept',
                               'temporalAnalysisRef', 'temporalCovariateRef', 'temporalCovariateValue', 
                               'temporalCovariateValueDist')
      for (i in (1:length(exepectedDataTables))) {
        if (exepectedDataTables[[i]] %in% names(x)) {
          if (!is.null(x[[exepectedDataTables[[i]]]])) {
            x[[exepectedDataTables[[i]]]] <- x[[exepectedDataTables[[i]]]] %>% 
              dplyr::mutate(characterizationSource = as.integer(!!characterizationSourceValue))
          }
        }
      }
      return(x)
    }
    
    featureExtractioncharacterization <-
      getCohortCharacterizationResults(dataSource = dataSource,
                                       cohortIds = cohortIds,
                                       databaseIds = databaseIds)
    featureExtractioncharacterization <- addCharacterizationSource(x = featureExtractioncharacterization, 
                                                                   characterizationSourceValue = 1)
    
    if (!is.null(featureExtractioncharacterization$covariateValue)) {
      featureExtractioncharacterization$covariateValue <- featureExtractioncharacterization$covariateValue %>% 
        dplyr::mutate(timeId = 0)
    }
    
    featureExtractionTemporalcharacterization <-
      getTemporalCohortCharacterizationResults(dataSource = dataSource,
                                               cohortIds = cohortIds,
                                               databaseIds = databaseIds)
    featureExtractionTemporalcharacterization <- addCharacterizationSource(x = featureExtractionTemporalcharacterization, 
                                                                           characterizationSourceValue = 2)
    
    cohortAsFeatureCharacterizationResults <-
      getCohortAsFeatureCharacterizationResults(dataSource = dataSource,
                                                cohortIds = cohortIds,
                                                databaseIds = databaseIds)
    cohortAsFeatureCharacterizationResults <- addCharacterizationSource(x = cohortAsFeatureCharacterizationResults, 
                                                                        characterizationSourceValue = 3)
    if (!is.null(cohortAsFeatureCharacterizationResults$covariateValue)) {
      cohortAsFeatureCharacterizationResults$covariateValue <- cohortAsFeatureCharacterizationResults$covariateValue %>% 
        dplyr::mutate(timeId = 0)
    }
    
    # cohortAsFeatureTemporalCharacterizationResults <-
    #   getCohortAsFeatureTemporalCharacterizationResults(dataSource = dataSource,
    #                                                     cohortIds = cohortIds,
    #                                                     databaseIds = databaseIds)
    # cohortAsFeatureTemporalCharacterizationResults <- addCharacterizationSource(x = cohortAsFeatureTemporalCharacterizationResults, 
    #                                                                             characterizationSourceValue = 4)
    
    
    analysisRef <- dplyr::bind_rows(featureExtractioncharacterization$analysisRef,
                                    featureExtractionTemporalcharacterization$temporalAnalysisRef,
                                    cohortAsFeatureCharacterizationResults$analysisRef %>% 
                                      dplyr::mutate(domainId = 'Cohort') %>% 
                                      dplyr::select(.data$analysisId,
                                                    .data$analysisName,
                                                    .data$domainId,
                                                    .data$startDay,
                                                    .data$endDay,
                                                    .data$isBinary,
                                                    .data$missingMeansZero,
                                                    .data$characterizationSource)
    )
    if (nrow(analysisRef) == 0) {
      analysisRef <- NULL
    }
    
    covariateRef <- dplyr::bind_rows(featureExtractioncharacterization$covariateRef,
                                     featureExtractionTemporalcharacterization$temporalCovariateRef,
                                     cohortAsFeatureCharacterizationResults$covariateRef
    )
    if (nrow(covariateRef) == 0) {
      covariateRef <- NULL
    }
    
    covariateValue <- dplyr::bind_rows(featureExtractioncharacterization$covariateValue,
                                       featureExtractionTemporalcharacterization$temporalCovariateValue,
                                       cohortAsFeatureCharacterizationResults$covariateValue
    )
    if (nrow(covariateValue) == 0) {
      covariateValue <- NULL
    }
    
    covariateValueDist <- dplyr::bind_rows(featureExtractioncharacterization$covariateValueDist,
                                           featureExtractionTemporalcharacterization$temporalCovariateValueDist,
                                           cohortAsFeatureCharacterizationResults$covariateValueDist
    )
    if (nrow(covariateValueDist) == 0) {
      covariateValueDist <- NULL
    }
    
    concept <- dplyr::bind_rows(featureExtractioncharacterization$concept,
                                featureExtractionTemporalcharacterization$concept,
                                cohortAsFeatureCharacterizationResults$concept
    )
    if (nrow(concept) == 0) {
      concept <- NULL
    }
    
    return(list(analysisRef = analysisRef,
                covariateRef = covariateRef, 
                covariateValue = covariateValue,
                covariateValueDist = covariateValueDist,
                concept = concept))
  }


resolveMappedConceptSetFromVocabularyDatabaseSchema <-
  function(dataSource = .GlobalEnv,
           conceptSets,
           vocabularyDatabaseSchema = "vocabulary") {
    if (is(dataSource, "environment")) {
      stop("Cannot resolve concept sets without a database connection")
    } else {
      sqlBase <-
        paste(
          "SELECT DISTINCT codeset_id AS concept_set_id, concept.*",
          "FROM (",
          paste(conceptSets$conceptSetSql, collapse = ("\nUNION ALL\n")),
          ") concept_sets",
          sep = "\n"
        )
      sqlResolved <- paste(
        sqlBase,
        "INNER JOIN @vocabulary_database_schema.concept",
        "  ON concept_sets.concept_id = concept.concept_id;",
        sep = "\n"
      )
      
      sqlBaseMapped <-
        paste(
          "SELECT DISTINCT codeset_id AS concept_set_id,
                           concept_sets.concept_id AS resolved_concept_id,
                           concept.*",
          "FROM (",
          paste(conceptSets$conceptSetSql, collapse = ("\nUNION ALL\n")),
          ") concept_sets",
          sep = "\n"
        )
      sqlMapped <- paste(
        sqlBaseMapped,
        "INNER JOIN @vocabulary_database_schema.concept_relationship",
        "  ON concept_sets.concept_id = concept_relationship.concept_id_2",
        "INNER JOIN @vocabulary_database_schema.concept",
        "  ON concept_relationship.concept_id_1 = concept.concept_id",
        "WHERE relationship_id = 'Maps to'",
        "  AND standard_concept IS NULL;",
        sep = "\n"
      )
      
      resolved <-
        renderTranslateQuerySql(
          connection = dataSource$connection,
          sql = sqlResolved,
          vocabulary_database_schema = vocabularyDatabaseSchema,
          snakeCaseToCamelCase = TRUE
        ) %>%
        dplyr::select(
          .data$conceptSetId,
          .data$conceptId,
          .data$conceptName,
          .data$domainId,
          .data$vocabularyId,
          .data$conceptClassId,
          .data$standardConcept,
          .data$conceptCode,
          .data$invalidReason
        ) %>%
        dplyr::arrange(.data$conceptId)
      mapped <-
        renderTranslateQuerySql(
          connection = dataSource$connection,
          sql = sqlMapped,
          vocabulary_database_schema = vocabularyDatabaseSchema,
          snakeCaseToCamelCase = TRUE
        ) %>%
        dplyr::select(
          .data$resolvedConceptId,
          .data$conceptId,
          .data$conceptName,
          .data$domainId,
          .data$vocabularyId,
          .data$conceptClassId,
          .data$standardConcept,
          .data$conceptCode,
          .data$conceptSetId
        ) %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$resolvedConceptId, .data$conceptId)
    }
    data <- list(resolved = resolved, mapped = mapped)
    return(data)
  }

# not exported
getResultsCovariateRef <- function(dataSource,
                                   covariateIds = NULL) {
  dataTableName <- 'covariateRef'
  if (is(dataSource, "environment")) {
    if (!exists(dataTableName)) {
      return(NULL)
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(dataTableName)
    if (!is.null(covariateIds)) {
      data <- data %>%
        dplyr::filter(.data$covariateId %in% covariateIds)
    }
  } else {
    sql <- "SELECT *
            FROM @results_database_schema.covariate_ref
            {@covariate_ids == ''} ? { WHERE covariate_id IN (@covariate_ids)}
            ;"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$results_database_schema,
        covariate_id = covariateIds,
        snakeCaseToCamelCase = TRUE
      )
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}

# not exported
getResultsTemporalCovariateRef <- function(dataSource,
                                           covariateIds = NULL) {
  dataTableName <- 'temporalCovariateRef'
  if (is(dataSource, "environment")) {
    if (!exists(dataTableName)) {
      return(NULL)
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(dataTableName)
    if (!is.null(covariateIds)) {
      data <- data %>%
        dplyr::filter(.data$covariateId %in% covariateIds)
    }
  } else {
    sql <- "SELECT *
            FROM @results_database_schema.temporal_covariate_ref
            {@covariate_ids == ''} ? { WHERE covariate_id IN (@covariate_ids)};"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$results_database_schema,
        covariate_id = covariateIds,
        snakeCaseToCamelCase = TRUE
      )
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}


# not exported
getResultsTemporalTimeRef <- function(dataSource) {
  dataTableName <- 'temporalTimeRef'
  if (is(dataSource, "environment")) {
    if (!exists(dataTableName)) {
      return(NULL)
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(dataTableName)
  } else {
    sql <- "SELECT *
            FROM @results_database_schema.temporal_time_ref;"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$results_database_schema,
        snakeCaseToCamelCase = TRUE
      )
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}


# not exported
getResultsCohort <- function(dataSource) {
  dataTableName <- 'cohort'
  if (is(dataSource, "environment")) {
    if (!exists(dataTableName)) {
      return(NULL)
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(dataTableName)
  } else {
    sql <- "SELECT *
            FROM @results_database_schema.cohort;"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$results_database_schema,
        snakeCaseToCamelCase = TRUE
      )
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}



# not exported
getResultsAnalysisRef <- function(dataSource) {
  dataTableName <- 'analysisRef'
  if (is(dataSource, "environment")) {
    if (!exists(dataTableName)) {
      return(NULL)
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(dataTableName)
  } else {
    sql <- "SELECT *
            FROM @results_database_schema.analysisRef;"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$results_database_schema,
        snakeCaseToCamelCase = TRUE
      )
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}


# not exported
getResultsTemporalAnalysisRef <- function(dataSource) {
  dataTableName <- 'temporalAnalysisRef'
  if (is(dataSource, "environment")) {
    if (!exists(dataTableName)) {
      return(NULL)
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(dataTableName)
  } else {
    sql <- "SELECT *
            FROM @results_database_schema.temporalAnalysisRef;"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$results_database_schema,
        snakeCaseToCamelCase = TRUE
      )
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}
