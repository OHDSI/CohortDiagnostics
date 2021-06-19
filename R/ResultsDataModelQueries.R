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
    ) %>%
    tidyr::tibble()
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
#' Returns data from time_series table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble) with results that conform to time series
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsFromTimeSeries <- function(dataSource,
                                     cohortIds = NULL,
                                     databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    dataTableName = "timeSeries"
  )
  return(data)
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
getConceptDetails <- function(dataSource = .GlobalEnv,
                              vocabularyDatabaseSchema = NULL,
                              conceptIds) {
  table <- "concept"
  if (!is.null(vocabularyDatabaseSchema) &&
      is(dataSource, "environment")) {
    warning(
      "vocabularyDatabaseSchema provided for function getConceptDetails in non database mode. This will be ignored."
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
    data <- get(table, envir = dataSource) %>%
      dplyr::filter(.data$conceptId %in% conceptIds)
  } else {
    sql <- "SELECT *
            FROM @vocabulary_database_schema.concept
            WHERE concept_id IN (@concept_ids);"
    if (!is.null(vocabularyDatabaseSchema)) {
      sql <-
        SqlRender::render(
          sql = sql,
          vocabularyDatabaseSchema = !!vocabularyDatabaseSchema
        )
    }
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        vocabulary_database_schema = dataSource$vocabularyDatabaseSchema,
        concept_ids = conceptIds,
        snakeCaseToCamelCase = TRUE
      ) %>%
      tidyr::tibble()
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
getResultsFromTemporalValue <- function(dataSource,
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
getResultsFromTemporalValueDist <- function(dataSource,
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
                    WHERE database_id IN (@databaseIds)
                    	AND cohort_id = @cohortId
                    ORDER BY concept.concept_id;"
    resolved <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sqlResolved,
        results_database_schema = dataSource$resultsDatabaseSchema,
        databaseIds = quoteLiterals(databaseIds),
        cohortId = cohortIds,
        snakeCaseToCamelCase = TRUE
      ) %>%
      tidyr::tibble() %>%
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
                  	WHERE database_id IN (@databaseIds)
                  		AND cohort_id = @cohortId
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
        databaseIds = quoteLiterals(databaseIds),
        cohortId = cohortIds,
        snakeCaseToCamelCase = TRUE
      ) %>%
      tidyr::tibble() %>%
      dplyr::arrange(.data$resolvedConceptId)
  }
  data <- list(resolved = resolved,
               mapped = mapped)
  return(data)
}



#' Returns covariate_value and covariate_value_dist output of feature extraction
#'
#' @description
#' Returns covariate_value and covariate_value_dist output of feature extraction.
#' The covariate_value and temporal_covariate_value are appended with timeId = 0
#' assigned to covariate_value. Similarly, covariate_value_dist and
#' temporal_covariate_value_dist are also appended with timeId = 0 in
#' covariate_value_dist.
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a list object with two data frames (tibble) covariate_value and
#' covariate_value_dist
#'
#' @export
getResultsCovariateValue <- function(dataSource = .GlobalEnv,
                                     cohortIds = NULL,
                                     databaseIds = NULL) {
  covariateValue <-
    getResultsFromCovariateValue(dataSource = dataSource,
                                 cohortIds = cohortIds,
                                 databaseIds = databaseIds)
  covariateValueDist <-
    getResultsFromCovariateValueDist(dataSource = dataSource,
                                     cohortIds = cohortIds,
                                     databaseIds = databaseIds)
  temporalCovariateValue <-
    getResultsFromTemporalValue(dataSource = dataSource,
                                cohortIds = cohortIds,
                                databaseIds = databaseIds)
  # temporary till https://github.com/OHDSI/FeatureExtraction/issues/127
  # temporalCovariateValueDist <- getResultsFromTemporalValueDist(dataSource = dataSource,
  #                                                               cohortIds = cohortIds,
  #                                                               databaseIds = databaseIds)
  temporalCovariateValueDist <- covariateValueDist[0,] %>%
    dplyr::mutate(timeId = 0)
  covariateValue <- dplyr::bind_rows(temporalCovariateValue,
                                     covariateValue %>% dplyr::mutate(timeId = 0))
  covariateValueDist <- dplyr::bind_rows(temporalCovariateValueDist,
                                         covariateValueDist %>% dplyr::mutate(timeId = 0))
  
  data <- list(covariateValue = covariateValue,
               covariateValueDist = covariateValueDist)
  return(data)
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
        tidyr::tibble() %>%
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
        tidyr::tibble() %>%
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
                                   covariateIds) {
  dataTableName <- 'covariateRef'
  if (is(dataSource, "environment")) {
    if (!exists(dataTableName)) {
      return(NULL)
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(dataTableName) %>%
      dplyr::filter(.data$covariateId %in% covariateIds)
  } else {
    sql <- "SELECT *
            FROM @results_database_schema.covariate_ref
            WHERE covariate_id IN (@covariate_ids);"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$results_database_schema,
        covariate_id = covariateIds,
        snakeCaseToCamelCase = TRUE
      ) %>%
      tidyr::tibble()
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}

# not exported
getResultsTemporalCovariateRef <- function(dataSource,
                                           covariateIds) {
  dataTableName <- 'temporalCovariateRef'
  if (is(dataSource, "environment")) {
    if (!exists(dataTableName)) {
      return(NULL)
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(dataTableName) %>%
      dplyr::filter(.data$covariateId %in% covariateIds)
  } else {
    sql <- "SELECT *
            FROM @results_database_schema.temporal_covariate_ref
            WHERE covariate_id IN (@covariate_ids);"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$results_database_schema,
        covariate_id = covariateIds,
        snakeCaseToCamelCase = TRUE
      ) %>%
      tidyr::tibble()
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}
