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

# general functions ----
# this script is shared between Cohort Diagnostics and Diagnostics Explorer


# private function - not exported
hasData <- function(data) {
  if (is.null(data)) {
    return(FALSE)
  }
  if (is.data.frame(data)) {
    if (nrow(data) == 0) {
      return(FALSE)
    }
  }
  if (!is.data.frame(data)) {
    if (length(data) == 0) {
      return(FALSE)
    }
    if (length(data) == 1) {
      if (is.na(data)) {
        return(FALSE)
      }
      if (data == "") {
        return(FALSE)
      }
    }
  }
  return(TRUE)
}

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

is.date <- function(x) {
  inherits(x, c("Date", "POSIXt"))
}

# private function - not exported
titleCaseToCamelCase <- function(string) {
  string <- stringr::str_replace_all(string = string,
                                     pattern = ' ',
                                     replacement = '')
  substr(string, 1, 1) <- tolower(substr(string, 1, 1))
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


#' Get specifications for Cohort Diagnostics results data model
#'
#' @param versionNumber Which version of Cohort Diagnostics. Default will be the most recent version.
#'
#' @param packageName e.g. 'CohortDiagnostics'
#'
#' @return
#' A tibble data frame object with specifications
#'
#' @export
getResultsDataModelSpecifications <- function(versionNumber = NULL,
                                              packageName = NULL) {
  if (is.null(packageName)) {
    if (file.exists("resultsDataModelSpecification.csv")) {
      resultsDataModelSpecifications <-
        readr::read_csv("resultsDataModelSpecification.csv",
                        col_types = readr::cols())
      ParallelLogger::logTrace(
        paste0(
          "  - Retrieved results data model specifications from package ",
          packageName
        )
      )
    } else {
      stop("Can't find resultsDataModelSpecifications file.")
    }
  } else {
    pathToCsv <-
      system.file("settings",
                  "resultsDataModelSpecification.csv",
                  package = packageName)
    if (!pathToCsv == "") {
      resultsDataModelSpecifications <-
        readr::read_csv(file = pathToCsv, col_types = readr::cols())
    } else {
      stop(
        paste0(
          "resultsDataModelSpecification.csv was not found in installed package: ",
          packageName
        )
      )
    }
  }
  
  #get various version options in csv file
  versions <- resultsDataModelSpecifications$version %>% unique()
  if (!is.null(versionNumber)) {
    if (versionNumber %in% versions) {
      ParallelLogger::logTrace(paste0(
        "  - Retrieving data model specifications for version ",
        version
      ))
      resultsDataModelSpecifications <-
        resultsDataModelSpecifications %>%
        dplyr::filter(.data$version == !!versionNumber)
    } else {
      stop(
        paste0(
          "version requested",
          versionNumber,
          " not found. The available option are ",
          paste0(versions, collapse = ", ")
        )
      )
    }
  } else {
    #max version/recent version if no version provided
    versions <- max(as.numeric(versions))
    ParallelLogger::logTrace(paste0(
      "  - Retrieving data model specifications for version ",
      versions
    ))
    resultsDataModelSpecifications <-
      resultsDataModelSpecifications %>%
      dplyr::filter(.data$version == !!versions)
  }
  return(resultsDataModelSpecifications)
}


# Connections and query ----
#' Return a database data source object
#'
#' @description
#' Collects a list of objects needed to connect to a database dataSource. This includes one of
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
    if (all(is.null(connectionDetails),
            is.null(connection))) {
      stop('Please provide either connection or connectionDetails to connect to database.')
    }
    ## Set up connection to server
    if (is.null(connection)) {
      if (!is.null(connectionDetails)) {
        writeLines("Connecting to database using provided connection details.")
        connection <- DatabaseConnector::connect(connectionDetails)
        on.exit(DatabaseConnector::disconnect(connection))
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
      data <-
        suppressWarnings(
          DatabaseConnector::renderTranslateQuerySql(
            connection = connection,
            sql = sql,
            ...,
            snakeCaseToCamelCase = snakeCaseToCamelCase
          )
        ) %>% dplyr::tibble()
      return(data)
    }
  }

# private function - not exported
getDataFromResultsDatabaseSchema <- function(dataSource,
                                             cohortId = NULL,
                                             comparatorCohortId = NULL,
                                             conceptId = NULL,
                                             coConceptId = NULL,
                                             conceptId1 = NULL,
                                             relationshipId = NULL,
                                             conceptSetId = NULL,
                                             daysRelativeIndex = NULL,
                                             databaseId = NULL,
                                             domainTable = NULL,
                                             vocabularyDatabaseSchema = NULL,
                                             startDay = NULL,
                                             relationshipDays = NULL,
                                             endDay = NULL,
                                             seriesType = NULL,
                                             eventMonth = NULL,
                                             eventYear = NULL,
                                             minThreshold = NULL,
                                             dataTableName) {
  if (is(dataSource, "environment")) {
    object <- c(
      "cohortId",
      "comparatorCohortId",
      "conceptId",
      "relationshipId",
      "coConceptId",
      "databaseId",
      "conceptSetId",
      "startDay",
      "endDay",
      "domainTable",
      "seriesType",
      "eventMonth",
      "eventYear",
      "daysRelativeIndex"
    )
    if (!is.null(vocabularyDatabaseSchema)) {
      paste0(
        "vocabularyDatabaseSchema provided for function 'getResultsConcept', ",
        "\nbut working in local file mode. VocabularyDatabaseSchema will be ignored."
      )
    }
    if (!exists(dataTableName, envir = dataSource)) {
      return(NULL)
    }
    if (is.null(get(dataTableName))) {
      return(NULL)
    }
    data <- get(dataTableName, envir = dataSource)
    if (is.null(data)) {
      warning(paste0(dataTableName, " in environment has no data."))
    }
    if (nrow(data) == 0) {
      warning(paste0(dataTableName, " in environment was found to have o rows."))
    }
    colnamesData <- colnames(data)
    for (i in (1:length(object))) {
      if (all(!is.null(get(object[[i]])),
              object[[i]] %in% colnamesData)) {
        data <- data %>%
          dplyr::filter(!!as.name(object[[i]]) %in% !!get(object[[i]]))
      }
    }
    if (hasData(conceptId1)) {
      #for concept relationship only
      data <- dplyr::bind_rows(data %>%
                                 dplyr::filter(.data$conceptId1 %in% !!conceptId1),
                               data %>%
                                 dplyr::filter(.data$conceptId2 %in% !!conceptId1)) %>% 
        dplyr::distinct()
    }
    if (dataTableName %in% c('covariateValue', 'covariateValueDist')) {
      if (hasData(minThreshold)) {
        data <- data %>% 
          dplyr::filter(.data$mean > minThreshold)
      }
    }
    if (all(dataTableName %in% c('cohortRelationships'),
            !is.null(relationshipDays))) {
        data <- data %>% 
          dplyr::filter(.data$startDay == .data$endDay) %>% 
          dplyr::filter(.data$startDay %in% c(relationshipDays))
    }
  } else {
    if (is.null(dataSource$connection)) {
      stop("No connection provided. Unable to query database.")
    }
    
    if (!DatabaseConnector::dbIsValid(dataSource$connection)) {
      stop("Connection to database seems to be closed.")
    }
    
    if (dataTableName %in% c('covariateValue', 'covariateValueDist')) {
      if (hasData(minThreshold)) {
        covariate_mean_filter <- minThreshold
      }
    } else {
      covariate_mean_filter <- NULL
    }
    
    sql <- "SELECT * \n
            FROM  @results_database_schema.@data_table \n
            WHERE 1 = 1 \n
              {@database_id !=''} ? {AND database_id in (@database_id) \n}
              {@cohort_id !=''} ? {AND cohort_id in (@cohort_id) \n}
              {@comparator_cohort_id !=''} ? {AND comparator_cohort_id in (@comparator_cohort_id) \n}
              {@concept_id !=''} ? {AND concept_id in (@concept_id) \n}
              {@co_concept_id !=''} ? {AND co_concept_id in (@co_concept_id) \n}
              {@concept_set_id !=''} ? {AND concept_set_id in (@concept_set_id) \n}
              {@concept_id_1 !=''} ? {AND (concept_id_1 IN (@concept_id_1) OR concept_id_2 IN (@concept_id_1)) \n}
              {@start_day !=''} ? {AND start_day IN (@start_day) \n}
              {@cohort_relationship_days !=''} ? {AND start_day = end_day AND start_day IN (@cohort_relationship_days) \n}
              {@end_day !=''} ? {AND end_day IN (@end_day) \n}
              {@relationship_id !=''} ? {AND relationship_id IN (@relationship_id) \n}
              {@series_type !=''} ? {AND series_type IN (@series_type) \n}
              {@domain_table !=''} ? {AND domain_table IN (@domain_table) \n}
              {@event_month !=''} ? {AND event_month IN (@event_month) \n}
              {@event_year !=''} ? {AND event_year IN (@event_year) \n}
              {@days_relative_index !=''} ? {AND days_relative_index IN (@days_relative_index) \n}
              {@covariate_mean_filter !=''} ? {AND mean > @covariate_mean_filter \n}
            ;"
    if (!is.null(vocabularyDatabaseSchema)) {
      resultsDatabaseSchema <- vocabularyDatabaseSchema
    } else {
      resultsDatabaseSchema <- dataSource$resultsDatabaseSchema
    }
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = resultsDatabaseSchema,
        cohort_id = cohortId,
        comparator_cohort_id = comparatorCohortId,
        data_table = camelCaseToSnakeCase(dataTableName),
        database_id = quoteLiterals(databaseId),
        concept_id = conceptId,
        co_concept_id = coConceptId,
        concept_set_id = conceptSetId,
        concept_id_1 = conceptId1,
        # for concept relationship only
        relationship_id = quoteLiterals(relationshipId),
        start_day = startDay,
        cohort_relationship_days = relationshipDays,
        end_day = endDay,
        domain_table = quoteLiterals(domainTable),
        days_relative_index = daysRelativeIndex,
        series_type = quoteLiterals(seriesType),
        event_month = eventMonth,
        event_year = eventYear,
        days_relative_index = daysRelativeIndex,
        covariate_mean_filter = minThreshold,
        snakeCaseToCamelCase = TRUE
      )
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}


# Metadata ----
#' Returns data from meta data table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from meta data table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getExecutionMetadata <- function(dataSource) {
  databaseMetadata <-
    getDataFromResultsDatabaseSchema(dataSource,
                                     dataTableName = "metadata")
  if (is.null(databaseMetadata)) {
    return(NULL)
  }
  databaseMetadata <- databaseMetadata %>%
    dplyr::filter(!.data$variableField == "databaseId")
  columnNames <-
    databaseMetadata$variableField %>% unique() %>% sort()
  columnNamesNoJson <-
    columnNames[stringr::str_detect(string = tolower(columnNames),
                                    pattern = "json",
                                    negate = TRUE)]
  columnNamesJson <-
    columnNames[stringr::str_detect(string = tolower(columnNames),
                                    pattern = "json",
                                    negate = FALSE)]
  transposeNonJsons <- databaseMetadata %>%
    dplyr::filter(.data$variableField %in% c(columnNamesNoJson)) %>%
    dplyr::rename(name = "variableField") %>%
    dplyr::group_by(.data$databaseId, .data$startTime, .data$name) %>%
    dplyr::summarise(valueField = max(.data$valueField),
                     .groups = "keep") %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(names_from = .data$name,
                       values_from = .data$valueField) %>%
    dplyr::mutate(startTime = stringr::str_replace(
      string = .data$startTime,
      pattern = "TM_",
      replacement = ""
    ))
  transposeNonJsons$startTime <-
    transposeNonJsons$startTime %>% lubridate::as_datetime()
  
  transposeJsons <- databaseMetadata %>%
    dplyr::filter(.data$variableField %in% c(columnNamesJson)) %>%
    dplyr::rename(name = "variableField") %>%
    dplyr::group_by(.data$databaseId, .data$startTime, .data$name) %>%
    dplyr::summarise(valueField = max(.data$valueField),
                     .groups = "keep") %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(names_from = .data$name,
                       values_from = .data$valueField) %>%
    dplyr::mutate(startTime = stringr::str_replace(
      string = .data$startTime,
      pattern = "TM_",
      replacement = ""
    ))
  transposeJsons$startTime <-
    transposeJsons$startTime %>% lubridate::as_datetime()
  
  transposeJsonsTemp <- list()
  for (i in (1:nrow(transposeJsons))) {
    transposeJsonsTemp[[i]] <- transposeJsons[i,]
    for (j in (1:length(columnNamesJson))) {
      transposeJsonsTemp[[i]][[columnNamesJson[[j]]]] <-
        transposeJsonsTemp[[i]][[columnNamesJson[[j]]]] %>%
        RJSONIO::fromJSON(digits = 23) %>%
        RJSONIO::toJSON(digits = 23)
    }
  }
  transposeJsons <- dplyr::bind_rows(transposeJsonsTemp)
  data <- transposeNonJsons %>%
    dplyr::left_join(transposeJsons,
                     by = c("databaseId", "startTime"))
  if ('observationPeriodMaxDate' %in% colnames(data)) {
    data$observationPeriodMaxDate <-
      tryCatch(
        expr = lubridate::as_date(data$observationPeriodMaxDate),
        error = data$observationPeriodMaxDate
      )
  }
  if ('observationPeriodMinDate' %in% colnames(data)) {
    data$observationPeriodMinDate <-
      tryCatch(
        expr = lubridate::as_date(data$observationPeriodMinDate),
        error = data$observationPeriodMinDate
      )
  }
  if ('sourceReleaseDate' %in% colnames(data)) {
    data$sourceReleaseDate <-
      tryCatch(
        expr = lubridate::as_date(data$sourceReleaseDate),
        error = data$sourceReleaseDate
      )
  }
  if ('personDaysInDatasource' %in% colnames(data)) {
    data$personDaysInDatasource <-
      tryCatch(
        expr = as.numeric(data$personDaysInDatasource),
        error = data$personDaysInDatasource
      )
  }
  if ('recordsInDatasource' %in% colnames(data)) {
    data$recordsInDatasource <-
      tryCatch(
        expr = as.numeric(data$recordsInDatasource),
        error = data$recordsInDatasource
      )
  }
  if ('personDaysInDatasource' %in% colnames(data)) {
    data$personDaysInDatasource <-
      tryCatch(
        expr = as.numeric(data$personDaysInDatasource),
        error = data$personDaysInDatasource
      )
  }
  return(data)
}



# Database database level counts ----
#' Returns database level counts
#'
#' @description
#' Returns database level counts
#'
#' @template DataSource
#' 
#' @template DatabaseId
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getDatabaseCounts <- function(dataSource,
                              databaseIds = NULL) {
  databaseExecutionData <- getExecutionMetadata(dataSource = dataSource)
  if (!is.null(databaseIds)) {
    databaseExecutionData <- databaseExecutionData %>% 
      dplyr::filter(.data$databaseId %in% databaseIds)
  }
  databaseExecutionData <- databaseExecutionData %>% 
    dplyr::group_by(.data$databaseId) %>% 
    dplyr::arrange(.data$databaseId, dplyr::desc(.data$startTime)) %>% 
    dplyr::mutate(rn = dplyr::row_number()) %>% 
    dplyr::filter(.data$rn == 1) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(records = as.double(.data$recordsInDatasource),
                  persons = as.double(.data$personsInDatasource)) %>% 
    dplyr::select(.data$databaseId,
                  .data$records,
                  .data$persons)
  return(databaseExecutionData)
}





# Concept ----
#' Returns conceptIds details from concept table
#'
#' @description
#' Returns concept details from concept table for provided list of concept ids
#'
#' @template DataSource
#'
#' @template ConceptIds
#'
#' @template VocabularyDatabaseSchema
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getConcept <- function(dataSource = .GlobalEnv,
                       vocabularyDatabaseSchema = NULL,
                       conceptIds = NULL) {
  resultsDatabaseSchema <- NULL
  if (!is.null(vocabularyDatabaseSchema)) {
    resultsDatabaseSchema <- vocabularyDatabaseSchema
  }
  data <- getDataFromResultsDatabaseSchema(
    dataSource = dataSource,
    dataTableName = "concept",
    conceptId = conceptIds,
    vocabularyDatabaseSchema = resultsDatabaseSchema
  )
  return(data)
}


#' Returns the relationship table from results data model
#'
#' @description
#' Returns the relationship table from results data model
#'
#' @template DataSource
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getRelationship <- function(dataSource = .GlobalEnv) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource = dataSource,
    dataTableName = "relationship"
  )
  return(data)
}


#' Returns data from concept relationship table for list of concept ids
#'
#' @description
#' Returns data from concept relationship table for list of concept ids
#'
#' @template DataSource
#'
#' @template ConceptIds
#'
#' @template VocabularyDatabaseSchema
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getConceptRelationship <- function(dataSource = .GlobalEnv,
                                   vocabularyDatabaseSchema = NULL,
                                   conceptIds = NULL,
                                   relationshipIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource = dataSource,
    dataTableName = "conceptRelationship",
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    conceptId1 = conceptIds,
    relationshipId = relationshipIds
  )
  return(data)
}



#' Returns data from concept ancestor table for vector of concept ids
#'
#' @description
#' Returns data from concept ancestor table for vector of concept ids
#' 
#' @template ConceptIds
#'
#' @template DataSource
#'
#' @template VocabularyDatabaseSchema
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getConceptAncestor <- function(dataSource = .GlobalEnv,
                               vocabularyDatabaseSchema = NULL,
                               conceptIds = NULL) {
  table <- "conceptAncestor"
  if (!is.null(vocabularyDatabaseSchema) &&
      is(dataSource, "environment")) {
    warning(
      "vocabularyDatabaseSchema provided for function 'getConceptAncestor', \nbut working in local file mode. VocabularyDatabaseSchema will be ignored."
    )
  }
  if (is(dataSource, "environment")) {
    if (any(!exists(table),
            length(table) == 0,
            nrow(table) == 0)) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>%
      dplyr::filter(
        .data$ancestorConceptId %in% conceptIds |
          .data$descendantConceptId %in% conceptIds
      )
    
  } else {
    sql <-
      "SELECT *
       FROM @vocabulary_database_schema.concept_ancestor
       WHERE ancestor_concept_id IN (@concept_ids)
           OR descendant_concept_id IN (@concept_ids);"
    if (!is.null(conceptIds)) {
      sql <-
        SqlRender::render(sql = sql,
                          concept_ids = conceptIds)
    }
    if (!is.null(vocabularyDatabaseSchema)) {
      sql <-
        SqlRender::render(sql = sql,
                          vocabulary_database_schema = vocabularyDatabaseSchema)
    } else {
      sql <-
        SqlRender::render(
          sql = sql,
          vocabulary_database_schema = dataSource$vocabularyDatabaseSchema
        )
    }
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        snakeCaseToCamelCase = TRUE
      )
  }
  return(data)
}


#' Returns data from concept synonym table for list of concept ids
#'
#' @description
#' Returns data from concept synonym table for list of concept ids
#'
#' @template DataSource
#'
#' @template ConceptId
#'
#' @template VocabularyDatabaseSchema
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getConceptSynonym <- function(dataSource = .GlobalEnv,
                              vocabularyDatabaseSchema = NULL,
                              conceptId = NULL) {
  table <- "conceptSynonym"
  if (!is.null(vocabularyDatabaseSchema) &&
      is(dataSource, "environment")) {
    warning(
      "vocabularyDatabaseSchema provided for function 'getConceptSynonym', \nbut working in local file mode. VocabularyDatabaseSchema will be ignored."
    )
  }
  if (is(dataSource, "environment")) {
    if (any(!exists(table),
            length(table) == 0,
            nrow(table) == 0)) {
      return(NULL)
    }
    data <- get(table, envir = dataSource) %>%
      dplyr::filter(.data$conceptId %in% !!conceptId)
    
  } else {
    sql <-
      "SELECT *
       FROM @vocabulary_database_schema.concept_synonym
       WHERE concept_id IN (@concept_ids);"
    if (!is.null(conceptId)) {
      sql <-
        SqlRender::render(sql = sql,
                          concept_ids = conceptId)
    }
    if (!is.null(vocabularyDatabaseSchema)) {
      sql <-
        SqlRender::render(sql = sql,
                          vocabulary_database_schema = vocabularyDatabaseSchema)
    } else {
      sql <-
        SqlRender::render(
          sql = sql,
          vocabulary_database_schema = dataSource$vocabularyDatabaseSchema
        )
    }
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        snakeCaseToCamelCase = TRUE
      )
  }
  return(data)
}


#' Returns data from concept_count table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from concept_count table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template DatabaseIds
#'
#' @template ConceptIds
#' 
#' @template CalendarMonths
#' 
#' @template CalendarYears
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getResultsConceptCount <- function(dataSource,
                                   databaseIds = NULL,
                                   conceptIds = NULL,
                                   calendarMonths = NULL,
                                   calendarYears = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    databaseId = databaseIds,
    conceptId = conceptIds,
    eventMonth = calendarMonths,
    eventYear = calendarYears,
    dataTableName = "conceptCount"
  )
  return(data)
}



#' Returns data from concept_mapping table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from concept_mapping table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template DatabaseIds
#'
#' @template ConceptIds
#'
#' @param domainTable    A vector of strings representing the OMOP CDM domain tables. Valid options are
#'                       'All' (Default) for data source level, 'condition_occurrence', 'procedure_occurrence',
#'                      'measurement', 'visit_occurrence', 'observation', 'device_exposure'
#'                      In this case 'All' - provides count across domain tables.
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getResultsConceptMapping <- function(dataSource,
                                     databaseIds = NULL,
                                     conceptIds = NULL,
                                     domainTables = 'All') {
  intersectOfTwo <- domainTables
  if (!is.null(domainTables)) {
    domainTable <- getDomainInformation()$long
    intersectOfTwo <- intersect(x = domainTables,
                                y = c("All", domainTable$domainTable)) %>% 
      unique()
    setdiffOfTwo <- setdiff(x = domainTables,
                            y = c("All", domainTable$domainTable)) %>% 
      unique()
    if (length(setdiffOfTwo) > 1) {
      warning(paste0("Cant match the following in domainTables parameter: ", paste(setdiffOfTwo, collapse = ", ")))
      if (length(intersectOfTwo) == 0) {
        stop("Cannot match given domainTables")
      }
      if (length(intersectOfTwo) > 1) {
        warning(paste0("Returning results for following domain tables: ", paste(intersectOfTwo, collapse = ", ")))
      }
    }
  }
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    databaseId = databaseIds,
    conceptId = conceptIds,
    domainTable = intersectOfTwo,
    dataTableName = "conceptMapping"
  )
  return(data)
}

#' Returns data from concept_subjects table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from concept_subjects table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template DatabaseIds
#'
#' @template ConceptIds
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getResultsConceptSubjects <- function(dataSource,
                                      databaseIds = NULL,
                                      conceptIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    databaseId = databaseIds,
    conceptId = conceptIds,
    dataTableName = "conceptSubjects"
  )
  return(data)
}


#' Returns a metadata for concept ids
#'
#' @description
#' Returns a metadata for a given list of concept ids that includes concept synonyms,
#' concept relationship, concept ancestor, concept count per database,
#' concept cooccurrence on index date per database and cohortId.
#'
#' @template DataSource
#'
#' @template DatabaseIds
#'
#' @template CohortIds
#'
#' @template ConceptIds
#' 
#' @template VocabularyDatabaseSchema
#' 
#' @param getDatabaseMetadata Do you want to get database metadata i.e. person/records/person days in observation table
#' 
#' @param getConceptRelationship  Do you want conceptRelationship?
#' 
#' @param getConceptAncestor  Do you want conceptAncestor?
#' 
#' @param getConceptSynonym  Do you want conceptSynonym?
#' 
#' @param getConceptCount  Do you want conceptCount?
#' 
#' @param getConceptCooccurrence  Do you want concept cooccurrence?
#' 
#' @param getIndexEventCount  Do you want index event concept count?
#' 
#' @param getConceptMappingCount  Do you want concept mapping count?
#' 
#' @param getFixedTimeSeries Do you want a conceptIds datasource level time series data reported on actual dates?
#' 
#' @param getRelativeTimeSeries Do you want cohort level time series data reported relative to cohort start date?
#'
#' @return
#' Returns a list of data frames (tibbles)
#'
#' @export
getConceptMetadata <- function(dataSource,
                               databaseIds = NULL,
                               cohortIds = NULL,
                               vocabularyDatabaseSchema = NULL,
                               conceptIds = NULL,
                               getDatabaseMetadata = TRUE,
                               getConceptRelationship = TRUE,
                               getConceptAncestor = TRUE,
                               getConceptSynonym = TRUE,
                               getConceptCount = TRUE,
                               getConceptCooccurrence = TRUE,
                               getIndexEventCount = TRUE,
                               getConceptMappingCount = TRUE,
                               getFixedTimeSeries = TRUE,
                               getRelativeTimeSeries = TRUE) {
  data <- list()
  if (!is.null(databaseIds)) {
    if (getDatabaseMetadata) {
      databaseMetadata <- getExecutionMetadata(dataSource = dataSource)
      if (!is.null(databaseMetadata)) {
        data$databaseCount <- databaseMetadata %>%
          dplyr::filter(.data$databaseId %in% c(databaseIds)) %>%
          dplyr::select(
            .data$databaseId,
            .data$personDaysInDatasource,
            .data$personsInDatasource,
            .data$recordsInDatasource
          )
      }
    }
  }
  # results not dependent on cohort definition
  if (getConceptRelationship) {
    data$relationship <-
      getVocabularyRelationship(dataSource = dataSource)
    if (!hasData(data$relationship)) {
      return(NULL)
    }
    data$conceptRelationship <-
      getConceptRelationship(
        dataSource = dataSource,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        conceptIds = conceptIds
      )
    if (!hasData(data$conceptRelationship)) {
      return(NULL)
    }
    #output for concept relationship table in shiny app
    conceptRelationship <- dplyr::bind_rows(
      data$conceptRelationship %>%
        dplyr::filter(is.na(.data$invalidReason)) %>%
        dplyr::rename(
          "conceptId" = .data$conceptId2,
          "referenceConceptId" = .data$conceptId1
        ) %>%
        dplyr::select(
          .data$referenceConceptId,
          .data$conceptId,
          .data$relationshipId
        ),
      data$conceptRelationship %>%
        dplyr::filter(is.na(.data$invalidReason)) %>%
        dplyr::rename(
          "conceptId" = .data$conceptId1,
          "referenceConceptId" = .data$conceptId2
        ) %>%
        dplyr::select(
          .data$referenceConceptId,
          .data$conceptId,
          .data$relationshipId
        )
    ) %>%
      dplyr::distinct() %>%
      dplyr::arrange(.data$conceptId)
    
    #!!!!!!!!! need to collapse relationshipId - to avoid duplication. need to make them come with line break
    # %>% 
    #   dplyr::mutate(relationshipId = paste(.data$relationshipId, sep = "<br>", collapse = ";")) %>% 
    #   dplyr::ungroup()
  }
  
  if (getConceptAncestor) {
    data$conceptAncestor <-
      getConceptAncestor(
        dataSource = dataSource,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        conceptIds = conceptIds
      )
    
    #output for concept relationship table in shiny app
    conceptAncestor <- dplyr::bind_rows(
      data$conceptAncestor %>%
        dplyr::filter(.data$descendantConceptId == conceptIds) %>%
        dplyr::rename(
          "referenceConceptId" = .data$descendantConceptId,
          "conceptId" = .data$ancestorConceptId,
          "levelsOfSeparation" = .data$minLevelsOfSeparation
        ) %>%
        dplyr::select(
          .data$referenceConceptId,
          .data$conceptId,
          .data$levelsOfSeparation
        ) %>%
        dplyr::distinct() %>%
        dplyr::mutate(levelsOfSeparation = .data$levelsOfSeparation * -1) %>%
        dplyr::filter(.data$referenceConceptId != .data$conceptId),
      data$conceptAncestor %>%
        dplyr::filter(.data$ancestorConceptId == conceptIds) %>%
        dplyr::rename(
          "referenceConceptId" = .data$ancestorConceptId,
          "conceptId" = .data$descendantConceptId,
          "levelsOfSeparation" = .data$minLevelsOfSeparation
        ) %>%
        dplyr::select(
          .data$referenceConceptId,
          .data$conceptId,
          .data$levelsOfSeparation
        ) %>%
        dplyr::distinct() %>%
        dplyr::filter(.data$referenceConceptId != .data$conceptId)
    ) %>%
      dplyr::distinct() %>%
      dplyr::arrange(dplyr::desc(.data$levelsOfSeparation))
  }
  
  if (getConceptSynonym) {
    data$conceptSynonym <- getConceptSynonym(
      dataSource = dataSource,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      conceptId = conceptIds
    ) %>%
      dplyr::distinct()
  }
  
  if (getConceptMappingCount) {
    data$conceptMapping <-
      getResultsConceptMapping(dataSource = dataSource,
                               databaseIds = databaseIds,
                               conceptIds = conceptIds)
  }
  
  conceptIdList <- c(
    conceptIds,
    data$conceptRelationship$conceptId1,
    data$conceptRelationship$conceptId2,
    data$conceptAncestor$ancestorConceptId,
    data$conceptAncestor$descendantConceptId,
    data$conceptMapping$sourceConceptId
  ) %>%
    unique()
  
  data$concept <- getConcept(
    dataSource = dataSource,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    conceptIds = conceptIdList
  )
  
  if (all(getConceptAncestor, getConceptRelationship)) {
    #output for concept relationship table in shiny app
    data$conceptRelationshipTable <-
      dplyr::bind_rows(
        conceptRelationship %>% dplyr::select(.data$referenceConceptId,
                                              .data$conceptId),
        conceptAncestor %>% dplyr::select(.data$referenceConceptId,
                                          .data$conceptId)
      ) %>%
      dplyr::distinct() %>%
      dplyr::left_join(conceptRelationship,
                       by = c('referenceConceptId', 'conceptId')) %>%
      dplyr::left_join(conceptAncestor,
                       by = c('referenceConceptId', 'conceptId')) %>%
      dplyr::mutate(levelsOfSeparation = as.character(.data$levelsOfSeparation)) %>%
      tidyr::replace_na(list(relationshipId = "Not applicable",
                             levelsOfSeparation = "Not applicable"))
    
    relationship <- getRelationship(dataSource = dataSource)
    #drop down for concept relationship table in shiny app
    data$relationshipName <- c(
      "Not applicable",
      relationship %>%
        dplyr::select(.data$relationshipId, .data$relationshipName) %>%
        dplyr::inner_join(data$conceptRelationshipTable, by = "relationshipId") %>%
        dplyr::select(.data$relationshipName) %>%
        dplyr::distinct() %>%
        dplyr::arrange() %>%
        dplyr::pull()
    ) %>% unique()
    #drop down for concept relationship table in shiny app
    data$conceptAncestorDistance <-
      data$conceptRelationshipTable %>%
      dplyr::select(.data$levelsOfSeparation) %>%
      dplyr::distinct() %>%
      dplyr::arrange(.data$levelsOfSeparation) %>%
      dplyr::pull() %>%
      unique()
  }
  
  # results dependent on databaseId
  if (all(getConceptCount,!is.null(databaseIds))) {
    data$cdmTables <- getDomainInformation()$long
    data$databaseConceptCountDetails <-
      getResultsConceptCount(dataSource = dataSource,
                             databaseIds = databaseIds,
                             conceptIds = conceptIdList)
    if (!is.null(data$databaseConceptCountDetails)) {
      data$databaseConceptCount <-
        data$databaseConceptCountDetails %>%
        dplyr::filter(.data$eventYear == 0) %>%
        dplyr::filter(.data$eventMonth == 0) %>%
        dplyr::select(.data$conceptId,
                      .data$databaseId,
                      .data$conceptCount,
                      .data$subjectCount)
      
      if (getFixedTimeSeries) {
        data$databaseConceptIdYearMonthLevelTsibble <-
          data$databaseConceptCountDetails %>%
          dplyr::filter(.data$eventYear > 0, .data$eventMonth > 0) %>%
          dplyr::mutate(periodBegin = lubridate::as_date(paste0(
            .data$eventYear,
            "-",
            .data$eventMonth,
            "-01"
          ))) %>%
          dplyr::select(
            .data$conceptId,
            .data$databaseId,
            .data$periodBegin,
            .data$conceptCount,
            .data$subjectCount
          ) %>%
          dplyr::mutate(periodBegin = tsibble::yearmonth(.data$periodBegin)) %>%
          tsibble::as_tsibble(
            key = c(
              .data$conceptId,
              .data$databaseId
            ),
            index = .data$periodBegin
          ) %>%
          dplyr::arrange(
            .data$conceptId,
            .data$databaseId,
            .data$periodBegin,
            .data$conceptCount,
            .data$subjectCount
          )
        data$databaseConceptIdYearLevelTsibble <-
          data$databaseConceptCountDetails %>%
          dplyr::filter(.data$eventYear > 0, .data$eventMonth == 0) %>%
          dplyr::mutate(periodBegin = lubridate::as_date(paste0(.data$eventYear,
                                                                "-",
                                                                "01-01"))) %>%
          dplyr::select(
            .data$conceptId,
            .data$databaseId,
            .data$periodBegin,
            .data$conceptCount,
            .data$subjectCount
          ) %>%
          dplyr::mutate(periodBegin = clock::get_year(.data$periodBegin)) %>%
          tsibble::as_tsibble(
            key = c(
              .data$conceptId,
              .data$databaseId
            ),
            index = .data$periodBegin
          ) %>%
          dplyr::arrange(
            .data$conceptId,
            .data$databaseId,
            .data$periodBegin,
            .data$conceptCount,
            .data$subjectCount
          )
      }
    }
  }
  
  if (!is.null(cohortIds)) {
    if (getConceptCooccurrence) {
      # data$conceptCooccurrence <-
      #   getResultsConceptCooccurrence(
      #     dataSource = dataSource,
      #     databaseIds = databaseIds,
      #     cohortIds = cohortIds,
      #     conceptIds = data$concept$conceptId %>% unique()
      #   ) %>%
      #   dplyr::select(
      #     .data$conceptId,
      #     .data$databaseId,
      #     .data$cohortId,
      #     .data$coConceptId,
      #     .data$subjectCount
      #   ) %>%
      #   dplyr::rename("referenceConceptId" = .data$conceptId) %>%
      #   dplyr::rename("conceptId" = .data$coConceptId) %>%
      #   dplyr::arrange(
      #     .data$referenceConceptId,
      #     .data$databaseId,
      #     .data$cohortId,
      #     dplyr::desc(.data$subjectCount)
      #   )
    }
    if (getIndexEventCount) {
      data$indexEventBreakdown <-
        getResultsIndexEventBreakdown(
          dataSource = dataSource,
          cohortIds = cohortIds,
          databaseIds = databaseIds,
          conceptIds = data$concept$conceptId %>% unique()
        )
    }
  }
  
  if (getRelativeTimeSeries) {
    relativeTimeSeries <-
      getFeatureExtractionTemporalCharacterization(dataSource = dataSource,
                                                   cohortIds = cohortIds,
                                                   databaseIds = databaseIds)
    if (!is.null(relativeTimeSeries)) {
      if (!is.null(relativeTimeSeries$temporalCovariateRef)) {
        relativeTimeSeries$temporalCovariateRef <-
          relativeTimeSeries$temporalCovariateRef %>%
          dplyr::filter(.data$conceptId %in% conceptIds)
      }
      if (!is.null(relativeTimeSeries$temporalCovariateValue)) {
        browser()
        data$cohortConceptIdYearMonthLevelTsibble <-
          relativeTimeSeries$temporalCovariateValue %>%
          dplyr::filter(
            .data$covariateId %in% c(
              relativeTimeSeries$temporalCovariateRef$covariateId %>% unique()
            )
          ) %>%
          dplyr::inner_join(relativeTimeSeries$temporalCovariateRef,
                            by = "covariateId") %>%
          dplyr::select(-.data$covariateId, -.data$covariateName) %>%
          dplyr::inner_join(relativeTimeSeries$temporalAnalysisRef,
                            by = "analysisId") %>%
          dplyr::filter(.data$isBinary == 'Y') %>%
          dplyr::select(-.data$isBinary,-.data$missingMeansZero,-.data$analysisId) %>%
          dplyr::inner_join(
            relativeTimeSeries$temporalTimeRef %>%
              dplyr::filter(.data$endDay - .data$startDay == 30) %>%
              dplyr::mutate(periodBegin = .data$endDay %/% 30),
            by = c("timeId")
          ) %>%
          dplyr::select(-.data$timeId) %>%
          dplyr::select(
            .data$conceptId,
            .data$cohortId,
            .data$databaseId,
            .data$domainId,
            .data$analysisName,
            .data$periodBegin,
            .data$startDay,
            .data$endDay,
            .data$mean,
            .data$sumValue,
            .data$sd
          ) %>%
          dplyr::arrange(
            .data$conceptId,
            .data$cohortId,
            .data$databaseId,
            .data$domainId,
            .data$analysisName,
            .data$periodBegin,
            .data$startDay,
            .data$endDay
          ) %>%
          tsibble::as_tsibble(
            key = c(
              .data$conceptId,
              .data$cohortId,
              .data$databaseId,
              .data$domainId,
              .data$analysisName
            ),
            index = .data$periodBegin  #x-axis
          )
      }
    }
  }
  return(data)
}

## Concept details based on cohort id -----

#' Returns resolved concepts for a list of cohortIds and databaseIds combinations
#'
#' @description
#' Given a list of cohortIds, databaseIds combinations the function returns
#' precomputed resolved conceptIds for the combination.
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getResultsResolvedConcepts <- function(dataSource,
                                       databaseIds = NULL,
                                       cohortIds = NULL,
                                       conceptSetIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    conceptSetId = conceptSetIds,
    dataTableName = "conceptResolved"
  )
  if (any((is.null(data)),
          nrow(data) == 0)) {
    return(NULL)
  }
  return(data)
}


#' Returns excluded concepts for a list of cohortIds and databaseIds combinations
#'
#' @description
#' Given a list of cohortIds, databaseIds combinations the function returns
#' precomputed excluded conceptIds for the combination.
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getResultsExcludedConcepts <- function(dataSource,
                                       databaseIds = NULL,
                                       cohortId = NULL,
                                       conceptSetId = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortId,
    databaseId = databaseIds,
    conceptSetId = conceptSetId,
    dataTableName = "conceptExcluded"
  )
  if (any((is.null(data)),
          nrow(data) == 0)) {
    return(NULL)
  }
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
#' @template ConceptSetIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble).
#'
#' @export
getResultsOrphanConcept <- function(dataSource,
                                    cohortIds = NULL,
                                    databaseIds = NULL,
                                    conceptSetIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    conceptSetId = conceptSetIds,
    dataTableName = "orphanConcept"
  )
  if (any((is.null(data)),
          nrow(data) == 0)) {
    return(NULL)
  }
  
  resolved <- getResultsResolvedConcepts(
    dataSource = dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    conceptSetIds = conceptSetIds
  )
  if (all(!is.null(resolved),
          nrow(resolved) > 0)) {
    relationship1 <- getConceptRelationship(
      dataSource = dataSource,
      conceptIds = resolved$conceptId %>% unique(),
      relationshipIds = c("Maps to",
                          "Mapped from",
                          "Is a")
    )
    relationship2 <- getConceptRelationship(
      dataSource = dataSource,
      conceptIds = relationship1$conceptId2 %>% unique(),
      relationshipIds = c("Maps to",
                          "Mapped from",
                          "Is a")
    )
    relationship <-
      dplyr::bind_rows(relationship1, relationship2) %>%
      dplyr::distinct()
    
    toExcludeFromOrphan <- c(relationship$conceptId2,
                             resolved$conceptId) %>%
      unique()
    
    data <- data %>%
      dplyr::filter(!.data$conceptId %in% !!toExcludeFromOrphan)
  }
  
  # removed excluded conceptIds.
  excludedConceptIds <-
    getResultsExcludedConcepts(
      dataSource = dataSource,
      cohortId = cohortIds,
      databaseIds = databaseIds,
      conceptSetId = conceptSetIds
    )
  if (all(!is.null(excludedConceptIds),
          nrow(excludedConceptIds) > 0)) {
    data <- data %>%
      dplyr::anti_join(excludedConceptIds,
                       by = c("databaseId", "cohortId", "conceptId", "conceptSetId"))
  }
  data <- data %>%
    dplyr::distinct()
  return(data)
}


getConceptSetDetailsFromCohortDefinition <-
  function(cohortDefinitionExpression) {
    if ("expression" %in% names(cohortDefinitionExpression)) {
      expression <- cohortDefinitionExpression$expression
    } else {
      expression <- cohortDefinitionExpression
    }
    
    if (is.null(expression$ConceptSets)) {
      return(NULL)
    }
    
    conceptSetExpression <- expression$ConceptSets %>%
      dplyr::bind_rows() %>%
      dplyr::mutate(json = RJSONIO::toJSON(x = .data$expression,
                                           pretty = TRUE))
    
    conceptSetExpressionDetails <- list()
    i <- 0
    for (id in conceptSetExpression$id) {
      i <- i + 1
      conceptSetExpressionDetails[[i]] <-
        getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression =
                                                         conceptSetExpression[i, ]$expression$items) %>%
        dplyr::mutate(id = conceptSetExpression[i,]$id) %>%
        dplyr::relocate(.data$id) %>%
        dplyr::arrange(.data$id)
    }
    conceptSetExpressionDetails <-
      dplyr::bind_rows(conceptSetExpressionDetails)
    output <- list(conceptSetExpression = conceptSetExpression,
                   conceptSetExpressionDetails = conceptSetExpressionDetails)
    return(output)
  }


getConceptSetDataFrameFromConceptSetExpression <-
  function(conceptSetExpression) {
    if ("items" %in% names(conceptSetExpression)) {
      items <- conceptSetExpression$items
    } else {
      items <- conceptSetExpression
    }
    conceptSetExpressionDetails <- items %>%
      purrr::map_df(.f = purrr::flatten)
    if ('CONCEPT_ID' %in% colnames(conceptSetExpressionDetails)) {
      if ('isExcluded' %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(IS_EXCLUDED = .data$isExcluded)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(IS_EXCLUDED = FALSE)
      }
      if ('includeDescendants' %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(INCLUDE_DESCENDANTS = .data$includeDescendants)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(INCLUDE_DESCENDANTS = FALSE)
      }
      if ('includeMapped' %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(INCLUDE_MAPPED = .data$includeMapped)
      } else {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::mutate(INCLUDE_MAPPED = FALSE)
      }
      conceptSetExpressionDetails <-
        conceptSetExpressionDetails %>%
        tidyr::replace_na(list(
          IS_EXCLUDED = FALSE,
          INCLUDE_DESCENDANTS = FALSE,
          INCLUDE_MAPPED = FALSE
        ))
      colnames(conceptSetExpressionDetails) <-
        snakeCaseToCamelCase(colnames(conceptSetExpressionDetails))
    }
    return(conceptSetExpressionDetails)
  }

# Vocabulary ----
#' Returns data from relationship table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from relationship table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getVocabularyRelationship <- function(dataSource) {
  data <- getDataFromResultsDatabaseSchema(dataSource,
                                           dataTableName = "relationship")
  return(data)
}



# Cohort ----
#' Returns data from cohort table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from cohort table of Cohort Diagnostics results data model
#'
#' @template DataSource
#' 
#' @template CohortIds
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getResultsCohort <- function(dataSource, cohortIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(dataSource,
                                           dataTableName = "cohort", 
                                           cohortId = cohortIds)
  return(data)
}



#' Returns matrix of relationship between target and comparator cohortIds
#'
#' @description
#' Given a list of target and comparator cohortIds gets temporal relationship.
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template ComparatorCohortIds
#'
#' @template DatabaseIds
#' 
#' @param    relationshipDays A vector of integer representing days comparator cohort start to target cohort start
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getCohortTemporalRelationshipMatrix <- function(dataSource,
                                                databaseIds = NULL,
                                                cohortIds = NULL,
                                                comparatorCohortIds = NULL,
                                                relationshipDays = c(-3:3)) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    comparatorCohortId = comparatorCohortIds,
    databaseId = databaseIds,
    relationshipDays = relationshipDays,
    dataTableName = "cohortRelationships"
  )
  if (any((is.null(data)),
          nrow(data) == 0)) {
    return(NULL)
  }
  data <- data %>% 
    dplyr::select(.data$databaseId, .data$cohortId, .data$comparatorCohortId, .data$startDay, .data$subCsWindowT) %>%
    dplyr::mutate(day = dplyr::case_when(.data$startDay < 0 ~ paste0("dm", abs(.data$startDay)),
                                         .data$startDay > 0 ~ paste0("dp", abs(.data$startDay)),
                                         .data$startDay == 0 ~ paste0("d", abs(.data$startDay)))) %>% 
    dplyr::arrange(.data$databaseId, .data$cohortId, .data$comparatorCohortId, .data$startDay) %>% 
    tidyr::pivot_wider(id_cols = c("databaseId", "cohortId", "comparatorCohortId"), 
                       names_from = "day", 
                       values_from = "subCsWindowT")
  
  return(data)
}


#' Generates and return cohort SQL
#'
#' @description
#' Given a cohort definition expression, the function generates and return cohort SQL 
#' in OHDSI SQL form. This maybe be rendered and translated using 
#' OHDSI R packages like \code{SqlRender::render} 
#'
#' @param cohortExpression  A cohort definition expression as a list object. This is commonly generated by
#'                          by reading in a cohort definition json and converting it into a R-object
#'                          using for example RJSONIO::fromJSON(digits = 23). Please use minimum digits = 23,
#'                          to ensure that big numbers don't loose precision.'                          
#' 
#' @param generateStats     Do you want generate intermediary inclusion rules tables?
#'
#' @return
#' Returns text (OHDSI SQL)
#'
#' @export
getOhdsiSqlFromCohortDefinitionExpression <-
  function(cohortDefinitionExpression,
           generateStats = TRUE) {
    if (!is.list(cohortDefinitionExpression)) {
      stop("Provided cohortDefinitionExpression is not a list object.")
    }
    options <- CirceR::createGenerateOptions(generateStats = TRUE)
    json <- RJSONIO::toJSON(x = cohortDefinitionExpression, digits = 23)
    expression <-
      CirceR::cohortExpressionFromJson(expressionJson = json)
    if (is.null(expression)) {
      return(NULL)
    }
    sql <-
      CirceR::buildCohortQuery(expression = expression, 
                               options = options)
    return(sql)
  }


# Database ----
#' Returns data from Database table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from Database table of Cohort Diagnostics results data model
#'
#' @template DataSource
#' 
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getResultsDatabase <- function(dataSource, 
                               databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(dataSource,
                                           dataTableName = "database", 
                                           databaseId = databaseIds)
  return(data)
}


# Concept Sets ----
#' Returns data from Concept sets table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from Concept sets table of Cohort Diagnostics results data model
#'
#' @template DataSource
#' 
#' @template CohortIds
#' 
#' @template ConceptSetIds
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getResultsConceptSet <- function(dataSource,
                                 cohortIds = NULL,
                                 conceptSetIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    dataTableName = "conceptSets",
    cohortId = cohortIds,
    conceptSetId = conceptSetIds
  )
  return(data)
}


# Concept Set expression
#' Returns a Concept Set expression (R-object) from a cohort definition
#'
#' @description
#' Returns a Concept Set expression from a cohort definition
#'
#' @template DataSource
#'
#' @template CohortId
#'
#' @template ConceptSetId
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getResultsConceptSetExpression <- function(dataSource,
                                           cohortId,
                                           conceptSetId) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    dataTableName = "conceptSets",
    cohortId = cohortId,
    conceptSetId = conceptSetId
  )
  if (length(cohortId) == 0) {
    stop("cohortId is not specified")
  }
  if (length(cohortId) > 1) {
    stop("Please only provide one integer value for conceptSetId")
  }
  if (is.null(data)) {
    warning(
      paste0(
        "No concept set returned for the combination of cohort id = ",
        cohortId,
        " and concept set id = ",
        conceptSetId
      )
    )
  }
  if (nrow(data) > 1) {
    browser()
    stop("More than one expression returned. Please check the integerity of your results.")
  }
  
  expression <- data %>%
    dplyr::pull(.data$conceptSetExpression) %>%
    RJSONIO::fromJSON(digits = 23)
  
  return(expression)
}




# Concept Set optimization ----
#' Returns optimization recommendation for concept set expression
#'
#' @description
#' Returns optimization recommendation for a selected concept set expression in the cohort definition
#' and database id. Note: this optimization is pre-computed and may vary based on the vocabulary
#' version that was used at the time of optimization.
#'
#' @template DataSource
#'
#' @template DatabaseIds
#'
#' @template CohortIds
#'
#' @template ConceptSetIds
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getOptimizedConceptSet <- function(dataSource,
                                   databaseIds = NULL,
                                   cohortIds = NULL,
                                   conceptSetIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    dataTableName = "conceptSetsOptimized",
    databaseId = databaseIds,
    cohortId = cohortIds,
    conceptSetId = conceptSetIds
  )
  if (is.null(data)) {
    return(NULL)
  }
  if (nrow(data %>% 
           dplyr::filter(.data$removed == 1)) == 0) {
    return(NULL)
  }
  originalConceptSetExpression <- getResultsConceptSetExpression(dataSource = dataSource,
                                                                 cohortId = cohortIds,
                                                                 conceptSetId = conceptSetIds)
  originalConceptSetExpressionTable <- getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression = 
                                                                                        originalConceptSetExpression)
  
  excluded <- tidyr::crossing(data %>% 
                                dplyr::select(.data$databaseId) %>% 
                                dplyr::distinct(),
                              originalConceptSetExpressionTable %>% 
                                dplyr::filter(.data$isExcluded == TRUE)) %>% 
    dplyr::inner_join(data %>% 
                        dplyr::filter(.data$excluded == 1) %>% 
                        dplyr::filter(.data$removed == 0),
                      by = c("databaseId", "conceptId")) %>% 
    dplyr::select(-.data$excluded, -.data$removed)
  
  notExcluded <- tidyr::crossing(data %>% 
                                   dplyr::select(.data$databaseId) %>% 
                                   dplyr::distinct(),
                                 originalConceptSetExpressionTable %>% 
                                   dplyr::filter(.data$isExcluded == FALSE)) %>% 
    dplyr::inner_join(data %>% 
                        dplyr::filter(.data$excluded == 0) %>% 
                        dplyr::filter(.data$removed == 0),
                      by = c("databaseId", "conceptId")) %>% 
    dplyr::select(-.data$excluded, -.data$removed)
  
  final <- dplyr::bind_rows(excluded,
                            notExcluded) %>% 
    dplyr::arrange(.data$conceptId) %>% 
    dplyr::select(-.data$cohortId,
                  -.data$conceptSetId)
  return(final)
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
#' Returns a data frame (tibble)
#'
#' @export
getResultsCohortCount <- function(dataSource,
                                  cohortIds = NULL,
                                  databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "cohortCount"
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
#' Returns a data frame (tibble).
#'
#' @export
getResultsInclusionRuleStatistics <- function(dataSource,
                                              cohortIds = NULL,
                                              databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "inclusionRuleStats"
  )
  return(data)
}

#' Returns data from cohort_inclusion table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from cohort_inclusion table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble).
#'
#' @export
getResultsCohortInclusion <- function(dataSource,
                                      cohortIds = NULL,
                                      databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "cohortInclusion"
  )
  return(data)
}


#' Returns data from cohort_inclusion_stats table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from cohort_inclusion_stats table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble).
#'
#' @export
getResultsCohortInclusionStats <- function(dataSource,
                                           cohortIds = NULL,
                                           databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "cohortInclusionStats"
  )
  return(data)
}


#' Returns data from cohort_summary_stats table of Cohort Diagnostics results data model
#'
#' @description
#' Returns data from cohort_summary_stats table of Cohort Diagnostics results data model
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @template DatabaseIds
#'
#' @return
#' Returns a data frame (tibble).
#'
#' @export
getResultsCohortSummaryStats <- function(dataSource,
                                         cohortIds = NULL,
                                         databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "cohortSummaryStats"
  )
  return(data)
}

# Results ----
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
#' table in Cohort Diagnostics results data model. There are three list objects, labeled
#' m for monthly, q for quarterly and y for yearly. The periodBegin variable is in the
#' format of tsibble::yearmonth for monthly, tsibble::yearquarter for quarter and integer
#' for year.
#'
#' @export
getResultsFixedTimeSeries <- function(dataSource,
                                      cohortIds = NULL,
                                      databaseIds = NULL,
                                      seriesType = c('T1', 'T2', 'T3')) {
  fixedTimeSeriesColumnNameLong <- dplyr::tibble(
    shortName = c("records", "subjects", "personDays", 
                  "subjectsStartIn", "personDaysIn", "subjectsEndIn", 
                  "recordsStart", "subjectsStart", 
                  "recordsEnd", "subjectsEnd"
    ),
    longName = c("Records Overlapping",
                 "Subjects Overlapping",
                 "Days Overlapping", 
                 "Subjects Incident",
                 "Person days Incident",
                 "Subjects Incident Ending",
                 "Records Starting", 
                 "Subjects Starting",
                 "Records Ending", 
                 "Subjects Ending"),
    sequence = c(1,2,3,4,5,6,7,8,9,10)
  ) %>% 
    dplyr::arrange(.data$sequence)
  # cohortId = 0, represent all persons in observation_period
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = c(cohortIds, 0) %>% unique(),
    databaseId = databaseIds,
    seriesType = seriesType,
    dataTableName = "timeSeries"
  )
  if (any(is.null(data),
          nrow(data) ==0)) {
    return(NULL)
  }
  dataCohort0 <- data %>%
    dplyr::filter(.data$cohortId == 0) %>%
    dplyr::select(-.data$cohortId) %>%
    tidyr::crossing(dplyr::tibble(cohortId = cohortIds))
  data <-
    dplyr::bind_rows(data %>% dplyr::filter(.data$cohortId > 0),
                     dataCohort0)
  
  t1 <- data %>%
    dplyr::filter(.data$seriesType == 'T1') %>%
    dplyr::select(-.data$seriesType)
  t3 <- data %>%
    dplyr::filter(.data$seriesType == 'T3') %>%
    dplyr::select(-.data$seriesType)
  
  if (all(nrow(t1) > 0,
          nrow(t3) > 0)) {
    r1 <- t1 %>%
      dplyr::full_join(
        t3,
        by = c(
          'databaseId',
          'cohortId',
          'periodBegin',
          'calendarInterval'
        ),
        suffix = c("_1",
                   "_2")
      ) %>%
      dplyr::mutate(
        records = .data$records_1 / .data$records_2,
        subjects = .data$subjects_1 / .data$subjects_2,
        personDays = .data$personDays_1 / .data$personDays_2,
        personDaysIn = NA,
        recordsStart = .data$recordsStart_1 / .data$recordsStart_2,
        subjectsStart = .data$subjectsStart_1 / .data$subjectsStart_2,
        subjectsStartIn = NA,
        recordsEnd = .data$recordsEnd_1 / .data$recordsEnd_2,
        subjectsEnd = .data$subjectsEnd_1 / .data$subjectsEnd_2,
        subjectsEndIn = NA
      ) %>%
      dplyr::select(-dplyr::ends_with("1")) %>%
      dplyr::select(-dplyr::ends_with("2")) %>%
      dplyr::mutate(seriesType = 'R1')
    data <- dplyr::bind_rows(data, r1)
  }
  
  # commenting out time series segments that have persons/subject embedded within observation period, as it 
  # does not seem to have value
  timeSeriesDescription <- dplyr::tibble(
    seriesType = c('T1', 'T2', 'T3',# 'T4', 'T5', 'T6',
                   'R1'),
    seriesTypeShort = c(
      'Subjects in cohort period',
      'Subjects in obs period',
      'Persons in obs period',
      # ,
      # 'Subjects cohort embedded in period',
      # 'Subjects observation embedded in period',
      # 'Persons observation embedded in period',
      'Percent of Subjects among persons in period'
    ),
    seriesTypeLong = c(
      'Subjects in the cohort who have atleast one cohort day in calendar period',
      'Subjects in the cohort who have atleast one observation day in calendar period',
      'Persons in the data source who have atleast one observation day in calendar period',
      # 'Subjects in the cohorts whose cohort period are embedded within calendar period',
      # 'Subjects in the cohorts whose observation period is embedded within calendar period',
      # 'Persons in the observation table whose observation period is embedded within calendar period',
      'Proportion of persons who met the cohort definition in the calendar period to persons in the observation period in the same calendar period'
    )
  )
  
  if (any(is.null(data),
          nrow(data) == 0)) {
    return(NULL)
  }
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
        recordsStart = abs(.data$recordsStart),
        subjectsStart = abs(.data$subjectsStart),
        recordsEnd = abs(.data$recordsEnd),
        subjectsEnd = abs(.data$subjectsEnd)
      ) %>%
      tsibble::as_tsibble(
        key = c(.data$databaseId, .data$cohortId, .data$seriesType),
        index = .data$periodBegin
      ) %>%
      dplyr::arrange(.data$databaseId, .data$cohortId, .data$seriesType)
    dataList[[intervals[[i]]]] <- intervalData
    attr(x = dataList[[intervals[[i]]]],
         which = 'timeSeriesDescription') <- timeSeriesDescription
    attr(x = dataList[[intervals[[i]]]],
         which = 'timeSeriesColumnNameCrosswalk') <- fixedTimeSeriesColumnNameLong
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
#' Returns a data frame (tibble).
#'
#' @export
getResultsTimeDistribution <- function(dataSource,
                                       cohortIds = NULL,
                                       databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
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
#' Returns a data frame (tibble).
#'
#' @export
getResultsIncidenceRate <- function(dataSource,
                                    cohortIds = NULL,
                                    databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "incidenceRate"
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
#' @template ConceptIds
#' 
#' @param CoConceptIds A vector of integers representing co-concept ids
#'
#' @template DatabaseIds
#'
#' @param daysRelativeIndex  A vector of integers representing the offset in relation to index date (-40 to 40)
#' @return
#' Returns a data frame (tibble) with results that conform to index_event_breakdown
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsIndexEventBreakdown <- function(dataSource,
                                          cohortIds = NULL,
                                          databaseIds = NULL,
                                          conceptIds = NULL,
                                          coConceptIds = 0,
                                          daysRelativeIndex = 0) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "indexEventBreakdown",
    conceptId = conceptIds,
    coConceptId = coConceptIds,
    daysRelativeIndex = daysRelativeIndex
  )
  #!!!!!!!!!!!!!!!!!!!TEMPORARY FIX ###!!!!!!!!!!to be removed - index event breakdown daysRelativeIndex fix
  if (!is.null(data)) {
    data <- data %>% 
      dplyr::mutate(daysRelativeIndex = .data$daysRelativeIndex * -1)
    warning("temporary fix = please remove")
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
getResultsVisitContext <- function(dataSource,
                                   cohortIds = NULL,
                                   databaseIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "visitContext"
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
#' @template CohortIds
#'
#' @template ComparatorCohortIds
#'
#' @template DatabaseIds
#' 
#' @param startDays A vector of days in relation to cohort_start_date of target 
#' 
#' @param endDays A vector of days in relation to cohort_end_date of target 
#'
#' @return
#' Returns a data frame (tibble) with results that conform to cohort_relationships
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsCohortRelationships <- function(dataSource,
                                          cohortIds = NULL,
                                          comparatorCohortIds = NULL,
                                          databaseIds = NULL,
                                          startDays = NULL,
                                          endDays = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource = dataSource,
    cohortId = cohortIds,
    comparatorCohortId = comparatorCohortIds,
    databaseId = databaseIds,
    startDay = startDays,
    endDay = endDays,
    dataTableName = "cohortRelationships"
  )
  return(data)
}



#' Returns data for use in cohort co-occurrence matrix
#'
#' @description
#' Returns a a data frame (tibble) that shows the percent (optionally number) of subjects
#' in target cohort that are also in comparator cohort at certain days relative to 
#' first start date of a subject in target cohort.
#'
#' @template DataSource
#'
#' @template TargetCohortIds
#' 
#' @template ComparatorCohortIds
#'
#' @template DatabaseIds
#' 
#' @template StartDays
#' 
#' @template endDays
#' 
#' @param showPercent Return percent instead of raw numbers
#'
#' @return
#' Returns a data frame (tibble). Note - the computation is in relation
#' to first start of target cohort only.
#'
#' @export
getResultsCohortCoOccurrenceMatrix <- function(dataSource,
                                               targetCohortIds = NULL,
                                               comparatorCohortIds = NULL,
                                               databaseIds = NULL,
                                               startDays = NULL,
                                               endDays = NULL,
                                               showPercent = TRUE) {
  cohortCount <- getResultsCohortCount(
    dataSource = dataSource,
    cohortIds = c(targetCohortIds, comparatorCohortIds) %>% unique(),
    databaseIds = databaseIds
  )
  if (is.null(data$cohortCount)) {
    return(NULL)
  }
  
  cohortRelationship <- getResultsCohortRelationships(
    dataSource = dataSource,
    cohortIds = targetCohortIds,
    comparatorCohortIds = comparatorCohortIds,
    databaseIds = databaseIds,
    startDays = startDays,
    endDays = endDays
  )
  if (is.null(cohortRelationship)) {
    return(NULL)
  }
  cohortRelationship <- cohortRelationship %>%
    dplyr::mutate(records = 0) %>%
    dplyr::rename(
      "targetCohortId" = .data$cohortId,
      "comparatorCohortId" = .data$comparatorCohortId,
      "bothSubjects" = .data$subjects,
      "bothRecords" = .data$records
    ) %>%
    dplyr::select(
      .data$databaseId,
      .data$targetCohortId,
      .data$comparatorCohortId,
      .data$startDay,
      .data$endDay,
      # overlap - comparator period overlaps target period (offset)
      .data$bothSubjects,
      .data$bothRecords,
      # comparator start on Target Start
      .data$recCsOnTs,
      .data$subCsOnTs,
      .data$subCsWindowT
    )
  
  coOccurrenceMatrix <- cohortRelationship %>%
    dplyr::filter(.data$startDay == .data$endDay)  %>%
    dplyr::mutate(dayName = dplyr::case_when(
      .data$startDay < 0 ~ paste0("dayNeg", abs(.data$startDay)),
      TRUE ~ paste0("dayPos", abs(.data$startDay))
    )) %>%
    dplyr::select(
      .data$databaseId,
      .data$targetCohortId,
      .data$comparatorCohortId,
      .data$dayName,
      .data$bothSubjects,
      .data$subCsOnTs,
      .data$subCsWindowT
    )
  
  matrixOverlap <- coOccurrenceMatrix %>%
    dplyr::filter(!is.na(.data$bothSubjects)) %>%
    dplyr::select(
      .data$databaseId,
      .data$targetCohortId,
      .data$comparatorCohortId,
      .data$dayName,
      .data$bothSubjects
    ) %>%
    tidyr::pivot_wider(
      id_cols = c(
        .data$databaseId,
        .data$targetCohortId,
        .data$comparatorCohortId
      ),
      names_from = .data$dayName,
      values_from = .data$bothSubjects
    ) %>%
    dplyr::mutate(type = 'overlap')
  
  matrixStart <- coOccurrenceMatrix %>%
    dplyr::filter(!is.na(.data$subCsOnTs)) %>%
    dplyr::select(
      .data$databaseId,
      .data$targetCohortId,
      .data$comparatorCohortId,
      .data$dayName,
      .data$subCsOnTs
    ) %>%
    tidyr::pivot_wider(
      id_cols = c(
        .data$databaseId,
        .data$targetCohortId,
        .data$comparatorCohortId
      ),
      names_from = .data$dayName,
      values_from = .data$subCsOnTs
    ) %>%
    dplyr::mutate(type = 'start')
  
  matrixStartWindows <- coOccurrenceMatrix %>%
    dplyr::filter(!is.na(.data$subCsWindowT)) %>%
    dplyr::select(
      .data$databaseId,
      .data$targetCohortId,
      .data$comparatorCohortId,
      .data$dayName,
      .data$subCsWindowT
    ) %>%
    tidyr::pivot_wider(
      id_cols = c(
        .data$databaseId,
        .data$targetCohortId,
        .data$comparatorCohortId
      ),
      names_from = .data$dayName,
      values_from = .data$subCsWindowT
    ) %>%
    dplyr::mutate(type = 'startWindow')
  
  matrix <- dplyr::bind_rows(matrixOverlap,
                             matrixStart,
                             matrixStartWindows)
  if (showPercent) {
    matrix <- matrix %>%
      dplyr::inner_join(
        cohortCount %>%
          dplyr::select(.data$databaseId,
                        .data$cohortId,
                        .data$cohortSubjects) %>%
          dplyr::rename("targetCohortId" = .data$cohortId),
        by = c("targetCohortId", "databaseId")
      ) %>%
      dplyr::mutate(dplyr::across(.cols = dplyr::starts_with("day")) / .data$cohortSubjects)
  }
  return(matrix)
}








#' Returns data for use in cohort_overlap
#'
#' @description
#' Returns data for use in cohort_overlap
#'
#' @template DataSource
#'
#' @param targetCohortIds A vector of cohort ids representing target cohorts
#' 
#' @param comparatorCohortIds A vector of cohort ids representing comparator cohorts
#'
#' @template DatabaseIds
#'
#' @return
#' Returns data for use in cohort_overlap
#'
#' @export
getResultsCohortOverlap <- function(dataSource,
                                    targetCohortIds = NULL,
                                    comparatorCohortIds = NULL,
                                    databaseIds = NULL) {
  cohortIds <- c(targetCohortIds, comparatorCohortIds) %>% unique()
  cohortCounts <-
    getResultsCohortCount(dataSource = dataSource,
                          cohortIds = cohortIds,
                          databaseIds = databaseIds)
  
  if (any(is.null(cohortCounts),
          nrow(cohortCounts) == 0)) {
    return(NULL)
  }
  
  cohortRelationship <-
    getResultsCohortRelationships(dataSource = dataSource,
                                  cohortIds = cohortIds,
                                  comparatorCohortIds = comparatorCohortIds,
                                  databaseIds = databaseIds, 
                                  startDays = c(-99999,0),
                                  endDays = c(99999,0))
  
  if (any(is.null(cohortRelationship),
          nrow(cohortRelationship) == 0)) {
    return(NULL)
  }
  
  fullOffSet <-  cohortRelationship %>%
    dplyr::filter(.data$startDay == -99999) %>%
    dplyr::filter(.data$endDay == 99999) %>%
    dplyr::filter(.data$cohortId %in% targetCohortIds) %>%
    dplyr::filter(.data$comparatorCohortId %in% comparatorCohortIds) %>%
    dplyr::select(.data$databaseId,
                  .data$cohortId,
                  .data$comparatorCohortId,
                  .data$subjects) %>%
    dplyr::inner_join(
      cohortCounts %>%
        dplyr::select(-.data$cohortEntries) %>%
        dplyr::rename(targetCohortSubjects = .data$cohortSubjects),
      by = c('databaseId', 'cohortId')
    ) %>%
    dplyr::mutate(tOnlySubjects = .data$targetCohortSubjects - .data$subjects) %>%
    dplyr::inner_join(
      cohortCounts %>%
        dplyr::select(-.data$cohortEntries) %>%
        dplyr::rename(
          comparatorCohortSubjects = .data$cohortSubjects,
          comparatorCohortId = .data$cohortId
        ),
      by = c('databaseId', 'comparatorCohortId')
    ) %>%
    dplyr::mutate(cOnlySubjects = .data$comparatorCohortSubjects - .data$subjects) %>%
    dplyr::mutate(eitherSubjects = .data$cOnlySubjects + .data$tOnlySubjects + .data$subjects) %>%
    dplyr::rename(targetCohortId = .data$cohortId,
                  bothSubjects = .data$subjects) %>%
    dplyr::select(
      .data$databaseId,
      .data$targetCohortId,
      .data$comparatorCohortId,
      .data$bothSubjects,
      .data$tOnlySubjects,
      .data$cOnlySubjects,
      .data$eitherSubjects
    )
  
  noOffset <- cohortRelationship %>%
    dplyr::filter(.data$comparatorCohortId %in% comparatorCohortIds) %>%
    dplyr::filter(.data$cohortId %in% targetCohortIds) %>%
    dplyr::filter(.data$startDay == 0) %>%
    dplyr::filter(.data$endDay == 0) %>%
    dplyr::select(
      .data$databaseId,
      .data$cohortId,
      .data$comparatorCohortId,
      .data$subCsBeforeTs,
      .data$subCWithinT,
      .data$subCsAfterTs,
      .data$subCsAfterTe,
      .data$subCsBeforeTs,
      .data$subCsBeforeTe,
      .data$subCsOnTs,
      .data$subCsOnTe
    ) %>% 
    dplyr::rename(cBeforeTSubjects = .data$subCsBeforeTs,
                  targetCohortId = .data$cohortId,
                  cInTSubjects = .data$subCWithinT,
                  cStartAfterTStart = .data$subCsAfterTs,
                  cStartAfterTEnd = .data$subCsAfterTe,
                  cStartBeforeTStart = .data$subCsBeforeTs,
                  cStartBeforeTEnd = .data$subCsBeforeTe,
                  cStartOnTStart = .data$subCsOnTs,
                  cStartOnTEnd = .data$subCsOnTe)
  
  result <- fullOffSet %>%
    dplyr::left_join(noOffset,
                     by = c('databaseId', 'targetCohortId', 'comparatorCohortId')) %>%
    dplyr::filter(.data$targetCohortId != .data$comparatorCohortId)
  
  return(result)
}


## Characterization ----
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
#' @param minThreshold Do you want to set the minimum threshold for db extraction
#'
#' @return
#' Returns a list object with covariateValue, covariateValueDist,
#' covariateRef, analysisRef output of feature extraction along with
#' concept information.
#'
#' @export
getFeatureExtractionCharacterization <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL,
           minThreshold = 0.01) {
    analysisRef <- getResultsAnalysisRef(dataSource = dataSource)
    covariateRef <- getResultsCovariateRef(dataSource = dataSource)
    concept <- getConcept(dataSource = dataSource,
                          conceptIds = covariateRef$conceptId %>% unique())
    covariateValue <-
      getResultsCovariateValue(dataSource = dataSource,
                               cohortIds = cohortIds,
                               databaseIds = databaseIds,
                               minThreshold = minThreshold)
    covariateValueDist <-
      getResultsCovariateValueDist(dataSource = dataSource,
                                   cohortIds = cohortIds,
                                   databaseIds = databaseIds,
                                   minThreshold = minThreshold)
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
#' @param minThreshold Do you want to set the minimum threshold for db extraction
#' 
#' @return
#' Returns a list object with temporalCovariateValue, temporalCovariateValueDist,
#' temporalCovariateRef, temporalAnalysisRef, temporalTimeRef, Concept output of feature extraction.
#'
#' @export
getFeatureExtractionTemporalCharacterization <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NUL,
           minThreshold = 0.01) {
    temporalAnalysisRef <-
      getResultsTemporalAnalysisRef(dataSource = dataSource)
    temporalCovariateRef <-
      getResultsTemporalCovariateRef(dataSource = dataSource)
    temporalTimeRef <-
      getResultsTemporalTimeRef(dataSource = dataSource)
    concept <- getConcept(dataSource = dataSource,
                          conceptIds = temporalCovariateRef$conceptId %>% unique())
    temporalCovariateValue <-
      getResultsTemporalCovariateValue(dataSource = dataSource,
                                       cohortIds = cohortIds,
                                       databaseIds = databaseIds)
    # temporary till https://github.com/OHDSI/FeatureExtraction/issues/127
    temporalCovariateValueDist <-
      getResultsTemporalCovariateValueDist(dataSource = dataSource,
                                           cohortIds = cohortIds,
                                           databaseIds = databaseIds)
    if (all(!is.null(temporalCovariateValueDist),
            nrow(temporalCovariateValueDist) > 0)) {
      temporalCovariateValueDist <- temporalCovariateValueDist %>%
        dplyr::filter(!is.na(.data$timeId))
    }
    
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
getCohortRelationshipCharacterizationResults <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL) {
    
    browser()
    # need to add standardized difference
    # 1. for each time window get its corresponding denominator population size
    # 2. compute proportion dplyr::mutate(p = .data$sumValue / .data$populationSize)
    # 3. put a check "During characterization, population size (denominator) was found to be smaller than features Value (numerator).",
    #"- this may have happened because of an error in Feature generation process. Please contact the package developer."
    # 4. compute sd as dplyr::mutate(sd = sqrt(.data$p * (1 - .data$p)))
    cohortCounts <-
      getResultsCohortCount(dataSource = dataSource,
                            cohortIds = cohortIds,
                            databaseIds = databaseIds)
    cohort <- getResultsCohort(dataSource = dataSource)
    
    cohortRelationships <-
      getResultsCohortRelationships(dataSource = dataSource,
                                    cohortIds = cohortIds,
                                    databaseIds = databaseIds)
    
    analysisRef <-
      dplyr::tibble(analysisId = c(-101,-201,-301), 
                    analysisName = c("cohortOccurrence", "cohortEraStart", "cohortEraOverlap"), 
                    domainId = "Cohort",
                    isBinary = "Y",
                    missingMeansZero = "Y")
    
    covariateRef <- tidyr::crossing(cohort,
                                    analysisRef %>%
                                      dplyr::select(.data$analysisId)) %>%
      dplyr::mutate(covariateId = (.data$cohortId * -1000) + .data$analysisId) %>%
      dplyr::mutate(covariateName = paste0(.data$shortName)) %>%
      dplyr::mutate(conceptId = .data$cohortId * -1) %>%
      dplyr::arrange(.data$covariateId) %>%
      dplyr::select(.data$analysisId,
                    .data$conceptId,
                    .data$covariateId,
                    .data$covariateName
                    )
    
    concept <- cohort %>%
      dplyr::filter(.data$cohortId %in% abs(covariateRef$conceptId) %>% unique()) %>%
      dplyr::mutate(
        conceptId = .data$cohortId * -1,
        conceptName = .data$cohortName,
        domainId = 'Cohort',
        vocabularyId = 'Cohort',
        conceptClassId = 'Cohort',
        standardConcept = 'S',
        conceptCode = as.character(.data$cohortId),
        validStartDate = as.Date('2002-01-31'),
        validEndDate = as.Date('2099-12-31'),
        invalidReason = as.character(NA)
      ) %>%
      dplyr::select(
        .data$conceptId,
        .data$conceptName,
        .data$domainId,
        .data$vocabularyId,
        .data$conceptClassId,
        .data$standardConcept,
        .data$conceptCode,
        .data$validStartDate,
        .data$validEndDate
      ) %>%
      dplyr::arrange(.data$conceptId)
    
    covariateValue1 <- cohortRelationships %>%
      dplyr::left_join(cohortCount %>% 
                         dplyr::select(.data$cohortId, 
                                       .data$databaseId,
                                       .data$cohortSubjects),
                       by = c("cohortId", "databaseId")) %>% 
      dplyr::mutate(cohortOccurrence = .data$subCsBeforeTs/.data$cohortSubjects) %>% 
      dplyr::mutate(cohortEraStart = .data$subCsWindowTs/.data$cohortSubjects) %>% 
      dplyr::mutate(cohortEraOverlap = (.data$subCeWindowT + .data$subCsWindowT - .data$subCWithinT)/
                      .data$cohortSubjects) %>% 
      dplyr::select(.data$databaseId,
                    .data$cohortId,
                    .data$comparatorCohortId,
                    .data$startDay,
                    .data$endDay,
                    .data$cohortOccurrence,
                    .data$cohortEraStart,
                    .data$cohortEraOverlap) %>% 
      tidyr::pivot_longer(cols = dplyr::all_of(c("cohortOccurrence", "cohortEraStart", "cohortEraOverlap")), 
                          names_to = "analysisName", 
                          values_to = "mean")
    
    covariateValue2 <- cohortRelationships %>%
      dplyr::mutate(cohortOccurrence = .data$subCsBeforeTs) %>% 
      dplyr::mutate(cohortEraStart = .data$subCsWindowTs) %>% 
      dplyr::mutate(cohortEraOverlap = (.data$subCeWindowT + .data$subCsWindowT - .data$subCWithinT)) %>% 
      dplyr::select(.data$databaseId,
                    .data$cohortId,
                    .data$comparatorCohortId,
                    .data$startDay,
                    .data$endDay,
                    .data$cohortOccurrence,
                    .data$cohortEraStart,
                    .data$cohortEraOverlap) %>% 
      tidyr::pivot_longer(cols = dplyr::all_of(c("cohortOccurrence", "cohortEraStart", "cohortEraOverlap")), 
                          names_to = "analysisName", 
                          values_to = "sumValue")
    
    
    covariateValue <- covariateValue1 %>% 
      dplyr::inner_join(covariateValue2,
                        by = c("databaseId", "cohortId", "comparatorCohortId", "startDay", "endDay", "analysisName")) %>% 
      dplyr::inner_join(analysisRef %>% 
                          dplyr::select(.data$analysisId, .data$analysisName), 
                        by = "analysisName") %>% 
      dplyr::select(-.data$analysisName) %>%
      dplyr::mutate(covariateId = (.data$comparatorCohortId * -1000) + .data$analysisId) %>% 
      dplyr::mutate(sd = as.double(NA)) %>% 
      dplyr::select(-.data$analysisId) %>% 
      dplyr::select(.data$cohortId, .data$covariateId, .data$databaseId,
                    .data$startDay, .data$endDay, .data$mean,
                    .data$sd, .data$sumValue)
    
    data <- list(
      covariateRef = covariateRef,
      covariateValue = covariateValue,
      covariateValueDist = NULL,
      analysisRef = analysisRef,
      concept = concept
    )
    return(data)
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
#' @param temporalTimeRef   A dataframe object with three columns timeId (integer), startDay (integer), endDay (integer)
#'
#' @return
#' Returns a list object with temporalCovariateValue,
#' temporalCovariateRef, temporalAnalysisRef, temporalRef output of cohort as features.
#'
#' @export
getCohortAsFeatureTemporalCharacterizationResults <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL,
           temporalTimeRef = getResultsTemporalTimeRef(dataSource = dataSource)) {
    if (is.null(temporalTimeRef)) {
      return(NULL)
    }
    # meta information
    cohortCounts <-
      getResultsCohortCount(dataSource = dataSource,
                            cohortIds = cohortIds,
                            databaseIds = databaseIds)
    cohort <- getResultsCohort(dataSource = dataSource)
    
    seqStart30 <- seq(-420, +420, by = 30)
    seqEnd30 <- seqStart30 + 30
    cohortRelationships <-
      getResultsCohortRelationships(dataSource = dataSource,
                                    cohortIds = cohortIds,
                                    startDays = c(seqStart30),
                                    endDays = c(seqEnd30),
                                    databaseIds = databaseIds)
    
    if (is.null(cohortRelationships) ||
        nrow(cohortRelationships) == 0) {
      return(NULL)
    }
    
    # comparator cohort was on or after target cohort
    summarizeCohortRelationship <- function(data,
                                            valueField = 'records',
                                            analysisId,
                                            temporalTimeRef,
                                            cohortCounts) {
      browser()
      if (is.null(data) || nrow(data) == 0) {
        return(NULL)
      }
      data$sumValue <- data[[valueField]]
      data <- data  %>%
        dplyr::inner_join(temporalTimeRef,
                          by = c("startDay", "endDay")) %>%
        dplyr::select(
          .data$databaseId,
          .data$cohortId,
          .data$comparatorCohortId,
          .data$timeId,
          .data$startDay,
          .data$endDay,
          .data$sumValue
        )
      
      denominator <- cohortCounts  %>%
        dplyr::rename(denominator = .data$cohortSubjects) %>%
        dplyr::select(.data$cohortId,
                      .data$databaseId,
                      .data$denominator)
      
      data <- data %>%
        dplyr::inner_join(denominator, by = c('databaseId', 'cohortId')) %>%
        dplyr::mutate(mean = .data$sumValue / .data$denominator) %>%
        dplyr::mutate(sd = NA) %>%
        # dplyr::mutate(sd = sqrt(.data$mean * (1 - .data$mean)))
        dplyr::mutate(analysisId = !!analysisId) %>%
        dplyr::mutate(covariateId = (.data$comparatorCohortId * -1000)+!!analysisId) %>%
        dplyr::select(
          .data$cohortId,
          .data$covariateId,
          .data$timeId,
          .data$startDay,
          .data$endDay,
          .data$sumValue,
          .data$mean,
          .data$sd,
          .data$databaseId
        )
      return(data)
    }
    
    analysisId <- c(-101,-201)
    analysisName <- c("CohortEraStart", "CohortEraOverlap")
    valueField <- c("subCsAfterTs",
                    "subjects")
    analysisRef <-
      dplyr::tibble(analysisId, analysisName, valueField) %>%
      dplyr::mutate(isBinary = 'Y',
                    missingMeansZero = 'Y') %>%
      dplyr::arrange(.data$analysisId) %>%
      dplyr::mutate(domainId = 'Cohort') %>%
      dplyr::select(
        .data$analysisId,
        .data$analysisName,
        .data$valueField,
        .data$domainId,
        .data$isBinary,
        .data$missingMeansZero
      ) %>%
      dplyr::distinct()
    
    result <- list()
    for (j in (1:nrow(analysisRef))) {
      result[[j]] <-
        summarizeCohortRelationship(
          data = cohortRelationships,
          valueField = analysisRef[j,]$valueField,
          analysisId = analysisRef[j,]$analysisId,
          temporalTimeRef = temporalTimeRef,
          cohortCounts = cohortCounts
        )
    }
    result <- dplyr::bind_rows(result)
    
    if (nrow(result) == 0) {
      result <- NULL
    }
    
    covariateRef <- tidyr::crossing(cohort,
                                    analysisRef %>%
                                      dplyr::select(.data$analysisId) %>%
                                      dplyr::distinct()) %>%
      dplyr::mutate(covariateId = (.data$cohortId * -1000) + .data$analysisId) %>%
      dplyr::mutate(covariateName = paste0(.data$cohortName)) %>% #, "(", .data$covariateId, ")")) %>%
      dplyr::mutate(conceptId = .data$cohortId * -1) %>%
      dplyr::arrange(.data$covariateId) %>%
      dplyr::select(.data$covariateId,
                    .data$covariateName,
                    .data$analysisId,
                    .data$conceptId) %>%
      dplyr::distinct()
    
    concept <- cohort %>%
      dplyr::filter(.data$cohortId %in% abs(covariateRef$conceptId) %>% unique()) %>%
      dplyr::mutate(
        conceptId = .data$cohortId * -1,
        conceptName = .data$cohortName,
        domainId = 'Cohort',
        vocabularyId = 'Cohort',
        conceptClassId = 'Cohort',
        standardConcept = 'S',
        conceptCode = as.character(.data$cohortId),
        validStartDate = as.Date('2002-01-31'),
        validEndDate = as.Date('2099-12-31'),
        invalidReason = as.character(NA)
      ) %>%
      dplyr::select(
        .data$conceptId,
        .data$conceptName,
        .data$domainId,
        .data$vocabularyId,
        .data$conceptClassId,
        .data$standardConcept,
        .data$conceptCode,
        .data$validStartDate,
        .data$validEndDate
      ) %>%
      dplyr::arrange(.data$conceptId)
    
    if ('valueField' %in% colnames(analysisRef)) {
      analysisRef$valueField <- NULL
    }
    return(
      list(
        temporalCovariateRef = covariateRef,
        temporalCovariateValue = result,
        temporalCovariateValueDist = NULL,
        temporalAnalysisRef = analysisRef,
        temporalTimeRef = temporalTimeRef,
        concept = concept
      )
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
#' @param featureExtractionCharacterization Do you want to get feature extraction characterization results?
#' 
#' @param cohortRelationshipCharacterizationResults Do you want to get cohort relationship characterization results?
#'
#' @param minThreshold Do you want to set the minimum threshold for db extraction
#'
#' @return
#' Returns multiple characterization output
#'
#' @export
getMultipleCharacterizationResults <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL,
           featureExtractionCharacterization = TRUE,
           cohortRelationshipCharacterizationResults = TRUE,
           minThreshold = 0.01) {
    
    addCharacterizationSource <-
      function(x, characterizationSourceValue) {
        exepectedDataTables <-
          c('analysisRef',
            'covariateRef',
            'covariateValue',
            'covariateValueDist',
            'concept')
        for (i in (1:length(exepectedDataTables))) {
          if (exepectedDataTables[[i]] %in% names(x)) {
            if (!is.null(x[[exepectedDataTables[[i]]]])) {
              x[[exepectedDataTables[[i]]]] <- x[[exepectedDataTables[[i]]]] %>%
                dplyr::mutate(characterizationSource = !!characterizationSourceValue)
            }
          }
        }
        return(x)
      }
    
    analysisRef <- dplyr::tibble()
    covariateRef <- dplyr::tibble()
    covariateValue <- dplyr::tibble()
    covariateValueDist <- dplyr::tibble()
    concept <- dplyr::tibble()
    temporalTimeRef <- dplyr::tibble()
    
    if (featureExtractionCharacterization) {
      featureExtractioncharacterization <-
        getFeatureExtractionCharacterization(
          dataSource = dataSource,
          cohortIds = cohortIds,
          databaseIds = databaseIds,
          minThreshold = minThreshold
        )
      
      featureExtractioncharacterization <-
        addCharacterizationSource(x = featureExtractioncharacterization,
                                  characterizationSourceValue = 'F')
      
      analysisRef <-
        dplyr::bind_rows(analysisRef,
                         featureExtractioncharacterization$analysisRef) %>% dplyr::distinct()
      
      covariateRef <-
        dplyr::bind_rows(covariateRef,
                         featureExtractioncharacterization$covariateRef) %>% dplyr::distinct()
      
      covariateValue <-
        dplyr::bind_rows(covariateValue,
                         featureExtractioncharacterization$covariateValue)
      
      covariateValueDist <-
        dplyr::bind_rows(covariateValueDist,
                         featureExtractioncharacterization$covariateValueDist) %>% dplyr::distinct()
      
      concept <-
        dplyr::bind_rows(concept,
                         featureExtractioncharacterization$concept) %>% dplyr::distinct()
    }
    
    if (cohortRelationshipCharacterizationResults) {
      cohortRelationshipCharacterizationResults <-
        getCohortRelationshipCharacterizationResults(dataSource = dataSource,
                                                     cohortIds = cohortIds,
                                                     databaseIds = databaseIds)
      cohortRelationshipCharacterizationResults <-
        addCharacterizationSource(x = cohortRelationshipCharacterizationResults,
                                  characterizationSourceValue = 'C')
      
      analysisRef <-
        dplyr::bind_rows(analysisRef,
                         cohortRelationshipCharacterizationResults$analysisRef) %>% dplyr::distinct()
      
      covariateRef <-
        dplyr::bind_rows(covariateRef,
                         cohortRelationshipCharacterizationResults$covariateRef) %>% dplyr::distinct()
      
      covariateValue <-
        dplyr::bind_rows(covariateValue,
                         cohortRelationshipCharacterizationResults$covariateValue) %>% dplyr::distinct()
      
      covariateValueDist <-
        dplyr::bind_rows(
          covariateValueDist,
          cohortRelationshipCharacterizationResults$covariateValueDist
        ) %>% dplyr::distinct()
      
      concept <-
        dplyr::bind_rows(concept,
                         cohortRelationshipCharacterizationResults$concept) %>% dplyr::distinct()
    }
    
    if (all(!is.null(analysisRef),
            nrow(analysisRef) == 0)) {
      analysisRef <- NULL
    }
    if (!is.null(analysisRef)) {
      analysisRef <- analysisRef  %>%
        dplyr::arrange(.data$analysisId, .data$characterizationSource)
    }
    
    
    if (all(!is.null(covariateRef),
            nrow(covariateRef) == 0)) {
      covariateRef <- NULL
    }
    if (!is.null(covariateRef)) {
      covariateRef <- covariateRef %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$covariateId, .data$characterizationSource)
    }
    
    
    if (all(!is.null(covariateValue),
            nrow(covariateValue) == 0)) {
      covariateValue <- NULL
    }
    if (!is.null(covariateValue)) {
      covariateValue <- covariateValue %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$cohortId,
                       .data$covariateId,
                       .data$characterizationSource)
    }
    
    if (all(!is.null(covariateValueDist),
            nrow(covariateValueDist) == 0)) {
      covariateValueDist <- NULL
    }
    if (!is.null(covariateValueDist)) {
      covariateValueDist <- covariateValueDist %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$cohortId,
                       .data$covariateId,
                       .data$characterizationSource)
    }
    
    if (all(!is.null(concept),
            nrow(concept) == 0)) {
      concept <- NULL
    }
    if (!is.null(concept)) {
      concept <- concept %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$conceptId)
    }
    
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

# not exported
getResultsCovariateValue <- function(dataSource,
                                     cohortIds,
                                     databaseIds,
                                     minThreshold = 0.01) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "covariateValue",
    minThreshold = minThreshold
  )
  return(data)
}

# not exported
getResultsCovariateValueDist <- function(dataSource,
                                         cohortIds,
                                         databaseIds,
                                         minThreshold) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "covariateValueDist",
    minThreshold = minThreshold
  )
  return(data)
}

# not exported
getResultsTemporalCovariateValue <- function(dataSource,
                                             cohortIds,
                                             databaseIds) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "temporalCovariateValue"
  )
  return(data)
}

# not exported
getResultsTemporalCovariateValueDist <- function(dataSource,
                                                 cohortIds,
                                                 databaseIds) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "temporalCovariateValueDist"
  )
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
    if (is.null(get(dataTableName, envir = dataSource))) {
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
        results_database_schema = dataSource$resultsDatabaseSchema,
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
    if (is.null(get(dataTableName, envir = dataSource))) {
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
        results_database_schema = dataSource$resultsDatabaseSchema,
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
    if (is.null(get(dataTableName, envir = dataSource))) {
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
        results_database_schema = dataSource$resultsDatabaseSchema,
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
    if (is.null(get(dataTableName, envir = dataSource))) {
      return(NULL)
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(dataTableName)
  } else {
    sql <- "SELECT *
            FROM @results_database_schema.analysis_ref;"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
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
    if (is.null(get(dataTableName, envir = dataSource))) {
      return(NULL)
    }
    if (nrow(get(dataTableName, envir = dataSource)) == 0) {
      return(NULL)
    }
    data <- get(dataTableName)
  } else {
    sql <- "SELECT *
            FROM @results_database_schema.temporal_analysis_ref;"
    data <-
      renderTranslateQuerySql(
        connection = dataSource$connection,
        sql = sql,
        results_database_schema = dataSource$resultsDatabaseSchema,
        snakeCaseToCamelCase = TRUE
      )
  }
  if (nrow(data) == 0) {
    return(NULL)
  }
  return(data)
}




#' Returns list with circe generated documentation
#'
#' @description
#' Returns list with circe generated documentation
#'
#' @param cohortDefinition An R object (list) with a list representation of the cohort definition expression.
#'
#' @param cohortName Name for the cohort definition
#'
#' @param embedText Any additional text to embed in the top of circe generated content.
#'
#' @param includeConceptSets Do you want to inclued concept set in the documentation
#' 
#' @return list object
#'
#' @export
getCirceRenderedExpression <- function(cohortDefinition,
                                       cohortName = NULL,
                                       embedText = NULL,
                                       includeConceptSets = FALSE) {
  cohortJson <-
    RJSONIO::toJSON(x = cohortDefinition,
                    digits = 23,
                    pretty = TRUE)
  circeExpression <-
    CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
  circeExpressionMarkdown <-
    CirceR::cohortPrintFriendly(circeExpression)
  circeConceptSetListmarkdown <-
    CirceR::conceptSetListPrintFriendly(circeExpression$conceptSets)
  
  if (hasData(embedText)) {
    circeExpressionMarkdown <-
      paste0("##### ",
             embedText,
             "\r\n\r\n",
             "# Cohort Definition:",
             "\r\n\r\n",
             "### ",
             cohortName,
             "\r\n\r\n",
             circeExpressionMarkdown)
  }
  if (includeConceptSets) {
    circeExpressionMarkdown <-
      paste0(circeExpressionMarkdown,
             "\r\n\r\n",
             "\r\n\r\n",
             "## Concept Sets:",
             "\r\n\r\n",
             circeConceptSetListmarkdown)
  }
  
  htmlExpressionCohort <-
    convertMdToHtml(circeExpressionMarkdown)
  htmlExpressionConceptSetExpression <-
    convertMdToHtml(circeConceptSetListmarkdown)
  return(
    list(
      cohortJson = cohortJson,
      cohortMarkdown = circeExpressionMarkdown,
      conceptSetMarkdown = circeConceptSetListmarkdown,
      cohortHtmlExpression = htmlExpressionCohort,
      conceptSetHtmlExpression = htmlExpressionConceptSetExpression
    )
  )
}

convertMdToHtml <- function(markdown) {
  markdown <- gsub("'", "%sq%", markdown)
  mdFile <- tempfile(fileext = ".md")
  htmlFile <- tempfile(fileext = ".html")
  SqlRender::writeSql(markdown, mdFile)
  rmarkdown::render(
    input = mdFile,
    output_format = "html_fragment",
    output_file = htmlFile,
    clean = TRUE,
    quiet = TRUE
  )
  html <- SqlRender::readSql(htmlFile)
  unlink(mdFile)
  unlink(htmlFile)
  html <- gsub("%sq%", "'", html)
  
  return(html)
}


#' Get domain information
#'
#' @param packageName e.g. 'CohortDiagnostics'
#'
#' @return
#' A list with two tibble data frame objects with domain information represented in wide and long format respectively.
getDomainInformation <- function(packageName = NULL) {
  ParallelLogger::logTrace("  - Reading domains.csv")
  
  
  if (is.null(packageName)) {
    if (file.exists("domains.csv")) {
      domains <-
        readr::read_csv("domains.csv",
                        guess_max = min(1e7),
                        col_types = readr::cols())
      ParallelLogger::logTrace(paste0("  - Retrieved domains.csv ",
                                      packageName))
    } else {
      stop("Can't find domains.csv file in package")
    }
  } else {
    pathToCsv <-
      system.file("csv",
                  "domains.csv",
                  package = packageName)
    if (!pathToCsv == "") {
      domains <-
        readr::read_csv(
          file = pathToCsv,
          guess_max = min(1e7),
          col_types = readr::cols()
        )
    } else {
      stop(paste0(
        "domains.csv was not found in installed package: ",
        packageName
      ))
    }
  }
  
  domains <- domains %>%
    .replaceNaInDataFrameWithEmptyString() %>%
    dplyr::mutate(domainTableShort = stringr::str_sub(
      string = toupper(.data$domain),
      start = 1,
      end = 2
    )) %>%
    dplyr::mutate(
      domainTableShort = dplyr::case_when(
        stringr::str_detect(string = tolower(.data$domain), pattern = 'era') ~ paste0(.data$domainTableShort, 'E'),
        TRUE ~ .data$domainTableShort
      )
    )
  
  domains$domainConceptIdShort <-
    stringr::str_replace_all(
      string = sapply(
        stringr::str_extract_all(
          string = camelCaseToTitleCase(snakeCaseToCamelCase(domains$domainConceptId)),
          pattern = '[A-Z]'
        ),
        paste,
        collapse = ' '
      ),
      pattern = " ",
      replacement = ""
    )
  domains$domainSourceConceptIdShort <-
    stringr::str_replace_all(
      string = sapply(
        stringr::str_extract_all(
          string = camelCaseToTitleCase(snakeCaseToCamelCase(domains$domainSourceConceptId)),
          pattern = '[A-Z]'
        ),
        paste,
        collapse = ' '
      ),
      pattern = " ",
      replacement = ""
    )
  domains <- domains  %>%
    dplyr::mutate(isEraTable = stringr::str_detect(string = .data$domainTable,
                                                   pattern = 'era'))
  data <- list()
  data$wide <- domains
  data$long <- dplyr::bind_rows(
    data$wide %>%
      dplyr::select(
        .data$domainTableShort,
        .data$domainTable,
        .data$domainConceptIdShort,
        .data$domainConceptId
      ) %>%
      dplyr::rename(
        domainFieldShort = .data$domainConceptIdShort,
        domainField = .data$domainConceptId
      ),
    data$wide %>%
      dplyr::select(
        .data$domainTableShort,
        .data$domainSourceConceptIdShort,
        .data$domainTable,
        .data$domainSourceConceptId
      ) %>%
      dplyr::rename(
        domainFieldShort = .data$domainSourceConceptIdShort,
        domainField = .data$domainSourceConceptId
      )
  ) %>%
    dplyr::distinct() %>%
    dplyr::filter(.data$domainFieldShort != "") %>%
    dplyr::mutate(eraTable = stringr::str_detect(string = .data$domainTable,
                                                 pattern = 'era')) %>%
    dplyr::mutate(isSourceField = stringr::str_detect(string = .data$domainField,
                                                      pattern = 'source'))
  return(data)
}



.replaceNaInDataFrameWithEmptyString <- function(data) {
  #https://github.com/r-lib/tidyselect/issues/201
  data %>%
    dplyr::collect() %>%
    dplyr::mutate(dplyr::across(tidyselect:::where(is.character), ~ tidyr::replace_na(.x, as.character('')))) %>%
    dplyr::mutate(dplyr::across(tidyselect:::where(is.logical), ~ tidyr::replace_na(.x, as.character('')))) %>%
    dplyr::mutate(dplyr::across(tidyselect:::where(is.numeric), ~ tidyr::replace_na(.x, as.numeric(''))))
}


#' Extract results from cohort diagnostics
#'
#' @description
#' Extract results from cohort diagnostics results data model for vector of cohort ids
#'
#' @template DataSource
#'
#' @template CohortIds
#'
#' @return
#' list of R objects that may then be converted to reports
#' @export
getResultsCompiledOutput <- function(dataSource,
                                     cohortIds) {
  output <- list()
  for (i in (1:length(cohortIds))) {
    output[[paste0("cohortId", cohortIds[[i]])]][["cohort"]] <-
      getResultsCohort(dataSource = dataSource) %>%
      dplyr::filter(.data$cohortId %in% cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["cohortCount"]] <-
      getResultsCohortCount(dataSource = dataSource,
                            cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["inclusionRuleStatistics"]] <-
      getResultsInclusionRuleStatistics(dataSource = dataSource,
                                        cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["cohortInclusion"]] <-
      getResultsCohortInclusion(dataSource = dataSource,
                                cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["cohortInclusionStats"]] <-
      getResultsCohortInclusionStats(dataSource = dataSource,
                                     cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["cohortSummaryStats"]] <-
      getResultsCohortSummaryStats(dataSource = dataSource,
                                   cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["fixedTimeSeries"]] <-
      getResultsFixedTimeSeries(dataSource = dataSource,
                                cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["timeDistribution"]] <-
      getResultsTimeDistribution(dataSource = dataSource,
                                 cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["incidenceRate"]] <-
      getResultsIncidenceRate(dataSource = dataSource,
                              cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["indexEventBreakdown"]] <-
      getResultsIndexEventBreakdown(dataSource = dataSource,
                                    cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["visitContext"]] <-
      getResultsVisitContext(dataSource = dataSource,
                             cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["cohortRelationships"]] <-
      getResultsCohortRelationships(dataSource = dataSource,
                                    cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["cohortOverlap"]] <-
      getResultsCohortOverlap(dataSource = dataSource,
                              targetCohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["circe"]] <-
      getCirceRenderedExpression(
        cohortDefinition = output[[paste0("cohortId", cohortIds[[i]])]][["cohort"]] %>%
          dplyr::pull(.data$json) %>%
          RJSONIO::fromJSON(digits = 23)
      )
    output[[paste0("cohortId", cohortIds[[i]])]][["resolvedConcepts"]] <-
      getResultsResolvedConcepts(dataSource = dataSource,
                                 cohortIds = cohortIds[[i]])
    output[[paste0("cohortId", cohortIds[[i]])]][["characterization"]] <-
      getMultipleCharacterizationResults(dataSource = dataSource,
                                         cohortIds = cohortIds[[i]])
  }
  return(output)
}
