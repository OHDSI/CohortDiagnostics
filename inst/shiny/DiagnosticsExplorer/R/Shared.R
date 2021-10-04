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
doesObjectHaveData <- function(data) {
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
    # if (length(data) == 1) {
    #   if (data == "") {
    #     return(FALSE)
    #   }
    # }
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
                                             conceptId = NULL,
                                             coConceptId = NULL,
                                             conceptId1 = NULL,
                                             conceptSetId = NULL,
                                             databaseId = NULL,
                                             domainTable = NULL,
                                             vocabularyDatabaseSchema = NULL,
                                             daysRelativeIndex = NULL,
                                             startDay = NULL,
                                             endDay = NULL,
                                             seriesType = NULL,
                                             dataTableName) {
  if (is(dataSource, "environment")) {
    object <- c(
      "cohortId",
      "conceptId",
      "coConceptId",
      "databaseId",
      "conceptSetId",
      "startDay",
      "endDay",
      "daysRelativeIndex",
      "domainTable",
      "seriesType"
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
    if (doesObjectHaveData(conceptId1)) {
      #for concept relationship only
      browser()
      data <- data %>%
        dplyr::filter(.data$conceptId1 %in% conceptId |
                        .data$conceptId2 %in% conceptId)
    }
  } else {
    if (is.null(dataSource$connection)) {
      stop("No connection provided. Unable to query database.")
    }
    
    if (!DatabaseConnector::dbIsValid(dataSource$connection)) {
      stop("Connection to database seems to be closed.")
    }
    
    sql <- "SELECT * \n
            FROM  @results_database_schema.@data_table \n
            WHERE 1 = 1 \n
              {@database_id !=''} ? {AND database_id in (@database_id) \n}
              {@cohort_id !=''} ? {AND cohort_id in (@cohort_id) \n}
              {@concept_id !=''} ? {AND concept_id in (@concept_id) \n}
              {@co_concept_id !=''} ? {AND co_concept_id in (@co_concept_id) \n}
              {@concept_set_id !=''} ? {AND concept_set_id in (@concept_set_id) \n}
              {@concept_id_1 !=''} ? {AND (concept_id_1 IN (@concept_ids) OR concept_id_2 IN (@concept_ids)) \n}
              {@start_day !=''} ? {AND start_day IN (@start_day) \n}
              {@end_day !=''} ? {AND end_day IN (@end_day) \n}
              {@days_relative_index !=''} ? {AND end_day IN (@days_relative_index) \n}
              {@series_type !=''} ? {AND series_type IN (@series_type) \n}
              {@domain_table !=''} ? {AND domain_table IN (@domain_table) \n}
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
        data_table = camelCaseToSnakeCase(dataTableName),
        database_id = quoteLiterals(databaseId),
        concept_id = conceptId,
        co_concept_id = coConceptId,
        concept_set_id = conceptSetId,
        concept_id_1 = conceptId1,
        # for concept relationship only
        start_day = startDay,
        end_day = endDay,
        domain_table = domainTable,
        days_relative_index = daysRelativeIndex,
        series_type = seriesType,
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
                                   conceptIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource = dataSource,
    dataTableName = "conceptRelationship",
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    conceptId1 = conceptIds
  )
  return(data)
}



#' Returns data from concept ancestor table for vector of concept ids
#'
#' @description
#' Returns data from concept ancestor table for vector of concept ids
#' 
#' @param conceptIds a vector of concept ids 
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
      dplyr::filter(.data$conceptId %in% conceptId)
    
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
#' @param conceptIds     A list of concept ids to get counts for
#'
#' @return
#' Returns a data frame (tibble)
#'
#' @export
getResultsConceptCount <- function(dataSource,
                                   databaseIds = NULL,
                                   conceptIds = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    databaseId = databaseIds,
    conceptId = conceptIds,
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
#' @param conceptIds     A vector of concept ids to get counts for
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
    intersectOfTwo <- intersect(x = tolower(domainTables),
                                y = domainTable$domainTable) %>% 
      unique()
    setdiffOfTwo <- setdiff(x = tolower(domainTables),
                            y = domainTable$domainTable) %>% 
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
    intersectOfTwo <- domainTable %>% 
      dplyr::filter(.data$domainTable %in% c(intersectOfTwo)) %>% 
      dplyr::pull(.data$domainTableShort) %>% 
      unique()
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
#' @param conceptIds     A list of concept ids to get counts for
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
    data$conceptRelationship <-
      getConceptRelationship(
        dataSource = dataSource,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        conceptIds = conceptIds
      )
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
      dplyr::arrange(.data$conceptId) %>% 
      dplyr::group_by(.data$referenceConceptId, .data$conceptId)
    browser()
    
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
      conceptIds = conceptIds
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
          dplyr::rename("domainTableShort" = .data$domainTable) %>%
          dplyr::rename("domainFieldShort" = .data$domainField) %>%
          dplyr::filter(
            .data$domainTableShort %in% c(data$cdmTables$domainTableShort %>% unique(), "All")
          ) %>%
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
            .data$domainFieldShort,
            .data$domainTableShort,
            .data$periodBegin,
            .data$conceptCount,
            .data$subjectCount
          ) %>%
          dplyr::mutate(periodBegin = tsibble::yearmonth(.data$periodBegin)) %>%
          tsibble::as_tsibble(
            key = c(
              .data$conceptId,
              .data$databaseId,
              .data$domainFieldShort,
              .data$domainTableShort
            ),
            index = .data$periodBegin
          ) %>%
          dplyr::arrange(
            .data$conceptId,
            .data$databaseId,
            .data$domainFieldShort,
            .data$domainTableShort,
            .data$periodBegin,
            .data$conceptCount,
            .data$subjectCount
          )
        
        data$databaseConceptIdYearLevelTsibble <-
          data$databaseConceptCountDetails %>%
          dplyr::rename("domainTableShort" = .data$domainTable) %>%
          dplyr::rename("domainFieldShort" = .data$domainField) %>%
          dplyr::filter(
            .data$domainTableShort %in% c(data$cdmTables$domainTableShort %>% unique(), "All")
          ) %>%
          dplyr::filter(.data$eventYear > 0, .data$eventMonth == 0) %>%
          dplyr::mutate(periodBegin = lubridate::as_date(paste0(.data$eventYear,
                                                                "-",
                                                                "01-01"))) %>%
          dplyr::select(
            .data$conceptId,
            .data$databaseId,
            .data$domainFieldShort,
            .data$domainTableShort,
            .data$periodBegin,
            .data$conceptCount,
            .data$subjectCount
          ) %>%
          dplyr::mutate(periodBegin = clock::get_year(.data$periodBegin)) %>%
          tsibble::as_tsibble(
            key = c(
              .data$conceptId,
              .data$databaseId,
              .data$domainFieldShort,
              .data$domainTableShort
            ),
            index = .data$periodBegin
          ) %>%
          dplyr::arrange(
            .data$conceptId,
            .data$databaseId,
            .data$domainFieldShort,
            .data$domainTableShort,
            .data$periodBegin,
            .data$conceptCount,
            .data$subjectCount
          )
      }
    }
  }
  
  if (!is.null(cohortIds)) {
    if (getConceptCooccurrence) {
      data$conceptCooccurrence <-
        getResultsConceptCooccurrence(
          dataSource = dataSource,
          databaseIds = databaseIds,
          cohortIds = cohortIds,
          conceptIds = data$concept$conceptId %>% unique()
        ) %>%
        dplyr::select(
          .data$conceptId,
          .data$databaseId,
          .data$cohortId,
          .data$coConceptId,
          .data$subjectCount
        ) %>%
        dplyr::rename("referenceConceptId" = .data$conceptId) %>%
        dplyr::rename("conceptId" = .data$coConceptId) %>%
        dplyr::arrange(
          .data$referenceConceptId,
          .data$databaseId,
          .data$cohortId,
          dplyr::desc(.data$subjectCount)
        )
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
    dataTableName = "conceptSet",
    cohortId = cohortId,
    conceptSetId = conceptSetId
  )
  if (length(cohortId) > 0) {
    stop("Please only provide one integer value for cohortId")
  }
  if (length(cohortId) > 0) {
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
    stop("More than one expression returned. Please check the integerity of your results.")
  }
  
  expression <- data %>%
    dplyr::pull(.data$concept_set_expression) %>%
    RJSONIO::fromJSON(digits = 23)
  
  return(expression)
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
    longName = c("Records Found Per Period",
                 "Subjects Found Per Period",
                 "Person Days Per Period", 
                 "Incidence Subjects Per Period",
                 "Incidence Person days Per Period",
                 "Incident Subjects Ending Per Period",
                 "Records Starting Per Period", 
                 "Subjects Starting Per Period",
                 "Records Ending Per Period", 
                 "Subjects Ending Per Period"),
    sequence = c(1,2,3,4,5,6,7,8,9,10)
  ) %>% 
    dplyr::arrange(.data$sequence)
  # cohortId = 0, represent all persons in observation_period
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = c(cohortIds, 0) %>% unique(),
    databaseId = databaseIds,
    seriesType = c('T1', 'T2', 'T3'),
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
      'Subjects in data source limited to cohort period',
      'Subjects in data source not limited to cohort period',
      'Persons in data source',
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
    coConceptId = coConceptIds
  )
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
#' @template cohortIds
#'
#' @template DatabaseIds
#' 
#' @param startDay A vector of days in relation to cohort_start_date of target 
#' 
#' @param endDay A vector of days in relation to cohort_end_date of target 
#'
#' @return
#' Returns a data frame (tibble) with results that conform to cohort_relationships
#' table in Cohort Diagnostics results data model.
#'
#' @export
getResultsCohortRelationships <- function(dataSource,
                                          cohortIds = NULL,
                                          databaseIds = NULL,
                                          startDay = NULL,
                                          endDay = NULL) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource = dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    startDay = startDay,
    endDay = endDay,
    dataTableName = "cohortRelationships"
  )
  return(data)
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
  
  combisOfTargetComparator <- tidyr::crossing(dplyr::tibble(targetCohortId = targetCohortIds),
                                              dplyr::tibble(comparatorCohortId = comparatorCohortIds)) %>%
    dplyr::filter(.data$targetCohortId != .data$comparatorCohortId)
  
  cohortRelationship <-
    getResultsCohortRelationships(dataSource = dataSource,
                                  cohortIds = cohortIds,
                                  databaseIds = databaseIds)
  
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
#' @return
#' Returns a list object with covariateValue, covariateValueDist,
#' covariateRef, analysisRef output of feature extraction along with
#' concept information.
#'
#' @export
getFeatureExtractionCharacterization <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL) {
    analysisRef <- getResultsAnalysisRef(dataSource = dataSource)
    covariateRef <- getResultsCovariateRef(dataSource = dataSource)
    concept <- getConcept(dataSource = dataSource,
                          conceptIds = covariateRef$conceptId %>% unique())
    covariateValue <-
      getResultsCovariateValue(dataSource = dataSource,
                               cohortIds = cohortIds,
                               databaseIds = databaseIds)
    covariateValueDist <-
      getResultsCovariateValueDist(dataSource = dataSource,
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
getFeatureExtractionTemporalCharacterization <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL) {
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
    # meta information
    cohortCounts <-
      getResultsCohortCount(dataSource = dataSource,
                            cohortIds = cohortIds,
                            databaseIds = databaseIds)
    cohort <- getResultsCohort(dataSource = dataSource)
    
    cohortRelationships <-
      getResultsCohortRelationships(dataSource = dataSource,
                                    cohortIds = cohortIds,
                                    databaseIds = databaseIds,
                                    startDay = c(-99999,-365,-180,-30,-99999,-365,-180,-30),
                                    endDay = 0)
    # comparator cohort was on or after target cohort
    summarizeCohortRelationship <- function(data,
                                            startDay = NULL,
                                            endDay = NULL,
                                            valueField = 'records',
                                            analysisId,
                                            cohortCounts) {
      if (is.null(data) || nrow(data) == 0) {
        return(NULL)
      }
      
      data$sumValue <- data[[valueField]]
      data <- data  %>%
        dplyr::filter(.data$startDay == !!startDay) %>%
        dplyr::filter(.data$endDay == !!endDay) %>%
        dplyr::select(.data$databaseId,
                      .data$cohortId,
                      .data$comparatorCohortId,
                      .data$sumValue)
      
      if (stringr::str_detect(string = valueField, pattern = 'record')) {
        denominator <- cohortCounts  %>%
          dplyr::rename(denominator = .data$cohortEntries) %>%
          dplyr::select(.data$cohortId,
                        .data$databaseId,
                        .data$denominator)
      } else {
        denominator <- cohortCounts  %>%
          dplyr::rename(denominator = .data$cohortSubjects) %>%
          dplyr::select(.data$cohortId,
                        .data$databaseId,
                        .data$denominator)
      }
      
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
          .data$sumValue,
          .data$mean,
          .data$sd,
          .data$databaseId
        )
      return(data)
    }
    
    analysisId <- c(-101,-102,-103,-104,-201,-202,-203,-204)
    analysisName <- c(
      "CohortOccurrenceAnyTimePrior",  #subjects in comparator that start anytime prior to target start
      "CohortOccurrenceLongTerm",      #subjects in comparator that start in long term window in relation to target start
      "CohortOccurrenceMediumTerm",    #subjects in comparator that start in medium term window in relation to target start
      "CohortOccurrenceShortTerm",     #subjects in comparator that start in short term window in relation to target start
      "CohortEraAnyTimePrior",         #subjects in comparator that exist anytime prior to target start
      "CohortEraLongTerm",             #subjects in comparator that exist in long term window in relation to target start
      "CohortEraMediumTerm",           #subjects in comparator that exist in medium term window in relation to target start
      "CohortEraShortTerm"             #subjects in comparator that exist in short term window in relation to target start
    )
    valueField <- c(
      "subCsBeforeTs",
      "subCsBeforeTs",
      "subCsBeforeTs",
      "subCsBeforeTs", #comparator cohort subjects start within window in relation to target cohort start
      "subjects",
      "subjects",
      "subjects",
      "subjects"          #comparator cohort subjects exist within the window in relation to target cohort start/end date
    )
    startDay <- c(-99999,-365,-180,-30,-99999,-365,-180,-30)
    endDay <- c(0)
    analysisRef <-
      dplyr::tibble(analysisId, analysisName, valueField, startDay, endDay) %>%
      dplyr::mutate(isBinary = 'Y',
                    missingMeansZero = 'Y') %>%
      dplyr::arrange(.data$analysisId) %>%
      dplyr::mutate(domainId = 'Cohort') %>%
      dplyr::select(
        .data$analysisId,
        .data$analysisName,
        .data$valueField,
        .data$domainId,
        .data$startDay,
        .data$endDay,
        .data$isBinary,
        .data$missingMeansZero
      )
    
    result <- list()
    for (j in (1:nrow(analysisRef))) {
      result[[j]] <-
        summarizeCohortRelationship(
          data = cohortRelationships,
          startDay = analysisRef[j,]$startDay,
          endDay = analysisRef[j,]$endDay,
          analysisId = analysisRef[j,]$analysisId,
          valueField = analysisRef[j,]$valueField,
          cohortCounts = cohortCounts
        )
    }
    result <- dplyr::bind_rows(result)
    
    analysisRef <- analysisRef %>%
      dplyr::select(-.data$valueField)
    
    if (nrow(result) == 0) {
      result <- NULL
    }
    
    covariateRef <- tidyr::crossing(cohort,
                                    analysisRef %>%
                                      dplyr::select(.data$analysisId)) %>%
      dplyr::mutate(covariateId = (.data$cohortId * -1000) + .data$analysisId) %>%
      dplyr::mutate(covariateName = paste0(.data$cohortName)) %>% #, "(", .data$covariateId, ")")) %>%
      dplyr::mutate(conceptId = .data$cohortId * -1) %>%
      dplyr::arrange(.data$covariateId) %>%
      dplyr::select(.data$covariateId,
                    .data$covariateName,
                    .data$analysisId,
                    .data$conceptId)
    
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
    
    data <- list(
      covariateRef = covariateRef,
      covariateValue = result,
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
                                    startDay = c(seqStart30),
                                    endDay = c(seqEnd30),
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
#' @return
#' Returns multiple characterization output
#'
#' @export
getMultipleCharacterizationResults <-
  function(dataSource = .GlobalEnv,
           cohortIds = NULL,
           databaseIds = NULL) {
    addCharacterizationSource <-
      function(x, characterizationSourceValue) {
        exepectedDataTables <-
          c(
            'analysisRef',
            'covariateRef',
            'covariateValue',
            'covariateValueDist',
            'concept',
            'temporalAnalysisRef',
            'temporalCovariateRef',
            'temporalCovariateValue',
            'temporalCovariateValueDist'
          )
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
    
    featureExtractioncharacterization <-
      getFeatureExtractionCharacterization(dataSource = dataSource,
                                           cohortIds = cohortIds,
                                           databaseIds = databaseIds)
    featureExtractioncharacterization <-
      addCharacterizationSource(x = featureExtractioncharacterization,
                                characterizationSourceValue = 'F')
    
    if (!is.null(featureExtractioncharacterization$covariateValue)) {
      featureExtractioncharacterization$covariateValue <-
        featureExtractioncharacterization$covariateValue %>%
        dplyr::mutate(timeId = 0)
    }
    
    featureExtractionTemporalcharacterization <-
      getFeatureExtractionTemporalCharacterization(dataSource = dataSource,
                                                   cohortIds = cohortIds,
                                                   databaseIds = databaseIds)
    featureExtractionTemporalcharacterization <-
      addCharacterizationSource(x = featureExtractionTemporalcharacterization,
                                characterizationSourceValue = 'FT')
    
    cohortRelationshipCharacterizationResults <-
      getCohortRelationshipCharacterizationResults(dataSource = dataSource,
                                                   cohortIds = cohortIds,
                                                   databaseIds = databaseIds)
    cohortRelationshipCharacterizationResults <-
      addCharacterizationSource(x = cohortRelationshipCharacterizationResults,
                                characterizationSourceValue = 'C')
    if (!is.null(cohortRelationshipCharacterizationResults$covariateValue)) {
      cohortRelationshipCharacterizationResults$covariateValue <-
        cohortRelationshipCharacterizationResults$covariateValue %>%
        dplyr::mutate(timeId = 0)
    }
    
    cohortAsFeatureTemporalCharacterizationResults <-
      getCohortAsFeatureTemporalCharacterizationResults(
        dataSource = dataSource,
        cohortIds = cohortIds,
        databaseIds = databaseIds,
        temporalTimeRef = featureExtractionTemporalcharacterization$temporalTimeRef
      )
    cohortAsFeatureTemporalCharacterizationResults <-
      addCharacterizationSource(x = cohortAsFeatureTemporalCharacterizationResults,
                                characterizationSourceValue = 'CT')
    
    analysisRef <-
      dplyr::bind_rows(
        featureExtractioncharacterization$analysisRef,
        featureExtractionTemporalcharacterization$temporalAnalysisRef,
        cohortRelationshipCharacterizationResults$analysisRef,
        cohortAsFeatureTemporalCharacterizationResults$temporalAnalysisRef
      ) %>% dplyr::distinct()
    if (all(!is.null(analysisRef), nrow(analysisRef) == 0)) {
      analysisRef <- NULL
    }
    if (!is.null(analysisRef)) {
      analysisRef <- analysisRef  %>%
        dplyr::arrange(.data$analysisId, .data$characterizationSource)
    }
    if (all(!is.null(analysisRef), nrow(analysisRef) == 0)) {
      analysisRef <- NULL
    }
    
    covariateRef <-
      dplyr::bind_rows(
        featureExtractioncharacterization$covariateRef,
        featureExtractionTemporalcharacterization$temporalCovariateRef,
        cohortRelationshipCharacterizationResults$covariateRef,
        cohortAsFeatureTemporalCharacterizationResults$temporalCovariateRef
      )
    if (all(!is.null(covariateRef), nrow(covariateRef) == 0)) {
      covariateRef <- NULL
    }
    if (!is.null(covariateRef)) {
      covariateRef <- covariateRef %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$covariateId, .data$characterizationSource)
    }
    if (all(!is.null(covariateRef), nrow(covariateRef) == 0)) {
      covariateRef <- NULL
    }
    
    covariateValue <-
      dplyr::bind_rows(
        featureExtractioncharacterization$covariateValue,
        featureExtractionTemporalcharacterization$temporalCovariateValue,
        cohortRelationshipCharacterizationResults$covariateValue,
        cohortAsFeatureTemporalCharacterizationResults$temporalCovariateValue
      )
    if (all(!is.null(covariateValue), nrow(covariateValue) == 0)) {
      covariateValue <- NULL
    }
    if (!is.null(covariateValue)) {
      covariateValue <- covariateValue %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$cohortId,
                       .data$covariateId,
                       .data$characterizationSource)
    }
    if (all(!is.null(covariateValue), nrow(covariateValue) == 0)) {
      covariateValue <- NULL
    }
    
    covariateValueDist <-
      dplyr::bind_rows(
        featureExtractioncharacterization$covariateValueDist,
        featureExtractionTemporalcharacterization$temporalCovariateValueDist,
        cohortRelationshipCharacterizationResults$covariateValueDist,
        cohortAsFeatureTemporalCharacterizationResults$temporalCovariateValueDist
      )
    if (all(!is.null(covariateValueDist), nrow(covariateValueDist) == 0)) {
      covariateValueDist <- NULL
    }
    if (!is.null(covariateValueDist)) {
      covariateValueDist <- covariateValueDist %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$cohortId,
                       .data$covariateId,
                       .data$characterizationSource)
    }
    if (all(!is.null(covariateValueDist), nrow(covariateValueDist) == 0)) {
      covariateValueDist <- NULL
    }
    
    concept <-
      dplyr::bind_rows(
        featureExtractioncharacterization$concept,
        featureExtractionTemporalcharacterization$concept,
        cohortRelationshipCharacterizationResults$concept,
        cohortAsFeatureTemporalCharacterizationResults$concept
      )
    if (all(!is.null(concept), nrow(concept) == 0)) {
      concept <- NULL
    }
    if (!is.null(concept)) {
      concept <- concept %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$conceptId)
    }
    if (all(!is.null(concept), nrow(concept) == 0)) {
      concept <- NULL
    }
    
    temporalTimeRef <-
      dplyr::bind_rows(
        featureExtractionTemporalcharacterization$temporalTimeRef,
        cohortAsFeatureTemporalCharacterizationResults$temporalTimeRef
      ) %>%
      dplyr::distinct()
    if (all(!is.null(temporalTimeRef), nrow(temporalTimeRef) == 0)) {
      temporalTimeRef <- NULL
    }
    
    return(
      list(
        analysisRef = analysisRef,
        covariateRef = covariateRef,
        covariateValue = covariateValue,
        covariateValueDist = covariateValueDist,
        concept = concept,
        temporalTimeRef = temporalTimeRef
      )
    )
  }

# not exported
getResultsCovariateValue <- function(dataSource,
                                     cohortIds,
                                     databaseIds) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "covariateValue"
  )
  return(data)
}

# not exported
getResultsCovariateValueDist <- function(dataSource,
                                         cohortIds,
                                         databaseIds) {
  data <- getDataFromResultsDatabaseSchema(
    dataSource,
    cohortId = cohortIds,
    databaseId = databaseIds,
    dataTableName = "covariateValueDist"
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
#' @param cohortDefinition An object with a list representation of the cohort definition expression.
#'
#' @return list object
#'
#' @export
getCirceRenderedExpression <- function(cohortDefinition) {
  cohortJson <- RJSONIO::toJSON(x = cohortDefinition, digits = 23, pretty = TRUE)
  circeExpression <-
    CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
  circeExpressionMarkdown <-
    CirceR::cohortPrintFriendly(circeExpression)
  circeConceptSetListmarkdown <-
    CirceR::conceptSetListPrintFriendly(circeExpression$conceptSets)
  htmlExpressionCohort <-
    convertMdToHtml(circeExpressionMarkdown)
  htmlExpressionConceptSetExpression <-
    convertMdToHtml(circeConceptSetListmarkdown)
  return(
    dplyr::tibble(
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
  # tried utils::globalVariables("where") but get the message The namespace for package "CohortDiagnostics" is locked; no changes in the global variables list may be made.
  data %>%
    dplyr::collect() %>%
    dplyr::mutate(dplyr::across(where(is.character), ~ tidyr::replace_na(.x, as.character('')))) %>%
    dplyr::mutate(dplyr::across(where(is.logical), ~ tidyr::replace_na(.x, as.character('')))) %>%
    dplyr::mutate(dplyr::across(where(is.numeric), ~ tidyr::replace_na(.x, as.numeric(''))))
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
                              cohortIds = cohortIds[[i]])
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
