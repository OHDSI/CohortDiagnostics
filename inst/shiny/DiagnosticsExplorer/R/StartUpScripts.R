# Copyright 2022 Observational Health Data Sciences and Informatics
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


loadResultsTable <- function(dataSource, tableName, required = FALSE) {
  resultsTablesOnServer <-
    tolower(DatabaseConnector::dbListTables(dataSource$connection, schema = dataSource$resultsDatabaseSchema))

  if (required || tableName %in% resultsTablesOnServer) {
    tryCatch(
    {
      table <- DatabaseConnector::dbReadTable(
        dataSource$connection,
        paste(dataSource$resultsDatabaseSchema, tableName, sep = ".")
      )
    },
      error = function(err) {
        stop(
          "Error reading from ",
          paste(dataSource$resultsDatabaseSchema, tableName, sep = "."),
          ": ",
          err$message
        )
      }
    )
    colnames(table) <-
      SqlRender::snakeCaseToCamelCase(colnames(table))
    if (nrow(table) > 0) {
      return(dplyr::as_tibble(table))
    }
  }

  return(NULL)
}


# Create empty objects in memory for all other tables. This is used by the Shiny app to decide what tabs to show:
tableIsEmpty <- function(dataSource, tableName) {
  sql <-
    sprintf(
      "SELECT 1 FROM %s.%s LIMIT 1;",
      dataSource$resultsDatabaseSchema,
      tableName
    )
  oneRow <- DatabaseConnector::dbGetQuery(dataSource$connection, sql)
  return(nrow(oneRow) == 0)
}

getTimeAsInteger <- function(time = Sys.time(),
                             tz = "UTC") {
  return(as.numeric(as.POSIXlt(time, tz = tz)))
}

getTimeFromInteger <- function(x, tz = "UTC") {
  originDate <- as.POSIXct("1970-01-01", tz = tz)
  originDate <- originDate + x
  return(originDate)
}

processMetadata <- function(data) {
  data <- data %>%
    tidyr::pivot_wider(
      id_cols = c(.data$startTime, .data$databaseId),
      names_from = .data$variableField,
      values_from = .data$valueField
    ) %>%
    dplyr::mutate(
      startTime = stringr::str_replace(
        string = .data$startTime,
        pattern = stringr::fixed("TM_"),
        replacement = ""
      )
    ) %>%
    dplyr::mutate(startTime = paste0(.data$startTime, " ", .data$timeZone)) %>%
    dplyr::mutate(startTime = as.POSIXct(.data$startTime)) %>%
    dplyr::group_by(
      .data$databaseId,
      .data$startTime
    ) %>%
    dplyr::arrange(.data$databaseId, dplyr::desc(.data$startTime), .by_group = TRUE) %>%
    dplyr::mutate(rn = dplyr::row_number()) %>%
    dplyr::filter(.data$rn == 1) %>%
    dplyr::select(-.data$timeZone)

  if ("runTime" %in% colnames(data)) {
    data$runTime <- round(x = as.numeric(data$runTime), digits = 2)
  }
  if ("observationPeriodMinDate" %in% colnames(data)) {
    data$observationPeriodMinDate <-
      as.Date(data$observationPeriodMinDate)
  }
  if ("observationPeriodMaxDate" %in% colnames(data)) {
    data$observationPeriodMaxDate <-
      as.Date(data$observationPeriodMaxDate)
  }
  if ("personsInDatasource" %in% colnames(data)) {
    data$personsInDatasource <- as.numeric(data$personsInDatasource)
  }
  if ("recordsInDatasource" %in% colnames(data)) {
    data$recordsInDatasource <- as.numeric(data$recordsInDatasource)
  }
  if ("personDaysInDatasource" %in% colnames(data)) {
    data$personDaysInDatasource <-
      as.numeric(data$personDaysInDatasource)
  }
  colnamesOfInterest <-
    c(
      "startTime",
      "databaseId",
      "runTime",
      "runTimeUnits",
      "sourceReleaseDate",
      "cdmVersion",
      "cdmReleaseDate",
      "observationPeriodMinDate",
      "observationPeriodMaxDate",
      "personsInDatasource",
      "recordsInDatasource",
      "personDaysInDatasource"
    )

  commonColNames <- intersect(colnames(data), colnamesOfInterest)

  data <- data %>%
    dplyr::select(dplyr::all_of(commonColNames))
  return(data)
}

checkErrorCohortIdsDatabaseIds <- function(errorMessage,
                                           cohortIds,
                                           databaseIds) {
  checkmate::assertDouble(
    x = cohortIds,
    null.ok = FALSE,
    lower = 1,
    upper = 2^53,
    any.missing = FALSE,
    add = errorMessage
  )
  checkmate::assertCharacter(
    x = databaseIds,
    min.len = 1,
    any.missing = FALSE,
    unique = TRUE,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  return(errorMessage)
}

quoteLiterals <- function(x) {
  if (is.null(x)) {
    return("")
  } else {
    return(paste0("'", paste(x, collapse = "', '"), "'"))
  }
}

getConnectionPool <- function(connectionDetails) {
  if (is(connectionDetails$server, "function")) {
    connectionPool <-
      pool::dbPool(
        drv = DatabaseConnector::DatabaseConnectorDriver(),
        dbms = connectionDetails$dbms,
        server = connectionDetails$server(),
        port = connectionDetails$port(),
        user = connectionDetails$user(),
        password = connectionDetails$password(),
        connectionString = connectionDetails$connectionString()
      )
  } else {
    # For backwards compatibility with older versions of DatabaseConnector:
    connectionPool <-
      pool::dbPool(
        drv = DatabaseConnector::DatabaseConnectorDriver(),
        dbms = connectionDetails$dbms,
        server = connectionDetails$server,
        port = connectionDetails$port,
        user = connectionDetails$user,
        password = connectionDetails$password,
        connectionString = connectionDetails$connectionString
      )
  }

  return(connectionPool)
}

createDatabaseDataSource <- function(connection,
                                     resultsDatabaseSchema,
                                     vocabularyDatabaseSchema = resultsDatabaseSchema,
                                     dbms) {
  return(
    list(
      connection = connection,
      resultsDatabaseSchema = resultsDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      dbms = dbms,
      resultsTablesOnServer = tolower(DatabaseConnector::dbListTables(connection, schema = resultsDatabaseSchema))
    )
  )
}

initializeTables <- function(dataSource, dataModelSpecifications, envir = .GlobalEnv) {

  envir$database <- loadResultsTable(dataSource, "database", required = TRUE)
  envir$cohort <- loadResultsTable(dataSource, "cohort", required = TRUE)
  envir$metadata <- loadResultsTable(dataSource, "metadata", required = TRUE)
  envir$temporalTimeRef <- loadResultsTable(dataSource, "temporal_time_ref")
  envir$temporalAnalysisRef <- loadResultsTable(dataSource, "temporal_analysis_ref")
  envir$conceptSets <- loadResultsTable(dataSource, "concept_sets")
  envir$cohortCount <- loadResultsTable(dataSource, "cohort_count", required = TRUE)
  envir$relationship <- loadResultsTable(dataSource, "relationship")

  if (!is.null(envir$cohort)) {
    envir$cohort <- cohort %>%
      dplyr::arrange(.data$cohortId) %>%
      dplyr::mutate(shortName = paste0("C", .data$cohortId)) %>%
      dplyr::mutate(compoundName = paste0(.data$shortName, ": ", .data$cohortName))
  }

  if (!is.null(envir$database)) {
    if (nrow(envir$database) > 0 &
      "vocabularyVersion" %in% colnames(envir$database)) {
      envir$database <- envir$database %>%
        dplyr::mutate(
          databaseIdWithVocabularyVersion = paste0(.data$databaseId, " (", .data$vocabularyVersion, ")")
        )
    }

    envir$databaseMetadata <- processMetadata(envir$metadata)
    envir$database <- database %>%
      dplyr::distinct() %>%
      dplyr::mutate(id = dplyr::row_number()) %>%
      dplyr::mutate(shortName = paste0("D", .data$id)) %>%
      dplyr::left_join(envir$databaseMetadata,
                       by = "databaseId"
      ) %>%
      dplyr::relocate(.data$id, .data$databaseId, .data$shortName)
  }

  envir$temporalChoices <- NULL
  envir$temporalCharacterizationTimeIdChoices <- NULL
  if (!is.null(envir$temporalTimeRef)) {
    envir$temporalChoices <- getResultsTemporalTimeRef(dataSource = dataSource)
    envir$temporalCharacterizationTimeIdChoices <- envir$temporalChoices %>%
        dplyr::arrange(.data$sequence)

    envir$characterizationTimeIdChoices <- envir$temporalChoices %>%
        dplyr::filter(.data$isTemporal == 0) %>%
        dplyr::filter(.data$primaryTimeId == 1) %>%
        dplyr::arrange(.data$sequence)
  }

  if (!is.null(envir$temporalAnalysisRef)) {
    envir$temporalAnalysisRef <- dplyr::bind_rows(
      envir$temporalAnalysisRef,
      dplyr::tibble(
        analysisId = c(-201, -301),
        analysisName = c("CohortEraStart", "CohortEraOverlap"),
        domainId = "Cohort",
        isBinary = "Y",
        missingMeansZero = "Y"
      )
    )

    envir$domainIdOptions <- envir$temporalAnalysisRef %>%
      dplyr::select(.data$domainId) %>%
      dplyr::pull(.data$domainId) %>%
      unique() %>%
      sort()
    envir$analysisNameOptions <- envir$temporalAnalysisRef %>%
      dplyr::select(.data$analysisName) %>%
      dplyr::pull(.data$analysisName) %>%
      unique() %>%
      sort()
  }


  resultsTables <- tolower(DatabaseConnector::dbListTables(connectionPool, schema = resultsDatabaseSchema))
  enabledTabs <- c()
  for (table in c(dataModelSpecifications$tableName)) {
    # , "recommender_set"
    if (table %in% resultsTables &
      !tableIsEmpty(dataSource, table)) {
      enabledTabs <- c(enabledTabs, SqlRender::snakeCaseToCamelCase(table))
    }
  }


  return(enabledTabs)
}