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
#

#' Get specifications for Cohort Diagnostics results data model
#'
#' @return
#' A tibble data frame object with specifications
#'
#' @export
getResultsDataModelSpecifications <- function() {
  pathToCsv <-
    system.file("settings", "resultsDataModelSpecification.csv", package= utils::packageName())
  resultsDataModelSpecifications <-
    readr::read_csv(file = pathToCsv, col_types = readr::cols())
  return(resultsDataModelSpecifications)
}

fixTableMetadataForBackwardCompatibility <- function(table, tableName) {
  if (tableName %in% c("cohort", "phenotype_description")) {
    if (!'metadata' %in% colnames(table)) {
      data <- list()
      for (i in (1:nrow(table))) {
        data[[i]] <- table[i,]
        colnamesDf <- colnames(data[[i]])
        metaDataList <- list()
        for (j in (1:length(colnamesDf))) {
          metaDataList[[colnamesDf[[j]]]] = data[[i]][colnamesDf[[j]]] %>% dplyr::pull()
        }
        data[[i]]$metadata <-
          RJSONIO::toJSON(metaDataList, pretty = TRUE, digits = 23)
      }
      table <- dplyr::bind_rows(data)
    }
    if ('referent_concept_id' %in% colnames(table)) {
      table <- table %>%
        dplyr::select(-.data$referent_concept_id)
    }
  }
  if (tableName %in% c('covariate_value', 'temporal_covariate_value')) {
    if (!'sum_value' %in% colnames(table)) {
      table$sum_value <- -1
    }
  }
  return(table)
}

checkFixColumnNames <-
  function(table,
           tableName,
           zipFileName,
           specifications = getResultsDataModelSpecifications()) {
    if (tableName %in% c('cohort', 'phenotype_description', 
                         'covariate_value', 'temporal_covariate_value')) {
      table <- fixTableMetadataForBackwardCompatibility(table = table,
                                                        tableName = tableName)
    }
    observeredNames <- colnames(table)[order(colnames(table))]
    
    tableSpecs <- specifications %>%
      dplyr::filter(.data$tableName == !!tableName)
    
    optionalNames <- tableSpecs %>%
      dplyr::filter(.data$optional == "Yes") %>%
      dplyr::select(.data$fieldName)
    
    expectedNames <- tableSpecs %>%
      dplyr::select(.data$fieldName) %>%
      dplyr::anti_join(dplyr::filter(optionalNames, !.data$fieldName %in% observeredNames),
                       by = "fieldName") %>%
      dplyr::arrange(.data$fieldName) %>%
      dplyr::pull()
    
    if (!checkmate::testNames(observeredNames, must.include = expectedNames)) {
      stop(
        sprintf(
          "Column names of table %s in zip file %s do not match specifications.\n- Observed columns: %s\n- Expected columns: %s",
          tableName,
          zipFileName,
          paste(observeredNames, collapse = ", "),
          paste(expectedNames, collapse = ", ")
        )
      )
    }
    return(table)
  }

checkAndFixDataTypes <-
  function(table,
           tableName,
           zipFileName,
           specifications = getResultsDataModelSpecifications()) {
    tableSpecs <- specifications %>%
      filter(.data$tableName == !!tableName)
    
    observedTypes <- sapply(table, class)
    for (i in 1:length(observedTypes)) {
      fieldName <- names(observedTypes)[i]
      expectedType <-
        gsub("\\(.*\\)", "", tolower(tableSpecs$type[tableSpecs$fieldName == fieldName]))
      if (expectedType == "bigint" || expectedType == "float") {
        if (observedTypes[i] != "numeric" && observedTypes[i] != "double") {
          ParallelLogger::logDebug(
            sprintf(
              "Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              fieldName,
              tableName,
              zipFileName,
              observedTypes[i],
              expectedType
            )
          )
          table <- mutate_at(table, i, as.numeric)
        }
      } else if (expectedType == "int") {
        if (observedTypes[i] != "integer") {
          ParallelLogger::logDebug(
            sprintf(
              "Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              fieldName,
              tableName,
              zipFileName,
              observedTypes[i],
              expectedType
            )
          )
          table <- mutate_at(table, i, as.integer)
        }
      } else if (expectedType == "varchar") {
        if (observedTypes[i] != "character") {
          ParallelLogger::logDebug(
            sprintf(
              "Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              fieldName,
              tableName,
              zipFileName,
              observedTypes[i],
              expectedType
            )
          )
          table <- mutate_at(table, i, as.character)
        }
      } else if (expectedType == "date") {
        if (observedTypes[i] != "Date") {
          ParallelLogger::logDebug(
            sprintf(
              "Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              fieldName,
              tableName,
              zipFileName,
              observedTypes[i],
              expectedType
            )
          )
          table <- mutate_at(table, i, as.Date)
        }
      }
    }
    return(table)
  }

checkAndFixDuplicateRows <-
  function(table,
           tableName,
           zipFileName,
           specifications = getResultsDataModelSpecifications()) {
    primaryKeys <- specifications %>%
      dplyr::filter(.data$tableName == !!tableName &
                      .data$primaryKey == "Yes") %>%
      dplyr::select(.data$fieldName) %>%
      dplyr::pull()
    duplicatedRows <- duplicated(table[, primaryKeys])
    if (any(duplicatedRows)) {
      ParallelLogger::logInfo(
        sprintf(
          "Table %s in zip file %s has duplicate rows. Removing %s records.",
          tableName,
          zipFileName,
          sum(duplicatedRows)
        )
      )
      return(table[!duplicatedRows,])
    } else {
      return(table)
    }
  }

appendNewRows <-
  function(data,
           newData,
           tableName,
           specifications = getResultsDataModelSpecifications()) {
    if (nrow(data) > 0) {
      primaryKeys <- specifications %>%
        dplyr::filter(.data$tableName == !!tableName &
                        .data$primaryKey == "Yes") %>%
        dplyr::select(.data$fieldName) %>%
        dplyr::pull()
      newData <- newData %>%
        dplyr::anti_join(data, by = primaryKeys)
    }
    return(dplyr::bind_rows(data, newData))
  }


#' Create the results data model tables on a database server.
#'
#' @details
#' Only PostgreSQL servers are supported.
#'
#' @template Connection
#' @param schema         The schema on the postgres server where the tables will be created.
#'
#' @export
createResultsDataModel <- function(connection = NULL,
                                   connectionDetails = NULL,
                                   schema) {
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }
  schemas <- unlist(
    DatabaseConnector::querySql(
      connection,
      "SELECT schema_name FROM information_schema.schemata;",
      snakeCaseToCamelCase = TRUE
    )[, 1]
  )
  if (!tolower(schema) %in% tolower(schemas)) {
    stop(
      "Schema '",
      schema,
      "' not found on database. Only found these schemas: '",
      paste(schemas, collapse = "', '"),
      "'"
    )
  }
  DatabaseConnector::executeSql(
    connection,
    sprintf("SET search_path TO %s;", schema),
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  pathToSql <-
    system.file("sql", "postgresql", "CreateResultsDataModel.sql", package= utils::packageName())
  sql <- SqlRender::readSql(pathToSql)
  DatabaseConnector::executeSql(connection, sql)
}

naToEmpty <- function(x) {
  x[is.na(x)] <- ""
  return(x)
}

naToZero <- function(x) {
  x[is.na(x)] <- 0
  return(x)
}

#' Upload results to the database server.
#'
#' @description
#' Requires the results data model tables have been created using the \code{\link{createResultsDataModel}} function.
#'
#' Set the POSTGRES_PATH environmental variable to the path to the folder containing the psql executable to enable
#' bulk upload (recommended).
#'
#' @param connectionDetails   An object of type \code{connectionDetails} as created using the
#'                            \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                            DatabaseConnector package.
#' @param schema         The schema on the postgres server where the tables have been created.
#' @param zipFileName    The name of the zip file.
#' @param forceOverWriteOfSpecifications  If TRUE, specifications of the phenotypes, cohort definitions, and analysis
#'                       will be overwritten if they already exist on the database. Only use this if these specifications
#'                       have changed since the last upload.
#' @param purgeSiteDataBeforeUploading If TRUE, before inserting data for a specific databaseId all the data for
#'                       that site will be dropped. This assumes the input zip file contains the full data for that
#'                       data site.
#' @param tempFolder     A folder on the local file system where the zip files are extracted to. Will be cleaned
#'                       up when the function is finished. Can be used to specify a temp folder on a drive that
#'                       has sufficient space if the default system temp space is too limited.
#'
#' @export
uploadResults <- function(connectionDetails = NULL,
                          schema,
                          zipFileName,
                          forceOverWriteOfSpecifications = FALSE,
                          purgeSiteDataBeforeUploading = TRUE,
                          tempFolder = tempdir()) {
  start <- Sys.time()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  unzipFolder <- tempfile("unzipTempFolder", tmpdir = tempFolder)
  dir.create(path = unzipFolder, recursive = TRUE)
  on.exit(unlink(unzipFolder, recursive = TRUE), add = TRUE)
  
  ParallelLogger::logInfo("Unzipping ", zipFileName)
  zip::unzip(zipFileName, exdir = unzipFolder)
  
  specifications <- getResultsDataModelSpecifications()
  
  if (purgeSiteDataBeforeUploading) {
    database <-
      readr::read_csv(file = file.path(unzipFolder, "database.csv"),
                      col_types = readr::cols())
    colnames(database) <-
      SqlRender::snakeCaseToCamelCase(colnames(database))
    databaseId <- database$databaseId
  }
  
  uploadTable <- function(tableName) {
    ParallelLogger::logInfo("Uploading table ", tableName)
    
    primaryKey <- specifications %>%
      filter(.data$tableName == !!tableName &
               .data$primaryKey == "Yes") %>%
      select(.data$fieldName) %>%
      pull()
    
    if (purgeSiteDataBeforeUploading &&
        "database_id" %in% primaryKey) {
      deleteAllRecordsForDatabaseId(
        connection = connection,
        schema = schema,
        tableName = tableName,
        databaseId = databaseId
      )
    }
    
    csvFileName <- paste0(tableName, ".csv")
    if (csvFileName %in% list.files(unzipFolder)) {
      env <- new.env()
      env$schema <- schema
      env$tableName <- tableName
      env$primaryKey <- primaryKey
      if (purgeSiteDataBeforeUploading &&
          "database_id" %in% primaryKey) {
        env$primaryKeyValuesInDb <- NULL
      } else {
        sql <- "SELECT DISTINCT @primary_key FROM @schema.@table_name;"
        sql <- SqlRender::render(
          sql = sql,
          primary_key = primaryKey,
          schema = schema,
          table_name = tableName
        )
        primaryKeyValuesInDb <-
          DatabaseConnector::querySql(connection, sql)
        colnames(primaryKeyValuesInDb) <-
          tolower(colnames(primaryKeyValuesInDb))
        env$primaryKeyValuesInDb <- primaryKeyValuesInDb
      }
      
      uploadChunk <- function(chunk, pos) {
        ParallelLogger::logInfo("- Preparing to upload rows ",
                                pos,
                                " through ",
                                pos + nrow(chunk) - 1)
        
        chunk <- checkFixColumnNames(
          table = chunk,
          tableName = env$tableName,
          zipFileName = zipFileName,
          specifications = specifications
        )
        chunk <- checkAndFixDataTypes(
          table = chunk,
          tableName = env$tableName,
          zipFileName = zipFileName,
          specifications = specifications
        )
        chunk <- checkAndFixDuplicateRows(
          table = chunk,
          tableName = env$tableName,
          zipFileName = zipFileName,
          specifications = specifications
        )
        
        # Primary key fields cannot be NULL, so for some tables convert NAs to empty or zero:
        toEmpty <- specifications %>%
          filter(
            .data$tableName == env$tableName &
              .data$emptyIsNa == "No" & grepl("varchar", .data$type)
          ) %>%
          select(.data$fieldName) %>%
          pull()
        if (length(toEmpty) > 0) {
          chunk <- chunk %>%
            dplyr::mutate_at(toEmpty, naToEmpty)
        }
        
        tozero <- specifications %>%
          filter(
            .data$tableName == env$tableName &
              .data$emptyIsNa == "No" &
              .data$type %in% c("int", "bigint", "float")
          ) %>%
          select(.data$fieldName) %>%
          pull()
        if (length(tozero) > 0) {
          chunk <- chunk %>%
            dplyr::mutate_at(tozero, naToZero)
        }
        
        # Check if inserting data would violate primary key constraints:
        if (!is.null(env$primaryKeyValuesInDb)) {
          primaryKeyValuesInChunk <- unique(chunk[env$primaryKey])
          duplicates <- inner_join(env$primaryKeyValuesInDb,
                                   primaryKeyValuesInChunk,
                                   by = env$primaryKey)
          if (nrow(duplicates) != 0) {
            if ("database_id" %in% env$primaryKey ||
                forceOverWriteOfSpecifications) {
              ParallelLogger::logInfo(
                "- Found ",
                nrow(duplicates),
                " rows in database with the same primary key ",
                "as the data to insert. Deleting from database before inserting."
              )
              deleteFromServer(
                connection = connection,
                schema = env$schema,
                tableName = env$tableName,
                keyValues = duplicates
              )
              
            } else {
              ParallelLogger::logInfo(
                "- Found ",
                nrow(duplicates),
                " rows in database with the same primary key ",
                "as the data to insert. Removing from data to insert."
              )
              chunk <- chunk %>%
                anti_join(duplicates, by = env$primaryKey)
            }
            # Remove duplicates we already dealt with:
            env$primaryKeyValuesInDb <- env$primaryKeyValuesInDb %>%
              anti_join(duplicates, by = env$primaryKey)
          }
        }
        if (nrow(chunk) == 0) {
          ParallelLogger::logInfo("- No data left to insert")
        } else {
          DatabaseConnector::insertTable(
            connection = connection,
            tableName = paste(env$schema, env$tableName, sep = "."),
            data = chunk,
            dropTableIfExists = FALSE,
            createTable = FALSE,
            tempTable = FALSE,
            progressBar = TRUE
          )
        }
      }
      readr::read_csv_chunked(
        file = file.path(unzipFolder, csvFileName),
        callback = uploadChunk,
        chunk_size = 1e7,
        col_types = readr::cols(),
        guess_max = 1e6,
        progress = FALSE
      )
      
      # chunk <- readr::read_csv(file = file.path(unzipFolder, csvFileName),
      # col_types = readr::cols(),
      # guess_max = 1e6)
      
    }
  }
  invisible(lapply(unique(specifications$tableName), uploadTable))
  delta <- Sys.time() - start
  writeLines(paste("Uploading data took", signif(delta, 3), attr(delta, "units")))
}

deleteFromServer <-
  function(connection, schema, tableName, keyValues) {
    createSqlStatement <- function(i) {
      sql <- paste0(
        "DELETE FROM ",
        schema,
        ".",
        tableName,
        "\nWHERE ",
        paste(paste0(
          colnames(keyValues), " = '", keyValues[i,], "'"
        ), collapse = " AND "),
        ";"
      )
      return(sql)
    }
    batchSize <- 1000
    for (start in seq(1, nrow(keyValues), by = batchSize)) {
      end <- min(start + batchSize - 1, nrow(keyValues))
      sql <- sapply(start:end, createSqlStatement)
      sql <- paste(sql, collapse = "\n")
      DatabaseConnector::executeSql(
        connection,
        sql,
        progressBar = FALSE,
        reportOverallTime = FALSE,
        runAsBatch = TRUE
      )
    }
  }

deleteAllRecordsForDatabaseId <- function(connection,
                                          schema,
                                          tableName,
                                          databaseId) {
  sql <-
    "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
  sql <- SqlRender::render(
    sql = sql,
    schema = schema,
    table_name = tableName,
    database_id = databaseId
  )
  databaseIdCount <-
    DatabaseConnector::querySql(connection, sql)[, 1]
  if (databaseIdCount != 0) {
    ParallelLogger::logInfo(
      sprintf(
        "- Found %s rows in  database with database ID '%s'. Deleting all before inserting.",
        databaseIdCount,
        databaseId
      )
    )
    sql <-
      "DELETE FROM @schema.@table_name WHERE database_id = '@database_id';"
    sql <- SqlRender::render(
      sql = sql,
      schema = schema,
      table_name = tableName,
      database_id = databaseId
    )
    DatabaseConnector::executeSql(connection,
                                  sql,
                                  progressBar = FALSE,
                                  reportOverallTime = FALSE)
  }
}
