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

#' Create postgres database schema for cohort diagnostics results storage
#'
#' @description
#' Builds full schema for cohort
#' @param connectionDetails
#'
#' @export
buildPostgresDatabaseSchema <- function(connectionDetails, schemaName, overwrite = FALSE) {
  checkmate::assert(connectionDetails$dbms == "postgresql")
  connection <- DatabaseConnector::connect(connectionDetails)
  if (overwrite) {
    ParallelLogger::logInfo("Dropping schema")
    DatabaseConnector::renderTranslateExecuteSql(connection, "DROP SCHEMA IF EXISTS @schema CASCADE;", schema = schemaName)
  }
  DatabaseConnector::renderTranslateExecuteSql(connection, "CREATE SCHEMA IF NOT EXISTS @schema;", schema = schemaName)

  ParallelLogger::logInfo("Creating tables")

  sql <- SqlRender::readSql(system.file("sql/sql_server", "sql_server_ddl_results_data_model.sql", package = "CohortDiagnostics"))
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    resultsDatabaseSchema = schemaName
  )

  ParallelLogger::logInfo("Disconnecting from database")
  DatabaseConnector::disconnect(connection)
}

.checkPsqlExists <- function(testCmd = "psql --version") {
  res <- base::system(testCmd)
  if (res != 0) {
    stop("Error psql command not found in system path. Copy util will not function")
  }
}

#' Import csv files in to postgres database instance
#'
#' @description
#'
#' @param connectionDetails
#'
#' @export
importCsvFilesToPostgres <- function(connectionDetails, schemaName, pathToCsvFiles) {
  checkmate::assert(connectionDetails$dbms == "postgresql")
  checkmate::assertDirectoryExists(pathToCsvFiles)

  hostServerDb <- strsplit(connectionDetails$server, "/")[[1]]
  pgPassword <- paste0("PGPASSWORD=", connectionDetails$password)

  # Test connection actually works
  connection <- DatabaseConnector::connect(connectionDetails)
  tables <- DatabaseConnector::getTableNames(connection, schemaName)
  DatabaseConnector::disconnect(connection)

  files <- Sys.glob(file.path(pathToCsvFiles, "*.csv"))
  for (csvFile in files) {
    ParallelLogger::logInfo(paste("Uploading file", csvFile))
    tableName <- stringr::str_to_upper(stringr::str_split(basename(csvFile), ".csv")[[1]][[1]])

    if (!(tableName %in% tables)) {
      ParallelLogger::logWarn(paste("Skipping table", tableName, "not found in schema with tables", stringi::stri_join(tables, collapse = ", ")))
      next
    }
    # Read first line to get header column order, we assume these are large files
    head <- read.csv(file=csvFile, nrows=1)
    headers <- stringi::stri_join(names(head), collapse = ", ")
    headers <- paste0("(", headers, ")")
    tablePath <-  paste0(schemaName, ".", tableName)
    filePathStr <-  paste0("'", csvFile, "'")

    copyCommand <- paste(
      pgPassword,
      "psql",
      "-h", hostServerDb[[1]], # Host
      "-d", hostServerDb[[2]], # Database
      "-p", connectionDetails$port,
      "-U", connectionDetails$user,
      "-c \"\\copy", tablePath,
      headers,
      "FROM", filePathStr,
      "DELIMITER ',' CSV HEADER;\""
    )

    result <- base::system(copyCommand)
    if (result != 0) {
      stop("Copy failure, psql returned a non zero status")
    }
    ParallelLogger::logInfo(paste("Copy file complete", csvFile))
  }
}