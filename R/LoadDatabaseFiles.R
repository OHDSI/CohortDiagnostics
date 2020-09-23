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
#' Builds full schema for Diagnostics Explorer
#' @param connectionDetails DatabaseConnector connection details compatible object. 
#'                          Must be dbms="postgres"
#' @param schemaName        Schema to install to
#' @param overwrite         Bool optionally delete and create entire schema again if it exists
#' @export
buildPostgresDatabaseSchema <- function(connectionDetails, 
                                        schemaName, 
                                        overwrite = FALSE) {

  
  checkmate::assert(connectionDetails$dbms == "postgresql")
  connection <- DatabaseConnector::connect(connectionDetails)
  
  if (overwrite) {
    ParallelLogger::logInfo("Dropping schema")
    DatabaseConnector::renderTranslateExecuteSql(connection = connection, 
                                                 sql = "DROP SCHEMA IF EXISTS @schema CASCADE;", 
                                                 schema = schemaName)
  }
  
  DatabaseConnector::renderTranslateExecuteSql(connection = connection, 
                                               sql = "CREATE SCHEMA IF NOT EXISTS @schema;", 
                                               schema = schemaName)

  ParallelLogger::logInfo("Creating tables")
  sql <- SqlRender::readSql(system.file("sql/sql_server", 
                                        "sql_server_ddl_results_data_model.sql",
                                        package = "CohortDiagnostics"))
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    resultsDatabaseSchema = schemaName
  )
  ParallelLogger::logInfo("Disconnecting from database")
  DatabaseConnector::disconnect(connection)
}

#' Import csv files in to postgres database instance
#'
#' @description
#'
#' This utility is a wrapper around the psql command to 
#' allow uploading large datasets to a postgres database
#'
#' @param connectionDetails   DatabaseConnector connection details compatible object. 
#'                            Must be dbms="postgres"
#' @param schemaName          Name where schema stores results
#' @param schemaName          Path to cohort diagnostics csv files to export to
#' @param pathToCsvFiles      Path to cohort diagnostics (results) csv files to import from
#' @param winPsqlPath         Path to folder containing postgres executables 
#'                            e.g. "C:/Program Files/PostgresSQL/12/bin"
#'
#' @export
importCsvFilesToPostgres <- function(connectionDetails, 
                                     schemaName, 
                                     pathToCsvFiles, 
                                     winPsqlPath = "") {
  
  checkmate::assert(connectionDetails$dbms == "postgresql")
  checkmate::assertDirectoryExists(pathToCsvFiles)

  hostServerDb <- strsplit(connectionDetails$server, "/")[[1]]


  Sys.setenv("PGPASSWORD" = connectionDetails$password)
  if (.Platform$OS.type == "windows") {
    command <- file.path(winPsqlPath, "psql.exe")

    if (!file.exists(command)) {
      stop("Error, could not find psql")
    }
  } else {
    command <- "psql"
  }

  # Test connection actually works
  connection <- DatabaseConnector::connect(connectionDetails)
  DatabaseConnector::disconnect(connection)

  files <- Sys.glob(file.path(pathToCsvFiles, "*.csv"))
  
  for (csvFile in files) {
    ParallelLogger::logInfo(paste("Uploading file", csvFile))
    tableName <- stringr::str_to_upper(stringr::str_split(basename(csvFile), 
                                                          ".csv")[[1]][[1]])
    # Read first line to get header column order, we assume these are large files
    head <- utils::read.csv(file = csvFile, nrows = 1)
    headers <- stringi::stri_join(names(head), collapse = ", ")
    headers <- paste0("(", headers, ")")
    tablePath <- paste0(schemaName, ".", tableName)
    filePathStr <- paste0("'", csvFile, "'")

    copyCommand <- paste(
      command,
      "-h", hostServerDb[[1]], # Host
      "-d", hostServerDb[[2]], # Database
      "-p", connectionDetails$port,
      "-U", connectionDetails$user,
      "-c \"\\copy", tablePath,
      headers,
      "FROM", filePathStr,
      "DELIMITER ',' CSV HEADER;\""
    )
    ParallelLogger::logDebug(copyCommand)
    result <- base::system(copyCommand)
    if (result != 0) {
      stop("Copy failure, psql returned a non zero status")
    }
    ParallelLogger::logInfo(paste("Copy file complete", csvFile))
  }
}
