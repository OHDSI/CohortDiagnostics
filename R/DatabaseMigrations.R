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

.migrationFileRexp <- "(Migration_([0-9]+))-(.+).sql"
.migrationDir <- system.file("sql", "sql_server", "migrations", package = utils::packageName())

getCompletedMigrations <- function(connection, schema, tablePrefix) {
  sql <- "
    {DEFAULT @migration = migration}
    SELECT migration_file FROM @result_schema.@table_prefix@migration ORDER BY migration_order;"
  migrationsExecuted <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                                   sql = sql,
                                                                   result_schema = schema,
                                                                   table_prefix = tablePrefix,
                                                                   snakeCaseToCamelCase = TRUE)

  return(migrationsExecuted)
}

.getMigrationOrder <- function(migrationsToExecute, regExp = .migrationFileRexp) {
  execution <- data.frame(
    migrationFile = migrationsToExecute,
    sortOrder = as.integer(gsub(regExp, "\\2", migrationsToExecute))
  )
  return(execution[order(execution$sortOrder), ])
}

#' getMigrationStatus
#' @description
#' Get set of migrations that are yet to be run
#'
#' @inheritParams createResultsDataModel
#' @export
getMigrationStatus <- function(connection = NULL,
                               connectionDetails = NULL,
                               schema = NULL,
                               tablePrefix = "") {
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }
  # List all migration files
  allMigrations <- list.files(.migrationDir, pattern = .migrationFileRexp)
  # List files that have been executed
  tables <- DatabaseConnector::getTableNames(connection, schema)
  if (toupper(paste0(tablePrefix, "migration")) %in% tables) {
    migrationsExecuted <- getCompletedMigrations(connection, schema, tablePrefix)
    migrationsToExecute <- setdiff(allMigrations, migrationsExecuted$migrationFile)
  } else {
    migrationsToExecute <- allMigrations
  }

  return(.getMigrationOrder(migrationsToExecute))
}

#' getMigrationStatus
#' @description
#' Check if migrations conform to specific naming conventions.
checkMigrationFiles <- function(dir = .migrationDir) {
  sqlFiles <- list.files(dir, pattern = "*.sql")
  fileNameValidity <- grepl(.migrationFileRexp, sqlFiles)

  if (any(fileNameValidity == 0)) {
    ParallelLogger::logError(paste("File name not valid", sqlFiles[fileNameValidity == 0], collapse = "\n"))
  }

  return(all(fileNameValidity > 0))
}


#' Migrate Data model
#' @description
#' Migrate data from current state to next state
#'
#' It is strongly advised that you have a backup of all data (either sqlite files, a backup database (in the case you
#' are using a postgres backend) or have kept the csv/zip files from your data generation.
#'
#' @inheritParams createResultsDataModel
#' @export
migrateDataModel <- function(connection = NULL,
                             connectionDetails = NULL,
                             schema = NULL,
                             tablePrefix = "") {
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }

  if (connection@dbms == "sqlite" & schema != "main") {
    stop("Invalid schema for sqlite, use schema = 'main'")
  }

  migrationsToExecute <- getMigrationStatus(connection = connection, schema = schema, tablePrefix = tablePrefix)
  ParallelLogger::logInfo("Executing database migrations on schema - ", schema)
  # Run files that have not been executed (in sequence order)
  for (migration in migrationsToExecute$migrationFile) {
    ParallelLogger::logInfo("STARTING MIGRATION: ", migration)
    sql <- SqlRender::loadRenderTranslateSql(file.path("migrations", migration),
                                             packageName = utils::packageName(),
                                             results_schema = schema,
                                             table_prefix = tablePrefix,
                                             dbms = connection@dbms)
    tryCatch({
      DatabaseConnector::executeSql(connection, sql)
    }, error = function(err) {
      ParallelLogger::logError("MIGRATION ERROR: ", migration)
      stop(paste("Error executing migration", migration, "\n", err))
    })

    ParallelLogger::logInfo("MIGRATION COMPLETE: ", migration)
  }

  # Verify that files are in migration table
  migrationsCompleted <- getCompletedMigrations(connection, schema, tablePrefix)
  migrationsNotCompleted <- setdiff(migrationsToExecute$migrationFile, migrationsCompleted$migrationFile)
  if (length(migrationsNotCompleted) > 0) {
    stop(paste("Migration :", migrationsNotCompleted, "has not completed", collapse = "\n"))
  }

  # Complete by updating version number to current package version
  sql <- SqlRender::loadRenderTranslateSql("UpdateVersionNumber.sql",
                                           packageName = utils::packageName(),
                                           results_schema = schema,
                                           table_prefix = tablePrefix,
                                           dbms = connection@dbms)
  DatabaseConnector::executeSql(connection, sql)
}