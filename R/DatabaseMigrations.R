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

#' Migrate Data model
#' @description
#' Migrate data from current state to next state
#'
#' It is strongly advised that you have a backup of all data (either sqlite files, a backup database (in the case you
#' are using a postgres backend) or have kept the csv/zip files from your data generation.
#'
#' @inheritParams getDataMigrator
#' @export
migrateDataModel <- function(connectionDetails, databaseSchema, tablePrefix = "") {
  migrator <- getDataMigrator(connectionDetails = connectionDetails, databaseSchema = databaseSchema, tablePrefix = tablePrefix)
  migrator$executeMigrations()
}


#' Get database migrations instance
#' @description
#'
#' Returns ResultModelManager DataMigrationsManager instance.
# '@seealso [ResultModelManager::DataMigrationManager] which this function is a utility for.
#'
#' @param connectionDetails             DatabaseConnector connection details object
#' @param databaseSchema                String schema where database schema lives
#' @param  tablePrefix                   (Optional) Use if a table prefix is used before table names (e.g. "cd_")
#' @returns Instance of ResultModelManager::DataMigrationManager that has interface for converting existing data models
#' @export
getDataMigrator <- function(connectionDetails, databaseSchema, tablePrefix = "") {
  ResultModelManager::DataMigrationManager$new(connectionDetails = connectionDetails,
                                               databaseSchema = databaseSchema,
                                               tablePrefix = tablePrefix,
                                               migrationPath = "migrations",
                                               packageName = utils::packageName())
}