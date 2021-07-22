# Copyright 2021 Observational Health Data Sciences and Informatics
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



#' Returns information from CDM source table.
#'
#' @description
#' Returns CDM source name, description, release date, CDM release date, version
#' and vocabulary version, where available.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @return
#' Returns a data frame from CDM Data source.
#'
#' @export
getCdmDataSourceInformation <-
  function(connectionDetails = NULL,
           connection = NULL,
           cdmDatabaseSchema) {
    
    # Set up connection to server ----
    if (is.null(connection)) {
      if (!is.null(connectionDetails)) {
        connection <- DatabaseConnector::connect(connectionDetails)
        on.exit(DatabaseConnector::disconnect(connection))
      } else {
        stop("No connection or connectionDetails provided.")
      }
    }
    
    if (!DatabaseConnector::dbExistsTable(conn = connection, name = 'cdm_source')) {
      ParallelLogger::logWarn("CDM Source table not found in CDM. Metadata on CDM source will be limited.")
      return(NULL)
    }
    sqlCdmDataSource <-
      "select * from @cdm_database_schema.cdm_source;"
    cdmDataSource <-
      renderTranslateQuerySql(
        connection = connection,
        sql = sqlCdmDataSource,
        cdm_database_schema = cdmDatabaseSchema,
        snakeCaseToCamelCase = TRUE
      )
    
    if (nrow(cdmDataSource) == 0) {
      ParallelLogger::logWarn("CDM Source table does not have any records. Metadata on CDM source will be limited.")
      return(NULL)
    }
    
    sourceDescription <- as.character(NA)
    if ('sourceDescription' %in% colnames(cdmDataSource)) {
      sourceDescription <- cdmDataSource$sourceDescription
    }
    
    cdmSourceName <- as.character(NA)
    if ('cdmSourceName' %in% colnames(cdmDataSource)) {
      cdmSourceName <- cdmDataSource$cdmSourceName
    }
    
    sourceReleaseDate <- as.Date(NA)
    if ('sourceReleaseDate' %in% colnames(cdmDataSource)) {
      if (class(cdmDataSource$sourceReleaseDate) != 'Date') {
        try(sourceReleaseDate <- as.Date(cdmDataSource$sourceReleaseDate),
            silent = TRUE)
      } else {
        sourceReleaseDate <- as.Date(cdmDataSource$sourceReleaseDate)
      }
    }
    
    cdmReleaseDate <- as.Date(NA)
    if ('cdmReleaseDate' %in% colnames(cdmDataSource)) {
      if (class(cdmDataSource$cdmReleaseDate) != 'Date') {
        try(cdmReleaseDate <- as.Date(cdmDataSource$cdmReleaseDate),
            silent = TRUE)
      } else {
        cdmReleaseDate <- as.Date(cdmDataSource$cdmReleaseDate)
      }
    }
    
    cdmVersion <- as.character(NA)
    if ('cdmVersion' %in% colnames(cdmDataSource)) {
      cdmVersion <- cdmDataSource$cdmVersion
    }
    
    vocabularyVersion <- as.character(NA)
    if ('vocabularyVersion' %in% colnames(cdmDataSource)) {
      vocabularyVersion <- cdmDataSource$vocabularyVersion
    }
    
    return(
      dplyr::tibble(
        sourceDescription = !!sourceDescription,
        cdmSourceName = !!cdmSourceName,
        sourceReleaseDate = !!sourceReleaseDate,
        cdmReleaseDate = !!cdmReleaseDate,
        cdmVersion = !!cdmVersion,
        vocabularyVersion = !!vocabularyVersion
      )
    )
  }