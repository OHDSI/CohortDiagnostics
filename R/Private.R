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

createIfNotExist <-
  function(type,
           name,
           recursive = TRUE,
           errorMessage = NULL) {
    if (is.null(errorMessage) |
        !class(errorMessage) == 'AssertColection') {
      errorMessage <- checkmate::makeAssertCollection()
    }
    if (!is.null(type)) {
      if (length(name) == 0) {
        stop(ParallelLogger::logError("Must specify ", name))
      }
      if (type %in% c("folder")) {
        if (!file.exists(gsub("/$", "", name))) {
          dir.create(name, recursive = recursive)
          ParallelLogger::logInfo("Created ", type, " at ", name)
        } else {
          # ParallelLogger::logInfo(type, " already exists at ", name)
        }
      }
      checkmate::assertDirectory(x = name,
                                 access = 'x',
                                 add = errorMessage)
    }
    invisible(errorMessage)
  }

enforceMinCellValue <-
  function(data, fieldName, minValues, silent = FALSE) {
    toCensor <-
      !is.na(data[, fieldName]) &
      data[, fieldName] < minValues & data[, fieldName] != 0
    if (!silent) {
      percent <- round(100 * sum(toCensor) / nrow(data), 1)
      ParallelLogger::logInfo(
        "- Censoring ",
        sum(toCensor),
        " values (",
        percent,
        "%) from ",
        fieldName,
        " because value below minimum"
      )
    }
    if (length(minValues) == 1) {
      data[toCensor, fieldName] <- -minValues
    } else {
      data[toCensor, fieldName] <- -minValues[toCensor]
    }
    return(data)
  }


naToEmpty <- function(x) {
  x[is.na(x)] <- ""
  return(x)
}

naToZero <- function(x) {
  x[is.na(x)] <- 0
  return(x)
}



### cdm source information
getDataSourceInformation <-
  function(connection,
           cdm_database_schema) {
    if (!DatabaseConnector::dbExistsTable(conn = connection, name = 'cdm_source')) {
      ParallelLogger::logWarn("CDM Source table not found in CDM. Metadata on CDM source will be limited.")
      return(NULL)
    }
    sqlCdmDataSource <- "select * from @cdm_database_schema.cdm_source;"
    cdmDataSource <-
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sqlCdmDataSource,
        cdm_database_schema = cdmDatabaseSchema,
        snakeCaseToCamelCase = TRUE
      )
    
    if (nrow(cdmDataSource) == 0) {
      ParallelLogger::logWarn("CDM Source table does not have any records. Metadata on CDM source will be limited.")
      return(NULL)
    }
    
    if ('sourceDescription' %in% colnames(cdmDataSource)) {
      sourceDescription <- cdmDataSource$sourceDescription
    } else {
      sourceDescription <- as.character(NA)
    }
    if ('cdmSourceName' %in% colnames(cdmDataSource)) {
      cdmSourceName <- cdmDataSource$cdmSourceName
    } else {
      sourceName <- as.character(NA)
    }
    if ('sourceReleaseDate' %in% colnames(cdmDataSource)) {
      sourceReleaseDate <- cdmDataSource$sourceReleaseDate
    } else {
      sourceReleaseDate <- as.Date(NA)
    }
    if ('cdmReleaseDate' %in% colnames(cdmDataSource)) {
      cdmReleaseDate <- cdmDataSource$cdmReleaseDate
    } else {
      cdmReleaseDate <- as.Date(NA)
    }
    if ('cdmVersion' %in% colnames(cdmDataSource)) {
      cdmVersion <- cdmDataSource$cdmVersion
    } else {
      cdmVersion <- as.character(NA)
    }
    if ('vocabularyVersion' %in% colnames(cdmDataSource)) {
      vocabularyVersion <- cdmDataSource$vocabularyVersion
    } else {
      vocabularyVersion <- as.character(NA)
    }
    return(list(sourceDescription = sourceDescription,
                cdmSourceName = cdmSourceName,
                sourceReleaseDate = sourceReleaseDate,
                cdmReleaseDate = cdmReleaseDate,
                cdmVersion = cdmVersion,
                vocabularyVersion = vocabularyVersion))
  }