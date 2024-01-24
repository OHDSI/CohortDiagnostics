# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of FeatureExtraction
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

#' Get covariate information from the database
#'
#' @description
#' Uses one or several covariate builder functions to construct covariates.
#'
#' @details
#' This function uses the data in the CDM to construct a large set of covariates for the provided
#' cohort. The cohort is assumed to be in an existing table with these fields: 'subject_id',
#' 'cohort_definition_id', 'cohort_start_date'. Optionally, an extra field can be added containing the
#' unique identifier that will be used as rowID in the output.
#'
#' @param connectionDetails      An R object of type \code{connectionDetails} created using the
#'                               function \code{createConnectionDetails} in the
#'                               \code{DatabaseConnector} package. Either the \code{connection} or
#'                               \code{connectionDetails} argument should be specified.
#' @param connection             A connection to the server containing the schema as created using the
#'                               \code{connect} function in the \code{DatabaseConnector} package.
#'                               Either the \code{connection} or \code{connectionDetails} argument
#'                               should be specified.
#' @param oracleTempSchema       A schema where temp tables can be created in Oracle.
#' @param cdmDatabaseSchema      The name of the database schema that contains the OMOP CDM instance.
#'                               Requires read permissions to this database. On SQL Server, this should
#'                               specify both the database and the schema, so for example
#'                               'cdm_instance.dbo'.
#' @param cdmVersion             Define the OMOP CDM version used: currently supported is "5".
#' @param cohortTable            Name of the (temp) table holding the cohort for which we want to
#'                               construct covariates
#' @param cohortDatabaseSchema   If the cohort table is not a temp table, specify the database schema
#'                               where the cohort table can be found. On SQL Server, this should
#'                               specify both the database and the schema, so for example
#'                               'cdm_instance.dbo'.
#' @param cohortTableIsTemp      Is the cohort table a temp table?
#' @param cohortId               DEPRECATED:For which cohort ID(s) should covariates be constructed? If set to -1,
#'                               covariates will be constructed for all cohorts in the specified cohort
#'                               table.
#' @param cohortIds              For which cohort ID(s) should covariates be constructed? If set to c(-1),
#'                               covariates will be constructed for all cohorts in the specified cohort
#'                               table.
#' @param rowIdField             The name of the field in the cohort table that is to be used as the
#'                               row_id field in the output table. This can be especially usefull if
#'                               there is more than one period per person.
#' @param covariateSettings      Either an object of type \code{covariateSettings} as created using one
#'                               of the createCovariate functions, or a list of such objects.
#' @param aggregated             Should aggregate statistics be computed instead of covariates per
#'                               cohort entry?
#'
#' @return
#' Returns an object of type \code{covariateData}, containing information on the covariates.
#'
#' @examples
#' \dontrun{
#' eunomiaConnectionDetails <- Eunomia::getEunomiaConnectionDetails()
#' covSettings <- createDefaultCovariateSettings()
#' Eunomia::createCohorts(
#'   connectionDetails = eunomiaConnectionDetails,
#'   cdmDatabaseSchema = "main",
#'   cohortDatabaseSchema = "main",
#'   cohortTable = "cohort"
#' )
#' covData <- getDbCovariateData(
#'   connectionDetails = eunomiaConnectionDetails,
#'   oracleTempSchema = NULL,
#'   cdmDatabaseSchema = "main",
#'   cdmVersion = "5",
#'   cohortTable = "cohort",
#'   cohortDatabaseSchema = "main",
#'   cohortTableIsTemp = FALSE,
#'   cohortIds = -1,
#'   rowIdField = "subject_id",
#'   covariateSettings = covSettings,
#'   aggregated = FALSE
#' )
#' }
#' 
getDbCovariateData <- function(connection = NULL,
                               oracleTempSchema = NULL,
                               cdmDatabaseSchema,
                               cdmVersion = "5",
                               cohortTable = "cohort",
                               cohortDatabaseSchema = cdmDatabaseSchema,
                               cohortTableIsTemp = FALSE,
                               cohortIds = c(-1),
                               rowIdField = "subject_id",
                               covariateSettings,
                               aggregated = FALSE) {
  if (cdmVersion == "4") {
    stop("CDM version 4 is not supported any more")
  }
  if (cohortTableIsTemp) {
    if (substr(cohortTable, 1, 1) == "#") {
      cohortDatabaseSchemaTable <- cohortTable
    } else {
      cohortDatabaseSchemaTable <- paste0("#", cohortTable)
    }
  } else {
    cohortDatabaseSchemaTable <- paste(cohortDatabaseSchema, cohortTable, sep = ".")
  }
  sql <- "SELECT cohort_definition_id, COUNT_BIG(*) AS population_size FROM @cohort_database_schema_table {@cohort_ids != -1} ? {WHERE cohort_definition_id IN (@cohort_ids)} GROUP BY cohort_definition_id;"
  sql <- SqlRender::render(sql = sql,
                           cohort_database_schema_table = cohortDatabaseSchemaTable,
                           cohort_ids = cohortIds)
  sql <- SqlRender::translate(sql = sql,
                              targetDialect = dbms(connection),
                              oracleTempSchema = oracleTempSchema)
  temp <- querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  if (aggregated) {
    populationSize <- temp$populationSize
    names(populationSize) <- temp$cohortDefinitionId
  } else {
    populationSize <- sum(temp$populationSize)
  }
  if (sum(populationSize) == 0) {
    covariateData <- FeatureExtraction:::createEmptyCovariateData(cohortIds, aggregated, covariateSettings$temporal)
    warning("Population is empty. No covariates were constructed")
  } else {
    if (inherits(covariateSettings, "covariateSettings")) {
      covariateSettings <- list(covariateSettings)
    }
    if (is.list(covariateSettings)) {
      covariateData <- NULL
      hasData <- function(data) {
        return(!is.null(data) && (data %>% count() %>% pull()) > 0)
      }
      for (i in 1:length(covariateSettings)) {
        fun <- attr(covariateSettings[[i]], "fun")
        args <- list(connection = connection,
                     oracleTempSchema = oracleTempSchema,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     cohortTable = cohortDatabaseSchemaTable,
                     cohortIds = cohortIds,
                     cdmVersion = cdmVersion,
                     rowIdField = rowIdField,
                     covariateSettings = covariateSettings[[i]],
                     aggregated = aggregated)
        tempCovariateData <- do.call(eval(parse(text = fun)), args)
        if (is.null(covariateData)) {
          covariateData <- tempCovariateData
        } else {
          if (hasData(covariateData$covariates)) {
            if (hasData(tempCovariateData$covariates)) {
              Andromeda::appendToTable(covariateData$covariates, tempCovariateData$covariates)
            }
          } else if (hasData(tempCovariateData$covariates)) {
            covariateData$covariates <- tempCovariateData$covariates
          }
          if (hasData(covariateData$covariatesContinuous)) {
            if (hasData(tempCovariateData$covariatesContinuous)) {
              Andromeda::appendToTable(covariateData$covariatesContinuous, tempCovariateData$covariatesContinuous)
            } else if (hasData(tempCovariateData$covariatesContinuous)) {
              covariateData$covariatesContinuous <- tempCovariateData$covariatesContinuous
            }
          }
          Andromeda::appendToTable(covariateData$covariateRef, tempCovariateData$covariateRef)
          Andromeda::appendToTable(covariateData$analysisRef, tempCovariateData$analysisRef)
          for (name in names(attr(tempCovariateData, "metaData"))) {
            if (is.null(attr(covariateData, "metaData")[name])) {
              attr(covariateData, "metaData")[[name]] <- attr(tempCovariateData, "metaData")[[name]]
            } else {
              attr(covariateData, "metaData")[[name]] <- list(
                attr(covariateData, "metaData")[[name]],
                attr(tempCovariateData, "metaData")[[name]]
              )
            }
          }
        }
      }
    }
    attr(covariateData, "metaData")$populationSize <- populationSize
    attr(covariateData, "metaData")$cohortIds <- cohortIds
  }
  return(covariateData)
}

#' Get default covariate information from the database
#'
#' @description
#' Constructs a large default set of covariates for one or more cohorts using data in the CDM schema.
#' Includes covariates for all drugs, drug classes, condition, condition classes, procedures,
#' observations, etc.
#'
#' @param covariateSettings      Either an object of type \code{covariateSettings} as created using one
#'                               of the createCovariate functions, or a list of such objects.
#' @param targetDatabaseSchema   (Optional) The name of the database schema where the resulting covariates
#'                               should be stored.
#' @param targetCovariateTable  (Optional) The name of the table where the resulting covariates will
#'                               be stored. If not provided, results will be fetched to R. The table can be
#'                               a permanent table in the \code{targetDatabaseSchema} or a temp table. If
#'                               it is a temp table, do not specify \code{targetDatabaseSchema}.
#' @param targetCovariateRefTable (Optional) The name of the table where the covariate reference will be stored.
#' @param targetAnalysisRefTable (Optional) The name of the table where the analysis reference will be stored.
#'
#' @examples
#' \dontrun{
#' connection <- DatabaseConnector::connect(connectionDetails)
#' Eunomia::createCohorts(connectionDetails)
#'
#' results <- getDbDefaultCovariateData(
#'   connection = connection,
#'   cdmDatabaseSchema = "main",
#'   cohortTable = "cohort",
#'   covariateSettings = createDefaultCovariateSettings(),
#'   targetDatabaseSchema = "main",
#'   targetCovariateTable = "ut_cov",
#'   targetCovariateRefTable = "ut_cov_ref",
#'   targetAnalysisRefTable = "ut_cov_analysis_ref"
#' )
#' }
#'
getDbDefaultCovariateData <- function(connection,
                                      oracleTempSchema = NULL,
                                      cdmDatabaseSchema,
                                      cohortTable = "#cohort_person",
                                      cohortIds = c(-1),
                                      cdmVersion = "5",
                                      rowIdField = "subject_id",
                                      covariateSettings,
                                      targetDatabaseSchema,
                                      targetCovariateTable,
                                      targetCovariateRefTable,
                                      targetAnalysisRefTable,
                                      aggregated = FALSE) {
  if (!is(covariateSettings, "covariateSettings")) {
    stop("Covariate settings object not of type covariateSettings")
  }
  if (cdmVersion == "4") {
    stop("Common Data Model version 4 is not supported")
  }
  if (!missing(targetCovariateTable) && !is.null(targetCovariateTable) && aggregated) {
    stop("Writing aggregated results to database is currently not supported")
  }
  
  settings <- .toJson(covariateSettings)
  rJava::J("org.ohdsi.featureExtraction.FeatureExtraction")$init(system.file("", package = "FeatureExtraction"))
  json <- rJava::J("org.ohdsi.featureExtraction.FeatureExtraction")$createSql(settings, aggregated, cohortTable, rowIdField, rJava::.jarray(as.character(cohortIds)), cdmDatabaseSchema)
  todo <- .fromJson(json)
  if (length(todo$tempTables) != 0) {
    ParallelLogger::logInfo("Sending temp tables to server")
    for (i in 1:length(todo$tempTables)) {
      tableName <- names(todo$tempTables)[i]
      if (dbms(connection) == "duckdb") {
        tableName <- sub("#", "", tableName)
      }
      insertTable(connection,
                  tableName = tableName,
                  data = as.data.frame(todo$tempTables[[i]]),
                  dropTableIfExists = TRUE,
                  createTable = TRUE,
                  tempTable = TRUE,
                  oracleTempSchema = oracleTempSchema)
    }
  }
  
  ParallelLogger::logInfo("Constructing features on server")
  
  sql <- SqlRender::translate(sql = todo$sqlConstruction,
                              targetDialect = dbms(connection),
                              oracleTempSchema = oracleTempSchema)
  profile <- (!is.null(getOption("dbProfile")) && getOption("dbProfile") == TRUE)
  executeSql(connection, sql, profile = profile)
  
  if (missing(targetCovariateTable) || is.null(targetCovariateTable)) {
    ParallelLogger::logInfo("Fetching data from server")
    start <- Sys.time()
    # Binary or non-aggregated features
    covariateData <- Andromeda::andromeda()
    if (!is.null(todo$sqlQueryFeatures)) {
      sql <- SqlRender::translate(sql = todo$sqlQueryFeatures,
                                  targetDialect = dbms(connection),
                                  oracleTempSchema = oracleTempSchema)
      
      covariateData <- querySqlToAndromeda(connection = connection,
                                           sql = sql,
                                           andromeda = covariateData,
                                           andromedaTableName = "covariates",
                                           snakeCaseToCamelCase = TRUE)
    }
    
    # Continuous aggregated features
    if (!is.null(todo$sqlQueryContinuousFeatures)) {
      sql <- SqlRender::translate(sql = todo$sqlQueryContinuousFeatures,
                                  targetDialect = dbms(connection),
                                  oracleTempSchema = oracleTempSchema)
      covariateData <- querySqlToAndromeda(connection = connection,
                                           sql = sql,
                                           andromeda = covariateData,
                                           andromedaTableName = "covariatesContinuous",
                                           snakeCaseToCamelCase = TRUE)
    }
    
    # Covariate reference
    sql <- SqlRender::translate(sql = todo$sqlQueryFeatureRef,
                                targetDialect = dbms(connection),
                                oracleTempSchema = oracleTempSchema)
    
    covariateData <- querySqlToAndromeda(connection = connection,
                                         sql = sql,
                                         andromeda = covariateData,
                                         andromedaTableName = "covariateRef",
                                         snakeCaseToCamelCase = TRUE)
    
    # Analysis reference
    sql <- SqlRender::translate(sql = todo$sqlQueryAnalysisRef,
                                targetDialect = dbms(connection),
                                oracleTempSchema = oracleTempSchema)
    covariateData <- querySqlToAndromeda(connection = connection,
                                         sql = sql,
                                         andromeda = covariateData,
                                         andromedaTableName = "analysisRef",
                                         snakeCaseToCamelCase = TRUE)
    
    # Time reference
    if (!is.null(todo$sqlQueryTimeRef)) {
      sql <- SqlRender::translate(sql = todo$sqlQueryTimeRef,
                                  targetDialect = dbms(connection),
                                  oracleTempSchema = oracleTempSchema)
      covariateData <- querySqlToAndromeda(connection = connection,
                                           sql = sql,
                                           andromeda = covariateData,
                                           andromedaTableName = "timeRef",
                                           snakeCaseToCamelCase = TRUE)
    }
    delta <- Sys.time() - start
    ParallelLogger::logInfo("Fetching data took ", signif(delta, 3), " ", attr(delta, "units"))
  } else {
    # Don't fetch to R , but create on server instead
    ParallelLogger::logInfo("Writing data to table")
    start <- Sys.time()
    convertQuery <- function(sql, databaseSchema, table) {
      if (missing(databaseSchema) || is.null(databaseSchema)) {
        tableName <- table
      } else {
        tableName <- paste(databaseSchema, table, sep = ".")
      }
      return(sub("FROM", paste("INTO", tableName, "FROM"), sql))
    }
    
    # Covariates
    if (!is.null(todo$sqlQueryFeatures)) {
      sql <- convertQuery(todo$sqlQueryFeatures, targetDatabaseSchema, targetCovariateTable)
      sql <- SqlRender::translate(sql = sql,
                                  targetDialect = dbms(connection),
                                  oracleTempSchema = oracleTempSchema)
      DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    }
    
    # Covariate reference
    if (!missing(targetCovariateRefTable) && !is.null(targetCovariateRefTable)) {
      sql <- convertQuery(todo$sqlQueryFeatureRef, targetDatabaseSchema, targetCovariateRefTable)
      sql <- SqlRender::translate(sql = sql,
                                  targetDialect = dbms(connection),
                                  oracleTempSchema = oracleTempSchema)
      executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    }
    
    # Analysis reference
    if (!missing(targetAnalysisRefTable) && !is.null(targetAnalysisRefTable)) {
      sql <- convertQuery(todo$sqlQueryAnalysisRef, targetDatabaseSchema, targetAnalysisRefTable)
      sql <- SqlRender::translate(sql = sql,
                                  targetDialect = dbms(connection),
                                  oracleTempSchema = oracleTempSchema)
      executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    }
    delta <- Sys.time() - start
    ParallelLogger::logInfo("Writing data took", signif(delta, 3), " ", attr(delta, "units"))
  }
  # Drop temp tables
  sql <- SqlRender::translate(sql = todo$sqlCleanup,
                              targetDialect = dbms(connection),
                              oracleTempSchema = oracleTempSchema)
  executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  if (length(todo$tempTables) != 0) {
    for (i in 1:length(todo$tempTables)) {
      sql <- "TRUNCATE TABLE @table;\nDROP TABLE @table;\n"
      sql <- SqlRender::render(sql, table = names(todo$tempTables)[i])
      sql <- SqlRender::translate(sql = sql,
                                  targetDialect = dbms(connection),
                                  oracleTempSchema = oracleTempSchema)
      executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    }
  }
  
  if (missing(targetCovariateTable) || is.null(targetCovariateTable)) {
    attr(covariateData, "metaData") <- list()
    if (is.null(covariateData$covariates) && is.null(covariateData$covariatesContinuous)) {
      warning("No data found, probably because no covariates were specified.")
      covariateData <- createEmptyCovariateData(
        cohortId = cohortId,
        aggregated = aggregated,
        temporal = covariateSettings$temporal
      )
    }
    class(covariateData) <- "CovariateData"
    attr(class(covariateData), "package") <- "FeatureExtraction"
    return(covariateData)
  }
}

.toJson <- function(object) {
  return(as.character(jsonlite::toJSON(object, force = TRUE, auto_unbox = TRUE)))
}

.fromJson <- function(json) {
  return(jsonlite::fromJSON(json, simplifyVector = TRUE, simplifyDataFrame = FALSE))
}
