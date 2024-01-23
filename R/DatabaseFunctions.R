# Copyright 2023 Observational Health Data Sciences and Informatics
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

#' renderTranslateExecuteSql
#'
#' @param connection 
#' @param sql 
#' @param errorReportFile 
#' @param snakeCaseToCamelCase 
#' @param oracleTempSchema 
#' @param tempEmulationSchema 
#' @param integerAsNumeric 
#' @param integer64AsNumeric 
#' @param ... 
#'
#' @return NONE
renderTranslateExecuteSql <- function(connection,
                                      sql,
                                      errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                      snakeCaseToCamelCase = FALSE,
                                      oracleTempSchema = NULL,
                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                      integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                      integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE),
                                      ...) {
  # renderSQL
  sql <- SqlRender::render(sql = sql, ...)
  # translate
  sql <- SqlRender::translate(sql = sql, targetDialect = CDMConnector::dbms(connection))
  # execute
  DBI::dbExecute(conn = connection, statement = sql)
}

#' renderTranslateQuerySql
#'
#' @param connection 
#' @param sql 
#' @param errorReportFile 
#' @param snakeCaseToCamelCase 
#' @param oracleTempSchema 
#' @param tempEmulationSchema 
#' @param integerAsNumeric 
#' @param integer64AsNumeric 
#' @param ... 
#'
#' @return the query results
renderTranslateQuerySql <- function(connection,
                                    sql,
                                    errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                    snakeCaseToCamelCase = FALSE,
                                    oracleTempSchema = NULL,
                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                    integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                    integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE),
                                    ...) {
  # renderSQL
  sql <- SqlRender::render(sql = sql, ...)
  # translate
  sql <- SqlRender::translate(sql = sql, targetDialect = CDMConnector::dbms(connection))
  # get results
  result <- DBI::dbGetQuery(conn = connection, statement = sql)
  if (snakeCaseToCamelCase) {
    colnames(result) <- SqlRender::snakeCaseToCamelCase(colnames(result))
  }
  return(result)
}

#' querySql
#'
#' @param connection 
#' @param sql 
#' @param errorReportFile 
#' @param snakeCaseToCamelCase 
#' @param integerAsNumeric 
#' @param integer64AsNumeric 
#'
#' @return
#' @export
#'
#' @examples
querySql <- function(connection,
                     sql,
                     errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                     snakeCaseToCamelCase = FALSE,
                     integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                     integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE)) {
  result <- DBI::dbGetQuery(conn = connection, statement = sql)
  if (snakeCaseToCamelCase) {
    colnames(result) <- SqlRender::snakeCaseToCamelCase(colnames(result))
  }
  return(result)
}

querySqlToAndromeda <- function(connection,
                                sql,
                                andromeda,
                                andromedaTableName,
                                errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                snakeCaseToCamelCase = FALSE,
                                appendToTable = FALSE,
                                integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                             default = TRUE
                                ),
                                integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                               default = TRUE
                                )) {
  if (isCDMConnection(connection)) {
    andromeda[[andromedaTableName]] <- querySql(connection = connection,
                                                sql = sql,
                                                snakeCaseToCamelCase = snakeCaseToCamelCase)
  } else {
    DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      snakeCaseToCamelCase = snakeCaseToCamelCase,
      andromeda = andromeda,
      andromedaTableName = andromedaTableName
    )
    return(andromeda)
  }
}

#' executeSql
#'
#' @param connection 
#' @param sql 
#' @param profile 
#' @param progressBar 
#' @param reportOverallTime 
#' @param errorReportFile 
#' @param runAsBatch 
#'
#' @return
#' @export
#'
#' @examples
executeSql <- function(connection,
                       sql,
                       profile = FALSE,
                       progressBar = !as.logical(Sys.getenv("TESTTHAT", unset = FALSE)),
                       reportOverallTime = TRUE,
                       errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                       runAsBatch = FALSE) {
  # execute
  DBI::dbExecute(conn = connection, statement = sql)
}


isCDMConnection <- function(connection) {
  return(!("dbms" %in% names(attributes(connection))))
}

#' getDbms
#'
#' @param connection 
#'
#' @return
#' @export
#'
#' @examples
getDbms <- function(connection) {
  result <- NULL
  if (isCDMConnection(connection)) {
    result <- CDMConnector::dbms(connection)
  } else {
    result <- connection@dbms
  }
  return(result)
}

#' getTableNames
#'
#' @param connection 
#' @param cdmDatabaseSchema 
#'
#' @return
#' @export
#'
#' @examples
getTableNames <- function(connection, cdmDatabaseSchema) {
  result <- NULL
  if (isCDMConnection(connection)) {
    result <- CDMConnector::listTables(connection, cdmDatabaseSchema)
  } else {
    result <- DatabaseConnector::getTableNames(connection, cdmDatabaseSchema)
  }
  return(result)
}

#' Insert a table on the server
#'
#' @description
#' This function sends the data in a data frame to a table on the server. Either a new table
#' is created, or the data is appended to an existing table.
#'
#' @template Connection
#' @template DatabaseSchema
#' @param tableName           The name of the table where the data should be inserted.
#' @param data                The data frame containing the data to be inserted.
#' @param dropTableIfExists   Drop the table if the table already exists before writing?
#' @param createTable         Create a new table? If false, will append to existing table.
#' @param tempTable           Should the table created as a temp table?
#' @template TempEmulationSchema 
#' @param bulkLoad            If using Redshift, PDW, Hive or Postgres, use more performant bulk loading
#'                            techniques. Does not work for temp tables (except for HIVE). See Details for
#'                            requirements for the various platforms.
#' @param useMppBulkLoad      DEPRECATED. Use `bulkLoad` instead.
#' @param progressBar         Show a progress bar when uploading?
#' @param camelCaseToSnakeCase If TRUE, the data frame column names are assumed to use camelCase and
#'                             are converted to snake_case before uploading.
#'
#' @details
#' This function sends the data in a data frame to a table on the server. Either a new table is
#' created, or the data is appended to an existing table. NA values are inserted as null values in the
#' database.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(
#'   dbms = "mysql",
#'   server = "localhost",
#'   user = "root",
#'   password = "blah"
#' )
#' conn <- connect(connectionDetails)
#' data <- data.frame(x = c(1, 2, 3), y = c("a", "b", "c"))
#' insertTable(conn, "my_schema", "my_table", data)
#' disconnect(conn)
#'
#' ## bulk data insert with Redshift or PDW
#' connectionDetails <- createConnectionDetails(
#'   dbms = "redshift",
#'   server = "localhost",
#'   user = "root",
#'   password = "blah",
#'   schema = "cdm_v5"
#' )
#' conn <- connect(connectionDetails)
#' data <- data.frame(x = c(1, 2, 3), y = c("a", "b", "c"))
#' insertTable(
#'   connection = connection,
#'   databaseSchema = "scratch",
#'   tableName = "somedata",
#'   data = data,
#'   dropTableIfExists = TRUE,
#'   createTable = TRUE,
#'   tempTable = FALSE,
#'   bulkLoad = TRUE
#' ) # or, Sys.setenv("DATABASE_CONNECTOR_BULK_UPLOAD" = TRUE)
#' }
insertTable <- function(connection,
                        databaseSchema = NULL,
                        tableName,
                        data,
                        dropTableIfExists = TRUE,
                        createTable = TRUE,
                        tempTable = FALSE,
                        oracleTempSchema = NULL,
                        tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                        bulkLoad = Sys.getenv("DATABASE_CONNECTOR_BULK_UPLOAD"),
                        useMppBulkLoad = Sys.getenv("USE_MPP_BULK_LOAD"),
                        progressBar = FALSE,
                        camelCaseToSnakeCase = FALSE) {
  
  if (isCDMConnection(connection)) {
    if (camelCaseToSnakeCase) {
      colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
    }
    DBI::dbWriteTable(connection, tableName, data, overwrite = TRUE, temporary = tempTable)
  } else {
    DatabaseConnector::insertTable(connection,
                                   databaseSchema,
                                   tableName,
                                   data,
                                   dropTableIfExists,
                                   createTable,
                                   tempTable,
                                   oracleTempSchema,
                                   tempEmulationSchema,
                                   bulkLoad,
                                   useMppBulkLoad,
                                   progressBar,
                                   camelCaseToSnakeCase)
  }
}

#' Used to insert the inclusion rule names from a cohort definition set
#' when generating cohorts that include cohort statistics
#'
#' @description
#' This function will take a cohortDefinitionSet that inclusions the Circe JSON
#' representation of each cohort, parse the InclusionRule property to obtain
#' the inclusion rule name and sequence number and insert the values into the
#' cohortInclusionTable. This function is only required when generating cohorts
#' that include cohort statistics.
#'
#' @template Connection
#'
#' @template CohortDefinitionSet
#'
#' @template CohortDatabaseSchema
#'
#' @param cohortInclusionTable         Name of the inclusion table, one of the tables for storing
#'                                     inclusion rule statistics.
#'
#' @returns
#' A data frame containing the inclusion rules by cohort and sequence ID
#'
insertInclusionRuleNames <- function(connection,
                                     cohortDefinitionSet,
                                     cohortDatabaseSchema,
                                     cohortInclusionTable = getCohortTableNames()$cohortInclusionTable) {
  # Parameter validation
  if (is.null(connection)) {
    stop("You must provide a database connection.")
  }
  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assertNames(colnames(cohortDefinitionSet),
                         must.include = c(
                           "cohortId",
                           "cohortName",
                           "json"
                         )
  )
  
  tableList <- getTableNames(connection, cohortDatabaseSchema)
  if (!toupper(cohortInclusionTable) %in% toupper(tableList)) {
    stop(paste0(cohortInclusionTable, " table not found in schema: ", cohortDatabaseSchema, ". Please make sure the table is created using the createCohortTables() function before calling this function."))
  }
  
  # Assemble the cohort inclusion rules
  # NOTE: This data frame must match the @cohort_inclusion_table
  # structure as defined in inst/sql/sql_server/CreateCohortTables.sql
  inclusionRules <- data.frame(
    cohortDefinitionId = bit64::integer64(),
    ruleSequence = integer(),
    name = character(),
    description = character()
  )
  # Remove any cohort definitions that do not include the JSON property
  cohortDefinitionSet <- cohortDefinitionSet[!(is.null(cohortDefinitionSet$json) | is.na(cohortDefinitionSet$json)), ]
  for (i in 1:nrow(cohortDefinitionSet)) {
    cohortDefinition <- RJSONIO::fromJSON(content = cohortDefinitionSet$json[i], digits = 23)
    if (!is.null(cohortDefinition$InclusionRules)) {
      nrOfRules <- length(cohortDefinition$InclusionRules)
      if (nrOfRules > 0) {
        for (j in 1:nrOfRules) {
          ruleName <- cohortDefinition$InclusionRules[[j]]$name
          ruleDescription <- cohortDefinition$InclusionRules[[j]]$description
          if (is.na(ruleName) || ruleName == "") {
            ruleName <- paste0("Unamed rule (Sequence ", j - 1, ")")
          }
          if (is.null(ruleDescription)) {
            ruleDescription <- ""
          }
          inclusionRules <- rbind(
            inclusionRules,
            data.frame(
              cohortDefinitionId = bit64::as.integer64(cohortDefinitionSet$cohortId[i]),
              ruleSequence = as.integer(j - 1),
              name = ruleName,
              description = ruleDescription
            )
          )
        }
      }
    }
  }
  
  # Remove any existing data to prevent duplication
  renderTranslateExecuteSql(
    connection = connection,
    sql = "TRUNCATE TABLE @cohort_database_schema.@table;",
    progressBar = FALSE,
    reportOverallTime = FALSE,
    cohort_database_schema = cohortDatabaseSchema,
    table = cohortInclusionTable
  )
  
  # Insert the inclusion rules
  if (nrow(inclusionRules) > 0) {
    ParallelLogger::logInfo("Inserting inclusion rule names")
    insertTable(
      connection = connection,
      databaseSchema = cohortDatabaseSchema,
      tableName = cohortInclusionTable,
      data = inclusionRules,
      dropTableIfExists = FALSE,
      createTable = FALSE,
      camelCaseToSnakeCase = TRUE
    )
  } else {
    warning("No inclusion rules found in the cohortDefinitionSet")
  }
  
  invisible(inclusionRules)
}

# Get stats data
getStatsTable <- function(connection,
                          cohortDatabaseSchema,
                          table,
                          snakeCaseToCamelCase = FALSE,
                          databaseId = NULL,
                          includeDatabaseId = TRUE) {
  # Force databaseId to NULL when includeDatabaseId is FALSE
  if (!includeDatabaseId) {
    databaseId <- NULL
  }
  
  ParallelLogger::logInfo("- Fetching data from ", table)
  sql <- "SELECT {@database_id != ''}?{CAST('@database_id' as VARCHAR(255)) as database_id,} * FROM @cohort_database_schema.@table"
  data <- renderTranslateQuerySql(
    sql = sql,
    connection = connection,
    snakeCaseToCamelCase = snakeCaseToCamelCase,
    table = table,
    cohort_database_schema = cohortDatabaseSchema,
    database_id = ifelse(test = is.null(databaseId),
                         yes = "",
                         no = databaseId
    )
  )
  
  if (!snakeCaseToCamelCase) {
    colnames(data) <- tolower(colnames(data))
  }
  
  return(data)
}

#' Get Cohort Inclusion Stats Table Data
#' @description
#' This function returns a data frame of the data in the Cohort Inclusion Tables.
#' Results are organized in to a list with 5 different data frames:
#'  * cohortInclusionTable
#'  * cohortInclusionResultTable
#'  * cohortInclusionStatsTable
#'  * cohortSummaryStatsTable
#'  * cohortCensorStatsTable
#'
#'
#' These can be optionally specified with the `outputTables`.
#' See `exportCohortStatsTables` function for saving data to csv.
#'
#' @md
#' @inheritParams exportCohortStatsTables
#'
#' @param snakeCaseToCamelCase        Convert column names from snake case to camel case.
#' @param outputTables                Character vector. One or more of "cohortInclusionTable", "cohortInclusionResultTable",
#'                                    "cohortInclusionStatsTable", "cohortInclusionStatsTable", "cohortSummaryStatsTable"
#'                                    or "cohortCensorStatsTable". Output is limited to these tables. Cannot export, for,
#'                                    example, the cohort table. Defaults to all stats tables.
getCohortStats <- function(connection,
                           cohortDatabaseSchema,
                           databaseId = NULL,
                           snakeCaseToCamelCase = TRUE,
                           outputTables = c(
                             "cohortInclusionTable",
                             "cohortInclusionResultTable",
                             "cohortInclusionStatsTable",
                             "cohortInclusionStatsTable",
                             "cohortSummaryStatsTable",
                             "cohortCensorStatsTable"
                           ),
                           cohortTableNames = getCohortTableNames()) {
  # Names of cohort table names must include output tables
  checkmate::assertNames(names(cohortTableNames), must.include = outputTables)
  # ouput tables strictly the set of allowed tables
  checkmate::assertNames(outputTables,
                         subset.of = c(
                           "cohortInclusionTable",
                           "cohortInclusionResultTable",
                           "cohortInclusionStatsTable",
                           "cohortInclusionStatsTable",
                           "cohortSummaryStatsTable",
                           "cohortCensorStatsTable"
                         )
  )
  results <- list()
  for (table in outputTables) {
    # The cohortInclusionTable does not hold database
    # specific information so the databaseId
    # should NOT be included.
    includeDatabaseId <- ifelse(test = table != "cohortInclusionTable",
                                yes = TRUE,
                                no = FALSE
    )
    results[[table]] <- getStatsTable(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      table = cohortTableNames[[table]],
      snakeCaseToCamelCase = snakeCaseToCamelCase,
      includeDatabaseId = includeDatabaseId
    )
  }
  return(results)
}
