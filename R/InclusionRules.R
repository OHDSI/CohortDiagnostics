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

getInclusionStats <- function(connection,
                              exportFolder,
                              databaseId,
                              cohortDefinitionSet,
                              cohortDatabaseSchema,
                              cohortTableNames,
                              incremental,
                              instantiatedCohorts,
                              minCellCount,
                              recordKeepingFile) {
  ParallelLogger::logInfo("Fetching inclusion statistics from files")
  subset <- subsetToRequiredCohorts(
    cohorts = cohortDefinitionSet %>%
      dplyr::filter(.data$cohortId %in% instantiatedCohorts),
    task = "runInclusionStatistics",
    incremental = incremental,
    recordKeepingFile = recordKeepingFile
  )

  if (incremental &&
    (length(instantiatedCohorts) - nrow(subset)) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Skipping %s cohorts in incremental mode.",
      length(instantiatedCohorts) - nrow(subset)
    ))
  }
  if (nrow(subset) > 0) {
    ParallelLogger::logInfo("Exporting inclusion rules with CohortGenerator")

    timeExecution(exportFolder,
      "getInclusionStatsCohortGenerator",
      parent = "getInclusionStats",
      expr = {
        insertInclusionRuleNames(
          connection = connection,
          cohortDefinitionSet = subset,
          cohortDatabaseSchema = cohortDatabaseSchema,
          cohortInclusionTable = cohortTableNames$cohortInclusionTable
        )

        stats <- getCohortStats(
          connection = connection,
          cohortTableNames = cohortTableNames,
          cohortDatabaseSchema = cohortDatabaseSchema
        )
      }
    )
    if (!is.null(stats)) {
      if ("cohortInclusionTable" %in% (names(stats))) {
        cohortInclusion <- makeDataExportable(
          x = stats$cohortInclusionTable,
          tableName = "cohort_inclusion",
          databaseId = databaseId,
          minCellCount = minCellCount
        )
        writeToCsv(
          data = cohortInclusion,
          fileName = file.path(exportFolder, "cohort_inclusion.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      }
      if ("cohortInclusionStatsTable" %in% (names(stats))) {
        cohortIncStats <- makeDataExportable(
          x = stats$cohortInclusionStatsTable,
          tableName = "cohort_inc_stats",
          databaseId = databaseId,
          minCellCount = minCellCount
        )
        writeToCsv(
          data = cohortIncStats,
          fileName = file.path(exportFolder, "cohort_inc_stats.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      }
      if ("cohortInclusionResultTable" %in% (names(stats))) {
        cohortIncResult <- makeDataExportable(
          x = stats$cohortInclusionResultTable,
          tableName = "cohort_inc_result",
          databaseId = databaseId,
          minCellCount = minCellCount
        )
        writeToCsv(
          data = cohortIncResult,
          fileName = file.path(exportFolder, "cohort_inc_result.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      }
      if ("cohortSummaryStatsTable" %in% (names(stats))) {
        cohortSummaryStats <- makeDataExportable(
          x = stats$cohortSummaryStatsTable,
          tableName = "cohort_summary_stats",
          databaseId = databaseId,
          minCellCount = minCellCount
        )
        writeToCsv(
          data = cohortSummaryStats,
          fileName = file.path(exportFolder, "cohort_summary_stats.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      }

      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runInclusionStatistics",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
    }
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
#' @param connection                  db connection
#' @param cohortDefinitionSet         cohort definition set
#' @template CohortDatabaseSchema
#'
#' @param cohortInclusionTable        Name of the inclusion table, one of the tables for storing
#'                                    inclusion rule statistics.
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
