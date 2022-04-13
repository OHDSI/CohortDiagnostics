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


getInclusionStatisticsFromFiles <- function(cohortIds = NULL,
                                            folder,
                                            cohortInclusionFile = file.path(
                                              folder,
                                              "cohortInclusion.csv"
                                            ),
                                            cohortInclusionResultFile = file.path(
                                              folder,
                                              "cohortIncResult.csv"
                                            ),
                                            cohortInclusionStatsFile = file.path(
                                              folder,
                                              "cohortIncStats.csv"
                                            ),
                                            cohortSummaryStatsFile = file.path(
                                              folder,
                                              "cohortSummaryStats.csv"
                                            )) {
  start <- Sys.time()
  
  if (!file.exists(cohortInclusionFile)) {
    return(NULL)
  }
  
  fetchStats <- function(file) {
    ParallelLogger::logDebug("- Fetching data from ", file)
    stats <- readr::read_csv(file,
                             col_types = readr::cols(),
                             guess_max = min(1e7)
    )
    if (!is.null(cohortIds)) {
      stats <- stats %>%
        dplyr::filter(.data$cohortDefinitionId %in% cohortIds)
    }
    return(stats)
  }
  
  inclusion <- fetchStats(cohortInclusionFile)
  if ("description" %in% names(inclusion)) {
    inclusion$description <- as.character(inclusion$description)
    inclusion$description[is.na(inclusion$description)] <- ""
  } else {
    inclusion$description <- ""
  }
  
  summaryStats <- fetchStats(cohortSummaryStatsFile)
  inclusionStats <- fetchStats(cohortInclusionStatsFile)
  inclusionResults <- fetchStats(cohortInclusionResultFile)
  
  #create empty tibble to hold output of simplified cohort inclusion rules
  inclusionRuleStats <- dplyr::tibble(
    ruleSequenceId = as.integer(),
    ruleName = as.character(),
    meetSubjects = as.integer(),
    gainSubjects = as.integer(),
    totalSubjects = as.integer(),
    remainSubjects = as.integer(),
    cohortDefinitionId = as.integer()
  )
    
  for (cohortId in unique(inclusion$cohortDefinitionId)) {
    cohortResult <-
      processInclusionStats(
        inclusion = filter(inclusion, .data$cohortDefinitionId == cohortId),
        inclusionResults = filter(inclusionResults, .data$cohortDefinitionId == cohortId),
        inclusionStats = filter(inclusionStats, .data$cohortDefinitionId == cohortId)
      )
    if (!is.null(cohortResult)) {
      cohortResult$cohortDefinitionId <- cohortId
      inclusionRuleStats <-
        dplyr::bind_rows(inclusionRuleStats, cohortResult)
    }
  }
  delta <- Sys.time() - start
  writeLines(paste(
    "Fetching inclusion statistics took",
    signif(delta, 3),
    attr(delta, "units")
  ))
  inclusionRule <- list(inclusionRuleStats = inclusionRuleStats,
                        cohortInclusion = inclusion,
                        cohortIncStats = inclusionStats,
                        cohortIncResult = inclusionResults,
                        cohortSummaryStats = summaryStats)
  return(inclusionRule)
}

processInclusionStats <- function(inclusion,
                                  inclusionResults,
                                  inclusionStats) {
  if (!hasData(inclusion) || !hasData(inclusionStats)) {
    return(NULL)
  }
  
  result <- inclusion %>%
    dplyr::select(.data$ruleSequence, .data$name) %>%
    dplyr::distinct() %>%
    dplyr::inner_join(
      inclusionStats %>%
        dplyr::filter(.data$modeId == 0) %>%
        dplyr::select(
          .data$ruleSequence,
          .data$personCount,
          .data$gainCount,
          .data$personTotal
        ),
      by = "ruleSequence"
    ) %>%
    dplyr::mutate(remain = 0)
  
  inclusionResults <- inclusionResults %>%
    dplyr::filter(.data$modeId == 0)
  mask <- 0
  for (ruleId in 0:(nrow(result) - 1)) {
    mask <- bitwOr(mask, 2 ^ ruleId)
    idx <-
      bitwAnd(inclusionResults$inclusionRuleMask, mask) == mask
    result$remain[result$ruleSequence == ruleId] <-
      sum(inclusionResults$personCount[idx])
  }
  colnames(result) <- c(
    "ruleSequenceId",
    "ruleName",
    "meetSubjects",
    "gainSubjects",
    "totalSubjects",
    "remainSubjects"
  )
  return(result)
}

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
    CohortGenerator::insertInclusionRuleNames(
      connection = connection,
      cohortDefinitionSet = subset,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortInclusionTable = cohortTableNames$cohortInclusionTable
    )
    # This part will change in future version, with a patch to CohortGenerator that
    # supports the usage of exporting tables without writing to disk
    inclusionStatisticsFolder <-
      tempfile("CdCohortStatisticsFolder")
    on.exit(unlink(inclusionStatisticsFolder), add = TRUE)
    CohortGenerator::exportCohortStatsTables(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTableNames = cohortTableNames,
      cohortStatisticsFolder = inclusionStatisticsFolder,
      incremental = FALSE
    ) # Note use of FALSE to always generate stats here
    stats <-
      getInclusionStatisticsFromFiles(
        cohortIds = subset$cohortId,
        folder = inclusionStatisticsFolder
      )
    if (!is.null(stats)) {
      if ("inclusionRuleStats" %in% (names(stats))) {
        inclusionRuleStats <- makeDataExportable(
          x = stats$inclusionRuleStats,
          tableName = "inclusion_rule_stats",
          databaseId = databaseId,
          minCellCount = minCellCount
        )
        writeToCsv(
          data = inclusionRuleStats,
          fileName = file.path(exportFolder, "inclusion_rule_stats.csv"),
          incremental = incremental,
          cohortId = subset$cohortId
        )
      }
      if ("cohortInclusion" %in% (names(stats))) {
        cohortInclusion <- makeDataExportable(
          x = stats$cohortInclusion,
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
      if ("cohortIncStats" %in% (names(stats))) {
        cohortIncStats <- makeDataExportable(
          x = stats$cohortIncStats,
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
      if ("cohortIncResult" %in% (names(stats))) {
        cohortIncResult <- makeDataExportable(
          x = stats$cohortIncResult,
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
      if ("cohortSummaryStats" %in% (names(stats))) {
        cohortSummaryStats <- makeDataExportable(
          x = stats$cohortSummaryStats,
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
