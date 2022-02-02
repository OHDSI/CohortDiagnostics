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

computeCohortOverlap <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cohortDatabaseSchema,
                                 cohortTable = "cohort",
                                 targetCohortId,
                                 comparatorCohortId) {
  start <- Sys.time()
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (!checkIfCohortInstantiated(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = targetCohortId
  )) {
    warning(
      "- Target cohort with ID ",
      targetCohortId,
      " appears to be empty. Was it instantiated? Skipping overlap computation."
    )
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste(
      "Computing overlap took",
      signif(delta, 3),
      attr(delta, "units")
    ))
    return(tidyr::tibble())
  }
  
  if (!checkIfCohortInstantiated(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = comparatorCohortId
  )) {
    warning(
      "- Comparator cohort with ID ",
      comparatorCohortId,
      " appears to be empty. Was it instantiated? Skipping overlap computation."
    )
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste(
      "Computing overlap took",
      signif(delta, 3),
      attr(delta, "units")
    ))
    return(tidyr::tibble())
  }
  
  sql <- SqlRender::loadRenderTranslateSql(
    "CohortOverlap.sql",
    packageName = utils::packageName(),
    dbms = connection@dbms,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    target_cohort_id = targetCohortId,
    comparator_cohort_id = comparatorCohortId
  )
  overlap <- DatabaseConnector::querySql(
    connection = connection,
    sql = sql,
    snakeCaseToCamelCase = TRUE
  ) %>%
    tidyr::tibble()
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste(
    "Computing overlap took",
    signif(delta, 3),
    attr(delta, "units")
  ))
  return(overlap)
}

executeCohortComparisonDiagnostics <- function(connection,
                                               databaseId,
                                               exportFolder,
                                               cohortDatabaseSchema,
                                               cohortTable,
                                               cohorts,
                                               minCellCount,
                                               recordKeepingFile,
                                               incremental) {
  ParallelLogger::logInfo("Computing cohort overlap")
  startCohortOverlap <- Sys.time()

  combis <- cohorts %>%
    dplyr::select(.data$phenotypeId, .data$cohortId) %>%
    dplyr::distinct()

  combis <- combis %>%
    dplyr::rename(targetCohortId = .data$cohortId) %>%
    dplyr::inner_join(combis %>%
                        dplyr::rename(comparatorCohortId = .data$cohortId),
                      by = "phenotypeId") %>%
    dplyr::filter(.data$targetCohortId < .data$comparatorCohortId) %>%
    dplyr::select(.data$targetCohortId, .data$comparatorCohortId) %>%
    dplyr::distinct()

  if (incremental) {
    combis <- combis %>%
      dplyr::inner_join(
        dplyr::tibble(
          targetCohortId = cohorts$cohortId,
          targetChecksum = cohorts$checksum
        ),
        by = "targetCohortId"
      ) %>%
      dplyr::inner_join(
        dplyr::tibble(
          comparatorCohortId = cohorts$cohortId,
          comparatorChecksum = cohorts$checksum
        ),
        by = "comparatorCohortId"
      ) %>%
      dplyr::mutate(checksum = paste(.data$targetChecksum, .data$comparatorChecksum))
  }
  subset <- subsetToRequiredCombis(
    combis = combis,
    task = "runCohortOverlap",
    incremental = incremental,
    recordKeepingFile = recordKeepingFile
  )

  if (incremental && (nrow(combis) - nrow(subset)) > 0) {
    ParallelLogger::logInfo(sprintf(
      "Skipping %s cohort combinations in incremental mode.",
      nrow(combis) - nrow(subset)
    ))
  }
  if (nrow(subset) > 0) {

    runCohortOverlap <- function(row) {
      ParallelLogger::logInfo(
        "- Computing overlap for cohorts ",
        row$targetCohortId,
        " and ",
        row$comparatorCohortId
      )
      data <- computeCohortOverlap(
        connection = connection,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        targetCohortId = row$targetCohortId,
        comparatorCohortId = row$comparatorCohortId
      )
      if (nrow(data) > 0) {
        data <- data %>%
          dplyr::mutate(
            targetCohortId = row$targetCohortId,
            comparatorCohortId = row$comparatorCohortId
          )
      }
      return(data)
    }

    data <-
      lapply(split(subset, 1:nrow(subset)), runCohortOverlap)
    data <- dplyr::bind_rows(data)
    if (nrow(data) > 0) {
      revData <- data
      revData <-
        swapColumnContents(revData, "targetCohortId", "comparatorCohortId")
      revData <-
        swapColumnContents(revData, "tOnlySubjects", "cOnlySubjects")
      revData <-
        swapColumnContents(revData, "tBeforeCSubjects", "cBeforeTSubjects")
      revData <-
        swapColumnContents(revData, "tInCSubjects", "cInTSubjects")
      data <- dplyr::bind_rows(data, revData) %>%
        dplyr::mutate(databaseId = !!databaseId)
      data <-
        enforceMinCellValue(data, "eitherSubjects", minCellCount)
      data <-
        enforceMinCellValue(data, "bothSubjects", minCellCount)
      data <-
        enforceMinCellValue(data, "tOnlySubjects", minCellCount)
      data <-
        enforceMinCellValue(data, "cOnlySubjects", minCellCount)
      data <-
        enforceMinCellValue(data, "tBeforeCSubjects", minCellCount)
      data <-
        enforceMinCellValue(data, "cBeforeTSubjects", minCellCount)
      data <-
        enforceMinCellValue(data, "sameDaySubjects", minCellCount)
      data <-
        enforceMinCellValue(data, "tInCSubjects", minCellCount)
      data <-
        enforceMinCellValue(data, "cInTSubjects", minCellCount)

      data <- data %>%
        tidyr::replace_na(replace =
                            list(
                              eitherSubjects = 0,
                              bothSubjects = 0,
                              tOnlySubjects = 0,
                              cOnlySubjects = 0,
                              tBeforeCSubjects = 0,
                              cBeforeTSubjects = 0,
                              sameDaySubjects = 0,
                              tInCSubjects = 0,
                              cInTSubjects = 0
                            ))

      writeToCsv(
        data = data,
        fileName = file.path(exportFolder, "cohort_overlap.csv"),
        incremental = incremental,
        targetCohortId = subset$targetCohortId,
        comparatorCohortId = subset$comparatorCohortId
      )
    }
    recordTasksDone(
      cohortId = subset$targetCohortId,
      comparatorId = subset$comparatorCohortId,
      task = "runCohortOverlap",
      checksum = subset$checksum,
      recordKeepingFile = recordKeepingFile,
      incremental = incremental
    )
  }

  delta <- Sys.time() - startCohortOverlap
  ParallelLogger::logInfo("Running Cohort Overlap took ",
                          signif(delta, 3),
                          " ",
                          attr(delta, "units"))
}
