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



#' Given a set of cohorts get relationships between the cohorts.
#'
#' @description
#' Given a set of cohorts, get temporal relationships between the
#' cohort_start_date of the cohorts.
#'
#' @template Connection
#'
#' @template CohortDatabaseSchema
#'
#' @template TempEmulationSchema
#'
#' @template CohortTable
#'
#' @param targetCohortIds              A vector of one or more Cohort Ids for use as target cohorts.
#'
#' @param comparatorCohortIds          A vector of one or more Cohort Ids for use as feature/comparator cohorts.
#'
#' @param relationshipDays             A dataframe with two columns startDay and endDay representing periods of time to compute relationship
#'
#'
#' @export
runCohortRelationshipDiagnostics <-
  function(connectionDetails = NULL,
           connection = NULL,
           cohortDatabaseSchema = NULL,
           tempEmulationSchema = NULL,
           cohortTable = "cohort",
           targetCohortIds,
           comparatorCohortIds,
           relationshipDays) {
    startTime <- Sys.time()

    # Assert checks
    errorMessage <- checkmate::makeAssertCollection()
    checkmate::assertDataFrame(relationshipDays, add = errorMessage)
    checkmate::assertNames(
      names(relationshipDays),
      must.include = c(
        "startDay",
        "endDay"
      ),
      add = errorMessage
    )
    checkmate::assertIntegerish(
      x = targetCohortIds,
      lower = 0,
      any.missing = FALSE,
      min.len = 1,
      unique = TRUE,
      add = errorMessage
    )
    checkmate::assertIntegerish(
      x = comparatorCohortIds,
      lower = 0,
      any.missing = FALSE,
      min.len = 1,
      unique = TRUE,
      add = errorMessage
    )
    checkmate::reportAssertions(collection = errorMessage)

    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }

    timePeriods <- relationshipDays %>%
      dplyr::distinct() %>%
      dplyr::arrange(.data$startDay, .data$endDay) %>%
      dplyr::mutate(timeId = dplyr::row_number())

    ParallelLogger::logTrace("   - Creating Andromeda object to collect results")

    ParallelLogger::logTrace(paste0("   - Working with ", scales::comma(nrow(timePeriods)), " time ids."))
    resultsInAndromeda <- Andromeda::andromeda()

    # looping over timePeriods
    # obviously if there are lot of timePeriods this may take for ever - as execution of each timePeriod
    # depends on the number of combis of targetCohortId * comparatorCohortId
    # in future version we could introduce a permanent table that stores the results of the cohortRelationship
    # and maybe retrieved - but this will need the use of startDay/endDay instead of timeId
    for (i in (1:nrow(timePeriods))) {
      ParallelLogger::logTrace(
        paste0(
          "       - Working on ",
          scales::comma(timePeriods[i, ]$startDay),
          " to ",
          scales::comma(timePeriods[i, ]$endDay),
          " days (",
          scales::comma(i),
          " of ",
          scales::comma(nrow(timePeriods)),
          ")"
        )
      )

      cohortRelationshipSql <-
        SqlRender::readSql(
          sourceFile = system.file(
            "sql",
            "sql_server",
            "CohortRelationship.sql",
            package = utils::packageName()
          )
        )
      
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        sql = cohortRelationshipSql,
        time_id = timePeriods[i, ]$timeId,
        start_day_offset = timePeriods[i, ]$startDay,
        end_day_offset = timePeriods[i, ]$endDay,
        target_cohort_ids = targetCohortIds,
        comparator_cohort_ids = comparatorCohortIds,
        cohort_database_schema = cohortDatabaseSchema,
        cohort_table = cohortTable
      )
      
      DatabaseConnector::renderTranslateQuerySqlToAndromeda(
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        sql = "SELECT * FROM #cohort_rel_output;",
        snakeCaseToCamelCase = TRUE,
        andromeda = resultsInAndromeda,
        andromedaTableName = "temp"
      )

      if (!"cohortRelationships" %in% names(resultsInAndromeda)) {
        resultsInAndromeda$cohortRelationships <- resultsInAndromeda$temp
      } else {
        Andromeda::appendToTable(
          resultsInAndromeda$cohortRelationships,
          resultsInAndromeda$temp
        )
      }
    }

    resultsInAndromeda$timePeriods <- timePeriods
    resultsInAndromeda$temp <- NULL

    resultsInAndromeda$cohortRelationships <-
      resultsInAndromeda$cohortRelationships %>%
      dplyr::inner_join(resultsInAndromeda$timePeriods, by = "timeId") %>%
      dplyr::select(-.data$timeId) %>%
      dplyr::arrange(
        .data$cohortId,
        .data$comparatorCohortId,
        .data$startDay,
        .data$endDay
      )
    resultsInAndromeda$timePeriods <- NULL

    unlink(
      x = file.path(
        "resumeTimeId",
        "timeIdResults.csv"
      ),
      force = TRUE
    )
    delta <- Sys.time() - startTime
    ParallelLogger::logTrace(paste(
      "   - Computing cohort relationship took",
      signif(delta, 3),
      attr(delta, "units")
    ))
    data <-
      resultsInAndromeda$cohortRelationships %>% dplyr::collect()
    return(data)
  }





executeCohortRelationshipDiagnostics <- function(connection,
                                                 databaseId,
                                                 exportFolder,
                                                 cohortDatabaseSchema,
                                                 cdmDatabaseSchema,
                                                 tempEmulationSchema,
                                                 cohortTable,
                                                 cohortDefinitionSet,
                                                 temporalCovariateSettings,
                                                 minCellCount,
                                                 recordKeepingFile,
                                                 incremental,
                                                 batchSize = getOption("CohortDiagnostics-Relationship-batch-size", default = 500)) {
  ParallelLogger::logInfo("Computing Cohort Relationship")
  startCohortRelationship <- Sys.time()

  allCohortIds <- cohortDefinitionSet %>%
    dplyr::select(.data$cohortId, .data$checksum) %>%
    dplyr::rename(targetCohortId = .data$cohortId,
                  targetChecksum = .data$checksum) %>%
    dplyr::distinct()
  combinationsOfPossibleCohortRelationships <- allCohortIds %>%
    tidyr::crossing(allCohortIds %>%
                      dplyr::rename(comparatorCohortId = .data$targetCohortId,
                                    comparatorChecksum = .data$targetChecksum)) %>%
    dplyr::filter(.data$targetCohortId != .data$comparatorCohortId) %>%
    dplyr::arrange(.data$targetCohortId, .data$comparatorCohortId) %>%
    dplyr::mutate(checksum = paste0(.data$targetChecksum, .data$comparatorChecksum))

  subset <- subsetToRequiredCombis(
    combis = combinationsOfPossibleCohortRelationships,
    task = "runCohortRelationship",
    incremental = incremental,
    recordKeepingFile = recordKeepingFile
  )

  if (nrow(subset) > 0) {
    if (incremental &&
        (nrow(cohortDefinitionSet) - (length(subset$targetCohortId %>% unique()))) > 0) {
      ParallelLogger::logInfo(
        sprintf(
          " - Skipping %s target cohorts in incremental mode because the relationships has already been computed with other cohorts.",
          nrow(cohortDefinitionSet) - (length(subset$targetCohortId %>% unique()))
        )
      )
    }

    if (incremental &&
        (nrow(combinationsOfPossibleCohortRelationships) - (
          nrow(
            combinationsOfPossibleCohortRelationships %>%
            dplyr::filter(.data$targetCohortId %in% c(subset$targetCohortId))
          )
        )) > 0) {
      ParallelLogger::logInfo(
        sprintf(
          " - Skipping %s combinations in incremental mode because these were previously computed.",
          nrow(combinationsOfPossibleCohortRelationships) - nrow(
            combinationsOfPossibleCohortRelationships %>%
              dplyr::filter(.data$targetCohortId %in% c(subset$targetCohortId))
          )
        )
      )
    }

    ParallelLogger::logTrace(" - Beginning Cohort Relationship SQL")
    if (all(exists("temporalCovariateSettings"), !is.null(temporalCovariateSettings))) {
      temporalStartDays <- temporalCovariateSettings$temporalStartDays
      temporalEndDays <- temporalCovariateSettings$temporalEndDays
    } else {
      temporalStartDays <- c(
        -365, -30,
        0,
        1,
        31, -9999, -365, -180, -30, -9999, -365, -180, -30, -9999,
        seq(
          from = -421,
          to = -31,
          by = 30
        ),
        seq(
          from = 0,
          to = 390,
          by = 30
        ),
        seq(
          from = -5,
          to = 5,
          by = 1
        )
      )
      temporalEndDays <- c(
        -31, -1,
        0,
        30,
        365,
        0,
        0,
        0,
        0, -1, -1, -1, -1,
        9999,
        seq(
          from = -391,
          to = -1,
          by = 30
        ),
        seq(
          from = 30,
          to = 420,
          by = 30
        ),
        seq(
          from = -5,
          to = 5,
          by = 1
        )
      )
    }

    outputFile <- file.path(exportFolder, "cohort_relationships.csv")
    if (!incremental & file.exists(outputFile)) {
      ParallelLogger::logInfo("Time series file exists, removing before batch operations")
      unlink(outputFile)
    }

    for (start in seq(1, nrow(subset), by = batchSize)) {
      end <- min(start + batchSize - 1, nrow(subset))

      if (nrow(subset) > batchSize) {
        ParallelLogger::logInfo(sprintf(
          "  - Batch cohort relationship. Processing cohorts %s through %s combinations of %s total combinations",
          start,
          end,
          nrow(subset)
        ))
      }


      timeExecution(
        exportFolder,
        "runCohortRelationshipDiagnostics",
        c(subset[start:end,]$targetCohortId %>% unique(), subset[start:end,]$comparatorCohortId %>% unique()),
        parent = "executeCohortRelationshipDiagnostics",
        expr = {
          output <-
            runCohortRelationshipDiagnostics(
              connection = connection,
              cohortDatabaseSchema = cohortDatabaseSchema,
              tempEmulationSchema = tempEmulationSchema,
              cohortTable = cohortTable,
              targetCohortIds = subset[start:end,]$targetCohortId %>% unique(),
              comparatorCohortIds = subset[start:end,]$comparatorCohortId %>% unique(),
              relationshipDays = dplyr::tibble(startDay = temporalStartDays,
                                               endDay = temporalEndDays)
            )
        }
      )

      data <- makeDataExportable(
        x = output,
        tableName = "cohort_relationships",
        minCellCount = minCellCount,
        databaseId = databaseId
      )

      writeToCsv(
        data = data,
        fileName = outputFile,
        incremental = TRUE
      )

      recordTasksDone(
        cohortId = subset[start:end,]$targetCohortId,
        comparatorId = subset[start:end,]$comparatorCohortId,
        targetChecksum = subset[start:end,]$targetChecksum,
        comparatorChecksum = subset[start:end,]$comparatorChecksum,
        task = "runCohortRelationship",
        checksum = subset[start:end,]$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
      deltaIteration <- Sys.time() - startCohortRelationship
      ParallelLogger::logInfo("    - Running Cohort Relationship iteration with batchsize ",
                              batchSize,
                              " from row number ",
                              start,
                              " to ",
                              end,
                              " took ",
                              signif(deltaIteration, 3),
                              " ",
                              attr(deltaIteration, "units"))
    }
  } else {
    ParallelLogger::logInfo("    - Skipping in incremental mode.")
  }
  delta <- Sys.time() - startCohortRelationship
  ParallelLogger::logInfo(
    " - Computing cohort relationships took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
}
