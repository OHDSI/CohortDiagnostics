# Copyright 2025 Observational Health Data Sciences and Informatics
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

extractConceptSetsSqlFromCohortSql <- function(cohortSql) {
  if (length(cohortSql) > 1) {
    stop("Please check if more than one cohort SQL was provided.")
  }
  sql <- gsub("with primary_events.*", "", cohortSql)

  if (is.null(sql) || length(nchar(sql)) == 0 || is.na(nchar(sql)) || is.nan(nchar(sql))) {
    return(tidyr::tibble())
  }
  # Find opening and closing parentheses:
  starts <- stringr::str_locate_all(sql, "\\(")[[1]][, 1]
  ends <- stringr::str_locate_all(sql, "\\)")[[1]][, 1]

  x <- rep(0, nchar(sql))
  x[starts] <- 1
  x[ends] <- -1
  level <- cumsum(x)
  level0 <- which(level == 0)

  subQueryLocations <-
    stringr::str_locate_all(sql, "SELECT [0-9]+ as codeset_id")[[1]]
  subQueryCount <- nrow(subQueryLocations)
  conceptsetSqls <- vector("character", subQueryCount)
  conceptSetIds <- vector("integer", subQueryCount)

  temp <- list()
  if (subQueryCount > 0) {
    for (i in 1:subQueryCount) {
      startForSubQuery <- min(starts[starts > subQueryLocations[i, 2]])
      endForSubQuery <- min(level0[level0 > startForSubQuery])
      subQuery <-
        paste(
          stringr::str_sub(sql, subQueryLocations[i, 1], endForSubQuery),
          "C"
        )
      conceptsetSqls[i] <- subQuery
      conceptSetIds[i] <- stringr::str_replace(
        subQuery,
        pattern = stringr::regex(
          pattern = "SELECT ([0-9]+) as codeset_id.*",
          ignore_case = TRUE,
          multiline = TRUE,
          dotall = TRUE
        ),
        replacement = "\\1"
      ) %>%
        utils::type.convert(as.is = TRUE)
      temp[[i]] <- tidyr::tibble(
        conceptSetId = conceptSetIds[i],
        conceptSetSql = conceptsetSqls[i]
      )
    }
  } else {
    temp <- tidyr::tibble()
  }
  return(dplyr::bind_rows(temp))
}


extractConceptSetsJsonFromCohortJson <- function(cohortJson) {
  cohortDefinition <- tryCatch(
    {
      jsonlite::fromJSON(cohortJson, simplifyDataFrame = FALSE)
    },
    error = function(msg) {
      return(list())
    }
  )
  if ("expression" %in% names(cohortDefinition)) {
    expression <- cohortDefinition$expression
  } else {
    expression <- cohortDefinition
  }
  conceptSetExpression <- list()
  if (length(expression$ConceptSets) > 0) {
    for (i in (1:length(expression$ConceptSets))) {
      jsonExpr <- expression$ConceptSets[[i]]$expression$items |>
        jsonlite::toJSON(digits = 23) |>
        as.character()
      conceptSetExpression[[i]] <-
        tidyr::tibble(
          conceptSetId = expression$ConceptSets[[i]]$id,
          conceptSetName = expression$ConceptSets[[i]]$name,
          conceptSetExpression = jsonExpr
        )
    }
  } else {
    conceptSetExpression <- tidyr::tibble()
  }
  return(dplyr::bind_rows(conceptSetExpression))
}

getParentCohort <- function(cohort, cohortDefinitionSet) {
  if (is.null(cohort$subsetParent) || cohort$cohortId == cohort$subsetParent) {
    return(cohort)
  }

  return(getParentCohort(
    cohortDefinitionSet %>% dplyr::filter(.data$cohortId == cohort$subsetParent),
    cohortDefinitionSet
  ))
}

combineConceptSetsFromCohorts <- function(cohorts) {
  # cohorts should be a dataframe with at least cohortId, sql and json

  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertDataFrame(
    x = cohorts,
    min.cols = 4,
    add = errorMessage
  )
  checkmate::assertNames(
    x = colnames(cohorts),
    must.include = c("cohortId", "sql", "json", "cohortName")
  )
  checkmate::reportAssertions(errorMessage)
  checkmate::assertDataFrame(
    x = cohorts %>% dplyr::select(
      "cohortId",
      "sql",
      "json",
      "cohortName"
    ),
    any.missing = FALSE,
    min.cols = 4,
    add = errorMessage
  )
  checkmate::reportAssertions(errorMessage)

  conceptSets <- list()
  conceptSetCounter <- 0

  for (i in (1:nrow(cohorts))) {
    cohort <- cohorts[i, ]

    if (isTRUE(cohort$isSubset)) {
      parent <- getParentCohort(cohort, cohorts)
      cohortSql <- parent$sql
      cohortJson <- parent$json
    } else {
      cohortSql <- cohort$sql
      cohortJson <- cohort$json
    }

    sqlCs <-
      extractConceptSetsSqlFromCohortSql(cohortSql = cohortSql)
    jsonCs <-
      extractConceptSetsJsonFromCohortJson(cohortJson = cohortJson)

    if (nrow(sqlCs) == 0 || nrow(jsonCs) == 0) {
      ParallelLogger::logInfo(
        "Cohort Definition expression does not have a concept set expression. ",
        "Skipping Cohort: ",
        cohort$cohortName
      )
    } else {
      if (!length(sqlCs$conceptSetId %>% unique()) == length(jsonCs$conceptSetId %>% unique())) {
        stop(
          "Mismatch in concept set IDs between SQL and JSON for cohort ",
          cohort$cohortFullName
        )
      }
      if (length(sqlCs) > 0 && length(jsonCs) > 0) {
        conceptSetCounter <- conceptSetCounter + 1
        conceptSets[[conceptSetCounter]] <-
          tidyr::tibble(
            cohortId = cohort$cohortId,
            dplyr::inner_join(x = sqlCs %>% dplyr::distinct(), y = jsonCs %>% dplyr::distinct(), by = "conceptSetId")
          )
      }
    }
  }
  if (length(conceptSets) == 0) {
    return(data.frame())
  }
  conceptSets <- dplyr::bind_rows(conceptSets) %>%
    dplyr::arrange(.data$cohortId, .data$conceptSetId)

  uniqueConceptSets <- conceptSets %>%
    dplyr::select("conceptSetExpression") %>%
    dplyr::mutate(uniqueConceptSetId = dplyr::row_number()) %>%
    dplyr::distinct()

  conceptSets <- conceptSets %>%
    dplyr::inner_join(uniqueConceptSets,
      by = "conceptSetExpression",
      relationship = "many-to-many"
    ) %>%
    dplyr::distinct() %>%
    dplyr::relocate(
      "uniqueConceptSetId",
      "cohortId",
      "conceptSetId"
    ) %>%
    dplyr::arrange(
      .data$uniqueConceptSetId,
      .data$cohortId,
      .data$conceptSetId
    )
  return(conceptSets)
}


mergeTempTables <-
  function(connection,
           tableName,
           tempTables,
           tempEmulationSchema) {
    valueString <-
      paste(tempTables, collapse = "\n\n  UNION ALL\n\n  SELECT *\n  FROM ")
    sql <-
      sprintf(
        "SELECT *\nINTO %s\nFROM (\n  SELECT *\n  FROM %s\n) tmp;",
        tableName,
        valueString
      )
    sql <-
      SqlRender::translate(sql,
        targetDialect = connection@dbms,
        tempEmulationSchema = tempEmulationSchema
      )
    DatabaseConnector::executeSql(connection,
      sql,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )

    # Drop temp tables:
    for (tempTable in tempTables) {
      sql <-
        sprintf("TRUNCATE TABLE %s;\nDROP TABLE %s;", tempTable, tempTable)
      sql <-
        SqlRender::translate(sql,
          targetDialect = connection@dbms,
          tempEmulationSchema = tempEmulationSchema
        )
      DatabaseConnector::executeSql(connection,
        sql,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
  }

instantiateUniqueConceptSets <- function(uniqueConceptSets,
                                         connection,
                                         vocabularyDatabaseSchema,
                                         tempEmulationSchema,
                                         conceptSetsTable = "#inst_concept_sets") {
  ParallelLogger::logInfo("Instantiating concept sets")

  if (nrow(uniqueConceptSets) > 0) {
    sql <- sapply(
      split(uniqueConceptSets, 1:nrow(uniqueConceptSets)),
      function(x) {
        sub(
          "SELECT [0-9]+ as codeset_id",
          sprintf("SELECT %s as codeset_id", x$uniqueConceptSetId),
          x$conceptSetSql
        )
      }
    )

    batchSize <- 100
    tempTables <- c()
    pb <- utils::txtProgressBar(style = 3)
    for (start in seq(1, length(sql), by = batchSize)) {
      utils::setTxtProgressBar(pb, start / length(sql))
      tempTable <-
        paste("#", paste(sample(letters, 20, replace = TRUE), collapse = ""), sep = "")
      tempTables <- c(tempTables, tempTable)
      end <- min(start + batchSize - 1, length(sql))
      sqlSubset <- sql[start:end]
      sqlSubset <- paste(sqlSubset, collapse = "\n\n  UNION ALL\n\n")
      sqlSubset <-
        sprintf(
          "SELECT *\nINTO %s\nFROM (\n %s\n) tmp;",
          tempTable,
          sqlSubset
        )
      sqlSubset <-
        SqlRender::render(sqlSubset, vocabulary_database_schema = vocabularyDatabaseSchema)
      sqlSubset <- SqlRender::translate(sqlSubset,
        targetDialect = connection@dbms,
        tempEmulationSchema = tempEmulationSchema
      )
      DatabaseConnector::executeSql(connection,
        sqlSubset,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
    utils::setTxtProgressBar(pb, 1)
    close(pb)

    mergeTempTables(
      connection = connection,
      tableName = conceptSetsTable,
      tempTables = tempTables,
      tempEmulationSchema = tempEmulationSchema
    )
  }
}

getCodeSetId <- function(criterion) {
  if (is.list(criterion)) {
    criterion$CodesetId
  } else if (is.vector(criterion)) {
    return(criterion["CodesetId"])
  } else {
    return(NULL)
  }
}

getCodeSetIds <- function(criterionList) {
  codeSetIds <- lapply(criterionList, getCodeSetId)
  codeSetIds <- do.call(c, codeSetIds)
  if (is.null(codeSetIds)) {
    return(NULL)
  } else {
    return(dplyr::tibble(domain = names(criterionList), codeSetIds = codeSetIds)
    %>% dplyr::filter(!is.na(.data$codeSetIds)))
  }
}

exportConceptSets <- function(cohortDefinitionSet, exportFolder, minCellCount, databaseId) {
  ParallelLogger::logInfo("Exporting cohort concept sets to csv")
  # We need to get concept sets from all cohorts in case subsets are present and
  # Added incrementally after cohort generation
  conceptSets <- combineConceptSetsFromCohorts(cohortDefinitionSet)

  if (!hasData(conceptSets)) {
    return(invisible(NULL))
  }

  conceptSets <- conceptSets %>%
    dplyr::select(-"uniqueConceptSetId") %>%
    dplyr::distinct()
  # Save concept set metadata ---------------------------------------
  conceptSetsExport <- makeDataExportable(
    x = conceptSets,
    tableName = "concept_sets",
    minCellCount = minCellCount,
    databaseId = databaseId
  )

  # Always write all concept sets for all cohorts as they are always needed
  writeToCsv(
    data = conceptSetsExport,
    fileName = file.path(exportFolder, "concept_sets.csv"),
    incremental = FALSE,
    cohortId = conceptSetsExport$cohortId
  )
}

runConceptSetDiagnostics <- function(connection,
                                     tempEmulationSchema,
                                     cdmDatabaseSchema,
                                     vocabularyDatabaseSchema = cdmDatabaseSchema,
                                     databaseId,
                                     cohorts,
                                     runIncludedSourceConcepts,
                                     runOrphanConcepts,
                                     runBreakdownIndexEvents,
                                     exportFolder,
                                     minCellCount,
                                     conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                     conceptCountsTable = "concept_counts",
                                     conceptCountsTableIsTemp = FALSE,
                                     cohortDatabaseSchema,
                                     cohortTable,
                                     useExternalConceptCountsTable = FALSE,
                                     incremental = FALSE,
                                     conceptIdTable = NULL,
                                     recordKeepingFile) {
  ParallelLogger::logInfo("Starting concept set diagnostics")
  startConceptSetDiagnostics <- Sys.time()
  subset <- dplyr::tibble()

  # We need to get concept sets from all cohorts in case subsets are present and
  # Added incrementally after cohort generation
  conceptSets <- combineConceptSetsFromCohorts(cohorts)
  if (is.null(conceptSets) || nrow(conceptSets) == 0) {
    ParallelLogger::logInfo(
      "Cohorts being diagnosed does not have concept ids. Skipping concept set diagnostics."
    )
    return(NULL)
  }


  if (runIncludedSourceConcepts) {
    subsetIncluded <- subsetToRequiredCohorts(
      cohorts = cohorts,
      task = "runIncludedSourceConcepts",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    subset <- dplyr::bind_rows(subset, subsetIncluded)
  }
  if (runBreakdownIndexEvents) {
    subsetBreakdown <- subsetToRequiredCohorts(
      cohorts = cohorts,
      task = "runBreakdownIndexEvents",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    subset <- dplyr::bind_rows(subset, subsetBreakdown)
  }

  if (runOrphanConcepts) {
    subsetOrphans <- subsetToRequiredCohorts(
      cohorts = cohorts,
      task = "runOrphanConcepts",
      incremental = incremental,
      recordKeepingFile = recordKeepingFile
    )
    subset <- dplyr::bind_rows(subset, subsetOrphans)
  }
  subset <- dplyr::distinct(subset)

  if (nrow(subset) == 0) {
    return(NULL)
  }

  conceptSets <- conceptSets %>% dplyr::filter(.data$cohortId %in% subset$cohortId)

  uniqueConceptSets <-
    conceptSets[!duplicated(conceptSets$uniqueConceptSetId), ] %>%
    dplyr::select(-"cohortId", -"conceptSetId")

  if (nrow(uniqueConceptSets) == 0) {
    ParallelLogger::logInfo("No concept sets found - skipping")
    return(NULL)
  }

  timeExecution(
    exportFolder,
    taskName = "instantiateUniqueConceptSets",
    cohortIds = NULL,
    parent = "runConceptSetDiagnostics",
    expr = {
      instantiateUniqueConceptSets(
        uniqueConceptSets = uniqueConceptSets,
        connection = connection,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        conceptSetsTable = "#inst_concept_sets"
      )
    }
  )

  if ((runIncludedSourceConcepts && nrow(subsetIncluded) > 0) ||
    (runOrphanConcepts && nrow(subsetOrphans) > 0)) {
    timeExecution(
      exportFolder,
      taskName = "createConceptCountsTable",
      cohortIds = NULL,
      parent = "runConceptSetDiagnostics",
      expr = {
        createConceptCountsTable(
          connection = connection,
          cdmDatabaseSchema = cdmDatabaseSchema,
          tempEmulationSchema = tempEmulationSchema,
          conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
          conceptCountsTable = conceptCountsTable,
          conceptCountsTableIsTemp = conceptCountsTableIsTemp
        )
      }
    )
  }
  if (runIncludedSourceConcepts) {
    timeExecution(
      exportFolder,
      taskName = "runIncludedSourceConcepts",
      cohortIds = NULL,
      parent = "runConceptSetDiagnostics",
      expr = {
        # Included concepts ------------------------------------------------------------------
        ParallelLogger::logInfo("Fetching included source concepts")
        # TODO: Disregard empty cohorts in tally:
        if (incremental && (nrow(cohorts) - nrow(subsetIncluded)) > 0) {
          ParallelLogger::logInfo(sprintf(
            "Skipping %s cohorts in incremental mode.",
            nrow(cohorts) - nrow(subsetIncluded)
          ))
        }
        if (nrow(subsetIncluded) > 0) {
          start <- Sys.time()
          if (useExternalConceptCountsTable) {
            stop("Use of external concept count table is not supported")
          } else {
            sql <- SqlRender::loadRenderTranslateSql(
              "CohortSourceCodes.sql",
              packageName = utils::packageName(),
              dbms = connection@dbms,
              tempEmulationSchema = tempEmulationSchema,
              cdm_database_schema = cdmDatabaseSchema,
              instantiated_concept_sets = "#inst_concept_sets",
              include_source_concept_table = "#inc_src_concepts",
              by_month = FALSE
            )
            DatabaseConnector::executeSql(connection = connection, sql = sql)
            counts <-
              DatabaseConnector::renderTranslateQuerySql(
                connection = connection,
                sql = "SELECT * FROM @include_source_concept_table;",
                include_source_concept_table = "#inc_src_concepts",
                tempEmulationSchema = tempEmulationSchema,
                snakeCaseToCamelCase = TRUE
              ) %>%
              tidyr::tibble()

            counts <- counts %>%
              dplyr::distinct() %>%
              dplyr::rename("uniqueConceptSetId" = "conceptSetId") %>%
              dplyr::inner_join(
                conceptSets %>% dplyr::select(
                  "uniqueConceptSetId",
                  "cohortId",
                  "conceptSetId"
                ) %>% dplyr::distinct(),
                by = "uniqueConceptSetId",
                relationship = "many-to-many"
              ) %>%
              dplyr::select(-"uniqueConceptSetId") %>%
              dplyr::mutate(databaseId = !!databaseId) %>%
              dplyr::relocate(
                "databaseId",
                "cohortId",
                "conceptSetId",
                "conceptId"
              ) %>%
              dplyr::distinct()

            counts <- counts %>%
              dplyr::group_by(
                .data$databaseId,
                .data$cohortId,
                .data$conceptSetId,
                .data$conceptId,
                .data$sourceConceptId
              ) %>%
              dplyr::summarise(
                conceptCount = max(.data$conceptCount),
                conceptSubjects = max(.data$conceptSubjects)
              ) %>%
              dplyr::ungroup()

            counts <- makeDataExportable(
              x = counts,
              tableName = "included_source_concept",
              minCellCount = minCellCount,
              databaseId = databaseId
            )

            writeToCsv(
              counts,
              file.path(exportFolder, "included_source_concept.csv"),
              incremental = incremental,
              cohortId = subsetIncluded$cohortId
            )

            recordTasksDone(
              cohortId = subsetIncluded$cohortId,
              task = "runIncludedSourceConcepts",
              checksum = subsetIncluded$checksum,
              recordKeepingFile = recordKeepingFile,
              incremental = incremental
            )

            if (!is.null(conceptIdTable)) {
              sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @include_source_concept_table;

                  INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT source_concept_id
                  FROM @include_source_concept_table;"
              DatabaseConnector::renderTranslateExecuteSql(
                connection = connection,
                sql = sql,
                tempEmulationSchema = tempEmulationSchema,
                concept_id_table = conceptIdTable,
                include_source_concept_table = "#inc_src_concepts",
                progressBar = FALSE,
                reportOverallTime = FALSE
              )
            }
            sql <-
              "TRUNCATE TABLE @include_source_concept_table;\nDROP TABLE @include_source_concept_table;"
            DatabaseConnector::renderTranslateExecuteSql(
              connection = connection,
              sql = sql,
              tempEmulationSchema = tempEmulationSchema,
              include_source_concept_table = "#inc_src_concepts",
              progressBar = FALSE,
              reportOverallTime = FALSE
            )

            delta <- Sys.time() - start
            ParallelLogger::logInfo(paste(
              "Finding source codes took",
              signif(delta, 3),
              attr(delta, "units")
            ))
          }
        }
      }
    )
  }

  if (runBreakdownIndexEvents) {
    # Index event breakdown --------------------------------------------------------------------------
    ParallelLogger::logInfo("Breaking down index events")
    if (incremental &&
      (nrow(cohorts) - nrow(subsetBreakdown)) > 0) {
      ParallelLogger::logInfo(sprintf(
        "Skipping %s cohorts in incremental mode.",
        nrow(cohorts) - nrow(subsetBreakdown)
      ))
    }
    if (nrow(subsetBreakdown) > 0) {
      start <- Sys.time()
      readr::local_edition(1)
      domains <-
        readr::read_csv(
          system.file("csv", "domains.csv", package = utils::packageName()),
          col_types = readr::cols(),
          guess_max = min(1e7)
        )

      getCohortIndexEventBreakdown <- function(cohort) {
        ParallelLogger::logInfo(
          "- Breaking down index events for cohort '",
          cohort$cohortName,
          "'"
        )

        timeExecution(
          exportFolder,
          taskName = "getBreakdownIndexEvents",
          cohortIds = cohort$cohortId,
          parent = "runConceptSetDiagnostics",
          expr = {
            if (isTRUE(cohort$isSubset)) {
              parent <- getParentCohort(cohort, cohorts)
              jsonDef <- parent$json
            } else {
              jsonDef <- cohort$json
            }

            cohortDefinition <-
              jsonlite::fromJSON(jsonDef, simplifyDataFrame = FALSE)

            primaryCodesetIds <-
              lapply(
                cohortDefinition$PrimaryCriteria$CriteriaList,
                getCodeSetIds
              )

            if (length(primaryCodesetIds)) {
              primaryCodesetIds <- dplyr::bind_rows(primaryCodesetIds)
            } else {
              primaryCodesetIds <- data.frame()
            }

            if (nrow(primaryCodesetIds) == 0) {
              warning(
                "No primary event criteria concept sets found for cohort id: ",
                cohort$cohortId
              )
              return(tidyr::tibble())
            }
            primaryCodesetIds <- primaryCodesetIds %>% dplyr::filter(.data$domain %in%
              c(domains$domain %>% unique()))
            if (nrow(primaryCodesetIds) == 0) {
              warning(
                "Primary event criteria concept sets found for cohort id: ",
                cohort$cohortId, " but,", "\nnone of the concept sets belong to the supported domains.",
                "\nThe supported domains are:\n", paste(domains$domain,
                  collapse = ", "
                )
              )
              return(tidyr::tibble())
            }
            primaryCodesetIds <- conceptSets %>%
              dplyr::filter(.data$cohortId %in% cohort$cohortId) %>%
              dplyr::select(
                codeSetIds = "conceptSetId",
                "uniqueConceptSetId"
              ) %>%
              dplyr::distinct() %>%
              dplyr::inner_join(primaryCodesetIds %>% dplyr::distinct(), by = "codeSetIds")

            pasteIds <- function(row) {
              return(dplyr::tibble(
                domain = row$domain[1],
                uniqueConceptSetId = paste(row$uniqueConceptSetId, collapse = ", ")
              ))
            }

            primaryCodesetIds <-
              lapply(
                split(primaryCodesetIds, primaryCodesetIds$domain),
                pasteIds
              )

            if (length(primaryCodesetIds) == 0) {
              primaryCodesetIds <- data.frame()
            } else {
              primaryCodesetIds <- dplyr::bind_rows(primaryCodesetIds)
            }

            getCounts <- function(row) {
              domain <- domains %>% dplyr::filter(.data$domain == row$domain)
              sql <-
                SqlRender::loadRenderTranslateSql(
                  "CohortEntryBreakdown.sql",
                  packageName = utils::packageName(),
                  dbms = connection@dbms,
                  tempEmulationSchema = tempEmulationSchema,
                  cdm_database_schema = cdmDatabaseSchema,
                  vocabulary_database_schema = vocabularyDatabaseSchema,
                  cohort_database_schema = cohortDatabaseSchema,
                  cohort_table = cohortTable,
                  cohort_id = cohort$cohortId,
                  domain_table = domain$domainTable,
                  domain_start_date = domain$domainStartDate,
                  domain_concept_id = domain$domainConceptId,
                  domain_source_concept_id = domain$domainSourceConceptId,
                  use_source_concept_id = !(is.na(domain$domainSourceConceptId) | is.null(domain$domainSourceConceptId)),
                  primary_codeset_ids = row$uniqueConceptSetId,
                  concept_set_table = "#inst_concept_sets",
                  store = TRUE,
                  store_table = "#breakdown"
                )

              DatabaseConnector::executeSql(
                connection = connection,
                sql = sql,
                progressBar = FALSE,
                reportOverallTime = FALSE
              )
              sql <- "SELECT * FROM @store_table;"
              counts <-
                DatabaseConnector::renderTranslateQuerySql(
                  connection = connection,
                  sql = sql,
                  tempEmulationSchema = tempEmulationSchema,
                  store_table = "#breakdown",
                  snakeCaseToCamelCase = TRUE
                ) %>%
                tidyr::tibble()
              if (!is.null(conceptIdTable)) {
                sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @store_table;"
                DatabaseConnector::renderTranslateExecuteSql(
                  connection = connection,
                  sql = sql,
                  tempEmulationSchema = tempEmulationSchema,
                  concept_id_table = conceptIdTable,
                  store_table = "#breakdown",
                  progressBar = FALSE,
                  reportOverallTime = FALSE
                )
              }
              sql <-
                "TRUNCATE TABLE @store_table;\nDROP TABLE @store_table;"
              DatabaseConnector::renderTranslateExecuteSql(
                connection = connection,
                sql = sql,
                tempEmulationSchema = tempEmulationSchema,
                store_table = "#breakdown",
                progressBar = FALSE,
                reportOverallTime = FALSE
              )
              return(counts)
            }


            if (nrow(primaryCodesetIds) > 0) {
              counts <-
                lapply(split(primaryCodesetIds, 1:nrow(primaryCodesetIds)), getCounts) %>%
                dplyr::bind_rows() %>%
                dplyr::arrange(.data$conceptCount)
            } else {
              counts <- data.frame()
            }


            if (nrow(counts) > 0) {
              counts$cohortId <- cohort$cohortId
            } else {
              ParallelLogger::logInfo(
                "Index event breakdown results were not returned for: ",
                cohort$cohortId
              )
              return(dplyr::tibble())
            }
            return(counts)
          }
        )
      }

      data <-
        lapply(
          split(subsetBreakdown, subsetBreakdown$cohortId),
          getCohortIndexEventBreakdown
        )
      data <- dplyr::bind_rows(data)
      if (nrow(data) > 0) {
        data <- data %>%
          dplyr::mutate(databaseId = !!databaseId)
        data <-
          enforceMinCellValue(data, "conceptCount", minCellCount)
        if ("subjectCount" %in% colnames(data)) {
          data <-
            enforceMinCellValue(data, "subjectCount", minCellCount)
        }
      }

      if (nrow(data) == 0 && ncol(data) == 0) {
        data <- dplyr::tibble(
          conceptId = numeric(),
          conceptCount = numeric(),
          subjectCount = numeric(),
          cohortId = numeric(),
          databaseId = character(),
          domainField = character(),
          domainTable = character()
        )
      }

      data <- makeDataExportable(
        x = data,
        tableName = "index_event_breakdown",
        minCellCount = minCellCount,
        databaseId = databaseId
      )

      writeToCsv(
        data = data,
        fileName = file.path(exportFolder, "index_event_breakdown.csv"),
        incremental = incremental,
        cohortId = subset$cohortId
      )
      recordTasksDone(
        cohortId = subset$cohortId,
        task = "runBreakdownIndexEvents",
        checksum = subset$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )
      delta <- Sys.time() - start
      ParallelLogger::logInfo(paste(
        "Breaking down index event took",
        signif(delta, 3),
        attr(delta, "units")
      ))
    }
  }

  if (runOrphanConcepts) {
    # Orphan concepts ---------------------------------------------------------
    ParallelLogger::logInfo("Finding orphan concepts")
    if (incremental && (nrow(cohorts) - nrow(subsetOrphans)) > 0) {
      ParallelLogger::logInfo(sprintf(
        "Skipping %s cohorts in incremental mode.",
        nrow(cohorts) - nrow(subsetOrphans)
      ))
    }
    if (nrow(subsetOrphans > 0)) {
      start <- Sys.time()

      if (!useExternalConceptCountsTable) {
        ParallelLogger::logTrace("Using internal concept count table.")
      } else {
        stop("Use of external concept count table is not supported")
      }

      # [OPTIMIZATION idea] can we modify the sql to do this for all uniqueConceptSetId in one query using group by?
      data <- list()
      for (i in (1:nrow(uniqueConceptSets))) {
        conceptSet <- uniqueConceptSets[i, ]
        ParallelLogger::logInfo(
          "- Finding orphan concepts for concept set '",
          conceptSet$conceptSetName,
          "'"
        )

        timeExecution(
          exportFolder,
          taskName = "orphanConcepts",
          parent = "runConceptSetDiagnostics",
          cohortIds = paste("concept_set-", conceptSet$conceptSetName),
          expr = {
            data[[i]] <- .findOrphanConcepts(
              connection = connection,
              cdmDatabaseSchema = cdmDatabaseSchema,
              tempEmulationSchema = tempEmulationSchema,
              useCodesetTable = TRUE,
              codesetId = conceptSet$uniqueConceptSetId,
              conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
              conceptCountsTable = conceptCountsTable,
              conceptCountsTableIsTemp = conceptCountsTableIsTemp,
              instantiatedCodeSets = "#inst_concept_sets",
              orphanConceptTable = "#orphan_concepts"
            )

            if (!is.null(conceptIdTable)) {
              sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @orphan_concept_table;"
              DatabaseConnector::renderTranslateExecuteSql(
                connection = connection,
                sql = sql,
                tempEmulationSchema = tempEmulationSchema,
                concept_id_table = conceptIdTable,
                orphan_concept_table = "#orphan_concepts",
                progressBar = FALSE,
                reportOverallTime = FALSE
              )
            }
          }
        )
        sql <-
          "TRUNCATE TABLE @orphan_concept_table;\nDROP TABLE @orphan_concept_table;"
        DatabaseConnector::renderTranslateExecuteSql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          orphan_concept_table = "#orphan_concepts",
          progressBar = FALSE,
          reportOverallTime = FALSE
        )
      }

      data <- dplyr::bind_rows(data) %>%
        dplyr::distinct() %>%
        dplyr::rename("uniqueConceptSetId" = "codesetId") %>%
        dplyr::inner_join(
          conceptSets %>%
            dplyr::select(
              "uniqueConceptSetId",
              "cohortId",
              "conceptSetId"
            ) %>% dplyr::distinct(),
          by = "uniqueConceptSetId",
          relationship = "many-to-many"
        ) %>%
        dplyr::select(-"uniqueConceptSetId") %>%
        dplyr::select(
          "cohortId",
          "conceptSetId",
          "conceptId",
          "conceptCount",
          "conceptSubjects"
        ) %>%
        dplyr::group_by(
          .data$cohortId,
          .data$conceptSetId,
          .data$conceptId
        ) %>%
        dplyr::summarise(
          conceptCount = max(.data$conceptCount),
          conceptSubjects = max(.data$conceptSubjects)
        ) %>%
        dplyr::ungroup()
      data <- makeDataExportable(
        x = data,
        tableName = "orphan_concept",
        minCellCount = minCellCount,
        databaseId = databaseId
      )

      writeToCsv(
        data,
        file.path(exportFolder, "orphan_concept.csv"),
        incremental = incremental,
        cohortId = subsetOrphans$cohortId
      )

      recordTasksDone(
        cohortId = subsetOrphans$cohortId,
        task = "runOrphanConcepts",
        checksum = subsetOrphans$checksum,
        recordKeepingFile = recordKeepingFile,
        incremental = incremental
      )

      delta <- Sys.time() - start

      timeExecution(
        exportFolder,
        taskName = "allOrphanConcepts",
        parent = "runConceptSetDiagnostics",
        start = start,
        execTime = delta
      )

      ParallelLogger::logInfo(
        "Finding orphan concepts took ",
        signif(delta, 3),
        " ",
        attr(delta, "units")
      )
    }
  }

  # put all instantiated concepts into #concept_ids table
  # this is extracted with vocabulary tables
  # this will have more codes than included source concepts
  # included source concepts is limited to resolved concept ids in source data
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "INSERT INTO #concept_ids (concept_id)
            SELECT DISTINCT concept_id
            FROM #inst_concept_sets;",
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )

  resolvedConceptIds <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM #inst_concept_sets;",
      tempEmulationSchema = tempEmulationSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
    dplyr::tibble() %>%
    dplyr::rename("uniqueConceptSetId" = "codesetId") %>%
    dplyr::inner_join(conceptSets %>% dplyr::distinct(),
      by = "uniqueConceptSetId",
      relationship = "many-to-many"
    ) %>%
    dplyr::select(
      "cohortId",
      "conceptSetId",
      "conceptId"
    ) %>%
    dplyr::distinct()

  resolvedConceptIds <- makeDataExportable(
    x = resolvedConceptIds,
    tableName = "resolved_concepts",
    minCellCount = minCellCount,
    databaseId = databaseId
  )

  writeToCsv(
    resolvedConceptIds,
    file.path(exportFolder, "resolved_concepts.csv"),
    incremental = TRUE
  )

  ParallelLogger::logTrace("Dropping temp concept set table")
  sql <-
    "TRUNCATE TABLE #inst_concept_sets; DROP TABLE #inst_concept_sets;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )

  if ((runIncludedSourceConcepts && nrow(subsetIncluded) > 0) ||
    (runOrphanConcepts && nrow(subsetOrphans) > 0)) {
    ParallelLogger::logTrace("Dropping temp concept count table")
    if (conceptCountsTableIsTemp) {
      countTable <- conceptCountsTable
    } else {
      countTable <-
        paste(conceptCountsDatabaseSchema, conceptCountsTable, sep = ".")
    }

    sql <- "TRUNCATE TABLE @count_table; DROP TABLE @count_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection,
      sql,
      tempEmulationSchema = tempEmulationSchema,
      count_table = countTable,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }

  delta <- Sys.time() - startConceptSetDiagnostics
  ParallelLogger::logInfo(
    "Running concept set diagnostics took ",
    signif(delta, 3),
    " ",
    attr(delta, "units")
  )
}
