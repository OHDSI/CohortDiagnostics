# Copyright 2024 Observational Health Data Sciences and Informatics
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
      RJSONIO::fromJSON(content = cohortJson, digits = 23)
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
      conceptSetExpression[[i]] <-
        tidyr::tibble(
          conceptSetId = expression$ConceptSets[[i]]$id,
          conceptSetName = expression$ConceptSets[[i]]$name,
          conceptSetExpression = expression$ConceptSets[[i]]$expression$items %>% RJSONIO::toJSON(digits = 23)
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
      extractConceptSetsSqlFromCohortSql(cohortSql = cohort$sql)
    jsonCs <-
      extractConceptSetsJsonFromCohortJson(cohortJson = cohort$json)
    
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
  exportDataToCsv(
    data = conceptSets,
    tableName = "concept_sets",
    fileName = file.path(exportFolder, "concept_sets.csv"),
    minCellCount = minCellCount,
    databaseId = databaseId,
    incremental = FALSE,
    cohortId = conceptSetsExport$cohortId
  )
}
