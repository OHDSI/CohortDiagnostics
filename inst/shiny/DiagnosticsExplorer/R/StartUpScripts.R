getSubjectCountsByDatabasae <- function(data, cohortId, databaseIds) {
  data %>% 
    dplyr::left_join(cohortCount, by = c('databaseId', 'cohortId')) %>% 
    dplyr::filter(.data$cohortId == cohortId) %>% 
    dplyr::filter(.data$databaseId %in% databaseIds) %>% 
    dplyr::arrange(.data$databaseId) %>% 
    dplyr::mutate(cohortSubjects = dplyr::coalesce(.data$cohortSubjects, 0)) %>% 
    dplyr::mutate(databaseIdsWithCount = paste0(.data$databaseId, 
                                                "<br>(n = ",
                                                scales::comma(.data$cohortSubjects, accuracy = 1),
                                                ")"
    )) %>% 
    dplyr::mutate(databaseIdsWithCountWithoutBr = paste0(.data$databaseId, 
                                                         " (n = ",
                                                         scales::comma(.data$cohortSubjects, accuracy = 1),
                                                         ")"
    )) %>% 
    dplyr::select(.data$databaseId, .data$databaseIdsWithCount, .data$databaseIdsWithCountWithoutBr) %>% 
    dplyr::distinct() %>% 
    dplyr::arrange(.data$databaseId)
}


loadResultsTable <- function(tableName, required = FALSE) {
  if (required || tableName %in% resultsTablesOnServer) {
    tryCatch({
      table <- DatabaseConnector::dbReadTable(connectionPool,
                                              paste(resultsDatabaseSchema, tableName, sep = "."))
    }, error = function(err) {
      stop(
        "Error reading from ",
        paste(resultsDatabaseSchema, tableName, sep = "."),
        ": ",
        err$message
      )
    })
    colnames(table) <-
      SqlRender::snakeCaseToCamelCase(colnames(table))
    if (nrow(table) > 0) {
      assign(
        SqlRender::snakeCaseToCamelCase(tableName),
        dplyr::as_tibble(table),
        envir = .GlobalEnv
      )
    }
  }
}


# Create empty objects in memory for all other tables. This is used by the Shiny app to decide what tabs to show:
isEmpty <- function(tableName) {
  sql <-
    sprintf("SELECT 1 FROM %s.%s LIMIT 1;",
            resultsDatabaseSchema,
            tableName)
  oneRow <- DatabaseConnector::dbGetQuery(connectionPool, sql)
  return(nrow(oneRow) == 0)
}



# borrowed from https://stackoverflow.com/questions/19747384/create-new-column-in-dataframe-based-on-partial-string-matching-other-column
patternReplacement <- function(x, patterns, replacements = patterns, fill = NA, ...)
{
  stopifnot(length(patterns) == length(replacements))
  
  ans = rep_len(as.character(fill), length(x))    
  empty = seq_along(x)
  
  for (i in seq_along(patterns)) {
    greps = grepl(patterns[[i]], x[empty], ...)
    ans[empty[greps]] = replacements[[i]]  
    empty = empty[!greps]
  }
  return(ans)
}


getConceptSetDataFrameFromConceptSetExpression <-
  function(conceptSetExpression) {
    if ("items" %in% names(conceptSetExpression)) {
      items <- conceptSetExpression$items
    } else {
      items <- conceptSetExpression
    }
    conceptSetExpressionDetails <- items %>%
      purrr::map_df(.f = purrr::flatten)
    if ('CONCEPT_ID' %in% colnames(conceptSetExpressionDetails)) {
      if ('isExcluded' %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(IS_EXCLUDED = .data$isExcluded)
      }
      if ('includeDescendants' %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(INCLUDE_DESCENDANTS = .data$includeDescendants)
      }
      if ('includeMapped' %in% colnames(conceptSetExpressionDetails)) {
        conceptSetExpressionDetails <- conceptSetExpressionDetails %>%
          dplyr::rename(INCLUDE_MAPPED = .data$includeMapped)
      }
      colnames(conceptSetExpressionDetails) <-
        snakeCaseToCamelCase(colnames(conceptSetExpressionDetails))
    }
    return(conceptSetExpressionDetails)
  }

getConceptSetDetailsFromCohortDefinition <-
  function(cohortDefinitionExpression) {
    if ("expression" %in% names(cohortDefinitionExpression)) {
      expression <- cohortDefinitionExpression$expression
    }
    else {
      expression <- cohortDefinitionExpression
    }
    
    if (is.null(expression$ConceptSets)) {
      return(NULL)
    }
    
    conceptSetExpression <- expression$ConceptSets %>%
      dplyr::bind_rows() %>%
      dplyr::mutate(json = RJSONIO::toJSON(x = .data$expression,
                                           pretty = TRUE))
    
    conceptSetExpressionDetails <- list()
    i <- 0
    for (id in conceptSetExpression$id) {
      i <- i + 1
      conceptSetExpressionDetails[[i]] <-
        getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression =
                                                         conceptSetExpression[i, ]$expression$items) %>%
        dplyr::mutate(id = conceptSetExpression[i,]$id) %>%
        dplyr::relocate(.data$id) %>%
        dplyr::arrange(.data$id)
    }
    conceptSetExpressionDetails <-
      dplyr::bind_rows(conceptSetExpressionDetails)
    output <- list(conceptSetExpression = conceptSetExpression,
                   conceptSetExpressionDetails = conceptSetExpressionDetails)
    return(output)
  }


getFormattedFileName <- function(fileName) {
  date <- stringr::str_replace_all(Sys.Date(),pattern = "-", replacement = "")
  time <- stringr::str_split(string = Sys.time(), pattern = " ", n = 2)[[1]][2]
  timeArray <- stringr::str_split(string = time, pattern = ":", n = 3)
  return(paste(fileName, "_",  date, "_", timeArray[[1]][1], timeArray[[1]][2], ".csv", sep = ""))
}


downloadCsv <- function(x, fileName) {
  if (all(!is.null(x), nrow(x) > 0)) {
    write.csv(x, fileName)
  }
}