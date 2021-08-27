getSubjectCountsByDatabasae <-
  function(data, cohortId, databaseIds) {
    data %>%
      dplyr::left_join(cohortCount, by = c('databaseId', 'cohortId')) %>%
      dplyr::filter(.data$cohortId == cohortId) %>%
      dplyr::filter(.data$databaseId %in% databaseIds) %>%
      dplyr::arrange(.data$databaseId) %>%
      dplyr::mutate(cohortSubjects = dplyr::coalesce(.data$cohortSubjects, 0)) %>%
      dplyr::mutate(databaseIdsWithCount = paste0(
        .data$databaseId,
        "<br>(n = ",
        scales::comma(.data$cohortSubjects, accuracy = 1),
        ")"
      )) %>%
      dplyr::mutate(databaseIdsWithCountWithoutBr = paste0(
        .data$databaseId,
        " (n = ",
        scales::comma(.data$cohortSubjects, accuracy = 1),
        ")"
      )) %>%
      dplyr::select(
        .data$databaseId,
        .data$databaseIdsWithCount,
        .data$databaseIdsWithCountWithoutBr
      ) %>%
      dplyr::distinct() %>%
      dplyr::arrange(.data$databaseId)
  }

#Load results table ----
# used by global.R to load data into R memory
loadResultsTable <- function(tableName, resultsTablesOnServer, required = FALSE) {
  writeLines(text = paste0(" - Loading data from ", tableName))
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
    } else {
      if (required) {
        stop(paste0("Required table '", tableName, "' has 0 records."))
      } else {
        warning(paste0("Optional table '", tableName, "' has 0 records."))
      }
    }
  }
}

# isEmpty ----
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
patternReplacement <-
  function(x,
           patterns,
           replacements = patterns,
           fill = NA,
           ...)
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


downloadCsv <- function(x, fileName) {
  if (all(!is.null(x), nrow(x) > 0)) {
    write.csv(x, fileName)
  }
}

# where is the vocabulary tables ----
getSourcesOfVocabularyTables <- function(dataSource,
                                         database) {
  if (is(dataSource, "environment")) {
    sourceOfVocabularyTables <-
      c(database$databaseIdWithVocabularyVersion)
  } else {
    sourceOfVocabularyTables <- list(
      'From source data' = database$databaseIdWithVocabularyVersion,
      'From external reference' = vocabularyDatabaseSchemas
    )
  }
  return(sourceOfVocabularyTables)
}


sumCounts <- function(counts) {
  result <- sum(abs(counts))
  if (any(counts < 0)) {
    return(-result)
  } else {
    return(result)
  }
}



consolidationOfSelectedFieldValues <- function(input,
                                               cohort = NULL,
                                               conceptSets = NULL,
                                               conceptSetExpressionLeft = NULL,
                                               conceptSetExpressionRight = NULL,
                                               database = NULL,
                                               resolvedConceptSetDataLeft = NULL,
                                               resolvedConceptSetDataRight = NULL,
                                               orphanConceptSetDataLeft = NULL,
                                               orphanConceptSetDataRight = NULL,
                                               excludedConceptSetDataLeft = NULL,
                                               excludedConceptSetDataRight = NULL) {
  data <- list()
  ##########################Cohort Definition tab ##########################
  if (input$tabs == 'cohortDefinition') {
    data$leftSideActive <- FALSE
    data$rightSideActive <- FALSE
    #selection of cohort
    if (doesObjectHaveData(input$cohortDefinitionTable_rows_selected)) {
      if (length(input$cohortDefinitionTable_rows_selected) > 1) {
        # get the last two rows selected - this is only for cohort table to enable LEFT/RIGHT comparison
        lastRowsSelected <-
          input$cohortDefinitionTable_rows_selected[c(
            length(input$cohortDefinitionTable_rows_selected),
            length(input$cohortDefinitionTable_rows_selected) - 1
          )]
        data$cohortIdLeft <-
          cohort[lastRowsSelected[[1]], ]$cohortId
        data$cohortIdRight <-
          cohort[lastRowsSelected[[2]], ]$cohortId
      } else {
        lastRowsSelected <- input$cohortDefinitionTable_rows_selected
        data$cohortIdLeft <-
          cohort[lastRowsSelected[[1]], ]$cohortId
        data$cohortIdRight <- NULL
      }
    }
    
    #selection on concept set id
    if (all(
      doesObjectHaveData(input$conceptSetsInCohortLeft_rows_selected),
      doesObjectHaveData(data$cohortIdLeft)
    )) {
      selectedConceptSet <-
        conceptSetExpressionLeft[input$conceptSetsInCohortLeft_rows_selected,]
      data$conceptSetIdLeft <- selectedConceptSet$conceptSetId
      
      if (all(
        doesObjectHaveData(input$conceptSetsInCohortRight_rows_selected),
        doesObjectHaveData(data$cohortIdRight)
      )) {
        selectedConceptSet <-
          conceptSetExpressionRight[input$conceptSetsInCohortLeft_rows_selected,]
        data$conceptSetIdRight <- selectedConceptSet$conceptSetId
      }
    }
    #selection on database id
    if (doesObjectHaveData(input$choiceForConceptSetDetailsLeft)) {
      data$selectedDatabaseIdLeft <- database %>%
        dplyr::filter(
          .data$databaseIdWithVocabularyVersion == input$choiceForConceptSetDetailsLeft
        ) %>% 
        dplyr::pull(.data$databaseId)
    }
    if (doesObjectHaveData(input$choiceForConceptSetDetailsRight)) {
      data$selectedDatabaseIdRight <- database %>%
        dplyr::filter(
          .data$databaseIdWithVocabularyVersion == input$choiceForConceptSetDetailsRight
        ) %>% 
        dplyr::pull(.data$databaseId)
    }
    #selection on concept id
    if (doesObjectHaveData(input$cohortDefinitionResolvedConceptTableLeft_rows_selected)) {
      data$selectedConceptIdLeft <- resolvedConceptSetDataLeft[input$cohortDefinitionResolvedConceptTableLeft_rows_selected,]$conceptId
      data$leftSideActive <- TRUE
    }
    if (doesObjectHaveData(input$cohortDefinitionResolvedConceptTableRight_rows_selected)) {
      data$selectedConceptIdRight <- resolvedConceptSetDataRight[input$cohortDefinitionResolvedConceptTableRight_rows_selected,]$conceptId
      data$rightSideActive <- TRUE
    }
    if (doesObjectHaveData(input$cohortDefinitionExcludedConceptTableLeft_rows_selected)) {
      data$selectedConceptIdLeft <- excludedConceptSetDataLeft[input$cohortDefinitionExcludedConceptTableLeft_rows_selected,]$conceptId
      data$leftSideActive <- TRUE
    }
    if (doesObjectHaveData(input$cohortDefinitionExcludedConceptTableRight_rows_selected)) {
      data$selectedConceptIdRight <- excludedConceptSetDataRight[input$cohortDefinitionExcludedConceptTableRight_rows_selected,]$conceptId
      data$rightSideActive <- TRUE
    }
    if (doesObjectHaveData(input$cohortDefinitionOrphanConceptTableLeft_rows_selected)) {
      data$selectedConceptIdLeft <- orphanConceptSetDataLeft[input$cohortDefinitionOrphanConceptTableLeft_rows_selected,]$conceptId
      data$leftSideActive <- TRUE
    }
    if (doesObjectHaveData(input$cohortDefinitionOrphanConceptTableRight_rows_selected)) {
      data$selectedConceptIdRight <- orphanConceptSetDataRight[input$cohortDefinitionOrphanConceptTableRight_rows_selected,]$conceptId
      data$rightSideActive <- TRUE
    }
  }
  
  ####################################################
  if (input$tabs == 'indexEventBreakdown') {
    data <- list()
    #single select cohortId
    if (all(!is.null(input$selectedCompoundCohortName),
            !is.null(cohort))) {
      data$cohortIdLeft <- cohort %>%
        dplyr::filter(.data$compoundName %in% input$selectedCompoundCohortName) %>%
        dplyr::arrange(.data$cohortId) %>%
        dplyr::pull(.data$cohortId) %>%
        unique()
    }
    #mutli select databaseId
    if (doesObjectHaveData(input$database)) {
      data$selectedDatabaseIdLeft <- input$database
    }
    #mutli select concept set id for one cohort
    if (doesObjectHaveData(input$conceptSetsSelectedFromOneCohort)) {
      browser()
      data$conceptSetIdLeft <- input$conceptSetsSelectedFromOneCohort
    }
  }
  ####################################################
  if (input$tabs == 'cohortCharacterization') {
    data <- list()
  }
  ####################################################
  if (input$tabs == 'temporalCharacterization') {
    data <- list()
  }
  ####################################################
  if (input$tabs == 'compareCohortCharacterization') {
    data <- list()
  }
  ####################################################
  if (input$tabs == 'compareTemporalCharacterization') {
    data <- list()
  }
  return(data)
}