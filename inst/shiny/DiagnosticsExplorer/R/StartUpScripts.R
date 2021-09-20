
# used to get the short name in plots
addShortName <- function(data, shortNameRef = NULL, cohortIdColumn = "cohortId", shortNameColumn = "shortName") {
  if (is.null(shortNameRef)) {
    shortNameRef <- data %>%
      dplyr::distinct(.data$cohortId) %>%
      dplyr::arrange(.data$cohortId) %>%
      dplyr::mutate(shortName = paste0("C", dplyr::row_number()))
  } 
  
  shortNameRef <- shortNameRef %>%
    dplyr::distinct(.data$cohortId, .data$shortName) 
  colnames(shortNameRef) <- c(cohortIdColumn, shortNameColumn)
  data <- data %>%
    dplyr::inner_join(shortNameRef, by = cohortIdColumn)
  return(data)
}


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
                                               conceptSetExpressionTarget = NULL,
                                               conceptSetExpressionComparator = NULL,
                                               database = NULL,
                                               resolvedConceptSetDataTarget = NULL,
                                               resolvedConceptSetDataComparator = NULL,
                                               orphanConceptSetDataTarget = NULL,
                                               orphanConceptSetDataComparator = NULL,
                                               excludedConceptSetDataTarget = NULL,
                                               excludedConceptSetDataComparator = NULL,
                                               indexEventBreakdownDataTable = NULL) {
  data <- list()
  ##########################Cohort Definition tab ##########################
  if (input$tabs == 'cohortDefinition') {
    #selection of cohort
    if (doesObjectHaveData(input$cohortDefinitionTable_rows_selected)) {
      if (length(input$cohortDefinitionTable_rows_selected) > 1) {
        # get the last two rows selected - this is only for cohort table to enable LEFT/RIGHT comparison
        lastRowsSelected <-
          input$cohortDefinitionTable_rows_selected[c(
            length(input$cohortDefinitionTable_rows_selected),
            length(input$cohortDefinitionTable_rows_selected) - 1
          )]
        data$cohortIdTarget <-
          cohort[lastRowsSelected[[1]], ]$cohortId
        data$cohortIdComparator <-
          cohort[lastRowsSelected[[2]], ]$cohortId
      } else {
        lastRowsSelected <- input$cohortDefinitionTable_rows_selected
        data$cohortIdTarget <-
          cohort[lastRowsSelected[[1]], ]$cohortId
        data$cohortIdComparator <- NULL
      }
    }
    
    #selection on concept set id
    if (all(
      doesObjectHaveData(input$targetCohortDefinitionConceptSetsTable_rows_selected),
      doesObjectHaveData(data$cohortIdTarget)
    )) {
      selectedConceptSet <-
        conceptSetExpressionTarget[input$targetCohortDefinitionConceptSetsTable_rows_selected,]
      data$conceptSetIdTarget <- selectedConceptSet$conceptSetId
      
      if (all(
        doesObjectHaveData(input$comparatorCohortDefinitionConceptSets_rows_selected),
        doesObjectHaveData(data$cohortIdComparator)
      )) {
        selectedConceptSet <-
          conceptSetExpressionComparator[input$comparatorCohortDefinitionConceptSets_rows_selected,]
        data$conceptSetIdComparator <- selectedConceptSet$conceptSetId
      }
    }
    #selection on database id
    if (doesObjectHaveData(input$selectedDatabaseIds)) {
        data$selectedDatabaseIdTarget <- input$selectedDatabaseIds
    }
    #selection on concept id
    if (doesObjectHaveData(input$targetCohortDefinitionResolvedConceptTable_rows_selected)) {
      data$selectedConceptIdTarget <- resolvedConceptSetDataTarget[input$targetCohortDefinitionResolvedConceptTable_rows_selected,]$conceptId
      data$leftSideActive <- TRUE
    }
    if (doesObjectHaveData(input$comparatorCohortDefinitionResolvedConceptTable_rows_selected)) {
      data$selectedConceptIdComparator <- resolvedConceptSetDataComparator[input$comparatorCohortDefinitionResolvedConceptTable_rows_selected,]$conceptId
      data$rightSideActive <- TRUE
    }
    if (doesObjectHaveData(input$targetCohortDefinitionExcludedConceptTable_rows_selected)) {
      data$selectedConceptIdTarget <- excludedConceptSetDataTarget[input$targetCohortDefinitionExcludedConceptTable_rows_selected,]$conceptId
      data$leftSideActive <- TRUE
    }
    if (doesObjectHaveData(input$comparatorCohortDefinitionExcludedConceptTable_rows_selected)) {
      data$selectedConceptIdComparator <- excludedConceptSetDataComparator[input$comparatorCohortDefinitionExcludedConceptTable_rows_selected,]$conceptId
      data$rightSideActive <- TRUE
    }
    if (doesObjectHaveData(input$targetCohortDefinitionOrphanConceptTable_rows_selected)) {
      data$selectedConceptIdTarget <- orphanConceptSetDataTarget[input$targetCohortDefinitionOrphanConceptTable_rows_selected,]$conceptId
      data$leftSideActive <- TRUE
    }
    if (doesObjectHaveData(input$comparatorCohortDefinitionOrphanConceptTable_rows_selected)) {
      data$selectedConceptIdComparator <- orphanConceptSetDataComparator[input$comparatorCohortDefinitionOrphanConceptTable_rows_selected,]$conceptId
      data$rightSideActive <- TRUE
    }
  }
  ####################################################
  if (input$tabs == 'indexEventBreakdown' || 
      input$tabs == 'visitContext' ||
      input$tabs == 'cohortCharacterization' ||
      input$tabs == 'temporalCharacterization' ||
      input$tabs == 'compareCohortCharacterization' ||
      input$tabs == 'compareTemporalCharacterization') {
    data <- list()
    
    #single select cohortId
    if (all(!is.null(input$selectedCompoundCohortName),
            !is.null(cohort))) {
      data$cohortIdTarget <- cohort %>%
        dplyr::filter(.data$compoundName %in% input$selectedCompoundCohortName) %>%
        dplyr::arrange(.data$cohortId) %>%
        dplyr::pull(.data$cohortId) %>%
        unique()
    }
    #mutli select databaseId/ single select databaseId
    if (input$tabs == 'temporalCharacterization' ||
        input$tabs == 'compareCohortCharacterization' ||
        input$tabs == 'compareTemporalCharacterization') {
      
      if (doesObjectHaveData(input$selectedDatabaseId)) {
        data$selectedDatabaseIdTarget <- input$selectedDatabaseId
      } else {
        data$selectedDatabaseIdTarget <- NULL
      }
    } else {
      if (doesObjectHaveData(input$selectedDatabaseIds)) {
        data$selectedDatabaseIdTarget <- input$selectedDatabaseIds
      }
    }
   
    #mutli select concept set id for one cohort
    if (doesObjectHaveData(input$conceptSetsSelectedCohortLeft)) {
      data$conceptSetIdTarget <- conceptSets %>% 
        dplyr::filter(.data$cohortId %in% data$cohortIdTarget) %>% 
        dplyr::filter(.data$conceptSetName %in% input$conceptSetsSelectedCohortLeft) %>% 
        dplyr::pull(.data$conceptSetId)
    }
    if (all(doesObjectHaveData(indexEventBreakdownDataTable),
            doesObjectHaveData(input$indexEventBreakdownTable_rows_selected))) {
      lastRowsSelected <- input$indexEventBreakdownTable_rows_selected[length(input$indexEventBreakdownTable_rows_selected)]
      data$selectedConceptIdTarget <- indexEventBreakdownDataTable[lastRowsSelected, ]$conceptId
      data$leftSideActive <- TRUE
    }
  }
  
  ####################################################
  if (input$tabs == 'cohortCounts' ||
      input$tabs == 'incidenceRate' ||
      input$tabs == 'timeSeries' ||
      input$tabs == 'timeDistribution' ||
      input$tabs == 'cohortOverlap') {
    data <- list()
    #multi select cohortId
    if (doesObjectHaveData(input$selectedCompoundCohortNames)) {
        data$cohortIdTarget <- cohort %>%
          dplyr::filter(.data$compoundName %in% input$selectedCompoundCohortNames) %>%
          dplyr::arrange(.data$cohortId) %>%
          dplyr::pull(.data$cohortId) %>%
          unique()
    }
    
    #mutli select databaseId
    if (doesObjectHaveData(input$selectedDatabaseIds)) {
      data$selectedDatabaseIdTarget <- input$selectedDatabaseIds
    }
    
  }
  return(data)
}

#takes data as input and returns data table with sketch design
#used in resolved, excluded and orphan concepts in cohort definition tab
getSketchDesignForTablesInCohortDefinitionTab <- function(data, 
                                                          databaseCount,
                                                          numberOfColums = 4) {
  colnamesInData <- colnames(data)
  colnamesInData <- colnamesInData[!colnamesInData %in% "databaseId"]
  colnamesInData <- colnamesInData[!colnamesInData %in% "persons"]
  colnamesInData <- colnamesInData[!colnamesInData %in% "records"]
  databaseIds <- sort(unique(data$databaseId))
  
  fieldsInData <- c()
  maxCount <- NULL
  maxSubject <- NULL
  if (all('records' %in% colnames(data),
          'records' %in% colnames(databaseCount))) {
    fieldsInData <- c(fieldsInData, "records")
    maxCount <- max(data$records, na.rm = TRUE)
  }
  if (all('persons' %in% colnames(data),
          'persons' %in% colnames(databaseCount))) {
    fieldsInData <- c(fieldsInData, "persons")
    maxSubject <- max(data$persons, na.rm = TRUE)
  }
  databasePersonAndRecordCount <- data %>%
    dplyr::select(.data$databaseId) %>% 
    dplyr::inner_join(databaseCount,
                      by = c("databaseId")) %>% 
    dplyr::select(.data$databaseId, dplyr::all_of(fieldsInData)) %>% 
    dplyr::distinct()
  if ('persons' %in% colnames(databasePersonAndRecordCount)) {
    databasePersonAndRecordCount <- databasePersonAndRecordCount %>% 
      dplyr::mutate(persons = scales::comma(.data$persons,
                                                   accuracy = 1))
  }
  if ('records' %in% colnames(databasePersonAndRecordCount)) {
    databasePersonAndRecordCount <- databasePersonAndRecordCount %>% 
      dplyr::mutate(records = scales::comma(.data$records,
                                                  accuracy = 1))
  }
  databasePersonAndRecordCount <- databasePersonAndRecordCount %>% 
    dplyr::arrange(.data$databaseId)
  
  dataTransformed <- data %>%
    dplyr::arrange(.data$databaseId) %>% 
    tidyr::pivot_longer(
      names_to = "type",
      cols = dplyr::all_of(fieldsInData),
      values_to = "count"
    ) %>%
    dplyr::mutate(type = paste0(.data$type,
                                " ",
                                .data$databaseId)) %>%
    dplyr::distinct() %>% 
    dplyr::arrange(.data$databaseId, dplyr::desc(.data$type)) %>% #descending to ensure records before persons
    tidyr::pivot_wider(
      id_cols = dplyr::all_of(colnamesInData),
      names_from = type,
      values_from = count,
      values_fill = 0
    )
  # sort descending by first count field
  dataTransformed <- dataTransformed %>% 
    dplyr::arrange(dplyr::desc(abs(dplyr::across(dplyr::contains("records")))))
  
  options = list(
    pageLength = 1000,
    lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
    searching = TRUE,
    lengthChange = TRUE,
    ordering = TRUE,
    paging = TRUE,
    info = TRUE,
    searchHighlight = TRUE,
    scrollX = TRUE,
    scrollY = "20vh",
    columnDefs = list(truncateStringDef(1, 50),
                      minCellCountDef(numberOfColums + (1:(
                        2 * length(databaseIds)
                      ))))
  )
  databaseRecordAndPersonColumnName <- c()
  for (i in 1:nrow(databasePersonAndRecordCount)) {
    if ('records' %in% colnames(databasePersonAndRecordCount)) {
      databaseRecordAndPersonColumnName <-
        c(
          databaseRecordAndPersonColumnName,
          paste0("Records (", databasePersonAndRecordCount[i,]$records, ")"))
    }
    if ('persons' %in% colnames(databasePersonAndRecordCount)) {
      databaseRecordAndPersonColumnName <-
        c(
          databaseRecordAndPersonColumnName,
          paste0("Persons (", databasePersonAndRecordCount[i,]$persons, ")"))
    }
  }
  #!!!!!!! dynamically generate rowspan from colnamesInData
  sketch <- htmltools::withTags(table(class = "display",
                                      thead(
                                        tr(
                                          lapply(camelCaseToTitleCase(colnamesInData),
                                                 th,
                                                 rowspan = 2),
                                          lapply(
                                            databaseIds,
                                            th,
                                            colspan = 2,
                                            class = "dt-center",
                                            style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                          )
                                        ),
                                        tr(
                                          lapply(databaseRecordAndPersonColumnName, th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                        )
                                      )))
  
  dataTable <- DT::datatable(
    dataTransformed,
    options = options,
    rownames = FALSE,
    container = sketch,
    colnames = colnames(dataTransformed) %>% camelCaseToTitleCase(),
    escape = FALSE,
    selection = 'single',
    filter = "top",
    class = "stripe nowrap compact"
  )
  
  if (!is.null(maxSubject)) {
    dataTable <- DT::formatStyle(
      table = dataTable,
      columns =  (numberOfColums + 1) + 1:(length(databaseIds) * 2),
      background = DT::styleColorBar(c(0, maxSubject), "lightblue"),
      backgroundSize = "98% 88%",
      backgroundRepeat = "no-repeat",
      backgroundPosition = "center"
    )
  }
  if (!is.null(maxCount)) {
    dataTable <- DT::formatStyle(
      table = dataTable,
      columns =  (numberOfColums + 1) + 1:(length(databaseIds) * 2),
      background = DT::styleColorBar(c(0, maxCount), "lightblue"),
      backgroundSize = "98% 88%",
      backgroundRepeat = "no-repeat",
      backgroundPosition = "center"
    )
  }
  return(dataTable)
}

getStlModelOutputForTsibbleDataValueFields <- function(tsibbleData, valueFields = "value") {
  if (!doesObjectHaveData(tsibbleData)) {
    return(NULL)
  }
  if (!"tbl_ts" %in% class(tsibbleData)) {
    warning("object is not of tsibble class")
    return(NULL)
  }
  keys <- colnames(attributes(tsibbleData)$key %>%
                     dplyr::select(-".rows"))
  index <- attributes(tsibbleData)$index %>%
    as.character()
  if (!all(valueFields %in% colnames(tsibbleData))) {
    warning(paste0("Cannot find: " , paste0(setdiff(valueFields, colnames(tsibbleData)), collapse = ",")))
    valueFields <- valueFields[!valueFields %in% setdiff(valueFields, colnames(tsibbleData))]
  }
  modelData <- list()
  for (i in (1:length(valueFields))) {
    valueField <- valueFields[[i]]
    modelData[[valueField]] <- tsibbleData %>% 
      dplyr::select(dplyr::all_of(keys), 
                    dplyr::all_of(index), 
                    dplyr::all_of(valueField)) %>%
      dplyr::rename(Total = dplyr::all_of(valueField)) %>% 
      tsibble::fill_gaps(Total = 0) %>% 
      fabletools::model(feasts::STL(Total ~  trend(window = 36))) %>%
      fabletools::components()
    if (tsibble::is_yearmonth(modelData[[valueField]]$periodBegin)) {
      modelData[[valueField]] <- modelData[[valueField]] %>% 
        dplyr::mutate(periodDate = as.Date(.data$periodBegin))
    } else if (is.double(modelData[[valueField]]$periodBegin) || is.integer(modelData[[valueField]]$periodBegin)) {
      modelData[[valueField]] <- modelData[[valueField]] %>% 
        dplyr::mutate(periodDate = as.Date(paste0(.data$periodBegin, "-01-01")))
    }
  }
  return(modelData)
}

getDatabaseOrCohortCountForConceptIds <- function(data, dataSource, databaseCount = TRUE) {
  conceptMetadata <- getConceptMetadata(
    dataSource = dataSource,
    cohortId = data$cohortId %>% unique(),
    databaseId = data$databaseId %>% unique(),
    conceptId = data$conceptId %>% unique(),
    getIndexEventCount = TRUE,
    getConceptCount = TRUE,
    getConceptRelationship = FALSE,
    getConceptAncestor = FALSE,
    getConceptSynonym = FALSE,
    getDatabaseMetadata = TRUE,
    getConceptCooccurrence = FALSE,
    getConceptMappingCount = FALSE,
    getFixedTimeSeries = FALSE,
    getRelativeTimeSeries = FALSE
  )
  if (!databaseCount) {
    if (!is.null(conceptMetadata$indexEventBreakdown)) {
      if (all(
        !is.null(conceptMetadata$indexEventBreakdown),
        nrow(conceptMetadata$indexEventBreakdown) > 0
      )) {
        data <- data %>%
          dplyr::left_join(
            conceptMetadata$indexEventBreakdown %>%
              dplyr::filter(.data$domainTable == "All") %>%
              dplyr::select(
                .data$conceptId,
                .data$databaseId,
                .data$conceptCount,
                .data$subjectCount
              ) %>%
              dplyr::rename(
                "records" = .data$conceptCount,
                "persons" = .data$subjectCount
              ),
            by = c("conceptId", "databaseId")
          )
      }
    }
  } else {
    if (!is.null(conceptMetadata$databaseConceptCount)) {
      data <- data %>%
        dplyr::left_join(conceptMetadata$databaseConceptCount,
                         by = c("conceptId", "databaseId")) %>%
        dplyr::rename(
          "records" = .data$conceptCount,
          "persons" = .data$subjectCount
        )
    }
  }
  if (!is.null(conceptMetadata$concept)) {
    data <- data %>%
      dplyr::inner_join(
        conceptMetadata$concept %>%
          dplyr::select(
            .data$conceptId,
            .data$conceptName,
            .data$vocabularyId,
            .data$domainId,
            .data$standardConcept
          ),
        by = c("conceptId")
      ) %>% 
      dplyr::relocate(.data$conceptId,
                      .data$conceptName,
                      .data$vocabularyId,
                      .data$domainId,
                      .data$standardConcept) %>% 
      dplyr::rename("standard" = .data$standardConcept)
  }
  if (!is.null(conceptMetadata$databaseCount)) {
    attr(x = data, which = "databaseCount") <-
      conceptMetadata$databaseCount
  }
  if ('records' %in% colnames(data)) {
    data <- data %>%
      dplyr::arrange(dplyr::desc(abs(.data$records)))
  } else {
    data$records <- as.integer(NA)
  }
  if (!'persons' %in% colnames(data)) {
    data$persons <- as.integer(NA)
  }
  data <- data %>%
    dplyr::mutate(dplyr::across(.cols = dplyr::contains("ount"),
                                .fns = ~ tidyr::replace_na(.x, 0))) %>% 
    dplyr::distinct()
  return(data)
}


getMaxValueForStringMatchedColumnsInDataFrame <- function(data, string) {
  data %>% 
    dplyr::summarise(dplyr::across(dplyr::contains(string), ~ max(.x))) %>% 
    tidyr::pivot_longer(values_to = "value", cols = dplyr::everything()) %>% 
    dplyr::pull() %>% 
    max(na.rm = TRUE)
}
