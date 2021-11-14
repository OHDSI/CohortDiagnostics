
# used to get the short name in plots
# addShortName <- function(data, shortNameRef = NULL, cohortIdColumn = "cohortId", shortNameColumn = "shortName") {
#   if (is.null(shortNameRef)) {
#     shortNameRef <- data %>%
#       dplyr::distinct(.data$cohortId) %>%
#       dplyr::arrange(.data$cohortId) %>%
#       dplyr::mutate(shortName = paste0("C", dplyr::row_number()))
#   } 
#   
#   shortNameRef <- shortNameRef %>%
#     dplyr::distinct(.data$cohortId, .data$shortName) 
#   colnames(shortNameRef) <- c(cohortIdColumn, shortNameColumn)
#   data <- data %>%
#     dplyr::inner_join(shortNameRef, by = cohortIdColumn)
#   return(data)
# }
# 
# addDatabaseShortName <- function(data, shortNameRef = NULL, databaseIdColumn = "databaseId", shortNameColumn = "databaseShortName") {
#   if (is.null(shortNameRef)) {
#     shortNameRef <- data %>%
#       dplyr::distinct(.data$databaseId) %>%
#       dplyr::arrange(.data$databaseId) %>%
#       dplyr::mutate(databaseShortName = paste0("C", dplyr::row_number()))
#   } 
#   
#   shortNameRef <- shortNameRef %>%
#     dplyr::distinct(.data$databaseId, .data$shortName) 
#   colnames(shortNameRef) <- c(databaseIdColumn, shortNameColumn)
#   data <- data %>%
#     dplyr::inner_join(shortNameRef, by = databaseIdColumn)
#   return(data)
# }


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
                                               indexEventBreakdownDataTable = NULL,
                                               mappedConceptSetTarget = NULL,
                                               mappedConceptSetComparator = NULL) {
  data <- list()
  ##########################Cohort Definition tab ##########################
  if (input$tabs == 'cohortDefinition') {
    #selection of cohort
    if (hasData(input$selectedCompoundCohortName) ||
        hasData(input$selectedComparatorCompoundCohortName)) {
      # get the last two rows selected - this is only for cohort table to enable LEFT/RIGHT comparison
      if (input$selectedCompoundCohortName != "") {
        data$cohortIdTarget <- cohort %>% 
          dplyr::filter(.data$compoundName == input$selectedCompoundCohortName) %>% 
          dplyr::pull(.data$cohortId)
      }
      if (hasData(input$selectedComparatorCompoundCohortName)) {
        data$cohortIdComparator <- cohort %>% 
          dplyr::filter(.data$compoundName == input$selectedComparatorCompoundCohortName) %>% 
          dplyr::pull(.data$cohortId)
      }
    }
    
    #selection on concept set id
    if (all(
      hasData(input$targetCohortDefinitionConceptSetsTable_rows_selected),
      hasData(data$cohortIdTarget)
    )) {
      selectedConceptSet <-
        conceptSetExpressionTarget[input$targetCohortDefinitionConceptSetsTable_rows_selected,]
      data$conceptSetIdTarget <- selectedConceptSet$conceptSetId
      
      if (all(
        hasData(input$comparatorCohortDefinitionConceptSets_rows_selected),
        hasData(data$cohortIdComparator)
      )) {
        selectedConceptSet <-
          conceptSetExpressionComparator[input$comparatorCohortDefinitionConceptSets_rows_selected,]
        data$conceptSetIdComparator <- selectedConceptSet$conceptSetId
      }
    }
    #selection on database id
    if (hasData(input$selectedDatabaseIds)) {
        data$selectedDatabaseIdTarget <-   database %>% 
          dplyr::filter(.data$compoundName == input$selectedDatabaseIds) %>% 
          dplyr::pull(.data$databaseId) 
    }
    #selection on concept id

    if (hasData(input$targetCohortDefinitionResolvedConceptTable_rows_selected)) {
      data$selectedConceptIdTarget <- resolvedConceptSetDataTarget[input$targetCohortDefinitionResolvedConceptTable_rows_selected,]$conceptId
      data$TargetActive <- TRUE
    }
    if (hasData(input$comparatorCohortDefinitionResolvedConceptTable_rows_selected)) {
      data$selectedConceptIdComparator <- resolvedConceptSetDataComparator[input$comparatorCohortDefinitionResolvedConceptTable_rows_selected,]$conceptId
      data$comparatorActive <- TRUE
    }
    if (hasData(input$targetCohortDefinitionExcludedConceptTable_rows_selected)) {
      data$selectedConceptIdTarget <- excludedConceptSetDataTarget[input$targetCohortDefinitionExcludedConceptTable_rows_selected,]$conceptId
      data$TargetActive <- TRUE
    }
    if (hasData(input$comparatorCohortDefinitionExcludedConceptTable_rows_selected)) {
      data$selectedConceptIdComparator <- excludedConceptSetDataComparator[input$comparatorCohortDefinitionExcludedConceptTable_rows_selected,]$conceptId
      data$comparatorActive <- TRUE
    }
    if (hasData(input$targetCohortDefinitionOrphanConceptTable_rows_selected)) {
      data$selectedConceptIdTarget <- orphanConceptSetDataTarget[input$targetCohortDefinitionOrphanConceptTable_rows_selected,]$conceptId
      data$TargetActive <- TRUE
    }
    if (hasData(input$comparatorCohortDefinitionOrphanConceptTable_rows_selected)) {
      data$selectedConceptIdComparator <- orphanConceptSetDataComparator[input$comparatorCohortDefinitionOrphanConceptTable_rows_selected,]$conceptId
      data$comparatorActive <- TRUE
    }
    
    if (hasData(input$targetCohortDefinitionMappedConceptTable_rows_selected)) {
      data$selectedConceptIdTarget <- mappedConceptSetTarget[input$targetCohortDefinitionMappedConceptTable_rows_selected,]$conceptId
      data$TargetActive <- TRUE
    }
    
    if (hasData(input$comparatorCohortDefinitionMappedConceptTable_rows_selected)) {
      data$selectedConceptIdComparator <- mappedConceptSetComparator[input$comparatorCohortDefinitionMappedConceptTable_rows_selected,]$conceptId
      data$comparatorActive <- TRUE
    }
  }
  ####################################################
  if (input$tabs == 'indexEventBreakdown' || 
      input$tabs == 'visitContext' ||
      input$tabs == 'cohortCharacterization' ||
      input$tabs == 'temporalCharacterization' ||
      input$tabs == 'compareCohortCharacterization' ||
      input$tabs == 'compareTemporalCharacterization' ||
      input$tabs == 'databaseInformation') {
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
    
    if (input$tabs == 'cohortCharacterization') {
      if (all(!is.null(input$selectedComparatorCompoundCohortNames),
              !is.null(cohort))) {
        data$cohortIdComparator <- cohort %>%
          dplyr::filter(.data$compoundName %in% input$selectedComparatorCompoundCohortNames) %>%
          dplyr::arrange(.data$cohortId) %>%
          dplyr::pull(.data$cohortId) %>%
          unique()
      }
    } else {
      if (all(!is.null(input$selectedComparatorCompoundCohortName),
              !is.null(cohort))) {
        data$cohortIdComparator <- cohort %>%
          dplyr::filter(.data$compoundName %in% input$selectedComparatorCompoundCohortName) %>%
          dplyr::arrange(.data$cohortId) %>%
          dplyr::pull(.data$cohortId) %>%
          unique()
      }
    }
    
    #mutli select databaseId/ single select databaseId
    if (input$tabs == 'temporalCharacterization' ||
        input$tabs == 'compareTemporalCharacterization' ||
        input$tabs == 'databaseInformation') {
      
      if (hasData(input$selectedDatabaseId)) {
        data$selectedDatabaseIdTarget <- database %>% 
          dplyr::filter(.data$compoundName == input$selectedDatabaseId) %>% 
          dplyr::pull(.data$databaseId)  
      } else {
        data$selectedDatabaseIdTarget <- NULL
      }
    } else {
      if (hasData(input$selectedDatabaseIds)) {
        data$selectedDatabaseIdTarget <- database %>% 
          dplyr::filter(.data$compoundName == input$selectedDatabaseIds) %>% 
          dplyr::pull(.data$databaseId)
      }
    }
   
    #mutli select concept set id for one cohort
    if (hasData(input$conceptSetsSelectedTargetCohort)) {
      data$conceptSetIdTarget <- conceptSets %>% 
        dplyr::filter(.data$cohortId %in% data$cohortIdTarget) %>% 
        dplyr::filter(.data$compoundName %in% input$conceptSetsSelectedTargetCohort) %>% 
        dplyr::pull(.data$conceptSetId)
    }
    
    if (all(hasData(indexEventBreakdownDataTable),
            hasData(input$indexEventBreakdownTable_rows_selected))) {
      lastRowsSelected <- input$indexEventBreakdownTable_rows_selected[length(input$indexEventBreakdownTable_rows_selected)]
      data$selectedConceptIdTarget <- indexEventBreakdownDataTable[lastRowsSelected, ]$conceptId
      data$TargetActive <- TRUE
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
    if (hasData(input$selectedCompoundCohortNames)) {
      if(input$tabs != 'cohortOverlap') {
        data$cohortIdTarget <- cohort %>%
          dplyr::filter(.data$compoundName %in% input$selectedCompoundCohortNames) %>%
          dplyr::arrange(.data$cohortId) %>%
          dplyr::pull(.data$cohortId) %>%
          unique()
      }
    }
    #Single select cohortId only for cohort overlap
    if (hasData(input$selectedCompoundCohortName)) {
     if (input$tabs == 'cohortOverlap') {
        data$cohortIdTarget <- cohort %>%
          dplyr::filter(.data$compoundName == input$selectedCompoundCohortName) %>%
          dplyr::arrange(.data$cohortId) %>%
          dplyr::pull(.data$cohortId) %>%
          unique()
      }
    }
    if (hasData(input$selectedComparatorCompoundCohortNames) && input$tabs == 'cohortOverlap') {
      data$cohortIdComparator <- cohort %>%
        dplyr::filter(.data$compoundName %in% input$selectedComparatorCompoundCohortNames) %>%
        dplyr::arrange(.data$cohortId) %>%
        dplyr::pull(.data$cohortId) %>%
        unique()
    }
    
    #mutli select databaseId
    if (hasData(input$selectedDatabaseIds)) {
      data$selectedDatabaseIdTarget <- database %>% 
        dplyr::filter(.data$compoundName == input$selectedDatabaseIds) %>% 
        dplyr::pull(.data$databaseId) 
    }
    
  }
  return(data)
}

#takes data as input and returns data table with sketch design
#used in resolved, excluded and orphan concepts in cohort definition tab
getDtWithColumnsGroupedByDatabaseId <- function(data, 
                                     headerCount = NULL,
                                     keyColumns,
                                     dataColumns,
                                     countLocation,
                                     maxCount,
                                     sort = TRUE,
                                     showResultsAsPercent = FALSE,
                                     rowSpan = 2) {

  # ensure the data has required fields
  keyColumns <- keyColumns %>% unique()
  dataColumns <- dataColumns %>% unique()
  missingColumns <-
    setdiff(x = c(keyColumns, dataColumns) %>% unique(),
            y = colnames(data))
  if (length(missingColumns) > 0)  {
    stop(
      paste0(
        "Improper specification for sketch, following fields are missing in data ",
        paste0(missingColumns, collapse = ", ")
      )
    )
  }
  data <- data %>% 
    dplyr::select(c(dplyr::all_of(keyColumns),"databaseId", dplyr::all_of(dataColumns)) %>% unique())
  
  
  #get all unique databsaeIds - and sort data by it
  uniqueDatabases <- data %>%
    dplyr::select(.data$databaseId) %>%
    dplyr::distinct() %>%
    dplyr::arrange(.data$databaseId)
  #long form
  data <- data %>% 
    tidyr::pivot_longer(cols = dplyr::all_of(dataColumns), 
                        names_to = "type", 
                        values_to = "valuesData")
  if (hasData(headerCount)) {
    if (countLocation == 1) {
      if (length(setdiff(c("databaseId", "count"), colnames(headerCount))) != 0) {
        warning("missing required fields to draw formatted datatable.")
      }
      data <- data %>%
        dplyr::inner_join(headerCount,
                          by = c("databaseId")) %>%
        dplyr::mutate(databaseId = paste0(.data$databaseId,
                                          " (",
                                          scales::comma(.data$count),
                                          ")")) %>%
        dplyr::mutate(dataColumnsLevel2 = .data$type) 
      if (showResultsAsPercent) {
        if (!is.null(maxCount)) {
          maxCount <- suppressWarnings(ceiling(maxCount / (max(data$count))))
        }
        data <- data %>%
          dplyr::mutate(valuesData = round(x = .data$valuesData / .data$count, digits = 4))
        
      }
      data <- data %>%
        dplyr::mutate(valuesData = round(x = .data$valuesData, digits = 2)) %>%
        dplyr::select(-.data$count)
      
      databaseIdHeaders <- uniqueDatabases %>%
        dplyr::inner_join(headerCount,
                          by = "databaseId") %>%
        dplyr::mutate(newDatabaseId = paste0(.data$databaseId,
                                             " (",
                                             scales::comma(.data$count),
                                             ")")) %>%
        dplyr::select(.data$databaseId, .data$newDatabaseId, .data$count) %>%
        dplyr::rename(headerCount = .data$count) %>%
        dplyr::distinct() %>%
        dplyr::select(-.data$headerCount) %>%
        dplyr::arrange(.data$databaseId)
      
      # headers for sketch
      databaseIdsForUseAsHeader <- databaseIdHeaders$newDatabaseId
      
      # wide form
      data <- data %>%
        dplyr::mutate(type = paste0(.data$databaseId, " ", .data$type)) %>%
        tidyr::pivot_wider(
          id_cols = dplyr::all_of(keyColumns),
          names_from = "type",
          values_from = valuesData,
          values_fill = 0
        ) 
      if (sort) {
        data <- data %>% 
          dplyr::arrange(dplyr::desc(abs(dplyr::across(
            dplyr::contains(dplyr::all_of(dataColumns))
          ))))
      }
      dataColumnsLevel2 <- colnames(data)
      dataColumnsLevel2 <-
        dataColumnsLevel2[stringr::str_detect(
          string = dataColumnsLevel2,
          pattern = paste0(keyColumns, collapse = "|"),
          negate = TRUE
        )] %>%
        stringr::word(start = -1)
      
    } else if (countLocation == 2) {
      if (length(setdiff(c("databaseId", dataColumns), colnames(headerCount))) != 0) {
        warning("missing required fields to draw formatted datatable.")
      }
      data <- data %>%
        dplyr::inner_join(
          headerCount %>%
            tidyr::pivot_longer(
              cols = dplyr::all_of(dataColumns),
              names_to = "type",
              values_to = "valuesHeader"
            ),
          by = c("databaseId", "type")
        )  %>%
        dplyr::arrange(.data$databaseId, .data$type) %>%
        dplyr::mutate(type = paste0(
          .data$databaseId,
          " ",
          .data$type,
          " (",
          scales::comma(.data$valuesHeader),
          ")"
        )) %>%
        dplyr::mutate(dataColumnsLevel2 = paste0(.data$type,
                                                 " (",
                                                 scales::comma(.data$valuesHeader),
                                                 ")"))
      if (showResultsAsPercent) {
        if (!is.null(maxCount)) {
          maxCount <- suppressWarnings(ceiling(maxCount / (max(data$valuesHeader))))
        }
        data <- data %>%
          dplyr::mutate(valuesData = .data$valuesData / .data$valuesHeader)
      }
      
      databaseIdHeaders <- uniqueDatabases %>%
        dplyr::mutate(newDatabaseId = .data$databaseId) %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$databaseId)
      # headers for sketch
      databaseIdsForUseAsHeader <- databaseIdHeaders %>%
        dplyr::pull(.data$newDatabaseId)
      # wide form
      data <- data %>%
        dplyr::select(-.data$valuesHeader) %>%
        tidyr::pivot_wider(
          id_cols = dplyr::all_of(keyColumns),
          names_from = "type",
          values_from = "valuesData",
          values_fill = 0
        ) 
      
      dataColumnsLevel2 <- colnames(data)
      dataColumnsLevel2 <-
        dataColumnsLevel2[stringr::str_detect(string = dataColumnsLevel2,
                                              pattern = paste0(keyColumns, collapse = "|"),
                                              negate = TRUE)]
      dataColumnsLevel2 <-
        stringr::str_remove_all(
          string = dataColumnsLevel2,
          pattern = paste0(uniqueDatabases$databaseId, collapse = "|")
        ) %>%
        stringr::str_squish() %>%
        stringr::str_trim()
    }
  }
  
  ### format
  numberOfColumns <- length(keyColumns) - 1
  numberOfSubstitutableColums <- length(dataColumns)
  
  if (!showResultsAsPercent) {
    minimumCellCountDefs <- minCellCountDef(numberOfColumns + (1:(
      numberOfSubstitutableColums * length(databaseIdsForUseAsHeader)
    )))  
  } else if (showResultsAsPercent) {
    minimumCellCountDefs <- minCellPercentDef(numberOfColumns + (1:(
      numberOfSubstitutableColums * length(databaseIdsForUseAsHeader)
    )))  
  }
  
  if (sort) {
    sortByColumns <- colnames(data)
    sortByColumns <-
      sortByColumns[stringr::str_detect(string = sortByColumns,
                                        pattern = paste(dataColumns, collapse = "|"))]
    if (length(sortByColumns) > 0) {
      sortByColumns <- sortByColumns[[1]]
      data <- data %>%
        dplyr::arrange(dplyr::desc(dplyr::across(dplyr::all_of(sortByColumns))))
    }
  }
  
  if (nrow(data) > 20) {
    datableHeight <- '65vh'
  } else {
    datableHeight <- TRUE
  }
  
  options = list(
    pageLength = 1000,
    lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
    searching = TRUE,
    lengthChange = TRUE,
    ordering = TRUE,
    paging = TRUE,
    info = TRUE,
    searchHighlight = TRUE,
    scrollX = TRUE,
    scrollY = datableHeight,
    columnDefs = list(truncateStringDef(1, 50),
                      minimumCellCountDefs) #!!!!!!!!!!! note in percent form, this may cause error because of -ve number
  )

  databaseColumnNamesInRightOrder <-
    colnames(data)[stringr::str_detect(
      string = colnames(data),
      pattern = paste0(keyColumns, collapse = "|"),
      negate = TRUE
    )] %>%
    stringr::word(start = 1) %>% 
    unique()
  sketch <- htmltools::withTags(table(class = "display",
                                      thead(tr(
                                        lapply(camelCaseToTitleCase(keyColumns),
                                               th,
                                               rowspan = rowSpan),
                                        lapply(
                                          databaseColumnNamesInRightOrder,
                                          th,
                                          colspan = numberOfSubstitutableColums,
                                          class = "dt-center",
                                          style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                        )
                                      ),
                                      tr(
                                        lapply(camelCaseToTitleCase(dataColumnsLevel2), th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                      ))))

    dataTable <- DT::datatable(
      data,
      options = options,
      rownames = FALSE,
      container = sketch,
      colnames = colnames(data) %>% camelCaseToTitleCase(),
      escape = FALSE,
      selection = 'single',
      filter = "top",
      class = "stripe nowrap compact"
    )
  if (!is.null(maxCount)) {
    dataTable <- DT::formatStyle(
      table = dataTable,
      columns =  (numberOfColumns + 1) + 1:(length(databaseColumnNamesInRightOrder) * numberOfSubstitutableColums),
      background = DT::styleColorBar(c(0, maxCount), "lightblue"),
      backgroundSize = "98% 88%",
      backgroundRepeat = "no-repeat",
      backgroundPosition = "center"
    )
  }
  return(dataTable)
}


getCountsForHeaderForUseInDataTable <- function(dataSource,
                                                databaseIds = NULL,
                                                cohortIds = NULL,
                                                source = "Datasource Level",
                                                fields = "Both") {
  if (all(!hasData(databaseIds),
          !hasData(cohortIds))) {
    stop("Please provide either databaseIds or cohortids")
  }
  if (source == "Datasource Level") {
    countsForHeader <- getDatabaseCounts(dataSource = dataSource,
                                         databaseIds = databaseIds)
    if (!hasData(countsForHeader)) {
      warning("Did not get counts for table header in metadata file. Please check the output from Cohort Diagnostics (metadata file is generated in the last step, is it in the zip file?), is it corrupted?")
    }
  } else if (source == "Cohort Level") {
    if (length(cohortIds) > 1) {
      stop("Only one cohort id is supported")
    }
    countsForHeader <-
      getResultsCohortCount(
        dataSource = dataSource,
        cohortIds = cohortIds,
        databaseIds = databaseIds
      ) %>%
      dplyr::rename(records = .data$cohortEntries,
                    persons = .data$cohortSubjects) %>%
      dplyr::select(-.data$cohortId) #only one cohort id is supported
    if (!hasData(countsForHeader)) {
      warning("Did not get counts for table header in cohort table. Please check the output from Cohort Diagnostics, is it corrupted?")
    }
  }
  
  if (fields  == "Persons") {
    countsForHeader <- countsForHeader %>%
      dplyr::select(-.data$records) %>%
      dplyr::rename(count = .data$persons)
  } else if (fields %in% c("Events", "Records")) {
    countsForHeader <- countsForHeader %>%
      dplyr::select(-.data$persons) %>%
      dplyr::rename(count = .data$records)
  }
  
  return(countsForHeader)
}




getStlModelOutputForTsibbleDataValueFields <- function(tsibbleData, valueFields = "value") {
  if (!hasData(tsibbleData)) {
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
    } else if (tsibble::is_yearquarter(modelData[[valueField]]$periodBegin)) {
      modelData[[valueField]] <- modelData[[valueField]] %>% 
        dplyr::mutate(periodDate = as.Date(.data$periodBegin))
    }  else if (is.double(modelData[[valueField]]$periodBegin) || 
               is.integer(modelData[[valueField]]$periodBegin)) {
      modelData[[valueField]] <- modelData[[valueField]] %>% 
        dplyr::mutate(periodDate = as.Date(paste0(.data$periodBegin, "-01-01")))
    }
  }
  return(modelData)
}

getConceptCountForCohortAndDatabase <- function(dataSource,
                                                databaseIds,
                                                conceptIds,
                                                cohortIds,
                                                databaseCount = TRUE) {
  if (databaseCount) {
    data <- getResultsConceptCount(
      dataSource = dataSource,
      databaseIds = databaseIds,
      conceptIds = conceptIds,
      calendarMonths = 0,
      calendarYears = 0
    )
    if (is.null(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::select(.data$databaseId,
                    .data$conceptId,
                    .data$subjectCount,
                    .data$conceptCount) %>%
      dplyr::rename("records" = .data$conceptCount,
                    "persons" = .data$subjectCount)
  } else {
    data <-
      getResultsIndexEventBreakdown(
        dataSource = dataSource,
        cohortIds = cohortIds,
        databaseIds = databaseIds,
        conceptIds = conceptIds,
        coConceptIds = 0
      )
    if (is.null(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::select(
        .data$databaseId,
        .data$cohortId,
        .data$conceptId,
        .data$subjectCount,
        .data$conceptCount
      ) %>%
      dplyr::rename("records" = .data$conceptCount,
                    "persons" = .data$subjectCount)
  }
  return(data)
}


getMaxValueForStringMatchedColumnsInDataFrame <- function(data, string) {
  data %>% 
    dplyr::summarise(dplyr::across(dplyr::contains(string), ~ max(.x, na.rm = TRUE))) %>% 
    tidyr::pivot_longer(values_to = "value", cols = dplyr::everything()) %>% 
    dplyr::pull() %>% 
    max(na.rm = TRUE)
}

exportCohortDetailsAsZip <- function(dataSource,
                                     cohortIds = NULL,
                                     zipFile = NULL) {
  cohort <- getResultsCohort(dataSource = dataSource)
  
  rootFolder <-  stringr::str_replace_all(string = Sys.time(), pattern = "-",replacement = "") 
  rootFolder <- stringr::str_replace_all(string = rootFolder, pattern = ":",replacement = "")
  tempdir <- file.path(tempdir(), rootFolder) 
  
  for (i in (1:nrow(cohort))) {
    cohortId <- cohort[i,]$cohortId
    cohortName <- cohort[i,]$cohortName
    
    dir.create(path = file.path(tempdir, cohortName), recursive = TRUE, showWarnings = FALSE)
    cohortExpression <- cohort[i,]$json %>% 
      RJSONIO::fromJSON(digits = 23)
    
    details <-
      getCirceRenderedExpression(cohortDefinition =  cohortExpression)
    
    SqlRender::writeSql(sql = details$cohortJson,
                        targetFile = file.path(tempdir, 
                                               cohortName, 
                                               paste0(cohortId,'_cohortDefinitionJson.json')))
    SqlRender::writeSql(sql = details$cohortMarkdown,
                        targetFile = file.path(tempdir, 
                                               cohortName, 
                                               paste0(cohortId, '_cohortDefinitionMarkdown.md')))
    SqlRender::writeSql(sql = details$conceptSetMarkdown,
                        targetFile = file.path(tempdir, 
                                               cohortName, 
                                               paste0(cohortId,'_conceptSetMarkdown.md')))
    SqlRender::writeSql(sql = details$cohortHtmlExpression,
                        targetFile = file.path(tempdir, 
                                               cohortName, 
                                               paste0(cohortId, '_cohortDefinitionHtml.html')))
  }
   
  return(DatabaseConnector::createZipFile(zipFile = zipFile,
                                          files = tempdir,
                                          rootFolder = tempdir))
}
