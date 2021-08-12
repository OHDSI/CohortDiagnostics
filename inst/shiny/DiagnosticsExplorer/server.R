shiny::shinyServer(function(input, output, session) {
  
  #Reactive Functions for Dropdown ----
  
  ##getCohortIdFromDropdown----
  getCohortIdFromDropdown <- shiny::reactive({
    data <- cohort %>% 
      dplyr::filter(.data$compoundName %in% input$cohort) %>% 
      dplyr::arrange(.data$cohortId) %>% 
      dplyr::pull(.data$cohortId) %>% 
      unique()
    return(data)
  })
  
  ##getCohortIdsFromDropdown----
  getCohortIdsFromDropdown <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$cohorts_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$cohorts_open) || !is.null(input$tabs)) {
      selectedCohortIds <- cohort %>% 
        dplyr::filter(.data$compoundName %in% input$cohorts) %>% 
        dplyr::arrange(.data$cohortId) %>% 
        dplyr::pull(.data$cohortId) %>% 
        unique()
      getCohortIdsFromDropdown(selectedCohortIds)
    }
  })
  
  ##getComparatorCohortIdFromDropdowm----
  getComparatorCohortIdFromDropdowm <- shiny::reactive({
    data <- cohort %>% 
      dplyr::filter(.data$compoundName %in% input$comparatorCohort) %>% 
      dplyr::arrange(.data$cohortId) %>% 
      dplyr::pull(.data$cohortId) %>% 
      unique()
    return(data)
  })
  
  ##getConceptSetIdsfromDropdown----
  getConceptSetIdsfromDropdown <- shiny::reactive(x = {
    data <- conceptSets %>% 
      dplyr::filter(.data$cohortId %in% c(getCohortIdFromDropdown()) %>% unique()) %>%
      dplyr::filter(.data$conceptSetName %in% input$conceptSetsToFilterCharacterization) %>% 
      dplyr::arrange(.data$conceptSetId) %>% 
      dplyr::pull(.data$conceptSetId) %>% 
      unique()
    return(data)
  })
  
  ##getTimeIdsFromDropdowm----
  getTimeIdsFromDropdowm <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$timeIdChoices_open,
         input$tabs)
  }, handlerExpr = {
    if (exists('temporalCovariateChoices') &&
        (isFALSE(input$timeIdChoices_open) ||
         !is.null(input$tabs))) {
      selectedTimeIds <- temporalCovariateChoices %>%
        dplyr::filter(.data$choices %in% input$timeIdChoices) %>%
        dplyr::pull(.data$timeId)
      getTimeIdsFromDropdowm(selectedTimeIds)
    }
  })
  
  ##getDatabaseIdsFromDropdown----
  getDatabaseIdsFromDropdown <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$databases_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$databases_open) || !is.null(input$tabs)) {
      selectedDatabaseIds <- input$databases
      getDatabaseIdsFromDropdown(selectedDatabaseIds)
    }
  })
  
  
  
  #Reactive functions that are initiated on start up----
  ##getConceptCountData----
  #loads the entire data into R memory. 
  # an alternative design maybe to load into R memory on start up (global.R)
  # but the size may become too large and we may want to filter to cohorts/database of choice
  # to do this - we have to replace by function that joins cohortId/databaseId choices to resolved concepts
  getConceptCountData <- 
    shiny::reactive(x = {
      #!!!!!!put progress
      conceptCount <- getResultsConceptCount(
        dataSource = dataSource,
        databaseIds = database$databaseId
      )
      conceptSubjects <- getResultsConceptSubjects(
        dataSource = dataSource,
        databaseIds = database$databaseId
      )
      conceptCount <- conceptCount %>% 
        dplyr::inner_join(conceptSubjects,
                          by = c('databaseId',
                                 'domainTable',
                                 'domainField',
                                 'conceptId')) %>% 
        dplyr::rename(domainTableShort = .data$domainTable) %>% 
        dplyr::rename(domainFieldShort = .data$domainField)
      return(conceptCount)
    })
  
  ##getConceptCountSummaryData----
  getConceptCountSummaryData <- 
    shiny::reactive(x = {
      #!!!!!!put progress
      data <- getConceptCountData()
      data <- data %>% 
        dplyr::group_by(.data$conceptId,
                        .data$databaseId) %>% 
        dplyr::summarise(conceptCount = sum(.data$conceptCount),
                         subjectCount = max(.data$subjectCount), 
                         .groups = 'keep') %>% 
        dplyr::ungroup() %>% 
        dplyr::arrange(.data$conceptId, .data$databaseId)
      return(data)
    })
  
  ##getCohortSortedByCohortId ----
  getCohortSortedByCohortId <- shiny::reactive({
    data <- cohort %>%
      dplyr::arrange(.data$cohortId)
    return(data)
  })
  
  ##Compound cohort name in single select drop down----
  shiny::observe({
    subset <- getCohortSortedByCohortId()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "cohort",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset
    )
  })
  
  ##Compound cohort name in multi-select dropdown----
  shiny::observe({
    subset <- getCohortSortedByCohortId()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "cohorts",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = c(subset[1], subset[2])
    )
  })
  
  ##Compound cohort name in single select comparator dropdown----
  shiny::observe({
    subset <- getCohortSortedByCohortId()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "comparatorCohort",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset[2]
    )
  })
  

  #Reactive functions used in Cohort tab----
  ##Cohort definition----
  ###cohortDefinitionTableData----
  cohortDefinitionTableData <- shiny::reactive(x = {
    data <-  getCohortSortedByCohortId() %>%
      dplyr::select(cohort = .data$shortName,
                    .data$cohortId,
                    .data$cohortName)
    return(data)
  })
  ###getLastTwoRowSelectedInCohortTable----
  # What rows were selected in cohort table
  getLastTwoRowSelectedInCohortTable <- reactive({
    idx <- input$cohortDefinitionTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      cohortData <- getCohortSortedByCohortId()
      if (length(idx) > 1) {
        # get the last two rows selected
        lastRowsSelected <- idx[c(length(idx), length(idx) - 1)]
      } else {
        lastRowsSelected <- idx
      }
      return(cohortData[lastRowsSelected, ])
    }
  })
  
  ###getSelectedCohortMetaData----
  getSelectedCohortMetaData <- shiny::reactive(x = {
    data <- getLastTwoRowSelectedInCohortTable()
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    } else {
      details <- list()
      for (i in 1:nrow(data)) {
        details[[i]] <-  tags$table(
          style = "margin-top: 5px;",
          tags$tr(
            tags$td(tags$strong("Cohort ID: ")),
            tags$td(HTML("&nbsp;&nbsp;")),
            tags$td(data[i, ]$cohortId)
          ),
          tags$tr(
            tags$td(tags$strong("Cohort Name: ")),
            tags$td(HTML("&nbsp;&nbsp;")),
            tags$td(data[i, ]$cohortName)
          ),
          tags$tr(
            tags$td(tags$strong("Metadata: ")),
            tags$td(HTML("&nbsp;&nbsp;")),
            tags$td(data[i, ]$metadata)
          )
        )
        #!!!!!!!!!!!!!!!!!!parse cohort[i,]$metadata from JSON to data table, iterate and present
      }
      return(details)
    }
  })
  
  ##Cohort count----
  ###getCountsForSelectedCohortsLeft----
  getCountsForSelectedCohortsLeft <- shiny::reactive(x = {
    row <- getLastTwoRowSelectedInCohortTable()[1,]
    if (is.null(row)) {
      return(NULL)
    }
    data <- cohortCount %>%
      dplyr::filter(.data$cohortId %in% row$cohortId) %>% 
      dplyr::filter(.data$databaseId %in% database$databaseId) %>% 
      dplyr::select(.data$databaseId, 
                    .data$cohortSubjects, 
                    .data$cohortEntries) %>% 
      dplyr::arrange(.data$databaseId)
    return(data)
  })
  
  ###getCountsForSelectedCohortsRight----
  getCountsForSelectedCohortsRight <- shiny::reactive(x = {
    row <- getLastTwoRowSelectedInCohortTable()[2,]
    if (is.null(row)) {
      return(NULL)
    }
    data <- cohortCount %>%
      dplyr::filter(.data$cohortId %in% row$cohortId) %>% 
      dplyr::filter(.data$databaseId %in% database$databaseId) %>% 
      dplyr::select(.data$databaseId, 
                    .data$cohortSubjects, 
                    .data$cohortEntries) %>% 
      dplyr::arrange(.data$databaseId)
    return(data)
  })
  
  ###getSubjectRecordCountForCohortDatabaseCombinationLeft----
  getSubjectRecordCountForCohortDatabaseCombinationLeft <-
    shiny::reactive(x = {
      databaseId <- database %>%
        dplyr::filter(.data$databaseIdWithVocabularyVersion == input$choiceForConceptSetDetailsLeft) %>%
        dplyr::pull(.data$databaseId) %>%
        unique()
      data <- getCountsForSelectedCohortsLeft()
      if (all(!is.null(data),
              nrow(data) > 0)) {
        data <- data %>%
        dplyr::filter(.data$databaseId == !!databaseId) %>%
        dplyr::select(.data$cohortSubjects, .data$cohortEntries)
      }
      if (nrow(data) == 0 || is.null(data)) {
        return(NULL)
      } else {
        return(data)
      }
    })
  
  ###getSubjectRecordCountForCohortDatabaseCombinationRight----
  getSubjectRecordCountForCohortDatabaseCombinationRight <-
    shiny::reactive(x = {
      databaseId <- database %>%
        dplyr::filter(.data$databaseIdWithVocabularyVersion == input$choiceForConceptSetDetailsRight) %>%
        dplyr::pull(.data$databaseId) %>%
        unique()
      data <- getCountsForSelectedCohortsRight() %>%
        dplyr::filter(.data$databaseId == !!databaseId) %>%
        dplyr::select(.data$cohortSubjects, .data$cohortEntries)
      if (nrow(data) == 0 || is.null(data)) {
        return(NULL)
      } else {
        return(data)
      }
    })
  
  ##Human readable text----
  ##!!!!!!!!!!!!! move to under 'Details' - make collapsible box, default OPEN
  ###getCirceRenderedExpressionDetails----
  getCirceRenderedExpressionDetails <- shiny::reactive(x = {
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Rendering human readable cohort description using CirceR", value = 0)
    
    selectionsInCohortTable <- getLastTwoRowSelectedInCohortTable()
    if (nrow(getLastTwoRowSelectedInCohortTable()) > 0) {
      details <- list()
      for (i in (1:nrow(selectionsInCohortTable))) {
        progress$inc(1/nrow(selectionsInCohortTable), detail = paste("Doing part", i))
        
        #!!!!!!!!!!!!!! can this be replaced by function getCirceRenderedExpression in shared.R
        circeExpression <-
          CirceR::cohortExpressionFromJson(expressionJson = selectionsInCohortTable[i, ]$json)
        circeExpressionMarkdown <-
          CirceR::cohortPrintFriendly(circeExpression)
        circeConceptSetListmarkdown <-
          CirceR::conceptSetListPrintFriendly(circeExpression$conceptSets)
        details[[i]] <- selectionsInCohortTable[i, ]
        details[[i]]$circeConceptSetListmarkdown <-
          circeConceptSetListmarkdown
        details[[i]]$htmlExpressionCohort <-
          convertMdToHtml(circeExpressionMarkdown)
        details[[i]]$htmlExpressionConceptSetExpression <-
          convertMdToHtml(circeConceptSetListmarkdown)
      }
      details <- dplyr::bind_rows(details)
    } else {
      return(NULL)
    }
    return(details)
  })
  
  ###getCirceRPackageVersion----
  getCirceRPackageVersion <- shiny::reactive(x = {
    #!!!!!!!!!!!!why cant this just be fixed text - it doesnt change - packageVersion('CirceR')
    # is it just to get cohortId? why not just use selected row ?
    row <- getLastTwoRowSelectedInCohortTable()
    if (is.null(row)) {
      return(NULL)
    } else {
      details <- list()
      for (i in 1:nrow(row)) {
        details[[i]] <- tags$table(
          tags$tr(
            tags$td(
              paste("rendered for cohort id:", row[i, ]$cohortId, "using CirceR version: ", packageVersion('CirceR'))
            )
          )
        )
      }
      return(details)
    }
  })
  
  ##Concept set ----
  ###getConceptSetExpressionAndDetails----
  getConceptSetExpressionAndDetails <- shiny::reactive({
    if (is.null(getLastTwoRowSelectedInCohortTable())) {
      return(NULL)
    }
    details <- list()
    for (i in 1:nrow(getLastTwoRowSelectedInCohortTable())) {
      conceptSetDetailsFromCohortDefinition <-
        getConceptSetDetailsFromCohortDefinition(
          cohortDefinitionExpression = RJSONIO::fromJSON(getLastTwoRowSelectedInCohortTable()[i,]$json)
        )
      details[[i]] <- conceptSetDetailsFromCohortDefinition
    }
    return(details)
  })
  
  
  ###getConceptSetExpressionLeft----
  getConceptSetExpressionLeft <- shiny::reactive(x = {
    idx <- input$conceptsetExpressionTableLeft_rows_selected
    if (length(idx) == 0 || is.null(idx)) {
      return(NULL)
    }
    if (!is.null(getConceptSetExpressionAndDetails()[[1]]$conceptSetExpression) &&
        nrow(getConceptSetExpressionAndDetails()[[1]]$conceptSetExpression) > 0) {
      data <-
        getConceptSetExpressionAndDetails()[[1]]$conceptSetExpression[idx,]
      if (!is.null(data)) {
        return(data)
      } else {
        return(NULL)
      }
    }
  })
  
  ###getConceptSetExpressionRight----
  getConceptSetExpressionRight <- shiny::reactive(x = {
    idx <- input$conceptsetExpressionTableRight_rows_selected
    if (length(idx) == 0 || is.null(idx)) {
      return(NULL)
    }
    if (!is.null(getConceptSetExpressionAndDetails()[[2]]$conceptSetExpression) &&
        nrow(getConceptSetExpressionAndDetails()[[2]]$conceptSetExpression) > 0) {
      data <-
        getConceptSetExpressionAndDetails()[[2]]$conceptSetExpression[idx,]
      if (!is.null(data)) {
        return(data)
      } else {
        return(NULL)
      }
    }
  })
  
  ###getConceptSetsExpressionDetailsLeft----
  getConceptSetsExpressionDetailsLeft <- shiny::reactive(x = {
    if (is.null(getConceptSetExpressionLeft())) {
      return(NULL)
    }
    data <-
      getConceptSetExpressionAndDetails()[[1]]$conceptSetExpressionDetails
    data <- data %>%
      dplyr::filter(.data$id == getConceptSetExpressionLeft()$id)
    validate(need((all(!is.null(data), nrow(data) > 0)),
                  "No details available for the concept set expression."))
    data <- data %>%
      dplyr::select(
        .data$conceptId,
        .data$conceptName,
        .data$isExcluded,
        .data$includeDescendants,
        .data$includeMapped,
        .data$standardConcept,
        .data$invalidReason,
        .data$conceptCode,
        .data$domainId,
        .data$vocabularyId,
        .data$conceptClassId
      )
    return(data)
  })
  
  ###getConceptSetsExpressionDetailsRight----
  getConceptSetsExpressionDetailsRight <- shiny::reactive(x = {
    if (is.null(getConceptSetExpressionRight())) {
      return(NULL)
    }
    data <-
      getConceptSetExpressionAndDetails()[[2]]$conceptSetExpressionDetails
    data <- data %>%
      dplyr::filter(.data$id == getConceptSetExpressionRight()$id)
    validate(need((all(!is.null(data), nrow(data) > 0)),
                  "No details available for the concept set expression."))
    data <- data %>%
      dplyr::select(
        .data$conceptId,
        .data$conceptName,
        .data$isExcluded,
        .data$includeDescendants,
        .data$includeMapped,
        .data$standardConcept,
        .data$invalidReason,
        .data$conceptCode,
        .data$domainId,
        .data$vocabularyId,
        .data$conceptClassId
      )
    return(data)
  })
  
  
  ###getResolvedConceptsLeft----
  getResolvedConceptsLeft <- shiny::reactive({
    row <- getLastTwoRowSelectedInCohortTable()[1,]
    if (is.null(input$choiceForConceptSetDetailsLeft)) {
      return(NULL)
    }
    data <- getResultsResolvedConcepts(
      dataSource = dataSource,
      cohortIds = row$cohortId
    )
    if (is.null(data)) {
      return(NULL)
    }
    if (nrow(data) > 0) {
      data <- data %>% 
        dplyr::select(.data$databaseId, .data$conceptSetId, .data$conceptId)
    }
    return(data)
  })
  
  ###getResolvedConceptsRight----
  getResolvedConceptsRight <- shiny::reactive({
    row <- getLastTwoRowSelectedInCohortTable()
    if (nrow(row) == 2) {
      row <- row[2,]
    } else {
      return(NULL)
    }
    if (is.null(input$choiceForConceptSetDetailsRight)) {
      return(NULL)
    }
    data <- getResultsResolvedConcepts(
      dataSource = dataSource,
      cohortIds = row$cohortId
    )
    if (is.null(data)) {
      return(NULL)
    }
    if (nrow(data) > 0) {
      data <- data %>% 
        dplyr::select(.data$databaseId, .data$conceptSetId, .data$conceptId)
    }
    return(data)
  })
  
  ###getExcludedConceptsLeft----
  getExcludedConceptsLeft <- shiny::reactive({
    row <- getLastTwoRowSelectedInCohortTable()[1,]
    if (is.null(input$choiceForConceptSetDetailsLeft)) {
      return(NULL)
    }
    data <- getResultsExcludedConcepts(
      dataSource = dataSource,
      cohortIds = row$cohortId
    )
    if (is.null(data)) {
      return(NULL)
    }
    if (nrow(data) > 0) {
      data <- data %>% 
        dplyr::select(.data$databaseId, .data$conceptSetId, .data$conceptId)
    }
    return(data)
  })
  
  ###getExcludedConceptsRight----
  getExcludedConceptsRight <- shiny::reactive({
    row <- getLastTwoRowSelectedInCohortTable()
    if (nrow(row) == 2) {
      row <- row[2,]
    } else {
      return(NULL)
    }
    if (is.null(input$choiceForConceptSetDetailsRight)) {
      return(NULL)
    }
    data <- getResultsExcludedConcepts(
      dataSource = dataSource,
      cohortIds = row$cohortId
    )
    if (is.null(data)) {
      return(NULL)
    }
    if (nrow(data) > 0) {
      data <- data %>% 
        dplyr::select(.data$databaseId, .data$conceptSetId, .data$conceptId)
    }
    return(data)
  })
  
  ###getOrphanConceptsLeft----
  getOrphanConceptsLeft <- shiny::reactive({
    row <- getLastTwoRowSelectedInCohortTable()[1,]
    if (is.null(input$choiceForConceptSetDetailsLeft)) {
      return(NULL)
    }
    data <- getResultsOrphanConcept(
      dataSource = dataSource,
      cohortIds = row$cohortId
    )
    if (is.null(data)) {
      return(NULL)
    }
    if (nrow(data) > 0) {
      data <- data %>% 
        dplyr::select(.data$databaseId, .data$conceptSetId, .data$conceptId)
    }
    return(data)
  })
  
  ###getOrphanConceptsRight----
  getOrphanConceptsRight <- shiny::reactive({
    row <- getLastTwoRowSelectedInCohortTable()
    if (nrow(row) == 2) {
      row <- row[2,]
    } else {
      return(NULL)
    }
    if (is.null(input$choiceForConceptSetDetailsRight)) {
      return(NULL)
    }
    data <- getResultsOrphanConcept(
      dataSource = dataSource,
      cohortIds = row$cohortId
    )
    if (is.null(data)) {
      return(NULL)
    }
    if (nrow(data) > 0) {
      data <- data %>% 
        dplyr::select(.data$databaseId, .data$conceptSetId, .data$conceptId)
    }
    return(data)
  })
  
  
  ###getConceptRelationshipsLeft----
  getConceptRelationshipsLeft <- shiny::reactive({
    conceptIds <- dplyr::bind_rows(getResolvedConceptsLeft(),
                       getExcludedConceptsLeft(),
                       getOrphanConceptsLeft()) %>% 
      dplyr::pull(.data$conceptId) %>% 
      unique() %>% 
      sort()
    if (any(is.null(conceptIds),
            length(conceptIds) == 0)) {
      return(NULL)
    }
    data <- getConceptRelationship(
      dataSource = dataSource,
      vocabularyDatabaseSchema = NULL, #for alternate vocabulary
      conceptIds = conceptIds)
    return(data)
  })
  
  ###getConceptRelationshipsRight----
  getConceptRelationshipsRight <- shiny::reactive({
    conceptIds <- dplyr::bind_rows(getResolvedConceptsRight(),
                                   getExcludedConceptsRight(),
                                   getOrphanConceptsRight()) %>% 
      dplyr::pull(.data$conceptId) %>% 
      unique() %>% 
      sort()
    if (any(is.null(conceptIds),
            length(conceptIds) == 0)) {
      return(NULL)
    }
    data <- getConceptRelationship(
      dataSource = dataSource,
      vocabularyDatabaseSchema = NULL, #for alternate vocabulary
      conceptIds = conceptIds)
    return(data)
  })
  
  ###getConceptDetailsLeft----
  getConceptDetailsLeft <- shiny::reactive({
    conceptIds <- dplyr::bind_rows(getResolvedConceptsLeft(),
                                   getExcludedConceptsLeft(),
                                   getOrphanConceptsLeft()) %>% 
      dplyr::pull(.data$conceptId) %>% 
      unique() %>% 
      sort()
    conceptRelationship <- getConceptRelationshipsLeft()
    if (all(!is.null(conceptRelationship),
            nrow(conceptRelationship) > 0)) {
    conceptIds <- c(conceptIds,
                    conceptRelationship$conceptId1,
                    conceptRelationship$conceptId2) %>% 
      unique()
    }
    if (any(is.null(conceptIds),
            length(conceptIds) == 0)) {
      return(NULL)
    }
    data <- getConcept(
      dataSource = dataSource,
      vocabularyDatabaseSchema = NULL, #for alternate vocabulary
      conceptIds = conceptIds)
    return(data)
  })
  
  ###getConceptDetailsRight----
  getConceptDetailsRight <- shiny::reactive({
    conceptIds <- dplyr::bind_rows(getResolvedConceptsRight(),
                                   getExcludedConceptsRight(),
                                   getOrphanConceptsRight()) %>% 
      dplyr::pull(.data$conceptId) %>% 
      unique() %>% 
      sort()
    conceptRelationship <- getConceptRelationshipsRight()
    if (all(!is.null(conceptRelationship),
            nrow(conceptRelationship) > 0)) {
      conceptIds <- c(conceptIds,
                      conceptRelationship$conceptId1,
                      conceptRelationship$conceptId2) %>% 
        unique()
    }
    if (any(is.null(conceptIds),
            length(conceptIds) == 0)) {
      return(NULL)
    }
    data <- getConcept(
      dataSource = dataSource,
      vocabularyDatabaseSchema = NULL, #for alternate vocabulary
      conceptIds = conceptIds)
    return(data)
  })
  
  ###getConceptSetDetailsLeft----
  getConceptSetDetailsLeft <- shiny::reactive({
    data <- list()
    if (is.null(input$choiceForConceptSetDetailsLeft)) {return(NULL)}
    databaseIdToFilter <- database %>%
      dplyr::filter(.data$databaseIdWithVocabularyVersion == input$choiceForConceptSetDetailsLeft) %>%
      dplyr::pull(.data$databaseId)
    conceptCountSummary <- getConceptCountSummaryData()
    resolvedConcepts <- getResolvedConceptsLeft()
    if (all(!is.null(resolvedConcepts),
            nrow(resolvedConcepts) > 0)) {
      data$resolvedConcepts <- resolvedConcepts %>% 
        dplyr::filter(.data$databaseId %in% databaseIdToFilter) %>% 
        dplyr::left_join(conceptCountSummary,
                         by = c('conceptId',
                                'databaseId')) %>% 
        tidyr::replace_na(list(conceptCount = 0,
                               subjectCount = 0)) %>% 
        dplyr::arrange(dplyr::desc(.data$conceptCount))
    }
    excludedConcepts <- getExcludedConceptsLeft()
    if (all(!is.null(excludedConcepts),
            nrow(excludedConcepts) > 0)) {
      data$excludedConcepts <- excludedConcepts %>% 
        dplyr::filter(.data$databaseId %in% databaseIdToFilter) %>% 
        dplyr::left_join(conceptCountSummary,
                         by = c('conceptId',
                                'databaseId')) %>% 
        tidyr::replace_na(list(conceptCount = 0,
                               subjectCount = 0)) %>% 
        dplyr::arrange(dplyr::desc(.data$conceptCount))
    }
    orphanConcepts <- getOrphanConceptsLeft()
    if (all(!is.null(orphanConcepts),
            nrow(orphanConcepts) > 0)) {
      data$orphanConcepts <- orphanConcepts %>% 
        dplyr::filter(.data$databaseId %in% databaseIdToFilter) %>% 
        dplyr::left_join(conceptCountSummary,
                         by = c('conceptId',
                                'databaseId')) %>% 
        tidyr::replace_na(list(conceptCount = 0,
                               subjectCount = 0)) %>% 
        dplyr::arrange(dplyr::desc(.data$conceptCount))
    }
    data$conceptRelationship <- getConceptRelationshipsLeft()
    data$relationship <- relationship
    data$concept <- getConceptDetailsLeft()
    data$conceptClass <- conceptClass
    data$domain <- domain
    data$vocabulary <- vocabulary
    concept <- concept %>% 
      dplyr::select(.data$conceptId,
                    .data$conceptName,
                    .data$vocabularyId,
                    .data$domainId,
                    .data$conceptClassId,
                    .data$standardConcept,
                    .data$invalidReason,
                    .data$conceptCode)
    if (!is.null(data$resolvedConcepts)) {
      data$resolvedConcepts <- data$resolvedConcepts %>% 
        dplyr::left_join(concept,
                         by = "conceptId") %>% 
        dplyr::relocate(.data$conceptId, .data$conceptName) %>% 
        dplyr::select(-.data$databaseId, -.data$conceptSetId) %>% 
        dplyr::arrange(dplyr::desc(.data$conceptCount))
    }    
    if (!is.null(data$excludedConcepts)) {
      data$excludedConcepts <- data$excludedConcepts %>% 
        dplyr::left_join(concept,
                         by = "conceptId") %>% 
        dplyr::relocate(.data$conceptId, .data$conceptName) %>% 
        dplyr::select(-.data$databaseId, -.data$conceptSetId) %>% 
        dplyr::arrange(dplyr::desc(.data$conceptCount))
    }    
    if (!is.null(data$orphanConcepts)) {
      data$orphanConcepts <- data$orphanConcepts %>% 
        dplyr::left_join(concept,
                         by = "conceptId") %>% 
        dplyr::relocate(.data$conceptId, .data$conceptName) %>% 
        dplyr::select(-.data$databaseId, -.data$conceptSetId) %>% 
        dplyr::arrange(dplyr::desc(.data$conceptCount))
    }
    return(data)
  })
  
  ###getConceptSetDetailsRight----
  getConceptSetDetailsRight <- shiny::reactive({
    data <- list()
    if (is.null(input$choiceForConceptSetDetailsRight)) {return(NULL)}
    databaseIdToFilter <- database %>%
      dplyr::filter(.data$databaseIdWithVocabularyVersion == input$choiceForConceptSetDetailsRight) %>%
      dplyr::pull(.data$databaseId)
    conceptCountSummary <- getConceptCountSummaryData()
    resolvedConcepts <- getResolvedConceptsRight()
    if (all(!is.null(resolvedConcepts),
            nrow(resolvedConcepts) > 0)) {
      data$resolvedConcepts <- resolvedConcepts %>% 
        dplyr::filter(.data$databaseId %in% databaseIdToFilter) %>% 
        dplyr::left_join(conceptCountSummary,
                         by = c('conceptId',
                                'databaseId')) %>% 
        tidyr::replace_na(list(conceptCount = 0,
                               subjectCount = 0)) %>% 
        dplyr::arrange(dplyr::desc(.data$conceptCount))
    }
    excludedConcepts <- getExcludedConceptsRight()
    if (all(!is.null(excludedConcepts),
            nrow(excludedConcepts) > 0)) {
      data$excludedConcepts <- excludedConcepts %>% 
        dplyr::filter(.data$databaseId %in% databaseIdToFilter) %>% 
        dplyr::left_join(conceptCountSummary,
                         by = c('conceptId',
                                'databaseId')) %>% 
        tidyr::replace_na(list(conceptCount = 0,
                               subjectCount = 0)) %>% 
        dplyr::arrange(dplyr::desc(.data$conceptCount))
    }
    orphanConcepts <- getOrphanConceptsRight()
    if (all(!is.null(orphanConcepts),
            nrow(orphanConcepts) > 0)) {
      data$orphanConcepts <- orphanConcepts %>% 
        dplyr::filter(.data$databaseId %in% databaseIdToFilter) %>% 
        dplyr::left_join(conceptCountSummary,
                         by = c('conceptId',
                                'databaseId')) %>% 
        tidyr::replace_na(list(conceptCount = 0,
                               subjectCount = 0)) %>% 
        dplyr::arrange(dplyr::desc(.data$conceptCount))
    }
    data$conceptRelationship <- getConceptRelationshipsRight()
    data$relationship <- relationship
    data$concept <- getConceptDetailsRight()
    data$conceptClass <- conceptClass
    data$domain <- domain
    data$vocabulary <- vocabulary
    concept <- concept %>% 
      dplyr::select(.data$conceptId,
                    .data$conceptName,
                    .data$vocabularyId,
                    .data$domainId,
                    .data$conceptClassId,
                    .data$standardConcept,
                    .data$invalidReason,
                    .data$conceptCode)
    if (!is.null(data$resolvedConcepts)) {
      data$resolvedConcepts <- data$resolvedConcepts %>% 
        dplyr::left_join(concept,
                         by = "conceptId") %>% 
        dplyr::relocate(.data$conceptId, .data$conceptName) %>% 
        dplyr::select(-.data$databaseId, -.data$conceptSetId) %>% 
        dplyr::arrange(.data$conceptCount, .data$subjectCount)
    }    
    if (!is.null(data$excludedConcepts)) {
      data$excludedConcepts <- data$excludedConcepts %>% 
        dplyr::left_join(concept,
                         by = "conceptId") %>% 
        dplyr::relocate(.data$conceptId, .data$conceptName) %>% 
        dplyr::select(-.data$databaseId, -.data$conceptSetId) %>% 
        dplyr::arrange(.data$conceptCount, .data$subjectCount)
    }    
    if (!is.null(data$orphanConcepts)) {
      data$orphanConcepts <- data$orphanConcepts %>% 
        dplyr::left_join(concept,
                         by = "conceptId") %>% 
        dplyr::relocate(.data$conceptId, .data$conceptName) %>% 
        dplyr::select(-.data$databaseId, -.data$conceptSetId) %>% 
        dplyr::arrange(.data$conceptCount, .data$subjectCount)
    }
    return(data)
  })
  

  
  ##Inclusion rule ----
  ###getSimplifiedInclusionRuleData----
  getSimplifiedInclusionRuleData <- shiny::reactive(x = {
    data <-
      getResultsInclusionRuleStatistics(dataSource = dataSource) %>%
      dplyr::relocate(.data$cohortId,
                      .data$databaseId,
                      .data$ruleSequenceId,
                      .data$ruleName)
    return(data)
  })
  
  ###getSimplifiedInclusionRuleResultsLeft----
  getSimplifiedInclusionRuleResultsLeft <- shiny::reactive(x = {
    browser()
    if (length(getDatabaseIdForSelectedCohortCountLeft()) == 0) {
      return(NULL)
    }
    if (any(is.null(getLastTwoRowSelectedInCohortTable()),
            nrow(getLastTwoRowSelectedInCohortTable() == 0))) {
      return(NULL)
    }
    data <- getSimplifiedInclusionRuleData()
    if (any(is.null(data),
            nrow(data))) {
      return(NULL)
    }
    data <- data %>% 
      dplyr::filter(.data$databaseId %in% getDatabaseIdForSelectedCohortCountLeft()) %>% 
      dplyr::filter(.data$cohortId %in% getLastTwoRowSelectedInCohortTable()[1,]$cohortId)
    return(data)
  })
  
  ###getSimplifiedInclusionRuleResultsRight----
  #!!!!!!! add radio button, show simple (default) and detailed
  getSimplifiedInclusionRuleResultsRight <- shiny::reactive(x = {
    if (length(getDatabaseIdForSelectedCohortCountRight()) == 0) {
      return(NULL)
    }
    if (any(is.null(getLastTwoRowSelectedInCohortTable()),
            nrow(getLastTwoRowSelectedInCohortTable() == 0))) {
      return(NULL)
    }
    data <- getSimplifiedInclusionRuleData()
    if (any(is.null(data),
            nrow(data))) {
      return(NULL)
    }
    data <- data %>% 
      dplyr::filter(.data$databaseId %in% getDatabaseIdForSelectedCohortCountRight()) %>% 
      dplyr::filter(.data$cohortId %in% getLastTwoRowSelectedInCohortTable()[2,]$cohortId)
    return(data)
  })
  
  ###getFullCohortInclusionResults----
  #!!!!!!! add radio button, show simple (default) and detailed. this is detailed
  ##!!! table and visualization not created. similar to Atlas TO DO
  getFullCohortInclusionResults <- shiny::reactive({
    data <- list()
    data$cohortInclusion <-
      getResultsCohortInclusion(dataSource = dataSource)
    data$cohortInclusionStats <-
      getResultsCohortInclusionStats(dataSource = dataSource)
    data$cohortSummaryStats <-
      getResultsCohortSummaryStats(dataSource = dataSource)
    return(data)
  })
  
  
  #Output Functions----
  #output: saveCohortDefinitionButton----
  output$saveCohortDefinitionButton <- downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "CohortDefinition")
    },
    content = function(file) {
      data <- getCohortSortedByCohortId() %>%
        dplyr::select(cohort = .data$shortName,
                      .data$cohortId,
                      .data$cohortName,
                      .data$sql,
                      .data$json)
      downloadCsv(x = data, fileName = file)
    }
  )
  
  #output: cohortDefinitionTable----
  output$cohortDefinitionTable <- DT::renderDataTable(expr = {
    data <- cohortDefinitionTableData()
    
    if (nrow(data) < 20) {
      scrollYHeight <- '15vh'
    } else {
      scrollYHeight <- '25vh'
    }
    #!!!!!!!!!!!!!!!!!! create new function for data table rendering
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      ordering = TRUE,
      paging = TRUE,
      scrollX = TRUE,
      scrollY = scrollYHeight,
      info = TRUE,
      searchHighlight = TRUE
    )
    
    dataTable <- DT::datatable(
      data,
      options = options,
      rownames = FALSE,
      colnames = colnames(data) %>% camelCaseToTitleCase(),
      escape = FALSE,
      filter = "top",
      selection = list(mode = "multiple", target = "row"),
      class = "stripe compact"
    )
    return(dataTable)
  }, server = TRUE)
  

  
  #output: cohortDefinitionIsRowSelected----
  output$cohortDefinitionIsRowSelected <- reactive({
    return(!is.null(getLastTwoRowSelectedInCohortTable()))
  })
  # send output to UI
  shiny::outputOptions(x = output,
                       name = "cohortDefinitionIsRowSelected",
                       suspendWhenHidden = FALSE)
  
  #output: selectedCohortInCohortDefinitionLeft----
  #Show cohort names in UI
  output$selectedCohortInCohortDefinitionLeft <- shiny::renderUI(expr = {
    row <- getLastTwoRowSelectedInCohortTable()[1,]
    if (is.null(row)) {
      return(NULL)
    } else {
      tags$table(height = '60', style = "overflow : auto",
        tags$tr(
          tags$td(
            tags$b( "Selected cohort: ")
          ),
          tags$td(
            row$compoundName
          )
        )
      )
    }
  })
  #output: selectedCohortInCohortDefinitionRight----
  output$selectedCohortInCohortDefinitionRight <- shiny::renderUI(expr = {
    row <- getLastTwoRowSelectedInCohortTable()[2,]
    if (is.null(row)) {
      return(NULL)
    } else {
      tags$table(height = '60', style = "overflow : auto",
        tags$tr(
          tags$td(
           tags$b( "Selected cohort:")
          ),
          tags$td(
            row$compoundName
          )
        )
      )
    }
  })

  
  #output: cohortDetailsTextLeft----
  output$cohortDetailsTextLeft <- shiny::renderUI({
    row <- getSelectedCohortMetaData()[[1]]
    if (is.null(row) || length(row) == 0) {
      return(NULL)
    }
    return(row)
  })
  

  #output: cohortCountsTableForSelectedCohortLeft----
  output$cohortCountsTableForSelectedCohortLeft <-
    DT::renderDataTable(expr = {
      data <- getCountsForSelectedCohortsLeft()
      validate(need(all(!is.null(data),
                        nrow(data) > 0),
                    "There is no inclusion rule data for this cohort."))
      maxCohortSubjects <- max(data$cohortSubjects)
      maxCohortEntries <- max(data$cohortEntries)
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
        searching = TRUE,
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        info = TRUE,
        searchHighlight = TRUE,
        scrollX = TRUE,
        columnDefs = list(minCellCountDef(1:2))
      )
      
      dataTable <- DT::datatable(
        data,
        options = options,
        colnames = colnames(data) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns = 2,
        background = DT::styleColorBar(c(0, maxCohortSubjects), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns = 3,
        background = DT::styleColorBar(c(0, maxCohortEntries), "#ffd699"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      return(dataTable)
    }, server = TRUE)
  
  
  #reactive: getDatabaseIdForSelectedCohortCountLeft----
  getDatabaseIdForSelectedCohortCountLeft <- shiny::reactive(x = {
    idx <- input$cohortCountsTableForSelectedCohortLeft_rows_selected
    if (is.null(idx)) {
      return(NULL)
    }
    return(getCountsForSelectedCohortsLeft()[idx,]$databaseId)
  })
  
  #output: isDatabaseIdFoundForSelectedCohortCountLeft----
  output$isDatabaseIdFoundForSelectedCohortCountLeft <- shiny::reactive(x = {
    return(!is.null(getDatabaseIdForSelectedCohortCountLeft()))
  })
  shiny::outputOptions(x = output,
                       name = "isDatabaseIdFoundForSelectedCohortCountLeft",
                       suspendWhenHidden = FALSE)

  
  #output: inclusionRuleTableForSelectedCohortCountLeft----
  output$inclusionRuleTableForSelectedCohortCountLeft <- DT::renderDataTable(expr = {
    
    table <- getSimplifiedInclusionRuleResultsLeft()
    
    validate(need((nrow(table) > 0),
                  "There is no inclusion rule data for this cohort."))
    
    databaseIds <- unique(table$databaseId)
    cohortCounts <- table %>% 
      dplyr::inner_join(cohortCount,
                        by = c("cohortId", "databaseId")) %>% 
      dplyr::filter(.data$cohortId == getLastTwoRowSelectedInCohortTable()[1,]$cohortId) %>% 
      dplyr::filter(.data$databaseId %in% getDatabaseIdForSelectedCohortCountLeft()) %>% 
      dplyr::select(.data$cohortSubjects) %>% 
      dplyr::pull(.data$cohortSubjects) %>% unique()
    
    databaseIdsWithCount <- paste(databaseIds, "(n = ", format(cohortCounts, big.mark = ","), ")")
    
    table <- table %>%
      dplyr::inner_join(cohortCount %>% 
                          dplyr::select(.data$databaseId, .data$cohortId, .data$cohortSubjects), 
                        by = c('databaseId', 'cohortId')) %>% 
      tidyr::pivot_longer(
        cols = c(
          .data$meetSubjects,
          .data$gainSubjects,
          .data$totalSubjects,
          .data$remainSubjects
        )
      ) %>%
      dplyr::mutate(name = paste0(.data$databaseId, 
                                  "<br>(n = ", 
                                  scales::comma(x = .data$cohortSubjects, accuracy = 1),
                                  ")_", 
                                  .data$name)) %>%
      tidyr::pivot_wider(
        id_cols = c(.data$cohortId, .data$ruleSequenceId, .data$ruleName),
        names_from = .data$name,
        values_from = .data$value
      ) %>%
      dplyr::select(-.data$cohortId)
    
    if (input$cohortDefinitionInclusionRuleTableFilters == "Meet") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Total"),-dplyr::contains("Gain"),-dplyr::contains("Remain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_meetSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    } else if (input$cohortDefinitionInclusionRuleTableFilters == "Totals") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Meet"),-dplyr::contains("Gain"),-dplyr::contains("Remain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_totalSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    } else if (input$cohortDefinitionInclusionRuleTableFilters == "Gain") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Total"),-dplyr::contains("Meet"),-dplyr::contains("Remain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_gainSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    } else if (input$cohortDefinitionInclusionRuleTableFilters == "Remain") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Total"),-dplyr::contains("Meet"),-dplyr::contains("Gain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_remainSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    }  else {
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(tr(
                                            th(rowspan = 2, "Rule Sequence ID"),
                                            th(rowspan = 2, "Rule Name"),
                                            lapply(databaseIdsWithCount, th, colspan = 4, class = "dt-center", style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ),
                                          tr(
                                            lapply(rep(
                                              c("Meet", "Gain", "Remain", "Total"), length(databaseIds)
                                            ), th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ))))
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds) * 4)
      ))
    }
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      columnDefs = list(truncateStringDef(1, 100),
                        columnDefs)
    )
    
    if (input$cohortDefinitionInclusionRuleTableFilters == "All") {
      table <- DT::datatable(
        table,
        options = options,
        colnames = colnames(table) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
    } else {
      table <- DT::datatable(
        table,
        options = options,
        colnames = colnames(table) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
    }
    return(table)
  }, server = TRUE)
  
  #output: saveInclusionRuleTableForSelectedCohortCountLeft----
  output$saveInclusionRuleTableForSelectedCohortCountLeft <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "InclusionRule")
    },
    content = function(file) {
      downloadCsv(x = getSimplifiedInclusionRuleResultsLeft(), fileName = file)
    }
  )
  
  #output: cohortDefinitionTextLeft----
  #!!!!!!!put as collapsible text box in Details - with open
  output$cohortDefinitionTextLeft <- shiny::renderUI(expr = {
    getCirceRenderedExpressionDetails()[1,]$htmlExpressionCohort %>%
      shiny::HTML()
  })
  
  #output: circeRVersionInCohortDefinitionLeft----
  #!!!!!!!put as collapsible text box in Details - with open
  output$circeRVersionInCohortDefinitionLeft <- shiny::renderUI(expr = {
    version <- getCirceRPackageVersion()[[1]]
    if (is.null(version)) {
      return(NULL)
    } else {
      version
    }
  })
  
  #output: cohortDefinitionJsonLeft----
  output$cohortDefinitionJsonLeft <- shiny::renderText({
    row <- getLastTwoRowSelectedInCohortTable()[1,]
    if (is.null(row)) {
      return(NULL)
    } else {
      row$json
    }
  })
  
  #output: cohortDefinitionSqlLeft----
  output$cohortDefinitionSqlLeft <- shiny::renderText({
    row <- getLastTwoRowSelectedInCohortTable()[1,]
    
    if (is.null(row)) {
      return(NULL)
    } else {
      options <- CirceR::createGenerateOptions(
        generateStats = TRUE
      )
      expression <- CirceR::cohortExpressionFromJson(expressionJson = row$json)
      
      if (!is.null(expression)) {
        CirceR::buildCohortQuery(expression = expression, options = options)
      } else {
        return(NULL)
      }
    }
  })
  
  #output: circeRVersionIncohortDefinitionSqlLeft----
  output$circeRVersionIncohortDefinitionSqlLeft <- shiny::renderUI(expr = {
    version <- getCirceRPackageVersion()[[1]]
    if (is.null(version)) {
      return(NULL)
    } else {
      version
    }
  })
  
  
  #output: widthOfLeftPanelBasedOnNoOfRowSelected----
  #Used to set the half view or full view
  widthOfLeftPanelBasedOnNoOfRowSelected <-  shiny::reactive(x = {
    length <- length(input$cohortDefinitionTable_rows_selected)
    if (length == 2) {
      return(6)
    } else {
      return(12)
    }
  })
  
  #output: cohortDefinitionSelectedRowCount----
  output$cohortDefinitionSelectedRowCount <- shiny::reactive({
    return(length(input$cohortDefinitionTable_rows_selected))
  })
  shiny::outputOptions(x = output,
                       name = "cohortDefinitionSelectedRowCount",
                       suspendWhenHidden = FALSE)
  
  
  #Dynamic UI rendering for left side -----
  output$dynamicUIGenerationForCohortSelectedLeft <- shiny::renderUI(expr = {
    shiny::column(
      widthOfLeftPanelBasedOnNoOfRowSelected(),
      shiny::conditionalPanel(
        condition = "output.cohortDefinitionSelectedRowCount > 0 & 
                     output.cohortDefinitionIsRowSelected == true",
        shiny::htmlOutput(outputId = "selectedCohortInCohortDefinitionLeft"),
        shiny::tabsetPanel(
          type = "tab",
          id = "cohortDefinitionOneTabSetPanel",
          shiny::tabPanel(title = "Details",
                          value = "cohortDefinitionOneDetailsTextTabPanel",
                          shiny::htmlOutput("cohortDetailsTextLeft")),
          shiny::tabPanel(title = "Cohort Count",
                          value = "cohortDefinitionOneCohortCountTabPanel",
                          tags$br(),
                          DT::dataTableOutput(outputId = "cohortCountsTableForSelectedCohortLeft"),
                          tags$br(),
                          shiny::conditionalPanel(
                            condition = "output.isDatabaseIdFoundForSelectedCohortCountLeft",
                            # tags$h3("Inclusion Rules"), !!!!!!!!!two headers???
                            tags$table(width = "100%", 
                                       tags$tr(
                                         tags$td(
                                           shiny::radioButtons(
                                             inputId = "cohortDefinitionInclusionRuleTableFilters",
                                             label = "Inclusion Rule Events",
                                             choices = c("All", "Meet", "Gain", "Remain", "Totals"),
                                             selected = "All",
                                             inline = TRUE
                                           )
                                         ),
                                         tags$td(align = "right",
                                                 shiny::downloadButton(
                                                   "saveInclusionRuleTableForSelectedCohortCountLeft",
                                                   label = "",
                                                   icon = shiny::icon("download"),
                                                   style = "margin-top: 5px; margin-bottom: 5px;"
                                                 )
                                         )
                                       )
                            ),
                            DT::dataTableOutput(outputId = "inclusionRuleTableForSelectedCohortCountLeft")
                          )),
          shiny::tabPanel(title = "Readable definition",
                          value = "cohortDefinitionOneTextTabPanel",
                          copyToClipboardButton(toCopyId = "cohortDefinitionTextLeft",
                                                style = "margin-top: 5px; margin-bottom: 5px;"),
                          shiny::htmlOutput("circeRVersionInCohortDefinitionLeft"),
                          shiny::htmlOutput("cohortDefinitionTextLeft")),
          shiny::tabPanel(
            title = "Concept Sets",
            value = "conceptSetOneTabPanel",
            DT::dataTableOutput(outputId = "conceptsetExpressionTableLeft"),
            tags$br(),
            shiny::conditionalPanel(condition = "output.doesConceptSetExpressionFoundForSelectedConceptSetLeft == true",
                                    shinydashboard::box(
                                      title = "Left Panel", ###!!!! selected concept set name
                                      width = NULL,
                                      solidHeader = FALSE,
                                      collapsible = TRUE,
                                      collapsed = FALSE,
                                      shiny::conditionalPanel(condition = "output.doesConceptSetExpressionFoundForSelectedConceptSetLeft == true",
                                                              tags$table(tags$tr(
                                                                tags$td(
                                                                  shinyWidgets::pickerInput(
                                                                    inputId = "choiceForConceptSetDetailsLeft",
                                                                    label = "Vocabulary version choices:",
                                                                    choices = sourcesOfVocabularyTables,
                                                                    multiple = FALSE,
                                                                    width = 200,
                                                                    inline = TRUE,
                                                                    choicesOpt = list(style = rep_len("color: black;", 999)),
                                                                    options = shinyWidgets::pickerOptions(
                                                                      actionsBox = TRUE,
                                                                      liveSearch = TRUE,
                                                                      size = 10,
                                                                      liveSearchStyle = "contains",
                                                                      liveSearchPlaceholder = "Type here to search",
                                                                      virtualScroll = 50
                                                                    )
                                                                  )
                                                                ),
                                                                tags$td(
                                                                  shiny::htmlOutput("personAndRecordCountForConceptSetRowSelectedLeft")
                                                                )
                                                              ),
                                                              tags$tr(
                                                                tags$td(colspan = 2,
                                                                        shiny::radioButtons(
                                                                          inputId = "conceptSetsTypeLeft",
                                                                          label = "",
                                                                          choices = c("Concept Set Expression",
                                                                                      "Resolved",
                                                                                      "Excluded",
                                                                                      "Orphan concepts",
                                                                                      "Json"),
                                                                          selected = "Concept Set Expression",
                                                                          inline = TRUE
                                                                        )
                                                                ))
                                                              )),
                                      shiny::conditionalPanel(
                                        condition = "output.doesConceptSetExpressionFoundForSelectedConceptSetLeft == true &
                                                      input.conceptSetsTypeLeft != 'Resolved' &
                                                      input.conceptSetsTypeLeft != 'Excluded' &
                                                      input.conceptSetsTypeLeft != 'Json' &
                                                      input.conceptSetsTypeLeft != 'Orphan concepts'",
                                        tags$table(width = "100%", 
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveConceptSetsExpressionTableLeft",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ), 
                                        DT::dataTableOutput(outputId = "conceptSetsExpressionTableLeft")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeLeft == 'Resolved'",
                                        tags$table(width = "100%", 
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveResolvedConceptsTableLeft",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ), 
                                        DT::dataTableOutput(outputId = "resolvedConceptsTableLeft")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeLeft == 'Excluded'",
                                        tags$table(width = "100%", 
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveExcludedConceptsTableLeft",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ), 
                                        DT::dataTableOutput(outputId = "excludedConceptsTableLeft")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeLeft == 'Orphan concepts'",
                                        tags$table(width = "100%", 
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveOrphanConceptsTableLeft",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ), 
                                        DT::dataTableOutput(outputId = "cohortDefinitionOrphanConceptTableLeft")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeLeft == 'Json'",
                                        copyToClipboardButton(toCopyId = "conceptsetExpressionJsonLeft",
                                                              style = "margin-top: 5px; margin-bottom: 5px;"),
                                        shiny::verbatimTextOutput(outputId = "conceptsetExpressionJsonLeft"),
                                        tags$head(
                                          tags$style("#conceptsetExpressionJsonLeft { max-height:400px};")
                                        )
                                      )
                                    ))
            
          ), 
            
            shiny::tabPanel(
              title = "JSON",
              value = "cohortDefinitionOneJsonTabPanel",
              copyToClipboardButton("cohortDefinitionJsonLeft", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput("cohortDefinitionJsonLeft"),
              tags$head(
                tags$style("#cohortDefinitionJsonLeft { max-height:400px};")
              )
            ),
            shiny::tabPanel(
              title = "SQL",
              value = "cohortDefinitionOneSqlTabPanel",
              copyToClipboardButton("cohortDefinitionSqlLeft", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::htmlOutput("circeRVersionIncohortDefinitionSqlLeft"),
              shiny::verbatimTextOutput("cohortDefinitionSqlLeft"),
              tags$head(
                tags$style("#cohortDefinitionSqlLeft { max-height:400px};")
              )
            )
          )
        )
      )
  })
  
  #Dynamic UI rendering for right side -----
  output$dynamicUIGenerationForCohortSelectedRight <- shiny::renderUI(expr = {
    shiny::column(
      widthOfLeftPanelBasedOnNoOfRowSelected(),
      shiny::conditionalPanel(
        condition = "output.cohortDefinitionSelectedRowCount == 2 & 
                     output.cohortDefinitionIsRowSelected == true",
        shiny::htmlOutput(outputId = "selectedCohortInCohortDefinitionRight"),
        shiny::tabsetPanel(
          id = "cohortDefinitionTwoTabSetPanel",
          type = "tab",
          shiny::tabPanel(title = "Details",
                          value = "cohortDefinitionTwoDetailsTextTabPanel",
                          shiny::htmlOutput("cohortDetailsTextRight")),
          shiny::tabPanel(title = "Cohort Count",
                          value = "cohortDefinitionTwoCohortCountTabPanel",
                          tags$br(),
                          DT::dataTableOutput(outputId = "cohortCountsTableForSelectedCohortRight"),
                          tags$br(),
                          shiny::conditionalPanel(
                            condition = "output.doesDatabaseIdFoundForSelectedCohortCountRight",
                            # tags$h3("Inclusion Rules"),#!!!two headers 
                            tags$table(width = "100%", 
                                       tags$tr(
                                         tags$td(
                                           shiny::radioButtons(
                                             inputId = "cohortDefinitionSecondInclusionRuleTableFilters",
                                             label = "Inclusion Rule Events",
                                             choices = c("All", "Meet", "Gain", "Remain", "Totals"),
                                             selected = "All",
                                             inline = TRUE
                                           )
                                         ),
                                         tags$td(align = "right",
                                                 shiny::downloadButton(
                                                   "saveInclusionRuleTableForSelectedCohortCountRight",
                                                   label = "",
                                                   icon = shiny::icon("download"),
                                                   style = "margin-top: 5px; margin-bottom: 5px;"
                                                 )
                                         )
                                       )
                            ),
                            DT::dataTableOutput(outputId = "inclusionRuleTableForSelectedCohortCountRight")
                          )),
          shiny::tabPanel(title = "Readable definition",
                          value = "cohortDefinitionTwoTextTabPanel",
                          copyToClipboardButton(toCopyId = "cohortDefinitionTextRight",
                                                style = "margin-top: 5px; margin-bottom: 5px;"),
                          shiny::htmlOutput("circeRVersionInCohortDefinitionRight"),
                          shiny::htmlOutput("cohortDefinitionTextRight")),
          shiny::tabPanel(
            title = "Concept Sets",
            value = "conceptSetTwoTabPanel",
            DT::dataTableOutput(outputId = "conceptsetExpressionTableRight"),
            tags$br(),
            shiny::conditionalPanel(condition = "output.doesConceptSetExpressionFoundForSelectedConceptSetRight == true",
                                    shinydashboard::box(
                                      title = "Right Panel", #!!! selected concept set name
                                      solidHeader = FALSE,
                                      width = NULL,
                                      collapsible = TRUE,
                                      collapsed = FALSE,
                                      shiny::conditionalPanel(condition = "output.doesConceptSetExpressionFoundForSelectedConceptSetRight == true",
                                                              tags$table(tags$tr(
                                                                tags$td(
                                                                  shinyWidgets::pickerInput(
                                                                    inputId = "choiceForConceptSetDetailsRight",
                                                                    label = "Vocabulary version choices:",
                                                                    choices = sourcesOfVocabularyTables,
                                                                    multiple = FALSE,
                                                                    width = 200,
                                                                    inline = TRUE,
                                                                    choicesOpt = list(style = rep_len("color: black;", 999)),
                                                                    options = shinyWidgets::pickerOptions(
                                                                      actionsBox = TRUE,
                                                                      liveSearch = TRUE,
                                                                      size = 10,
                                                                      liveSearchStyle = "contains",
                                                                      liveSearchPlaceholder = "Type here to search",
                                                                      virtualScroll = 50
                                                                    )
                                                                  )
                                                                ),
                                                                tags$td(
                                                                  shiny::htmlOutput("personAndRecordCountForConceptSetRowSelectedRight")
                                                                )
                                                              ),
                                                              tags$tr(
                                                                tags$td(colspan = 2,
                                                                        shiny::radioButtons(
                                                                          inputId = "conceptSetsTypeRight",
                                                                          label = "",
                                                                          choices = c("Concept Set Expression",
                                                                                      "Resolved",
                                                                                      "Excluded",
                                                                                      "Orphan concepts",
                                                                                      "Json"),
                                                                          selected = "Concept Set Expression",
                                                                          inline = TRUE
                                                                        )
                                                                )
                                                              ))),
                                      shiny::conditionalPanel(
                                        condition = "output.doesConceptSetExpressionFoundForSelectedConceptSetRight == true &
                                                      input.conceptSetsTypeRight != 'Resolved' &
                                                      input.conceptSetsTypeRight != 'Excluded' &
                                                      input.conceptSetsTypeRight != 'Json' &
                                                      input.conceptSetsTypeRight != 'Orphan concepts'",
                                        tags$table(width = "100%",
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveConceptSetsExpressionTableRight",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ),
                                        DT::dataTableOutput(outputId = "conceptSetsExpressionTableRight")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeRight == 'Resolved'",
                                        tags$table(width = "100%",
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveResolvedConceptsTableRight",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ),
                                        DT::dataTableOutput(outputId = "resolvedConceptsTableRight")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeRight == 'Excluded'",
                                        tags$table(width = "100%",
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveExcludedConceptsTableRight",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ),
                                        DT::dataTableOutput(outputId = "excludedConceptsTableRight")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeRight == 'Orphan concepts'",
                                        tags$table(width = "100%",
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveCohortDefinitionOrphanConceptTableRight",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ),
                                        DT::dataTableOutput(outputId = "cohortDefinitionOrphanConceptTableRight")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeRight == 'Json'",
                                        copyToClipboardButton(toCopyId = "conceptsetExpressionJsonRight",
                                                              style = "margin-top: 5px; margin-bottom: 5px;"),
                                        shiny::verbatimTextOutput(outputId = "conceptsetExpressionJsonRight"),
                                        tags$head(
                                          tags$style("#conceptsetExpressionJsonRight { max-height:400px};")
                                        )
                                      )
                                    ))
            
          ), 
          
          shiny::tabPanel(
            title = "JSON",
            value = "cohortDefinitionTwoJsonTabPanel",
            copyToClipboardButton("cohortDefinitionJsonRight", style = "margin-top: 5px; margin-bottom: 5px;"),
            shiny::verbatimTextOutput("cohortDefinitionJsonRight"),
            tags$head(
              tags$style("#cohortDefinitionJsonRight { max-height:400px};")
            )
          ),
          shiny::tabPanel(
            title = "SQL",
            value = "cohortDefinitionTwoSqlTabPanel",
            copyToClipboardButton("cohortDefinitionSqlRight", style = "margin-top: 5px; margin-bottom: 5px;"),
            shiny::htmlOutput("circeRVersionInCohortDefinitionSqlRight"),
            shiny::verbatimTextOutput("cohortDefinitionSqlRight"),
            tags$head(
              tags$style("#cohortDefinitionSqlRight { max-height:400px};")
            )
          )
        )
      ))
  })
  

  #output: conceptsetExpressionTableLeft----
  output$conceptsetExpressionTableLeft <- DT::renderDataTable(expr = {
    validate(need((any(!is.null(getConceptSetExpressionAndDetails()),
                       length(getConceptSetExpressionAndDetails()) != 0)),
                  "Cohort definition does not appear to have concept set expression(s)."))
    if (any(is.null(getConceptSetExpressionAndDetails()),
            length(getConceptSetExpressionAndDetails()) == 0)) {
      return(NULL)
    }
    if (!is.null(getConceptSetExpressionAndDetails()[[1]]$conceptSetExpression) &&
        nrow(getConceptSetExpressionAndDetails()[[1]]$conceptSetExpression) > 0) {
      data <- getConceptSetExpressionAndDetails()[[1]]$conceptSetExpression %>%
        dplyr::select(.data$id, .data$name)
    } else {
      return(NULL)
    }
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      info = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE, 
      scrollY = '15vh'
    )
    
    dataTable <- DT::datatable(
      data,
      options = options,
      colnames = colnames(data) %>% camelCaseToTitleCase(),
      rownames = FALSE,
      selection = 'single',
      escape = FALSE,
      filter = "top",
      class = "stripe nowrap compact"
    )
    return(dataTable)
  }, server = TRUE)
  

  
  #output: doesConceptSetExpressionFoundForSelectedConceptSetLeft----
  output$doesConceptSetExpressionFoundForSelectedConceptSetLeft <- shiny::reactive(x = {
    return(!is.null(getConceptSetExpressionLeft()))
  })
  shiny::outputOptions(x = output,
                       name = "doesConceptSetExpressionFoundForSelectedConceptSetLeft",
                       suspendWhenHidden = FALSE)
  
  # output$isDataSourceBelongsToEnvironment <- shiny::reactive(x = {
  #   return(is(dataSource, "environment"))
  # })
  # shiny::outputOptions(x = output,
  #                      name = "isDataSourceBelongsToEnvironment",
  #                      suspendWhenHidden = FALSE)
  
  

  
  #output: saveConceptSetsExpressionTableLeft----
  output$saveConceptSetsExpressionTableLeft <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "ConceptSetsExpression")
    },
    content = function(file) {
      downloadCsv(x = getConceptSetsExpressionDetailsLeft(), fileName = file)
    }
  )
  
  #output: conceptSetsExpressionTableLeft----
  output$conceptSetsExpressionTableLeft <-
    DT::renderDataTable(expr = {
      data <- getConceptSetsExpressionDetailsLeft()
      if (is.null(data)) {
        return(NULL)
      }
      
      data$isExcluded <- ifelse(data$isExcluded,as.character(icon("check")),"")
      data$includeDescendants <- ifelse(data$includeDescendants,as.character(icon("check")),"")
      data$includeMapped <- ifelse(data$includeMapped,as.character(icon("check")),"")
      data$invalidReason <- ifelse(data$invalidReason != 'V',as.character(icon("check")),"")
      
      data <- data %>% 
        dplyr::rename(exclude = .data$isExcluded,
                      descendants = .data$includeDescendants,
                      mapped = .data$includeMapped,
                      invalid = .data$invalidReason)
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        info = TRUE,
        searchHighlight = TRUE,
        scrollX = TRUE,
        scrollY = "20vh",
        columnDefs = list(
          truncateStringDef(1, 80)
        )
      )
      
      dataTable <- DT::datatable(
        data,
        options = options,
        colnames = colnames(data) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        escape = FALSE,
        selection = 'single',
        filter = "top",
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }, server = TRUE)
  

  
  #output: personAndRecordCountForConceptSetRowSelectedLeft----
  output$personAndRecordCountForConceptSetRowSelectedLeft <- shiny::renderUI({
    row <- getSubjectRecordCountForCohortDatabaseCombinationLeft()
    if (is.null(row)) {
      return(NULL)
    } else {
      tags$table(
        tags$tr(
          tags$td(tags$b("Subjects: ")),
          tags$td(scales::comma(row$cohortSubjects, accuracy = 1)),
          tags$td(tags$b("Records: ")),
          tags$td(scales::comma(row$cohortEntries, accuracy = 1))
        )
      )
    }
  })

  
  #output: saveResolvedConceptsTableLeft----
  output$saveResolvedConceptsTableLeft <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "ResolvedConcepts")
    },
    content = function(file) {
      data <- getConceptSetDetailsLeft()
      if (resolvedConcepts %in% names(data)) {
        data <- data$resolvedConcepts
      }
      downloadCsv(x = data, fileName = file)
    }
  )
  
  #output: resolvedConceptsTableLeft----
  output$resolvedConceptsTableLeft <-
    DT::renderDataTable(expr = {
      validate(need(length(getConceptSetExpressionLeft()$id) > 0,
                    "Please select concept set"))
      data <- getConceptSetDetailsLeft()
      if ("resolvedConcepts" %in% names(data)) {
        data <- data$resolvedConcepts
      } else {
        data <- NULL
      }
      validate(need((all(!is.null(data), nrow(data) > 0)),
                    "No resolved concept ids"))
      
      columnDef <- list(
        truncateStringDef(1, 80)
      )
      maxCount <- NULL
      maxSubject <- NULL
      if ("subjects" %in% colnames(data) && "count" %in% colnames(data)) {
        columnDef <- list(
          truncateStringDef(1, 80),minCellCountDef(2:3))
        maxCount <- max(data$count, na.rm = TRUE)
        maxSubject <- max(data$subjects, na.rm = TRUE)
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
        scrollY = "20vh",
        columnDefs = columnDef
      )
      
      dataTable <- DT::datatable(
        data,
        options = options,
        rownames = FALSE,
        colnames = colnames(data) %>% camelCaseToTitleCase(),
        escape = FALSE,
        selection = 'single',
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns =  3,
        background = DT::styleColorBar(c(0, maxSubject), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns =  4,
        background = DT::styleColorBar(c(0, maxCount), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      return(dataTable)
    }, server = TRUE)
  
  #output: saveOrphanConceptsTableLeft----
  output$saveOrphanConceptsTableLeft <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "orphanConcepts")
    },
    content = function(file) {
      data <- getConceptSetDetailsLeft()
      if (orphanConcepts %in% names(data)) {
        data <- data$orphanConcepts
      }
      downloadCsv(x = data, fileName = file)
    }
  )
  
  #output: cohortDefinitionOrphanConceptTableLeft----
  output$cohortDefinitionOrphanConceptTableLeft <- DT::renderDataTable(expr = {
    data <- getConceptSetDetailsLeft()
    if ("orphanConcepts" %in% names(data)) {
      data <- data$orphanConcepts
    }
    validate(need(any(!is.null(data),
                      nrow(data) > 0),
                  "No orphan concepts"))
    
    sketch <- htmltools::withTags(table(
      class = "display",
      thead(
        tr(
          th(rowspan = 2, "Concept ID"),
          th(rowspan = 2, "Concept Name"),
          th(rowspan = 2, "Vocabulary ID"),
          th(rowspan = 2, "Concept Code"),
          lapply(orphanConceptDataDatabaseIds, th, colspan = 2, class = "dt-center")
        ),
        tr(
          lapply(rep(c("Subjects", "Counts"), 
                     length(orphanConceptDataDatabaseIds)), th)
        )
      )
    ))
    
    options = list(pageLength = 10,
                   searching = TRUE,
                   scrollX = TRUE,
                   scrollY = '50vh',
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(truncateStringDef(1, 100),
                                     minCellCountDef(3 + (1:(length(orphanConceptDataDatabaseIds) * 2)))))
    
    table <- DT::datatable(orphanConceptData,
                           options = options,
                           colnames = colnames(orphanConceptData),
                           rownames = FALSE,
                           container = sketch,
                           escape = FALSE,
                           filter = "top",
                           class = "stripe nowrap compact")
    
    table <- DT::formatStyle(table = table,
                             columns =  4 + (1:(length(orphanConceptDataDatabaseIds)*2)),
                             background = DT::styleColorBar(c(0, orphanConceptDataMaxCount), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  #output: conceptsetExpressionJsonLeft----
  output$conceptsetExpressionJsonLeft <- shiny::renderText({
    if (is.null(getConceptSetExpressionLeft())) {
      return(NULL)
    }
    getConceptSetExpressionLeft()$json
  })
  
  
  # Cohort Details/definitions/concept sets/JSON/SQL - right side ----
  ##output: cohortDetailsTextRight----
  output$cohortDetailsTextRight <- shiny::renderUI({
    row <- getSelectedCohortMetaData()
    if (is.null(row) || length(row) == 0) {
      return(NULL)
    }
    if (length(row) == 2) {
      row <- row[[2]]
    }
    return(row)
  })
  
  ##output: cohortCountsTableForSelectedCohortRight----
  output$cohortCountsTableForSelectedCohortRight <- DT::renderDataTable(expr = {
    row <- getLastTwoRowSelectedInCohortTable()[2,]
    if (is.null(row)) {
      return(NULL)
    } else {
      data <- getCountsForSelectedCohortsRight()
      maxCohortSubjects <- max(data$cohortSubjects)
      maxCohortEntries <- max(data$cohortEntries)
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        info = TRUE,
        searchHighlight = TRUE,
        scrollX = TRUE,
        columnDefs = list(minCellCountDef(1:2))
      )
      
      dataTable <- DT::datatable(
        data,
        options = options,
        colnames = colnames(data) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns = 2,
        background = DT::styleColorBar(c(0, maxCohortSubjects), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns = 3,
        background = DT::styleColorBar(c(0, maxCohortEntries), "#ffd699"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      return(dataTable)
    }
  }, server = TRUE)
  
  ##reactive: getDatabaseIdForSelectedCohortCountRight----
  getDatabaseIdForSelectedCohortCountRight <- shiny::reactive(x = {
    idx <- input$cohortCountsTableForSelectedCohortRight_rows_selected
    
    if (is.null(idx)) {
      return(NULL)
    } else {
      subset <- database[idx,] %>% 
        dplyr::select(.data$databaseId)
      if (is.null(subset)) {
        return(NULL)
      } else {
        return(subset)
      }
    }
  })
  
  ##reactive: doesDatabaseIdFoundForSelectedCohortCountRight----
  output$doesDatabaseIdFoundForSelectedCohortCountRight <- shiny::reactive(x = {
    return(!is.null(getDatabaseIdForSelectedCohortCountRight()))
  })
  shiny::outputOptions(x = output,
                       name = "doesDatabaseIdFoundForSelectedCohortCountRight",
                       suspendWhenHidden = FALSE)
  
  

  
  ##output: inclusionRuleTableForSelectedCohortCountRight----
  output$inclusionRuleTableForSelectedCohortCountRight <- DT::renderDataTable(expr = {
   
    table <- getSimplifiedInclusionRuleResultsRight()
    validate(need((nrow(table) > 0),
                  "There is no inclusion rule data for this cohort."))
    
    databaseIds <- unique(table$databaseId)
    cohortCounts <- table %>% 
      dplyr::inner_join(cohortCount,
                        by = c("cohortId", "databaseId")) %>% 
      dplyr::filter(.data$cohortId == getLastTwoRowSelectedInCohortTable()[2,]$cohortId) %>% 
      dplyr::filter(.data$databaseId %in% getDatabaseIdForSelectedCohortCountLeft()) %>% 
      dplyr::select(.data$cohortSubjects) %>% 
      dplyr::pull(.data$cohortSubjects) %>% unique()
    
    databaseIdsWithCount <- paste(databaseIds, "(n = ", format(cohortCounts, big.mark = ","), ")")
    
    table <- table %>%
      dplyr::inner_join(cohortCount %>% 
                          dplyr::select(.data$databaseId, .data$cohortId, .data$cohortSubjects), 
                        by = c('databaseId', 'cohortId')) %>% 
      tidyr::pivot_longer(
        cols = c(
          .data$meetSubjects,
          .data$gainSubjects,
          .data$totalSubjects,
          .data$remainSubjects
        )
      ) %>%
      dplyr::mutate(name = paste0(.data$databaseId, 
                                  "<br>(n = ", 
                                  scales::comma(x = .data$cohortSubjects, accuracy = 1),
                                  ")_", 
                                  .data$name)) %>%
      tidyr::pivot_wider(
        id_cols = c(.data$cohortId, .data$ruleSequenceId, .data$ruleName),
        names_from = .data$name,
        values_from = .data$value
      ) %>%
      dplyr::select(-.data$cohortId)
    
    if (input$cohortDefinitionSecondInclusionRuleTableFilters == "Meet") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Total"),-dplyr::contains("Gain"),-dplyr::contains("Remain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_meetSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    } else if (input$cohortDefinitionSecondInclusionRuleTableFilters == "Totals") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Meet"),-dplyr::contains("Gain"),-dplyr::contains("Remain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_totalSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    } else if (input$cohortDefinitionSecondInclusionRuleTableFilters == "Gain") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Total"),-dplyr::contains("Meet"),-dplyr::contains("Remain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_gainSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    } else if (input$cohortDefinitionSecondInclusionRuleTableFilters == "Remain") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Total"),-dplyr::contains("Meet"),-dplyr::contains("Gain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_remainSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    }  else {
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(tr(
                                            th(rowspan = 2, "Rule Sequence ID"),
                                            th(rowspan = 2, "Rule Name"),
                                            lapply(databaseIdsWithCount, th, colspan = 4, class = "dt-center", style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ),
                                          tr(
                                            lapply(rep(
                                              c("Meet", "Gain", "Remain", "Total"), length(databaseIds)
                                            ), th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ))))
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds) * 4)
      ))
    }
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      columnDefs = list(truncateStringDef(1, 100),
                        columnDefs)
    )
    
    if (input$cohortDefinitionSecondInclusionRuleTableFilters == "All") {
      table <- DT::datatable(
        table,
        options = options,
        colnames = colnames(table) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
    } else {
      table <- DT::datatable(
        table,
        options = options,
        colnames = colnames(table) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
    }
    return(table)
  }, server = TRUE)
  
  ##output: saveInclusionRuleTableForSelectedCohortCountRight----
  output$saveInclusionRuleTableForSelectedCohortCountRight <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "InclusionRule")
    },
    content = function(file) {
      downloadCsv(x = getSimplifiedInclusionRuleResultsRight(), fileName = file)
    }
  )
  
  ##output: circeRVersionInCohortDefinitionRight----
  output$circeRVersionInCohortDefinitionRight <- shiny::renderUI(expr = {
    version <- getCirceRPackageVersion()
    if (is.null(version)) {
      return(NULL)
    }
    if (length(version) > 1) {
      version <- version[[2]]
    }
    return(version)
  })
  
  ##output: cohortDefinitionTextRight----
  output$cohortDefinitionTextRight <- shiny::renderUI(expr = {
    getCirceRenderedExpressionDetails()[2,]$htmlExpressionCohort %>%
      shiny::HTML()
  })
  
  ##output: cohortDefinitionJsonRight----
  output$cohortDefinitionJsonRight <- shiny::renderText({
    row <- getLastTwoRowSelectedInCohortTable()[2,]
    if (is.null(row)) {
      return(NULL)
    } else {
      row$json
    }
  })
  
  ##output: circeRVersionInCohortDefinitionSqlRight----
  output$circeRVersionInCohortDefinitionSqlRight <- shiny::renderUI(expr = {
    version <- getCirceRPackageVersion()[[2]]
    if (is.null(version)) {
      return(NULL)
    } else {
      version
    }
  })
  
  ##output: cohortDefinitionSqlRight----
  output$cohortDefinitionSqlRight <- shiny::renderText({
    row <- getLastTwoRowSelectedInCohortTable()[2,]
    
    if (is.null(row)) {
      return(NULL)
    } else {
      options <- CirceR::createGenerateOptions(
        generateStats = TRUE
      )
      expression <- CirceR::cohortExpressionFromJson(expressionJson = row$json)
      if (!is.null(expression)) {
        CirceR::buildCohortQuery(expression = expression, options = options)
      } else {
        return(NULL)
      }
    }
  })
  
  ##output: conceptsetExpressionTableRight----
  output$conceptsetExpressionTableRight <- DT::renderDataTable(expr = {
    if (length(getConceptSetExpressionAndDetails()) != 2) {
      return(NULL)
    }
    
    if (!is.null(getConceptSetExpressionAndDetails()[[2]]$conceptSetExpression) &&
        nrow(getConceptSetExpressionAndDetails()[[2]]$conceptSetExpression) > 0) {
      data <- getConceptSetExpressionAndDetails()[[2]]$conceptSetExpression %>%
        dplyr::select(.data$id, .data$name)
      
    } else {
      return(NULL)
    }
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      info = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      scrollY = '15vh'
    )
    
    dataTable <- DT::datatable(
      data,
      options = options,
      colnames = colnames(data) %>% camelCaseToTitleCase(),
      rownames = FALSE,
      selection = 'single',
      escape = FALSE,
      filter = "top",
      class = "stripe nowrap compact"
    )
    return(dataTable)
  }, server = TRUE)
  
  
  ##output: doesConceptSetExpressionFoundForSelectedConceptSetRight----
  output$doesConceptSetExpressionFoundForSelectedConceptSetRight <- shiny::reactive(x = {
    return(!is.null(getConceptSetExpressionRight()))
  })
  shiny::outputOptions(x = output,
                       name = "doesConceptSetExpressionFoundForSelectedConceptSetRight",
                       suspendWhenHidden = FALSE)
  
  ##output: conceptSetsExpressionTableRight----
  output$conceptSetsExpressionTableRight <-
    DT::renderDataTable(expr = {
      data <- getConceptSetsExpressionDetailsRight()
      if (is.null(getConceptSetsExpressionDetailsRight())) {
        return(NULL)
      }
      
      data$isExcluded <- ifelse(data$isExcluded,as.character(icon("check")),"")
      data$includeDescendants <- ifelse(data$includeDescendants,as.character(icon("check")),"")
      data$includeMapped <- ifelse(data$includeMapped,as.character(icon("check")),"")
      data$invalidReason <- ifelse(data$invalidReason != 'V',as.character(icon("check")),"")
      
      data <- data %>% 
        dplyr::rename(exclude = .data$isExcluded,
                      descendants = .data$includeDescendants,
                      mapped = .data$includeMapped,
                      invalid = .data$invalidReason)
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        info = TRUE,
        searchHighlight = TRUE,
        scrollX = TRUE,
        scrollY = "20vh",
        columnDefs = list(
          truncateStringDef(1, 80)
        )
      )
      
      dataTable <- DT::datatable(
        data,
        options = options,
        colnames = colnames(data) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        escape = FALSE,
        selection = 'single',
        filter = "top",
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }, server = TRUE)
  
  ##output: saveConceptSetsExpressionTableRight----
  output$saveConceptSetsExpressionTableRight <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "conceptset")
    },
    content = function(file) {
      downloadCsv(x = getConceptSetsExpressionDetailsRight(), 
                  fileName = file)
    }
  )

  
  ##output: personAndRecordCountForConceptSetRowSelectedRight----
  output$personAndRecordCountForConceptSetRowSelectedRight <- shiny::renderUI({
    row <- getSubjectRecordCountForCohortDatabaseCombinationRight()
    if (is.null(row)) {
      return(NULL)
    } else {
      tags$table(
        tags$tr(
          tags$td(tags$b("Subjects: ")),
          tags$td(scales::comma(row$cohortSubjects, accuracy = 1)),
          tags$td(tags$b("Records: ")),
          tags$td(scales::comma(row$cohortEntries, accuracy = 1))
        )
      )
    }
  })
  
  ##reactive: getResoledAndMappedConceptIdsForFilters----
  getResoledAndMappedConceptIdsForFilters <- shiny::reactive({
    validate(need(all(!is.null(getDatabaseIdsFromDropdown()), length(getDatabaseIdsFromDropdown()) > 0), 
                  "No data sources chosen"))
    validate(need(all(!is.null( getCohortIdFromDropdown()),length( getCohortIdFromDropdown()) > 0),
                  "No cohort chosen"))
    output <-
      getResultsResolveMappedConceptSet(
        dataSource = dataSource,
        databaseIds = getDatabaseIdsFromDropdown(),
        cohortIds = getCohortIdFromDropdown()
      )
    if (is.null(output)) {
      return(NULL)
    }
    
    conceptIdsForFilters <- output$resolved %>% 
      dplyr::filter(.data$conceptSetId %in% getConceptSetIds()) %>% 
      dplyr::select(.data$conceptId) %>% 
      dplyr::distinct()
    
    if (!is.null(output$mapped) &&
        nrow(output$mapped) > 0) {
      mappedConceptSetIds <- output$mapped %>% 
        dplyr::filter(.data$conceptSetId %in% getConceptSetIds()) %>% 
        dplyr::select(.data$conceptId) %>% 
        dplyr::distinct()
      
      conceptIdsForFilters <- conceptIdsForFilters %>% 
        dplyr::bind_rows(mappedConceptSetIds) %>%
        dplyr::distinct()
    }
    output <- conceptIdsForFilters %>% 
      dplyr::distinct() %>% 
      dplyr::pull(.data$conceptId)
    return(output)
  })
  
  ##reactive: getResolvedOrMappedConceptSetForAllDatabaseRight----
  getResolvedOrMappedConceptSetForAllDatabaseRight <-
    shiny::reactive(x = {
      row <- getLastTwoRowSelectedInCohortTable()
      if (is.null(row) ||
          is.null(getConceptSetExpressionRight()$id)) {
        return(NULL)
      }
      
      output <-
        getResultsResolveMappedConceptSet(
          dataSource = dataSource,
          databaseIds = database$databaseId,
          cohortIds =  getLastTwoRowSelectedInCohortTable()[2,]$cohortId
        )
      
      if (!is.null(output)) {
        return(output)
      } else {
        return(NULL)
      }
    })
  
  ##output: resolvedConceptsTableRight----
  output$resolvedConceptsTableRight <-
    DT::renderDataTable(expr = {
      validate(need(length(getConceptSetExpressionRight()$id) > 0,
                    "Please select concept set"))
      data <- getResolvedOrMappedConceptRight()
      if ("resolvedConcepts" %in% names(data)) {
        data <- data$resolvedConcepts %>% 
          dplyr::select(-.data$databaseId, -.data$conceptSetId)
      } else {
        data <- NULL
      }
      validate(need(all(!is.null(data), nrow(data) > 0),
                    "No resolved concept ids"))
      columnDef <- list(
        truncateStringDef(1, 80)
      )
      maxCount <- NULL
      maxSubject <- NULL
      if ("subjects" %in% colnames(data) && "count" %in% colnames(data)) {
        columnDef <- list(
          truncateStringDef(1, 80),minCellCountDef(2:3))
        maxCount <- max(data$count, na.rm = TRUE)
        maxSubject <- max(data$subjects, na.rm = TRUE)
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
        scrollY = "20vh",
        columnDefs = columnDef
      )
      
      dataTable <- DT::datatable(
        data,
        options = options,
        rownames = FALSE,
        colnames = colnames(data) %>% camelCaseToTitleCase(),
        escape = FALSE,
        selection = 'single',
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns =  3,
        background = DT::styleColorBar(c(0, maxSubject), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns =  4,
        background = DT::styleColorBar(c(0, maxCount), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      return(dataTable)
    }, server = TRUE)
  
  ##output: saveResolvedConceptsTableRight----
  output$saveResolvedConceptsTableRight <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "resolvedConceptSet")
    },
    content = function(file) {
      data <- getConceptSetDetailsRight()
      if (resolvedConcepts %in% names(data)) {
        data <- data$resolvedConcepts %>% 
          dplyr::select(-.data$databaseId, -.data$conceptSetId)
      }
      downloadCsv(x = data, fileName = file)
    }
  )

  output$cohortDefinitionOrphanConceptTableRight <- DT::renderDataTable(expr = {
    data <- getConceptSetDetailsRight()
    if ("orphanConcepts" %in% names(data)) {
      data <- data$orphanConcepts
    }
    validate(need(any(!is.null(data),
                      nrow(data) > 0),
                  "No orphan concepts"))
    
    sketch <- htmltools::withTags(table(
      class = "display",
      thead(
        tr(
          th(rowspan = 2, "Concept ID"),
          th(rowspan = 2, "Concept Name"),
          th(rowspan = 2, "Vocabulary ID"),
          th(rowspan = 2, "Concept Code"),
          lapply(orphanConceptDataDatabaseIds, th, colspan = 2, class = "dt-center")
        ),
        tr(
          lapply(rep(c("Subjects", "Counts"), length(orphanConceptDataDatabaseIds)), th)
        )
      )
    ))
    
    options = list(pageLength = 10,
                   searching = TRUE,
                   scrollX = TRUE,
                   scrollY = '50vh',
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(truncateStringDef(1, 100),
                                     minCellCountDef(3 + (1:(length(orphanConceptDataDatabaseIds) * 2)))))
    
    table <- DT::datatable(orphanConceptData,
                           options = options,
                           colnames = colnames(orphanConceptData),
                           rownames = FALSE,
                           container = sketch,
                           escape = FALSE,
                           filter = "top",
                           class = "stripe nowrap compact")
    
    table <- DT::formatStyle(table = table,
                             columns =  4 + (1:(length(orphanConceptDataDatabaseIds)*2)),
                             background = DT::styleColorBar(c(0, orphanConceptDataMaxCount), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
    
  }, server = TRUE)
  
  ##output: saveCohortDefinitionOrphanConceptTableRight----
  output$saveCohortDefinitionOrphanConceptTableRight <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "orphanconcepts")
    },
    content = function(file) {
      downloadCsv(x = getFilteredOrphanConceptForConceptSetRowSelectedRight(), 
                  fileName = file)
    }
  )
  
  ##output: conceptsetExpressionJsonRight----
  output$conceptsetExpressionJsonRight <- shiny::renderText({
    if (is.null(getConceptSetExpressionRight())) {
      return(NULL)
    }
    getConceptSetExpressionRight()$json
  })
  
  
  getConceptSetIds <- shiny::reactive(x = {
    return(conceptSets$conceptSetId[conceptSets$conceptSetName  %in% 
                                      input$conceptSetsToFilterCharacterization])
  })
  
  
  
  #Radio button synchronization
  shiny::observeEvent(eventExpr = {
    list(input$conceptSetsTypeLeft, input$cohortDefinitionOneTabSetPanel)
  }, handlerExpr = {
    if (widthOfLeftPanelBasedOnNoOfRowSelected() == 6) {
      if (!is.null(input$conceptSetsTypeLeft)) {
        if (input$conceptSetsTypeLeft == "Concept Set Expression") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeRight", selected = "Concept Set Expression")
        } else if (input$conceptSetsTypeLeft == "Resolved") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeRight", selected = "Resolved")
        } 
        #!!!!!!!!!removed mapped
        else if (input$conceptSetsTypeLeft == "Excluded") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeRight", selected = "Excluded")
        } else if (input$conceptSetsTypeLeft == "Orphan concepts") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeRight", selected = "Orphan concepts")
        } else if (input$conceptSetsTypeLeft == "Json") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeRight", selected = "Json")
        }
      }
      
      if (!is.null(input$cohortDefinitionOneTabSetPanel)) {
        if (input$cohortDefinitionOneTabSetPanel == "cohortDefinitionOneDetailsTextTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionTwoTabSetPanel", selected = "cohortDefinitionTwoDetailsTextTabPanel")
        } else if (input$cohortDefinitionOneTabSetPanel == "cohortDefinitionOneCohortCountTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionTwoTabSetPanel", selected = "cohortDefinitionTwoCohortCountTabPanel")
        } else if (input$cohortDefinitionOneTabSetPanel == "cohortDefinitionOneTextTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionTwoTabSetPanel", selected = "cohortDefinitionTwoTextTabPanel")
        } else if (input$cohortDefinitionOneTabSetPanel == "conceptSetOneTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionTwoTabSetPanel", selected = "conceptSetTwoTabPanel")
        } else if (input$cohortDefinitionOneTabSetPanel == "cohortDefinitionOneJsonTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionTwoTabSetPanel", selected = "cohortDefinitionTwoJsonTabPanel")
        } else if (input$cohortDefinitionOneTabSetPanel == "cohortDefinitionOneSqlTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionTwoTabSetPanel", selected = "cohortDefinitionTwoSqlTabPanel")
        }
      }
    }
  })
  
  shiny::observeEvent(eventExpr = {
    list(input$conceptSetsTypeRight, input$cohortDefinitionTwoTabSetPanel)
  }, handlerExpr = {
    if (widthOfLeftPanelBasedOnNoOfRowSelected() == 6) {
      if (!is.null(input$conceptSetsTypeRight)) {
        if (input$conceptSetsTypeRight == "Concept Set Expression") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeLeft", selected = "Concept Set Expression")
        } else if (input$conceptSetsTypeRight == "Resolved") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeLeft", selected = "Resolved")
        } else if (input$conceptSetsTypeRight == "Excluded") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeLeft", selected = "Excluded")
        } else if (input$conceptSetsTypeRight == "Orphan concepts") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeLeft", selected = "Orphan concepts")
        } else if (input$conceptSetsTypeRight == "Json") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeLeft", selected = "Json")
        }
      }
      
      if (!is.null(input$cohortDefinitionTwoTabSetPanel)) {
        if (input$cohortDefinitionTwoTabSetPanel == "cohortDefinitionTwoDetailsTextTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionOneTabSetPanel", selected = "cohortDefinitionOneDetailsTextTabPanel")
        } else if (input$cohortDefinitionTwoTabSetPanel == "cohortDefinitionTwoCohortCountTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionOneTabSetPanel", selected = "cohortDefinitionOneCohortCountTabPanel")
        } else if (input$cohortDefinitionTwoTabSetPanel == "cohortDefinitionTwoTextTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionOneTabSetPanel", selected = "cohortDefinitionOneTextTabPanel")
        } else if (input$cohortDefinitionTwoTabSetPanel == "conceptSetTwoTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionOneTabSetPanel", selected = "conceptSetOneTabPanel")
        } else if (input$cohortDefinitionTwoTabSetPanel == "cohortDefinitionTwoJsonTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionOneTabSetPanel", selected = "cohortDefinitionOneJsonTabPanel")
        } else if (input$cohortDefinitionTwoTabSetPanel == "cohortDefinitionTwoSqlTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionOneTabSetPanel", selected = "cohortDefinitionOneSqlTabPanel")
        }
      }
    }
  })
  
  #Concept set comparison -----
  ##reactive: getJoinedResolvedOrMappedConceptsList----
  getJoinedResolvedOrMappedConceptsList <- shiny::reactive(x = {
    leftData <- getResolvedOrMappedConeptsLeft()
    rightData <- getResolvedOrMappedConceptRight()
    data <- list(leftData = leftData, rightData = rightData)
    return(data)
  })
  
  ##output: resolvedConceptsPresentInLeft----
  output$resolvedConceptsPresentInLeft <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, "Please select same database for comparison"))
    result <- dplyr::setdiff(getJoinedResolvedOrMappedConceptsList()$leftData, 
                             getJoinedResolvedOrMappedConceptsList()$rightData)
    
    if (all(is.null(result), nrow(result) == 0)) {
      return(NULL)
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        ordering = TRUE,
        paging = TRUE,
        scrollX = TRUE,
        scrollY = scrollYHeight,
        info = TRUE,
        searchHighlight = TRUE
      )
      
      dataTable <- DT::datatable(
        result,
        options = options,
        rownames = FALSE,
        colnames = colnames(result) %>% camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        selection = list(mode = "none"),
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }
  })
  
  
  ##output: resolvedConceptsPresentInRight----
  output$resolvedConceptsPresentInRight <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, "Please select same database for comparison"))
    result <- dplyr::setdiff(getJoinedResolvedOrMappedConceptsList()$rightData, 
                             getJoinedResolvedOrMappedConceptsList()$leftData)
    
    if (nrow(result) == 0) {
      return(NULL)
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        ordering = TRUE,
        paging = TRUE,
        scrollX = TRUE,
        scrollY = scrollYHeight,
        info = TRUE,
        searchHighlight = TRUE
      )
      
      dataTable <- DT::datatable(
        result,
        options = options,
        rownames = FALSE,
        colnames = colnames(result) %>% camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        selection = list(mode = "none"),
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }
  })
  
  ##output: resolvedConceptsPresentInBoth----
  output$resolvedConceptsPresentInBoth <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, "Please select same database for comparison"))
    result <- dplyr::intersect(getJoinedResolvedOrMappedConceptsList()$leftData, 
                               getJoinedResolvedOrMappedConceptsList()$rightData)
    
    if (nrow(result) == 0) {
      return(NULL)
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        ordering = TRUE,
        paging = TRUE,
        scrollX = TRUE,
        scrollY = scrollYHeight,
        info = TRUE,
        searchHighlight = TRUE
      )
      
      dataTable <- DT::datatable(
        result,
        options = options,
        rownames = FALSE,
        colnames = colnames(result) %>% camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        selection = list(mode = "none"),
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }
  })
  
  ##output: resolvedConceptsPresentInEither----
  output$resolvedConceptsPresentInEither <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, "Please select same database for comparison"))
    result <- dplyr::union(getJoinedResolvedOrMappedConceptsList()$leftData,
                           getJoinedResolvedOrMappedConceptsList()$rightData)
    
    if (nrow(result) == 0) {
      return(NULL)
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        ordering = TRUE,
        paging = TRUE,
        scrollX = TRUE,
        scrollY = scrollYHeight,
        info = TRUE,
        searchHighlight = TRUE
      )
      
      dataTable <- DT::datatable(
        result,
        options = options,
        rownames = FALSE,
        colnames = colnames(result) %>% camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        selection = list(mode = "none"),
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }
  })
  
  ##output: mappedConceptsPresentInLeft----
  output$mappedConceptsPresentInLeft <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, "Please select same database for comparison"))
    result <- dplyr::setdiff(getJoinedResolvedOrMappedConceptsList()$leftData, 
                             getJoinedResolvedOrMappedConceptsList()$rightData)
    
    if (any(is.null(result), nrow(result) == 0)) {
      return(NULL)
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        ordering = TRUE,
        paging = TRUE,
        scrollX = TRUE,
        scrollY = scrollYHeight,
        info = TRUE,
        searchHighlight = TRUE
      )
      
      dataTable <- DT::datatable(
        result,
        options = options,
        rownames = FALSE,
        colnames = colnames(result) %>% camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        selection = list(mode = "none"),
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }
  })
  
  ##output: mappedConceptsPresentInRight----
  output$mappedConceptsPresentInRight <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, "Please select same database for comparison"))
    result <- dplyr::setdiff(getJoinedResolvedOrMappedConceptsList()$rightData, 
                             getJoinedResolvedOrMappedConceptsList()$leftData)
    
    if (nrow(result) == 0) {
      return(NULL)
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        ordering = TRUE,
        paging = TRUE,
        scrollX = TRUE,
        scrollY = scrollYHeight,
        info = TRUE,
        searchHighlight = TRUE
      )
      
      dataTable <- DT::datatable(
        result,
        options = options,
        rownames = FALSE,
        colnames = colnames(result) %>% camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        selection = list(mode = "none"),
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }
  })
  
  ##output: mappedConceptsPresentInBoth----
  output$mappedConceptsPresentInBoth <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, "Please select same database for comparison"))
    result <- dplyr::intersect(getJoinedResolvedOrMappedConceptsList()$leftData, 
                               getJoinedResolvedOrMappedConceptsList()$rightData)
    
    if (nrow(result) == 0) {
      return(NULL)
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        ordering = TRUE,
        paging = TRUE,
        scrollX = TRUE,
        scrollY = scrollYHeight,
        info = TRUE,
        searchHighlight = TRUE
      )
      
      dataTable <- DT::datatable(
        result,
        options = options,
        rownames = FALSE,
        colnames = colnames(result) %>% camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        selection = list(mode = "none"),
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }
  })
  
  ##output: mappedConceptsPresentInEither----
  output$mappedConceptsPresentInEither <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, "Please select same database for comparison"))
    result <- dplyr::union(getJoinedResolvedOrMappedConceptsList()$leftData,
                           getJoinedResolvedOrMappedConceptsList()$rightData)
    
    if (nrow(result) == 0) {
      return(NULL)
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        ordering = TRUE,
        paging = TRUE,
        scrollX = TRUE,
        scrollY = scrollYHeight,
        info = TRUE,
        searchHighlight = TRUE
      )
      
      dataTable <- DT::datatable(
        result,
        options = options,
        rownames = FALSE,
        colnames = colnames(result) %>% camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        selection = list(mode = "none"),
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }
  })
  
  ##output: orphanConceptsPresentInLeft----
  output$orphanConceptsPresentInLeft <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, 
                  "Please select same database for comparison"))
    result <- dplyr::setdiff(getPivotOrphanConceptResultLeft(), 
                             getPivotOrphanConceptResultRight())
    orphanConceptDataDatabaseIds <- attr(x = getPivotOrphanConceptResultLeft(), which = 'databaseIds')
    orphanConceptDataMaxCount <- attr(x = getPivotOrphanConceptResultLeft(), which = 'maxCount')
    if (nrow(result) == 0) {
      validate(need(nrow(result) > 0, "No data found"))
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      sketch <- htmltools::withTags(table(
        class = "display",
        thead(
          tr(
            th(rowspan = 2, "Concept ID"),
            th(rowspan = 2, "Concept Name"),
            th(rowspan = 2, "Vocabulary ID"),
            th(rowspan = 2, "Concept Code"),
            lapply(attr(x = getPivotOrphanConceptResultLeft(), 
                        which = "databaseIds"), th, colspan = 2, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Subjects", "Counts"), length(orphanConceptDataDatabaseIds)), th)
          )
        )
      ))
      
      options = list(pageLength = 10,
                     searching = TRUE,
                     scrollX = TRUE,
                     scrollY = '50vh',
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(truncateStringDef(1, 100),
                                       minCellCountDef(3 + (1:(length(orphanConceptDataDatabaseIds) * 2)))))
      
      table <- DT::datatable(result,
                             options = options,
                             colnames = colnames(result),
                             rownames = FALSE,
                             container = sketch,
                             escape = FALSE,
                             filter = "top",
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns =  4 + (1:(length(orphanConceptDataDatabaseIds)*2)),
                               background = DT::styleColorBar(c(0, orphanConceptDataDatabaseIds), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
      return(table)
    }
  })
  
  ##output: orphanConceptsPresentInRight----
  output$orphanConceptsPresentInRight <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, 
                  "Please select same database for comparison"))
    result <- dplyr::setdiff(getPivotOrphanConceptResultRight()$table,
                             getPivotOrphanConceptResultLeft()$table)
    orphanConceptDataDatabaseIds <- attr(x = getPivotOrphanConceptResultRight(), which = 'databaseIds')
    orphanConceptDataMaxCount <- attr(x = getPivotOrphanConceptResultRight(), which = 'maxCount')
    
    if (nrow(result) == 0) {
      validate(need(nrow(result) > 0, "No data found"))
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      sketch <- htmltools::withTags(table(
        class = "display",
        thead(
          tr(
            th(rowspan = 2, "Concept ID"),
            th(rowspan = 2, "Concept Name"),
            th(rowspan = 2, "Vocabulary ID"),
            th(rowspan = 2, "Concept Code"),
            lapply(orphanConceptDataDatabaseIds, th, colspan = 2, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Subjects", "Counts"), length(orphanConceptDataDatabaseIds)), th)
          )
        )
      ))
      
      options = list(pageLength = 10,
                     searching = TRUE,
                     scrollX = TRUE,
                     scrollY = '50vh',
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(truncateStringDef(1, 100),
                                       minCellCountDef(3 + (1:(length(orphanConceptDataDatabaseIds) * 2)))))
      
      table <- DT::datatable(result,
                             options = options,
                             colnames = colnames(result),
                             rownames = FALSE,
                             container = sketch,
                             escape = FALSE,
                             filter = "top",
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns =  4 + (1:(length(orphanConceptDataDatabaseIds)*2)),
                               background = DT::styleColorBar(c(0, orphanConceptDataMaxCount), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
      return(table)
    }
  })
  
  ##output: orphanConceptsPresentInBoth----
  output$orphanConceptsPresentInBoth <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, "Please select same database for comparison"))
    result <- dplyr::intersect(getPivotOrphanConceptResultLeft()$table, 
                               getPivotOrphanConceptResultRight()$table)
    orphanConceptDataDatabaseIds <- attr(x = getPivotOrphanConceptResultLeft(), which = 'databaseIds')
    orphanConceptDataMaxCount <- attr(x = getPivotOrphanConceptResultLeft(), which = 'maxCount')
    
    if (nrow(result) == 0) {
      validate(need(nrow(result) > 0, "No data found"))
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      sketch <- htmltools::withTags(table(
        class = "display",
        thead(
          tr(
            th(rowspan = 2, "Concept ID"),
            th(rowspan = 2, "Concept Name"),
            th(rowspan = 2, "Vocabulary ID"),
            th(rowspan = 2, "Concept Code"),
            lapply(orphanConceptDataDatabaseIds, th, colspan = 2, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Subjects", "Counts"), length(orphanConceptDataDatabaseIds)), th)
          )
        )
      ))
      
      options = list(pageLength = 10,
                     searching = TRUE,
                     scrollX = TRUE,
                     scrollY = '50vh',
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(truncateStringDef(1, 100),
                                       minCellCountDef(3 + (1:(length(orphanConceptDataDatabaseIds) * 2)))))
      
      table <- DT::datatable(result,
                             options = options,
                             colnames = colnames(result),
                             rownames = FALSE,
                             container = sketch,
                             escape = FALSE,
                             filter = "top",
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns =  4 + (1:(length(orphanConceptDataDatabaseIds)*2)),
                               background = DT::styleColorBar(c(0, orphanConceptDataMaxCount), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
      return(table)
    }
  })
  
  ##output: orphanConceptsPresentInEither----
  output$orphanConceptsPresentInEither <- DT::renderDT({
    validate(need(input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight, "Please select same database for comparison"))
    result <- dplyr::union(getPivotOrphanConceptResultLeft()$table, 
                           getPivotOrphanConceptResultRight()$table)
    orphanConceptDataDatabaseIds <- attr(x = getPivotOrphanConceptResultLeft(), which = 'databaseIds')
    orphanConceptDataMaxCount <- attr(x = getPivotOrphanConceptResultLeft(), which = 'maxCount')
    
    if (nrow(result) == 0) {
      validate(need(nrow(result) > 0, "No data found"))
    } else {
      if (nrow(result) < 20) {
        scrollYHeight <- TRUE
      } else {
        scrollYHeight <- '25vh'
      }
      
      sketch <- htmltools::withTags(table(
        class = "display",
        thead(
          tr(
            th(rowspan = 2, "Concept ID"),
            th(rowspan = 2, "Concept Name"),
            th(rowspan = 2, "Vocabulary ID"),
            th(rowspan = 2, "Concept Code"),
            lapply(orphanConceptDataDatabaseIds, th, colspan = 2, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Subjects", "Counts"), length(orphanConceptDataDatabaseIds)), th)
          )
        )
      ))
      
      options = list(pageLength = 10,
                     searching = TRUE,
                     scrollX = TRUE,
                     scrollY = '50vh',
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(truncateStringDef(1, 100),
                                       minCellCountDef(3 + (1:(length(orphanConceptDataDatabaseIds) * 2)))))
      
      table <- DT::datatable(result,
                             options = options,
                             colnames = colnames(result),
                             rownames = FALSE,
                             container = sketch,
                             escape = FALSE,
                             filter = "top",
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns =  4 + (1:(length(orphanConceptDataDatabaseIds)*2)),
                               background = DT::styleColorBar(c(0, orphanConceptDataMaxCount), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
      return(table)
    }
  })
  
  # Cohort Counts Tab -----
  ##reactive: getSortedCohortCountResult----
  getSortedCohortCountResult <- shiny::reactive(x = {
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    validate(need(length( getCohortIdsFromDropdown()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('cohortCount'))) {
      return(NULL)
    }
    data <- getResultsCohortCount(
      dataSource = dataSource,
      databaseIds = getDatabaseIdsFromDropdown(),
      cohortIds =  getCohortIdsFromDropdown()
    ) 
    validate(need(all(!is.null(data) && nrow(data) > 0), "No data on cohort counts."))
    
    data <- data %>% 
      addShortName(cohort) %>%
      dplyr::arrange(.data$shortName, .data$databaseId)
    return(data)
  })
  
  ##output: saveCohortCountsTable----
  output$saveCohortCountsTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "cohortCount")
    },
    content = function(file) {
      downloadCsv(x = getSortedCohortCountResult(), 
                  fileName = file)
    }
  )
  
  ##output: cohortCountsTable----
  output$cohortCountsTable <- DT::renderDataTable(expr = {
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    validate(need(length( getCohortIdsFromDropdown()) > 0, "No cohorts chosen"))
    data <- getSortedCohortCountResult() %>%
      dplyr::select(
        .data$databaseId,
        .data$shortName,
        .data$cohortSubjects,
        .data$cohortEntries,
        .data$cohortId
      ) %>%
      dplyr::rename(cohort = .data$shortName) #%>%
    # dplyr::mutate(cohort = as.factor(.data$cohort))
    
    if (nrow(data) == 0) {
      return(tidyr::tibble("There is no data on any cohort"))
    }
    
    databaseIds <- sort(unique(data$databaseId))
    
    if (input$cohortCountsTableColumnFilter == "Both") {
      table <- dplyr::full_join(
        data %>%
          dplyr::select(.data$cohort, .data$databaseId,
                        .data$cohortSubjects) %>%
          dplyr::mutate(columnName = paste0(.data$databaseId, "_subjects")) %>%
          tidyr::pivot_wider(
            id_cols = .data$cohort,
            names_from = columnName,
            values_from = .data$cohortSubjects,
            values_fill = 0
          ),
        data %>%
          dplyr::select(.data$cohort, .data$databaseId,
                        .data$cohortEntries) %>%
          dplyr::mutate(columnName = paste0(.data$databaseId, "_entries")) %>%
          tidyr::pivot_wider(
            id_cols = .data$cohort,
            names_from = columnName,
            values_from = .data$cohortEntries,
            values_fill = 0
          ),
        by = c("cohort")
      )
      
      table <- table %>%
        dplyr::select(order(colnames(table))) %>%
        dplyr::relocate(.data$cohort) %>%
        dplyr::arrange(.data$cohort)
      
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(tr(
                                            th(rowspan = 2, "Cohort"),
                                            lapply(databaseIds, th, colspan = 2, class = "dt-center", style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ),
                                          tr(
                                            lapply(rep(
                                              c("Records", "Subjects"), length(databaseIds)
                                            ), th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ))))
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
        scrollY = "30vh",
        columnDefs = list(minCellCountDef(1:(
          2 * length(databaseIds)
        )))
      )
      
      dataTable <- DT::datatable(
        table,
        options = options,
        selection = "single",
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      for (i in 1:length(databaseIds)) {
        dataTable <- DT::formatStyle(
          table = dataTable,
          columns = i * 2,
          background = DT::styleColorBar(c(0, max(
            table[, i * 2], na.rm = TRUE
          )), "lightblue"),
          backgroundSize = "98% 88%",
          backgroundRepeat = "no-repeat",
          backgroundPosition = "center"
        )
        dataTable <- DT::formatStyle(
          table = dataTable,
          columns = i * 2 + 1,
          background = DT::styleColorBar(c(0, max(
            table[, i * 2 + 1], na.rm = TRUE
          )), "#ffd699"),
          backgroundSize = "98% 88%",
          backgroundRepeat = "no-repeat",
          backgroundPosition = "center"
        )
      }
    } else if (input$cohortCountsTableColumnFilter == "Subjects Only" || input$cohortCountsTableColumnFilter == "Records Only") {
      
      if (input$cohortCountsTableColumnFilter == "Subjects Only") {
        maxValue <- max(data$cohortSubjects)
        table <- data %>%
          dplyr::select(.data$cohort, .data$databaseId,
                        .data$cohortSubjects) %>%
          dplyr::mutate(columnName = paste0(.data$databaseId)) %>%
          dplyr::arrange(.data$cohort, .data$databaseId) %>%
          tidyr::pivot_wider(
            id_cols = .data$cohort,
            names_from = columnName,
            values_from = .data$cohortSubjects,
            values_fill = 0
          )
      } else {
        maxValue <- max(data$cohortEntries)
        table <- data %>%
          dplyr::select(.data$cohort, .data$databaseId,
                        .data$cohortEntries) %>%
          dplyr::mutate(columnName = paste0(.data$databaseId)) %>%
          dplyr::arrange(.data$cohort, .data$databaseId) %>%
          tidyr::pivot_wider(
            id_cols = .data$cohort,
            names_from = columnName,
            values_from = .data$cohortEntries,
            values_fill = 0
          )
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
        scrollY = "30vh",
        columnDefs = list(minCellCountDef(1:(
          length(databaseIds)
        )))
      )
      
      dataTable <- DT::datatable(
        table,
        options = options,
        selection = "single",
        colnames = colnames(table) %>% 
          camelCaseToTitleCase(),
        rownames = FALSE,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns = 1 + 1:(length(databaseIds)),
        background = DT::styleColorBar(c(0, maxValue), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      
    }
    return(dataTable)
  }, server = TRUE)
  
  ##reactive: doesCohortCountTableHasData----
  output$doesCohortCountTableHasData <- shiny::reactive({
    return(nrow(getSortedCohortCountResult()) > 0)
  })
  
  shiny::outputOptions(output,
                       "doesCohortCountTableHasData",
                       suspendWhenHidden = FALSE)
  
  ##output: getCohortIdOnCohortCountRowSelect----
  getCohortIdOnCohortCountRowSelect <- reactive({
    idx <- input$cohortCountsTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      subset <- getSortedCohortCountResult() %>%  
        dplyr::distinct(.data$cohortId)
      
      if (!is.null(subset)) {
        return(subset[idx,])
      } else {
        return(NULL)
      }
    }
    
  })
  
  ##output: doesCohortIdFoundOnCohortCountRowSelect----
  output$doesCohortIdFoundOnCohortCountRowSelect <- reactive({
    return(!is.null(getCohortIdOnCohortCountRowSelect()))
  })
  
  outputOptions(output,
                "doesCohortIdFoundOnCohortCountRowSelect",
                suspendWhenHidden = FALSE)
  
  ##output: inclusionRuleStatForCohortSeletedTable----
  output$inclusionRuleStatForCohortSeletedTable <- DT::renderDataTable(expr = {
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    validate(need(
      nrow(getCohortIdOnCohortCountRowSelect()) > 0,
      "No cohorts chosen"
    ))
    table <- getResultsInclusionRuleStatistics(
      dataSource = dataSource,
      cohortIds = getCohortIdOnCohortCountRowSelect()$cohortId,
      databaseIds = getDatabaseIdsFromDropdown()
    )
    
    validate(need((nrow(table) > 0),
                  "There is no inclusion rule data for this cohort."))
    
    databaseIds <- unique(table$databaseId)
    
    table <- table %>%
      tidyr::pivot_longer(
        cols = c(
          .data$meetSubjects,
          .data$gainSubjects,
          .data$totalSubjects,
          .data$remainSubjects
        )
      ) %>%
      dplyr::mutate(name = paste0(databaseId, "_", .data$name)) %>%
      tidyr::pivot_wider(
        id_cols = c(.data$cohortId, .data$ruleSequenceId, .data$ruleName),
        names_from = .data$name,
        values_from = .data$value
      ) %>%
      dplyr::select(-.data$cohortId)
    
    
    
    sketch <- htmltools::withTags(table(class = "display",
                                        thead(tr(
                                          th(rowspan = 2, "Rule Sequence ID"),
                                          th(rowspan = 2, "Rule Name"),
                                          lapply(
                                            databaseIds,
                                            th,
                                            colspan = 4,
                                            class = "dt-center",
                                            style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                          )
                                        ),
                                        tr(
                                          lapply(rep(
                                            c("Meet", "Gain", "Remain", "Total"), length(databaseIds)
                                          ), th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                        ))))
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
      searching = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      columnDefs = list(truncateStringDef(1, 100),
                        minCellCountDef(1 + (1:(
                          length(databaseIds) * 4
                        ))))
    )
    table <- DT::datatable(
      table,
      options = options,
      colnames = colnames(table) %>% camelCaseToTitleCase(),
      rownames = FALSE,
      container = sketch,
      escape = FALSE,
      filter = "top",
      class = "stripe nowrap compact"
    )
    return(table)
  }, server = TRUE)
  
  # Incidence rate -------
  ##reactive: getIncidenceRateData----
  getIncidenceRateData <- reactive({
    if (input$tabs == "incidenceRate") {
      validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
      validate(need(length( getCohortIdsFromDropdown()) > 0, "No cohorts chosen"))
      if (all(is(dataSource, "environment"), !exists('incidenceRate'))) {
        return(NULL)
      }
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Getting incidence rate data."), value = 0)
      
      data <- getResultsIncidenceRate(
        dataSource = dataSource,
        cohortIds =  getCohortIdsFromDropdown(),
        databaseIds = getDatabaseIdsFromDropdown())
      return(data)
    } else {
      return(NULL)
    }
  })
  
  ##reactive: getFilteredIncidenceRateData----
  getFilteredIncidenceRateData <- reactive({
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    data <- getIncidenceRateData()
    if (any(is.null(data), nrow(data) == 0)) {return(NULL)}
    
    if (stratifyByGender) {
      data <- data %>% 
        dplyr::filter(.data$gender != '')
    }
    if (stratifyByAge) {
      data <- data %>% 
        dplyr::filter(.data$ageGroup != '')
    }
    if (stratifyByCalendarYear) {
      data <- data %>% 
        dplyr::filter(.data$calendarYear != '')
    }
    
    if (!is.na(input$minPersonYear) && !is.null(input$minPersonYear)) {
      data <- data %>% 
        dplyr::filter(.data$personYears >= input$minPersonYear)
    }
    if (!is.na(input$minSubjetCount) && !is.null(input$minSubjetCount)) {
      data <- data %>% 
        dplyr::filter(.data$cohortCount >= input$minSubjetCount)
    }
    if (any(is.null(data), nrow(data) == 0)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::mutate(incidenceRate = dplyr::case_when(.data$incidenceRate < 0 ~ 0,
                                                     TRUE ~ .data$incidenceRate))
    return(data)
  })
  
  ##update incidenceRateAgeFilter----
  shiny::observe({
    if (!is.null(getIncidenceRateData()) &&
        nrow(getIncidenceRateData()) > 0) {
      ageFilter <- getIncidenceRateData() %>%
        dplyr::select(.data$ageGroup) %>%
        dplyr::filter(.data$ageGroup != "NA", !is.na(.data$ageGroup)) %>%
        dplyr::distinct() %>%
        dplyr::arrange(as.integer(sub(
          pattern = '-.+$', '', x = .data$ageGroup
        )))
      
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "incidenceRateAgeFilter",
        selected = ageFilter$ageGroup,
        choices = ageFilter$ageGroup,
        choicesOpt = list(style = rep_len("color: black;", 999))
      )
    }
  })
  
  ##update incidenceRateGenderFilter----
  shiny::observe({
    if (!is.null(getIncidenceRateData()) &&
        nrow(getIncidenceRateData()) > 0) {
      genderFilter <- getIncidenceRateData() %>%
        dplyr::select(.data$gender) %>%
        dplyr::filter(.data$gender != "NA",
                      !is.na(.data$gender)) %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$gender)
      
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "incidenceRateGenderFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = genderFilter$gender,
        selected = genderFilter$gender
      )
    }
  })
  
  ##update incidenceRateCalendarFilter----
  shiny::observe({
    if (!is.null(getIncidenceRateData()) &&
        nrow(getIncidenceRateData()) > 0) {
      calendarFilter <- getIncidenceRateData() %>%
        dplyr::select(.data$calendarYear) %>%
        dplyr::filter(.data$calendarYear != "NA",
                      !is.na(.data$calendarYear)) %>%
        dplyr::distinct(.data$calendarYear) %>%
        dplyr::arrange(.data$calendarYear)
      
      minValue <- min(calendarFilter$calendarYear)
      
      maxValue <- max(calendarFilter$calendarYear)
      
      shiny::updateSliderInput(
        session = session,
        inputId = "incidenceRateCalendarFilter",
        min = minValue,
        max = maxValue,
        value = c(2010, maxValue)
      )
      
      minIncidenceRateValue <- round(min(getIncidenceRateData()$incidenceRate),digits = 2)
      
      maxIncidenceRateValue <- round(max(getIncidenceRateData()$incidenceRate),digits = 2)
      
      shiny::updateSliderInput(
        session = session,
        inputId = "YscaleMinAndMax",
        min = 0,
        max = maxIncidenceRateValue,
        value = c(minIncidenceRateValue, maxIncidenceRateValue),
        step = round((maxIncidenceRateValue - minIncidenceRateValue)/5,digits = 2)
      )
    }
  })
  
  ##reactiveVal: incidenceRateAgeFilterValues----
  incidenceRateAgeFilterValues <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$incidenceRateAgeFilter_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$incidenceRateAgeFilter_open) ||
        !is.null(input$tabs)) {
      incidenceRateAgeFilterValues(input$incidenceRateAgeFilter)
    }
  })
  
  ##reactiveVal: incidenceRateGenderFilterValues----
  incidenceRateGenderFilterValues <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$incidenceRateGenderFilter_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$incidenceRateGenderFilter_open) ||
        !is.null(input$tabs)) {
      incidenceRateGenderFilterValues(input$incidenceRateGenderFilter)
    }
  })
  
  ##reactive: getIncidenceRateFilteredOnCalendarFilterValue----
  getIncidenceRateFilteredOnCalendarFilterValue <- shiny::reactive({
    calendarFilter <- getIncidenceRateData() %>%
      dplyr::select(.data$calendarYear) %>%
      dplyr::filter(.data$calendarYear != "NA",
                    !is.na(.data$calendarYear)) %>%
      dplyr::distinct(.data$calendarYear) %>%
      dplyr::arrange(.data$calendarYear)
    calendarFilter <-
      calendarFilter[calendarFilter$calendarYear >= input$incidenceRateCalendarFilter[1] &
                       calendarFilter$calendarYear <= input$incidenceRateCalendarFilter[2], , drop = FALSE] %>%
      dplyr::pull(.data$calendarYear)
    return(calendarFilter)
  })
  
  ##reactive: getIncidenceRateFilteredOnYScaleFilterValue----
  getIncidenceRateFilteredOnYScaleFilterValue <- shiny::reactive({
    incidenceRateFilter <- getIncidenceRateData() %>%
      dplyr::select(.data$incidenceRate) %>%
      dplyr::filter(.data$incidenceRate != "NA",
                    !is.na(.data$incidenceRate)) %>%
      dplyr::distinct(.data$incidenceRate) %>%
      dplyr::arrange(.data$incidenceRate)
    incidenceRateFilter <-
      incidenceRateFilter[incidenceRateFilter$incidenceRate >= input$YscaleMinAndMax[1] &
                            incidenceRateFilter$incidenceRate <= input$YscaleMinAndMax[2],] %>%
      dplyr::pull(.data$incidenceRate)
    return(incidenceRateFilter)
  })
  
  ##output: saveIncidenceRatePlot----
  output$saveIncidenceRatePlot <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "IncidenceRate")
    },
    content = function(file) {
      downloadCsv(x = getIncidenceRateData(), 
                  fileName = file)
    }
  )
  
  ##output: incidenceRatePlot----
  output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    validate(need(length( getCohortIdsFromDropdown()) > 0, "No cohorts chosen"))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Rendering incidence rate plot."), value = 0)
    
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    shiny::withProgress(
      message = paste(
        "Building incidence rate plot data for ",
        length( getCohortIdsFromDropdown()),
        " cohorts and ",
        length(getDatabaseIdsFromDropdown()),
        " databases"
      ),{
        data <- getFilteredIncidenceRateData()
        
        validate(need(all(!is.null(data), nrow(data) > 0), 
                      paste0("No data for this combination")))
        
        if (stratifyByAge && !"All" %in% incidenceRateAgeFilterValues()) {
          data <- data %>%
            dplyr::filter(.data$ageGroup %in% incidenceRateAgeFilterValues())
        }
        if (stratifyByGender &&
            !"All" %in% incidenceRateGenderFilterValues()) {
          data <- data %>%
            dplyr::filter(.data$gender %in% incidenceRateGenderFilterValues())
        }
        if (stratifyByCalendarYear) {
          data <- data %>%
            dplyr::filter(.data$calendarYear %in% getIncidenceRateFilteredOnCalendarFilterValue())
        }
        if (input$irYscaleFixed) {
          data <- data %>%
            dplyr::filter(.data$incidenceRate %in% getIncidenceRateFilteredOnYScaleFilterValue())
        }
        
        if (all(!is.null(data), nrow(data) > 0)) {
          plot <- plotIncidenceRate(
            data = data,
            cohortCount = cohortCount,
            shortNameRef = cohort,
            stratifyByAgeGroup = stratifyByAge,
            stratifyByGender = stratifyByGender,
            stratifyByCalendarYear = stratifyByCalendarYear,
            yscaleFixed = input$irYscaleFixed
          )
          return(plot)
        }
      },detail = "Please Wait"
    )
  })
  
  # Time Series -----
  ##reactive: getTimeSeriesData and Tssible data ----
  timeSeriesTssibleData <- shiny::reactiveVal(NULL)
  getTimeSeriesData <- reactive({
    if (input$tabs == "timeSeries") {
      validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
      validate(need(length( getCohortIdsFromDropdown()) > 0, "No cohorts chosen"))
      if (all(is(dataSource, "environment"), !exists('timeSeries'))) {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Getting time series data."), value = 0)
      
      data <- getResultsFixedTimeSeries(
        dataSource = dataSource,
        cohortIds =  getCohortIdsFromDropdown(),
        databaseIds = getDatabaseIdsFromDropdown()
      )
      return(data)
    } else {
      return(NULL)
    }
  })
  
  ##reactive: getFilteredTimeSeriesData - calendarInterval + range ----
  getFilteredTimeSeriesData <- reactive({
    calendarIntervalFirstLetter <- tolower(substr(input$timeSeriesFilter,1,1))
    data <- getTimeSeriesData()
    data <- data[[calendarIntervalFirstLetter]]
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    data <- data[as.character(data$periodBegin) >= input$timeSeriesPeriodRangeFilter[1] &
           as.character(data$periodBegin) <= input$timeSeriesPeriodRangeFilter[2],]
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    
    data <- data  %>%
      tsibble::fill_gaps(
        records = 0,
        subjects = 0,
        personDays = 0,
        recordsStart = 0,
        subjectsStart = 0,
        recordsEnd = 0,
        subjectsEnd = 0
      )
    
    timeSeriesTssibleData(data)
    
    if (calendarIntervalFirstLetter == 'y') {
      data <- data %>% 
        dplyr::mutate(periodBeginRaw = as.Date(paste0(as.character(.data$periodBegin), '-01-01')))
    } else {
      data <- data %>% 
        dplyr::mutate(periodBeginRaw = as.Date(.data$periodBegin))
    }
    
    if (nrow(data) == 0) {
      return(NULL)
    }
    return(data)
  })
  
  ##reactive: getTimeSeriesDescription----
  getTimeSeriesDescription <- shiny::reactive({
    data <- getTimeSeriesData()
    if (any(is.null(data), nrow(data) == 0)) {
      return(NULL)
    }
    calendarIntervalFirstLetter <- tolower(substr(input$timeSeriesFilter,1,1))
    
    data <- data[[calendarIntervalFirstLetter]]
    timeSeriesDescription <- attr(x = data,which = "timeSeriesDescription")
    return(timeSeriesDescription)
  })
  
  ##output: timeSeriesTypeLong----
  output$timeSeriesTypeLong <- shiny::renderUI({
    timeSeriesDescription <- getTimeSeriesDescription()
    if (any(is.null(timeSeriesDescription), 
            nrow(timeSeriesDescription) == 0)) {
      return(NULL)
    }
    
    seriesTypeLong <- timeSeriesDescription %>% 
      dplyr::filter(.data$seriesTypeShort %in% input$timeSeriesTypeFilter) %>% 
      dplyr::pull(.data$seriesTypeLong) %>% 
      unique()
    
    return(seriesTypeLong)
    
  })
  
  ##update: timeSeriesTypeFilter----
  shiny::observe({
    
    timeSeriesDescription <- getTimeSeriesDescription()
    if (any(is.null(timeSeriesDescription), 
            nrow(timeSeriesDescription) == 0)) {
      return(NULL)
    }
    seriesTypeShort <- timeSeriesDescription %>% 
      dplyr::pull(.data$seriesTypeShort) %>% 
      unique()
  
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "timeSeriesTypeFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = seriesTypeShort
    )
  })
  
  ##update: timeSeriesPeriodRangeFilter----
  shiny::observe({
    calendarIntervalFirstLetter <- tolower(substr(input$timeSeriesFilter,1,1))
    data <- getTimeSeriesData()
    data <- data[[calendarIntervalFirstLetter]]
    if (any(is.null(data), nrow(data) == 0)) {
      return(NULL)
    }
    minValue <- as.integer(strsplit(min(as.character(data$periodBegin))," ")[[1]][1])
    maxValue <- as.integer(strsplit(max(as.character(data$periodBegin))," ")[[1]][1])
    
    shiny::updateSliderInput(
      session = session,
      inputId = "timeSeriesPeriodRangeFilter",
      min = minValue,
      max = maxValue,
      value = c(minValue, maxValue)
    )
  })
  
  ##output: timeSeriesTable----
  output$timeSeriesTable <- DT::renderDataTable({
    
    timeSeriesDescription <- getTimeSeriesDescription()
    
    validate(need(all(!is.null(timeSeriesDescription),
                      nrow(timeSeriesDescription) > 0,
                  !is.null(getFilteredTimeSeriesData()),
                  nrow(getFilteredTimeSeriesData()) > 0),
                  "No timeseries data for the combination."))
    data <- getFilteredTimeSeriesData() %>% 
      dplyr::inner_join(timeSeriesDescription)
    validate(need(all(!is.null(data),
                      nrow(data) > 0),
                  "No timeseries data for the combination."))
    data <- data %>% 
      dplyr::filter(.data$seriesTypeShort %in% input$timeSeriesTypeFilter) %>% 
      dplyr::select(-.data$seriesType,-.data$seriesTypeShort,-.data$seriesTypeLong) %>% 
      dplyr::mutate(periodBegin = .data$periodBeginRaw) %>% 
      dplyr::relocate(.data$periodBegin) %>% 
      dplyr::arrange(.data$periodBegin) %>% 
      dplyr::select(-.data$periodBeginRaw)
    validate(need(all(!is.null(data),
                      nrow(data) > 0),
                  "No timeseries data for the combination."))
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      ordering = TRUE,
      paging = TRUE,
      scrollX = TRUE,
      info = TRUE,
      searchHighlight = TRUE
    )
    
    dataTable <- DT::datatable(
      data,
      options = options,
      rownames = FALSE,
      colnames = colnames(data) %>% camelCaseToTitleCase(),
      escape = FALSE,
      filter = "top",
      selection = list(mode = "multiple", target = "row"),
      class = "stripe compact"
    )
    return(dataTable)
  })
  
  ##output: timeSeriesPlot----
  output$timeSeriesPlot <- ggiraph::renderggiraph({
    
    timeSeriesDescription <- getTimeSeriesDescription()
    
    validate(need(all(!is.null(timeSeriesDescription),
                      nrow(timeSeriesDescription) > 0,
                      !is.null(getFilteredTimeSeriesData()),
                      nrow(getFilteredTimeSeriesData()) > 0),
                  "No timeseries data for the combination."))
    data <- timeSeriesTssibleData() %>% 
      dplyr::inner_join(timeSeriesDescription)
    
    
    data <- data %>% 
      dplyr::filter(.data$seriesTypeShort %in% input$timeSeriesTypeFilter) %>% 
      dplyr::select(-.data$seriesType)
    
    validate(need(nrow(data) > 0,
                  "No timeseries data for the combination."))
    
    plot <- plotTimeSeries(data, titleCaseToCamelCase(input$timeSeriesPlotFilters),input$timeSeriesFilter,input$timeSeriesPlotCategory)
    
    # return(plot)
  })
  
  ##output: getTimeDistributionData----
  getTimeDistributionData <- reactive({
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    validate(need(length( getCohortIdsFromDropdown()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('timeDistribution'))) {
      return(NULL)
    }
    data <- getResultsTimeDistribution(
      dataSource = dataSource,
      cohortIds =  getCohortIdsFromDropdown(),
      databaseIds = getDatabaseIdsFromDropdown()
    )
    return(data)
  })
  
  ##output: timeSeriesPlot----
  output$timeDistributionPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    data <- getTimeDistributionData()
    validate(need(nrow(data) > 0, paste0("No data for this combination")))
    plot <- plotTimeDistribution(data = data, shortNameRef = cohort)
    return(plot)
  })
  
  ##output: saveTimeDistributionTable----
  output$saveTimeDistributionTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "timeDistribution")
    },
    content = function(file) {
      downloadCsv(x = getTimeDistributionData(), 
                  fileName = file)
    }
  )
  
  ##output: timeDistributionTable----
  output$timeDistributionTable <- DT::renderDataTable(expr = {
    data <- getTimeDistributionData()  %>%
      addShortName(cohort) %>%
      dplyr::arrange(.data$databaseId, .data$cohortId) %>%
      dplyr::mutate(
        # shortName = as.factor(.data$shortName),
        databaseId = as.factor(.data$databaseId)
      ) %>%
      dplyr::select(
        Database = .data$databaseId,
        Cohort = .data$shortName,
        TimeMeasure = .data$timeMetric,
        Average = .data$averageValue,
        SD = .data$standardDeviation,
        Min = .data$minValue,
        P10 = .data$p10Value,
        P25 = .data$p25Value,
        Median = .data$medianValue,
        P75 = .data$p75Value,
        P90 = .data$p90Value,
        Max = .data$maxValue
      )
    
    
    validate(need(all(!is.null(data), nrow(data) > 0),
                  "No data available for selected combination."))
    
    # if (is.null(data) || nrow(data) == 0) {
    #   return(dplyr::tibble(
    #     Note = paste0("No data available for selected combination")
    #   ))
    # }
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      info = TRUE,
      columnDefs = list(minCellCountDef(3))
    )
    table <- DT::datatable(
      data,
      options = options,
      rownames = FALSE,
      filter = "top",
      class = "stripe nowrap compact"
    )
    table <- DT::formatRound(table, c("Average", "SD"), digits = 2)
    table <-
      DT::formatRound(table,
                      c("Min", "P10", "P25", "Median", "P75", "P90", "Max"),
                      digits = 0)
    return(table)
  }, server = TRUE)
  
  # resolved concepts in data source-----
  ##reactive: getResolvedConceptData----
  getResolvedConceptData <- shiny::reactive(x = {
    validate(need(all(!is.null(getDatabaseIdsFromDropdown()), length(getDatabaseIdsFromDropdown()) > 0), 
                  "No data sources chosen"))
    validate(need(all(!is.null( getCohortIdFromDropdown()),length( getCohortIdFromDropdown()) > 0),
                  "No cohort chosen"))
    if (all(is(dataSource, "environment"), !exists('includedSourceConcept'))) {
      return(NULL)
    }
    resolvedConcepts <- getResultsResolvedConcepts(dataSource = dataSource,
                                                   databaseIds = getDatabaseIdsFromDropdown(), 
                                                   cohortIds = getCohortIdFromDropdown())
    return(resolvedConcepts)
  })
  
  
  ##output: saveIncludedConceptsTable----
  output$saveIncludedConceptsTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "includedConcept")
    },
    content = function(file) {
      downloadCsv(x = getResolvedConceptData(), 
                  fileName = file)
    }
  )
  
  ##output: includedConceptsTable----
  output$includedConceptsTable <- DT::renderDataTable(expr = {
    validate(need(all(!is.null(getDatabaseIdsFromDropdown()), length(getDatabaseIdsFromDropdown()) > 0), 
                  "No data sources chosen"))
    validate(need(all(!is.null( getCohortIdFromDropdown()),length( getCohortIdFromDropdown()) > 0),
                  "No cohort chosen"))
    
    data <- getResolvedConceptData()
    validate(need(all(!is.null(data), nrow(data) > 0),
                  "No data available for selected combination"))
    
    if (!is.null(input$conceptSetsToFilterCharacterization) && 
        length(input$conceptSetsToFilterCharacterization) > 0) {
      if (length(getResoledAndMappedConceptIdsForFilters()) > 0) {
        data <- data %>% 
          dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
      } else {
        data <- data[0,]
      }
    }
    
    validate(need(all(!is.null(data), nrow(data) > 0),
                  "No data available for selected combination"))
    
    databaseIdsWithCount <- getSubjectCountsByDatabase(data = data, 
                                                        cohortId = getCohortIdFromDropdown(), 
                                                        databaseIds = getDatabaseIdsFromDropdown())
    
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    
    if (input$includedType == "Source fields") {
      data <- data %>%
        dplyr::filter(.data$sourceConceptId > 0) %>%
        dplyr::select(
          .data$databaseId,
          .data$sourceConceptId,
          .data$sourceConceptName,
          .data$sourceVocabularyId,
          .data$sourceConceptCode,
          .data$conceptSubjects,
          .data$conceptCount
        ) %>% 
        dplyr::rename("conceptId" = .data$sourceConceptId,
                      "conceptName" = .data$sourceConceptName,
                      "vocabularyId" = .data$sourceVocabularyId,
                      "conceptCode" = .data$sourceConceptCode) %>%
        dplyr::group_by(.data$databaseId,.data$conceptId, .data$conceptName, .data$vocabularyId, .data$conceptCode) %>% 
        dplyr::summarise(conceptSubjects = sum(.data$conceptSubjects),
                         conceptCount = sum(.data$conceptCount), 
                         .groups = 'keep') %>% 
        dplyr::ungroup() %>% 
        dplyr::arrange(.data$databaseId) 
    }
    if (input$includedType == "Standard fields") {
      data <- data %>%
        dplyr::filter(.data$conceptId > 0) %>%
        dplyr::mutate(conceptCode = '') %>% 
        dplyr::select(
          .data$databaseId,
          .data$conceptId,
          .data$conceptName,
          .data$vocabularyId,
          .data$conceptCode,
          .data$conceptSubjects,
          .data$conceptCount
        ) %>%
        dplyr::group_by(.data$databaseId,.data$conceptId, .data$conceptName, .data$vocabularyId, .data$conceptCode) %>% 
        dplyr::summarise(conceptSubjects = max(.data$conceptSubjects),
                         conceptCount = max(.data$conceptCount), 
                         .groups = 'keep') %>% 
        dplyr::ungroup() %>% 
        dplyr::arrange(.data$databaseId)
    }
    
    validate(need(all(!is.null(data), nrow(data) > 0),
                  "No data available for selected combination"))
    
    data <- data %>%
      tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>%
      dplyr::mutate(name = paste0(
        databaseId,
        "_",
        stringr::str_replace(
          string = .data$name,
          pattern = 'concept',
          replacement = ''
        )
      )) %>%
      tidyr::pivot_wider(
        id_cols = c(.data$conceptId, .data$conceptName, .data$vocabularyId, .data$conceptCode),
        names_from = .data$name,
        values_from = .data$value,
        values_fill = 0
      ) %>%
      dplyr::relocate(
        .data$conceptId,
        .data$conceptName,
        .data$vocabularyId,
        .data$conceptCode
      )
    
    data <- data[order(-data[, 5]), ]
    
    if (input$includedConceptsTableColumnFilter == "Subjects only") {
      data <- data %>% 
        dplyr::select(-dplyr::contains("Count"))
      columnDefs <- minCellCountDef(3 + (
        1:(nrow(databaseIdsWithCount))
      ))
    } else if (input$includedConceptsTableColumnFilter == "Records only") {
      data <- data %>% 
        dplyr::select(-dplyr::contains("Subjects"))
      columnDefs <- minCellCountDef(3 + (
        1:(nrow(databaseIdsWithCount))
      ))
    } else {
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(
                                            tr(
                                              th(rowspan = 2, 'Concept ID'),
                                              th(rowspan = 2, 'Concept Name'),
                                              th(rowspan = 2, 'Vocabulary ID'),
                                              th(rowspan = 2, 'Concept Code'),
                                              lapply(databaseIdsWithCount$databaseIdsWithCountWithoutBr, th, colspan = 2, class = "dt-center", style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                            ),
                                            tr(lapply(rep(
                                              c("Subjects", "Records"), nrow(databaseIdsWithCount)
                                            ), th, style = "border-right:1px solid silver;border-bottom:1px solid silver"))
                                          )))
      
      columnDefs <- minCellCountDef(3 + (
        1:(nrow(databaseIdsWithCount) * 2)
      ))
    }
    
    data$conceptId <- as.character(data$conceptId)
    data$vocabularyId <- as.factor(data$vocabularyId)
    
    options = list(
      pageLength = 1000,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      scrollX = TRUE,
      scrollY = "30vh",
      lengthChange = TRUE,
      searchHighlight = TRUE,
      ordering = TRUE,
      paging = TRUE,
      columnDefs = list(truncateStringDef(1, 100),
                        columnDefs)
    )
    
    if (input$includedConceptsTableColumnFilter == "Both") {
      dataTable <- DT::datatable(
        data,
        options = options,
        #       colnames = colnames(table),
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
    } else {
      dataTable <- DT::datatable(
        data,
        colnames = c(camelCaseToTitleCase(colnames(data[,1:4])),databaseIdsWithCount$databaseIdsWithCount),
        options = options,
        rownames = FALSE,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
    }
    
    dataTable <- DT::formatStyle(
      table = dataTable,
      columns =  4 + (1:(nrow(databaseIdsWithCount) * 2)),
      background = DT::styleColorBar(c(0, maxCount), "lightblue"),
      backgroundSize = "98% 88%",
      backgroundRepeat = "no-repeat",
      backgroundPosition = "center"
    )
    return(dataTable)
  }, server = TRUE)
  
  ##output: doesIncludeConceptsTableHasData----
  output$doesIncludeConceptsTableHasData <- shiny::reactive({
    return(nrow(getResolvedConceptData()) > 0)
  })
  
  shiny::outputOptions(output,
                       "doesIncludeConceptsTableHasData",
                       suspendWhenHidden = FALSE)
  
  
  # orphan concepts table -------
  ##reactive: getOrphanConceptsData----
  ##!!!!!!!!!!!!!remove
  getOrphanConceptsData <- shiny::reactive(x = {
    validate(need(all(!is.null(getDatabaseIdsFromDropdown()), length(getDatabaseIdsFromDropdown()) > 0), "No data sources chosen"))
    validate(need(length( getCohortIdFromDropdown()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('orphanConcept'))) {
      return(NULL)
    }
    orphanConcepts <- getResultsOrphanConcept(
      dataSource = dataSource,
      cohortIds = getCohortIdFromDropdown(),
      databaseIds = getDatabaseIdsFromDropdown()
    )
    validate(need(!is.null(orphanConcepts), "No orphan concepts"))
    orphanConcepts <- orphanConcepts %>%
      dplyr::inner_join(conceptSets %>% dplyr::select(
        .data$cohortId,
        .data$conceptSetId,
        .data$conceptSetName),
        by = c("cohortId", "conceptSetId"))
    concepts <- getConcept(dataSource = dataSource,
                                  conceptIds = orphanConcepts$conceptId %>% unique())
    orphanConcepts <- orphanConcepts %>%
      dplyr::inner_join(concepts %>% dplyr::select(
        .data$conceptId,
        .data$conceptName,
        .data$vocabularyId,
        .data$conceptCode,
        .data$standardConcept),
        by = c("conceptId"))
    return(orphanConcepts)
  })
  
  ##reactive: saveOrphanConceptsTable----
  output$saveOrphanConceptsTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "orphanConcept")
    },
    content = function(file) {
      downloadCsv(x = getOrphanConceptsData(), 
                  fileName = file)
    }
  )
  
  ##reactive: orphanConceptsTable----
  output$orphanConceptsTable <- DT::renderDataTable(expr = {
    
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    validate(need(all(!is.null( getCohortIdFromDropdown()),
                      length( getCohortIdFromDropdown()) > 0), "No cohorts chosen"))
    
    data <- getOrphanConceptsData()
    validate(need(all(!is.null(data), nrow(data) > 0),
                  "There is no data for the selected combination."))
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    
    if (!is.null(input$conceptSetsToFilterCharacterization) && 
        length(input$conceptSetsToFilterCharacterization) > 0) {
      if (!is.null(input$conceptSetsToFilterCharacterization)) {
        if (length(getConceptSetIdsfromDropdown()) > 0) {
          data <- data %>% 
            dplyr::filter(.data$conceptSetId %in% getConceptSetIdsfromDropdown())
        } else {
          data <- data[0,]
        }
      }
    }

    if (input$orphanConceptsType == "Standard Only") {
      data <- data %>%
        dplyr::filter(.data$standardConcept == "S")
    } else if (input$orphanConceptsType == "Non Standard Only") {
      data <- data %>%
        dplyr::filter(is.na(.data$standardConcept) |
                        (!is.na(.data$standardConcept) && .data$standardConcept != "S"))
    }

    validate(need((nrow(data) > 0),
                  "There is no data for the selected combination."))

    # databaseIds for data table names
    databaseIdsWithCount <- getSubjectCountsByDatabasae(data = data, cohortId = getCohortIdFromDropdown(), databaseIds = getDatabaseIdsFromDropdown())
    table <- pivotOrphanConceptResult(data = data,
                                      dataSource = dataSource)
    
    if (input$orphanConceptsColumFilterType == "Subjects only") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Count"))
      
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = 'Subjects', replacement = '')
      columDefs <- minCellCountDef(3 + (1:(
                                nrow(databaseIdsWithCount)
                              )))
      
      colorableColumns <- (1:(nrow(databaseIdsWithCount)))
    } else if (input$orphanConceptsColumFilterType == "Records only") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("subject"))
      
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = 'Count', replacement = '')
      columDefs <- minCellCountDef(3 + (1:(
        nrow(databaseIdsWithCount)
      )))
      
      colorableColumns <- (1:(nrow(databaseIdsWithCount)))
    } else {
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(
                                            tr(
                                              th(rowspan = 2, "Concept ID"),
                                              th(rowspan = 2, "Concept Name"),
                                              th(rowspan = 2, "Vocabulary ID"),
                                              th(rowspan = 2, "Concept Code"),
                                              lapply(databaseIdsWithCount$databaseIdsWithCountWithoutBr, th, colspan = 2, class = "dt-center", style = "border-bottom:1px solid silver;border-bottom:1px solid silver")
                                            ),
                                            tr(lapply(rep(
                                              c("Subjects", "Records"), nrow(databaseIdsWithCount)
                                            ), th, style = "border-right:1px solid silver;border-bottom:1px solid silver"))
                                          )))
      
      columDefs <- minCellCountDef(3 + (1:(
        nrow(databaseIdsWithCount) * 2
      )))
      
      colorableColumns <- (1:(nrow(databaseIdsWithCount) * 2))
    }
    
    options = list(
          pageLength = 1000,
          lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
          searching = TRUE,
          scrollX = TRUE,
          scrollY = "30vh",
          lengthChange = TRUE,
          ordering = TRUE,
          paging = TRUE,
          columnDefs = list(truncateStringDef(1, 100),
                            columDefs)
        )
    
    if (input$orphanConceptsColumFilterType == "All") {
      table <- DT::datatable(
            table,
            options = options,
            colnames = colnames(table),
            rownames = FALSE,
            container = sketch,
            escape = FALSE,
            filter = "top",
            class = "stripe nowrap compact"
          )
    } else {
      table <- DT::datatable(
            table,
            options = options,
            colnames = c(colnames(table[,1:4]),databaseIdsWithCount$databaseIdsWithCount),
            rownames = FALSE,
            escape = FALSE,
            filter = "top",
            class = "stripe nowrap compact"
          )
    }
    
      table <- DT::formatStyle(
        table = table,
        columns =  4 + colorableColumns,
        background = DT::styleColorBar(c(0, maxCount), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
    
    return(table)
  }, server = TRUE)
  
  output$orphanconceptContainData <- shiny::reactive({
    return(nrow(getOrphanConceptsData()) > 0)
  })
  
  shiny::outputOptions(output,
                       "orphanconceptContainData",
                       suspendWhenHidden = FALSE)
  
  # Inclusion rules table ----
  inclusionRuleTableData <- shiny::reactive(x = {
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    validate(need(length( getCohortIdFromDropdown()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('inclusionRuleStats'))) {
      return(NULL)
    }
    data <- getResultsInclusionRuleStatistics(
      dataSource = dataSource,
      cohortIds = getCohortIdFromDropdown(),
      databaseIds = getDatabaseIdsFromDropdown()
    )
    return(data)
  })
  
  output$saveInclusionRuleTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "inclusionRule")
    },
    content = function(file) {
      downloadCsv(x = inclusionRuleTableData(), 
                  fileName = file)
    }
  )
  
  output$inclusionRuleTable <- DT::renderDataTable(expr = {
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    table <- inclusionRuleTableData()
    
    validate(need((nrow(table) > 0),
                  "There is no data for the selected combination."))
    
    databaseIds <- unique(table$databaseId)
    
    table <- table %>%
      dplyr::inner_join(cohortCount %>% 
                          dplyr::select(.data$databaseId, .data$cohortId, .data$cohortSubjects), 
                        by = c('databaseId', 'cohortId')) %>% 
      tidyr::pivot_longer(
        cols = c(
          .data$meetSubjects,
          .data$gainSubjects,
          .data$totalSubjects,
          .data$remainSubjects
        )
      ) %>%
      dplyr::mutate(name = paste0(.data$databaseId, 
                                  "<br>(n = ", 
                                  scales::comma(x = .data$cohortSubjects, accuracy = 1),
                                  ")_", 
                                  .data$name)) %>%
      tidyr::pivot_wider(
        id_cols = c(.data$cohortId, .data$ruleSequenceId, .data$ruleName),
        names_from = .data$name,
        values_from = .data$value
      ) %>%
      dplyr::select(-.data$cohortId)
    
    if (input$inclusionRuleTableFilters == "Meet") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Total"),-dplyr::contains("Gain"),-dplyr::contains("Remain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_meetSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    } else if (input$inclusionRuleTableFilters == "Totals") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Meet"),-dplyr::contains("Gain"),-dplyr::contains("Remain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_totalSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    } else if (input$inclusionRuleTableFilters == "Gain") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Total"),-dplyr::contains("Meet"),-dplyr::contains("Remain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_gainSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    } else if (input$inclusionRuleTableFilters == "Remain") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Total"),-dplyr::contains("Meet"),-dplyr::contains("Gain"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_remainSubjects', replacement = '')
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds))
      ))
      
    }  else {
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(tr(
                                            th(rowspan = 2, "Rule Sequence ID"),
                                            th(rowspan = 2, "Rule Name"),
                                            lapply(databaseIds, th, colspan = 4, class = "dt-center", style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ),
                                          tr(
                                            lapply(rep(
                                              c("Meet", "Gain", "Remain", "Total"), length(databaseIds)
                                            ), th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ))))
      
      columnDefs <- minCellCountDef(1 + (
        1:(length(databaseIds) * 4)
      ))
    }
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      columnDefs = list(truncateStringDef(1, 100),
                        columnDefs)
    )
    
    if (input$inclusionRuleTableFilters == "All") {
      table <- DT::datatable(
        table,
        options = options,
        colnames = colnames(table) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
    } else {
      table <- DT::datatable(
        table,
        options = options,
        colnames = colnames(table) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
    }
    return(table)
  }, server = TRUE)
  
  output$inclusionRuleStatsContainsData <- shiny::reactive({
    return(nrow(inclusionRuleTableData()) > 0)
  })
  
  outputOptions(output,
                "inclusionRuleStatsContainsData",
                suspendWhenHidden = FALSE)
  
  # Index event breakdown ------
  indexEventBreakDownDataFull <- shiny::reactive(x = {
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    validate(need(length( getCohortIdFromDropdown()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('indexEventBreakdown'))) {
      return(NULL)
    }
    data <- getResultsIndexEventBreakdown(
      dataSource = dataSource,
      cohortIds = getCohortIdFromDropdown(),
      databaseIds = getDatabaseIdsFromDropdown())
  })
  
  indexEventBreakDownData <- shiny::reactive(x = {
    indexEventBreakdown <- indexEventBreakDownDataFull()
    if (is.null(indexEventBreakdown)) {return(NULL)}
    if (nrow(indexEventBreakdown) == 0) {return(NULL)}
    if (!'domainTable' %in% colnames(indexEventBreakdown)) {
      indexEventBreakdown$domainTable <- "Not in data"
    }
    if (!'domainField' %in% colnames(indexEventBreakdown)) {
      indexEventBreakdown$domainField <- "Not in data"
    }
    conceptIdDetails <- getConcept(dataSource = dataSource,
                                          conceptIds = indexEventBreakdown$conceptId %>% unique())
    if (is.null(conceptIdDetails)) {return(NULL)}
    indexEventBreakdown <- indexEventBreakdown %>%
      dplyr::inner_join(conceptIdDetails %>% 
                          dplyr::select(
                            .data$conceptId,
                            .data$conceptName,
                            .data$domainId,
                            .data$vocabularyId,
                            .data$standardConcept),
                        by = c("conceptId"))
    
    if (is.null(cohortCount)) {return(NULL)}
    indexEventBreakdown <- indexEventBreakdown %>% 
      dplyr::inner_join(cohortCount, 
                        by = c('databaseId', 'cohortId')) %>% 
      dplyr::mutate(subjectPercent = .data$subjectCount/.data$cohortSubjects,
                    conceptPercent = .data$conceptCount/.data$cohortEntries)
    return(indexEventBreakdown)
  })
  
  indexEventBreakDownDataFilteredByRadioButton <-
    shiny::reactive(x = {
      data <- indexEventBreakDownData()
      if (!is.null(data) && nrow(data) > 0) {
        if (input$indexEventBreakdownTableRadioButton == 'All') {
          return(data)
        } else if (input$indexEventBreakdownTableRadioButton == "Standard concepts") {
          return(data %>% dplyr::filter(.data$standardConcept == 'S'))
        } else {
          return(data %>% dplyr::filter(is.na(.data$standardConcept)))
        }
      } else {
        return(NULL)
      }
    })
  
  domaintable <- shiny::reactive(x = {
    if (!is.null(indexEventBreakDownDataFilteredByRadioButton())) {
      return(
        indexEventBreakDownDataFilteredByRadioButton() %>%
          dplyr::pull(.data$domainTable) %>% unique()
      )
    } else {
      return(NULL)
    }
  })
  
  shiny::observe({
    data <- domaintable()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "breakdownDomainTable",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = data,
      selected = data
    )
  })
  
  shiny::observe({
    data <- indexEventBreakDownDataFilteredByRadioButton()
    if (!is.null(data) &&
        nrow(data) > 0) {
      data <- data %>%
        dplyr::filter(.data$domainTable %in% input$breakdownDomainTable) %>%
        dplyr::pull(.data$domainField) %>% unique()
    }
    
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "breakdownDomainField",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = data,
      selected = data
    )
  })
  
  # selectedDomainTable <- reactiveVal(NULL)
  # shiny::observeEvent(eventExpr = {
  #   list(input$breakdownDomainTable_open,
  #        input$tabs)
  # }, handlerExpr = {
  #   if (isFALSE(input$breakdownDomainTable_open) ||
  #       !is.null(input$tabs)) {
  #     selectedDomainTable(input$breakdownDomainTable)
  #   }
  # })
  # 
  # selectedDomainField <- reactiveVal(NULL)
  # shiny::observeEvent(eventExpr = {
  #   list(input$breakdownDomainField_open,
  #        input$tabs)
  # }, handlerExpr = {
  #   if (isFALSE(input$breakdownDomainField_open) ||
  #       !is.null(input$tabs)) {
  #     selectedDomainField(input$breakdownDomainField)
  #   }
  # })
  
  output$saveBreakdownTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "indexEventBreakdown")
    },
    content = function(file) {
      downloadCsv(x = indexEventBreakDownDataFilteredByRadioButton(), 
                  fileName = file)
    }
  )
  
  output$breakdownTable <- DT::renderDataTable(expr = {
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    validate(need(length( getCohortIdFromDropdown()) > 0, "No cohorts chosen chosen"))
    data <- indexEventBreakDownDataFilteredByRadioButton()
    
    validate(need(all(!is.null(data),nrow(data) > 0),
                  "There is no data for the selected combination."))
    if (input$indexEventBreakdownValueFilter == "Percentage") {
      data <- data %>% 
        dplyr::mutate(conceptCount = .data$conceptCount/.data$cohortEntries) %>% 
        dplyr::mutate(subjectCount = .data$subjectCount/.data$cohortSubjects)
    }
    
    data <- data %>%
      dplyr::filter(.data$domainTable %in% input$breakdownDomainTable) %>%
      dplyr::filter(.data$domainField %in% input$breakdownDomainField) %>%
      dplyr::select(
        -.data$domainTable,
        .data$domainField,
        -.data$domainId,
        #-.data$vocabularyId,-.data$standardConcept
      ) 
    
    if (input$indexEventBreakdownTableFilter == "Records") {
      data <- data %>%
        dplyr::mutate(databaseId = paste0(.data$databaseId,
                                          "(",
                                          scales::comma(.data$cohortEntries,accuracy = 1),
                                          ")"))
    } else if (input$indexEventBreakdownTableFilter == "Persons") {
      data <- data %>%
        dplyr::mutate(databaseId = paste0(.data$databaseId,
                                          "(",
                                          scales::comma(.data$cohortSubjects,accuracy = 1),
                                          ")"))
    }
    

    personCount <- data %>% 
      dplyr::select(.data$cohortSubjects) %>% 
      dplyr::distinct() %>% 
      dplyr::mutate(cohortSubjects = scales::comma(.data$cohortSubjects,accuracy = 1)) %>% 
      dplyr::pull()
    
    recordCount <- data %>% 
      dplyr::select(.data$cohortEntries) %>% 
      dplyr::distinct() %>% 
      dplyr::mutate(cohortEntries = scales::comma(.data$cohortEntries,accuracy = 1)) %>% 
      dplyr::pull()
    
    validate(need(nrow(data) > 0,
                  "No data available for selected combination."))
    
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    databaseIds <- unique(data$databaseId)
    
    if (!"subjectCount" %in% names(data)) {
      data$subjectCount <- 0
    }
    
    data <- data %>%
      dplyr::arrange(.data$databaseId) %>%
      dplyr::select(
        .data$conceptId,
        .data$conceptName,
        .data$domainField,
        .data$databaseId,
        .data$vocabularyId,
        .data$conceptCount,
        .data$subjectCount,
        .data$cohortSubjects 
      ) %>%
      dplyr::filter(.data$conceptId > 0) %>%
      dplyr::distinct() %>% # distinct is needed here because many time condition_concept_id and condition_source_concept_id
      # may have the same value leading to duplication of row records
      tidyr::pivot_longer(names_to = "type", 
                          cols = c("conceptCount", "subjectCount"), 
                          values_to = "count") %>% 
      dplyr::mutate(names = paste0(.data$databaseId, " ", .data$type)) %>% 
      dplyr::arrange(.data$databaseId, .data$type) %>% 
      tidyr::pivot_wider(id_cols = c("conceptId",
                                     "conceptName",
                                     "domainField",
                                     "vocabularyId"),
                         names_from = "names",
                         values_from = count,
                         values_fill = 0)
    
    data <- data[order(-data[5]), ]
    
    noOfMergeColumns <- 1
    if (input$indexEventBreakdownTableFilter == "Records") {
      
      data <- data %>% 
        dplyr::select(-dplyr::contains("subjectCount"))
      
      colnames(data) <- stringr::str_replace(string = colnames(data), pattern = 'conceptCount', replacement = '')
      columnColor <- 4 + 1:(length(databaseIds))
      
    } else if (input$indexEventBreakdownTableFilter == "Persons") {
      data <- data %>% 
        dplyr::select(-dplyr::contains("conceptCount"))
      
      colnames(data) <- stringr::str_replace(string = colnames(data), pattern = 'subjectCount', replacement = '')
      columnColor <- 4 + 1:(length(databaseIds))

    } else {
      recordAndPersonColumnName <- c()
      for (i in 1:length(getDatabaseIdsFromDropdown())) {
        recordAndPersonColumnName <-
          c(
            recordAndPersonColumnName,
            paste0("Records (", recordCount[i],")"),
            paste0("Person (", personCount[i],")")
          )
      }
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(
                                            tr(
                                              th(rowspan = 2, "Concept Id"),
                                              th(rowspan = 2, "Concept Name"),
                                              th(rowspan = 2, "Domain field"),
                                              th(rowspan = 2, "Vocabulary Id"),
                                              lapply(databaseIds, th, colspan = 2, class = "dt-center", style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                            ),
                                            tr(lapply(recordAndPersonColumnName, th, style = "border-right:1px solid silver;border-bottom:1px solid silver"))
                                          )))
      
      columnColor <- 4 + 1:(length(databaseIds) * 2)
      noOfMergeColumns <- 2
    }
    
    if (input$indexEventBreakdownValueFilter == "Percentage") {
      minimumCellPercent <- minCellPercentDef(3 + 1:(
        length(databaseIds) * noOfMergeColumns
      ))
    } else {
      minimumCellPercent <- minCellCountDef(3 + 1:(
        length(databaseIds) * noOfMergeColumns
      ))
    }
    options = list(
      pageLength = 1000,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      scrollY = "50vh",
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      columnDefs = list(minimumCellPercent)
    )
    
    if (input$indexEventBreakdownTableFilter == "Both") {
      table <- DT::datatable(
        data,
        options = options,
        colnames = colnames(table) %>%
          camelCaseToTitleCase(),
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top"
      )
    } else {
      table <- DT::datatable(
        data,
        options = options,
        rownames = FALSE,
        colnames = colnames(data) %>%
          camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
    }
   
    
    table <- DT::formatStyle(
      table = table,
      columns = columnColor,
      background = DT::styleColorBar(c(0, maxCount), "lightblue"),
      backgroundSize = "98% 88%",
      backgroundRepeat = "no-repeat",
      backgroundPosition = "center"
    )
    return(table)
  }, server = TRUE)
  
  # Visit Context -----
  visitContexData <- shiny::reactive(x = {
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    validate(need(length( getCohortIdFromDropdown()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('visitContext'))) {
      return(NULL)
    }
    visitContext <- getResultsVisitContext(
      dataSource = dataSource,
      cohortIds = getCohortIdFromDropdown(),
      databaseIds = getDatabaseIdsFromDropdown()
    )
    
    if (is.null(visitContext) || nrow(visitContext) == 0) {
      return(NULL)
    }
    # to ensure backward compatibility to 2.1 when visitContext did not have visitConceptName
    if (!'visitConceptName' %in% colnames(visitContext)) {
      concepts <- getConcept(dataSource = dataSource, 
                                    conceptIds = visitContext$visitConceptId %>% unique()
      ) %>% 
        dplyr::rename(visitConceptId = .data$conceptId,
                      visitConceptName = .data$conceptName) %>% 
        dplyr::filter(is.na(.data$invalidReason)) %>% 
        dplyr::select(.data$visitConceptId, .data$visitConceptName)
      
      visitContext <- visitContext %>% 
        dplyr::left_join(concepts,
                         by = c('visitConceptId'))
    }
    
    visitContext <- visitContext %>%
      dplyr::inner_join(cohortCount,
                        by = c("cohortId", "databaseId")) %>% 
      dplyr::mutate(subjectPercent = .data$subjects/.data$cohortSubjects) %>% 
      dplyr::mutate(recordPercent = .data$records / .data$cohortEntries)
    return(visitContext)
  })
  
  output$saveVisitContextTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "visitContext")
    },
    content = function(file) {
      downloadCsv(x = visitContexData(), 
                  fileName = file)
    }
  )
  
  output$visitContextTable <- DT::renderDataTable(expr = {
    validate(need(length(getDatabaseIdsFromDropdown()) > 0, "No data sources chosen"))
    validate(need(length( getCohortIdFromDropdown()) > 0, "No cohorts chosen"))
    data <- visitContexData()
    validate(need(nrow(data) > 0,
                  "No data available for selected combination."))
    
    if (input$visitContextValueFilter == "Percentage") {
      data <- data %>% 
        dplyr::mutate(subjects = .data$subjects/.data$cohortSubjects) %>% 
        dplyr::mutate(records = .data$records / .data$cohortEntries)
    }
    
    databaseIds <- sort(unique(data$databaseId))
    cohortCounts <- data %>% 
      dplyr::filter(.data$cohortId == getCohortIdFromDropdown()) %>% 
      dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown()) %>% 
      dplyr::select(.data$cohortSubjects, .data$cohortEntries) %>% unique()
    
    isPerson <- input$visitContextPersonOrRecords == 'Person'
    if (isPerson) {
      cohortCounts <- cohortCounts$cohortSubjects
    } else {
      cohortCounts <- cohortCounts$cohortEntries
    }
    
    databaseIdsWithCount <- paste(databaseIds, "(n = ", format(cohortCounts, big.mark = ","), ")")
    
    maxSubjects <- max(data$subjects)
    visitContextReference <-
      expand.grid(
        visitContext = c("Before", "During visit", "On visit start", "After"),
        visitConceptName = unique(data$visitConceptName),
        databaseId = databaseIds
      ) %>%
      dplyr::tibble()
    
    table <- visitContextReference %>%
      dplyr::left_join(data,
                       by = c("visitConceptName", "visitContext", "databaseId")) %>%
      dplyr::select(.data$visitConceptName,
                    .data$visitContext,
                    .data$subjects,
                    .data$records,
                    .data$databaseId) %>%
      dplyr::mutate(visitContext = paste0(.data$databaseId, "_", .data$visitContext)) %>%
      dplyr::select(-.data$databaseId) %>%
      dplyr::arrange(.data$visitConceptName)
    
    if (isPerson) {
      table <- table %>% 
        tidyr::pivot_wider(
          id_cols = c(.data$visitConceptName),
          names_from = .data$visitContext,
          values_from = .data$subjects,
          values_fill = 0
        )
    } else {
      table <- table %>% 
        tidyr::pivot_wider(
          id_cols = c(.data$visitConceptName),
          names_from = .data$visitContext,
          values_from = .data$records,
          values_fill = 0
        )
    }
    table <- table %>% 
         dplyr::relocate(.data$visitConceptName)
      
    totalColumns <- 1
    
    if (input$visitContextTableFilters == "Before") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("During"),-dplyr::contains("On visit"),-dplyr::contains("After"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_Before', replacement = '')
      
    } else if (input$visitContextTableFilters == "During") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("Before"),-dplyr::contains("On visit"),-dplyr::contains("After"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_During visit', replacement = '')
      
    } else if (input$visitContextTableFilters == "Simultaneous") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("During"),-dplyr::contains("Before"),-dplyr::contains("After"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_On visit start', replacement = '')
      
    } else if (input$visitContextTableFilters == "After") {
      table <- table %>% 
        dplyr::select(-dplyr::contains("During"),-dplyr::contains("Before"),-dplyr::contains("On visit"))
      colnames(table) <- stringr::str_replace(string = colnames(table), pattern = '_After', replacement = '')
      
    }  else {
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(tr(
                                            th(rowspan = 2, "Visit"),
                                            lapply(databaseIdsWithCount, th, colspan = 4, class = "dt-center",style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ),
                                          tr(
                                            lapply(rep(
                                              c(
                                                "Visits Before",
                                                "Visits Ongoing",
                                                "Starting Simultaneous",
                                                "Visits After"
                                              ),
                                              length(databaseIds)
                                            ), th,style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ))))
      
      totalColumns <- 4
      }
    
    columnDefs <- minCellCountDef(1:(
      length(databaseIds) * totalColumns
    ))
    
    if (input$visitContextValueFilter == "Percentage") {
      columnDefs <- minCellPercentDef(1:(
        length(databaseIds) * totalColumns
      ))
    }
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      scrollY = "60vh",
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      columnDefs = list(truncateStringDef(0, 60),
                        list(width = "40%", targets = 0),
                        columnDefs)
    )
    
    if (input$visitContextTableFilters == "All") {
      table <- DT::datatable(
        table,
        options = options,
        colnames = colnames(table) %>%
          camelCaseToTitleCase(),
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top"
      )
    } else {
      table <- DT::datatable(
        table,
        options = options,
        colnames = colnames(table) %>%
          camelCaseToTitleCase(),
        rownames = FALSE,
        escape = FALSE,
        filter = "top"
      )
    }
    
    table <- DT::formatStyle(
      table = table,
      columns = 1+ 1:(length(databaseIds) * 4),
      background = DT::styleColorBar(c(0, maxSubjects), "lightblue"),
      backgroundSize = "98% 88%",
      backgroundRepeat = "no-repeat",
      backgroundPosition = "center"
    )
  }, server = TRUE)
  
  output$visitContextContainData <- shiny::reactive({
    return(nrow(visitContexData()) > 0)
  })
  
  shiny::outputOptions(output,
                       "visitContextContainData",
                       suspendWhenHidden = FALSE)
  
  # Characterization/Temporal Characterization ------
  # Characterization and temporal characterization data for one cohortId and multiple databaseIds
  characterizationTemporalCharacterizationData <- shiny::reactive(x = {
    if (input$tabs == "temporalCharacterization" || input$tabs == "cohortCharacterization") {
      if (all(is(dataSource, "environment"), 
              !any(exists('covariateValue'), 
                   exists('temporalCovariateValue')))) {
        return(NULL)
      }
      if (any(length( getCohortIdFromDropdown()) != 1,
              length(getDatabaseIdsFromDropdown()) == 0)) {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Extracting characterization data for target cohort:", getCohortIdFromDropdown()), 
                   value = 0)
      
      data <- getMultipleCharacterizationResults(
        dataSource = dataSource,
        cohortIds = getCohortIdFromDropdown(),
        databaseIds = getDatabaseIdsFromDropdown()
      )
      return(data)
    } else {
      return(NULL)
    }
  })
  
  ## Characterization data ------
  characterizationData <- shiny::reactive(x = {
    if (any(length( getCohortIdFromDropdown()) != 1,
            length(getDatabaseIdsFromDropdown()) == 0,
            length(characterizationTemporalCharacterizationData()) == 0)) {
      return(NULL)
    }
    if (input$charType == "Pretty") {
      analysisIds <- prettyAnalysisIds
    } else {
      analysisIds <- NULL
    }
    covariatesTofilter <- characterizationTemporalCharacterizationData()$covariateRef
    if (!is.null(analysisIds)) {
      covariatesTofilter <- covariatesTofilter %>% 
        dplyr::filter(.data$analysisId %in% analysisIds)
    }
    if (!is.null(characterizationTemporalCharacterizationData()$covariateValue)) {
      characterizationDataValue <- characterizationTemporalCharacterizationData()$covariateValue %>%
        dplyr::filter(.data$characterizationSource %in% c('C', 'F')) %>% 
        dplyr::select(-.data$timeId) %>% 
        dplyr::inner_join(covariatesTofilter, 
                          by = c('covariateId', 'characterizationSource')) %>% 
        dplyr::inner_join(characterizationTemporalCharacterizationData()$analysisRef, 
                          by = c('analysisId','characterizationSource')) %>%
        dplyr::mutate(covariateNameShort = gsub(".*: ","",.data$covariateName)) %>% 
        dplyr::mutate(covariateNameShortCovariateId = paste0(.data$covariateNameShort, 
                                                             " (", 
                                                             .data$covariateId, ")"))
    } else {
      characterizationDataValue <- NULL
    }
    if (any(is.null(characterizationDataValue), 
            nrow(characterizationDataValue) == 0)) {
      return(NULL)
    }
    return(characterizationDataValue)
  })
  
  getConceptSetNameForFilter <- shiny::reactive(x = {
    if (any(length( getCohortIdFromDropdown()) == 0,
            length(getDatabaseIdsFromDropdown()) == 0)) {
      return(NULL)
    }
    
    jsonExpression <- getCohortSortedByCohortId() %>% 
      dplyr::filter(.data$cohortId == getCohortIdFromDropdown()) %>% 
      dplyr::select(.data$json)
    
    jsonExpression <- RJSONIO::fromJSON(jsonExpression$json, digits = 23)
    expression <-
      getConceptSetDetailsFromCohortDefinition(cohortDefinitionExpression = jsonExpression)
    
    if (!is.null(expression)) {
      expression <- expression$conceptSetExpression %>% 
        dplyr::select(.data$name)
      
      return(expression)
    } else {
      return(NULL)
    }
  })
  
  characterizationDomainNameFilter <- shiny::reactive({
    return(input$characterizationDomainNameFilter)
  })
  
  characterizationAnalysisNameFilter <- shiny::reactive({
    return(input$characterizationAnalysisNameFilter)
  })
  
  ### Data ------
  characterizationTableData <- shiny::reactive(x = {
    data <- characterizationData()
    if (any(is.null(data), nrow(data) == 0)) {
      return(NULL)
    }
    if (input$charType == "Raw" &&
        input$charProportionOrContinuous == "Proportion") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else if (input$charType == "Raw" &&
               input$charProportionOrContinuous == "Continuous") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'N')
    }
    if (any(is.null(data), nrow(data) == 0)) {
      return(NULL)
    }
    if (input$charType == "Raw") {
      data <- data %>%
        dplyr::rename(covariateNameFull = .data$covariateName) %>%
        dplyr::mutate(covariateName =
                        gsub(".*: ", "", .data$covariateNameFull)) %>%
        dplyr::mutate(
          covariateName = dplyr::case_when(
            stringr::str_detect(
              string = tolower(.data$covariateNameFull),
              pattern = 'age group|gender'
            ) ~ .data$covariateNameFull,
            TRUE ~ gsub(".*: ", "", .data$covariateNameFull)
          )
        ) %>%
        dplyr::mutate(
          covariateName = dplyr::case_when(
            stringr::str_detect(string = tolower(.data$domainId),
                                pattern = 'cohort') ~ .data$covariateNameFull,
            TRUE ~ .data$covariateName
          )
        ) %>%
        dplyr::mutate(covariateName = paste0(.data$covariateName, " (", .data$covariateId, ")"))
    }
    return(data)
  })
  
  shiny::observe({
    data <- characterizationTableData()
    if (any(is.null(data), 
            nrow(data$analysisName) == 0)) 
      {return(NULL)}
    subset <-
      data$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "characterizationAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  shiny::observe({
    data <- characterizationTableData()
    if (all(!is.null(data), 
            nrow(data$domainId) > 0)) {
      subset <-
        data$domainId %>% unique() %>% sort()
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "characterizationDomainNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = subset
      )
    }
  })
  
  shiny::observe({
    if (is.null(getConceptSetNameForFilter())) {
      return(NULL)
    }
    subset <- getConceptSetNameForFilter()$name %>% 
      sort() %>% 
      unique()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "conceptSetsToFilterCharacterization",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset
    )
  })
  
  output$saveCohortCharacterizationTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "cohortCharacterization")
    },
    content = function(file) {
      downloadCsv(x = characterizationTableData(), 
                  fileName = file)
    }
  )
  ### Output ------
  output$characterizationTable <- DT::renderDataTable(expr = {
    data <- characterizationTableData()
    validate(need(all(!is.null( getCohortIdFromDropdown()),
                      length( getCohortIdFromDropdown()) > 0),
                  "No data for the combination"))
    validate(need(!is.null(data), "No data for the combination"))
    
    databaseIds <- sort(unique(data$databaseId))
    
    cohortCounts <- data %>% 
      dplyr::inner_join(cohortCount,
                        by = c("cohortId", "databaseId")) %>% 
      dplyr::filter(.data$cohortId == getCohortIdFromDropdown()) %>% 
      dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown()) %>% 
      dplyr::select(.data$cohortSubjects) %>% 
      dplyr::pull(.data$cohortSubjects) %>% unique()
    
    databaseIdsWithCount <- paste(databaseIds, "(n = ", format(cohortCounts, big.mark = ","), ")")
    
    if (input$charType == "Pretty") {
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Rendering pretty table for cohort characterization."), value = 0)
      
      countData <- getResultsCohortCount(
        dataSource = dataSource,
        databaseIds = getDatabaseIdsFromDropdown(),
        cohortIds = getCohortIdFromDropdown()
      ) %>%
        dplyr::arrange(.data$databaseId)
      
      table <- data %>%
        prepareTable1()
      
      validate(need(nrow(table) > 0,
                    "No data available for selected combination."))
      
      characteristics <- table %>%
        dplyr::select(.data$characteristic,
                      .data$position,
                      .data$header,
                      .data$sortOrder) %>%
        dplyr::distinct() %>%
        dplyr::group_by(.data$characteristic, .data$position, .data$header) %>%
        dplyr::summarise(sortOrder = max(.data$sortOrder)) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(.data$position, desc(.data$header)) %>%
        dplyr::mutate(sortOrder = dplyr::row_number()) %>%
        dplyr::distinct()
      
      characteristics <- dplyr::bind_rows(
        characteristics %>%
          dplyr::filter(.data$header == 1) %>%
          dplyr::mutate(
            cohortId = sort( getCohortIdFromDropdown())[[1]],
            databaseId = sort(databaseIds[[1]])
          ),
        characteristics %>%
          dplyr::filter(.data$header == 0) %>%
          tidyr::crossing(dplyr::tibble(databaseId = databaseIds)) %>%
          tidyr::crossing(dplyr::tibble(cohortId = getCohortIdFromDropdown()))
      ) %>%
        dplyr::arrange(.data$sortOrder, .data$databaseId, .data$cohortId)
      
      table <- characteristics %>%
        dplyr::left_join(
          table %>%
            dplyr::select(-.data$sortOrder),
          by = c(
            "characteristic",
            "position",
            "header",
            "databaseId",
            "cohortId"
          )
        )  %>% 
        dplyr::inner_join(cohortCount %>% 
                            dplyr::select(-.data$cohortEntries),
                          by = c("databaseId", "cohortId")) %>% 
        dplyr::mutate(databaseId = paste0(.data$databaseId, 
                                          "<br>(n = ", 
                                          scales::comma(.data$cohortSubjects,accuracy = 1), 
                                          ")")) %>%
        dplyr::arrange(.data$sortOrder) %>%
        tidyr::pivot_wider(
          id_cols = c("cohortId", "characteristic"),
          names_from = "databaseId",
          values_from = "value" ,
          names_sep = "_"
        )
      table <- table %>%
        dplyr::relocate(.data$characteristic) %>%
        dplyr::select(-.data$cohortId)
      
      options = list(
        pageLength = 1000,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        scrollX = TRUE,
        scrollY = "100vh",
        lengthChange = TRUE,
        ordering = FALSE,
        paging = TRUE,
        columnDefs = list(
          truncateStringDef(0, 150),
          minCellPercentDef(1:length(databaseIds))
        )
      )
      
      table <- DT::datatable(
        table,
        options = options,
        rownames = FALSE,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      table <- DT::formatStyle(
        table = table,
        columns = 1 + 1:length(databaseIds),
        background = DT::styleColorBar(c(0, 1), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
    } else {
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Rendering raw table for cohort characterization."), value = 0)
      
      data <- data %>%
        dplyr::filter(.data$analysisName %in% characterizationAnalysisNameFilter()) %>%
        dplyr::filter(.data$domainId %in% characterizationDomainNameFilter())
      
      if (!is.null(input$conceptSetsToFilterCharacterization)) {
        if (length(getResoledAndMappedConceptIdsForFilters()) > 0) {
          data <- data %>% 
            dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
        } else {
          data <- data[0,]
        }
      }
      
      validate(need(nrow(data) > 0,
                    "No data available for selected combination."))
      
      covariateNames <- data %>% dplyr::select(
        .data$covariateId,
        .data$covariateName,
        .data$conceptId
      ) %>%
        dplyr::distinct()
      
      if (input$characterizationColumnFilters == "Mean and Standard Deviation") {
        data <- data %>%
          dplyr::arrange(.data$databaseId, .data$cohortId) %>%
          tidyr::pivot_longer(cols = c(.data$mean, .data$sd), names_to = 'names') %>% 
          dplyr::mutate(names = paste0(.data$databaseId, " ", .data$names)) %>% 
          dplyr::arrange(.data$databaseId, .data$names, .data$covariateId) %>% 
          tidyr::pivot_wider(
            id_cols = c(.data$cohortId, .data$covariateId),
            names_from = .data$names,
            values_from = .data$value,
            values_fill = 0 
          )
      } else {
        data <- data %>%
          dplyr::arrange(.data$databaseId, .data$cohortId) %>%
          dplyr::select(-.data$sd) %>% 
          tidyr::pivot_wider(
            id_cols = c(.data$cohortId, .data$covariateId),
            names_from = .data$databaseId,
            values_from = .data$mean,
            values_fill = 0 
          )
      }
      
      data <-  data %>%
        dplyr::inner_join(covariateNames,
                          by = "covariateId"
        ) %>%
        dplyr::select(-.data$covariateId, -.data$cohortId, -.data$conceptId) %>% 
        dplyr::relocate(.data$covariateName)
      
      data <- data[order(-data[2]), ]
      
      if (input$characterizationColumnFilters == "Mean and Standard Deviation") {
        options = list(
          pageLength = 1000,
          lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
          searching = TRUE,
          searchHighlight = TRUE,
          scrollX = TRUE,
          scrollY = "60vh",
          lengthChange = TRUE,
          ordering = TRUE,
          paging = TRUE,
          columnDefs = list(
            truncateStringDef(0, 80),
            minCellRealDef(1:(length(databaseIds) * 2), digits = 3)
          )
        )
        sketch <- htmltools::withTags(table(class = "display",
                                            thead(tr(
                                              th(rowspan = 2, "Covariate Name"),
                                              lapply(databaseIdsWithCount, th, colspan = 2, class = "dt-center", style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                            ),
                                            tr(
                                              lapply(rep(
                                                c("Mean", "SD"), length(databaseIds)
                                              ), th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                            ))))
        
        table <- DT::datatable(
          data,
          options = options,
          rownames = FALSE,
          container = sketch,
          escape = FALSE,
          filter = "top",
          class = "stripe nowrap compact"
        )
        
        table <- DT::formatStyle(
          table = table,
          columns = (1 + 1:(length(databaseIds) * 2)),
          background = DT::styleColorBar(c(0, 1), "lightblue"),
          backgroundSize = "98% 88%",
          backgroundRepeat = "no-repeat",
          backgroundPosition = "center"
        )
      } else {
        
        options = list(
          pageLength = 1000,
          lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
          searching = TRUE,
          searchHighlight = TRUE,
          scrollX = TRUE,
          scrollY = "60vh",
          lengthChange = TRUE,
          ordering = TRUE,
          paging = TRUE,
          columnDefs = list(
            truncateStringDef(0, 80),
            minCellRealDef(1:(length(databaseIds)), digits = 3)
          )
        )
        
        colnames <- cohortCount %>% 
          dplyr::filter(.data$databaseId %in% colnames(data)) %>% 
          dplyr::filter(.data$cohortId == characterizationTableData()$cohortId %>% unique()) %>% 
          dplyr::mutate(colnames = paste0(.data$databaseId, 
                                          "<br>(n = ",
                                          scales::comma(.data$cohortSubjects, accuracy = 1),
                                          ")")) %>% 
          dplyr::arrange(.data$databaseId) %>% 
          dplyr::pull(colnames)
        
        table <- DT::datatable(
          data,
          options = options,
          rownames = FALSE,
          colnames = colnames,
          escape = FALSE,
          filter = "top",
          class = "stripe nowrap compact"
        )
        table <- DT::formatStyle(
          table = table,
          columns = (1 + 1:(length(databaseIds))),
          background = DT::styleColorBar(c(0, 1), "lightblue"),
          backgroundSize = "98% 88%",
          backgroundRepeat = "no-repeat",
          backgroundPosition = "center"
        )
      } 
    }
    return(table)
  }, server = TRUE)
  
  ## Temporal Characterization Data ------
  ### Data ------
  temporalCharacterizationData <- shiny::reactive(x = {
    if (any(length( getCohortIdFromDropdown()) != 1,
            length(getDatabaseIdsFromDropdown()) == 0,
            is.null(characterizationTemporalCharacterizationData()$covariateValue),
            nrow(characterizationTemporalCharacterizationData()$covariateValue) == 0,
            is.null(characterizationTemporalCharacterizationData()$covariateRef),
            nrow(characterizationTemporalCharacterizationData()$covariateRef) == 0,
            is.null(characterizationTemporalCharacterizationData()$analysisRef),
            nrow(characterizationTemporalCharacterizationData()$analysisRef) == 0,
            exists("temporalCovariateChoices"))) {
      return(NULL)
    }
    
    if (!is.null(characterizationTemporalCharacterizationData()$covariateValue)) {
      characterizationDataValue <-
        characterizationTemporalCharacterizationData()$covariateValue %>%
        dplyr::filter(.data$characterizationSource %in% c('CT', 'FT')) %>%
        dplyr::inner_join(
          characterizationTemporalCharacterizationData()$covariateRef,
          by = c('covariateId', 'characterizationSource')
        ) %>%
        dplyr::inner_join(
          characterizationTemporalCharacterizationData()$analysisRef %>%
            dplyr::select(-.data$startDay,-.data$endDay),
          by = c('analysisId', 'characterizationSource')
        ) %>%
        dplyr::distinct() %>%
        dplyr::inner_join(temporalCovariateChoices, by = "timeId") %>%
        dplyr::arrange(.data$timeId) %>%
        dplyr::mutate(covariateNameShort = gsub(".*: ", "", .data$covariateName)) %>%
        dplyr::mutate(
          covariateNameShortCovariateId = paste0(.data$covariateNameShort,
                                                 " (",
                                                 .data$covariateId, ")")
        )
    } else {
      characterizationDataValue <- NULL
    }
    if (any(is.null(characterizationDataValue), 
            nrow(characterizationDataValue) == 0)) {
      return(NULL)
    }
    return(characterizationDataValue)
  })
  
  output$saveTemporalCharacterizationTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "temporalCharacterizationTableData")
    },
    content = function(file) {
      downloadCsv(x = temporalCharacterizationData(), 
                  fileName = file)
    }
  )
  
  temporalAnalysisNameFilter <- shiny::reactive(x = {
    return(input$temporalAnalysisNameFilter)
  })
  
  temporalDomainNameFilter <- shiny::reactive(x = {
    return(input$temporalDomainNameFilter)
  })
  
  ### Output ------
  temporalCharacterizationTableData <- shiny::reactive({
    data <- temporalCharacterizationData()
    if (any(is.null(data), nrow(data) == 0)) {return(NULL)}
    if (any(!exists('temporalCovariateChoices'),
            is.null(temporalCovariateChoices),
            nrow(temporalCovariateChoices) == 0)) {return(NULL)}
    if (length(getTimeIdsFromDropdowm()) > 0) {
      data <- data %>% 
        dplyr::filter(.data$timeId %in% getTimeIdsFromDropdowm())
    }
    data <- data %>%
      dplyr::select(-.data$cohortId, -.data$databaseId)
    if (input$temporalProportionOrContinuous == "Proportion") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else if (input$temporalProportionOrContinuous == "Continuous") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'N')
    }
    if (!is.null(input$conceptSetsToFilterCharacterization)) {
      if (length(getResoledAndMappedConceptIdsForFilters()) > 0) {
        data <- data %>% 
          dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
      }
    }
    if (any(is.null(data), nrow(data) == 0)) {return(NULL)}
    return(data)
  })
  
  shiny::observe({
    subset <-
      temporalCharacterizationTableData()$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  shiny::observe({
    subset <-
      temporalCharacterizationTableData()$domainId %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalDomainNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  output$temporalCharacterizationTable <-
    DT::renderDataTable(expr = {
      data <- temporalCharacterizationTableData() 
      validate(need(nrow(data) > 0,
                    "No data available for selected combination."))
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Rendering raw table for temporal cohort characterization."), value = 0)
      
      data <- data %>%
        dplyr::filter(.data$analysisName %in% temporalAnalysisNameFilter()) %>%
        dplyr::filter(.data$domainId %in% temporalDomainNameFilter())
      validate(need(nrow(data) > 0,
                    "No data available for selected combination."))
      
      table <- data %>%
        tidyr::pivot_wider(
          id_cols = c("covariateName"),
          names_from = "choices",
          values_from = "mean" ,
          names_sep = "_"
        ) %>%
        dplyr::relocate(.data$covariateName) %>%
        dplyr::arrange(dplyr::desc(dplyr::across(dplyr::starts_with('Start'))))
      
      temporalCovariateChoicesSelected <-
        temporalCovariateChoices %>%
        dplyr::filter(.data$timeId %in% c(getTimeIdsFromDropdowm())) %>%
        dplyr::arrange(.data$timeId)
      
      options = list(
        pageLength = 1000,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        searchHighlight = TRUE,
        scrollX = TRUE,
        scrollY = "60vh",
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        columnDefs = list(truncateStringDef(0, 80),
                          minCellPercentDef(1:(
                            length(temporalCovariateChoicesSelected$choices)
                          )))
      )
      
      table <- DT::datatable(
        table,
        options = options,
        rownames = FALSE,
        colnames = colnames(table) %>%
          camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      table <- DT::formatStyle(
        table = table,
        columns = (1 + (
          1:length(temporalCovariateChoicesSelected$choices)
        )),
        #0 index
        background = DT::styleColorBar(c(0, 1), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      return(table)
    }, server = TRUE)
  
  # Cohort Overlap ------
  cohortOverlapData <- reactive({
    if (all(is(dataSource, "environment"), 
            !exists('cohortRelationships'))) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Extracting cohort overlap data."), value = 0)
    
    data <- getCohortOverlapData(
      dataSource = dataSource,
      cohortIds =  getCohortIdsFromDropdown(),
      databaseIds = getDatabaseIdsFromDropdown()
    )
    validate(need(
      !is.null(data),
      paste0("No cohort overlap data for this combination")
    ))
    validate(need(
      nrow(data) > 0,
      paste0("No cohort overlap data for this combination.")
    ))
    return(data)
  })
  
  output$overlapPlot <- ggiraph::renderggiraph(expr = {
    validate(need(
      length( getCohortIdsFromDropdown()) > 0,
      paste0("Please select Target Cohort(s)")
    ))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Plotting cohort overlap."), value = 0)
    
    data <- cohortOverlapData()
    validate(need(
      !is.null(data),
      paste0("No cohort overlap data for this combination")
    ))
    validate(need(
      nrow(data) > 0,
      paste0("No cohort overlap data for this combination.")
    ))
    
    plot <- plotCohortOverlap(
      data = data,
      shortNameRef = cohort,
      yAxis = input$overlapPlotType
    )
    return(plot)
  })
  
  output$saveCohortOverlapTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "cohortOverlap")
    },
    content = function(file) {
      downloadCsv(x = cohortOverlapData(), 
                  fileName = file)
    }
  )
  
  # Compare Characterization/Temporal Characterization ------
  compareCharacterizationTemporalCharacterizationData <- shiny::reactive(x = {
    if (input$tabs == "temporalCharacterization" || input$tabs == "cohortCharacterization") {
      if (all(is(dataSource, "environment"), 
              !any(exists('covariateValue'), 
                   exists('temporalCovariateValue')))) {
        return(NULL)
      }
      if (any(length( getCohortIdFromDropdown()) != 1,
              length(getComparatorCohortIdFromDropdowm()) != 1,
              length(getDatabaseIdsFromDropdown()) != 1)) {
        return(NULL)
      }
      if (all(is(dataSource, "environment"), 
              !any(exists('covariateValue'), 
                   exists('temporalCovariateValue')))) {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Extracting characterization data for target cohort:", 
                                    getCohortIdFromDropdown(),
                                    " and comparator cohort:",
                                    getComparatorCohortIdFromDropdowm(),
                                    ' for ',
                                    input$database), 
                   value = 0)
      
      data <- getMultipleCharacterizationResults(
        dataSource = dataSource,
        cohortIds = c( getCohortIdFromDropdown(), getComparatorCohortIdFromDropdowm()) %>% unique(),
        databaseIds = input$database
      )
      return(data)
    } else {
      return(NULL)
    }
  })
  
  ## Characterization data ----
  # Characterization and temporal characterization data for target, comparator cohortId and one databaseIds
  charCompareAnalysisNameFilter <- shiny::reactive(x = {
    return(input$charCompareAnalysisNameFilter)
  })
  
  charaCompareDomainNameFilter <- shiny::reactive(x = {
    return(input$charaCompareDomainNameFilter)
  })
  
  ### Data ------
  computeBalance <- shiny::reactive({
    if (any(length( getCohortIdFromDropdown()) != 1,
            length(getComparatorCohortIdFromDropdowm()) != 1,
            length(getDatabaseIdsFromDropdown()) == 0,
            is.null(compareCharacterizationTemporalCharacterizationData()$covariateValue),
            nrow(compareCharacterizationTemporalCharacterizationData()$covariateValue) == 0,
            is.null(compareCharacterizationTemporalCharacterizationData()$covariateRef),
            nrow(compareCharacterizationTemporalCharacterizationData()$covariateRef) == 0,
            is.null(compareCharacterizationTemporalCharacterizationData()$analysisRef),
            nrow(compareCharacterizationTemporalCharacterizationData()$analysisRef) == 0)) {
      return(NULL)
    }

    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Computing compare characterization."), value = 0)

    data <- compareCharacterizationTemporalCharacterizationData()$covariateValue %>% 
      dplyr::filter(.data$characterizationSource %in% c('C', 'F')) %>% 
      dplyr::select(-.data$timeId, -.data$startDay, -.data$endDay) %>% 
      dplyr::inner_join(compareCharacterizationTemporalCharacterizationData()$covariateRef, 
                        by = c("covariateId", "characterizationSource")) %>% 
      dplyr::inner_join(compareCharacterizationTemporalCharacterizationData()$analysisRef , 
                        by = c("analysisId", "characterizationSource"))
    
    covs1 <- data %>% 
      dplyr::filter(.data$cohortId == getCohortIdFromDropdown()) %>% 
      dplyr::mutate(analysisNameLong = paste0(.data$analysisName, 
                                              " (", 
                                              as.character(.data$startDay), 
                                              " to ", 
                                              as.character(.data$endDay), 
                                              ")")) %>% 
      dplyr::relocate(.data$cohortId, 
                      .data$databaseId, 
                      .data$analysisId,
                      .data$covariateId, 
                      .data$covariateName,
                      .data$isBinary) %>% 
      dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
    
    covs2 <- data %>% 
      dplyr::filter(.data$cohortId == getComparatorCohortIdFromDropdowm()) %>% 
      dplyr::mutate(analysisNameLong = paste0(.data$analysisName, 
                                              " (", 
                                              as.character(.data$startDay), 
                                              " to ", 
                                              as.character(.data$endDay), 
                                              ")")) %>% 
      dplyr::relocate(.data$cohortId, 
                      .data$databaseId, 
                      .data$analysisId,
                      .data$covariateId, 
                      .data$covariateName,
                      .data$isBinary) %>% 
      dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
    
    if (is.null(covs1) || is.null(covs2)) {
      return(NULL)
    }
    
    balance <- compareCohortCharacteristics(covs1, covs2)
    
    if (any(is.null(balance), nrow(balance) == 0)) {
      return(NULL)
    }
    balance <- balance %>%
      dplyr::mutate(absStdDiff = abs(.data$stdDiff)) %>% 
      dplyr::rename(covariateNameFull = .data$covariateName) %>% 
      dplyr::mutate(covariateName = gsub(".*: ","",.data$covariateNameFull)) %>% 
      dplyr::mutate(covariateName = dplyr::case_when(stringr::str_detect(string = tolower(.data$covariateNameFull), 
                                                                         pattern = 'age group|gender') ~ .data$covariateNameFull,
                                                     TRUE ~ gsub(".*: ","",.data$covariateNameFull))) %>% 
      dplyr::mutate(covariateName = paste0(.data$covariateName, " (", .data$covariateId, ")"))
    
    if (input$charCompareType == "Raw table" &&
        input$charCompareProportionOrContinuous == "Proportion") {
      balance <- balance %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else if (input$charCompareType == "Raw table" &&
               input$charCompareProportionOrContinuous == "Continuous") {
      balance <- balance %>%
        dplyr::filter(.data$isBinary == 'N')
    }
    if (any(is.null(balance), nrow(balance) == 0)) {
      return(NULL)
    }
    return(balance)
  })
  
  shiny::observe({
    data <- computeBalance()
    if (all(!is.null(data), nrow(data) > 0)) {
      subset <- data$analysisName %>% unique() %>% sort()
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "charCompareAnalysisNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = subset
      )
    }
  })
  
  shiny::observe({
    data <- computeBalance()
    if (all(!is.null(data), nrow(data) > 0)) {
      subset <- data$domainId %>% unique() %>% sort()
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "charaCompareDomainNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = subset
      )
    }
  })
  
  output$saveCompareCohortCharacterizationTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "compareCohortCharacterization")
    },
    content = function(file) {
      downloadCsv(x = computeBalance(), 
                  fileName = file)
    }
  )
  ### Output ------
  output$charCompareTable <- DT::renderDataTable(expr = {
    validate(need((length( getCohortIdFromDropdown()) > 0), 
                  paste0("Please select cohort.")))
    validate(need((length(getComparatorCohortIdFromDropdowm()) > 0), 
                  paste0("Please select comparator cohort.")))
    validate(need((getComparatorCohortIdFromDropdowm() != getCohortIdFromDropdown()),
                  paste0("Please select different cohorts for target and comparator cohorts.")))
    validate(need((length(input$database) > 0),
                  paste0("Please select atleast one datasource.")
    ))
    validate(need(!is.null(compareCharacterizationTemporalCharacterizationData()$covariateValue) &&
                    nrow(compareCharacterizationTemporalCharacterizationData()$covariateValue) > 0, 
                  "No Characterization data"))
    
    
    balance <- computeBalance()
    
    validate(need(all(!is.null(balance), nrow(balance) > 0),
                  "No data available for selected combination."))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Rendering compare characterization data table."), value = 0)
    
    targetCohortIdValue <- balance %>% dplyr::filter(!is.na(.data$cohortId1)) %>% dplyr::pull(.data$cohortId1) %>% unique()
    comparatorcohortIdValue <- balance %>% dplyr::filter(!is.na(.data$cohortId2)) %>% dplyr::pull(.data$cohortId2) %>% unique()
    databaseIdForCohortCharacterization <- balance$databaseId %>% unique()
    
    targetCohortShortName <- cohort %>% 
      dplyr::filter(.data$cohortId == !!targetCohortIdValue) %>% 
      dplyr::select(.data$shortName) %>% 
      dplyr::pull()
    
    comparatorCohortShortName <- cohort %>% 
      dplyr::filter(.data$cohortId == !!comparatorcohortIdValue) %>% 
      dplyr::select(.data$shortName) %>% 
      dplyr::pull()
    
    targetCohortSubjects <- cohortCount %>% 
      dplyr::filter(.data$databaseId == databaseIdForCohortCharacterization) %>% 
      dplyr::filter(.data$cohortId == !!targetCohortIdValue) %>% 
      dplyr::pull(.data$cohortSubjects)
    
    comparatorCohortSubjects <- cohortCount %>% 
      dplyr::filter(.data$databaseId == databaseIdForCohortCharacterization) %>% 
      dplyr::filter(.data$cohortId == !!comparatorcohortIdValue) %>% 
      dplyr::pull(.data$cohortSubjects)
    
    targetCohortHeader <- paste0(targetCohortShortName,
                                 " (n = ",
                                 scales::comma(targetCohortSubjects,
                                               accuracy = 1),
                                 ")")
    
    comparatorCohortHeader <- paste0(comparatorCohortShortName,
                                     " (n = ",
                                     scales::comma(comparatorCohortSubjects,
                                                   accuracy = 1),
                                     ")")
    
    if (input$charCompareType == "Pretty table") {
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Rendering pretty table for compare characterization."), value = 0)
      
      data <- balance %>% 
        dplyr::mutate(covariateName = .data$covariateNameFull)
      
      table <- prepareTable1Comp(data)
      
      validate(need(nrow(table) > 0,
                    "No data available for selected combination."))
      
      table <- table %>%
        dplyr::arrange(.data$sortOrder) %>%
        dplyr::select(-.data$sortOrder) %>%
        dplyr::select(-.data$cohortId1, -.data$cohortId2)
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        scrollX = TRUE,
        scrollY = "60vh",
        searchHighlight = TRUE,
        lengthChange = TRUE,
        ordering = FALSE,
        paging = TRUE,
        columnDefs = list(minCellPercentDef(1:2))
      )
      table <- DT::datatable(
        table,
        options = options,
        rownames = FALSE,
        colnames = c("Characteristic", targetCohortHeader, comparatorCohortHeader, "Std. Diff."),
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      table <- DT::formatStyle(
        table = table,
        columns = 2:4,
        background = DT::styleColorBar(c(0, 1), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      table <- DT::formatStyle(
        table = table,
        columns = 4,
        background = styleAbsColorBar(1, "lightblue", "pink"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      table <- DT::formatRound(table, 4, digits = 2)
    } else {
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Rendering raw table for compare characterization."), value = 0)
      
      balance <- balance %>%
        dplyr::filter(.data$analysisName %in% charCompareAnalysisNameFilter()) %>%
        dplyr::filter(.data$domainId %in% charaCompareDomainNameFilter())
      
      if (!is.null(input$conceptSetsToFilterCharacterization)) {
        balance <- balance %>% 
          dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
      }
      
      validate(need(all(!is.null(balance), nrow(balance) > 0),
                    "No data available for selected combination."))
      
      balance <- balance %>% 
        dplyr::rename("meanTarget" = mean1,
                      "sdTarget" = sd1,
                      "meanComparator" = mean2,
                      "sdComparator" = sd2,
                      "StdDiff" = absStdDiff)
      
      if (input$compareCharacterizationColumnFilters == "Mean and Standard Deviation") {
        
        table <- balance %>%
          dplyr::select(
            .data$covariateName,
            .data$meanTarget,
            .data$sdTarget,
            .data$meanComparator,
            .data$sdComparator,
            .data$StdDiff
          ) %>% 
          dplyr::arrange(desc(StdDiff))
        
        columsDefs <- list(truncateStringDef(0, 80),
                           minCellRealDef(1:5, digits = 2))
        
        colorBarColumns <- c(2,4)
        
        standardDifferenceColumn <- 6
        
        table <- table %>% 
          dplyr::rename(!!targetCohortHeader := .data$meanTarget) %>% 
          dplyr::rename(!!comparatorCohortHeader := .data$meanComparator)
        
      } else {
        table <- balance %>%
          dplyr::select(
            .data$covariateName,
            .data$meanTarget,
            .data$meanComparator,
            .data$StdDiff
          ) %>% 
          dplyr::rename("target" = meanTarget,
                        "comparator" = meanComparator)
        
        columsDefs <- list(truncateStringDef(0, 80),
                           minCellRealDef(1:3, digits = 2))
        
        colorBarColumns <- c(2,3)
        standardDifferenceColumn <- 4
        
        table <- table %>% 
          dplyr::rename(!!targetCohortHeader := .data$target) %>% 
          dplyr::rename(!!comparatorCohortHeader := .data$comparator)
      }
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        searchHighlight = TRUE,
        scrollX = TRUE,
        scrollY = "60vh",
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        columnDefs = columsDefs
      )
      
      table <- DT::datatable(
        table,
        options = options,
        rownames = FALSE,
        colnames = colnames(table) %>%
          camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      table <- DT::formatStyle(
        table = table,
        columns = colorBarColumns,
        background = DT::styleColorBar(c(0, 1), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      table <- DT::formatStyle(
        table = table,
        columns = standardDifferenceColumn,
        background = styleAbsColorBar(1, "lightblue", "pink"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
    }
    return(table)
  }, server = TRUE)
  
  output$charComparePlot <- ggiraph::renderggiraph(expr = {
    data <- computeBalance()
    
    validate(need(nrow(data) > 0,
                  "No data available for selected combination."))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Rendering plot for compare characterization."), value = 0)
    
    data <- data %>%
      dplyr::filter(.data$analysisName %in% charCompareAnalysisNameFilter()) %>%
      dplyr::filter(.data$domainId %in% charaCompareDomainNameFilter())
    
    validate(need(nrow(data) > 0,
                  "No data available for selected combination."))
    
    if (!is.null(input$conceptSetsToFilterCharacterization)) {
      if (length(getResoledAndMappedConceptIdsForFilters()) > 0) {
        data <- data %>% 
          dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
      } else {
        data <- data[0,]
      }
    }
    
    validate(need(nrow(data) > 0,
                  "No data available for selected combination."))
    
    if (input$charCompareType == "Plot" &&
        input$charCompareProportionOrContinuous == "Proportion") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else if (input$charCompareType == "Plot" &&
               input$charCompareProportionOrContinuous == "Continuous") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'N')
    }
    
    validate(need(nrow(data) > 0,
                  "No data available for selected combination."))
    
    plot <-
      plotCohortComparisonStandardizedDifference(
        balance = data,
        shortNameRef = cohort,
        xLimitMin = input$compareCohortXMeanFilter[1],
        xLimitMax = input$compareCohortXMeanFilter[2],
        yLimitMin = input$compareCohortYMeanFilter[1],
        yLimitMax = input$compareCohortYMeanFilter[2]
      )
    return(plot)
  })
  
  #Compare Temporal Characterization.-----
  temporalCompareAnalysisNameFilter <- shiny::reactive(x = {
    return(input$temporalCompareAnalysisNameFilter)
  })
  
  temporalCompareDomainNameFilter <-  shiny::reactive(x = {
    return(input$temporalCompareDomainNameFilter)
  })
  ## Data ------

  computeBalanceForCompareTemporalCharacterization <-
    shiny::reactive({
      validate(need((length( getCohortIdFromDropdown(
      )) > 0),
      paste0("Please select cohort.")))
      validate(need((length(
        getComparatorCohortIdFromDropdowm()
      ) > 0),
      paste0("Please select comparator cohort.")))
      validate(need((getComparatorCohortIdFromDropdowm() != getCohortIdFromDropdown()),
                    paste0("Please select different cohorts for target and comparator cohorts.")
      ))
      validate(need((length(input$database) > 0),
                    paste0("Please select atleast one datasource.")
      ))
      validate(need((length(getTimeIdsFromDropdowm()) > 0), paste0("Please select time id")))
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Computing compare temporal characterization."), value = 0)
      
      validate(need(!is.null(compareCharacterizationTemporalCharacterizationData()$covariateValue) &&
                      nrow(compareCharacterizationTemporalCharacterizationData()$covariateValue) > 0, "No Characterization data"))
      
      data <- compareCharacterizationTemporalCharacterizationData()$covariateValue %>% 
        dplyr::filter(.data$characterizationSource %in% c('CT', 'FT')) %>% 
        dplyr::filter(.data$timeId %in% getTimeIdsFromDropdowm()) %>% 
        dplyr::select(-.data$startDay, -.data$endDay) %>% 
        dplyr::inner_join(compareCharacterizationTemporalCharacterizationData()$covariateRef, 
                          by = c("covariateId", "characterizationSource")) %>% 
        dplyr::inner_join(compareCharacterizationTemporalCharacterizationData()$analysisRef, 
                          by = c("analysisId", "characterizationSource")) %>% 
        dplyr::select(-.data$startDay, -.data$endDay) %>% 
        dplyr::distinct() %>% 
        dplyr::inner_join(compareCharacterizationTemporalCharacterizationData()$temporalTimeRef, 
                          by = 'timeId') %>% 
        dplyr::inner_join(temporalCovariateChoices, by = 'timeId') %>% 
        dplyr::select(-.data$missingMeansZero)
      
      covs1 <- data %>% 
        dplyr::filter(.data$cohortId == getCohortIdFromDropdown()) %>% 
        dplyr::mutate(analysisNameLong = paste0(.data$analysisName, 
                                                " (", 
                                                as.character(.data$startDay), 
                                                " to ", 
                                                as.character(.data$endDay), 
                                                ")")) %>% 
        dplyr::relocate(.data$cohortId, 
                        .data$databaseId, 
                        .data$analysisId,
                        .data$covariateId, 
                        .data$covariateName,
                        .data$isBinary) %>% 
        dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
      
      validate(need((nrow(covs1) > 0), paste0("Target cohort id:", getCohortIdFromDropdown(), " does not have data.")))
      covs2 <- data %>% 
        dplyr::filter(.data$cohortId == getComparatorCohortIdFromDropdowm()) %>% 
        dplyr::mutate(analysisNameLong = paste0(.data$analysisName, 
                                                " (", 
                                                as.character(.data$startDay), 
                                                " to ", 
                                                as.character(.data$endDay), 
                                                ")")) %>% 
        dplyr::relocate(.data$cohortId, 
                        .data$databaseId, 
                        .data$analysisId,
                        .data$covariateId, 
                        .data$covariateName,
                        .data$isBinary) %>% 
        dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
      validate(need((nrow(covs2) > 0), paste0("Target cohort id:", getComparatorCohortIdFromDropdowm(), " does not have data.")))
      
      balance <-
        compareTemporalCohortCharacteristics(covs1, covs2) %>%
        dplyr::mutate(absStdDiff = abs(.data$stdDiff)) %>% 
        dplyr::mutate(covariateName = gsub(".*: ","",.data$covariateName)) %>% 
        dplyr::mutate(covariateName = paste0(.data$covariateName, " (", .data$covariateId, ")"))
      
      if (input$temporalCharacterizationType == "Raw table" &&
          input$temporalCharacterProportionOrContinuous == "Proportion") {
        balance <- balance %>%
          dplyr::filter(.data$isBinary == 'Y')
      } else if (input$temporalCharacterizationType == "Raw table" &&
                 input$temporalCharacterProportionOrContinuous == "Continuous") {
        balance <- balance %>%
          dplyr::filter(.data$isBinary == 'N')
      }
      
      if (input$temporalCharacterizationType == "Plot" &&
          input$temporalCharacterProportionOrContinuous == "Proportion") {
        balance <- balance %>%
          dplyr::filter(.data$isBinary == 'Y')
      } else if (input$temporalCharacterizationType == "Plot" &&
                 input$temporalCharacterProportionOrContinuous == "Continuous") {
        balance <- balance %>%
          dplyr::filter(.data$isBinary == 'N')
      }
      
      return(balance)
    })
  
  output$saveCompareTemporalCharacterizationTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "compareTemporalCharacterization")
    },
    content = function(file) {
      downloadCsv(x = computeBalanceForCompareTemporalCharacterization(), 
                  fileName = file)
    }
  )
  
  shiny::observe({
    subset <-
      computeBalanceForCompareTemporalCharacterization()$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalCompareAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  shiny::observe({
    subset <-
      computeBalanceForCompareTemporalCharacterization()$domainId %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalCompareDomainNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  ## Output ------
  output$temporalCharacterizationCompareTable <-
    DT::renderDataTable(expr = {
      balance <- computeBalanceForCompareTemporalCharacterization()
      
      validate(need(nrow(balance) > 0,
                    "No data available for selected combination."))
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Rendering compare temporal characterization table."), value = 0)
      
      if (input$temporalCharacterizationType == "Pretty table") {
        # table <- prepareTable1Comp(balance)
        # if (nrow(table) > 0) {
        #   table <- table %>%
        #     dplyr::arrange(.data$sortOrder) %>%
        #     dplyr::select(-.data$sortOrder) %>%
        #     dplyr::select(-.data$cohortId1, -.data$cohortId2)
        # } else {
        #   return(dplyr::tibble(Note = "No data for covariates that are part of pretty table."))
        # }
        # options = list(
        #   pageLength = 100,
        #   lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        #   searching = TRUE,
        #   scrollX = TRUE,
        #   scrollY = "60vh",
        #   searchHighlight = TRUE,
        #   lengthChange = TRUE,
        #   ordering = FALSE,
        #   paging = TRUE,
        #   columnDefs = list(minCellPercentDef(1:2))
        # )
        # 
        # table <- DT::datatable(
        #   table,
        #   options = options,
        #   rownames = FALSE,
        #   colnames = c("Characteristic", "Target", "Comparator", "Std. Diff."),
        #   escape = FALSE,
        #   filter = "top",
        #   class = "stripe nowrap compact"
        # )
        # table <- DT::formatStyle(
        #   table = table,
        #   columns = 2:4,
        #   background = DT::styleColorBar(c(0, 1), "lightblue"),
        #   backgroundSize = "98% 88%",
        #   backgroundRepeat = "no-repeat",
        #   backgroundPosition = "center"
        # )
        # table <- DT::formatStyle(
        #   table = table,
        #   columns = 4,
        #   background = styleAbsColorBar(1, "lightblue", "pink"),
        #   backgroundSize = "98% 88%",
        #   backgroundRepeat = "no-repeat",
        #   backgroundPosition = "center"
        # )
        # table <- DT::formatRound(table, 4, digits = 2)
      } else {
        balance <- balance %>%
          dplyr::filter(.data$analysisName %in% temporalCompareAnalysisNameFilter()) %>%
          dplyr::filter(.data$domainId %in% temporalCompareDomainNameFilter())
        
        if (!is.null(input$conceptSetsToFilterCharacterization)) {
          balance <- balance %>% 
            dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
        }
        
        validate(need(all(!is.null(balance), nrow(balance) > 0),
                      "No data available for selected combination."))
        # 
        # if (nrow(balance) == 0) {
        #   return(dplyr::tibble(Note = "No data for the selected combination."))
        # }
        
        targetCohortIdValue <- balance %>% dplyr::filter(!is.na(.data$cohortId1)) %>% dplyr::pull(.data$cohortId1) %>% unique()
        comparatorcohortIdValue <- balance %>% dplyr::filter(!is.na(.data$cohortId2)) %>% dplyr::pull(.data$cohortId2) %>% unique()
        databaseIdForCohortCharacterization <- balance$databaseId %>% unique()
        
        targetCohortShortName <- cohort %>% 
          dplyr::filter(.data$cohortId == !!targetCohortIdValue) %>% 
          dplyr::select(.data$shortName) %>% 
          dplyr::pull()
        
        comparatorCohortShortName <- cohort %>% 
          dplyr::filter(.data$cohortId == !!comparatorcohortIdValue) %>% 
          dplyr::select(.data$shortName) %>% 
          dplyr::pull()
        
        targetCohortSubjects <- cohortCount %>% 
          dplyr::filter(.data$databaseId == databaseIdForCohortCharacterization) %>% 
          dplyr::filter(.data$cohortId == !!targetCohortIdValue) %>% 
          dplyr::pull(.data$cohortSubjects)
        
        comparatorCohortSubjects <- cohortCount %>% 
          dplyr::filter(.data$databaseId == databaseIdForCohortCharacterization) %>% 
          dplyr::filter(.data$cohortId == !!comparatorcohortIdValue) %>% 
          dplyr::pull(.data$cohortSubjects)
        
        targetCohortHeader <- paste0(targetCohortShortName,
                                     " (n = ",
                                     scales::comma(targetCohortSubjects,
                                                   accuracy = 1),
                                     ")")
        
        comparatorCohortHeader <- paste0(comparatorCohortShortName,
                                         " (n = ",
                                         scales::comma(comparatorCohortSubjects,
                                                       accuracy = 1),
                                         ")")
        
        balance <- balance %>% 
          dplyr::rename("meanTarget" = mean1, 
                        "sDTarget" = sd1,
                        "meanComparator" = mean2,
                        "sDComparator" = sd2,
                        "stdDiff" = stdDiff)
        
        temporalCovariateChoicesSelected <-
          temporalCovariateChoices %>%
          dplyr::filter(.data$timeId %in% c(getTimeIdsFromDropdowm())) %>%
          dplyr::arrange(.data$timeId) %>% 
          dplyr::pull(.data$choices)
        
        if (input$temporalCharacterizationTypeColumnFilter == "Mean and Standard Deviation") {
          table <- balance %>%
            # dplyr::mutate(covariateName = paste(.data$covariateName, "(", .data$conceptId, ")")) %>% 
            dplyr::arrange(desc(abs(.data$stdDiff)))
          
          if (length(temporalCovariateChoicesSelected) == 1) {
            table <- table %>%
              dplyr::arrange(.data$choices) %>% 
              tidyr::pivot_wider(id_cols = c("covariateName"),
                                 names_from = "choices",
                                 values_from = c("meanTarget","sDTarget","meanComparator","sDComparator","stdDiff"),
                                 values_fill = 0
              )
            
            columnDefs <- list(truncateStringDef(0, 80),
                               minCellRealDef(1:(length(temporalCovariateChoicesSelected) * 5), digits = 2))
            
            colorBarColumns <- 1 + 1:(length(temporalCovariateChoicesSelected) * 5)
            
            colspan <- 5
            
            containerColumns <- c(paste0("Mean ", targetCohortShortName),
                                  paste0("SD ", targetCohortShortName),
                                  paste0("Mean ", comparatorCohortShortName),
                                  paste0("SD ", comparatorCohortShortName),
                                  "Std. Diff")
          } else {
            table <- table %>% 
              dplyr::arrange(.data$choices) %>% 
              dplyr::rename(aMeanTarget = "meanTarget", 
                            bSdTarget = "sDTarget",
                            cMeanComparator = "meanComparator",
                            dSdComparator = "sDComparator") %>% 
              tidyr::pivot_longer(cols = c("aMeanTarget","bSdTarget","cMeanComparator","dSdComparator"),
                                  names_to = "type", 
                                  values_to = "values" 
              ) %>% 
              dplyr::mutate(names = paste0(.data$databaseId, " ", .data$choices, " ", .data$type)) %>% 
              dplyr::arrange(.data$databaseId, .data$startDay, .data$endDay, .data$type) %>% 
              tidyr::pivot_wider(id_cols = c("covariateName"),
                                 names_from = "names",
                                 values_from = c("values"),
                                 values_fill = 0
              )
            
            columnDefs <- list(truncateStringDef(0, 80),
                               minCellRealDef(1:(length(temporalCovariateChoicesSelected) * 4), digits = 2))
            
            colorBarColumns <- 1 + 1:(length(temporalCovariateChoicesSelected) * 4)
            
            colspan <- 4
            
            containerColumns <- c(paste0("Mean ", targetCohortShortName),
                                  paste0("SD ", targetCohortShortName),
                                  paste0("Mean ", comparatorCohortShortName),
                                  paste0("SD ", comparatorCohortShortName))
          }
        } else {
          
          table <- balance %>%
            # dplyr::mutate(covariateName = paste(.data$covariateName, "(", .data$conceptId, ")")) %>% 
            dplyr::arrange(desc(abs(.data$stdDiff))) 
          
          if (length(temporalCovariateChoicesSelected) == 1) {
            table <- table %>% 
              tidyr::pivot_wider(id_cols = c("covariateName"),
                                 names_from = "choices",
                                 values_from = c("meanTarget", "meanComparator", "stdDiff"),
                                 values_fill = 0
              )
            
            containerColumns <- c(targetCohortShortName, comparatorCohortShortName, "Std. Diff")
            
            columnDefs <- list(truncateStringDef(0, 80),
                               minCellRealDef(1:(length(temporalCovariateChoicesSelected) * 3), digits = 2))
            colorBarColumns <- 1 + 1:(length(temporalCovariateChoicesSelected) * 3)
            
            colspan <- 3
          } else {
            table <- table %>% 
              tidyr::pivot_longer(cols = c("meanTarget", "meanComparator"), 
                                  names_to = "type", 
                                  values_to = "values") %>% 
              dplyr::mutate(names = paste0(.data$databaseId, " ", .data$choices, " ", .data$type)) %>%
              dplyr::arrange(.data$startDay, .data$endDay) %>% 
              tidyr::pivot_wider(id_cols = c("covariateName"),
                                 names_from = "names",
                                 values_from = "values",
                                 values_fill = 0
              )
            
            containerColumns <- c(targetCohortShortName, comparatorCohortShortName)
            
            columnDefs <- list(truncateStringDef(0, 80),
                               minCellRealDef(1:(length(temporalCovariateChoicesSelected) * 2), digits = 2))
            colorBarColumns <- 1 + 1:(length(temporalCovariateChoicesSelected) * 2)
            colspan <- 2
          }
        }
        
        sketch <- htmltools::withTags(table(class = "display",
                                            thead(tr(
                                              th(rowspan = 2, "Covariate Name"),
                                              lapply(temporalCovariateChoicesSelected, th, colspan = colspan, class = "dt-center", style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                            ),
                                            tr(
                                              lapply(rep(
                                                containerColumns, length(temporalCovariateChoicesSelected)
                                              ), th, style = "border-right:1px solid grey")
                                            ))))
        
        options = list(
          pageLength = 100,
          lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
          searching = TRUE,
          searchHighlight = TRUE,
          scrollX = TRUE,
          scrollY = "60vh",
          lengthChange = TRUE,
          ordering = TRUE,
          paging = TRUE,
          columnDefs = columnDefs
        )
        
        table <- DT::datatable(
          table,
          options = options,
          rownames = FALSE,
          container = sketch,
          colnames = colnames(table) %>%
            camelCaseToTitleCase(),
          escape = FALSE,
          filter = "top",
          class = "stripe nowrap compact"
        )
        table <- DT::formatStyle(
          table = table,
          columns = colorBarColumns,
          background = DT::styleColorBar(c(0, 1), "lightblue"),
          backgroundSize = "98% 88%",
          backgroundRepeat = "no-repeat",
          backgroundPosition = "center"
        )
      }
      return(table)
    }, server = TRUE)
  
  output$temporalCharComparePlot <- ggiraph::renderggiraph(expr = {
    data <- computeBalanceForCompareTemporalCharacterization()
    validate(need(nrow(data) != 0, paste0("No data for the selected combination.")))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Rendering plot for compare temporal characterization."), value = 0)
    
    data <- data %>%
      dplyr::filter(.data$analysisName %in% temporalCompareAnalysisNameFilter()) %>%
      dplyr::filter(.data$domainId %in% temporalCompareDomainNameFilter()) 
    
    if (!is.null(input$conceptSetsToFilterCharacterization)) {
      if (length(getResoledAndMappedConceptIdsForFilters()) > 0) {
        data <- data %>% 
          dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
      } else {
        data <- data[0,]
      }
    }
    
    validate(need(nrow(data) != 0, paste0("No data for the selected combination.")))
    
    validate(need((nrow(data) - nrow(data[data$mean1 < 0.001, ])) > 5 &&
                    (nrow(data) - nrow(data[data$mean2 < 0.001, ])) > 5, paste0("No data for the selected combination.")))
    
    plot <-
      plotTemporalCompareStandardizedDifference(
        balance = data,
        shortNameRef = cohort,
        xLimitMin = input$temporalCharacterizationXMeanFilter[1],
        xLimitMax = input$temporalCharacterizationXMeanFilter[2],
        yLimitMin = input$temporalCharacterizationYMeanFilter[1],
        yLimitMax = input$temporalCharacterizationYMeanFilter[2]
      )
    return(plot)
  })
  
  output$databaseInformationTable <- DT::renderDataTable(expr = {
    
    validate(need(all(!is.null(database), nrow(database) > 0),
                  "No data available for selected combination."))

    data <- database 
    if (!'vocabularyVersionCdm' %in% colnames(database)) {
      data$vocabularyVersionCdm <- "Not in data"
    }
    if (!'vocabularyVersion' %in% colnames(database)) {
      data$vocabularyVersion <- "Not in data"
    }
    if (!'persons' %in% colnames(data)) {
      data$persons <- as.integer(NA)
    }
    if (!'records' %in% colnames(data)) {
      data$records <- as.integer(NA)
    }
    if (!'personDays' %in% colnames(data)) {
      data$personDays <- as.integer(NA)
    }
    if (!'observationPeriodMinDate' %in% colnames(data)) {
      data$observationPeriodMinDate <- as.Date(NA)
    }
    if (!'observationPeriodMaxDate' %in% colnames(data)) {
      data$observationPeriodMaxDate <- as.Date(NA)
    }
    data <- data %>%
      dplyr::select(
        .data$databaseId,
        .data$databaseName,
        .data$vocabularyVersionCdm,
        .data$vocabularyVersion,
        .data$description,
        # .data$isMetaAnalysis,
        .data$observationPeriodMinDate,
        .data$observationPeriodMaxDate,
        .data$persons,
        .data$records,
        .data$personDays
      )
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      searchHighlight = TRUE,
      columnDefs = list(
        list(width = "50%", targets = 4)
      )
    )
    sketch <- htmltools::withTags(table(class = "display",
                                        thead(
                                          tr(
                                            th(rowspan = 2, "ID"),
                                            th(rowspan = 2, "Name"),
                                            th(
                                              "Vocabulary version",
                                              colspan = 2,
                                              class = "dt-center",
                                              style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                            ),
                                            th(rowspan = 2, "Description"),
                                            # th(rowspan = 2, "Is Meta Analysis"),
                                            th(rowspan = 2, "Observation Period Min Date"),
                                            th(rowspan = 2, "Observation Period Max Date"),
                                            th(rowspan = 2, "Persons"),
                                            th(rowspan = 2, "Records"),
                                            th(rowspan = 2, "Person Days")
                                          ),
                                          tr(lapply(
                                            c("CDM source", "Vocabulary table"), th, style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                          ))
                                        )))
    table <- DT::datatable(
      data ,
      options = options,
      container = sketch,
      rownames = FALSE,
      class = "stripe compact"
    ) 
    return(table)
  }, server = TRUE)
  
  output$packageDependencySnapShotTable <- DT::renderDataTable(expr = {
    data <- metadata %>% 
      dplyr::filter(.data$variableField == "packageDependencySnapShotJson") %>% 
      dplyr::pull(.data$valueField)
    
    if (length(data) == 0) {
      return(NULL)
    }
    
    result <- as.data.frame(RJSONIO::fromJSON(data[1]))
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      scrollY = "60vh",
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE
    )
    
    table <- DT::datatable(
      result,
      options = options,
      rownames = FALSE,
      colnames = colnames(result) %>%
        camelCaseToTitleCase(),
      escape = FALSE,
      filter = "top",
      class = "stripe nowrap compact"
    )
    return(table)
  })
  
  output$argumentsAtDiagnosticsInitiationTable <- DT::renderDataTable(expr = {
    data <- metadata %>% 
      dplyr::filter(.data$variableField == "argumentsAtDiagnosticsInitiationJson") %>% 
      dplyr::pull(.data$valueField)
    
    if (length(data) == 0) {
      return(NULL)
    }
    
    result <- RJSONIO::fromJSON(data[1])
    result$temporalCovariateSettings <- NULL
    result$covariateSettings <- NULL
    
    result <- as.data.frame(result)
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      scrollY = "60vh",
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE
    )
    
    table <- DT::datatable(
      result,
      options = options,
      rownames = FALSE,
      colnames = colnames(result) %>%
        camelCaseToTitleCase(),
      escape = FALSE,
      filter = "top",
      class = "stripe nowrap compact"
    )
    return(table)
  })
  
  
  # Infoboxes ------
  showInfoBox <- function(title, htmlFileName) {
    shiny::showModal(shiny::modalDialog(
      title = title,
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(readChar(
        htmlFileName, file.info(htmlFileName)$size
      ))
    ))
  }
  
  shiny::observeEvent(input$cohortCountsInfo, {
    showInfoBox("Cohort Counts", "html/cohortCounts.html")
  })
  
  shiny::observeEvent(input$incidenceRateInfo, {
    showInfoBox("Incidence Rate", "html/incidenceRate.html")
  })
  
  shiny::observeEvent(input$timeDistributionInfo, {
    showInfoBox("Time Distributions", "html/timeDistribution.html")
  })
  
  shiny::observeEvent(input$includedConceptsInfo, {
    showInfoBox("Concepts in data source",
                "html/conceptsInDataSource.html")
  })
  
  shiny::observeEvent(input$orphanConceptsInfo, {
    showInfoBox("Orphan (Source) Concepts", "html/orphanConcepts.html")
  })
  
  shiny::observeEvent(input$conceptSetDiagnosticsInfo, {
    showInfoBox("Concept Set Diagnostics",
                "html/conceptSetDiagnostics.html")
  })
  
  shiny::observeEvent(input$inclusionRuleStatsInfo, {
    showInfoBox("Inclusion Rule Statistics",
                "html/inclusionRuleStats.html")
  })
  
  shiny::observeEvent(input$indexEventBreakdownInfo, {
    showInfoBox("Index Event Breakdown", "html/indexEventBreakdown.html")
  })
  
  shiny::observeEvent(input$visitContextInfo, {
    showInfoBox("Visit Context", "html/visitContext.html")
  })
  
  shiny::observeEvent(input$cohortCharacterizationInfo, {
    showInfoBox("Cohort Characterization",
                "html/cohortCharacterization.html")
  })
  
  shiny::observeEvent(input$temporalCharacterizationInfo, {
    showInfoBox("Temporal Characterization",
                "html/temporalCharacterization.html")
  })
  
  shiny::observeEvent(input$cohortOverlapInfo, {
    showInfoBox("Cohort Overlap", "html/cohortOverlap.html")
  })
  
  shiny::observeEvent(input$compareCohortCharacterizationInfo, {
    showInfoBox("Compare Cohort Characteristics",
                "html/compareCohortCharacterization.html")
  })
  
  shiny::observeEvent(input$timeSeriesInfo, {
    showInfoBox("Time Series", "html/timeSeries.html")
  })
  
  # Cohort labels ----
  targetCohortCount <- shiny::reactive({
    targetCohortWithCount <-
      getCohortCountResult(
        dataSource = dataSource,
        cohortIds = getCohortIdFromDropdown(),
        databaseIds = input$database
      ) %>%
      dplyr::left_join(y = cohort, by = "cohortId") %>%
      dplyr::arrange(.data$cohortName)
    return(targetCohortWithCount)
  })
  
  targetCohortCountHtml <- shiny::reactive({
    targetCohortCount <- targetCohortCount()
    return(htmltools::withTags(div(
      h5(
        "Target: ",
        targetCohortCount$cohortName,
        " ( n = ",
        scales::comma(x = targetCohortCount$cohortSubjects),
        " )"
      )
    )))
  })
  
  selectedCohorts <- shiny::reactive({
    
    if (any(is.null( getCohortIdsFromDropdown()), length( getCohortIdsFromDropdown()) == 0)) {return(NULL)}
    if (any(is.null(getCohortSortedByCohortId()), nrow(getCohortSortedByCohortId()) == 0)) {return(NULL)}
    if (any(is.null(getDatabaseIdsFromDropdown()), nrow(getDatabaseIdsFromDropdown()) == 0)) {return(NULL)}
    
    cohortSelected <- getCohortSortedByCohortId() %>%
      dplyr::filter(.data$cohortId %in%  getCohortIdsFromDropdown()) %>%
      dplyr::arrange(.data$cohortId)
    
    databaseIdsWithCount <- cohortCount %>% 
      dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown()) %>% 
      dplyr::distinct(.data$cohortId,.data$databaseId)
    
    distinctDatabaseIdsWithCount <- length(databaseIdsWithCount$databaseId %>% unique())
    
    for (i in 1:nrow(cohortSelected)) {
      filteredDatabaseIds <-
        databaseIdsWithCount[databaseIdsWithCount$cohortId == cohortSelected$cohortId[i], ] %>%
        dplyr::pull()
      
      count <- length(filteredDatabaseIds)
      
      if (distinctDatabaseIdsWithCount == count) {
        cohortSelected$compoundName[i] <- cohortSelected$compoundName[i]
          # paste( #, "- in all data sources", sep = " ")
      } else {
        countPercentage <- round(count / distinctDatabaseIdsWithCount * 100, 2)
        
        cohortSelected$compoundName[i] <-
          paste(
            cohortSelected$compoundName[i],
            " - in ",
            count,
            "/",
            distinctDatabaseIdsWithCount,
            " (",
            countPercentage,
            "%)",
            " data sources ",
            paste(unlist(filteredDatabaseIds), collapse = ', ')
          )
      }
    }
    return(cohortSelected);
  })
  
  renderedSelectedCohorts <- shiny::reactive({
    cohortSelected <- selectedCohorts() %>% 
      dplyr::select(.data$compoundName)
    
    return(apply(cohortSelected, 1, function(x)
      tags$tr(lapply(x, tags$td))))
  })
  
  selectedCohort <- shiny::reactive({
    return(input$cohort)
  })
  
  selectedComparatorCohort <- shiny::reactive({
    return(input$comparatorCohort)
  })
  
  buildCohortConditionTable <- function(messsege,cohortCompoundNameArray) {
    tags$table(tags$tr(tags$td(tags$b(messsege))),
               lapply(cohortCompoundNameArray, function(x)
                 tags$tr(lapply(x, tags$td)))
    )
  }
  
  # Notes Cohort Count ----
  output$cohortCountsCategories <-
    shiny::renderUI({
      cohortSelected <- selectedCohorts()
      cohortCountSelected <- cohortSelected %>%
        dplyr::inner_join(cohortCount, by = c('cohortId')) %>%
        dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown())
      
      cohortSubjectCountEq0 <- c()   # category 1 -> n == 0
      cohortSubjectCount0To100 <- c()   # category 2 -> 0 < n < 100
      cohortSubjectCount100To2500 <- c()   # category 3 -> 100 < n < 2500
      
      cohortSubjectRecordRatioEq1 <- c()    # category 1 -> 1 record per subject (ratio = 1)
      cohortSubjectRecordRatioGt1 <- c()    # category 2 -> more than 1 record per subject (ratio > 1)
      
      cohortsWithLowestSubjectConts <- c()
      cohortsWithHighestSubjectConts <- c()
      
      for (i in 1:nrow(cohortCountSelected)) {
        if (cohortCountSelected$cohortSubjects[i] == 0) {
          if (length(cohortSubjectCountEq0[grep(cohortCountSelected$compoundName[i],
                                                cohortSubjectCountEq0,
                                                fixed = TRUE)]) <= 0) {
            cohortSubjectCountEq0 <-
              c(
                cohortSubjectCountEq0,
                paste(
                  cohortCountSelected$compoundName[i],
                  cohortCountSelected$databaseId[i],
                  sep = " - "
                )
              )
          } else {
            cohortSubjectCountEq0[grep(cohortCountSelected$compoundName[i],
                                       cohortSubjectCountEq0,
                                       fixed = TRUE)] <-
              paste(cohortSubjectCountEq0[grep(cohortCountSelected$compoundName[i],
                                               cohortSubjectCountEq0,
                                               fixed = TRUE)],
                    cohortCountSelected$databaseId[i],
                    sep = ",")
          }
        } else if (cohortCountSelected$cohortSubjects[i] > 0 &&
                   cohortCountSelected$cohortSubjects[i] <= 100) {
          if (length(cohortSubjectCount0To100[grep(cohortCountSelected$compoundName[i],
                                                   cohortSubjectCount0To100,
                                                   fixed = TRUE)]) <= 0) {
            cohortSubjectCount0To100 <-
              c(
                cohortSubjectCount0To100,
                paste(
                  cohortCountSelected$compoundName[i],
                  cohortCountSelected$databaseId[i],
                  sep = " - "
                )
              )
          } else {
            cohortSubjectCount0To100[grep(cohortCountSelected$compoundName[i],
                                          cohortSubjectCount0To100,
                                          fixed = TRUE)] <-
              paste(cohortSubjectCount0To100[grep(cohortCountSelected$compoundName[i],
                                                  cohortSubjectCount0To100,
                                                  fixed = TRUE)],
                    cohortCountSelected$databaseId[i],
                    sep = ",")
          }
        } else if (cohortCountSelected$cohortSubjects[i] > 100 &&
                   cohortCountSelected$cohortSubjects[i] < 2500) {
          if (length(cohortSubjectCount100To2500[grep(cohortCountSelected$compoundName[i],
                                                      cohortSubjectCount100To2500,
                                                      fixed = TRUE)]) <= 0) {
            cohortSubjectCount100To2500 <-
              c(
                cohortSubjectCount100To2500,
                paste(
                  cohortCountSelected$compoundName[i],
                  cohortCountSelected$databaseId[i],
                  sep = " - "
                )
              )
          } else {
            cohortSubjectCount100To2500[grep(cohortCountSelected$compoundName[i],
                                             cohortSubjectCount100To2500,
                                             fixed = TRUE)] <-
              paste(cohortSubjectCount100To2500[grep(cohortCountSelected$compoundName[i],
                                                     cohortSubjectCount100To2500,
                                                     fixed = TRUE)],
                    cohortCountSelected$databaseId[i],
                    sep = ",")
          }
        }
        
        recordPerSubject <-
          cohortCountSelected$cohortEntries[i] / cohortCountSelected$cohortSubjects[i]
        if (recordPerSubject == 1 &&
            !(cohortCountSelected$databaseId[i] %in% cohortSubjectRecordRatioEq1)) {
          cohortSubjectRecordRatioEq1 <-
            c(cohortSubjectRecordRatioEq1,
              cohortCountSelected$databaseId[i])
        } else if (recordPerSubject > 1 &&
                   !(cohortCountSelected$databaseId[i] %in% cohortSubjectRecordRatioGt1)) {
          cohortSubjectRecordRatioGt1 <-
            c(cohortSubjectRecordRatioGt1,
              cohortCountSelected$databaseId[i])
        }
      }
      
      distinctCohortIds <- cohortCountSelected$cohortId %>%  unique()
      
      for (i in 1:length(distinctCohortIds)) {
        cohortDetailsForDistinctCohortIds <- cohortCountSelected %>%
          dplyr::filter(.data$cohortId == distinctCohortIds[i])
        cohortNameOfDistinctCohortId <-
          cohortDetailsForDistinctCohortIds$compoundName %>% unique()
        if (nrow(cohortDetailsForDistinctCohortIds) >= 10) {
          cohortPercentile <-
            cohortDetailsForDistinctCohortIds$cohortSubjects %>%
            quantile(c(0.1, 0.9)) %>%
            round(0)
          
          filteredCohortDetailsWithLowPercentile <-
            cohortDetailsForDistinctCohortIds %>%
            dplyr::filter(.data$cohortSubjects < cohortPercentile[[1]])
          
          if (nrow(filteredCohortDetailsWithLowPercentile) > 0) {
            cohortsWithLowestSubjectConts <- c(cohortsWithLowestSubjectConts,
                                               paste(
                                                 cohortNameOfDistinctCohortId,
                                                 paste(filteredCohortDetailsWithLowPercentile$databaseId, collapse = ", "),
                                                 sep = " - "
                                               ))
          }
          
          filteredCohortDetailsWithHighPercentile <-
            cohortDetailsForDistinctCohortIds %>%
            dplyr::filter(.data$cohortSubjects > cohortPercentile[[2]])
          
          if (nrow(filteredCohortDetailsWithHighPercentile) > 0) {
            cohortsWithHighestSubjectConts <- c(cohortsWithHighestSubjectConts,
                                                paste(
                                                  cohortNameOfDistinctCohortId,
                                                  paste(filteredCohortDetailsWithHighPercentile$databaseId, collapse = ", "),
                                                  sep = " - "
                                                ))
          }
        }
      }
      
      tags$div(
        tags$b("Cohorts with low subject count :"),
        tags$div(if (length(cohortSubjectCountEq0) > 0) {
          buildCohortConditionTable("cohorts were found to be empty", cohortSubjectCountEq0)
        }),
        tags$div(if (length(cohortSubjectCount0To100) > 0) {
          buildCohortConditionTable(
            "cohorts were found to have low cohort counts and may not be suitable for most studies",
            cohortSubjectCount0To100
          )
        }),
        tags$div(if (length(cohortSubjectCount100To2500) > 0) {
          buildCohortConditionTable(
            "Cohorts were found to have counts less than 2,500. As a general rule of thumb - these cohorts may not be suitable for use as exposure cohorts",
            cohortSubjectCount100To2500
          )
        }),
        tags$div(if (length(cohortSubjectCountEq0) <= 0 &&
                     length(cohortSubjectCount0To100) <= 0 &&
                     length(cohortSubjectCount100To2500) <= 0) {
          tags$p("There are no cohorts with subject counts less than 2,500")
        }),
        tags$br(),
        tags$b("Records per subjects :"),
        tags$div(if (length(cohortSubjectRecordRatioEq1) > 0) {
          tags$p(
            paste0(
              scales::percent(length(cohortSubjectRecordRatioEq1)/length(getDatabaseIdsFromDropdown()), accuracy = 0.1),
              " of the datasources have one record per subject - ",
              paste(
                cohortSubjectRecordRatioEq1, collapse =  ", "
              )
            )
          )
        }),
        tags$div(if (length(cohortSubjectRecordRatioGt1) > 0) {
          tags$p(
            paste0(
              "    ",
              length(cohortSubjectRecordRatioGt1),
              "/",
              length(getDatabaseIdsFromDropdown()),
              " of the datasources that have more than 1 record per subject count - ",
              paste(
                cohortSubjectRecordRatioGt1, collapse = ", "
              )
            )
          )
        }),
        tags$br(),
        tags$div(if (length(cohortsWithLowestSubjectConts) > 0) {
          buildCohortConditionTable(
            "Cohorts with lowest subject count(s): ",
            cohortsWithLowestSubjectConts
          )
        }),
        tags$br(),
        tags$div(if (length(cohortsWithHighestSubjectConts) > 0) {
          buildCohortConditionTable(
            "Cohorts with highest subject count(s): ",
            cohortsWithHighestSubjectConts
          )
        })
      )
    })
  
  
  output$cohortCountsSelectedCohorts <-
    shiny::renderUI({
      renderedSelectedCohorts()
    })
  output$inclusionRuleStatSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$includedConceptsSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$orphanConceptsSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$indexEventBreakdownSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$characterizationSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$inclusionRuleStatSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$cohortOverlapSelectedCohort <-
    shiny::renderUI({
      renderedSelectedCohorts()
    })
  output$incidenceRateSelectedCohorts <-
    shiny::renderUI({
      renderedSelectedCohorts()
    })
  output$timeSeriesSelectedCohorts <-
    shiny::renderUI({
      renderedSelectedCohorts()
    })
  output$timeDistSelectedCohorts <-
    shiny::renderUI({
      renderedSelectedCohorts()
    })
  output$visitContextSelectedCohort <-
    shiny::renderUI({
      selectedCohort()
    })
  output$temporalCharacterizationSelectedCohort <-
    shiny::renderUI({
      return(selectedCohort())
    })
  
  output$temporalCharacterizationSelectedDatabase <-
    shiny::renderUI({
      return(input$database)
    })
  
  output$cohortCharCompareSelectedCohort <- shiny::renderUI({
    htmltools::withTags(table(tr(td(
      selectedCohort()
    )),
    tr(td(
      selectedComparatorCohort()
    ))))
  })
  
  output$cohortCharCompareSelectedDatabase <-
    shiny::renderUI({
      return(input$database)
    })
  
  output$temporalCharCompareSelectedCohort <-
    shiny::renderUI({
      htmltools::withTags(table(tr(td(
        selectedCohort()
      )),
      tr(td(
        selectedComparatorCohort()
      ))))
    })
  
  output$temporalCharCompareSelectedDatabase <-
    shiny::renderUI({
      return(input$database)
    })
  
})
