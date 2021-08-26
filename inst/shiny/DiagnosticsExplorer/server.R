shiny::shinyServer(function(input, output, session) {
  #______________----
  #Reactive functions that are initiated on start up----
  ##getOmopDomainInformation---
  getOmopDomainInformation <- shiny::reactive(x = {
    data <- getDomainInformation()
    data <- data$wide
    return(data)
  })
  
  ##getOmopDomainInformationLong---
  getOmopDomainInformationLong <- shiny::reactive(x = {
    data <- getDomainInformation()
    data <- data$long
    return(data)
  })
  
  ##getNonEraCdmTableShortNames----
  getNonEraCdmTableShortNames <- shiny::reactive({
    data <- getOmopDomainInformation() %>%
      dplyr::filter(.data$isEraTable == FALSE) %>%
      dplyr::select(.data$domainTableShort) %>%
      dplyr::distinct() %>%
      dplyr::arrange() %>%
      dplyr::pull()
    return(data)
  })
  
  
  
  #!!!!!!!!!!!!!!lets remove it
  ##getConceptCountData----
  #loads the entire data into R memory.
  # an alternative design maybe to load into R memory on start up (global.R)
  # but the size may become too large and we may want to filter to cohorts/database of choice
  # to do this - we have to replace by function that joins cohortId/databaseId choices to resolved concepts
  getConceptCountData <-
    shiny::reactive(x = {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Caching concept counts for reuse"),
                   value = 0)
      data <- list()
      conceptCount <-
        getResultsConceptCount(dataSource = dataSource,
                               databaseIds = database$databaseId)
      if (any(is.null(conceptCount),
              nrow(conceptCount) == 0)) {
        return(NULL)
      }
      conceptCount <- conceptCount %>%
        dplyr::rename(domainTableShort = .data$domainTable) %>%
        dplyr::rename(domainFieldShort = .data$domainField)
      
      conceptSubjects <-
        getResultsConceptSubjects(dataSource = dataSource,
                                  databaseIds = database$databaseId)
      if (any(is.null(conceptSubjects),
              nrow(conceptSubjects) == 0)) {
        return(NULL)
      }
      conceptSubjects <- conceptSubjects %>%
        dplyr::rename(domainTableShort = .data$domainTable) %>%
        dplyr::rename(domainFieldShort = .data$domainField)
      data$conceptCount <- conceptCount
      data$conceptSubjects <- conceptSubjects
      return(data)
    })
  
  
  ##getConceptCountConceptIdLevel----
  getConceptCountConceptIdLevel <-
    shiny::reactive(x = {
      data <- getConceptCountData()
      if (any(is.null(data),
              length(data) == 0)) {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Aggregating concept counts"),
                   value = 0)
      
      conceptCount <- data$conceptCount %>%
        dplyr::filter(.data$domainTableShort %in% getNonEraCdmTableShortNames()) %>%
        dplyr::group_by(.data$conceptId,
                        .data$databaseId) %>%
        dplyr::summarise(conceptCount = sum(.data$conceptCount),
                         .groups = 'keep') %>%
        dplyr::ungroup()
      
      conceptSubjects <- data$conceptSubjects %>%
        dplyr::filter(.data$domainTableShort %in% getNonEraCdmTableShortNames()) %>%
        dplyr::group_by(.data$conceptId,
                        .data$databaseId) %>%
        dplyr::summarise(subjectCount = max(.data$subjectCount),
                         .groups = 'keep') %>%
        dplyr::ungroup()
      
      data <- conceptCount %>%
        dplyr::inner_join(conceptSubjects,
                          by = c('databaseId',
                                 'conceptId')) %>%
        dplyr::arrange(.data$conceptId, .data$databaseId)
      
      return(data)
    })
  
  ##!!!! use getConceptCountTsibbleAtConceptIdYearMonthLevel to plot time series when
  # a concept id is selected any where in resolved, excluded or orphan
  ##getConceptCountTsibbleAtConceptIdYearMonthLevel----
  getConceptCountTsibbleAtConceptIdYearMonthLevel <-
    shiny::reactive(x = {
      data <- getConceptCountData()
      if (any(is.null(data),
              length(data) == 0)) {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Aggregating concept counts (Year & Month)"),
        value = 0
      )
      
      conceptCount <- data$conceptCount %>%
        dplyr::filter(.data$domainTableShort %in% getNonEraCdmTableShortNames()) %>%
        dplyr::mutate(periodBegin = ISOdate(
          year = .data$eventYear,
          month = .data$eventMonth,
          day = 1
        )) %>%
        dplyr::group_by(.data$conceptId,
                        .data$databaseId,
                        .data$periodBegin) %>%
        dplyr::summarise(conceptCount = sum(.data$conceptCount),
                         .groups = 'keep') %>%
        dplyr::ungroup() %>%
        dplyr::filter(.data$conceptCount > 0) %>%
        tsibble::as_tsibble(
          key = c(.data$conceptId, .data$databaseId),
          index = .data$periodBegin
        ) %>%
        tsibble::fill_gaps(conceptCount = 0) %>%
        dplyr::arrange(.data$conceptId,
                       .data$databaseId,
                       .data$periodBegin)
      return(data)
    })
  
  ##!!!! use getConceptCountTsibbleAtConceptIdYearMonthLevel to plot time series when
  # a concept id is selected any where in resolved, excluded or orphan
  ##getConceptCountTsibbleAtConceptIdYearLevel----
  getConceptCountTsibbleAtConceptIdYearLevel <-
    shiny::reactive(x = {
      data <- getConceptCountData()
      if (any(is.null(data),
              length(data) == 0)) {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Aggregating concept counts (Year)"),
                   value = 0)
      
      conceptCount <- data$conceptCount %>%
        dplyr::filter(.data$domainTableShort %in% getNonEraCdmTableShortNames()) %>%
        dplyr::mutate(periodBegin = lubridate::as_date(paste0(.data$eventYear, "-01-01"))) %>% #Lubridate exponetially faster that baseR as.Date and  ISODate
        dplyr::group_by(.data$conceptId,
                        .data$databaseId,
                        .data$periodBegin) %>%
        dplyr::summarise(conceptCount = sum(.data$conceptCount),
                         .groups = 'keep') %>%
        dplyr::ungroup() %>%
        dplyr::filter(.data$conceptCount > 0) %>%
        tsibble::as_tsibble(
          key = c(.data$conceptId, .data$databaseId),
          index = .data$periodBegin
        ) %>%
        tsibble::fill_gaps(conceptCount = 0) %>%
        dplyr::arrange(.data$conceptId,
                       .data$databaseId,
                       .data$periodBegin)
      return(data)
    })
  
  ##getConceptCooccurrenceData----
  #loads the entire data into R memory.
  # an alternative design maybe to load into R memory on start up (global.R)
  # but the size may become too large and we may want to filter to cohorts/database of choice
  # to do this - we have to replace by function that joins cohortId/databaseId choices to resolved concepts
  getConceptCooccurrenceData <-
    shiny::reactive(x = {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Caching concept cooccurrence for reuse"),
                   value = 0)
      cooccurrence <-
        getResultsConceptCooccurrence(
          dataSource = dataSource,
          databaseIds = database$databaseId,
          cohortIds = cohort$cohortId
        )
      if (any(is.null(cooccurrence),
              nrow(cooccurrence) == 0)) {
        return(NULL)
      }
      return(cooccurrence)
    })
  
  ##getCohortSortedByCohortId ----
  getCohortSortedByCohortId <- shiny::reactive({
    data <- cohort %>%
      dplyr::arrange(.data$cohortId)
    return(data)
  })
  
  #______________----
  #Selections----
  
  ##getCohortIdFromSelectedCompoundCohortName----
  getCohortIdFromSelectedCompoundCohortName <- shiny::reactive({
    data <- cohort %>%
      dplyr::filter(.data$compoundName %in% input$selectedCompoundCohortName) %>%
      dplyr::arrange(.data$cohortId) %>%
      dplyr::pull(.data$cohortId) %>%
      unique()
    return(data)
  })
  
  ##reactiveVal: getCohortIdsFromSelectedCompoundCohortNames----
  getCohortIdsFromSelectedCompoundCohortNames <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$selectedCompoundCohortNames_open,
         input$tabs)#observeEvent limits reactivity to when a tab changes, or 'cohorts' selection changes.
  }, handlerExpr = {
    if (any(isFALSE(input$selectedCompoundCohortNames_open),
            !is.null(input$tabs))) {
      selectedCohortIds <- cohort %>%
        dplyr::filter(.data$compoundName %in% input$selectedCompoundCohortNames) %>%
        dplyr::arrange(.data$cohortId) %>%
        dplyr::pull(.data$cohortId) %>%
        unique() %>%
        sort()
      getCohortIdsFromSelectedCompoundCohortNames(selectedCohortIds)
    }
  })
  
  
  ##pickerInput: conceptSetsSelectedFromOneCohort----
  #defined in UI
  shiny::observe({
    if (is.null(getConceptSetNamesFromOneCohort())) {
      return(NULL)
    }
    subset <- getConceptSetNamesFromOneCohort()$name
    if (input$tabs == "indexEventBreakdown") {
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "conceptSetsSelectedFromOneCohort",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = subset
      )
    } else {
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "conceptSetsSelectedFromOneCohort",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset
      )
    }
  })
  
  ##getComparatorCohortIdFromSelectedCompoundCohortName----
  getComparatorCohortIdFromSelectedCompoundCohortName <-
    shiny::reactive({
      data <- cohort %>%
        dplyr::filter(.data$compoundName %in% input$selectedComparatorCompoundCohortNames) %>%
        dplyr::arrange(.data$cohortId) %>%
        dplyr::pull(.data$cohortId) %>%
        unique()
      return(data)
    })
  
  ##reactiveVal: getTimeIdsFromSelectedTemporalCovariateChoices----
  getTimeIdsFromSelectedTemporalCovariateChoices <-
    reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$typesOfTemporalCovariates_open,
         input$tabs)
  }, handlerExpr = {
    if (exists('temporalCovariateChoices') &&
        (any(
          isFALSE(input$typesOfTemporalCovariates_open),!is.null(input$tabs)
        ))) {
      selectedTimeIds <- temporalCovariateChoices %>%
        dplyr::filter(.data$choices %in% input$typesOfTemporalCovariates) %>%
        dplyr::pull(.data$timeId)
      getTimeIdsFromSelectedTemporalCovariateChoices(selectedTimeIds)
    }
  })
  
  ##reactiveVal: getDatabaseIdsFromDropdown----
  #reactiveVal is being used to manage reactivity only after input$selectedDatabaseIds_open is closed
  getDatabaseIdsFromDropdown <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$selectedDatabaseIds_open,
         input$tabs)
  }, handlerExpr = {
    if (any(isFALSE(input$selectedDatabaseIds_open),
            !is.null(input$tabs))) {
      selectedDatabaseIds <- input$selectedDatabaseIds
      getDatabaseIdsFromDropdown(selectedDatabaseIds)
    }
  })
  
  
  ##pickerInput: selectedCompoundCohortName----
  shiny::observe({
    subset <- getCohortSortedByCohortId()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "selectedCompoundCohortName",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset
    )
  })
  
  ##pickerinput: selectedCompoundCohortNames----
  shiny::observe({
    subset <- getCohortSortedByCohortId()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "selectedCompoundCohortNames",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = c(subset[1], subset[2])
    )
  })
  
  ##pickerInput: selectedComparatorCompoundCohortNames----
  shiny::observe({
    subset <- getCohortSortedByCohortId()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "selectedComparatorCompoundCohortNames",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset[2]
    )
  })
  
  #______________----
  #Shared/reused----
  ##getResolvedConceptIdsForCohort----
  getResolvedConceptIdsForCohort <- shiny::reactive({
    if (any(
      is.null(getCohortIdFromSelectedCompoundCohortName()),
      length(getCohortIdFromSelectedCompoundCohortName()) == 0
    )) {
      return(NULL)
    }
    resolvedConcepts <-
      getResultsResolvedConcepts(dataSource = dataSource,
                                 cohortIds = getCohortIdFromSelectedCompoundCohortName())
    return(resolvedConcepts)
  })
  
  ##getResolvedConceptIdsForCohortFilteredBySelectedConceptSets----
  getResolvedConceptIdsForCohortFilteredBySelectedConceptSets <-
    shiny::reactive({
      data <- getResolvedConceptIdsForCohort()
      if (any(is.null(data),
              nrow(data) == 0)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::inner_join(
          conceptSets %>%
            dplyr::select(.data$cohortId,
                          .data$conceptSetId,
                          .data$conceptSetName) %>%
            dplyr::filter(
              .data$conceptSetName %in%
                input$conceptSetsSelectedFromOneCohort
            ),
          by = c("cohortId", "conceptSetId")
        )
      return(data)
    })
  
  ##getUserSelection----
  getUserSelection <- shiny::reactive(x = {
    list(
      tabs = input$tabs,
      cohortDefinitionTable_rows_selected = input$cohortDefinitionTable_rows_selected,
      conceptsetExpressionTableLeft_rows_selected = input$conceptSetsInCohortLeft_rows_selected,
      conceptsetExpressionTableRight_rows_selected = input$conceptSetsInCohortRight_rows_selected,
      cohortDefinitionResolvedConceptTableLeft_rows_selected = input$cohortDefinitionResolvedConceptTableLeft_rows_selected,
      cohortDefinitionResolvedConceptTableRight_rows_selected = input$cohortDefinitionResolvedConceptTableRight_rows_selected,
      cohortDefinitionExcludedConceptTableLeft_rows_selected = input$cohortDefinitionExcludedConceptTableLeft_rows_selected,
      cohortDefinitionExcludedConceptTableRight_rows_selected = input$cohortDefinitionExcludedConceptTableRight_rows_selected,
      cohortDefinitionOrphanConceptTableLeft_rows_selected = input$cohortDefinitionOrphanConceptTableLeft_rows_selected,
      cohortDefinitionOrphanConceptTableRight_rows_selected = input$cohortDefinitionOrphanConceptTableRight_rows_selected
      # cohortDefinitionSimplifiedInclusionRuleTableLeft_rows_selected = input$simplifiedInclusionRuleTableForSelectedCohortCountLeft_rows_selected,
      # cohortDefinitionSimplifiedInclusionTableRight_rows_selected = input$simplifiedInclusionRuleTableForSelectedCohortCountRight_rows_selected
    )
  })
  
  consolidatedCohortIdLeft <- reactiveVal(NULL)
  consolidatedCohortIdRight <- reactiveVal(NULL)
  consolidatedConceptSetIdLeft <- reactiveVal(NULL)
  consolidatedConceptSetIdRight <- reactiveVal(NULL)
  consolidatedDatabaseIdLeft <- reactiveVal(NULL)
  consolidatedDatabaseIdRight <- reactiveVal(NULL)
  consolidatedConceptIdLeft <- reactiveVal(NULL)
  consolidatedConceptIdRight <- reactiveVal(NULL)
  
  ##reactiveVal: consolidatedSelectedFieldValue----
  consolidatedSelectedFieldValue <- reactiveVal(list())
  #Reset Consolidated reactive val
  observeEvent(eventExpr = getUserSelection(),
               handlerExpr = {
                 data <- consolidationOfSelectedFieldValues(
                   input = input,
                   cohort = getCohortSortedByCohortId(),
                   conceptSets = conceptSets,
                   conceptSetExpressionLeft = getConceptSetsInCohortDataLeft(),
                   conceptSetExpressionRight = getConceptSetsInCohortDataRight(),
                   database = database,
                   resolvedConceptSetDataLeft = getResolvedConceptsLeft(),
                   resolvedConceptSetDataRight = getResolvedConceptsRight(),
                   orphanConceptSetDataLeft = getOrphanConceptsLeft(),
                   orphanConceptSetDataRight = getOrphanConceptsRight(),
                   excludedConceptSetDataLeft = getExcludedConceptsLeft(),
                   excludedConceptSetDataRight = getExcludedConceptsRight()
                 )
                 consolidatedCohortIdLeft(data$cohortIdLeft)
                 consolidatedCohortIdRight(data$cohortIdRight)
                 consolidatedConceptSetIdLeft(data$conceptSetIdLeft)
                 consolidatedConceptSetIdRight(data$conceptSetIdRight)
                 consolidatedDatabaseIdLeft(data$selectedDatabaseIdLeft)
                 consolidatedDatabaseIdRight(data$selectedDatabaseIdRight)
                 consolidatedConceptIdLeft(data$selectedConceptIdLeft)
                 consolidatedConceptIdRight(data$selectedConceptIdRight)
               })
  
  #______________----
  #cohortDefinition tab----
  ##Cohort definition----
  ###cohortDefinitionTableData----
  cohortDefinitionTableData <- shiny::reactive(x = {
    data <-  getCohortSortedByCohortId() %>%
      dplyr::select(cohort = .data$shortName,
                    .data$cohortId,
                    .data$cohortName)
    return(data)
  })
  ###output: cohortDefinitionTable----
  output$cohortDefinitionTable <- DT::renderDataTable(expr = {
    data <- cohortDefinitionTableData()
    
    if (nrow(data) < 20) {
      scrollYHeight <- '15vh'
    } else {
      scrollYHeight <- '25vh'
    }
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
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
  

  ###output: isCohortDefinitionRowSelected----
  output$isCohortDefinitionRowSelected <- reactive({
    return(any(!is.null(consolidatedCohortIdLeft()),
               !is.null(consolidatedCohortIdRight())))
  })
  # send output to UI
  shiny::outputOptions(x = output,
                       name = "isCohortDefinitionRowSelected",
                       suspendWhenHidden = FALSE)
  
  ###getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable----
  #Used to set the half view or full view
  getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable <-
    shiny::reactive(x = {
      length <- length(input$cohortDefinitionTable_rows_selected)
      if (length == 2) {
        return(6)
      } else {
        return(12)
      }
    })
  
  ###output: downloadAllCohortDetails----
  output$downloadAllCohortDetails <- downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "CohortDefinition")
    },
    content = function(file) {
      data <- getCohortSortedByCohortId()
      downloadCsv(x = data, fileName = file)
    }
  )
  
  
  ###getCohortIdFromSelectedRowInCohortCountTable----
  getCohortIdFromSelectedRowInCohortCountTable <- reactive({
    idx <- input$cohortCountsTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      subset <- getCohortCountDataForSelectedDatabaseIdsCohortIds() %>%
        dplyr::distinct(.data$cohortId)
      
      if (!is.null(subset)) {
        return(subset[idx, ])
      } else {
        return(NULL)
      }
    }
  })
  
  ##Human readable text----
  ###getCirceRPackageVersionInformation----
  getCirceRPackageVersionInformation <- shiny::reactive(x = {
    packageVersion <- as.character(packageVersion('CirceR'))
    return(packageVersion)
  })
  
  ###getCohortMetadataLeft----
  getCohortMetadataLeft <- shiny::reactive(x = {
    data <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdLeft())
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    details <-  tags$table(style = "margin-top: 5px;",
                           tags$tr(
                             tags$td(tags$strong("Metadata: ")),
                             tags$td(HTML("&nbsp;&nbsp;")),
                             tags$td(data$metadata)
                           ))
    
    return(details)
  })
  
  ###getCohortMetadataRight----
  getCohortMetadataRight <- shiny::reactive(x = {
    data <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdRight())
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    details <-  tags$table(style = "margin-top: 5px;",
                           tags$tr(
                             tags$td(tags$strong("Metadata: ")),
                             tags$td(HTML("&nbsp;&nbsp;")),
                             tags$td(data$metadata)
                           ))
    
    return(details)
  })
  
  ###getCirceRenderedExpressionDetailsLeft----
  getCirceRenderedExpressionDetailsLeft <- shiny::reactive(x = {
    if (!doesObjectHaveData(consolidatedCohortIdLeft())) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0(
        "Rendering human readable cohort definition using CirceR ",
        getCirceRPackageVersionInformation(),
        " for cohort id: ",
        consolidatedCohortIdLeft()
      ),
      value = 0
    )
    selectionsInCohortTable <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdLeft())
    if (!doesObjectHaveData(selectionsInCohortTable)) {
      return(NULL)
    }
    cohortDefinition <-
      RJSONIO::fromJSON(selectionsInCohortTable$json,
                        digits = 23)
    details <-
      getCirceRenderedExpression(cohortDefinition = cohortDefinition)
    return(details)
  })
  
  ###getCirceRenderedExpressionDetailsRight----
  getCirceRenderedExpressionDetailsRight <- shiny::reactive(x = {
    if (!doesObjectHaveData(consolidatedCohortIdRight())) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0(
        "Rendering human readable cohort definition using CirceR ",
        getCirceRPackageVersionInformation(),
        " for cohort id: ",
        consolidatedCohortIdRight()
      ),
      value = 0
    )
    selectionsInCohortTable <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdRight())
    if (!doesObjectHaveData(selectionsInCohortTable)) {
      return(NULL)
    }
    cohortDefinition <-
      RJSONIO::fromJSON(selectionsInCohortTable$json,
                        digits = 23)
    details <-
      getCirceRenderedExpression(cohortDefinition = cohortDefinition)
    return(details)
  })
  
  ###output: cohortDefinitionTextRight----
  output$cohortDefinitionTextRight <- shiny::renderUI(expr = {
    getCirceRenderedExpressionDetailsRight()$cohortHtmlExpression %>%
      shiny::HTML()
  })
  
  ###output: cohortDefinitionTextLeft----
  output$cohortDefinitionTextLeft <- shiny::renderUI(expr = {
    getCirceRenderedExpressionDetailsLeft()$cohortHtmlExpression %>%
      shiny::HTML()
  })
  
  ###output: circeRVersionInCohortDefinitionLeft----
  output$circeRVersionInCohortDefinitionLeft <-
    shiny::renderUI(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdLeft())) {
        return(NULL)
      }
      if (!doesObjectHaveData(getCirceRPackageVersionInformation())) {
        return(NULL)
      }
      version <- tags$table(tags$tr(tags$td(
        paste(
          "rendered for cohort id:",
          consolidatedCohortIdLeft(),
          "using CirceR version: ",
          getCirceRPackageVersionInformation()
        )
      )))
      return(version)
    })
  
  ###output: circeRVersionInCohortDefinitionRight----
  output$circeRVersionInCohortDefinitionRight <-
    shiny::renderUI(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdRight())) {
        return(NULL)
      }
      if (!doesObjectHaveData(getCirceRPackageVersionInformation())) {
        return(NULL)
      }
      version <- tags$table(tags$tr(tags$td(
        paste(
          "rendered for cohort id:",
          consolidatedCohortIdRight(),
          "using CirceR version: ",
          getCirceRPackageVersionInformation()
        )
      )))
      return(version)
    })
  
  ###output: circeRVersionIncohortDefinitionSqlLeft----
  output$circeRVersionIncohortDefinitionSqlLeft <-
    shiny::renderUI(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdLeft())) {
        return(NULL)
      }
      if (!doesObjectHaveData(getCirceRPackageVersionInformation())) {
        return(NULL)
      }
      version <- tags$table(tags$tr(tags$td(
        paste(
          "rendered for cohort id:",
          consolidatedCohortIdLeft(),
          "using CirceR version: ",
          getCirceRPackageVersionInformation()
        )
      )))
      return(version)
    })
  
  ###output: circeRVersionInCohortDefinitionSqlRight----
  output$circeRVersionInCohortDefinitionSqlRight <-
    shiny::renderUI(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdRight())) {
        return(NULL)
      }
      if (!doesObjectHaveData(getCirceRPackageVersionInformation())) {
        return(NULL)
      }
      version <- tags$table(tags$tr(tags$td(
        paste(
          "rendered for cohort id:",
          consolidatedCohortIdRight(),
          "using CirceR version: ",
          getCirceRPackageVersionInformation()
        )
      )))
      return(version)
    })
  
  ###output: nameOfSelectedCohortInCohortDefinitionTableLeft----
  #Show cohort names in UI
  output$nameOfSelectedCohortInCohortDefinitionTableLeft <-
    shiny::renderUI(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdLeft())) {
        return(NULL)
      }
      cohortName <- cohort %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdLeft()) %>%
        dplyr::pull(.data$compoundName)
      
      if (!doesObjectHaveData(cohortName)) {
        return(NULL)
      }
      tags$table(height = '60',
                 style = "overflow : auto",
                 tags$tr(tags$td(tags$b(
                   "Selected cohort: "
                 )),
                 tags$td(cohortName)))
      
    })
  
  
  ###output: nameOfSelectedCohortInCohortDefinitionTableRight----
  output$nameOfSelectedCohortInCohortDefinitionTableRight <-
    shiny::renderUI(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdRight())) {
        return(NULL)
      }
      cohortName <- cohort %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdRight()) %>%
        dplyr::pull(.data$compoundName)
      
      if (!doesObjectHaveData(cohortName)) {
        return(NULL)
      }
      tags$table(height = '60',
                 style = "overflow : auto",
                 tags$tr(tags$td(tags$b(
                   "Selected cohort:"
                 )),
                 tags$td(cohortName)))
      
    })
  
  ##Cohort SQL----
  ###output: cohortDefinitionSqlLeft----
  output$cohortDefinitionSqlLeft <- shiny::renderText({
    if (!doesObjectHaveData(consolidatedCohortIdLeft())) {
      return(NULL)
    }
    json <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdLeft()) %>%
      dplyr::pull(.data$json)
    if (!doesObjectHaveData(json)) {
      return(NULL)
    }
    options <- CirceR::createGenerateOptions(generateStats = TRUE)
    expression <-
      CirceR::cohortExpressionFromJson(expressionJson = json)
    if (is.null(expression)) {
      return(NULL)
    }
    return(CirceR::buildCohortQuery(expression = expression, options = options))
  })
  
  ###output: cohortDefinitionSqlRight----
  output$cohortDefinitionSqlRight <- shiny::renderText({
    if (!doesObjectHaveData(consolidatedCohortIdRight())) {
      return(NULL)
    }
    json <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdRight()) %>%
      dplyr::pull(.data$json)
    if (!doesObjectHaveData(json)) {
      return(NULL)
    }
    options <-
      CirceR::createGenerateOptions(generateStats = TRUE)
    expression <-
      CirceR::cohortExpressionFromJson(expressionJson = json)
    if (is.null(expression)) {
      return(NULL)
    }
    return(CirceR::buildCohortQuery(expression = expression, options = options))
  })
  
  ##Cohort count in cohort definition tab----
  ###getCountsForSelectedCohortsLeft----
  getCountsForSelectedCohortsLeft <- shiny::reactive(x = {
    data <- cohortCount %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdLeft()) %>%
      dplyr::select(.data$databaseId,
                    .data$cohortSubjects,
                    .data$cohortEntries) %>%
      dplyr::arrange(.data$databaseId)
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ###getCountsForSelectedCohortsRight----
  getCountsForSelectedCohortsRight <- shiny::reactive(x = {
    data <- cohortCount %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdRight()) %>%
      dplyr::select(.data$databaseId,
                    .data$cohortSubjects,
                    .data$cohortEntries) %>%
      dplyr::arrange(.data$databaseId)
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  
  ###getCountsForSelectedCohortsLeftFilteredToDatabaseId----
  getCountsForSelectedCohortsLeftFilteredToDatabaseId <-
    shiny::reactive(x = {
      databaseId <- consolidatedDatabaseIdLeft()
      data <- getCountsForSelectedCohortsLeft()
      if (all(!is.null(data),
              nrow(data) > 0)) {
        data <- data %>%
          dplyr::filter(.data$databaseId == !!databaseId) %>%
          dplyr::select(.data$cohortSubjects, .data$cohortEntries)
      }
      if (any(is.null(data),
              nrow(data) == 0)) {
        return(NULL)
      }
      return(data)
    })
  
  ###getCountsForSelectedCohortsRightFilteredToDatabaseId----
  getCountsForSelectedCohortsRightFilteredToDatabaseId <-
    shiny::reactive(x = {
      databaseId <- consolidatedDatabaseIdRight()
      data <- getCountsForSelectedCohortsRight()
      if (all(!is.null(data),
              nrow(data) > 0)) {
        data <- data %>%
          dplyr::filter(.data$databaseId == !!databaseId) %>%
          dplyr::select(.data$cohortSubjects, .data$cohortEntries)
      }
      if (any(is.null(data),
              nrow(data) == 0)) {
        return(NULL)
      }
      return(data)
    })
  
  ##Concept set ----
  ###getConceptSetExpressionLeft----
  getConceptSetExpressionLeft <- shiny::reactive(x = {
    if (!doesObjectHaveData(consolidatedCohortIdLeft())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdLeft())) {
      return(NULL)
    }
    conceptSetExpression <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdLeft()) %>%
      dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdLeft())
    if (!doesObjectHaveData(conceptSetExpression)) {
      return(NULL)
    }
    conceptSetExpressionList <- conceptSetExpression %>% 
      dplyr::pull(.data$conceptSetExpression) %>%
      RJSONIO::fromJSON(digits = 23)
    
    data <-
      getConceptSetDataFrameFromConceptSetExpression(conceptSetExpressionList)
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    data <- data %>% 
      dplyr::select(.data$conceptId,
                    .data$conceptCode,
                    .data$conceptName,
                    .data$domainId,
                    .data$standardConcept,
                    .data$invalidReason,
                    .data$isExcluded,
                    .data$includeDescendants,
                    .data$includeMapped
                    )
    return(data)
  })
  
  ###getConceptSetExpressionRight----
  getConceptSetExpressionRight <- shiny::reactive(x = {
    if (all(
      !doesObjectHaveData(consolidatedCohortIdRight()),!doesObjectHaveData(consolidatedConceptSetIdRight())
    )) {
      return(NULL)
    }
    conceptSetExpression <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdRight()) %>%
      dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdRight())
    if (!doesObjectHaveData(conceptSetExpression)) {
      return(NULL)
    }
    conceptSetExpressionList <- conceptSetExpression %>%
      dplyr::pull(.data$conceptSetExpression) %>%
      RJSONIO::fromJSON(digits = 23)
    data <-
      getConceptSetDataFrameFromConceptSetExpression(conceptSetExpressionList)
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::select(
        .data$conceptId,
        .data$conceptCode,
        .data$conceptName,
        .data$domainId,
        .data$standardConcept,
        .data$invalidReason,
        .data$isExcluded,
        .data$includeDescendants,
        .data$includeMapped
      )
    return(data)
  })
  
  
  ###getResolvedConceptsLeft----
  getResolvedConceptsLeft <- shiny::reactive({
    data <- getResultsResolvedConcepts(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdLeft(),
      databaseIds = consolidatedDatabaseIdLeft()
    )
    if (is.null(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdLeft()) %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId) %>% 
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    return(data)
  })
  
  ###getResolvedConceptsRight----
  getResolvedConceptsRight <- shiny::reactive({
    data <- getResultsResolvedConcepts(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdRight(),
      databaseIds = consolidatedDatabaseIdRight()
    )
    if (is.null(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdRight()) %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId) %>% 
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    return(data)
  })
  
  ###getExcludedConceptsLeft----
  getExcludedConceptsLeft <- shiny::reactive({
    data <- getResultsExcludedConcepts(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdLeft(),
      databaseIds = consolidatedDatabaseIdLeft()
    )
    if (is.null(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdLeft()) %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId) %>% 
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    return(data)
  })
  
  ###getExcludedConceptsRight----
  getExcludedConceptsRight <- shiny::reactive({
    data <- getResultsExcludedConcepts(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdRight(),
      databaseIds = consolidatedDatabaseIdRight()
    )
    if (is.null(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdRight()) %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId) %>% 
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    return(data)
  })
  
  
  ###getOrphanConceptsLeft----
  getOrphanConceptsLeft <- shiny::reactive({
    data <- getResultsOrphanConcept(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdLeft(),
      databaseIds = consolidatedDatabaseIdLeft()
    )
    if (is.null(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdLeft())
    excluded <- getExcludedConceptsLeft()
    if (doesObjectHaveData(excluded)) {
      excludedConceptIds <- excluded %>%
        dplyr::select(.data$conceptId) %>%
        dplyr::distinct()
      data <- data %>%
        dplyr::anti_join(y = excludedConceptIds, by = "conceptId")
    }
    data <- data  %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdLeft()) %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId) %>% 
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    return(data)
  })
  
  ###getOrphanConceptsRight----
  getOrphanConceptsRight <- shiny::reactive({
    data <- getResultsOrphanConcept(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdRight(),
      databaseIds = consolidatedDatabaseIdRight()
    )
    if (is.null(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdRight())
    excluded <- getExcludedConceptsRight()
    if (doesObjectHaveData(excluded)) {
      excludedConceptIds <- excluded %>%
        dplyr::select(.data$conceptId) %>%
        dplyr::distinct()
      data <- data %>%
        dplyr::anti_join(y = excludedConceptIds, by = "conceptId")
    }
    data <- data %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId) %>% 
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    return(data)
  })
  
  ###getConceptRelationshipsLeft----
  getConceptRelationshipsLeft <- shiny::reactive({
    conceptIds <- dplyr::bind_rows(
      getResolvedConceptsLeft(),
      getExcludedConceptsLeft(),
      getOrphanConceptsLeft()
    ) %>%
      dplyr::pull(.data$conceptId) %>%
      unique() %>%
      sort()
    if (any(is.null(conceptIds),
            length(conceptIds) == 0)) {
      return(NULL)
    }
    data <- getConceptRelationship(
      dataSource = dataSource,
      vocabularyDatabaseSchema = NULL,
      #for alternate vocabulary
      conceptIds = conceptIds
    )
    return(data)
  })
  
  ###getConceptRelationshipsRight----
  getConceptRelationshipsRight <- shiny::reactive({
    conceptIds <- dplyr::bind_rows(
      getResolvedConceptsRight(),
      getExcludedConceptsRight(),
      getOrphanConceptsRight()
    ) %>%
      dplyr::pull(.data$conceptId) %>%
      unique() %>%
      sort()
    if (any(is.null(conceptIds),
            length(conceptIds) == 0)) {
      return(NULL)
    }
    data <- getConceptRelationship(
      dataSource = dataSource,
      vocabularyDatabaseSchema = NULL,
      #for alternate vocabulary
      conceptIds = conceptIds
    )
    return(data)
  })
  
  ###getConceptAncestorLeft----
  getConceptAncestorLeft <- shiny::reactive({
    conceptIds <- dplyr::bind_rows(
      getResolvedConceptsLeft(),
      getExcludedConceptsLeft(),
      getOrphanConceptsLeft()
    ) %>%
      dplyr::pull(.data$conceptId) %>%
      unique() %>%
      sort()
    if (any(is.null(conceptIds),
            length(conceptIds) == 0)) {
      return(NULL)
    }
    data <- getConceptAncestor(
      dataSource = dataSource,
      vocabularyDatabaseSchema = NULL,
      #for alternate vocabulary
      conceptIds = conceptIds
    )
    return(data)
  })
  
  ###getConceptAncestorRight----
  getConceptAncestorRight <- shiny::reactive({
    conceptIds <- dplyr::bind_rows(
      getResolvedConceptsRight(),
      getExcludedConceptsRight(),
      getOrphanConceptsRight()
    ) %>%
      dplyr::pull(.data$conceptId) %>%
      unique() %>%
      sort()
    if (any(is.null(conceptIds),
            length(conceptIds) == 0)) {
      return(NULL)
    }
    data <- getConceptAncestor(
      dataSource = dataSource,
      vocabularyDatabaseSchema = NULL,
      #for alternate vocabulary
      conceptIds = conceptIds
    )
    return(data)
  })
  
  ###getConceptIdOfInterestLeft----
  getConceptIdOfInterestLeft <- shiny::reactive({
    conceptIds <- dplyr::bind_rows(
      getResolvedConceptsLeft(),
      getExcludedConceptsLeft(),
      getOrphanConceptsLeft()
    ) %>%
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
    conceptAncestor <- getConceptAncestorLeft()
    if (all(!is.null(conceptAncestor),
            nrow(conceptAncestor) > 0)) {
      conceptIds <- c(
        conceptIds,
        conceptAncestor$ancestorConceptId,
        conceptAncestor$descendantConceptId
      ) %>%
        unique()
    }
    if (any(is.null(conceptIds),
            length(conceptIds) == 0)) {
      return(NULL)
    }
    return(conceptIds %>% unique() %>% sort())
  })
  
  ###getConceptIdOfInterestRight----
  getConceptIdOfInterestRight <- shiny::reactive({
    conceptIds <- dplyr::bind_rows(
      getResolvedConceptsRight(),
      getExcludedConceptsRight(),
      getOrphanConceptsRight()
    ) %>%
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
    conceptAncestor <- getConceptAncestorRight()
    if (all(!is.null(conceptAncestor),
            nrow(conceptAncestor) > 0)) {
      conceptIds <- c(
        conceptIds,
        conceptAncestor$ancestorConceptId,
        conceptAncestor$descendantConceptId
      ) %>%
        unique()
    }
    if (any(is.null(conceptIds),
            length(conceptIds) == 0)) {
      return(NULL)
    }
    if (any(is.null(conceptIds),
            length(conceptIds) == 0)) {
      return(NULL)
    }
    return(conceptIds %>% unique() %>% sort())
  })
  
  ###getConceptSetComparisonDetailsRight----
  getConceptSetComparisonDetailsRight <- shiny::reactive(x = {
    data <- getConceptSetDetailsRight()
    if ("orphanConcepts" %in% names(data)) {
      data <- pivotOrphanConceptResult(data = data$orphanConcepts,
                                       dataSource = dataSource)
    } else {
      return(NULL)
    }
  })
  
  ###getDataForConceptSetComparison----
  getDataForConceptSetComparison <- shiny::reactive(x = {
    leftData <- getConcept()$resolvedConcepts
    rightData <- getConceptSetDetailsRight()$resolvedConcepts
    data <- list(leftData = leftData, rightData = rightData)
    return(data)
  })
  
  ###getWidthOfRelationshipTableForSelectedConcepts----
  getWidthOfRelationshipTableForSelectedConcepts <-
    shiny::reactive(x = {
      length <- length(input$cohortDefinitionTable_rows_selected)
      if (length == 2) {
        return(4)
      } else {
        return(8)
      }
    })
  ###!!!!!!!!!!! make vocabulary choices multiselect - if more than one is sleected
  # append all conceptId from all the selected vocabulary choices into one table
  # make each selected vocabulary choice a column name
  # add check mark if the concept id is present in that vocabulary choice
  # by default - make this multiselected - top 3
  
  ##Inclusion rule ----
  ###getDatabaseIdForSelectedCohortCountLeft----
  getDatabaseIdForSelectedCohortCountLeft <- shiny::reactive(x = {
    idx <- input$cohortCountsTableForSelectedCohortLeft_rows_selected
    if (!doesObjectHaveData(idx)) {
      return(NULL)
    }
    
    databaseIds <- getCountsForSelectedCohortsLeft()[idx, ] %>% 
      dplyr::pull(.data$databaseId)
    
    return(databaseIds)
  })
  
  ###getSimplifiedInclusionRuleResultsLeft----
  getSimplifiedInclusionRuleResultsLeft <- shiny::reactive(x = {
    if (any(
      !doesObjectHaveData(consolidatedCohortIdLeft()),
      !doesObjectHaveData(getDatabaseIdForSelectedCohortCountLeft())
    )) {
      return(NULL)
    }
    browser()
    data <-
      getResultsInclusionRuleStatistics(
        dataSource = dataSource,
        cohortIds = consolidatedCohortIdLeft(),
        databaseIds = getDatabaseIdForSelectedCohortCountLeft()
      )
    
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    
    return(data)
  })
  
  ###getDatabaseIdForSelectedCohortCountRight----
  getDatabaseIdForSelectedCohortCountRight <- shiny::reactive(x = {
    idx <- input$cohortCountsTableForSelectedCohortRight_rows_selected
    if (!doesObjectHaveData(idx)) {
      return(NULL)
    }
    
    databaseIds <- getCountsForSelectedCohortsRight()[idx, ] %>% 
      dplyr::pull(.data$databaseId)
    
    return(databaseIds)
  })
  
  ###getSimplifiedInclusionRuleResultsRight----
  getSimplifiedInclusionRuleResultsRight <- shiny::reactive(x = {
    if (any(
      !doesObjectHaveData(consolidatedCohortIdRight()),
      !doesObjectHaveData(getDatabaseIdForSelectedCohortCountRight())
    )) {
      return(NULL)
    }
    browser()
    data <-
      getResultsInclusionRuleStatistics(
        dataSource = dataSource,
        cohortIds = consolidatedCohortIdRight(),
        databaseIds = getDatabaseIdForSelectedCohortCountRight()
      )
    
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    return(data)
  })
  
  ###getFullCohortInclusionResults----
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
  
  
 ################----------------------------
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  #output: cohortDetailsTextLeft----
  output$cohortDetailsTextLeft <- shiny::renderUI({
    row <- getCohortMetadataLeft()
    if (is.null(row) || length(row) == 0) {
      return(NULL)
    }
    return(row)
  })
  #output: cohortDetailsTextRight----
  output$cohortDetailsTextRight <- shiny::renderUI({
    row <- getCohortMetadataRight()
    if (is.null(row) || length(row) == 0) {
      return(NULL)
    }
    if (length(row) == 2) {
      row <- row[[2]]
    }
    return(row)
  })
  
  
  #output: cohortCountsTableForSelectedCohortLeft----
  output$cohortCountsTableForSelectedCohortLeft <-
    DT::renderDataTable(expr = {
      data <- getCountsForSelectedCohortsLeft()
      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
        "There is no inclusion rule data for this cohort."
      ))
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
    }, server = TRUE)
  
  #output: isDatabaseIdFoundForSelectedCohortCountLeft----
  output$isDatabaseIdFoundForSelectedCohortCountLeft <-
    shiny::reactive(x = {
      return(!is.null(getSimplifiedInclusionRuleResultsLeft()))
    })
  shiny::outputOptions(x = output,
                       name = "isDatabaseIdFoundForSelectedCohortCountLeft",
                       suspendWhenHidden = FALSE)
  
  #!!!!!! inclusion rule needs simple and detailed tabs. detailed will replicate Atlas UI
  #output: simplifiedInclusionRuleTableForSelectedCohortCountLeft----
  output$simplifiedInclusionRuleTableForSelectedCohortCountLeft <-
    DT::renderDataTable(expr = {
      if (any(
        is.null(consolidatedCohortIdLeft())
      )) {
        return(NULL)
      }
      table <- getSimplifiedInclusionRuleResultsLeft()
      validate(need((nrow(table) > 0),
                    "There is no inclusion rule data for this cohort."))
      
      databaseIds <- unique(table$databaseId)
      cohortCounts <- table %>%
        dplyr::inner_join(cohortCount,
                          by = c("cohortId", "databaseId")) %>%
        dplyr::filter(.data$cohortId == consolidatedCohortIdLeft()) %>%
        dplyr::filter(.data$databaseId %in% getDatabaseIdForSelectedCohortCountLeft()) %>%
        dplyr::select(.data$cohortSubjects) %>%
        dplyr::pull(.data$cohortSubjects) %>% unique()
      
      databaseIdsWithCount <-
        paste(databaseIds,
              "(n = ",
              format(cohortCounts, big.mark = ","),
              ")")
      
      table <- table %>%
        dplyr::inner_join(
          cohortCount %>%
            dplyr::select(.data$databaseId, .data$cohortId, .data$cohortSubjects),
          by = c('databaseId', 'cohortId')
        ) %>%
        tidyr::pivot_longer(
          cols = c(
            .data$meetSubjects,
            .data$gainSubjects,
            .data$totalSubjects,
            .data$remainSubjects
          )
        ) %>%
        dplyr::mutate(name = paste0(
          .data$databaseId,
          "<br>(n = ",
          scales::comma(x = .data$cohortSubjects, accuracy = 1),
          ")_",
          .data$name
        )) %>%
        tidyr::pivot_wider(
          id_cols = c(.data$cohortId, .data$ruleSequenceId, .data$ruleName),
          names_from = .data$name,
          values_from = .data$value
        ) %>%
        dplyr::select(-.data$cohortId)
      
      if (input$cohortDefinitionInclusionRuleTableFilters == "Meet") {
        table <- table %>%
          dplyr::select(
            -dplyr::contains("Total"),
            -dplyr::contains("Gain"),
            -dplyr::contains("Remain")
          )
        colnames(table) <-
          stringr::str_replace(
            string = colnames(table),
            pattern = '_meetSubjects',
            replacement = ''
          )
        
        columnDefs <- minCellCountDef(1 + (1:(length(databaseIds))))
        
      } else if (input$cohortDefinitionInclusionRuleTableFilters == "Totals") {
        table <- table %>%
          dplyr::select(
            -dplyr::contains("Meet"),
            -dplyr::contains("Gain"),
            -dplyr::contains("Remain")
          )
        colnames(table) <-
          stringr::str_replace(
            string = colnames(table),
            pattern = '_totalSubjects',
            replacement = ''
          )
        
        columnDefs <- minCellCountDef(1 + (1:(length(databaseIds))))
        
      } else if (input$cohortDefinitionInclusionRuleTableFilters == "Gain") {
        table <- table %>%
          dplyr::select(
            -dplyr::contains("Total"),
            -dplyr::contains("Meet"),
            -dplyr::contains("Remain")
          )
        colnames(table) <-
          stringr::str_replace(
            string = colnames(table),
            pattern = '_gainSubjects',
            replacement = ''
          )
        
        columnDefs <- minCellCountDef(1 + (1:(length(databaseIds))))
        
      } else if (input$cohortDefinitionInclusionRuleTableFilters == "Remain") {
        table <- table %>%
          dplyr::select(
            -dplyr::contains("Total"),
            -dplyr::contains("Meet"),
            -dplyr::contains("Gain")
          )
        colnames(table) <-
          stringr::str_replace(
            string = colnames(table),
            pattern = '_remainSubjects',
            replacement = ''
          )
        
        columnDefs <- minCellCountDef(1 + (1:(length(databaseIds))))
        
      }  else {
        sketch <- htmltools::withTags(table(class = "display",
                                            thead(tr(
                                              th(rowspan = 2, "Rule Sequence ID"),
                                              th(rowspan = 2, "Rule Name"),
                                              lapply(
                                                databaseIdsWithCount,
                                                th,
                                                colspan = 4,
                                                class = "dt-center",
                                                style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                              )
                                            ),
                                            tr(
                                              lapply(rep(
                                                c("Meet", "Gain", "Remain", "Total"),
                                                length(databaseIds)
                                              ), th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                            ))))
        
        columnDefs <-
          minCellCountDef(1 + (1:(length(databaseIds) * 4)))
      }
      
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
  
  #output: saveSimplifiedInclusionRuleTableForSelectedCohortCountLeft----
  output$saveSimplifiedInclusionRuleTableForSelectedCohortCountLeft <-
    downloadHandler(
      filename = function() {
        getCsvFileNameWithDateTime(string = "InclusionRule")
      },
      content = function(file) {
        downloadCsv(x = getSimplifiedInclusionRuleResultsLeft(), fileName = file)
      }
    )
  
  ##output: getSimplifiedInclusionRuleResultsLeftHasData----
  output$getSimplifiedInclusionRuleResultsLeftHasData <-
    shiny::reactive(x = {
      return(nrow(getSimplifiedInclusionRuleResultsLeft()) > 0)
    })
  
  shiny::outputOptions(x = output,
                       name = "getSimplifiedInclusionRuleResultsLeftHasData",
                       suspendWhenHidden = FALSE)
  
  #output: cohortDefinitionJsonLeft----
  output$cohortDefinitionJsonLeft <- shiny::renderText({
    json <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdLeft()) %>%
      dplyr::pull(.data$json)
    if (!doesObjectHaveData(json)) {
      return(NULL)
    }
    json <- json %>% 
      RJSONIO::fromJSON(digits = 23) %>% 
      RJSONIO::toJSON(digits = 23, pretty = TRUE)
    return(json)
  })
  
  #output: cohortDefinitionSelectedRowCount----
  output$cohortDefinitionSelectedRowCount <- shiny::reactive({
    return(length(input$cohortDefinitionTable_rows_selected))
  })
  shiny::outputOptions(x = output,
                       name = "cohortDefinitionSelectedRowCount",
                       suspendWhenHidden = FALSE)
  
  #Dynamic UI rendering for left side -----
  output$dynamicUIGenerationForCohortSelectedLeft <-
    shiny::renderUI(expr = {
      shiny::column(
        getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable(),
        shiny::conditionalPanel(
          condition = "output.cohortDefinitionSelectedRowCount > 0 &
                     output.isCohortDefinitionRowSelected == true",
          shiny::htmlOutput(outputId = "nameOfSelectedCohortInCohortDefinitionTableLeft"),
          shiny::tabsetPanel(
            type = "tab",
            id = "cohortDefinitionOneTabSetPanel",
            shiny::tabPanel(
              title = "Cohort Count",
              value = "cohortDefinitionOneCohortCountTabPanel",
              tags$br(),
              DT::dataTableOutput(outputId = "cohortCountsTableForSelectedCohortLeft"),
              tags$br(),
              shiny::conditionalPanel(
                condition = "output.isDatabaseIdFoundForSelectedCohortCountLeft == true",
                tags$h3("Inclusion Rules"),
                tags$table(width = "100%",
                           tags$tr(
                             tags$td(
                               shiny::radioButtons(
                                 inputId = "cohortDefinitionInclusionRuleType",
                                 label = "Select: ",
                                 choices = c("Simplified", "Detailed"),
                                 selected = "Simplified",
                                 inline = TRUE
                               )
                             ),
                             tags$td(
                               shiny::conditionalPanel(
                                 condition = "input.cohortDefinitionInclusionRuleType == 'Simplified' &
                                         output.getSimplifiedInclusionRuleResultsLeftHasData == true",
                                 shiny::radioButtons(
                                   inputId = "cohortDefinitionInclusionRuleTableFilters",
                                   label = "Filter by",
                                   choices = c("All", "Meet", "Gain", "Remain", "Totals"),
                                   selected = "All",
                                   inline = TRUE
                                 )
                               )
                             ),
                             tags$td(
                               align = "right",
                               shiny::downloadButton(
                                 "saveSimplifiedInclusionRuleTableForSelectedCohortCountLeft",
                                 label = "",
                                 icon = shiny::icon("download"),
                                 style = "margin-top: 5px; margin-bottom: 5px;"
                               )
                             )
                           )),
                shiny::conditionalPanel(
                  condition = "input.cohortDefinitionInclusionRuleType == 'Simplified'",
                  DT::dataTableOutput(outputId = "simplifiedInclusionRuleTableForSelectedCohortCountLeft")
                )
              )
            ),
            shiny::tabPanel(
              title = "Details",
              value = "cohortDefinitionOneDetailsTextTabPanel",
              tags$br(),
              shinydashboard::box(
                title = "Readable definitions",
                width = NULL,
                status = NULL,
                collapsible = TRUE,
                collapsed = FALSE,
                solidHeader = FALSE,
                copyToClipboardButton(toCopyId = "cohortDefinitionTextLeft",
                                      style = "margin-top: 5px; margin-bottom: 5px;"),
                shiny::htmlOutput("circeRVersionInCohortDefinitionLeft"),
                shiny::htmlOutput("cohortDefinitionTextLeft")
              ),
              shinydashboard::box(
                title = "Meta data",
                width = NULL,
                status = NULL,
                collapsible = TRUE,
                collapsed = FALSE,
                solidHeader = FALSE,
                shiny::htmlOutput("cohortDetailsTextLeft")
              )
            ),
            shiny::tabPanel(
              #!!!!!!!!!if cohort has no concept sets - make gray color or say 'No Concept sets'
              title = "Concept Sets",
              value = "conceptSetOneTabPanel",
              DT::dataTableOutput(outputId = "conceptSetsInCohortLeft"),
              tags$br(),
              shiny::conditionalPanel(
                condition = "output.isAConceptSetSelectedForCohortLeft == true",
                shinydashboard::box(
                  title = shiny::textOutput(outputId = "conceptSetExpressionNameLeft"),
                  ###!!!! selected concept set name + cohort information
                  width = NULL,
                  solidHeader = FALSE,
                  collapsible = TRUE,
                  collapsed = FALSE,
                  shiny::conditionalPanel(condition = "output.isAConceptSetSelectedForCohortLeft == true",
                                          tags$table(
                                            tags$tr(
                                              tags$td(
                                                shinyWidgets::pickerInput(
                                                  #!!! lets make this multi-selected for databaseId/vocabulary choices
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
                                            tags$tr(tags$td(
                                              colspan = 2,
                                              shiny::radioButtons(
                                                inputId = "conceptSetsTypeLeft",
                                                label = "",
                                                choices = c(
                                                  "Concept Set Expression",
                                                  "Resolved",
                                                  "Excluded",
                                                  "Orphan concepts",
                                                  "Json" #!!! change to 'Concept Set Json'
                                                  #!!! add  "Concept Set Sql"
                                                ),
                                                #!!! add concept set sql
                                                selected = "Concept Set Expression",
                                                inline = TRUE
                                              )
                                            ))
                                          )),
                  shiny::conditionalPanel(
                    condition = "output.isAConceptSetSelectedForCohortLeft == true &
                                                      input.conceptSetsTypeLeft != 'Resolved' &
                                                      input.conceptSetsTypeLeft != 'Excluded' &
                                                      input.conceptSetsTypeLeft != 'Json' &
                                                      input.conceptSetsTypeLeft != 'Orphan concepts'",
                    #!!! add concept set sql
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveConceptSetsExpressionTableLeft",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "conceptSetsExpressionTableLeft")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.conceptSetsTypeLeft == 'Resolved'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveCohortDefinitionResolvedConceptTableLeft",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "cohortDefinitionResolvedConceptTableLeft")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.conceptSetsTypeLeft == 'Excluded'",
                    #!!!!! currently not working - also should be dynamic
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveCohortDefinitionExcludedConceptTableLeft",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "cohortDefinitionExcludedConceptTableLeft")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.conceptSetsTypeLeft == 'Orphan concepts'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveOrphanConceptsTableLeft",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
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
                )
              )
            ),
            
            shiny::tabPanel(
              title = "Cohort JSON",
              value = "cohortDefinitionOneJsonTabPanel",
              copyToClipboardButton("cohortDefinitionJsonLeft", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput("cohortDefinitionJsonLeft"),
              tags$head(
                tags$style("#cohortDefinitionJsonLeft { max-height:400px};")
              )
            ),
            shiny::tabPanel(
              title = "Cohort SQL",
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
  
  #!!!!! inclusion rule - make by default selected and shown
  #Dynamic UI rendering for right side -----
  output$dynamicUIGenerationForCohortSelectedRight <-
    shiny::renderUI(expr = {
      shiny::column(
        getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable(),
        shiny::conditionalPanel(
          condition = "output.cohortDefinitionSelectedRowCount == 2 &
                     output.isCohortDefinitionRowSelected == true",
          shiny::htmlOutput(outputId = "nameOfSelectedCohortInCohortDefinitionTableRight"),
          shiny::tabsetPanel(
            id = "cohortDefinitionTwoTabSetPanel",
            type = "tab",
            shiny::tabPanel(
              title = "Cohort Count",
              value = "cohortDefinitionTwoCohortCountTabPanel",
              tags$br(),
              DT::dataTableOutput(outputId = "cohortCountsTableForSelectedCohortRight"),
              tags$br(),
              shiny::conditionalPanel(
                condition = "output.isDatabaseIdFoundForSelectedCohortCountRight == true",
                tags$h3("Inclusion Rules"),
                tags$table(width = "100%",
                           tags$tr(
                             tags$td(
                               shiny::radioButtons(
                                 inputId = "cohortDefinitionSecondInclusionRuleType",
                                 label = "Filter by",
                                 choices = c("Simplified", "Detailed"),
                                 selected = "Simplified",
                                 inline = TRUE
                               )
                             ),
                             tags$td(
                               shiny::conditionalPanel(
                                 condition = "input.cohortDefinitionSecondInclusionRuleType == 'Simplified' &
                                         output.getSimplifiedInclusionRuleResultsRightHasData == true",
                                 shiny::radioButtons(
                                   inputId = "cohortDefinitionSecondInclusionRuleTableFilters",
                                   label = "Filter by",
                                   choices = c("All", "Meet", "Gain", "Remain", "Totals"),
                                   selected = "All",
                                   inline = TRUE
                                 )
                               )
                             ),
                             tags$td(
                               align = "right",
                               shiny::downloadButton(
                                 "saveSimplifiedInclusionRuleTableForSelectedCohortCountRight",
                                 label = "",
                                 icon = shiny::icon("download"),
                                 style = "margin-top: 5px; margin-bottom: 5px;"
                               )
                             )
                           )),
                shiny::conditionalPanel(
                  condition = "input.cohortDefinitionSecondInclusionRuleType == 'Simplified'",
                  DT::dataTableOutput(outputId = "simplifiedInclusionRuleTableForSelectedCohortCountRight")
                )
              )
            ),
            shiny::tabPanel(
              title = "Details",
              value = "cohortDefinitionTwoDetailsTextTabPanel",
              tags$br(),
              shinydashboard::box(
                title = "Readable definitions",
                width = NULL,
                status = NULL,
                collapsible = TRUE,
                collapsed = FALSE,
                solidHeader = FALSE,
                copyToClipboardButton(toCopyId = "cohortDefinitionTextRight",
                                      style = "margin-top: 5px; margin-bottom: 5px;"),
                shiny::htmlOutput("circeRVersionInCohortDefinitionRight"),
                shiny::htmlOutput("cohortDefinitionTextRight")
              ),
              shinydashboard::box(
                title = "Meta data",
                width = NULL,
                status = NULL,
                collapsible = TRUE,
                collapsed = FALSE,
                solidHeader = FALSE,
                shiny::htmlOutput("cohortDetailsTextRight")
              )
            ),
            shiny::tabPanel(
              title = "Concept Sets",
              #!!!!!!!!!if cohort has no concept sets - make gray color or say 'No Concept sets'
              value = "conceptSetTwoTabPanel",
              DT::dataTableOutput(outputId = "conceptSetsInCohortRight"),
              tags$br(),
              shiny::conditionalPanel(
                condition = "output.isAConceptSetSelectedForCohortRight == true",
                shinydashboard::box(
                  title = shiny::textOutput(outputId = "conceptSetExpressionNameRight"),
                  solidHeader = FALSE,
                  width = NULL,
                  collapsible = TRUE,
                  collapsed = FALSE,
                  shiny::conditionalPanel(condition = "output.isAConceptSetSelectedForCohortRight == true",
                                          tags$table(
                                            tags$tr(
                                              tags$td(
                                                shinyWidgets::pickerInput(
                                                  #!!! lets make this multi-selected for databaseId/vocabulary choices
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
                                            tags$tr(tags$td(
                                              colspan = 2,
                                              shiny::radioButtons(
                                                inputId = "conceptSetsTypeRight",
                                                label = "",
                                                choices = c(
                                                  "Concept Set Expression",
                                                  "Resolved",
                                                  "Excluded",
                                                  "Orphan concepts",
                                                  "Json"
                                                ),
                                                #!!! add concept set sql
                                                selected = "Concept Set Expression",
                                                inline = TRUE
                                              )
                                            ))
                                          )),
                  shiny::conditionalPanel(
                    condition = "output.isAConceptSetSelectedForCohortRight == true &
                                                      input.conceptSetsTypeRight != 'Resolved' &
                                                      input.conceptSetsTypeRight != 'Excluded' &
                                                      input.conceptSetsTypeRight != 'Json' &
                                                      input.conceptSetsTypeRight != 'Orphan concepts'",
                    #!!! add concept set sql
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveConceptSetsExpressionTableRight",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "conceptSetsExpressionTableRight")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.conceptSetsTypeRight == 'Resolved'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveCohortDefinitionResolvedConceptTableRight",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "cohortDefinitionResolvedConceptTableRight")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.conceptSetsTypeRight == 'Excluded'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveCohortDefinitionExcludedConceptTableRight",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "cohortDefinitionExcludedConceptTableRight")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.conceptSetsTypeRight == 'Orphan concepts'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveCohortDefinitionOrphanConceptTableRight",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
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
                )
              )
            ),
            
            shiny::tabPanel(
              title = "Cohort JSON",
              value = "cohortDefinitionTwoJsonTabPanel",
              copyToClipboardButton("cohortDefinitionJsonRight", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput("cohortDefinitionJsonRight"),
              tags$head(
                tags$style("#cohortDefinitionJsonRight { max-height:400px};")
              )
            ),
            shiny::tabPanel(
              title = "Cohort SQL",
              value = "cohortDefinitionTwoSqlTabPanel",
              copyToClipboardButton("cohortDefinitionSqlRight", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::htmlOutput("circeRVersionInCohortDefinitionSqlRight"),
              shiny::verbatimTextOutput("cohortDefinitionSqlRight"),
              tags$head(
                tags$style("#cohortDefinitionSqlRight { max-height:400px};")
              )
            )
          )
        )
      )
    })
  
  
  getSelectedConceptIdActive <- reactiveVal(NULL)
  getSelectedConceptNameActive <- reactiveVal(NULL)
  getDatabaseIdsForselectedConceptSet <- reactiveVal(NULL)
  
  #Dynamic UI rendering for relationship table -----
  output$dynamicUIForRelationshipAndComparisonTable <-
    shiny::renderUI({
      inc <-  1
      panels <- list()
      #Adopts new method, Since UI is rendered dynamically,We can only Hide/Show the tab only after DOM loads.
      if (!is.null(getSelectedConceptIdActive())) {
        panels[[inc]] <- shiny::tabPanel(
          title = "Concept Set Browser",
          value = "conceptSetBrowser",
          shiny::conditionalPanel(
            condition = "output.isConceptIdFromLeftOrRightConceptTableSelected==true",
            tags$h4(
              paste0(
                getSelectedConceptNameActive(),
                " (",
                getSelectedConceptIdActive(),
                ")"
              )
            ),
            tags$table(width = "100%",
                       tags$tr(
                         tags$td(
                           shinyWidgets::pickerInput(
                             inputId = "choicesForRelationshipName",
                             label = "Relationship Category:",
                             choices = c(
                               'Not applicable',
                               relationship$relationshipName %>% sort()
                             ),
                             selected = c(
                               'Not applicable',
                               relationship$relationshipName %>% sort()
                             ),
                             multiple = TRUE,
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
                           shinyWidgets::pickerInput(
                             inputId = "choicesForRelationshipDistance",
                             label = "Distance:",
                             choices = getConceptRelationshipDistanceChoices(),
                             selected = getConceptRelationshipDistanceChoices(),
                             multiple = TRUE,
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
                           align = "right",
                           shiny::downloadButton(
                             "saveDetailsOfSelectedConceptId",
                             label = "",
                             icon = shiny::icon("download"),
                             style = "margin-top: 5px; margin-bottom: 5px;"
                           )
                         )
                       )),
            DT::dataTableOutput(outputId = "detailsOfSelectedConceptId")
          )
        )
        inc = inc + 1
        
        
        panels[[inc]] <- shiny::tabPanel(
          title = "Time Series Plot",
          value = "conceptSetTimeSeries",
          tags$h5(
            paste0(
              getSelectedConceptNameActive(),
              " (",
              getSelectedConceptIdActive(),
              ")"
            )
          ),
          DT::dataTableOutput(outputId = "conceptSetTimeSeriesPlot")
          
        )
        inc = inc + 1
      }
      
      if (all(
        length(input$cohortDefinitionTable_rows_selected) == 2,
        !is.null(getConceptSetExpressionLeft()),
        !is.null(getConceptSetExpressionRight())
      )) {
        panels[[inc]] <- shiny::tabPanel(
          title = "Concept Set Comparison",
          value = "conceptSetComparison",
          DT::dataTableOutput(outputId = "conceptSetComparisonTable")
        )
      }
      do.call(tabsetPanel, panels)
    })
  
  output$conceptSetComparisonTable <- DT::renderDT(expr = {
    resolvedConceptsLeft <- getResolvedConceptsLeft()
    resolvedConceptsRight <- getResolvedConceptsRight()
    if (any(
      is.null(resolvedConceptsLeft),
      length(resolvedConceptsLeft) == 0,
      is.null(resolvedConceptsRight),
      length(resolvedConceptsRight) == 0
    )) {
      return(NULL)
    }
    
    if (input$conceptSetsTypeLeft == "Resolved" &
        input$conceptSetsTypeRight == 'Resolved') {
      combinedResult <-
        resolvedConceptsLeft %>%
        dplyr::union(resolvedConceptsRight) %>%
        dplyr::arrange(.data$conceptId) %>%
        dplyr::select(.data$conceptId, .data$conceptName) %>%
        dplyr::mutate(left = "", right = "")
      
      cohortIdsPresentInLeft <- resolvedConceptsLeft %>%
        dplyr::pull(.data$conceptId) %>%
        unique()
      
      cohortIdsPresentInRight <- resolvedConceptsRight %>%
        dplyr::pull(.data$conceptId) %>%
        unique()
    }
    
    for (i in 1:nrow(combinedResult)) {
      combinedResult$left[i] <-
        ifelse(
          combinedResult$conceptId[i] %in% cohortIdsPresentInLeft,
          as.character(icon("check")),
          ""
        )
      combinedResult$right[i] <-
        ifelse(
          combinedResult$conceptId[i] %in% cohortIdsPresentInRight,
          as.character(icon("check")),
          ""
        )
    }
    
    options = list(
      pageLength = 20,
      searching = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      info = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      scrollY = "20vh",
      columnDefs = list(truncateStringDef(1, 30))
    )
    
    dataTable <- DT::datatable(
      combinedResult,
      options = options,
      colnames = colnames(combinedResult) %>% camelCaseToTitleCase(),
      rownames = FALSE,
      escape = FALSE,
      selection = 'single',
      filter = "top",
      class = "stripe nowrap compact"
    )
    return(dataTable)
  })
  
  output$conceptSetTimeSeriesPlot <-  ggiraph::renderggiraph({
    # working on the plot
    getConceptCountTsibbleAtConceptIdYearLevel()
    
    data <- getFixedTimeSeriesDataForPlot()
    validate(need(
      all(!is.null(data),
          nrow(data) > 0),
      "No timeseries data for the cohort of this series type"
    ))
    plot <- plotTimeSeriesFromTsibble(
      tsibbleData = data,
      yAxisLabel = titleCaseToCamelCase(input$timeSeriesPlotFilters),
      indexAggregationType = input$timeSeriesAggregationPeriodSelection,
      timeSeriesStatistics = input$timeSeriesStatistics
    )
    plot <- ggiraph::girafe(
      ggobj = plot,
      options = list(
        ggiraph::opts_sizing(width = .5),
        ggiraph::opts_zoom(max = 5)
      )
    )
    return(plot)
    return(NULL)
  })
  
  #getConceptMetadataDetails----
  getConceptMetadataDetails <- shiny::reactive(x = {
    selectedConceptId <- getSelectedConceptIdActive()
    if (any(is.null(selectedConceptId),
            length(selectedConceptId) == 0)) {
      return(NULL)
    }
    selectedDatabaseId <- getDatabaseIdsForselectedConceptSet()
    if (any(is.null(selectedDatabaseId),
            length(selectedDatabaseId) == 0)) {
      return(NULL)
    }
    data <- getConceptMetadata(dataSource = dataSource,
                               conceptIds = selectedConceptId)
    if (any(is.null(data),
            length(data) == 0)) {
      return(NULL)
    }
    return(data)
  })
  
  #getConceptRelationshipForSelectedConceptId----
  getConceptRelationshipForSelectedConceptId <-
    shiny::reactive(x = {
      selectedConceptId <- getSelectedConceptIdActive()
      if (any(is.null(selectedConceptId),
              length(selectedConceptId) == 0)) {
        return(NULL)
      }
      selectedDatabaseId <- getDatabaseIdsForselectedConceptSet()
      if (any(is.null(selectedDatabaseId),
              length(selectedDatabaseId) == 0)) {
        return(NULL)
      }
      conceptMetadata <- getConceptMetadataDetails()
      if (any(is.null(conceptMetadata),
              length(conceptMetadata) == 0)) {
        return(NULL)
      }
      conceptRelationship <- conceptMetadata$conceptRelationship %>%
        dplyr::filter(.data$conceptId1 %in% selectedConceptId) %>%
        dplyr::select(-.data$conceptId1) %>%
        dplyr::filter(is.na(.data$invalidReason)) %>%
        dplyr::rename("conceptId" = .data$conceptId2) %>%
        dplyr::select(.data$conceptId,
                      .data$relationshipId)
      return(conceptRelationship)
    })
  
  #getConceptAncestorForSelectedConceptId----
  getConceptAncestorForSelectedConceptId <-
    shiny::reactive(x = {
      selectedConceptId <- getSelectedConceptIdActive()
      if (any(is.null(selectedConceptId),
              length(selectedConceptId) == 0)) {
        return(NULL)
      }
      selectedDatabaseId <- getDatabaseIdsForselectedConceptSet()
      if (any(is.null(selectedDatabaseId),
              length(selectedDatabaseId) == 0)) {
        return(NULL)
      }
      conceptMetadata <- getConceptMetadataDetails()
      if (any(is.null(conceptMetadata),
              length(conceptMetadata) == 0)) {
        return(NULL)
      }
      conceptAncestor <- conceptMetadata$conceptAncestor %>%
        dplyr::filter(.data$descendantConceptId %in% selectedConceptId) %>%
        dplyr::rename(
          "conceptId" = .data$ancestorConceptId,
          "levelsOfSeparation" = .data$minLevelsOfSeparation
        ) %>%
        dplyr::select(.data$conceptId,
                      .data$levelsOfSeparation) %>%
        dplyr::distinct() %>%
        dplyr::mutate(levelsOfSeparation = levelsOfSeparation * -1)
      conceptDescendant <- conceptMetadata$conceptAncestor %>%
        dplyr::filter(.data$ancestorConceptId %in% selectedConceptId) %>%
        dplyr::rename(
          "conceptId" = .data$descendantConceptId,
          "levelsOfSeparation" = .data$minLevelsOfSeparation
        ) %>%
        dplyr::select(.data$conceptId,
                      .data$levelsOfSeparation) %>%
        dplyr::distinct()
      conceptAncestor <- dplyr::bind_rows(conceptAncestor,
                                          conceptDescendant) %>%
        dplyr::distinct() %>%
        dplyr::arrange(dplyr::desc(.data$levelsOfSeparation))
      return(conceptAncestor)
    })
  
  getConceptRelationshipDistanceChoices <- reactive({
    data <- getConceptAncestorForSelectedConceptId()
    if (is.null(data)) {
      return(NULL)
    }
    data <-
      c(
        "Not applicable",
        data %>%
          dplyr::distinct(.data$levelsOfSeparation) %>%
          dplyr::arrange(dplyr::desc(.data$levelsOfSeparation)) %>%
          dplyr::pull(.data$levelsOfSeparation) %>%
          as.character()
      ) %>% unique()
    return(data)
  })
  
  #getDetailsForSelectedConceptId----
  getDetailsForSelectedConceptId <- shiny::reactive(x = {
    #!!!!!!!!! add error handling
    conceptMetadata <- getConceptMetadataDetails()
    if (any(is.null(conceptMetadata),
            length(conceptMetadata) == 0)) {
      return(NULL)
    }
    conceptRelationships <-
      getConceptRelationshipForSelectedConceptId() %>%
      dplyr::inner_join(
        relationship %>%
          dplyr::select(.data$relationshipId,
                        .data$relationshipName),
        by = "relationshipId"
      ) %>%
      dplyr::group_by(.data$conceptId) %>%
      dplyr::mutate(relationships = paste0(.data$relationshipName, collapse = ",<br> ")) %>%  #!!! collapse with line break if possible
      dplyr::ungroup() %>%
      dplyr::select(.data$conceptId,
                    .data$relationships,
                    .data$relationshipId) %>%
      dplyr::distinct() %>%
      dplyr::arrange(.data$conceptId)
    
    concept <- conceptMetadata$concept %>%
      dplyr::left_join(
        conceptMetadata$conceptCount %>%
          dplyr::filter(
            .data$databaseId %in% getDatabaseIdsForselectedConceptSet()
          ),
        by = "conceptId"
      ) %>%
      dplyr::left_join(conceptRelationships,
                       by = 'conceptId') %>%
      dplyr::left_join(getConceptAncestorForSelectedConceptId(),
                       by = "conceptId") %>%
      dplyr::select(
        .data$conceptId,
        .data$conceptName,
        .data$relationships,
        .data$levelsOfSeparation,
        .data$conceptCount,
        .data$subjectCount,
        .data$conceptCode,
        .data$standardConcept,
        .data$vocabularyId,
        .data$relationshipId
      ) %>%
      dplyr::arrange(dplyr::desc(.data$conceptCount)) %>%
      dplyr::mutate(levelsOfSeparation = as.character(.data$levelsOfSeparation)) %>%
      tidyr::replace_na(list(relationships = "Not applicable",
                             levelsOfSeparation = "Not applicable"))
    
    if (!is.null(input$choicesForRelationshipName)) {
      concept <- concept %>%
        dplyr::rowwise() %>%
        dplyr::mutate(hits = sum(
          stringr::str_detect(
            string = .data$relationships,
            pattern = input$choicesForRelationshipName
          )
        )) %>%
        dplyr::filter(.data$hits > 0) %>%
        dplyr::select(-.data$hits)
    }
    
    if (!is.null(input$choicesForRelationshipDistance)) {
      concept <- concept %>%
        dplyr::filter(.data$levelsOfSeparation %in%
                        input$choicesForRelationshipDistance)
    }
    concept <- concept %>%
      dplyr::select(-.data$relationshipId)
    browser()
    conceptCooccurrence <-
      getResultsConceptCooccurrence(dataSource = dataSource,
                                    cohortIds = getSelectedRowsInCohortTableOfCohortDefinitionTab()$cohortId[[1]])
    if (all(!is.null(conceptCooccurrence),
            nrow(conceptCooccurrence) > 0)) {
      conceptCooccurrence <- conceptCooccurrence %>%
        dplyr::filter(.data$conceptId %in% getSelectedConceptIdActive()) %>%
        dplyr::select(-.data$conceptId, -.data$cohortId) %>%
        dplyr::rename(conceptId = .data$coConceptId) %>%
        tidyr::pivot_wider(
          id_cols = c("conceptId"),
          names_from = "databaseId",
          names_prefix = "indexDateCoOccurrence ",
          values_from = "conceptCount"
        )
      concept <- concept %>%
        dplyr::left_join(conceptCooccurrence,
                         by = "conceptId") %>%
        dplyr::relocate(colnames(conceptCooccurrence)) %>%
        dplyr::relocate("conceptId", "conceptName")
    }
    return(concept)
  })
  
  #output: detailsOfSelectedConceptId----
  output$detailsOfSelectedConceptId <- DT::renderDT(expr = {
    validate(need(all(
      !is.null(getSelectedConceptIdActive()),
      length(getSelectedConceptIdActive()) > 0
    ),
    "No concept id selected."))
    validate(need(all(
      !is.null(getDatabaseIdsForselectedConceptSet()),
      length(getDatabaseIdsForselectedConceptSet()) > 0
    ),
    "No database id selected."))
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0(
        "Computing concept relationship for concept id:",
        getSelectedConceptIdActive()
      ),
      value = 0
    )
    
    data <- getDetailsForSelectedConceptId()
    options = list(
      pageLength = 10,
      searching = TRUE,
      scrollX = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE
    )
    
    table <- DT::datatable(
      data,
      options = options,
      colnames = colnames(data) %>% camelCaseToTitleCase(),
      rownames = FALSE,
      escape = FALSE,
      filter = "top",
      class = "stripe nowrap compact"
    )
    return(table)
  })
  
  
  ##output: isConceptIdFromLeftOrRightConceptTableSelected----
  output$isConceptIdFromLeftOrRightConceptTableSelected <-
    shiny::reactive(x = {
      return(any(!is.null(consolidatedConceptSetIdLeft()),
                 !is.null(consolidatedConceptSetIdRight())))
    })
  shiny::outputOptions(x = output,
                       name = "isConceptIdFromLeftOrRightConceptTableSelected",
                       suspendWhenHidden = FALSE)
  
  #output: conceptSetExpressionNameLeft----
  output$conceptSetExpressionNameLeft <-
    shiny::renderText(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdLeft())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdLeft())) {
        return(NULL)
      }
      data <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdLeft()) %>%
        dplyr::filter(.data$cohortId %in% consolidatedConceptSetIdLeft()) %>%
        dplyr::pull(.data$conceptSetName)
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
    })
  
  getConceptSetsInCohortDataLeft <- reactive({
    if (!doesObjectHaveData(consolidatedCohortIdLeft())) {
      return(NULL)
    }
    data <- conceptSets %>% 
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdLeft()) %>% 
      dplyr::select(.data$conceptSetId, .data$conceptSetName) %>% 
      dplyr::arrange(.data$conceptSetId)
    return(data)
  })
  
  #output: conceptSetsInCohortLeft----
  output$conceptSetsInCohortLeft <-
    DT::renderDataTable(expr = {
      data <- getConceptSetsInCohortDataLeft()
      if (!doesObjectHaveData(data)) {
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
  
  getConceptSetsInCohortDataRight <- reactive({
    if (!doesObjectHaveData(consolidatedCohortIdRight())) {
      return(NULL)
    }
    data <- conceptSets %>% 
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdRight()) %>% 
      dplyr::select(.data$conceptSetId, .data$conceptSetName) %>% 
      dplyr::arrange(.data$conceptSetId)
    return(data)
  })
  
  #output: conceptSetsInCohortRight----
  output$conceptSetsInCohortRight <-
    DT::renderDataTable(expr = {
      data <- getConceptSetsInCohortDataRight()
      if (!doesObjectHaveData(data)) {
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
  
  #output: conceptsetExpressionTableLeft----
  output$conceptsetExpressionTableLeft <-
    DT::renderDataTable(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdLeft())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdLeft())) {
        return(NULL)
      }
      
      data <- conceptSets %>% 
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdLeft()) %>% 
        dplyr::filter(.data$conceptSetid %in% consolidatedConceptSetIdLeft()) %>% 
        dplyr::select(.data$conceptSetExpression)
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
      data <- getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression = data)
      
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
  
  #output: isAConceptSetSelectedForCohortLeft----
  output$isAConceptSetSelectedForCohortLeft <-
    shiny::reactive(x = {
      data <- input$conceptSetsInCohortLeft_rows_selected
      return(!is.null(data))
    })
  shiny::outputOptions(x = output,
                       name = "isAConceptSetSelectedForCohortLeft",
                       suspendWhenHidden = FALSE)
  
  #output: saveConceptSetsExpressionTableLeft----
  output$saveConceptSetsExpressionTableLeft <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "ConceptSetsExpression")
    },
    content = function(file) {
      downloadCsv(x = getConceptSetExpressionLeft(), fileName = file)
      #!!!! this may need downloadExcel() with formatted and multiple tabs
    }
  )
  
  #output: conceptSetsExpressionTableLeft----
  output$conceptSetsExpressionTableLeft <-
    DT::renderDataTable(expr = {
      data <- getConceptSetExpressionLeft()
      if (is.null(data)) {
        return(NULL)
      }
      
      data$isExcluded <-
        ifelse(data$isExcluded, as.character(icon("check")), "")
      data$includeDescendants <-
        ifelse(data$includeDescendants, as.character(icon("check")), "")
      data$includeMapped <-
        ifelse(data$includeMapped, as.character(icon("check")), "")
      data$invalidReason <-
        ifelse(data$invalidReason != 'V', as.character(icon("check")), "")
      
      data <- data %>%
        dplyr::rename(
          exclude = .data$isExcluded,
          descendants = .data$includeDescendants,
          mapped = .data$includeMapped,
          invalid = .data$invalidReason
        )
      
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
        scrollY = "20vh",
        columnDefs = list(truncateStringDef(1, 80))
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
  output$personAndRecordCountForConceptSetRowSelectedLeft <-
    shiny::renderUI({
      row <- getCountsForSelectedCohortsLeftFilteredToDatabaseId()
      if (is.null(row)) {
        return(NULL)
      } else {
        tags$table(tags$tr(
          tags$td(tags$b("Subjects: ")),
          tags$td(scales::comma(row$cohortSubjects, accuracy = 1)),
          tags$td(tags$b("Records: ")),
          tags$td(scales::comma(row$cohortEntries, accuracy = 1))
        ))
      }
    })
  
  
  #output: saveCohortDefinitionResolvedConceptTableLeft----
  output$saveCohortDefinitionResolvedConceptTableLeft <-
    downloadHandler(
      filename = function() {
        getCsvFileNameWithDateTime(string = "ResolvedConcepts")
      },
      content = function(file) {
        data <- getResolvedConceptsLeft()
        downloadCsv(x = data, fileName = file)
      }
    )
  
  #output: cohortDefinitionResolvedConceptTableLeft----
  output$cohortDefinitionResolvedConceptTableLeft <-
    DT::renderDataTable(expr = {
      validate(need(
        length(consolidatedCohortIdLeft()) > 0,
        "Please select concept set"
      ))
      data <- getResolvedConceptsLeft()
      validate(need((all(
        !is.null(data), nrow(data) > 0
      )),
      "No resolved concept ids"))
      
      columnDef <- list(truncateStringDef(1, 80))
      maxCount <- NULL
      maxSubject <- NULL
      if ("subjects" %in% colnames(data) &&
          "count" %in% colnames(data)) {
        columnDef <- list(truncateStringDef(1, 80), minCellCountDef(2:3))
        maxCount <- max(data$count, na.rm = TRUE)
        maxSubject <- max(data$subjects, na.rm = TRUE)
      }
      
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
      data <- getOrphanConceptsLeft()
      downloadCsv(x = data, fileName = file)
    }
  )
  
  #output: cohortDefinitionExcludedConceptTableLeft----
  output$cohortDefinitionExcludedConceptTableLeft <-
    DT::renderDataTable(expr = {
      validate(need(
        length(consolidatedCohortIdLeft()) > 0,
        "Please select concept set"
      ))
      data <- getExcludedConceptsLeft()
      validate(need((all(
        !is.null(data), nrow(data) > 0
      )),
      "No excluded concept ids"))
      
      columnDef <- list(truncateStringDef(1, 80))
      maxCount <- NULL
      maxSubject <- NULL
      if ("subjects" %in% colnames(data) &&
          "count" %in% colnames(data)) {
        columnDef <- list(truncateStringDef(1, 80), minCellCountDef(2:3))
        maxCount <- max(data$count, na.rm = TRUE)
        maxSubject <- max(data$subjects, na.rm = TRUE)
      }
      
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
  
  #output: saveExcludedConceptsTableLeft----
  output$saveExcludedConceptsTableLeft <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "excludedConcepts")
    },
    content = function(file) {
      data <- getExcludedConceptsLeft()
      downloadCsv(x = data, fileName = file)
    }
  )
  
  #output: cohortDefinitionOrphanConceptTableLeft----
  output$cohortDefinitionOrphanConceptTableLeft <-
    DT::renderDataTable(expr = {
      data <- getOrphanConceptsLeft()
      if (any(is.null(data),
              nrow(data) == 0)) {
        return(NULL)
      } else {
        orphanConceptDataDatabaseIds <-
          unique(data$databaseId)
        orphanConceptDataMaxCount <-
          max(data$subjectCount , na.rm = TRUE)
      }
      validate(need(any(!is.null(data),
                        nrow(data) > 0),
                    "No orphan concepts"))
      
      columnDef <- list(truncateStringDef(1, 80))
      maxCount <- NULL
      maxSubject <- NULL
      if ("subjects" %in% colnames(data) &&
          "count" %in% colnames(data)) {
        columnDef <- list(truncateStringDef(1, 80), minCellCountDef(2:3))
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
  
  #output: conceptsetExpressionJsonLeft----
  output$conceptsetExpressionJsonLeft <- shiny::renderText({
    if (any(!doesObjectHaveData(getConceptSetExpressionLeft()),
            !doesObjectHaveData(consolidatedConceptSetIdLeft()))) {
      return(NULL)
    }
    conceptSetExpression <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdLeft()) %>%
      dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdLeft()) %>% 
      dplyr::pull(.data$conceptSetExpression)
    
  })
  
  #!!!!!!!!!!!! add excluded
  
  #!!! on row select for resolved/excluded/orphan - we need to show for selected cohort
  #!!! a trend plot with conceptCount over time and
  #!!! concept details (concept synonyms, concept relationships, concept ancestor, concept descendants)
  #!!! for both left and right
  #!!! as collapsible text box, that is by default collapsed with lazy loading
  #!!! data is in getConceptSetDetailsLeft reactive function and getConceptCountData reactive function
  
  
  ##output: cohortCountsTableForSelectedCohortRight----
  output$cohortCountsTableForSelectedCohortRight <-
    DT::renderDataTable(expr = {
      data <- getCountsForSelectedCohortsLeft()
      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
        "There is no inclusion rule data for this cohort."
      ))
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
      
    }, server = TRUE)
  
  ##reactive: isDatabaseIdFoundForSelectedCohortCountRight----
  output$isDatabaseIdFoundForSelectedCohortCountRight <-
    shiny::reactive(x = {
      return(!is.null(getSimplifiedInclusionRuleResultsRight()))
    })
  shiny::outputOptions(x = output,
                       name = "isDatabaseIdFoundForSelectedCohortCountRight",
                       suspendWhenHidden = FALSE)
  
  ##output: simplifiedInclusionRuleTableForSelectedCohortCountRight----
  output$simplifiedInclusionRuleTableForSelectedCohortCountRight <-
    DT::renderDataTable(expr = {
      if (any(
        is.null(consolidatedCohortIdRight())
      )) {
        return(NULL)
      }
      
      table <- getSimplifiedInclusionRuleResultsRight()
      validate(need((nrow(table) > 0),
                    "There is no inclusion rule data for this cohort."))
      
      databaseIds <- unique(table$databaseId)
      cohortCounts <- table %>%
        dplyr::inner_join(cohortCount,
                          by = c("cohortId", "databaseId")) %>%
        dplyr::filter(.data$cohortId == consolidatedCohortIdRight()) %>%
        dplyr::filter(.data$databaseId %in% getDatabaseIdForSelectedCohortCountRight()) %>%
        dplyr::select(.data$cohortSubjects) %>%
        dplyr::pull(.data$cohortSubjects) %>%
        unique()
      
      databaseIdsWithCount <-
        paste(databaseIds,
              "(n = ",
              format(cohortCounts, big.mark = ","),
              ")")
      
      table <- table %>%
        dplyr::inner_join(
          cohortCount %>%
            dplyr::select(.data$databaseId, .data$cohortId, .data$cohortSubjects),
          by = c('databaseId', 'cohortId')
        ) %>%
        tidyr::pivot_longer(
          cols = c(
            .data$meetSubjects,
            .data$gainSubjects,
            .data$totalSubjects,
            .data$remainSubjects
          )
        ) %>%
        dplyr::mutate(name = paste0(
          .data$databaseId,
          "<br>(n = ",
          scales::comma(x = .data$cohortSubjects, accuracy = 1),
          ")_",
          .data$name
        )) %>%
        tidyr::pivot_wider(
          id_cols = c(.data$cohortId, .data$ruleSequenceId, .data$ruleName),
          names_from = .data$name,
          values_from = .data$value
        ) %>%
        dplyr::select(-.data$cohortId)
      
      if (input$cohortDefinitionSecondInclusionRuleTableFilters == "Meet") {
        table <- table %>%
          dplyr::select(
            -dplyr::contains("Total"),-dplyr::contains("Gain"),-dplyr::contains("Remain")
          )
        colnames(table) <-
          stringr::str_replace(
            string = colnames(table),
            pattern = '_meetSubjects',
            replacement = ''
          )
        
        columnDefs <- minCellCountDef(1 + (1:(length(databaseIds))))
        
      } else if (input$cohortDefinitionSecondInclusionRuleTableFilters == "Totals") {
        table <- table %>%
          dplyr::select(
            -dplyr::contains("Meet"),-dplyr::contains("Gain"),-dplyr::contains("Remain")
          )
        colnames(table) <-
          stringr::str_replace(
            string = colnames(table),
            pattern = '_totalSubjects',
            replacement = ''
          )
        
        columnDefs <- minCellCountDef(1 + (1:(length(databaseIds))))
        
      } else if (input$cohortDefinitionSecondInclusionRuleTableFilters == "Gain") {
        table <- table %>%
          dplyr::select(
            -dplyr::contains("Total"),-dplyr::contains("Meet"),-dplyr::contains("Remain")
          )
        colnames(table) <-
          stringr::str_replace(
            string = colnames(table),
            pattern = '_gainSubjects',
            replacement = ''
          )
        
        columnDefs <- minCellCountDef(1 + (1:(length(databaseIds))))
        
      } else if (input$cohortDefinitionSecondInclusionRuleTableFilters == "Remain") {
        table <- table %>%
          dplyr::select(
            -dplyr::contains("Total"),-dplyr::contains("Meet"),-dplyr::contains("Gain")
          )
        colnames(table) <-
          stringr::str_replace(
            string = colnames(table),
            pattern = '_remainSubjects',
            replacement = ''
          )
        
        columnDefs <- minCellCountDef(1 + (1:(length(databaseIds))))
        
      }  else {
        sketch <- htmltools::withTags(table(class = "display",
                                            thead(tr(
                                              th(rowspan = 2, "Rule Sequence ID"),
                                              th(rowspan = 2, "Rule Name"),
                                              lapply(
                                                databaseIdsWithCount,
                                                th,
                                                colspan = 4,
                                                class = "dt-center",
                                                style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                              )
                                            ),
                                            tr(
                                              lapply(rep(
                                                c("Meet", "Gain", "Remain", "Total"),
                                                length(databaseIds)
                                              ), th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                            ))))
        
        columnDefs <-
          minCellCountDef(1 + (1:(length(databaseIds) * 4)))
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
  
  ##output: saveSimplifiedInclusionRuleTableForSelectedCohortCountRight----
  output$saveSimplifiedInclusionRuleTableForSelectedCohortCountRight <-
    downloadHandler(
      filename = function()
      {
        getCsvFileNameWithDateTime(string = "InclusionRule")
      },
      content = function(file)
      {
        downloadCsv(x = getSimplifiedInclusionRuleResultsRight(), fileName = file)
      }
    )
  
  ##output: getSimplifiedInclusionRuleResultsRightHasData----
  output$getSimplifiedInclusionRuleResultsRightHasData <-
    shiny::reactive(x = {
      return(!is.null(getSimplifiedInclusionRuleResultsRight()))
    })
  
  shiny::outputOptions(x = output,
                       name = "getSimplifiedInclusionRuleResultsRightHasData",
                       suspendWhenHidden = FALSE)
  
  
  ##output: cohortDefinitionJsonRight----
  output$cohortDefinitionJsonRight <- shiny::renderText({
    json <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdRight()) %>%
      dplyr::pull(.data$json)
    if (!doesObjectHaveData(json)) {
      return(NULL)
    }
    json <- json %>% 
      RJSONIO::fromJSON(digits = 23) %>% 
      RJSONIO::toJSON(digits = 23, pretty = TRUE)
    return(json)
  })
  
  
  #output: conceptSetExpressionNameRight----
  output$conceptSetExpressionNameRight <-
    shiny::renderText(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdRight())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdRight())) {
        return(NULL)
      }
      data <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdRight()) %>%
        dplyr::filter(.data$cohortId %in% consolidatedConceptSetIdRight()) %>%
        dplyr::pull(.data$conceptSetName)
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
    })
  
  ##output: conceptsetExpressionTableRight----
  output$conceptsetExpressionTableRight <-
    DT::renderDataTable(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdRight())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdRight())) {
        return(NULL)
      }
      data <- conceptSets %>% 
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdRight()) %>% 
        dplyr::filter(.data$conceptSetid %in% consolidatedConceptSetIdRight()) %>% 
        dplyr::pull(.data$conceptSetExpression)
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
      data <- getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression = data)
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
  
  
  ##output: output.isAConceptSetSelectedForCohortRight----
  output$isAConceptSetSelectedForCohortRight <-
    shiny::reactive(x = {
      input$conceptSetsInCohortRight_rows_selected
    })
  shiny::outputOptions(x = output,
                       name = "isAConceptSetSelectedForCohortRight",
                       suspendWhenHidden = FALSE)
  
  ##output: conceptSetsExpressionTableRight----
  output$conceptSetsExpressionTableRight <-
    DT::renderDataTable(expr = {
      data <- getConceptSetExpressionRight()
      if (!doesObjectHaveData(data))
      {
        return(NULL)
      }
      
      data$isExcluded <-
        ifelse(data$isExcluded, as.character(icon("check")), "")
      data$includeDescendants <-
        ifelse(data$includeDescendants, as.character(icon("check")), "")
      data$includeMapped <-
        ifelse(data$includeMapped, as.character(icon("check")), "")
      data$invalidReason <-
        ifelse(data$invalidReason != 'V', as.character(icon("check")), "")
      
      data <- data %>%
        dplyr::rename(
          exclude = .data$isExcluded,
          descendants = .data$includeDescendants,
          mapped = .data$includeMapped,
          invalid = .data$invalidReason
        )
      
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
        scrollY = "20vh",
        columnDefs = list(truncateStringDef(1, 80))
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
  output$saveConceptSetsExpressionTableRight <-
    downloadHandler(
      filename = function()
      {
        getCsvFileNameWithDateTime(string = "conceptset")
      },
      content = function(file)
      {
        downloadCsv(x = getConceptSetsExpressionDetailsRight(),
                    fileName = file)
      }
    )
  
  ##output: personAndRecordCountForConceptSetRowSelectedRight----
  output$personAndRecordCountForConceptSetRowSelectedRight <-
    shiny::renderUI({
      row <- getCountsForSelectedCohortsRightFilteredToDatabaseId()
      if (is.null(row))
      {
        return(NULL)
      } else {
        tags$table(tags$tr(
          tags$td(tags$b("Subjects: ")),
          tags$td(scales::comma(row$cohortSubjects, accuracy = 1)),
          tags$td(tags$b("Records: ")),
          tags$td(scales::comma(row$cohortEntries, accuracy = 1))
        ))
      }
    })
  
  
  ##output: cohortDefinitionResolvedConceptTableRight----
  output$cohortDefinitionResolvedConceptTableRight <-
    DT::renderDataTable(expr = {
      data <- getResolvedConceptsRight()
      validate(need((all(
        !is.null(data), nrow(data) > 0
      )),
      "No resolved concept ids"))
      columnDef <- list(truncateStringDef(1, 80))
      maxCount <- NULL
      maxSubject <- NULL
      if ("subjects" %in% colnames(data) &&
          "count" %in% colnames(data))
      {
        columnDef <- list(truncateStringDef(1, 80), minCellCountDef(2:3))
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
  
  ##output: saveCohortDefinitionResolvedConceptTableRight----
  output$saveCohortDefinitionResolvedConceptTableRight <-
    downloadHandler(
      filename = function()
      {
        getCsvFileNameWithDateTime(string = "resolvedConceptSet")
      },
      content = function(file)
      {
        data <- getResolvedConceptsRight()
        downloadCsv(x = data, fileName = file)
      }
    )
  
  #output: cohortDefinitionExcludedConceptTableRight----
  output$cohortDefinitionExcludedConceptTableRight <-
    DT::renderDataTable(expr = {
      data <- getExcludedConceptsRight()
      validate(need((all(
        !is.null(data), nrow(data) > 0
      )),
      "No excluded concept ids"))
      
      columnDef <- list(truncateStringDef(1, 80))
      maxCount <- NULL
      maxSubject <- NULL
      if ("subjects" %in% colnames(data) &&
          "count" %in% colnames(data))
      {
        columnDef <- list(truncateStringDef(1, 80), minCellCountDef(2:3))
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
  
  #output: saveExcludedConceptsTableRight----
  output$saveExcludedConceptsTableRight <-  downloadHandler(
    filename = function()
    {
      getCsvFileNameWithDateTime(string = "excludedConcepts")
    },
    content = function(file)
    {
      data <- getExcludedConceptsRight()
      downloadCsv(x = data, fileName = file)
    }
  )
  
  output$cohortDefinitionOrphanConceptTableRight <-
    DT::renderDataTable(expr = {
      data <- getOrphanConceptsRight()
      if (any(is.null(data),
              nrow(data) == 0)) {
        return(NULL)
      } else {
        orphanConceptDataDatabaseIds <-
          unique(data$databaseId)
        orphanConceptDataMaxCount <-
          max(data$subjectCount , na.rm = TRUE)
      }
      
      validate(need(any(!is.null(data),
                        nrow(data) > 0),
                    "No orphan concepts"))
      
      columnDef <- list(truncateStringDef(1, 80))
      maxCount <- NULL
      maxSubject <- NULL
      if ("subjects" %in% colnames(data) &&
          "count" %in% colnames(data)) {
        columnDef <- list(truncateStringDef(1, 80), minCellCountDef(2:3))
        maxCount <- max(data$count, na.rm = TRUE)
        maxSubject <- max(data$subjects, na.rm = TRUE)
      }
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
  
  ##output: saveCohortDefinitionOrphanConceptTableRight----
  output$saveCohortDefinitionOrphanConceptTableRight <-
    downloadHandler(
      filename = function()
      {
        getCsvFileNameWithDateTime(string = "orphanconcepts")
      },
      content = function(file)
      {
        downloadCsv(x = getOrphanConceptsRight(),
                    fileName = file)
      }
    )
  
  ##output: conceptsetExpressionJsonRight----
  output$conceptsetExpressionJsonRight <- shiny::renderText({
    if (any(!doesObjectHaveData(consolidatedCohortIdRight()),
            !doesObjectHaveData(consolidatedConceptSetIdRight()))) {
      return(NULL)
    }
    conceptSetExpression <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdRight()) %>%
      dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdRight()) %>% 
      dplyr::pull(.data$conceptSetExpression)
    return(conceptSetExpression)
  })

  #Radio button synchronization----
  shiny::observeEvent(eventExpr = {
    list(input$conceptSetsTypeLeft,
         input$cohortDefinitionOneTabSetPanel)
  }, handlerExpr = {
    if (getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable() == 6) {
      if (!is.null(input$conceptSetsTypeLeft)) {
        if (input$conceptSetsTypeLeft == "Concept Set Expression") {
          updateRadioButtons(session = session,
                             inputId = "conceptSetsTypeRight",
                             selected = "Concept Set Expression")
        } else if (input$conceptSetsTypeLeft == "Resolved") {
          updateRadioButtons(session = session,
                             inputId = "conceptSetsTypeRight",
                             selected = "Resolved")
        }
        #!!!!!!!!!removed mapped
        else if (input$conceptSetsTypeLeft == "Excluded") {
          updateRadioButtons(session = session,
                             inputId = "conceptSetsTypeRight",
                             selected = "Excluded")
        } else if (input$conceptSetsTypeLeft == "Orphan concepts") {
          updateRadioButtons(session = session,
                             inputId = "conceptSetsTypeRight",
                             selected = "Orphan concepts")
        } else if (input$conceptSetsTypeLeft == "Json") {
          #!!! call this "Concept Set JSON"
          updateRadioButtons(session = session,
                             inputId = "conceptSetsTypeRight",
                             selected = "Json")
        }
      }
      
      if (!is.null(input$cohortDefinitionOneTabSetPanel)) {
        if (input$cohortDefinitionOneTabSetPanel == "cohortDefinitionOneDetailsTextTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionTwoTabSetPanel", selected = "cohortDefinitionTwoDetailsTextTabPanel")
        } else if (input$cohortDefinitionOneTabSetPanel == "cohortDefinitionOneCohortCountTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionTwoTabSetPanel", selected = "cohortDefinitionTwoCohortCountTabPanel")
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
    list(input$conceptSetsTypeRight,
         input$cohortDefinitionTwoTabSetPanel)
  }, handlerExpr = {
    if (getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable() == 6) {
      if (!is.null(input$conceptSetsTypeRight)) {
        if (input$conceptSetsTypeRight == "Concept Set Expression") {
          updateRadioButtons(session = session,
                             inputId = "conceptSetsTypeLeft",
                             selected = "Concept Set Expression")
        } else if (input$conceptSetsTypeRight == "Resolved") {
          updateRadioButtons(session = session,
                             inputId = "conceptSetsTypeLeft",
                             selected = "Resolved")
        } else if (input$conceptSetsTypeRight == "Excluded") {
          updateRadioButtons(session = session,
                             inputId = "conceptSetsTypeLeft",
                             selected = "Excluded")
        } else if (input$conceptSetsTypeRight == "Orphan concepts") {
          updateRadioButtons(session = session,
                             inputId = "conceptSetsTypeLeft",
                             selected = "Orphan concepts")
        } else if (input$conceptSetsTypeRight == "Json") {
          #!! call this "Concept Set JSON"
          updateRadioButtons(session = session,
                             inputId = "conceptSetsTypeLeft",
                             selected = "Json")
        }
      }
      
      if (!is.null(input$cohortDefinitionTwoTabSetPanel)) {
        if (input$cohortDefinitionTwoTabSetPanel == "cohortDefinitionTwoDetailsTextTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionOneTabSetPanel", selected = "cohortDefinitionOneDetailsTextTabPanel")
        } else if (input$cohortDefinitionTwoTabSetPanel == "cohortDefinitionTwoCohortCountTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "cohortDefinitionOneTabSetPanel", selected = "cohortDefinitionOneCohortCountTabPanel")
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
  #!!!!!!!!!! should be only shown when there is left and right
  ##output: resolvedConceptsPresentInLeft----
  output$resolvedConceptsPresentInLeft <- DT::renderDT({
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    result <-
      dplyr::setdiff(
        getDataForConceptSetComparison()$leftData,
        getDataForConceptSetComparison()$rightData
      )
    
    if (all(is.null(result), nrow(result) == 0))
    {
      return(NULL)
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
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
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    result <-
      dplyr::setdiff(
        getDataForConceptSetComparison()$rightData,
        getDataForConceptSetComparison()$leftData
      )
    
    if (nrow(result) == 0)
    {
      return(NULL)
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
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
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    result <-
      dplyr::intersect(
        getDataForConceptSetComparison()$leftData,
        getDataForConceptSetComparison()$rightData
      )
    
    if (nrow(result) == 0)
    {
      return(NULL)
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
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
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    result <-
      dplyr::union(
        getDataForConceptSetComparison()$leftData,
        getDataForConceptSetComparison()$rightData
      )
    
    if (nrow(result) == 0)
    {
      return(NULL)
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
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
  
  ##output: ExcludedConceptsPresentInLeft----
  output$excludedConceptsPresentInLeft <- DT::renderDT({
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    
    if (any(
      is.null(getExcludedConceptsLeft()),
      nrow(getExcludedConceptsLeft()) == 0,
      is.null(getExcludedConceptsRight()),
      nrow(getExcludedConceptsRight()) == 0
    )) {
      return(NULL)
    }
    
    result <-
      dplyr::setdiff(
        getExcludedConceptsLeft(),
        getExcludedConceptsRight()
      )
    
    if (any(is.null(result), nrow(result) == 0))
    {
      return(NULL)
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
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
  
  ##output: excludedConceptsPresentInRight----
  output$excludedConceptsPresentInRight <- DT::renderDT({
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    
    if (any(
      is.null(getExcludedConceptsLeft()),
      length(getExcludedConceptsLeft()) == 0,
      is.null(getExcludedConceptsRight),
      length(getExcludedConceptsRight()) == 0
    )) {
      return(NULL)
    }
    
    result <-
      dplyr::setdiff(
        getExcludedConceptsLeft(),
        getExcludedConceptsRight()
      )
    
    if (nrow(result) == 0)
    {
      return(NULL)
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
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
  
  ##output: excludedConceptsPresentInBoth----
  output$excludedConceptsPresentInBoth <- DT::renderDT({
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    
    if (any(
      is.null(getExcludedConceptsLeft()),
      nrow(getExcludedConceptsLeft() == 0),
      is.null(getExcludedConceptsRight()),
      nrow(getExcludedConceptsLeft() == 0)
    )) {
      return(NULL)
    }
    
    result <-
      dplyr::intersect(
        getExcludedConceptsLeft(),
        getExcludedConceptsRight()
      )
    
    if (nrow(result) == 0)
    {
      return(NULL)
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
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
  
  ##output: excludedConceptsPresentInEither----
  output$excludedConceptsPresentInEither <- DT::renderDT({
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    
    if (any(
      is.null(getExcludedConceptsLeft()),
      nrow(getExcludedConceptsLeft() == 0),
      is.null(getExcludedConceptsRight()),
      nrow(getExcludedConceptsLeft() == 0)
    )) {
      return(NULL)
    }
    
    result <-
      dplyr::union(
        getExcludedConceptsLeft(),
        getExcludedConceptsRight()
      )
    
    if (nrow(result) == 0)
    {
      return(NULL)
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
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
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    
    if (any(
      length(getConceptSetComparisonDetailsLeft()) == 0,
      length(getConceptSetComparisonDetailsRight()) == 0
    )) {
      return(NULL)
    }
    
    result <-
      dplyr::setdiff(getConceptSetComparisonDetailsLeft(),
                     getConceptSetComparisonDetailsRight())
    
    orphanConceptDataDatabaseIds <-
      attr(x = getConceptSetComparisonDetailsLeft(), which = 'databaseIds')
    orphanConceptDataMaxCount <-
      attr(x = getConceptSetComparisonDetailsLeft(), which = 'maxCount')
    if (nrow(result) == 0)
    {
      validate(need(nrow(result) > 0, "No data found"))
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
        scrollYHeight <- '25vh'
      }
      
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(
                                            tr(
                                              th(rowspan = 2, "Concept ID"),
                                              th(rowspan = 2, "Concept Name"),
                                              th(rowspan = 2, "Vocabulary ID"),
                                              th(rowspan = 2, "Concept Code"),
                                              lapply(
                                                orphanConceptDataDatabaseIds,
                                                th,
                                                colspan = 2,
                                                class = "dt-center"
                                              )
                                            ),
                                            tr(lapply(rep(
                                              c("Subjects", "Counts"),
                                              length(orphanConceptDataDatabaseIds)
                                            ), th))
                                          )))
      
      options = list(
        pageLength = 10,
        searching = TRUE,
        scrollX = TRUE,
        scrollY = '50vh',
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        columnDefs = list(truncateStringDef(1, 100),
                          minCellCountDef(3 + (
                            1:(length(orphanConceptDataDatabaseIds) * 2)
                          )))
      )
      
      table <- DT::datatable(
        result,
        options = options,
        colnames = colnames(result),
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      table <- DT::formatStyle(
        table = table,
        columns =  4 + (1:(
          length(orphanConceptDataDatabaseIds) * 2
        )),
        background = DT::styleColorBar(c(0, orphanConceptDataMaxCount), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      return(table)
    }
  })
  
  ##output: orphanConceptsPresentInRight----
  output$orphanConceptsPresentInRight <- DT::renderDT({
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    
    if (any(
      length(getConceptSetComparisonDetailsLeft()) == 0,
      length(getConceptSetComparisonDetailsRight()) == 0
    )) {
      return(NULL)
    }
    
    result <-
      dplyr::setdiff(getConceptSetComparisonDetailsRight(),
                     getConceptSetComparisonDetailsLeft())
    orphanConceptDataDatabaseIds <-
      attr(x = getConceptSetComparisonDetailsRight(), which = 'databaseIds')
    orphanConceptDataMaxCount <-
      attr(x = getConceptSetComparisonDetailsRight(), which = 'maxCount')
    
    if (nrow(result) == 0)
    {
      validate(need(nrow(result) > 0, "No data found"))
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
        scrollYHeight <- '25vh'
      }
      
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(
                                            tr(
                                              th(rowspan = 2, "Concept ID"),
                                              th(rowspan = 2, "Concept Name"),
                                              th(rowspan = 2, "Vocabulary ID"),
                                              th(rowspan = 2, "Concept Code"),
                                              lapply(
                                                orphanConceptDataDatabaseIds,
                                                th,
                                                colspan = 2,
                                                class = "dt-center"
                                              )
                                            ),
                                            tr(lapply(rep(
                                              c("Subjects", "Counts"),
                                              length(orphanConceptDataDatabaseIds)
                                            ), th))
                                          )))
      
      options = list(
        pageLength = 10,
        searching = TRUE,
        scrollX = TRUE,
        scrollY = '50vh',
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        columnDefs = list(truncateStringDef(1, 100),
                          minCellCountDef(3 + (
                            1:(length(orphanConceptDataDatabaseIds) * 2)
                          )))
      )
      
      table <- DT::datatable(
        result,
        options = options,
        colnames = colnames(result),
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      table <- DT::formatStyle(
        table = table,
        columns =  4 + (1:(
          length(orphanConceptDataDatabaseIds) * 2
        )),
        background = DT::styleColorBar(c(0, orphanConceptDataMaxCount), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      return(table)
    }
  })
  
  ##output: orphanConceptsPresentInBoth----
  output$orphanConceptsPresentInBoth <- DT::renderDT({
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    
    if (any(
      length(getConceptSetComparisonDetailsLeft()) == 0,
      length(getConceptSetComparisonDetailsRight()) == 0
    )) {
      return(NULL)
    }
    
    result <-
      dplyr::intersect(getConceptSetComparisonDetailsLeft(),
                       getConceptSetComparisonDetailsRight())
    orphanConceptDataDatabaseIds <-
      attr(x = getConceptSetComparisonDetailsLeft(), which = 'databaseIds')
    orphanConceptDataMaxCount <-
      attr(x = getConceptSetComparisonDetailsLeft(), which = 'maxCount')
    
    if (nrow(result) == 0)
    {
      validate(need(nrow(result) > 0, "No data found"))
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
        scrollYHeight <- '25vh'
      }
      
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(
                                            tr(
                                              th(rowspan = 2, "Concept ID"),
                                              th(rowspan = 2, "Concept Name"),
                                              th(rowspan = 2, "Vocabulary ID"),
                                              th(rowspan = 2, "Concept Code"),
                                              lapply(
                                                orphanConceptDataDatabaseIds,
                                                th,
                                                colspan = 2,
                                                class = "dt-center"
                                              )
                                            ),
                                            tr(lapply(rep(
                                              c("Subjects", "Counts"),
                                              length(orphanConceptDataDatabaseIds)
                                            ), th))
                                          )))
      
      options = list(
        pageLength = 10,
        searching = TRUE,
        scrollX = TRUE,
        scrollY = '50vh',
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        columnDefs = list(truncateStringDef(1, 100),
                          minCellCountDef(3 + (
                            1:(length(orphanConceptDataDatabaseIds) * 2)
                          )))
      )
      
      table <- DT::datatable(
        result,
        options = options,
        colnames = colnames(result),
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      table <- DT::formatStyle(
        table = table,
        columns =  4 + (1:(
          length(orphanConceptDataDatabaseIds) * 2
        )),
        background = DT::styleColorBar(c(0, orphanConceptDataMaxCount), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      return(table)
    }
  })
  
  ##output: orphanConceptsPresentInEither----
  output$orphanConceptsPresentInEither <- DT::renderDT({
    validate(
      need(
        input$choiceForConceptSetDetailsLeft == input$choiceForConceptSetDetailsRight,
        "Please select same database for comparison"
      )
    )
    
    if (any(
      length(getConceptSetComparisonDetailsLeft()) == 0,
      length(getConceptSetComparisonDetailsRight()) == 0
    )) {
      return(NULL)
    }
    
    result <- dplyr::union(getConceptSetComparisonDetailsLeft(),
                           getConceptSetComparisonDetailsRight())
    orphanConceptDataDatabaseIds <-
      attr(x = getConceptSetComparisonDetailsLeft(), which = 'databaseIds')
    orphanConceptDataMaxCount <-
      attr(x = getConceptSetComparisonDetailsLeft(), which = 'maxCount')
    
    if (nrow(result) == 0)
    {
      validate(need(nrow(result) > 0, "No data found"))
    } else
    {
      if (nrow(result) < 20)
      {
        scrollYHeight <- TRUE
      } else
      {
        scrollYHeight <- '25vh'
      }
      
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(
                                            tr(
                                              th(rowspan = 2, "Concept ID"),
                                              th(rowspan = 2, "Concept Name"),
                                              th(rowspan = 2, "Vocabulary ID"),
                                              th(rowspan = 2, "Concept Code"),
                                              lapply(
                                                orphanConceptDataDatabaseIds,
                                                th,
                                                colspan = 2,
                                                class = "dt-center"
                                              )
                                            ),
                                            tr(lapply(rep(
                                              c("Subjects", "Counts"),
                                              length(orphanConceptDataDatabaseIds)
                                            ), th))
                                          )))
      
      options = list(
        pageLength = 10,
        searching = TRUE,
        scrollX = TRUE,
        scrollY = '50vh',
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        columnDefs = list(truncateStringDef(1, 100),
                          minCellCountDef(3 + (
                            1:(length(orphanConceptDataDatabaseIds) * 2)
                          )))
      )
      
      table <- DT::datatable(
        result,
        options = options,
        colnames = colnames(result),
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      table <- DT::formatStyle(
        table = table,
        columns =  4 + (1:(
          length(orphanConceptDataDatabaseIds) * 2
        )),
        background = DT::styleColorBar(c(0, orphanConceptDataMaxCount), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      return(table)
    }
  })
  
  #______________----
  # Cohort Counts Tab -----
  ###getCohortCountDataForSelectedDatabaseIdsCohortIds----
  #used to display counts in cohort definition panels
  getCohortCountDataForSelectedDatabaseIdsCohortIds <-
    shiny::reactive(x = {
      if (all(is(dataSource, "environment"),!exists('cohortCount')))
      {
        return(NULL)
      }
      if (any(
        length(getDatabaseIdsFromDropdown()) == 0,
        length(getCohortIdsFromSelectedCompoundCohortNames()) == 0
      )) {
        return(NULL)
      }
      data <- cohortCount %>%
        dplyr::filter(.data$cohortId %in% getCohortIdsFromSelectedCompoundCohortNames()) %>%
        dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown()) %>%
        dplyr::inner_join(cohort %>%
                            dplyr::select(.data$cohortId, .data$shortName),
                          by = "cohortId") %>%
        dplyr::arrange(.data$shortName, .data$databaseId)
      if (any(is.null(data),
              nrow(data) == 0))
      {
        return(NULL)
      }
      return(data)
    })
  
  ##doesCohortCountTableHaveData----
  output$doesCohortCountTableHaveData <- shiny::reactive({
    data <- getCohortCountDataForSelectedDatabaseIdsCohortIds()
    if (any(
      is.null(getCohortCountDataForSelectedDatabaseIdsCohortIds()),
      nrow(getCohortCountDataForSelectedDatabaseIdsCohortIds())
    ))
    {
      return(FALSE)
    } else {
      return(TRUE)
    }
  })
  shiny::outputOptions(output,
                       "doesCohortCountTableHaveData",
                       suspendWhenHidden = FALSE)
  
  ##getCohortCountDataSubjectRecord----
  getCohortCountDataSubjectRecord <- shiny::reactive(x = {
    data <- getCohortCountDataForSelectedDatabaseIdsCohortIds()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    data <- data %>%
      dplyr::select(
        .data$databaseId,
        .data$shortName,
        .data$cohortSubjects,
        .data$cohortEntries,
        .data$cohortId
      ) %>%
      dplyr::rename(cohort = .data$shortName)
    
    data <- dplyr::full_join(
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
    
    data <- data %>%
      dplyr::select(order(colnames(data))) %>%
      dplyr::relocate(.data$cohort) %>%
      dplyr::arrange(.data$cohort)
    
    return(data)
  })
  
  ##getCohortCountDataSubject----
  getCohortCountDataSubject <- shiny::reactive(x = {
    data <- getCohortCountDataForSelectedDatabaseIdsCohortIds()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    data <- data %>%
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
    return(data)
  })
  
  ##getCohortCountDataRecord----
  getCohortCountDataRecord <- shiny::reactive(x = {
    data <- getCohortCountDataForSelectedDatabaseIdsCohortIds()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    data <- data %>%
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
    return(data)
  })
  
  #!!!!!!!!!! bug inclusion rule is not workin\
  #!!!!!!!!! no down load button
  #!!!!!!!!! no radio button for records/subjects
  ##output: cohortCountsTable----
  output$cohortCountsTable <- DT::renderDataTable(expr = {
    validate(need(
      length(getDatabaseIdsFromDropdown()) > 0,
      "No data sources chosen"
    ))
    validate(need(
      length(getCohortIdsFromSelectedCompoundCohortNames()) > 0,
      "No cohorts chosen"
    ))
    #!!! add error handling
    
    data <- getCohortCountDataForSelectedDatabaseIdsCohortIds()
    maxValueSubjects <- max(data$cohortSubjects)
    maxValueEntries <- max(data$cohortEntries)
    databaseIds <- sort(unique(data$databaseId))
    
    if (input$cohortCountsTableColumnFilter == "Both")
    {
      table <- getCohortCountDataSubjectRecord()
      #!!!!!!!! add a radio button to toggle cohort short name vs cohort full name
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(tr(
                                            th(rowspan = 2, "Cohort"),
                                            lapply(
                                              databaseIds,
                                              th,
                                              colspan = 2,
                                              class = "dt-center",
                                              style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                            )
                                          ),
                                          tr(
                                            lapply(rep(
                                              c("Records", "Subjects"), length(databaseIds)
                                            ),
                                            th,
                                            style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ))))
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
      for (i in 1:length(databaseIds))
      {
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
    } else
      if (input$cohortCountsTableColumnFilter == "Subjects Only" ||
          input$cohortCountsTableColumnFilter == "Records Only")
      {
        if (input$cohortCountsTableColumnFilter == "Subjects Only")
        {
          maxValue <- maxValueSubjects
          table <- getCohortCountDataSubject()
        } else
        {
          maxValue <- maxValueEntries
          table <- getCohortCountDataRecord()
        }
        
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
        
        #!!!!!!!! add a radio button to toggle cohort short name vs cohort full name
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
  
  ##output: saveCohortCountsTable----
  output$saveCohortCountsTable <-  downloadHandler(
    filename = function()
    {
      getCsvFileNameWithDateTime(string = "cohortCount")
    },
    content = function(file)
    {
      if (input$cohortCountsTableColumnFilter == "Both")
      {
        table <- getCohortCountDataSubjectRecord()
      }
      if (input$cohortCountsTableColumnFilter == "Subjects Only" ||
          input$cohortCountsTableColumnFilter == "Records Only")
      {
        if (input$cohortCountsTableColumnFilter == "Subjects Only")
        {
          table <- getCohortCountDataSubject()
        } else
        {
          table <- getCohortCountDataRecord()
        }
      }
      downloadCsv(x = table,
                  fileName = file)
    }
  )
  
  
  ##output: doesSelectedRowInCohortCountTableHaveCohortId----
  output$doesSelectedRowInCohortCountTableHaveCohortId <-
    reactive({
      return(!is.null(getCohortIdFromSelectedRowInCohortCountTable()))
    })
  outputOptions(output,
                "doesSelectedRowInCohortCountTableHaveCohortId",
                suspendWhenHidden = FALSE)
  
  ##output: inclusionRuleStatForCohortSeletedTable----
  output$inclusionRuleStatForCohortSeletedTable <-
    DT::renderDataTable(expr = {
      validate(need(
        length(getDatabaseIdsFromDropdown()) > 0,
        "No data sources chosen"
      ))
      validate(need(
        nrow(getCohortIdFromSelectedRowInCohortCountTable()) > 0,
        "No cohorts chosen"
      ))
      
      table <- getResultsInclusionRuleStatistics(
        dataSource = dataSource,
        cohortIds = getCohortIdFromSelectedRowInCohortCountTable()$cohortId,
        databaseIds = getDatabaseIdsFromDropdown()
      )
      
      validate(need((nrow(table) > 0),
                    "There is no inclusion rule data for this cohort."))
      
      databaseIds <- unique(table$databaseId)
      #!!!! move to seperate reactive function - to do the pivot step. reuse that for download button and here.
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
                                            ),
                                            th,
                                            style = "border-right:1px solid silver;border-bottom:1px solid silver")
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
  
  #______________----
  # Incidence rate -------
  ##reactive: getIncidenceRateData----
  getIncidenceRateData <- reactive({
    if (input$tabs == "incidenceRate")
    {
      if (all(is(dataSource, "environment"),!exists('incidenceRate')))
      {
        return(NULL)
      }
      if (any(length(getCohortIdsFromSelectedCompoundCohortNames()) == 0)) {
        return(NULL)
      }
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Getting incidence rate data."),
                   value = 0)
      
      data <- getResultsIncidenceRate(dataSource = dataSource,
                                      cohortIds =  getCohortIdsFromSelectedCompoundCohortNames())
      if (all(!is.null(data),
              nrow(data) > 0)) {
        data <- data %>%
          dplyr::mutate(
            incidenceRate = dplyr::case_when(.data$incidenceRate < 0 ~ 0,
                                             TRUE ~ .data$incidenceRate)
          )
      }
      return(data)
    } else
    {
      return(NULL)
    }
  })
  
  
  ##reactive: getFilteredIncidenceRateData----
  getFilteredIncidenceRateData <- reactive({
    if (any(
      length(getDatabaseIdsFromDropdown()) == 0,
      length(getCohortIdsFromSelectedCompoundCohortNames()) == 0
    )) {
      return(NULL)
    }
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    
    data <- getIncidenceRateData()
    if (any(is.null(data), nrow(data) == 0))
    {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown())
    if (any(is.null(data), nrow(data) == 0))
    {
      return(NULL)
    }
    
    if (stratifyByGender)
    {
      data <- data %>%
        dplyr::filter(.data$gender != '')
    }
    if (stratifyByAge)
    {
      data <- data %>%
        dplyr::filter(.data$ageGroup != '')
    }
    if (stratifyByCalendarYear)
    {
      data <- data %>%
        dplyr::filter(.data$calendarYear != '')
    }
    
    if (!is.na(input$minPersonYear) &&
        !is.null(input$minPersonYear))
    {
      data <- data %>%
        dplyr::filter(.data$personYears >= input$minPersonYear)
    }
    if (!is.na(input$minSubjetCount) &&
        !is.null(input$minSubjetCount))
    {
      data <- data %>%
        dplyr::filter(.data$cohortCount >= input$minSubjetCount)
    }
    if (any(is.null(data), nrow(data) == 0))
    {
      return(NULL)
    }
    return(data)
  })
  
  
  ##pickerInput - incidenceRateAgeFilter----
  shiny::observe({
    if (!is.null(getIncidenceRateData()) &&
        nrow(getIncidenceRateData()) > 0)
    {
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
  
  ##pickerInput - incidenceRateGenderFilter----
  shiny::observe({
    if (!is.null(getIncidenceRateData()) &&
        nrow(getIncidenceRateData()) > 0)
    {
      genderFilter <- getIncidenceRateData() %>%
        dplyr::select(.data$gender) %>%
        dplyr::filter(.data$gender != "NA", !is.na(.data$gender)) %>%
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
  
  ##pickerInput - incidenceRateCalendarFilter & YscaleMinAndMax----
  ##!!!why are two picker input in the same observe event - should it be seperate?
  shiny::observe({
    if (!is.null(getIncidenceRateData()) &&
        nrow(getIncidenceRateData()) > 0)
    {
      calendarFilter <- getIncidenceRateData() %>%
        dplyr::select(.data$calendarYear) %>%
        dplyr::filter(.data$calendarYear != "NA", !is.na(.data$calendarYear)) %>%
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
      
      minIncidenceRateValue <-
        round(min(getIncidenceRateData()$incidenceRate), digits = 2)
      
      maxIncidenceRateValue <-
        round(max(getIncidenceRateData()$incidenceRate), digits = 2)
      
      shiny::updateSliderInput(
        session = session,
        inputId = "YscaleMinAndMax",
        min = 0,
        max = maxIncidenceRateValue,
        value = c(minIncidenceRateValue, maxIncidenceRateValue),
        step = round((maxIncidenceRateValue - minIncidenceRateValue) / 5,
                     digits = 2
        )
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
  getIncidenceRateFilteredOnCalendarFilterValue <-
    shiny::reactive({
      calendarFilter <- getIncidenceRateData() %>%
        dplyr::select(.data$calendarYear) %>%
        dplyr::filter(.data$calendarYear != "NA", !is.na(.data$calendarYear)) %>%
        dplyr::distinct(.data$calendarYear) %>%
        dplyr::arrange(.data$calendarYear)
      calendarFilter <-
        calendarFilter[calendarFilter$calendarYear >= input$incidenceRateCalendarFilter[1] &
                         calendarFilter$calendarYear <= input$incidenceRateCalendarFilter[2], , drop = FALSE] %>%
        dplyr::pull(.data$calendarYear)
      return(calendarFilter)
    })
  
  ##reactive: getIncidenceRateFilteredOnYScaleFilterValue----
  getIncidenceRateFilteredOnYScaleFilterValue <-
    shiny::reactive({
      incidenceRateFilter <- getIncidenceRateData() %>%
        dplyr::select(.data$incidenceRate) %>%
        dplyr::filter(.data$incidenceRate != "NA", !is.na(.data$incidenceRate)) %>%
        dplyr::distinct(.data$incidenceRate) %>%
        dplyr::arrange(.data$incidenceRate)
      incidenceRateFilter <-
        incidenceRateFilter[incidenceRateFilter$incidenceRate >= input$YscaleMinAndMax[1] &
                              incidenceRateFilter$incidenceRate <= input$YscaleMinAndMax[2], ] %>%
        dplyr::pull(.data$incidenceRate)
      return(incidenceRateFilter)
    })
  
  ##output: saveIncidenceRateData----
  output$saveIncidenceRateData <-  downloadHandler(
    filename = function()
    {
      getCsvFileNameWithDateTime(string = "IncidenceRate")
    },
    content = function(file)
    {
      downloadCsv(x = getFilteredIncidenceRateData(),
                  fileName = file)
    }
  )
  
  ##output: incidenceRatePlot----
  #!!! put generate plot button to prevent reactive rendering of plot before user has finished selecting
  output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
    validate(need(
      length(getDatabaseIdsFromDropdown()) > 0,
      "No data sources chosen"
    ))
    validate(need(
      length(getCohortIdsFromSelectedCompoundCohortNames()) > 0,
      "No cohorts chosen"
    ))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Rendering incidence rate plot."),
                 value = 0)
    
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    shiny::withProgress(
      message = paste(
        "Building incidence rate plot data for ",
        length(getCohortIdsFromSelectedCompoundCohortNames()),
        " cohorts and ",
        length(getDatabaseIdsFromDropdown()),
        " databases"
      ),
      {
        data <- getFilteredIncidenceRateData()
        
        validate(need(
          all(!is.null(data), nrow(data) > 0),
          paste0("No data for this combination")
        ))
        
        if (stratifyByAge &&
            !"All" %in% incidenceRateAgeFilterValues())
        {
          data <- data %>%
            dplyr::filter(.data$ageGroup %in% incidenceRateAgeFilterValues())
        }
        if (stratifyByGender &&
            !"All" %in% incidenceRateGenderFilterValues())
        {
          data <- data %>%
            dplyr::filter(.data$gender %in% incidenceRateGenderFilterValues())
        }
        if (stratifyByCalendarYear)
        {
          data <- data %>%
            dplyr::filter(.data$calendarYear %in% getIncidenceRateFilteredOnCalendarFilterValue())
        }
        if (input$irYscaleFixed)
        {
          data <- data %>%
            dplyr::filter(.data$incidenceRate %in% getIncidenceRateFilteredOnYScaleFilterValue())
        }
        
        if (all(!is.null(data), nrow(data) > 0))
        {
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
      },
      detail = "Please Wait"
    )
  })
  
  #______________----
  # Time Series -----
  ##reactive: getFixedTimeSeriesTsibble ------
  getFixedTimeSeriesTsibble <- reactive({
    if (input$tabs == "timeSeries")
    {
      #!!!getCohortIdsFromSelectedCompoundCohortNames() is returning '' -why?
      if (any(length(getCohortIdsFromSelectedCompoundCohortNames()) == 0))
      {
        return(NULL)
      }
      if (all(is(dataSource, "environment"), !exists('timeSeries'))) {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Getting time series data."),
                   value = 0)
      
      data <- getResultsFixedTimeSeries(dataSource = dataSource,
                                        cohortIds =  getCohortIdsFromSelectedCompoundCohortNames())
      return(data)
    } else {
      return(NULL)
    }
  })
  
  ##reactive: getFixedTimeSeriesTsibbleFiltered----
  getFixedTimeSeriesTsibbleFiltered <- reactive({
    if (any(
      length(getDatabaseIdsFromDropdown()) == 0,
      length(getCohortIdsFromSelectedCompoundCohortNames()) == 0
    )) {
      return(NULL)
    }
    calendarIntervalFirstLetter <-
      tolower(substr(input$timeSeriesAggregationPeriodSelection, 1, 1))
    data <- getFixedTimeSeriesTsibble()
    if (is.null(data))
    {
      return(NULL)
    }
    
    data <- data[[calendarIntervalFirstLetter]]
    if (any(is.null(data),
            length(data) == 0))
    {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown())
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    if (calendarIntervalFirstLetter == 'y')
    {
      data <- data %>%
        dplyr::mutate(periodBeginRaw = as.Date(paste0(
          as.character(.data$periodBegin), '-01-01'
        )))
    } else
    {
      data <- data %>%
        dplyr::mutate(periodBeginRaw = as.Date(.data$periodBegin))
    }
    
    if (calendarIntervalFirstLetter == 'm')
    {
      data <- data %>%
        dplyr::mutate(periodEnd = clock::add_months(x = as.Date(.data$periodBeginRaw), n = 1) %>%
                        clock::add_days(n = -1))
    }
    if (calendarIntervalFirstLetter == 'q')
    {
      data <- data %>%
        dplyr::mutate(periodEnd = clock::add_quarters(x = as.Date(.data$periodBeginRaw), n = 1) %>%
                        clock::add_days(n = -1))
    }
    if (calendarIntervalFirstLetter == 'y')
    {
      data <- data %>%
        dplyr::mutate(periodEnd = clock::add_years(x = as.Date(.data$periodBeginRaw), n = 1) %>%
                        clock::add_days(n = -1))
    }
    
    data <- data %>%
      dplyr::relocate(
        .data$databaseId,
        .data$cohortId,
        .data$seriesType,
        .data$periodBegin,
        .data$periodEnd
      )
    
    ###!!! there is a bug here input$timeSeriesPeriodRangeFilter - min and max is returning 0
    if (any(
      input$timeSeriesPeriodRangeFilter[1] != 0,
      input$timeSeriesPeriodRangeFilter[2] != 0
    )) {
      data <-
        data[as.character(data$periodBegin) >= input$timeSeriesPeriodRangeFilter[1] &
               as.character(data$periodBegin) <= input$timeSeriesPeriodRangeFilter[2],]
    }
    if (any(is.null(data),
            nrow(data) == 0))
    {
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
    
    if (nrow(data) == 0)
    {
      return(NULL)
    }
    return(data)
  })
  
  ##reactive: getTimeSeriesDescription----
  getTimeSeriesDescription <- shiny::reactive({
    data <- getFixedTimeSeriesTsibble()
    if (any(is.null(data), nrow(data) == 0))
    {
      return(NULL)
    }
    calendarIntervalFirstLetter <-
      tolower(substr(input$timeSeriesAggregationPeriodSelection, 1, 1))
    
    data <- data[[calendarIntervalFirstLetter]]
    timeSeriesDescription <- attr(x = data,
                                  which = "timeSeriesDescription")
    return(timeSeriesDescription)
  })
  
  ##output: timeSeriesTypeLong----
  output$timeSeriesTypeLong <- shiny::renderUI({
    timeSeriesDescription <- getTimeSeriesDescription()
    if (any(is.null(timeSeriesDescription),
            nrow(timeSeriesDescription) == 0))
    {
      return(NULL)
    }
    seriesTypeLong <- timeSeriesDescription %>%
      dplyr::filter(.data$seriesTypeShort %in% input$timeSeriesTypeFilter) %>%
      dplyr::pull(.data$seriesTypeLong) %>%
      unique()
    return(seriesTypeLong)
  })
  
  ##pickerInput: timeSeriesTypeFilter (short)----
  shiny::observe({
    timeSeriesDescription <- getTimeSeriesDescription()
    if (any(is.null(timeSeriesDescription),
            nrow(timeSeriesDescription) == 0))
    {
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
  
  ##sliderInput: timeSeriesPeriodRangeFilter----
  shiny::observe({
    #!!! should this be conditional on timeSeries tab selection? Otherwise,
    #it is pulling time series data at start up
    calendarIntervalFirstLetter <-
      tolower(substr(input$timeSeriesAggregationPeriodSelection, 1, 1))
    data <- getFixedTimeSeriesTsibble()
    if (is.null(data))
    {
      return(NULL)
    }
    data <- data[[calendarIntervalFirstLetter]]
    if (any(is.null(data), nrow(data) == 0)) {
      return(NULL)
    }
    if (calendarIntervalFirstLetter == 'y') {
      minValue <- min(data$periodBegin) %>% as.integer()
      maxValue <- max(data$periodBegin) %>% as.integer()
    } else {
      minValue <-
        format(as.Date(data$periodBegin) %>% min(), "%Y") %>% as.integer()
      maxValue <-
        format(as.Date(data$periodBegin) %>% max(), "%Y") %>% as.integer()
    }
    
    shiny::updateSliderInput(
      session = session,
      inputId = "timeSeriesPeriodRangeFilter",
      min = minValue,
      max = maxValue,
      value = c(minValue, maxValue)
    )
  })
  
  ##reactive: getFixedTimeSeriesDataForTable----
  getFixedTimeSeriesDataForTable <- shiny::reactive({
    if (any(
      is.null(input$timeSeriesTypeFilter),
      length(input$timeSeriesTypeFilter) == 0,
      input$timeSeriesTypeFilter == ''
    )) {
      return(NULL)
    }
    timeSeriesDescription <- getTimeSeriesDescription()
    data <- getFixedTimeSeriesTsibbleFiltered()
    validate(need(all(!is.null(data),
                      nrow(data) > 0),
                  "No timeseries data for the cohort."))
    data <- data %>%
      dplyr::inner_join(
        timeSeriesDescription %>%
          dplyr::filter(.data$seriesTypeShort %in% input$timeSeriesTypeFilter) %>%
          dplyr::select(.data$seriesType),
        by = "seriesType"
      ) %>%
      dplyr::select(-.data$seriesType) %>%
      dplyr::mutate(periodBegin = .data$periodBeginRaw) %>%
      dplyr::relocate(.data$periodBegin, .data$periodEnd) %>%
      dplyr::arrange(.data$periodBegin) %>%
      dplyr::select(-.data$periodBeginRaw)
    return(data)
  })
  
  #!!!!!!!!!BUG missing download csv
  
  ##reactive: getFixedTimeSeriesDataForPlot----
  getFixedTimeSeriesDataForPlot <- shiny::reactive({
    if (any(
      is.null(input$timeSeriesTypeFilter),
      length(input$timeSeriesTypeFilter) == 0,
      input$timeSeriesTypeFilter == ''
    )) {
      return(NULL)
    }
    timeSeriesDescription <- getTimeSeriesDescription()
    data <- getFixedTimeSeriesTsibbleFiltered()
    validate(need(all(!is.null(data),
                      nrow(data) > 0),
                  "No timeseries data for the cohort."))
    data <- data %>%
      dplyr::inner_join(
        timeSeriesDescription %>%
          dplyr::filter(.data$seriesTypeShort %in% input$timeSeriesTypeFilter) %>%
          dplyr::select(.data$seriesType),
        by = "seriesType"
      ) %>%
      dplyr::select(
        .data$databaseId,
        .data$cohortId,
        .data$seriesType,
        .data$periodBegin,
        titleCaseToCamelCase(input$timeSeriesPlotFilters)
      ) %>%
      dplyr::rename(value = titleCaseToCamelCase(input$timeSeriesPlotFilters))
    data <- data %>%
      dplyr::left_join(
        cohort %>%
          dplyr::select(.data$shortName,
                        .data$cohortId) %>%
          dplyr::mutate(cohortShortName = .data$shortName),
        by = "cohortId"
      ) %>%
      dplyr::select(-.data$cohortId,-.data$shortName) %>%
      dplyr::relocate(.data$databaseId,
                      .data$cohortShortName,
                      .data$seriesType) %>%
      tsibble::as_tsibble(
        key = c(
          .data$databaseId,
          .data$cohortShortName,
          .data$seriesType
        ),
        index = .data$periodBegin
      ) %>%
      tsibble::fill_gaps(value = 0)
    return(data)
  })
  
  ##output: fixedTimeSeriesTable----
  ####!!! conditional on input$timeSeriesTypeFilter having a value?
  output$fixedTimeSeriesTable <- DT::renderDataTable({
    validate(need(
      all(
        !is.null(input$timeSeriesTypeFilter),
        length(input$timeSeriesTypeFilter) > 0,
        input$timeSeriesTypeFilter != ''
      ),
      "Please select time series type."
    ))
    
    data <- getFixedTimeSeriesDataForTable()
    validate(need(
      all(!is.null(data),
          nrow(data) > 0),
      "No timeseries data for the cohort of this series type"
    ))
    
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
  
  ##output: fixedTimeSeriesPlot----
  output$fixedTimeSeriesPlot <- ggiraph::renderggiraph({
    validate(need(
      all(
        !is.null(input$timeSeriesTypeFilter),
        length(input$timeSeriesTypeFilter) > 0,
        input$timeSeriesTypeFilter != ''
      ),
      "Please select time series type."
    ))
    data <- getFixedTimeSeriesDataForPlot()
    validate(need(
      all(!is.null(data),
          nrow(data) > 0),
      "No timeseries data for the cohort of this series type"
    ))
    plot <- plotTimeSeriesFromTsibble(
      tsibbleData = data,
      yAxisLabel = titleCaseToCamelCase(input$timeSeriesPlotFilters),
      indexAggregationType = input$timeSeriesAggregationPeriodSelection,
      timeSeriesStatistics = input$timeSeriesStatistics
    )
    plot <- ggiraph::girafe(
      ggobj = plot,
      options = list(
        ggiraph::opts_sizing(width = .5),
        ggiraph::opts_zoom(max = 5)
      )
    )
    return(plot)
  })
  
  #______________----
  #Time Distribution----
  ##output: getTimeDistributionData----
  getTimeDistributionData <- reactive({
    if (any(
      is.null(getDatabaseIdsFromDropdown()),
      length(getDatabaseIdsFromDropdown()) == 0
    )) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"),
            !exists('timeDistribution')))
    {
      return(NULL)
    }
    data <- getResultsTimeDistribution(
      dataSource = dataSource,
      cohortIds =  getCohortIdsFromSelectedCompoundCohortNames(),
      databaseIds = getDatabaseIdsFromDropdown()
    )
    return(data)
  })
  
  getTimeDistributionTableData <- reactive({
    data <- getTimeDistributionData()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    
    data <- data %>%
      dplyr::inner_join(cohort %>%
                          dplyr::select(.data$cohortId,
                                        .data$shortName),
                        by = "cohortId") %>%
      dplyr::arrange(.data$databaseId, .data$cohortId) %>%
      dplyr::mutate(# shortName = as.factor(.data$shortName),
        databaseId = as.factor(.data$databaseId)) %>%
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
  })
  
  ##output: saveTimeDistributionTable----
  output$saveTimeDistributionTable <-  downloadHandler(
    filename = function()
    {
      getCsvFileNameWithDateTime(string = "timeDistribution")
    },
    content = function(file)
    {
      downloadCsv(x = getTimeDistributionTableData(),
                  fileName = file)
    }
  )
  
  ##output: timeDistributionTable----
  output$timeDistributionTable <- DT::renderDataTable(expr = {
    data <- getTimeDistributionTableData()
    validate(need(
      all(!is.null(data), nrow(data) > 0),
      "No data available for selected combination."
    ))
    
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
    table <-
      DT::formatRound(table, c("Average", "SD"), digits = 2)
    table <-
      DT::formatRound(table,
                      c("Min", "P10", "P25", "Median", "P75", "P90", "Max"),
                      digits = 0)
    return(table)
  }, server = TRUE)
  
  ##output: timeDistributionPlot----
  output$timeDistributionPlot <- ggiraph::renderggiraph(expr = {
    validate(need(
      length(getDatabaseIdsFromDropdown()) > 0,
      "No data sources chosen"
    ))
    data <- getTimeDistributionData()
    validate(need(nrow(data) > 0, paste0("No data for this combination")))
    plot <-
      plotTimeDistribution(data = data, shortNameRef = cohort)
    return(plot)
  })
  
  #______________----
  # Index event breakdown ------
  ##getIndexEventBreakdownData----
  getIndexEventBreakdownData <- shiny::reactive(x = {
    if (any(length(getDatabaseIdsFromDropdown()) == 0))
    {
      return(NULL)
    }
    if (all(is(dataSource, "environment"),!exists('indexEventBreakdown'))) {
      return(NULL)
    }
    data <-
      getResultsIndexEventBreakdown(dataSource = dataSource,
                                    cohortIds = getCohortIdFromSelectedCompoundCohortName())
    return(data)
  })
  
  ##pickerInput: domainTableOptionsInIndexEventData----
  shiny::observe({
    if (all(!is.null(getIndexEventBreakdownData()),
            nrow(getIndexEventBreakdownData()) > 0))
    {
      data <- getIndexEventBreakdownData() %>%
        dplyr::rename("domainTableShort" = .data$domainTable) %>%
        dplyr::inner_join(
          getOmopDomainInformationLong() %>%
            dplyr::select(
              .data$domainTableShort,
              .data$domainTable,
              .data$eraTable
            ),
          by = "domainTableShort"
        ) %>%
        dplyr::arrange(.data$domainTable,
                       .data$domainField)
      choices <- data %>%
        dplyr::select(.data$domainTable) %>%
        dplyr::distinct() %>%
        dplyr::pull() %>%
        snakeCaseToCamelCase() %>%
        camelCaseToTitleCase()
      choicesSelected <- data %>%
        dplyr::filter(.data$eraTable == FALSE) %>%
        dplyr::select(.data$domainTable) %>%
        dplyr::distinct() %>%
        dplyr::pull() %>%
        snakeCaseToCamelCase() %>%
        camelCaseToTitleCase()
    } else
    {
      return(NULL)
    }
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "domainTableOptionsInIndexEventData",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = choices,
      selected = choicesSelected
    )
  })
  
  ##pickerInput: domainFieldOptionsInIndexEventData----
  shiny::observe({
    if (all(!is.null(getIndexEventBreakdownData()),
            nrow(getIndexEventBreakdownData()) > 0))
    {
      data <- getIndexEventBreakdownData() %>%
        dplyr::rename("domainFieldShort" = .data$domainField) %>%
        dplyr::inner_join(
          getOmopDomainInformationLong() %>%
            dplyr::select(
              .data$domainFieldShort,
              .data$domainField,
              .data$eraTable
            ),
          by = "domainFieldShort"
        )
      choices <- data %>%
        dplyr::select(.data$domainField) %>%
        dplyr::distinct() %>%
        dplyr::pull() %>%
        snakeCaseToCamelCase() %>%
        camelCaseToTitleCase()
      choicesSelected <- data %>%
        dplyr::filter(.data$eraTable == FALSE) %>%
        dplyr::select(.data$domainField) %>%
        dplyr::distinct() %>%
        dplyr::pull() %>%
        snakeCaseToCamelCase() %>%
        camelCaseToTitleCase()
    } else
    {
      return(NULL)
    }
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "domainFieldOptionsInIndexEventData",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = choices,
      selected = choicesSelected
    )
  })
  
  ##getIndexEventBreakdownDataEnhanced----
  getIndexEventBreakdownDataEnhanced <- shiny::reactive(x = {
    indexEventBreakdown <- getIndexEventBreakdownData()
    if (any(is.null(indexEventBreakdown),
            nrow(indexEventBreakdown) == 0))
    {
      return(NULL)
    }
    if (is.null(cohortCount)) {
      return(NULL)
    }
    conceptIdDetails <- getConcept(dataSource = dataSource,
                                   conceptIds = indexEventBreakdown$conceptId %>%
                                     unique())
    if (is.null(conceptIdDetails)) {
      return(NULL)
    }
    indexEventBreakdown <- indexEventBreakdown %>%
      dplyr::inner_join(
        conceptIdDetails %>%
          dplyr::select(
            .data$conceptId,
            .data$conceptName,
            .data$domainId,
            .data$vocabularyId,
            .data$standardConcept
          ),
        by = c("conceptId")
      )
    
    indexEventBreakdown <- indexEventBreakdown %>%
      dplyr::inner_join(cohortCount,
                        by = c('databaseId', 'cohortId')) %>%
      dplyr::mutate(
        subjectPercent = .data$subjectCount / .data$cohortSubjects,
        conceptPercent = .data$conceptCount / .data$cohortEntries
      ) %>%
      dplyr::rename(
        domainFieldShort = .data$domainField,
        domainTableShort = .data$domainTable
      ) %>%
      dplyr::inner_join(getOmopDomainInformationLong(),
                        by = c('domainTableShort',
                               'domainFieldShort')) %>%
      dplyr::select(-.data$domainTableShort, -.data$domainFieldShort)
    return(indexEventBreakdown)
  })
  
  ##getIndexEventBreakdownDataFiltered----
  getIndexEventBreakdownDataFiltered <- shiny::reactive(x = {
    indexEventBreakdown <- getIndexEventBreakdownDataEnhanced()
    if (any(is.null(indexEventBreakdown),
            nrow(indexEventBreakdown) == 0))
    {
      return(NULL)
    }
    if (is.null(input$domainTableOptionsInIndexEventData)) {
      return(NULL)
    }
    if (is.null(input$domainFieldOptionsInIndexEventData)) {
      return(NULL)
    }
    
    if (all(
      !is.null(input$conceptSetsSelectedFromOneCohort),
      length(input$conceptSetsSelectedFromOneCohort) > 0
    )) {
      indexEventBreakdown <- indexEventBreakdown %>%
        dplyr::inner_join(
          getResolvedConceptIdsForCohortFilteredBySelectedConceptSets(),
          by = c("cohortId", "conceptId", "databaseId")
        )
    }
    
    domainTableSelected <- getOmopDomainInformationLong() %>%
      dplyr::filter(
        .data$domainTable %in% c(
          input$domainTableOptionsInIndexEventData %>%
            titleCaseToCamelCase() %>%
            camelCaseToSnakeCase()
        )
      ) %>%
      dplyr::pull(.data$domainTable) %>%
      unique() %>%
      sort()
    if (any(is.null(domainTableSelected),
            length(domainTableSelected) == 0))
    {
      return(NULL)
    }
    
    domainFieldSelected <- getOmopDomainInformationLong() %>%
      dplyr::filter(
        .data$domainField %in% c(
          input$domainFieldOptionsInIndexEventData %>%
            titleCaseToCamelCase() %>%
            camelCaseToSnakeCase()
        )
      ) %>%
      dplyr::pull(.data$domainField) %>%
      unique() %>%
      sort()
    if (any(is.null(domainFieldSelected),
            length(domainFieldSelected) == 0))
    {
      return(NULL)
    }
    
    indexEventBreakdown <- indexEventBreakdown %>%
      dplyr::filter(.data$domainTable %in% domainTableSelected) %>%
      dplyr::filter(.data$domainField %in% domainFieldSelected)
    
    if (input$indexEventBreakdownTableRadioButton == 'All')
    {
      return(indexEventBreakdown)
    } else
      if (input$indexEventBreakdownTableRadioButton == "Standard concepts")
      {
        return(indexEventBreakdown %>% dplyr::filter(.data$standardConcept == 'S'))
      } else
      {
        return(indexEventBreakdown %>% dplyr::filter(is.na(.data$standardConcept)))
      }
    return(indexEventBreakdown)
  })
  
  ##getIndexEventBreakdownDataLong----
  getIndexEventBreakdownDataLong <- shiny::reactive(x = {
    data <- getIndexEventBreakdownDataFiltered()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    if (input$indexEventBreakdownValueFilter == "Percentage") {
      data <- data %>%
        dplyr::mutate(conceptValue = .data$conceptPercent) %>%
        dplyr::mutate(subjectValue = .data$subjectPercent)
    } else {
      data <- data %>%
        dplyr::mutate(conceptValue = .data$conceptCount) %>%
        dplyr::mutate(subjectValue = .data$subjectCount)
    }
    data <- data %>%
      dplyr::filter(.data$conceptId > 0) %>%
      dplyr::arrange(.data$databaseId) %>%
      dplyr::select(
        .data$databaseId,
        .data$cohortId,
        .data$conceptId,
        .data$conceptName,
        .data$domainTable,
        .data$domainField,
        .data$vocabularyId,
        .data$conceptValue,
        .data$subjectValue
      ) %>%
      dplyr::group_by(
        .data$databaseId,
        .data$cohortId,
        .data$conceptId,
        .data$conceptName,
        .data$domainTable,
        .data$domainField,
        .data$vocabularyId
      ) %>%
      dplyr::summarise(
        conceptValue = sum(.data$conceptValue),
        subjectValue = max(.data$subjectValue),
        .groups = 'keep'
      ) %>%
      dplyr::ungroup() %>%
      dplyr::distinct() %>% # distinct is needed here because many time condition_concept_id and condition_source_concept_id
      # may have the same value leading to duplication of row records
      tidyr::pivot_longer(
        names_to = "type",
        cols = c("conceptValue", "subjectValue"),
        values_to = "count"
      ) %>%
      dplyr::arrange(.data$databaseId, .data$cohortId, .data$type)
    return(data)
  })
  
  ##getIndexEventBreakdownDataWide----
  getIndexEventBreakdownDataWide <- shiny::reactive(x = {
    data <- getIndexEventBreakdownDataLong()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    data <- data %>%
      dplyr::mutate(type = paste0(.data$databaseId,
                                  " ",
                                  .data$type)) %>%
      dplyr::group_by(
        .data$databaseId,
        .data$cohortId,
        .data$conceptId,
        .data$conceptName,
        .data$vocabularyId,
        .data$domainTable,
        .data$domainField,
        .data$type
      ) %>%
      dplyr::summarise(
        conceptValue = sum(.data$conceptValue),
        subjectValue = max(.data$subjectValue),
        .groups = 'keep'
      ) %>%
      dplyr::ungroup() %>%
      dplyr::distinct() %>%
      tidyr::pivot_wider(
        id_cols = c(
          "cohortId",
          "conceptId",
          "conceptName",
          "vocabularyId",
          "domainTable",
          "domainField"
        ),
        names_from = type,
        values_from = count,
        values_fill = 0
      ) %>%
      dplyr::distinct()
    data <- data[order(-data[6]),]
    return(data)
  })
  
  ##output: saveBreakdownTable----
  output$saveBreakdownTable <-  downloadHandler(
    filename = function()
    {
      getCsvFileNameWithDateTime(string = "indexEventBreakdown")
    },
    content = function(file)
    {
      downloadCsv(x = getIndexEventBreakdownDataWide(),
                  fileName = file)
    }
  )
  
  ##getIndexEventBreakdownDataTable----
  getIndexEventBreakdownDataTable <- shiny::reactive(x = {
    data <- getIndexEventBreakdownDataLong()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    cohortAndPersonCount <- data %>%
      dplyr::select(.data$cohortId,
                    .data$databaseId) %>%
      dplyr::distinct() %>%
      dplyr::inner_join(cohortCount,
                        by = c('databaseId',
                               'cohortId')) %>%
      dplyr::distinct() %>%
      dplyr::mutate(cohortSubjects = scales::comma(.data$cohortSubjects,
                                                   accuracy = 1)) %>%
      dplyr::mutate(cohortEntries = scales::comma(.data$cohortEntries,
                                                  accuracy = 1))
    
    data <- data %>%
      dplyr::inner_join(cohortAndPersonCount,
                        by = c('cohortId',
                               'databaseId'))
    
    if (input$indexEventBreakdownTableFilter == "Records") {
      data <- data %>%
        dplyr::mutate(cohortCountValue = .data$cohortEntries)
    }
    if (input$indexEventBreakdownTableFilter == "Persons") {
      data <- data %>%
        dplyr::mutate(cohortCountValue = .data$cohortSubjects)
    }
    
    data <- data %>%
      dplyr::select(-.data$cohortEntries, -.data$cohortSubjects) %>%
      dplyr::mutate(type = paste0(
        .data$databaseId,
        " (",
        .data$cohortCountValue,
        ") ",
        .data$type
      )) %>%
      tidyr::pivot_wider(
        id_cols = c(
          "cohortId",
          "conceptId",
          "conceptName",
          "vocabularyId",
          "domainTable",
          "domainField"
        ),
        names_from = type,
        values_from = count,
        values_fill = 0
      ) %>%
      dplyr::distinct()
    data <- data[order(-data[7]),]
    return(data)
  })
  
  ##output: indexEventBreakdownTable----
  output$indexEventBreakdownTable <-
    DT::renderDataTable(expr = {
      validate(need(
        length(getDatabaseIdsFromDropdown()) > 0,
        "No data sources chosen"
      ))
      validate(need(
        length(getCohortIdFromSelectedCompoundCohortName()) > 0,
        "No cohorts chosen chosen"
      ))
      
      indexEventBreakdownDataTable <-
        getIndexEventBreakdownDataTable()
      validate(need(
        all(
          !is.null(indexEventBreakdownDataTable),
          nrow(indexEventBreakdownDataTable) > 0
        ),
        "No index event breakdown data for the chosen combination."
      ))
      data <- indexEventBreakdownDataTable %>%
        dplyr::select(-.data$cohortId)
      maxCount <-
        max(indexEventBreakdownDataTable[7], na.rm = TRUE)
      databaseIds <- input$selectedDatabaseIds
      
      noOfMergeColumns <- 1
      if (input$indexEventBreakdownTableFilter == "Records")
      {
        data <- data %>%
          dplyr::select(-dplyr::contains("subjectValue"))
        colnames(data) <-
          stringr::str_replace(
            string = colnames(data),
            pattern = 'conceptValue',
            replacement = ''
          )
        columnColor <- 4 + 1:(length(databaseIds))
      } else
        if (input$indexEventBreakdownTableFilter == "Persons")
        {
          data <- data %>%
            dplyr::select(-dplyr::contains("conceptValue"))
          colnames(data) <-
            stringr::str_replace(
              string = colnames(data),
              pattern = 'subjectValue',
              replacement = ''
            )
          columnColor <- 4 + 1:(length(databaseIds))
        } else
        {
          recordAndPersonColumnName <- c()
          for (i in 1:length(getDatabaseIdsFromDropdown()))
          {
            recordAndPersonColumnName <-
              c(
                recordAndPersonColumnName,
                paste0("Records (", recordCount[i], ")"),
                paste0("Person (", personCount[i], ")")
              )
          }
          sketch <- htmltools::withTags(table(class = "display",
                                              thead(
                                                tr(
                                                  th(rowspan = 2, "Concept Id"),
                                                  th(rowspan = 2, "Concept Name"),
                                                  th(rowspan = 2, "Domain field"),
                                                  th(rowspan = 2, "Vocabulary Id"),
                                                  lapply(
                                                    databaseIds,
                                                    th,
                                                    colspan = 2,
                                                    class = "dt-center",
                                                    style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                                  )
                                                ),
                                                tr(
                                                  lapply(recordAndPersonColumnName, th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                                )
                                              )))
          columnColor <- 4 + 1:(length(databaseIds) * 2)
          noOfMergeColumns <- 2
        }
      
      if (input$indexEventBreakdownValueFilter == "Percentage")
      {
        minimumCellPercent <-
          minCellPercentDef(3 + 1:(length(databaseIds) * noOfMergeColumns))
      } else
      {
        minimumCellPercent <-
          minCellCountDef(3 + 1:(length(databaseIds) * noOfMergeColumns))
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
      
      if (input$indexEventBreakdownTableFilter == "Both")
      {
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
      } else
      {
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
  
  ## IndexEventBreakdown Consolidated reactive val update----
  observeEvent(
    eventExpr = input$indexEventBreakdownTable_rows_selected,
    handlerExpr = {
      idx <-
        input$indexEventBreakdownTable_rows_selected
      selectedConceptId <-
        getIndexEventBreakdownDataTable()$conceptId[idx]
      selectedConceptSetId <- conceptSets %>%
        dplyr::filter(.data$conceptSetName %in% input$conceptSetsSelectedFromOneCohort) %>%
        dplyr::pull(.data$conceptSetId) %>%
        unique()
      selectedDatabaseId <-
        input$selectedDatabaseIds
      #Extracting cohortId from selectedCompoundCohortName
      selectedCohortId <-
        gsub("\\(|\\)",
             "",
             substring(
               input$selectedCompoundCohortName,
               regexpr("[(][0-9]+[)]{1}", input$selectedCompoundCohortName)
             ))
      if (all(
        !doesObjectHaveData(selectedConceptId),
        !doesObjectHaveData(selectedConceptSetId),
        !doesObjectHaveData(selectedDatabaseId),
        !doesObjectHaveData(selectedCohortId)
      )) {
        consolidatedSelectedFieldValue(
          list(
            cohortId = selectedCohortId,
            conceptSetId = selectedConceptSetId,
            databaseId = selectedDatabaseId,
            conceptId = selectedConceptId
          )
        )
      } else
      {
        consolidatedSelectedFieldValue(list())
      }
      
    }
  )
  
  #______________----
  # Visit Context -----
  ##getVisitContextData----
  getVisitContextData <- shiny::reactive(x = {
    if (any(
      is.null(getDatabaseIdsFromDropdown()),
      length(getDatabaseIdsFromDropdown()) == 0
    )) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"), !exists('visitContext')))
    {
      return(NULL)
    }
    visitContext <-
      getResultsVisitContext(dataSource = dataSource,
                             cohortIds = getCohortIdFromSelectedCompoundCohortName())
    if (any(is.null(visitContext),
            nrow(visitContext) == 0))
    {
      return(NULL)
    }
    
    # to ensure backward compatibility to 2.1 when visitContext did not have visitConceptName
    if (!'visitConceptName' %in% colnames(visitContext))
    {
      concepts <- getConcept(dataSource = dataSource,
                             conceptIds = visitContext$visitConceptId %>% unique()) %>%
        dplyr::rename(
          visitConceptId = .data$conceptId,
          visitConceptName = .data$conceptName
        ) %>%
        dplyr::filter(is.na(.data$invalidReason)) %>%
        dplyr::select(.data$visitConceptId, .data$visitConceptName)
      
      visitContext <- visitContext %>%
        dplyr::left_join(concepts,
                         by = c('visitConceptId'))
    }
    return(visitContext)
  })
  
  ##getVisitContexDataEnhanced----
  getVisitContexDataEnhanced <- shiny::reactive(x = {
    if (any(
      is.null(getDatabaseIdsFromDropdown()),
      length(getDatabaseIdsFromDropdown()) == 0
    )) {
      return(NULL)
    }
    visitContextData <- getVisitContextData()
    if (any(is.null(visitContextData),
            nrow(visitContextData) == 0))
    {
      return(NULL)
    }
    if (is.null(cohortCount))
    {
      return(NULL)
    }
    visitContextData <- visitContextData %>%
      dplyr::inner_join(cohortCount,
                        by = c('databaseId', 'cohortId'))
    if (input$visitContextValueFilter == "Percentage")
    {
      visitContextData <- visitContextData %>%
        dplyr::mutate(subjectsValue = .data$subjects / .data$cohortSubjects) %>%
        dplyr::mutate(recordsValue = .data$records / .data$cohortEntries)
    } else
    {
      visitContextData <- visitContextData %>%
        dplyr::mutate(subjectsValue = .data$subjects) %>%
        dplyr::mutate(recordsValue = .data$records)
    }
    visitContextData <- visitContextData %>%
      dplyr::select(-.data$subjects,
                    -.data$records,
                    -.data$cohortSubjects,
                    -.data$cohortEntries) %>%
      dplyr::rename(subjects = .data$subjectsValue,
                    records = .data$recordsValue)
    visitContextReference <-
      expand.grid(
        visitContext = c("Before", "During visit", "On visit start", "After"),
        visitConceptName = unique(visitContextData$visitConceptName),
        databaseId = unique(visitContextData$databaseId),
        cohortId = unique(visitContextData$cohortId)
      ) %>%
      dplyr::tibble()
    
    visitContextReference <- visitContextReference %>%
      dplyr::left_join(
        visitContextData,
        by = c(
          "visitConceptName",
          "visitContext",
          "databaseId",
          "cohortId"
        )
      ) %>%
      dplyr::select(
        .data$databaseId,
        .data$cohortId,
        .data$visitConceptName,
        .data$visitContext,
        .data$subjects,
        .data$records
      )
    # dplyr::mutate(visitContext = paste0(.data$databaseId, "_", .data$visitContext))
    return(visitContextReference)
  })
  
  ##getVisitContexDataFiltered----
  getVisitContexDataFiltered <- shiny::reactive(x = {
    if (any(
      is.null(getDatabaseIdsFromDropdown()),
      length(getDatabaseIdsFromDropdown()) == 0
    )) {
      return(NULL)
    }
    data <- getVisitContexDataEnhanced()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$cohortId == getCohortIdFromSelectedCompoundCohortName()) %>%
      dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown())
    return(data)
  })
  
  ##getVisitContextDataLong----
  getVisitContextDataLong <- shiny::reactive(x = {
    data <- getVisitContexDataFiltered()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    data <- data %>%
      tidyr::pivot_longer(
        names_to = "type",
        cols = c("records", "subjects"),
        values_to = "count"
      )
    data <- tidyr::replace_na(data,
                              replace = list("count" = 0))
    return(data)
  })
  
  ##getVisitContextDataWide----
  getVisitContextDataWide <- shiny::reactive(x = {
    if (any(
      is.null(getDatabaseIdsFromDropdown()),
      length(getDatabaseIdsFromDropdown()) == 0
    )) {
      return(NULL)
    }
    data <- getVisitContextDataLong()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    data <- data %>%
      dplyr::arrange(.data$databaseId,
                     .data$visitContext,
                     .data$type) %>%
      dplyr::mutate(type = paste0(.data$databaseId,
                                  " ",
                                  .data$visitContext,
                                  " ",
                                  .data$type)) %>%
      tidyr::pivot_wider(
        id_cols = c("cohortId",
                    "visitConceptName"),
        names_from = type,
        values_from = count,
        values_fill = 0
      ) %>%
      dplyr::distinct()
    data <- data[order(-data[3]),]
    return(data)
  })
  
  ##getVisitContextTableData----
  getVisitContextTableData <- shiny::reactive(x = {
    if (any(
      is.null(getDatabaseIdsFromDropdown()),
      length(getDatabaseIdsFromDropdown()) == 0
    )) {
      return(NULL)
    }
    data <- getVisitContextDataWide()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    isPerson <- input$visitContextPersonOrRecords == 'Person'
    if (isPerson)
    {
      data <- data %>%
        dplyr::select(-dplyr::contains(" records"))
      colnames(data) <- colnames(data) %>%
        stringr::str_replace_all(pattern = " subjects",
                                 replacement = "")
    } else
    {
      data <- data %>%
        dplyr::select(-dplyr::contains(" subjects"))
      colnames(data) <- colnames(data) %>%
        stringr::str_replace_all(pattern = " records",
                                 replacement = "")
    }
    
    if (input$visitContextTableFilters == "Before")
    {
      data <- data %>%
        dplyr::select(
          -dplyr::contains("During"),-dplyr::contains("On visit"),-dplyr::contains("After")
        )
      colnames(data) <-
        stringr::str_replace(
          string = colnames(data),
          pattern = ' Before',
          replacement = ''
        )
      
    } else
      if (input$visitContextTableFilters == "During")
      {
        data <- data %>%
          dplyr::select(
            -dplyr::contains("Before"),-dplyr::contains("On visit"),-dplyr::contains("After")
          )
        colnames(data) <-
          stringr::str_replace(
            string = colnames(data),
            pattern = ' During visit',
            replacement = ''
          )
        
      } else
        if (input$visitContextTableFilters == "Simultaneous")
        {
          data <- data %>%
            dplyr::select(
              -dplyr::contains("During"),-dplyr::contains("Before"),-dplyr::contains("After")
            )
          colnames(data) <-
            stringr::str_replace(
              string = colnames(data),
              pattern = ' On visit start',
              replacement = ''
            )
          
        } else
          if (input$visitContextTableFilters == "After")
          {
            data <- data %>%
              dplyr::select(
                -dplyr::contains("During"),-dplyr::contains("Before"),-dplyr::contains("On visit")
              )
            colnames(data) <-
              stringr::str_replace(
                string = colnames(data),
                pattern = ' After',
                replacement = ''
              )
          }
    return(data)
  })
  
  ##saveVisitContextTable----
  output$saveVisitContextTable <-  downloadHandler(
    filename = function()
    {
      getCsvFileNameWithDateTime(string = "visitContext")
    },
    content = function(file)
    {
      downloadCsv(x = getVisitContextTableData(),
                  fileName = file)
    }
  )
  
  ##doesVisitContextContainData----
  output$doesVisitContextContainData <- shiny::reactive({
    return(nrow(getVisitContextTableData()) > 0)
  })
  shiny::outputOptions(output,
                       "doesVisitContextContainData",
                       suspendWhenHidden = FALSE)
  
  ##visitContextTable----
  output$visitContextTable <- DT::renderDataTable(expr = {
    validate(need(
      length(getDatabaseIdsFromDropdown()) > 0,
      "No data sources chosen"
    ))
    validate(need(
      length(getCohortIdFromSelectedCompoundCohortName()) > 0,
      "No cohorts chosen"
    ))
    data <- getVisitContextTableData()
    validate(need(
      all(!is.null(data),
          nrow(data) > 0),
      "No data available for selected combination."
    ))
    table <- data %>%
      dplyr::select(-.data$cohortId)
    
    # header labels
    cohortCounts <- cohortCount %>%
      dplyr::filter(.data$cohortId == getCohortIdFromSelectedCompoundCohortName()) %>%
      dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown()) %>%
      dplyr::arrange(.data$cohortId, .data$databaseId)
    isPerson <- input$visitContextPersonOrRecords == 'Person'
    if (isPerson)
    {
      databaseIdsWithCount <- cohortCounts %>%
        dplyr::mutate(databaseIdWithCount = paste0(
          .data$databaseId,
          " (n = ",
          scales::comma(.data$cohortSubjects),
          ")"
        )) %>%
        dplyr::pull(.data$databaseIdWithCount)
    } else
    {
      databaseIdsWithCount <- cohortCounts %>%
        dplyr::mutate(databaseIdWithCount = paste0(
          .data$databaseId,
          " (n = ",
          scales::comma(.data$cohortEntries),
          ")"
        )) %>%
        dplyr::pull(.data$databaseIdWithCount)
    }
    
    #max count
    visitConceptLong <- getVisitContextDataLong()
    if (any(is.null(visitConceptLong),
            nrow(visitConceptLong) == 0))
    {
      return(NULL)
    }
    maxSubjects <- visitConceptLong$count %>% max()
    visitContextSequence <- visitConceptLong$visitContext %>%
      unique()
    #ensure columns names are aligned
    for (i in (length(visitContextSequence):1))
    {
      table <- table %>%
        dplyr::relocate(.data$visitConceptName,
                        dplyr::contains(visitContextSequence[[i]]))
    }
    visitContextSequence <- visitContextSequence %>%
      stringr::str_replace(pattern = "Before",
                           replacement = "Visits Before") %>%
      stringr::str_replace(pattern = "During visit",
                           replacement = "Visits Ongoing") %>%
      stringr::str_replace(pattern = "On visit start",
                           replacement = "Starting Simultaneous") %>%
      stringr::str_replace(pattern = "After",
                           replacement = "Visits After")
    
    totalColumns <- 1
    if (input$visitContextTableFilters == "All")
    {
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(tr(
                                            th(rowspan = 2, "Visit"),
                                            lapply(
                                              databaseIdsWithCount,
                                              th,
                                              colspan = 4,
                                              class = "dt-center",
                                              style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                            )
                                          ),
                                          tr(
                                            lapply(rep(
                                              c(visitContextSequence), #avoid hard coding sequence
                                              length(databaseIdsWithCount)
                                            ),
                                            th,
                                            style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                          ))))
      totalColumns <- 4
    }
    columnDefs <-
      minCellCountDef(1:(length(databaseIdsWithCount) * totalColumns))
    
    if (input$visitContextValueFilter == "Percentage")
    {
      columnDefs <-
        minCellPercentDef(1:(length(databaseIdsWithCount) * totalColumns))
    }
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
      searching = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      scrollY = "60vh",
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      columnDefs = list(
        truncateStringDef(0, 60),
        list(width = "40%", targets = 0),
        columnDefs
      )
    )
    
    if (input$visitContextTableFilters == "All")
    {
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
    } else
    {
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
      columns = 1 + 1:(length(databaseIdsWithCount) * 4),
      background = DT::styleColorBar(c(0, maxSubjects), "lightblue"),
      backgroundSize = "98% 88%",
      backgroundRepeat = "no-repeat",
      backgroundPosition = "center"
    )
  }, server = TRUE)
  
  
  #______________----
  # Cohort Overlap ------
  ##getCohortOverlapData----
  getCohortOverlapData <- reactive({
    if (any(
      length(getDatabaseIdsFromDropdown()) == 0,
      length(getCohortIdsFromSelectedCompoundCohortNames()) == 0
    )) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"),
            !exists('cohortRelationships')))
    {
      return(NULL)
    }
    data <- getCohortOverlap(dataSource = dataSource,
                             cohortIds = getCohortIdsFromSelectedCompoundCohortNames())
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    return(data)
  })
  
  ##getCohortOverlapDataFiltered----
  getCohortOverlapDataFiltered <- reactive(x = {
    data <- getCohortOverlapData()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown())
    return(data)
  })
  
  ##output: overlapPlot----
  output$overlapPlot <- ggiraph::renderggiraph(expr = {
    validate(need(
      length(getCohortIdsFromSelectedCompoundCohortNames()) > 0,
      paste0("Please select Target Cohort(s)")
    ))
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Plotting cohort overlap."),
                 value = 0)
    
    data <- getCohortOverlapDataFiltered()
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
  
  ##output: saveCohortOverlapTable----
  output$saveCohortOverlapTable <-  downloadHandler(
    filename = function()
    {
      getCsvFileNameWithDateTime(string = "cohortOverlap")
    },
    content = function(file)
    {
      downloadCsv(x = getCohortOverlapDataFiltered(),
                  fileName = file)
    }
  )
  
  #______________----
  # Characterization/Temporal Characterization ------
  ## Shared----
  ###getConceptSetNamesFromOneCohort----
  getConceptSetNamesFromOneCohort <- shiny::reactive(x = {
    if (any(
      length(getCohortIdFromSelectedCompoundCohortName()) == 0,
      length(getDatabaseIdsFromDropdown()) == 0
    )) {
      return(NULL)
    }
    
    jsonExpression <- getCohortSortedByCohortId() %>%
      dplyr::filter(.data$cohortId == getCohortIdFromSelectedCompoundCohortName()) %>%
      dplyr::select(.data$json)
    
    jsonExpression <-
      RJSONIO::fromJSON(jsonExpression$json, digits = 23)
    expression <-
      getConceptSetDetailsFromCohortDefinition(cohortDefinitionExpression = jsonExpression)
    
    if (!is.null(expression))
    {
      expression <- expression$conceptSetExpression %>%
        dplyr::select(.data$name) %>%
        dplyr::distinct() %>%
        dplyr::arrange(.data$name)
      return(expression)
    } else
    {
      return(NULL)
    }
  })
  
  ###getCharacterizationDomainNameOptions----
  getCharacterizationDomainNameOptions <- shiny::reactive({
    return(input$characterizationDomainNameOptions)
  })
  
  ###getCharacterizationAnalysisNameOptions----
  getCharacterizationAnalysisNameOptions <- shiny::reactive({
    return(input$characterizationAnalysisNameOptions)
  })
  
  ###Update: characterizationAnalysisNameOptions----
  shiny::observe({
    data <- getCharacterizationTableData()
    if (any(is.null(data),
            nrow(data$analysisName) == 0))
    {
      return(NULL)
    }
    subset <-
      data$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "characterizationAnalysisNameOptions",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  ###Update: characterizationDomainNameOptions----
  shiny::observe({
    data <- getCharacterizationTableData()
    if (all(!is.null(data),
            nrow(data$domainId) > 0))
    {
      subset <-
        data$domainId %>% unique() %>% sort()
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "characterizationDomainNameOptions",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = subset
      )
    }
  })
  
  ### getTemporalCharacterizationAnalysisNameOptions----
  getTemporalCharacterizationAnalysisNameOptions <-
    shiny::reactive(x = {
      return(input$temporalCharacterizationAnalysisNameOptions)
    })
  ### getTemporalCharacterizationDomainNameOptions----
  getTemporalCharacterizationDomainNameOptions <-
    shiny::reactive(x = {
      return(input$temporalCharacterizationDomainNameOptions)
    })
  
  ###Update: temporalCharacterizationAnalysisNameOptions----
  shiny::observe({
    subset <-
      getTemporalCharacterizationData()$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalCharacterizationAnalysisNameOptions",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  ###Update: temporalCharacterizationDomainNameOptions----
  shiny::observe({
    subset <-
      getTemporalCharacterizationData()$domainId %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "temporalCharacterizationDomainNameOptions",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  
  
  
  ###getMultipleCharacterizationData----
  getMultipleCharacterizationData <- shiny::reactive(x = {
    if (all(is(dataSource, "environment"), !any(
      exists('covariateValue'),
      exists('temporalCovariateValue')
    ))) {
      return(NULL)
    }
    if (!any(
      input$tabs == "temporalCharacterization",
      input$tabs == "cohortCharacterization"
    )) {
      return(NULL)
    }
    if (any(
      length(getCohortIdFromSelectedCompoundCohortName()) != 1,
      length(getDatabaseIdsFromDropdown()) == 0
    )) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0(
        "Extracting characterization data for target cohort:",
        getCohortIdFromSelectedCompoundCohortName()
      ),
      value = 0
    )
    data <- getMultipleCharacterizationResults(
      dataSource = dataSource,
      cohortIds = getCohortIdFromSelectedCompoundCohortName(),
      databaseIds = getDatabaseIdsFromDropdown()
    )
    return(data)
  })
  
  ##Characterization----
  ### getCharacterizationDataFiltered ----
  getCharacterizationDataFiltered <- shiny::reactive(x = {
    if (input$tabs != "cohortCharacterization")
    {
      return(NULL)
    }
    if (any(
      is.null(getMultipleCharacterizationData()),
      length(getMultipleCharacterizationData()) == 0
    )) {
      return(NULL)
    }
    if (is.null(getMultipleCharacterizationData()$covariateRef))
    {
      warning("No covariate reference data found")
      return(NULL)
    }
    if (is.null(getMultipleCharacterizationData()$covariateValue))
    {
      return(NULL)
    }
    if (is.null(getMultipleCharacterizationData()$analysisRef))
    {
      warning("No analysis ref dta found")
      return(NULL)
    }
    
    covariatesTofilter <-
      getMultipleCharacterizationData()$covariateRef
    
    if (all(
      !is.null(input$conceptSetsSelectedFromOneCohort),
      length(input$conceptSetsSelectedFromOneCohort) > 0,
      input$conceptSetsSelectedFromOneCohort != ""
    )) {
      covariatesTofilter <- covariatesTofilter  %>%
        dplyr::inner_join(
          getResolvedConceptIdsForCohortFilteredBySelectedConceptSets() %>%
            dplyr::select(.data$conceptId) %>%
            dplyr::distinct(),
          by = c("conceptId")
        )
    }
    
    #Pretty analysis
    if (input$charType == "Pretty")
    {
      covariatesTofilter <- covariatesTofilter %>%
        dplyr::filter(.data$analysisId %in% prettyAnalysisIds)
      #prettyAnalysisIds this is global variable
    }
    
    characterizationDataValue <-
      getMultipleCharacterizationData()$covariateValue %>%
      dplyr::filter(.data$characterizationSource %in% c('C', 'F')) %>% #C - cohort, F is Feature
      dplyr::select(-.data$timeId, -.data$startDay, -.data$endDay) %>% # remove temporal characterization data
      dplyr::inner_join(covariatesTofilter,
                        by = c('covariateId', 'characterizationSource')) %>%
      dplyr::inner_join(
        getMultipleCharacterizationData()$analysisRef,
        by = c('analysisId', 'characterizationSource')
      )
    
    #enhancement
    characterizationDataValue <- characterizationDataValue %>%
      dplyr::mutate(covariateNameShort = gsub(".*: ", "", .data$covariateName)) %>%
      dplyr::mutate(
        covariateNameShortCovariateId = paste0(.data$covariateNameShort,
                                               " (",
                                               .data$covariateId, ")")
      )
    
    if (any(is.null(characterizationDataValue),
            nrow(characterizationDataValue) == 0))
    {
      return(NULL)
    }
    
    if (all(input$charType == "Raw",
            input$charProportionOrContinuous == "Proportion"))
    {
      #!!!! show numbers as percentage in data table
      characterizationDataValue <- characterizationDataValue %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else
      if (all(input$charType == "Raw",
              input$charProportionOrContinuous == "Continuous"))
      {
        characterizationDataValue <- characterizationDataValue %>%
          dplyr::filter(.data$isBinary == 'N')
      }
    return(characterizationDataValue)
  })
  
  
  ###getCharacterizationTableData----
  getCharacterizationTableData <- shiny::reactive(x = {
    if (input$tabs != "cohortCharacterization")
    {
      return(NULL)
    }
    data <- getCharacterizationDataFiltered()
    if (any(is.null(data), nrow(data) == 0)) {
      return(NULL)
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
  
  ###getCharacterizationTableDataPretty----
  getCharacterizationTableDataPretty <- shiny::reactive(x = {
    if (input$tabs != "cohortCharacterization")
    {
      return(NULL)
    }
    data <- getCharacterizationTableData()
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    table <- data %>%
      prepareTable1()
    
    characteristics <- table %>%
      dplyr::select(.data$characteristic,
                    .data$position,
                    .data$header,
                    .data$sortOrder) %>%
      dplyr::distinct() %>%
      dplyr::group_by(.data$characteristic, .data$position, .data$header) %>%
      dplyr::summarise(sortOrder = max(.data$sortOrder),
                       .groups = 'keep') %>%
      dplyr::ungroup() %>%
      dplyr::arrange(.data$position, desc(.data$header)) %>%
      dplyr::mutate(sortOrder = dplyr::row_number()) %>%
      dplyr::distinct()
    
    characteristics <- dplyr::bind_rows(
      characteristics %>%
        dplyr::filter(.data$header == 1) %>%
        dplyr::mutate(
          cohortId = sort(getCohortIdFromSelectedCompoundCohortName())[[1]],
          databaseId = sort(getDatabaseIdsFromDropdown()[[1]])
        ),
      characteristics %>%
        dplyr::filter(.data$header == 0) %>%
        tidyr::crossing(dplyr::tibble(databaseId = getDatabaseIdsFromDropdown())) %>%
        tidyr::crossing(
          dplyr::tibble(cohortId = getCohortIdFromSelectedCompoundCohortName())
        )
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
      dplyr::inner_join(
        cohortCount %>%
          dplyr::select(-.data$cohortEntries),
        by = c("databaseId", "cohortId")
      ) %>%
      dplyr::mutate(databaseId = paste0(
        .data$databaseId,
        "<br>(n = ",
        scales::comma(.data$cohortSubjects, accuracy = 1),
        ")"
      )) %>%
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
    return(table)
  })
  
  
  ###getCharacterizationRawData----
  getCharacterizationRawData <- shiny::reactive(x = {
    if (input$tabs != "cohortCharacterization")
    {
      return(NULL)
    }
    data <- getCharacterizationTableData()
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    if (all(
      !is.null(getCharacterizationAnalysisNameOptions()),
      getCharacterizationAnalysisNameOptions() != "",
      length(getCharacterizationAnalysisNameOptions()) > 0
    )) {
      data <- data %>%
        dplyr::filter(.data$analysisName %in% getCharacterizationAnalysisNameOptions())
    }
    if (all(
      !is.null(getCharacterizationDomainNameOptions()),
      getCharacterizationDomainNameOptions() != "",
      length(getCharacterizationDomainNameOptions()) > 0
    )) {
      data <- data %>%
        dplyr::filter(.data$domainId %in% getCharacterizationDomainNameOptions())
    }
    
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    
    covariateNames <- data %>%
      dplyr::select(.data$covariateId,
                    .data$covariateName,
                    .data$conceptId) %>%
      dplyr::distinct()
    
    if (input$characterizationColumnFilters == "Mean and Standard Deviation")
    {
      data <- data %>%
        dplyr::arrange(.data$databaseId,
                       .data$cohortId) %>%
        tidyr::pivot_longer(cols = c(.data$mean,
                                     .data$sd),
                            names_to = 'names') %>%
        dplyr::mutate(names = paste0(.data$databaseId, " ", .data$names)) %>%
        dplyr::arrange(.data$databaseId, .data$names, .data$covariateId) %>%
        tidyr::pivot_wider(
          id_cols = c(.data$cohortId, .data$covariateId),
          names_from = .data$names,
          values_from = .data$value,
          values_fill = 0
        )
    } else
    {
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
                        by = "covariateId") %>%
      dplyr::select(-.data$covariateId, -.data$cohortId, -.data$conceptId) %>%
      dplyr::relocate(.data$covariateName)
    data <- data[order(-data[2]), ]
    return(data)
  })
  
  ### Output: characterizationTable ------
  output$characterizationTable <- DT::renderDataTable(expr = {
    if (input$tabs != "cohortCharacterization")
    {
      return(NULL)
    }
    data <- getCharacterizationTableData()
    validate(need(all(
      !is.null(getCohortIdFromSelectedCompoundCohortName()),
      length(getCohortIdFromSelectedCompoundCohortName()) > 0
    ),
    "No data for the combination"))
    validate(need(!is.null(data), "No data for the combination"))
    
    databaseIds <- sort(unique(data$databaseId))
    
    cohortCounts <- data %>%
      dplyr::inner_join(cohortCount,
                        by = c("cohortId", "databaseId")) %>%
      dplyr::filter(.data$cohortId == getCohortIdFromSelectedCompoundCohortName()) %>%
      dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown()) %>%
      dplyr::select(.data$cohortSubjects) %>%
      dplyr::pull(.data$cohortSubjects) %>%
      unique()
    databaseIdsWithCount <-
      paste(databaseIds,
            "(n = ",
            format(cohortCounts, big.mark = ","),
            ")")
    
    if (input$charType == "Pretty")
    {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Rendering pretty table for cohort characterization."),
        value = 0
      )
      
      table <- getCharacterizationTableDataPretty()
      validate(need(
        nrow(table) > 0,
        "No data available for selected combination."
      ))
      
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
    } else
    {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Rendering raw table for cohort characterization."),
        value = 0
      )
      data <- getCharacterizationRawData()
      validate(need(
        nrow(data) > 0,
        "No data available for selected combination."
      ))
      
      if (input$characterizationColumnFilters == "Mean and Standard Deviation")
      {
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
            minCellRealDef(1:(length(
              databaseIds
            ) * 2), digits = 3)
          )
        )
        sketch <- htmltools::withTags(table(class = "display",
                                            thead(tr(
                                              th(rowspan = 2, "Covariate Name"),
                                              lapply(
                                                databaseIdsWithCount,
                                                th,
                                                colspan = 2,
                                                class = "dt-center",
                                                style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                              )
                                            ),
                                            tr(
                                              lapply(rep(
                                                c("Mean", "SD"), length(databaseIds)
                                              ),
                                              th,
                                              style = "border-right:1px solid silver;border-bottom:1px solid silver")
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
          columns = (1 + 1:(length(
            databaseIds
          ) * 2)),
          background = DT::styleColorBar(c(0, 1), "lightblue"),
          backgroundSize = "98% 88%",
          backgroundRepeat = "no-repeat",
          backgroundPosition = "center"
        )
      } else
      {
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
            minCellRealDef(1:(length(
              databaseIds
            )), digits = 3)
          )
        )
        
        colnames <- cohortCount %>%
          dplyr::filter(.data$databaseId %in% colnames(data)) %>%
          dplyr::filter(.data$cohortId == getCharacterizationTableData()$cohortId %>% unique()) %>%
          dplyr::mutate(colnames = paste0(
            .data$databaseId,
            "<br>(n = ",
            scales::comma(.data$cohortSubjects, accuracy = 1),
            ")"
          )) %>%
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
          columns = (1 + 1:(length(
            databaseIds
          ))),
          background = DT::styleColorBar(c(0, 1), "lightblue"),
          backgroundSize = "98% 88%",
          backgroundRepeat = "no-repeat",
          backgroundPosition = "center"
        )
      }
    }
    return(table)
  }, server = TRUE)
  
  ###saveCohortCharacterizationTable----
  output$saveCohortCharacterizationTable <-  downloadHandler(
    filename = function()
    {
      getCsvFileNameWithDateTime(string = "cohortCharacterization")
    },
    content = function(file)
    {
      if (input$charType == "Pretty")
      {
        data <- getCharacterizationTableDataPretty()
      } else
      {
        data <- getCharacterizationRawData()
      }
      downloadCsv(x = data,
                  fileName = file)
    }
  )
  
  ## Temporal Characterization ------
  ### getTemporalCharacterizationData ------
  getTemporalCharacterizationData <- shiny::reactive(x = {
    if (input$tabs != "temporalCharacterization")
    {
      return(NULL)
    }
    if (any(
      is.null(getMultipleCharacterizationData()),
      length(getMultipleCharacterizationData()) == 0
    )) {
      return(NULL)
    }
    if (is.null(getMultipleCharacterizationData()$covariateRef))
    {
      warning("No covariate ref dta found")
      return(NULL)
    }
    if (is.null(getMultipleCharacterizationData()$covariateValue))
    {
      return(NULL)
    }
    if (is.null(getMultipleCharacterizationData()$analysisRef))
    {
      warning("No analysis ref dta found")
      return(NULL)
    }
    data <-
      getMultipleCharacterizationData()$covariateValue %>%
      dplyr::filter(.data$characterizationSource %in% c('CT', 'FT')) %>%
      dplyr::inner_join(
        getMultipleCharacterizationData()$covariateRef,
        by = c('covariateId', 'characterizationSource')
      ) %>%
      dplyr::inner_join(
        getMultipleCharacterizationData()$analysisRef %>%
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
    return(data)
  })
  
  ### getTemporalCharacterizationDataFiltered ------
  getTemporalCharacterizationDataFiltered <-
    shiny::reactive(x = {
      if (input$tabs != "temporalCharacterization")
      {
        return(NULL)
      }
      data <- getTemporalCharacterizationData()
      if (any(is.null(data),
              nrow(data) == 0)) {
        return(NULL)
      }
      
      if (all(
        !is.null(input$conceptSetsSelectedFromOneCohort),
        length(input$conceptSetsSelectedFromOneCohort) > 0
      )) {
        data <- data  %>%
          dplyr::inner_join(
            getResolvedConceptIdsForCohortFilteredBySelectedConceptSets() %>%
              dplyr::select(.data$conceptId, .data$cohortId) %>%
              dplyr::distinct(),
            by = c("conceptId", "cohortId")
          )
      }
      
      if (all(
        !is.null(getTemporalCharacterizationAnalysisNameOptions()),
        getTemporalCharacterizationAnalysisNameOptions() != "",
        length(getTemporalCharacterizationAnalysisNameOptions()) > 0
      )) {
        data <- data %>%
          dplyr::filter(.data$analysisName %in% getTemporalCharacterizationAnalysisNameOptions())
      }
      
      if (all(
        !is.null(getCharacterizationDomainNameOptions()),
        getCharacterizationDomainNameOptions() != "",
        length(getCharacterizationDomainNameOptions()) > 0
      )) {
        #!!!!!!!!!!!!why isnt this working?
        data <- data %>%
          dplyr::filter(.data$domainId %in% getCharacterizationDomainNameOptions())
      }
      
      if (length(getTimeIdsFromSelectedTemporalCovariateChoices()) > 0)
      {
        data <- data %>%
          dplyr::filter(.data$timeId %in% getTimeIdsFromSelectedTemporalCovariateChoices())
      }
      
      if (input$temporalCharacterizationOutputTypeProportionOrContinuous == "Proportion")
      {
        data <- data %>%
          dplyr::filter(.data$isBinary == 'Y')
      } else
        if (input$temporalCharacterizationOutputTypeProportionOrContinuous == "Continuous")
        {
          data <- data %>%
            dplyr::filter(.data$isBinary == 'N')
        }
      
      if (any(is.null(data),
              nrow(data) == 0))
      {
        return(NULL)
      }
      return(data)
    })
  
  
  ### getTemporalCharacterizationTableData ------
  getTemporalCharacterizationTableData <- shiny::reactive({
    if (input$tabs != "temporalCharacterization")
    {
      return(NULL)
    }
    if (any(
      !exists('temporalCovariateChoices'),
      is.null(temporalCovariateChoices),
      nrow(temporalCovariateChoices) == 0
    )) {
      return(NULL)
    }
    data <- getTemporalCharacterizationDataFiltered()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    data <- data %>%
      dplyr::select(-.data$cohortId, -.data$databaseId)
    
    data <- data %>%
      tidyr::pivot_wider(
        id_cols = c("covariateId", "covariateName"),
        names_from = "choices",
        values_from = "mean" ,
        names_sep = "_"
      ) %>%
      dplyr::relocate(.data$covariateId,
                      .data$covariateName) %>%
      dplyr::arrange(dplyr::desc(dplyr::across(dplyr::starts_with('Start'))))
    
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    return(data)
  })
  
  ### output: temporalCharacterizationTable----
  output$temporalCharacterizationTable <-
    DT::renderDataTable(expr = {
      if (input$tabs != "temporalCharacterization")
      {
        return(NULL)
      }
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Rendering raw table for temporal cohort characterization."),
        value = 0
      )
      data <- getTemporalCharacterizationTableData()
      validate(need(nrow(data) > 0,
                    "No data available for selected combination."))
      
      temporalCovariateChoicesSelected <-
        temporalCovariateChoices %>%
        dplyr::filter(.data$timeId %in% c(getTimeIdsFromSelectedTemporalCovariateChoices())) %>%
        dplyr::arrange(.data$timeId)
      
      options = list(
        pageLength = 1000,
        lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
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
        data,
        options = options,
        rownames = FALSE,
        colnames = colnames(data) %>%
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
  
  ###saveTemporalCharacterizationTable----
  output$saveTemporalCharacterizationTable <-  downloadHandler(
    filename = function()
    {
      getCsvFileNameWithDateTime(string = "getTemporalCharacterizationTableData")
    },
    content = function(file)
    {
      downloadCsv(x = getTemporalCharacterizationTableData(),
                  fileName = file)
    }
  )
  
  #______________----
  ## Compare Characterization/Temporal Characterization ------
  ## Shared----
  ###getCompareCharacterizationAnalysisNameFilter----
  getCompareCharacterizationAnalysisNameFilter <-
    shiny::reactive(x = {
      return(input$compareCharacterizationAnalysisNameFilter)
    })
  
  ###getCompareCharacterizationDomainNameFilter----
  getCompareCharacterizationDomainNameFilter <-
    shiny::reactive(x = {
      return(input$compareCharacterizationDomainNameFilter)
    })
  
  ###Update: compareCharacterizationAnalysisNameFilter----
  shiny::observe({
    data <- getCompareCharacterizationData()
    if (all(!is.null(data),
            nrow(data) > 0))
    {
      subset <- data$analysisName %>% unique() %>% sort()
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "compareCharacterizationAnalysisNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = subset
      )
    }
  })
  
  ###Update: compareCharacterizationDomainNameFilter----
  shiny::observe({
    data <- getCompareCharacterizationData()
    if (all(!is.null(data),
            nrow(data) > 0))
    {
      subset <- data$domainId %>% unique() %>% sort()
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "compareCharacterizationDomainNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = subset
      )
    }
  })
  
  ###getCompareTemporalCharacterizationDomainNameFilter----
  getCompareTemporalCharacterizationDomainNameFilter <-
    shiny::reactive(x = {
      return(input$compareTemporalCharacterizationDomainNameFilter)
    })
  ###Update: compareTemporalCharacterizationDomainNameFilter----
  shiny::observe({
    subset <-
      getCompareTemporalCharcterizationData()$domainId %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "compareTemporalCharacterizationDomainNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  ###getCompareTemporalCharacterizationAnalysisNameFilter----
  getCompareTemporalCharacterizationAnalysisNameFilter <-
    shiny::reactive(x = {
      return(input$compareTemporalCharacterizationAnalysisNameFilter)
    })
  ###Update: compareTemporalCharacterizationAnalysisNameFilter----
  shiny::observe({
    subset <-
      getCompareTemporalCharcterizationData()$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "compareTemporalCharacterizationAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  ###getMultipleCompareCharacterizationData----
  getMultipleCompareCharacterizationData <-
    shiny::reactive(x = {
      if (!any(
        input$tabs == "compareCohortCharacterization",
        input$tabs == "compareTemporalCharacterization"
      )) {
        return(NULL)
      }
      
      if (all(is(dataSource, "environment"), !any(
        exists('covariateValue'),
        exists('temporalCovariateValue')
      ))) {
        return(NULL)
      }
      
      if (any(
        length(getCohortIdFromSelectedCompoundCohortName()) != 1,
        length(getComparatorCohortIdFromSelectedCompoundCohortName()) != 1,
        length(getDatabaseIdsFromDropdown()) == 0
      )) {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0(
          "Extracting temporal characterization data for target cohort:",
          getCohortIdFromSelectedCompoundCohortName(),
          " and comparator cohort:",
          getComparatorCohortIdFromSelectedCompoundCohortName(),
          ' for ',
          input$database
        ),
        value = 0
      )
      
      data <- getMultipleCharacterizationResults(
        dataSource = dataSource,
        cohortIds = c(
          getCohortIdFromSelectedCompoundCohortName(),
          getComparatorCohortIdFromSelectedCompoundCohortName()
        ) %>% unique(),
        databaseIds = input$database
      )
      return(data)
    })
  
  ## Compare Characterization ----
  ### getCompareCharacterizationData ------
  getCompareCharacterizationData <- shiny::reactive({
    if (input$tabs != "compareCohortCharacterization")
    {
      return(NULL)
    }
    if (any(
      is.null(getMultipleCompareCharacterizationData()),
      length(getMultipleCompareCharacterizationData()) == 0
    )) {
      return(NULL)
    }
    if (is.null(getMultipleCompareCharacterizationData()$covariateRef))
    {
      warning("No covariate reference data found")
      return(NULL)
    }
    if (is.null(getMultipleCompareCharacterizationData()$covariateValue))
    {
      return(NULL)
    }
    if (is.null(getMultipleCompareCharacterizationData()$analysisRef))
    {
      warning("No analysis reference data found")
      return(NULL)
    }
    
    data <-
      getMultipleCompareCharacterizationData()$covariateValue %>%
      dplyr::filter(.data$characterizationSource %in% c('C', 'F')) %>%
      dplyr::select(-.data$timeId,-.data$startDay,-.data$endDay) %>%
      dplyr::inner_join(
        getMultipleCompareCharacterizationData()$covariateRef,
        by = c("covariateId", "characterizationSource")
      ) %>%
      dplyr::inner_join(
        getMultipleCompareCharacterizationData()$analysisRef ,
        by = c("analysisId", "characterizationSource")
      )
    
    covs1 <- data %>%
      dplyr::filter(.data$cohortId == getCohortIdFromSelectedCompoundCohortName()) %>%
      dplyr::mutate(
        analysisNameLong = paste0(
          .data$analysisName,
          " (",
          as.character(.data$startDay),
          " to ",
          as.character(.data$endDay),
          ")"
        )
      ) %>%
      dplyr::relocate(
        .data$cohortId,
        .data$databaseId,
        .data$analysisId,
        .data$covariateId,
        .data$covariateName,
        .data$isBinary
      ) %>%
      dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
    
    covs2 <- data %>%
      dplyr::filter(.data$cohortId == getComparatorCohortIdFromSelectedCompoundCohortName()) %>%
      dplyr::mutate(
        analysisNameLong = paste0(
          .data$analysisName,
          " (",
          as.character(.data$startDay),
          " to ",
          as.character(.data$endDay),
          ")"
        )
      ) %>%
      dplyr::relocate(
        .data$cohortId,
        .data$databaseId,
        .data$analysisId,
        .data$covariateId,
        .data$covariateName,
        .data$isBinary
      ) %>%
      dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
    
    if (any(is.null(covs1),
            is.null(covs2)))
    {
      return(NULL)
    }
    
    balance <- compareCohortCharacteristics(covs1, covs2)
    if (any(is.null(balance),
            nrow(balance) == 0))
    {
      return(NULL)
    }
    
    # enhanced
    balance <- balance %>%
      dplyr::mutate(absStdDiff = abs(.data$stdDiff)) %>%
      dplyr::rename(covariateNameFull = .data$covariateName) %>%
      dplyr::mutate(covariateName = gsub(".*: ", "", .data$covariateNameFull)) %>%
      dplyr::mutate(
        covariateName = dplyr::case_when(
          stringr::str_detect(
            string = tolower(.data$covariateNameFull),
            pattern = 'age group|gender'
          ) ~ .data$covariateNameFull,
          TRUE ~ gsub(".*: ", "", .data$covariateNameFull)
        )
      ) %>%
      dplyr::mutate(covariateName = paste0(.data$covariateName, " (", .data$covariateId, ")"))
  })
  
  ###getCompareCharacterizationDataFiltered----
  getCompareCharacterizationDataFiltered <-  shiny::reactive({
    if (input$tabs != "compareCohortCharacterization")
    {
      return(NULL)
    }
    data <- getCompareCharacterizationData()
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    
    if (all(
      input$characterizationCompareMethod == "Raw table",
      input$charCompareProportionOrContinuous == "Proportion"
    )) {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else
      if (all(
        input$characterizationCompareMethod == "Raw table",
        input$charCompareProportionOrContinuous == "Continuous"
      )) {
        data <- data %>%
          dplyr::filter(.data$isBinary == 'N')
      }
    if (input$compareCharacterizationProportionOrContinous == "Proportion")
    {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else
      if (input$compareCharacterizationProportionOrContinous == "Continuous")
      {
        data <- data %>%
          dplyr::filter(.data$isBinary == 'N')
      }
    
    if (all(
      !is.null(input$conceptSetsSelectedFromOneCohort),
      length(input$conceptSetsSelectedFromOneCohort) > 0
    )) {
      data <- data  %>%
        dplyr::inner_join(
          getResolvedConceptIdsForCohortFilteredBySelectedConceptSets() %>%
            dplyr::select(.data$conceptId) %>%
            dplyr::distinct(),
          by = c("conceptId")
        )
    }
    
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    return(data)
  })
  
  ###getCompareCharacterizationTablePretty----
  getCompareCharacterizationTablePretty <- shiny::reactive({
    if (input$tabs != "compareCohortCharacterization")
    {
      return(NULL)
    }
    data <- getCompareCharacterizationDataFiltered()
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::mutate(covariateName = .data$covariateNameFull) %>%
      prepareTable1Comp()
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::arrange(.data$sortOrder) %>%
      dplyr::select(-.data$sortOrder) %>%
      dplyr::select(-.data$cohortId1,-.data$cohortId2)
    return(data)
  })
  
  ###getCompareCharacterizationTableRaw----
  getCompareCharacterizationTableRaw <- shiny::reactive({
    if (input$tabs != "compareCohortCharacterization")
    {
      return(NULL)
    }
    data <- getCompareCharacterizationDataFiltered()
    if (any(is.null(data),
            nrow(data) == 0)) {
      return(NULL)
    }
    if (all(
      !is.null(getCompareCharacterizationAnalysisNameFilter()),
      getCompareCharacterizationAnalysisNameFilter() != "",
      length(getCompareCharacterizationAnalysisNameFilter()) > 0
    )) {
      data <- data %>% #!!!! why is it not working?
        dplyr::filter(.data$analysisName %in% getCompareCharacterizationAnalysisNameFilter())
      
    }
    if (all(
      !is.null(getCompareCharacterizationDomainNameFilter()),
      getCompareCharacterizationDomainNameFilter() != "",
      length(getCompareCharacterizationDomainNameFilter()) > 0
    )) {
      data <- data %>% #!!!! why is it not working?
        dplyr::filter(.data$analysisName %in% getCompareCharacterizationDomainNameFilter())
    }
    if (all(
      !is.null(input$conceptSetsSelectedFromOneCohort),
      input$conceptSetsSelectedFromOneCohort != "",
      length(input$conceptSetsSelectedFromOneCohort) > 0
    )) {
      data <-
        data %>% #!!! there is a bug here getResoledAndMappedConceptIdsForFilters
        dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
    }
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    
    # enhancement
    data <- data %>%
      dplyr::rename(
        "meanTarget" = mean1,
        "sdTarget" = sd1,
        "meanComparator" = mean2,
        "sdComparator" = sd2,
        "StdDiff" = absStdDiff
      )
  })
  
  ###output: compareCharacterizationTable----
  output$compareCharacterizationTable <-
    DT::renderDataTable(expr = {
      if (input$tabs != "compareCohortCharacterization")
      {
        return(NULL)
      }
      
      balance <- getCompareCharacterizationDataFiltered()
      validate(need(
        all(!is.null(balance), nrow(balance) > 0),
        "No data available for selected combination."
      ))
      targetCohortIdValue <- balance %>%
        dplyr::filter(!is.na(.data$cohortId1)) %>%
        dplyr::pull(.data$cohortId1) %>%
        unique()
      comparatorcohortIdValue <- balance %>%
        dplyr::filter(!is.na(.data$cohortId2)) %>%
        dplyr::pull(.data$cohortId2) %>%
        unique()
      databaseIdForCohortCharacterization <-
        balance$databaseId %>%
        unique()
      
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
      
      targetCohortHeader <- paste0(
        targetCohortShortName,
        " (n = ",
        scales::comma(targetCohortSubjects,
                      accuracy = 1),
        ")"
      )
      comparatorCohortHeader <- paste0(
        comparatorCohortShortName,
        " (n = ",
        scales::comma(comparatorCohortSubjects,
                      accuracy = 1),
        ")"
      )
      
      if (input$characterizationCompareMethod == "Pretty table")
      {
        progress <- shiny::Progress$new()
        on.exit(progress$close())
        progress$set(
          message = paste0("Rendering pretty table for compare characterization."),
          value = 0
        )
        data <- getCompareCharacterizationTablePretty()
        validate(need(
          nrow(data) > 0,
          "No data available for selected combination."
        ))
        
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
          data,
          options = options,
          rownames = FALSE,
          colnames = c(
            "Characteristic",
            targetCohortHeader,
            comparatorCohortHeader,
            "Std. Diff."
          ),
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
      } else
      {
        progress <- shiny::Progress$new()
        on.exit(progress$close())
        progress$set(
          message = paste0("Rendering raw table for compare characterization."),
          value = 0
        )
        if (input$compareCharacterizationColumnFilters == "Mean and Standard Deviation")
        {
          table <- balance %>%
            dplyr::select(
              .data$covariateName,
              .data$mean1,
              .data$sd1,
              .data$mean2,
              .data$sd2,
              .data$stdDiff
            ) %>%
            dplyr::arrange(desc(.data$stdDiff))
          
          columsDefs <- list(truncateStringDef(0, 80),
                             minCellRealDef(1:5, digits = 2))
          
          colorBarColumns <- c(2, 4)
          
          standardDifferenceColumn <- 6
          
          table <- table %>%
            dplyr::rename(!!targetCohortHeader := .data$meanTarget) %>%
            dplyr::rename(!!comparatorCohortHeader := .data$meanComparator)
          
        } else
        {
          table <- balance %>%
            dplyr::select(.data$covariateName,
                          .data$mean1,
                          .data$mean2,
                          .data$stdDiff) %>%
            dplyr::rename("target" = mean1,
                          "comparator" = mean2)
          
          columsDefs <- list(truncateStringDef(0, 80),
                             minCellRealDef(1:3, digits = 2))
          
          colorBarColumns <- c(2, 3)
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
  
  ###saveCompareCohortCharacterizationTable----
  output$saveCompareCohortCharacterizationTable <-
    downloadHandler(
      filename = function()
      {
        getCsvFileNameWithDateTime(string = "compareCohortCharacterization")
      },
      content = function(file)
      {
        if (input$characterizationCompareMethod == "Pretty table")
        {
          data <- getCompareCharacterizationTablePretty()
        } else
        {
          data <- getCompareCharacterizationTableRaw()
        }
        downloadCsv(x = getCompareCharacterizationDataFiltered(),
                    fileName = file)
      }
    )
  
  ###compareCharacterizationPlot----
  output$compareCharacterizationPlot <-
    ggiraph::renderggiraph(expr = {
      if (input$tabs != "compareCohortCharacterization")
      {
        return(NULL)
      }
      data <- getCompareCharacterizationDataFiltered()
      validate(need(nrow(data) > 0,
                    "No data available for selected combination."))
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Rendering plot for compare characterization."),
        value = 0
      )
      validate(need(nrow(data) > 0,
                    "No data available for selected combination."))
      #!!!!!!! radio buttons and drop down not working
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
  
  ##Compare Temporal Characterization.-----
  ###getCompareTemporalCharcterizationData----
  getCompareTemporalCharcterizationData <- shiny::reactive(x = {
    if (input$tabs != "compareTemporalCharacterization")
    {
      return(NULL)
    }
    if (any(
      is.null(getMultipleCompareCharacterizationData()),
      length(getMultipleCompareCharacterizationData()) == 0
    )) {
      return(NULL)
    }
    if (is.null(getMultipleCompareCharacterizationData()$covariateRef))
    {
      warning("No covariate reference data found")
      return(NULL)
    }
    if (is.null(getMultipleCompareCharacterizationData()$covariateValue))
    {
      return(NULL)
    }
    if (is.null(getMultipleCompareCharacterizationData()$analysisRef))
    {
      warning("No analysis reference data found")
      return(NULL)
    }
    data <-
      getMultipleCompareCharacterizationData()$covariateValue %>%
      dplyr::filter(.data$characterizationSource %in% c('CT', 'FT')) %>%
      dplyr::filter(.data$timeId %in% getTimeIdsFromSelectedTemporalCovariateChoices()) %>%
      dplyr::select(-.data$startDay,-.data$endDay) %>%
      dplyr::inner_join(
        getMultipleCompareCharacterizationData()$covariateRef,
        by = c("covariateId", "characterizationSource")
      ) %>%
      dplyr::inner_join(
        getMultipleCompareCharacterizationData()$analysisRef,
        by = c("analysisId", "characterizationSource")
      ) %>%
      dplyr::select(-.data$startDay,-.data$endDay) %>%
      dplyr::distinct() %>%
      dplyr::inner_join(getMultipleCompareCharacterizationData()$temporalTimeRef,
                        by = 'timeId') %>%
      dplyr::inner_join(temporalCovariateChoices, by = 'timeId') %>%
      dplyr::select(-.data$missingMeansZero)
    
    covs1 <- data %>%
      dplyr::filter(.data$cohortId == getCohortIdFromSelectedCompoundCohortName()) %>%
      dplyr::mutate(
        analysisNameLong = paste0(
          .data$analysisName,
          " (",
          as.character(.data$startDay),
          " to ",
          as.character(.data$endDay),
          ")"
        )
      ) %>%
      dplyr::relocate(
        .data$cohortId,
        .data$databaseId,
        .data$analysisId,
        .data$covariateId,
        .data$covariateName,
        .data$isBinary
      ) %>%
      dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
    if (any(is.null(covs1),
            nrow(covs1) == 0))
    {
      return(NULL)
    }
    
    covs2 <- data %>%
      dplyr::filter(.data$cohortId == getComparatorCohortIdFromSelectedCompoundCohortName()) %>%
      dplyr::mutate(
        analysisNameLong = paste0(
          .data$analysisName,
          " (",
          as.character(.data$startDay),
          " to ",
          as.character(.data$endDay),
          ")"
        )
      ) %>%
      dplyr::relocate(
        .data$cohortId,
        .data$databaseId,
        .data$analysisId,
        .data$covariateId,
        .data$covariateName,
        .data$isBinary
      ) %>%
      dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
    if (any(is.null(covs2),
            nrow(covs2) == 0))
    {
      return(NULL)
    }
    
    # enhancement
    data <-
      compareTemporalCohortCharacteristics(covs1, covs2) %>%
      dplyr::mutate(absStdDiff = abs(.data$stdDiff)) %>%
      dplyr::mutate(covariateName = gsub(".*: ", "", .data$covariateName)) %>%
      dplyr::mutate(covariateName = paste0(.data$covariateName, " (", .data$covariateId, ")"))
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    return(data)
  })
  
  ###getCompareTemporalCharcterizationDataFiltered----
  getCompareTemporalCharcterizationDataFiltered <-
    shiny::reactive(x = {
      if (input$tabs != "compareTemporalCharacterization")
      {
        return(NULL)
      }
      data <- getCompareTemporalCharcterizationData()
      if (any(is.null(data),
              nrow(data) == 0)) {
        return(NULL)
      }
      if (input$temporalCharacterProportionOrContinuous == "Proportion") {
        data <- data %>%
          dplyr::filter(.data$isBinary == 'Y')
      } else if (input$temporalCharacterProportionOrContinuous == "Continuous") {
        data <- data %>%
          dplyr::filter(.data$isBinary == 'N')
      }
      
      if (all(
        !is.null(getCompareTemporalCharacterizationAnalysisNameFilter()),
        length(getCompareTemporalCharacterizationAnalysisNameFilter()) > 0,
        getCompareTemporalCharacterizationAnalysisNameFilter() != ""
      )) {
        data <- data %>%
          dplyr::filter(
            .data$analysisName %in% getCompareTemporalCharacterizationAnalysisNameFilter()
          )
      }
      
      if (all(
        !is.null(getCompareTemporalCharacterizationDomainNameFilter()),
        length(getCompareTemporalCharacterizationDomainNameFilter()) > 0,
        getCompareTemporalCharacterizationDomainNameFilter() != ""
      )) {
        data <- data %>%
          dplyr::filter(.data$domainId %in% getCompareTemporalCharacterizationDomainNameFilter())
      }
      
      if (all(
        !is.null(input$conceptSetsSelectedFromOneCohort),
        length(input$conceptSetsSelectedFromOneCohort) > 0
      )) {
        data <- data  %>%
          dplyr::inner_join(
            getResolvedConceptIdsForCohortFilteredBySelectedConceptSets() %>%
              dplyr::select(.data$conceptId) %>%
              dplyr::distinct(),
            by = c("conceptId")
          )
      }
      
      if (any(is.null(data),
              nrow(data) == 0))
      {
        return(NULL)
      }
      return(data)
    })
  
  ###getCompareTemporalCharcterizationTableData----
  getCompareTemporalCharcterizationTableData <-
    shiny::reactive({
      if (input$tabs != "compareTemporalCharacterization")
      {
        return(NULL)
      }
      data <- getCompareTemporalCharcterizationDataFiltered()
      if (any(is.null(data),
              nrow(data) == 0)) {
        return(NULL)
      }
      
      if (input$temporalCharacterizationTypeColumnFilter == "Mean and Standard Deviation") {
        table <- data %>%
          dplyr::arrange(desc(abs(.data$stdDiff)))
        
        if (length(getTimeIdsFromSelectedTemporalCovariateChoices()) == 1) {
          table <- table %>%
            dplyr::arrange(.data$choices) %>%
            tidyr::pivot_wider(
              id_cols = c("databaseId", "covariateId", "covariateName"),
              names_from = "choices",
              values_from = c("mean1",
                              "sd1",
                              "mean2",
                              "sd2",
                              "stdDiff"),
              values_fill = 0
            )
        } else
        {
          table <- table %>%
            dplyr::arrange(.data$choices) %>%
            dplyr::rename(
              aMeanTarget = "mean1",
              bSdTarget = "sd1",
              cMeanComparator = "mean2",
              dSdComparator = "sd2"
            ) %>%
            tidyr::pivot_longer(
              cols = c(
                "aMeanTarget",
                "bSdTarget",
                "cMeanComparator",
                "dSdComparator"
              ),
              names_to = "type",
              values_to = "values"
            ) %>%
            dplyr::mutate(names = paste0(.data$databaseId, " ", .data$choices, " ", .data$type)) %>%
            dplyr::arrange(.data$databaseId,
                           .data$startDay,
                           .data$endDay,
                           .data$type) %>%
            tidyr::pivot_wider(
              id_cols = c("covariateName"),
              names_from = "names",
              values_from = c("values"),
              values_fill = 0
            )
        }
      } else
      {
        # only Mean
        table <- data %>%
          dplyr::arrange(desc(abs(.data$stdDiff)))
        
        if (length(getTimeIdsFromSelectedTemporalCovariateChoices()) == 1)
        {
          table <- data %>%
            tidyr::pivot_wider(
              id_cols = c("covariateName"),
              names_from = "choices",
              values_from = c("mean1",
                              "mean2",
                              "stdDiff"),
              values_fill = 0
            )
        } else
        {
          table <- data %>%
            tidyr::pivot_longer(
              cols = c("mean1",
                       "mean2"),
              names_to = "type",
              values_to = "values"
            ) %>%
            dplyr::mutate(names = paste0(.data$databaseId, " ", .data$choices, " ", .data$type)) %>%
            dplyr::arrange(.data$startDay, .data$endDay) %>%
            tidyr::pivot_wider(
              id_cols = c("covariateName"),
              names_from = "names",
              values_from = "values",
              values_fill = 0
            )
        }
      }
      return(data)
    })
  
  ### Output: compareTemporalCharacterizationTable ------
  output$compareTemporalCharacterizationTable <-
    DT::renderDataTable(expr = {
      if (input$tabs != "compareTemporalCharacterization")
      {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Computing compare temporal characterization."),
        value = 0
      )
      data <- getCompareTemporalCharcterizationTableData()
      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
        "No data available for selected combination."
      ))
      
      targetCohortIdValue <- data %>%
        dplyr::filter(!is.na(.data$cohortId1)) %>%
        dplyr::pull(.data$cohortId1) %>%
        unique()
      comparatorcohortIdValue <- data %>%
        dplyr::filter(!is.na(.data$cohortId2)) %>%
        dplyr::pull(.data$cohortId2) %>%
        unique()
      databaseIdForCohortCharacterization <-
        data$databaseId %>%
        unique()
      
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
      
      targetCohortHeader <- paste0(
        targetCohortShortName,
        " (n = ",
        scales::comma(targetCohortSubjects,
                      accuracy = 1),
        ")"
      )
      comparatorCohortHeader <- paste0(
        comparatorCohortShortName,
        " (n = ",
        scales::comma(comparatorCohortSubjects,
                      accuracy = 1),
        ")"
      )
      
      temporalCovariateChoicesSelected <-
        temporalCovariateChoices %>%
        dplyr::filter(.data$timeId %in% c(getTimeIdsFromSelectedTemporalCovariateChoices())) %>%
        dplyr::arrange(.data$timeId) %>%
        dplyr::pull(.data$choices)
      
      if (input$temporalCharacterizationTypeColumnFilter == "Mean and Standard Deviation")
      {
        table <- data %>%
          dplyr::arrange(desc(abs(.data$stdDiff)))
        
        if (length(temporalCovariateChoicesSelected) == 1)
        {
          table <- table %>%
            dplyr::arrange(.data$choices) %>%
            tidyr::pivot_wider(
              id_cols = c("covariateName"),
              names_from = "choices",
              values_from = c("mean1",
                              "sd1",
                              "mean2",
                              "sd2",
                              "stdDiff"),
              values_fill = 0
            )
          
          columnDefs <- list(truncateStringDef(0, 80),
                             minCellRealDef(1:(
                               length(temporalCovariateChoicesSelected) * 5
                             ),
                             digits = 2))
          colorBarColumns <-
            1 + 1:(length(temporalCovariateChoicesSelected) * 5)
          colspan <- 5
          containerColumns <-
            c(
              paste0("Mean ", targetCohortShortName),
              paste0("SD ", targetCohortShortName),
              paste0("Mean ", comparatorCohortShortName),
              paste0("SD ", comparatorCohortShortName),
              "Std. Diff"
            )
        } else
        {
          table <- table %>%
            dplyr::arrange(.data$choices) %>%
            dplyr::rename(
              aMeanTarget = "mean1",
              bSdTarget = "sd1",
              cMeanComparator = "mean2",
              dSdComparator = "sd2"
            ) %>%
            tidyr::pivot_longer(
              cols = c(
                "aMeanTarget",
                "bSdTarget",
                "cMeanComparator",
                "dSdComparator"
              ),
              names_to = "type",
              values_to = "values"
            ) %>%
            dplyr::mutate(names = paste0(.data$databaseId, " ", .data$choices, " ", .data$type)) %>%
            dplyr::arrange(.data$databaseId,
                           .data$startDay,
                           .data$endDay,
                           .data$type) %>%
            tidyr::pivot_wider(
              id_cols = c("covariateName"),
              names_from = "names",
              values_from = c("values"),
              values_fill = 0
            )
          
          columnDefs <- list(truncateStringDef(0, 80),
                             minCellRealDef(1:(
                               length(temporalCovariateChoicesSelected) * 4
                             ),
                             digits = 2))
          colorBarColumns <-
            1 + 1:(length(temporalCovariateChoicesSelected) * 4)
          colspan <- 4
          containerColumns <-
            c(
              paste0("Mean ", targetCohortShortName),
              paste0("SD ", targetCohortShortName),
              paste0("Mean ", comparatorCohortShortName),
              paste0("SD ", comparatorCohortShortName)
            )
        }
      } else
      {
        # only Mean
        table <- data %>%
          dplyr::arrange(desc(abs(.data$stdDiff)))
        
        if (length(temporalCovariateChoicesSelected) == 1)
        {
          table <- table %>%
            tidyr::pivot_wider(
              id_cols = c("covariateName"),
              names_from = "choices",
              values_from = c("mean1",
                              "mean1",
                              "stdDiff"),
              values_fill = 0
            )
          
          containerColumns <-
            c(targetCohortShortName,
              comparatorCohortShortName,
              "Std. Diff")
          columnDefs <- list(truncateStringDef(0, 80),
                             minCellRealDef(1:(
                               length(temporalCovariateChoicesSelected) * 3
                             ),
                             digits = 2))
          colorBarColumns <-
            1 + 1:(length(temporalCovariateChoicesSelected) * 3)
          colspan <- 3
        } else
        {
          table <- table %>%
            tidyr::pivot_longer(
              cols = c("mean1",
                       "mean2"),
              names_to = "type",
              values_to = "values"
            ) %>%
            dplyr::mutate(names = paste0(.data$databaseId, " ", .data$choices, " ", .data$type)) %>%
            dplyr::arrange(.data$startDay, .data$endDay) %>%
            tidyr::pivot_wider(
              id_cols = c("covariateName"),
              names_from = "names",
              values_from = "values",
              values_fill = 0
            )
          
          containerColumns <-
            c(targetCohortShortName, comparatorCohortShortName)
          columnDefs <- list(truncateStringDef(0, 80),
                             minCellRealDef(1:(
                               length(temporalCovariateChoicesSelected) * 2
                             ),
                             digits = 2))
          colorBarColumns <-
            1 + 1:(length(temporalCovariateChoicesSelected) * 2)
          colspan <- 2
        }
      }
      
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(tr(
                                            th(rowspan = 2, "Covariate Name"),
                                            lapply(
                                              temporalCovariateChoicesSelected,
                                              th,
                                              colspan = colspan,
                                              class = "dt-center",
                                              style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                            )
                                          ),
                                          tr(
                                            lapply(rep(
                                              containerColumns,
                                              length(temporalCovariateChoicesSelected)
                                            ),
                                            th,
                                            style = "border-right:1px solid grey")
                                          ))))
      
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
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
      
      return(table)
    }, server = TRUE)
  
  ###saveCompareTemporalCharacterizationTable----
  output$saveCompareTemporalCharacterizationTable <-
    downloadHandler(
      filename = function()
      {
        getCsvFileNameWithDateTime(string = "compareTemporalCharacterization")
      },
      content = function(file)
      {
        downloadCsv(x = getCompareTemporalCharcterizationTableData(),
                    fileName = file)
      }
    )
  
  ##!!!!!!!!!!!!! address https://github.com/OHDSI/CohortDiagnostics/issues/444
  ###compareTemporalCharacterizationPlot----
  output$compareTemporalCharacterizationPlot <-
    ggiraph::renderggiraph(expr = {
      if (input$tabs != "compareTemporalCharacterization")
      {
        return(NULL)
      }
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Rendering plot for compare temporal characterization."),
        value = 0
      )
      data <- getCompareTemporalCharcterizationDataFiltered()
      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
        paste0("No data for the selected combination.")
      ))
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
  
  #______________----
  #Metadata----
  #getMetadataInformation----
  getMetadataInformation <- shiny::reactive(x = {
    data <- metadata %>%
      dplyr::filter(.data$databaseId == input$database)
    return(data)
  })
  
  #getDataSourceInformation----
  getDataSourceInformation <- shiny::reactive(x = {
    data <- database
    if (!'vocabularyVersionCdm' %in% colnames(database))
    {
      data$vocabularyVersionCdm <- "NA"
    }
    if (!'vocabularyVersion' %in% colnames(database)) {
      data$vocabularyVersion <- "NA"
    }
    data <- data %>%
      dplyr::select(
        .data$databaseId,
        .data$databaseName,
        .data$vocabularyVersionCdm,
        .data$vocabularyVersion,
        .data$description
      )
    return(data)
  })
  
  ##output: databaseInformationTable----
  output$databaseInformationTable <- DT::renderDataTable(expr = {
    data <- getDataSourceInformation()
    validate(need(all(!is.null(data),
                      nrow(data) > 0),
                  "Not available."))
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
      searching = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      searchHighlight = TRUE,
      columnDefs = list(list(
        width = "50%", targets = 4
      ))
    )
    sketch <- htmltools::withTags(table(class = "display",
                                        thead(tr(
                                          th(rowspan = 2, "ID"),
                                          th(rowspan = 2, "Name"),
                                          th(
                                            "Vocabulary version",
                                            colspan = 2,
                                            class = "dt-center",
                                            style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                          ),
                                          th(rowspan = 2, "Description")
                                        ),
                                        tr(
                                          lapply(c("CDM source", "Vocabulary table"), th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                        ))))
    table <- DT::datatable(
      data ,
      options = options,
      container = sketch,
      rownames = FALSE,
      class = "stripe compact"
    )
    return(table)
  }, server = TRUE)
  
  # Construct texts:
  #   Drop down for databaseId
  #   Check number of startTime per databaseId - for each runTime create collapsible box
  #   Title of collapsible box:  Run on <databaseId> on <startTime> <timeZone>
  #    - Ran for <runTime> <runTimeUnits> on <CurrentPackage> (<CurrentPackageVersion>) <RVersion>
  #    - scrollable: packageDependencySnapShotJson (show pretty json with scroll bar vertical)
  #    - scrollable: argumentsAtDiagnosticsInitiation (show pretty json with scroll bar vertical)
  
  ##getAllStartTimeFromMetadata----
  getAllStartTimeFromMetadata <- shiny::reactive(x = {
    metadataInformation <- getMetadataInformation()
    #startTime is a list object and can be more than 1
    # this should have a dependency on databaseId
    startTimes <- metadataInformation$startTime %>%
      #filter by selected databaseId
      unique() %>%
      sort()
    return(startTimes)
  })
  #!!!!!!!! should be part of picker input getAllStartTimeFromMetadata()
  
  ##getMetadataParsed----
  getMetadataParsed <- shiny::reactive(x = {
    data <- list()
    metadataInformation <- getMetadataInformation()
    if (any(is.null(metadataInformation),
            nrow(metadataInformation) == 0))
    {
      return(NULL)
    }
    #get start time from picker input
    # temporary solution till picker input is coded
    startTime <- getAllStartTimeFromMetadata()[[1]]
    metadataInformation <- metadataInformation %>%
      #filter by selected databaseId and then filter by selected startTime
      dplyr::filter(.data$startTime == startTime)
    
    data$timeZone <- metadataInformation %>%
      dplyr::filter(.data$variableField == "timeZone") %>%
      dplyr::pull(.data$valueField)
    data$runTime <- metadataInformation %>%
      dplyr::filter(.data$variableField == "runTime")  %>%
      dplyr::pull(.data$valueField) %>%
      as.numeric() %>%
      scales::comma(accuracy = 0.1)
    data$runTimeUnits <- metadataInformation %>%
      dplyr::filter(.data$variableField == "runTimeUnits") %>%
      dplyr::pull(.data$valueField)
    data$currentPackage <- metadataInformation %>%
      dplyr::filter(.data$variableField == "CurrentPackage") %>%
      dplyr::pull(.data$valueField)
    data$currentPackageVersion <- metadataInformation %>%
      dplyr::filter(.data$variableField == "CurrentPackageVersion") %>%
      dplyr::pull(.data$valueField)
    data$databaseId <- metadataInformation %>%
      dplyr::filter(.data$variableField == "databaseId") %>%
      dplyr::pull(.data$valueField)
    return(data)
  })
  
  ##output: metadataInfoTitle----
  output$metadataInfoTitle <- shiny::renderUI(expr = {
    data <- getMetadataParsed()
    if (any(is.null(data),
            length(data) == 0))
    {
      return(NULL)
    }
    tags$table(tags$tr(tags$td(
      paste(
        "Run on ",
        data$databaseId,
        "on ",
        getAllStartTimeFromMetadata()[[1]],
        ##!!! replace with picker input
        data$runTimeUnits
      )
    )))
  })
  #!!!! whats the difference between metadataInfoDetailsText and metadataInfoTitle
  ##output: metadataInfoDetailsText----
  output$metadataInfoDetailsText <- shiny::renderUI(expr = {
    data <- getMetadataParsed()
    if (any(is.null(data),
            nrow(data) == 0))
    {
      return(NULL)
    }
    tags$table(tags$tr(tags$td(
      paste(
        "Ran for ",
        data$runTime,
        data$runTimeUnits,
        "on ",
        data$currentPackage,
        "(",
        data$currentPackageVersion,
        ")"
      )
    )))
  })
  
  ##output: packageDependencySnapShotTable----
  output$packageDependencySnapShotTable <-
    DT::renderDataTable(expr = {
      data <- getMetadataInformation()
      if (all(!is.null(data),
              nrow(data) == 0))
      {
        return(NULL)
      }
      data <- data %>%
        dplyr::filter(.data$variableField == "packageDependencySnapShotJson") %>%
        dplyr::pull(.data$valueField)
      if (any(is.null(data),
              length(data) == 0)) {
        return(NULL)
      }
      result <-
        dplyr::as_tibble(RJSONIO::fromJSON(content = data,
                                           digits = 23))
      options = list(
        pageLength = 100,
        lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
        searching = TRUE,
        searchHighlight = TRUE,
        scrollX = TRUE,
        scrollY = "40vh",
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
  
  ##output: argumentsAtDiagnosticsInitiationJson----
  output$argumentsAtDiagnosticsInitiationJson <-
    shiny::renderText(expr = {
      data <- getMetadataInformation()
      if (any(is.null(data),
              nrow(data) == 0))
      {
        return(NULL)
      }
      data <- data %>%
        dplyr::filter(.data$variableField == "argumentsAtDiagnosticsInitiationJson") %>%
        dplyr::pull(.data$valueField) %>%
        RJSONIO::fromJSON(digits = 23) %>%
        RJSONIO::toJSON(digits = 23,
                        pretty = TRUE)
      return(data)
    })
  
  #__________________----
  # Infoboxes ------
  showInfoBox <- function(title, htmlFileName)
  {
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
  
  shiny::observeEvent(input$cohortCountsInfo,
                      {
                        showInfoBox("Cohort Counts", "html/cohortCounts.html")
                      })
  
  shiny::observeEvent(input$incidenceRateInfo,
                      {
                        showInfoBox("Incidence Rate", "html/incidenceRate.html")
                      })
  
  shiny::observeEvent(input$timeDistributionInfo,
                      {
                        showInfoBox("Time Distributions", "html/timeDistribution.html")
                      })
  
  shiny::observeEvent(input$includedConceptsInfo,
                      {
                        showInfoBox("Concepts in data source",
                                    "html/conceptsInDataSource.html")
                      })
  
  shiny::observeEvent(input$orphanConceptsInfo,
                      {
                        showInfoBox("Orphan (Source) Concepts", "html/orphanConcepts.html")
                      })
  
  shiny::observeEvent(input$conceptSetDiagnosticsInfo,
                      {
                        showInfoBox("Concept Set Diagnostics",
                                    "html/conceptSetDiagnostics.html")
                      })
  
  shiny::observeEvent(input$inclusionRuleStatsInfo,
                      {
                        showInfoBox("Inclusion Rule Statistics",
                                    "html/inclusionRuleStats.html")
                      })
  
  shiny::observeEvent(input$indexEventBreakdownInfo,
                      {
                        showInfoBox("Index Event Breakdown", "html/indexEventBreakdown.html")
                      })
  
  shiny::observeEvent(input$visitContextInfo,
                      {
                        showInfoBox("Visit Context", "html/visitContext.html")
                      })
  
  shiny::observeEvent(input$cohortCharacterizationInfo,
                      {
                        showInfoBox("Cohort Characterization",
                                    "html/cohortCharacterization.html")
                      })
  
  shiny::observeEvent(input$temporalCharacterizationInfo,
                      {
                        showInfoBox("Temporal Characterization",
                                    "html/temporalCharacterization.html")
                      })
  
  shiny::observeEvent(input$cohortOverlapInfo,
                      {
                        showInfoBox("Cohort Overlap", "html/cohortOverlap.html")
                      })
  
  shiny::observeEvent(input$compareCohortCharacterizationInfo,
                      {
                        showInfoBox("Compare Cohort Characteristics",
                                    "html/compareCohortCharacterization.html")
                      })
  
  shiny::observeEvent(input$timeSeriesInfo,
                      {
                        showInfoBox("Time Series", "html/timeSeries.html")
                      })
  
  # Cohort labels ----
  targetCohortCount <- shiny::reactive({
    targetCohortWithCount <-
      getCohortCountResult(
        dataSource = dataSource,
        cohortIds = getCohortIdFromSelectedCompoundCohortName(),
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
    if (any(
      is.null(getCohortIdsFromSelectedCompoundCohortNames()),
      length(getCohortIdsFromSelectedCompoundCohortNames()) == 0
    )) {
      return(NULL)
    }
    if (any(is.null(getCohortSortedByCohortId()),
            nrow(getCohortSortedByCohortId()) == 0))
    {
      return(NULL)
    }
    if (any(is.null(getDatabaseIdsFromDropdown()),
            nrow(getDatabaseIdsFromDropdown()) == 0))
    {
      return(NULL)
    }
    
    cohortSelected <- getCohortSortedByCohortId() %>%
      dplyr::filter(.data$cohortId %in%  getCohortIdsFromSelectedCompoundCohortNames()) %>%
      dplyr::arrange(.data$cohortId)
    
    databaseIdsWithCount <- cohortCount %>%
      dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown()) %>%
      dplyr::distinct(.data$cohortId, .data$databaseId)
    
    distinctDatabaseIdsWithCount <-
      length(databaseIdsWithCount$databaseId %>% unique())
    
    for (i in 1:nrow(cohortSelected))
    {
      filteredDatabaseIds <-
        databaseIdsWithCount[databaseIdsWithCount$cohortId == cohortSelected$cohortId[i], ] %>%
        dplyr::pull()
      
      count <- length(filteredDatabaseIds)
      
      if (distinctDatabaseIdsWithCount == count)
      {
        cohortSelected$compoundName[i] <- cohortSelected$compoundName[i]
        # paste( #, "- in all data sources", sep = " ")
      } else
      {
        countPercentage <-
          round(count / distinctDatabaseIdsWithCount * 100, 2)
        
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
    return(cohortSelected)
    
  })
  
  
  #!!!!!!!!what is the purpose of this?
  renderedSelectedCohorts <- shiny::reactive({
    cohortSelected <- selectedCohorts()
    if (any(is.null(cohortSelected),
            nrow(cohortSelected) == 0))
    {
      return(NULL)
    }
    cohortSelected <- cohortSelected %>%
      dplyr::select(.data$compoundName)
    
    return(apply(cohortSelected, 1, function(x)
      tags$tr(lapply(x, tags$td))))
  })
  
  selectedCohort <- shiny::reactive({
    return(input$cohort)
  })
  
  buildCohortConditionTable <-
    function(messsege, cohortCompoundNameArray)
    {
      tags$table(tags$tr(tags$td(tags$b(messsege))),
                 lapply(cohortCompoundNameArray, function(x)
                   tags$tr(lapply(x, tags$td))))
    }
  
  # Notes Cohort Count ----
  output$cohortCountsCategories <-
    shiny::renderUI({
      cohortSelected <- selectedCohorts()
      cohortCountSelected <- cohortSelected %>%
        dplyr::inner_join(cohortCount, by = c('cohortId')) %>%
        dplyr::filter(.data$databaseId %in% getDatabaseIdsFromDropdown())
      
      cohortSubjectCountEq0 <- c()   # category 1 -> n == 0
      cohortSubjectCount0To100 <-
        c()   # category 2 -> 0 < n < 100
      cohortSubjectCount100To2500 <-
        c()   # category 3 -> 100 < n < 2500
      
      cohortSubjectRecordRatioEq1 <-
        c()    # category 1 -> 1 record per subject (ratio = 1)
      cohortSubjectRecordRatioGt1 <-
        c()    # category 2 -> more than 1 record per subject (ratio > 1)
      
      cohortsWithLowestSubjectConts <- c()
      cohortsWithHighestSubjectConts <- c()
      
      for (i in 1:nrow(cohortCountSelected))
      {
        if (cohortCountSelected$cohortSubjects[i] == 0)
        {
          if (length(cohortSubjectCountEq0[grep(cohortCountSelected$compoundName[i],
                                                cohortSubjectCountEq0,
                                                fixed = TRUE)]) <= 0)
          {
            cohortSubjectCountEq0 <-
              c(
                cohortSubjectCountEq0,
                paste(
                  cohortCountSelected$compoundName[i],
                  cohortCountSelected$databaseId[i],
                  sep = " - "
                )
              )
          } else
          {
            cohortSubjectCountEq0[grep(cohortCountSelected$compoundName[i],
                                       cohortSubjectCountEq0,
                                       fixed = TRUE)] <-
              paste(cohortSubjectCountEq0[grep(cohortCountSelected$compoundName[i],
                                               cohortSubjectCountEq0,
                                               fixed = TRUE)],
                    cohortCountSelected$databaseId[i],
                    sep = ",")
          }
        } else
          if (cohortCountSelected$cohortSubjects[i] > 0 &&
              cohortCountSelected$cohortSubjects[i] <= 100)
          {
            if (length(cohortSubjectCount0To100[grep(cohortCountSelected$compoundName[i],
                                                     cohortSubjectCount0To100,
                                                     fixed = TRUE)]) <= 0)
            {
              cohortSubjectCount0To100 <-
                c(
                  cohortSubjectCount0To100,
                  paste(
                    cohortCountSelected$compoundName[i],
                    cohortCountSelected$databaseId[i],
                    sep = " - "
                  )
                )
            } else
            {
              cohortSubjectCount0To100[grep(cohortCountSelected$compoundName[i],
                                            cohortSubjectCount0To100,
                                            fixed = TRUE)] <-
                paste(cohortSubjectCount0To100[grep(cohortCountSelected$compoundName[i],
                                                    cohortSubjectCount0To100,
                                                    fixed = TRUE)],
                      cohortCountSelected$databaseId[i],
                      sep = ",")
            }
          } else
            if (cohortCountSelected$cohortSubjects[i] > 100 &&
                cohortCountSelected$cohortSubjects[i] < 2500)
            {
              if (length(cohortSubjectCount100To2500[grep(cohortCountSelected$compoundName[i],
                                                          cohortSubjectCount100To2500,
                                                          fixed = TRUE)]) <= 0)
              {
                cohortSubjectCount100To2500 <-
                  c(
                    cohortSubjectCount100To2500,
                    paste(
                      cohortCountSelected$compoundName[i],
                      cohortCountSelected$databaseId[i],
                      sep = " - "
                    )
                  )
              } else
              {
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
            !(cohortCountSelected$databaseId[i] %in% cohortSubjectRecordRatioEq1))
        {
          cohortSubjectRecordRatioEq1 <-
            c(cohortSubjectRecordRatioEq1,
              cohortCountSelected$databaseId[i])
        } else
          if (recordPerSubject > 1 &&
              !(cohortCountSelected$databaseId[i] %in% cohortSubjectRecordRatioGt1))
          {
            cohortSubjectRecordRatioGt1 <-
              c(cohortSubjectRecordRatioGt1,
                cohortCountSelected$databaseId[i])
          }
      }
      
      distinctCohortIds <-
        cohortCountSelected$cohortId %>%  unique()
      
      for (i in 1:length(distinctCohortIds))
      {
        cohortDetailsForDistinctCohortIds <- cohortCountSelected %>%
          dplyr::filter(.data$cohortId == distinctCohortIds[i])
        cohortNameOfDistinctCohortId <-
          cohortDetailsForDistinctCohortIds$compoundName %>% unique()
        if (nrow(cohortDetailsForDistinctCohortIds) >= 10)
        {
          cohortPercentile <-
            cohortDetailsForDistinctCohortIds$cohortSubjects %>%
            quantile(c(0.1, 0.9)) %>%
            round(0)
          
          filteredCohortDetailsWithLowPercentile <-
            cohortDetailsForDistinctCohortIds %>%
            dplyr::filter(.data$cohortSubjects < cohortPercentile[[1]])
          
          if (nrow(filteredCohortDetailsWithLowPercentile) > 0)
          {
            cohortsWithLowestSubjectConts <- c(
              cohortsWithLowestSubjectConts,
              paste(
                cohortNameOfDistinctCohortId,
                paste(
                  filteredCohortDetailsWithLowPercentile$databaseId,
                  collapse = ", "
                ),
                sep = " - "
              )
            )
          }
          
          filteredCohortDetailsWithHighPercentile <-
            cohortDetailsForDistinctCohortIds %>%
            dplyr::filter(.data$cohortSubjects > cohortPercentile[[2]])
          
          if (nrow(filteredCohortDetailsWithHighPercentile) > 0)
          {
            cohortsWithHighestSubjectConts <- c(
              cohortsWithHighestSubjectConts,
              paste(
                cohortNameOfDistinctCohortId,
                paste(
                  filteredCohortDetailsWithHighPercentile$databaseId,
                  collapse = ", "
                ),
                sep = " - "
              )
            )
          }
        }
      }
      
      tags$div(
        tags$b("Cohorts with low subject count :"),
        tags$div(if (length(cohortSubjectCountEq0) > 0)
        {
          buildCohortConditionTable("cohorts were found to be empty", cohortSubjectCountEq0)
        }),
        tags$div(if (length(cohortSubjectCount0To100) > 0)
        {
          buildCohortConditionTable(
            "cohorts were found to have low cohort counts and may not be suitable for most studies",
            cohortSubjectCount0To100
          )
        }),
        tags$div(if (length(cohortSubjectCount100To2500) > 0)
        {
          buildCohortConditionTable(
            "Cohorts were found to have counts less than 2,500. As a general rule of thumb - these cohorts may not be suitable for use as exposure cohorts",
            cohortSubjectCount100To2500
          )
        }),
        tags$div(if (length(cohortSubjectCountEq0) <= 0 &&
                     length(cohortSubjectCount0To100) <= 0 &&
                     length(cohortSubjectCount100To2500) <= 0)
        {
          tags$p("There are no cohorts with subject counts less than 2,500")
        }),
        tags$br(),
        tags$b("Records per subjects :"),
        tags$div(if (length(cohortSubjectRecordRatioEq1) > 0)
        {
          tags$p(
            paste0(
              scales::percent(
                length(cohortSubjectRecordRatioEq1) / length(getDatabaseIdsFromDropdown()),
                accuracy = 0.1
              ),
              " of the datasources have one record per subject - ",
              paste(cohortSubjectRecordRatioEq1, collapse =  ", ")
            )
          )
        }),
        tags$div(if (length(cohortSubjectRecordRatioGt1) > 0)
        {
          tags$p(
            paste0(
              "    ",
              length(cohortSubjectRecordRatioGt1),
              "/",
              length(getDatabaseIdsFromDropdown()),
              " of the datasources that have more than 1 record per subject count - ",
              paste(cohortSubjectRecordRatioGt1, collapse = ", ")
            )
          )
        }),
        tags$br(),
        tags$div(if (length(cohortsWithLowestSubjectConts) > 0)
        {
          buildCohortConditionTable("Cohorts with lowest subject count(s): ",
                                    cohortsWithLowestSubjectConts)
        }),
        tags$br(),
        tags$div(if (length(cohortsWithHighestSubjectConts) > 0)
        {
          buildCohortConditionTable("Cohorts with highest subject count(s): ",
                                    cohortsWithHighestSubjectConts)
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
    tr(
      td(input$selectedComparatorCompoundCohortNames)
    )))
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
      tr(
        td(input$selectedComparatorCompoundCohortNames)
      )))
    })
  
  output$temporalCharCompareSelectedDatabase <-
    shiny::renderUI({
      return(input$database)
    })
  
})
