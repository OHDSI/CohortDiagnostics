shiny::shinyServer(function(input, output, session) {
  cohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$compoundName == input$cohort])
  })
  
  cohortIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$cohorts_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$cohorts_open) || !is.null(input$tabs)) {
      selectedCohortIds <-
        cohort$cohortId[cohort$compoundName  %in% input$cohorts]
      cohortIds(selectedCohortIds)
    }
  })
  
  comparatorCohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$compoundName == input$comparatorCohort])
  })
  
  conceptSetIds <- shiny::reactive(x = {
    return(conceptSets$conceptSetId[conceptSets$conceptSetName %in% input$conceptSetsToFilterCharacterization])
  })
  
  timeIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$timeIdChoices_open,
         input$tabs)
  }, handlerExpr = {
    if (exists('temporalCovariateChoices') &&
        (isFALSE(input$timeIdChoices_open) ||
         !is.null(input$tabs))) {
      selectedTimeIds <- temporalCovariateChoices %>%
        dplyr::filter(choices %in% input$timeIdChoices) %>%
        dplyr::pull(timeId)
      timeIds(selectedTimeIds)
    }
  })
  
  databaseIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$databases_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$databases_open) || !is.null(input$tabs)) {
      selectedDatabaseIds <- input$databases
      databaseIds(selectedDatabaseIds)
    }
  })
  
  cohortSubset <- shiny::reactive({
    return(cohort %>%
             dplyr::arrange(.data$cohortId))
  })
  
  shiny::observe({
    subset <- cohortSubset()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "cohort",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset
    )
  })
  
  shiny::observe({
    subset <- cohortSubset()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "cohorts",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = c(subset[1], subset[2])
    )
  })
  
  shiny::observe({
    subset <- cohortSubset()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "comparatorCohort",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset[2]
    )
  })
  
  # Cohort Definition ---------------------------------------------------------
  output$cohortDefinitionTable <- DT::renderDataTable(expr = {
    data <- cohortSubset() %>%
      dplyr::select(cohort = .data$shortName, .data$cohortId, .data$cohortName) %>%
      dplyr::mutate(cohort = as.factor(.data$cohort),
                    cohortName = as.factor(.data$cohortName),
                    cohortId = as.factor(.data$cohortId))
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      ordering = TRUE,
      paging = TRUE,
      scrollX = TRUE,
      scrollY = '25vh',
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
      selection = list(mode = "single", target = "row"),
      class = "stripe compact"
    )
    return(dataTable)
  }, server = TRUE)
  
  selectedCohortDefinitionRow <- reactive({
    idx <- input$cohortDefinitionTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      subset <- cohortSubset()
      if (nrow(subset) == 0) {
        return(NULL)
      }
      row <- subset[idx[1],]
      return(row)
    }
  })
  
  output$cohortDefinitionRowIsSelected <- reactive({
    return(!is.null(selectedCohortDefinitionRow()))
  })
  
  outputOptions(output,
                "cohortDefinitionRowIsSelected",
                suspendWhenHidden = FALSE)
  
  output$cohortDetailsText <- shiny::renderUI({
    row <- selectedCohortDefinitionRow()
    if (!'logicDescription' %in% colnames(row)) {
      row$logicDescription <- row$cohortName
    }
    if (is.null(row)) {
      return(NULL)
    } else {
      tags$table(
        style = "margin-top: 5px;",
        tags$tr(
          tags$td(tags$strong("Cohort ID: ")),
          tags$td(HTML("&nbsp;&nbsp;")),
          tags$td(row$cohortId)
        ),
        tags$tr(
          tags$td(tags$strong("Cohort Name: ")),
          tags$td(HTML("&nbsp;&nbsp;")),
          tags$td(row$cohortName)
        ),
        tags$tr(
          tags$td(tags$strong("Logic: ")),
          tags$td(HTML("&nbsp;&nbsp;")),
          tags$td(row$logicDescription)
        )
      )
    }
  })
  
  cohortDefinitionCirceRDetails <- shiny::reactive(x = {
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Rendering human readable cohort description using CirceR (may take time)", value = 0)
    
    data <- selectedCohortDefinitionRow()
    if (nrow(selectedCohortDefinitionRow()) > 0) {
      details <- list()
      circeExpression <-
        CirceR::cohortExpressionFromJson(expressionJson = data$json)
      circeExpressionMarkdown <-
        CirceR::cohortPrintFriendly(circeExpression)
      circeConceptSetListmarkdown <-
        CirceR::conceptSetListPrintFriendly(circeExpression$conceptSets)
      details <- data
      details$circeConceptSetListmarkdown <-
        circeConceptSetListmarkdown
      details$htmlExpressionCohort <-
        convertMdToHtml(circeExpressionMarkdown)
      details$htmlExpressionConceptSetExpression <-
        convertMdToHtml(circeConceptSetListmarkdown)
      
      details <- dplyr::bind_rows(details)
    } else {
      return(NULL)
    }
    return(details)
  })
  
  output$cohortDefinitionText <- shiny::renderUI(expr = {
    cohortDefinitionCirceRDetails()$htmlExpressionCohort %>%
      shiny::HTML()
  })
  
  output$cohortDefinitionJson <- shiny::renderText({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$json
    }
  })
  
  output$cohortDefinitionSql <- shiny::renderText({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$sql
    }
  })
  
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
        return(dplyr::tibble())
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
  
  cohortDefinistionConceptSetExpression <- shiny::reactive({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    }
    
    expression <- RJSONIO::fromJSON(row$json, digits = 23)
    if (is.null(expression)) {
      return(NULL)
    }
    
    expression <-
      getConceptSetDetailsFromCohortDefinition(cohortDefinitionExpression = expression)
    
    return(expression)
  })
  
  output$conceptsetExpressionTable <- DT::renderDataTable(expr = {
    data <- cohortDefinistionConceptSetExpression()
    if (is.null(data)) {
      return(NULL)
    }
    
    if (!is.null(data$conceptSetExpression) &&
        nrow(data$conceptSetExpression) > 0) {
      data <- data$conceptSetExpression %>%
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
      scrollX = TRUE
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
  
  
  cohortDefinitionConceptSetExpressionRow <- shiny::reactive(x = {
    idx <- input$conceptsetExpressionTable_rows_selected
    if (length(idx) == 0 || is.null(idx)) {
      return(NULL)
    }
    if (!is.null(cohortDefinistionConceptSetExpression()$conceptSetExpression) &&
        nrow(cohortDefinistionConceptSetExpression()$conceptSetExpression) > 0) {
      data <-
        cohortDefinistionConceptSetExpression()$conceptSetExpression[idx, ]
      if (!is.null(data)) {
        return(data)
      } else {
        return(NULL)
      }
    }
  })
  
  output$conceptSetExpressionRowSelected <- shiny::reactive(x = {
    return(!is.null(cohortDefinitionConceptSetExpressionRow()))
  })
  shiny::outputOptions(x = output,
                       name = "conceptSetExpressionRowSelected",
                       suspendWhenHidden = FALSE)
  
  output$isDataSourceEnvironment <- shiny::reactive(x = {
    return(is(dataSource, "environment"))
  })
  shiny::outputOptions(x = output,
                       name = "isDataSourceEnvironment",
                       suspendWhenHidden = FALSE)
  
  cohortDefinitionConceptSets <- shiny::reactive(x = {
    if (is.null(cohortDefinitionConceptSetExpressionRow())) {
      return(NULL)
    }
    
    data <-
      cohortDefinistionConceptSetExpression()$conceptSetExpressionDetails
    data <- data %>%
      dplyr::filter(.data$id == cohortDefinitionConceptSetExpressionRow()$id)
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
  
  getResolvedOrMappedConceptSetForAllDatabase <-
    shiny::reactive(x = {
      row <- selectedCohortDefinitionRow()
      if (is.null(row) ||
          is.null(cohortDefinitionConceptSetExpressionRow()$id)) {
        return(NULL)
      }
      
      output <-
        resolveMappedConceptSet(
          dataSource = dataSource,
          databaseIds = database$databaseId,
          cohortId =  row$cohortId
        )
      
      if (!is.null(output)) {
        return(output)
      } else {
        return(NULL)
      }
    })
  
  getResolvedOrMappedConceptSetForAllVocabulary <-
    shiny::reactive(x = {
      data <- NULL
      row <- selectedCohortDefinitionRow()
      if (is.null(row) ||
          is.null(cohortDefinitionConceptSetExpressionRow()$id)) {
        return(NULL)
      }
      outputResolved <- list()
      outputMapped <- list()
      for (i in (1:length(vocabularyDatabaseSchemas))) {
        vocabularyDatabaseSchema <- vocabularyDatabaseSchemas[[i]]
        output <-
          resolveMappedConceptSetFromVocabularyDatabaseSchema(
            dataSource = dataSource,
            conceptSets = conceptSets %>%
              dplyr::filter(cohortId == selectedCohortDefinitionRow()$cohortId),
            vocabularyDatabaseSchema = vocabularyDatabaseSchema
          )
        outputResolved <- output$resolved
        outputMapped <- output$mapped
        outputResolved$vocabularyDatabaseSchema <-
          vocabularyDatabaseSchema
        outputMapped$vocabularyDatabaseSchema <-
          vocabularyDatabaseSchema
      }
      outputResolved <- dplyr::bind_rows(outputResolved)
      outputMapped <- dplyr::bind_rows(outputMapped)
      return(list(resolved = outputResolved, mapped = outputMapped))
    })
  
  getIncludeOrSourceConcepts <- shiny::reactive({
    data <- NULL
    databaseIdToFilter <- database %>%
      dplyr::filter(.data$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema) %>%
      dplyr::pull(.data$databaseId)
    
    if (length(databaseIdToFilter) > 0) {
      resolvedOrMappedConceptSetForAllDatabase <-
        getResolvedOrMappedConceptSetForAllDatabase()
      if (!is.null(resolvedOrMappedConceptSetForAllDatabase) &&
          length(resolvedOrMappedConceptSetForAllDatabase) == 2) {
        source <-
          (input$conceptSetsType == "Mapped")
        if (source) {
          data <- resolvedOrMappedConceptSetForAllDatabase$mapped 
          if (!is.null(data) && nrow(data) > 0) {
            data <- data %>%
              dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionRow()$id) %>%
              dplyr::filter(.data$databaseId == !!databaseIdToFilter) %>%
              dplyr::select(-.data$databaseId, -.data$conceptSetId)
            data$resolvedConceptId <-
              as.factor(data$resolvedConceptId)
          } else {
            data <- NULL
          }
        } else {
          data <- resolvedOrMappedConceptSetForAllDatabase$resolved %>%
            dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionRow()$id) %>%
            dplyr::filter(.data$databaseId == !!databaseIdToFilter) %>%
            dplyr::select(-.data$databaseId, -.data$conceptSetId, -.data$cohortId)
        }
      }
    }
    
    if (exists("vocabularyDatabaseSchemas") &&
        !is.null(input$databaseOrVocabularySchema) &&
        length(input$databaseOrVocabularySchema) > 0) {
      vocabularyDataSchemaToFilter <-
        intersect(vocabularyDatabaseSchemas,
                  input$databaseOrVocabularySchema)
    } else {
      vocabularyDataSchemaToFilter <- NULL
    }
    
    if (length(vocabularyDataSchemaToFilter) > 0) {
      resolvedOrMappedConceptSetForAllVocabulary <-
        getResolvedOrMappedConceptSetForAllVocabulary()
      if (!is.null(resolvedOrMappedConceptSetForAllVocabulary) &&
          length(resolvedOrMappedConceptSetForAllVocabulary) == 2) {
        source <-
          (input$conceptSetsType == "Mapped")
        if (source) {
          data <- resolvedOrMappedConceptSetForAllVocabulary$mapped %>%
            dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionRow()$id) %>%
            dplyr::filter(.data$vocabularyDatabaseSchema == !!vocabularyDataSchemaToFilter) %>%
            dplyr::select(-.data$vocabularyDatabaseSchema, -.data$conceptSetId)
          data$resolvedConceptId <-
            as.factor(data$resolvedConceptId)
        } else {
          data <- resolvedOrMappedConceptSetForAllVocabulary$resolved %>%
            dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionRow()$id) %>%
            dplyr::filter(.data$vocabularyDatabaseSchema == !!vocabularyDataSchemaToFilter) %>%
            dplyr::select(-.data$vocabularyDatabaseSchema, -.data$conceptSetId)
        }
      }
    }
    if (!is.null(data) && nrow(data) > 0) {
      data$conceptClassId <- as.factor(data$conceptClassId)
      data$domainId <- as.factor(data$domainId)
      data$conceptCode <- as.factor(data$conceptCode)
      data$conceptId <- as.factor(data$conceptId)
      data$conceptName <- as.factor(data$conceptName)
      data$vocabularyId <- as.factor(data$vocabularyId)
      data$standardConcept <- as.factor(data$standardConcept)
    }
    return(data)
  })
  
  output$cohortDefinitionIncludedStandardConceptsTable <-
    DT::renderDataTable(expr = {
      data <- getIncludeOrSourceConcepts()
      if (is.null(data)) {
        return(dplyr::tibble("No included standard concepts."))
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
        columnDefs = list(
          truncateStringDef(1, 80)
        )
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
      return(dataTable)
    }, server = TRUE)
  
  output$cohortDefinitionIncludedSourceConceptsTable <-
    DT::renderDataTable(expr = {
      data <- getIncludeOrSourceConcepts()
      if (is.null(data)) {
        return("No included source concepts")
      }
      data <- data %>% 
        dplyr::mutate(conceptId = as.factor(.data$conceptId),
                      conceptName = as.factor(.data$conceptName),
                      vocabularyId = as.factor(.data$vocabularyId))
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
          truncateStringDef(2, 80)
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
  
  output$cohortDefinitionConceptSetsTable <-
    DT::renderDataTable(expr = {
      data <- cohortDefinitionConceptSets()
      if (is.null(cohortDefinitionConceptSets())) {
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
  
  output$cohortConceptsetExpressionJson <- shiny::renderText({
    if (is.null(cohortDefinitionConceptSetExpressionRow())) {
      return(NULL)
    }
    cohortDefinitionConceptSetExpressionRow()$json
  })
  
  output$saveConceptSetButton <- downloadHandler(
    filename = function() {
      paste("conceptSet-", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      data <- cohortDefinitionConceptSets()
      write.csv(data, file)
    }
  )
  
  getConceptSetIds <- shiny::reactive(x = {
    return(conceptSets$conceptSetId[conceptSets$conceptSetName  %in% input$conceptSetsToFilterCharacterization])
  })
  
  getResoledAndMappedConceptIdsForFilters <- shiny::reactive({
    output <-
      resolveMappedConceptSet(
        dataSource = dataSource,
        databaseIds = databaseIds(),
        cohortId = cohortId()
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
  
  # Cohort Counts ---------------------------------------------------------------------------
  output$cohortCountsTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getCohortCountResult(
      dataSource = dataSource,
      databaseIds = databaseIds(),
      cohortIds = cohortIds()
    ) %>%
      addShortName(cohort) %>%
      dplyr::select(
        .data$databaseId,
        .data$shortName,
        .data$cohortSubjects,
        .data$cohortEntries,
        .data$cohortId
      ) %>%
      dplyr::rename(cohort = .data$shortName) %>%
      dplyr::mutate(cohort = as.factor(.data$cohort))
    
    if (nrow(data) == 0) {
      return(tidyr::tibble("There is no data on any cohort"))
    }
    
    # instead maybe we can just convert this to a warning message in header.
    # if (!isTRUE(all.equal(
    #   data$databaseId %>% unique %>% sort(),
    #   databaseIds() %>% unique() %>% sort()
    # ))) {
    #   return(dplyr::tibble(
    #     Note = paste0(
    #       "There is no data for the databases:\n",
    #       paste0(
    #         setdiff(databaseIds(),
    #                 data$databaseId %>% unique()),
    #         collapse = ",\n "
    #       ),
    #       ".\n Please unselect them."
    #     )
    #   ))
    # }
    
    table <- dplyr::full_join(
      data %>%
        dplyr::select(.data$cohort, .data$databaseId,
                      .data$cohortSubjects) %>%
        dplyr::mutate(columnName = paste0(.data$databaseId, "_subjects")) %>%
        dplyr::arrange(.data$cohort, .data$databaseId) %>%
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
        dplyr::arrange(.data$cohort, .data$databaseId) %>%
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
    
    databaseIds <- sort(unique(data$databaseId))
    
    sketch <- htmltools::withTags(table(class = "display",
                                        thead(tr(
                                          th(rowspan = 2, "Cohort"),
                                          lapply(databaseIds, th, colspan = 2, class = "dt-center", style = "border-right:1px solid silver")
                                        ),
                                        tr(
                                          lapply(rep(
                                            c("Records", "Subjects"), length(databaseIds)
                                          ), th, style = "border-right:1px solid silver")
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
      scrollY = "50vh",
      columnDefs = list(minCellCountDef(1:(
        2 * length(databaseIds)
      )))
    )
    
    dataTable <- DT::datatable(
      table,
      options = options,
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
    return(dataTable)
  }, server = TRUE)
  
  # Incidence rate --------------------------------------------------------------------------------
  
  incidenceRateData <- reactive({
    if (!exists('incidenceRate')) {
      return(NULL)
    }
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    if (length(cohortIds()) > 0) {
      data <- getIncidenceRateResult(
        dataSource = dataSource,
        cohortIds = cohortIds(),
        databaseIds = databaseIds(),
        stratifyByGender =  stratifyByGender,
        stratifyByAgeGroup =  stratifyByAge,
        stratifyByCalendarYear =  stratifyByCalendarYear,
        minPersonYears = 1000
      ) %>%
        dplyr::mutate(
          incidenceRate = dplyr::case_when(.data$incidenceRate < 0 ~ 0,
                                           TRUE ~ .data$incidenceRate)
        )
    } else {
      data <- tidyr::tibble()
    }
    return(data)
  })
  
  shiny::observe({
    if (!is.null(incidenceRateData()) &&
        nrow(incidenceRateData()) > 0) {
      ageFilter <- incidenceRateData() %>%
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
  
  shiny::observe({
    if (!is.null(incidenceRateData()) &&
        nrow(incidenceRateData()) > 0) {
      genderFilter <- incidenceRateData() %>%
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
  
  shiny::observe({
    if (!is.null(incidenceRateData()) &&
        nrow(incidenceRateData()) > 0) {
      calenderFilter <- incidenceRateData() %>%
        dplyr::select(.data$calendarYear) %>%
        dplyr::filter(.data$calendarYear != "NA",
                      !is.na(.data$calendarYear)) %>%
        dplyr::distinct(.data$calendarYear) %>%
        dplyr::arrange(.data$calendarYear)
      
      minValue <- min(calenderFilter$calendarYear)
      
      maxValue <- max(calenderFilter$calendarYear)
      
      shiny::updateSliderInput(
        session = session,
        inputId = "incidenceRateCalenderFilter",
        min = minValue,
        max = maxValue,
        value = c(2010, maxValue)
      )
      
    }
  })
  
  incidenceRateAgeFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$incidenceRateAgeFilter_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$incidenceRateAgeFilter_open) ||
        !is.null(input$tabs)) {
      selectedIncidenceRateAgeFilter <- input$incidenceRateAgeFilter
      incidenceRateAgeFilter(selectedIncidenceRateAgeFilter)
    }
  })
  
  incidenceRateGenderFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$incidenceRateGenderFilter_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$incidenceRateGenderFilter_open) ||
        !is.null(input$tabs)) {
      selectedIncidenceRateGenderFilter <- input$incidenceRateGenderFilter
      incidenceRateGenderFilter(selectedIncidenceRateGenderFilter)
    }
  })
  
  incidenceRateCalenderFilter <- shiny::reactive({
    calenderFilter <- incidenceRateData() %>%
      dplyr::select(.data$calendarYear) %>%
      dplyr::filter(.data$calendarYear != "NA",
                    !is.na(.data$calendarYear)) %>%
      dplyr::distinct(.data$calendarYear) %>%
      dplyr::arrange(.data$calendarYear)
    calenderFilter <-
      calenderFilter[calenderFilter$calendarYear >= input$incidenceRateCalenderFilter[1] &
                       calenderFilter$calendarYear <= input$incidenceRateCalenderFilter[2], , drop = FALSE] %>%
      dplyr::pull(.data$calendarYear)
    return(calenderFilter)
  })
  
  output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    data <- incidenceRateData()
    
    if (stratifyByAge && !"All" %in% incidenceRateAgeFilter()) {
      data <- data %>%
        dplyr::filter(.data$ageGroup %in% incidenceRateAgeFilter())
    }
    if (stratifyByGender &&
        !"All" %in% incidenceRateGenderFilter()) {
      data <- data %>%
        dplyr::filter(.data$gender %in% incidenceRateGenderFilter())
    }
    if (stratifyByCalendarYear) {
      data <- data %>%
        dplyr::filter(.data$calendarYear %in% incidenceRateCalenderFilter())
    }
    
    validate(need(nrow(data) > 0, paste0("No data for this combination")))
    
    plot <- plotIncidenceRate(
      data = data,
      shortNameRef = cohort,
      stratifyByAgeGroup = stratifyByAge,
      stratifyByGender = stratifyByGender,
      stratifyByCalendarYear = stratifyByCalendarYear,
      yscaleFixed = input$irYscaleFixed
    )
    return(plot)
  })
  
  # Time distribution -----------------------------------------------------------------------------
  timeDist <- reactive({
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getTimeDistributionResult(
      dataSource = dataSource,
      cohortIds = cohortIds(),
      databaseIds = databaseIds()
    )
    return(data)
  })
  
  output$timeDisPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    data <- timeDist()
    validate(need(nrow(data) > 0, paste0("No data for this combination")))
    plot <- plotTimeDistribution(data = data, shortNameRef = cohort)
    return(plot)
  })
  
  output$timeDistTable <- DT::renderDataTable(expr = {
    data <- timeDist()  %>%
      addShortName(cohort) %>%
      dplyr::arrange(.data$databaseId, .data$cohortId) %>%
      dplyr::mutate(
        shortName = as.factor(.data$shortName),
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
    
    
    if (is.null(data) || nrow(data) == 0) {
      return(dplyr::tibble(
        Note = paste0("No data available for selected databases and cohorts")
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
  
  # included concepts table --------------------------------------------------------------------------
  output$includedConceptsTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    if (is.null(cohortId()) || length(cohortId()) == 0) {
      return(dplyr::tibble("No data available for selected databases and cohorts"))
    }
    data <- getIncludedConceptResult(
      dataSource = dataSource,
      cohortId = cohortId(),
      databaseIds = databaseIds()
    )
    if (is.null(data) || nrow(data) == 0) {
      return(dplyr::tibble("No data available for selected databases and cohorts"))
    }
    
    if (!is.null(input$conceptSetsToFilterCharacterization) && length(input$conceptSetsToFilterCharacterization) > 0) {
      
      if (!is.null(input$conceptSetsToFilterCharacterization)) {
        if (length(getResoledAndMappedConceptIdsForFilters()) > 0) {
          data <- data %>% 
            dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
        } else {
          data <- data[0,]
        }
      }
    }
    
    if (nrow(data) == 0) {
      return(dplyr::tibble("No data available for selected databases and cohorts"))
    }
    databaseIds <- unique(data$databaseId)
    cohortCounts <- data %>% 
      dplyr::inner_join(cohortCount) %>% 
      dplyr::filter(.data$cohortId == cohortId()) %>% 
      dplyr::filter(.data$databaseId %in% databaseIds()) %>% 
      dplyr::select(.data$cohortSubjects) %>% 
      dplyr::pull(.data$cohortSubjects) %>% unique()
    
    databaseIdsWithCount <- paste(databaseIds, "(n = ", format(cohortCounts, big.mark = ","), ")")
    # if (!all(databaseIds() %in% databaseIds)) {
    #   return(dplyr::tibble(
    #     Note = paste0(
    #       "There is no data for the databases:\n",
    #       paste0(setdiff(databaseIds(), databaseIds),
    #              collapse = ",\n "),
    #       ".\n Please unselect them."
    #     )
    #   ))
    # }
    
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    
    if (input$includedType == "Source fields") {
      table <- data %>%
        dplyr::filter(.data$sourceConceptId > 0) %>%
        dplyr::select(
          .data$databaseId,
          .data$sourceConceptId,
          .data$conceptSubjects,
          .data$conceptCount
        ) %>%
        dplyr::group_by(.data$databaseId,.data$sourceConceptId) %>% 
        dplyr::summarise(conceptSubjects = max(.data$conceptSubjects),
                         conceptCount = max(.data$conceptCount)) %>% 
        dplyr::ungroup() %>% 
        dplyr::arrange(.data$databaseId) %>%
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
          id_cols = c(.data$sourceConceptId),
          names_from = .data$name,
          values_from = .data$value,
          values_fill = 0
        ) %>%
        dplyr::inner_join(
          data %>%
            dplyr::select(
              .data$sourceConceptId,
              .data$sourceConceptName,
              .data$sourceVocabularyId,
              .data$sourceConceptCode
            ) %>%
            dplyr::distinct(),
          by = "sourceConceptId"
        ) %>%
        dplyr::relocate(
          .data$sourceConceptId,
          .data$sourceConceptName,
          .data$sourceVocabularyId,
          .data$sourceConceptCode
        )
      
      if (nrow(table) == 0) {
        return(dplyr::tibble(
          Note = paste0("No data available for selected databases and cohorts")
        ))
      }
      table <- table[order(-table[, 5]), ]
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(
                                            tr(
                                              th(rowspan = 2, 'Concept ID'),
                                              th(rowspan = 2, 'Concept Name'),
                                              th(rowspan = 2, 'Vocabulary ID'),
                                              th(rowspan = 2, 'Concept Code'),
                                              lapply(databaseIdsWithCount, th, colspan = 2, class = "dt-center", style = "border-right:1px solid silver")
                                            ),
                                            tr(lapply(rep(
                                              c("Subjects", "Records"), length(databaseIds)
                                            ), th, style = "border-right:1px solid silver"))
                                          )))
      options = list(
        pageLength = 1000,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        scrollX = TRUE,
        scrollY = "50vh",
        lengthChange = TRUE,
        searchHighlight = TRUE,
        ordering = TRUE,
        paging = TRUE,
        columnDefs = list(truncateStringDef(1, 100),
                          minCellCountDef(3 + (
                            1:(length(databaseIds) * 2)
                          )))
      )
      
      dataTable <- DT::datatable(
        table,
        colnames = colnames(table),
        options = options,
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns =  4 + (1:(length(databaseIds) * 2)),
        background = DT::styleColorBar(c(0, maxCount), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
    } else {
      table <- data %>%
        dplyr::filter(.data$conceptId > 0) %>%
        dplyr::select(
          .data$databaseId,
          .data$conceptId,
          .data$conceptSubjects,
          .data$conceptCount
        ) %>%
        dplyr::group_by(.data$databaseId,
                        .data$conceptId) %>%
        dplyr::summarise(
          conceptSubjects = sum(.data$conceptSubjects),
          conceptCount = sum(.data$conceptCount)
        ) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(.data$databaseId) %>%
        tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>%
        dplyr::mutate(name = paste0(
          databaseId,
          "_",
          stringr::str_replace(
            string = .data$name,
            pattern = "concept",
            replacement = ""
          )
        )) %>%
        tidyr::pivot_wider(
          id_cols = c(.data$conceptId),
          names_from = .data$name,
          values_from = .data$value,
          values_fill = 0
        ) %>%
        dplyr::inner_join(
          data %>%
            dplyr::select(.data$conceptId,
                          .data$conceptName,
                          .data$vocabularyId) %>%
            dplyr::distinct(),
          by = "conceptId"
        ) %>%
        dplyr::relocate(.data$conceptId, .data$conceptName, .data$vocabularyId)
      
      if (nrow(table) == 0) {
        return(dplyr::tibble(
          Note = paste0('No data available for selected databases and cohorts')
        ))
      }
      
      table <- table[order(-table[, 4]), ]
      
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(
                                            tr(
                                              th(rowspan = 2, "Concept ID"),
                                              th(rowspan = 2, "Concept Name"),
                                              th(rowspan = 2, "Vocabulary ID"),
                                              lapply(databaseIdsWithCount, th, colspan = 2, class = "dt-center", style = "border-right:1px solid silver")
                                            ),
                                            tr(lapply(rep(
                                              c("Subjects", "Records"), length(databaseIds)
                                            ), th, style = "border-right:1px solid silver"))
                                          )))
      
      options = list(
        pageLength = 1000,
        lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
        searching = TRUE,
        scrollX = TRUE,
        scrollY = "50vh",
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        columnDefs = list(truncateStringDef(1, 100),
                          minCellCountDef(2 + (
                            1:(length(databaseIds) * 2)
                          )))
      )
      
      dataTable <- DT::datatable(
        table,
        options = options,
        colnames = colnames(table),
        rownames = FALSE,
        container = sketch,
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns =  3 + (1:(length(databaseIds) *
                             2)),
        background = DT::styleColorBar(c(0, maxCount), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
    }
    return(dataTable)
  }, server = TRUE)
  
  # orphan concepts table -------------------------------------------------------------------------
  output$orphanConceptsTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    
    if (is.null(cohortId()) || length(cohortId()) == 0) {
      return(dplyr::tibble(Note = paste0(
        "There is no data for the selected combination."
      )))
    }
    
    data <- getOrphanConceptResult(
      dataSource = dataSource,
      cohortId = cohortId(),
      databaseIds = databaseIds()
    )
    if (is.null(data) || nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0(
        "There is no data for the selected combination."
      )))
    }
    
    if (!is.null(input$conceptSetsToFilterCharacterization) && length(input$conceptSetsToFilterCharacterization) > 0) {
      
      if (!is.null(input$conceptSetsToFilterCharacterization)) {
        if (length(conceptSetIds()) > 0) {
          data <- data %>% 
            dplyr::filter(.data$conceptSetId %in% conceptSetIds())
        } else {
          data <- data[0,]
        }
      }
    }
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0(
        "There is no data for the selected combination."
      )))
    }
    databaseIds <- unique(data$databaseId)
    
    cohortCounts <- data %>% 
      dplyr::inner_join(cohortCount) %>% 
      dplyr::filter(.data$cohortId == cohortId()) %>% 
      dplyr::filter(.data$databaseId %in% databaseIds()) %>% 
      dplyr::select(.data$cohortSubjects) %>% 
      dplyr::pull(.data$cohortSubjects) %>% unique()
    
    databaseIdsWithCount <- paste(databaseIds, "(n = ", format(cohortCounts, big.mark = ","), ")")
    # if (!all(databaseIds() %in% databaseIds)) {
    #   return(dplyr::tibble(
    #     Note = paste0(
    #       "There is no data for the databases:\n",
    #       paste0(setdiff(databaseIds(), databaseIds),
    #              collapse = ",\n "),
    #       ".\n Please unselect them."
    #     )
    #   ))
    # }
    
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    
    table <- data %>%
      dplyr::select(.data$databaseId,
                    .data$conceptId,
                    .data$conceptSubjects,
                    .data$conceptCount) %>%
      dplyr::group_by(.data$databaseId,
                      .data$conceptId) %>%
      dplyr::summarise(
        conceptSubjects = sum(.data$conceptSubjects),
        conceptCount = sum(.data$conceptCount)
      ) %>%
      dplyr::ungroup() %>%
      dplyr::arrange(.data$databaseId) %>%
      tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>%
      dplyr::mutate(name = paste0(
        databaseId,
        "_",
        stringr::str_replace(
          string = .data$name,
          pattern = "concept",
          replacement = ""
        )
      )) %>%
      tidyr::pivot_wider(
        id_cols = c(.data$conceptId),
        names_from = .data$name,
        values_from = .data$value,
        values_fill = 0
      ) %>%
      dplyr::inner_join(
        data %>%
          dplyr::select(
            .data$conceptId,
            .data$conceptName,
            .data$vocabularyId,
            .data$conceptCode
          ) %>%
          dplyr::distinct(),
        by = "conceptId"
      ) %>%
      dplyr::relocate(.data$conceptId,
                      .data$conceptName,
                      .data$vocabularyId,
                      .data$conceptCode)
    
    if (nrow(table) == 0) {
      return(dplyr::tibble(
        Note = paste0('No data available for selected databases and cohorts')
      ))
    }
    
    table <- table[order(-table[, 5]), ]
    
    sketch <- htmltools::withTags(table(class = "display",
                                        thead(
                                          tr(
                                            th(rowspan = 2, "Concept ID"),
                                            th(rowspan = 2, "Concept Name"),
                                            th(rowspan = 2, "Vocabulary ID"),
                                            th(rowspan = 2, "Concept Code"),
                                            lapply(databaseIdsWithCount, th, colspan = 2, class = "dt-center", style = "border-right:1px solid silver")
                                          ),
                                          tr(lapply(rep(
                                            c("Subjects", "Counts"), length(databaseIds)
                                          ), th, style = "border-right:1px solid silver"))
                                        )))
    
    options = list(
      pageLength = 1000,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      scrollX = TRUE,
      scrollY = "50vh",
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      columnDefs = list(truncateStringDef(1, 100),
                        minCellCountDef(3 + (1:(
                          length(databaseIds) * 2
                        ))))
    )
    
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
    
    table <- DT::formatStyle(
      table = table,
      columns =  4 + (1:(length(databaseIds) * 2)),
      background = DT::styleColorBar(c(0, maxCount), "lightblue"),
      backgroundSize = "98% 88%",
      backgroundRepeat = "no-repeat",
      backgroundPosition = "center"
    )
    return(table)
  }, server = TRUE)
  
  # Inclusion rules table -----------------------------------------------------------------------
  output$inclusionRuleTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    table <- getInclusionRuleStats(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = databaseIds()
    )
    if (nrow(table) == 0) {
      return(dplyr::tibble(
        Note = paste0("No data available for selected databases and cohorts")
      ))
    }
    
    databaseIds <- unique(table$databaseId)
    
    # if (!all(databaseIds() %in% databaseIds)) {
    #   return(dplyr::tibble(
    #     Note = paste0(
    #       "There is no data for the databases:\n",
    #       paste0(setdiff(databaseIds(), databaseIds),
    #              collapse = ",\n "),
    #       ".\n Please unselect them."
    #     )
    #   ))
    # }
    
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
                                          lapply(databaseIds, th, colspan = 4, class = "dt-center", style = "border-right:1px solid silver")
                                        ),
                                        tr(
                                          lapply(rep(
                                            c("Meet", "Gain", "Remain", "Total"), length(databaseIds)
                                          ), th, style = "border-right:1px solid silver")
                                        ))))
    
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      searchHighlight = TRUE,
      scrollX = TRUE,
      lengthChange = TRUE,
      ordering = TRUE,
      paging = TRUE,
      columnDefs = list(minCellCountDef(1 + (1:(
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
    
    # table <- DT::formatStyle(table = table,
    #                          columns = 2 + (1:(length(databaseIds) * 4)),
    #                          background = DT::styleColorBar(lims, "lightblue"),
    #                          backgroundSize = "98% 88%",
    #                          backgroundRepeat = "no-repeat",
    #                          backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  # Index event breakdown ----------------------------------------------------------------
  indexEventBreakDownData <- shiny::reactive(x = {
    if (length(cohortId()) > 0 &&
        length(databaseIds()) > 0) {
      data <- getIndexEventBreakdown(
        dataSource = dataSource,
        cohortIds = cohortId(),
        databaseIds = databaseIds()
      )
      if (!is.null(data)) {
        if (!'domainTable' %in% colnames(data)) {
          data$domainTable <- "Not in data"
        }
        if (!'domainField' %in% colnames(data)) {
          data$domainField <- "Not in data"
        }
        return(data)
      } else {
        return(NULL)
      }
    } else {
      return(NULL)
    }
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
  
  selectedDomainTable <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$breakdownDomainTable_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$breakdownDomainTable_open) ||
        !is.null(input$tabs)) {
      selectedDomainTable(input$breakdownDomainTable)
    }
  })
  
  selectedDomainField <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(input$breakdownDomainField_open,
         input$tabs)
  }, handlerExpr = {
    if (isFALSE(input$breakdownDomainField_open) ||
        !is.null(input$tabs)) {
      selectedDomainField(input$breakdownDomainField)
    }
  })
  
  output$breakdownTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen chosen"))
    data <- indexEventBreakDownDataFilteredByRadioButton()
    if (is.null(data) || nrow(data) == 0) {
      return(dplyr::tibble("No data for the combination."))
    }
    data <- data %>%
      # dplyr::filter(.data$domainTable %in% selectedDomainTable()) %>%
      # dplyr::filter(.data$domainField %in% selectedDomainField()) %>%
      dplyr::select(
        -.data$domainTable,
        .data$domainField,
        -.data$domainId,
        #-.data$vocabularyId,-.data$standardConcept
      )
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(
        Note = paste0("No data available for selected databases and cohorts")
      ))
    }
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    databaseIds <- unique(data$databaseId)
    
    cohortCounts <- data %>% 
      dplyr::inner_join(y = cohortCount, by = c("cohortId", "databaseId")) %>% 
      dplyr::filter(.data$cohortId == cohortId()) %>% 
      dplyr::filter(.data$databaseId %in% databaseIds()) %>% 
      dplyr::select(.data$cohortSubjects) %>% 
      dplyr::pull(.data$cohortSubjects) %>% unique()
    
    databaseIdsWithCount <- paste(databaseIds, "(n = ", format(cohortCounts, big.mark = ","), ")")
    
    if (!"subjectCount" %in% names(data)) {
      data$subjectCount <- 0
    }
    
    if (input$indexEventBreakdownTableFilter == "Both") {
      data <- data %>%
        dplyr::arrange(.data$databaseId) %>%
        dplyr::select(
          .data$conceptId,
          .data$conceptName,
          .data$domainField,
          .data$databaseId,
          .data$vocabularyId,
          .data$conceptCount,
          .data$subjectCount 
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
      
      sketch <- htmltools::withTags(table(class = "display",
                                          thead(
                                            tr(
                                              th(rowspan = 2, "Concept Id"),
                                              th(rowspan = 2, "Concept Name"),
                                              th(rowspan = 2, "Domain field"),
                                              th(rowspan = 2, "Vocabulary Id"),
                                              lapply(databaseIdsWithCount, th, colspan = 2, class = "dt-center", style = "border-right:1px solid silver")
                                            ),
                                            tr(lapply(rep(
                                              c("Records", "Persons"), length(databaseIds)
                                            ), th, style = "border-right:1px solid silver"))
                                          )))
      
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
        columnDefs = list(minCellCountDef(3 + 1:(
          length(databaseIds) * 2
        )), truncateStringDef(1, 80))
      )
      
      dataTable <- DT::datatable(
        data,
        options = options,
        rownames = FALSE,
        container = sketch,
        colnames = colnames(data) %>%
          camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns = 4 + 1:(length(databaseIds) * 2),
        background = DT::styleColorBar(c(0, maxCount), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
    } else if (input$indexEventBreakdownTableFilter == "Records" || input$indexEventBreakdownTableFilter == "Persons") {
      data <-  data %>%
        dplyr::arrange(.data$databaseId) %>%
        dplyr::filter(.data$conceptId > 0) %>%
        dplyr::distinct() # distinct is needed here because many time condition_concept_id and condition_source_concept_id
        # may have the same value
      
      if (input$indexEventBreakdownTableFilter == "Records") {
        data <- data %>% 
          tidyr::pivot_wider(
            id_cols = c("conceptId",
                        "conceptName",
                        "domainField",
                        "vocabularyId"),
            names_from = "databaseId",
            values_from = "conceptCount",
            values_fill = 0
          )
      } else {
        data <- data %>% 
          tidyr::pivot_wider(
            id_cols = c("conceptId",
                        "conceptName",
                        "domainField",
                        "vocabularyId"),
            names_from = "databaseId",
            values_from = "subjectCount",
            values_fill = 0
          )
      }
      
      data <- data[order(-data[5]), ]
      
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
        columnDefs = list(minCellCountDef(3 + 1:(length(databaseIds))))
      )
      
      dataTable <- DT::datatable(
        data,
        options = options,
        rownames = FALSE,
        colnames = colnames(data) %>%
          camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      
      dataTable <- DT::formatStyle(
        table = dataTable,
        columns = 4 + 1:(length(databaseIds)),
        background = DT::styleColorBar(c(0, maxCount), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
    }
    
    return(dataTable)
  }, server = TRUE)
  
  # Visit Context ---------------------------------------------------------------------------------------------
  output$visitContextTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    data <- getVisitContextResults(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = databaseIds()
    )
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(
        Note = paste0("No data available for selected databases and cohort")
      ))
    }
    
    databaseIds <- sort(unique(data$databaseId))
    cohortCounts <- data %>% 
      dplyr::inner_join(cohortCount) %>% 
      dplyr::filter(.data$cohortId == cohortId()) %>% 
      dplyr::filter(.data$databaseId %in% databaseIds()) %>% 
      dplyr::select(.data$cohortSubjects) %>% 
      dplyr::pull(.data$cohortSubjects) %>% unique()
    
    databaseIdsWithCount <- paste(databaseIds, "(n = ", format(cohortCounts, big.mark = ","), ")")
    
    # if (!all(databaseIds() %in% databaseIds)) {
    #   return(dplyr::tibble(
    #     Note = paste0(
    #       "There is no data for the databases:\n",
    #       paste0(setdiff(databaseIds(), databaseIds),
    #              collapse = ",\n "),
    #       ".\n Please unselect them."
    #     )
    #   ))
    # }
    
    maxSubjects <- max(data$subjects)
    visitContextReference <-
      expand.grid(
        visitContext = c("Before", "During visit", "On visit start", "After"),
        visitConceptName = unique(data$visitConceptName),
        databaseId = databaseIds
      ) %>%
      tidyr::tibble()
    
    table <- visitContextReference %>%
      dplyr::left_join(data,
                       by = c("visitConceptName", "visitContext", "databaseId")) %>%
      dplyr::select(.data$visitConceptName,
                    .data$visitContext,
                    .data$subjects,
                    .data$databaseId) %>%
      dplyr::mutate(visitContext = paste0(.data$databaseId, "_", .data$visitContext)) %>%
      dplyr::select(-.data$databaseId) %>%
      dplyr::arrange(.data$visitConceptName) %>%
      tidyr::pivot_wider(
        id_cols = c(.data$visitConceptName),
        names_from = .data$visitContext,
        values_from = .data$subjects,
        values_fill = 0
      ) %>%
      dplyr::relocate(.data$visitConceptName) %>%
      dplyr::mutate(visitConceptName = as.factor(visitConceptName))
    
    sketch <- htmltools::withTags(table(class = "display",
                                        thead(tr(
                                          th(rowspan = 2, "Visit"),
                                          lapply(databaseIdsWithCount, th, colspan = 4, class = "dt-center",style = "border-right:1px solid black")
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
                                          ), th,style = "border-right:1px solid black")
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
      columnDefs = list(truncateStringDef(0, 60),
                        list(width = "40%", targets = 0),
                        minCellCountDef(1:(
                          length(databaseIds) * 4
                        )))
    )
    
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
    
    table <- DT::formatStyle(
      table = table,
      columns = 1:(length(databaseIds) * 4),
      background = DT::styleColorBar(c(0, maxSubjects), "lightblue"),
      backgroundSize = "98% 88%",
      backgroundRepeat = "no-repeat",
      backgroundPosition = "center"
    )
    
  }, server = TRUE)
  
  # Characterization -------------------------------------------------
  getConceptSetNameForFilter <- shiny::reactive(x = {
    if (length(cohortId()) == 0 || length(databaseIds()) == 0) {
      return(NULL)
    }
    
    jsonExpression <- cohortSubset() %>% 
      dplyr::filter(.data$cohortId == cohortId()) %>% 
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
  
  characterizationTableData <- shiny::reactive(x = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    if (input$charType == "Pretty") {
      analysisIds <- prettyAnalysisIds
    } else {
      analysisIds <- NULL
    }
    data <- getCovariateValueResult(
      dataSource = dataSource,
      analysisIds = analysisIds,
      cohortIds = cohortId(),
      databaseIds = databaseIds(),
      isTemporal = FALSE
    )
    
    if (!is.null(data)) {
      if (input$charType == "Raw" &&
          input$charProportionOrContinuous == "Proportion") {
        data <- data %>%
          dplyr::filter(.data$isBinary == 'Y')
      } else if (input$charType == "Raw" &&
                 input$charProportionOrContinuous == "Continuous") {
        data <- data %>%
          dplyr::filter(.data$isBinary == 'N')
      }
      return(data)
    } else {
      return(NULL)
    }
  })
  
  shiny::observe({
    subset <-
      characterizationTableData()$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "characterizationAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  shiny::observe({
    subset <-
      characterizationTableData()$domainId %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "characterizationDomainNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  shiny::observe({
    subset <- getConceptSetNameForFilter()$name %>% sort() %>% unique()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "conceptSetsToFilterCharacterization",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset
    )
  })
  
  
  output$characterizationTable <- DT::renderDataTable(expr = {
    data <- characterizationTableData()
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(
        Note = paste0("No data available for selected databases and cohorts")
      ))
    }
    
    databaseIds <- sort(unique(data$databaseId))
    
    cohortCounts <- data %>% 
      dplyr::inner_join(cohortCount) %>% 
      dplyr::filter(.data$cohortId == cohortId()) %>% 
      dplyr::filter(.data$databaseId %in% databaseIds()) %>% 
      dplyr::select(.data$cohortSubjects) %>% 
      dplyr::pull(.data$cohortSubjects) %>% unique()
    
    databaseIdsWithCount <- paste(databaseIds, "(n = ", format(cohortCounts, big.mark = ","), ")")
    # if (!all(databaseIds() %in% databaseIds)) {
    #   return(dplyr::tibble(
    #     Note = paste0(
    #       "There is no data for the databases:\n",
    #       paste0(setdiff(databaseIds(), databaseIds),
    #              collapse = ",\n "),
    #       ".\n Please unselect them."
    #     )
    #   ))
    # }
    
    if (input$charType == "Pretty") {
      countData <- getCohortCountResult(
        dataSource = dataSource,
        databaseIds = databaseIds(),
        cohortIds = cohortId()
      ) %>%
        dplyr::arrange(.data$databaseId)
      
      table <- data %>%
        prepareTable1()
      if (nrow(table) == 0) {
        return(dplyr::tibble(Note = "There is no data to return."))
      }
      
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
            cohortId = sort(cohortId())[[1]],
            databaseId = sort(databaseIds[[1]])
          ),
        characteristics %>%
          dplyr::filter(.data$header == 0) %>%
          tidyr::crossing(dplyr::tibble(databaseId = databaseIds)) %>%
          tidyr::crossing(dplyr::tibble(cohortId = cohortId()))
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
      
      if (nrow(data) == 0) {
        return(dplyr::tibble(
          Note = paste0("No data available for selected databases and cohorts")
        ))
      }
      
      if (input$characterizationColumnFilters == "Mean and Standard Deviation") {
        data <- data %>%
          dplyr::arrange(.data$databaseId, .data$cohortId) %>%
          tidyr::pivot_longer(cols = c(.data$mean, .data$sd))
      } else {
        data <- data %>%
          dplyr::arrange(.data$databaseId, .data$cohortId) %>%
          tidyr::pivot_longer(cols = c(.data$mean))
      }
      
       data <-  data %>% 
        dplyr::mutate(name = paste0(databaseId, "_", .data$name)) %>%
        tidyr::pivot_wider(
          id_cols = c(.data$cohortId, .data$covariateId),
          names_from = .data$name,
          values_from = .data$value
        ) %>%
        dplyr::inner_join(
          data %>% dplyr::select(
            .data$covariateId,
            .data$covariateName,
            .data$conceptId
          ) %>%
            dplyr::distinct(),
          by = "covariateId"
        ) %>%
        dplyr::mutate(covariateName = paste(.data$covariateName, "(",.data$conceptId, ")")) %>% 
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
                                              lapply(databaseIdsWithCount, th, colspan = 2, class = "dt-center", style = "border-right:1px solid silver")
                                            ),
                                            tr(
                                              lapply(rep(
                                                c("Mean", "SD"), length(databaseIds)
                                              ), th, style = "border-right:1px solid silver")
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
        
        table <- DT::datatable(
          data,
          options = options,
          rownames = FALSE,
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
  
  # Temporal characterization -----------------------------------------------------------------
  temporalAnalysisNameFilter <- shiny::reactive(x = {
    return(input$temporalAnalysisNameFilter)
  })
  
  temporalDomainNameFilter <- shiny::reactive(x = {
    return(input$temporalDomainNameFilter)
  })
  
  temporalCharacterization <- shiny::reactive({
    validate(need(length(timeIds()) > 0, "No time periods selected"))
    validate(need(length(input$database) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    data <- getCovariateValueResult(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = input$database,
      timeIds = timeIds(),
      isTemporal = TRUE
    ) %>%
      dplyr::select(-.data$choices) %>% 
      dplyr::inner_join(temporalCovariateChoices, by = "timeId") %>%
      dplyr::arrange(.data$timeId) %>%
      dplyr::select(-.data$cohortId, -.data$databaseId, -.data$covariateId)
    
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
      } else {
        data <- data[0,]
      }
    }
    return(data)
  })
  
  shiny::observe({
    subset <-
      temporalCharacterization()$analysisName %>% unique() %>% sort()
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
      temporalCharacterization()$domainId %>% unique() %>% sort()
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
      data <- temporalCharacterization() %>%
        dplyr::filter(.data$analysisName %in% temporalAnalysisNameFilter()) %>%
        dplyr::filter(.data$domainId %in% temporalDomainNameFilter())
      if (nrow(data) == 0) {
        return(dplyr::tibble(
          Note = paste0("No data available for selected databases and cohorts")
        ))
      }
      
      table <- data %>%
        dplyr::mutate(covariateName = paste(.data$covariateName, "(", .data$conceptId, ")")) %>% 
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
        dplyr::filter(.data$timeId %in% c(timeIds())) %>%
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
  
  #Cohort Overlap ------------------------
  cohortOverlap <- reactive({
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    combisOfTargetComparator <-
      tidyr::crossing(targetCohortId = cohortIds(),
                      comparatorCohortId = cohortIds()) %>%
      dplyr::filter(!.data$targetCohortId == .data$comparatorCohortId) %>%
      dplyr::distinct()
    validate(need(
      nrow(combisOfTargetComparator) > 0,
      paste0("Please select at least two cohorts.")
    ))
    
    data <- getCohortOverlapResult(
      dataSource = dataSource,
      targetCohortIds = combisOfTargetComparator$targetCohortId,
      comparatorCohortIds = combisOfTargetComparator$comparatorCohortId,
      databaseIds = databaseIds()
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
      length(cohortIds()) > 0,
      paste0("Please select Target Cohort(s)")
    ))
    
    data <- cohortOverlap()
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
  
  # Compare cohort characteristics --------------------------------------------
  charCompareAnalysisNameFilter <- shiny::reactive(x = {
    return(input$charCompareAnalysisNameFilter)
  })
  
  charaCompareDomainNameFilter <- shiny::reactive(x = {
    return(input$charaCompareDomainNameFilter)
  })
  
  computeBalance <- shiny::reactive({
    validate(need((length(cohortId(
    )) > 0), paste0("Please select cohort.")))
    validate(need((length(
      comparatorCohortId()
    ) > 0), paste0("Please select comparator cohort.")))
    validate(need((comparatorCohortId() != cohortId()),
                  paste0("Please select different cohort and comparator.")
    ))
    validate(need((length(input$database) > 0),
                  paste0("Please select atleast one datasource.")
    ))
    covs1 <- getCovariateValueResult(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = input$database,
      isTemporal = FALSE
    )
    
    covs2 <- getCovariateValueResult(
      dataSource = dataSource,
      cohortIds = comparatorCohortId(),
      databaseIds = input$database,
      isTemporal = FALSE
    )
    
    if (is.null(covs1) || is.null(covs2)) {
      return(NULL)
    }
    
    balance <- compareCohortCharacteristics(covs1, covs2) %>%
      dplyr::mutate(absStdDiff = abs(.data$stdDiff))
    
    if (input$charCompareType == "Raw table" &&
        input$charCompareProportionOrContinuous == "Proportion") {
      balance <- balance %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else if (input$charCompareType == "Raw table" &&
               input$charCompareProportionOrContinuous == "Continuous") {
      balance <- balance %>%
        dplyr::filter(.data$isBinary == 'N')
    }
    return(balance)
  })
  
  shiny::observe({
    subset <- computeBalance()$analysisName %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "charCompareAnalysisNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  shiny::observe({
    subset <- computeBalance()$domainId %>% unique() %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "charaCompareDomainNameFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  output$charCompareTable <- DT::renderDataTable(expr = {
    balance <- computeBalance()
    if (nrow(balance) == 0) {
      return(dplyr::tibble(Note = "No data for the selected combination."))
    }
    
    if (input$charCompareType == "Pretty table") {
      table <- prepareTable1Comp(balance)
      if (nrow(table) > 0) {
        table <- table %>%
          dplyr::arrange(.data$sortOrder) %>%
          dplyr::select(-.data$sortOrder) %>%
          dplyr::select(-.data$cohortId1, -.data$cohortId2)
      } else {
        return(dplyr::tibble(Note = "No data for covariates that are part of pretty table."))
      }
      
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
        colnames = c("Characteristic", "Target", "Comparator", "Std. Diff."),
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
      balance <- balance %>%
        dplyr::filter(.data$analysisName %in% charCompareAnalysisNameFilter()) %>%
        dplyr::filter(.data$domainId %in% charaCompareDomainNameFilter())
      
      if (!is.null(input$conceptSetsToFilterCharacterization)) {
        balance <- balance %>% 
          dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
      }
      
      table <- balance %>%
        dplyr::select(
          .data$covariateName,
          .data$conceptId,
          .data$mean1,
          .data$sd1,
          .data$mean2,
          .data$sd2,
          .data$absStdDiff
        ) %>%
        dplyr::mutate(covariateName =  paste(.data$covariateName, "(", .data$conceptId, ")")) %>% 
        dplyr::arrange(desc(absStdDiff)) %>% 
        dplyr::select(-.data$conceptId) %>% 
        dplyr::relocate(.data$covariateName,
                        .data$mean1,
                        .data$sd1,
                        .data$mean2,
                        .data$sd2,
                        .data$absStdDiff)
      
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
        columnDefs = list(truncateStringDef(0, 80),
                          minCellRealDef(1:5, digits = 2))
      )
      
      table <- DT::datatable(
        table,
        options = options,
        rownames = FALSE,
        colnames = c(
          "Covariate Name",
          "Mean Target",
          "SD Target",
          "Mean Comparator",
          "SD Comparator",
          "StdDiff"
        ),
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      table <- DT::formatStyle(
        table = table,
        columns = c(2, 4),
        background = DT::styleColorBar(c(0, 1), "lightblue"),
        backgroundSize = "98% 88%",
        backgroundRepeat = "no-repeat",
        backgroundPosition = "center"
      )
      table <- DT::formatStyle(
        table = table,
        columns = 6,
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
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = "No data for the selected combination."))
    }
    data <- data %>%
      dplyr::filter(.data$analysisName %in% charCompareAnalysisNameFilter()) %>%
      dplyr::filter(.data$domainId %in% charaCompareDomainNameFilter())
    
    if (!is.null(input$conceptSetsToFilterCharacterization)) {
      if (length(getResoledAndMappedConceptIdsForFilters()) > 0) {
        data <- data %>% 
          dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
      } else {
        data <- data[0,]
      }
    }
    
    if (input$charCompareType == "Plot" &&
        input$charCompareProportionOrContinuous == "Proportion") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else if (input$charCompareType == "Plot" &&
               input$charCompareProportionOrContinuous == "Continuous") {
      data <- data %>%
        dplyr::filter(.data$isBinary == 'N')
    }
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = "No data for the selected combination."))
    }
    plot <-
      plotCohortComparisonStandardizedDifference(
        balance = data,
        shortNameRef = cohort,
        xLimitMin = 0,
        xLimitMax = 1,
        yLimitMin = 0,
        yLimitMax = 1
      )
    return(plot)
  })
  
  #Compare Temporal Characterization.-----------------------------------------
  temporalCompareAnalysisNameFilter <- shiny::reactive(x = {
    return(input$temporalCompareAnalysisNameFilter)
  })
  
  temporalCompareDomainNameFilter <-  shiny::reactive(x = {
    return(input$temporalCompareDomainNameFilter)
  })
  
  computeBalanceForCompareTemporalCharacterization <-
    shiny::reactive({
      validate(need((length(cohortId(
      )) > 0),
      paste0("Please select cohort.")))
      validate(need((length(
        comparatorCohortId()
      ) > 0),
      paste0("Please select comparator cohort.")))
      # validate(need((comparatorCohortId() != cohortId()),
      #               paste0("Please select different cohort and comparator.")
      # ))
      validate(need((length(input$database) > 0),
                    paste0("Please select atleast one datasource.")
      ))
      validate(need((length(timeIds()) > 0), paste0("Please select time id")))
      
      covs1 <- getCovariateValueResult(
        dataSource = dataSource,
        cohortIds = cohortId(),
        databaseIds = input$database,
        isTemporal = TRUE,
        timeIds = timeIds()
      )
      covs2 <- getCovariateValueResult(
        dataSource = dataSource,
        cohortIds = comparatorCohortId(),
        databaseIds = input$database,
        isTemporal = TRUE,
        timeIds = timeIds()
      )
      balance <-
        compareTemporalCohortCharacteristics(covs1, covs2) %>%
        dplyr::mutate(absStdDiff = abs(.data$stdDiff))
      
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
  
  output$temporalCharacterizationCompareTable <-
    DT::renderDataTable(expr = {
      balance <- computeBalanceForCompareTemporalCharacterization()
      if (nrow(balance) == 0) {
        return(dplyr::tibble(Note = "No data for the selected combination."))
      }
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
        
        if (nrow(balance) == 0) {
          return(dplyr::tibble(Note = "No data for the selected combination."))
        }
        
        balance <- balance %>% 
          dplyr::rename("meanTarget" = mean1, 
                        "sDTarget" = sd1,
                        "meanComparator" = mean2,
                        "sDComparator" = sd2,
                        "stdDiff" = stdDiff)
        
        temporalCovariateChoicesSelected <-
          temporalCovariateChoices %>%
          dplyr::filter(.data$timeId %in% c(timeIds())) %>%
          dplyr::arrange(.data$timeId) %>% 
          dplyr::pull(.data$choices)
        
        if (input$temporalCharacterizationTypeColumnFilter == "Mean and Standard Deviation") {
          table <- balance %>%
            dplyr::mutate(covariateName = paste(.data$covariateName, "(", .data$conceptId, ")")) %>% 
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
            
            containerColumns <- c("Mean Target","SD Target","Mean Comparator","SD Comparator","Std. Diff")
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
            
            containerColumns <- c("Mean Target","SD Target","Mean Comparator","SD Comparator")
          }
        } else {
          
          table <- balance %>%
            dplyr::mutate(covariateName = paste(.data$covariateName, "(", .data$conceptId, ")")) %>% 
            dplyr::arrange(desc(abs(.data$stdDiff))) 
          
          if (length(temporalCovariateChoicesSelected) == 1) {
            table <- table %>% 
              tidyr::pivot_wider(id_cols = c("covariateName"),
                                 names_from = "choices",
                                 values_from = c("meanTarget", "meanComparator", "stdDiff"),
                                 values_fill = 0
              )
            
            containerColumns <- c("Mean Target", "Mean Comparator", "Std. Diff")
            
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
            
            containerColumns <- c("Target", "Comparator")
            
            columnDefs <- list(truncateStringDef(0, 80),
                               minCellRealDef(1:(length(temporalCovariateChoicesSelected) * 2), digits = 2))
            colorBarColumns <- 1 + 1:(length(temporalCovariateChoicesSelected) * 2)
            colspan <- 2
          }
        }
        
        sketch <- htmltools::withTags(table(class = "display",
                                            thead(tr(
                                              th(rowspan = 2, "Covariate Name"),
                                              lapply(temporalCovariateChoicesSelected, th, colspan = colspan, class = "dt-center", style = "border-right:1px solid silver")
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
        xLimitMin = 0,
        xLimitMax = 1,
        yLimitMin = 0,
        yLimitMax = 1
      )
    return(plot)
  })
  
  output$databaseInformationTable <- DT::renderDataTable(expr = {
    if (nrow(database) == 0) {
      return(dplyr::tibble("No information on the data source."))
    }
    data <- database 
    if (!'vocabularyVersionCdm' %in% colnames(database)) {
      data$vocabularyVersionCdm <- "Not in data"
    }
    if (!'vocabularyVersion' %in% colnames(database)) {
      data$vocabularyVersion <- "Not in data"
    }
    data <- data %>%
      dplyr::select(
        .data$databaseId,
        .data$databaseName,
        .data$vocabularyVersionCdm,
        .data$vocabularyVersion,
        .data$description
      ) %>%
      dplyr::mutate(
        match = dplyr::case_when(
          .data$vocabularyVersionCdm == .data$vocabularyVersion ~ TRUE,
          TRUE ~ FALSE
        )
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
        list(width = "20%", targets = 0),
        list(width = "20%", targets = 1),
        list(width = "30%", targets = 4)
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
                                              style = "border-right:1px solid silver"
                                            ),
                                            th(rowspan = 2, "Description"),
                                            th(rowspan = 2, "Match"),
                                          ),
                                          tr(lapply(
                                            c("CDM source", "Vocabulary table"), th, style = "border-right:1px solid silver"
                                          ))
                                        )))
    table <- DT::datatable(
      data ,
      options = options,
      container = sketch,
      rownames = FALSE,
      class = "stripe compact"
    ) %>%
      DT::formatStyle('match',
                      target = 'row',
                      color = DT::styleEqual(FALSE, 'red'))
    return(table)
  }, server = TRUE)
  
  
  # Infoboxes ------------------------------------------------------------------------
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
  
  # Cohort labels --------------------------------------------------------------------------------------------
  targetCohortCount <- shiny::reactive({
    targetCohortWithCount <-
      getCohortCountResult(
        dataSource = dataSource,
        cohortIds = cohortId(),
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
    cohorts <- cohortSubset() %>%
      dplyr::filter(.data$cohortId %in% cohortIds()) %>%
      dplyr::arrange(.data$cohortId) %>%
      dplyr::select(.data$compoundName)
    return(apply(cohorts, 1, function(x)
      tags$tr(lapply(x, tags$td))))
  })
  
  selectedCohort <- shiny::reactive({
    return(input$cohort)
  })
  
  selectedComparatorCohort <- shiny::reactive({
    return(input$comparatorCohort)
  })
  
  
  output$cohortCountsSelectedCohorts <-
    shiny::renderUI({
      selectedCohorts()
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
      selectedCohorts()
    })
  output$incidenceRateSelectedCohorts <-
    shiny::renderUI({
      selectedCohorts()
    })
  output$timeDistSelectedCohorts <-
    shiny::renderUI({
      selectedCohorts()
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
  #Download
  # download_box <- function(exportname, plot){
  #   downloadHandler(
  #     filename = function() {
  #       paste(exportname, Sys.Date(), ".png", sep = "")
  #     },
  #     content = function(file) {
  #       ggplot2::ggsave(file, plot = plot, device = "png", width = 9, height = 7, dpi = 400)
  #     }
  #   )
  # }
  
})
