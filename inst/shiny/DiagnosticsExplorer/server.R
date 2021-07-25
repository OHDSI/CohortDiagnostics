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
    return(conceptSets$conceptSetId[conceptSets$conceptSetName %in% 
                                      input$conceptSetsToFilterCharacterization])
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
  
  getDatabaseIdInCohortConceptSetSecond <- shiny::reactive({
    return(database$databaseId[database$databaseIdWithVocabularyVersion == 
                                 input$databaseOrVocabularySchemaSecond])
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
  

  # Cohort Definition -------
  cohortDefinitionTableData <- shiny::reactive(x = {
    data <-  cohortSubset() %>%
      dplyr::select(cohort = .data$shortName, .data$cohortId, .data$cohortName)
    return(data)
  })
  
  output$saveCohortDefinitionButton <- downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "CohortDefinition")
    },
    content = function(file) {
      x <- cohortSubset() %>%
        dplyr::select(cohort = .data$shortName, 
                      .data$cohortId, 
                      .data$cohortName,
                      .data$sql,
                      .data$json)
      downloadCsv(x = x, fileName = file)
    }
  )
  output$cohortDefinitionTable <- DT::renderDataTable(expr = {
    data <- cohortDefinitionTableData()  %>%
      dplyr::mutate(
        cohortId = as.character(.data$cohortId))
    if (nrow(data) < 20) {
      scrollYHeight <- '15vh'
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
  
  selectedCohortDefinitionRow <- reactive({
    idx <- input$cohortDefinitionTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      subset <- cohortSubset()
      if (nrow(subset) == 0) {
        return(NULL)
      }
      if (length(idx) > 1) {
        # get the last two rows selected
        lastRowsSelected <- idx[c(length(idx), length(idx) - 1)]
      } else {
        lastRowsSelected <- idx
      }
      
      return(subset[lastRowsSelected,])
    }
  })
  
  output$cohortDefinitionRowIsSelected <- reactive({
    return(!is.null(selectedCohortDefinitionRow()))
  })
  
  outputOptions(output,
                "cohortDefinitionRowIsSelected",
                suspendWhenHidden = FALSE)
  
  output$selectedCohortInCohortDefinition <- shiny::renderUI(expr = {
    row <- selectedCohortDefinitionRow()[1,]
    
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
  
  output$selectedSecondCohortInCohortDefinition <- shiny::renderUI(expr = {
    row <- selectedCohortDefinitionRow()[2,]
    
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
  
  cohortDetailsText <- shiny::reactive(x = {
    data <- selectedCohortDefinitionRow()
    if (nrow(data) == 0) {
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
          )
          # , #need to part cohort.metadata (a json) and show all contents in that json
          # tags$tr(
          #   tags$td(tags$strong("Logic: ")),
          #   tags$td(HTML("&nbsp;&nbsp;")),
          #   tags$td(data[i, ]$logicDescription)
          # )
        )
      }
      return(details)
    }
  })
  
  output$cohortDetailsText <- shiny::renderUI({
    row <- cohortDetailsText()[[1]]
    if (is.null(row) || length(row) == 0) {
      return(NULL)
    }
    return(row)
  
  })
  
  output$cohortCountsTableInCohortDefinition <- DT::renderDataTable(expr = {
    row <- selectedCohortDefinitionRow()[1,]
    if (is.null(row)) {
      return(NULL)
    } else {
      
      data <- cohortCount %>%
        dplyr::filter(.data$cohortId == row$cohortId) %>% 
        dplyr::filter(.data$databaseId %in% database$databaseId) %>% 
        dplyr::select(.data$databaseId, .data$cohortSubjects, .data$cohortEntries)
      
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
  
  getSelectedCohortCountRow <- shiny::reactive(x = {
    idx <- input$cohortCountsTableInCohortDefinition_rows_selected
    
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
  
  output$cohortCountsTableInCohortDefinitionRowIsSelected <- shiny::reactive(x = {
    return(!is.null(getSelectedCohortCountRow()))
  })
  shiny::outputOptions(x = output,
                       name = "cohortCountsTableInCohortDefinitionRowIsSelected",
                       suspendWhenHidden = FALSE)
  
  cohortDefinitionInclusionRuleData <- shiny::reactive(x = {
    validate(need(nrow(getSelectedCohortCountRow()) > 0, "No data sources chosen"))
    validate(need(
      nrow(selectedCohortDefinitionRow()) > 0,
      "No cohorts chosen"
    ))
    
    table <- getResultsFromInclusionRuleStatistics(
      dataSource = dataSource,
      cohortIds = selectedCohortDefinitionRow()[1,]$cohortId,
      databaseIds = getSelectedCohortCountRow()$databaseId
    )
    return(table)
  })
  
  output$inclusionRuleInCohortDefinition <- DT::renderDataTable(expr = {
   
    table <- cohortDefinitionInclusionRuleData()
    
    validate(need((nrow(table) > 0),
                  "There is no inclusion rule data for this cohort."))
    
    databaseIds <- unique(table$databaseId)
    cohortCounts <- table %>% 
      dplyr::inner_join(cohortCount,
                        by = c("cohortId", "databaseId")) %>% 
      dplyr::filter(.data$cohortId == selectedCohortDefinitionRow()[1,]$cohortId) %>% 
      dplyr::filter(.data$databaseId %in% getSelectedCohortCountRow()$databaseId) %>% 
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
  
  output$saveCohortDefinitionInclusionRuleTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "InclusionRule")
    },
    content = function(file) {
      downloadCsv(x = cohortDefinitionInclusionRuleData(), fileName = file)
    }
  )
  
  cohortDefinitionCirceRDetails <- shiny::reactive(x = {
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Rendering human readable cohort description using CirceR", value = 0)
  
    data <- selectedCohortDefinitionRow()
    if (nrow(selectedCohortDefinitionRow()) > 0) {
      details <- list()
      for (i in (1:nrow(data))) {
        progress$inc(1/nrow(data), detail = paste("Doing part", i))
        circeExpression <-
          CirceR::cohortExpressionFromJson(expressionJson = data[i, ]$json)
        circeExpressionMarkdown <-
          CirceR::cohortPrintFriendly(circeExpression)
        circeConceptSetListmarkdown <-
          CirceR::conceptSetListPrintFriendly(circeExpression$conceptSets)
        details[[i]] <- data[i, ]
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
  
  output$cohortDefinitionText <- shiny::renderUI(expr = {
    cohortDefinitionCirceRDetails()[1,]$htmlExpressionCohort %>%
      shiny::HTML()
  })
  
  getCirceRPackageVersion <- shiny::reactive(x = {
    row <- selectedCohortDefinitionRow()
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
  
  output$circerVersionInCohortDefinition <- shiny::renderUI(expr = {
    version <- getCirceRPackageVersion()[[1]]
    if (is.null(version)) {
      return(NULL)
    } else {
      version
    }
  })
  
  output$cohortDefinitionJson <- shiny::renderText({
    row <- selectedCohortDefinitionRow()[1,]
    if (is.null(row)) {
      return(NULL)
    } else {
      row$json
    }
  })
  
  output$cohortDefinitionSql <- shiny::renderText({
    row <- selectedCohortDefinitionRow()[1,]
    
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
  
  output$circerVersionInCohortDefinitionSql <- shiny::renderUI(expr = {
    version <- getCirceRPackageVersion()[[1]]
    if (is.null(version)) {
      return(NULL)
    } else {
      version
    }
  })
  
  noOfRowSelectedInCohortDefinitionTable <-  shiny::reactive(x = {
    length <- length(input$cohortDefinitionTable_rows_selected)
    
    if (length == 2) {
      return(6)
    } else {
      return(12)
    }
  })
  
  if (is(dataSource, "environment")) {
    choicesFordatabaseOrVocabularySchema <-
      c(database$databaseIdWithVocabularyVersion)
  } else {
    choicesFordatabaseOrVocabularySchema <- list(
      'From site' = database$databaseIdWithVocabularyVersion,
      'Reference Vocabulary' = vocabularyDatabaseSchemas
    )
  }
  
  
  output$cohortDefinitionCountOfSelectedRows <- shiny::reactive({
    return(length(input$cohortDefinitionTable_rows_selected))
  })
  shiny::outputOptions(x = output,
                       name = "cohortDefinitionCountOfSelectedRows",
                       suspendWhenHidden = FALSE)
  #Dynamic UI rendering -----
  output$dynamicUIGenerationCohortDefinitionConceptsetsOne <- shiny::renderUI(expr = {
    shiny::column(
      noOfRowSelectedInCohortDefinitionTable(),
      shiny::conditionalPanel(
        condition = "output.cohortDefinitionCountOfSelectedRows > 0 & 
                     output.cohortDefinitionRowIsSelected == true",
        shiny::htmlOutput(outputId = "selectedCohortInCohortDefinition"),
        shiny::tabsetPanel(
          type = "tab",
          id = "cohortDefinitionOneTabSetPanel",
          shiny::tabPanel(title = "Details",
                          shiny::htmlOutput("cohortDetailsText")),
          shiny::tabPanel(title = "Cohort Count",
                          tags$br(),
                          DT::dataTableOutput(outputId = "cohortCountsTableInCohortDefinition"),
                          tags$br(),
                          shiny::conditionalPanel(
                            condition = "output.cohortCountsTableInCohortDefinitionRowIsSelected",
                            tags$h3("Inclusion Rules"),
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
                                                   "saveCohortDefinitionInclusionRuleTable",
                                                   label = "",
                                                   icon = shiny::icon("download"),
                                                   style = "margin-top: 5px; margin-bottom: 5px;"
                                                 )
                                         )
                                       )
                            ),
                            DT::dataTableOutput(outputId = "inclusionRuleInCohortDefinition")
                          )),
          shiny::tabPanel(title = "Cohort definition",
                          copyToClipboardButton(toCopyId = "cohortDefinitionText",
                                                style = "margin-top: 5px; margin-bottom: 5px;"),
                          shiny::htmlOutput("circerVersionInCohortDefinition"),
                          shiny::htmlOutput("cohortDefinitionText")),
          shiny::tabPanel(
            title = "Concept Sets",
            value = "conceptSetOneTabPanel",
            DT::dataTableOutput(outputId = "conceptsetExpressionTable"),
            tags$br(),
            shiny::conditionalPanel(condition = "output.conceptSetExpressionRowSelected == true",
                                    shinydashboard::box(
                                      title = "Left Panel",
                                      width = NULL,
                                      solidHeader = FALSE,
                                      collapsible = TRUE,
                                      collapsed = TRUE,
                                      shiny::conditionalPanel(condition = "output.conceptSetExpressionRowSelected == true",
                                                              tags$table(tags$tr(
                                                                tags$td(
                                                                  shinyWidgets::pickerInput(
                                                                    inputId = "databaseOrVocabularySchema",
                                                                    label = "Vocabulary version choices:",
                                                                    choices = choicesFordatabaseOrVocabularySchema,
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
                                                                  shiny::htmlOutput("personAndRecordCountInCohortDefinitionConceptSet")
                                                                )
                                                              ),
                                                              tags$tr(
                                                                tags$td(colspan = 2,
                                                                        shiny::radioButtons(
                                                                          inputId = "conceptSetsType",
                                                                          label = "",
                                                                          choices = c("Concept Set Expression",
                                                                                      "Resolved (included)",
                                                                                      "Mapped (source)",
                                                                                      "Orphan concepts",
                                                                                      "Json"),
                                                                          selected = "Concept Set Expression",
                                                                          inline = TRUE
                                                                        )
                                                                ))
                                                              )),
                                      shiny::conditionalPanel(
                                        condition = "output.conceptSetExpressionRowSelected == true &
                                                      input.conceptSetsType != 'Resolved (included)' &
                                                      input.conceptSetsType != 'Mapped (source)' &
                                                      input.conceptSetsType != 'Json' &
                                                      input.conceptSetsType != 'Orphan concepts'",
                                        tags$table(width = "100%", 
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveCohortDefinitionConceptSetsTable",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ), 
                                        DT::dataTableOutput(outputId = "cohortDefinitionConceptSetsTable")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsType == 'Resolved (included)'",
                                        tags$table(width = "100%", 
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveCohortDefinitionIncludedResolvedConceptsTable",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ), 
                                        DT::dataTableOutput(outputId = "cohortDefinitionIncludedResolvedConceptsTable")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsType == 'Mapped (source)'",
                                        tags$table(width = "100%", 
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveCohortDefinitionMappedConceptsTable",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ), 
                                        DT::dataTableOutput(outputId = "cohortDefinitionMappedConceptsTable")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsType == 'Orphan concepts'",
                                        tags$table(width = "100%", 
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveCohortDefinitionOrphanConceptsTable",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ), 
                                        DT::dataTableOutput(outputId = "cohortDefinitionOrphanConceptTable")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsType == 'Json'",
                                        copyToClipboardButton(toCopyId = "cohortConceptsetExpressionJson",
                                                              style = "margin-top: 5px; margin-bottom: 5px;"),
                                        shiny::verbatimTextOutput(outputId = "cohortConceptsetExpressionJson"),
                                        tags$head(
                                          tags$style("#cohortConceptsetExpressionJson { max-height:400px};")
                                        )
                                      )
                                    ))
            
          ), 
            
            shiny::tabPanel(
              title = "JSON",
              copyToClipboardButton("cohortDefinitionJson", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput("cohortDefinitionJson"),
              tags$head(
                tags$style("#cohortDefinitionJson { max-height:400px};")
              )
            ),
            shiny::tabPanel(
              title = "SQL",
              copyToClipboardButton("cohortDefinitionSql", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::htmlOutput("circerVersionInCohortDefinitionSql"),
              shiny::verbatimTextOutput("cohortDefinitionSql"),
              tags$head(
                tags$style("#cohortDefinitionSql { max-height:400px};")
              )
            )
          )
        )
      )
  })
  
  output$dynamicUIGenerationCohortDefinitionConceptsetsTwo <- shiny::renderUI(expr = {
    shiny::column(
      noOfRowSelectedInCohortDefinitionTable(),
      shiny::conditionalPanel(
        condition = "output.cohortDefinitionCountOfSelectedRows == 2 & 
                     output.cohortDefinitionRowIsSelected == true",
        shiny::htmlOutput(outputId = "selectedSecondCohortInCohortDefinition"),
        shiny::tabsetPanel(
          id = "cohortDefinitionTwoTabSetPanel",
          type = "tab",
          shiny::tabPanel(title = "Details",
                          shiny::htmlOutput("cohortDetailsTextSecond")),
          shiny::tabPanel(title = "Cohort Count",
                          tags$br(),
                          DT::dataTableOutput(outputId = "cohortCountsTableInCohortDefinitionSecond"),
                          tags$br(),
                          shiny::conditionalPanel(
                            condition = "output.cohortCountsSecondTableInCohortDefinitionRowIsSelected",
                            tags$h3("Inclusion Rules"),
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
                                                   "saveCohortDefinitionSecondInclusionRuleTable",
                                                   label = "",
                                                   icon = shiny::icon("download"),
                                                   style = "margin-top: 5px; margin-bottom: 5px;"
                                                 )
                                         )
                                       )
                            ),
                            DT::dataTableOutput(outputId = "inclusionRuleInCohortDefinitionSecond")
                          )),
          shiny::tabPanel(title = "Cohort definition",
                          copyToClipboardButton(toCopyId = "cohortDefinitionTextSecond",
                                                style = "margin-top: 5px; margin-bottom: 5px;"),
                          shiny::htmlOutput("circerVersionInCohortDefinitionSecond"),
                          shiny::htmlOutput("cohortDefinitionTextSecond")),
          shiny::tabPanel(
            title = "Concept Sets",
            value = "conceptSetTwoTabPanel",
            DT::dataTableOutput(outputId = "conceptsetExpressionSecondTable"),
            tags$br(),
            shiny::conditionalPanel(condition = "output.conceptSetExpressionSecondRowSelected == true",
                                    shinydashboard::box(
                                      title = "Right Panel",
                                      solidHeader = FALSE,
                                      width = NULL,
                                      collapsible = TRUE,
                                      collapsed = TRUE,
                                      shiny::conditionalPanel(condition = "output.conceptSetExpressionSecondRowSelected == true",
                                                              tags$table(tags$tr(
                                                                tags$td(
                                                                  shinyWidgets::pickerInput(
                                                                    inputId = "databaseOrVocabularySchemaSecond",
                                                                    label = "Vocabulary version choices:",
                                                                    choices = choicesFordatabaseOrVocabularySchema,
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
                                                                  shiny::htmlOutput("personAndRecordCountInCohortDefinitionConceptSetSecond")
                                                                )
                                                              ),
                                                              tags$tr(
                                                                tags$td(colspan = 2,
                                                                        shiny::radioButtons(
                                                                          inputId = "conceptSetsTypeSecond",
                                                                          label = "",
                                                                          choices = c("Concept Set Expression",
                                                                                      "Resolved (included)",
                                                                                      "Mapped (source)",
                                                                                      "Orphan concepts",
                                                                                      "Json"),
                                                                          selected = "Concept Set Expression",
                                                                          inline = TRUE
                                                                        )
                                                                )
                                                              ))),
                                      shiny::conditionalPanel(
                                        condition = "output.conceptSetExpressionSecondRowSelected == true &
                                                      input.conceptSetsTypeSecond != 'Resolved (included)' &
                                                      input.conceptSetsTypeSecond != 'Mapped (source)' &
                                                      input.conceptSetsTypeSecond != 'Json' &
                                                      input.conceptSetsTypeSecond != 'Orphan concepts'",
                                        tags$table(width = "100%",
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveCohortDefinitionConceptSetsTableSecond",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ),
                                        DT::dataTableOutput(outputId = "cohortDefinitionConceptSetsSecondTable")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeSecond == 'Resolved (included)'",
                                        tags$table(width = "100%",
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveCohortDefinitionIncludedResolvedConceptsSecondTable",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ),
                                        DT::dataTableOutput(outputId = "cohortDefinitionIncludedResolvedConceptsSecondTable")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeSecond == 'Mapped (source)'",
                                        tags$table(width = "100%",
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveCohortDefinitionMappedConceptsSecondTable",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ),
                                        DT::dataTableOutput(outputId = "cohortDefinitionMappedConceptsSecondTable")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeSecond == 'Orphan concepts'",
                                        tags$table(width = "100%",
                                                   tags$tr(
                                                     tags$td(align = "right",
                                                             shiny::downloadButton(
                                                               "saveCohortDefinitionOrphanConceptsSecondTable",
                                                               label = "",
                                                               icon = shiny::icon("download"),
                                                               style = "margin-top: 5px; margin-bottom: 5px;"
                                                             )
                                                     )
                                                   )
                                        ),
                                        DT::dataTableOutput(outputId = "cohortDefinitionOrphanConceptSecondTable")
                                      ),
                                      shiny::conditionalPanel(
                                        condition = "input.conceptSetsTypeSecond == 'Json'",
                                        copyToClipboardButton(toCopyId = "cohortConceptsetExpressionJsonSecond",
                                                              style = "margin-top: 5px; margin-bottom: 5px;"),
                                        shiny::verbatimTextOutput(outputId = "cohortConceptsetExpressionJsonSecond"),
                                        tags$head(
                                          tags$style("#cohortConceptsetExpressionJsonSecond { max-height:400px};")
                                        )
                                      )
                                    ))
            
          ), 
          
          shiny::tabPanel(
            title = "JSON",
            copyToClipboardButton("cohortDefinitionJsonSecond", style = "margin-top: 5px; margin-bottom: 5px;"),
            shiny::verbatimTextOutput("cohortDefinitionJsonSecond"),
            tags$head(
              tags$style("#cohortDefinitionJsonSecond { max-height:400px};")
            )
          ),
          shiny::tabPanel(
            title = "SQL",
            copyToClipboardButton("cohortDefinitionSqlSecond", style = "margin-top: 5px; margin-bottom: 5px;"),
            shiny::htmlOutput("circerVersionInCohortDefinitionSqlSecond"),
            shiny::verbatimTextOutput("cohortDefinitionSqlSecond"),
            tags$head(
              tags$style("#cohortDefinitionSqlSecond { max-height:400px};")
            )
          )
        )
      ))
  })
  
  # Cohort definition concept set expression ----
  cohortDefinitionConceptSetExpression <- shiny::reactive({
    if (is.null(selectedCohortDefinitionRow())) {
      return(NULL)
    }
    details <- list()
    for (i in 1:nrow(selectedCohortDefinitionRow())) {
      conceptSetDetailsFromCohortDefinition <-
        getConceptSetDetailsFromCohortDefinition(
          cohortDefinitionExpression = RJSONIO::fromJSON(selectedCohortDefinitionRow()[i,]$json)
        )
      details[[i]] <- conceptSetDetailsFromCohortDefinition
    }
    return(details)
  })
  
  ##Concept set expression table one
  output$conceptsetExpressionTable <- DT::renderDataTable(expr = {
    validate(need((any(is.null(cohortDefinitionConceptSetExpression()),
                       length(cohortDefinitionConceptSetExpression()) == 0)),
                  "Cohort definition does not appear to have concept set expression(s)."))
    if (any(is.null(cohortDefinitionConceptSetExpression()),
            length(cohortDefinitionConceptSetExpression()) == 0)) {
      return(NULL)
    }
    if (!is.null(cohortDefinitionConceptSetExpression()[[1]]$conceptSetExpression) &&
        nrow(cohortDefinitionConceptSetExpression()[[1]]$conceptSetExpression) > 0) {
      data <- cohortDefinitionConceptSetExpression()[[1]]$conceptSetExpression %>%
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
  
  
  cohortDefinitionConceptSetExpressionRow <- shiny::reactive(x = {
    idx <- input$conceptsetExpressionTable_rows_selected
    if (length(idx) == 0 || is.null(idx)) {
      return(NULL)
    }
    if (!is.null(cohortDefinitionConceptSetExpression()[[1]]$conceptSetExpression) &&
        nrow(cohortDefinitionConceptSetExpression()[[1]]$conceptSetExpression) > 0) {
      data <-
        cohortDefinitionConceptSetExpression()[[1]]$conceptSetExpression[idx,]
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
      cohortDefinitionConceptSetExpression()[[1]]$conceptSetExpressionDetails
    data <- data %>%
      dplyr::filter(.data$id == cohortDefinitionConceptSetExpressionRow()$id)
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
  
  getDatabaseIdInCohortConceptSet <- shiny::reactive({
    return(database$databaseId[database$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema])
  })
  
  getPersonAndRecordCountForVocabularySchema <-  function(cohortId, databaseId) {
    data <- cohortCount %>%
      dplyr::filter(.data$cohortId == !!cohortId) %>% 
      dplyr::filter(.data$databaseId == !!databaseId) %>% 
      dplyr::select(.data$cohortSubjects, .data$cohortEntries)
    
    if (nrow(data) == 0) {
      return(NULL)
    } else {
      return(data)
    }
  }
  
  getSubjectAndRecordCountForCohortConceptSet <- shiny::reactive(x = {
    row <- selectedCohortDefinitionRow()[1,]
    
    if (is.null(row) || length(getDatabaseIdInCohortConceptSet()) == 0) {
      return(NULL)
    } else {
      
      data <- getPersonAndRecordCountForVocabularySchema(cohortId = row$cohortId, 
                                                         databaseId = getDatabaseIdInCohortConceptSet())
      
      if (nrow(data) == 0 || is.null(data)) {
        return(NULL)
      } else {
        return(data)
      }
    }
  })
  
  output$personAndRecordCountInCohortDefinitionConceptSet <- shiny::renderUI({
    row <- getSubjectAndRecordCountForCohortConceptSet()
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
  
  getResolvedOrMappedConceptSetForAllDatabase <-
    shiny::reactive(x = {
      row <- selectedCohortDefinitionRow()
      if (is.null(row) ||
          is.null(cohortDefinitionConceptSetExpressionRow()$id)) {
        return(NULL)
      }
      
      output <-
        getResultsResolveMappedConceptSet(
          dataSource = dataSource,
          databaseIds = database$databaseId,
          cohortIds =  row$cohortId
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
  
  getConceptCountForAllDatabase <- 
    shiny::reactive(x = {
      row <- selectedCohortDefinitionRow()
      if (is.null(row) ||
          is.null(cohortDefinitionConceptSetExpressionRow()$id)) {
        return(NULL)
      }
      browser()
      conceptCount <- getResultsFromConceptCount(
        dataSource = dataSource,
        databaseIds = database$databaseId,
        cohortId = row$cohortId
      )
      return(conceptCount)
    })
  
  getResolvedOrMappedConcepts <- shiny::reactive({
    data <- NULL
    if (is.null(input$databaseOrVocabularySchema)) {return(NULL)}
    databaseIdToFilter <- database %>%
      dplyr::filter(.data$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchema) %>%
      dplyr::pull(.data$databaseId)
    browser()
    conceptCounts <- getConceptCountForAllDatabase()
    if (all(!is.null(conceptCounts),
            nrow(conceptCounts) > 0)) {
      browser()
      conceptCounts <- conceptCounts %>% 
        dplyr::filter(.data$databaseId %in% !!databaseIdToFilter) %>% 
        dplyr::select(.data$conceptId, .data$sourceConceptId, .data$conceptSubjects, .data$conceptCount) %>% 
        dplyr::distinct()
      conceptCounts <- dplyr::bind_rows(
        conceptCounts %>% 
          dplyr::select(.data$conceptId, .data$conceptSubjects, .data$conceptCount),
        conceptCounts %>% 
          dplyr::select(.data$sourceConceptId, .data$conceptSubjects, .data$conceptCount) %>% 
          dplyr::rename("conceptId" = .data$sourceConceptId)
      ) %>% 
        dplyr::group_by(.data$conceptId) %>% 
        dplyr::summarise(conceptSubjects = conceptSubjects,
                         conceptCount = conceptCount) %>% 
        dplyr::distinct() %>% 
        dplyr::ungroup() %>% 
        dplyr::arrange(dplyr::desc(.data$conceptCount))
    }
    
    if (length(databaseIdToFilter) > 0) {
      resolvedOrMappedConceptSetForAllDatabase <-
        getResolvedOrMappedConceptSetForAllDatabase()
      if (!is.null(resolvedOrMappedConceptSetForAllDatabase) &&
          length(resolvedOrMappedConceptSetForAllDatabase) == 2) {
        source <-
          (input$conceptSetsType == "Mapped (source)")
        if (source) {
          data <- resolvedOrMappedConceptSetForAllDatabase$mapped 
          if (!is.null(data) && nrow(data) > 0) {
            data <- data %>%
              dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionRow()$id) %>%
              dplyr::filter(.data$databaseId %in% !!databaseIdToFilter) %>%
              dplyr::select(-.data$databaseId, -.data$conceptSetId) %>% 
              dplyr::filter(.data$conceptId != .data$resolvedConceptId) %>% 
              dplyr::relocate(.data$resolvedConceptId) %>% 
              dplyr::inner_join(resolvedOrMappedConceptSetForAllDatabase$resolved %>% 
                                  dplyr::select(.data$conceptId, .data$conceptName) %>% 
                                  dplyr::distinct() %>% 
                                  dplyr::rename("resolvedConceptId" = .data$conceptId,
                                                "resolvedConceptName" = .data$conceptName),
                                by = "resolvedConceptId") %>% 
              dplyr::mutate(resolvedConcept = paste0(.data$resolvedConceptId, " (", .data$resolvedConceptName, ")")) %>% 
              dplyr::select(-.data$resolvedConceptId, -.data$resolvedConceptName) %>% 
              dplyr::relocate(.data$resolvedConcept)
            # data$resolvedConcept <-
            #   as.factor(data$resolvedConcept)
          } else {
            data <- NULL
          }
        } else {
          data <- resolvedOrMappedConceptSetForAllDatabase$resolved %>%
            dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionRow()$id) %>%
            dplyr::filter(.data$databaseId %in% !!databaseIdToFilter) %>%
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
          (input$conceptSetsType == "Mapped (source)")
        if (source) {
          data <- resolvedOrMappedConceptSetForAllVocabulary$mapped %>%
            dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionRow()$id) %>%
            dplyr::filter(.data$vocabularyDatabaseSchema == !!vocabularyDataSchemaToFilter) %>%
            dplyr::select(-.data$vocabularyDatabaseSchema, -.data$conceptSetId) %>% 
            dplyr::filter(.data$conceptId != .data$resolvedConceptId) %>% 
            dplyr::relocate(.data$resolvedConceptId) %>% 
            dplyr::inner_join(resolvedOrMappedConceptSetForAllVocabulary$resolved %>% 
                                dplyr::select(.data$conceptId, .data$conceptName) %>% 
                                dplyr::distinct() %>% 
                                dplyr::rename("resolvedConceptId" = .data$conceptId,
                                              "resolvedConceptName" = .data$conceptName),
                              by = "resolvedConceptId") %>% 
            dplyr::mutate(resolvedConcept = paste0(.data$resolvedConceptId, " (", .data$resolvedConceptName, ")")) %>% 
            dplyr::select(-.data$resolvedConceptId, -.data$resolvedConceptName) %>% 
            dplyr::relocate(.data$resolvedConcept)
          # data$resolvedConcept <-
          #   as.factor(data$resolvedConcept)
        } else {
          data <- resolvedOrMappedConceptSetForAllVocabulary$resolved %>%
            dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionRow()$id) %>%
            dplyr::filter(.data$vocabularyDatabaseSchema == !!vocabularyDataSchemaToFilter) %>%
            dplyr::select(-.data$vocabularyDatabaseSchema, -.data$conceptSetId)
        }
      }
    }
    if (!is.null(data) && nrow(data) > 0) {
      if (all(nrow(conceptCounts) > 0,
              'conceptId' %in% colnames(conceptCounts))) {
        data <- data %>% 
          dplyr::left_join(conceptCounts, by = "conceptId") %>% 
          dplyr::arrange(dplyr::desc(.data$conceptSubjects)) %>% 
          dplyr::relocate(.data$conceptSubjects, .data$conceptCount) %>% 
          dplyr::rename("subjects" = .data$conceptSubjects,
                        "count" = .data$conceptCount)
      }
      
      data$conceptClassId <- as.factor(data$conceptClassId)
      data$domainId <- as.factor(data$domainId)
      # data$conceptCode <- as.factor(data$conceptCode)
      data$conceptId <- as.character(data$conceptId)
      # data$conceptName <- as.factor(data$conceptName)
      data$vocabularyId <- as.factor(data$vocabularyId)
      data$standardConcept <- as.factor(data$standardConcept)
      
      data <- data %>% 
        dplyr::relocate(.data$conceptId, .data$conceptName)
    }
    return(data)
  })
  
  output$saveCohortDefinitionIncludedResolvedConceptsTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "ResolvedConcepts")
    },
    content = function(file) {
      downloadCsv(x = getResolvedOrMappedConcepts(), fileName = file)
    }
  )
  
  output$cohortDefinitionIncludedResolvedConceptsTable <-
    DT::renderDataTable(expr = {
      data <- getResolvedOrMappedConcepts()
      
      validate(need(length(cohortDefinitionConceptSetExpressionRow()$id) > 0,
                    "Please select concept set"))
      
      validate(need((all(!is.null(data), nrow(data) > 0)),
                    "No resolved or mapped concept ids"))
      
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
  
  output$saveCohortDefinitionMappedConceptsTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "MappedConcepts")
    },
    content = function(file) {
      downloadCsv(x = getResolvedOrMappedConcepts(), fileName = file)
    }
  )
  
  output$cohortDefinitionMappedConceptsTable <-
    DT::renderDataTable(expr = {
      data <- getResolvedOrMappedConcepts()
      
      validate(need(length(cohortDefinitionConceptSetExpressionRow()$id) > 0,
                    "Please select concept set"))
      
      validate(need((all(!is.null(data), nrow(data) > 0)),
                    "No resolved or mapped concept ids"))
      
      data <- data %>% 
        dplyr::mutate(
          conceptId = as.character(.data$conceptId),
          # conceptName = as.factor(.data$conceptName),
          vocabularyId = as.factor(.data$vocabularyId))
      
      columnDef <- list(
        truncateStringDef(2, 80)
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
        columnDefs = columnDef
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
  
  output$saveCohortDefinitionConceptSetsTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "ConceptSetsExpression")
    },
    content = function(file) {
      downloadCsv(x = cohortDefinitionConceptSets(), fileName = file)
    }
  )
  
  output$cohortDefinitionConceptSetsTable <-
    DT::renderDataTable(expr = {
      data <- cohortDefinitionConceptSets()
      if (is.null(cohortDefinitionConceptSets())) {
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
  
  
  
  output$cohortConceptsetExpressionJson <- shiny::renderText({
    if (is.null(cohortDefinitionConceptSetExpressionRow())) {
      return(NULL)
    }
    cohortDefinitionConceptSetExpressionRow()$json
  })
  
  output$saveConceptSetButton <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "ConceptSetsExpression")
    },
    content = function(file) {
      downloadCsv(x = cohortDefinitionConceptSets(), fileName = file)
    }
  )
  
  ## Orphan 1 concepts for cohort definition ----
  cohortDefinitionOrphanConceptTableData <- shiny::reactive(x = {
    if (any(is.null(getDatabaseIdInCohortConceptSet()),
            length(getDatabaseIdInCohortConceptSet()) == 0)) {return(NULL)}
    row <- selectedCohortDefinitionRow()
    if (is.null(row) || length(cohortDefinitionConceptSetExpressionRow()$name) == 0) {
      return(NULL)
    }
    if (length(input$databaseOrVocabularySchema) == 0) {return(NULL)}
    browser()
    data <- getResultsFromOrphanConcept(dataSource = dataSource,
                                        cohortId = row$cohortId,
                                        databaseIds = getDatabaseIdInCohortConceptSet())
    if (!is.null(data)) {
      data <- data %>% 
        dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionRow()$id)
    }
    return(data)
  })
  
  ### Save orphan concepts table ----
  output$saveCohortDefinitionOrphanConceptsTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "orphanConcepts")
    },
    content = function(file) {
      downloadCsv(x = cohortDefinitionOrphanConceptTableData(), 
                  fileName = file)
    }
  )
  
  ### Left panel orphan concept ----
  orphanConceptComparisionLeftPanelData <- shiny::reactive(x = {
    browser()
    data <- cohortDefinitionOrphanConceptTableData()
    if (any(nrow(data) == 0,is.null(data))) {return(NULL)}
    data <- pivotOrphanConceptResult(data = data,
                                     dataSource = dataSource)
    return(data)
  })
  
  output$cohortDefinitionOrphanConceptTable <- DT::renderDataTable(expr = {
    orphanConceptData <- orphanConceptComparisionLeftPanelData()
    orphanConceptDataDatabaseIds <- attr(x = orphanConceptData, which = 'databaseIds')
    orphanConceptDataMaxCount <- attr(x = orphanConceptData, which = 'maxCount')
    
    if (any(nrow(orphanConceptData) == 0,
            is.null(orphanConceptData))) {return(NULL)}
    
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
  
  # Concept set expression 2 ----
  output$cohortDetailsTextSecond <- shiny::renderUI({
    row <- cohortDetailsText()[[2]]
    if (is.null(row) || length(row) == 0) {
      return(NULL)
    }
    return(row)
  })
  
  output$cohortCountsTableInCohortDefinitionSecond <- DT::renderDataTable(expr = {
    row <- selectedCohortDefinitionRow()[2,]
    if (is.null(row)) {
      return(NULL)
    } else {
      
      data <- cohortCount %>%
        dplyr::filter(.data$cohortId == row$cohortId) %>% 
        dplyr::filter(.data$databaseId %in% database$databaseId) %>% 
        dplyr::select(.data$databaseId, .data$cohortSubjects, .data$cohortEntries)
      
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
  
  getSelectedCohortCountSecondRow <- shiny::reactive(x = {
    idx <- input$cohortCountsTableInCohortDefinitionSecond_rows_selected
    
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
  
  output$cohortCountsSecondTableInCohortDefinitionRowIsSelected <- shiny::reactive(x = {
    return(!is.null(getSelectedCohortCountSecondRow()))
  })
  shiny::outputOptions(x = output,
                       name = "cohortCountsSecondTableInCohortDefinitionRowIsSelected",
                       suspendWhenHidden = FALSE)
  
  cohortDefinitionSecondInclusionRuleData <- shiny::reactive(x = {
    validate(need(nrow(getSelectedCohortCountSecondRow()) > 0, "No data sources chosen"))
    validate(need(
      nrow(selectedCohortDefinitionRow()) > 0,
      "No cohorts chosen"
    ))
    
    table <- getResultsFromInclusionRuleStatistics(
      dataSource = dataSource,
      cohortIds = selectedCohortDefinitionRow()[2,]$cohortId,
      databaseIds = getSelectedCohortCountSecondRow()$databaseId
    )
    return(table)
  })
  
  output$inclusionRuleInCohortDefinitionSecond <- DT::renderDataTable(expr = {
   
    table <- cohortDefinitionSecondInclusionRuleData()
    validate(need((nrow(table) > 0),
                  "There is no inclusion rule data for this cohort."))
    
    databaseIds <- unique(table$databaseId)
    cohortCounts <- table %>% 
      dplyr::inner_join(cohortCount,
                        by = c("cohortId", "databaseId")) %>% 
      dplyr::filter(.data$cohortId == selectedCohortDefinitionRow()[2,]$cohortId) %>% 
      dplyr::filter(.data$databaseId %in% getSelectedCohortCountRow()$databaseId) %>% 
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
  
  output$saveCohortDefinitionSecondInclusionRuleTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "InclusionRule")
    },
    content = function(file) {
      downloadCsv(x = cohortDefinitionSecondInclusionRuleData(), fileName = file)
    }
  )
  
  output$circerVersionInCohortDefinitionSecond <- shiny::renderUI(expr = {
    version <- getCirceRPackageVersion()[[2]]
    if (is.null(version)) {
      return(NULL)
    } else {
      version
    }
  })
  
  output$cohortDefinitionTextSecond <- shiny::renderUI(expr = {
    cohortDefinitionCirceRDetails()[2,]$htmlExpressionCohort %>%
      shiny::HTML()
  })
  
  output$cohortDefinitionJsonSecond <- shiny::renderText({
    row <- selectedCohortDefinitionRow()[2,]
    if (is.null(row)) {
      return(NULL)
    } else {
      row$json
    }
  })
  
  output$circerVersionInCohortDefinitionSqlSecond <- shiny::renderUI(expr = {
    version <- getCirceRPackageVersion()[[2]]
    if (is.null(version)) {
      return(NULL)
    } else {
      version
    }
  })
  
  output$cohortDefinitionSqlSecond <- shiny::renderText({
    row <- selectedCohortDefinitionRow()[2,]
    
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
  
  output$conceptsetExpressionSecondTable <- DT::renderDataTable(expr = {
    if (length(cohortDefinitionConceptSetExpression()) != 2) {
      return(NULL)
    }
    
    if (!is.null(cohortDefinitionConceptSetExpression()[[2]]$conceptSetExpression) &&
        nrow(cohortDefinitionConceptSetExpression()[[2]]$conceptSetExpression) > 0) {
      data <- cohortDefinitionConceptSetExpression()[[2]]$conceptSetExpression %>%
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
  
  
  cohortDefinitionConceptSetExpressionSecondRow <- shiny::reactive(x = {
    idx <- input$conceptsetExpressionSecondTable_rows_selected
    if (length(idx) == 0 || is.null(idx)) {
      return(NULL)
    }
    if (!is.null(cohortDefinitionConceptSetExpression()[[2]]$conceptSetExpression) &&
        nrow(cohortDefinitionConceptSetExpression()[[2]]$conceptSetExpression) > 0) {
      data <-
        cohortDefinitionConceptSetExpression()[[2]]$conceptSetExpression[idx,]
      if (!is.null(data)) {
        return(data)
      } else {
        return(NULL)
      }
    }
  })
  
  output$conceptSetExpressionSecondRowSelected <- shiny::reactive(x = {
    return(!is.null(cohortDefinitionConceptSetExpressionSecondRow()))
  })
  shiny::outputOptions(x = output,
                       name = "conceptSetExpressionSecondRowSelected",
                       suspendWhenHidden = FALSE)
  
  cohortDefinitionConceptSetSecond <- shiny::reactive(x = {
    if (is.null(cohortDefinitionConceptSetExpressionSecondRow())) {
      return(NULL)
    }
    
    data <-
      cohortDefinitionConceptSetExpression()[[2]]$conceptSetExpressionDetails
    data <- data %>%
      dplyr::filter(.data$id == cohortDefinitionConceptSetExpressionSecondRow()$id)
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
  
  getSubjectAndRecordCountForCohortConceptSetSecond <- shiny::reactive(x = {
    row <- selectedCohortDefinitionRow()[2,]
    
    if (is.null(row) || length(getDatabaseIdInCohortConceptSetSecond()) == 0) {
      return(NULL)
    } else {
      
      data <- getPersonAndRecordCountForVocabularySchema(cohortId = row$cohortId, 
                                                         databaseId = getDatabaseIdInCohortConceptSetSecond())
      
      if (nrow(data) == 0 || is.null(data)) {
        return(NULL)
      } else {
        return(data)
      }
    }
  })
  
  output$personAndRecordCountInCohortDefinitionConceptSetSecond <- shiny::renderUI({
    row <- getSubjectAndRecordCountForCohortConceptSetSecond()
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
  
  output$cohortDefinitionConceptSetsSecondTable <-
    DT::renderDataTable(expr = {
      data <- cohortDefinitionConceptSetSecond()
      if (is.null(cohortDefinitionConceptSetSecond())) {
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
  
  output$saveCohortDefinitionConceptSetsTableSecond <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "conceptset")
    },
    content = function(file) {
      downloadCsv(x = cohortDefinitionConceptSetSecond(), 
                  fileName = file)
    }
  )
  
  output$cohortConceptsetExpressionJsonSecond <- shiny::renderText({
    if (is.null(cohortDefinitionConceptSetExpressionSecondRow())) {
      return(NULL)
    }
    cohortDefinitionConceptSetExpressionSecondRow()$json
  })
  
  getConceptSetIds <- shiny::reactive(x = {
    return(conceptSets$conceptSetId[conceptSets$conceptSetName  %in% 
                                      input$conceptSetsToFilterCharacterization])
  })
  
  getResoledAndMappedConceptIdsForFilters <- shiny::reactive({
    validate(need(all(!is.null(databaseIds()), length(databaseIds()) > 0), 
                  "No data sources chosen"))
    validate(need(all(!is.null(cohortId()),length(cohortId()) > 0),
                  "No cohort chosen"))
    output <-
      getResultsResolveMappedConceptSet(
        dataSource = dataSource,
        databaseIds = databaseIds(),
        cohortIds = cohortId()
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
  
  
  ## Orphan 2 concepts for cohort definition ----
  cohortDefinitionOrphanConceptSecondTableData <- shiny::reactive(x = {
    if (any(is.null(getDatabaseIdInCohortConceptSetSecond()),
            length(getDatabaseIdInCohortConceptSetSecond()) == 0)) {return(NULL)}
    row <- selectedCohortDefinitionRow()
    if (is.null(row) || length(cohortDefinitionConceptSetExpressionSecondRow()$name) == 0) {
      return(NULL)
    }
    browser()
    # if (length(input$databaseOrVocabularySchema) == 0) {return(NULL)}
    data <- getResultsFromOrphanConcept(dataSource = dataSource,
                                        cohortId = row$cohortId,
                                        databaseIds = getDatabaseIdInCohortConceptSetSecond())
    if (!is.null(data)) {
      data <- data %>% 
        dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSecondRow()$id)
    }
    return(data)
  })
  
  ### Right panel orphan concept ----
  orphanConceptComparisionRightPanelData <- shiny::reactive(x = {
    data <- cohortDefinitionOrphanConceptSecondTableData()
    if (any(nrow(data) == 0,is.null(data))) {return(NULL)}
    data <- pivotOrphanConceptResult(data = data,
                                     dataSource = dataSource)
  })
  
  output$cohortDefinitionOrphanConceptSecondTable <- DT::renderDataTable(expr = {
    orphanConceptData <- orphanConceptComparisionRightPanelData()
    orphanConceptDataDatabaseIds <- attr(x = orphanConceptData, which = 'databaseIds')
    orphanConceptDataMaxCount <- attr(x = orphanConceptData, which = 'maxCount')
    
    if (any(nrow(orphanConceptData) == 0,
            is.null(orphanConceptData))) {return(NULL)}
    
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
  
  ### Save orphan concepts table ----
  output$saveCohortDefinitionOrphanConceptsSecondTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "orphanconcepts")
    },
    content = function(file) {
      downloadCsv(x = cohortDefinitionOrphanConceptSecondTableData(), 
                  fileName = file)
    }
  )
  
  getResolvedOrMappedConceptSetSecondForAllDatabase <-
    shiny::reactive(x = {
      row <- selectedCohortDefinitionRow()
      if (is.null(row) ||
          is.null(cohortDefinitionConceptSetExpressionSecondRow()$id)) {
        return(NULL)
      }
      
      output <-
        getResultsResolveMappedConceptSet(
          dataSource = dataSource,
          databaseIds = database$databaseId,
          cohortIds =  selectedCohortDefinitionRow()[2,]$cohortId
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
          is.null(cohortDefinitionConceptSetExpressionSecondRow()$id)) {
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
              dplyr::filter(cohortId == selectedCohortDefinitionRow()[2,]$cohortId),
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
  
  getConceptSecondCountForAllDatabase <- 
    shiny::reactive(x = {
      row <- selectedCohortDefinitionRow()
      if (is.null(row) ||
          is.null(cohortDefinitionConceptSetExpressionSecondRow()$id)) {
        return(NULL)
      }
      browser()
      conceptCount <- getResultsFromConceptCount(
        dataSource = dataSource,
        databaseIds = database$databaseId,
        cohortId = selectedCohortDefinitionRow()[2,]$cohortId
      )
      return(conceptCount)
    })
  
  getResolvedOrMappedConceptSecond <- shiny::reactive({
    data <- NULL
    if (is.null(input$databaseOrVocabularySchemaSecond)) {return(NULL)}
    databaseIdToFilter <- database %>%
      dplyr::filter(.data$databaseIdWithVocabularyVersion == input$databaseOrVocabularySchemaSecond) %>%
      dplyr::pull(.data$databaseId)
    
    conceptCounts <- getConceptSecondCountForAllDatabase()
    if (all(!is.null(conceptCounts),
            nrow(conceptCounts) > 0)) {
      conceptCounts <- conceptCounts %>% 
        dplyr::filter(.data$databaseId %in% !!databaseIdToFilter) %>% 
        dplyr::select(.data$conceptId, .data$sourceConceptId, .data$conceptSubjects, .data$conceptCount) %>% 
        dplyr::distinct()
      conceptCounts <- dplyr::bind_rows(
        conceptCounts %>% 
          dplyr::select(.data$conceptId, .data$conceptSubjects, .data$conceptCount),
        conceptCounts %>% 
          dplyr::select(.data$sourceConceptId, .data$conceptSubjects, .data$conceptCount) %>% 
          dplyr::rename("conceptId" = .data$sourceConceptId)
      ) %>% 
        dplyr::group_by(.data$conceptId) %>% 
        dplyr::summarise(conceptSubjects = conceptSubjects,
                         conceptCount = conceptCount) %>% 
        dplyr::distinct() %>% 
        dplyr::ungroup() %>% 
        dplyr::arrange(dplyr::desc(.data$conceptCount))
    }
    
    if (length(databaseIdToFilter) > 0) {
      resolvedOrMappedConceptSetForAllDatabase <-
        getResolvedOrMappedConceptSetSecondForAllDatabase()
      if (!is.null(resolvedOrMappedConceptSetForAllDatabase) &&
          length(resolvedOrMappedConceptSetForAllDatabase) == 2) {
        source <-
          (input$conceptSetsTypeSecond == "Mapped (source)")
        if (source) {
          data <- resolvedOrMappedConceptSetForAllDatabase$mapped 
          if (!is.null(data) && nrow(data) > 0) {
            data <- data %>%
              dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSecondRow()$id) %>%
              dplyr::filter(.data$databaseId %in% !!databaseIdToFilter) %>%
              dplyr::select(-.data$databaseId, -.data$conceptSetId) %>% 
              dplyr::filter(.data$conceptId != .data$resolvedConceptId) %>% 
              dplyr::relocate(.data$resolvedConceptId) %>% 
              dplyr::inner_join(resolvedOrMappedConceptSetForAllDatabase$resolved %>% 
                                  dplyr::select(.data$conceptId, .data$conceptName) %>% 
                                  dplyr::distinct() %>% 
                                  dplyr::rename("resolvedConceptId" = .data$conceptId,
                                                "resolvedConceptName" = .data$conceptName),
                                by = "resolvedConceptId") %>% 
              dplyr::mutate(resolvedConcept = paste0(.data$resolvedConceptId, " (", .data$resolvedConceptName, ")")) %>% 
              dplyr::select(-.data$resolvedConceptId, -.data$resolvedConceptName) %>% 
              dplyr::relocate(.data$resolvedConcept)
            # data$resolvedConcept <-
            #   as.factor(data$resolvedConcept)
          } else {
            data <- NULL
          }
        } else {
          data <- resolvedOrMappedConceptSetForAllDatabase$resolved %>%
            dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSecondRow()$id) %>%
            dplyr::filter(.data$databaseId %in% !!databaseIdToFilter) %>%
            dplyr::select(-.data$databaseId, -.data$conceptSetId, -.data$cohortId)
        }
      }
    }
    
    if (exists("vocabularyDatabaseSchemas") &&
        !is.null(input$databaseOrVocabularySchemaSecond) &&
        length(input$databaseOrVocabularySchemaSecond) > 0) {
      vocabularyDataSchemaToFilter <-
        intersect(vocabularyDatabaseSchemas,
                  input$databaseOrVocabularySchemaSecond)
    } else {
      vocabularyDataSchemaToFilter <- NULL
    }
    
    if (length(vocabularyDataSchemaToFilter) > 0) {
      resolvedOrMappedConceptSetForAllVocabulary <-
        getResolvedOrMappedConceptSetForAllVocabulary()
      if (!is.null(resolvedOrMappedConceptSetForAllVocabulary) &&
          length(resolvedOrMappedConceptSetForAllVocabulary) == 2) {
        source <-
          (input$conceptSetsTypeSecond == "Mapped (source)")
        if (source) {
          data <- resolvedOrMappedConceptSetForAllVocabulary$mapped %>%
            dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSecondRow()$id) %>%
            dplyr::filter(.data$vocabularyDatabaseSchema == !!vocabularyDataSchemaToFilter) %>%
            dplyr::select(-.data$vocabularyDatabaseSchema, -.data$conceptSetId) %>% 
            dplyr::filter(.data$conceptId != .data$resolvedConceptId) %>% 
            dplyr::relocate(.data$resolvedConceptId) %>% 
            dplyr::inner_join(resolvedOrMappedConceptSetForAllVocabulary$resolved %>% 
                                dplyr::select(.data$conceptId, .data$conceptName) %>% 
                                dplyr::distinct() %>% 
                                dplyr::rename("resolvedConceptId" = .data$conceptId,
                                              "resolvedConceptName" = .data$conceptName),
                              by = "resolvedConceptId") %>% 
            dplyr::mutate(resolvedConcept = paste0(.data$resolvedConceptId, " (", .data$resolvedConceptName, ")")) %>% 
            dplyr::select(-.data$resolvedConceptId, -.data$resolvedConceptName) %>% 
            dplyr::relocate(.data$resolvedConcept)
          # data$resolvedConcept <-
          #   as.factor(data$resolvedConcept)
        } else {
          data <- resolvedOrMappedConceptSetForAllVocabulary$resolved %>%
            dplyr::filter(.data$conceptSetId == cohortDefinitionConceptSetExpressionSecondRow()$id) %>%
            dplyr::filter(.data$vocabularyDatabaseSchema == !!vocabularyDataSchemaToFilter) %>%
            dplyr::select(-.data$vocabularyDatabaseSchema, -.data$conceptSetId)
        }
      }
    }
    if (!is.null(data) && nrow(data) > 0) {
      if (all(nrow(conceptCounts) > 0,
              'conceptId' %in% colnames(conceptCounts))) {
        data <- data %>% 
          dplyr::left_join(conceptCounts, by = "conceptId") %>% 
          dplyr::arrange(dplyr::desc(.data$conceptSubjects)) %>% 
          dplyr::relocate(.data$conceptSubjects, .data$conceptCount) %>% 
          dplyr::rename("subjects" = .data$conceptSubjects,
                        "count" = .data$conceptCount)
      }
      
      data$conceptClassId <- as.factor(data$conceptClassId)
      data$domainId <- as.factor(data$domainId)
      # data$conceptCode <- as.factor(data$conceptCode)
      data$conceptId <- as.character(data$conceptId)
      # data$conceptName <- as.factor(data$conceptName)
      data$vocabularyId <- as.factor(data$vocabularyId)
      data$standardConcept <- as.factor(data$standardConcept)
      
      data <- data %>% 
        dplyr::relocate(.data$conceptId, .data$conceptName)
    }
    return(data)
  })
  
  output$cohortDefinitionIncludedResolvedConceptsSecondTable <-
    DT::renderDataTable(expr = {
      data <- getResolvedOrMappedConceptSecond()
      
      validate(need(length(cohortDefinitionConceptSetExpressionSecondRow()$id) > 0,
                    "Please select concept set"))
      
      validate(need((all(!is.null(data), nrow(data) > 0)),
                    "No resolved or mapped concept ids"))
      
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
  
  output$saveCohortDefinitionIncludedResolvedConceptsSecondTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "resolvedConceptSet")
    },
    content = function(file) {
      downloadCsv(x = getResolvedOrMappedConceptSecond(), 
                  fileName = file)
    }
  )
  
  output$cohortDefinitionMappedConceptsSecondTable <-
    DT::renderDataTable(expr = {
      data <- getResolvedOrMappedConceptSecond()
      
      validate(need(length(cohortDefinitionConceptSetExpressionSecondRow()$id) > 0,
                    "Please select concept set"))
      
      validate(need((all(!is.null(data), nrow(data) > 0)),
                    "No resolved or mapped concept ids"))
      
      data <- data %>% 
        dplyr::mutate(
          conceptId = as.character(.data$conceptId),
          # conceptName = as.factor(.data$conceptName),
          vocabularyId = as.factor(.data$vocabularyId))
      
      columnDef <- list(
        truncateStringDef(2, 80)
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
        columnDefs = columnDef
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
  
  output$saveCohortDefinitionMappedConceptsSecondTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "mappedConceptSet")
    },
    content = function(file) {
      downloadCsv(x = getResolvedOrMappedConceptSecond(), 
                  fileName = file)
    }
  )
  
  #Radio button synchronization
  shiny::observeEvent(eventExpr = {
    input$conceptSetsType
  }, handlerExpr = {
    if (noOfRowSelectedInCohortDefinitionTable() == 6) {
      if (!is.null(input$conceptSetsType)) {
        if (input$conceptSetsType == "Concept Set Expression") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeSecond", selected = "Concept Set Expression")
        } else if (input$conceptSetsType == "Resolved (included)") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeSecond", selected = "Resolved (included)")
        } else if (input$conceptSetsType == "Mapped (source)") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeSecond", selected = "Mapped (source)")
        } else if (input$conceptSetsType == "Orphan concepts") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeSecond", selected = "Orphan concepts")
        } else if (input$conceptSetsType == "Json") {
          updateRadioButtons(session = session, inputId = "conceptSetsTypeSecond", selected = "Json")
        }
      }
    }
  })
  
  shiny::observeEvent(eventExpr = {
    input$conceptSetsTypeSecond
  }, handlerExpr = {
    if (noOfRowSelectedInCohortDefinitionTable() == 6) {
      if (!is.null(input$conceptSetsTypeSecond)) {
        if (input$conceptSetsTypeSecond == "Concept Set Expression") {
          updateRadioButtons(session = session, inputId = "conceptSetsType", selected = "Concept Set Expression")
        } else if (input$conceptSetsTypeSecond == "Resolved (included)") {
          updateRadioButtons(session = session, inputId = "conceptSetsType", selected = "Resolved (included)")
        } else if (input$conceptSetsTypeSecond == "Mapped (source)") {
          updateRadioButtons(session = session, inputId = "conceptSetsType", selected = "Mapped (source)")
        } else if (input$conceptSetsTypeSecond == "Orphan concepts") {
          updateRadioButtons(session = session, inputId = "conceptSetsType", selected = "Orphan concepts")
        } else if (input$conceptSetsTypeSecond == "Json") {
          updateRadioButtons(session = session, inputId = "conceptSetsType", selected = "Json")
        }
      }
    }
  })
  
  #Concept set comparison -----
  conceptsetComparisonData <- shiny::reactive(x = {
    leftData <- getResolvedOrMappedConcepts()
    rightData <- getResolvedOrMappedConceptSecond()
    data <- list(leftData = leftData, rightData = rightData)
    return(data)
  })
  
  output$resolvedConceptsPresentInLeft <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, "Please select same database for comparison"))
    result <- dplyr::setdiff(conceptsetComparisonData()$leftData, 
                             conceptsetComparisonData()$rightData)
    
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
  
  output$resolvedConceptsPresentInRight <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, "Please select same database for comparison"))
    result <- dplyr::setdiff(conceptsetComparisonData()$rightData, 
                             conceptsetComparisonData()$leftData)
    
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
  
  output$resolvedConceptsPresentInBoth <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, "Please select same database for comparison"))
    result <- dplyr::intersect(conceptsetComparisonData()$leftData, 
                               conceptsetComparisonData()$rightData)
    
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
  
  output$resolvedConceptsPresentInEither <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, "Please select same database for comparison"))
    result <- dplyr::union(conceptsetComparisonData()$leftData,
                           conceptsetComparisonData()$rightData)
    
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
  
  output$mappedConceptsPresentInLeft <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, "Please select same database for comparison"))
    result <- dplyr::setdiff(conceptsetComparisonData()$leftData, 
                             conceptsetComparisonData()$rightData)
    
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
  
  output$mappedConceptsPresentInRight <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, "Please select same database for comparison"))
    result <- dplyr::setdiff(conceptsetComparisonData()$rightData, 
                             conceptsetComparisonData()$leftData)
    
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
  
  output$mappedConceptsPresentInBoth <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, "Please select same database for comparison"))
    result <- dplyr::intersect(conceptsetComparisonData()$leftData, 
                               conceptsetComparisonData()$rightData)
    
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
  
  output$mappedConceptsPresentInEither <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, "Please select same database for comparison"))
    result <- dplyr::union(conceptsetComparisonData()$leftData,
                           conceptsetComparisonData()$rightData)
    
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
  
  output$orphanConceptsPresentInLeft <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, 
                  "Please select same database for comparison"))
    result <- dplyr::setdiff(orphanConceptComparisionLeftPanelData(), 
                             orphanConceptComparisionRightPanelData())
    orphanConceptDataDatabaseIds <- attr(x = orphanConceptComparisionLeftPanelData(), which = 'databaseIds')
    orphanConceptDataMaxCount <- attr(x = orphanConceptComparisionLeftPanelData(), which = 'maxCount')
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
            lapply(attr(x = orphanConceptComparisionLeftPanelData(), 
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
  
  output$orphanConceptsPresentInRight <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, 
                  "Please select same database for comparison"))
    result <- dplyr::setdiff(orphanConceptComparisionRightPanelData()$table,
                             orphanConceptComparisionLeftPanelData()$table)
    orphanConceptDataDatabaseIds <- attr(x = orphanConceptComparisionRightPanelData(), which = 'databaseIds')
    orphanConceptDataMaxCount <- attr(x = orphanConceptComparisionRightPanelData(), which = 'maxCount')
    
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
  
  output$orphanConceptsPresentInBoth <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, "Please select same database for comparison"))
    result <- dplyr::intersect(orphanConceptComparisionLeftPanelData()$table, 
                               orphanConceptComparisionRightPanelData()$table)
    orphanConceptDataDatabaseIds <- attr(x = orphanConceptComparisionLeftPanelData(), which = 'databaseIds')
    orphanConceptDataMaxCount <- attr(x = orphanConceptComparisionLeftPanelData(), which = 'maxCount')
    
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
  
  output$orphanConceptsPresentInEither <- DT::renderDT({
    validate(need(input$databaseOrVocabularySchema == input$databaseOrVocabularySchemaSecond, "Please select same database for comparison"))
    result <- dplyr::union(orphanConceptComparisionLeftPanelData()$table, 
                           orphanConceptComparisionRightPanelData()$table)
    orphanConceptDataDatabaseIds <- attr(x = orphanConceptComparisionLeftPanelData(), which = 'databaseIds')
    orphanConceptDataMaxCount <- attr(x = orphanConceptComparisionLeftPanelData(), which = 'maxCount')
    
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
  
  # Cohort Counts -----
  getCohortCountResultReactive <- shiny::reactive(x = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('cohortCount'))) {
      return(NULL)
    }
    data <- getResultsFromCohortCount(
      dataSource = dataSource,
      databaseIds = databaseIds(),
      cohortIds = cohortIds()
    ) 
    validate(need(all(!is.null(data) && nrow(data) > 0), "No data on cohort counts."))
    
    data <- data %>% 
      addShortName(cohort) %>%
      dplyr::arrange(.data$shortName, .data$databaseId)
    return(data)
  })
  
  output$saveCohortCountsTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "cohortCount")
    },
    content = function(file) {
      downloadCsv(x = getCohortCountResultReactive(), 
                  fileName = file)
    }
  )
  
  output$cohortCountsTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getCohortCountResultReactive() %>%
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
  
  output$cohortCountTableContainsData <- shiny::reactive({
    return(nrow(getCohortCountResultReactive()) > 0)
  })
  
  shiny::outputOptions(output,
                       "cohortCountTableContainsData",
                       suspendWhenHidden = FALSE)
  
  getCohortIdOnCohortCountRowSelect <- reactive({
    idx <- input$cohortCountsTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      subset <- getCohortCountResultReactive() %>%  
        dplyr::distinct(.data$cohortId)
      
      if (!is.null(subset)) {
        return(subset[idx,])
      } else {
        return(NULL)
      }
    }
    
  })
  
  output$cohortCountRowIsSelected <- reactive({
    return(!is.null(getCohortIdOnCohortCountRowSelect()))
  })
  
  outputOptions(output,
                "cohortCountRowIsSelected",
                suspendWhenHidden = FALSE)
  
  output$InclusionRuleStatForCohortSeletedTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(
      nrow(getCohortIdOnCohortCountRowSelect()) > 0,
      "No cohorts chosen"
    ))
    table <- getResultsFromInclusionRuleStatistics(
      dataSource = dataSource,
      cohortIds = getCohortIdOnCohortCountRowSelect()$cohortId,
      databaseIds = databaseIds()
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
  incidenceRateDataFull <- reactive({
    if (input$tabs == "incidenceRate") {
      validate(need(length(databaseIds()) > 0, "No data sources chosen"))
      validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
      if (all(is(dataSource, "environment"), !exists('incidenceRate'))) {
        return(NULL)
      }
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Getting incidence rate data."), value = 0)
      
      data <- getResultsFromIncidenceRate(
        dataSource = dataSource,
        cohortIds = cohortIds(),
        databaseIds = databaseIds())
      return(data)
    } else {
      return(NULL)
    }
  })
  
  incidenceRateData <- reactive({
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    data <- incidenceRateDataFull()
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
  
  shiny::observe({
    if (!is.null(incidenceRateDataFull()) &&
        nrow(incidenceRateDataFull()) > 0) {
      ageFilter <- incidenceRateDataFull() %>%
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
    if (!is.null(incidenceRateDataFull()) &&
        nrow(incidenceRateDataFull()) > 0) {
      genderFilter <- incidenceRateDataFull() %>%
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
    if (!is.null(incidenceRateDataFull()) &&
        nrow(incidenceRateDataFull()) > 0) {
      calendarFilter <- incidenceRateDataFull() %>%
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
      
      minIncidenceRateValue <- round(min(incidenceRateDataFull()$incidenceRate),digits = 2)
      
      maxIncidenceRateValue <- round(max(incidenceRateDataFull()$incidenceRate),digits = 2)
      
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
  
  incidenceRateCalendarFilter <- shiny::reactive({
    calendarFilter <- incidenceRateDataFull() %>%
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
  
  
  incidenceRateYScaleFilter <- shiny::reactive({
    incidenceRateFilter <- incidenceRateDataFull() %>%
      dplyr::select(.data$incidenceRate) %>%
      dplyr::filter(.data$incidenceRate != "NA",
                    !is.na(.data$incidenceRate)) %>%
      dplyr::distinct(.data$incidenceRate) %>%
      dplyr::arrange(.data$incidenceRate)
    incidenceRateFilter <-
      incidenceRateFilter[incidenceRateFilter$incidenceRate >= input$YscaleMinAndMax[1] &
                            incidenceRateFilter$incidenceRate <= input$YscaleMinAndMax[2], drop = FALSE] %>%
      dplyr::pull(.data$incidenceRate)
    return(incidenceRateFilter)
  })
  
  output$saveIncidenceRatePlot <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "IncidenceRate")
    },
    content = function(file) {
      downloadCsv(x = incidenceRateDataFull(), 
                  fileName = file)
    }
  )
  
 
  
  output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    
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
        length(cohortIds()),
        " cohorts and ",
        length(databaseIds()),
        " databases"
      ),{
        data <- incidenceRateData()
        
        validate(need(all(!is.null(data), nrow(data) > 0), paste0("No data for this combination")))
        
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
            dplyr::filter(.data$calendarYear %in% incidenceRateCalendarFilter())
        }
        if (input$irYscaleFixed) {
          data <- data %>%
            dplyr::filter(.data$incidenceRate %in% incidenceRateYScaleFilter())
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
  
  # calendar Incidence -----
  # calendarIncidenceData <- shiny::reactive({
  #   data <- getResultsFromCalendarIncidence(
  #     dataSource,
  #     cohortIds = cohortIds(),
  #     databaseIds = databaseIds()
  #   )
  #   return(data);
  # })
  # 
  # output$calendarIncidencePlot <- ggiraph::renderggiraph(expr = {
  #   validate(need(length(databaseIds()) > 0, "No data sources chosen"))
  #   validate(need(length(cohortIds()) > 0, "No cohorts chosen")) 
  #   shiny::withProgress(
  #     message = paste(
  #       "Building calendar incidence plot data for ",
  #       length(cohortIds()),
  #       " cohorts and ",
  #       length(databaseIds()),
  #       " databases"
  #     ),{
  #       data <- calendarIncidenceData()
  #       
  #       validate(need(all(!is.null(data), nrow(data) > 0), paste0("No data for this combination"))) 
  #       plot <- plotCalendarIncidence(
  #         data = data,
  #         cohortCount = cohortCount,
  #         shortNameRef = cohort,
  #         yscaleFixed = input$calendarIncidenceYscaleFixed
  #       )
  #       return(plot)
  #     })
  #   })
  # 
  # output$saveCalendarIncidencePlot <-  downloadHandler(
  #   filename = function() {
  #     getFormattedFileName(fileName = "CalendarIncidence")
  #   },
  #   content = function(file) {
  #     write.csv(calendarIncidenceData(), file)
  #   }
  # )
  
  
  # Time Series -----
  ## Tssible data ----
  timeSeriesTssibleData <- shiny::reactiveVal(NULL)
  timeSeriesData <- reactive({
    if (input$tabs == "timeSeries") {
      validate(need(length(databaseIds()) > 0, "No data sources chosen"))
      validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
      if (all(is(dataSource, "environment"), !exists('timeSeries'))) {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Getting time series data."), value = 0)
      
      data <- getResultsFromFixedTimeSeries(
        dataSource = dataSource,
        cohortIds = cohortIds(),
        databaseIds = databaseIds()
      )
      return(data)
    } else {
      return(NULL)
    }
  })
  
  ## Data filtered by calendarInterval + range ----
  timeSeriesDataFiltered <- reactive({
    calendarIntervalFirstLetter <- tolower(substr(input$timeSeriesFilter,1,1))
    data <- timeSeriesData()
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
  
  getTimeSeriesDescription <- shiny::reactive({
    data <- timeSeriesData()
    if (any(is.null(data), nrow(data) == 0)) {
      return(NULL)
    }
    calendarIntervalFirstLetter <- tolower(substr(input$timeSeriesFilter,1,1))
    
    data <- data[[calendarIntervalFirstLetter]]
    timeSeriesDescription <- attr(x = data,which = "timeSeriesDescription")
    return(timeSeriesDescription)
  })
  
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
  
  ## Filter: series type ----
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
  
  ## Filter: Period range ----
  shiny::observe({
    calendarIntervalFirstLetter <- tolower(substr(input$timeSeriesFilter,1,1))
    data <- timeSeriesData()
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
  
  ## Output Data table ----
  output$timeSeriesTable <- DT::renderDataTable({
    
    timeSeriesDescription <- getTimeSeriesDescription()
    
    validate(need(all(!is.null(timeSeriesDescription),
                      nrow(timeSeriesDescription) > 0,
                  !is.null(timeSeriesDataFiltered()),
                  nrow(timeSeriesDataFiltered()) > 0),
                  "No timeseries data for the combination."))
    data <- timeSeriesDataFiltered() %>% 
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
  
  ## Output Time series plot ----
  output$timeSeriesPlot <- ggiraph::renderggiraph({
    
    timeSeriesDescription <- getTimeSeriesDescription()
    
    validate(need(all(!is.null(timeSeriesDescription),
                      nrow(timeSeriesDescription) > 0,
                      !is.null(timeSeriesDataFiltered()),
                      nrow(timeSeriesDataFiltered()) > 0),
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
  
  # Time distribution -------
  timeDistributionData <- reactive({
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('timeDistribution'))) {
      return(NULL)
    }
    data <- getResultsFromTimeDistribution(
      dataSource = dataSource,
      cohortIds = cohortIds(),
      databaseIds = databaseIds()
    )
    return(data)
  })
  
  output$timeDisPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    data <- timeDistributionData()
    validate(need(nrow(data) > 0, paste0("No data for this combination")))
    plot <- plotTimeDistribution(data = data, shortNameRef = cohort)
    return(plot)
  })
  
  output$saveTimeDistTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "timeDistribution")
    },
    content = function(file) {
      downloadCsv(x = timeDistributionData(), 
                  fileName = file)
    }
  )
  
  output$timeDistTable <- DT::renderDataTable(expr = {
    data <- timeDistributionData()  %>%
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
  
  # included concepts table /concepts in data source-----
  includedConceptsData <- shiny::reactive(x = {
    validate(need(all(!is.null(databaseIds()), length(databaseIds()) > 0), 
                  "No data sources chosen"))
    validate(need(all(!is.null(cohortId()),length(cohortId()) > 0),
                  "No cohort chosen"))
    if (all(is(dataSource, "environment"), !exists('includedSourceConcept'))) {
      return(NULL)
    }
    browser()
    includedConcepts <- getResultsFromConceptCount(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = databaseIds()
    )
    
    if (is.null(includedConcepts)) {return(NULL)}
    
    includedConcepts <- includedConcepts %>% 
      dplyr::inner_join(conceptSets %>% dplyr::select(.data$cohortId,
                                                      .data$conceptSetId,
                                                      .data$conceptSetName), 
                        by = c("cohortId", "conceptSetId"))
    concept <- getResultsFromConcept(dataSource = dataSource,
                                 conceptIds = c(includedConcepts$conceptId, includedConcepts$sourceConceptId) %>% unique())
    includedConcepts <- includedConcepts %>% 
      dplyr::inner_join(concept %>% 
                          dplyr::rename(sourceConceptId = .data$conceptId,
                                        sourceConceptName = .data$conceptName,
                                        sourceVocabularyId = .data$vocabularyId,
                                        sourceConceptCode = .data$conceptCode) %>% 
                          dplyr::select(.data$sourceConceptId, .data$sourceConceptName, 
                                        .data$sourceVocabularyId, .data$sourceConceptCode),
                        by = c("sourceConceptId")) %>%
      dplyr::inner_join(concept %>% 
                          dplyr::select(
                            .data$conceptId,
                            .data$conceptName,
                            .data$vocabularyId),
                        by = c("conceptId"))
    return(includedConcepts)
  })
  
  output$saveIncludedConceptsTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "includedConcept")
    },
    content = function(file) {
      downloadCsv(x = includedConceptsData(), 
                  fileName = file)
    }
  )
  
  output$includedConceptsTable <- DT::renderDataTable(expr = {
    validate(need(all(!is.null(databaseIds()), length(databaseIds()) > 0), 
                  "No data sources chosen"))
    validate(need(all(!is.null(cohortId()),length(cohortId()) > 0),
                  "No cohort chosen"))
    
    data <- includedConceptsData()
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
    
    databaseIdsWithCount <- getSubjectCountsByDatabasae(data = data, cohortId = cohortId(), databaseIds = databaseIds())
    
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
  
  output$includeConceptsTableContainsData <- shiny::reactive({
    return(nrow(includedConceptsData()) > 0)
  })
  
  shiny::outputOptions(output,
                       "includeConceptsTableContainsData",
                       suspendWhenHidden = FALSE)
  
  # orphan concepts table -------
  
  orphanConceptsData <- shiny::reactive(x = {
    validate(need(all(!is.null(databaseIds()), length(databaseIds()) > 0), "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('orphanConcept'))) {
      return(NULL)
    }
    orphanConcepts <- getResultsFromOrphanConcept(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = databaseIds()
    )
    validate(need(!is.null(orphanConcepts), "No orphan concepts"))
    orphanConcepts <- orphanConcepts %>% 
      dplyr::inner_join(conceptSets %>% dplyr::select(
        .data$cohortId,
        .data$conceptSetId,
        .data$conceptSetName), 
        by = c("cohortId", "conceptSetId"))
    concepts <- getResultsFromConcept(dataSource = dataSource,
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
  
  output$saveOrphanConceptsTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "orphanConcept")
    },
    content = function(file) {
      downloadCsv(x = orphanConceptsData(), 
                  fileName = file)
    }
  )
  
  output$orphanConceptsTable <- DT::renderDataTable(expr = {
    
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(all(!is.null(cohortId()),
                      length(cohortId()) > 0), "No cohorts chosen"))
    
    data <- orphanConceptsData()
    validate(need(all(!is.null(data), nrow(data) > 0),
                  "There is no data for the selected combination."))
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    
    if (!is.null(input$conceptSetsToFilterCharacterization) && 
        length(input$conceptSetsToFilterCharacterization) > 0) {
      if (!is.null(input$conceptSetsToFilterCharacterization)) {
        if (length(conceptSetIds()) > 0) {
          data <- data %>% 
            dplyr::filter(.data$conceptSetId %in% conceptSetIds())
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
    databaseIdsWithCount <- getSubjectCountsByDatabasae(data = data, cohortId = cohortId(), databaseIds = databaseIds())
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
    return(nrow(orphanConceptsData()) > 0)
  })
  
  shiny::outputOptions(output,
                       "orphanconceptContainData",
                       suspendWhenHidden = FALSE)
  
  # Inclusion rules table ----
  inclusionRuleTableData <- shiny::reactive(x = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('inclusionRuleStats'))) {
      return(NULL)
    }
    data <- getResultsFromInclusionRuleStatistics(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = databaseIds()
    )
    return(data)
  })
  
  output$saveInclusionRuleTable <-  downloadHandler(
    filename = function() {
      getFormattedFileName(fileName = "inclusionRule")
    },
    content = function(file) {
      downloadCsv(x = inclusionRuleTableData(), 
                  fileName = file)
    }
  )
  
  output$inclusionRuleTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
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
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('indexEventBreakdown'))) {
      return(NULL)
    }
    data <- getResultsFromIndexEventBreakdown(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = databaseIds())
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
    conceptIdDetails <- getResultsFromConcept(dataSource = dataSource,
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
      getFormattedFileName(fileName = "indexEventBreakdown")
    },
    content = function(file) {
      downloadCsv(x = indexEventBreakDownDataFilteredByRadioButton(), 
                  fileName = file)
    }
  )
  
  output$breakdownTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen chosen"))
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
      for (i in 1:length(databaseIds())) {
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
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    if (all(is(dataSource, "environment"), !exists('visitContext'))) {
      return(NULL)
    }
    visitContext <- getResultsFromVisitContext(
      dataSource = dataSource,
      cohortIds = cohortId(),
      databaseIds = databaseIds()
    )
    
    if (is.null(visitContext) || nrow(visitContext) == 0) {
      return(NULL)
    }
    # to ensure backward compatibility to 2.1 when visitContext did not have visitConceptName
    if (!'visitConceptName' %in% colnames(visitContext)) {
      concepts <- getResultsFromConcept(dataSource = dataSource, 
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
      getFormattedFileName(fileName = "visitContext")
    },
    content = function(file) {
      downloadCsv(x = visitContexData(), 
                  fileName = file)
    }
  )
  
  output$visitContextTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
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
      dplyr::filter(.data$cohortId == cohortId()) %>% 
      dplyr::filter(.data$databaseId %in% databaseIds()) %>% 
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
      if (any(length(cohortId()) != 1,
              length(databaseIds()) == 0)) {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Extracting characterization data for target cohort:", cohortId()), 
                   value = 0)
      
      data <- getMultipleCharacterizationResults(
        dataSource = dataSource,
        cohortIds = cohortId(),
        databaseIds = databaseIds()
      )
      return(data)
    } else {
      return(NULL)
    }
  })
  
  ## Characterization data ------
  characterizationData <- shiny::reactive(x = {
    if (any(length(cohortId()) != 1,
            length(databaseIds()) == 0,
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
    if (any(length(cohortId()) == 0,
            length(databaseIds()) == 0)) {
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
      getFormattedFileName(fileName = "cohortCharacterization")
    },
    content = function(file) {
      downloadCsv(x = characterizationTableData(), 
                  fileName = file)
    }
  )
  ### Output ------
  output$characterizationTable <- DT::renderDataTable(expr = {
    data <- characterizationTableData()
    validate(need(all(!is.null(cohortId()),
                      length(cohortId()) > 0),
                  "No data for the combination"))
    validate(need(!is.null(data), "No data for the combination"))
    
    databaseIds <- sort(unique(data$databaseId))
    
    cohortCounts <- data %>% 
      dplyr::inner_join(cohortCount,
                        by = c("cohortId", "databaseId")) %>% 
      dplyr::filter(.data$cohortId == cohortId()) %>% 
      dplyr::filter(.data$databaseId %in% databaseIds()) %>% 
      dplyr::select(.data$cohortSubjects) %>% 
      dplyr::pull(.data$cohortSubjects) %>% unique()
    
    databaseIdsWithCount <- paste(databaseIds, "(n = ", format(cohortCounts, big.mark = ","), ")")
    
    if (input$charType == "Pretty") {
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Rendering pretty table for cohort characterization."), value = 0)
      
      countData <- getResultsFromCohortCount(
        dataSource = dataSource,
        databaseIds = databaseIds(),
        cohortIds = cohortId()
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
    if (any(length(cohortId()) != 1,
            length(databaseIds()) == 0,
            is.null(characterizationTemporalCharacterizationData()$covariateValue),
            nrow(characterizationTemporalCharacterizationData()$covariateValue) == 0,
            is.null(characterizationTemporalCharacterizationData()$covariateRef),
            nrow(characterizationTemporalCharacterizationData()$covariateRef) == 0,
            is.null(characterizationTemporalCharacterizationData()$analysisRef),
            nrow(characterizationTemporalCharacterizationData()$analysisRef) == 0)) {
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
      getFormattedFileName(fileName = "temporalCharacterizationTableData")
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
    if (length(timeIds()) > 0) {
      data <- data %>% 
        dplyr::filter(.data$timeId %in% timeIds())
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
      cohortIds = cohortIds(),
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
      getFormattedFileName(fileName = "cohortOverlap")
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
      if (any(length(cohortId()) != 1,
              length(comparatorCohortId()) != 1,
              length(databaseIds()) != 1)) {
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
                                    cohortId(),
                                    " and comparator cohort:",
                                    comparatorCohortId(),
                                    ' for ',
                                    input$database), 
                   value = 0)
      
      data <- getMultipleCharacterizationResults(
        dataSource = dataSource,
        cohortIds = c(cohortId(), comparatorCohortId()) %>% unique(),
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
    if (any(length(cohortId()) != 1,
            length(comparatorCohortId()) != 1,
            length(databaseIds()) == 0,
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
      dplyr::filter(.data$cohortId == cohortId()) %>% 
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
      dplyr::filter(.data$cohortId == comparatorCohortId()) %>% 
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
      getFormattedFileName(fileName = "compareCohortCharacterization")
    },
    content = function(file) {
      downloadCsv(x = computeBalance(), 
                  fileName = file)
    }
  )
  ### Output ------
  output$charCompareTable <- DT::renderDataTable(expr = {
    validate(need((length(cohortId()) > 0), 
                  paste0("Please select cohort.")))
    validate(need((length(comparatorCohortId()) > 0), 
                  paste0("Please select comparator cohort.")))
    validate(need((comparatorCohortId() != cohortId()),
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
      validate(need((length(cohortId(
      )) > 0),
      paste0("Please select cohort.")))
      validate(need((length(
        comparatorCohortId()
      ) > 0),
      paste0("Please select comparator cohort.")))
      validate(need((comparatorCohortId() != cohortId()),
                    paste0("Please select different cohorts for target and comparator cohorts.")
      ))
      validate(need((length(input$database) > 0),
                    paste0("Please select atleast one datasource.")
      ))
      validate(need((length(timeIds()) > 0), paste0("Please select time id")))
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Computing compare temporal characterization."), value = 0)
      
      validate(need(!is.null(compareCharacterizationTemporalCharacterizationData()$covariateValue) &&
                      nrow(compareCharacterizationTemporalCharacterizationData()$covariateValue) > 0, "No Characterization data"))
      
      data <- compareCharacterizationTemporalCharacterizationData()$covariateValue %>% 
        dplyr::filter(.data$characterizationSource %in% c('CT', 'FT')) %>% 
        dplyr::filter(.data$timeId %in% timeIds()) %>% 
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
        dplyr::filter(.data$cohortId == cohortId()) %>% 
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
      
      validate(need((nrow(covs1) > 0), paste0("Target cohort id:", cohortId(), " does not have data.")))
      covs2 <- data %>% 
        dplyr::filter(.data$cohortId == comparatorCohortId()) %>% 
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
      validate(need((nrow(covs2) > 0), paste0("Target cohort id:", comparatorCohortId(), " does not have data.")))
      
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
      getFormattedFileName(fileName = "compareTemporalCharacterization")
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
          dplyr::filter(.data$timeId %in% c(timeIds())) %>%
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
    
    if (any(is.null(cohortIds()), length(cohortIds()) == 0)) {return(NULL)}
    if (any(is.null(cohortSubset()), nrow(cohortSubset()) == 0)) {return(NULL)}
    if (any(is.null(databaseIds()), nrow(databaseIds()) == 0)) {return(NULL)}
    
    cohortSelected <- cohortSubset() %>%
      dplyr::filter(.data$cohortId %in% cohortIds()) %>%
      dplyr::arrange(.data$cohortId)
    
    databaseIdsWithCount <- cohortCount %>% 
      dplyr::filter(.data$databaseId %in% databaseIds()) %>% 
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
        dplyr::filter(.data$databaseId %in% databaseIds())
      
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
              scales::percent(length(cohortSubjectRecordRatioEq1)/length(databaseIds()), accuracy = 0.1),
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
              length(databaseIds()),
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
