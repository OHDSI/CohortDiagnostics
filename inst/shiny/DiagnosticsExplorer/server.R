shiny::shinyServer(function(input, output, session) {
  #______________----
  #Reactive functions that are initiated on start up----
  ##getNonEraCdmTableShortNames----
  getNonEraCdmTableShortNames <- shiny::reactive({
    data <- getDomainInformation()$wide %>%
      dplyr::filter(.data$isEraTable == FALSE) %>%
      dplyr::select(.data$domainTableShort) %>%
      dplyr::distinct() %>%
      dplyr::arrange() %>%
      dplyr::pull()
    return(data)
  })
 
  ##getCohortSortedByCohortId ----
  getCohortSortedByCohortId <- shiny::reactive({
    data <- cohort %>%
      dplyr::arrange(.data$cohortId)
    return(data)
  })
  
  ##getDataSourceTimeSeries ----
  getDataSourceTimeSeries <- shiny::reactive({
    data <- getResultsFixedTimeSeries(dataSource = dataSource, 
                                      seriesType = "T3", 
                                      cohortIds = 0)
    return(data)
  })
  
  #______________----
  #Selections----
  ##pickerInput: conceptSetsSelectedTargetCohort----
  #defined in UI
  shiny::observe({
    if (exists("conceptSets")) {
      if (!hasData(conceptSets)) {
        return(NULL)
      }
    }
    
    data <- conceptSets
    dataFiltered <- data %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::pull(.data$compoundName) %>% 
      unique() %>% 
      sort()
    data <- data %>%
      dplyr::pull(.data$compoundName) %>% 
      sort()
    
    if (!hasData(data)) {
      return(NULL)
    }
    if (input$tabs == "indexEventBreakdown") {
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "conceptSetsSelectedTargetCohort",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = data,
        selected = dataFiltered
      )
    } else {
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "conceptSetsSelectedTargetCohort",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = data
      )
    }
  })
  
  ##getComparatorCohortIdFromSelectedCompoundCohortNames----
  getComparatorCohortIdFromSelectedCompoundCohortNames <-
    shiny::reactive({
      data <- cohort %>%
        dplyr::filter(.data$compoundName %in% input$selectedComparatorCompoundCohortNames) %>%
        dplyr::arrange(.data$cohortId) %>%
        dplyr::pull(.data$cohortId) %>%
        unique()
      if (!hasData(data)) {
        return(NULL)
      }
      return(data)
    })
  
  # ##reactiveVal: getTimeIdsFromSelectedTemporalCovariateChoices----
  # getTimeIdsFromSelectedTemporalCovariateChoices <-
  #   reactiveVal(NULL)
  # shiny::observeEvent(eventExpr = {
  #   list(input$timeIdChoices_open,
  #        input$tabs)
  # }, handlerExpr = {
  #   if (exists('temporalCovariateChoices') &&
  #       (any(
  #         isFALSE(input$timeIdChoices_open),
  #         !is.null(input$tabs)
  #       ))) {
  #     browser()
  #     selectedTemporalCovariateChoices <- temporalCovariateChoices %>%
  #       dplyr::filter(.data$choices %in% input$timeIdChoices)
  #     getTimeIdsFromSelectedTemporalCovariateChoices(selectedTemporalCovariateChoices)
  #   }
  # })
  
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
  
  ##pickerInput: selectedComparatorCompoundCohortName----
  shiny::observe({
    subset <- getCohortSortedByCohortId()$compoundName
    if (input$tabs == "cohortDefinition") {
      selected <-  NULL
    } else {
      selected <- subset[2]
    }
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "selectedComparatorCompoundCohortName",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = selected
    )
  })
  
  ##pickerInput: selectedComparatorCompoundCohortNames----
  shiny::observe({
    subset <- getCohortSortedByCohortId()$compoundName
    
    if (input$tabs == "cohortCharacterization") {
      selected <- NULL
    } else {
      selected <- c(subset[3], subset[4])
    }
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "selectedComparatorCompoundCohortNames",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = selected
    )
  })
  
  ##getUserSelection----
  getUserSelection <- shiny::reactive(x = {
    list(
      input$tabs,
      reactable::getReactableState("targetCohortDefinitionConceptSetsTable", "selected"),
      reactable::getReactableState("comparatorCohortDefinitionConceptSets", "selected"),
      reactable::getReactableState("targetCohortDefinitionResolvedConceptTable", "selected"),
      reactable::getReactableState("comparatorCohortDefinitionResolvedConceptTable", "selected"),
      reactable::getReactableState("targetCohortDefinitionExcludedConceptTable", "selected"),
      reactable::getReactableState("comparatorCohortDefinitionExcludedConceptTable", "selected"),
      reactable::getReactableState("targetCohortDefinitionOrphanConceptTable", "selected"),
      reactable::getReactableState("comparatorCohortDefinitionOrphanConceptTable", "selected"),
      reactable::getReactableState("targetCohortDefinitionMappedConceptTable", "selected"),
      reactable::getReactableState("comparatorCohortDefinitionMappedConceptTable", "selected"),
      input$selectedDatabaseId,
      input$selectedDatabaseIds,
      input$selectedDatabaseIds_open,
      input$selectedCompoundCohortName,
      input$selectedComparatorCompoundCohortName,
      input$selectedCompoundCohortNames,
      input$selectedCompoundCohortNames_open,
      input$conceptSetsSelectedTargetCohort,
      reactable::getReactableState("indexEventBreakdownReactTable", "selected"),
      input$targetVocabularyChoiceForConceptSetDetails,
      input$selectedComparatorCompoundCohortNames,
      input$selectedComparatorCompoundCohortNames_open
    )
  })
  
  consolidatedCohortIdTarget <- reactiveVal(NULL)
  consolidatedCohortIdComparator <- reactiveVal(NULL)
  consolidatedConceptSetIdTarget <- reactiveVal(NULL)
  consolidatedConceptSetIdComparator <- reactiveVal(NULL)
  consolidatedDatabaseIdTarget <- reactiveVal(NULL)
  consolidatedConceptIdTarget <- reactiveVal(NULL)
  consolidatedConceptIdComparator <- reactiveVal(NULL)
  consolidateCohortDefinitionActiveSideTarget <- reactiveVal(NULL)
  consolidateCohortDefinitionActiveSideComparator <-
    reactiveVal(NULL)
  
  ##reactiveVal: consolidatedSelectedFieldValue----
  consolidatedSelectedFieldValue <- reactiveVal(list())
  #Reset Consolidated reactive val
  observeEvent(eventExpr = getUserSelection(),
               handlerExpr = {
                 data <- consolidationOfSelectedFieldValues(
                   input = input,
                   cohort = getCohortSortedByCohortId(),
                   conceptSets = conceptSets,
                   conceptSetExpressionTarget = getConceptSetsInCohortDataTarget(),
                   conceptSetExpressionComparator = getConceptSetsInCohortDataComparator(),
                   database = database,
                   resolvedConceptSetDataTarget = getResolvedConceptsTarget(),
                   resolvedConceptSetDataComparator = getResolvedConceptsComparator(),
                   orphanConceptSetDataTarget = getOrphanConceptsTarget(),
                   orphanConceptSetDataComparator = getOrphanConceptsComparator(),
                   excludedConceptSetDataTarget = getExcludedConceptsTarget(),
                   excludedConceptSetDataComparator = getExcludedConceptsComparator(),
                   indexEventBreakdownDataTable = getIndexEventBreakdownTargetDataFiltered(),
                   mappedConceptSetTarget = getMappedConceptsTarget(),
                   mappedConceptSetComparator = getMappedConceptsComparator()
                 ) 
                 
                 if ((isFALSE(input$selectedCompoundCohortNames_open) ||
                     is.null(input$selectedCompoundCohortNames_open)) && 
                     hasData(input$tabs)) {
                   consolidatedCohortIdTarget(data$cohortIdTarget)
                 }
                 
                 if ((isFALSE(input$selectedComparatorCompoundCohortNames_open) ||
                      is.null(input$selectedComparatorCompoundCohortNames_open)) && 
                     hasData(input$tabs)) {
                   consolidatedCohortIdComparator(data$cohortIdComparator)
                 }
                 
                 consolidatedConceptSetIdTarget(data$conceptSetIdTarget)
                 consolidatedConceptSetIdComparator(data$conceptSetIdComparator)
                 
                 if ((isFALSE(input$selectedDatabaseIds_open) ||
                      is.null(input$selectedDatabaseIds_open)) && 
                     hasData(input$tabs)) {
                   consolidatedDatabaseIdTarget(data$selectedDatabaseIdTarget)
                 }
                 
                 consolidatedConceptIdTarget(data$selectedConceptIdTarget)
                 consolidatedConceptIdComparator(data$selectedConceptIdComparator)
                 consolidateCohortDefinitionActiveSideTarget(data$TargetActive)
                 consolidateCohortDefinitionActiveSideComparator(data$comparatorActive)
               })
  
  #______________----
  #cohortDefinition tab----
  ##Cohort definition----
  
  ##Dynamic UI rendering for target side -----
  output$dynamicUIGenerationForCohortSelectedTarget <-
    shiny::renderUI(expr = {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = "Rendering the UI for target cohort details",
        value = 0
      )
      shiny::column(
        getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable(),
        shiny::conditionalPanel(
          condition = "output.isCohortDefinitionRowSelected == true",
          shiny::htmlOutput(outputId = "targetCohortSelectedInCohortDefinitionTable"),
          shiny::tabsetPanel(
            type = "tab",
            id = "targetCohortDefinitionTabSetPanel",
            shiny::tabPanel(
              title = "Cohort Count",
              value = "targetCohortDefinitionCohortCountTabPanel",
              tags$br(),
              tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('targetCohortDefinitionCohortCountTable')"),
              reactable::reactableOutput(outputId = "targetCohortDefinitionCohortCountTable")
            ),
            shiny::tabPanel(
              title = "Inclusion rules",
              value = "targetCohortdefinitionInclusionRuleTabPanel",
              tags$br(),
              tags$table(width = "100%",
                         tags$tr(
                           tags$td(
                             shiny::radioButtons(
                               inputId = "targetCohortDefinitionInclusionRuleType",
                               label = "Select: ",
                               choices = c("Events"), #, "Persons"
                               selected = "Events",
                               inline = TRUE
                             )
                           ),
                           tags$td(
                             shiny::conditionalPanel(
                               condition = "input.targetCohortDefinitionInclusionRuleType == 'Events' &
                                         output.getSimplifiedInclusionRuleResultsTargetHasData == true",
                               shiny::radioButtons(
                                 inputId = "targetCohortDefinitionSimplifiedInclusionRuleTableFilters",
                                 label = "Filter by",
                                 choices = c("All", "Meet", "Gain", "Remain", "Totals"),
                                 selected = "All",
                                 inline = TRUE
                               )
                             )
                           ),
                           tags$td(
                             shiny::checkboxInput(
                               inputId = "targetCohortInclusionRulesAsPercent",
                               label = "Show As Percent",
                               value = FALSE
                             )
                           )
                         )),
              shiny::conditionalPanel(
                condition = "input.targetCohortDefinitionInclusionRuleType == 'Events'",
                tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('targetCohortDefinitionSimplifiedInclusionRuleTable')"),
                reactable::reactableOutput(outputId = "targetCohortDefinitionSimplifiedInclusionRuleTable")
              )
            ),
            shiny::tabPanel(
              title = "Details",
              value = "targetCohortDefinitionDetailsTextTabPanel",
              tags$br(),
              shinydashboard::box(
                title = "Readable definitions",
                width = NULL,
                status = NULL,
                collapsible = TRUE,
                collapsed = FALSE,
                solidHeader = FALSE,
                copyToClipboardButton(toCopyId = "targetCohortDefinitionText",
                                      style = "margin-top: 5px; margin-bottom: 5px;"),
                shiny::htmlOutput("targetCohortDefinitionText")
              ),
              shinydashboard::box(
                title = "Meta data",
                width = NULL,
                status = NULL,
                collapsible = TRUE,
                collapsed = TRUE,
                solidHeader = FALSE,
                tags$p("To do")
              )
            ),
            shiny::tabPanel(
              title = "Concept Sets",
              value = "targetCohortDefinitionConceptSetTabPanel",
              tags$br(),
              tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('targetCohortDefinitionConceptSetsTable')"),
              reactable::reactableOutput(outputId = "targetCohortDefinitionConceptSetsTable"),
              tags$br(),
              shiny::conditionalPanel(
                condition = "output.isTargetCohortDefinitionConceptSetsTableRowSelected == true",
                shinydashboard::box(
                  title = shiny::htmlOutput(outputId = "targetConceptSetExpressionName"),
                  width = NULL,
                  solidHeader = FALSE,
                  collapsible = TRUE,
                  collapsed = FALSE,
                  shiny::conditionalPanel(condition = "output.isTargetCohortDefinitionConceptSetsTableRowSelected == true",
                                          tags$table(width = "100%",
                                            tags$tr(tags$td(
                                              colspan = 2,
                                              shiny::radioButtons(
                                                inputId = "targetConceptSetsType",
                                                label = "",
                                                choices = c(
                                                  "Concept Set Expression",
                                                  "Resolved",
                                                  "Excluded",
                                                  "Recommended",
                                                  "Mapped",
                                                  "Concept Set Json",
                                                  "Concept Set Sql"
                                                ),
                                                selected = "Concept Set Expression",
                                                inline = TRUE
                                              )
                                            )),
                                            tags$tr(
                                              tags$td(align = "left",
                                                      shiny::conditionalPanel(
                                                        condition = "input.targetConceptSetsType != 'Concept Set Json' &
                                                                   input.targetConceptSetsType != 'Concept Set Sql' &
                                                                   input.targetConceptSetsType != 'Concept Set Expression'",
                                                        shiny::radioButtons(
                                                          inputId = "targetConceptIdCountSource",
                                                          label = "",
                                                          choices = c("Datasource Level", "Cohort Level"),
                                                          selected = "Datasource Level",
                                                          inline = TRUE
                                                        )
                                                      )
                                              ),
                                              tags$td(align = "right",
                                                      shiny::conditionalPanel(
                                                        condition = "input.targetConceptSetsType != 'Concept Set Json' &
                                                                   input.targetConceptSetsType != 'Concept Set Sql' &
                                                                   input.targetConceptSetsType != 'Concept Set Expression'",
                                                        shiny::radioButtons(
                                                          inputId = "targetCohortConceptSetColumnFilter",
                                                          label = "",
                                                          choices = c("Both", "Persons", "Records"),
                                                          selected = "Both",
                                                          inline = TRUE
                                                        )
                                                      )
                                              ),
                                              tags$td(align = "right",
                                                      shiny::conditionalPanel(
                                                        condition = "input.targetConceptSetsType != 'Concept Set Json' &
                                                                   input.targetConceptSetsType != 'Concept Set Sql' &
                                                                   input.targetConceptSetsType != 'Concept Set Expression'",
                                                        shiny::checkboxInput(
                                                          inputId = "showAsPercentageColumnTarget",
                                                          label = "Show As Percent",
                                                          value = FALSE
                                                        )
                                                      )
                                              )
                                            )
                                          )),
                  shiny::conditionalPanel(
                    condition = "output.isTargetCohortDefinitionConceptSetsTableRowSelected == true &
                                                      input.targetConceptSetsType == 'Concept Set Expression'",
                    tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('targetConceptSetsExpressionTable')"),
                    reactable::reactableOutput(outputId = "targetConceptSetsExpressionTable"),
                    tags$br(),
                    shiny::conditionalPanel(
                      condition = "output.canTargetConceptSetExpressionBeOptimized &
                                   !output.isComparatorSelected ",
                      shinydashboard::box(
                        title = "Optimized Concept set expressions",
                        width = NULL,
                        status = "primary",
                        solidHeader = TRUE,
                        collapsible = TRUE,
                        collapsed = TRUE,
                        tags$table(width = "100%",
                                   tags$tr(
                                     tags$td(
                                       align = "right",
                                       
                                       tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('targetConceptSetsExpressionOptimizedTable')")
                                     )
                                   )),
                        reactable::reactableOutput(outputId = "targetConceptSetsExpressionOptimizedTable")
                      )
                      )
                  ),
                  shiny::conditionalPanel(
                    condition = "input.targetConceptSetsType == 'Resolved'",
                    tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('targetCohortDefinitionResolvedConceptTable')"),
                    reactable::reactableOutput(outputId = "targetCohortDefinitionResolvedConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.targetConceptSetsType == 'Excluded'",
                    tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('targetCohortDefinitionExcludedConceptTable')"),
                    reactable::reactableOutput(outputId = "targetCohortDefinitionExcludedConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.targetConceptSetsType == 'Recommended'",
                    tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('targetCohortDefinitionOrphanConceptTable')"),
                    reactable::reactableOutput(outputId = "targetCohortDefinitionOrphanConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.targetConceptSetsType == 'Mapped'",
                    tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('targetCohortDefinitionMappedConceptTable')"),
                    reactable::reactableOutput(outputId = "targetCohortDefinitionMappedConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.targetConceptSetsType == 'Concept Set Json'",
                    copyToClipboardButton(toCopyId = "targetConceptsetExpressionJson",
                                          style = "margin-top: 5px; margin-bottom: 5px;"),
                    shiny::verbatimTextOutput(outputId = "targetConceptsetExpressionJson"),
                    tags$head(
                      tags$style("#targetConceptsetExpressionJson { max-height:400px};")
                    )
                  ),
                  shiny::conditionalPanel(
                    condition = "input.targetConceptSetsType == 'Concept Set Sql'",
                    copyToClipboardButton(toCopyId = "targetConceptsetExpressionSql",
                                          style = "margin-top: 5px; margin-bottom: 5px;"),
                    shiny::verbatimTextOutput(outputId = "targetConceptsetExpressionSql"),
                    tags$head(
                      tags$style("#targetConceptsetExpressionSql { max-height:400px};")
                    )
                  )
                )
              )
            ),
            
            shiny::tabPanel(
              title = "Cohort JSON",
              value = "targetCohortDefinitionJsonTabPanel",
              copyToClipboardButton("targetCohortDefinitionJson", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput("targetCohortDefinitionJson"),
              tags$head(
                tags$style("#targetCohortDefinitionJson { max-height:400px};")
              )
            ),
            shiny::tabPanel(
              title = "Cohort SQL",
              value = "targetCohortDefinitionSqlTabPanel",
              copyToClipboardButton("targetCohortDefinitionSql", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput("targetCohortDefinitionSql"),
              tags$head(
                tags$style("#targetCohortDefinitionSql { max-height:400px};")
              )
            )
          )
        )
      )
    })
  
  ##Dynamic UI rendering for comparator side -----
  output$dynamicUIGenerationForCohortSelectedComparator <-
    shiny::renderUI(expr = {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = "Rendering the UI for target cohort details",
        value = 0
      )
      shiny::column(
        getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable(),
        shiny::conditionalPanel(
          condition = "output.isComparatorSelected == true &
                     output.isCohortDefinitionRowSelected == true",
          shiny::htmlOutput(outputId = "comparatorCohortSelectedInCohortDefinitionTable"),
          shiny::tabsetPanel(
            id = "comparatorCohortDefinitionTabSetPanel",
            type = "tab",
            shiny::tabPanel(
              title = "Cohort Count",
              value = "comparatorCohortDefinitionCohortCountTabPanel",
              tags$br(),
              tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('comparatorCohortDefinitionCohortCountsTable')"),
              reactable::reactableOutput(outputId = "comparatorCohortDefinitionCohortCountsTable")
            ),
            shiny::tabPanel(
              title = "Inclusion rules",
              value = "comparatorCohortDefinitionUnclusionRuleTabPanel",
              tags$br(),
              tags$table(width = "100%",
                         tags$tr(
                           tags$td(
                             shiny::radioButtons(
                               inputId = "comparatorCohortDefinitionInclusionRuleType",
                               label = "Filter by",
                               choices = c("Events"), #, "Persons"
                               selected = "Events",
                               inline = TRUE
                             )
                           ),
                           tags$td(
                             shiny::conditionalPanel(
                               condition = "input.comparatorCohortDefinitionInclusionRuleType == 'Events' &
                                         output.getComparatorSimplifiedInclusionRuleResultsHasData == true",
                               shiny::radioButtons(
                                 inputId = "comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters",
                                 label = "Filter by",
                                 choices = c("All", "Meet", "Gain", "Remain", "Totals"),
                                 selected = "All",
                                 inline = TRUE
                               )
                             )
                           ),
                           tags$td(
                             shiny::checkboxInput(
                               inputId = "comparatorCohortInclusionRulesAsPercent",
                               label = "Show As Percent",
                               value = FALSE
                             )
                           )
                         )),
              shiny::conditionalPanel(
                condition = "input.comparatorCohortDefinitionInclusionRuleType == 'Events'",
                tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('comparatorCohortDefinitionSimplifiedInclusionRuleTable')"),
                reactable::reactableOutput(outputId = "comparatorCohortDefinitionSimplifiedInclusionRuleTable")
              )
            ),
            shiny::tabPanel(
              title = "Details",
              value = "comparatorCohortDefinitionDetailsTextTabPanel",
              tags$br(),
              shinydashboard::box(
                title = "Readable definitions",
                width = NULL,
                status = NULL,
                collapsible = TRUE,
                collapsed = FALSE,
                solidHeader = FALSE,
                copyToClipboardButton(toCopyId = "comparatorCohortDefinitionText",
                                      style = "margin-top: 5px; margin-bottom: 5px;"),
                shiny::htmlOutput("comparatorCohortDefinitionText")
              ),
              shinydashboard::box(
                title = "Meta data",
                width = NULL,
                status = NULL,
                collapsible = TRUE,
                collapsed = TRUE,
                solidHeader = FALSE,
                tags$p("To do")
              )
            ),
            shiny::tabPanel(
              title = "Concept Sets",
              value = "comparatorCohortDefinitionConceptSetTabPanel",
              tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('comparatorCohortDefinitionConceptSets')"),
              reactable::reactableOutput(outputId = "comparatorCohortDefinitionConceptSets"),
              tags$br(),
              shiny::conditionalPanel(
                condition = "output.isComparatorCohortDefinitionConceptSetRowSelected == true",
                shinydashboard::box(
                  title = shiny::htmlOutput(outputId = "comparatorConceptSetExpressionName"),
                  solidHeader = FALSE,
                  width = NULL,
                  collapsible = TRUE,
                  collapsed = FALSE,
                  shiny::conditionalPanel(condition = "output.isComparatorCohortDefinitionConceptSetRowSelected == true",
                                          tags$table(width = "100%",
                                            tags$tr(tags$td(
                                              colspan = 2,
                                              shiny::radioButtons(
                                                inputId = "comparatorConceptSetsType",
                                                label = "",
                                                choices = c(
                                                  "Concept Set Expression",
                                                  "Resolved",
                                                  "Excluded",
                                                  "Recommended",
                                                  "Mapped",
                                                  "Concept Set Json",
                                                  "Concept Set Sql"
                                                ),
                                                selected = "Concept Set Expression",
                                                inline = TRUE
                                              )
                                            )),
                                            tags$tr(
                                              tags$td(align = "left",
                                                      shiny::conditionalPanel(
                                                        condition = "input.comparatorConceptSetsType != 'Concept Set Json' &
                                                                   input.comparatorConceptSetsType != 'Concept Set Expression' &
                                                                   input.comparatorConceptSetsType != 'Concept Set Sql'",
                                                        shiny::radioButtons(
                                                          inputId = "comparatorConceptIdCountSource",
                                                          label = "",
                                                          choices = c("Datasource Level", "Cohort Level"),
                                                          selected = "Datasource Level",
                                                          inline = TRUE
                                                        )
                                                      )
                                              ),
                                              tags$td(align = "right",
                                                      shiny::conditionalPanel(
                                                        condition = "input.targetConceptSetsType != 'Concept Set Json' &
                                                                   input.targetConceptSetsType != 'Concept Set Sql' &
                                                                   input.targetConceptSetsType != 'Concept Set Expression'",
                                                        shiny::radioButtons(
                                                          inputId = "comparatorCohortConceptSetColumnFilter",
                                                          label = "",
                                                          choices = c("Both", "Persons", "Records"),
                                                          selected = "Both",
                                                          inline = TRUE
                                                        )
                                                      )
                                              ),
                                              tags$td(
                                                
                                                shiny::conditionalPanel(
                                                  condition = "input.targetConceptSetsType != 'Concept Set Json' &
                                                                   input.targetConceptSetsType != 'Concept Set Sql' &
                                                                   input.targetConceptSetsType != 'Concept Set Expression'",
                                                  shiny::checkboxInput(
                                                    inputId = "showAsPercentageColumnComparator",
                                                    label = "Show As Percent"
                                                  )
                                                )
                                              )
                                            )
                                          )),
                  shiny::conditionalPanel(
                    condition = "output.isComparatorCohortDefinitionConceptSetRowSelected == true &
                                                      input.comparatorConceptSetsType == 'Concept Set Expression'",
                    tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('comparatorConceptSetsExpressionTable')"),
                    reactable::reactableOutput(outputId = "comparatorConceptSetsExpressionTable")
                    # tags$br(),
                    # shiny::conditionalPanel(
                    #   condition = "output.canComparatorConceptSetExpressionBeOptimized",
                    #   shinydashboard::box(
                    #     title = "Optimized Concept set expressions",
                    #     width = NULL,
                    #     solidHeader = FALSE,
                    #     collapsible = TRUE,
                    #     collapsed = TRUE,
                    #     tags$table(width = "100%",
                    #                tags$tr(
                    #                  tags$td(
                    #                    align = "right",
                    #                    shiny::downloadButton(
                    #                      "saveComparatorConceptSetsExpressionOptimizedTable",
                    #                      label = "",
                    #                      icon = shiny::icon("download"),
                    #                      style = "margin-top: 5px; margin-bottom: 5px;"
                    #                    )
                    #                  )
                    #                )),
                    #     DT::dataTableOutput(outputId = "comparatorConceptSetsExpressionOptimizedTable"),
                    #   )
                    # )
                  ),
                  shiny::conditionalPanel(
                    condition = "input.comparatorConceptSetsType == 'Resolved'",
                    tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('comparatorCohortDefinitionResolvedConceptTable')"),
                    reactable::reactableOutput(outputId = "comparatorCohortDefinitionResolvedConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.comparatorConceptSetsType == 'Excluded'",
                    tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('comparatorCohortDefinitionExcludedConceptTable')"),
                    reactable::reactableOutput(outputId = "comparatorCohortDefinitionExcludedConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.comparatorConceptSetsType == 'Recommended'",
                    tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('comparatorCohortDefinitionOrphanConceptTable')"),
                    reactable::reactableOutput(outputId = "comparatorCohortDefinitionOrphanConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.comparatorConceptSetsType == 'Mapped'",
                    tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('comparatorCohortDefinitionMappedConceptTable')"),
                    reactable::reactableOutput(outputId = "comparatorCohortDefinitionMappedConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.comparatorConceptSetsType == 'Concept Set Json'",
                    copyToClipboardButton(toCopyId = "comparatorCohortDefinitionConceptsetExpressionJson",
                                          style = "margin-top: 5px; margin-bottom: 5px;"),
                    shiny::verbatimTextOutput(outputId = "comparatorCohortDefinitionConceptsetExpressionJson"),
                    tags$head(
                      tags$style(
                        "#comparatorCohortDefinitionConceptsetExpressionJson { max-height:400px};"
                      )
                    )
                  ),
                  shiny::conditionalPanel(
                    condition = "input.comparatorConceptSetsType == 'Concept Set Sql'",
                    copyToClipboardButton(toCopyId = "comparatorCohortDefinitionConceptsetExpressionSql",
                                          style = "margin-top: 5px; margin-bottom: 5px;"),
                    shiny::verbatimTextOutput(outputId = "comparatorCohortDefinitionConceptsetExpressionSql"),
                    tags$head(
                      tags$style(
                        "#comparatorCohortDefinitionConceptsetExpressionSql { max-height:400px};"
                      )
                    )
                  )
                )
              )
            ),
            
            shiny::tabPanel(
              title = "Cohort JSON",
              value = "comparatorCohortDefinitionJsonTabPanel",
              copyToClipboardButton("comparatorCohortDefinitionJson", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput("comparatorCohortDefinitionJson"),
              tags$head(
                tags$style("#comparatorCohortDefinitionJson { max-height:400px};")
              )
            ),
            shiny::tabPanel(
              title = "Cohort SQL",
              value = "comparatorCohortDefinitionSqlTabPanel",
              copyToClipboardButton("comparatorCohortDefinitionSql", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput("comparatorCohortDefinitionSql"),
              tags$head(
                tags$style("#comparatorCohortDefinitionSql { max-height:400px};")
              )
            )
          )
        )
      )
    })
  
  ###cohortDefinitionTableData----
  cohortDefinitionTableData <- shiny::reactive(x = {
    data <-  getCohortSortedByCohortId() %>%
      dplyr::select(cohort = .data$shortName,
                    .data$cohortId,
                    .data$cohortName) %>%
      dplyr::mutate(cohortName = stringr::str_wrap(
        string = .data$cohortName,
        width = 80,
        exdent = 1
      )) %>%
      dplyr::mutate(
        cohortName = stringr::str_replace_all(
          string = .data$cohortName,
          pattern = stringr::fixed(pattern = "\n"),
          replacement = "<br/>"
        )
      )
    return(data)
  })
  ###output: cohortDefinitionTable----
  output$cohortDefinitionTable <- reactable::renderReactable(expr = {
    data <- cohortDefinitionTableData()
    
    if (!hasData(data)) {
      return(NULL)
    }
    
    keyColumns <- colnames(data)
    getSimpleReactable(data = data,
                       dataColumns = c(),
                       keyColumns = keyColumns)
  })
  
  
  ###output: isCohortDefinitionRowSelected----
  output$isCohortDefinitionRowSelected <- reactive({
    return(any(
      !is.null(consolidatedCohortIdTarget()),
      !is.null(consolidatedCohortIdComparator())
    ))
  })
  # send output to UI
  shiny::outputOptions(x = output,
                       name = "isCohortDefinitionRowSelected",
                       suspendWhenHidden = FALSE)
  
  ###getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable----
  #Used to set the half view or full view
  getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable <-
    shiny::reactive(x = {
      if (hasData(consolidatedCohortIdComparator())) {
        return(6)
      } else {
        return(12)
      }
    })
  
  ##Human readable text----
  ###getCirceRPackageVersionInformation----
  getCirceRPackageVersionInformation <- shiny::reactive(x = {
    packageVersion <- as.character(packageVersion('CirceR'))
    return(packageVersion)
  })
  
  ###getCirceRenderedExpressionDetailsTarget----
  getCirceRenderedExpressionDetailsTarget <- shiny::reactive(x = {
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0(
        "Rendering human readable cohort definition using CirceR ",
        getCirceRPackageVersionInformation(),
        " for target cohort id: ",
        consolidatedCohortIdTarget()
      ),
      value = 0
    )
    cohortExpression <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>% 
      dplyr::pull(.data$json) %>% 
      RJSONIO::fromJSON(digits = 23)
    cohortName <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>% 
      dplyr::mutate(cohortName = paste0(.data$cohortName, " (", .data$cohortId, ")")) %>%
      dplyr::select(.data$cohortName)
    
    if (!hasData(cohortExpression)) {
      return(NULL)
    }
    
    embedCohortDetailsText <- paste(
      "Rendered for cohort id:",
      consolidatedCohortIdTarget(),
      " using CirceR version: ",
      getCirceRPackageVersionInformation()
    )
    
    details <-
      getCirceRenderedExpression(cohortDefinition = cohortExpression,
                                 cohortName = cohortName,
                                 embedText = embedCohortDetailsText,
                                 includeConceptSets = TRUE)
    return(details)
  })
  
  ###getCirceRenderedExpressionDetailsComparator----
  getCirceRenderedExpressionDetailsComparator <-
    shiny::reactive(x = {
      if (!hasData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0(
          "Rendering human readable cohort definition using CirceR ",
          getCirceRPackageVersionInformation(),
          " for comparator cohort id: ",
          consolidatedCohortIdComparator()
        ),
        value = 0
      )
      cohortExpression <- cohort %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>% 
        dplyr::pull(.data$json) %>% 
        RJSONIO::fromJSON(digits = 23)
      cohortName <- cohort %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>% 
        dplyr::mutate(cohortName = paste0(.data$cohortName, " (", .data$cohortId, ")")) %>%
        dplyr::select(.data$cohortName)
      
      if (!hasData(cohortExpression)) {
        return(NULL)
      }
      
      embedCohortDetailsText <- paste(
        "Rendered for cohort id:",
        consolidatedCohortIdTarget(),
        " using CirceR version: ",
        getCirceRPackageVersionInformation()
      )
      
      details <-
        getCirceRenderedExpression(cohortDefinition = cohortExpression,
                                   cohortName = cohortName,
                                   embedText = embedCohortDetailsText,
                                   includeConceptSets = TRUE)
      return(details)
    })
  
  ###output: comparatorCohortDefinitionText----
  output$comparatorCohortDefinitionText <- shiny::renderUI(expr = {
    getCirceRenderedExpressionDetailsComparator()$cohortHtmlExpression %>%
      shiny::HTML()
  })
  
  ###output: targetCohortDefinitionText----
  output$targetCohortDefinitionText <- shiny::renderUI(expr = {
    getCirceRenderedExpressionDetailsTarget()$cohortHtmlExpression %>%
      shiny::HTML()
  })
  
  ###output: targetCohortSelectedInCohortDefinitionTable----
  #Show cohort names in UI
  output$targetCohortSelectedInCohortDefinitionTable <-
    shiny::renderUI(expr = {
      if (!hasData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      cohortName <- cohort %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
        dplyr::pull(.data$compoundName)
      
      if (!hasData(cohortName)) {
        return(NULL)
      }
      tags$table(height = '60',
                 style = "overflow : auto",
                 tags$tr(
                   tags$td(
                     tags$h4(style = "font-weight: bold","Target cohort: ")
                   ),
                   tags$td(
                     tags$h4(style = "font-weight: bold", cohortName)
                   )
                 )
      )
    })
  
  
  ###output: comparatorCohortSelectedInCohortDefinitionTable----
  output$comparatorCohortSelectedInCohortDefinitionTable <-
    shiny::renderUI(expr = {
      if (!hasData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      cohortName <- cohort %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
        dplyr::pull(.data$compoundName)
      
      if (!hasData(cohortName)) {
        return(NULL)
      }
      tags$table(height = '60',
                 style = "overflow : auto",
                 tags$tr(tags$td(
                   tags$h4(style = "font-weight: bold","Comparator cohort:")
                 ),
                 tags$td(
                   tags$h4(style = "font-weight: bold",cohortName))
                 ))
    })
  
  ##Cohort SQL----
  ###output: targetCohortDefinitionSql----
  output$targetCohortDefinitionSql <- shiny::renderText({
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    cohortDefinition <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::pull(.data$json) %>%
      RJSONIO::fromJSON(digits = 23)
    if (!hasData(cohortDefinition)) {
      return(NULL)
    }
    sql <-
      getOhdsiSqlFromCohortDefinitionExpression(cohortDefinitionExpression = cohortDefinition)
    
    sql <- paste0(
      "-- Rendered for cohort id:",
      consolidatedCohortIdTarget(),
      " using CirceR version: ",
      getCirceRPackageVersionInformation(),
      "\n\n",
      sql
    )
    return(sql)
  })
  
  ###output: comparatorCohortDefinitionSql----
  output$comparatorCohortDefinitionSql <- shiny::renderText({
    if (!hasData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    json <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
      dplyr::pull(.data$json) %>%
      RJSONIO::fromJSON(digits = 23)
    if (!hasData(cohortDefinition)) {
      return(NULL)
    }
    sql <-
      getOhdsiSqlFromCohortDefinitionExpression(cohortDefinitionExpression = cohortDefinition)
    sql <- paste0(
      "-- Rendered for cohort id:",
      consolidatedCohortIdComparator(),
      "using CirceR version: ",
      getCirceRPackageVersionInformation(), "\n\n",
      sql)
    return(sql)
  })
  
  ##Cohort count in cohort definition tab----
  ###getCountsForSelectedCohortsTarget----
  getCountsForSelectedCohortsTarget <- shiny::reactive(x = {
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    data <- cohortCount %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::select(.data$databaseId,
                    .data$cohortSubjects,
                    .data$cohortEntries) %>%
      dplyr::arrange(.data$databaseId)
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ###getCountsForSelectedCohortsComparator----
  getCountsForSelectedCohortsComparator <- shiny::reactive(x = {
    data <- cohortCount %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
      dplyr::select(.data$databaseId,
                    .data$cohortSubjects,
                    .data$cohortEntries) %>%
      dplyr::arrange(.data$databaseId)
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ###output: targetCohortDefinitionCohortCountTable----
  output$targetCohortDefinitionCohortCountTable <-
    reactable::renderReactable(expr = {
      data <- getCountsForSelectedCohortsTarget()
      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
        "There is no inclusion rule data for this cohort."
      ))
      
      keyColumns <- c("databaseId")
      dataColumns <- c("cohortSubjects", "cohortEntries")
      
      getSimpleReactable(data = data,
                         keyColumns = keyColumns,
                         dataColumns = dataColumns)
    })
  
  ##Concept set ----
  ###getConceptSetExpressionTarget----
  getConceptSetExpressionTarget <- shiny::reactive(x = {
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedConceptSetIdTarget())) {
      return(NULL)
    }
    conceptSetExpression <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdTarget())
    if (!hasData(conceptSetExpression)) {
      return(NULL)
    }
    conceptSetExpressionList <- conceptSetExpression %>%
      dplyr::pull(.data$conceptSetExpression) %>%
      RJSONIO::fromJSON(digits = 23)
    
    data <-
      getConceptSetDataFrameFromConceptSetExpression(conceptSetExpressionList)
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::rename(standard = .data$standardConcept) %>% 
      dplyr::select(
        .data$conceptId,
        .data$conceptName,
        .data$isExcluded,
        .data$includeDescendants,
        .data$includeMapped,
        .data$domainId,
        .data$standard,
        .data$conceptCode,
        .data$invalidReason
      )
    return(data)
  })
  
  ###getConceptSetExpressionComparator----
  getConceptSetExpressionComparator <- shiny::reactive(x = {
    if (all(
      !hasData(consolidatedCohortIdComparator()),!hasData(consolidatedConceptSetIdComparator())
    )) {
      return(NULL)
    }
    conceptSetExpression <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
      dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdComparator())
    if (!hasData(conceptSetExpression)) {
      return(NULL)
    }
    conceptSetExpressionList <- conceptSetExpression %>%
      dplyr::pull(.data$conceptSetExpression) %>%
      RJSONIO::fromJSON(digits = 23)
    data <-
      getConceptSetDataFrameFromConceptSetExpression(conceptSetExpressionList)
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::rename(standard = .data$standardConcept) %>% 
      dplyr::select(
        .data$conceptId,
        .data$conceptName,
        .data$isExcluded,
        .data$includeDescendants,
        .data$includeMapped,
        .data$domainId,
        .data$standard,
        .data$conceptCode,
        .data$invalidReason
      )
    return(data)
  })
  
  # ###getResolvedConceptsAllData----
  # getResolvedConceptsAllData <- shiny::reactive({
  #   data <- conceptResolved
  #   if (!hasData(data)) {
  #     return(NULL)
  #   }
  #   return(data)
  # })
  
  
  # ###getResolvedConceptsAllDataConceptIdDetails----
  # getResolvedConceptsAllDataConceptIdDetails <- shiny::reactive({ 
  # resolvedConcepts <- getResolvedConceptsAllData()
  # if (!hasData(resolvedConcepts)) {
  #   return(NULL)
  # }
  # progress <- shiny::Progress$new()
  # on.exit(progress$close())
  # progress$set(message = "Caching concept count for resolved concepts",
  #              value = 0)
  # conceptDetails <- getConcept(dataSource = dataSource,
  #                              conceptIds = resolvedConcepts$conceptId %>% unique())
  # if (!hasData(conceptDetails)) {
  #   return(NULL)
  # }
  # conceptDetails <- conceptDetails %>%
  #   dplyr::select(.data$conceptId,
  #                 .data$conceptName,
  #                 .data$vocabularyId,
  #                 .data$domainId,
  #                 .data$standardConcept)
  # return(conceptDetails)
  # })
  
  ###getResolvedConceptsTargetData----
  getResolvedConceptsTargetData <- shiny::reactive({
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedConceptSetIdTarget())) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Retrieving resolved concepts for target",
                 value = 0)
    
    data <- getResultsResolvedConcepts(dataSource = dataSource,
                                       databaseIds = consolidatedDatabaseIdTarget(), 
                                       cohortIds = consolidatedCohortIdTarget(), 
                                       conceptSetIds = consolidatedConceptSetIdTarget())
    if (!hasData(data)) {
      return(NULL)
    }
    
    conceptDetails <- getConcept(dataSource = dataSource, 
                                 conceptIds = c(data$conceptId %>% unique()))
    if (!hasData(conceptDetails)) {
      return(NULL)
    }
    
    conceptDetails <- conceptDetails %>%
      dplyr::select(.data$conceptId,
                    .data$conceptName,
                    .data$vocabularyId)
    
    data <- data %>%
      dplyr::left_join(conceptDetails,
                       by = "conceptId") %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId)
    return(data)
  })
  
  
  ###getResolvedConceptsTarget----
  getResolvedConceptsTarget <- shiny::reactive({
    data <- getResolvedConceptsTargetData()
    if (!hasData(data)) {
      return(NULL)
    }
    count <-
      getConceptCountForCohortAndDatabase(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        conceptIds = data$conceptId %>% unique(),
        cohortIds = consolidatedCohortIdTarget(),
        databaseCount = (input$targetConceptIdCountSource == "Datasource Level")
      )
    if (!hasData(count)) {
      return(data %>% 
               dplyr::mutate("records" = NA, 
                             "persons" = NA))
    }
    data <- data %>% 
      dplyr::left_join(count, 
                       by = c('databaseId', 'conceptId'))
    
    data <- data %>% 
      dplyr::arrange(dplyr::desc(abs(dplyr::across(c("records", "persons")))))
    return(data)
  })
  
  ###getMappedConceptsTargetData----
  getMappedConceptsTargetData <- shiny::reactive({
    data <- getResolvedConceptsTargetData()
    if (!hasData(data)) {
      return(NULL)
    }
    resolvedConceptIds <- data$conceptId %>% unique()
    
    mappedConcepts <- getConceptRelationship(
      dataSource = dataSource,
      conceptIds = resolvedConceptIds, 
      relationshipIds = "Mapped from"
    )
    if (!hasData(mappedConcepts)) {
      return(NULL)
    }
    mappedConcepts <- mappedConcepts %>% 
      dplyr::rename("conceptId" = .data$conceptId1)
    mappedconceptIds <- mappedConcepts$conceptId2 %>% unique()
    
    conceptIdDetails <- getConcept(dataSource = dataSource,
                                   conceptIds = c(mappedconceptIds) %>% unique())
    
    outputData <- data %>% 
      dplyr::select(.data$databaseId,
                    .data$conceptId,
                    .data$conceptName) %>% 
      dplyr::distinct() %>% 
      dplyr::mutate(resolvedConcept = paste0(.data$conceptId, " (", .data$conceptName, ")")) %>% 
      dplyr::select(-.data$conceptName) %>% 
      dplyr::relocate(.data$resolvedConcept) %>% 
      dplyr::inner_join(mappedConcepts, by = "conceptId") %>% 
      dplyr::filter(.data$conceptId != .data$conceptId2) %>% 
      dplyr::select(-.data$conceptId) %>% 
      dplyr::rename(conceptId = .data$conceptId2) %>% 
      dplyr::select(.data$databaseId, .data$resolvedConcept, .data$conceptId) %>% 
      dplyr::inner_join(conceptIdDetails, by = "conceptId")
    
    return(outputData)
  })
  
  
  ###getMappedConceptsTarget----
  getMappedConceptsTarget <- shiny::reactive({
    data <- getMappedConceptsTargetData()
    if (!hasData(data)) {
      return(NULL)
    }
    count <-
      getConceptCountForCohortAndDatabase(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        conceptIds = data$conceptId %>% unique(),
        cohortIds = consolidatedCohortIdTarget(),
        databaseCount = (input$targetConceptIdCountSource == "Datasource Level")
      )
    if (!hasData(count)) {
      return(data %>% 
               dplyr::mutate("records" = NA, "persons" = NA))
    }
    data <- data %>% 
      dplyr::left_join(count, 
                       by = c('databaseId', 'conceptId')) %>% 
      dplyr::arrange(dplyr::desc(abs(dplyr::across(c("records", "persons")))))
    return(data)
  })
  
  ###getResolvedConceptsComparatorData----
  getResolvedConceptsComparatorData <- shiny::reactive({
    if (!hasData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedConceptSetIdComparator())) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Retrieving resolved concepts for comparator",
                 value = 0)
    
    data <- getResultsResolvedConcepts(dataSource = dataSource,
                                       databaseIds = consolidatedDatabaseIdTarget(), 
                                       cohortIds = consolidatedCohortIdComparator(), 
                                       conceptSetIds = consolidatedConceptSetIdComparator())
    if (!hasData(data)) {
      return(NULL)
    }
    
    conceptDetails <- getConcept(dataSource = dataSource, 
                                 conceptIds = c(data$conceptId %>% unique()))
    if (!hasData(conceptDetails)) {
      return(NULL)
    }
    
    conceptDetails <- conceptDetails %>%
      dplyr::select(.data$conceptId,
                    .data$conceptName,
                    .data$vocabularyId)
    
    data <- data %>%
      dplyr::left_join(conceptDetails,
                       by = "conceptId") %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId)
    return(data)
  })
  
  
  ###getResolvedConceptsComparator----
  getResolvedConceptsComparator <- shiny::reactive({
    data <- getResolvedConceptsComparatorData()
    if (!hasData(data)) {
      return(NULL)
    }
    count <-
      getConceptCountForCohortAndDatabase(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        conceptIds = data$conceptId %>% unique(),
        cohortIds = consolidatedCohortIdComparator(),
        databaseCount = (input$targetConceptIdCountSource == "Datasource Level")
      )
    if (!hasData(count)) {
      return(data %>% 
               dplyr::mutate("records" = NA, 
                             "persons" = NA))
    }
    data <- data %>% 
      dplyr::left_join(count, 
                       by = c('databaseId', 'conceptId'))
    return(data)
  })
  
  
  #getExcludedConceptsTargetData
  getExcludedConceptsTargetData <- shiny::reactive(x = {
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedConceptSetIdTarget())) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Retrieving Exluded concepts for target",
                 value = 0)
    data <- getResultsExcludedConcepts(
      dataSource = dataSource,
      cohortId = consolidatedCohortIdTarget(),
      databaseId = consolidatedDatabaseIdTarget(),
      conceptSetId = consolidatedConceptSetIdTarget()
    )
    if (!hasData(data)) {
      return(NULL)
    }
    conceptDetails <- getConcept(dataSource = dataSource,
                                 conceptIds = data$conceptId %>% unique())
    if (is.null(conceptDetails)) {
      return(NULL)
    }
    conceptDetails <- conceptDetails %>%
      dplyr::select(.data$conceptId,
                    .data$conceptName,
                    .data$vocabularyId)
    data <- data %>%
      dplyr::left_join(conceptDetails,
                       by = "conceptId") %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId)
    return(data)
  })
  
  ###getExcludedConceptsTarget----
  getExcludedConceptsTarget <- shiny::reactive({
    data <- getExcludedConceptsTargetData()
    if (!hasData(data)) {
      return(NULL)
    }
    count <-
      getConceptCountForCohortAndDatabase(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        conceptIds = data$conceptId %>% unique(),
        cohortIds = consolidatedCohortIdTarget(),
        databaseCount = (input$targetConceptIdCountSource == "Datasource Level")
      )
    if (!hasData(count)) {
      return(data %>% 
               dplyr::mutate("records" = NA, "persons" = NA))
    }
    data <- data %>% 
      dplyr::left_join(count, 
                       by = c('databaseId', 'conceptId')) %>% 
      dplyr::arrange(dplyr::desc(abs(dplyr::across(c("records", "persons")))))
    
    return(data)
   })
  
  #getExcludedConceptsComparatorData
  getExcludedConceptsComparatorData <- shiny::reactive({
    if (!hasData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedConceptSetIdComparator())) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Retrieving Exluded concepts for comparator",
                 value = 0)
    data <- getResultsExcludedConcepts(
      dataSource = dataSource,
      cohortId = consolidatedCohortIdComparator(),
      databaseId = consolidatedDatabaseIdTarget(),
      conceptSetId = consolidatedConceptSetIdComparator()
    )
    if (!hasData(data)) {
      return(NULL)
    }
    conceptDetails <- getConcept(dataSource = dataSource,
                                 conceptIds = data$conceptId %>% unique())
    if (is.null(conceptDetails)) {
      return(NULL)
    }
    conceptDetails <- conceptDetails %>%
      dplyr::select(.data$conceptId,
                    .data$conceptName,
                    .data$vocabularyId)
    
    data <- data %>%
      dplyr::left_join(conceptDetails,
                       by = "conceptId") %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId)
    return(data)
  })
  
  ###getExcludedConceptsComparator----
  getExcludedConceptsComparator <- shiny::reactive({
    data <- getExcludedConceptsComparatorData()
    if (!hasData(data)) {
      return(NULL)
    }
    count <-
      getConceptCountForCohortAndDatabase(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        conceptIds = data$conceptId %>% unique(),
        cohortIds = consolidatedCohortIdComparator(),
        databaseCount = (input$comparatorConceptIdCountSource == "Datasource Level")
      )
    if (!hasData(count)) {
      return(data %>% 
               dplyr::mutate("records" = NA, "persons" = NA))
    }
    data <- data %>% 
      dplyr::left_join(count, 
                       by = c('databaseId', 'conceptId'))
    return(data)
  })
  
  ###getMappedConceptsComparatorData----
  getMappedConceptsComparatorData <- shiny::reactive({
    data <- getResolvedConceptsComparatorData()
    if (!hasData(data)) {
      return(NULL)
    }
    resolvedConceptIds <- data$conceptId %>% unique()
    
    mappedConcepts <- getConceptRelationship(
      dataSource = dataSource,
      conceptIds = resolvedConceptIds, 
      relationshipIds = "Mapped from"
    )
    if (!hasData(mappedConcepts)) {
      return(NULL)
    }
    mappedConcepts <- mappedConcepts %>% 
      dplyr::rename("conceptId" = .data$conceptId1)
    mappedconceptIds <- mappedConcepts$conceptId2 %>% unique()
    
    conceptIdDetails <- getConcept(dataSource = dataSource,
                                   conceptIds = c(resolvedConceptIds, mappedconceptIds) %>% unique())
    
    outputData <- data %>% 
      dplyr::select(.data$databaseId,
                    .data$conceptId,
                    .data$conceptName) %>% 
      dplyr::distinct() %>% 
      dplyr::mutate(resolvedConcept = paste0(.data$conceptId, " (", .data$conceptName, ")")) %>% 
      dplyr::select(-.data$conceptName) %>% 
      dplyr::relocate(.data$resolvedConcept) %>% 
      dplyr::inner_join(mappedConcepts, by = "conceptId") %>% 
      dplyr::filter(.data$conceptId != .data$conceptId2) %>% 
      dplyr::select(-.data$conceptId) %>% 
      dplyr::rename(conceptId = .data$conceptId2) %>% 
      dplyr::select(.data$databaseId, .data$resolvedConcept, .data$conceptId) %>% 
      dplyr::inner_join(conceptIdDetails, by = "conceptId")
    
    return(outputData)
  })
  
  ###getMappedConceptsComparator----
  getMappedConceptsComparator <- shiny::reactive({
    data <- getMappedConceptsComparatorData()
    if (!hasData(data)) {
      return(NULL)
    }
    count <-
      getConceptCountForCohortAndDatabase(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        conceptIds = data$conceptId %>% unique(),
        cohortIds = consolidatedCohortIdComparator(),
        databaseCount = (input$targetConceptIdCountSource == "Datasource Level")
      )
    if (!hasData(count)) {
      return(data %>% 
               dplyr::mutate("records" = NA, "persons" = NA))
    }
    data <- data %>% 
      dplyr::left_join(count, 
                       by = c('databaseId', 'conceptId')) %>% 
      dplyr::arrange(dplyr::desc(abs(dplyr::across(c("persons","records")))))
    return(data)
  })
  
  #getOrphanConceptsTargetData
  getOrphanConceptsTargetData <- shiny::reactive(x = {
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedConceptSetIdTarget())) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Retrieving Recommended concepts for target",
                 value = 0)
    data <- getResultsOrphanConcept(
      dataSource = dataSource,
      cohortId = consolidatedCohortIdTarget(),
      databaseId = consolidatedDatabaseIdTarget(),
      conceptSetId = consolidatedConceptSetIdTarget()
    )
    if (!hasData(data)) {
      return(NULL)
    }
    conceptDetails <- getConcept(dataSource = dataSource,
                                 conceptIds = data$conceptId %>% unique())
    if (is.null(conceptDetails)) {
      return(NULL)
    }
    conceptDetails <- conceptDetails %>%
      dplyr::select(.data$conceptId,
                    .data$conceptName,
                    .data$vocabularyId)
    data <- data %>%
      dplyr::left_join(conceptDetails,
                       by = "conceptId") %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId)
    return(data)
  })
  
  ###getOrphanConceptsTarget----
  getOrphanConceptsTarget <- shiny::reactive({
    data <- getOrphanConceptsTargetData()
    if (!hasData(data)) {
      return(NULL)
    }
    count <-
      getConceptCountForCohortAndDatabase(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        conceptIds = data$conceptId %>% unique(),
        cohortIds = consolidatedCohortIdTarget(),
        databaseCount = (input$targetConceptIdCountSource == "Datasource Level")
      )
    if (!hasData(count)) {
      return(data %>% 
               dplyr::mutate("records" = NA, "persons" = NA))
    }
    data <- data %>% 
      dplyr::left_join(count, 
                       by = c('databaseId', 'conceptId')) %>% 
      dplyr::arrange(dplyr::desc(abs(dplyr::across(c("persons","records")))))
    return(data)
  })
  
  ##getOrphanConceptsComparatorData
  getOrphanConceptsComparatorData <- shiny::reactive(x = {
    if (!hasData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedConceptSetIdComparator())) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Retrieving Recommended concepts for comparator",
                 value = 0)
    data <- getResultsOrphanConcept(
      dataSource = dataSource,
      cohortId = consolidatedCohortIdComparator(),
      databaseId = consolidatedDatabaseIdTarget(),
      conceptSetId = consolidatedConceptSetIdComparator()
    )
    if (!hasData(data)) {
      return(NULL)
    }
    conceptDetails <- getConcept(dataSource = dataSource,
                                 conceptIds = data$conceptId %>% unique())
    if (is.null(conceptDetails)) {
      return(NULL)
    }
    conceptDetails <- conceptDetails %>%
      dplyr::select(.data$conceptId,
                    .data$conceptName,
                    .data$vocabularyId)
    
    data <- data %>%
      dplyr::left_join(conceptDetails,
                       by = "conceptId") %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId)
    return(data)
  })
  
  ###getOrphanConceptsComparator----
  getOrphanConceptsComparator <- shiny::reactive({
    data <- getOrphanConceptsComparatorData()
    if (!hasData(data)) {
      return(NULL)
    }
    count <-
      getConceptCountForCohortAndDatabase(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        conceptIds = data$conceptId %>% unique(),
        cohortIds = consolidatedCohortIdComparator(),
        databaseCount = (input$comparatorConceptIdCountSource == "Datasource Level")
      )
    if (!hasData(count)) {
      return(data %>% 
               dplyr::mutate("records" = NA, "persons" = NA))
    }
    data <- data %>% 
      dplyr::left_join(count, 
                       by = c('databaseId', 'conceptId')) %>% 
      dplyr::arrange(dplyr::desc(abs(dplyr::across(c("persons","records")))))
    return(data)
  })
  
  ###getNameAndSynonymForActiveSelectedConceptId----
  getNameAndSynonymForActiveSelectedConceptId <- shiny::reactive(x = {
    if (!hasData(activeSelected()$conceptId)) {
      return(NULL)
    }
    if (length(activeSelected()$conceptId) != 1) {#currently expecting to be vector of 1 (single select)
      warning("Only one concept id may be selected to get concept synonym")
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Retrieving synonyms for concept id: ", activeSelected()$conceptId),
                 value = 0)
    data <- list()
    data$concept <- getConcept(dataSource = dataSource,
                               conceptId = activeSelected()$conceptId)
    data$conceptSynonym <- getConceptSynonym(dataSource = dataSource,
                                              conceptId = activeSelected()$conceptId)
    return(data)
  })
  
  ###getConceptSetSynonymsHtmlTextString-----
  getConceptSetSynonymsHtmlTextString <- shiny::reactive(x = {
    data <- getNameAndSynonymForActiveSelectedConceptId()
    if (!hasData(data)) {
      return(NULL)
    }
    tags$table(
      tags$tr(
        tags$td(
          tags$h4(paste0(
            data$concept %>% 
              dplyr::filter(.data$conceptId == activeSelected()$conceptId) %>% 
              dplyr::pull(.data$conceptName),
            " (",
            activeSelected()$conceptId,
            ")"
          ))
        )
      ),
      tags$tr(
        tags$td(
          tags$h6(data$conceptSynonym$conceptSynonymName %>% 
                    unique() %>% 
                    sort() %>% 
                    paste0(collapse = ", ") %>% 
                    stringr::str_trunc(1000, "right"))
        )
      )
    )
  })
  
  ###getSimplifiedInclusionRuleResultsTarget----
  getSimplifiedInclusionRuleResultsTarget <- shiny::reactive(x = {
    if (any(
      !hasData(consolidatedCohortIdTarget()),
      !hasData(consolidatedDatabaseIdTarget())
    )) {
      return(NULL)
    }
    data <-
      getResultsInclusionRuleStatistics(
        dataSource = dataSource,
        cohortId = consolidatedCohortIdTarget(),
        databaseId = consolidatedDatabaseIdTarget()
      )
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ###getSimplifiedInclusionRuleResultsComparator----
  getSimplifiedInclusionRuleResultsComparator <-
    shiny::reactive(x = {
      if (any(
        !hasData(consolidatedCohortIdComparator()),
        !hasData(consolidatedDatabaseIdTarget())
      )) {
        return(NULL)
      }
      data <-
        getResultsInclusionRuleStatistics(
          dataSource = dataSource,
          cohortId = consolidatedCohortIdComparator(),
          databaseId = consolidatedDatabaseIdTarget()
        )
      if (!hasData(data)) {
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
  
  #output: isDatabaseIdFoundForSelectedTargetCohortCount----
  output$isDatabaseIdFoundForSelectedTargetCohortCount <-
    shiny::reactive(x = {
      return(!is.null(getSimplifiedInclusionRuleResultsTarget()))
    })
  shiny::outputOptions(x = output,
                       name = "isDatabaseIdFoundForSelectedTargetCohortCount",
                       suspendWhenHidden = FALSE)
  
  #!!!!!! inclusion rule needs simple and detailed tabs. detailed will replicate Atlas UI
  #output: targetCohortDefinitionSimplifiedInclusionRuleTable----
  output$targetCohortDefinitionSimplifiedInclusionRuleTable <-
    reactable::renderReactable(expr = {
      if (any(is.null(consolidatedCohortIdTarget()))) {
        return(NULL)
      }
      
      data <- getSimplifiedInclusionRuleResultsTarget()
      validate(need((nrow(data) > 0),
                    "There is no inclusion rule data for this cohort."))
      keyColumnFields <- c("ruleSequenceId", "ruleName")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("totalSubjects",
          "remainSubjects",
          "meetSubjects",
          "gainSubjects")
      if (input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters != "All") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower(
              input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters
            )
          )]
      }
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = "Cohort Level",
          fields = input$targetCohortDefinitionInclusionRuleType
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = 1,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent =  input$targetCohortInclusionRulesAsPercent, 
        sort = FALSE
      )
    })
  
  ##output: getSimplifiedInclusionRuleResultsTargetHasData----
  output$getSimplifiedInclusionRuleResultsTargetHasData <-
    shiny::reactive(x = {
      return(nrow(getSimplifiedInclusionRuleResultsTarget()) > 0)
    })
  
  shiny::outputOptions(x = output,
                       name = "getSimplifiedInclusionRuleResultsTargetHasData",
                       suspendWhenHidden = FALSE)
  
  #output: targetCohortDefinitionJson----
  output$targetCohortDefinitionJson <- shiny::renderText({
    json <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::pull(.data$json)
    if (!hasData(json)) {
      return(NULL)
    }
    json <- json %>%
      RJSONIO::fromJSON(digits = 23) %>%
      RJSONIO::toJSON(digits = 23, pretty = TRUE)
    return(json)
  })
  
  #output: isTargetSelected----
  output$isTargetSelected <- shiny::reactive({
    return(!is.null(consolidatedCohortIdTarget()))
  })
  shiny::outputOptions(x = output,
                       name = "isTargetSelected",
                       suspendWhenHidden = FALSE)
  
  #output: isComparatorSelected----
  output$isComparatorSelected <- shiny::reactive({
    return(!is.null(consolidatedCohortIdComparator()))
  })
  shiny::outputOptions(x = output,
                       name = "isComparatorSelected",
                       suspendWhenHidden = FALSE)
  
  ###getConceptSetComparisonTableData----
  getConceptSetComparisonTableData <- shiny::reactive({
    if (!hasData(input$targetConceptSetsType)) {
      return(NULL)
    }
    if (!hasData(input$comparatorConceptSetsType)) {
      return(NULL)
    }
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    #Setting Initial values
    target <- NULL
    comparator <- NULL
    if (input$targetConceptSetsType == "Resolved" &
        input$comparatorConceptSetsType == "Resolved") {
      target <- getResolvedConceptsTarget()
      comparator <- getResolvedConceptsComparator()
    } else if (input$targetConceptSetsType == "Excluded" &
               input$comparatorConceptSetsType == "Excluded") {
      target <- getExcludedConceptsTarget()
      comparator <- getExcludedConceptsComparator()
    } else if (input$targetConceptSetsType == "Recommended" &
                input$comparatorConceptSetsType == "Recommended") {
      target <- getOrphanConceptsTarget()
      comparator <- getOrphanConceptsComparator()
    } else if (input$targetConceptSetsType == "Mapped" &
               input$comparatorConceptSetsType == "Mapped") {
      target <- getMappedConceptsTarget()
      comparator <- getMappedConceptsComparator()
    }
    if (any(!hasData(target),
            !hasData(comparator))) {
      return(NULL)
    }
    
    conceptIdSortOrder <- dplyr::bind_rows(target, comparator) %>% 
      dplyr::select(.data$conceptId, .data$persons) %>% 
      dplyr::filter(!is.na(.data$persons)) %>% 
      dplyr::arrange(dplyr::desc(.data$persons)) %>% 
      dplyr::select(.data$conceptId) %>% 
      dplyr::distinct() %>% 
      dplyr::mutate(rn = dplyr::row_number())
    
    targetCohortShortName <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::mutate(type = "target") %>% 
      dplyr::select(.data$type, .data$shortName)
    comparatorCohortShortName <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
      dplyr::mutate(type = "comparator") %>% 
      dplyr::select(.data$type, .data$shortName)
    cohortShortName <- dplyr::bind_rows(targetCohortShortName, comparatorCohortShortName) %>% 
      dplyr::distinct()
    
    combinedResult <-
      target %>%
      dplyr::union(comparator) %>%
      dplyr::select(.data$conceptId, .data$conceptName, .data$databaseId) %>%
      dplyr::arrange(.data$conceptId, .data$conceptName, .data$databaseId) %>%
      dplyr::distinct() %>%
      dplyr::left_join(y = target %>% 
                         dplyr::select(.data$conceptId, .data$conceptName, .data$databaseId) %>% 
                         dplyr::mutate(target = TRUE), 
                       by = c("conceptId", "conceptName", "databaseId")) %>%
      dplyr::left_join(y = comparator %>% 
                         dplyr::select(.data$conceptId, .data$conceptName, .data$databaseId) %>% 
                         dplyr::mutate(comparator = TRUE), 
                       by = c("conceptId", "conceptName", "databaseId")) %>% 
      tidyr::replace_na(replace = list(target = FALSE, comparator = FALSE))
   
    if (input$conceptSetComparisonChoices == "Target Only") {
      combinedResult <- combinedResult %>% 
        dplyr::filter(.data$comparator == FALSE) %>% 
        dplyr::filter(.data$target == TRUE)
    } else if (input$conceptSetComparisonChoices == "Comparator Only") {
      combinedResult <- combinedResult %>% 
        dplyr::filter(.data$target == FALSE) %>% 
        dplyr::filter(.data$comparator == TRUE)
    } else if (input$conceptSetComparisonChoices == "Both") {
      combinedResult <- combinedResult %>% 
        dplyr::filter(.data$target == TRUE) %>% 
        dplyr::filter(.data$comparator == TRUE)
    } else if (input$conceptSetComparisonChoices == "Either") {
      combinedResult <- combinedResult
    }
    
    return(combinedResult)
  })
  
  output$conceptSetComparisonTable <- reactable::renderReactable(expr = {
    data <- getConceptSetComparisonTableData()
    if (!hasData(data)) {
      return(NULL)
    }
    
    data <- data %>%
      dplyr::mutate(target = dplyr::case_when(.data$target == TRUE ~ as.character(icon("check")),
                                              .data$target == FALSE ~ as.character(''))) %>% 
      dplyr::mutate(comparator = dplyr::case_when(.data$comparator == TRUE ~ as.character(icon("check")),
                                                  .data$comparator == FALSE ~ as.character('')))
    keyColumnFields <- c("conceptId", "conceptName")
    #depending on user selection - what data Column Fields Will Be Presented?
    dataColumnFields <-
      c("target",
        "comparator")
    
    countsForHeader <-
      getCountsForHeaderForUseInDataTable(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds = consolidatedCohortIdTarget(),
        source = "Cohort Level",
        fields = "Persons Only"
      )
    if (!hasData(countsForHeader)) {
      return(NULL)
    }
    
    getReactTableWithColumnsGroupedByDatabaseId(
      data = data,
      cohort = cohort, 
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = 0,
      dataColumns = dataColumnFields,
      maxCount = NULL,
      showResultsAsPercent = FALSE, 
      sort = FALSE,
      valueFill = ''
    )
  })
  
  #activeSelected----
  activeSelected <- reactiveVal(list())
  observe({
    # if (any(!is.null(consolidatedCohortIdTarget()),
    #         !is.null(consolidatedConceptSetIdTarget()),
    #         !is.null(consolidatedDatabaseIdTarget()),
    #         !is.null(consolidatedConceptIdTarget()))) {
    tempList <- list()
    tempList$cohortId <- consolidatedCohortIdTarget()
    tempList$conceptSetId <-
      consolidatedConceptSetIdTarget()
    # tempList$databaseId <-
    #   consolidatedDatabaseIdTarget()
    tempList$conceptId <- consolidatedConceptIdTarget()
    activeSelected(tempList)
    })
    
  observe({
    tempList <- list()
    # if (any(!is.null(consolidatedCohortIdComparator()),
    #         !is.null(consolidatedConceptSetIdComparator()),
    #         !is.null(consolidatedConceptIdComparator()))) {
      #there is no databaseId for comparator.
      tempList$cohortId <-
        consolidatedCohortIdComparator()
      tempList$conceptSetId <-
        consolidatedConceptSetIdComparator()
      tempList$conceptId <-
        consolidatedConceptIdComparator()
    activeSelected(tempList)
  })
  
  ##getMetadataForConceptId----
  getMetadataForConceptId <- shiny::reactive(x = {
    if (!hasData(activeSelected()$conceptId)) {
      #currently expecting to be vector of 1 (single select)
      return(NULL)
    }
    if (length(activeSelected()$conceptId) != 1) {
      stop("Only single select is supported for conceptId")
    }
    if (!hasData(activeSelected()$cohortId)) {
      #currently expecting to be vector of 1 (single select)
      return(NULL)
    }
    if (length(activeSelected()$cohortId) != 1) {
      stop("Only single select is supported for cohortId")
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      #currently expecting to be vector of 1 or more (multiselect)
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0(
        "Getting metadata for concept id:",
        activeSelected()$conceptId,
        " in cohort: ",
        activeSelected()$cohortId
      ),
      value = 0
    )
    data <-
      getConceptMetadata(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds = activeSelected()$cohortId,
        conceptIds = activeSelected()$conceptId,
        getDatabaseMetadata = FALSE,
        getConceptRelationship = TRUE,
        getConceptAncestor = TRUE,
        getConceptSynonym = TRUE,
        getConceptCount = TRUE,
        getConceptCooccurrence = FALSE,
        getIndexEventCount = FALSE,
        getConceptMappingCount = FALSE,
        getFixedTimeSeries = TRUE,
        getRelativeTimeSeries = FALSE
      )
    if (!hasData(data)) {
      return(NULL)
    }
    if (!hasData(data$databaseConceptCount)) {
      return(NULL)
    }
    if (!is.null(data$conceptCooccurrence)) {
      data$conceptCooccurrence <-
        data$conceptCooccurrence %>%
        dplyr::filter(.data$referenceConceptId == activeSelected()$conceptId) %>%
        dplyr::filter(.data$cohortId == activeSelected()$cohortId) %>%
        dplyr::select(-.data$referenceConceptId,-.data$cohortId) %>%
        dplyr::arrange(.data$databaseId, dplyr::desc(.data$subjectCount))
    }
    if (!is.null(data$conceptRelationshipTable)) {
      data$conceptRelationshipTable <-
        data$conceptRelationshipTable %>%
        dplyr::filter(.data$referenceConceptId == activeSelected()$conceptId) %>%
        dplyr::select(-.data$referenceConceptId)
    }
    if (!is.null(data$conceptMapping)) {
      data$mappedNonStandard <-
        data$concept %>%
        dplyr::filter(is.na(.data$standardConcept) |
                        !.data$standardConcept == 'S') %>%
        dplyr::select(.data$conceptId,
                      .data$conceptName,
                      .data$vocabularyId) %>%
        dplyr::inner_join(
          data$conceptMapping %>%
            dplyr::filter(.data$conceptId == activeSelected()$conceptId) %>%
            dplyr::select(-.data$conceptId) %>%
            dplyr::rename("conceptId" = .data$sourceConceptId) %>%
            dplyr::select(
              .data$databaseId,
              .data$domainTable,
              .data$conceptId,
              .data$conceptCount,
              .data$subjectCount
            ) %>%
            dplyr::distinct() %>%
            dplyr::arrange(dplyr::desc(.data$subjectCount)),
          by = "conceptId"
        ) %>%
        dplyr::distinct()
    }
    return(data)
  })
  
  observeEvent(eventExpr = input$targetConceptSetsType,handlerExpr = {
    consolidateCohortDefinitionActiveSideTarget(c())
  })
  observeEvent(eventExpr = input$comparatorConceptSetsType,handlerExpr = {
    consolidateCohortDefinitionActiveSideComparator(c())
  })
  
  ##output: isConceptIdFromTargetOrComparatorConceptTableSelected----
  output$isConceptIdFromTargetOrComparatorConceptTableSelected <-
    shiny::reactive(x = {
      return(
        any(
          consolidateCohortDefinitionActiveSideTarget(),
          consolidateCohortDefinitionActiveSideComparator()
        )
      )
    })
  shiny::outputOptions(x = output,
                       name = "isConceptIdFromTargetOrComparatorConceptTableSelected",
                       suspendWhenHidden = FALSE)
  
  #output: targetConceptSetExpressionName----
  output$targetConceptSetExpressionName <-
    shiny::renderUI(expr = {
      if (!hasData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (!hasData(consolidatedConceptSetIdTarget())) {
        return(NULL)
      }
      
      conceptSetName <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
        dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdTarget()) %>%
        dplyr::pull(.data$conceptSetName)
      
      tags$table(
        tags$tr(
          tags$td(
            tags$h4(paste0("Concept set name:", conceptSetName))
          )
        )
      )
    })
  
  ##getConceptSetsInCohortDataTarget----
  getConceptSetsInCohortDataTarget <- reactive({
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!hasData(conceptSets)) {
      return(NULL)
    }
    data <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::select(.data$conceptSetId, .data$conceptSetName) %>%
      dplyr::arrange(.data$conceptSetId)
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  #output: targetCohortDefinitionConceptSetsTable----
  output$targetCohortDefinitionConceptSetsTable <-
    reactable::renderReactable(expr = {
      data <- getConceptSetsInCohortDataTarget()
      validate(need(all(!is.null(data),
                        nrow(data) > 0),
        "Concept set details not available for this cohort"
      ))
      keyColumns <- colnames(data)
      getSimpleReactable(data = data,
                         keyColumns = keyColumns,
                         dataColumns = c(),
                         selection = 'single',
                         defaultSelected = 1)
    })
  
  getConceptSetsInCohortDataComparator <- reactive({
    if (!hasData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    data <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
      dplyr::select(.data$conceptSetId, .data$conceptSetName) %>%
      dplyr::arrange(.data$conceptSetId)
    return(data)
  })
  
  #output: comparatorCohortDefinitionConceptSets----
  output$comparatorCohortDefinitionConceptSets <-
    reactable::renderReactable(expr = {
      data <- getConceptSetsInCohortDataComparator()
      validate(need(all(!is.null(data),
                        nrow(data) > 0),
                    "Concept set details not available for this cohort"
      ))
      if (!hasData(data)) {
        return(NULL)
      }
      keyColumns <- colnames(data)
      getSimpleReactable(data = data,
                         keyColumns = keyColumns,
                         dataColumns = c(),
                         selection = 'single',
                         defaultSelected = 1)
     
    })
  
  #output: conceptsetExpressionTableTarget----
  output$conceptsetExpressionTableTarget <-
    DT::renderDataTable(expr = {
      if (!hasData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (!hasData(consolidatedConceptSetIdTarget())) {
        return(NULL)
      }
      
      data <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
        dplyr::filter(.data$conceptSetid %in% consolidatedConceptSetIdTarget()) %>%
        dplyr::select(.data$conceptSetExpression)
      if (!hasData(data)) {
        return(NULL)
      }
      data <-
        getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression = data)
      
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
  
  #output: isTargetCohortDefinitionConceptSetsTableRowSelected----
  output$isTargetCohortDefinitionConceptSetsTableRowSelected <-
    shiny::reactive(x = {
      data <- reactable::getReactableState("targetCohortDefinitionConceptSetsTable", "selected")
      return(hasData(data))
    })
  shiny::outputOptions(x = output,
                       name = "isTargetCohortDefinitionConceptSetsTableRowSelected",
                       suspendWhenHidden = FALSE)
  
  showOptimizedConceptSetExpressionTable <- reactiveVal(FALSE)
  shiny::observeEvent(eventExpr = input$optimizeConceptSetButton,
                      handlerExpr = {
                        showOptimizedConceptSetExpressionTable(TRUE)
                      })
  
  #reactive: getOptimizedTargetConceptSetsExpressionTable----
  getOptimizedTargetConceptSetsExpressionTable <-
    shiny::reactive(x = {
      if (!input$tabs == "cohortDefinition") {
        return(NULL)
      }
      if (!hasData(consolidatedDatabaseIdTarget())) {
        return(NULL)
      }
      if (!hasData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (!hasData(consolidatedConceptSetIdTarget())) {
        return(NULL)
      }
      result <- getOptimizedConceptSet(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds = consolidatedCohortIdTarget(),
        conceptSetIds = consolidatedConceptSetIdTarget()
      )
      return(result)
    })
  
  output$canTargetConceptSetExpressionBeOptimized <-
    shiny::reactive(x = {
      if (!hasData(consolidatedDatabaseIdTarget())) {
        return(NULL)
      }
      if (!hasData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (!hasData(consolidatedConceptSetIdTarget())) {
        return(NULL)
      }
      optimizedConceptSetExpression <-
        getOptimizedTargetConceptSetsExpressionTable()
      if (!hasData(optimizedConceptSetExpression)) {
        return(FALSE)
      }
      originalConceptSetExpression <-
        getResultsConceptSetExpression(
          dataSource = dataSource,
          cohortId = consolidatedCohortIdTarget(),
          conceptSetId = consolidatedConceptSetIdTarget()
        )
      originalConceptSetExpression <-
        getConceptSetDataFrameFromConceptSetExpression(originalConceptSetExpression)
      originalConceptSetExpression <-
        tidyr::crossing(
          dplyr::tibble(databaseId = consolidatedDatabaseIdTarget()),
          originalConceptSetExpression
        )
      
      removed <-
        originalConceptSetExpression %>%
        dplyr::select(.data$databaseId,
                      .data$conceptId,
                      .data$isExcluded,
                      .data$includeDescendants,
                      .data$includeMapped) %>% 
        dplyr::anti_join(
          optimizedConceptSetExpression %>%
            dplyr::select(.data$databaseId,
                          .data$conceptId,
                          .data$isExcluded,
                          .data$includeDescendants,
                          .data$includeMapped),
          by = c(
            "databaseId",
            "conceptId",
            "isExcluded",
            "includeDescendants",
            "includeMapped"
          )
        ) %>% dplyr::distinct()
      
      if (!hasData(removed)) {
        return(FALSE)
      }
      return(nrow(removed) != 0)
    })
  
  shiny::outputOptions(x = output,
                       name = "canTargetConceptSetExpressionBeOptimized",
                       suspendWhenHidden = FALSE)
  
  #output: targetConceptSetsExpressionOptimizedTable----
  output$targetConceptSetsExpressionOptimizedTable <-
    reactable::renderReactable(expr = {
      optimizedConceptSetExpression <-
        getOptimizedTargetConceptSetsExpressionTable()
      if (!hasData(optimizedConceptSetExpression)) {
        return(NULL)
      }
      
      optimizedConceptSetExpression$isExcluded <-
        ifelse(optimizedConceptSetExpression$isExcluded, as.character(icon("check")), "")
      optimizedConceptSetExpression$includeDescendants <-
        ifelse(optimizedConceptSetExpression$includeDescendants, as.character(icon("check")), "")
      optimizedConceptSetExpression$includeMapped <-
        ifelse(optimizedConceptSetExpression$includeMapped, as.character(icon("check")), "")
      
      optimizedConceptSetExpression <- optimizedConceptSetExpression %>%
        dplyr::rename(
          exclude = .data$isExcluded,
          descendants = .data$includeDescendants,
          mapped = .data$includeMapped,
          invalid = .data$invalidReason,
          standard = .data$standardConcept
        ) %>% 
        dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>% 
        dplyr::select(.data$databaseId,
                      .data$conceptId,
                      .data$conceptName,
                      .data$exclude,
                      .data$descendants,
                      .data$mapped,
                      .data$domainId,
                      .data$standard,
                      .data$conceptCode,
                      .data$invalid) %>% 
        dplyr::arrange(.data$databaseId)
      
      keyColumns <- colnames(optimizedConceptSetExpression)
      getSimpleReactable(data = optimizedConceptSetExpression,
                         dataColumns = c(),
                         keyColumns = keyColumns)
    })
  
  #output: targetConceptSetsExpressionTable----
  output$targetConceptSetsExpressionTable <-
    reactable::renderReactable(expr = {
      data <- getConceptSetExpressionTarget()
      if (!hasData(data)) {
        return(NULL)
      }
      
      data$isExcluded <-
        ifelse(data$isExcluded, as.character(icon("check")), "")
      data$includeDescendants <-
        ifelse(data$includeDescendants, as.character(icon("check")), "")
      data$includeMapped <-
        ifelse(data$includeMapped, as.character(icon("check")), "")
      
      data <- data %>%
        dplyr::rename(
          exclude = .data$isExcluded,
          descendants = .data$includeDescendants,
          mapped = .data$includeMapped,
          invalid = .data$invalidReason
        )
      keyColumns <- colnames(data)
      getSimpleReactable(data = data,
                         keyColumns = keyColumns,
                         dataColumns = c())
    })
  
  #output: targetCohortDefinitionResolvedConceptTable----
  output$targetCohortDefinitionResolvedConceptTable <-
    reactable::renderReactable(expr = {
      validate(need(
        length(consolidatedCohortIdTarget()) > 0,
        "Please select concept set"
      ))
      validate(need(
        length(consolidatedDatabaseIdTarget()) > 0,
        "Please select database id"
      ))
      data <- getResolvedConceptsTarget()
      validate(need(hasData(data), "No resolved concept ids"))
      keyColumnFields <- c("conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$targetCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        countLocation <- 2
      } else if (input$targetCohortConceptSetColumnFilter == "Persons") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        countLocation <- 1
      } else if (input$targetCohortConceptSetColumnFilter == "Records") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        countLocation <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$targetConceptIdCountSource,
          fields = input$targetCohortConceptSetColumnFilter
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)

      getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort, 
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnTarget, 
        sort = FALSE
      )
    })
  
  #output: targetCohortDefinitionExcludedConceptTable----
  output$targetCohortDefinitionExcludedConceptTable <-
    reactable::renderReactable(expr = {
      validate(need(
        length(consolidatedCohortIdTarget()) > 0,
        "Please select concept set"
      ))
      data <- getExcludedConceptsTarget()
      validate(need(hasData(data), "No excluded concept ids"))
      
      keyColumnFields <- c("conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$targetCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        countLocation <- 2
      } else if (input$targetCohortConceptSetColumnFilter == "Persons") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        countLocation <- 1
      } else if (input$targetCohortConceptSetColumnFilter == "Records") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        countLocation <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$targetConceptIdCountSource,
          fields = input$targetCohortConceptSetColumnFilter
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort, 
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnTarget, 
        sort = FALSE
      )
    })
  
  #output: targetCohortDefinitionOrphanConceptTable----
  output$targetCohortDefinitionOrphanConceptTable <-
    reactable::renderReactable(expr = {
      data <- getOrphanConceptsTarget()
      validate(need(any(!is.null(data),
                        nrow(data) > 0),
                    "No Recommended concepts"))
      
      keyColumnFields <- c("conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$targetCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        countLocation <- 2
      } else if (input$targetCohortConceptSetColumnFilter == "Persons") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        countLocation <- 1
      } else if (input$targetCohortConceptSetColumnFilter == "Records") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        countLocation <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$targetConceptIdCountSource,
          fields = input$targetCohortConceptSetColumnFilter
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort, 
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnTarget, 
        sort = FALSE
      )
    })
  
  #output: targetCohortDefinitionMappedConceptTable----
  output$targetCohortDefinitionMappedConceptTable <-
    reactable::renderReactable(expr = {
      validate(need(
        length(consolidatedCohortIdTarget()) > 0,
        "Please select concept set"
      ))
      validate(need(
        length(consolidatedDatabaseIdTarget()) > 0,
        "Please select database id"
      ))
      data <- getMappedConceptsTarget()
      validate(need(hasData(data), "No resolved concept ids"))
      keyColumnFields <- c("resolvedConcept", "conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$targetCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        countLocation <- 2
      } else if (input$targetCohortConceptSetColumnFilter == "Persons") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        countLocation <- 1
      } else if (input$targetCohortConceptSetColumnFilter == "Records") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        countLocation <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$targetConceptIdCountSource,
          fields = input$targetCohortConceptSetColumnFilter
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort, 
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnTarget, 
        sort = FALSE
      )
    })
  
  #output: targetConceptsetExpressionJson----
  output$targetConceptsetExpressionJson <- shiny::renderText({
    if (any(
      !hasData(getConceptSetExpressionTarget()),
      !hasData(consolidatedConceptSetIdTarget())
    )) {
      return(NULL)
    }
    conceptSetExpression <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdTarget()) %>%
      dplyr::pull(.data$conceptSetExpression) %>%
      RJSONIO::fromJSON(digits = 23) %>%
      RJSONIO::toJSON(digits = 23, pretty = TRUE)
  })
  
  #output: targetConceptsetExpressionSql----
  output$targetConceptsetExpressionSql <- shiny::renderText({
    if (any(
      !hasData(getConceptSetExpressionTarget()),
      !hasData(consolidatedConceptSetIdTarget())
    )) {
      return(NULL)
    }
    conceptSetExpression <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdTarget()) %>%
      dplyr::pull(.data$conceptSetSql)
    
  })
  
  #!!! on row select for resolved/excluded/Recommended - we need to show for selected cohort
  #!!! a trend plot with conceptCount over time and
  #!!! concept details (concept synonyms, concept relationships, concept ancestor, concept descendants)
  #!!! for both left and right
  #!!! as collapsible text box, that is by default collapsed with lazy loading
  #!!! data is in getConceptSetDetailsLeft reactive function and getConceptCountData reactive function
  
  
  ##output: comparatorCohortDefinitionCohortCountsTable----
  output$comparatorCohortDefinitionCohortCountsTable <-
    reactable::renderReactable(expr = {
      data <- getCountsForSelectedCohortsComparator()
      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
        "There is no inclusion rule data for this cohort."
      ))
      keyColumns <- c("databaseId")
      dataColumns <- c("cohortSubjects", "cohortEntries")
      getSimpleReactable(data = data,
                         keyColumns = keyColumns,
                         dataColumns = dataColumns)
      
    })
  
  ##reactive: isDatabaseIdFoundForSelectedComparatorCohortCount----
  output$isDatabaseIdFoundForSelectedComparatorCohortCount <-
    shiny::reactive(x = {
      return(!is.null(getSimplifiedInclusionRuleResultsComparator()))
    })
  shiny::outputOptions(x = output,
                       name = "isDatabaseIdFoundForSelectedComparatorCohortCount",
                       suspendWhenHidden = FALSE)
  
  ##output: comparatorCohortDefinitionSimplifiedInclusionRuleTable----
  output$comparatorCohortDefinitionSimplifiedInclusionRuleTable <-
    reactable::renderReactable(expr = {
      data <- getSimplifiedInclusionRuleResultsComparator()
      validate(need((nrow(data) > 0),
                    "There is no inclusion rule data for this cohort."))
      keyColumnFields <- c("ruleSequenceId", "ruleName")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("totalSubjects",
          "remainSubjects",
          "meetSubjects",
          "gainSubjects")
      if (input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters != "All") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower(
              input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters
            )
          )]
      }
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdComparator(),
          source = "Cohort Level",
          fields = input$comparatorCohortDefinitionInclusionRuleType
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = 1,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent =  input$comparatorCohortInclusionRulesAsPercent, 
        sort = FALSE
      )
    })
  
  ##output: getComparatorSimplifiedInclusionRuleResultsHasData----
  output$getComparatorSimplifiedInclusionRuleResultsHasData <-
    shiny::reactive(x = {
      return(!is.null(getSimplifiedInclusionRuleResultsComparator()))
    })
  
  shiny::outputOptions(x = output,
                       name = "getComparatorSimplifiedInclusionRuleResultsHasData",
                       suspendWhenHidden = FALSE)
  
  
  ##output: comparatorCohortDefinitionJson----
  output$comparatorCohortDefinitionJson <- shiny::renderText({
    json <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
      dplyr::pull(.data$json)
    if (!hasData(json)) {
      return(NULL)
    }
    json <- json %>%
      RJSONIO::fromJSON(digits = 23) %>%
      RJSONIO::toJSON(digits = 23, pretty = TRUE)
    return(json)
  })
  
  
  #output: comparatorConceptSetExpressionName----
  output$comparatorConceptSetExpressionName <-
    shiny::renderUI(expr = {
      if (!hasData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      if (!hasData(consolidatedConceptSetIdComparator())) {
        return(NULL)
      }
      
      conceptSetName <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
        dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdComparator()) %>%
        dplyr::pull(.data$conceptSetName)
      
      tags$table(
        tags$tr(
          tags$td(
            tags$h4(paste0("Concept set name:", conceptSetName))
          )
        )
      )
    })
  
  ##output: conceptsetExpressionTableComparator----
  output$conceptsetExpressionTableComparator <-
    DT::renderDataTable(expr = {
      if (!hasData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      if (!hasData(consolidatedConceptSetIdComparator())) {
        return(NULL)
      }
      data <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
        dplyr::filter(.data$conceptSetid %in% consolidatedConceptSetIdComparator()) %>%
        dplyr::pull(.data$conceptSetExpression)
      if (!hasData(data)) {
        return(NULL)
      }
      data <-
        getConceptSetDataFrameFromConceptSetExpression(conceptSetExpression = data)
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
  
  
  ##output: isComparatorCohortDefinitionConceptSetRowSelected----
  output$isComparatorCohortDefinitionConceptSetRowSelected <-
    shiny::reactive(x = {
      return(!is.null(reactable::getReactableState("comparatorCohortDefinitionConceptSets", "selected")))
    })
  shiny::outputOptions(x = output,
                       name = "isComparatorCohortDefinitionConceptSetRowSelected",
                       suspendWhenHidden = FALSE)
  
  #reactive: getOptimizedComparatorConceptSetsExpressionTable----
  getOptimizedComparatorConceptSetsExpressionTable <-
    shiny::reactive(x = {
      if (!input$tabs == "cohortDefinition") {
        return(NULL)
      }
      if (!hasData(consolidatedDatabaseIdTarget())) {
        return(NULL)
      }
      if (!hasData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      if (!hasData(consolidatedConceptSetIdComparator())) {
        return(NULL)
      }
      result <- getOptimizedConceptSet(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds = consolidatedCohortIdComparator(),
        conceptSetIds = consolidatedConceptSetIdComparator()
      )
      return(result)
    })
  
  # output$canComparatorConceptSetExpressionBeOptimized <-
  #   shiny::reactive(x = {
  #     if (!hasData(consolidatedDatabaseIdTarget())) {
  #       return(NULL)
  #     }
  #     if (!hasData(consolidatedCohortIdComparator())) {
  #       return(NULL)
  #     }
  #     if (!hasData(consolidatedConceptSetIdComparator())) {
  #       return(NULL)
  #     }
  #     optimizedConceptSetExpression <-
  #       getOptimizedComparatorConceptSetsExpressionTable()
  #     if (!hasData(optimizedConceptSetExpression)) {
  #       return(FALSE)
  #     }
  #     originalConceptSetExpression <-
  #       getResultsConceptSetExpression(
  #         dataSource = dataSource,
  #         cohortId = consolidatedCohortIdComparator(),
  #         conceptSetId = consolidatedConceptSetIdComparator()
  #       )
  #     originalConceptSetExpression <-
  #       getConceptSetDataFrameFromConceptSetExpression(originalConceptSetExpression)
  #     originalConceptSetExpression <-
  #       tidyr::crossing(
  #         dplyr::tibble(databaseId = consolidatedDatabaseIdTarget()),
  #         originalConceptSetExpression
  #       )
  #     
  #     removed <-
  #       originalConceptSetExpression %>%
  #       dplyr::select(.data$databaseId,
  #                     .data$conceptId,
  #                     .data$isExcluded,
  #                     .data$includeDescendants,
  #                     .data$includeMapped) %>% 
  #       dplyr::anti_join(
  #         optimizedConceptSetExpression %>%
  #           dplyr::select(.data$databaseId,
  #                         .data$conceptId,
  #                         .data$isExcluded,
  #                         .data$includeDescendants,
  #                         .data$includeMapped),
  #         by = c(
  #           "databaseId",
  #           "conceptId",
  #           "isExcluded",
  #           "includeDescendants",
  #           "includeMapped"
  #         )
  #       ) %>% dplyr::distinct()
  #     
  #     if (!hasData(removed)) {
  #       return(FALSE)
  #     }
  #     return(nrow(removed) != 0)
  #   })
  # 
  # shiny::outputOptions(x = output,
  #                      name = "canComparatorConceptSetExpressionBeOptimized",
  #                      suspendWhenHidden = FALSE)
  
  # ##output: saveComparatorConceptSetsExpressionOptimizedTable----
  # output$saveComparatorConceptSetsExpressionOptimizedTable <-
  #   downloadHandler(
  #     filename = function() {
  #       getCsvFileNameWithDateTime(string = "conceptsetOptimized")
  #     },
  #     content = function(file) {
  #       downloadCsv(x = getOptimizedComparatorConceptSetsExpressionTable(),
  #                   fileName = file)
  #     }
  #   )
  # 
  # #output: comparatorConceptSetsExpressionOptimizedTable----
  # output$comparatorConceptSetsExpressionOptimizedTable <-
  #   DT::renderDataTable(expr = {
  #     optimizedConceptSetExpression <-
  #       getOptimizedComparatorConceptSetsExpressionTable()
  #     if (!hasData(optimizedConceptSetExpression)) {
  #       return(NULL)
  #     }
  #     
  #     
  #     optimizedConceptSetExpression$isExcluded <-
  #       ifelse(optimizedConceptSetExpression$isExcluded, as.character(icon("check")), "")
  #     optimizedConceptSetExpression$includeDescendants <-
  #       ifelse(optimizedConceptSetExpression$includeDescendants, as.character(icon("check")), "")
  #     optimizedConceptSetExpression$includeMapped <-
  #       ifelse(optimizedConceptSetExpression$includeMapped, as.character(icon("check")), "")
  #     
  #     optimizedConceptSetExpression <- optimizedConceptSetExpression %>%
  #       dplyr::rename(
  #         exclude = .data$isExcluded,
  #         descendants = .data$includeDescendants,
  #         mapped = .data$includeMapped,
  #         invalid = .data$invalidReason,
  #         standard = .data$standardConcept
  #       ) %>% 
  #       dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>% 
  #       dplyr::select(.data$databaseId,
  #                     .data$conceptId,
  #                     .data$conceptName,
  #                     .data$exclude,
  #                     .data$descendants,
  #                     .data$mapped,
  #                     .data$domainId,
  #                     .data$standard,
  #                     .data$conceptCode,
  #                     .data$invalid) %>% 
  #       dplyr::arrange(.data$databaseId)
  #     
  #     options = list(
  #       pageLength = 100,
  #       lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
  #       searching = TRUE,
  #       lengthChange = TRUE,
  #       ordering = TRUE,
  #       paging = TRUE,
  #       info = TRUE,
  #       searchHighlight = TRUE,
  #       scrollX = TRUE,
  #       scrollY = "20vh",
  #       columnDefs = list(truncateStringDef(1, 80))
  #     )
  #     
  #     dataTable <- DT::datatable(
  #       optimizedConceptSetExpression,
  #       options = options,
  #       colnames = colnames(optimizedConceptSetExpression) %>% camelCaseToTitleCase(),
  #       rownames = FALSE,
  #       escape = FALSE,
  #       selection = 'none',
  #       filter = "top",
  #       class = "stripe nowrap compact"
  #     )
  #     return(dataTable)
  #   }, server = TRUE)
  
  
  
  ##output: comparatorConceptSetsExpressionTable----
  output$comparatorConceptSetsExpressionTable <-
    reactable::renderReactable(expr = {
      data <- getConceptSetExpressionComparator()
      if (!hasData(data)) {
        return(NULL)
      }
      
      data$isExcluded <-
        ifelse(data$isExcluded, as.character(icon("check")), "")
      data$includeDescendants <-
        ifelse(data$includeDescendants, as.character(icon("check")), "")
      data$includeMapped <-
        ifelse(data$includeMapped, as.character(icon("check")), "")
      
      data <- data %>%
        dplyr::rename(
          exclude = .data$isExcluded,
          descendants = .data$includeDescendants,
          mapped = .data$includeMapped,
          invalid = .data$invalidReason
        )
      keyColumns <- colnames(data)
      getSimpleReactable(data = data,
                         keyColumns = keyColumns,
                         dataColumns = c())
    })
  
  ##output: comparatorCohortDefinitionResolvedConceptTable----
  output$comparatorCohortDefinitionResolvedConceptTable <-
    reactable::renderReactable(expr = {
      data <- getResolvedConceptsComparator()
      validate(need((all(
        !is.null(data), nrow(data) > 0
      )),
      "No resolved concept ids"))
      keyColumnFields <- c("conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$comparatorCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        countLocation <- 2
      } else if (input$comparatorCohortConceptSetColumnFilter == "Persons") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        countLocation <- 1
      } else if (input$comparatorCohortConceptSetColumnFilter == "Records") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        countLocation <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$comparatorConceptIdCountSource,
          fields = input$comparatorCohortConceptSetColumnFilter
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent =  input$showAsPercentageColumnComparator, 
        sort = FALSE
      )
    })
  
  #output: comparatorCohortDefinitionExcludedConceptTable----
  output$comparatorCohortDefinitionExcludedConceptTable <-
    reactable::renderReactable(expr = {
      data <- getExcludedConceptsComparator()
      validate(need((all(
        !is.null(data), nrow(data) > 0
      )),
      "No excluded concept ids"))
      keyColumnFields <- c("conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$comparatorCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        countLocation <- 2
      } else if (input$comparatorCohortConceptSetColumnFilter == "Persons") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        countLocation <- 1
      } else if (input$comparatorCohortConceptSetColumnFilter == "Records") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        countLocation <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$comparatorConceptIdCountSource,
          fields = input$comparatorCohortConceptSetColumnFilter
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent =  input$showAsPercentageColumnComparator, 
        sort = FALSE
      )
    })
  
  output$comparatorCohortDefinitionOrphanConceptTable <-
    reactable::renderReactable(expr = {
      data <- getOrphanConceptsComparator()
      validate(need(any(!is.null(data),
                        nrow(data) > 0),
                    "No Recommended concepts"))
      keyColumnFields <- c("conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$comparatorCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        countLocation <- 2
      } else if (input$comparatorCohortConceptSetColumnFilter == "Persons") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        countLocation <- 1
      } else if (input$comparatorCohortConceptSetColumnFilter == "Records") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        countLocation <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$comparatorConceptIdCountSource,
          fields = input$comparatorCohortConceptSetColumnFilter
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent =  input$showAsPercentageColumnComparator, 
        sort = FALSE
      )
    })
  
  #output: comparatorCohortDefinitionMappedConceptTable----
  output$comparatorCohortDefinitionMappedConceptTable <-
    reactable::renderReactable(expr = {
      validate(need(
        length(consolidatedCohortIdComparator()) > 0,
        "Please select concept set"
      ))
      validate(need(
        length(consolidatedDatabaseIdTarget()) > 0,
        "Please select database id"
      ))
      data <- getMappedConceptsComparator()
      validate(need(hasData(data), "No resolved concept ids"))
      keyColumnFields <- c("resolvedConcept", "conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$targetCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        countLocation <- 2
      } else if (input$targetCohortConceptSetColumnFilter == "Persons") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        countLocation <- 1
      } else if (input$targetCohortConceptSetColumnFilter == "Records") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        countLocation <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$targetConceptIdCountSource,
          fields = input$targetCohortConceptSetColumnFilter
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent =  input$showAsPercentageColumnTarget , 
        sort = FALSE
      )
    })
  
  ##output: comparatorCohortDefinitionConceptsetExpressionJson----
  output$comparatorCohortDefinitionConceptsetExpressionJson <-
    shiny::renderText({
      if (any(
        !hasData(consolidatedCohortIdComparator()),
        !hasData(consolidatedConceptSetIdComparator())
      )) {
        return(NULL)
      }
      conceptSetExpression <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
        dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdComparator()) %>%
        dplyr::pull(.data$conceptSetExpression) %>%
        RJSONIO::fromJSON(digits = 23) %>%
        RJSONIO::toJSON(digits = 23, pretty = TRUE)
      return(conceptSetExpression)
    })
  
  ##output: comparatorCohortDefinitionConceptsetExpressionSql----
  output$comparatorCohortDefinitionConceptsetExpressionSql <-
    shiny::renderText({
      if (any(
        !hasData(consolidatedCohortIdComparator()),
        !hasData(consolidatedConceptSetIdComparator())
      )) {
        return(NULL)
      }
      conceptSetExpression <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
        dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdComparator()) %>%
        dplyr::pull(.data$conceptSetSql)
      return(conceptSetExpression)
    })
  
  output$conceptSetTimeSeriesPlot <-  plotly::renderPlotly({
    data <- getMetadataForConceptId()
    if (!hasData(data)) {
      return(NULL)
    }
    databaseCount <- getDataSourceTimeSeries()
    if (!hasData(databaseCount)) {
      return(NULL)
    }
    # working on the plot
    if (input$timeSeriesAggregationForConceptId == "Monthly") {
      data <- data$databaseConceptIdYearMonthLevelTsibble
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>% 
        dplyr::filter(.data$conceptId == activeSelected()$conceptId)
      if (!hasData(databaseCount$m)) {
        return(NULL)
      }
      databaseCount <- databaseCount$m %>% 
        dplyr::select(.data$databaseId,
                      .data$periodBegin,
                      .data$records,
                      .data$subjects)
    } else {
      data <- data$databaseConceptIdYearLevelTsibble
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>% 
        dplyr::filter(.data$conceptId == activeSelected()$conceptId)
      if (!hasData(databaseCount$y)) {
        return(NULL)
      }
      databaseCount <- databaseCount$y %>% 
        dplyr::select(.data$databaseId,
                      .data$periodBegin,
                      .data$records,
                      .data$subjects)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0("Building Time Series plot for concept id:",
                       activeSelected()$conceptId),
      value = 0
    )
    validate(need(hasData(data),
      "No timeseries data for the cohort of this series type"
    ))
    data <- data %>% 
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>% 
      dplyr::rename("records" = .data$conceptCount,
                    "persons" = .data$subjectCount) %>% 
      dplyr::mutate(records = abs(.data$records),
                    persons = abs(.data$persons))
    
    data <- data %>% 
      dplyr::inner_join(databaseCount %>% 
                          dplyr::rename(recordsDatasource = .data$records,
                                        personsDatasource = .data$subjects),
                        by = c("databaseId", "periodBegin")) %>% 
      dplyr::mutate(recordsProportion = .data$records/.data$recordsDatasource,
                    personsProportion = .data$persons/.data$personsDatasource) %>% 
      dplyr::select(-.data$recordsDatasource, -.data$personsDatasource)
    
    

    tsibbleDataFromSTLModel <- getStlModelOutputForTsibbleDataValueFields(tsibbleData = data,
                                                                          valueFields = c("records", "persons", "recordsProportion", "personsProportion"))

    conceptName <- getMetadataForConceptId()$concept %>% 
      dplyr::filter(.data$conceptId == activeSelected()$conceptId) %>% 
      dplyr::pull(.data$conceptName)
    
    conceptSynonym <- getMetadataForConceptId()$conceptSynonym$conceptSynonymName %>% 
      unique() %>%
      sort() %>% 
      paste0(collapse = ", ")
    
    plot <- plotTimeSeriesForCohortDefinitionFromTsibble(
      stlModeledTsibbleData = tsibbleDataFromSTLModel,
      conceptId = activeSelected()$conceptId,
      conceptName = conceptName,
      conceptSynonym = conceptSynonym
    )
    plot <- plotly::ggplotly(plot)
    return(plot)
  })
  
  ##output:: conceptBrowserConceptSynonymNameInHtmlString
  output$conceptBrowserConceptSynonymNameInHtmlString <- shiny::renderUI(expr = {
     data <- getConceptSetSynonymsHtmlTextString()
     if (!hasData(data)) {
       return(NULL)
     }
     return(data)
  })
  
  conceptSetBrowserData <- shiny::reactive(x = {
    conceptId <- activeSelected()$conceptId
    validate(need(hasData(conceptId), "No concept id selected."))
    cohortId <- activeSelected()$cohortId
    validate(need(hasData(conceptId), "No cohort id selected."))
    databaseId <- consolidatedDatabaseIdTarget()
    validate(need(hasData(databaseId), "No database id selected."))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0("Computing concept relationship for concept id:",
                       conceptId),
      value = 0
    )
    
    data <- getMetadataForConceptId()
    validate(need(
      hasData(data),
      "No information for selected concept id."
    ))
    if (input$tabs == "indexEventBreakdown") {
      relationshipNameFilter <-
        input$choicesForRelationshipNameForIndexEvent
      relationshipDistanceFilter <-
        input$choicesForRelationshipDistanceForIndexEvent
    } else {
      relationshipNameFilter <- input$choicesForRelationshipName
      relationshipDistanceFilter <-
        input$choicesForRelationshipDistance
    }
    
    conceptRelationshipTable <- data$conceptRelationshipTable %>%
      dplyr::filter(.data$conceptId != activeSelected()$conceptId)
    if (any(
      hasData(relationshipNameFilter),
      hasData(relationshipDistanceFilter)
    )) {
      if (hasData(relationshipNameFilter)) {
        conceptRelationshipTable <- conceptRelationshipTable %>%
          dplyr::inner_join(
            relationship %>%
              dplyr::filter(.data$relationshipName %in% c(relationshipNameFilter)) %>%
              dplyr::select(.data$relationshipId) %>%
              dplyr::distinct(),
            by = "relationshipId"
          )
      }
      if (hasData(relationshipDistanceFilter)) {
        conceptRelationshipTable <- conceptRelationshipTable %>%
          dplyr::filter(.data$levelsOfSeparation %in%
                          relationshipDistanceFilter)
      }
    }
    conceptRelationshipTable <- conceptRelationshipTable %>%
      dplyr::inner_join(data$concept,
                        by = "conceptId") %>%
      tidyr::crossing(dplyr::tibble(databaseId = !!databaseId))
    if (!hasData(conceptRelationshipTable)) {
      return(NULL)
    }
    
    data <- conceptRelationshipTable %>%
      dplyr::left_join(data$databaseConceptCount,
                       by = c("databaseId", "conceptId")) %>%
      dplyr::rename("records" = .data$conceptCount,
                    "persons" = .data$subjectCount) %>%
      dplyr::select(
        .data$databaseId,
        .data$conceptId,
        .data$conceptName,
        .data$domainId,
        .data$vocabularyId,
        .data$standardConcept,
        .data$levelsOfSeparation,
        .data$relationshipId,
        .data$records,
        .data$persons
      ) %>%
      dplyr::arrange(.data$databaseId,
                     .data$conceptId,
                     dplyr::desc(.data$records))
    
    return(data)
  })
  
  ##output: conceptBrowserTable----
  output$conceptBrowserTable <- reactable::renderReactable(expr = {
    
    data <- conceptSetBrowserData()
    if (!hasData(data)) {
      return(NULL)
    }
    keyColumnFields <- c("conceptId", 
                         "conceptName",
                         "vocabularyId",
                         "domainId",
                         "standardConcept",
                         "levelsOfSeparation",
                         "relationshipId")
    #depending on user selection - what data Column Fields Will Be Presented?
    dataColumnFields <-
      c("persons",
        "records")
    if (input$targetCohortConceptSetColumnFilter == "Both") {
      dataColumnFields <- dataColumnFields
      countLocation <- 2
    } else if (input$targetCohortConceptSetColumnFilter == "Persons") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(
          string = tolower(dataColumnFields),
          pattern = tolower("person")
        )]
      countLocation <- 1
    } else if (input$targetCohortConceptSetColumnFilter == "Records") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(
          string = tolower(dataColumnFields),
          pattern = tolower("record")
        )]
      countLocation <- 1
    }
    
    countsForHeader <-
      getCountsForHeaderForUseInDataTable(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds = consolidatedCohortIdTarget(),
        source = input$targetConceptIdCountSource,
        fields = input$targetCohortConceptSetColumnFilter
      )
    if (!hasData(countsForHeader)) {
      return(NULL)
    }
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    getReactTableWithColumnsGroupedByDatabaseId(
      data = data,
      cohort = cohort,
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = countLocation,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showResultsAsPercent =  input$showAsPercentageColumnTarget, 
      sort = TRUE
    )
  })
  
  ##getSourceCodesObservedForConceptIdInDatasource----
  getSourceCodesObservedForConceptIdInDatasource <- shiny::reactive(x = {
    conceptId <- activeSelected()$conceptId
    cohortId <- activeSelected()$cohortId
    databaseId <- consolidatedDatabaseIdTarget()
    if (!all(
      hasData(conceptId),
      hasData(cohortId),
      hasData(databaseId)
    )){
      return(NULL)
    }
    data <- getResultsConceptMapping(
      dataSource,
      databaseIds = databaseId,
      conceptIds = conceptId,
      domainTables = 'All'
    )
    if (!hasData(data)) {
      return(NULL)
    }
    conceptMapping <- data %>%
      dplyr::inner_join(
        getConcept(dataSource, conceptIds = data$sourceConceptId),
        by = c("sourceConceptId" = "conceptId")
      ) %>%
      dplyr::select(
        .data$sourceConceptId,
        .data$conceptName,
        .data$vocabularyId,
        .data$databaseId,
        .data$conceptCount,
        .data$subjectCount
      ) %>%
      dplyr::rename(
        "conceptId" = .data$sourceConceptId,
        "persons" = .data$subjectCount,
        "records" = .data$conceptCount
      )
    return(conceptMapping)
  })
  
  
  ##output: observedSourceCodesTable----
  output$observedSourceCodesTable <- reactable::renderReactable(expr = {
    conceptId <- activeSelected()$conceptId
    validate(need(hasData(conceptId), "No concept id selected."))
    cohortId <- activeSelected()$cohortId
    validate(need(hasData(conceptId), "No cohort id selected."))
    databaseId <- consolidatedDatabaseIdTarget()
    validate(need(hasData(databaseId), "No database id selected."))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0("Computing concept relationship for concept id:",
                       conceptId),
      value = 0
    )
    data <- getSourceCodesObservedForConceptIdInDatasource()
    validate(need(
      hasData(data),
      "No information for selected concept id."
    ))
    
    keyColumnFields <- c("conceptId", 
                         "conceptName",
                         "vocabularyId")
    #depending on user selection - what data Column Fields Will Be Presented?
    dataColumnFields <-
      c("persons",
        "records")
    
    countsForHeader <-
      getCountsForHeaderForUseInDataTable(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds =  activeSelected()$cohortId,
        source = "Cohort Level",
        fields = input$cohortCountInclusionRuleType
      )
    if (!hasData(countsForHeader)) {
      return(NULL)
    }
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    
    getReactTableWithColumnsGroupedByDatabaseId(
      data = data,
      cohort = cohort,
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = 1,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showResultsAsPercent =  input$targetCohortInclusionRulesAsPercent, 
      sort = TRUE
    )
  })
  
  output$exportAllCohortDetails <- downloadHandler(
    filename = function() {
      paste("ExportDetails", "zip", sep = ".")
    },
    content = function(file) {
      exportCohortDetailsAsZip(dataSource = dataSource,
                               zipFile = file)
    },
    contentType = "application/zip"
  )
  
  #Radio button synchronization----
  shiny::observeEvent(eventExpr = {
    list(input$targetConceptSetsType,
         input$targetCohortDefinitionTabSetPanel,
         input$targetConceptIdCountSource,
         input$targetCohortDefinitionInclusionRuleType,
         input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters,
         input$targetCohortConceptSetColumnFilter
         )
  }, handlerExpr = {
    if (getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable() == 6) {
      if (!is.null(input$targetConceptSetsType)) {
        if (input$targetConceptSetsType == "Concept Set Expression") {
          updateRadioButtons(session = session,
                             inputId = "comparatorConceptSetsType",
                             selected = "Concept Set Expression")
        } else if (input$targetConceptSetsType == "Resolved") {
          updateRadioButtons(session = session,
                             inputId = "comparatorConceptSetsType",
                             selected = "Resolved")
        } else if (input$targetConceptSetsType == "Excluded") {
          updateRadioButtons(session = session,
                             inputId = "comparatorConceptSetsType",
                             selected = "Excluded")
        } else if (input$targetConceptSetsType == "Mapped") {
          updateRadioButtons(session = session,
                             inputId = "comparatorConceptSetsType",
                             selected = "Mapped")
        } else if (input$targetConceptSetsType == "Recommended") {
          updateRadioButtons(session = session,
                             inputId = "comparatorConceptSetsType",
                             selected = "Recommended")
        } else if (input$targetConceptSetsType == "Concept Set Json") {
          updateRadioButtons(session = session,
                             inputId = "comparatorConceptSetsType",
                             selected = "Concept Set Json")
        } else if (input$targetConceptSetsType == "Concept Set Sql") {
          updateRadioButtons(session = session,
                             inputId = "comparatorConceptSetsType",
                             selected = "Concept Set Sql")
        }
      }
    
      if (!is.null(input$targetConceptIdCountSource)) {
        if (input$targetConceptIdCountSource == "Datasource Level") {
          updateRadioButtons(session = session,
                             inputId = "comparatorConceptIdCountSource",
                             selected = "Datasource Level")
        } else {
          updateRadioButtons(session = session,
                             inputId = "comparatorConceptIdCountSource",
                             selected = "Cohort Level")
        }
      }
      
      if (!is.null(input$targetCohortDefinitionTabSetPanel)) {
        if (input$targetCohortDefinitionTabSetPanel == "targetCohortDefinitionDetailsTextTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "comparatorCohortDefinitionTabSetPanel", 
                                   selected = "comparatorCohortDefinitionDetailsTextTabPanel")
        } else if (input$targetCohortDefinitionTabSetPanel == "targetCohortDefinitionCohortCountTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "comparatorCohortDefinitionTabSetPanel", 
                                   selected = "comparatorCohortDefinitionCohortCountTabPanel")
        } else if (input$targetCohortDefinitionTabSetPanel == "targetCohortDefinitionConceptSetTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "comparatorCohortDefinitionTabSetPanel", 
                                   selected = "comparatorCohortDefinitionConceptSetTabPanel")
        } else if (input$targetCohortDefinitionTabSetPanel == "targetCohortDefinitionJsonTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "comparatorCohortDefinitionTabSetPanel", 
                                   selected = "comparatorCohortDefinitionJsonTabPanel")
        } else if (input$targetCohortDefinitionTabSetPanel == "targetCohortDefinitionSqlTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "comparatorCohortDefinitionTabSetPanel", 
                                   selected = "comparatorCohortDefinitionSqlTabPanel")
        } else if (input$targetCohortDefinitionTabSetPanel == "targetCohortdefinitionInclusionRuleTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "comparatorCohortDefinitionTabSetPanel", 
                                   selected = "comparatorCohortDefinitionUnclusionRuleTabPanel")
        }
      }
      
      if (!is.null(input$targetCohortDefinitionInclusionRuleType)) {
        if (input$targetCohortDefinitionInclusionRuleType == "Events") {
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortDefinitionInclusionRuleType",
                             selected = "Events")
        } else {
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortDefinitionInclusionRuleType",
                             selected = "Persons")
        }
      }
      
      if (!is.null(input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters)) {
        if (input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters == "All") {
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters",
                             selected = "All")
        } else if (input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters == "Meet"){
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters",
                             selected = "Meet")
        } else if (input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters == "Gain"){
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters",
                             selected = "Gain")
        } else if (input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters == "Remain"){
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters",
                             selected = "Remain")
        } else if (input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters == "Totals"){
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters",
                             selected = "Totals")
        }
      }
      
      if (!is.null(input$targetCohortConceptSetColumnFilter)) {
        if (input$targetCohortConceptSetColumnFilter == "Both") {
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortConceptSetColumnFilter",
                             selected = "Both")
        } else if (input$targetCohortConceptSetColumnFilter == "Persons") {
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortConceptSetColumnFilter",
                             selected = "Persons")
        } else {
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortConceptSetColumnFilter",
                             selected = "Records")
        }
      }
    }
  })
  
  shiny::observeEvent(eventExpr = {
    list(
      input$comparatorConceptSetsType,
      input$comparatorCohortDefinitionTabSetPanel,
      input$comparatorConceptIdCountSource,
      input$comparatorCohortDefinitionInclusionRuleType,
      input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters,
      input$comparatorCohortConceptSetColumnFilter
    )
  }, handlerExpr = {
    if (getWidthOfLeftPanelForCohortDetailBrowserInCohortDefinitionTabBasedOnNoOfRowSelectedInCohortTable() == 6) {
      if (!is.null(input$comparatorConceptSetsType)) {
        if (input$comparatorConceptSetsType == "Concept Set Expression") {
          updateRadioButtons(session = session,
                             inputId = "targetConceptSetsType",
                             selected = "Concept Set Expression")
        } else if (input$comparatorConceptSetsType == "Resolved") {
          updateRadioButtons(session = session,
                             inputId = "targetConceptSetsType",
                             selected = "Resolved")
        } else if (input$comparatorConceptSetsType == "Excluded") {
          updateRadioButtons(session = session,
                             inputId = "targetConceptSetsType",
                             selected = "Excluded")
        } else if (input$comparatorConceptSetsType == "Mapped") {
          updateRadioButtons(session = session,
                             inputId = "targetConceptSetsType",
                             selected = "Mapped")
        } else if (input$comparatorConceptSetsType == "Recommended") {
          updateRadioButtons(session = session,
                             inputId = "targetConceptSetsType",
                             selected = "Recommended")
        } else if (input$comparatorConceptSetsType == "Concept Set Json") {
          updateRadioButtons(session = session,
                             inputId = "targetConceptSetsType",
                             selected = "Concept Set Json")
        } else if (input$comparatorConceptSetsType == "Concept Set Sql") {
          updateRadioButtons(session = session,
                             inputId = "targetConceptSetsType",
                             selected = "Concept Set Sql")
        }
      }
      
      if (!is.null(input$comparatorCohortDefinitionTabSetPanel)) {
        if (input$comparatorCohortDefinitionTabSetPanel == "comparatorCohortDefinitionDetailsTextTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "targetCohortDefinitionTabSetPanel", 
                                   selected = "targetCohortDefinitionDetailsTextTabPanel")
        } else if (input$comparatorCohortDefinitionTabSetPanel == "comparatorCohortDefinitionCohortCountTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "targetCohortDefinitionTabSetPanel", 
                                   selected = "targetCohortDefinitionCohortCountTabPanel")
        } else if (input$comparatorCohortDefinitionTabSetPanel == "comparatorCohortDefinitionConceptSetTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "targetCohortDefinitionTabSetPanel", 
                                   selected = "targetCohortDefinitionConceptSetTabPanel")
        } else if (input$comparatorCohortDefinitionTabSetPanel == "comparatorCohortDefinitionJsonTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "targetCohortDefinitionTabSetPanel", 
                                   selected = "targetCohortDefinitionJsonTabPanel")
        } else if (input$comparatorCohortDefinitionTabSetPanel == "comparatorCohortDefinitionSqlTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "targetCohortDefinitionTabSetPanel", 
                                   selected = "targetCohortDefinitionSqlTabPanel")
        } else if (input$comparatorCohortDefinitionTabSetPanel == "comparatorCohortDefinitionSqlTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "targetCohortDefinitionTabSetPanel", 
                                   selected = "targetCohortdefinitionInclusionRuleTabPanel")
        } else if (input$comparatorCohortDefinitionTabSetPanel == "comparatorCohortDefinitionUnclusionRuleTabPanel") {
          shiny::updateTabsetPanel(session, 
                                   inputId = "targetCohortDefinitionTabSetPanel", 
                                   selected = "targetCohortdefinitionInclusionRuleTabPanel")
        }
      }
      
      if (!is.null(input$comparatorConceptIdCountSource)) {
        if (input$comparatorConceptIdCountSource == "Datasource Level") {
          updateRadioButtons(session = session,
                             inputId = "targetConceptIdCountSource",
                             selected = "Datasource Level")
        } else {
          updateRadioButtons(session = session,
                             inputId = "targetConceptIdCountSource",
                             selected = "Cohort Level")
        }
      }
      
      if (!is.null(input$comparatorCohortDefinitionInclusionRuleType)) {
        if (input$comparatorCohortDefinitionInclusionRuleType == "Events") {
          updateRadioButtons(session = session,
                             inputId = "targetCohortDefinitionInclusionRuleType",
                             selected = "Events")
        } else {
          updateRadioButtons(session = session,
                             inputId = "targetCohortDefinitionInclusionRuleType",
                             selected = "Persons")
        }
      }
      
      if (!is.null(input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters)) {
        if (input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters == "All") {
          updateRadioButtons(session = session,
                             inputId = "targetCohortDefinitionSimplifiedInclusionRuleTableFilters",
                             selected = "All")
        } else if (input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters == "Meet"){
          updateRadioButtons(session = session,
                             inputId = "targetCohortDefinitionSimplifiedInclusionRuleTableFilters",
                             selected = "Meet")
        } else if (input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters == "Gain"){
          updateRadioButtons(session = session,
                             inputId = "targetCohortDefinitionSimplifiedInclusionRuleTableFilters",
                             selected = "Gain")
        } else if (input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters == "Remain"){
          updateRadioButtons(session = session,
                             inputId = "targetCohortDefinitionSimplifiedInclusionRuleTableFilters",
                             selected = "Remain")
        } else if (input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters == "Totals"){
          updateRadioButtons(session = session,
                             inputId = "targetCohortDefinitionSimplifiedInclusionRuleTableFilters",
                             selected = "Totals")
        }
      }
      
      if (!is.null(input$comparatorCohortConceptSetColumnFilter)) {
        if (input$comparatorCohortConceptSetColumnFilter == "Both") {
          updateRadioButtons(session = session,
                             inputId = "targetCohortConceptSetColumnFilter",
                             selected = "Both")
        } else if (input$comparatorCohortConceptSetColumnFilter == "Persons") {
          updateRadioButtons(session = session,
                             inputId = "targetCohortConceptSetColumnFilter",
                             selected = "Persons")
        } else {
          updateRadioButtons(session = session,
                             inputId = "targetCohortConceptSetColumnFilter",
                             selected = "Records")
        }
      }
    }
  })
  #______________----
  # Cohort Counts Tab -----
  ###getCohortCountDataForSelectedDatabaseIdsCohortIds----
  #used to display counts in cohort definition panels
  getCohortCountDataForSelectedDatabaseIdsCohortIds <-
    shiny::reactive(x = {
      if (all(is(dataSource, "environment"),
              !exists('cohortCount'))) {
        return(NULL)
      }
      if (!hasData(consolidatedDatabaseIdTarget())) {
        return(NULL)
      }
      if (!hasData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      data <- cohortCount %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
        dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>%
        dplyr::inner_join(cohort %>%
                            dplyr::select(.data$cohortId, .data$shortName),
                          by = "cohortId") %>%
        dplyr::arrange(.data$shortName, .data$databaseId)
      if (!hasData(data)) {
        return(NULL)
      }
      return(data)
    })
  
  ##getCohortCountDataSubjectRecord----
  getCohortCountDataSubjectRecord <- shiny::reactive(x = {
    data <- getCohortCountDataForSelectedDatabaseIdsCohortIds()
    if (!hasData(data)) {
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
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::rename(cohort = .data$shortName) %>% 
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
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::rename(cohort = .data$shortName) %>% 
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
  
  ##output: cohortCountsTable----
  output$cohortCountsTable <- reactable::renderReactable(expr = {
    validate(need(
      length(consolidatedDatabaseIdTarget()) > 0,
      "No data sources chosen"
    ))
    validate(need(
      length(consolidatedCohortIdTarget()) > 0,
      "No cohorts chosen"
    ))
    data <- getCohortCountDataForSelectedDatabaseIdsCohortIds() %>% 
      dplyr::rename("records" = .data$cohortEntries,
                    "persons" = .data$cohortSubjects,
                    "cohort" = .data$shortName)
    validate(need(all(hasData(data)),
                  "No data for the combination"
    ))
    
    keyColumnFields <- c("cohort")
    dataColumnFields <- c("persons", "records")
    
    if (input$cohortCountsTableColumnFilter == "Both") {
      dataColumnFields <- dataColumnFields
      countLocation <- 2
    } else if (input$cohortCountsTableColumnFilter == "Persons") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(
          string = tolower(dataColumnFields),
          pattern = tolower("person")
        )]
      countLocation <- 1
    } else if (input$cohortCountsTableColumnFilter == "Records") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(
          string = tolower(dataColumnFields),
          pattern = tolower("record")
        )]
      countLocation <- 1
    }
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    
    countsForHeader <-
      getCountsForHeaderForUseInDataTable(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds =  activeSelected()$cohortId,
        source = "Datasource Level",
        fields = input$cohortCountsTableColumnFilter
      )
    
    getReactTableWithColumnsGroupedByDatabaseId(
      data = data,
      cohort = cohort,
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = countLocation,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showResultsAsPercent =  FALSE, 
      sort = TRUE
    )
  })
  
  ###getCohortIdFromSelectedRowInCohortCountTable----
  getCohortIdFromSelectedRowInCohortCountTable <- reactive({
    idx <- reactable::getReactableState("cohortCountsTable", "selected")
    if (is.null(idx)) {
      return(NULL)
    } else {
      if (!hasData(getCohortCountDataForSelectedDatabaseIdsCohortIds())) {
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
    }
  })
  
  ##output: doesSelectedRowInCohortCountTableHaveCohortId----
  output$doesSelectedRowInCohortCountTableHaveCohortId <-
    reactive({
      return(!is.null(getCohortIdFromSelectedRowInCohortCountTable()))
    })
  outputOptions(output,
                "doesSelectedRowInCohortCountTableHaveCohortId",
                suspendWhenHidden = FALSE)
  
  ##output: inclusionRuleStatisticsForCohortSeletedTable----
  output$inclusionRuleStatisticsForCohortSeletedTable <-
   reactable::renderReactable(expr = {
      validate(need(
        length(consolidatedDatabaseIdTarget()) > 0,
        "No data sources chosen"
      ))
      validate(need(
        nrow(getCohortIdFromSelectedRowInCohortCountTable()) > 0,
        "No cohorts chosen"
      ))
      
      data <- getResultsInclusionRuleStatistics(
        dataSource = dataSource,
        cohortId = getCohortIdFromSelectedRowInCohortCountTable()$cohortId,
        databaseId = consolidatedDatabaseIdTarget()
      )
      
      validate(need((nrow(data) > 0),
                    "There is no inclusion rule data for this cohort."))
      keyColumnFields <- c("ruleSequenceId", "ruleName")
      dataColumnFields <-
        c("totalSubjects",
          "remainSubjects",
          "meetSubjects",
          "gainSubjects")
      if (input$cohortCountInclusionRules != "All") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower(
              input$cohortCountInclusionRules
            )
          )]
      }
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds =  getCohortIdFromSelectedRowInCohortCountTable()$cohortId,
          source = "Cohort Level",
          fields = input$cohortCountInclusionRuleType
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = 1,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent =  input$inclusionRuleShowAsPercentInCohortCount, 
        sort = TRUE
      )
    })
  
  #______________----
  # Incidence rate -------
  ##reactive: getIncidenceRateData----
  getIncidenceRateData <- reactive({
    if (any(is.null(input$tabs), 
            !input$tabs == "incidenceRate")) {
      return(NULL)
    }
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Getting incidence rate data."),
                 value = 0)
    data <- getResultsIncidenceRate(dataSource = dataSource,
                                    cohortId =  consolidatedCohortIdTarget(), 
                                    databaseId = consolidatedDatabaseIdTarget())
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::mutate(incidenceRate = dplyr::case_when(.data$incidenceRate < 0 ~ 0,
                                                     TRUE ~ .data$incidenceRate))
    return(data)
  })
  
  ##pickerInput - incidenceRateAgeFilter----
  shiny::observe({
    if (!hasData(getIncidenceRateData())) {
      return(NULL)
    }
    ageFilter <- getIncidenceRateData() %>%
      dplyr::select(.data$ageGroup) %>%
      dplyr::filter(.data$ageGroup != "NA",!is.na(.data$ageGroup)) %>%
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
  })
  
  ##pickerInput - incidenceRateGenderFilter----
  shiny::observe({
    if (!hasData(getIncidenceRateData())) {
      return(NULL)
    }
    genderFilter <- getIncidenceRateData() %>%
      dplyr::select(.data$gender) %>%
      dplyr::filter(.data$gender != "NA",!is.na(.data$gender)) %>%
      dplyr::distinct() %>%
      dplyr::arrange(.data$gender)
    
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "incidenceRateGenderFilter",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = genderFilter$gender,
      selected = genderFilter$gender
    )
  })
  
  ##pickerInput - incidenceRateCalendarFilter & YscaleMinAndMax----
  shiny::observe({
    if (!hasData(getIncidenceRateData())) {
      return(NULL)
    }
    calendarFilter <- getIncidenceRateData() %>%
      dplyr::select(.data$calendarYear) %>%
      dplyr::filter(.data$calendarYear != "NA") %>% 
      dplyr::filter(!is.na(.data$calendarYear)) %>%
      dplyr::filter(.data$calendarYear != "") %>%
      dplyr::distinct(.data$calendarYear) %>%
      dplyr::arrange(.data$calendarYear) %>% 
      dplyr::mutate(calendarYear = as.double(.data$calendarYear))
    
    minValue <- max(1980, min(calendarFilter$calendarYear))
    maxValue <- max(calendarFilter$calendarYear)
    shiny::updateSliderInput(
      session = session,
      inputId = "incidenceRateCalendarFilter",
      min = minValue,
      max = maxValue,
      value = c(max(2010,minValue), maxValue)
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
      step = round((
        maxIncidenceRateValue - minIncidenceRateValue
      ) / 5,
      digits = 2)
    )
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
  
  ##reactive: getIncidenceRateCalendarYears----
  getIncidenceRateCalendarYears <-
    shiny::reactive({
      if (!hasData(getIncidenceRateData())) {
        return(NULL)
      }
      if (!hasData(input$incidenceRateCalendarFilter)) {
        return(NULL)
      }
      if (input$incidenceRateCalendarFilter[2] <=
          input$incidenceRateCalendarFilter[1]) {
        return(NULL)
      }
      calendarFilter <- getIncidenceRateData() %>%
        dplyr::select(.data$calendarYear) %>%
        dplyr::filter(.data$calendarYear != "NA") %>%
        dplyr::filter(!is.na(.data$calendarYear)) %>%
        dplyr::filter(.data$calendarYear != "") %>%
        dplyr::distinct(.data$calendarYear) %>%
        dplyr::arrange(.data$calendarYear) %>%
        dplyr::pull(.data$calendarYear)
      if (input$incidenceRateCalendarFilter[2] >
          input$incidenceRateCalendarFilter[1]) {
        calendarFilter <-
          calendarFilter[calendarFilter >= input$incidenceRateCalendarFilter[1] &
                           calendarFilter <= input$incidenceRateCalendarFilter[2]]
      }
      return(calendarFilter)
    })
  
  ##reactive: getIncidenceRateFilteredOnYScale----
  getIncidenceRateFilteredOnYScale <-
    shiny::reactive({
      incidenceRateFilter <- getIncidenceRateData() %>%
        dplyr::select(.data$incidenceRate) %>%
        dplyr::filter(.data$incidenceRate != "NA", 
                      !is.na(.data$incidenceRate)) %>%
        dplyr::distinct(.data$incidenceRate) %>%
        dplyr::arrange(.data$incidenceRate)
      incidenceRateFilter <-
        incidenceRateFilter[incidenceRateFilter$incidenceRate >= input$YscaleMinAndMax[1] &
                              incidenceRateFilter$incidenceRate <= input$YscaleMinAndMax[2], ] %>%
        dplyr::pull(.data$incidenceRate)
      return(incidenceRateFilter)
    })
  
  ##reactive: getIncidentRatePlotData ----
  getIncidentRatePlotData <- shiny::reactive({
    if (input$tabs != "incidenceRate") {
      return(NULL)
    }
    if (!hasData(input$irStratification)) {
      return(NULL)
    }
    if (!hasData(getIncidenceRateCalendarYears())) {
      return(NULL)
    }
    if (!hasData(input$minPersonYear)) {
      return(NULL)
    }
    if (!hasData(input$minSubjetCount)) {
      return(NULL)
    }
    if (!hasData(incidenceRateAgeFilterValues())) {
      return(NULL)
    }
    
    data <- getIncidenceRateData()
    if (!hasData(data)) {
      return(NULL)
    }
    
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    
    data <- data %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget())
    if (!hasData(data)) {
      return(NULL)
    }
    if (stratifyByGender) {
      data <- data %>%
        dplyr::filter(.data$gender != '')
    } else {
      data <- data %>%
        dplyr::filter(.data$gender == '')
    }
    
    if (stratifyByAge) {
      data <- data %>%
        dplyr::filter(.data$ageGroup != '')
    } else {
      data <- data %>%
        dplyr::filter(.data$ageGroup == '')
    }
    
    if (stratifyByCalendarYear) {
      data <- data %>%
        dplyr::filter(.data$calendarYear != '')
    } else {
      data <- data %>%
        dplyr::filter(.data$calendarYear == '')
    }
    
    if (!is.na(input$minPersonYear) &&
        !is.null(input$minPersonYear)) {
      data <- data %>%
        dplyr::filter(.data$personYears >= input$minPersonYear)
    }
    
    if (!is.na(input$minSubjetCount) &&
        !is.null(input$minSubjetCount)) {
      data <- data %>%
        dplyr::filter(.data$cohortCount >= input$minSubjetCount)
    }
    
    if (!hasData(data)) {
      return(NULL)
    }
    
    if (stratifyByAge &&
        !"All" %in% incidenceRateAgeFilterValues()) {
      data <- data %>%
        dplyr::filter(.data$ageGroup %in% incidenceRateAgeFilterValues())
    }
    
    if (stratifyByGender &&
        !"All" %in% incidenceRateGenderFilterValues()) {
      data <- data %>%
        dplyr::filter(.data$gender %in% incidenceRateGenderFilterValues())
    }
    
    if (stratifyByCalendarYear) {
      if (hasData(getIncidenceRateCalendarYears())) {
        data <- data %>%
          dplyr::filter(.data$calendarYear %in% getIncidenceRateCalendarYears())
      }
    }
    
    if (input$irYscaleFixed) {
      data <- data %>%
        dplyr::filter(.data$incidenceRate %in% getIncidenceRateFilteredOnYScale())
    }
    
    return(data)

  })
  
  
  ##reactive: getIncidentRatePlot ----
  getIncidentRatePlot <- shiny::reactive({
    data <- getIncidentRatePlotData()
    if (!hasData(data)) {
      return(NULL)
    }
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
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
  })
  
  ##output: saveIncidenceRateData----
  output$saveIncidenceRateData <-  downloadHandler(
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
    validate(need(
      length(consolidatedDatabaseIdTarget()) > 0,
      "No data sources chosen"
    ))
    validate(need(length(consolidatedCohortIdTarget()) > 0,
                  "No cohorts chosen"))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Rendering incidence rate plot."),
                 value = 0)
    data <- getIncidentRatePlot()
    validate(need(hasData(data),
                  "No incidence rate data"))
    shiny::withProgress(
      message = paste(
        "Building incidence rate plot data for ",
        length(consolidatedCohortIdTarget()),
        " cohorts and ",
        length(consolidatedDatabaseIdTarget()),
        " databases"
      ), {
        getIncidentRatePlot()
      },
      detail = "Please Wait"
    )
  })
  
  #______________----
  # Time Series -----
  ##reactive: getFixedTimeSeriesTsibble ------
  getFixedTimeSeriesTsibble <- reactive({
    if (any(is.null(input$tabs), !input$tabs == "timeSeries")) {
      return(NULL)
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"),!exists('timeSeries'))) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Getting time series data."),
                 value = 0)
    data <- getResultsFixedTimeSeries(dataSource = dataSource,
                                      cohortId =  consolidatedCohortIdTarget(),
                                      databaseIds = consolidatedDatabaseIdTarget())
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ##reactive: getFixedTimeSeriesTsibbleFiltered----
  getFixedTimeSeriesTsibbleFiltered <- reactive({
    if (any(is.null(input$tabs), !input$tabs == "timeSeries")) {
      return(NULL)
    }
    data <- getFixedTimeSeriesTsibble()
    if (!hasData(data)) {
      return(NULL)
    }
    calendarIntervalFirstLetter <-
      tolower(substr(input$timeSeriesAggregationPeriodSelection, 1, 1))
    
    data <- data[[calendarIntervalFirstLetter]]
    if (!hasData(data)) {
      return(NULL)
    }
    if (!hasData(data)) {
      return(NULL)
    }
    if (calendarIntervalFirstLetter == 'y') {
      data <- data %>%
        dplyr::mutate(periodBeginRaw = as.Date(paste0(
          as.character(.data$periodBegin), '-01-01'
        )))
    } else {
      data <- data %>%
        dplyr::mutate(periodBeginRaw = as.Date(.data$periodBegin))
    }
    
    if (calendarIntervalFirstLetter == 'm') {
      data <- data %>%
        dplyr::mutate(periodEnd = clock::add_months(x = as.Date(.data$periodBeginRaw), n = 1) %>%
                        clock::add_days(n = -1))
    }
    if (calendarIntervalFirstLetter == 'q') {
      data <- data %>%
        dplyr::mutate(periodEnd = clock::add_quarters(x = as.Date(.data$periodBeginRaw), n = 1) %>%
                        clock::add_days(n = -1))
    }
    if (calendarIntervalFirstLetter == 'y') {
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
    
    if (any(
      input$timeSeriesPeriodRangeFilter[1] != 0,
      input$timeSeriesPeriodRangeFilter[2] != 0
    )) {
      data <-
        data[as.character(data$periodBegin) >= input$timeSeriesPeriodRangeFilter[1] &
               as.character(data$periodBegin) <= input$timeSeriesPeriodRangeFilter[2],]
    }
    if (!hasData(data)) {
      return(NULL)
    }
    
    data <- data  %>%
      tsibble::fill_gaps(
        records = 0,
        subjects = 0,
        personDays = 0,
        personDaysIn = 0,
        recordsStart = 0,
        subjectsStart = 0,
        subjectsStartIn = 0,
        recordsEnd = 0,
        subjectsEnd = 0,
        subjectsEndIn = 0
      )
    
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ##reactive: getTimeSeriesDescription----
  getTimeSeriesDescription <- shiny::reactive({
    data <- getFixedTimeSeriesTsibble()
    if (!hasData(data)) {
      return(NULL)
    }
    if (!hasData(input$timeSeriesAggregationPeriodSelection)) {
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
            nrow(timeSeriesDescription) == 0)) {
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
  
  
  ##reactive: getTimeSeriesColumnNameCrosswalk----
  getTimeSeriesColumnNameCrosswalk <- shiny::reactive({
    data <- getFixedTimeSeriesTsibble()
    if (!hasData(data)) {
      return(NULL)
    }
    if (!hasData(input$timeSeriesAggregationPeriodSelection)) {
      return(NULL)
    }
    calendarIntervalFirstLetter <-
      tolower(substr(input$timeSeriesAggregationPeriodSelection, 1, 1))
    data <- data[[calendarIntervalFirstLetter]]
    timeSeriesPlotFilters <- attr(x = data,
                                  which = "timeSeriesColumnNameCrosswalk")
    if (any(is.null(timeSeriesPlotFilters),
            nrow(timeSeriesPlotFilters) == 0)) {
      return(NULL)
    }
    timeSeriesPlotFilters <- timeSeriesPlotFilters %>%
      dplyr::arrange(.data$sequence)
    return(timeSeriesPlotFilters)
  })
  
  
  ##pickerInput: timeSeriesPlotFilters (short)----
  shiny::observe({
    if (!hasData(getTimeSeriesColumnNameCrosswalk())) {
      return(NULL)
    }
    data <- getTimeSeriesColumnNameCrosswalk() %>% 
      dplyr::arrange(.data$sequence) %>% 
      dplyr::pull(.data$longName) %>%
      unique()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "timeSeriesPlotFilters",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = data,
      selected = data[c(2,4,8)]
    )
  })
  
  ##sliderInput: timeSeriesPeriodRangeFilter----
  shiny::observe({
    if (any(is.null(input$tabs), !input$tabs == "timeSeries")) {
      return(NULL)
    }
    calendarIntervalFirstLetter <-
      tolower(substr(input$timeSeriesAggregationPeriodSelection, 1, 1))
    data <- getFixedTimeSeriesTsibble()
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data[[calendarIntervalFirstLetter]]
    if (!hasData(data)) {
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
    if (!hasData(input$timeSeriesTypeFilter)) {
      return(NULL)
    }
    timeSeriesDescription <- getTimeSeriesDescription()
    if (!hasData(timeSeriesDescription)) {
      return(NULL)
    }
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
      dplyr::tibble() %>% 
      dplyr::mutate("periodBegin" = .data$periodBeginRaw) %>% 
      dplyr::select(-.data$periodBeginRaw) %>% 
      dplyr::relocate(.data$periodBegin, .data$periodEnd) %>%
      dplyr::arrange(.data$periodBegin) %>% 
      dplyr::filter(!is.na(.data$periodEnd))
    return(data)
  })
  
  #!!!!!!!!!BUG missing download csv
  
  ##reactive: getFixedTimeSeriesDataForPlot----
  getFixedTimeSeriesDataForPlot <- shiny::reactive({
    if (!hasData(input$timeSeriesTypeFilter)) {
      return(NULL)
    }
    timeSeriesColumnNameCrosswalk <- getTimeSeriesColumnNameCrosswalk()
    if (!hasData(timeSeriesColumnNameCrosswalk)) {
      return(NULL)
    }
    timeSeriesDescription <- getTimeSeriesDescription()
    if (!hasData(timeSeriesDescription)) {
      return(NULL)
    }
    if (!hasData(input$timeSeriesPlotFilters)) {
      return(NULL)
    }
    data <- getFixedTimeSeriesTsibbleFiltered()
    validate(need(all(!is.null(data),
                      nrow(data) > 0),
                  "No timeseries data for the cohort."))
    
    selectedColumns <- getTimeSeriesColumnNameCrosswalk() %>% 
      dplyr::filter(.data$longName %in% c(input$timeSeriesPlotFilters)) %>% 
      dplyr::pull(.data$shortName)
    
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
        titleCaseToCamelCase(selectedColumns)
      ) %>%
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
      )
    return(data)
  })
  
  ##output: fixedTimeSeriesTable----
  output$fixedTimeSeriesTable <- reactable::renderReactable({
    validate(need(hasData(input$timeSeriesTypeFilter),
                  "Please select time series type."
    ))
    
    data <- getFixedTimeSeriesDataForTable()
    validate(need(hasData(data),
      "No timeseries data for the cohort of this series type"
    ))
    keyColumns <- colnames(data)
    getSimpleReactable(data = data,
                       keyColumns = keyColumns,
                       dataColumns = c())
  })

  ##output: fixedTimeSeriesPlot----
  output$fixedTimeSeriesPlot <- plotly::renderPlotly ({
    validate(need(hasData(input$timeSeriesTypeFilter),
      "Please select time series type."
    ))
    data <- getFixedTimeSeriesDataForPlot()
    validate(need(hasData(data),
      "No timeseries data for the cohort of this series type"
    ))
    
    data <- data %>% 
      dplyr::mutate(
        dplyr::across(dplyr::everything(), ~tidyr::replace_na(.x, 0))
      )
    
    longNames <- getTimeSeriesColumnNameCrosswalk() %>% 
      dplyr::filter(.data$longName %in% c(input$timeSeriesPlotFilters)) %>% 
      dplyr::pull(.data$longName) %>% 
      titleCaseToCamelCase()
    shortNames <- getTimeSeriesColumnNameCrosswalk() %>% 
      dplyr::filter(.data$longName %in% c(input$timeSeriesPlotFilters)) %>% 
      dplyr::pull(.data$shortName)
    
    validate(need(titleCaseToCamelCase(shortNames) %in% colnames(data),
      paste0(paste0(shortNames, collapse = ","), " not found in tsibble")
    ))
    renameDf <- getTimeSeriesColumnNameCrosswalk() %>% 
      dplyr::mutate(longName = titleCaseToCamelCase(.data$longName)) %>% 
      dplyr::select(.data$shortName, .data$longName)
    
    
    for (i in (1:nrow(renameDf))) {
      if (renameDf[i,]$shortName %in% colnames(data)) {
        data <- data %>%
          dplyr::rename(!!as.name(renameDf[i,]$longName) := dplyr::all_of(renameDf[i,]$shortName))
      }
    }
    tsibbleDataFromSTLModel <- getStlModelOutputForTsibbleDataValueFields(tsibbleData = data, 
                                                      valueFields = titleCaseToCamelCase(longNames))
    plot <- plotTimeSeriesFromTsibble(
      tsibbleData = tsibbleDataFromSTLModel,
      plotFilters = longNames,
      indexAggregationType = input$timeSeriesAggregationPeriodSelection,
      timeSeriesPeriodRangeFilter = input$timeSeriesPeriodRangeFilter
    )
    return(plot)
  })
  
  #______________----
  #Time Distribution----
  ##output: getTimeDistributionData----
  getTimeDistributionData <- reactive({
    if (any(is.null(input$tabs), !input$tabs == "timeDistribution")) {
      return(NULL)
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"), !exists('timeDistribution'))) {
      return(NULL)
    }
    data <- getResultsTimeDistribution(
      dataSource = dataSource,
      cohortId =  consolidatedCohortIdTarget(),
      databaseId = consolidatedDatabaseIdTarget()
    )
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ##getTimeDistributionTableData----
  getTimeDistributionTableData <- reactive({
    data <- getTimeDistributionData()
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::inner_join(cohort %>%
                          dplyr::select(.data$cohortId,
                                        .data$shortName),
                        by = "cohortId") %>%
      dplyr::arrange(.data$databaseId, .data$cohortId) %>%
      dplyr::select(
        Database = .data$databaseId,
        Cohort = .data$shortName,
        TimeMeasure = .data$covariateName,
        Average = .data$mean,
        SD = .data$sd,
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
    filename = function() {
      getCsvFileNameWithDateTime(string = "timeDistribution")
    },
    content = function(file) {
      downloadCsv(x = getTimeDistributionTableData(),
                  fileName = file)
    }
  )
  
  ##output: timeDistributionTable----
  output$timeDistributionTable <- reactable::renderReactable(expr = {
    data <- getTimeDistributionTableData()
    validate(need(hasData(data),
                  "No data available for selected combination."))
    keyColumns <- colnames(data)
    getSimpleReactable(data = data,
                       keyColumns = keyColumns,
                       dataColumns = c())
  })
  
  ##output: timeDistributionPlot----
  output$timeDistributionPlot <- plotly::renderPlotly(expr = {
    validate(need(
      hasData(consolidatedDatabaseIdTarget()),
      "No data sources chosen"
    ))
    data <- getTimeDistributionData()
    validate(need(hasData(data),
                  "No data for this combination"))
    plot <- plotTimeDistribution(data = data, 
                                 database = database,
                                 colorReference = colorReference,
                                 cohort = cohort)
    return(plot)
  })
  
  #______________----
  # Index event breakdown ------
  ##getIndexEventBreakdownRawTarget----
  getIndexEventBreakdownRawTarget <- shiny::reactive(x = {
    if (any(is.null(input$tabs),
            !input$tabs == "indexEventBreakdown",
            length(consolidatedCohortIdTarget()) == 0)) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"),
            !exists('indexEventBreakdown'))) {
      return(NULL)
    }
    indexEventBreakdown <-
      getResultsIndexEventBreakdown(dataSource = dataSource,
                                    cohortIds = consolidatedCohortIdTarget(),
                                    databaseIds = NULL,
                                    coConceptIds = NULL,
                                    daysRelativeIndex = NULL) #!! in new design, we have multiple daysRelativeIndex
    if (!hasData(indexEventBreakdown)) {
      return(NULL)
    }
    return(indexEventBreakdown)
  })
  
  
  ##getIndexEventBreakdownConceptIdDetails----
  getIndexEventBreakdownConceptIdDetails <- shiny::reactive(x = {
    if (any(
      is.null(input$tabs),
      !input$tabs == "indexEventBreakdown",
      !hasData(getIndexEventBreakdownRawTarget())
    )) {
      return(NULL)
    }
    conceptIdDetails <- getConcept(dataSource = dataSource,
                                   conceptIds = c(getIndexEventBreakdownRawTarget()$conceptId,
                                                  getIndexEventBreakdownRawTarget()$coConceptId) %>% unique())
    if (!hasData(conceptIdDetails)) {
      return(NULL)
    }
    return(conceptIdDetails)
  })
  
  ##getIndexEventBreakdownTargetData----
  getIndexEventBreakdownTargetData <- shiny::reactive(x = {
    if (any(
      is.null(input$tabs),
      !input$tabs == "indexEventBreakdown",
      !hasData(getIndexEventBreakdownRawTarget())
    )) {
      return(NULL)
    }
    indexEventBreakdown <- getIndexEventBreakdownRawTarget() %>%
      dplyr::filter(.data$daysRelativeIndex == 0) %>%
      dplyr::filter(.data$coConceptId == 0)
    
    conceptIdDetails <- getIndexEventBreakdownConceptIdDetails()
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
        by = "conceptId"
      ) %>%
      dplyr::mutate(
        domainId = dplyr::case_when(
          !.data$domainId %in%
            domainOptionsInDomainTable[stringr::str_detect(string = domainOptionsInDomainTable,
                                                           pattern = c('Other'),
                                                           negate = TRUE)] ~ 'Other',
          TRUE ~ .data$domainId
        )
      )
    return(indexEventBreakdown)
  })
                                    
                                    
  ##getIndexEventBreakdownTargetDataFiltered----
  getIndexEventBreakdownTargetDataFiltered <- shiny::reactive(x = {
    data <- getIndexEventBreakdownTargetData()
    if (!hasData(data)) {
      return(NULL)
    }
    if (!hasData(consolidatedConceptSetIdTarget())) {
      return(NULL)
    }
    if (!hasData(input$indexEventDomainNameFilter)) {
      return(NULL)
    }
    if (!hasData(input$indexEventBreakdownTableRadioButton)) {
      return(NULL)
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$domainId %in% input$indexEventDomainNameFilter) %>% 
      dplyr::filter(.data$databaseId %in% c(consolidatedDatabaseIdTarget()))
    
    if (length(input$indexEventBreakdownTableRadioButton) > 0) {
      conceptIdsToFilter <- c()
      if ("Resolved" %in% c(input$indexEventBreakdownTableRadioButton)) {
        if (hasData(getResolvedConceptsTarget())) {
          conceptIdsToFilter <- c(conceptIdsToFilter,
                                  getResolvedConceptsTarget()$conceptId) %>%
            unique()
        }
      }
      if ("Mapped" %in% c(input$indexEventBreakdownTableRadioButton)) {
        if (hasData(getMappedConceptsTarget())) {
          conceptIdsToFilter <- c(conceptIdsToFilter,
                                  getMappedConceptsTarget()$conceptId) %>%
            unique()
        }
      }
      if ("Excluded" %in% c(input$indexEventBreakdownTableRadioButton)) {
        if (hasData(getExcludedConceptsTarget())) {
          conceptIdsToFilter <- c(conceptIdsToFilter,
                                  getExcludedConceptsTarget()$conceptId) %>%
            unique()
        }
      }
      if ("Recommended" %in% c(input$indexEventBreakdownTableRadioButton)) {
        if (hasData(getOrphanConceptsTarget())) {
          conceptIdsToFilter <- c(conceptIdsToFilter,
                                  getOrphanConceptsTarget()$conceptId) %>%
            unique()
        }
      }
      if ("Other" %in% c(input$indexEventBreakdownTableRadioButton)) {
        notPartOfOther <- c(
          getResolvedConceptsTarget()$conceptId,
          getMappedConceptsTarget()$conceptId,
          getExcludedConceptsTarget()$conceptId,
          getOrphanConceptsTarget()$conceptId
        ) %>% unique()
        notPartOfOther <- setdiff(data$conceptId %>% unique(),
                                  notPartOfOther)
        conceptIdsToFilter <- c(conceptIdsToFilter,
                                notPartOfOther) %>%
          unique()
        
      }
      validate(need(
        hasData(conceptIdsToFilter),
        "No index event breakdown data for the chosen combination."
      ))
      data <- data %>%
        dplyr::filter(.data$conceptId %in% c(conceptIdsToFilter))
    }
    data <- data %>% 
      dplyr::rename("persons" = .data$subjectCount,
                    "records" = .data$conceptCount)
    
    data <- data %>% 
      dplyr::arrange(dplyr::desc(abs(dplyr::across(
        dplyr::contains(c("persons", "records"))
      ))))
    return(data)
  })
  
  ##output: indexEventBreakdownTableReactable----
  output$indexEventBreakdownReactTable <-
    reactable::renderReactable(expr = {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0(
          "Get index event breakdown data ",
          " for cohort id: ",
          consolidatedCohortIdTarget()
        ),
        value = 0
      )
      
      data <- getIndexEventBreakdownTargetDataFiltered()
      
      validate(
        need(
          hasData(data),
          "No index event breakdown data for the chosen combination."
        )
      )
      keyColumnFields <-
        c("conceptId", "conceptName","cohortId", "vocabularyId", "standardConcept")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$indexEventBreakdownTableFilter == "Both") {
        dataColumnFields <- dataColumnFields
        countLocation <- 2
      } else if (input$indexEventBreakdownTableFilter == "Persons") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(string = tolower(dataColumnFields),
                                               pattern = tolower("person"))]
        countLocation <- 1
      } else if (input$indexEventBreakdownTableFilter == "Records") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(string = tolower(dataColumnFields),
                                               pattern = tolower("record"))]
        countLocation <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = "Cohort Level",
          fields = input$indexEventBreakdownTableFilter
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
        filteredConceptIds <-
          data %>%
          dplyr::select(.data$conceptId) %>%
          dplyr::distinct() %>%
          dplyr::mutate(sortOrder = dplyr::row_number())
       
        rawDataFiltered <- getIndexEventBreakdownRawTarget() %>%
          dplyr::filter(.data$coConceptId == 0) %>%
          dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>%
          dplyr::inner_join(filteredConceptIds, by = "conceptId") %>%
          dplyr::arrange(.data$sortOrder) %>%
          dplyr::select(.data$databaseId,
                        .data$cohortId,
                        .data$conceptId,
                        .data$sortOrder,
                        .data$daysRelativeIndex,
                        .data$conceptCount,
                        .data$subjectCount)
       
       getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        sparkLineData = rawDataFiltered,
        cohort = cohort, 
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$indexEventBreakdownShowAsPercent, 
        sort = FALSE
      )
        
    })
  
  
  # ##getIndexEventBreakdownPlotData----
  # getIndexEventBreakdownPlotData <- shiny::reactive(x = {
  #   if (!hasData(getIndexEventBreakdownRawTarget())) {
  #     return(NULL)
  #   }
  #   if (!hasData(getIndexEventBreakdownTargetDataFiltered())) {
  #     return(NULL)
  #   }
  #   filteredConceptIds <-
  #     getIndexEventBreakdownTargetDataFiltered() %>% 
  #     dplyr::select(.data$conceptId) %>% 
  #     dplyr::distinct() %>% 
  #     dplyr::mutate(sortOrder = dplyr::row_number())
  #   if (!hasData(filteredConceptIds)) {
  #     return(NULL)
  #   }
  #   data <- getIndexEventBreakdownRawTarget() %>%
  #     dplyr::filter(.data$coConceptId == 0) %>%
  #     dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>% 
  #     dplyr::inner_join(filteredConceptIds, by = "conceptId") %>% 
  #     dplyr::arrange(.data$sortOrder) %>% 
  #     dplyr::select(.data$databaseId,
  #                   .data$cohortId,
  #                   .data$conceptId,
  #                   .data$sortOrder,
  #                   .data$daysRelativeIndex,
  #                   .data$conceptCount,
  #                   .data$subjectCount)
  #   if (!hasData(data)) {
  #     return(NULL)
  #   }
  #   return(data)
  # })
  
  # observe({
  #   data <- getIndexEventBreakdownPlotData()
  #   if (hasData(data)) {
  #     maxValue <- data %>% 
  #       dplyr::pull(.data$rank) %>% 
  #       max() 
  #     
  #     shiny::updateSliderInput(
  #       session = session,
  #       inputId = "indexEventBreakdownConceptIdsRangeFilter",
  #       min = 0,
  #       max = maxValue,
  #       value = c(0, 45)
  #     )
  #   }
  # })
  # 
  # # When Left slider is moved
  # oldMinRangeValue <- reactiveVal(0)
  # oldMaxRangeValue <- reactiveVal(45)
  # observeEvent(eventExpr = input$indexEventBreakdownConceptIdsRangeFilter,{
  #   data <- getIndexEventBreakdownPlotData()
  #   if (hasData(data) &&
  #       hasData(input$indexEventBreakdownConceptIdsRangeFilter[1]) &&
  #       hasData(input$indexEventBreakdownConceptIdsRangeFilter[2])) {
  #     if (input$indexEventBreakdownConceptIdsRangeFilter[1] != oldMinRangeValue()) {
  #       maxValue <- data %>%
  #         dplyr::pull(.data$rank) %>%
  #         max()
  #       if (input$indexEventBreakdownConceptIdsRangeFilter[1] < as.integer(maxValue) - 45) {
  #         minRangeValue <- input$indexEventBreakdownConceptIdsRangeFilter[1]
  #         maxRangeValue <-
  #           input$indexEventBreakdownConceptIdsRangeFilter[1] + 45
  #         oldMinRangeValue(minRangeValue)
  #         oldMaxRangeValue(maxRangeValue)
  #         shiny::updateSliderInput(
  #           session = session,
  #           inputId = "indexEventBreakdownConceptIdsRangeFilter",
  #           value = c(minRangeValue, maxRangeValue)
  #         )
  #       }
  #     } else if (input$indexEventBreakdownConceptIdsRangeFilter[2] != oldMaxRangeValue()) {
  #       if (input$indexEventBreakdownConceptIdsRangeFilter[2] > 45) {
  #         minRangeValue <-
  #           input$indexEventBreakdownConceptIdsRangeFilter[2] - 45
  #         maxRangeValue <-
  #           input$indexEventBreakdownConceptIdsRangeFilter[2]
  #         oldMinRangeValue(minRangeValue)
  #         oldMaxRangeValue(maxRangeValue)
  #         shiny::updateSliderInput(
  #           session = session,
  #           inputId = "indexEventBreakdownConceptIdsRangeFilter",
  #           value = c(minRangeValue, maxRangeValue)
  #         )
  #       }
  #     }
  #   }
  #   
  # })
  
  
  # # When Right slider is moved
  # observe({
  #   if (hasData(input$indexEventBreakdownConceptIdsRangeFilter[2])) {
  #     if (input$indexEventBreakdownConceptIdsRangeFilter[2] < 45) {
  #       minRangeValue <- 0
  #       maxRangeValue <- 45
  #     } else {
  #       minRangeValue <-
  #         input$indexEventBreakdownConceptIdsRangeFilter[2] - 45
  #       maxRangeValue <-
  #         input$indexEventBreakdownConceptIdsRangeFilter[2]
  #     }
  #     shiny::updateSliderInput(
  #       session = session,
  #       inputId = "indexEventBreakdownConceptIdsRangeFilter",
  #       value = c(minRangeValue, maxRangeValue)
  #     )
  #   }
  # })
  
  # ##UpdatePicker : indexEventConceptIdRangeFilter----
  # shiny::observe({
  #   if (input$indexEventBreakbownTabset == "indexEventBreakbownPlotTab") {
  #     data <- getIndexEventBreakdownPlotData()
  #     if (!hasData(data)) {
  #       return(NULL)
  #     }
  #     maxSortOrder <- max(data$sortOrder %>% unique())
  #     if (maxSortOrder == 0) {
  #       return(NULL)
  #     }
  #     lowValue <- seq(from = 1,
  #                     to = (floor(maxSortOrder / 25) * 25) + 1,
  #                     by = 25)
  #     maxValue <- lowValue + 24
  #     conceptIdRange <-  paste0(lowValue, "-", maxValue)
  #     shinyWidgets::updatePickerInput(
  #       session = session,
  #       inputId = "indexEventConceptIdRangeFilter",
  #       choicesOpt = list(style = rep_len("color: black;", 999)),
  #       choices = conceptIdRange,
  #       selected = conceptIdRange[1]
  #     )
  #   }
  # })
  # 
  # ##indexEventBreakdownPlot----
  # output$indexEventBreakdownPlot <-
  #   plotly::renderPlotly({
  #     validate(need(
  #       hasData(getIndexEventBreakdownRawTarget()),
  #       "No index event breakdown data for the chosen combination."
  #     ))
  #     validate(
  #       need(
  #         hasData(getIndexEventBreakdownTargetDataFiltered()),
  #         "No index event breakdown data for the chosen combination. Maybe the concept id in the selected concept id is too restrictive?"
  #       )
  #     )
  #     
  #     if (!hasData(input$indexEventConceptIdRangeFilter)) {
  #       return(NULL)
  #     }
  #     
  #     data <- getIndexEventBreakdownPlotData()
  #     validate(need(
  #       hasData(data),
  #       "No index event breakdown data for the chosen combination."
  #     ))
  #     
  #     # sum of all counts, irrespective of filter
  #     data <- dplyr::bind_rows(
  #       data,
  #       data %>%
  #         dplyr::filter(.data$conceptId > 0) %>% 
  #         dplyr::select(
  #           .data$databaseId,
  #           .data$cohortId,
  #           .data$daysRelativeIndex,
  #           .data$conceptCount
  #         ) %>%
  #         dplyr::group_by(.data$databaseId,
  #                         .data$cohortId,
  #                         .data$daysRelativeIndex) %>%
  #         dplyr::summarise(
  #           "conceptCount" = sum(.data$conceptCount),
  #           "sortOrder" = -2,
  #           .groups = "keep"
  #         ) %>%
  #         dplyr::mutate(conceptId = -2,
  #                       subjectCount = 0)
  #     )
  #     #!!!put a UI drop to select pagination
  #     
  #     conceptidRangeFilter <- stringr::str_split(input$indexEventConceptIdRangeFilter,"-")[[1]]
  #     data <- dplyr::bind_rows(
  #       data %>% 
  #           dplyr::filter(.data$sortOrder >= as.integer(conceptidRangeFilter[1])) %>% 
  #           dplyr::filter(.data$sortOrder < as.integer(conceptidRangeFilter[2])),
  #         data %>% 
  #           dplyr::filter(.data$sortOrder < 0)
  #     )
  #     
  #     # sum of all counts, after filter
  #     data <- dplyr::bind_rows(
  #       data,
  #       data %>%
  #         dplyr::filter(.data$conceptId > 0) %>% 
  #         dplyr::select(
  #           .data$databaseId,
  #           .data$cohortId,
  #           .data$daysRelativeIndex,
  #           .data$conceptCount
  #         ) %>%
  #         dplyr::group_by(.data$databaseId,
  #                         .data$cohortId,
  #                         .data$daysRelativeIndex) %>%
  #         dplyr::summarise(
  #           "conceptCount" = sum(.data$conceptCount),
  #           "sortOrder" = -1,
  #           .groups = "keep"
  #         ) %>%
  #         dplyr::mutate(conceptId = -1,
  #                       subjectCount = 0)
  #     ) %>% 
  #       dplyr::arrange(.data$sortOrder)
  #     
  #     
  #     if (input$indexEventBreakdownTableFilter == "Both") {
  #       dataColumnFields <- c('conceptCount','subjectCount')
  #     } else if (input$indexEventBreakdownTableFilter == "Persons") {
  #       dataColumnFields <- c('subjectCount')
  #     } else if (input$indexEventBreakdownTableFilter == "Records") {
  #       dataColumnFields <- c('conceptCount')
  #     }
  #     
  #     plot <- plotIndexEventBreakdown(data = data,
  #                                     yAxisColumns = dataColumnFields,
  #                                     showAsPercentage = input$indexEventBreakdownShowAsPercent,
  #                                     logTransform = input$indexEventBreakdownShowLogTransform,
  #                                     cohort = cohort,
  #                                     database = database,
  #                                     colorReference = colorReference,
  #                                     conceptIdDetails = getIndexEventBreakdownConceptIdDetails())
  #     return(plot)
  #   })
  
  ##output: conceptSetSynonymsForIndexEventBreakdown----
  output$conceptSetSynonymsForIndexEventBreakdown <- shiny::renderUI(expr = {
    data <- getConceptSetSynonymsHtmlTextString()
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ##output: conceptBrowserTableForIndexEvent----
  output$conceptBrowserTableForIndexEvent <- reactable::renderReactable(expr = {
    if (hasData(consolidateCohortDefinitionActiveSideTarget())) {
      conceptId <- consolidatedConceptIdTarget()
    }
    data <- conceptSetBrowserData()
    validate(need(hasData(data),
                  "No information for selected concept id."))
    
    keyColumnFields <- c(
      "conceptId",
      "conceptName",
      "vocabularyId",
      "domainId",
      "standardConcept",
      "levelsOfSeparation",
      "relationshipId"
    )
    #depending on user selection - what data Column Fields Will Be Presented?
    dataColumnFields <-
      c("persons",
        "records")
    if (input$indexEventBreakdownTableFilter == "Both") {
      dataColumnFields <- dataColumnFields
      countLocation <- 2
    } else if (input$indexEventBreakdownTableFilter == "Persons") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(string = tolower(dataColumnFields),
                                             pattern = tolower("person"))]
      countLocation <- 1
    } else if (input$indexEventBreakdownTableFilter == "Records") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(string = tolower(dataColumnFields),
                                             pattern = tolower("record"))]
      countLocation <- 1
    }
    
    countsForHeader <-
      getCountsForHeaderForUseInDataTable(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds = consolidatedCohortIdTarget(),
        source = "Cohort Level",
        fields = input$indexEventBreakdownTableFilter
      )
    if (!hasData(countsForHeader)) {
      return(NULL)
    }
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    getReactTableWithColumnsGroupedByDatabaseId(
      data = data,
      cohort = cohort,
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = countLocation,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showResultsAsPercent =  input$targetCohortInclusionRulesAsPercent, 
      sort = TRUE
    )
  })
  
  ##output: conceptSetTimeSeriesPlotForIndexEvent----
  output$conceptSetTimeSeriesPlotForIndexEvent <-
    plotly::renderPlotly({
      data <- getMetadataForConceptId()
      validate(need(
        hasData(data),
        "No timeseries data for the cohort of this series type"
      ))
      # working on the plot
      if (input$timeSeriesAggregationPeriodSelectionForIndexEventBreakdown == "Monthly") {
        data <- data$databaseConceptIdYearMonthLevelTsibble %>%
          dplyr::filter(.data$conceptId == consolidatedConceptIdTarget())
      } else {
        data <- data$databaseConceptIdYearLevelTsibble %>%
          dplyr::filter(.data$conceptId == consolidatedConceptIdTarget())
      }
      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
        "No timeseries data for the cohort of this series type"
      ))
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Computing Time series plot for:",
                         activeSelected()$conceptId),
        value = 0
      )
      data <- data %>%
        dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>%
        dplyr::rename("records" = .data$conceptCount,
                      "persons" = .data$subjectCount)
      tsibbleDataFromSTLModel <-
        getStlModelOutputForTsibbleDataValueFields(tsibbleData = data,
                                                   valueFields = c("records", "persons"))
      
      conceptName <- getMetadataForConceptId()$concept %>%
        dplyr::filter(.data$conceptId == activeSelected()$conceptId) %>%
        dplyr::pull(.data$conceptName)
      
      conceptSynonym <-
        getMetadataForConceptId()$conceptSynonym$conceptSynonymName %>%
        unique() %>%
        sort() %>%
        paste0(collapse = ", ")
      
      plot <- plotTimeSeriesForCohortDefinitionFromTsibble(
        stlModeledTsibbleData = tsibbleDataFromSTLModel,
        conceptId = activeSelected()$conceptId,
        conceptName = conceptName,
        conceptSynonym = conceptSynonym
      )
      return(plot)
    })
  
  ##getCoCOnceptForIndexEvent----
  getCoCOnceptForIndexEvent <- shiny::reactive(x = {
    if (!hasData(getIndexEventBreakdownRawTarget())) {
      return(NULL)
    }
    if (!hasData(getIndexEventBreakdownConceptIdDetails())) {
      return(NULL)
    }
    data <- getIndexEventBreakdownRawTarget() %>%
      dplyr::filter(.data$daysRelativeIndex == 0) %>%
      dplyr::filter(.data$conceptId %in% c(activeSelected()$conceptId)) %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>%
      dplyr::inner_join(
        getIndexEventBreakdownConceptIdDetails() %>%
          dplyr::select(
            .data$conceptId,
            .data$conceptName,
            .data$vocabularyId,
            .data$standardConcept
          ),
        by = c("coConceptId" = "conceptId")
      ) %>%
      dplyr::rename("persons" = .data$subjectCount,
                    "records" = .data$conceptCount)
    if (!hasData(data)) {
      return(NULL)
    }
  })
  
  output$coConceptTableForIndexEvent <- reactable::renderReactable(expr = {
    data <- getCoCOnceptForIndexEvent()
    
    validate(need(hasData(data),
                  "No information for selected concept id."))
    
    keyColumnFields <- c("conceptId",
                         "conceptName",
                         "vocabularyId",
                         "standardConcept")
    #depending on user selection - what data Column Fields Will Be Presented?
    dataColumnFields <-
      c("persons",
        "records")
    if (input$indexEventBreakdownTableFilter == "Both") {
      dataColumnFields <- dataColumnFields
      countLocation <- 2
    } else if (input$indexEventBreakdownTableFilter == "Persons") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(string = tolower(dataColumnFields),
                                             pattern = tolower("person"))]
      countLocation <- 1
    } else if (input$indexEventBreakdownTableFilter == "Records") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(string = tolower(dataColumnFields),
                                             pattern = tolower("record"))]
      countLocation <- 1
    }
    
    countsForHeader <-
      getCountsForHeaderForUseInDataTable(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds = consolidatedCohortIdTarget(),
        source = "Cohort Level",
        fields = input$indexEventBreakdownTableFilter
      )
    if (!hasData(countsForHeader)) {
      return(NULL)
    }
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    getReactTableWithColumnsGroupedByDatabaseId(
      data = data,
      cohort = cohort,
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = countLocation,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showResultsAsPercent =  input$indexEventBreakdownShowAsPercent, 
      sort = TRUE
    )
  })
  
  #______________----
  # Visit Context -----
  ##getVisitContextData----
  getVisitContextData <- shiny::reactive(x = {
    if (all(hasData(input$tab),
            input$tab != "visitContext")) {
      return(NULL)
    }
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"), !exists('visitContext'))) {
      return(NULL)
    }
    visitContext <-
      getResultsVisitContext(dataSource = dataSource,
                             cohortId = consolidatedCohortIdTarget(),
                             consolidatedDatabaseIdTarget())
    if (!hasData(visitContext)) {
      return(NULL)
    }
    return(visitContext)
  })
  
  ##getVisitContexDataEnhanced----
  getVisitContexDataEnhanced <- shiny::reactive(x = {
    if (!hasData(cohortCount)) {
      return(NULL)
    }
    if (input$tabs != "visitContext") {
      return(NULL)
    }
    visitContextData <- getVisitContextData()
    if (!hasData(visitContextData)) {
      return(NULL)
    }
    visitContextData <-
      expand.grid(
        visitContext = c("Before", "During visit", "On visit start", "After"),
        visitConceptName = unique(visitContextData$visitConceptName),
        databaseId = unique(visitContextData$databaseId),
        cohortId = unique(visitContextData$cohortId)
      ) %>%
      dplyr::tibble() %>%
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
      ) %>%
      dplyr::mutate(
        visitContext = dplyr::case_when(
          .data$visitContext == "During visit" ~ "During",
          .data$visitContext == "On visit start" ~ "Simultaneous",
          TRUE ~ .data$visitContext
        )
      ) %>% 
      tidyr::replace_na(replace = list(subjects = 0, records = 0))
    
    if (input$visitContextTableFilters == "Before") {
      visitContextData <- visitContextData %>%
        dplyr::filter(.data$visitContext == "Before")
    } else if (input$visitContextTableFilters == "During") {
      visitContextData <- visitContextData %>%
        dplyr::filter(.data$visitContext == "During")
    } else if (input$visitContextTableFilters == "Simultaneous") {
      visitContextData <- visitContextData %>%
        dplyr::filter(.data$visitContext == "Simultaneous")
    } else if (input$visitContextTableFilters == "After") {
      visitContextData <- visitContextData %>%
        dplyr::filter(.data$visitContext == "After")
    }
    if (!hasData(visitContextData)) {
      return(NULL)
    }
    visitContextData <- visitContextData %>% 
      tidyr::pivot_wider(id_cols = c("databaseId", "visitConceptName"), 
                         names_from = "visitContext", 
                         values_from = c("subjects", "records"))
    return(visitContextData)
  })
  
  ##doesVisitContextContainData----
  output$doesVisitContextContainData <- shiny::reactive({
    visitContextData <- getVisitContexDataEnhanced()
    if (!hasData(visitContextData)) {
      return(NULL)
    }
    return(nrow(visitContextData) > 0)
  })
  shiny::outputOptions(output,
                       "doesVisitContextContainData",
                       suspendWhenHidden = FALSE)
  
  ##visitContextTable----
  output$visitContextTable <- reactable::renderReactable(expr = {
    validate(need(
      length(consolidatedDatabaseIdTarget()) > 0,
      "No data sources chosen"
    ))
    validate(need(
      length(consolidatedCohortIdTarget()) > 0,
      "No cohorts chosen"
    ))
    data <- getVisitContexDataEnhanced()
    validate(need(
      hasData(data),
      "No data available for selected combination."
    ))
    dataColumnFields <-
      c(
        "subjects_Before",
        "records_Before",
        "subjects_Simultaneous",
        "records_Simultaneous",
        "subjects_During",
        "records_During",
        "subjects_After",
        "records_After"
      )
    if (input$visitContextTableFilters == "Before") {
      dataColumnFields <- dataColumnFields[stringr::str_detect(string = dataColumnFields, 
                                                               pattern = c("Before"))]
    } else if (input$visitContextTableFilters == "During") {
      dataColumnFields <- dataColumnFields[stringr::str_detect(string = dataColumnFields, 
                                                               pattern = c("During"))]
    } else if (input$visitContextTableFilters == "Simultaneous") {
      dataColumnFields <- dataColumnFields[stringr::str_detect(string = dataColumnFields, 
                                                               pattern = c("Simultaneous"))]
    } else if (input$visitContextTableFilters == "After") {
      dataColumnFields <- dataColumnFields[stringr::str_detect(string = dataColumnFields, 
                                                               pattern = c("After"))]
    }
    keyColumnFields <- "visitConceptName"
    
    if (input$visitContextPersonOrRecords == "Persons") {
      dataColumnFields <- dataColumnFields[stringr::str_detect(string = dataColumnFields,
                                                               pattern = "subjects")]
      data <- data %>% 
        dplyr::select(dplyr::all_of(keyColumnFields), "databaseId", dplyr::all_of(dataColumnFields))
      colnames(data) <- colnames(data) %>% 
        stringr::str_replace_all(pattern = stringr::fixed("subjects_"), replacement = "") 
      dataColumnFields <- dataColumnFields %>% 
        stringr::str_replace_all(pattern = stringr::fixed("subjects_"), replacement = "")
    } else if (input$visitContextPersonOrRecords == "Records") {
      dataColumnFields <- dataColumnFields[stringr::str_detect(string = dataColumnFields,
                                                               pattern = "records")]
      data <- data %>% 
        dplyr::select(dplyr::all_of(keyColumnFields), "databaseId", dplyr::all_of(dataColumnFields))
      colnames(data) <- colnames(data) %>% 
        stringr::str_replace_all(pattern = stringr::fixed("records_"), replacement = "")
      dataColumnFields <- dataColumnFields %>% 
        stringr::str_replace_all(pattern = stringr::fixed("records_"), replacement = "")
    }
    
    countsForHeader <-
      getCountsForHeaderForUseInDataTable(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds = consolidatedCohortIdTarget(),
        source = "Cohort Level",
        fields = input$visitContextPersonOrRecords
      )
    if (!hasData(countsForHeader)) {
      return(NULL)
    }
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    
    table <- getReactTableWithColumnsGroupedByDatabaseId(
      data = data,
      cohort = cohort, 
      database = database,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      countLocation = 1,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showResultsAsPercent =  (input$visitContextValueFilter == "Percentage"), 
      sort = TRUE
    )
    return(table)
  })
  
  
  #______________----
  # Cohort Overlap ------
  ##cohortOverlapData----
  cohortOverlapData <- reactive({
    if (any(
      !hasData(consolidatedDatabaseIdTarget()),
      !hasData(consolidatedCohortIdTarget()),
      !hasData(getComparatorCohortIdFromSelectedCompoundCohortNames())
    )) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"),
            !exists('cohortRelationships'))) {
      return(NULL)
    }
    
    validate(need(
      length(consolidatedCohortIdTarget()) > 0,
      paste0("Please select Target cohort")
    ))
    validate(need(
      length(consolidatedCohortIdTarget()) == 1,
      paste0("Please only select one target cohort")
    ))
    validate(need(
      length(getComparatorCohortIdFromSelectedCompoundCohortNames()) > 0,
      paste0("Please select Comparator Cohort(s)")
    ))
    validate(need(
      consolidatedCohortIdTarget() != getComparatorCohortIdFromSelectedCompoundCohortNames(),
      paste0("Comparator cohort cannot be same as target cohort")
    ))
    targetCohortIds <- consolidatedCohortIdTarget()
    comparatorCohortIds <- setdiff(x = getComparatorCohortIdFromSelectedCompoundCohortNames(),
                                   y = consolidatedCohortIdTarget())
    data <- getResultsCohortOverlap(dataSource = dataSource,
                                    targetCohortIds = targetCohortIds,
                                    databaseIds = consolidatedDatabaseIdTarget(),
                                    comparatorCohortIds = comparatorCohortIds)
    if (!hasData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  #______________----
  # Cohort Overlap filtered ------
  ##cohortOverlapDataFiltered----
  cohortOverlapDataFiltered <- reactive({
    if (!hasData(cohortOverlapData())) {
      return(NULL)
    }
    return(cohortOverlapData())
  })

  ###output: isCohortDefinitionRowSelected----
  output$doesCohortAndComparatorsAreSingleSelected <- reactive({
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!hasData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    return(all(
      length(consolidatedCohortIdTarget()) == 1,
      length(getComparatorCohortIdFromSelectedCompoundCohortNames()) == 1))
  })
  # send output to UI
  shiny::outputOptions(x = output,
                       name = "doesCohortAndComparatorsAreSingleSelected",
                       suspendWhenHidden = FALSE)
  
  ##output: overlapPlot----
  output$overlapPlot <- plotly::renderPlotly(expr = {
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Plotting cohort overlap."),
                 value = 0)
    
    validate(need(
      length(consolidatedCohortIdTarget()) > 0,
      paste0("Please select Target cohort")
    ))
    validate(need(
      length(consolidatedCohortIdTarget()) == 1,
      paste0("Please only select one target cohort")
    ))
    validate(need(
      length(getComparatorCohortIdFromSelectedCompoundCohortNames()) > 0,
      paste0("Please select Comparator Cohort(s)")
    ))
    validate(need(
      consolidatedCohortIdTarget() != getComparatorCohortIdFromSelectedCompoundCohortNames(),
      paste0("Comparator cohort cannot be same as target cohort")
    ))
    data <- cohortOverlapDataFiltered()
    validate(need(
      hasData(data),
      paste0("No cohort overlap data for this combination")
    ))
    
    plot <- plotCohortOverlap(
      data = data,
      shortNameRef = cohort,
      yAxis = input$overlapPlotType
    )
    return(plot)
  })
  
  ##output: overlapPlot - Pie----
  output$overlapPiePlot <- plotly::renderPlotly(expr = {
    validate(need(
      length(consolidatedCohortIdTarget()) > 0,
      paste0("Please select Target Cohort(s)")
    ))
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Plotting cohort overlap."),
                 value = 0)
    data <- cohortOverlapDataFiltered()
    
    validate(need(
      !is.null(data),
      paste0("No cohort overlap data for this combination")
    ))
    validate(need(
      nrow(data) > 0,
      paste0("No cohort overlap data for this combination.")
    ))
    
    plot <- plotCohortOverlapPie(
      data = data,
      shortNameRef = cohort
    )
    return(plot)
  })
  
  ##output: cohortOverlapTable ----
  output$cohortOverlapTable <- reactable::renderReactable(expr = {
    data <- cohortOverlapDataFiltered()
    validate(need(
      !is.null(data),
      paste0("No cohort overlap data for this combination")
    ))
    validate(need(
      nrow(data) > 0,
      paste0("No cohort overlap data for this combination.")
    ))
    keyColumns <- colnames(data)
    getSimpleReactable(data = data,
                       keyColumns = keyColumns,
                       dataColumns = c())
  })
  
  #______________----
  # Characterization/Temporal Characterization ------
  ##Shared----
  ###getMultipleCharacterizationDataTarget----
  getMultipleCharacterizationDataTarget <-
    shiny::reactive(x = {
      if (!hasData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (all(is(dataSource, "environment"), !any(
        exists('covariateValue'),
        exists('temporalCovariateValue')
      ))) {
        return(NULL)
      }
      # if (any(length(consolidatedCohortIdTarget()) != 1)) {
      #   return(NULL)
      # }
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0(
          "Extracting characterization data for target cohort:",
          consolidatedCohortIdTarget()
        ),
        value = 0
      )
      data <- getMultipleCharacterizationResults(
        dataSource = dataSource,
        cohortIds = c(consolidatedCohortIdTarget()) %>% unique()
      )
      if (!hasData(data$analysisRef)) {
        return(NULL)
      }
      if (!hasData(data$covariateValue)) {
        return(NULL)
      }
      return(data)
    })
  
  ###getMultipleCharacterizationDataComparator----
  getMultipleCharacterizationDataComparator <-
    shiny::reactive(x = {
      if (!hasData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      validate(need(consolidatedCohortIdTarget() != consolidatedCohortIdComparator(), 
                    "Target and comparator cohorts are the same. Please change comparator selection."))
      if (all(is(dataSource, "environment"), !any(
        exists('covariateValue'),
        exists('temporalCovariateValue')
      ))) {
        return(NULL)
      }
     
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0(
          "Extracting characterization data for comparator cohort:",
          consolidatedCohortIdComparator()
        ),
        value = 0
      )
      data <- getMultipleCharacterizationResults(
        dataSource = dataSource,
        cohortIds = c(consolidatedCohortIdComparator()) %>% unique()
      )
      if (!hasData(data$analysisRef)) {
        return(NULL)
      }
      if (!hasData(data$covariateValue)) {
        return(NULL)
      }
      return(data)
    })
  
  ##getMultipleCharacterizationData----
  getMultipleCharacterizationData <- shiny::reactive(x = {
    if (!input$tabs == "cohortCharacterization" & 
        !input$tabs == "compareCohortCharacterization") {
      return(NULL)
    }
    dataTarget <- getMultipleCharacterizationDataTarget()
    if (!hasData(dataTarget)) {
      return(NULL)
    }
    dataComparator <- getMultipleCharacterizationDataComparator()
    
    data <- list()
    data$analysisRef <- dplyr::bind_rows(dataTarget$analysisRef,
                                         dataComparator$analysisRef) %>%
      dplyr::distinct()
    data$covariateRef <- dplyr::bind_rows(dataTarget$covariateRef,
                                          dataComparator$covariateRef) %>%
      dplyr::distinct()
    data$covariateValue <-
      dplyr::bind_rows(dataTarget$covariateValue,
                       dataComparator$covariateValue) %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>%
      dplyr::distinct()
    data$covariateValueDist <-
      dplyr::bind_rows(dataTarget$covariateValueDist,
                       dataComparator$covariateValueDist) %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>%
      dplyr::distinct()
    data$concept <- dplyr::bind_rows(dataTarget$concept,
                                     dataComparator$concept) %>%
      dplyr::distinct()
    data$temporalTimeRef <-
      dplyr::bind_rows(dataTarget$temporalTimeRef,
                       dataComparator$temporalTimeRef) %>%
      dplyr::distinct()
    return(data)
  })
  
  ###Update: characterizationDomainNameOptions----
  shiny::observe({
    if (!exists("analysisRef")) {
      return(NULL)
    }
    if (!hasData(analysisRef)) {
      return(NULL)
    }
    if (!hasData(getMultipleCharacterizationData())) {
      return(NULL)
    }
    data <- analysisRef$domainId %>% unique() %>% sort()
    characterizatoinDataAnalysisRef <- getMultipleCharacterizationData()
    if (!hasData(characterizatoinDataAnalysisRef)) {
      return(NULL)
    }
    subset <- intersect(data,
                        characterizatoinDataAnalysisRef$analysisRef$domainId %>% unique()) %>% 
      sort()
    
    
    if (input$tabs == "compareCohortCharacterization") {
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "compareCharacterizationDomainNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = subset
      )
    } else {
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "characterizationDomainNameOptions",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = subset
      )
    }
  })
  
  ####Update: characterizationAnalysisNameOptions----
  shiny::observe({
    if (!exists("analysisRef")) {
      return(NULL)
    }
    if (!hasData(analysisRef)) {
      return(NULL)
    }
    if (!hasData(getMultipleCharacterizationData())) {
      return(NULL)
    }
    data <- analysisRef$analysisName %>% 
      unique() %>% 
      sort()
    characterizatoinDataAnalysisRef <- getMultipleCharacterizationData()
    if (!hasData(characterizatoinDataAnalysisRef)) {
      return(NULL)
    }
    subset <- intersect(data,
                        characterizatoinDataAnalysisRef$analysisRef$analysisName %>% unique()) %>% 
      sort()
    
    if (input$tabs == "compareCohortCharacterization") {
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "compareCharacterizationAnalysisNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = subset
      )
    } else {
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "characterizationAnalysisNameOptions",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = subset,
        selected = subset
      )
    }
  })
  
  ##Characterization----
  ### getCharacterizationDataFiltered ----
  getCharacterizationDataFiltered <- shiny::reactive(x = {
    if (input$tabs != "cohortCharacterization") {
      return(NULL)
    }
    if (!hasData(getMultipleCharacterizationData())) {
      return(NULL)
    }
    if (!hasData(getMultipleCharacterizationData()$covariateRef)) {
      warning("No covariate reference data found")
      return(NULL)
    }
    if (!hasData(getMultipleCharacterizationData()$covariateValue)) {
      return(NULL)
    }
    if (!hasData(getMultipleCharacterizationData()$analysisRef)) {
      warning("No analysis ref data found")
      return(NULL)
    }
    
    if (!hasData(input$characterizationDomainNameOptions)) {
      return(NULL)
    }
    if (!hasData(input$characterizationAnalysisNameOptions)) {
      return(NULL)
    }
    analysisIdToFilter <-
      getMultipleCharacterizationData()$analysisRef %>%
      dplyr::filter(.data$domainId %in% c(input$characterizationDomainNameOptions)) %>%
      dplyr::filter(.data$analysisName %in% c(input$characterizationAnalysisNameOptions)) %>%
      dplyr::pull(.data$analysisId) %>%
      unique()
    if (!hasData(analysisIdToFilter)) {
      return(NULL)
    }
    covariatesTofilter <-
      getMultipleCharacterizationData()$covariateRef %>%
      dplyr::filter(.data$analysisId %in% c(analysisIdToFilter))
    if (!hasData(covariatesTofilter)) {
      return(NULL)
    }
    
    characterizationDataValue <- getMultipleCharacterizationData()$covariateValue %>% 
      dplyr::filter(.data$covariateId %in% c(covariatesTofilter$covariateId %>% unique()))
    
    if (all(
      hasData(input$conceptSetsSelectedTargetCohort),
      hasData(getResolvedConceptsTarget())
    )) {
      covariatesTofilter <- covariatesTofilter  %>%
        dplyr::inner_join(
          conceptSets %>%
            dplyr::filter(
              .data$compoundName %in% c(input$conceptSetsSelectedTargetCohort)
            ) %>%
            dplyr::select(.data$cohortId, .data$conceptSetId) %>%
            dplyr::inner_join(
              getResolvedConceptsTarget() %>%
                dplyr::filter(.data$databaseId %in% c(consolidatedDatabaseIdTarget())) %>%
                dplyr::select(.data$cohortId, .data$conceptSetId, .data$conceptId) %>%
                dplyr::distinct(),
              by = c("cohortId", "conceptSetId")
            ) %>%
            dplyr::select(.data$conceptId) %>%
            dplyr::distinct(),
          by = c("conceptId")
        )
    }
    
    characterizationDataValue <-
      getMultipleCharacterizationData()$covariateValue %>%
      dplyr::filter(.data$databaseId %in% c(consolidatedDatabaseIdTarget()))
    
    #Pretty analysis
    if (input$charType == "Pretty") {
      covariatesTofilter <- covariatesTofilter %>%
        dplyr::filter(.data$analysisId %in% c(prettyAnalysisIds))  #prettyAnalysisIds this is global variable
      characterizationDataValue <-
        characterizationDataValue %>%
        dplyr::inner_join(covariatesTofilter,
                          by = c('covariateId')) %>%
        dplyr::filter(is.na(.data$startDay) |
                        (.data$startDay == -365 &
                           .data$endDay == 0)) %>%
        dplyr::inner_join(getMultipleCharacterizationData()$analysisRef,
                          by = c('analysisId'))
    } else {
      if (!hasData(input$timeIdChoices)) {
        return(NULL)
      }
      characterizationDataValue <-
        characterizationDataValue %>%
        dplyr::inner_join(covariatesTofilter,
                          by = c('covariateId')) %>%
        dplyr::left_join(
          getMultipleCharacterizationData()$concept %>%
            dplyr::select(.data$conceptId,
                          .data$conceptName),
          by = "conceptId"
        ) %>%
        dplyr::mutate(conceptName = dplyr::case_when(
          !is.na(.data$conceptName) ~ .data$conceptName,
          TRUE ~ gsub(".*: ", "", .data$covariateName)
        ))
      
      characterizationDataValueTimeVarying <-
        characterizationDataValue %>%
        dplyr::filter(!is.na(.data$startDay)) %>%
        dplyr::inner_join(
          temporalCovariateChoices %>%
            dplyr::filter(.data$choices %in% c(input$timeIdChoices)),
          by = c("endDay", "startDay")
        ) %>%
        dplyr::inner_join(getMultipleCharacterizationData()$analysisRef,
                          by = c('analysisId'))
      characterizationDataValueNonTimeVarying <-
        characterizationDataValue %>%
        dplyr::filter(is.na(.data$startDay)) %>%
        dplyr::inner_join(getMultipleCharacterizationData()$analysisRef,
                          by = c('analysisId')) %>%
        tidyr::crossing(temporalCovariateChoices %>% 
                          dplyr::select(.data$choices, .data$choicesShort) %>% 
                          dplyr::filter(.data$choices %in% c(input$timeIdChoices)))
      characterizationDataValue <-
        dplyr::bind_rows(
          characterizationDataValueNonTimeVarying,
          characterizationDataValueTimeVarying
        ) %>%
        dplyr::arrange(.data$databaseId,
                       .data$cohortId,
                       .data$covariateId,
                       .data$choices)
    }
    
    if (!hasData(characterizationDataValue)) {
      return(NULL)
    }
    
    if (all(input$charType == "Raw",
            input$charProportionOrContinuous == "Proportion")) {
      characterizationDataValue <- characterizationDataValue %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else
      if (all(input$charType == "Raw",
              input$charProportionOrContinuous == "Continuous")) {
        characterizationDataValue <- characterizationDataValue %>%
          dplyr::filter(.data$isBinary == 'N')
      }
    return(characterizationDataValue)
  })
  
  
  ###getCharacterizationTableDataPretty----
  getCharacterizationTableDataPretty <- shiny::reactive(x = {
    if (input$tabs != "cohortCharacterization") {
      return(NULL)
    }
    data <- getCharacterizationDataFiltered()
    if (!hasData(data)) {
      return(NULL)
    }
    table <- data %>%
      prepareTable1(prettyTable1Specifications = prettyTable1Specifications,
                    cohort = cohort)
    if (!hasData(table)) {
      return(NULL)
    }
    return(table)
  })
  
  
  ###getCharacterizationTableDataRaw----
  getCharacterizationTableDataRaw <- shiny::reactive(x = {
    if (input$tabs != "cohortCharacterization") {
      return(NULL)
    }
    data <- getCharacterizationDataFiltered() %>% 
      dplyr::mutate(mean = round(.data$mean,3),
                    sd = round(.data$sd,3))
    if (!hasData(data)) {
      return(NULL)
    }
    #!!!! if user selects proportion then mean, else count. Also support option for both as 34,342 (33.3%)
    if (input$characterizationColumnFilters == "Mean only") {
      # data <- data %>%
      #   dplyr::select(-.data$mean) %>%
      #   dplyr::rename("mean" = .data$sumValue)
      keyColumnFields <-
        c("cohortId",
          "databaseId",
          "covariateId",
          "conceptName",
          "analysisName",
          "domainId")
      dataColumnFields <- c("mean")
      data <- tidyr::pivot_wider(
        data = data,
        id_cols = dplyr::all_of(keyColumnFields),
        names_from = .data$choicesShort,
        values_from = dplyr::all_of(dataColumnFields)
      )
    } else {
      keyColumnFields <-
        c("cohortId",
          "databaseId",
          "covariateId",
          "conceptName",
          "analysisName",
          "domainId")
      dataColumnFields <- c("mean", "sd")
      
      if (!all(intersect(x = colnames(data) %>% sort(),
                    y = dataColumnFields) %>% sort() == dataColumnFields)) {
        return(NULL)
      }
      data <- data %>%
        tidyr::pivot_longer(
          cols = dplyr::all_of(dataColumnFields),
          names_to = "type",
          values_to = "value"
        ) %>%
        dplyr::mutate(choicesShort = paste0(.data$choicesShort, "_", .data$type)) %>%
        dplyr::select(-.data$type) %>%
        tidyr::pivot_wider(
          id_cols = dplyr::all_of(keyColumnFields),
          names_from = .data$choicesShort,
          values_from = "value"
        )
    }
    return(data)
  })
  
  
  ### Output: characterizationTable ------
  output$characterizationTable <- reactable::renderReactable(expr = {
    if (input$tabs != "cohortCharacterization") {
      return(NULL)
    }
    validate(need(all(
      !is.null(consolidatedCohortIdTarget()),
      length(consolidatedCohortIdTarget()) > 0
    ), "No data for the combination"))
    
    if (input$charType == "Pretty") {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Rendering pretty table for cohort characterization."),
        value = 0
      )
      
      data <- getCharacterizationTableDataPretty()
      validate(need(nrow(data) > 0,
                    "No data available for selected combination."))
      keyColumnFields <- c("characteristic")
      dataColumnFields <- intersect(x = colnames(data),
                                    y = cohort$shortName)
      countLocation <- 2
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = "Cohort Level",
          fields = "Events"
        )
      if (!hasData(countsForHeader)) {
        return(NULL)
      }
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      table <- getReactTableWithColumnsGroupedByDatabaseId(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        sort = FALSE,
        showResultsAsPercent = TRUE,
        showAllRows = TRUE
      )
    } else {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Rendering raw table for cohort characterization."),
        value = 0
      )
      data <- getCharacterizationTableDataRaw()
      validate(need(nrow(data) > 0,
                    "No data available for selected combination."))
      keyColumnFields <-
        c("covariateId",
          "analysisName",
          "domainId",
          "conceptName")
      if (input$characterizationColumnFilters == "Mean only") {
        dataColumnFields <- setdiff(colnames(data),
                                    c("databaseId","cohortId", keyColumnFields))
        showPercent <- TRUE
      } else {
        dataColumnFields <- setdiff(colnames(data),
                                    c("databaseId","cohortId", keyColumnFields))
        showPercent <- FALSE
      }
      
      countLocation <- 1
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = "Cohort Level",
          fields = "Events"
        )
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      data <- data %>% 
        dplyr::mutate(conceptName = stringr::str_wrap(string = .data$conceptName,
                                                          width = 80,
                                                          exdent = 1)) %>% 
        dplyr::mutate(conceptName = stringr::str_replace_all(string = .data$conceptName,
                                                             pattern = stringr::fixed(pattern = "\n"), 
                                                             replacement = "<br/>"))
      table <- getNestedReactTable(
        data = data,
        cohort = cohort,
        database = database,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        sort = TRUE,
        showResultsAsPercent = FALSE
      )
    }
    return(table)
  })
  
  ## Temporal Characterization ------
  ### getTemporalCharacterizationData ------
  # getTemporalCharacterizationData <- shiny::reactive(x = {
  #   if (input$tabs != "temporalCharacterization") {
  #     return(NULL)
  #   }
  #   if (!exists("temporalTimeRef")) {
  #     return(NULL)
  #   }
  #   if (!exists("temporalCovariateChoices")) {
  #     return(NULL)
  #   }
  #   if (!hasData(getMultipleCharacterizationData())) {
  #     return(NULL)
  #   }
  #   if (is.null(getMultipleCharacterizationData()$covariateRef)) {
  #     warning("No covariate ref data found")
  #     return(NULL)
  #   }
  #   if (is.null(getMultipleCharacterizationData()$covariateValue)) {
  #     return(NULL)
  #   }
  #   if (is.null(getMultipleCharacterizationData()$analysisRef)) {
  #     warning("No analysis ref data found")
  #     return(NULL)
  #   }
  #   browser()
  #   data <-
  #     getMultipleCharacterizationData()$covariateValue %>%
  #     dplyr::filter(.data$characterizationSource %in% c('CT', 'FT')) %>%
  #     dplyr::inner_join(
  #       getMultipleCharacterizationData()$covariateRef,
  #       by = c('covariateId', 'characterizationSource')
  #     ) %>%
  #     dplyr::inner_join(
  #       getMultipleCharacterizationData()$analysisRef %>%
  #         dplyr::select(-.data$startDay,-.data$endDay),
  #       by = c('analysisId', 'characterizationSource')
  #     ) %>%
  #     dplyr::distinct() %>%
  #     dplyr::inner_join(temporalCovariateChoices, by = "timeId") %>%
  #     dplyr::arrange(.data$timeId) %>% 
  #     dplyr::mutate(covariateNameShortCovariateId = .data$covariateName)
  #   # %>%
  #   #   dplyr::mutate(covariateNameShort = gsub(".*: ", "", .data$covariateName)) %>%
  #   #   dplyr::mutate(
  #   #     covariateNameShortCovariateId = paste0(.data$covariateNameShort,
  #   #                                            " (",
  #   #                                            .data$covariateId, ")")
  #     # )
  #   return(data)
  # })
  
  ### getTemporalCharacterizationDataFiltered ------
  # getTemporalCharacterizationDataFiltered <-
    # shiny::reactive(x = {
    #   if (input$tabs != "temporalCharacterization") {
    #     return(NULL)
    #   }
    #   if (any(!hasData(input$temporalCharacterizationDomainNameOptions),
    #           input$temporalCharacterizationDomainNameOptions == "")) {
    #     return(NULL)
    #   }
    #   if (any(!hasData(input$temporalCharacterizationAnalysisNameOptions),
    #           input$temporalCharacterizationAnalysisNameOptions == "")) {
    #     return(NULL)
    #   }
    #   data <- getTemporalCharacterizationData()
    #   if (!hasData(data)) {
    #     return(NULL)
    #   }
    #   
    #   if (all(
    #     hasData(input$conceptSetsSelectedTargetCohort),
    #     hasData(getResolvedConceptsAllData())
    #   )) {
    #     data <- data  %>%
    #       dplyr::inner_join(
    #         conceptSets %>% 
    #           dplyr::filter(.data$compoundName %in% c(input$conceptSetsSelectedTargetCohort)) %>% 
    #           dplyr::select(.data$cohortId, .data$conceptSetId) %>% 
    #           dplyr::inner_join(getResolvedConceptsAllData() %>% 
    #                               dplyr::filter(.data$databaseId %in% c(consolidatedDatabaseIdTarget())) %>% 
    #                               dplyr::select(.data$cohortId, .data$conceptSetId, .data$conceptId) %>% 
    #                               dplyr::distinct(),
    #                             by = c("cohortId", "conceptSetId")) %>%
    #           dplyr::select(.data$conceptId) %>%
    #           dplyr::distinct(),
    #         by = c("conceptId")
    #       )
    #   }
    #   browser()
    #   data <- data %>%
    #       dplyr::filter(.data$analysisName %in% input$temporalCharacterizationAnalysisNameOptions) %>%
    #       dplyr::filter(.data$domainId %in% input$temporalCharacterizationDomainNameOptions) %>%
    #       dplyr::filter(.data$timeId %in% getTimeIdsFromSelectedTemporalCovariateChoices())
    #   
    #   if (input$temporalCharacterizationOutputTypeProportionOrContinuous == "Proportion") {
    #     data <- data %>%
    #       dplyr::filter(.data$isBinary == 'Y')
    #   } else if (input$temporalCharacterizationOutputTypeProportionOrContinuous == "Continuous") {
    #       data <- data %>%
    #         dplyr::filter(.data$isBinary == 'N')
    #     }
    #   if (!hasData(data)) {
    #     return(NULL)
    #   }
    #   return(data)
    # })
  
  
  ### getTemporalCharacterizationTableData ------
  # getTemporalCharacterizationTableData <- shiny::reactive({
  #   if (input$tabs != "temporalCharacterization") {
  #     return(NULL)
  #   }
  #   if (any(
  #     !exists('temporalCovariateChoices'),
  #     is.null(temporalCovariateChoices),
  #     nrow(temporalCovariateChoices) == 0
  #   )) {
  #     return(NULL)
  #   }
  #   data <- getTemporalCharacterizationDataFiltered()
  #   if (!hasData(data)) {
  #     return(NULL)
  #   }
  #   data <- data %>%
  #     dplyr::select(-.data$cohortId, -.data$databaseId)
  #   
  #   data <- data %>%
  #     tidyr::pivot_wider(
  #       id_cols = c("covariateId", "covariateName"),
  #       names_from = "choices",
  #       values_from = "mean" ,
  #       names_sep = "_"
  #     ) %>%
  #     dplyr::relocate(.data$covariateId,
  #                     .data$covariateName) %>%
  #     dplyr::arrange(dplyr::desc(dplyr::across(dplyr::starts_with('Start'))))
  #   
  #   if (!hasData(data)) {
  #     return(NULL)
  #   }
  #   return(data)
  # })
  
  # ### output: temporalCharacterizationTable----
  # output$temporalCharacterizationTable <-
  #   DT::renderDataTable(expr = {
  #     if (input$tabs != "temporalCharacterization") {
  #       return(NULL)
  #     }
  #     progress <- shiny::Progress$new()
  #     on.exit(progress$close())
  #     progress$set(
  #       message = paste0("Rendering raw table for temporal cohort characterization."),
  #       value = 0
  #     )
  #     data <- getTemporalCharacterizationTableData()
  #     validate(need(nrow(data) > 0,
  #                   "No data available for selected combination."))
  #     browser()
  #     temporalCovariateChoicesSelected <-
  #       temporalCovariateChoices %>%
  #       dplyr::filter(.data$timeId %in% c(getTimeIdsFromSelectedTemporalCovariateChoices())) %>%
  #       dplyr::arrange(.data$timeId)
  #     
  #     options = list(
  #       pageLength = 1000,
  #       lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
  #       searching = TRUE,
  #       searchHighlight = TRUE,
  #       scrollX = TRUE,
  #       scrollY = "60vh",
  #       lengthChange = TRUE,
  #       ordering = TRUE,
  #       paging = TRUE,
  #       columnDefs = list(truncateStringDef(1, 70),
  #                         minCellPercentDef(1 + 1:(
  #                           length(temporalCovariateChoicesSelected$choices)
  #                         )))
  #     )
  #     
  #     table <- DT::datatable(
  #       data,
  #       options = options,
  #       rownames = FALSE,
  #       colnames = colnames(data) %>%
  #         camelCaseToTitleCase(),
  #       escape = FALSE,
  #       filter = "top",
  #       class = "stripe nowrap compact"
  #     )
  #     
  #     table <- DT::formatStyle(
  #       table = table,
  #       columns = (2 + (
  #         1:length(temporalCovariateChoicesSelected$choices)
  #       )),
  #       #0 index
  #       background = DT::styleColorBar(c(0, 1), "lightblue"),
  #       backgroundSize = "98% 88%",
  #       backgroundRepeat = "no-repeat",
  #       backgroundPosition = "center"
  #     )
  #     return(table)
  #   }, server = TRUE)
  # 
  
#   ### parseMultipleCompareCharacterizationData ------
  parseMultipleCompareCharacterizationData <- shiny::reactive({
    if (!input$tabs %in% c("compareCohortCharacterization")) {
      return(NULL)
    }
    if (!hasData(getMultipleCharacterizationData())) {
      return(NULL)
    }
    if (!hasData(getMultipleCharacterizationData()$covariateRef)) {
      warning("No covariate reference data found")
      return(NULL)
    }
    if (!hasData(getMultipleCharacterizationData()$covariateValue)) {
      return(NULL)
    }
    if (!hasData(getMultipleCharacterizationData()$analysisRef)) {
      warning("No analysis reference data found")
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Parsing extracted characterization data",
                 value = 0)
    data <- getMultipleCharacterizationData()$covariateValue
    
    data <- data %>%
      dplyr::inner_join(
        getMultipleCharacterizationData()$covariateRef,
        by = c("covariateId")
      ) %>%
      dplyr::inner_join(
        getMultipleCharacterizationData()$analysisRef ,
        by = c("analysisId")
      )

    covs1 <- data %>%
      dplyr::filter(.data$cohortId == consolidatedCohortIdTarget()) %>%
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
    if (!hasData(covs1)) {
      return(NULL)
    }

    covs2 <- data %>%
      dplyr::filter(.data$cohortId == consolidatedCohortIdComparator()) %>%
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
    if (!hasData(covs2)) {
      return(NULL)
    }

    if (input$tabs %in% c("compareCohortCharacterization")) {
      balance <- compareCohortCharacteristics(covs1, covs2)
    }
    if (!hasData(balance)) {
      return(NULL)
    }
    balance <- balance %>%
      dplyr::mutate(absStdDiff = abs(.data$stdDiff))
    return(balance)
    
  })
#   
#   ###parseMultipleCompareCharacterizationDataFiltered----
  parseMultipleCompareCharacterizationDataFiltered <-  shiny::reactive({
    if (!input$tabs %in% c("compareCohortCharacterization")) {
      return(NULL)
    }
    if (input$tabs %in% c("compareCohortCharacterization")) {
      if (any(
        !hasData(input$compareCharacterizationDomainNameFilter),
        input$compareCharacterizationDomainNameFilter == ""
      )) {
        return(NULL)
      }
      if (any(
        !hasData(input$compareCharacterizationAnalysisNameFilter),
        input$compareCharacterizationAnalysisNameFilter == ""
      )) {
        return(NULL)
      }
    }
    
    data <- parseMultipleCompareCharacterizationData()
    if (!hasData(data)) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Filtering characterization data",
                 value = 0)
    
    analysisIdToFilter <-
      getMultipleCharacterizationData()$analysisRef %>%
      dplyr::filter(.data$domainId %in% c(input$compareCharacterizationDomainNameFilter)) %>%
      dplyr::filter(.data$analysisName %in% c(input$compareCharacterizationAnalysisNameFilter)) %>%
      dplyr::pull(.data$analysisId) %>%
      unique()
    if (!hasData(analysisIdToFilter)) {
      return(NULL)
    }
    covariatesTofilter <-
      getMultipleCharacterizationData()$covariateRef %>%
      dplyr::filter(.data$analysisId %in% c(analysisIdToFilter))
    if (!hasData(covariatesTofilter)) {
      return(NULL)
    }
    
    characterizationDataValue <- getMultipleCharacterizationData()$covariateValue %>% 
      dplyr::filter(.data$covariateId %in% c(covariatesTofilter$covariateId %>% unique()))
    
    if (all(
      hasData(input$conceptSetsSelectedTargetCohort),
      hasData(getResolvedConceptsTarget())
    )) {
      covariatesTofilter <- covariatesTofilter  %>%
        dplyr::inner_join(
          conceptSets %>%
            dplyr::filter(
              .data$compoundName %in% c(input$conceptSetsSelectedTargetCohort)
            ) %>%
            dplyr::select(.data$cohortId, .data$conceptSetId) %>%
            dplyr::inner_join(
              getResolvedConceptsTarget() %>%
                dplyr::filter(.data$databaseId %in% c(consolidatedDatabaseIdTarget())) %>%
                dplyr::select(.data$cohortId, .data$conceptSetId, .data$conceptId) %>%
                dplyr::distinct(),
              by = c("cohortId", "conceptSetId")
            ) %>%
            dplyr::select(.data$conceptId) %>%
            dplyr::distinct(),
          by = c("conceptId")
        )
    }
    
    characterizationDataValue <-
      getMultipleCharacterizationData()$covariateValue %>%
      dplyr::filter(.data$databaseId %in% c(consolidatedDatabaseIdTarget()))
    
    #Pretty analysis
    if (input$characterizationCompareMethod == "Pretty") {
      covariatesTofilter <- covariatesTofilter %>%
        dplyr::filter(.data$analysisId %in% c(prettyAnalysisIds))  #prettyAnalysisIds this is global variable
      characterizationDataValue <-
        characterizationDataValue %>%
        dplyr::inner_join(covariatesTofilter,
                          by = c('covariateId')) %>%
        dplyr::filter(is.na(.data$startDay) |
                        (.data$startDay == -365 &
                           .data$endDay == 0)) %>%
        dplyr::inner_join(getMultipleCharacterizationData()$analysisRef,
                          by = c('analysisId'))
    } else {
      if (!hasData(input$timeIdChoices)) {
        return(NULL)
      }
      characterizationDataValue <-
        characterizationDataValue %>%
        dplyr::inner_join(covariatesTofilter,
                          by = c('covariateId')) %>%
        dplyr::left_join(
          getMultipleCharacterizationData()$concept %>%
            dplyr::select(.data$conceptId,
                          .data$conceptName),
          by = "conceptId"
        ) %>%
        dplyr::mutate(conceptName = dplyr::case_when(
          !is.na(.data$conceptName) ~ .data$conceptName,
          TRUE ~ gsub(".*: ", "", .data$covariateName)
        ))
      
      characterizationDataValueTimeVarying <-
        characterizationDataValue %>%
        dplyr::filter(!is.na(.data$startDay)) %>%
        dplyr::inner_join(
          temporalCovariateChoices %>%
            dplyr::filter(.data$choices %in% c(input$timeIdChoices)),
          by = c("endDay", "startDay")
        ) %>%
        dplyr::inner_join(getMultipleCharacterizationData()$analysisRef,
                          by = c('analysisId'))
      characterizationDataValueNonTimeVarying <-
        characterizationDataValue %>%
        dplyr::filter(is.na(.data$startDay)) %>%
        dplyr::inner_join(getMultipleCharacterizationData()$analysisRef,
                          by = c('analysisId')) %>%
        tidyr::crossing(temporalCovariateChoices %>% 
                          dplyr::select(.data$choices, .data$choicesShort) %>% 
                          dplyr::filter(.data$choices %in% c(input$timeIdChoices)))
      characterizationDataValue <-
        dplyr::bind_rows(
          characterizationDataValueNonTimeVarying,
          characterizationDataValueTimeVarying
        ) %>%
        dplyr::arrange(.data$databaseId,
                       .data$cohortId,
                       .data$covariateId,
                       .data$choices)
    }
    
    
    if (input$compareCharacterizationProportionOrContinous == "Proportion") {
      characterizationDataValue <- characterizationDataValue %>%
        dplyr::filter(.data$isBinary == 'Y')
    } else
      if (input$compareCharacterizationProportionOrContinous == "Continuous") {
        characterizationDataValue <- characterizationDataValue %>%
          dplyr::filter(.data$isBinary == 'N')
      }
    
    if (!hasData(characterizationDataValue)) {
      return(NULL)
    }

    if (input$tabs %in% c("compareCohortCharacterization")) {
      characterizationDataValue <- characterizationDataValue %>%
        dplyr::filter(.data$domainId  %in% input$compareCharacterizationDomainNameFilter) %>%
        dplyr::filter(.data$analysisName  %in% input$compareCharacterizationAnalysisNameFilter)
    }
    return(data)
  })

  ## Compare Characterization ----
  ###getCompareCharacterizationTablePretty----
  getCompareCharacterizationTablePretty <- shiny::reactive({
    if (input$tabs != "compareCohortCharacterization") {
      return(NULL)
    }
    data <- parseMultipleCompareCharacterizationDataFiltered()
    if (!hasData(data)) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = "Rendering pretty table",
                 value = 0)
    data <- prepareTable1Comp(balance = data)
    if (!hasData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::arrange(.data$sortOrder) %>%
      dplyr::select(-.data$sortOrder) %>%
      dplyr::select(-.data$cohortId1, -.data$cohortId2)

    return(data)
  })

  ###getCompareCharacterizationTableRaw----
  getCompareCharacterizationTableRaw <- shiny::reactive({
    if (input$tabs != "compareCohortCharacterization") {
      return(NULL)
    }

    data <- parseMultipleCompareCharacterizationDataFiltered()
    if (!hasData(data)) {
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
      ) %>%
      dplyr::select(
        .data$covariateId,
        .data$covariateName,
        .data$meanTarget,
        .data$sdTarget,
        .data$meanComparator,
        .data$sdComparator,
        .data$StdDiff,
        .data$databaseId
      )
  })
#   
#   ###output: compareCharacterizationTable----
  output$compareCharacterizationTable <-
    DT::renderDataTable(expr = {
      if (input$tabs != "compareCohortCharacterization") {
        return(NULL)
      }
      browser()
      
      if (input$characterizationCompareMethod == "Pretty table") {
        progress <- shiny::Progress$new()
        on.exit(progress$close())
        progress$set(
          message = paste0("Rendering pretty table for compare characterization."),
          value = 0
        )

        data <- getCompareCharacterizationTablePretty()
        validate(need(nrow(data) > 0,
                      "No data available for selected combination."))

        
      } else {
        balance <-  getCompareCharacterizationTableRaw()
        progress <- shiny::Progress$new()
        on.exit(progress$close())
        progress$set(
          message = paste0("Rendering raw table for compare characterization."),
          value = 0
        )

    }
        
    }, server = TRUE)

  
  ###compareCharacterizationPlot----
  output$compareCharacterizationPlot <-
    plotly::renderPlotly(expr = {
      if (input$tabs != "compareCohortCharacterization") {
        return(NULL)
      }
      if (!hasData(parseMultipleCompareCharacterizationDataFiltered())) {
        return(NULL)
      }
      data <- parseMultipleCompareCharacterizationDataFiltered()
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Rendering plot for compare characterization."),
        value = 0
      )
      plot <-
        plotCompareCohortCharacterization(balance = data, isTemporal = FALSE)
      return(plot)
    })
  # 
  # ###getCompareTemporalCharcterizationTableData----
  # getCompareTemporalCharcterizationTableData <-
  #   shiny::reactive({
  #     if (input$tabs != "compareTemporalCharacterization") {
  #       return(NULL)
  #     }
  #     data <- parseMultipleCompareCharacterizationDataFiltered()
  #     if (!hasData(data)) {
  #       return(NULL)
  #     }
  # 
  #     if (input$temporalCharacterizationTypeColumnFilter == "Mean and Standard Deviation") {
  #       table <- data %>%
  #         dplyr::arrange(desc(abs(.data$stdDiff)))
  # 
  #         table <- table %>%
  #           dplyr::arrange(.data$choices) %>%
  #           tidyr::pivot_longer(
  #             cols = c(
  #               "mean1",
  #               "sd1",
  #               "mean2",
  #               "sd2"
  #             ),
  #             names_to = "type",
  #             values_to = "values"
  #           )
  # 
  #     } else {
  #       # only Mean
  #       table <- data %>%
  #         dplyr::arrange(desc(abs(.data$stdDiff)))
  # 
  #         table <- data %>%
  #           tidyr::pivot_longer(
  #             cols = c("mean1",
  #                      "mean2"),
  #             names_to = "type",
  #             values_to = "values"
  #           )
  # 
  # 
  #     }
  #     return(table)
  #   })
  # # # 
  # # ### Output: compareTemporalCharacterizationTable ------
  # output$compareTemporalCharacterizationTable <-
  #   DT::renderDataTable(expr = {
  #     if (input$tabs != "compareTemporalCharacterization") {
  #       return(NULL)
  #     }
  # 
  #     progress <- shiny::Progress$new()
  #     on.exit(progress$close())
  #     progress$set(
  #       message = paste0("Computing compare temporal characterization."),
  #       value = 0
  #     )
  #     data <- getCompareTemporalCharcterizationTableData()
  #     validate(need(
  #       all(!is.null(data),
  #           nrow(data) > 0),
  #       "No data available for selected combination."
  #     ))
  # 
  #     targetCohortIdValue <- data %>%
  #       dplyr::filter(!is.na(.data$cohortId1)) %>%
  #       dplyr::pull(.data$cohortId1) %>%
  #       unique()
  #     comparatorcohortIdValue <- data %>%
  #       dplyr::filter(!is.na(.data$cohortId2)) %>%
  #       dplyr::pull(.data$cohortId2) %>%
  #       unique()
  #     databaseIdForCohortCharacterization <-
  #       data$databaseId %>%
  #       unique()
  # 
  #     targetCohortShortName <- cohort %>%
  #       dplyr::filter(.data$cohortId == !!targetCohortIdValue) %>%
  #       dplyr::select(.data$shortName) %>%
  #       dplyr::pull()
  #     comparatorCohortShortName <- cohort %>%
  #       dplyr::filter(.data$cohortId == !!comparatorcohortIdValue) %>%
  #       dplyr::select(.data$shortName) %>%
  #       dplyr::pull()
  # 
  #     targetCohortSubjects <- cohortCount %>%
  #       dplyr::filter(.data$databaseId == databaseIdForCohortCharacterization) %>%
  #       dplyr::filter(.data$cohortId == !!targetCohortIdValue) %>%
  #       dplyr::pull(.data$cohortSubjects)
  #     comparatorCohortSubjects <- cohortCount %>%
  #       dplyr::filter(.data$databaseId == databaseIdForCohortCharacterization) %>%
  #       dplyr::filter(.data$cohortId == !!comparatorcohortIdValue) %>%
  #       dplyr::pull(.data$cohortSubjects)
  # 
  #     targetCohortHeader <- paste0(
  #       targetCohortShortName,
  #       " (n = ",
  #       scales::comma(targetCohortSubjects,
  #                     accuracy = 1),
  #       ")"
  #     )
  #     comparatorCohortHeader <- paste0(
  #       comparatorCohortShortName,
  #       " (n = ",
  #       scales::comma(comparatorCohortSubjects,
  #                     accuracy = 1),
  #       ")"
  #     )
  #     browser()
  #     temporalCovariateChoicesSelected <-
  #       temporalCovariateChoices %>%
  #       dplyr::filter(.data$timeId %in% c(getTimeIdsFromSelectedTemporalCovariateChoices())) %>%
  #       dplyr::arrange(.data$timeId) %>%
  #       dplyr::pull(.data$choices)
  # 
  #     if (input$temporalCharacterizationTypeColumnFilter == "Mean and Standard Deviation") {
  #       table <- data %>%
  #         dplyr::arrange(desc(abs(.data$stdDiff)))
  # 
  # 
  #         table <- table %>%
  #           dplyr::arrange(.data$choices) %>%
  #           dplyr::mutate(names = paste0(.data$databaseId, " ", .data$choices, " ", .data$type)) %>%
  #           dplyr::arrange(.data$databaseId,
  #                          .data$startDay,
  #                          .data$endDay,
  #                          .data$type) %>%
  #           tidyr::pivot_wider(
  #             id_cols = c("covariateName", "covariateId"),
  #             names_from = "names",
  #             values_from = c("values"),
  #             values_fill = 0
  #           ) %>%
  #           dplyr::select(-.data$covariateId)
  # 
  #         columnDefs <- list(truncateStringDef(0, 80),
  #                            minCellRealDef(1:(
  #                              length(temporalCovariateChoicesSelected) * 4
  #                            ),
  #                            digits = 2))
  #         colorBarColumns <-
  #           1 + 1:(length(temporalCovariateChoicesSelected) * 4)
  #         colspan <- 4
  #         containerColumns <-
  #           c(
  #             paste0("Mean ", targetCohortShortName),
  #             paste0("SD ", targetCohortShortName),
  #             paste0("Mean ", comparatorCohortShortName),
  #             paste0("SD ", comparatorCohortShortName)
  #           )
  # 
  #     } else {
  #       # only Mean
  #       table <- data %>%
  #         dplyr::arrange(desc(abs(.data$stdDiff)))
  # 
  #         table <- table %>%
  #           dplyr::mutate(names = paste0(.data$databaseId, " ", .data$choices, " ", .data$type)) %>%
  #           dplyr::arrange(.data$startDay, .data$endDay) %>%
  #           tidyr::pivot_wider(
  #             id_cols = c("covariateName", "covariateId"),
  #             names_from = "names",
  #             values_from = "values",
  #             values_fill = 0
  #           ) %>%
  #           dplyr::select(-.data$covariateId)
  # 
  #         containerColumns <-
  #           c(targetCohortShortName, comparatorCohortShortName)
  #         columnDefs <- list(truncateStringDef(0, 80),
  #                            minCellRealDef(1:(
  #                              length(temporalCovariateChoicesSelected) * 2
  #                            ),
  #                            digits = 2))
  #         colorBarColumns <-
  #           1 + 1:(length(temporalCovariateChoicesSelected) * 2)
  #         colspan <- 2
  # 
  #     }
  # 
  #     sketch <- htmltools::withTags(table(class = "display",
  #                                         thead(tr(
  #                                           th(rowspan = 2, "Covariate Name"),
  #                                           lapply(
  #                                             temporalCovariateChoicesSelected,
  #                                             th,
  #                                             colspan = colspan,
  #                                             class = "dt-center",
  #                                             style = "border-right:1px solid silver;border-bottom:1px solid silver"
  #                                           )
  #                                         ),
  #                                         tr(
  #                                           lapply(rep(
  #                                             containerColumns,
  #                                             length(temporalCovariateChoicesSelected)
  #                                           ),
  #                                           th,
  #                                           style = "border-right:1px solid grey")
  #                                         ))))
  # 
  #     options = list(
  #       pageLength = 100,
  #       lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
  #       searching = TRUE,
  #       searchHighlight = TRUE,
  #       scrollX = TRUE,
  #       scrollY = "60vh",
  #       lengthChange = TRUE,
  #       ordering = TRUE,
  #       paging = TRUE,
  #       columnDefs = columnDefs
  #     )
  # 
  #     table <- DT::datatable(
  #       table,
  #       options = options,
  #       rownames = FALSE,
  #       container = sketch,
  #       colnames = colnames(table) %>%
  #         camelCaseToTitleCase(),
  #       escape = FALSE,
  #       filter = "top",
  #       class = "stripe nowrap compact"
  #     )
  #     table <- DT::formatStyle(
  #       table = table,
  #       columns = colorBarColumns,
  #       background = DT::styleColorBar(c(0, 1), "lightblue"),
  #       backgroundSize = "98% 88%",
  #       backgroundRepeat = "no-repeat",
  #       backgroundPosition = "center"
  #     )
  # 
  #     return(table)
  #   }, server = TRUE)
  
  ###compareTemporalCharacterizationPlot----
  output$compareTemporalCharacterizationPlot2D <-
    plotly::renderPlotly(expr = {
      if (input$tabs != "compareTemporalCharacterization") {
        return(NULL)
      }
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Rendering plot for compare temporal characterization."),
        value = 0
      )
      data <- parseMultipleCompareCharacterizationDataFiltered()
      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
        paste0("No data for the selected combination.")
      ))
      plot <-
        plotCompareCohortCharacterization(balance = data,
                                          isTemporal = TRUE)
      return(plot)
    })
  
  ###compareTemporalCharacterizationPlot3D----
  # output$compareTemporalCharacterizationPlot3D <-
  #   plotly::renderPlotly(expr = {
  #     if (input$tabs != "compareTemporalCharacterization") {
  #       return(NULL)
  #     }
  #     progress <- shiny::Progress$new()
  #     on.exit(progress$close())
  #     progress$set(
  #       message = paste0("Rendering plot for compare temporal characterization."),
  #       value = 0
  #     )
  #     data <- parseMultipleCompareCharacterizationDataFiltered()
  #     validate(need(
  #       all(!is.null(data),
  #           nrow(data) > 0),
  #       paste0("No data for the selected combination.")
  #     ))
  #     plot <-
  #       plotTemporalCompareStandardizedDifference3D(
  #         balance = data,
  #         shortNameRef = cohort)
  #     return(plot)
  #   })
  
  # observeEvent (
  #   eventExpr = list(input$timeIdChoices_open,
  #                    input$tabs),
  #   handlerExpr = {
  #     if (!hasData(input$timeIdChoices_open)) {
  #       return(NULL)
  #     }
  #     browser()
  #     if (any(isFALSE(input$timeIdChoices_open)||!is.null(input$tabs))) {
  #       for (i in 1:nrow(temporalCovariateChoices)) {
  #         if (!(
  #           input$timeIdChoices[i] %in% c(
  #             "Start -365 to end -31",
  #             "Start -30 to end -1",
  #             "Start 0 to end 0",
  #             "Start 1 to end 30",
  #             "Start 31 to end 365"
  #           )
  #         ))
  #         {
  #           updateTabsetPanel(session,
  #                             "comparatorTemporalCharPlotTabSetPanel",
  #                             selected = "compareTemporalCharacterization3DPlotPanel")
  #           break
  #         }
  #       }
  #     }
  #   }
  # )
  
  #______________----
  #Metadata----
  #getMetadataInformation----
  getMetadataInformation <- shiny::reactive(x = {
    data <- getExecutionMetadata(dataSource = dataSource)
    if (!hasData(data)) {
      return(data)
    }
    
    data <- data %>%
      dplyr::filter(.data$databaseId == consolidatedDatabaseIdTarget())
    
    return(data)
  })
  
  #getMetadataInformationFilteredToDatabaseId----
  getMetadataInformationFilteredToDatabaseId <- shiny::reactive(x = {
    data <- getMetadataInformation() 
    if (!hasData(data)) {
      return(NULL)
    }
    
    if (!'vocabularyVersionCdm' %in% colnames(data)) {
      data$vocabularyVersionCdm <- "NA"
    }
    if (!'vocabularyVersion' %in% colnames(database)) {
      data$vocabularyVersion <- "NA"
    }
    colNamesRequired <- c("databaseId",
                          "startTime",
                          "cdmReleaseDate",
                          "cdmSourceName",
                          "datasourceName",
                          "datasourceDescription",
                          "sourceDescription",
                          "vocabularyVersionCdm",
                          "vocabularyVersion",
                          "sourceReleaseDate",
                          "observationPeriodMaxDate",
                          "observationPeriodMinDate",
                          "personsInDatasource",
                          "recordsInDatasource",
                          "personDaysInDatasource")
    colNamesObserved <- colnames(data)
    presentInBoth <- intersect(colNamesRequired, colNamesObserved)
    data <- data %>%
      dplyr::select(dplyr::all_of(presentInBoth))
    return(data)
  })
  
  ##output: databaseInformationTable----
  output$databaseInformationTable <- reactable::renderReactable(expr = {
    data <- getMetadataInformationFilteredToDatabaseId()
    validate(need(all(!is.null(data),
                      nrow(data) > 0),
                  "Not available."))
    keyColumns <- colnames(data)
    getSimpleReactable(data = data,
                       keyColumns = keyColumns,
                       dataColumns = c())
  })
  
  ##output: metadataInfoTitle----
  output$metadataInfoTitle <- shiny::renderUI(expr = {
    data <- getMetadataInformation()
    
    if (!hasData(data)) {
      return(NULL)
    }
    tags$p(paste(
          "Run on ",
          data$databaseId,
          "on ",
          data$startTime,
          data$runTimeUnits
        ))
  })
  
  output$metadataInfoDetailsText <- shiny::renderUI(expr = {
    data <- getMetadataInformation()
    if (!hasData(data)) {
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
    reactable::renderReactable(expr = {
      data <- getMetadataInformation()
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::pull(.data$packageDependencySnapShotJson)
      
      data <- dplyr::as_tibble(RJSONIO::fromJSON(content = data,
                                           digits = 23))
      keyColumns <- colnames(data)
      getSimpleReactable(data = data,
                        keyColumns = keyColumns,
                        dataColumns = c())
    })
  
  ##output: argumentsAtDiagnosticsInitiationJson----
  output$argumentsAtDiagnosticsInitiationJson <-
    shiny::renderText(expr = {
      data <- getMetadataInformation()
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::pull(.data$argumentsAtDiagnosticsInitiationJson) %>%
        RJSONIO::fromJSON(digits = 23) %>%
        RJSONIO::toJSON(digits = 23,
                        pretty = TRUE)
      return(data)
    })
  
  #__________________----
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
  
  shiny::observeEvent(input$orphanConceptsInfo,
                      {
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
  
  shiny::observeEvent(input$cohortCharacterizationInfo,
                      {
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
  
  selectedCohorts <- shiny::reactive({
    if (any(
      is.null(consolidatedCohortIdTarget()),
      length(consolidatedCohortIdTarget()) == 0
    )) {
      return(NULL)
    }
    if (any(is.null(getCohortSortedByCohortId()),
            nrow(getCohortSortedByCohortId()) == 0)) {
      return(NULL)
    }
    if (any(
      is.null(consolidatedDatabaseIdTarget()),
      nrow(consolidatedDatabaseIdTarget()) == 0
    )) {
      return(NULL)
    }
    
    cohortSelected <- getCohortSortedByCohortId() %>%
      dplyr::filter(.data$cohortId %in%  consolidatedCohortIdTarget()) %>%
      dplyr::arrange(.data$cohortId)
    
    databaseIdsWithCount <- cohortCount %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>%
      dplyr::distinct(.data$cohortId, .data$databaseId)
    
    distinctDatabaseIdsWithCount <-
      length(databaseIdsWithCount$databaseId %>% unique())
    
    for (i in 1:nrow(cohortSelected)) {
      filteredDatabaseIds <-
        databaseIdsWithCount[databaseIdsWithCount$cohortId == cohortSelected$cohortId[i], ] %>%
        dplyr::pull()
      
      count <- length(filteredDatabaseIds)
      
      if (distinctDatabaseIdsWithCount == count) {
        cohortSelected$compoundName[i] <- cohortSelected$compoundName[i]
        # paste( #, "- in all data sources", sep = " ")
      } else {
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
    targetSelectedCohort <- selectedCohorts()
    if (any(is.null(targetSelectedCohort),
            nrow(targetSelectedCohort) == 0)) {
      return(NULL)
    }
    selectedCohortIds <- targetSelectedCohort$cohortId
   
    
    if(hasData(consolidatedCohortIdComparator())) {
      selectedCohortIds <- c(selectedCohortIds,consolidatedCohortIdComparator())
    }
    selecteCohortCompondName <- cohort %>% 
      dplyr::filter(.data$cohortId %in% selectedCohortIds) %>% 
      dplyr::select(.data$compoundName)
    
    return(apply(selecteCohortCompondName, 1, function(x)
      tags$tr(lapply(x, tags$td))))
  })
  
  selectedDatabaseIds <- shiny::reactive({
    if (!hasData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    selectedDatabaseIds <- database %>% 
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>% 
      dplyr::select(.data$compoundName)
    
    return(apply(selectedDatabaseIds, 1, function(x)
      tags$tr(lapply(x, tags$td))))
  })
  
  selectedCohort <- shiny::reactive({
    return(input$selectedCompoundCohortName)
  })
  
  selectedComparatorCohort <- shiny::reactive({
    return(input$selectedComparatorCompoundCohortName)
  })
  
  buildCohortConditionTable <-
    function(messsege, cohortCompoundNameArray) {
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
        dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget())
      
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
      
      distinctCohortIds <-
        cohortCountSelected$cohortId %>%  unique()
      
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
          
          if (nrow(filteredCohortDetailsWithHighPercentile) > 0) {
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
              scales::percent(
                length(cohortSubjectRecordRatioEq1) / length(consolidatedDatabaseIdTarget()),
                accuracy = 0.1
              ),
              " of the datasources have one record per subject - ",
              paste(cohortSubjectRecordRatioEq1, collapse =  ", ")
            )
          )
        }),
        tags$div(if (length(cohortSubjectRecordRatioGt1) > 0) {
          tags$p(
            paste0(
              "    ",
              length(cohortSubjectRecordRatioGt1),
              "/",
              length(consolidatedDatabaseIdTarget()),
              " of the datasources that have more than 1 record per subject count - ",
              paste(cohortSubjectRecordRatioGt1, collapse = ", ")
            )
          )
        }),
        tags$br(),
        tags$div(if (length(cohortsWithLowestSubjectConts) > 0) {
          buildCohortConditionTable("Cohorts with lowest subject count(s): ",
                                    cohortsWithLowestSubjectConts)
        }),
        tags$br(),
        tags$div(if (length(cohortsWithHighestSubjectConts) > 0) {
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
      return(input$selectedDatabaseId)
    })
  
  output$cohortCharCompareSelectedCohort <- shiny::renderUI({
    htmltools::withTags(table(tr(td(
      selectedCohort()
    )),
    tr(
      td(selectedComparatorCohort())
    )))
  })
  
  output$cohortCharCompareSelectedDatabase <-
    shiny::renderUI({
      return(selectedDatabaseIds())
    })
  
  output$temporalCharCompareSelectedCohort <-
    shiny::renderUI({
      htmltools::withTags(table(tr(td(
        selectedCohort()
      )),
      tr(
        td(selectedComparatorCohort())
      )))
    })
  output$temporalCharCompareSelectedDatabase <-
    shiny::renderUI({
      return(input$selectedDatabaseId)
    })
  output$timeSeriesSelectedDatabase <-
    shiny::renderUI({
      return(selectedDatabaseIds())
    })
  output$timeDistSelectedDatabase <-
    shiny::renderUI({
      return(selectedDatabaseIds())
    })
  
  output$cohortOverlapSelectedDatabaseId <-
    shiny::renderUI({
      return(selectedDatabaseIds())
    })
})
