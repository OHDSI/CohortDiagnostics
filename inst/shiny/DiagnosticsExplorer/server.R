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
      if (!doesObjectHaveData(conceptSets)) {
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
    
    if (!doesObjectHaveData(data)) {
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
      if (!doesObjectHaveData(data)) {
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
      input$targetCohortDefinitionConceptSetsTable_rows_selected,
      input$comparatorCohortDefinitionConceptSets_rows_selected,
      input$targetCohortDefinitionResolvedConceptTable_rows_selected,
      input$comparatorCohortDefinitionResolvedConceptTable_rows_selected,
      input$targetCohortDefinitionExcludedConceptTable_rows_selected,
      input$comparatorCohortDefinitionExcludedConceptTable_rows_selected,
      input$targetCohortDefinitionOrphanConceptTable_rows_selected,
      input$comparatorCohortDefinitionOrphanConceptTable_rows_selected,
      input$targetCohortDefinitionMappedConceptTable_rows_selected,
      input$comparatorCohortDefinitionMappedConceptTable_rows_selected,
      input$selectedDatabaseId,
      input$selectedDatabaseIds,
      input$selectedDatabaseIds_open,
      input$selectedCompoundCohortName,
      input$selectedComparatorCompoundCohortName,
      input$selectedCompoundCohortNames,
      input$selectedCompoundCohortNames_open,
      input$conceptSetsSelectedTargetCohort,
      input$indexEventBreakdownTable_rows_selected,
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
                     doesObjectHaveData(input$tabs)) {
                   consolidatedCohortIdTarget(data$cohortIdTarget)
                 }
                 
                 if ((isFALSE(input$selectedComparatorCompoundCohortNames_open) ||
                      is.null(input$selectedComparatorCompoundCohortNames_open)) && 
                     doesObjectHaveData(input$tabs)) {
                   consolidatedCohortIdComparator(data$cohortIdComparator)
                 }
                 
                 consolidatedConceptSetIdTarget(data$conceptSetIdTarget)
                 consolidatedConceptSetIdComparator(data$conceptSetIdComparator)
                 
                 if ((isFALSE(input$selectedDatabaseIds_open) ||
                      is.null(input$selectedDatabaseIds_open)) && 
                     doesObjectHaveData(input$tabs)) {
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
              tags$table(width = "100%",
                         tags$tr(
                           tags$td(
                             align = "right",
                             shiny::downloadButton(
                               outputId = "downloadTargetCohortDefinitionCohortCount",
                               label = NULL,
                               icon = shiny::icon("download"),
                               style = "margin-top: 5px; margin-bottom: 5px;"
                             )
                           )
                         )),
              DT::dataTableOutput(outputId = "targetCohortDefinitionCohortCountTable")
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
                           ),
                           tags$td(
                             align = "right",
                             shiny::downloadButton(
                               "saveTargetCohortDefinitionSimplifiedInclusionRuleTable",
                               label = "",
                               icon = shiny::icon("download"),
                               style = "margin-top: 5px; margin-bottom: 5px;"
                             )
                           )
                         )),
              shiny::conditionalPanel(
                condition = "input.targetCohortDefinitionInclusionRuleType == 'Events'",
                DT::dataTableOutput(outputId = "targetCohortDefinitionSimplifiedInclusionRuleTable")
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
              DT::dataTableOutput(outputId = "targetCohortDefinitionConceptSetsTable"),
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
                                                          choices = c("Both", "Person Only", "Record Only"),
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
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveTargetConceptSetsExpressionTable",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "targetConceptSetsExpressionTable"),
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
                                       shiny::downloadButton(
                                         "saveTargetConceptSetsExpressionOptimizedTable",
                                         label = "",
                                         icon = shiny::icon("download"),
                                         style = "margin-top: 5px; margin-bottom: 5px;"
                                       )
                                     )
                                   )),
                        DT::dataTableOutput(outputId = "targetConceptSetsExpressionOptimizedTable"),
                      )
                      )
                  ),
                  shiny::conditionalPanel(
                    condition = "input.targetConceptSetsType == 'Resolved'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveTargetCohortDefinitionResolvedConceptTable",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "targetCohortDefinitionResolvedConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.targetConceptSetsType == 'Excluded'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveTargetCohortDefinitionExcludedConceptTable",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "targetCohortDefinitionExcludedConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.targetConceptSetsType == 'Recommended'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveOrphanConceptsTableTarget",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "targetCohortDefinitionOrphanConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.targetConceptSetsType == 'Mapped'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveTargetCohortDefinitionMappedConceptTable",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "targetCohortDefinitionMappedConceptTable")
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
              tags$table(width = "100%",
                         tags$tr(
                           tags$td(
                             align = "right",
                             shiny::downloadButton(
                               outputId = "downloadComparatorCohortDefinitionCohortCount",
                               label = NULL,
                               icon = shiny::icon("download"),
                               style = "margin-top: 5px; margin-bottom: 5px;"
                             )
                           )
                         )),
              DT::dataTableOutput(outputId = "comparatorCohortDefinitionCohortCountsTable")
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
                           ),
                           tags$td(
                             align = "right",
                             shiny::downloadButton(
                               "saveComparatorCohortDefinitionSimplifiedInclusionRuleTable",
                               label = "",
                               icon = shiny::icon("download"),
                               style = "margin-top: 5px; margin-bottom: 5px;"
                             )
                           )
                         )),
              shiny::conditionalPanel(
                condition = "input.comparatorCohortDefinitionInclusionRuleType == 'Events'",
                DT::dataTableOutput(outputId = "comparatorCohortDefinitionSimplifiedInclusionRuleTable")
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
              DT::dataTableOutput(outputId = "comparatorCohortDefinitionConceptSets"),
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
                                                          choices = c("Both", "Person Only", "Record Only"),
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
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveComparatorConceptSetsExpressionTable",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "comparatorConceptSetsExpressionTable"),
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
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveComparatorCohortDefinitionResolvedConceptTable",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "comparatorCohortDefinitionResolvedConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.comparatorConceptSetsType == 'Excluded'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveComparatorCohortDefinitionExcludedConceptTable",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "comparatorCohortDefinitionExcludedConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.comparatorConceptSetsType == 'Recommended'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveComparatorCohortDefinitionOrphanConceptTable",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "comparatorCohortDefinitionOrphanConceptTable")
                  ),
                  shiny::conditionalPanel(
                    condition = "input.comparatorConceptSetsType == 'Mapped'",
                    tags$table(width = "100%",
                               tags$tr(
                                 tags$td(
                                   align = "right",
                                   shiny::downloadButton(
                                     "saveComparatorCohortDefinitionMappedConceptTable",
                                     label = "",
                                     icon = shiny::icon("download"),
                                     style = "margin-top: 5px; margin-bottom: 5px;"
                                   )
                                 )
                               )),
                    DT::dataTableOutput(outputId = "comparatorCohortDefinitionMappedConceptTable")
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
      selection = "none",
      class = "stripe compact"
    )
    return(dataTable)
  }, server = TRUE)
  
  
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
      if (doesObjectHaveData(consolidatedCohortIdComparator())) {
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
  
  ##Human readable text----
  ###getCirceRPackageVersionInformation----
  getCirceRPackageVersionInformation <- shiny::reactive(x = {
    packageVersion <- as.character(packageVersion('CirceR'))
    return(packageVersion)
  })
  
  ###getCirceRenderedExpressionDetailsTarget----
  getCirceRenderedExpressionDetailsTarget <- shiny::reactive(x = {
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
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
    
    if (!doesObjectHaveData(cohortExpression)) {
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
      if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
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
      
      if (!doesObjectHaveData(cohortExpression)) {
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
      if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      cohortName <- cohort %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
        dplyr::pull(.data$compoundName)
      
      if (!doesObjectHaveData(cohortName)) {
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
      if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      cohortName <- cohort %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
        dplyr::pull(.data$compoundName)
      
      if (!doesObjectHaveData(cohortName)) {
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
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    cohortDefinition <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::pull(.data$json) %>%
      RJSONIO::fromJSON(digits = 23)
    if (!doesObjectHaveData(cohortDefinition)) {
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
    if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    json <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
      dplyr::pull(.data$json) %>%
      RJSONIO::fromJSON(digits = 23)
    if (!doesObjectHaveData(cohortDefinition)) {
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
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    data <- cohortCount %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::select(.data$databaseId,
                    .data$cohortSubjects,
                    .data$cohortEntries) %>%
      dplyr::arrange(.data$databaseId)
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ###output: targetCohortDefinitionCohortCountTable----
  output$targetCohortDefinitionCohortCountTable <-
    DT::renderDataTable(expr = {
      data <- getCountsForSelectedCohortsTarget()
      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
        "There is no inclusion rule data for this cohort."
      ))
      maxCohortSubjects <- getMaxValueForStringMatchedColumnsInDataFrame(data = data, string = "ubjects")
      maxCohortEntries <- getMaxValueForStringMatchedColumnsInDataFrame(data = data, string = "ntries")
      
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
        selection = "none",
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
  
  ###output: downloadTargetCohortDefinitionCohortCount----
  output$downloadTargetCohortDefinitionCohortCount <- downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "CohortCount")
    },
    content = function(file) {
      data <- getCountsForSelectedCohortsTarget()
      downloadCsv(x = data, fileName = file)
    }
  )
  
  ##Concept set ----
  ###getConceptSetExpressionTarget----
  getConceptSetExpressionTarget <- shiny::reactive(x = {
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
      return(NULL)
    }
    conceptSetExpression <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdTarget())
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
      !doesObjectHaveData(consolidatedCohortIdComparator()),!doesObjectHaveData(consolidatedConceptSetIdComparator())
    )) {
      return(NULL)
    }
    conceptSetExpression <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
      dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdComparator())
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
  #   if (!doesObjectHaveData(data)) {
  #     return(NULL)
  #   }
  #   return(data)
  # })
  
  
  # ###getResolvedConceptsAllDataConceptIdDetails----
  # getResolvedConceptsAllDataConceptIdDetails <- shiny::reactive({ 
  # resolvedConcepts <- getResolvedConceptsAllData()
  # if (!doesObjectHaveData(resolvedConcepts)) {
  #   return(NULL)
  # }
  # progress <- shiny::Progress$new()
  # on.exit(progress$close())
  # progress$set(message = "Caching concept count for resolved concepts",
  #              value = 0)
  # conceptDetails <- getConcept(dataSource = dataSource,
  #                              conceptIds = resolvedConcepts$conceptId %>% unique())
  # if (!doesObjectHaveData(conceptDetails)) {
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
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    
    conceptDetails <- getConcept(dataSource = dataSource, 
                                 conceptIds = c(data$conceptId %>% unique()))
    if (!doesObjectHaveData(conceptDetails)) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(count)) {
      return(data %>% 
               dplyr::mutate("records" = NA, 
                             "persons" = NA))
    }
    data <- data %>% 
      dplyr::left_join(count, 
                       by = c('databaseId', 'conceptId'))
    return(data)
  })
  
  ###getMappedConceptsTargetData----
  getMappedConceptsTargetData <- shiny::reactive({
    data <- getResolvedConceptsTargetData()
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    resolvedConceptIds <- data$conceptId %>% unique()
    
    mappedConcepts <- getConceptRelationship(
      dataSource = dataSource,
      conceptIds = resolvedConceptIds, 
      relationshipIds = "Mapped from"
    )
    if (!doesObjectHaveData(mappedConcepts)) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(count)) {
      return(data %>% 
               dplyr::mutate("records" = NA, "persons" = NA))
    }
    data <- data %>% 
      dplyr::left_join(count, 
                       by = c('databaseId', 'conceptId'))
    return(data)
  })
  
  ###getResolvedConceptsComparatorData----
  getResolvedConceptsComparatorData <- shiny::reactive({
    if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdComparator())) {
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    
    conceptDetails <- getConcept(dataSource = dataSource, 
                                 conceptIds = c(data$conceptId %>% unique()))
    if (!doesObjectHaveData(conceptDetails)) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(count)) {
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
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(count)) {
      return(data %>% 
               dplyr::mutate("records" = NA, "persons" = NA))
    }
    data <- data %>% 
      dplyr::left_join(count, 
                       by = c('databaseId', 'conceptId'))
    return(data)
   })
  
  #getExcludedConceptsComparatorData
  getExcludedConceptsComparatorData <- shiny::reactive({
    if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdComparator())) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(count)) {
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    resolvedConceptIds <- data$conceptId %>% unique()
    
    mappedConcepts <- getConceptRelationship(
      dataSource = dataSource,
      conceptIds = resolvedConceptIds, 
      relationshipIds = "Mapped from"
    )
    if (!doesObjectHaveData(mappedConcepts)) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(count)) {
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
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(count)) {
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
    if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdComparator())) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(count)) {
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
    if (!doesObjectHaveData(activeSelected()$conceptId)) {
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
    if (!doesObjectHaveData(data)) {
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
      !doesObjectHaveData(consolidatedCohortIdTarget()),
      !doesObjectHaveData(consolidatedDatabaseIdTarget())
    )) {
      return(NULL)
    }
    data <-
      getResultsInclusionRuleStatistics(
        dataSource = dataSource,
        cohortId = consolidatedCohortIdTarget(),
        databaseId = consolidatedDatabaseIdTarget()
      )
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ###getSimplifiedInclusionRuleResultsComparator----
  getSimplifiedInclusionRuleResultsComparator <-
    shiny::reactive(x = {
      if (any(
        !doesObjectHaveData(consolidatedCohortIdComparator()),
        !doesObjectHaveData(consolidatedDatabaseIdTarget())
      )) {
        return(NULL)
      }
      data <-
        getResultsInclusionRuleStatistics(
          dataSource = dataSource,
          cohortId = consolidatedCohortIdComparator(),
          databaseId = consolidatedDatabaseIdTarget()
        )
      if (!doesObjectHaveData(data)) {
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
    DT::renderDataTable(expr = {
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
      
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = 1,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$targetCohortInclusionRulesAsPercent
      )
      return(table)
    }, server = TRUE)
  
  #output: saveTargetCohortDefinitionSimplifiedInclusionRuleTable----
  output$saveTargetCohortDefinitionSimplifiedInclusionRuleTable <-
    downloadHandler(
      filename = function() {
        getCsvFileNameWithDateTime(string = "InclusionRule")
      },
      content = function(file) {
        downloadCsv(x = getSimplifiedInclusionRuleResultsTarget(), fileName = file)
      }
    )
  
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
    if (!doesObjectHaveData(json)) {
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
    if (!doesObjectHaveData(input$targetConceptSetsType)) {
      return(NULL)
    }
    if (!doesObjectHaveData(input$comparatorConceptSetsType)) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
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
    if (any(!doesObjectHaveData(target),
            !doesObjectHaveData(comparator))) {
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
    
    combinedResult <- combinedResult  %>% 
      tidyr::pivot_longer(
        names_to = "type",
        cols = c("target", "comparator"),
        values_to = "value"
      ) %>% 
      dplyr::inner_join(cohortShortName,
                        by = "type") %>% 
      dplyr::mutate(type = paste0(.data$type, " (", .data$shortName, ")")) %>% 
      dplyr::select(-.data$shortName) %>% 
      dplyr::left_join(conceptIdSortOrder, by = "conceptId") %>% 
      dplyr::arrange(.data$rn, .data$databaseId, .data$conceptId)
    return(combinedResult)
  })
  
  #output: saveConceptSetComparisonTable----
  output$saveConceptSetComparisonTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "ConceptSetsExpressionComparison")
    },
    content = function(file) {
      data <- getConceptSetComparisonTableData()
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
      data <- data %>% 
        dplyr::mutate(type = paste0(type, " ", .data$databaseId)) %>% 
        dplyr::select(-.data$databaseId, -.data$rn) %>% 
        tidyr::pivot_wider(
          id_cols = c(
            "conceptId",
            "conceptName"
          ),
          names_from = type,
          values_from = value
        )
      downloadCsv(x = data, fileName = file)
      #!!!! this may need downloadExcel() with formatted and multiple tabs
    }
  )
  
  
  output$conceptSetComparisonTable <- DT::renderDT(expr = {
    data <- getConceptSetComparisonTableData()
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    data <- data %>% 
      dplyr::arrange(.data$databaseId, dplyr::desc(.data$type))
    databaseIds <- unique(data$databaseId) %>% sort()
    
    data <- data %>% 
      dplyr::mutate(type = paste0(type, " ", .data$databaseId)) %>% 
      dplyr::select(-.data$databaseId) %>% 
      dplyr::mutate(value = dplyr::case_when(.data$value == TRUE ~ as.character(icon("check")),
                                             FALSE ~ as.character(NA))) %>% 
      tidyr::pivot_wider(
        id_cols = c(
          "conceptId",
          "conceptName",
          "rn"
        ),
        names_from = type,
        values_from = value
      ) %>% 
      dplyr::arrange(.data$rn) %>% 
      dplyr::select(-.data$rn)
    columnNames <- c("Target", "Comparator")
    
    sketch <- htmltools::withTags(table(class = "display",
                                        thead(tr(
                                          th(rowspan = 2, "Concept ID"),
                                          th(rowspan = 2, "Concept Name"),
                                          lapply(
                                            databaseIds,
                                            th,
                                            colspan = length(columnNames),
                                            class = "dt-center",
                                            style = "border-right:1px solid silver;border-bottom:1px solid silver"
                                          )
                                        ),
                                        tr(
                                          lapply(rep(
                                            columnNames,
                                            length(databaseIds)
                                          ), th, style = "border-right:1px solid silver;border-bottom:1px solid silver")
                                        ))))
    
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
      data,
      options = options,
      container = sketch,
      colnames = colnames(data) %>% camelCaseToTitleCase(),
      rownames = FALSE,
      escape = FALSE,
      selection = 'single',
      filter = "top",
      class = "stripe nowrap compact"
    )
    return(dataTable)
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
    if (!doesObjectHaveData(activeSelected()$conceptId)) {
      #currently expecting to be vector of 1 (single select)
      return(NULL)
    }
    if (length(activeSelected()$conceptId) != 1) {
      stop("Only single select is supported for conceptId")
    }
    if (!doesObjectHaveData(activeSelected()$cohortId)) {
      #currently expecting to be vector of 1 (single select)
      return(NULL)
    }
    if (length(activeSelected()$cohortId) != 1) {
      stop("Only single select is supported for cohortId")
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    if (!doesObjectHaveData(data$databaseConceptCount)) {
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
      if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
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
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(conceptSets)) {
      return(NULL)
    }
    data <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::select(.data$conceptSetId, .data$conceptSetName) %>%
      dplyr::arrange(.data$conceptSetId)
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  #output: targetCohortDefinitionConceptSetsTable----
  output$targetCohortDefinitionConceptSetsTable <-
    DT::renderDataTable(expr = {
      data <- getConceptSetsInCohortDataTarget()
      validate(need(all(!is.null(data),
                        nrow(data) > 0),
        "Concept set details not available for this cohort"
      ))
      
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
        selection = list(mode = 'single', selected = 1),
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }, server = TRUE)
  
  getConceptSetsInCohortDataComparator <- reactive({
    if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
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
    DT::renderDataTable(expr = {
      data <- getConceptSetsInCohortDataComparator()
      validate(need(all(!is.null(data),
                        nrow(data) > 0),
                    "Concept set details not available for this cohort"
      ))
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
        selection = list(mode = 'single', selected = 1),
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }, server = TRUE)
  
  #output: conceptsetExpressionTableTarget----
  output$conceptsetExpressionTableTarget <-
    DT::renderDataTable(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
        return(NULL)
      }
      
      data <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
        dplyr::filter(.data$conceptSetid %in% consolidatedConceptSetIdTarget()) %>%
        dplyr::select(.data$conceptSetExpression)
      if (!doesObjectHaveData(data)) {
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
      data <- input$targetCohortDefinitionConceptSetsTable_rows_selected
      return(doesObjectHaveData(data))
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
      if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
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
      if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
        return(NULL)
      }
      optimizedConceptSetExpression <-
        getOptimizedTargetConceptSetsExpressionTable()
      if (!doesObjectHaveData(optimizedConceptSetExpression)) {
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
      
      if (!doesObjectHaveData(removed)) {
        return(FALSE)
      }
      return(nrow(removed) != 0)
    })
  
  shiny::outputOptions(x = output,
                       name = "canTargetConceptSetExpressionBeOptimized",
                       suspendWhenHidden = FALSE)
  
  #output: saveTargetConceptSetsExpressionOptimizedTable----
  output$saveTargetConceptSetsExpressionOptimizedTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "ConceptSetsExpressionOptimized")
    },
    content = function(file) {
      downloadCsv(x = getOptimizedTargetConceptSetsExpressionTable(), fileName = file)
      #!!!! this may need downloadExcel() with formatted and multiple tabs
    }
  )
  
  #output: targetConceptSetsExpressionOptimizedTable----
  output$targetConceptSetsExpressionOptimizedTable <-
    DT::renderDataTable(expr = {
      optimizedConceptSetExpression <-
        getOptimizedTargetConceptSetsExpressionTable()
      if (!doesObjectHaveData(optimizedConceptSetExpression)) {
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
        optimizedConceptSetExpression,
        options = options,
        colnames = colnames(optimizedConceptSetExpression) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        escape = FALSE,
        selection = 'none',
        filter = "top",
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }, server = TRUE)
  
  
  #output: saveTargetConceptSetsExpressionTable----
  output$saveTargetConceptSetsExpressionTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "ConceptSetsExpression")
    },
    content = function(file) {
      downloadCsv(x = getConceptSetExpressionTarget(), fileName = file)
      #!!!! this may need downloadExcel() with formatted and multiple tabs
    }
  )
  
  #output: targetConceptSetsExpressionTable----
  output$targetConceptSetsExpressionTable <-
    DT::renderDataTable(expr = {
      data <- getConceptSetExpressionTarget()
      if (!doesObjectHaveData(data)) {
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
        selection = 'none',
        filter = "top",
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }, server = TRUE)
  
  #output: saveTargetCohortDefinitionResolvedConceptTable----
  output$saveTargetCohortDefinitionResolvedConceptTable <-
    downloadHandler(
      filename = function() {
        getCsvFileNameWithDateTime(string = "ResolvedConcepts")
      },
      content = function(file) {
        data <- getResolvedConceptsTarget()
        downloadCsv(x = data, fileName = file)
      }
    )
  
  #output: targetCohortDefinitionResolvedConceptTable----
  output$targetCohortDefinitionResolvedConceptTable <-
    DT::renderDataTable(expr = {
      validate(need(
        length(consolidatedCohortIdTarget()) > 0,
        "Please select concept set"
      ))
      validate(need(
        length(consolidatedDatabaseIdTarget()) > 0,
        "Please select database id"
      ))
      data <- getResolvedConceptsTarget()
      validate(need(doesObjectHaveData(data), "No resolved concept ids"))
      keyColumnFields <- c("conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$targetCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        sketchLevel <- 2
      } else if (input$targetCohortConceptSetColumnFilter == "Person Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        sketchLevel <- 1
      } else if (input$targetCohortConceptSetColumnFilter == "Record Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        sketchLevel <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$targetConceptIdCountSource,
          fields = input$targetCohortConceptSetColumnFilter
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)

      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = sketchLevel,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnTarget 
      )
      return(table)
    }, server = TRUE)
  
  #output: saveOrphanConceptsTableTarget----
  output$saveOrphanConceptsTableTarget <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "orphanConcepts")
    },
    content = function(file) {
      data <- getOrphanConceptsTarget()
      downloadCsv(x = data, fileName = file)
    }
  )
  
  #output: targetCohortDefinitionExcludedConceptTable----
  output$targetCohortDefinitionExcludedConceptTable <-
    DT::renderDataTable(expr = {
      validate(need(
        length(consolidatedCohortIdTarget()) > 0,
        "Please select concept set"
      ))
      data <- getExcludedConceptsTarget()
      validate(need(doesObjectHaveData(data), "No excluded concept ids"))
      
      keyColumnFields <- c("conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$targetCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        sketchLevel <- 2
      } else if (input$targetCohortConceptSetColumnFilter == "Person Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        sketchLevel <- 1
      } else if (input$targetCohortConceptSetColumnFilter == "Record Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        sketchLevel <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$targetConceptIdCountSource,
          fields = input$targetCohortConceptSetColumnFilter
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = sketchLevel,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnTarget
      )
      return(table)
    }, server = TRUE)
  
  #output: saveTargetCohortDefinitionExcludedConceptTable----
  output$saveTargetCohortDefinitionExcludedConceptTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "excludedConcepts")
    },
    content = function(file) {
      data <- getExcludedConceptsTarget()
      downloadCsv(x = data, fileName = file)
    }
  )
  
  #output: targetCohortDefinitionOrphanConceptTable----
  output$targetCohortDefinitionOrphanConceptTable <-
    DT::renderDataTable(expr = {
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
        sketchLevel <- 2
      } else if (input$targetCohortConceptSetColumnFilter == "Person Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        sketchLevel <- 1
      } else if (input$targetCohortConceptSetColumnFilter == "Record Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        sketchLevel <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$targetConceptIdCountSource,
          fields = input$targetCohortConceptSetColumnFilter
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = sketchLevel,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnTarget 
      )
      return(table)
    }, server = TRUE)
  
  #output: targetCohortDefinitionMappedConceptTable----
  output$targetCohortDefinitionMappedConceptTable <-
    DT::renderDataTable(expr = {
      validate(need(
        length(consolidatedCohortIdTarget()) > 0,
        "Please select concept set"
      ))
      validate(need(
        length(consolidatedDatabaseIdTarget()) > 0,
        "Please select database id"
      ))
      data <- getMappedConceptsTarget()
      validate(need(doesObjectHaveData(data), "No resolved concept ids"))
      keyColumnFields <- c("resolvedConcept", "conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$targetCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        sketchLevel <- 2
      } else if (input$targetCohortConceptSetColumnFilter == "Person Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        sketchLevel <- 1
      } else if (input$targetCohortConceptSetColumnFilter == "Record Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        sketchLevel <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$targetConceptIdCountSource,
          fields = input$targetCohortConceptSetColumnFilter
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = sketchLevel,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnTarget 
      )
      return(table)
    }, server = TRUE)
  
  ##saveTargetCohortDefinitionMappedConceptTable
  output$saveTargetCohortDefinitionMappedConceptTable <-
    downloadHandler(
      filename = function()
      {
        getCsvFileNameWithDateTime(string = "orphanconcepts")
      },
      content = function(file)
      {
        downloadCsv(x = getMappedConceptsTarget(),
                    fileName = file)
      }
    )
  
  #output: targetConceptsetExpressionJson----
  output$targetConceptsetExpressionJson <- shiny::renderText({
    if (any(
      !doesObjectHaveData(getConceptSetExpressionTarget()),
      !doesObjectHaveData(consolidatedConceptSetIdTarget())
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
      !doesObjectHaveData(getConceptSetExpressionTarget()),
      !doesObjectHaveData(consolidatedConceptSetIdTarget())
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
    DT::renderDataTable(expr = {
      data <- getCountsForSelectedCohortsComparator()
      validate(need(
        all(!is.null(data),
            nrow(data) > 0),
        "There is no inclusion rule data for this cohort."
      ))
      maxCohortSubjects <- getMaxValueForStringMatchedColumnsInDataFrame(data = data, string = "ubjects")
      maxCohortEntries <- getMaxValueForStringMatchedColumnsInDataFrame(data = data, string = "ntries")
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
        selection = "none",
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
  
  ###output: downloadComparatorCohortDefinitionCohortCount----
  output$downloadComparatorCohortDefinitionCohortCount <- downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "CohortCount")
    },
    content = function(file) {
      data <- getCountsForSelectedCohortsComparator()
      downloadCsv(x = data, fileName = file)
    }
  )
  
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
    DT::renderDataTable(expr = {
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
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = 1,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$comparatorCohortInclusionRulesAsPercent
      )
      return(table)
    }, server = TRUE)
  
  ##output: saveComparatorCohortDefinitionSimplifiedInclusionRuleTable----
  output$saveComparatorCohortDefinitionSimplifiedInclusionRuleTable <-
    downloadHandler(
      filename = function()
      {
        getCsvFileNameWithDateTime(string = "InclusionRule")
      },
      content = function(file)
      {
        downloadCsv(x = getSimplifiedInclusionRuleResultsComparator(), fileName = file)
      }
    )
  
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
    if (!doesObjectHaveData(json)) {
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
      if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdComparator())) {
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
      if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdComparator())) {
        return(NULL)
      }
      data <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
        dplyr::filter(.data$conceptSetid %in% consolidatedConceptSetIdComparator()) %>%
        dplyr::pull(.data$conceptSetExpression)
      if (!doesObjectHaveData(data)) {
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
      return(!is.null(input$comparatorCohortDefinitionConceptSets_rows_selected))
    })
  shiny::outputOptions(x = output,
                       name = "isComparatorCohortDefinitionConceptSetRowSelected",
                       suspendWhenHidden = FALSE)
  
  #reactive: getOptimizedComparatorConceptSetsExpressionTable----
  getOptimizedComparatorConceptSetsExpressionTable <-
    shiny::reactive(x = {
      if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdComparator())) {
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
  #     if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
  #       return(NULL)
  #     }
  #     if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
  #       return(NULL)
  #     }
  #     if (!doesObjectHaveData(consolidatedConceptSetIdComparator())) {
  #       return(NULL)
  #     }
  #     optimizedConceptSetExpression <-
  #       getOptimizedComparatorConceptSetsExpressionTable()
  #     if (!doesObjectHaveData(optimizedConceptSetExpression)) {
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
  #     if (!doesObjectHaveData(removed)) {
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
  #     if (!doesObjectHaveData(optimizedConceptSetExpression)) {
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
    DT::renderDataTable(expr = {
      data <- getConceptSetExpressionComparator()
      if (!doesObjectHaveData(data)) {
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
        columnDefs = list(truncateStringDef(2, 80))
      )
      
      dataTable <- DT::datatable(
        data,
        options = options,
        colnames = colnames(data) %>% camelCaseToTitleCase(),
        rownames = FALSE,
        escape = FALSE,
        selection = 'none',
        filter = "top",
        class = "stripe nowrap compact"
      )
      return(dataTable)
    }, server = TRUE)
  
  ##output: saveComparatorConceptSetsExpressionTable----
  output$saveComparatorConceptSetsExpressionTable <-
    downloadHandler(
      filename = function() {
        getCsvFileNameWithDateTime(string = "conceptset")
      },
      content = function(file) {
        downloadCsv(x = getConceptSetExpressionComparator(),
                    fileName = file)
      }
    )
  
  ##output: comparatorCohortDefinitionResolvedConceptTable----
  output$comparatorCohortDefinitionResolvedConceptTable <-
    DT::renderDataTable(expr = {
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
        sketchLevel <- 2
      } else if (input$comparatorCohortConceptSetColumnFilter == "Person Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        sketchLevel <- 1
      } else if (input$comparatorCohortConceptSetColumnFilter == "Record Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        sketchLevel <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$comparatorConceptIdCountSource,
          fields = input$comparatorCohortConceptSetColumnFilter
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = sketchLevel,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnComparator
      )
      return(table)
    }, server = TRUE)
  
  ##output: saveComparatorCohortDefinitionResolvedConceptTable----
  output$saveComparatorCohortDefinitionResolvedConceptTable <-
    downloadHandler(
      filename = function() {
        getCsvFileNameWithDateTime(string = "resolvedConceptSet")
      },
      content = function(file) {
        data <- getResolvedConceptsComparator()
        downloadCsv(x = data, fileName = file)
      }
    )
  
  #output: comparatorCohortDefinitionExcludedConceptTable----
  output$comparatorCohortDefinitionExcludedConceptTable <-
    DT::renderDataTable(expr = {
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
        sketchLevel <- 2
      } else if (input$comparatorCohortConceptSetColumnFilter == "Person Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        sketchLevel <- 1
      } else if (input$comparatorCohortConceptSetColumnFilter == "Record Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        sketchLevel <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$comparatorConceptIdCountSource,
          fields = input$comparatorCohortConceptSetColumnFilter
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = sketchLevel,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnComparator
      )
      return(table)
    }, server = TRUE)
  
  #output: saveComparatorCohortDefinitionExcludedConceptTable----
  output$saveComparatorCohortDefinitionExcludedConceptTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "excludedConcepts")
    },
    content = function(file) {
      downloadCsv(x = getExcludedConceptsComparator(), fileName = file)
    }
  )
  
  output$comparatorCohortDefinitionOrphanConceptTable <-
    DT::renderDataTable(expr = {
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
        sketchLevel <- 2
      } else if (input$comparatorCohortConceptSetColumnFilter == "Person Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        sketchLevel <- 1
      } else if (input$comparatorCohortConceptSetColumnFilter == "Record Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        sketchLevel <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$comparatorConceptIdCountSource,
          fields = input$comparatorCohortConceptSetColumnFilter
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = sketchLevel,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnComparator
      )
      return(table)
    }, server = TRUE)
  
  ##output: saveComparatorCohortDefinitionOrphanConceptTable----
  output$saveComparatorCohortDefinitionOrphanConceptTable <-
    downloadHandler(
      filename = function()
      {
        getCsvFileNameWithDateTime(string = "orphanconcepts")
      },
      content = function(file)
      {
        downloadCsv(x = getOrphanConceptsComparator(),
                    fileName = file)
      }
    )
  
  #output: comparatorCohortDefinitionMappedConceptTable----
  output$comparatorCohortDefinitionMappedConceptTable <-
    DT::renderDataTable(expr = {
      validate(need(
        length(consolidatedCohortIdComparator()) > 0,
        "Please select concept set"
      ))
      validate(need(
        length(consolidatedDatabaseIdTarget()) > 0,
        "Please select database id"
      ))
      data <- getMappedConceptsComparator()
      validate(need(doesObjectHaveData(data), "No resolved concept ids"))
      keyColumnFields <- c("resolvedConcept", "conceptId", "conceptName", "vocabularyId")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$targetCohortConceptSetColumnFilter == "Both") {
        dataColumnFields <- dataColumnFields
        sketchLevel <- 2
      } else if (input$targetCohortConceptSetColumnFilter == "Person Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("person")
          )]
        sketchLevel <- 1
      } else if (input$targetCohortConceptSetColumnFilter == "Record Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(
            string = tolower(dataColumnFields),
            pattern = tolower("record")
          )]
        sketchLevel <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = input$targetConceptIdCountSource,
          fields = input$targetCohortConceptSetColumnFilter
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = sketchLevel,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$showAsPercentageColumnTarget 
      )
      return(table)
    }, server = TRUE)
  
  ##output: saveComparatorCohortDefinitionMappedConceptTable----
  output$saveComparatorCohortDefinitionMappedConceptTable <-
    downloadHandler(
      filename = function()
      {
        getCsvFileNameWithDateTime(string = "orphanconcepts")
      },
      content = function(file)
      {
        downloadCsv(x = getMappedConceptsComparator(),
                    fileName = file)
      }
    )
  
  ##output: comparatorCohortDefinitionConceptsetExpressionJson----
  output$comparatorCohortDefinitionConceptsetExpressionJson <-
    shiny::renderText({
      if (any(
        !doesObjectHaveData(consolidatedCohortIdComparator()),
        !doesObjectHaveData(consolidatedConceptSetIdComparator())
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
        !doesObjectHaveData(consolidatedCohortIdComparator()),
        !doesObjectHaveData(consolidatedConceptSetIdComparator())
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    databaseCount <- getDataSourceTimeSeries()
    if (!doesObjectHaveData(databaseCount)) {
      return(NULL)
    }
    # working on the plot
    if (input$timeSeriesAggregationForConceptId == "Monthly") {
      data <- data$databaseConceptIdYearMonthLevelTsibble
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
      data <- data %>% 
        dplyr::filter(.data$conceptId == activeSelected()$conceptId)
      if (!doesObjectHaveData(databaseCount$m)) {
        return(NULL)
      }
      databaseCount <- databaseCount$m %>% 
        dplyr::select(.data$databaseId,
                      .data$periodBegin,
                      .data$records,
                      .data$subjects)
    } else {
      data <- data$databaseConceptIdYearLevelTsibble
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
      data <- data %>% 
        dplyr::filter(.data$conceptId == activeSelected()$conceptId)
      if (!doesObjectHaveData(databaseCount$y)) {
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
    validate(need(doesObjectHaveData(data),
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
     if (!doesObjectHaveData(data)) {
       return(NULL)
     }
     return(data)
  })
  
  conceptSetBrowserData <- shiny::reactive(x = {
    conceptId <- activeSelected()$conceptId
    validate(need(doesObjectHaveData(conceptId), "No concept id selected."))
    cohortId <- activeSelected()$cohortId
    validate(need(doesObjectHaveData(conceptId), "No cohort id selected."))
    databaseId <- consolidatedDatabaseIdTarget()
    validate(need(doesObjectHaveData(databaseId), "No database id selected."))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0("Computing concept relationship for concept id:",
                       conceptId),
      value = 0
    )
    
    data <- getMetadataForConceptId()
    validate(need(
      doesObjectHaveData(data),
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
      doesObjectHaveData(relationshipNameFilter),
      doesObjectHaveData(relationshipDistanceFilter)
    )) {
      if (doesObjectHaveData(relationshipNameFilter)) {
        conceptRelationshipTable <- conceptRelationshipTable %>%
          dplyr::inner_join(
            relationship %>%
              dplyr::filter(.data$relationshipName %in% c(relationshipNameFilter)) %>%
              dplyr::select(.data$relationshipId) %>%
              dplyr::distinct(),
            by = "relationshipId"
          )
      }
      if (doesObjectHaveData(relationshipDistanceFilter)) {
        conceptRelationshipTable <- conceptRelationshipTable %>%
          dplyr::filter(.data$levelsOfSeparation %in%
                          relationshipDistanceFilter)
      }
    }
    conceptRelationshipTable <- conceptRelationshipTable %>%
      dplyr::inner_join(data$concept,
                        by = "conceptId") %>%
      tidyr::crossing(dplyr::tibble(databaseId = !!databaseId))
    if (!doesObjectHaveData(conceptRelationshipTable)) {
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
  output$conceptBrowserTable <- DT::renderDT(expr = {
    
    data <- conceptSetBrowserData()
    if (!doesObjectHaveData(data)) {
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
      sketchLevel <- 2
    } else if (input$targetCohortConceptSetColumnFilter == "Person Only") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(
          string = tolower(dataColumnFields),
          pattern = tolower("person")
        )]
      sketchLevel <- 1
    } else if (input$targetCohortConceptSetColumnFilter == "Record Only") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(
          string = tolower(dataColumnFields),
          pattern = tolower("record")
        )]
      sketchLevel <- 1
    }
    
    countsForHeader <-
      getCountsForHeaderForUseInDataTable(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds = consolidatedCohortIdTarget(),
        source = input$targetConceptIdCountSource,
        fields = input$targetCohortConceptSetColumnFilter
      )
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    table <- getDtWithColumnsGroupedByDatabaseId(
      data = data,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      sketchLevel = sketchLevel,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showResultsAsPercent = input$showAsPercentageColumnTarget 
    )
    return(table)
  }) 
  
  output$saveDetailsOfSelectedConceptId <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "ConceptSetBrowser")
    },
    content = function(file) {
      downloadCsv(x = conceptSetBrowserData(),
                  fileName = file)
    }
  )
  
  ##getSourceCodesObservedForConceptIdInDatasource----
  getSourceCodesObservedForConceptIdInDatasource <- shiny::reactive(x = {
    conceptId <- activeSelected()$conceptId
    cohortId <- activeSelected()$cohortId
    databaseId <- consolidatedDatabaseIdTarget()
    if (!all(
      doesObjectHaveData(conceptId),
      doesObjectHaveData(cohortId),
      doesObjectHaveData(databaseId)
    )){
      return(NULL)
    }
    data <- getResultsConceptMapping(
      dataSource,
      databaseIds = databaseId,
      conceptIds = conceptId,
      domainTables = 'All'
    )
    if (!doesObjectHaveData(data)) {
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
  output$observedSourceCodesTable <- DT::renderDT(expr = {
    conceptId <- activeSelected()$conceptId
    validate(need(doesObjectHaveData(conceptId), "No concept id selected."))
    cohortId <- activeSelected()$cohortId
    validate(need(doesObjectHaveData(conceptId), "No cohort id selected."))
    databaseId <- consolidatedDatabaseIdTarget()
    validate(need(doesObjectHaveData(databaseId), "No database id selected."))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0("Computing concept relationship for concept id:",
                       conceptId),
      value = 0
    )
    data <- getSourceCodesObservedForConceptIdInDatasource()
    validate(need(
      doesObjectHaveData(data),
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
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    
    table <- getDtWithColumnsGroupedByDatabaseId(
      data = data,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      sketchLevel = 1,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showResultsAsPercent = FALSE
    )
    return(table)
  })
  
  shiny::observeEvent(eventExpr = input$exportAllCohortDetails,
                      handlerExpr = {
                        outputFolder <- "E:\\CohortDetails"
                        
                        for (i in (1:nrow(cohort))) {
                          cohortId <- cohort[i,]$cohortId
                          cohortName <- cohort[i,]$cohortName
                          dir.create(path = file.path(outputFolder, cohortName), recursive = TRUE, showWarnings = FALSE)
                          cohortExpression <- cohort[i,]$json %>% 
                            RJSONIO::fromJSON(digits = 23)
                          
                          details <-
                           getCirceRenderedExpression(cohortDefinition =  cohortExpression)
                          SqlRender::writeSql(sql = details$cohortJson,
                                              targetFile = file.path(outputFolder, cohortName, paste0('cohortDefinitionJson_', cohortId, '.json')))
                          SqlRender::writeSql(sql = details$cohortMarkdown,
                                              targetFile = file.path(outputFolder, cohortName, paste0('cohortDefinitionMarkdown_', cohortId, '.md')))
                          SqlRender::writeSql(sql = details$conceptSetMarkdown,
                                              targetFile = file.path(outputFolder, cohortName, paste0('conceptSetMarkdown_', cohortId, '.md')))
                          SqlRender::writeSql(sql = details$cohortHtmlExpression,
                                              targetFile = file.path(outputFolder, cohortName, paste0('cohortDefinitionHtml_', cohortId, '.html')))
                        }
                      })
  
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
        } else if (input$targetCohortConceptSetColumnFilter == "Person Only") {
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortConceptSetColumnFilter",
                             selected = "Person Only")
        } else {
          updateRadioButtons(session = session,
                             inputId = "comparatorCohortConceptSetColumnFilter",
                             selected = "Record Only")
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
        } else if (input$comparatorCohortConceptSetColumnFilter == "Person Only") {
          updateRadioButtons(session = session,
                             inputId = "targetCohortConceptSetColumnFilter",
                             selected = "Person Only")
        } else {
          updateRadioButtons(session = session,
                             inputId = "targetCohortConceptSetColumnFilter",
                             selected = "Record Only")
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
      if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      data <- cohortCount %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
        dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>%
        dplyr::inner_join(cohort %>%
                            dplyr::select(.data$cohortId, .data$shortName),
                          by = "cohortId") %>%
        dplyr::arrange(.data$shortName, .data$databaseId)
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
      return(data)
    })
  
  ##getCohortCountDataSubjectRecord----
  getCohortCountDataSubjectRecord <- shiny::reactive(x = {
    data <- getCohortCountDataForSelectedDatabaseIdsCohortIds()
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
  
  #!!!!!!!!!! bug inclusion rule is not workin\
  #!!!!!!!!! no down load button
  #!!!!!!!!! no radio button for records/subjects
  ##output: cohortCountsTable----
  output$cohortCountsTable <- DT::renderDataTable(expr = {
    validate(need(
      length(consolidatedDatabaseIdTarget()) > 0,
      "No data sources chosen"
    ))
    validate(need(
      length(consolidatedCohortIdTarget()) > 0,
      "No cohorts chosen"
    ))
    data <- getCohortCountDataForSelectedDatabaseIdsCohortIds()
    validate(need(all(doesObjectHaveData(data)),
      "No data for the combination"
    ))
    
    maxValueSubjects <- getMaxValueForStringMatchedColumnsInDataFrame(data = data, string = "ubjects")
    maxValueEntries <- getMaxValueForStringMatchedColumnsInDataFrame(data = data, string = "ntries")
    databaseIds <- sort(unique(data$databaseId))
    
    if (input$cohortCountsTableColumnFilter == "Both") {
      table <- getCohortCountDataSubjectRecord()
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
    } else if (input$cohortCountsTableColumnFilter == "Subjects Only" ||
          input$cohortCountsTableColumnFilter == "Records Only") {
        if (input$cohortCountsTableColumnFilter == "Subjects Only") {
          maxValue <- maxValueSubjects
          table <- getCohortCountDataSubject()
        } else {
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
    content = function(file) {
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
  
  ###getCohortIdFromSelectedRowInCohortCountTable----
  getCohortIdFromSelectedRowInCohortCountTable <- reactive({
    idx <- input$cohortCountsTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      if (!doesObjectHaveData(getCohortCountDataForSelectedDatabaseIdsCohortIds())) {
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
    DT::renderDataTable(expr = {
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
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = 1,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$inclusionRuleShowAsPercentInCohortCount #!!!!!!!! will need changes to minimumCellCountDefs function to support percentage
      )
      return(table)
    }, server = TRUE)
  
  #______________----
  # Incidence rate -------
  ##reactive: getIncidenceRateData----
  getIncidenceRateData <- reactive({
    if (any(is.null(input$tabs), 
            !input$tabs == "incidenceRate")) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Getting incidence rate data."),
                 value = 0)
    data <- getResultsIncidenceRate(dataSource = dataSource,
                                    cohortId =  consolidatedCohortIdTarget(), 
                                    databaseId = consolidatedDatabaseIdTarget())
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::mutate(incidenceRate = dplyr::case_when(.data$incidenceRate < 0 ~ 0,
                                                     TRUE ~ .data$incidenceRate))
    return(data)
  })
  
  ##pickerInput - incidenceRateAgeFilter----
  shiny::observe({
    if (!doesObjectHaveData(getIncidenceRateData())) {
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
    if (!doesObjectHaveData(getIncidenceRateData())) {
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
    if (!doesObjectHaveData(getIncidenceRateData())) {
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
      if (!doesObjectHaveData(getIncidenceRateData())) {
        return(NULL)
      }
      if (!doesObjectHaveData(input$incidenceRateCalendarFilter)) {
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
    if (!doesObjectHaveData(input$irStratification)) {
      return(NULL)
    }
    if (!doesObjectHaveData(getIncidenceRateCalendarYears())) {
      return(NULL)
    }
    if (!doesObjectHaveData(input$minPersonYear)) {
      return(NULL)
    }
    if (!doesObjectHaveData(input$minSubjetCount)) {
      return(NULL)
    }
    if (!doesObjectHaveData(incidenceRateAgeFilterValues())) {
      return(NULL)
    }
    
    data <- getIncidenceRateData()
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    
    data <- data %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget())
    if (!doesObjectHaveData(data)) {
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
    
    if (!doesObjectHaveData(data)) {
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
      if (doesObjectHaveData(getIncidenceRateCalendarYears())) {
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
    if (!doesObjectHaveData(data)) {
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
    validate(need(doesObjectHaveData(data),
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
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    calendarIntervalFirstLetter <-
      tolower(substr(input$timeSeriesAggregationPeriodSelection, 1, 1))
    
    data <- data[[calendarIntervalFirstLetter]]
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
    
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ##reactive: getTimeSeriesDescription----
  getTimeSeriesDescription <- shiny::reactive({
    data <- getFixedTimeSeriesTsibble()
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    if (!doesObjectHaveData(input$timeSeriesAggregationPeriodSelection)) {
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    if (!doesObjectHaveData(input$timeSeriesAggregationPeriodSelection)) {
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
    if (!doesObjectHaveData(getTimeSeriesColumnNameCrosswalk())) {
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    data <- data[[calendarIntervalFirstLetter]]
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(input$timeSeriesTypeFilter)) {
      return(NULL)
    }
    timeSeriesDescription <- getTimeSeriesDescription()
    if (!doesObjectHaveData(timeSeriesDescription)) {
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
    if (!doesObjectHaveData(input$timeSeriesTypeFilter)) {
      return(NULL)
    }
    timeSeriesColumnNameCrosswalk <- getTimeSeriesColumnNameCrosswalk()
    if (!doesObjectHaveData(timeSeriesColumnNameCrosswalk)) {
      return(NULL)
    }
    timeSeriesDescription <- getTimeSeriesDescription()
    if (!doesObjectHaveData(timeSeriesDescription)) {
      return(NULL)
    }
    if (!doesObjectHaveData(input$timeSeriesPlotFilters)) {
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
  output$fixedTimeSeriesTable <- DT::renderDataTable({
    validate(need(doesObjectHaveData(input$timeSeriesTypeFilter),
                  "Please select time series type."
    ))
    
    data <- getFixedTimeSeriesDataForTable()
    validate(need(doesObjectHaveData(data),
      "No timeseries data for the cohort of this series type"
    ))
    
    if (nrow(data) > 20) {
      scrollHeight <- "40vh"
    } else {
      scrollHeight <- TRUE
    }
   
    if (input$timeSeriesTypeFilter == "Percent of Subjects among persons in period") {
      columnDef <- list(minCellPercentDef(4:13))
    } else {
      columnDef <- list(minCellCountDef(4:13))
    }
    options = list(
      pageLength = 100,
      lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
      searching = TRUE,
      ordering = TRUE,
      paging = TRUE,
      scrollX = TRUE,
      scrollY = scrollHeight,
      info = TRUE,
      searchHighlight = TRUE,
      columnDefs = columnDef
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
  output$fixedTimeSeriesPlot <- plotly::renderPlotly ({
    validate(need(doesObjectHaveData(input$timeSeriesTypeFilter),
      "Please select time series type."
    ))
    data <- getFixedTimeSeriesDataForPlot()
    validate(need(doesObjectHaveData(data),
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
    if (any(is.null(input$tabs),!input$tabs == "timeDistribution")) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"),
            !exists('timeDistribution'))) {
      return(NULL)
    }
    data <- getResultsTimeDistribution(
      dataSource = dataSource,
      cohortId =  consolidatedCohortIdTarget(),
      databaseId = consolidatedDatabaseIdTarget()
    )
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  getTimeDistributionTableData <- reactive({
    data <- getTimeDistributionData()
    if (!doesObjectHaveData(data)) {
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
    filename = function() {
      getCsvFileNameWithDateTime(string = "timeDistribution")
    },
    content = function(file) {
      downloadCsv(x = getTimeDistributionTableData(),
                  fileName = file)
    }
  )
  
  ##output: timeDistributionTable----
  output$timeDistributionTable <- DT::renderDataTable(expr = {
    data <- getTimeDistributionTableData()
    validate(need(doesObjectHaveData(data), 
             "No data available for selected combination."))
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
  output$timeDistributionPlot <- plotly::renderPlotly(expr = {
    validate(need(doesObjectHaveData(consolidatedDatabaseIdTarget()),
      "No data sources chosen"))
    data <- getTimeDistributionData()
    validate(need(doesObjectHaveData(data), 
                  "No data for this combination"))
    plot <- plotTimeDistribution(data = data, shortNameRef = cohort)
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
    if (!doesObjectHaveData(indexEventBreakdown)) {
      return(NULL)
    }
    return(indexEventBreakdown)
  })
  
  
  ##getIndexEventBreakdownConceptIdDetails----
  getIndexEventBreakdownConceptIdDetails <- shiny::reactive(x = {
    if (any(
      is.null(input$tabs),
      !input$tabs == "indexEventBreakdown",
      !doesObjectHaveData(getIndexEventBreakdownRawTarget())
    )) {
      return(NULL)
    }
    conceptIdDetails <- getConcept(dataSource = dataSource,
                                   conceptIds = c(getIndexEventBreakdownRawTarget()$conceptId,
                                                  getIndexEventBreakdownRawTarget()$coConceptId) %>% unique())
    if (!doesObjectHaveData(conceptIdDetails)) {
      return(NULL)
    }
    return(conceptIdDetails)
  })
  
  ##getIndexEventBreakdownTargetData----
  getIndexEventBreakdownTargetData <- shiny::reactive(x = {
    if (any(
      is.null(input$tabs),
      !input$tabs == "indexEventBreakdown",
      !doesObjectHaveData(getIndexEventBreakdownRawTarget())
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(input$indexEventDomainNameFilter)) {
      return(NULL)
    }
    if (!doesObjectHaveData(input$indexEventBreakdownTableRadioButton)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$domainId %in% input$indexEventDomainNameFilter)
    if (length(input$indexEventBreakdownTableRadioButton) > 0) {
      conceptIdsToFilter <- c()
      if ("Resolved" %in% c(input$indexEventBreakdownTableRadioButton)) {
        if (doesObjectHaveData(getResolvedConceptsTarget())) {
          conceptIdsToFilter <- c(conceptIdsToFilter,
                                  getResolvedConceptsTarget()$conceptId) %>%
            unique()
        }
      }
      if ("Mapped" %in% c(input$indexEventBreakdownTableRadioButton)) {
        if (doesObjectHaveData(getMappedConceptsTarget())) {
          conceptIdsToFilter <- c(conceptIdsToFilter,
                                  getMappedConceptsTarget()$conceptId) %>%
            unique()
        }
      }
      if ("Excluded" %in% c(input$indexEventBreakdownTableRadioButton)) {
        if (doesObjectHaveData(getExcludedConceptsTarget())) {
          conceptIdsToFilter <- c(conceptIdsToFilter,
                                  getExcludedConceptsTarget()$conceptId) %>%
            unique()
        }
      }
      if ("Recommended" %in% c(input$indexEventBreakdownTableRadioButton)) {
        if (doesObjectHaveData(getOrphanConceptsTarget())) {
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
        doesObjectHaveData(conceptIdsToFilter),
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
  
  ##output: saveBreakdownTable----
  output$saveBreakdownTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "indexEventBreakdown")
    },
    content = function(file) {
      downloadCsv(x = getIndexEventBreakdownTargetDataFiltered(),
                  fileName = file)
    }
  )
  
  ##output: indexEventBreakdownTable----
  output$indexEventBreakdownTable <-
    DT::renderDataTable(expr = {
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
          doesObjectHaveData(data),
          "No index event breakdown data for the chosen combination."
        )
      )
      keyColumnFields <-
        c("conceptId", "conceptName", "vocabularyId", "standardConcept")
      #depending on user selection - what data Column Fields Will Be Presented?
      dataColumnFields <-
        c("persons",
          "records")
      if (input$indexEventBreakdownTableFilter == "Both") {
        dataColumnFields <- dataColumnFields
        sketchLevel <- 2
      } else if (input$indexEventBreakdownTableFilter == "Person Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(string = tolower(dataColumnFields),
                                               pattern = tolower("person"))]
        sketchLevel <- 1
      } else if (input$indexEventBreakdownTableFilter == "Record Only") {
        dataColumnFields <-
          dataColumnFields[stringr::str_detect(string = tolower(dataColumnFields),
                                               pattern = tolower("record"))]
        sketchLevel <- 1
      }
      
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = "Cohort Level",
          fields = input$indexEventBreakdownTableFilter
        )
      
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                      string = dataColumnFields)
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = sketchLevel,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showResultsAsPercent = input$indexEventBreakdownShowAsPercent
      )
      
      return(table)
    }, server = TRUE)
  
  
  ##getIndexEventBreakdownPlotData----
  getIndexEventBreakdownPlotData <- shiny::reactive(x = {
    if (!doesObjectHaveData(getIndexEventBreakdownRawTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(getIndexEventBreakdownTargetDataFiltered())) {
      return(NULL)
    }
    
    filteredConceptIds <-
      getIndexEventBreakdownTargetDataFiltered()$conceptId %>% unique()
    if (!doesObjectHaveData(filteredConceptIds)) {
      return(NULL)
    }
    
    data <- getIndexEventBreakdownRawTarget() %>%
      dplyr::filter(.data$coConceptId == 0) %>%
      dplyr::filter(.data$conceptId %in% c(filteredConceptIds)) %>% 
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>% 
      dplyr::select(.data$databaseId,
                    .data$cohortId,
                    .data$conceptId,
                    .data$daysRelativeIndex,
                    .data$conceptCount,
                    .data$subjectCount)
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    ## creating order
    data <- data %>% 
      dplyr::inner_join(data %>% 
                          dplyr::filter(.data$daysRelativeIndex == 0) %>% 
                          dplyr::arrange(.data$databaseId,
                                         .data$cohortId,
                                         .data$conceptId,
                                         dplyr::desc(.data$subjectCount),
                                         dplyr::desc(.data$conceptCount)) %>% 
                          dplyr::mutate(rank = dplyr::row_number()) %>% 
                          dplyr::select(.data$databaseId,
                                        .data$cohortId,
                                        .data$conceptId,
                                        .data$rank),
                        by = c("databaseId", "cohortId", "conceptId"))
    return(data)
  })
  
  
  ##indexEventBreakdownPlot----
  output$indexEventBreakdownPlot <-
    plotly::renderPlotly({
      validate(need(
        doesObjectHaveData(getIndexEventBreakdownRawTarget()),
        "No index event breakdown data for the chosen combination."
      ))
      validate(
        need(
          doesObjectHaveData(getIndexEventBreakdownTargetDataFiltered()),
          "No index event breakdown data for the chosen combination. Maybe the concept id in the selected concept id is too restrictive?"
        )
      )
      
      data <- getIndexEventBreakdownPlotData()
      validate(need(
        doesObjectHaveData(data),
        "No index event breakdown data for the chosen combination."
      ))
      
      if (input$indexEventBreakdownTableFilter == "Both") {
        dataColumnFields <- c('conceptCount','subjectCount')
      } else if (input$indexEventBreakdownTableFilter == "Person Only") {
        dataColumnFields <- c('subjectCount')
      } else if (input$indexEventBreakdownTableFilter == "Record Only") {
        dataColumnFields <- c('conceptCount')
      }
      
      
      
      
      #!!!put a UI for user to select concept id's between minValue and maxValue -- by default minValue = 0 to maxValue = 10
      minValue <- 0
      maxValue <- 10
      data <- data %>% 
        dplyr::filter(.data$rank >= !!minValue) %>% 
        dplyr::filter(.data$rank <= !!maxValue)
        
      plot <- plotIndexEventBreakdown(data = data,
                                      yAxisColumns = dataColumnFields,
                                      showAsPercentage = input$indexEventBreakdownShowAsPercent,
                                      logTransform = input$indexEventBreakdownShowLogTransform,
                                      cohort = cohort,
                                      database = database,
                                      colorReference = colorReference,
                                      conceptIdDetails = getIndexEventBreakdownConceptIdDetails())
      return(plot)
    })
  
  ##output: conceptSetSynonymsForIndexEventBreakdown----
  output$conceptSetSynonymsForIndexEventBreakdown <- shiny::renderUI(expr = {
    data <- getConceptSetSynonymsHtmlTextString()
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  #!!!!!!!! should be same as cohort - code duplication
  ##output: conceptBrowserTableForIndexEvent----
  output$conceptBrowserTableForIndexEvent <- DT::renderDT(expr = {
    if (doesObjectHaveData(consolidateCohortDefinitionActiveSideTarget())) {
      conceptId <- consolidatedConceptIdTarget()
    }
    data <- conceptSetBrowserData()
    validate(need(
      doesObjectHaveData(data),
      "No information for selected concept id."
    ))

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
    if (input$indexEventBreakdownTableFilter == "Both") {
      dataColumnFields <- dataColumnFields
      sketchLevel <- 2
    } else if (input$indexEventBreakdownTableFilter == "Person Only") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(
          string = tolower(dataColumnFields),
          pattern = tolower("person")
        )]
      sketchLevel <- 1
    } else if (input$indexEventBreakdownTableFilter == "Record Only") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(
          string = tolower(dataColumnFields),
          pattern = tolower("record")
        )]
      sketchLevel <- 1
    }
    
    countsForHeader <-
      getCountsForHeaderForUseInDataTable(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds = consolidatedCohortIdTarget(),
        source = "Cohort Level",
        fields = input$indexEventBreakdownTableFilter
      )
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    table <- getDtWithColumnsGroupedByDatabaseId(
      data = data,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      sketchLevel = sketchLevel,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showResultsAsPercent = input$indexEventBreakdownShowAsPercent
    )
    
    return(table)
  })
  
  ##output: conceptSetTimeSeriesPlotForIndexEvent----
  output$conceptSetTimeSeriesPlotForIndexEvent <-
    plotly::renderPlotly({
      data <- getMetadataForConceptId()
      validate(need(
        doesObjectHaveData(data),
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
      tsibbleDataFromSTLModel <- getStlModelOutputForTsibbleDataValueFields(tsibbleData = data,
                                                                            valueFields = c("records", "persons"))

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
      return(plot)
    })
  
  ##getCoCOnceptForIndexEvent----
  getCoCOnceptForIndexEvent <- shiny::reactive(x = {
    if (!doesObjectHaveData(getIndexEventBreakdownRawTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(getIndexEventBreakdownConceptIdDetails())) {
      return(NULL)
    }
    data <- getIndexEventBreakdownRawTarget() %>% 
      dplyr::filter(.data$daysRelativeIndex == 0) %>% 
      dplyr::filter(.data$conceptId %in% c(activeSelected()$conceptId)) %>% 
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>% 
      dplyr::inner_join(getIndexEventBreakdownConceptIdDetails() %>% 
                          dplyr::select(.data$conceptId,
                                        .data$conceptName,
                                        .data$vocabularyId,
                                        .data$standardConcept), 
                        by = c("coConceptId" = "conceptId")) %>% 
      dplyr::rename("persons" = .data$subjectCount,
                    "records" = .data$conceptCount)
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
  })
  
  output$coConceptTableForIndexEvent <- DT::renderDataTable(expr = {
    data <- getCoCOnceptForIndexEvent()
    
    validate(need(
      doesObjectHaveData(data),
      "No information for selected concept id."
    ))
    
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
      sketchLevel <- 2
    } else if (input$indexEventBreakdownTableFilter == "Person Only") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(string = tolower(dataColumnFields),
                                             pattern = tolower("person"))]
      sketchLevel <- 1
    } else if (input$indexEventBreakdownTableFilter == "Record Only") {
      dataColumnFields <-
        dataColumnFields[stringr::str_detect(string = tolower(dataColumnFields),
                                             pattern = tolower("record"))]
      sketchLevel <- 1
    }
    
    countsForHeader <-
      getCountsForHeaderForUseInDataTable(
        dataSource = dataSource,
        databaseIds = consolidatedDatabaseIdTarget(),
        cohortIds = consolidatedCohortIdTarget(),
        source = "Cohort Level",
        fields = input$indexEventBreakdownTableFilter
      )
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    table <- getDtWithColumnsGroupedByDatabaseId(
      data = data,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      sketchLevel = sketchLevel,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showResultsAsPercent = input$indexEventBreakdownShowAsPercent
    )
    
    return(table)
  }, server = TRUE)
  
  output$saveDetailsOfSelectedConceptIdForIndexEvent <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "indexEventBreakdownConceptSetBrowser")
    },
    content = function(file) {
      downloadCsv(x = conceptSetBrowserData(),
                  fileName = file)
    }
  )
  
  #______________----
  # Visit Context -----
  ##getVisitContextData----
  getVisitContextData <- shiny::reactive(x = {
    if (all(doesObjectHaveData(input$tab),
            input$tab != "visitContext")) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"), !exists('visitContext'))) {
      return(NULL)
    }
    visitContext <-
      getResultsVisitContext(dataSource = dataSource,
                             cohortId = consolidatedCohortIdTarget(),
                             consolidatedDatabaseIdTarget())
    if (!doesObjectHaveData(visitContext)) {
      return(NULL)
    }
    return(visitContext)
  })
  
  ##getVisitContexDataEnhanced----
  getVisitContexDataEnhanced <- shiny::reactive(x = {
    if (!doesObjectHaveData(cohortCount)) {
      return(NULL)
    }
    if (input$tabs != "visitContext") {
      return(NULL)
    }
    visitContextData <- getVisitContextData()
    if (!doesObjectHaveData(visitContextData)) {
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
    if (!doesObjectHaveData(visitContextData)) {
      return(NULL)
    }
    visitContextData <- visitContextData %>% 
      tidyr::pivot_wider(id_cols = c("databaseId", "visitConceptName"), 
                         names_from = "visitContext", 
                         values_from = c("subjects", "records"))
    return(visitContextData)
  })

  ##saveVisitContextTable----
  output$saveVisitContextTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "visitContext")
    },
    content = function(file) {
      downloadCsv(x = getVisitContexDataEnhanced(),
                  fileName = file)
    }
  )
  
  ##doesVisitContextContainData----
  output$doesVisitContextContainData <- shiny::reactive({
    visitContextData <- getVisitContexDataEnhanced()
    if (!doesObjectHaveData(visitContextData)) {
      return(NULL)
    }
    return(nrow(visitContextData) > 0)
  })
  shiny::outputOptions(output,
                       "doesVisitContextContainData",
                       suspendWhenHidden = FALSE)
  
  ##visitContextTable----
  output$visitContextTable <- DT::renderDataTable(expr = {
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
      doesObjectHaveData(data),
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
    
    if (input$visitContextPersonOrRecords == "Person Only") {
      dataColumnFields <- dataColumnFields[stringr::str_detect(string = dataColumnFields,
                                                               pattern = "subjects")]
      data <- data %>% 
        dplyr::select(dplyr::all_of(keyColumnFields), "databaseId", dplyr::all_of(dataColumnFields))
      colnames(data) <- colnames(data) %>% 
        stringr::str_replace_all(pattern = stringr::fixed("subjects_"), replacement = "") 
      dataColumnFields <- dataColumnFields %>% 
        stringr::str_replace_all(pattern = stringr::fixed("subjects_"), replacement = "")
    } else if (input$visitContextPersonOrRecords == "Record Only") {
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
    
    maxCountValue <-
      getMaxValueForStringMatchedColumnsInDataFrame(data = data,
                                                    string = dataColumnFields)
    
    table <- getDtWithColumnsGroupedByDatabaseId(
      data = data,
      headerCount = countsForHeader,
      keyColumns = keyColumnFields,
      sketchLevel = 1,
      dataColumns = dataColumnFields,
      maxCount = maxCountValue,
      showResultsAsPercent = (input$visitContextValueFilter == "Percentage")
    )
    return(table)
  }, server = TRUE)
  
  
  #______________----
  # Cohort Overlap ------
  ##cohortOverlapData----
  cohortOverlapData <- reactive({
    if (any(
      !doesObjectHaveData(consolidatedDatabaseIdTarget()),
      !doesObjectHaveData(consolidatedCohortIdTarget()),
      !doesObjectHaveData(getComparatorCohortIdFromSelectedCompoundCohortNames())
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  #______________----
  # Cohort Overlap filtered ------
  ##cohortOverlapDataFiltered----
  cohortOverlapDataFiltered <- reactive({
    if (!doesObjectHaveData(cohortOverlapData())) {
      return(NULL)
    }
    return(cohortOverlapData())
  })

  ###output: isCohortDefinitionRowSelected----
  output$doesCohortAndComparatorsAreSingleSelected <- reactive({
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
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
      doesObjectHaveData(data),
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
  output$cohortOverlapTable <- DT::renderDataTable(expr = {
    data <- cohortOverlapDataFiltered()
    validate(need(
      !is.null(data),
      paste0("No cohort overlap data for this combination")
    ))
    validate(need(
      nrow(data) > 0,
      paste0("No cohort overlap data for this combination.")
    ))
    if (nrow(data) > 25) {
      scrollY <- '50vh'
    } else {
      scrollY <- TRUE
    }
    
    options = list(
      pageLength = 1000,
      searching = TRUE,
      scrollX = TRUE,
      scrollY = scrollY,
      lengthChange = TRUE,
      ordering = FALSE,
      paging = TRUE,
      columnDefs = list(minCellCountDef(3:10))
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
   
  })
  
  ##output: saveCohortOverlapTable----
  output$saveCohortOverlapTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "cohortOverlap")
    },
    content = function(file) {
      downloadCsv(x = cohortOverlapData(),
                  fileName = file)
    }
  )
  
  #______________----
  # Characterization/Temporal Characterization ------
  ##Shared----
  ###getMultipleCharacterizationDataTarget----
  getMultipleCharacterizationDataTarget <-
    shiny::reactive(x = {
      if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (all(is(dataSource, "environment"), !any(
        exists('covariateValue'),
        exists('temporalCovariateValue')
      ))) {
        return(NULL)
      }
      if (any(length(consolidatedCohortIdTarget()) != 1)) {
        return(NULL)
      }
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
        cohortId = c(consolidatedCohortIdTarget()) %>% unique()
      )
      if (!doesObjectHaveData(data$analysisRef)) {
        return(NULL)
      }
      if (!doesObjectHaveData(data$covariateValue)) {
        return(NULL)
      }
      return(data)
    })
  
  ###getMultipleCharacterizationDataComparator----
  getMultipleCharacterizationDataComparator <-
    shiny::reactive(x = {
      if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
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
      if (length(consolidatedCohortIdComparator()) > 0) {
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
        cohortId = c(consolidatedCohortIdComparator()) %>% unique()
      )
      if (!doesObjectHaveData(data$analysisRef)) {
        return(NULL)
      }
      if (!doesObjectHaveData(data$covariateValue)) {
        return(NULL)
      }
      return(data)
    })
  
  ##getMultipleCharacterizationData----
  getMultipleCharacterizationData <- shiny::reactive(x = {
    if (!input$tabs == "cohortCharacterization") {
      return(NULL)
    }
    if (!doesObjectHaveData(getMultipleCharacterizationDataTarget())) {
      return(NULL)
    }
    dataTarget <- getMultipleCharacterizationDataTarget()
    if (!doesObjectHaveData(dataTarget)) {
      return(NULL)
    }
    dataComparator <- getMultipleCharacterizationDataComparator()
    if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
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
  
  ###getDomainOptionsForCharacterization----
  getDomainOptionsForCharacterization <- shiny::reactive({
    if (!exists("analysisRef")) {
      return(NULL)
    }
    if (!doesObjectHaveData(analysisRef)) {
      return(NULL)
    }
    data <- c(analysisRef$domainId %>% 
      unique(), "Cohort") %>% sort()
    return(data)
  })
  
  # ###getAnalysisNameOptionsForCharacterization----
  # getAnalysisNameOptionsForCharacterization <- shiny::reactive({
  #   if (!exists("analysisRef")) {
  #     return(NULL)
  #   }
  #   if (!doesObjectHaveData(analysisRef)) {
  #     return(NULL)
  #   }
  #   data <- analysisRef$analysisName %>% 
  #     unique() %>% 
  #     sort()
  #   return(data)
  # })
  
  ###Update: characterizationDomainNameOptions----
  shiny::observe({
    subset <- getDomainOptionsForCharacterization()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "characterizationDomainNameOptions",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = subset
    )
  })
  
  # ###Update: characterizationAnalysisNameOptions----
  # shiny::observe({
  #   subset <- getAnalysisNameOptionsForCharacterization()
  #   shinyWidgets::updatePickerInput(
  #     session = session,
  #     inputId = "characterizationAnalysisNameOptions",
  #     choicesOpt = list(style = rep_len("color: black;", 999)),
  #     choices = subset,
  #     selected = subset
  #   )
  # })
  
  ##Characterization----
  ### getCharacterizationDataFiltered ----
  getCharacterizationDataFiltered <- shiny::reactive(x = {
    if (input$tabs != "cohortCharacterization") {
      return(NULL)
    }
    if (!doesObjectHaveData(getMultipleCharacterizationData())) {
      return(NULL)
    }
    if (is.null(getMultipleCharacterizationData()$covariateRef)) {
      warning("No covariate reference data found")
      return(NULL)
    }
    if (is.null(getMultipleCharacterizationData()$covariateValue)) {
      return(NULL)
    }
    if (is.null(getMultipleCharacterizationData()$analysisRef)) {
      warning("No analysis ref data found")
      return(NULL)
    }
    analysisIdToFilter <- getMultipleCharacterizationData()$analysisRef %>% 
      dplyr::filter(.data$domainId %in% c(input$characterizationDomainNameOptions)) %>% 
      dplyr::pull(.data$analysisId) %>% 
      unique()
    covariatesTofilter <-
      getMultipleCharacterizationData()$covariateRef %>% 
      dplyr::filter(.data$analysisId %in% c(analysisIdToFilter))
    if (!doesObjectHaveData(covariatesTofilter)) {
      return(NULL)
    }
    if (all(
      doesObjectHaveData(input$conceptSetsSelectedTargetCohort),
      doesObjectHaveData(getResolvedConceptsAllData())
    )) {
      covariatesTofilter <- covariatesTofilter  %>%
        dplyr::inner_join(
          conceptSets %>% 
            dplyr::filter(.data$compoundName %in% c(input$conceptSetsSelectedTargetCohort)) %>% 
            dplyr::select(.data$cohortId, .data$conceptSetId) %>% 
            dplyr::inner_join(getResolvedConceptsAllData() %>% 
                                dplyr::filter(.data$databaseId %in% c(consolidatedDatabaseIdTarget())) %>% 
                                dplyr::select(.data$cohortId, .data$conceptSetId, .data$conceptId) %>% 
                                dplyr::distinct(),
                              by = c("cohortId", "conceptSetId")) %>%
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
        dplyr::filter(.data$analysisId %in% c(prettyAnalysisIds))
      characterizationDataValue <-
        characterizationDataValue %>%
        dplyr::inner_join(covariatesTofilter,
                          by = c('covariateId', 'characterizationSource')) %>%
        dplyr::filter(is.na(.data$startDay) |
                        (.data$startDay == -365 & .data$endDay == 0)) %>% 
        dplyr::inner_join(
          getMultipleCharacterizationData()$analysisRef,
          by = c('analysisId', 'characterizationSource')
        )
      #prettyAnalysisIds this is global variable
    } else {
      characterizationDataValue <-
        characterizationDataValue %>%
        dplyr::inner_join(covariatesTofilter,
                          by = c('covariateId', 'characterizationSource'))
      characterizationDataValueTimeVarying <- characterizationDataValue %>% 
        dplyr::filter(!is.na(.data$startDay)) %>% 
        dplyr::inner_join(temporalCovariateChoices, 
                          by = c("startDay", "endDay")) %>% 
        dplyr::inner_join(
          getMultipleCharacterizationData()$analysisRef,
          by = c('analysisId', 'characterizationSource')
        ) 
      characterizationDataValueNonTimeVarying <- characterizationDataValue %>% 
        dplyr::filter(is.na(.data$startDay)) %>% 
        dplyr::inner_join(
          getMultipleCharacterizationData()$analysisRef,
          by = c('analysisId', 'characterizationSource')
        ) %>% 
        tidyr::crossing(characterizationDataValueTimeVarying %>% dplyr::select(.data$choices))
      characterizationDataValue <- dplyr::bind_rows(characterizationDataValueNonTimeVarying,
                                                    characterizationDataValueTimeVarying) %>% 
        dplyr::arrange(.data$databaseId,
                       .data$cohortId, 
                       .data$covariateId,
                       .data$choices)
    }
    
    #enhancement
    characterizationDataValue <- characterizationDataValue %>%
      dplyr::mutate(covariateNameShortCovariateId = .data$covariateName)
      # dplyr::mutate(covariateNameShort = gsub(".*: ", "", .data$covariateName)) %>%
      # dplyr::mutate(
      #   covariateNameShortCovariateId = paste0(.data$covariateNameShort,
      #                                          " (",
      #                                          .data$covariateId, ")")
      # )
    
    if (!doesObjectHaveData(characterizationDataValue)) {
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    table <- data %>%
      prepareTable1(prettyTable1Specifications = prettyTable1Specifications,
                    cohort = cohort)
    if (!doesObjectHaveData(table)) {
      return(NULL)
    }
    return(table)
  })
  
  ### Output: characterizationTable ------
  output$characterizationTable <- DT::renderDataTable(expr = {
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
      #!!!! if user selects proportion then mean, else count. Also support option for both as 34,342 (33.3%)
      keyColumnFields <- c("characteristic")
      dataColumnFields <- intersect(x = colnames(data),
                                    y = cohort$shortName)
      sketchLevel <- 1
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
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = sketchLevel,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        sort = FALSE,
        showResultsAsPercent = TRUE
      )
    } else {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0("Rendering raw table for cohort characterization."),
        value = 0
      )
      
      data <- getCharacterizationDataFiltered()
      validate(need(nrow(data) > 0,
                    "No data available for selected combination."))
      if (input$characterizationColumnFilters == "Mean only") {
        data <- data %>%
          dplyr::select(-.data$mean) %>%
          dplyr::rename("mean" = .data$sumValue)
        keyColumnFields <-
          c(
            "covariateId",
            "covariateName",
            "analysisName",
            "domainId",
            "choices",
            "characterizationSource"
          )
        dataColumnFields <- c("mean")
        showPercent <- TRUE
      } else {
        keyColumnFields <-
          c(
            "covariateId",
            "covariateName",
            "analysisName",
            "domainId",
            "choices",
            "characterizationSource"
          )
        dataColumnFields <- c("mean", "sd")
        showPercent <- FALSE
      }
      
      sketchLevel <- 1
      countsForHeader <-
        getCountsForHeaderForUseInDataTable(
          dataSource = dataSource,
          databaseIds = consolidatedDatabaseIdTarget(),
          cohortIds = consolidatedCohortIdTarget(),
          source = "Cohort Level",
          fields = "Events"
        )
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(data = data %>%
                                                        dplyr::select(-.data$missingMeansZero),
                                                      string = dataColumnFields)
      table <- getDtWithColumnsGroupedByDatabaseId(
        data = data,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        sketchLevel = sketchLevel,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        sort = TRUE,
        showResultsAsPercent = showPercent
      )
    }
    return(table)
  }, server = TRUE)
  
  ###saveCohortCharacterizationTable----
  output$saveCohortCharacterizationTable <-  downloadHandler(
    filename = function() {
      getCsvFileNameWithDateTime(string = "cohortCharacterization")
    },
    content = function(file) {
      if (input$charType == "Pretty") {
        data <- getCharacterizationTableDataPretty()
      } else {
        data <- getCharacterizationRawData()
      }
      downloadCsv(x = data,
                  fileName = file)
    }
  )
  
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
  #   if (!doesObjectHaveData(getMultipleCharacterizationData())) {
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
    #   if (any(!doesObjectHaveData(input$temporalCharacterizationDomainNameOptions),
    #           input$temporalCharacterizationDomainNameOptions == "")) {
    #     return(NULL)
    #   }
    #   if (any(!doesObjectHaveData(input$temporalCharacterizationAnalysisNameOptions),
    #           input$temporalCharacterizationAnalysisNameOptions == "")) {
    #     return(NULL)
    #   }
    #   data <- getTemporalCharacterizationData()
    #   if (!doesObjectHaveData(data)) {
    #     return(NULL)
    #   }
    #   
    #   if (all(
    #     doesObjectHaveData(input$conceptSetsSelectedTargetCohort),
    #     doesObjectHaveData(getResolvedConceptsAllData())
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
    #   if (!doesObjectHaveData(data)) {
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
  #   if (!doesObjectHaveData(data)) {
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
  #   if (!doesObjectHaveData(data)) {
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
  # ###saveTemporalCharacterizationTable----
  # output$saveTemporalCharacterizationTable <-  downloadHandler(
  #   filename = function() {
  #     getCsvFileNameWithDateTime(string = "getTemporalCharacterizationTableData")
  #   },
  #   content = function(file) {
  #     downloadCsv(x = getTemporalCharacterizationTableData(),
  #                 fileName = file)
  #   }
  # )
  
 
  # ## Compare Characterization/Temporal Characterization ------
  # ###Update: compareCharacterizationDomainNameFilter----
  # shiny::observe({
  #   subset <- getDomainOptionsForCharacterization()
  #   shinyWidgets::updatePickerInput(
  #     session = session,
  #     inputId = "compareCharacterizationDomainNameFilter",
  #     choicesOpt = list(style = rep_len("color: black;", 999)),
  #     choices = subset,
  #     selected = subset
  #   )
  # })
  # 
  # ###Update: compareCharacterizationAnalysisNameFilter----
  # shiny::observe({
  #   subset <- getAnalysisNameOptionsForCharacterization()
  #   shinyWidgets::updatePickerInput(
  #     session = session,
  #     inputId = "compareCharacterizationAnalysisNameFilter",
  #     choicesOpt = list(style = rep_len("color: black;", 999)),
  #     choices = subset,
  #     selected = subset
  #   )
  # })
  # 
  # ###Update: compareTemporalCharacterizationDomainNameFilter----
  # shiny::observe({
  #   subset <- getDomainOptionsForCharacterization()
  #   shinyWidgets::updatePickerInput(
  #     session = session,
  #     inputId = "compareTemporalCharacterizationDomainNameFilter",
  #     choicesOpt = list(style = rep_len("color: black;", 999)),
  #     choices = subset,
  #     selected = subset
  #   )
  # })
  # 
  # ###Update: compareTemporalCharacterizationAnalysisNameFilter----
  # shiny::observe({
  #   subset <- getAnalysisNameOptionsForCharacterization()
  #   shinyWidgets::updatePickerInput(
  #     session = session,
  #     inputId = "compareTemporalCharacterizationAnalysisNameFilter",
  #     choicesOpt = list(style = rep_len("color: black;", 999)),
  #     choices = subset,
  #     selected = subset
  #   )
  # })
  
  
# 
#   ### parseMultipleCompareCharacterizationData ------
#   parseMultipleCompareCharacterizationData <- shiny::reactive({
#     if (!input$tabs %in% c("compareCohortCharacterization",
#                            "compareTemporalCharacterization")) {
#       return(NULL)
#     }
#     if (!doesObjectHaveData(getMultipleCharacterizationData())) {
#       return(NULL)
#     }
#     if (!doesObjectHaveData(getMultipleCharacterizationData()$covariateRef)) {
#       warning("No covariate reference data found")
#       return(NULL)
#     }
#     if (!doesObjectHaveData(getMultipleCharacterizationData()$covariateValue)) {
#       return(NULL)
#     }
#     if (!doesObjectHaveData(getMultipleCharacterizationData()$analysisRef)) {
#       warning("No analysis reference data found")
#       return(NULL)
#     }
#     progress <- shiny::Progress$new()
#     on.exit(progress$close())
#     progress$set(message = "Parsing extracted characterization data",
#                  value = 0)
#     data <- getMultipleCharacterizationData()$covariateValue
#     if (input$tabs %in% c("compareCohortCharacterization")) {
#       browser()
#       data <- data %>%
#         dplyr::filter(.data$characterizationSource %in% c('C', 'F')) %>%
#         dplyr::select(-.data$timeId, -.data$startDay, -.data$endDay)
#     }
#     if (input$tabs %in% c("compareTemporalCharacterization")) {
#       browser()
#       data <- data %>%
#         dplyr::filter(.data$characterizationSource %in% c('CT', 'FT')) %>%
#         dplyr::filter(.data$timeId %in% getTimeIdsFromSelectedTemporalCovariateChoices()) %>%
#         dplyr::select(-.data$startDay, -.data$endDay)
#     }
#     data <- data %>%
#       dplyr::inner_join(
#         getMultipleCharacterizationData()$covariateRef,
#         by = c("covariateId", "characterizationSource")
#       ) %>%
#       dplyr::inner_join(
#         getMultipleCharacterizationData()$analysisRef ,
#         by = c("analysisId", "characterizationSource")
#       )
#     if (input$tabs %in% c("compareTemporalCharacterization")) {
#       browser()
#       data <- data %>%
#         dplyr::select(-.data$startDay, -.data$endDay) %>%
#         dplyr::distinct() %>%
#         dplyr::inner_join(getMultipleCharacterizationData()$temporalTimeRef,
#                           by = 'timeId') %>%
#         dplyr::inner_join(temporalCovariateChoices, by = 'timeId') %>%
#         dplyr::select(-.data$missingMeansZero)
#     }
#     
#     covs1 <- data %>%
#       dplyr::filter(.data$cohortId == consolidatedCohortIdTarget()) %>%
#       dplyr::mutate(
#         analysisNameLong = paste0(
#           .data$analysisName,
#           " (",
#           as.character(.data$startDay),
#           " to ",
#           as.character(.data$endDay),
#           ")"
#         )
#       ) %>%
#       dplyr::relocate(
#         .data$cohortId,
#         .data$databaseId,
#         .data$analysisId,
#         .data$covariateId,
#         .data$covariateName,
#         .data$isBinary
#       ) %>%
#       dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
#     if (!doesObjectHaveData(covs1)) {
#       return(NULL)
#     }
#     
#     covs2 <- data %>%
#       dplyr::filter(.data$cohortId == consolidatedCohortIdComparator()) %>%
#       dplyr::mutate(
#         analysisNameLong = paste0(
#           .data$analysisName,
#           " (",
#           as.character(.data$startDay),
#           " to ",
#           as.character(.data$endDay),
#           ")"
#         )
#       ) %>%
#       dplyr::relocate(
#         .data$cohortId,
#         .data$databaseId,
#         .data$analysisId,
#         .data$covariateId,
#         .data$covariateName,
#         .data$isBinary
#       ) %>%
#       dplyr::arrange(.data$cohortId, .data$databaseId, .data$covariateId)
#     if (!doesObjectHaveData(covs2)) {
#       return(NULL)
#     }
#     
#     if (input$tabs %in% c("compareCohortCharacterization")) {
#       balance <- compareCohortCharacteristics(covs1, covs2)
#     }
#     if (input$tabs %in% c("compareTemporalCharacterization")) {
#       balance <- compareTemporalCohortCharacteristics(covs1, covs2)
#     }
#     if (!doesObjectHaveData(balance)) {
#       return(NULL)
#     }
#     balance <- balance %>%
#       dplyr::mutate(absStdDiff = abs(.data$stdDiff))
#     return(balance)
#     # # enhancement
#     # data <-
#     #   compareTemporalCohortCharacteristics(covs1, covs2) %>%
#     #   dplyr::mutate(absStdDiff = abs(.data$stdDiff)) %>%
#     #   dplyr::mutate(covariateName = gsub(".*: ", "", .data$covariateName)) %>%
#     #   dplyr::mutate(covariateName = paste0(.data$covariateName, " (", .data$covariateId, ")"))
#     
#     # # enhanced
#     # balance <- balance %>%
#     #   dplyr::mutate(absStdDiff = abs(.data$stdDiff)) %>%
#     #   dplyr::rename(covariateNameFull = .data$covariateName) %>%
#     #   dplyr::mutate(covariateName = gsub(".*: ", "", .data$covariateNameFull)) %>%
#     #   dplyr::mutate(
#     #     covariateName = dplyr::case_when(
#     #       stringr::str_detect(
#     #         string = tolower(.data$covariateNameFull),
#     #         pattern = 'age group|gender'
#     #       ) ~ .data$covariateNameFull,
#     #       TRUE ~ gsub(".*: ", "", .data$covariateNameFull)
#     #     )
#     #   ) %>%
#     #   dplyr::mutate(covariateName = paste0(.data$covariateName, " (", .data$covariateId, ")"))
#   })
#   
#   ###parseMultipleCompareCharacterizationDataFiltered----
#   parseMultipleCompareCharacterizationDataFiltered <-  shiny::reactive({
#     if (!input$tabs %in% c("compareCohortCharacterization",
#                            "compareTemporalCharacterization")) {
#       return(NULL)
#     }
#     if (input$tabs %in% c("compareCohortCharacterization")) {
#       if (any(
#         !doesObjectHaveData(input$compareCharacterizationDomainNameFilter),
#         input$compareCharacterizationDomainNameFilter == ""
#       )) {
#         return(NULL)
#       }
#       if (any(
#         !doesObjectHaveData(input$compareCharacterizationAnalysisNameFilter),
#         input$compareCharacterizationAnalysisNameFilter == ""
#       )) {
#         return(NULL)
#       }
#     }
#     if (input$tabs %in% c("compareTemporalCharacterization")) {
#       if (any(
#         !doesObjectHaveData(input$compareTemporalCharacterizationDomainNameFilter),
#         input$compareTemporalCharacterizationDomainNameFilter == ""
#       )) {
#         return(NULL)
#       }
#       if (any(
#         !doesObjectHaveData(
#           input$compareTemporalCharacterizationAnalysisNameFilter
#         ),
#         input$compareTemporalCharacterizationAnalysisNameFilter == ""
#       )) {
#         return(NULL)
#       }
#     }
#     data <- parseMultipleCompareCharacterizationData()
#     if (!doesObjectHaveData(data)) {
#       return(NULL)
#     }
#     progress <- shiny::Progress$new()
#     on.exit(progress$close())
#     progress$set(message = "Filtering characterization data",
#                  value = 0)
#     if (input$compareCharacterizationProportionOrContinous == "Proportion") {
#       data <- data %>%
#         dplyr::filter(.data$isBinary == 'Y')
#     } else
#       if (input$compareCharacterizationProportionOrContinous == "Continuous") {
#         data <- data %>%
#           dplyr::filter(.data$isBinary == 'N')
#       }
#     if (all(
#       doesObjectHaveData(input$conceptSetsSelectedTargetCohort),
#       doesObjectHaveData(getResolvedConceptsAllData())
#     )) {
#       data <- data  %>%
#         dplyr::inner_join(
#           conceptSets %>% 
#             dplyr::filter(.data$compoundName %in% c(input$conceptSetsSelectedTargetCohort)) %>% 
#             dplyr::select(.data$cohortId, .data$conceptSetId) %>% 
#             dplyr::inner_join(getResolvedConceptsAllData() %>% 
#                                 dplyr::filter(.data$databaseId %in% c(consolidatedDatabaseIdTarget())) %>% 
#                                 dplyr::select(.data$cohortId, .data$conceptSetId, .data$conceptId) %>% 
#                                 dplyr::distinct(),
#                               by = c("cohortId", "conceptSetId")) %>%
#             dplyr::select(.data$conceptId) %>%
#             dplyr::distinct(),
#           by = c("conceptId")
#         )
#     }
#     if (!doesObjectHaveData(data)) {
#       return(NULL)
#     }
#     
#     if (input$tabs %in% c("compareCohortCharacterization")) {
#       data <- data %>%
#         dplyr::filter(.data$domainId  %in% input$compareCharacterizationDomainNameFilter) %>%
#         dplyr::filter(.data$analysisName  %in% input$compareCharacterizationAnalysisNameFilter)
#     }
#     if (input$tabs %in% c("compareTemporalCharacterization")) {
#       data <- data %>%
#         dplyr::filter(.data$domainId %in% input$compareTemporalCharacterizationDomainNameFilter) %>%
#         dplyr::filter(
#           .data$analysisName %in% input$compareTemporalCharacterizationAnalysisNameFilter
#         )
#     }
#     return(data)
#   })
#   
#   ## Compare Characterization ----
#   ###getCompareCharacterizationTablePretty----
#   getCompareCharacterizationTablePretty <- shiny::reactive({
#     if (input$tabs != "compareCohortCharacterization") {
#       return(NULL)
#     }
#     data <- parseMultipleCompareCharacterizationDataFiltered()
#     if (!doesObjectHaveData(data)) {
#       return(NULL)
#     }
#     progress <- shiny::Progress$new()
#     on.exit(progress$close())
#     progress$set(message = "Rendering pretty table",
#                  value = 0)
#     data <- prepareTable1Comp(balance = data)
#     if (!doesObjectHaveData(data)) {
#       return(NULL)
#     }
#     data <- data %>%
#       dplyr::arrange(.data$sortOrder) %>%
#       dplyr::select(-.data$sortOrder) %>%
#       dplyr::select(-.data$cohortId1, -.data$cohortId2)
#     
#     return(data)
#   })
#   
#   ###getCompareCharacterizationTableRaw----
#   getCompareCharacterizationTableRaw <- shiny::reactive({
#     if (input$tabs != "compareCohortCharacterization") {
#       return(NULL)
#     }
#     
#     data <- parseMultipleCompareCharacterizationDataFiltered()
#     if (!doesObjectHaveData(data)) {
#       return(NULL)
#     }
#     # enhancement
#     data <- data %>%
#       dplyr::rename(
#         "meanTarget" = mean1,
#         "sdTarget" = sd1,
#         "meanComparator" = mean2,
#         "sdComparator" = sd2,
#         "StdDiff" = absStdDiff
#       ) %>%
#       dplyr::select(
#         .data$covariateId,
#         .data$covariateName,
#         .data$meanTarget,
#         .data$sdTarget,
#         .data$meanComparator,
#         .data$sdComparator,
#         .data$StdDiff,
#         .data$databaseId
#       )
#   })
#   
#   ###output: compareCharacterizationTable----
#   output$compareCharacterizationTable <-
#     DT::renderDataTable(expr = {
#       if (input$tabs != "compareCohortCharacterization") {
#         return(NULL)
#       }
#       
#       balance <- parseMultipleCompareCharacterizationDataFiltered()
#       validate(need(
#         all(!is.null(balance), nrow(balance) > 0),
#         "No data available for selected combination."
#       ))
#       targetCohortIdValue <- balance %>%
#         dplyr::filter(!is.na(.data$cohortId1)) %>%
#         dplyr::pull(.data$cohortId1) %>%
#         unique()
#       comparatorcohortIdValue <- balance %>%
#         dplyr::filter(!is.na(.data$cohortId2)) %>%
#         dplyr::pull(.data$cohortId2) %>%
#         unique()
#       databaseIdForCohortCharacterization <-
#         balance$databaseId %>%
#         unique()
#       
#       targetCohortShortName <- cohort %>%
#         dplyr::filter(.data$cohortId == !!targetCohortIdValue) %>%
#         dplyr::select(.data$shortName) %>%
#         dplyr::pull()
#       comparatorCohortShortName <- cohort %>%
#         dplyr::filter(.data$cohortId == !!comparatorcohortIdValue) %>%
#         dplyr::select(.data$shortName) %>%
#         dplyr::pull()
#       
#       targetCohortSubjects <- cohortCount %>%
#         dplyr::filter(.data$databaseId == databaseIdForCohortCharacterization) %>%
#         dplyr::filter(.data$cohortId == !!targetCohortIdValue) %>%
#         dplyr::pull(.data$cohortSubjects)
#       comparatorCohortSubjects <- cohortCount %>%
#         dplyr::filter(.data$databaseId == databaseIdForCohortCharacterization) %>%
#         dplyr::filter(.data$cohortId == !!comparatorcohortIdValue) %>%
#         dplyr::pull(.data$cohortSubjects)
#       
#       targetCohortHeader <- paste0(
#         targetCohortShortName,
#         " (n = ",
#         scales::comma(targetCohortSubjects,
#                       accuracy = 1),
#         ")"
#       )
#       comparatorCohortHeader <- paste0(
#         comparatorCohortShortName,
#         " (n = ",
#         scales::comma(comparatorCohortSubjects,
#                       accuracy = 1),
#         ")"
#       )
#       
#       if (input$characterizationCompareMethod == "Pretty table") {
#         progress <- shiny::Progress$new()
#         on.exit(progress$close())
#         progress$set(
#           message = paste0("Rendering pretty table for compare characterization."),
#           value = 0
#         )
#         
#         data <- getCompareCharacterizationTablePretty()
#         validate(need(nrow(data) > 0,
#                       "No data available for selected combination."))
#         
#         databaseIds <- data %>%
#           dplyr::select(.data$databaseId) %>%
#           dplyr::filter(.data$databaseId != "NA") %>%
#           dplyr::pull() %>% unique()
#         
#         table <- data %>%
#           tidyr::pivot_longer(
#             cols = c("MeanT",
#                      "MeanC",
#                      "StdDiff"),
#             names_to = "type",
#             values_to = "values"
#           ) %>%
#           dplyr::group_by(.data$type) %>%
#           dplyr::summarise(.data$characteristic,
#                            .data$databaseId,
#                            .data$type,
#                            .data$values) %>%
#           dplyr::mutate(names = paste0(.data$type, "", .data$databaseId)) %>%
#           dplyr::arrange(.data$databaseId, dplyr::desc(.data$names)) %>% 
#           dplyr::ungroup() %>%
#           tidyr::pivot_wider(id_cols = "characteristic",
#                              names_from = "names",
#                              values_from = "values") %>%
#           dplyr::select(-dplyr::contains("NA"))
#         
#         sketchColumns <- c(paste0("target - ",targetCohortHeader), 
#                            paste0("Comparator - ",comparatorCohortHeader), 
#                            "StdDiff")
#         sketchColspan <- 3
#         
#         columsDefs <- list(truncateStringDef(0, 80),
#                            minCellRealDef(1:(length(databaseIds) * 3), digits = 2))
#         colorBarColumns <- (1 + 1:(length(databaseIds) * 3))
#         
#         # table <- DT::formatRound(table, 4, digits = 2)
#       } else {
#         balance <-  getCompareCharacterizationTableRaw() 
#         progress <- shiny::Progress$new()
#         on.exit(progress$close())
#         progress$set(
#           message = paste0("Rendering raw table for compare characterization."),
#           value = 0
#         )
#         
#         databaseIds <- unique(balance$databaseId)
#         table <- balance  %>%
#           tidyr::pivot_longer(
#             cols = c(
#               "meanTarget",
#               "sdTarget",
#               "meanComparator",
#               "sdComparator",
#               "StdDiff"
#             ),
#             names_to = "type",
#             values_to = "values"
#           ) 
#         
#         if (input$compareCharacterizationColumnFilters == "Mean and Standard Deviation") {
#           sketchColumns <-
#             c("Mean Target",
#               "Sd Target",
#               "Mean Comarator",
#               "Sd Comparator",
#               "StdDiff")
#           sketchColspan <- 5
#           
#           columsDefs <- list(truncateStringDef(0, 80),
#                              minCellRealDef(1:(length(databaseIds) * 5), digits = 2))
#           
#           colorBarColumns <- (1 + 1:(length(databaseIds) * 5))
#           # standardDifferenceColumn <- 4
#           
#         } else {
#           table <- table %>%
#             dplyr::filter(
#                 .data$type == "meanTarget" |
#                 .data$type == "meanComparator" |
#                 .data$type == "StdDiff"
#             )
#           
#           sketchColumns <- c(paste0("target - ",targetCohortHeader), 
#                              paste0("Comparator - ",comparatorCohortHeader), 
#                              "StdDiff")
#           # sketchColumns <- c("Target", "Comparator", "StdDiff")
#           sketchColspan <- 3
#           
#           columsDefs <- list(truncateStringDef(0, 80),
#                              minCellRealDef(1:(length(databaseIds) * 3), digits = 2))
#           colorBarColumns <- (1 + 1:(length(databaseIds) * 3))
#         }
#         
#         table <- table %>%
#           dplyr::mutate(type = paste0(.data$type, " ", .data$databaseId)) %>% 
#           dplyr::arrange(.data$databaseId, dplyr::desc(.data$type)) %>%
#           tidyr::pivot_wider(
#             id_cols = c("covariateId", "covariateName"),
#             names_from = "type",
#             values_from = "values",
#             values_fill = 0
#           ) %>% 
#           dplyr::mutate(covariateName = paste0(.data$covariateName, "(", .data$covariateId, ")")) %>% 
#           dplyr::select(-.data$covariateId)
#       }
#       
#       sketch <- htmltools::withTags(table(class = "display",
#                                           thead(tr(
#                                             th(rowspan = 2, "Covariate Name"),
#                                             lapply(
#                                               databaseIds,
#                                               th,
#                                               colspan = sketchColspan,
#                                               class = "dt-center",
#                                               style = "border-right:1px solid silver;border-bottom:1px solid silver"
#                                             )
#                                           ),
#                                           tr(
#                                             lapply(rep(sketchColumns,
#                                                        length(databaseIds)),
#                                                    th,
#                                                    style = "border-right:1px solid grey")
#                                           ))))
#       
#       options = list(
#         pageLength = 100,
#         lengthMenu = list(c(10, 100, 1000,-1), c("10", "100", "1000", "All")),
#         searching = TRUE,
#         searchHighlight = TRUE,
#         scrollX = TRUE,
#         scrollY = "60vh",
#         lengthChange = TRUE,
#         ordering = TRUE,
#         paging = TRUE,
#         columnDefs = columsDefs
#       )
#       
#       table <- DT::datatable(
#         table,
#         options = options,
#         container = sketch,
#         rownames = FALSE,
#         colnames = colnames(table) %>%
#           camelCaseToTitleCase(),
#         escape = FALSE,
#         filter = "top",
#         class = "stripe nowrap compact"
#       )
#       table <- DT::formatStyle(
#         table = table,
#         columns = colorBarColumns,
#         background = DT::styleColorBar(c(0, 1), "lightblue"),
#         backgroundSize = "98% 88%",
#         backgroundRepeat = "no-repeat",
#         backgroundPosition = "center"
#       )
#       return(table)
#     }, server = TRUE)
#   
#   ###saveCompareCohortCharacterizationTable----
#   output$saveCompareCohortCharacterizationTable <-
#     downloadHandler(
#       filename = function() {
#         getCsvFileNameWithDateTime(string = "compareCohortCharacterization")
#       },
#       content = function(file) {
#         if (input$characterizationCompareMethod == "Pretty table") {
#           data <- getCompareCharacterizationTablePretty()
#         } else{
#           data <- getCompareCharacterizationTableRaw()
#         }
#         downloadCsv(x = parseMultipleCompareCharacterizationDataFiltered(),
#                     fileName = file)
#       }
#     )
  
  ###compareCharacterizationPlot----
  output$compareCharacterizationPlot <-
    plotly::renderPlotly(expr = {
      if (input$tabs != "compareCohortCharacterization") {
        return(NULL)
      }
      if (!doesObjectHaveData(parseMultipleCompareCharacterizationDataFiltered())) {
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
  #     if (!doesObjectHaveData(data)) {
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
  # # 
  # ### Output: compareTemporalCharacterizationTable ------
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
  # 
  # ###saveCompareTemporalCharacterizationTable----
  # output$saveCompareTemporalCharacterizationTable <-
  #   downloadHandler(
  #     filename = function() {
  #       getCsvFileNameWithDateTime(string = "compareTemporalCharacterization")
  #     },
  #     content = function(file) {
  #       downloadCsv(x = getCompareTemporalCharcterizationTableData(),
  #                   fileName = file)
  #     }
  #   )
  
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
  #     if (!doesObjectHaveData(input$timeIdChoices_open)) {
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
    if (!doesObjectHaveData(data)) {
      return(data)
    }
    
    data <- data %>%
      dplyr::filter(.data$databaseId == consolidatedDatabaseIdTarget())
    
    return(data)
  })
  
  #getMetadataInformationFilteredToDatabaseId----
  getMetadataInformationFilteredToDatabaseId <- shiny::reactive(x = {
    data <- getMetadataInformation() 
    if (!doesObjectHaveData(data)) {
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
  output$databaseInformationTable <- DT::renderDataTable(expr = {
    data <- getMetadataInformationFilteredToDatabaseId()
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
      columnDefs = list(minCellCountDef(10:14))
    )
  
    table <- DT::datatable(
      data ,
      options = options,
      colnames = colnames(data) %>%
        camelCaseToTitleCase(),
      rownames = FALSE,
      class = "stripe compact"
    )
    return(table)
  }, server = TRUE)
  
  ##output: metadataInfoTitle----
  output$metadataInfoTitle <- shiny::renderUI(expr = {
    data <- getMetadataInformation()
    
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::pull(.data$packageDependencySnapShotJson)
      
      data <- dplyr::as_tibble(RJSONIO::fromJSON(content = data,
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
        data,
        options = options,
        rownames = FALSE,
        colnames = colnames(data) %>%
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
      if (!doesObjectHaveData(data)) {
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
   
    
    if(doesObjectHaveData(consolidatedCohortIdComparator())) {
      selectedCohortIds <- c(selectedCohortIds,consolidatedCohortIdComparator())
    }
    selecteCohortCompondName <- cohort %>% 
      dplyr::filter(.data$cohortId %in% selectedCohortIds) %>% 
      dplyr::select(.data$compoundName)
    
    return(apply(selecteCohortCompondName, 1, function(x)
      tags$tr(lapply(x, tags$td))))
  })
  
  selectedDatabaseIds <- shiny::reactive({
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
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
