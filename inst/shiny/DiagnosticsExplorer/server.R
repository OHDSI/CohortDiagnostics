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
  
  #______________----
  #Selections----
  ##pickerInput: conceptSetsSelectedCohortLeft----
  #defined in UI
  shiny::observe({
    data <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::pull(.data$conceptSetName)
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    if (input$tabs == "indexEventBreakdown") {
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "conceptSetsSelectedCohortLeft",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = data,
        selected = data
      )
    } else {
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "conceptSetsSelectedCohortLeft",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = data
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
        dplyr::filter(.data$choices %in% input$timeIdChoices) %>%
        dplyr::pull(.data$timeId)
      getTimeIdsFromSelectedTemporalCovariateChoices(selectedTimeIds)
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
  
  ##getUserSelection----
  getUserSelection <- shiny::reactive(x = {
    list(
      input$tabs,
      input$cohortDefinitionTable_rows_selected,
      input$targetCohortDefinitionConceptSetsTable_rows_selected,
      input$comparatorCohortDefinitionConceptSets_rows_selected,
      input$targetCohortDefinitionResolvedConceptTable_rows_selected,
      input$comparatorCohortDefinitionResolvedConceptTable_rows_selected,
      input$targetCohortDefinitionExcludedConceptTable_rows_selected,
      input$comparatorCohortDefinitionExcludedConceptTable_rows_selected,
      input$targetCohortDefinitionOrphanConceptTable_rows_selected,
      input$comparatorCohortDefinitionOrphanConceptTable_rows_selected,
      input$selectedDatabaseId,
      input$selectedDatabaseIds,
      input$selectedDatabaseIds_open,
      input$selectedCompoundCohortName,
      input$selectedCompoundCohortNames,
      input$selectedCompoundCohortNames_open,
      input$conceptSetsSelectedCohortLeft,
      input$indexEventBreakdownTable_rows_selected,
      input$targetVocabularyChoiceForConceptSetDetails
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
                   indexEventBreakdownDataTable = getIndexEventBreakdownDataTable()
                 )
                 consolidatedCohortIdTarget(data$cohortIdTarget)
                 consolidatedCohortIdComparator(data$cohortIdComparator)
                 consolidatedConceptSetIdTarget(data$conceptSetIdTarget)
                 consolidatedConceptSetIdComparator(data$conceptSetIdComparator)
                 consolidatedDatabaseIdTarget(data$selectedDatabaseIdTarget)
                 consolidatedConceptIdTarget(data$selectedConceptIdTarget)
                 consolidatedConceptIdComparator(data$selectedConceptIdComparator)
                 consolidateCohortDefinitionActiveSideTarget(data$leftSideActive)
                 consolidateCohortDefinitionActiveSideComparator(data$rightSideActive)
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
          condition = "output.cohortDefinitionSelectedRowCount > 0 &
                     output.isCohortDefinitionRowSelected == true",
          shiny::htmlOutput(outputId = "targetCohortSelectedInCohortDefinitionTable"),
          shiny::tabsetPanel(
            type = "tab",
            id = "targetCohortDefinitionTabSetPanel",
            shiny::tabPanel(
              title = "Cohort Count",
              value = "targetCohortDefinitionCohortCountTabPanel",
              tags$br(),
              DT::dataTableOutput(outputId = "targetCohortDefinitionCohortCountTable"),
              tags$br(),
              #!!!!! here we will need to collapsible boxes (simplified/detailed with simplified selected by default. targetCohortDefinitionSimplifiedInclusionRuleTableFilters is in simplified)
              shiny::conditionalPanel(
                condition = "output.isDatabaseIdFoundForSelectedTargetCohortCount == true",
                tags$h3("Inclusion Rules"),
                tags$table(width = "100%",
                           tags$tr(
                             tags$td(
                               shiny::radioButtons(
                                 inputId = "targetCohortDefinitionInclusionRuleType",
                                 label = "Select: ",
                                 choices = c("Events", "Persons"),
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
                shiny::htmlOutput("targetCohortDefinitioncirceRVersion"),
                shiny::htmlOutput("targetCohortDefinitionText")
              ),
              shinydashboard::box(
                title = "Meta data",
                width = NULL,
                status = NULL,
                collapsible = TRUE,
                collapsed = FALSE,
                solidHeader = FALSE,
                shiny::htmlOutput("targetCohortDetailsText")
              )
            ),
            shiny::tabPanel(
              #!!!!!!!!!if cohort has no concept sets - make gray color or say 'No Concept sets'
              title = "Concept Sets",
              value = "targetCohortDefinitionConceptSetTabPanel",
              DT::dataTableOutput(outputId = "targetCohortDefinitionConceptSetsTable"),
              tags$br(),
              shiny::conditionalPanel(
                condition = "output.isTargetCohortDefinitionConceptSetsTableRowSelected == true",
                shinydashboard::box(
                  title = shiny::textOutput(outputId = "targetConceptSetExpressionName"),
                  ###!!!! add another textOutput with much smaller font and put cohort name here (next line)
                  width = NULL,
                  solidHeader = FALSE,
                  collapsible = TRUE,
                  collapsed = FALSE,
                  shiny::conditionalPanel(condition = "output.isTargetCohortDefinitionConceptSetsTableRowSelected == true",
                                          tags$table(
                                            tags$tr(tags$td(
                                              colspan = 2,
                                              shiny::radioButtons(
                                                inputId = "targetConceptSetsType",
                                                label = "",
                                                choices = c(
                                                  "Concept Set Expression",
                                                  "Resolved",
                                                  "Excluded",
                                                  "Orphan concepts",
                                                  "Concept Set Json",
                                                  "Concept Set Sql"
                                                ),
                                                selected = "Concept Set Expression",
                                                inline = TRUE
                                              )
                                            ))
                                          )),
                  shiny::conditionalPanel(
                    condition = "output.isTargetCohortDefinitionConceptSetsTableRowSelected == true &
                                                      input.targetConceptSetsType != 'Resolved' &
                                                      input.targetConceptSetsType != 'Excluded' &
                                                      input.targetConceptSetsType != 'Concept Set Json' &
                                                      input.targetConceptSetsType != 'Orphan concepts' &
                                                      input.targetConceptSetsType != 'Concept Set Sql'",
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
                    DT::dataTableOutput(outputId = "targetConceptSetsExpressionTable")
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
                    condition = "input.targetConceptSetsType == 'Orphan concepts'",
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
              shiny::htmlOutput("circeRVersionInTargetcohortDefinitionSql"),
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
          condition = "output.cohortDefinitionSelectedRowCount == 2 &
                     output.isCohortDefinitionRowSelected == true",
          shiny::htmlOutput(outputId = "nameOfComparatorSelectedCohortInCohortDefinitionTable"),
          shiny::tabsetPanel(
            id = "comparatorCohortDefinitionTabSetPanel",
            type = "tab",
            shiny::tabPanel(
              title = "Cohort Count",
              value = "comparatorCohortDefinitionCohortCountTabPanel",
              tags$br(),
              DT::dataTableOutput(outputId = "comparatorCohortDefinitionCohortCountsTable"),
              tags$br(),
              #!!!!! here we will need to collapsible boxes (simplified/detailed with simplified selected by default)
              #!!!! filter (all, meet, gain etc) are in simplified
              #!!!! comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters is in simplified)
              shiny::conditionalPanel(
                condition = "output.isDatabaseIdFoundForSelectedComparatorCohortCount == true",
                tags$h3("Inclusion Rules"),
                tags$table(width = "100%",
                           tags$tr(
                             tags$td(
                               shiny::radioButtons(
                                 inputId = "comparatorCohortDefinitionInclusionRuleType",
                                 label = "Filter by",
                                 choices = c("Events", "Persons"),
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
                shiny::htmlOutput("circeRVersionInComparatorCohortDefinition"),
                shiny::htmlOutput("comparatorCohortDefinitionText")
              ),
              shinydashboard::box(
                title = "Meta data",
                width = NULL,
                status = NULL,
                collapsible = TRUE,
                collapsed = FALSE,
                solidHeader = FALSE,
                shiny::htmlOutput("comparatorCohortDefinitioncohortDetailsText")
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
                  title = shiny::textOutput(outputId = "comparatorConceptSetExpressionName"),
                  ###!!!! add another textOutput with much smaller font and put cohort name here (next line)
                  solidHeader = FALSE,
                  width = NULL,
                  collapsible = TRUE,
                  collapsed = FALSE,
                  shiny::conditionalPanel(condition = "output.isComparatorCohortDefinitionConceptSetRowSelected == true",
                                          tags$table(
                                            tags$tr(tags$td(
                                              colspan = 2,
                                              shiny::radioButtons(
                                                inputId = "comparatorConceptSetsType",
                                                label = "",
                                                choices = c(
                                                  "Concept Set Expression",
                                                  "Resolved",
                                                  "Excluded",
                                                  "Orphan concepts",
                                                  "Concept Set Json",
                                                  "Concept Set Sql"
                                                ),
                                                selected = "Concept Set Expression",
                                                inline = TRUE
                                              )
                                            ))
                                          )),
                  shiny::conditionalPanel(
                    condition = "output.isComparatorCohortDefinitionConceptSetRowSelected == true &
                                                      input.comparatorConceptSetsType != 'Resolved' &
                                                      input.comparatorConceptSetsType != 'Excluded' &
                                                      input.comparatorConceptSetsType != 'Concept Set Json' &
                                                      input.comparatorConceptSetsType != 'Orphan concepts' &
                                                      input.comparatorConceptSetsType != 'Concept Set Sql'",
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
                    DT::dataTableOutput(outputId = "comparatorCohortDefinitionConceptSetsExpressionTable")
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
                    condition = "input.comparatorConceptSetsType == 'Orphan concepts'",
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
              shiny::htmlOutput("circeRVersionInComparatorCohortDefinitionSql"),
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
      selection = list(mode = "multiple", target = "row"),
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
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget())
    if (!doesObjectHaveData(data)) {
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
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator())
    if (!doesObjectHaveData(data)) {
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
    selectionsInCohortTable <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget())
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
      selectionsInCohortTable <- cohort %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator())
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
  
  ###output: targetCohortDefinitioncirceRVersion----
  output$targetCohortDefinitioncirceRVersion <-
    shiny::renderUI(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (!doesObjectHaveData(getCirceRPackageVersionInformation())) {
        return(NULL)
      }
      version <- tags$table(tags$tr(tags$td(
        paste(
          "rendered for target cohort id:",
          consolidatedCohortIdTarget(),
          "using CirceR version: ",
          getCirceRPackageVersionInformation()
        )
      )))
      return(version)
    })
  
  ###output: circeRVersionInComparatorCohortDefinition----
  output$circeRVersionInComparatorCohortDefinition <-
    shiny::renderUI(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      if (!doesObjectHaveData(getCirceRPackageVersionInformation())) {
        return(NULL)
      }
      version <- tags$table(tags$tr(tags$td(
        paste(
          "rendered for comparator cohort id:",
          consolidatedCohortIdComparator(),
          "using CirceR version: ",
          getCirceRPackageVersionInformation()
        )
      )))
      return(version)
    })
  
  ###output: circeRVersionInTargetcohortDefinitionSql----
  output$circeRVersionInTargetcohortDefinitionSql <-
    shiny::renderUI(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (!doesObjectHaveData(getCirceRPackageVersionInformation())) {
        return(NULL)
      }
      version <- tags$table(tags$tr(tags$td(
        paste(
          "rendered for target cohort id:",
          consolidatedCohortIdTarget(),
          "using CirceR version: ",
          getCirceRPackageVersionInformation()
        )
      )))
      if (!doesObjectHaveData(version)) {
        return(NULL)
      }
      return(version)
    })
  
  ###output: circeRVersionInComparatorCohortDefinitionSql----
  output$circeRVersionInComparatorCohortDefinitionSql <-
    shiny::renderUI(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      if (!doesObjectHaveData(getCirceRPackageVersionInformation())) {
        return(NULL)
      }
      version <- tags$table(tags$tr(tags$td(
        paste(
          "rendered for comparator cohort id:",
          consolidatedCohortIdComparator(),
          "using CirceR version: ",
          getCirceRPackageVersionInformation()
        )
      )))
      if (!doesObjectHaveData(version)) {
        return(NULL)
      }
      return(version)
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
                 tags$tr(tags$td(tags$b(
                   "Target cohort: "
                 )),
                 tags$td(cohortName)))
    })
  
  
  ###output: nameOfComparatorSelectedCohortInCohortDefinitionTable----
  output$nameOfComparatorSelectedCohortInCohortDefinitionTable <-
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
                 tags$tr(tags$td(tags$b(
                   "Comparator cohort:"
                 )),
                 tags$td(cohortName)))
      
    })
  
  #output: targetCohortDetailsText----
  output$targetCohortDetailsText <- shiny::renderUI({
    row <- getCohortMetadataLeft()
    if (doesObjectHaveData(row)) {
      return(NULL)
    }
    return(row)
  })
  #output: comparatorCohortDefinitioncohortDetailsText----
  output$comparatorCohortDefinitioncohortDetailsText <-
    shiny::renderUI({
      row <- getCohortMetadataRight()
      if (doesObjectHaveData(row)) {
        return(NULL)
      }
      if (length(row) == 2) {
        row <- row[[2]]
      }
      return(row)
    })
  
  ##Cohort SQL----
  ###output: targetCohortDefinitionSql----
  output$targetCohortDefinitionSql <- shiny::renderText({
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    json <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
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
  
  ###output: comparatorCohortDefinitionSql----
  output$comparatorCohortDefinitionSql <- shiny::renderText({
    if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    json <- cohort %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
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
  ###getCountsForSelectedCohortsTarget----
  getCountsForSelectedCohortsTarget <- shiny::reactive(x = {
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
        selection = list(mode = 'multiple', selected = 1),
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
  
  
  ###getResolvedConceptsTarget----
  getResolvedConceptsTarget <- shiny::reactive({
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    data <- getResultsResolvedConcepts(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdTarget(),
      databaseIds = consolidatedDatabaseIdTarget()
    )
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    if (doesObjectHaveData(consolidatedConceptSetIdTarget())) {
      data <- data %>%
        dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdTarget()) %>% 
        dplyr::select(-.data$conceptSetId, -.data$cohortId)
    }
    data <- data %>%
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ###getResolvedConceptsComparator----
  getResolvedConceptsComparator <- shiny::reactive({
    data <- getResultsResolvedConcepts(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdComparator(),
      databaseIds = consolidatedDatabaseIdTarget()
    )
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    if (doesObjectHaveData(consolidatedConceptSetIdComparator())) {
      data <- data %>%
        dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdComparator()) %>% 
        dplyr::select(-.data$conceptSetId, -.data$cohortId)
    }
    data <- data %>%
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ###getExcludedConceptsTarget----
  getExcludedConceptsTarget <- shiny::reactive({
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
      return(NULL)
    }
    data <- getResultsExcludedConcepts(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdTarget(),
      databaseIds = consolidatedDatabaseIdTarget()
    )
    if (is.null(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdTarget()) %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId) %>%
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    if (is.null(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ###getExcludedConceptsComparator----
  getExcludedConceptsComparator <- shiny::reactive({
    if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdComparator())) {
      return(NULL)
    }
    data <- getResultsExcludedConcepts(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdComparator(),
      databaseIds = consolidatedDatabaseIdTarget()
    )
    if (is.null(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdComparator()) %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId) %>%
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    return(data)
  })
  
  
  ###getOrphanConceptsTarget----
  getOrphanConceptsTarget <- shiny::reactive({
    data <- getResultsOrphanConcept(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdTarget(),
      databaseIds = consolidatedDatabaseIdTarget()
    )
    if (is.null(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdTarget())
    
    excluded <- getExcludedConceptsTarget()
    if (doesObjectHaveData(excluded)) {
      excludedConceptIds <- excluded %>%
        dplyr::select(.data$conceptId) %>%
        dplyr::distinct()
      data <- data %>%
        dplyr::anti_join(y = excludedConceptIds, by = "conceptId")
    }
    data <- data  %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdTarget()) %>% 
      dplyr::select(-.data$conceptSetId, -.data$cohortId) %>%
      dplyr::arrange(dplyr::desc(.data$conceptCount))
    if (is.null(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ###getOrphanConceptsComparator----
  getOrphanConceptsComparator <- shiny::reactive({
    data <- getResultsOrphanConcept(
      dataSource = dataSource,
      cohortIds = consolidatedCohortIdComparator(),
      databaseIds = consolidatedDatabaseIdTarget()
    )
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$conceptSetId == consolidatedConceptSetIdComparator())
    excluded <- getExcludedConceptsComparator()
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
  
  ##Inclusion rule ----
  ###getDatabaseIdFromSelectedRowInCohortCountTableTarget----
  getDatabaseIdFromSelectedRowInCohortCountTableTarget <-
    shiny::reactive(x = {
      idx <- input$targetCohortDefinitionCohortCountTable_rows_selected
      if (!doesObjectHaveData(idx)) {
        return(NULL)
      }
      databaseIds <- getCountsForSelectedCohortsTarget()[idx,]
      if (!doesObjectHaveData(databaseIds)) {
        return(NULL)
      }
      databaseIds <- databaseIds %>%
        dplyr::pull(.data$databaseId)
      return(databaseIds)
    })
  
  ###getSimplifiedInclusionRuleResultsTarget----
  getSimplifiedInclusionRuleResultsTarget <- shiny::reactive(x = {
    if (any(
      !doesObjectHaveData(consolidatedCohortIdTarget()),
      !doesObjectHaveData(getDatabaseIdFromSelectedRowInCohortCountTableTarget())
    )) {
      return(NULL)
    }
    data <-
      getResultsInclusionRuleStatistics(
        dataSource = dataSource,
        cohortIds = consolidatedCohortIdTarget(),
        databaseIds = getDatabaseIdFromSelectedRowInCohortCountTableTarget()
      )
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ###getDatabaseIdFromSelectedRowInCohortCountTableComparator----
  getDatabaseIdFromSelectedRowInCohortCountTableComparator <-
    shiny::reactive(x = {
      idx <-
        input$comparatorCohortDefinitionCohortCountsTable_rows_selected
      if (!doesObjectHaveData(idx)) {
        return(NULL)
      }
      databaseIds <- getCountsForSelectedCohortsComparator()[idx, ]
      if (!doesObjectHaveData(databaseIds)) {
        return(NULL)
      }
      databaseIds <- databaseIds %>%
        dplyr::pull(.data$databaseId)
      return(databaseIds)
    })
  
  ###getSimplifiedInclusionRuleResultsComparator----
  getSimplifiedInclusionRuleResultsComparator <-
    shiny::reactive(x = {
      if (any(
        !doesObjectHaveData(consolidatedCohortIdComparator()),
        !doesObjectHaveData(
          getDatabaseIdFromSelectedRowInCohortCountTableComparator()
        )
      )) {
        return(NULL)
      }
      data <-
        getResultsInclusionRuleStatistics(
          dataSource = dataSource,
          cohortIds = consolidatedCohortIdComparator(),
          databaseIds = getDatabaseIdFromSelectedRowInCohortCountTableComparator()
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
      table <- getSimplifiedInclusionRuleResultsTarget()
      validate(need((nrow(table) > 0),
                    "There is no inclusion rule data for this cohort."))
      
      databaseIds <- unique(table$databaseId)
      cohortCounts <- table %>%
        dplyr::inner_join(cohortCount,
                          by = c("cohortId", "databaseId")) %>%
        dplyr::filter(.data$cohortId == consolidatedCohortIdTarget()) %>%
        dplyr::filter(.data$databaseId %in% getDatabaseIdFromSelectedRowInCohortCountTableTarget()) %>%
        dplyr::select(.data$cohortEntries) %>%
        dplyr::pull(.data$cohortEntries) %>% unique()
      
      databaseIdsWithCount <-
        paste(databaseIds,
              "(n = ",
              format(cohortCounts, big.mark = ","),
              ")")
      
      table <- table %>%
        dplyr::inner_join(
          cohortCount %>%
            dplyr::select(.data$databaseId, .data$cohortId, .data$cohortEntries),
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
          scales::comma(x = .data$cohortEntries, accuracy = 1),
          ")_",
          .data$name
        )) %>%
        tidyr::pivot_wider(
          id_cols = c(.data$cohortId, .data$ruleSequenceId, .data$ruleName),
          names_from = .data$name,
          values_from = .data$value
        ) %>%
        dplyr::select(-.data$cohortId)
      
      if (input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters == "Meet") {
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
        
      } else if (input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters == "Totals") {
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
        
      } else if (input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters == "Gain") {
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
        
      } else if (input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters == "Remain") {
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
      
      if (input$targetCohortDefinitionSimplifiedInclusionRuleTableFilters == "All") {
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
  
  #output: cohortDefinitionSelectedRowCount----
  output$cohortDefinitionSelectedRowCount <- shiny::reactive({
    return(length(input$cohortDefinitionTable_rows_selected))
  })
  shiny::outputOptions(x = output,
                       name = "cohortDefinitionSelectedRowCount",
                       suspendWhenHidden = FALSE)
  
  output$conceptSetComparisonTable <- DT::renderDT(expr = {
    #Setting Initial values
    target <- NULL
    comparator <- NULL
    if (input$targetConceptSetsType == "Resolved" &
        input$comparatorConceptSetsType == 'Resolved') {
      target <- getResolvedConceptsTarget()
      comparator <- getResolvedConceptsComparator()
    } else if (input$targetConceptSetsType == "Excluded" &
               input$comparatorConceptSetsType == 'Excluded') {
      target <- getExcludedConceptsTarget()
      comparator <- getExcludedConceptsComparator()
    }  else if (input$targetConceptSetsType == "Orphan concepts" &
                input$comparatorConceptSetsType == 'Orphan concepts') {
      target <- getOrphanConceptsTarget()
      comparator <- getOrphanConceptsComparator()
    }
    if (any(!doesObjectHaveData(target),!doesObjectHaveData(comparator))) {
      return(NULL)
    }
    
    combinedResult <-
      target %>%
      dplyr::union(comparator) %>%
      dplyr::arrange(.data$conceptId) %>%
      dplyr::select(.data$conceptId, .data$conceptName, .data$databaseId) %>%
      dplyr::distinct() %>% 
      dplyr::mutate(left = "", right = "")
    
    databaseIds <- unique(combinedResult$databaseId)
    
    conceptIdsPresentInTarget <- target %>%
      dplyr::pull(.data$conceptId) %>%
      unique()
    
    conceptIdsPresentInComparator <- comparator %>%
      dplyr::pull(.data$conceptId) %>%
      unique()
    
    for (i in 1:nrow(combinedResult)) {
      combinedResult$left[i] <-
        ifelse(
          combinedResult$conceptId[i] %in% conceptIdsPresentInTarget,
          as.character(icon("check")),
          ""
        )
      combinedResult$right[i] <-
        ifelse(
          combinedResult$conceptId[i] %in% conceptIdsPresentInComparator,
          as.character(icon("check")),
          ""
        )
    }
    
    combinedResult <- combinedResult %>% 
      dplyr::mutate(left = as.character(.data$left)) %>% 
      dplyr::mutate(right = as.character(.data$right)) %>% 
      tidyr::pivot_longer(
        names_to = "type",
        cols = c("left", "right"),
        values_to = "count"
      ) %>% 
      dplyr::mutate(type = paste0(.data$type,
                                  " ",
                                  .data$databaseId)) %>% 
      tidyr::pivot_wider(
        id_cols = c(
          "conceptId",
          "conceptName"
        ),
        names_from = type,
        values_from = count
      )
    
    sketch <- htmltools::withTags(table(class = "display",
                                        thead(tr(
                                          th(rowspan = 2, "Concept ID"),
                                          th(rowspan = 2, "Concept Name"),
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
                                            c("From Target", "From Comparator"),
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
      combinedResult,
      options = options,
      container = sketch,
      colnames = colnames(combinedResult) %>% camelCaseToTitleCase(),
      rownames = FALSE,
      escape = FALSE,
      selection = 'single',
      filter = "top",
      class = "stripe nowrap compact"
    )
    return(dataTable)
  })
  
  activeSelected <- reactiveVal(list())
  observe({
    tempList <- list()
    tempList$cohortId <- consolidatedCohortIdTarget()
    tempList$conceptSetId <-
      consolidatedConceptSetIdTarget()
    tempList$databaseId <-
      consolidatedDatabaseIdTarget()
    if (is.null(consolidatedConceptIdTarget())) {
      tempList$conceptId <- consolidatedConceptIdComparator()
    } else {
      tempList$conceptId <- consolidatedConceptIdTarget()
    }
    activeSelected(tempList)
  })
  
  observe({
    tempList <- list()
    tempList$cohortId <-
      consolidatedCohortIdComparator()
    tempList$conceptSetId <-
      consolidatedConceptSetIdComparator()
    tempList$databaseId <-
      consolidatedDatabaseIdTarget()
    if (is.null(consolidatedConceptIdComparator())) {
      tempList$conceptId <- consolidatedConceptIdTarget()
    } else {
      tempList$conceptId <- consolidatedConceptIdComparator()
    }
    activeSelected(tempList)
  })
  
  
  ##getMetadataForConceptId----
  getMetadataForConceptId <- shiny::reactive(x = {
    if (is.null(activeSelected()$conceptId)) {#currently expecting to be vector of 1 (single select)
      return(NULL)
    }
    if (length(activeSelected()$conceptId) != 1) {
      stop("Only single select is supported for conceptId")
    }
    if (is.null(activeSelected()$cohortId)) {#currently expecting to be vector of 1 (single select)
      return(NULL)
    }
    if (length(activeSelected()$cohortId) != 1) {
      stop("Only single select is supported for cohortId")
    }
    if (is.null(activeSelected()$databaseId)) {#currently expecting to be vector of 1 or more (multiselect)
      return(NULL)
    }
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(
      message = paste0("Getting metadata for concept id:",
                       activeSelected()$conceptId,
                       " in cohort: ",
                       activeSelected()$cohortId),
      value = 0
    )
    data <-
      getConceptMetadata(
        dataSource = dataSource,
        databaseIds = activeSelected()$databaseId,
        cohortIds = activeSelected()$cohortId,
        conceptIds = activeSelected()$conceptId
      )
    if (!doesObjectHaveData(data$databaseConceptCount)) {
      return(NULL)
    }
    if (!is.null(data$conceptCooccurrence)) {
      data$conceptCooccurrence <-
        data$conceptCooccurrence %>%
        dplyr::filter(.data$referenceConceptId == activeSelected()$conceptId) %>%
        dplyr::filter(.data$cohortId == activeSelected()$cohortId) %>%
        dplyr::select(-.data$referenceConceptId, -.data$cohortId) %>% 
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
        dplyr::inner_join(data$conceptMapping %>%
                            dplyr::filter(.data$conceptId == activeSelected()$conceptId) %>%
                            dplyr::select(-.data$conceptId) %>% 
                            dplyr::rename("conceptId" = .data$sourceConceptId) %>% 
                            dplyr::select(.data$databaseId,
                                          .data$domainTable,
                                          .data$conceptId,
                                          .data$conceptCount,
                                          .data$subjectCount) %>% 
                            dplyr::distinct() %>% 
                            dplyr::arrange(dplyr::desc(.data$subjectCount)),
                          by = "conceptId") %>% 
        dplyr::distinct()
    }
    return(data)
  })
  
  #Dynamic UI rendering for relationship table -----
  output$dynamicUIForRelationshipAndComparisonTable <-
    shiny::renderUI({
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = "Rendering the UI for concept browser",
        value = 0
      )
      inc <-  1
      panels <- list()
      #Modifying rendered UI after load
      if (!is.null(activeSelected()$conceptId)) {
        data <- getMetadataForConceptId()
        validate(need(doesObjectHaveData(data), "No data for selected combination"))
        
        #!!!!! put selectd concept details as a shared header
        # tags$h4(paste0(
        #   data$concept %>% 
        #     dplyr::filter(.data$conceptId == activeSelected()$conceptId) %>% 
        #     dplyr::pull(.data$conceptName),
        #   " (",
        #   activeSelected()$conceptId,
        #   ")"
        # )),
        # tags$h6(data$conceptSynonym$conceptSynonymName %>% unique() %>% sort() %>% paste0(collapse = ", ")),
        panels[[inc]] <- shiny::tabPanel(
          title = "Concept Set Browser",
          value = "conceptSetBrowser",
          shiny::conditionalPanel(
            condition = "output.isConceptIdFromTargetOrComparatorConceptTableSelected==true",
            tags$table(width = "100%",
                       tags$tr(
                         tags$td(
                           shinyWidgets::pickerInput(
                             inputId = "choicesForRelationshipName",
                             label = "Relationship Category:",
                             choices = c(data$relationshipName),
                             selected = c(data$relationshipName),
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
                             choices = data$conceptAncestorDistance,
                             selected = data$conceptAncestorDistance,
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
            DT::dataTableOutput(outputId = "conceptBrowserTable")
          )
        )
        inc = inc + 1
        if (doesObjectHaveData(data$mappedNonStandard)) {
          panels[[inc]] <- shiny::tabPanel(
            title = "Non standard counts",
            value = "nonStandardCount",
            shiny::conditionalPanel(
              condition = "output.isConceptIdFromTargetOrComparatorConceptTableSelected==true",
              DT::dataTableOutput(outputId = "nonStandardCount")
            )
          )
          inc = inc + 1
        }
        panels[[inc]] <- shiny::tabPanel(
          title = "Time Series Plot",
          value = "conceptSetTimeSeries",
          shiny::column(
            width = 12,
            shiny::radioButtons(
              inputId = "timeSeriesAggregationForCohortDefinition",
              label = "Aggregation period:",
              choices = c("Monthly", "Yearly"),
              selected = "Monthly",
              inline = TRUE
            )
          ),
          shiny::column(
            width = 12,
            plotly::plotlyOutput(
              outputId = "conceptSetTimeSeriesPlot",
              width = "100%",
              height = "100%"
            )
          )
        )
        inc = inc + 1
      }
      
      if (all(
        length(input$cohortDefinitionTable_rows_selected) == 2,
        !is.null(getConceptSetExpressionTarget()),
        !is.null(getConceptSetExpressionComparator())
      )) {
        panels[[inc]] <- shiny::tabPanel(
          title = "Concept Set Comparison",
          value = "conceptSetComparison",
          DT::dataTableOutput(outputId = "conceptSetComparisonTable")
        )
      }
      shiny::conditionalPanel(
        condition = "output.isConceptIdFromTargetOrComparatorConceptTableSelected==true",
        shinydashboard::box(
          title = shiny::htmlOutput(outputId = "conceptSetBowserConceptSynonymName"),
          width =  NULL,
          status = NULL,
          collapsible = TRUE,
          collapsed = TRUE,
          do.call(tabsetPanel, panels)
        )
      )
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
    shiny::renderText(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
        return(NULL)
      }
      data <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
        dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdTarget()) %>%
        dplyr::pull(.data$conceptSetName)
      data <- paste0("Target cohort:", data, " (", consolidatedCohortIdTarget(), ")")
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
      return(data)
    })
  
  ##getConceptSetsInCohortDataTarget----
  getConceptSetsInCohortDataTarget <- reactive({
    if (!doesObjectHaveData(consolidatedCohortIdTarget())) {
      return(NULL)
    }
    data <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% consolidatedCohortIdTarget()) %>%
      dplyr::select(.data$conceptSetId, .data$conceptSetName) %>%
      dplyr::arrange(.data$conceptSetId)
    return(data)
  })
  
  #output: targetCohortDefinitionConceptSetsTable----
  output$targetCohortDefinitionConceptSetsTable <-
    DT::renderDataTable(expr = {
      data <- getConceptSetsInCohortDataTarget()
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
      data <- getResolvedConceptsTarget()
      validate(need((all(
        !is.null(data), nrow(data) > 0
      )),
      "No resolved concept ids"))
      databaseCount <- cohortCount %>% 
        dplyr::filter(.data$cohortId == consolidatedCohortIdTarget()) %>% 
        dplyr::rename("records" = .data$cohortEntries,
                      "persons" = .data$cohortSubjects)
      data <- data %>% 
        dplyr::rename("records" = .data$conceptCount,
                      "persons" = .data$subjectCount)
      table <- getSketchDesignForTablesInCohortDefinitionTab(data = data, 
                                                             databaseCount = databaseCount)
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
      databaseCount <- cohortCount %>% 
        dplyr::filter(.data$cohortId == consolidatedCohortIdTarget()) %>% 
        dplyr::rename("records" = .data$cohortEntries,
                      "persons" = .data$cohortSubjects)
      data <- data %>% 
        dplyr::rename("records" = .data$conceptCount,
                      "persons" = .data$subjectCount)
      table <- getSketchDesignForTablesInCohortDefinitionTab(data = data, 
                                                             databaseCount = databaseCount)
      return(table)
    }, server = TRUE)
  
  #output: saveExcludedConceptsTableLeft----
  output$saveExcludedConceptsTableLeft <-  downloadHandler(
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
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
      validate(need(any(!is.null(data),
                        nrow(data) > 0),
                    "No orphan concepts"))
      databaseCount <- cohortCount %>% 
        dplyr::filter(.data$cohortId == consolidatedCohortIdTarget()) %>% 
        dplyr::rename("records" = .data$cohortEntries,
                      "persons" = .data$cohortSubjects)
      data <- data %>% 
        dplyr::rename("records" = .data$conceptCount,
                      "persons" = .data$subjectCount)
      table <- getSketchDesignForTablesInCohortDefinitionTab(data = data, 
                                                             databaseCount = databaseCount)
      return(table)
    }, server = TRUE)
  
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
  
  #!!!!!!!!!!!! add excluded
  
  #!!! on row select for resolved/excluded/orphan - we need to show for selected cohort
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
        selection = list(mode = 'multiple', selected = 1),
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
      if (any(is.null(consolidatedCohortIdComparator()))) {
        return(NULL)
      }
      
      table <- getSimplifiedInclusionRuleResultsComparator()
      validate(need((nrow(table) > 0),
                    "There is no inclusion rule data for this cohort."))
      
      databaseIds <- unique(table$databaseId)
      cohortCounts <- table %>%
        dplyr::inner_join(cohortCount,
                          by = c("cohortId", "databaseId")) %>%
        dplyr::filter(.data$cohortId == consolidatedCohortIdComparator()) %>%
        dplyr::filter(
          .data$databaseId %in% getDatabaseIdFromSelectedRowInCohortCountTableComparator()
        ) %>%
        dplyr::select(.data$cohortEntries) %>%
        dplyr::pull(.data$cohortEntries) %>%
        unique()
      
      databaseIdsWithCount <-
        paste(databaseIds,
              "(n = ",
              format(cohortCounts, big.mark = ","),
              ")")
      
      table <- table %>%
        dplyr::inner_join(
          cohortCount %>%
            dplyr::select(.data$databaseId, .data$cohortId, .data$cohortEntries),
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
          scales::comma(x = .data$cohortEntries, accuracy = 1),
          ")_",
          .data$name
        )) %>%
        tidyr::pivot_wider(
          id_cols = c(.data$cohortId, .data$ruleSequenceId, .data$ruleName),
          names_from = .data$name,
          values_from = .data$value
        ) %>%
        dplyr::select(-.data$cohortId)
      
      if (input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters == "Meet") {
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
        
      } else if (input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters == "Totals") {
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
        
      } else if (input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters == "Gain") {
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
        
      } else if (input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters == "Remain") {
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
      
      if (input$comparatorCohortDefinitionSimplifiedInclusionRuleTableFilters == "All") {
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
    shiny::renderText(expr = {
      if (!doesObjectHaveData(consolidatedCohortIdComparator())) {
        return(NULL)
      }
      if (!doesObjectHaveData(consolidatedConceptSetIdComparator())) {
        return(NULL)
      }
      data <- conceptSets %>%
        dplyr::filter(.data$cohortId %in% consolidatedCohortIdComparator()) %>%
        dplyr::filter(.data$conceptSetId %in% consolidatedConceptSetIdComparator()) %>%
        dplyr::pull(.data$conceptSetName)
      data <- paste0("Comparator cohort:", data, " (", consolidatedCohortIdComparator(), ")")
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
      return(data)
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
  
  ##output: comparatorCohortDefinitionConceptSetsExpressionTable----
  output$comparatorCohortDefinitionConceptSetsExpressionTable <-
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
        columnDefs = list(truncateStringDef(2, 80))
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
  
  ##output: saveComparatorConceptSetsExpressionTable----
  output$saveComparatorConceptSetsExpressionTable <-
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
  
  ##output: comparatorCohortDefinitionResolvedConceptTable----
  output$comparatorCohortDefinitionResolvedConceptTable <-
    DT::renderDataTable(expr = {
      data <- getResolvedConceptsComparator()
      validate(need((all(
        !is.null(data), nrow(data) > 0
      )),
      "No resolved concept ids"))
      databaseCount <- cohortCount %>% 
        dplyr::filter(.data$cohortId == consolidatedCohortIdComparator()) %>% 
        dplyr::rename("records" = .data$cohortEntries,
                      "persons" = .data$cohortSubjects)
      data <- data %>% 
        dplyr::rename("records" = .data$conceptCount,
                      "persons" = .data$subjectCount)
      table <- getSketchDesignForTablesInCohortDefinitionTab(data = data, 
                                                             databaseCount = databaseCount)
      return(table)
    }, server = TRUE)
  
  ##output: saveComparatorCohortDefinitionResolvedConceptTable----
  output$saveComparatorCohortDefinitionResolvedConceptTable <-
    downloadHandler(
      filename = function()
      {
        getCsvFileNameWithDateTime(string = "resolvedConceptSet")
      },
      content = function(file)
      {
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
      
      databaseIds <- sort(unique(data$databaseId))
      maxCount <- max(data$conceptCount, na.rm = TRUE)
      maxSubject <- max(data$subjectCount, na.rm = TRUE)
      databaseCount <- cohortCount %>% 
        dplyr::filter(.data$cohortId == consolidatedCohortIdComparator()) %>% 
        dplyr::rename("records" = .data$cohortEntries,
                      "persons" = .data$cohortSubjects)
      data <- data %>% 
        dplyr::rename("records" = .data$conceptCount,
                      "persons" = .data$subjectCount)
      table <- getSketchDesignForTablesInCohortDefinitionTab(data = data, 
                                                             databaseCount = databaseCount)
      return(table)
    }, server = TRUE)
  
  #output: saveExcludedConceptsTableComparator----
  output$saveExcludedConceptsTableComparator <-  downloadHandler(
    filename = function()
    {
      getCsvFileNameWithDateTime(string = "excludedConcepts")
    },
    content = function(file)
    {
      data <- getExcludedConceptsComparator()
      downloadCsv(x = data, fileName = file)
    }
  )
  
  output$comparatorCohortDefinitionOrphanConceptTable <-
    DT::renderDataTable(expr = {
      data <- getOrphanConceptsComparator()
      if (!doesObjectHaveData(data)) {
        return(NULL)
      } 
      
      validate(need(any(!is.null(data),
                        nrow(data) > 0),
                    "No orphan concepts"))
      databaseCount <- cohortCount %>% 
        dplyr::filter(.data$cohortId == consolidatedCohortIdComparator()) %>% 
        dplyr::rename("records" = .data$cohortEntries,
                      "persons" = .data$cohortSubjects)
      data <- data %>% 
        dplyr::rename("records" = .data$conceptCount,
                      "persons" = .data$subjectCount)
      table <- getSketchDesignForTablesInCohortDefinitionTab(data = data, 
                                                             databaseCount = databaseCount)
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
    # working on the plot
    if (input$timeSeriesAggregationForCohortDefinition == "Monthly") {
      data <- data$databaseConceptIdYearMonthLevelTsibble %>% 
        dplyr::filter(.data$conceptId == activeSelected()$conceptId)
    } else {
      data <- data$databaseConceptIdYearLevelTsibble %>% 
        dplyr::filter(.data$conceptId == activeSelected()$conceptId)
    }
    #!!!!!!!!########## filter domain table and domain field in data$cdmTables - default "All"
    data <- data %>% 
      dplyr::filter(.data$domainTableShort == "All") %>% #need input object
      dplyr::filter(.data$domainFieldShort == "All") #need input object
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
      dplyr::filter(.data$databaseId %in% activeSelected()$databaseId) %>% 
      dplyr::rename("records" = .data$conceptCount,
                    "persons" = .data$subjectCount)
    tsibbleDataFromSTLModel <- getStlModelOutputForTsibbleDataValueFields(tsibbleData = data,
                                                                          valueFields = c("records", "persons"))
    

    
    plot <- plotTimeSeriesForCohortDefinitionFromTsibble(
      stlModeledTsibbleData = tsibbleDataFromSTLModel
    )
    plot <- plotly::ggplotly(plot)
    return(plot)
  })
  
  ##output:: conceptSetBowserConceptSynonymName
  output$conceptSetBowserConceptSynonymName <- shiny::renderUI(expr = {
     data <- getMetadataForConceptId()
    
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
          tags$h6(data$conceptSynonym$conceptSynonymName %>% unique() %>% sort() %>% paste0(collapse = ", "))
        )
      )
    )
  })
  
  ##output: conceptBrowserTable----
  output$conceptBrowserTable <- DT::renderDT(expr = {
    conceptId <- activeSelected()$conceptId
    validate(need(doesObjectHaveData(conceptId), "No concept id selected."))
    cohortId <- activeSelected()$cohortId
    validate(need(doesObjectHaveData(conceptId), "No cohort id selected."))
    databaseId <- activeSelected()$databaseId
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
    conceptRelationshipTable <- data$conceptRelationshipTable %>% 
      dplyr::filter(.data$conceptId != activeSelected()$conceptId)
    if (any(
      doesObjectHaveData(input$choicesForRelationshipName),
      doesObjectHaveData(input$choicesForRelationshipDistance)
    )) {
      if (doesObjectHaveData(input$choicesForRelationshipName)) {
        conceptRelationshipTable <- conceptRelationshipTable %>%
          dplyr::inner_join(
            relationship %>%
              dplyr::filter(
                .data$relationshipName %in% c(input$choicesForRelationshipName)
              ) %>%
              dplyr::select(.data$relationshipId) %>%
              dplyr::distinct(),
            by = "relationshipId"
          )
      }
      if (doesObjectHaveData(input$choicesForRelationshipDistance)) {
        conceptRelationshipTable <- conceptRelationshipTable %>%
          dplyr::filter(.data$levelsOfSeparation %in%
                          input$choicesForRelationshipDistance)
      }
    }
    conceptRelationshipTable <- conceptRelationshipTable %>%
      dplyr::inner_join(data$concept,
                        by = "conceptId") %>%
      tidyr::crossing(dplyr::tibble(databaseId = !!databaseId))
    if (!doesObjectHaveData(conceptRelationshipTable)) {
      return(NULL)
    }
    #!!!!!!!!!!switch
    #!!!!!!!!!!!!!!!if cohort count is selected then
    # conceptRelationshipTable <- conceptRelationshipTable %>% 
    #   dplyr::left_join(data$conceptCooccurrence %>%
    #                       dplyr::select(-.data$cohortId),
    #                     by = "conceptId")
    # databaseCount <- data$indexEventBreakdown %>%
    #   dplyr::filter(.data$conceptId %in% conceptId) %>%
    #   dplyr::select(.data$databaseId, .data$conceptCount, .data$subjectCount) %>%
    #   dplyr::rename("records" = .data$conceptCount,
    #                 "persons" = .data$subjectCount)
    #!!!!!!!!!!!!!!!if database count is selected then
    conceptRelationshipTable <- conceptRelationshipTable %>%
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
      dplyr::arrange(.data$databaseId, .data$conceptId, dplyr::desc(.data$records))
    databaseCount <- data$databaseConceptCount %>%
      dplyr::filter(.data$conceptId == activeSelected()$conceptId) %>% 
      dplyr::filter(.data$databaseId %in% activeSelected()$databaseId) %>%
      dplyr::rename("persons" = .data$subjectCount,
                    "records" = .data$conceptCount)
    table <-
      getSketchDesignForTablesInCohortDefinitionTab(conceptRelationshipTable,
                                                    databaseCount = databaseCount)
    return(table)
  })
  
  
  
  ##output: nonStandardCount----
  output$nonStandardCount <- DT::renderDT(expr = {
    conceptId <- activeSelected()$conceptId
    validate(need(doesObjectHaveData(conceptId), "No concept id selected."))
    cohortId <- activeSelected()$cohortId
    validate(need(doesObjectHaveData(conceptId), "No cohort id selected."))
    databaseId <- activeSelected()$databaseId
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
    conceptMapping <- data$mappedNonStandard %>%
      dplyr::rename("persons" = .data$subjectCount,
                    "records" = .data$conceptCount)
    databaseCount <- data$databaseConceptCount %>% 
      dplyr::filter(.data$conceptId == activeSelected()$conceptId) %>%
      dplyr::rename("persons" = .data$subjectCount,
                    "records" = .data$conceptCount)
    table <-
      getSketchDesignForTablesInCohortDefinitionTab(conceptMapping,
                                                    databaseCount = databaseCount)
    return(table)
  })
  
  
  
  #Radio button synchronization----
  shiny::observeEvent(eventExpr = {
    list(input$targetConceptSetsType,
         input$targetCohortDefinitionTabSetPanel)
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
        }
        else if (input$targetConceptSetsType == "Excluded") {
          updateRadioButtons(session = session,
                             inputId = "comparatorConceptSetsType",
                             selected = "Excluded")
        } else if (input$targetConceptSetsType == "Orphan concepts") {
          updateRadioButtons(session = session,
                             inputId = "comparatorConceptSetsType",
                             selected = "Orphan concepts")
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
      
      if (!is.null(input$targetCohortDefinitionTabSetPanel)) {
        if (input$targetCohortDefinitionTabSetPanel == "targetCohortDefinitionDetailsTextTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "comparatorCohortDefinitionTabSetPanel", selected = "comparatorCohortDefinitionDetailsTextTabPanel")
        } else if (input$targetCohortDefinitionTabSetPanel == "targetCohortDefinitionCohortCountTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "comparatorCohortDefinitionTabSetPanel", selected = "comparatorCohortDefinitionCohortCountTabPanel")
        } else if (input$targetCohortDefinitionTabSetPanel == "targetCohortDefinitionConceptSetTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "comparatorCohortDefinitionTabSetPanel", selected = "comparatorCohortDefinitionConceptSetTabPanel")
        } else if (input$targetCohortDefinitionTabSetPanel == "targetCohortDefinitionJsonTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "comparatorCohortDefinitionTabSetPanel", selected = "comparatorCohortDefinitionJsonTabPanel")
        } else if (input$targetCohortDefinitionTabSetPanel == "targetCohortDefinitionSqlTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "comparatorCohortDefinitionTabSetPanel", selected = "comparatorCohortDefinitionSqlTabPanel")
        }
      }
    }
  })
  
  shiny::observeEvent(eventExpr = {
    list(
      input$comparatorConceptSetsType,
      input$comparatorCohortDefinitionTabSetPanel
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
        } else if (input$comparatorConceptSetsType == "Orphan concepts") {
          updateRadioButtons(session = session,
                             inputId = "targetConceptSetsType",
                             selected = "Orphan concepts")
        } else if (input$comparatorConceptSetsType == "Concept Set Json") {
          #!! call this "Concept Set JSON"
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
          shiny::updateTabsetPanel(session, inputId = "targetCohortDefinitionTabSetPanel", selected = "targetCohortDefinitionDetailsTextTabPanel")
        } else if (input$comparatorCohortDefinitionTabSetPanel == "comparatorCohortDefinitionCohortCountTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "targetCohortDefinitionTabSetPanel", selected = "targetCohortDefinitionCohortCountTabPanel")
        } else if (input$comparatorCohortDefinitionTabSetPanel == "comparatorCohortDefinitionConceptSetTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "targetCohortDefinitionTabSetPanel", selected = "targetCohortDefinitionConceptSetTabPanel")
        } else if (input$comparatorCohortDefinitionTabSetPanel == "comparatorCohortDefinitionJsonTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "targetCohortDefinitionTabSetPanel", selected = "targetCohortDefinitionJsonTabPanel")
        } else if (input$comparatorCohortDefinitionTabSetPanel == "comparatorCohortDefinitionSqlTabPanel") {
          shiny::updateTabsetPanel(session, inputId = "targetCohortDefinitionTabSetPanel", selected = "targetCohortDefinitionSqlTabPanel")
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
        length(consolidatedDatabaseIdTarget()) > 0,
        "No data sources chosen"
      ))
      validate(need(
        nrow(getCohortIdFromSelectedRowInCohortCountTable()) > 0,
        "No cohorts chosen"
      ))
      
      table <- getResultsInclusionRuleStatistics(
        dataSource = dataSource,
        cohortIds = getCohortIdFromSelectedRowInCohortCountTable()$cohortId,
        databaseIds = consolidatedDatabaseIdTarget()
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
      if (any(length(consolidatedCohortIdTarget()) == 0)) {
        return(NULL)
      }
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = paste0("Getting incidence rate data."),
                   value = 0)
      
      data <- getResultsIncidenceRate(dataSource = dataSource,
                                      cohortIds =  consolidatedCohortIdTarget())
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
      if (!doesObjectHaveData(getIncidenceRateData())) {
        return(NULL)
      }
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
  
  ##reactive: getIncidentRatePlot ----
  getIncidentRatePlot <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {
    list(!is.null(input$tabs), input$renderIncidentRatePlot, 
         doesObjectHaveData(getIncidenceRateFilteredOnCalendarFilterValue()))
  },
  handlerExpr = {
    if (any(
      length(consolidatedDatabaseIdTarget()) == 0,
      length(consolidatedCohortIdTarget()) == 0
    )) {
      return(NULL)
    }
    
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    
    data <- getIncidenceRateData()
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget())
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    
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
      data <- data %>%
        dplyr::filter(.data$calendarYear %in% getIncidenceRateFilteredOnCalendarFilterValue())
    }
    
    if (input$irYscaleFixed) {
      data <- data %>%
        dplyr::filter(.data$incidenceRate %in% getIncidenceRateFilteredOnYScaleFilterValue())
    }
    
    validate(need(
      all(!is.null(data), nrow(data) > 0),
      paste0("No data for this combination")
    ))
    
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
    }
    
    getIncidentRatePlot(plot)
  })
  
  ##output: saveIncidenceRateData----
  output$saveIncidenceRateData <-  downloadHandler(
    filename = function()
    {
      getCsvFileNameWithDateTime(string = "IncidenceRate")
    },
    content = function(file)
    {
      downloadCsv(x = getIncidenceRateData(),
                  fileName = file)
    }
  )
  
  ##output: incidenceRatePlot----
  #!!! put generate plot button to prevent reactive rendering of plot before user has finished selecting
  output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
    validate(need(
      length(consolidatedDatabaseIdTarget()) > 0,
      "No data sources chosen"
    ))
    validate(need(
      length(consolidatedCohortIdTarget()) > 0,
      "No cohorts chosen"
    ))
    
    progress <- shiny::Progress$new()
    on.exit(progress$close())
    progress$set(message = paste0("Rendering incidence rate plot."),
                 value = 0)
    
    shiny::withProgress(
      message = paste(
        "Building incidence rate plot data for ",
        length(consolidatedCohortIdTarget()),
        " cohorts and ",
        length(consolidatedDatabaseIdTarget()),
        " databases"
      ),
      {
        return(getIncidentRatePlot())
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
      #!!!consolidatedCohortIdTarget() is returning '' -why?
      if (any(length(consolidatedCohortIdTarget()) == 0))
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
                                        cohortIds =  consolidatedCohortIdTarget())
      return(data)
    } else {
      return(NULL)
    }
  })
  
  ##reactive: getFixedTimeSeriesTsibbleFiltered----
  getFixedTimeSeriesTsibbleFiltered <- reactive({
    if (any(
      length(consolidatedDatabaseIdTarget()) == 0,
      length(consolidatedCohortIdTarget()) == 0
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
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget())
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
      ) 
    # %>%
    #   dplyr::rename(value = titleCaseToCamelCase(input$timeSeriesPlotFilters))
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
  output$fixedTimeSeriesPlot <- plotly::renderPlotly ({
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
    validate(need(titleCaseToCamelCase(input$timeSeriesPlotFilters) %in% colnames(data),
      paste0(paste0(input$timeSeriesPlotFilters, collapse = ","), " not found in tsibble")
    ))
    tsibbleDataFromSTLModel <- getStlModelOutputForTsibbleDataValueFields(tsibbleData = data, 
                                                      valueFields = titleCaseToCamelCase(input$timeSeriesPlotFilters))
    browser()
    plot <- plotTimeSeriesFromTsibble(
      tsibbleData = tsibbleDataFromSTLModel,
      plotFilters = titleCaseToCamelCase(input$timeSeriesPlotFilters),
      indexAggregationType = input$timeSeriesAggregationPeriodSelection,
      timeSeriesStatistics = input$timeSeriesStatistics,
      timeSeriesPeriodRangeFilter = input$timeSeriesPeriodRangeFilter
    )
    
    # distinctCohortShortName <- c()
    # for (i in 1:length(tsibbleDataFromSTLModel)) {
    #   data  <- tsibbleDataFromSTLModel[[i]]$cohortShortName %>% unique()
    #   distinctCohortShortName <- union(distinctCohortShortName,data)
    # }
    # 
    # plot <- plotly::ggplotly(plot)
    return(plot)
  })
  
  #______________----
  #Time Distribution----
  ##output: getTimeDistributionData----
  getTimeDistributionData <- reactive({
    if (any(
      is.null(consolidatedDatabaseIdTarget()),
      length(consolidatedDatabaseIdTarget()) == 0
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
      cohortIds =  consolidatedCohortIdTarget(),
      databaseIds = consolidatedDatabaseIdTarget()
    )
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
      length(consolidatedDatabaseIdTarget()) > 0,
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
    if (any(is.null(input$tabs),!input$tabs == "indexEventBreakdown")) {
      return(NULL)
    }
    if (any(length(consolidatedCohortIdTarget()) == 0)) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"),
            !exists('indexEventBreakdown'))) {
      return(NULL)
    }
    data <-
      getResultsIndexEventBreakdown(dataSource = dataSource,
                                    cohortIds = consolidatedCohortIdTarget())
    return(data)
  })
  
  ##pickerInput: domainTableOptionsInIndexEventData----
  shiny::observe({
    if (!doesObjectHaveData(getIndexEventBreakdownData())) {
      return(NULL)
    }
    data <- getIndexEventBreakdownData() %>%
      dplyr::rename("domainTableShort" = .data$domainTable) %>%
      dplyr::inner_join(
        getDomainInformation()$long %>%
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
    if (!doesObjectHaveData(getIndexEventBreakdownData())) {
      return(NULL)
    }
    data <- getIndexEventBreakdownData() %>%
      dplyr::rename("domainFieldShort" = .data$domainField) %>%
      dplyr::inner_join(
        getDomainInformation()$long %>%
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
    if (!doesObjectHaveData(indexEventBreakdown)) {
      return(NULL)
    }
    if (!doesObjectHaveData(cohortCount)) {
      return(NULL)
    }
    conceptIdDetails <- getConcept(dataSource = dataSource,
                                   conceptIds = indexEventBreakdown$conceptId %>%
                                     unique())
    if (is.null(conceptIdDetails)) {
      return(NULL)
    }
    #!!! future idea for index event breakdown - metric on conceptProportion
    # what proportion of concepts in dataSource is in index event
    
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
      ) %>%
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
      dplyr::inner_join(getDomainInformation()$long,
                        by = c('domainTableShort',
                               'domainFieldShort')) %>%
      dplyr::select(-.data$domainTableShort, -.data$domainFieldShort)
    return(indexEventBreakdown)
  })
  
  ##getIndexEventBreakdownDataFiltered----
  getIndexEventBreakdownDataFiltered <- shiny::reactive(x = {
    indexEventBreakdown <- getIndexEventBreakdownDataEnhanced()
    if (!doesObjectHaveData(indexEventBreakdown)) {
      return(NULL)
    }
    if (!doesObjectHaveData(input$domainTableOptionsInIndexEventData)) {
      return(NULL)
    }
    if (!doesObjectHaveData(input$domainFieldOptionsInIndexEventData)) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedDatabaseIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(consolidatedConceptSetIdTarget())) {
      return(NULL)
    }
    if (!doesObjectHaveData(getResolvedConceptsTarget())) {
      return(NULL)
    }
    indexEventBreakdown <- indexEventBreakdown %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>%
      dplyr::inner_join(
        getResolvedConceptsTarget() %>%
          dplyr::select(.data$conceptId,
                        .data$databaseId) %>%
          dplyr::distinct(),
        by = c("conceptId", "databaseId")
      )
    
    domainTableSelected <- getDomainInformation()$long %>%
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
    if (!doesObjectHaveData(domainTableSelected)) {
      return(NULL)
    }
    
    domainFieldSelected <- getDomainInformation()$long %>%
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
    if (!doesObjectHaveData(domainFieldSelected)) {
      return(NULL)
    }
    
    indexEventBreakdown <- indexEventBreakdown %>%
      dplyr::filter(.data$domainTable %in% domainTableSelected) %>%
      dplyr::filter(.data$domainField %in% domainFieldSelected)
    
    if (input$indexEventBreakdownTableRadioButton == 'All') {
      return(indexEventBreakdown)
    } else if (input$indexEventBreakdownTableRadioButton == "Standard concepts") {
      return(indexEventBreakdown %>% dplyr::filter(.data$standardConcept == 'S'))
    } else {
      #!!!! check why indexEventBreakdown is not returning data for non standard concept
      # why are there no non standard concepts in index event breakdown.
      return(indexEventBreakdown %>% dplyr::filter(is.na(.data$standardConcept)))
    }
    return(indexEventBreakdown)
  })
  
  ##getIndexEventBreakdownDataLong----
  getIndexEventBreakdownDataLong <- shiny::reactive(x = {
    data <- getIndexEventBreakdownDataFiltered()
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    
    if (input$indexEventBreakdownTableFilter == "Records") {
      data <- data %>%
        dplyr::mutate(type = paste0(
          .data$databaseId,
          " (",
          .data$cohortEntries,
          ") ",
          .data$type
        ))
    }
    if (input$indexEventBreakdownTableFilter == "Persons") {
      data <- data %>%
        dplyr::mutate(type = paste0(
          .data$databaseId,
          " (",
          .data$cohortSubjects,
          ") ",
          .data$type
        ))
    }
    
    data <- data %>%
      tidyr::pivot_wider(
        id_cols = c(
          "cohortId",
          "conceptId",
          "conceptName",
          "vocabularyId",
          "domainTable",
          "domainField",
          "cohortEntries",
          "cohortSubjects"
        ),
        names_from = type,
        values_from = count,
        values_fill = 0
      ) %>%
      dplyr::distinct()
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    data <- data[order(-data[9]),]
    return(data)
  })
  
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
      indexEventBreakdownDataTable <-
        getIndexEventBreakdownDataTable()
      validate(
        need(
          doesObjectHaveData(indexEventBreakdownDataTable),
          "No index event breakdown data for the chosen combination."
        )
      )
      data <- indexEventBreakdownDataTable %>%
        dplyr::select(-.data$cohortId, -.data$cohortSubjects, -.data$cohortEntries)
      maxCount <-
        max(indexEventBreakdownDataTable[9], na.rm = TRUE)
      databaseIds <- input$selectedDatabaseIds
      
      personCount <- indexEventBreakdownDataTable %>% 
        dplyr::pull(.data$cohortSubjects) %>% 
        unique()
      
      recordCount <- indexEventBreakdownDataTable %>% 
        dplyr::pull(.data$cohortEntries) %>% 
        unique()
      noOfMergeColumns <- 1
      if (input$indexEventBreakdownTableFilter == "Records") {
        data <- data %>%
          dplyr::select(-dplyr::contains("subjectValue"))
        colnames(data) <-
          stringr::str_replace(
            string = colnames(data),
            pattern = 'conceptValue',
            replacement = ''
          )
        columnColor <- 5 + 1:(length(databaseIds))
      } else if (input$indexEventBreakdownTableFilter == "Persons") {
          data <- data %>%
            dplyr::select(-dplyr::contains("conceptValue"))
          colnames(data) <-
            stringr::str_replace(
              string = colnames(data),
              pattern = 'subjectValue',
              replacement = ''
            )
          columnColor <- 5 + 1:(length(databaseIds))
        } else {
          recordAndPersonColumnName <- c()
          for (i in 1:length(consolidatedDatabaseIdTarget())) {
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
                                                  th(rowspan = 2, "Vocabulary Id"),
                                                  th(rowspan = 2, "Domain Table"),
                                                  th(rowspan = 2, "Domain Field"),
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
          columnColor <- 5 + 1:(length(databaseIds) * 2)
          noOfMergeColumns <- 2
        }
      
      if (input$indexEventBreakdownValueFilter == "Percentage") {
        minimumCellPercent <-
          minCellPercentDef(4 + 1:(length(databaseIds) * noOfMergeColumns))
      } else {
        minimumCellPercent <-
          minCellCountDef(4 + 1:(length(databaseIds) * noOfMergeColumns))
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
          selection = "single",
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
  
  output$dynamicUIForRelationshipAndTemeSeriesForIndexEvent <-
    shiny::renderUI({
      inc <-  1
      panels <- list()
      # Modifying rendered UI after load
      if (any(doesObjectHaveData(consolidatedConceptIdTarget()),doesObjectHaveData(consolidatedConceptIdComparator()))) {
        data <- getMetadataForConceptId()
        panels[[inc]] <- shiny::tabPanel(
          title = "Concept Set Browser",
          value = "conceptSetBrowser",
          shiny::conditionalPanel(
            condition = "output.isConceptIdFromTargetOrComparatorConceptTableSelected==true",
            tags$h4(paste0(
              data$conceptName,
              " (",
              data$conceptId,
              ")"
            )),
            tags$table(width = "100%",
                       tags$tr(
                         tags$td(
                           shinyWidgets::pickerInput(
                             inputId = "choicesForRelationshipNameForIndexEvent",
                             label = "Relationship Category:",
                             choices = c('Not applicable',
                                         data$relationshipName),
                             selected = c('Not applicable',
                                          data$relationshipName),
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
                             inputId = "choicesForRelationshipDistanceForIndexEvent",
                             label = "Distance:",
                             choices = data$conceptAncestorDistance,
                             selected = data$conceptAncestorDistance,
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
                             "saveDetailsOfSelectedConceptIdForIndexEvent",
                             label = "",
                             icon = shiny::icon("download"),
                             style = "margin-top: 5px; margin-bottom: 5px;"
                           )
                         )
                       )),
            DT::dataTableOutput(outputId = "conceptBrowserTableForIndexEvent")
          )
        )
        inc = inc + 1
        panels[[inc]] <- shiny::tabPanel(
          title = "Time Series Plot",
          value = "conceptSetTimeSeriesForIndexEvent",
          tags$h5(paste0(
            data$conceptName,
            " (",
            data$conceptId,
            ")"
          )),
          plotly::plotlyOutput(
            outputId = "conceptSetTimeSeriesPlotForIndexEvent",
            width = "100%",
            height = "100%"
          )
        )
        inc = inc + 1
      }
      
      do.call(tabsetPanel, panels)
    })
  
  ##output: conceptBrowserTableForIndexEvent----
  output$conceptBrowserTableForIndexEvent <- DT::renderDT(expr = {
    if (doesObjectHaveData(consolidateCohortDefinitionActiveSideTarget())) {
      conceptId <- consolidatedConceptIdTarget()
    }
    data <- getMetadataForConceptId()
    validate(need(
      doesObjectHaveData(data),
      "No information for selected concept id."
    ))
    data <- data$conceptRelationshipTable
    
    if (doesObjectHaveData(input$choicesForRelationshipNameForIndexEvent)) {
      data <- data %>%
        dplyr::inner_join(
          relationship %>%
            dplyr::filter(
              .data$relationshipName %in% input$choicesForRelationshipNameForIndexEvent
            ) %>%
            dplyr::select(.data$relationshipId) %>%
            dplyr::distinct(),
          by = "relationshipId"
        )
    }
    if (doesObjectHaveData(input$choicesForRelationshipDistanceForIndexEvent)) {
      data <- data %>%
        dplyr::filter(
          .data$levelsOfSeparation %in%
            input$choicesForRelationshipDistanceForIndexEvent
        )
    }
    
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
  
  ##output: conceptSetTimeSeriesPlotForIndexEvent----
  output$conceptSetTimeSeriesPlotForIndexEvent <-
    plotly::renderPlotly({
      data <- getMetadataForConceptId()
      if (!doesObjectHaveData(data)) {
        return(null)
      }
      # working on the plot
      if (input$timeSeriesAggregationPeriodSelection == "Monthly") {
        data <- data$databaseConceptIdYearMonthLevelTsibble %>% 
          dplyr::filter(.data$conceptId == consolidatedConceptIdTarget())
      } else {
        data <- data$databaseConceptIdYearLevelTsibble %>% 
          dplyr::filter(.data$conceptId == consolidatedConceptIdTarget())
      }
      data <- data %>% 
        dplyr::filter(.data$domainTableShort == "All") %>% #need input object
        dplyr::filter(.data$domainFieldShort == "All") #need input object
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
        dplyr::filter(.data$databaseId %in% input$selectedDatabaseIds) %>% 
        dplyr::rename("records" = .data$conceptCount,
                      "persons" = .data$subjectCount)
      tsibbleDataFromSTLModel <- getStlModelOutputForTsibbleDataValueFields(tsibbleData = data,
                                                                            valueFields = c("records", "persons"))
      
      
      
      plot <- plotTimeSeriesForCohortDefinitionFromTsibble(
        stlModeledTsibbleData = tsibbleDataFromSTLModel
      )
      return(plot)
    })
  
  #______________----
  # Visit Context -----
  ##getVisitContextData----
  getVisitContextData <- shiny::reactive(x = {
    if (all(doesObjectHaveData(input$tab),
            input$tab != "visitContext")) {
      return(NULL)
    }
    if (any(
      is.null(consolidatedDatabaseIdTarget()),
      length(consolidatedDatabaseIdTarget()) == 0
    )) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"), !exists('visitContext')))
    {
      return(NULL)
    }
    visitContext <-
      getResultsVisitContext(dataSource = dataSource,
                             cohortIds = consolidatedCohortIdTarget())
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
      is.null(consolidatedDatabaseIdTarget()),
      length(consolidatedDatabaseIdTarget()) == 0
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
      !doesObjectHaveData(input$visitContextTableFilters),
      !doesObjectHaveData(input$visitContextPersonOrRecords),
      !doesObjectHaveData(consolidatedCohortIdTarget())
    )) {
      return(NULL)
    }
    data <- getVisitContexDataEnhanced()
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$cohortId == consolidatedCohortIdTarget()) %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget())
    
    if (input$visitContextTableFilters == "Before") {
      data <- data %>%
        dplyr::filter(.data$visitContext == "Before")
    } else if (input$visitContextTableFilters == "During") {
      data <- data %>%
        dplyr::filter(.data$visitContext == "During visit")
    } else if (input$visitContextTableFilters == "During") {
      data <- data %>%
        dplyr::filter(.data$visitContext == "On visit start")
    } else if (input$visitContextTableFilters == "After") {
      data <- data %>%
        dplyr::filter(.data$visitContext == "After")
    }
    isPerson <- input$visitContextPersonOrRecords == 'Person'
    if (isPerson)
    {
      data <- data %>%
        dplyr::select(-.data$records)
    } else {
      data <- data %>%
        dplyr::select(-.data$subjects)
    }
    return(data)
  })
  
  ##getVisitContextTableData----
  getVisitContextTableData <- shiny::reactive(x = {
    data <- getVisitContexDataFiltered()
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    # Apply Pivot Longer
    pivotColumns <- c()
    if (input$visitContextPersonOrRecords == 'Person') {
      pivotColumns <- c("subjects")
    } else {
      pivotColumns <- c("records")
    }
    data <- data %>%
      tidyr::pivot_longer(names_to = "type",
                          cols = pivotColumns,
                          values_to = "count")
    data <- tidyr::replace_na(data,
                              replace = list("count" = 0))
    
    #Apply Pivot Wider
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
      length(consolidatedDatabaseIdTarget()) > 0,
      "No data sources chosen"
    ))
    validate(need(
      length(consolidatedCohortIdTarget()) > 0,
      "No cohorts chosen"
    ))
    data <- getVisitContextTableData()
    validate(need(
      doesObjectHaveData(data),
      "No data available for selected combination."
    ))
    table <- data %>%
      dplyr::select(-.data$cohortId)
    
    # header labels
    cohortCounts <- cohortCount %>%
      dplyr::filter(.data$cohortId == consolidatedCohortIdTarget()) %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>%
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
      maxSubjects <-
        getVisitContexDataFiltered()$subjects %>% max(na.rm = TRUE)
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
      maxSubjects <-
        getVisitContexDataFiltered()$records %>% max(na.rm = TRUE)
    }
    
    
    visitContextSequence <-
      getVisitContexDataFiltered()$visitContext %>%
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
      length(consolidatedDatabaseIdTarget()) == 0,
      length(consolidatedCohortIdTarget()) == 0
    )) {
      return(NULL)
    }
    if (all(is(dataSource, "environment"),
            !exists('cohortRelationships')))
    {
      return(NULL)
    }
    data <- getCohortOverlap(dataSource = dataSource,
                             cohortIds = consolidatedCohortIdTarget())
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    return(data)
  })
  
  ##getCohortOverlapDataFiltered----
  getCohortOverlapDataFiltered <- reactive(x = {
    data <- getCohortOverlapData()
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget())
    return(data)
  })
  
  ##output: overlapPlot----
  output$overlapPlot <- ggiraph::renderggiraph(expr = {
    validate(need(
      length(consolidatedCohortIdTarget()) > 0,
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
  getConceptSetNamesFromOneCohort <-
    shiny::reactive(x = {
      #!!!!!!!!!!!consolidated
      if (any(
        length(consolidatedCohortIdTarget()) == 0,
        length(consolidatedDatabaseIdTarget()) == 0
      )) {
        return(NULL)
      }
      
      jsonExpression <- getCohortSortedByCohortId() %>%
        dplyr::filter(.data$cohortId == consolidatedCohortIdTarget()) %>%
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
      length(consolidatedCohortIdTarget()) != 1,
      length(consolidatedDatabaseIdTarget()) == 0
    )) {
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
      cohortIds = consolidatedCohortIdTarget(),
      databaseIds = consolidatedDatabaseIdTarget()
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
      !is.null(input$conceptSetsSelectedCohortLeft),
      length(input$conceptSetsSelectedCohortLeft) > 0,
      input$conceptSetsSelectedCohortLeft != ""
    )) {
      covariatesTofilter <- covariatesTofilter  %>%
        dplyr::inner_join(
          getResolvedConceptsTarget() %>%
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
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
          cohortId = sort(consolidatedCohortIdTarget())[[1]],
          databaseId = sort(consolidatedDatabaseIdTarget()[[1]])
        ),
      characteristics %>%
        dplyr::filter(.data$header == 0) %>%
        tidyr::crossing(
          dplyr::tibble(databaseId = consolidatedDatabaseIdTarget())
        ) %>%
        tidyr::crossing(
          dplyr::tibble(cohortId = consolidatedCohortIdTarget())
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
    if (!doesObjectHaveData(data)) {
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
    
    if (!doesObjectHaveData(data)) {
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
      !is.null(consolidatedCohortIdTarget()),
      length(consolidatedCohortIdTarget()) > 0
    ),
    "No data for the combination"))
    validate(need(!is.null(data), "No data for the combination"))
    
    databaseIds <- sort(unique(data$databaseId))
    
    cohortCounts <- data %>%
      dplyr::inner_join(cohortCount,
                        by = c("cohortId", "databaseId")) %>%
      dplyr::filter(.data$cohortId == consolidatedCohortIdTarget()) %>%
      dplyr::filter(.data$databaseId %in% consolidatedDatabaseIdTarget()) %>%
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
      if (!doesObjectHaveData(data)) {
        return(NULL)
      }
      
      if (all(
        !is.null(input$conceptSetsSelectedCohortLeft),
        length(input$conceptSetsSelectedCohortLeft) > 0
      )) {
        data <- data  %>%
          dplyr::inner_join(
            getResolvedConceptsTarget() %>%
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
      
      if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
    
    if (!doesObjectHaveData(data)) {
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
        columnDefs = list(truncateStringDef(1, 70),
                          minCellPercentDef(1 + 1:(
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
        columns = (2 + (
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
        length(consolidatedCohortIdTarget()) != 1,
        length(getComparatorCohortIdFromSelectedCompoundCohortName()) != 1,
        length(consolidatedDatabaseIdTarget()) == 0
      )) {
        return(NULL)
      }
      
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0(
          "Extracting temporal characterization data for target cohort:",
          consolidatedCohortIdTarget(),
          " and comparator cohort:",
          getComparatorCohortIdFromSelectedCompoundCohortName(),
          ' for ',
          input$selectedDatabaseId
        ),
        value = 0
      )
      
      data <- getMultipleCharacterizationResults(
        dataSource = dataSource,
        cohortIds = c(
          consolidatedCohortIdTarget(),
          getComparatorCohortIdFromSelectedCompoundCohortName()
        ) %>% unique(),
        databaseIds = input$selectedDatabaseId
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
    if (!doesObjectHaveData(data)) {
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
      !is.null(input$conceptSetsSelectedCohortLeft),
      length(input$conceptSetsSelectedCohortLeft) > 0
    )) {
      data <- data  %>%
        dplyr::inner_join(
          getResolvedConceptsTarget() %>%
            dplyr::select(.data$conceptId) %>%
            dplyr::distinct(),
          by = c("conceptId")
        )
    }
    
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
      return(NULL)
    }
    data <- data %>%
      dplyr::mutate(covariateName = .data$covariateNameFull) %>%
      prepareTable1Comp()
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
      !is.null(input$conceptSetsSelectedCohortLeft),
      input$conceptSetsSelectedCohortLeft != "",
      length(input$conceptSetsSelectedCohortLeft) > 0
    )) {
      data <-
        data %>% #!!! there is a bug here getResoledAndMappedConceptIdsForFilters
        dplyr::filter(.data$conceptId %in% getResoledAndMappedConceptIdsForFilters())
    }
    if (!doesObjectHaveData(data)) {
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
    if (!doesObjectHaveData(data)) {
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
      if (!doesObjectHaveData(data)) {
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
        !is.null(input$conceptSetsSelectedCohortLeft),
        length(input$conceptSetsSelectedCohortLeft) > 0
      )) {
        data <- data  %>%
          dplyr::inner_join(
            getResolvedConceptsTarget() %>%
              dplyr::select(.data$conceptId) %>%
              dplyr::distinct(),
            by = c("conceptId")
          )
      }
      
      if (!doesObjectHaveData(data)) {
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
      if (!doesObjectHaveData(data)) {
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
      dplyr::filter(.data$databaseId == input$selectedDatabaseId)
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
      if (!doesObjectHaveData(data)) {
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
  
  selectedCohorts <- shiny::reactive({
    if (any(
      is.null(consolidatedCohortIdTarget()),
      length(consolidatedCohortIdTarget()) == 0
    )) {
      return(NULL)
    }
    if (any(is.null(getCohortSortedByCohortId()),
            nrow(getCohortSortedByCohortId()) == 0))
    {
      return(NULL)
    }
    if (any(
      is.null(consolidatedDatabaseIdTarget()),
      nrow(consolidatedDatabaseIdTarget()) == 0
    ))
    {
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
    return(input$selectedCompoundCohortName)
  })
  
  selectedComparatorCohort <- shiny::reactive({
    return(input$selectedComparatorCompoundCohortNames)
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
                length(cohortSubjectRecordRatioEq1) / length(consolidatedDatabaseIdTarget()),
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
              length(consolidatedDatabaseIdTarget()),
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
      return(input$selectedDatabaseId)
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
})
