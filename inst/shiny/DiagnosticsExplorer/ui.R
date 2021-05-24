addInfo <- function(item, infoId) {
  infoTag <- tags$small(
    class = "badge pull-right action-button",
    style = "padding: 1px 6px 2px 6px; background-color: steelblue;",
    type = "button",
    id = infoId,
    "i"
  )
  item$children[[1]]$children <-
    append(item$children[[1]]$children, list(infoTag))
  return(item)
}

cohortReference <- function(outputId) {
  shinydashboard::box(
    # title = "Reference",
    status = "warning",
    width = "100%",
    tags$div(style = "max-height: 100px; overflow-y: auto",
             shiny::uiOutput(outputId = outputId))
  )
}

cohortReferenceWithDatabaseId <- function(cohortOutputId, databaseOutputId) {
  shinydashboard::box(
    # title = "Reference",
    status = "warning",
    width = "100%",
    tags$div(style = "max-height: 100px; overflow-y: auto",
             tags$table(width = "100%",
                        tags$tr(
                          tags$td(width = "70%",
                                  tags$b("Cohorts :"),
                                  shiny::uiOutput(outputId = cohortOutputId)
                          ),
                          tags$td(style = "align: right !important;", width = "30%",
                                  tags$b("Database :"),
                                  shiny::uiOutput(outputId = databaseOutputId)
                          )
                        )
             )
    )
  )
}

if (is(dataSource, "environment")) {
  choicesFordatabaseOrVocabularySchema <-
    c(database$databaseIdWithVocabularyVersion)
} else {
  choicesFordatabaseOrVocabularySchema <- list(
    'From site' = database$databaseIdWithVocabularyVersion,
    'Reference Vocabulary' = vocabularyDatabaseSchemas
  )
}

header <-
  shinydashboard::dashboardHeader(title = "Cohort Diagnostics")

sidebarMenu <-
  shinydashboard::sidebarMenu(
    id = "tabs",
    if (exists("cohort"))
      shinydashboard::menuItem(text = "Cohort Definition", tabName = "cohortDefinition"),
    if (exists("includedSourceConcept"))
      addInfo(
        item = shinydashboard::menuItem(text = "Concepts in Data Source", tabName = "includedConcepts"),
        infoId = "includedConceptsInfo"
      ),
    if (exists("orphanConcept"))
      addInfo(
        item = shinydashboard::menuItem(text = "Orphan Concepts", tabName = "orphanConcepts"),
        infoId = "orphanConceptsInfo"
      ),
    if (exists("cohortCount"))
      addInfo(
        item = shinydashboard::menuItem(text = "Cohort Counts", tabName = "cohortCounts"),
        infoId = "cohortCountsInfo"
      ),
    if (exists("incidenceRate"))
      addInfo(
        item = shinydashboard::menuItem(text = "Incidence Rate", tabName = "incidenceRate"),
        infoId = "incidenceRateInfo"
      ),
    if (exists("timeDistribution"))
      addInfo(
        item = shinydashboard::menuItem(text = "Time Distributions", tabName = "timeDistribution"),
        infoId = "timeDistributionInfo"
      ),
    if (exists("inclusionRuleStats"))
      addInfo(
        item = shinydashboard::menuItem(text = "Inclusion Rule Statistics", tabName = "inclusionRuleStats"),
        infoId = "inclusionRuleStatsInfo"
      ),
    if (exists("indexEventBreakdown"))
      addInfo(
        item = shinydashboard::menuItem(text = "Index Event Breakdown", tabName = "indexEventBreakdown"),
        infoId = "indexEventBreakdownInfo"
      ),
    if (exists("visitContext"))
      addInfo(
        item = shinydashboard::menuItem(text = "Visit Context", tabName = "visitContext"),
        infoId = "visitContextInfo"
      ),
    if (exists("cohortOverlap"))
      addInfo(
        shinydashboard::menuItem(text = "Cohort Overlap", tabName = "cohortOverlap"),
        infoId = "cohortOverlapInfo"
      ),
    if (exists("covariateValue"))
      addInfo(
        shinydashboard::menuItem(text = "Cohort Characterization", tabName = "cohortCharacterization"),
        infoId = "cohortCharacterizationInfo"
      ),
    if (exists("temporalCovariateValue"))
      addInfo(
        shinydashboard::menuItem(text = "Temporal Characterization", tabName = "temporalCharacterization"),
        infoId = "temporalCharacterizationInfo"
      ),
    if (exists("covariateValue"))
      addInfo(
        item = shinydashboard::menuItem(text = "Compare Cohort Char.", tabName = "compareCohortCharacterization"),
        infoId = "compareCohortCharacterizationInfo"
      ),
    if (exists("temporalCovariateValue"))
      addInfo(
        shinydashboard::menuItem(text = "Compare Temporal Char.", tabName = "compareTemporalCharacterization"),
        infoId = "compareTemporalCharacterizationInfo"
      ),
    shinydashboard::menuItem(text = "Data Source Information", tabName = "databaseInformation"),
    # Conditional dropdown boxes in the side bar ------------------------------------------------------
    shiny::conditionalPanel(
      condition = "input.tabs!='incidenceRate' &
      input.tabs != 'timeDistribution' &
      input.tabs != 'cohortCharacterization' &
      input.tabs != 'cohortCounts' &
      input.tabs != 'indexEventBreakdown' &
      input.tabs != 'databaseInformation' &
      input.tabs != 'cohortDefinition' &
      input.tabs != 'includedConcepts' &
      input.tabs != 'orphanConcepts' &
      input.tabs != 'inclusionRuleStats' &
      input.tabs != 'visitContext' &
      input.tabs != 'cohortOverlap'",
      shinyWidgets::pickerInput(
        inputId = "database",
        label = "Database",
        choices = database$databaseId,
        selected = database$databaseId[1],
        multiple = FALSE,
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
    shiny::conditionalPanel(
      condition = "input.tabs=='incidenceRate' |
      input.tabs == 'timeDistribution' |
      input.tabs =='cohortCharacterization' |
      input.tabs == 'cohortCounts' |
      input.tabs == 'indexEventBreakdown' |
      input.tabs == 'includedConcepts' |
      input.tabs == 'orphanConcepts' |
      input.tabs == 'inclusionRuleStats' |
      input.tabs == 'visitContext' |
      input.tabs == 'cohortOverlap'",
      shinyWidgets::pickerInput(
        inputId = "databases",
        label = "Database",
        choices = database$databaseId,
        selected = database$databaseId[1],
        multiple = TRUE,
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
    if (exists("temporalCovariateValue")) {
      shiny::conditionalPanel(
        condition = "input.tabs=='temporalCharacterization' | input.tabs =='compareTemporalCharacterization'",
        shinyWidgets::pickerInput(
          inputId = "timeIdChoices",
          label = "Temporal Choice",
          choices = temporalCovariateChoices$choices,
          multiple = TRUE,
          choicesOpt = list(style = rep_len("color: black;", 999)),
          selected = temporalCovariateChoices %>%
            dplyr::filter(.data$timeId %in% (
              c(
                min(temporalCovariateChoices$timeId),
                temporalCovariateChoices %>%
                  dplyr::pull(.data$timeId)
              ) %>%
                unique() %>%
                sort()
            )) %>%
            dplyr::pull("choices"),
          options = shinyWidgets::pickerOptions(
            actionsBox = TRUE,
            liveSearch = TRUE,
            size = 10,
            liveSearchStyle = "contains",
            liveSearchPlaceholder = "Type here to search",
            virtualScroll = 50
          )
        )
      )
    },
    shiny::conditionalPanel(
      condition = "input.tabs != 'databaseInformation' &
      input.tabs != 'cohortDefinition' &
      input.tabs != 'cohortCounts' &
      input.tabs != 'cohortOverlap'&
      input.tabs != 'incidenceRate' &
      input.tabs != 'timeDistribution'",
      shinyWidgets::pickerInput(
        inputId = "cohort",
        label = "Cohort",
        choices = c(""),
        multiple = FALSE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          liveSearchStyle = "contains",
          size = 10,
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50
        )
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs == 'cohortCounts' |
      input.tabs == 'cohortOverlap' |
      input.tabs == 'incidenceRate' |
      input.tabs == 'timeDistribution'",
      shinyWidgets::pickerInput(
        inputId = "cohorts",
        label = "Cohorts",
        choices = c(""),
        selected = c(""),
        multiple = TRUE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          liveSearchStyle = "contains",
          size = 10,
          dropupAuto = TRUE,
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50
        )
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs == 'compareCohortCharacterization'|
        input.tabs == 'compareTemporalCharacterization'",
      shinyWidgets::pickerInput(
        inputId = "comparatorCohort",
        label = "Comparator",
        choices = c(""),
        multiple = FALSE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          liveSearchStyle = "contains",
          size = 10,
          dropupAuto = TRUE,
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50
        )
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs == 'cohortCharacterization' |
      input.tabs == 'compareCohortCharacterization' |
      input.tabs == 'temporalCharacterization' |
      input.tabs == 'compareTemporalCharacterization' |
      input.tabs == 'includedConcepts' |
      input.tabs == 'orphanConcepts'",
      shinyWidgets::pickerInput(
        inputId = "conceptSetsToFilterCharacterization",
        label = "Concept sets",
        choices = c(""),
        selected = c(""),
        multiple = TRUE,
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
    )
  )

#Side bar code
sidebar <-
  shinydashboard::dashboardSidebar(sidebarMenu, 
                                   width = NULL, 
                                   collapsed = FALSE
  )

# Body - items in tabs --------------------------------------------------
bodyTabItems <- shinydashboard::tabItems(
  shinydashboard::tabItem(tabName = "about",
                          if (exists("aboutText"))
                            HTML(aboutText)),
  shinydashboard::tabItem(
    tabName = "cohortDefinition",
    shinydashboard::box(
      width = NULL,
      status = "primary",
      column(6,tags$h4("Cohort Definition")),
      column(6,
             tags$table(width = "100%",
                        tags$tr(
                          tags$td(
                            align = "right",
                            shiny::downloadButton(
                              outputId = "saveCohortDefinitionButton",
                              label = NULL,
                              icon = shiny::icon("download"),
                              style = "margin-top: 5px; margin-bottom: 5px;"
                            )
                          )
                        ))),    
      DT::dataTableOutput(outputId = "cohortDefinitionTable"),
      column(
        12,
        conditionalPanel(
          "output.cohortDefinitionRowIsSelected == true",
          shiny::tabsetPanel(
            type = "tab",
            shiny::tabPanel(title = "Details",
                            shiny::htmlOutput("cohortDetailsText")),
            shiny::tabPanel(title = "Cohort Count",
                            tags$br(),
                            DT::dataTableOutput(outputId = "cohortCountsTableInCohortDefinition")),
            shiny::tabPanel(title = "Cohort definition",
                            copyToClipboardButton(toCopyId = "cohortDefinitionText",
                                                  style = "margin-top: 5px; margin-bottom: 5px;"),
                            shiny::htmlOutput("cohortDefinitionText")),
            shiny::tabPanel(
              title = "Concept Sets",
              tags$table(width = "100%",
                         tags$tr(tags$td(
                           align = "right",
                           # shiny::downloadButton(
                           #   "saveConceptSetButton",
                           #   label = "",
                           #   icon = shiny::icon("download"),
                           #   style = "margin-top: 5px; margin-bottom: 5px;"
                           # )
                         ))),
              DT::dataTableOutput(outputId = "conceptsetExpressionTable"),
              shiny::conditionalPanel(condition = "output.conceptSetExpressionRowSelected == true",
                                      tags$table(tags$tr(
                                        tags$td(
                                          shiny::radioButtons(
                                            inputId = "conceptSetsType",
                                            label = "",
                                            choices = c("Concept Set Expression",
                                                        "Resolved",
                                                        "Mapped",
                                                        "Orphan concepts",
                                                        "Json"),
                                            selected = "Concept Set Expression",
                                            inline = TRUE
                                          )
                                        ),
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
                                          shiny::htmlOutput("subjectCountInCohortConceptSet")
                                        ),
                                        tags$td(
                                          shiny::htmlOutput("recordCountInCohortConceptSet")
                                        )
                                      ))),
              shiny::conditionalPanel(
                condition = "output.conceptSetExpressionRowSelected == true &
                input.conceptSetsType != 'Resolved' &
                input.conceptSetsType != 'Mapped' &
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
                condition = "input.conceptSetsType == 'Resolved'",
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
                condition = "input.conceptSetsType == 'Mapped'",
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
              shiny::verbatimTextOutput("cohortDefinitionSql"),
              tags$head(
                tags$style("#cohortDefinitionSql { max-height:400px};")
              )
            )
          )
        )
      ),
    )
  ),
  shinydashboard::tabItem(
    tabName = "cohortCounts",
    cohortReference("cohortCountsSelectedCohorts"),
    shiny::radioButtons(
      inputId = "cohortCountsTableColumnFilter",
      label = "Display",
      choices = c("Both", "Subjects Only", "Records Only"), 
      selected = "Both",
      inline = TRUE
    ),
    tags$table(width = "100%", 
               tags$tr(
                 tags$td(align = "right",
                         shiny::downloadButton(
                           "saveCohortCountsTable",
                           label = "",
                           icon = shiny::icon("download"),
                           style = "margin-top: 5px; margin-bottom: 5px;"
                         )
                 )
               )
    ),
    DT::dataTableOutput("cohortCountsTable"),
    shiny::conditionalPanel(
      condition = "output.cohortCountRowIsSelected == true",
      DT::dataTableOutput("InclusionRuleStatForCohortSeletedTable")
    )
  ),
  shinydashboard::tabItem(
    tabName = "incidenceRate",
    cohortReference("incidenceRateSelectedCohorts"),
    shinydashboard::box(
      title = "Incidence Rate",
      width = NULL,
      status = "primary",
      tags$table(style = "width: 100%",
                 tags$tr(
                   tags$td(
                     valign = "bottom",
                     shiny::checkboxGroupInput(
                       inputId = "irStratification",
                       label = "Stratify by",
                       choices = c("Age", "Gender", "Calendar Year"),
                       selected = c("Age", "Gender", "Calendar Year"),
                       inline = TRUE
                     )
                   ),
                   tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;")),
                   tags$td(
                     valign = "bottom",
                     style = "width:30% !important;margin-top:10px;",
                     shiny::conditionalPanel(
                       condition = "input.irYscaleFixed",
                       shiny::sliderInput(
                         inputId = "YscaleMinAndMax",
                         label = "Limit y-scale range to:",
                         min = c(0),
                         max = c(0),
                         value = c(0, 0),
                         dragRange = TRUE,width = 400,
                         step = 1,
                         sep = "",
                       )
                     )
                   ),
                   tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;")),
                   tags$td(
                     valign = "bottom",
                     style = "text-align: right",
                     shiny::checkboxInput("irYscaleFixed", "Use same y-scale across databases")
                   )
                 )),
      tags$table(tags$tr(
        tags$td(
          shiny::conditionalPanel(
            condition = "input.irStratification.indexOf('Age') > -1",
            shinyWidgets::pickerInput(
              inputId = "incidenceRateAgeFilter",
              label = "Filter By Age",
              width = 400,
              choices = c("All"),
              selected = c("All"),
              multiple = TRUE,
              choicesOpt = list(style = rep_len("color: black;", 999)),
              options = shinyWidgets::pickerOptions(
                actionsBox = TRUE,
                liveSearch = TRUE,
                size = 10,
                dropupAuto = TRUE,
                liveSearchStyle = "contains",
                liveSearchPlaceholder = "Type here to search",
                virtualScroll = 50
              )
            )
          )
        ),
        tags$td(
          shiny::conditionalPanel(
            condition = "input.irStratification.indexOf('Gender') > -1",
            shinyWidgets::pickerInput(
              inputId = "incidenceRateGenderFilter",
              label = "Filter By Gender",
              width = 200,
              choices = c("All"),
              selected = c("All"),
              multiple = TRUE,
              choicesOpt = list(style = rep_len("color: black;", 999)),
              options = shinyWidgets::pickerOptions(
                actionsBox = TRUE,
                liveSearch = TRUE,
                size = 10,
                dropupAuto = TRUE,
                liveSearchStyle = "contains",
                liveSearchPlaceholder = "Type here to search",
                virtualScroll = 50
              )
            )
          )
        ),
        tags$td(
          style = "width:30% !important",
          shiny::conditionalPanel(
            condition = "input.irStratification.indexOf('Calendar Year') > -1",
            shiny::sliderInput(
              inputId = "incidenceRateCalenderFilter",
              label = "Filter By Calender Year",
              min = c(0),
              max = c(0),
              value = c(0, 0),
              dragRange = TRUE,
              pre = "Year ",
              step = 1,
              sep = ""
            )
          )
        ),
        tags$td(
          shiny::numericInput(
            inputId = "minPersonYear",
            label = "Minimum person years",
            value = 1000,
            min = 0
          )
        ),
        tags$td(
          shiny::numericInput(
            inputId = "minSubjetCount",
            label = "Minimum subject count",
            value = NULL
          )
        ),
        tags$td(
          tags$table(width = "100%", 
                     tags$tr(
                       tags$td(align = "right",
                               shiny::downloadButton(
                                 "saveIncidenceRatePlot",
                                 label = "",
                                 icon = shiny::icon("download"),
                                 style = "margin-top: 5px; margin-bottom: 5px;"
                               )
                       )
                     )
          ), 
        )
      )),
      shiny::htmlOutput(outputId = "hoverInfoIr"),
      ggiraph::ggiraphOutput(
        outputId = "incidenceRatePlot",
        width = "100%",
        height = "100%"
      )
    )
  ),
  shinydashboard::tabItem(
    tabName = "timeDistribution",
    cohortReference("timeDistSelectedCohorts"),
    shiny::radioButtons(
      inputId = "timeDistributionType",
      label = "",
      choices = c("Table", "Plot"),
      selected = "Plot",
      inline = TRUE
    ),
    shiny::conditionalPanel(condition = "input.timeDistributionType=='Table'",
                            tags$table(width = "100%", 
                                       tags$tr(
                                         tags$td(align = "right",
                                                 shiny::downloadButton(
                                                   "saveTimeDistTable",
                                                   label = "",
                                                   icon = shiny::icon("download"),
                                                   style = "margin-top: 5px; margin-bottom: 5px;"
                                                 )
                                         )
                                       )
                            ),
                            DT::dataTableOutput("timeDistTable")),
    shiny::conditionalPanel(
      condition = "input.timeDistributionType=='Plot'",
      shinydashboard::box(
        title = "Time Distributions",
        width = NULL,
        status = "primary",
        tags$br(),
        ggiraph::ggiraphOutput("timeDisPlot", width = "100%", height = "100%")
      )
    )
  ),
  shinydashboard::tabItem(
    tabName = "includedConcepts",
    cohortReference("includedConceptsSelectedCohort"),
    shinydashboard::box(
      title = "Concepts in Data Source",
      width = NULL,
      column(
        4,
        shiny::radioButtons(
          inputId = "includedType",
          label = "",
          choices = c("Source fields", "Standard fields"),
          selected = "Standard fields",
          inline = TRUE
        )
      ),
      column(
        4,
        shiny::radioButtons(
          inputId = "includedConceptsTableColumnFilter",
          label = "",
          choices = c("Both", "Subjects only", "Records only"), # 
          selected = "Subjects only",
          inline = TRUE
        )
      ),
      column(4,
             tags$table(width = "100%",
                        tags$tr(
                          tags$td(
                            align = "right",
                            shiny::downloadButton(
                              "saveIncludedConceptsTable",
                              label = "",
                              icon = shiny::icon("download"),
                              style = "margin-top: 5px; margin-bottom: 5px;"
                            )
                          )
                        ))),
    DT::dataTableOutput("includedConceptsTable")
    )
  ),
  shinydashboard::tabItem(
    tabName = "orphanConcepts",
    cohortReference("orphanConceptsSelectedCohort"),
    tags$table(
      tags$tr(
        tags$td(
          shiny::radioButtons(
            inputId = "orphanConceptsType",
            label = "Filters",
            choices = c("All", "Standard Only", "Non Standard Only"),
            selected = "All",
            inline = TRUE
          )
        ),
        tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
        tags$td(
          shiny::radioButtons(
            inputId = "orphanConceptsColumFilterType",
            label = "Display",
            choices = c("All", "Subjects only","Records only"),
            selected = "All",
            inline = TRUE
          )
        )
      )
    ),
    tags$table(width = "100%", 
               tags$tr(
                 tags$td(align = "right",
                         shiny::downloadButton(
                           "saveOrphanConceptsTable",
                           label = "",
                           icon = shiny::icon("download"),
                           style = "margin-top: 5px; margin-bottom: 5px;"
                         )
                 )
               )
    ),
    DT::dataTableOutput(outputId = "orphanConceptsTable")
  ),
  shinydashboard::tabItem(
    tabName = "inclusionRuleStats",
    cohortReference("inclusionRuleStatSelectedCohort"),
    column(
      6,
      shiny::radioButtons(
        inputId = "inclusionRuleTableFilters",
        label = "Inclusion Rule Events",
        choices = c("All", "Meet", "Gain", "Remain", "Totals"),
        selected = "All",
        inline = TRUE
      )
    ),
    column(6,
           tags$table(width = "100%",
                      tags$tr(
                        tags$td(
                          align = "right",
                          shiny::downloadButton(
                            "saveInclusionRuleTable",
                            label = "",
                            icon = shiny::icon("download"),
                            style = "margin-top: 5px; margin-bottom: 5px;"
                          )
                        )
                      ))),
    DT::dataTableOutput(outputId = "inclusionRuleTable")
  ),
  shinydashboard::tabItem(
    tabName = "indexEventBreakdown",
    cohortReference("indexEventBreakdownSelectedCohort"),
    tags$table(
      tags$tr(
        # tags$td(
        #   shinyWidgets::pickerInput(
        #     inputId = "breakdownDomainTable",
        #     label = "Domain Table",
        #     choices = c(""),
        #     multiple = TRUE,
        #     choicesOpt = list(style = rep_len("color: black;", 999)),
        #     options = shinyWidgets::pickerOptions(
        #       actionsBox = TRUE,
        #       liveSearch = TRUE,
        #       liveSearchStyle = "contains",
        #       size = 10,
        #       liveSearchPlaceholder = "Type here to search",
        #       virtualScroll = 50
        #     )
        #   )
        # ),
        # tags$td(
        #   shinyWidgets::pickerInput(
        #     inputId = "breakdownDomainField",
        #     label = "Domain Field",
        #     choices = c(""),
        #     multiple = TRUE,
        #     choicesOpt = list(style = rep_len("color: black;", 999)),
        #     options = shinyWidgets::pickerOptions(
        #       actionsBox = TRUE,
        #       liveSearch = TRUE,
        #       liveSearchStyle = "contains",
        #       size = 10,
        #       liveSearchPlaceholder = "Type here to search",
        #       virtualScroll = 50
        #     )
        #   )
        # ),
        tags$td(
          shiny::radioButtons(
            inputId = "indexEventBreakdownTableRadioButton",
            label = "",
            choices = c("All", "Standard concepts", "Non Standard Concepts"),
            selected = "All",
            inline = TRUE
          )
        ),
        tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
        tags$td(
          shiny::radioButtons(
            inputId = "indexEventBreakdownTableFilter",
            label = "Display",
            choices = c("Both", "Records", "Persons"), 
            selected = "Persons",
            inline = TRUE
          )
        )
      )
    ),
    tags$table(width = "100%", 
               tags$tr(
                 tags$td(align = "right",
                         shiny::downloadButton(
                           "saveBreakdownTable",
                           label = "",
                           icon = shiny::icon("download"),
                           style = "margin-top: 5px; margin-bottom: 5px;"
                         )
                 )
               )
    ),
    DT::dataTableOutput(outputId = "breakdownTable")
  ),
  shinydashboard::tabItem(
    tabName = "visitContext",
    cohortReference("visitContextSelectedCohort"),
    column(
      6,
      shiny::radioButtons(
        inputId = "visitContextTableFilters",
        label = "Display",
        choices = c("All", "Before", "During", "Simultaneous", "After"),
        selected = "All",
        inline = TRUE
      )
    ),
    column(6,
           tags$table(width = "100%",
                      tags$tr(
                        tags$td(
                          align = "right",
                          shiny::downloadButton(
                            "saveVisitContextTable",
                            label = "",
                            icon = shiny::icon("download"),
                            style = "margin-top: 5px; margin-bottom: 5px;"
                          )
                        )
                      ))),
    DT::dataTableOutput(outputId = "visitContextTable")
  ),
  shinydashboard::tabItem(
    tabName = "cohortOverlap",
    cohortReference("cohortOverlapSelectedCohort"),
    shinydashboard::box(
      title = "Cohort Overlap (Subjects)",
      width = NULL,
      status = "primary",
      shiny::radioButtons(
        inputId = "overlapPlotType",
        label = "",
        choices = c("Percentages", "Counts"),
        selected = "Percentages",
        inline = TRUE
      ),
      ggiraph::ggiraphOutput("overlapPlot", width = "100%", height = "100%")
    )
  ),
  shinydashboard::tabItem(
    tabName = "cohortCharacterization",
    cohortReference("characterizationSelectedCohort"),
    tags$table(
      tags$tr(
        tags$td(
          shiny::radioButtons(
            inputId = "charType",
            label = "",
            choices = c("Pretty", "Raw"),
            selected = "Pretty",
            inline = TRUE
          )
        ),
        tags$td(
          shiny::conditionalPanel(condition = "input.charType == 'Raw'",
                                  tags$table(tags$tr(
                                    tags$td(
                                      shinyWidgets::pickerInput(
                                        inputId = "characterizationAnalysisNameFilter",
                                        label = "Analysis name",
                                        choices = c(""),
                                        selected = c(""),
                                        inline = TRUE,
                                        multiple = TRUE,
                                        width = 300,
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
                                        inputId = "characterizationDomainNameFilter",
                                        label = "Domain name",
                                        choices = c(""),
                                        selected = c(""),
                                        inline = TRUE,
                                        multiple = TRUE,
                                        width = 300,
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
                                      shiny::radioButtons(
                                        inputId = "charProportionOrContinuous",
                                        label = "",
                                        choices = c("All", "Proportion", "Continuous"),
                                        selected = "Proportion",
                                        inline = TRUE
                                      )
                                    )
                                  )))
        )
      ),
      tags$tr(
        tags$td(colspan = 2,
                shiny::conditionalPanel(
                  condition = "input.charType == 'Raw'",
                  shiny::radioButtons(
                    inputId = "characterizationColumnFilters",
                    label = "Display",
                    choices = c("Mean and Standard Deviation", "Mean only"),
                    selected = "Mean only",
                    inline = TRUE
                  )
                )
        )
      )),
    tags$table(width = "100%", 
               tags$tr(
                 tags$td(align = "right",
                         shiny::downloadButton(
                           outputId = "saveCohortCharacterizationTable",
                           label = "",
                           icon = shiny::icon("download"),
                           style = "margin-top: 5px; margin-bottom: 5px;"
                         )
                 )
               )
    ),
    DT::dataTableOutput(outputId = "characterizationTable")
  ),
  shinydashboard::tabItem(
    tabName = "temporalCharacterization",
    cohortReferenceWithDatabaseId("temporalCharacterizationSelectedCohort", "temporalCharacterizationSelectedDatabase"),
    tags$table(tags$tr(
      tags$td(
        shinyWidgets::pickerInput(
          inputId = "temporalAnalysisNameFilter",
          label = "Analysis name",
          choices = c(""),
          selected = c(""),
          multiple = TRUE,
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
          inputId = "temporalDomainNameFilter",
          label = "Domain name",
          choices = c(""),
          selected = c(""),
          multiple = TRUE,
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
        shiny::radioButtons(
          inputId = "temporalProportionOrContinuous",
          label = "",
          choices = c("All", "Proportion", "Continuous"),
          selected = "Proportion",
          inline = TRUE
        )
      )
    )),
    tags$table(width = "100%", 
               tags$tr(
                 tags$td(align = "right",
                         shiny::downloadButton(
                           outputId = "saveTemporalCharacterizationTable",
                           label = "",
                           icon = shiny::icon("download"),
                           style = "margin-top: 5px; margin-bottom: 5px;"
                         )
                 )
               )
    ),
    DT::dataTableOutput("temporalCharacterizationTable")
  ),
  shinydashboard::tabItem(
    tabName = "compareCohortCharacterization",
    cohortReferenceWithDatabaseId("cohortCharCompareSelectedCohort", "cohortCharCompareSelectedDatabase"),
    tags$table(
      tags$tr(
        tags$td(
          shiny::radioButtons(
            inputId = "charCompareType",
            label = "",
            choices = c("Pretty table", "Raw table", "Plot"),
            selected = "Plot",
            inline = TRUE
          ),
        ),
        tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
        tags$td(
          shiny::conditionalPanel(
            condition = "input.charCompareType == 'Raw table'",
            shiny::radioButtons(
              inputId = "compareCharacterizationColumnFilters",
              label = "Display",
              choices = c("Mean and Standard Deviation", "Mean only"),
              selected = "Mean only",
              inline = TRUE
            )
          )
        )
      )
    ),
    
    shiny::conditionalPanel(condition = "input.charCompareType == 'Raw table' | input.charCompareType=='Plot'",
                            tags$table(tags$tr(
                              tags$td(
                                shinyWidgets::pickerInput(
                                  inputId = "charCompareAnalysisNameFilter",
                                  label = "Analysis name",
                                  choices = c(""),
                                  selected = c(""),
                                  multiple = TRUE,
                                  width = 200,
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
                                  inputId = "charaCompareDomainNameFilter",
                                  label = "Domain name",
                                  choices = c(""),
                                  selected = c(""),
                                  multiple = TRUE,
                                  width = 200,
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
                                shiny::radioButtons(
                                  inputId = "charCompareProportionOrContinuous",
                                  label = "",
                                  choices = c("All", "Proportion", "Continuous"),
                                  selected = "Proportion",
                                  inline = TRUE
                                )
                              )
                            ))),
    shiny::conditionalPanel(condition = "input.charCompareType=='Pretty table' | input.charCompareType=='Raw table'",
                            tags$table(width = "100%", 
                                       tags$tr(
                                         tags$td(align = "right",
                                                 shiny::downloadButton(
                                                   outputId = "saveCompareCohortCharacterizationTable",
                                                   label = "",
                                                   icon = shiny::icon("download"),
                                                   style = "margin-top: 5px; margin-bottom: 5px;"
                                                 )
                                         )
                                       )
                            ),
                            DT::dataTableOutput("charCompareTable")),
    shiny::conditionalPanel(
      condition = "input.charCompareType=='Plot'",
      shinydashboard::box(
        title = "Compare Cohort Characterization",
        width = NULL,
        status = "primary",
        shiny::htmlOutput("compareCohortCharacterizationSelectedCohort"),
        ggiraph::ggiraphOutput(
          outputId = "charComparePlot",
          width = "100%",
          height = "100%"
        )
      )
    )
  ),
  shinydashboard::tabItem(
    tabName = "compareTemporalCharacterization",
    cohortReferenceWithDatabaseId(cohortOutputId = "temporalCharCompareSelectedCohort", databaseOutputId = "temporalCharCompareSelectedDatabase"),
    tags$table(
      tags$tr(
        tags$td(
          shiny::radioButtons(
            inputId = "temporalCharacterizationType",
            label = "",
            choices = c("Raw table", "Plot"),
            #"Pretty table", removed pretty option for compare temporal characterization
            # Pretty table can be put back in - we will need a different Table1Specs for temporal characterization
            selected = "Plot",
            inline = TRUE
          )
        ),
        tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
        tags$td(
          shiny::conditionalPanel(
            condition = "input.temporalCharacterizationType == 'Raw table'",
            shiny::radioButtons(
              inputId = "temporalCharacterizationTypeColumnFilter",
              label = "Show  in table:",
              choices = c("Mean and Standard Deviation", "Mean only"),
              selected = "Mean only",
              inline = TRUE
            )
          )
        )
      )
    ),
    shiny::conditionalPanel(condition = "input.temporalCharacterizationType == 'Raw table' | input.temporalCharacterizationType=='Plot'",
                            tags$table(tags$tr(
                              tags$td(
                                shinyWidgets::pickerInput(
                                  inputId = "temporalCompareAnalysisNameFilter",
                                  label = "Analysis name",
                                  choices = c(""),
                                  selected = c(""),
                                  multiple = TRUE,
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
                                  inputId = "temporalCompareDomainNameFilter",
                                  label = "Domain name",
                                  choices = c(""),
                                  selected = c(""),
                                  multiple = TRUE,
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
                                shiny::radioButtons(
                                  inputId = "temporalCharacterProportionOrContinuous",
                                  label = "Filter to:",
                                  choices = c("All", "Proportion", "Continuous"),
                                  selected = "Proportion",
                                  inline = TRUE
                                )
                              )
                            ))),
    shiny::conditionalPanel(
      condition = "input.temporalCharacterizationType=='Pretty table' |
                            input.temporalCharacterizationType=='Raw table'",
      tags$table(width = "100%", 
                 tags$tr(
                   tags$td(align = "right",
                           shiny::downloadButton(
                             outputId = "saveCompareTemporalCharacterizationTable",
                             label = "",
                             icon = shiny::icon("download"),
                             style = "margin-top: 5px; margin-bottom: 5px;"
                           )
                   )
                 )
      ),
      DT::dataTableOutput(outputId = "temporalCharacterizationCompareTable")
    ),
    shiny::conditionalPanel(
      condition = "input.temporalCharacterizationType=='Plot'",
      shinydashboard::box(
        title = "Compare Temporal Characterization",
        width = NULL,
        status = "primary",
        ggiraph::ggiraphOutput(
          outputId = "temporalCharComparePlot",
          width = "100%",
          height = "100%"
        )
      )
    )
  ),
  shinydashboard::tabItem(tabName = "databaseInformation",
                          DT::dataTableOutput("databaseInformationTable"))
)


#body
body <- shinydashboard::dashboardBody(bodyTabItems,
                                      htmltools::withTags(
                                        div(style = "margin-left : 0px",
                                            h6(appInformationText)
                                        )))


#main
shinydashboard::dashboardPage(
  tags$head(tags$style(HTML(
    "
      th, td {
        padding-right: 10px;
      }

    "
  ))),
  header = header,
  sidebar = sidebar,
  body = body
)
