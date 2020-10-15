library(magrittr)

source("R/DisplayFunctions.R")
source("R/Tables.R")
source("R/Plots.R")
source("R/Results.R")

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

if (!exists("phenotypeDescription")) {
  appTitle <- cohortDiagnosticModeDefaultTitle
} else {
  appTitle <- phenotypeLibraryModeDefaultTitle
}

#header name
header <-
  shinydashboard::dashboardHeader(title = appTitle, 
                                  tags$li(
                                    tags$div(
                                      tags$strong("Phenotype:"),
                                      style = "color: white; margin-top: 14px; margin-right: 10px;"
                                    ),
                                    class = "dropdown"
                                  ), 
                                  tags$li(
                                    tags$div(
                                      shinyWidgets::pickerInput(
                                        inputId = "phenotypes",
                                        choices = phenotypeDescription$phenotypeName,
                                        selected = phenotypeDescription$phenotypeName[1],
                                        multiple = FALSE,
                                        choicesOpt = list(style = rep_len("color: black;", 999)),
                                        options = shinyWidgets::pickerOptions(
                                          actionsBox = FALSE,
                                          liveSearch = TRUE,
                                          size = 20,
                                          liveSearchStyle = "contains",
                                          liveSearchPlaceholder = "Type here to search",
                                          virtualScroll = 50,
                                          dropdownAlignRight = TRUE
                                        )
                                      ),
                                      style = "margin-top: 8px; margin-right: 10px; margin-bottom: -8px;"
                                    ),
                                    class = "dropdown"
                                  )
  )
#sidebarMenu
sidebarMenu <-
  shinydashboard::sidebarMenu(
    id = "tabs",
    if (exists("aboutText"))
      shinydashboard::menuItem(text = "About", tabName = "about"),
    if (exists("phenotypeDescription"))
      shinydashboard::menuItem(text = "Phenotype Description", tabName = "phenotypeDescription"),
    if (exists("cohort"))
      shinydashboard::menuItem(text = "Cohort Description", tabName = "cohortDescription"),
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
    if (exists("includedSourceConcept"))
      addInfo(
        item = shinydashboard::menuItem(text = "Included (Source) Concepts", tabName = "includedConcepts"),
        infoId = "includedConceptsInfo"
      ),
    if (exists("orphanConcept"))
      addInfo(
        item = shinydashboard::menuItem(text = "Orphan (Source) Concepts", tabName = "orphanConcepts"),
        infoId = "orphanConceptsInfo"
      ),
    if (exists("recommenderSet"))
      addInfo(
        item = shinydashboard::menuItem(text = "Concept Set Diagnostics", tabName = "conceptSetDiagnostics"),
        infoId = "conceptSetDiagnosticsInfo"
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
    if (exists("cohortOverlap"))
      addInfo(
        shinydashboard::menuItem(text = "Cohort Overlap", tabName = "cohortOverlap"),
        infoId = "cohortOverlapInfo"
      ),
    if (exists("covariateValue"))
      addInfo(
        item = shinydashboard::menuItem(text = "Compare Cohort Char.", tabName = "compareCohortCharacterization"),
        infoId = "compareCohortCharacterizationInfo"
      ),
    shinydashboard::menuItem(text = "Database information", tabName = "databaseInformation"),
    # Conditional dropdown boxes in the side bar ------------------------------------------------------
    shiny::conditionalPanel(
      condition = "input.tabs!='incidenceRate' & 
      input.tabs != 'timeDistribution' & 
      input.tabs != 'cohortCharacterization' & 
      input.tabs != 'cohortCounts' & 
      input.tabs != 'indexEventBreakdown' & 
      input.tabs != 'databaseInformation' & 
      input.tabs != 'cohortDescription' &
      input.tabs != 'phenotypeDescription' & 
      input.tabs != 'includedConcepts' & 
      input.tabs != 'orphanConcepts' & 
      input.tabs != 'conceptSetDiagnostics' & 
      input.tabs != 'inclusionRuleStats' & 
      input.tabs != 'visitContext' & 
      input.tabs != 'cohortOverlap' & 
      input.tabs != 'compareCohortCharacterization' &
      input.tabs != 'temporalCharacterization'",
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
      input.tabs == 'cohortOverlap' |
      input.tabs == 'compareCohortCharacterization'|
      input.tabs == 'temporalCharacterization'",
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
          virtualScroll = 50)
      )
    ),
    if (exists("temporalCovariateValue")) {
      shiny::conditionalPanel(
        condition = "input.tabs=='temporalCharacterization'",
        shinyWidgets::pickerInput(
          inputId = "timeIdChoices",
          label = "Temporal Choice",
          choices = temporalCovariateChoices$choices,
          multiple = TRUE,
          choicesOpt = list(style = rep_len("color: black;", 999)),
          selected = temporalCovariateChoices %>% 
            dplyr::filter(.data$timeId %in% (c(min(temporalCovariateChoices$timeId),
                                               temporalCovariateChoices %>% 
                                                 dplyr::filter(timeId %in% c(1,2,3,4,5)) %>% 
                                                 dplyr::pull(.data$timeId)) %>% 
                                               unique() %>% 
                                               sort())) %>%
            dplyr::pull("choices"),
          options = shinyWidgets::pickerOptions(
            actionsBox = TRUE,
            liveSearch = TRUE,
            size = 10,
            liveSearchStyle = "contains",
            liveSearchPlaceholder = "Type here to search",
            virtualScroll = 50)
        )
      )
    },
    shiny::conditionalPanel(
      condition = "input.tabs != 'databaseInformation' & 
      input.tabs != 'cohortDescription' &
      input.tabs != 'cohortCounts' &
      input.tabs != 'phenotypeDescription' & 
      input.tabs != 'cohortOverlap'&
      input.tabs != 'compareCohortCharacterization' &
      input.tabs != 'incidenceRate' &
      input.tabs != 'timeDistribution' &
      input.tabs != 'indexEventBreakdown' &
      input.tabs != 'cohortCharacterization' &
      input.tabs != 'temporalCharacterization' &
      input.tabs != 'visitContext'",
      shinyWidgets::pickerInput(
        inputId = "cohort",
        label = "Cohorts",
        choices = c(""),
        multiple = FALSE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE, 
          liveSearch = TRUE, 
          liveSearchStyle = "contains",
          size = 10,
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50)
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs=='includedConcepts' | input.tabs=='orphanConcepts' | input.tabs == 'conceptSetDiagnostics'",
      shinyWidgets::pickerInput(
        inputId = "conceptSet",
        label = "Concept Set",
        choices = c(""),
        multiple = FALSE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          size = 10,
          dropupAuto = TRUE,
          liveSearchStyle = "contains",
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50)
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs == 'cohortCounts' |
      input.tabs == 'cohortOverlap' |
      input.tabs == 'compareCohortCharacterization' |
      input.tabs == 'incidenceRate' |
      input.tabs == 'timeDistribution' |
      input.tabs == 'indexEventBreakdown' |
      input.tabs == 'cohortCharacterization' |
      input.tabs == 'temporalCharacterization' |
      input.tabs == 'visitContext'",
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
          virtualScroll = 50)
      )
    )
  )

#Side bar code
sidebar <-
  shinydashboard::dashboardSidebar(sidebarMenu, width = NULL, collapsed = FALSE)

# Body - items in tabs --------------------------------------------------
bodyTabItems <- shinydashboard::tabItems(
  shinydashboard::tabItem(
    tabName = "about",
    if (exists("aboutText")) HTML(aboutText)
  ), 
  shinydashboard::tabItem(
    tabName = "phenotypeDescription",
    shinydashboard::box(
      title = "Phenotype Description",
      width = NULL,
      status = "primary",
      DT::dataTableOutput(outputId = "phenoTypeDescriptionTable"),
      shiny::conditionalPanel(
        condition = "output.phenotypeRowIsSelected == true",
        shiny::actionButton("selectPhenotypeButton", label = "Select this phenotype", style = "margin-top: 5px; margin-bottom: 5px;"),
        shiny::tabsetPanel(id = "phenotypeInfoTab",
                           type = "tab",
                           shiny::tabPanel(title = "Description",
                                           tags$br(),
                                           shiny::uiOutput(outputId = "phenotypeDescriptionText")),
                           shiny::tabPanel(title = "Literature Review",
                                           tags$br(),
                                           shiny::htmlOutput(outputId = "phenotypeLiteratureReviewText")),
                           shiny::tabPanel(title = "Evaluation",
                                           tags$br(),
                                           shiny::htmlOutput(outputId = "phenotypeEvaluationText")),
                           shiny::tabPanel(title = "Notes",
                                           tags$br(),
                                           shiny::htmlOutput(outputId = "phenotypeNotesText"))))
    )
  ),
  shinydashboard::tabItem(
    tabName = "cohortDescription",
    shinydashboard::box(
      title = "Cohort Description",
      width = NULL,
      status = "primary",
      DT::dataTableOutput(outputId = "cohortDescriptionTable"),
      conditionalPanel("output.cohortDescriptionRowIsSelected == true",
                       shiny::tabsetPanel(type = "tab",
                                          shiny::tabPanel(title = "Details",
                                                          shiny::htmlOutput("cohortDetailsText")),
                                          if (exists("cohortExtra")) {
                                            shiny::tabPanel(title = "Definition",
                                                            copyToClipboardButton("cohortDescriptionDefinition", style = "margin-top: 5px; margin-bottom: 5px;"),
                                                            shiny::htmlOutput("cohortDescriptionDefinition"))
                                          },
                                          shiny::tabPanel(title = "Concept Sets",
                                                          shiny::downloadButton("saveConceptSetButton", 
                                                                                label = "Save to CSV file", 
                                                                                icon = shiny::icon("download"),
                                                                                style = "margin-top: 5px; margin-bottom: 5px;"),
                                                          if (!is(dataSource, "environment")) {
                                                            shiny::radioButtons(
                                                              inputId = "conceptSetsType",
                                                              label = "",
                                                              choices = c("Concept Set Expression", "Included Standard Concepts", "Included Source Concepts"),
                                                              selected = "Concept Set Expression",
                                                              inline = TRUE
                                                            )
                                                          },
                                                          DT::dataTableOutput(outputId = "cohortDescriptionConceptSetsTable")
                                          ),
                                          shiny::tabPanel(title = "JSON",
                                                          copyToClipboardButton("cohortDescriptionJson", style = "margin-top: 5px; margin-bottom: 5px;"),
                                                          shiny::verbatimTextOutput("cohortDescriptionJson")),
                                          shiny::tabPanel(title = "SQL",
                                                          copyToClipboardButton("cohortDescriptionSql", style = "margin-top: 5px; margin-bottom: 5px;"),
                                                          shiny::verbatimTextOutput("cohortDescriptionSql"))
                       )
      )
    )
  ),
  shinydashboard::tabItem(tabName = "cohortCounts",
                          shiny::htmlOutput(outputId = "cohortCountsSelectedCohort"),
                          DT::dataTableOutput("cohortCountsTable"),
  ),
  shinydashboard::tabItem(
    tabName = "incidenceRate",
    shiny::uiOutput(outputId = "incidenceRateSelectedCohort"),
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
                     style = "text-align: right",
                     shiny::checkboxInput("irYscaleFixed", "Use same y-scale across databases")
                   )
                 )),
      shiny::htmlOutput(outputId = "hoverInfoIr"),
      ggiraph::ggiraphOutput( outputId = "incidenceRatePlot", width = "100%", height = "100%" )
    )
  ),
  shinydashboard::tabItem(
    tabName = "timeDistribution",
    shiny::uiOutput(outputId = "timeDistSelectedCohort"),
    shinydashboard::box(
      title = "Time Distributions",
      width = NULL,
      status = "primary",
      tags$br(),
      ggiraph::ggiraphOutput("timeDisPlot", width = "100%", height = "100%")
    ),
    shinydashboard::box(
      title = "Time Distributions Table",
      width = NULL,
      status = "primary",
      DT::dataTableOutput("timeDistTable")
    )
  ),
  shinydashboard::tabItem(
    tabName = "includedConcepts",
    shiny::radioButtons(
      inputId = "includedType",
      label = "",
      choices = c("Source Concepts", "Standard Concepts"),
      selected = "Source Concepts",
      inline = TRUE
    ),
    DT::dataTableOutput("includedConceptsTable")
  ),
  shinydashboard::tabItem(tabName = "orphanConcepts",
                          DT::dataTableOutput("orphanConceptsTable")),
  shinydashboard::tabItem(tabName = "conceptSetDiagnostics",
                          shiny::radioButtons(
                            inputId = "conceptSetDiagnosticsType",
                            label = "",
                            choices = c("Standard Concepts", "Source Concepts"),
                            selected = "Standard Concepts",
                            inline = TRUE
                          ),
                          DT::dataTableOutput("conceptSetDiagnosticsTable")),
  shinydashboard::tabItem(tabName = "inclusionRuleStats",
                          div(style = "font-size:15px;font-weight: bold", "Target cohort:"),
                          shiny::htmlOutput(outputId = "inclusionRuleStatSelectedCohort"),
                          tags$br(),
                          DT::dataTableOutput("inclusionRuleTable")),
  shinydashboard::tabItem(tabName = "indexEventBreakdown",
                          shiny::htmlOutput(outputId = "indexEventBreakdownSelectedCohort"),
                          DT::dataTableOutput("breakdownTable")),
  shinydashboard::tabItem(tabName = "visitContext",
                          shiny::htmlOutput(outputId = "visitContextSelectedCohort"),
                          DT::dataTableOutput("visitContextTable")),
  shinydashboard::tabItem(
    tabName = "cohortCharacterization",
    shiny::htmlOutput(outputId = "characterizationSelectedCohort"),
    shiny::radioButtons(
      inputId = "charType",
      label = "",
      choices = c("Pretty", "Raw"),
      selected = "Pretty",
      inline = TRUE
    ),
    DT::dataTableOutput("characterizationTable")
  ),
  shinydashboard::tabItem(
    tabName = "temporalCharacterization",
    htmlOutput(outputId = "temporalCharacterizationSelectedCohort"),
    shinydashboard::box(
      title = "Temporal Characterization Table",
      width = NULL,
      status = "primary",
      DT::dataTableOutput("temporalCharacterizationTable")
    ),
    ggiraph::ggiraphOutput("compareTemporalCharacterizationPlot",height = "100%")
    # ,
    # shinydashboard::box(
    #   title = "Temporal Characterization Plot",
    #   width = NULL,
    #   status = "primary",
    #   ggiraph::ggiraphOutput(
    #     outputId = "covariateTimeSeriesPlot")
    # )
  ),
  shinydashboard::tabItem(
    tabName = "cohortOverlap",
    shiny::uiOutput(outputId = "cohortOverlapSelectedCohort"),
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
      ggiraph::ggiraphOutput("overlapPlot",height = "100%")
    )
    # shinydashboard::box(
    #   title = "Cohort Overlap Statistics",
    #   width = NULL,
    #   status = "primary",
    #   DT::dataTableOutput("overlapTable")
    # )
  ),
  shinydashboard::tabItem(
    tabName = "compareCohortCharacterization",
    shiny::radioButtons(
      inputId = "charCompareType",
      label = "",
      choices = c("Pretty table", "Raw table", "Plot"),
      selected = "Pretty table",
      inline = TRUE
    ),
    shiny::conditionalPanel(condition = "input.charCompareType=='Pretty table' | input.charCompareType=='Raw table'",
                            DT::dataTableOutput("charCompareTable")),
    shiny::conditionalPanel(
      condition = "input.charCompareType=='Plot'",
      shinydashboard::box(
        title = "Compare Cohort Characterization",
        width = NULL,
        status = "primary",
        shiny::htmlOutput("compareCohortCharacterizationSelectedCohort"),
        shinyWidgets::pickerInput(
          inputId = "domainId",
          label = "Filter By Domain",
          choices = c("all","condition", "device", "drug", "measurement", "observation", "procedure", "other"),
          multiple = FALSE,
          choicesOpt = list(style = rep_len("color: black;", 999)),
          options = shinyWidgets::pickerOptions(
            actionsBox = TRUE, 
            liveSearch = TRUE, 
            size = 10,
            liveSearchStyle = 'contains',
            liveSearchPlaceholder = "Type here to search",
            virtualScroll = 50)
          
        ),
        ggiraph::ggiraphOutput(outputId = "charComparePlot", width = "100%", height = "100%")
      )
    )
  ),
  shinydashboard::tabItem(tabName = "databaseInformation",
                          # uiOutput("databaseInformationPanel")
                          DT::dataTableOutput("databaseInformationTable"))
)


#body
body <- shinydashboard::dashboardBody(bodyTabItems)


#main
shinydashboard::dashboardPage(tags$head(
  tags$style(HTML("
      th, td {
        padding-right: 10px;
      }

    "))),
  header = header,
  sidebar = sidebar,
  body = body)
