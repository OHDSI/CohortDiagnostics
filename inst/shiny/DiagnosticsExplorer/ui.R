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
  shinydashboard::dashboardHeader(title = appTitle, titleWidth = NULL)

#sidebarMenu
sidebarMenu <-
  shinydashboard::sidebarMenu(
    id = "tabs",
    if (exists("phenotypeDescription") && exists("cohort"))
      shinydashboard::menuItem(text = "Description", tabName = "description"),
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
    shiny::conditionalPanel(
      condition = "input.tabs!='incidenceRate' & input.tabs!='timeDistribution' & input.tabs!='cohortCharacterization' & input.tabs!='cohortCounts' & input.tabs!='indexEventBreakdown' & input.tabs!='databaseInformation' & input.tabs != 'description' & input.tabs != 'includedConcepts' & input.tabs != 'orphanConcepts' & input.tabs != 'inclusionRuleStats' & input.tabs != 'visitContext' & input.tabs != 'cohortOverlap'",
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
      condition = "input.tabs=='incidenceRate' | input.tabs=='timeDistribution' | input.tabs=='cohortCharacterization' | input.tabs=='cohortCounts' | input.tabs=='indexEventBreakdown' | input.tabs == 'includedConcepts' | input.tabs == 'orphanConcepts' | input.tabs == 'inclusionRuleStats' | input.tabs == 'visitContext' | input.tabs == 'cohortOverlap'",
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
      condition = "input.tabs!='cohortCounts' & 
      input.tabs!='databaseInformation' & 
      input.tabs != 'description' & 
      input.tabs != 'cohortOverlap'",
      shinyWidgets::pickerInput(
        inputId = "cohort",
        label = "Cohort (Target)",
        choices = cohort$cohortName,
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
      condition = "input.tabs=='includedConcepts' | input.tabs=='orphanConcepts'",
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
          liveSearchStyle = "contains",
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50)
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs=='compareCohortCharacterization'",
      shinyWidgets::pickerInput(
        inputId = "comparator",
        label = "Comparator",
        choices = cohort$cohortName,
        selected = cohort$cohortName[min(2, nrow(cohort))],
        multiple = FALSE,
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
    shiny::conditionalPanel(
      condition = "input.tabs == 'cohortOverlap'" ,
      shinyWidgets::pickerInput(
        inputId = "cohorts",
        label = "Cohorts (Targets)",
        choices = cohort$cohortName,
        selected = cohort$cohortName[min(1, nrow(cohort))],
        multiple = TRUE,
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
      condition = "input.tabs == 'cohortOverlap'",
      shinyWidgets::pickerInput(
        inputId = "comparators",
        label = "Comparators",
        choices = cohort$cohortName,
        selected = cohort$cohortName[min(2, nrow(cohort))],
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
    )
  )

#Side bar code
sidebar <-
  shinydashboard::dashboardSidebar(sidebarMenu, width = NULL, collapsed = FALSE)

#body - items in tab
bodyTabItems <- shinydashboard::tabItems(
  shinydashboard::tabItem(
    tabName = "description",
    shinydashboard::box(
      title = "Description",
      width = NULL,
      status = "primary",
      shiny::tabsetPanel(type = "tab",
                         shiny::tabPanel(
                           tags$br(),
                           title = "Phenotype",
                           DT::dataTableOutput(outputId = "phenoTypeDescriptionTable")),
                         shiny::tabPanel(
                           tags$br(),
                           title = "Cohort", 
                           DT::dataTableOutput(outputId = "cohortDescriptionTable"))
      )
    )
  ),
  shinydashboard::tabItem(tabName = "cohortCounts",
                          DT::dataTableOutput("cohortCountsTable")),
  shinydashboard::tabItem(
    tabName = "incidenceRate",
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
  shinydashboard::tabItem(tabName = "inclusionRuleStats",
                          div(style = "font-size:15px;font-weight: bold", "Target cohort:"),
                          shiny::htmlOutput(outputId = "inclusionRuleStatSelectedCohort"),
                          tags$br(),
                          DT::dataTableOutput("inclusionRuleTable")),
  shinydashboard::tabItem(tabName = "indexEventBreakdown",
                          DT::dataTableOutput("breakdownTable")),
  shinydashboard::tabItem(tabName = "visitContext",
                          DT::dataTableOutput("visitContextTable")),
  shinydashboard::tabItem(
    tabName = "cohortCharacterization",
    shiny::radioButtons(
      inputId = "charType",
      label = "",
      choices = c("Pretty", "Raw"),
      selected = "Pretty",
      inline = TRUE
    ),
    div(style = "font-size:15px;font-weight: bold", "Target cohort:"),
    shiny::textOutput(outputId = "cohortCharacterizationSelectedCohort"),
    tags$br(),
    DT::dataTableOutput("characterizationTable")
  ),
  shinydashboard::tabItem(
    tabName = "temporalCharacterization",
    tags$table(style = "width: 100%",
               tags$tr(
                 tags$td(
                   htmlOutput(outputId = "temporalCharacterizationSelectedCohort")
                 ),
                 tags$td(
                   style = "text-align: right",
                   div("Selected database:"),
                   shiny::textOutput(outputId = "temporalCharacterizationSelectedDataBase")
                 ),
                 tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;"))
               )
    ),
    shinydashboard::box(
      title = "Temporal Characterization Table",
      width = NULL,
      status = "primary",
      DT::dataTableOutput("temporalCharacterizationTable")
    )
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
      ggiraph::ggiraphOutput("overlapPlot", width = "100%", height = 600)
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
    htmlOutput("compareCohortCharacterizationSelectedCohort"),
    shiny::conditionalPanel(condition = "input.charCompareType=='Pretty table' | input.charCompareType=='Raw table'",
                            DT::dataTableOutput("charCompareTable")),
    shiny::conditionalPanel(
      condition = "input.charCompareType=='Plot'",
      shinydashboard::box(
        title = "Compare Cohort Characterization",
        width = NULL,
        status = "primary",
        shiny::htmlOutput(outputId = "hoverInfoCharComparePlot"),
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
shinydashboard::dashboardPage(header = header,
                              sidebar = sidebar,
                              body = body)
