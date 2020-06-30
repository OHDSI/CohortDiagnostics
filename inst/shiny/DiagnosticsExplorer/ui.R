#library(shiny)
#library(shinydashboard)
#library(DT)

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

#header name
header <-
  shinydashboard::dashboardHeader(title = "Cohort Diagnostics", titleWidth = NULL)

#sidebarMenu
sidebarMenu <-
  shinydashboard::sidebarMenu(
    id = "tabs",
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
    if (exists("covariateValue"))
      addInfo(
        shinydashboard::menuItem(text = "Cohort Characterization", tabName = "cohortCharacterization"),
        infoId = "cohortCharacterizationInfo"
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
      condition = "input.tabs!='incidenceRate' & input.tabs!='timeDistribution' & input.tabs!='cohortCharacterization' & input.tabs!='cohortCounts' & input.tabs!='indexEventBreakdown' & input.tabs!='databaseInformation'",
      shiny::selectInput(
        inputId = "database",
        label = "Database",
        choices = database$databaseId,
        selectize = FALSE
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs=='incidenceRate' | input.tabs=='timeDistribution' | input.tabs=='cohortCharacterization' | input.tabs=='cohortCounts' | input.tabs=='indexEventBreakdown'",
      shiny::checkboxGroupInput(
        inputId = "databases",
        label = "Database",
        choices = database$databaseId,
        selected = database$databaseId[1]
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs!='cohortCounts' & input.tabs!='databaseInformation'",
      shiny::selectInput(
        inputId = "cohort",
        label = "Cohort (Target)",
        choices = cohort$cohortFullName,
        selectize = FALSE
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs=='includedConcepts' | input.tabs=='orphanConcepts'",
      shiny::selectInput(
        inputId = "conceptSet",
        label = "Concept Set",
        choices = c(""),
        selectize = FALSE
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs=='cohortOverlap' | input.tabs=='compareCohortCharacterization'",
      shiny::selectInput(
        inputId = "comparator",
        label = "Comparator",
        choices = cohort$cohortFullName,
        selectize = FALSE,
        selected = cohort$cohortFullName[min(2, nrow(cohort))]
      )
    )
  )


#Side bar code
sidebar <-
  shinydashboard::dashboardSidebar(sidebarMenu, width = NULL, collapsed = TRUE)


#body - items in tab
bodyTabItems <- shinydashboard::tabItems(
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
      shiny::plotOutput(
        outputId = "incidenceRatePlot",
        height = 700,
        hover = shiny::hoverOpts(
          id = "plotHoverIr",
          delay = 100,
          delayType = "debounce"
        )
      )
    )
  ),
  shinydashboard::tabItem(
    tabName = "timeDistribution",
    shinydashboard::box(
      title = "Time Distributions",
      width = NULL,
      status = "primary",
      shiny::plotOutput("timeDisPlot")
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
                          DT::dataTableOutput("inclusionRuleTable")),
  shinydashboard::tabItem(tabName = "indexEventBreakdown",
                          DT::dataTableOutput("breakdownTable")),
  shinydashboard::tabItem(
    tabName = "cohortCharacterization",
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
    tabName = "cohortOverlap",
    shinydashboard::box(
      title = "Cohort Overlap (Subjects)",
      width = NULL,
      status = "primary",
      shiny::plotOutput("overlapPlot")
    ),
    shinydashboard::box(
      title = "Cohort Overlap Statistics",
      width = NULL,
      status = "primary",
      DT::dataTableOutput("overlapTable")
    )
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
        shiny::htmlOutput(outputId = "hoverInfoCharComparePlot"),
        shiny::plotOutput(
          outputId = "charComparePlot",
          height = 700,
          hover = shiny::hoverOpts(
            id = "plotHoverCharCompare",
            delay = 100,
            delayType = "debounce"
          )
        )
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
