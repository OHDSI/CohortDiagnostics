library(magrittr)

source("R/Plots.R")
source("R/Tables.R")
source("R/Other.R")

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

if (!exists("cohortDescription")) {
  appTitle <- cohortDiagnosticModeDefaultTitle
}else{
  appTitle <- phenotypeLibraryModeDefaultTitle
}

#header name
header <-
  shinydashboard::dashboardHeader(title = appTitle, titleWidth = NULL)

#sidebarMenu
sidebarMenu <-
  shinydashboard::sidebarMenu(
    id = "tabs",
    if (exists("cohortDescription") && exists("phenotypeDescription"))
      addInfo(
         shinydashboard::menuItem(text = "Description", tabName = "description"),
         infoId = "descriptionInfo"
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
      condition = "input.tabs!='incidenceRate' & input.tabs!='timeDistribution' & input.tabs!='cohortCharacterization' & input.tabs!='cohortCounts' & input.tabs!='indexEventBreakdown' & input.tabs!='databaseInformation' & input.tabs != 'description' & input.tabs != 'includedConcepts' & input.tabs != 'orphanConcepts'",
      shinyWidgets::pickerInput(
        inputId = "database",
        label = "Database",
        choices = database$databaseId,
        selected = database$databaseId[1],
        multiple = FALSE,
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE, 
          liveSearch = TRUE,
          size = 10,
          liveSearchStyle = 'contains',
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50
          )
        
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs=='incidenceRate' | input.tabs=='timeDistribution' | input.tabs=='cohortCharacterization' | input.tabs=='cohortCounts' | input.tabs=='indexEventBreakdown' | input.tabs == 'includedConcepts' | input.tabs == 'orphanConcepts'",
      shinyWidgets::pickerInput(
        inputId = "databases",
        label = "Database",
        choices = database$databaseId,
        selected = database$databaseId[1],
        multiple = TRUE,
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE, 
          liveSearch = TRUE, 
          size = 10,
          liveSearchStyle = 'contains',
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50)
      )
    ),
    if (exists("temporalCovariate")) {
      shiny::conditionalPanel(
        condition = "input.tabs=='temporalCharacterization'",
        shinyWidgets::pickerInput(
          inputId = "timeIdChoices",
          label = "Temporal Choice",
          choices = temporalCovariateChoices$choices,
          multiple = TRUE,
          selected = temporalCovariateChoices %>% 
            dplyr::filter(.data$timeId == min(temporalCovariateChoices$timeId)) %>% 
            dplyr::pull('choices'),
          options = shinyWidgets::pickerOptions(
            actionsBox = TRUE,
            liveSearch = TRUE,
            size = 10,
            liveSearchStyle = 'contains',
            liveSearchPlaceholder = "Type here to search",
            virtualScroll = 50)
        )
      )
    },
    shiny::conditionalPanel(
      condition = "input.tabs!='cohortCounts' & input.tabs!='databaseInformation' & input.tabs != 'description'",
      shinyWidgets::pickerInput(
        inputId = "cohort",
        label = "Cohort (Target)",
        choices = cohort$cohortFullName,
        multiple = FALSE,
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE, 
          liveSearch = TRUE, 
          liveSearchStyle = 'contains',
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
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          liveSearch = TRUE,
          size = 10,
          liveSearchStyle = 'contains',
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50)
      )
    ),
    shiny::conditionalPanel(
      condition = "input.tabs=='cohortOverlap' | input.tabs=='compareCohortCharacterization'",
      shinyWidgets::pickerInput(
        inputId = "comparator",
        label = "Comparator",
        choices = cohort$cohortFullName,
        selected = cohort$cohortFullName[min(2, nrow(cohort))],
        multiple = FALSE,
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE, 
          liveSearch = TRUE, 
          size = 10,
          liveSearchStyle = 'contains',
          liveSearchPlaceholder = "Type here to search",
          virtualScroll = 50)
        
      )
    )
  )

#Side bar code
sidebar <-
  shinydashboard::dashboardSidebar(sidebarMenu, width = NULL, collapsed = TRUE)

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
                           DT::dataTableOutput(outputId = "phenoTypeDescriptionTable"),
                           tags$table(
                             tags$tr(
                               tags$td(
                                 div("Base url:")
                               ),
                               tags$td(HTML("&nbsp&nbsp")),
                               tags$td(
                                 shiny::textInput(inputId = "conceptIdBaseUrl",label = "", width = "300px", value = conceptBaseUrl)
                               )
                             )
                           )),
                         shiny::tabPanel(
                           tags$br(),
                           title = "Cohort", 
                           DT::dataTableOutput(outputId = "cohortDescriptionTable"),
                           tags$table(
                             tags$tr(
                               tags$td(
                                 div("Base url:")
                               ),
                               tags$td(HTML("&nbsp&nbsp")),
                               tags$td(
                                 shiny::textInput(inputId = "cohortBaseUrl",label = "", width = "300px", value = cohortBaseUrl)
                               )
                             )
                           ))
      )
    )
  ),
  shinydashboard::tabItem(tabName = "cohortCounts",
                          DT::dataTableOutput("cohortCountsTable"),
                          tags$table(
                            tags$tr(
                              tags$td(
                                div("Base url:")
                              ),
                              tags$td(HTML("&nbsp&nbsp")),
                              tags$td(
                                shiny::textInput(inputId = "cohortBaseUrl2",label = "", value = cohortBaseUrl)
                              )
                            )
                          )),
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
                 ),
                 tags$tr(
                   tags$td(
                     valign = "bottom",
                     shiny::htmlOutput(outputId = "incidentRateSelectedCohort") 
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
      ),
      shiny::downloadButton(outputId = "downloadIncidentRatePlot", label = "Download")
    )
  ),
  shinydashboard::tabItem(
    tabName = "timeDistribution",
    shinydashboard::box(
      title = "Time Distributions",
      width = NULL,
      status = "primary",
      shiny::htmlOutput(outputId = "timeDistributionSelectedCohort"),
      tags$br(),
      shiny::plotOutput("timeDisPlot"),
      shiny::downloadButton(outputId = "timeDistributionPlot", label = "Download")
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
    shiny::htmlOutput(outputId = "includeConceptsSelectedCohort"),
    tags$br(),
    DT::dataTableOutput("includedConceptsTable")
  ),
  shinydashboard::tabItem(tabName = "orphanConcepts",
                          shiny::htmlOutput(outputId = "orphanConceptSelectedCohort"),
                          tags$br(),
                          DT::dataTableOutput("orphanConceptsTable")),
  shinydashboard::tabItem(tabName = "inclusionRuleStats",
                          div(style = "font-size:15px;font-weight: bold", "Target cohort:"),
                          shiny::htmlOutput(outputId = "inclusionRuleStatSelectedCohort"),
                          tags$br(),
                          DT::dataTableOutput("inclusionRuleTable")),
  shinydashboard::tabItem(tabName = "indexEventBreakdown",
                          shiny::htmlOutput(outputId = "indexEventBreakdownSelectedCohort"),
                          tags$br(),
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
    
    tags$br(),
    DT::dataTableOutput("temporalCharacterizationTable"),
    tags$br(),
    shiny::conditionalPanel(
      condition = "input.timeIdChoices.length == 2",
      shinydashboard::box(
        title = "Temporal characterization plot",
        width = NULL,
        status = "primary",
        shiny::htmlOutput(outputId = "temporalCharacterizationPlotHover"),
        shiny::plotOutput(
          outputId = "temporalCharacterizationPlot",
          height = 700,
          hover = shiny::hoverOpts(
            id = "temporalCharacterizationPlotHoverInfo",
            delay = 100,
            delayType = "debounce"
          )
        ),
        shiny::downloadButton(outputId = "downloadTemporalCharacterizationPlot", label = "Download")
      )
    )
  ),
  shinydashboard::tabItem(
    tabName = "cohortOverlap",
    htmlOutput(outputId = "cohortOverlapSelectedCohort"),
    shinydashboard::box(
      title = "Cohort Overlap (Subjects)",
      width = NULL,
      status = "primary",
      shiny::plotOutput("overlapPlot"),
      shiny::downloadButton(outputId = "downloadOverlapPlot", label = "Download")
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
        shiny::plotOutput(
          outputId = "charComparePlot",
          height = 700,
          hover = shiny::hoverOpts(
            id = "plotHoverCharCompare",
            delay = 100,
            delayType = "debounce"
          )
        ),
        shiny::downloadButton(outputId = "downloadCompareCohortPlot")
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
