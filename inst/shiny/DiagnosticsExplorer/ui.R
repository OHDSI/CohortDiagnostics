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

cohortReference <- function(outputId) {
  shinydashboard::box(# title = "Reference",
                      status = "warning",
                      width = "100%",
                      shiny::uiOutput(outputId = outputId)
  )
}

if (exists("phenotypeDescription")) {
  header <- shinydashboard::dashboardHeader(title = phenotypeLibraryModeDefaultTitle, 
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
} else { 
  header <- shinydashboard::dashboardHeader(title = cohortDiagnosticModeDefaultTitle)
}
#sidebarMenu
sidebarMenu <-
  shinydashboard::sidebarMenu(
    id = "tabs",
    if (exists("aboutText"))
      shinydashboard::menuItem(text = "About", tabName = "about"),
    if (exists("phenotypeDescription"))
      shinydashboard::menuItem(text = "Phenotype Description", tabName = "phenotypeDescription"),
    if (exists("cohort"))
      shinydashboard::menuItem(text = "Cohort Definition", tabName = "cohortDefinition"),
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
      input.tabs != 'cohortDefinition' &
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
      input.tabs != 'cohortDefinition' &
      input.tabs != 'cohortCounts' &
      input.tabs != 'phenotypeDescription' & 
      input.tabs != 'cohortOverlap'&
      input.tabs != 'compareCohortCharacterization' &
      input.tabs != 'incidenceRate' &
      input.tabs != 'timeDistribution' &
      input.tabs != 'inclusionRuleStats' &
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
      input.tabs == 'inclusionRuleStats' |
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
    tabName = "cohortDefinition",
    shinydashboard::box(
      title = "Cohort Definition",
      width = NULL,
      status = "primary",
      DT::dataTableOutput(outputId = "cohortDefinitionTable"),
      conditionalPanel("output.cohortDefinitionRowIsSelected == true",
                       shiny::tabsetPanel(type = "tab",
                                          shiny::tabPanel(title = "Details",
                                                          shiny::htmlOutput("cohortDetailsText")),
                                          if (exists("cohortExtra")) {
                                            shiny::tabPanel(title = "Definition",
                                                            copyToClipboardButton("cohortDefinitionDetails", style = "margin-top: 5px; margin-bottom: 5px;"),
                                                            shiny::htmlOutput("cohortDefinitionDetails"))
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
                                                          DT::dataTableOutput(outputId = "cohortDefinitionConceptSetsTable")
                                          ),
                                          shiny::tabPanel(title = "JSON",
                                                          copyToClipboardButton("cohortDefinitionJson", style = "margin-top: 5px; margin-bottom: 5px;"),
                                                          shiny::verbatimTextOutput("cohortDefinitionJson")),
                                          shiny::tabPanel(title = "SQL",
                                                          copyToClipboardButton("cohortDefinitionSql", style = "margin-top: 5px; margin-bottom: 5px;"),
                                                          shiny::verbatimTextOutput("cohortDefinitionSql"))
                       )
      )
    )
  ),
  shinydashboard::tabItem(tabName = "cohortCounts",
                          cohortReference("cohortCountsSelectedCohort"),
                          DT::dataTableOutput("cohortCountsTable"),
  ),
  shinydashboard::tabItem(
    tabName = "incidenceRate",
    cohortReference("incidenceRateSelectedCohort"),
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
                 )
                 ),
      tags$table( 
        tags$tr(
          tags$td(
            shiny::conditionalPanel(condition = "input.irStratification.indexOf('Age') > -1",
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
                virtualScroll = 50)
            ))
          ),
          tags$td(
            shiny::conditionalPanel(condition = "input.irStratification.indexOf('Gender') > -1",
            shinyWidgets::pickerInput(
              inputId = "incidenceRateGenderFilter",
              label = "Filter By Gender",
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
                virtualScroll = 50)
            ))
          ),
          tags$td(style="width:30% !important",
            shiny::conditionalPanel(condition = "input.irStratification.indexOf('Calendar Year') > -1",
            shinyWidgets::pickerInput(
              inputId = "incidenceRateCalenderFilter",
              label = "Filter By Calender Year",
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
                virtualScroll = 50)
            ))
          )
        )
      ),
      shiny::htmlOutput(outputId = "hoverInfoIr"),
      ggiraph::ggiraphOutput( outputId = "incidenceRatePlot", width = "100%", height = "100%")
    )
  ),
  shinydashboard::tabItem(
    tabName = "timeDistribution",
    cohortReference("timeDistSelectedCohort"),
    shiny::radioButtons(
      inputId = "timeDistributionType",
      label = "",
      choices = c("Table", "Plot"),
      selected = "Table",
      inline = TRUE
    ),
    shiny::conditionalPanel(condition = "input.timeDistributionType=='Table'",
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
                          cohortReference("inclusionRuleStatSelectedCohort"),
                          DT::dataTableOutput("inclusionRuleTable")),
  shinydashboard::tabItem(tabName = "indexEventBreakdown",
                          cohortReference("indexEventBreakdownSelectedCohort"),
                          DT::dataTableOutput("breakdownTable")),
  shinydashboard::tabItem(tabName = "visitContext",
                          cohortReference("visitContextSelectedCohort"),
                          DT::dataTableOutput("visitContextTable")),
  shinydashboard::tabItem(
    tabName = "cohortCharacterization",
    cohortReference("characterizationSelectedCohort"),
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
    cohortReference("temporalCharacterizationSelectedCohort"),
    shiny::radioButtons(
      inputId = "tempCharType",
      label = "",
      choices = c("Table", "Plot"),
      selected = "Table",
      inline = TRUE
    ),
    shiny::conditionalPanel(condition = "input.tempCharType=='Table'",
                            DT::dataTableOutput("temporalCharacterizationTable")),
    shiny::conditionalPanel(
      condition = "input.tempCharType=='Plot'",
      
      
      tags$table(style = "width:100%",
                 tags$tr(
                   tags$td(
                     shinyWidgets::pickerInput(
                       inputId = "timeIdChoicesFilter",
                       label = "Filter By Temporal Choices",
                       choices = c("All", temporalCovariateChoices$choices),
                       multiple = FALSE,
                       choicesOpt = list(style = rep_len("color: black;", 999)),
                       options = shinyWidgets::pickerOptions(
                         actionsBox = TRUE,
                         liveSearch = TRUE,
                         size = 10,
                         liveSearchStyle = "contains",
                         liveSearchPlaceholder = "Type here to search",
                         virtualScroll = 50))
                   ),
                   tags$td(
                     shinyWidgets::pickerInput(
                       inputId = "temporalDomainId",
                       label = "Filter By Covariate Domain",
                       choices = c("all","condition", "device", "drug", "measurement", "observation", "procedure", "other"),
                       multiple = FALSE,
                       choicesOpt = list(style = rep_len("color: black;", 999)),
                       options = shinyWidgets::pickerOptions(
                         actionsBox = TRUE, 
                         liveSearch = TRUE, 
                         size = 10,
                         liveSearchStyle = 'contains',
                         liveSearchPlaceholder = "Type here to search",
                         virtualScroll = 50))
                   )
                 )
      ),
      shinydashboard::box(
        title = "Compare Temporal Characterization",
        width = NULL,
        status = "primary",
        ggiraph::ggiraphOutput("compareTemporalCharacterizationPlot", width = "100%", height = "100%")
      )
    )
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
    tabName = "compareCohortCharacterization",
    cohortReference("cohortCharCompareSelectedCohort"),
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
