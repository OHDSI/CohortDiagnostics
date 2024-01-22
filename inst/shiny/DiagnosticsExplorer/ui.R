cdUiControls <- function(ns) {
  panels <- shiny::tagList(
    shiny::conditionalPanel(
      condition = "input.tabs!='incidenceRate' &
      input.tabs != 'timeDistribution' &
      input.tabs != 'cohortCharacterization' &
      input.tabs != 'cohortCounts' &
      input.tabs != 'indexEventBreakdown' &
      input.tabs != 'cohortDefinition' &
      input.tabs != 'conceptsInDataSource' &
      input.tabs != 'orphanConcepts' &
      input.tabs != 'inclusionRuleStats' &
      input.tabs != 'visitContext' &
      input.tabs != 'compareCohortCharacterization' &
      input.tabs != 'cohortCharacterization' &
      input.tabs != 'cohortOverlap'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("database"),
        label = "Database",
        choices = NULL,
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
      input.tabs == 'cohortCounts' |
      input.tabs == 'indexEventBreakdown' |
      input.tabs == 'conceptsInDataSource' |
      input.tabs == 'orphanConcepts' |
      input.tabs == 'inclusionRuleStats' |
      input.tabs == 'visitContext' |
      input.tabs == 'cohortOverlap'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("databases"),
        label = "Database(s)",
        choices = NULL,
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
    shiny::conditionalPanel(
      condition = "input.tabs != 'databaseInformation' &
      input.tabs != 'cohortDefinition' &
      input.tabs != 'cohortCounts' &
      input.tabs != 'cohortOverlap'&
      input.tabs != 'incidenceRate' &
      input.tabs != 'compareCohortCharacterization' &
      input.tabs != 'cohortCharacterization' &
      input.tabs != 'timeDistribution'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("targetCohort"),
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
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("cohorts"),
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
      condition = "input.tabs == 'temporalCharacterization' |
      input.tabs == 'conceptsInDataSource' |
      input.tabs == 'orphanConcepts'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("conceptSetsSelected"),
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

  return(panels)
}

#' Cohort Diagnostics UI
#' @param id        Namespace id "DiagnosticsExplorer"
#' @param enabledReports   enabled reports
cohortDiagnosticsUi <- function(id = "DiagnosticsExplorer",
                                enabledReports) {
  ns <- shiny::NS(id)
  headerContent <- tags$li(
    class = "dropdown",
    style = "margin-top: 8px !important; margin-right : 5px !important"
  )

  header <-
    shinydashboard::dashboardHeader(title = "Cohort Diagnostics", headerContent)

  sidebarMenu <-
    shinydashboard::sidebarMenu(
      id = ns("tabs"),
      shinydashboard::menuItem(text = "Cohort Definition", tabName = "cohortDefinition", icon = shiny::icon("code")),
      shinydashboard::menuItem(text = "Concepts in Data Source", tabName = "conceptsInDataSource", icon = shiny::icon("table")),
      shinydashboard::menuItem(text = "Orphan Concepts", tabName = "orphanConcepts", icon = shiny::icon("notes-medical")),
      shinydashboard::menuItem(text = "Cohort Counts", tabName = "cohortCounts", icon = shiny::icon("bars")),
      shinydashboard::menuItem(text = "Incidence Rate", tabName = "incidenceRate", icon = shiny::icon("plus")),
      if ("temporalCovariateValue" %in% enabledReports) {
        shinydashboard::menuItem(text = "Time Distributions", tabName = "timeDistribution", icon = shiny::icon("clock"))
      },
      if ("indexEventBreakdown" %in% enabledReports) {
        shinydashboard::menuItem(text = "Index Event Breakdown", tabName = "indexEventBreakdown", icon = shiny::icon("hospital"))
      },
      if ("visitContext" %in% enabledReports) {
        shinydashboard::menuItem(text = "Visit Context", tabName = "visitContext", icon = shiny::icon("building"))
      },
      if ("relationship" %in% enabledReports) {
        shinydashboard::menuItem(text = "Cohort Overlap", tabName = "cohortOverlap", icon = shiny::icon("circle"))
      },
      if ("temporalCovariateValue" %in% enabledReports) {
        shinydashboard::menuItem(text = "Cohort Characterization", tabName = "cohortCharacterization", icon = shiny::icon("user"))
      },
      if ("temporalCovariateValue" %in% enabledReports) {
        shinydashboard::menuItem(text = "Compare Characterization", tabName = "compareCohortCharacterization", icon = shiny::icon("users"))
      },
      shinydashboard::menuItem(text = "Meta data", tabName = "databaseInformation", icon = shiny::icon("gear", verify_fa = FALSE)),
      # Conditional dropdown boxes in the side bar ------------------------------------------------------
      cdUiControls(ns)
    )

  # Side bar code
  sidebar <-
    shinydashboard::dashboardSidebar(sidebarMenu,
                                     width = NULL,
                                     collapsed = FALSE
    )

  # Body - items in tabs --------------------------------------------------
  bodyTabItems <- shinydashboard::tabItems(
    shinydashboard::tabItem(
      tabName = "about",
      if ("aboutText" %in% enabledReports) {
        HTML(aboutText)
      }
    ),
    shinydashboard::tabItem(
      tabName = "cohortDefinition",
      OhdsiShinyModules::cohortDefinitionsView(ns("cohortDefinitions"))
    ),
    shinydashboard::tabItem(
      tabName = "cohortCounts",
      OhdsiShinyModules::cohortCountsView(ns("cohortCounts"))
    ),
    shinydashboard::tabItem(
      tabName = "incidenceRate",
      OhdsiShinyModules::incidenceRatesView(ns("incidenceRates"))
    ),
    shinydashboard::tabItem(
      tabName = "timeDistribution",
      OhdsiShinyModules::timeDistributionsView(ns("timeDistributions"))
    ),
    shinydashboard::tabItem(
      tabName = "conceptsInDataSource",
      OhdsiShinyModules::conceptsInDataSourceView(ns("conceptsInDataSource"))
    ),
    shinydashboard::tabItem(
      tabName = "orphanConcepts",
      OhdsiShinyModules::orpahanConceptsView(ns("orphanConcepts"))
    ),
    shinydashboard::tabItem(
      tabName = "indexEventBreakdown",
      OhdsiShinyModules::indexEventBreakdownView(ns("indexEvents"))
    ),
    shinydashboard::tabItem(
      tabName = "visitContext",
      OhdsiShinyModules::visitContextView(ns("visitContext"))
    ),
    shinydashboard::tabItem(
      tabName = "cohortOverlap",
      OhdsiShinyModules::cohortOverlapView(ns("cohortOverlap"))
    ),
    shinydashboard::tabItem(
      tabName = "cohortCharacterization",
      OhdsiShinyModules::cohortDiagCharacterizationView(ns("characterization"))
    ),
    shinydashboard::tabItem(
      tabName = "compareCohortCharacterization",
      OhdsiShinyModules::compareCohortCharacterizationView(ns("compareCohortCharacterization"))
    ),
    shinydashboard::tabItem(
      tabName = "databaseInformation",
      OhdsiShinyModules::databaseInformationView(ns("databaseInformation")),
    )
  )

  # body
  body <- shinydashboard::dashboardBody(
    bodyTabItems
  )

  # main
  ui <- shinydashboard::dashboardPage(
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

  return(ui)
}

#OhdsiShinyModules::cohortDiagnosticsExplorerUi(id = "DiagnosticsExplorer")
cohortDiagnosticsUi(id = "DiagnosticsExplorer", dataSource$enabledReports)
