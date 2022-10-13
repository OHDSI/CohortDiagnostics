getAppInfo <- function(appVersionNum) {
  appInformationText <- paste0(
    "Powered by OHDSI Cohort Diagnostics application", paste0(appVersionNum, "."),
    "Application was last initated on ",
    lubridate::now(tzone = "EST"),
    " EST. Cohort Diagnostics website is at https://ohdsi.github.io/CohortDiagnostics/"
  )
}

uiControls <- function(ns,
                       enabledTabs) {
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

dashboardUi <- function(enabledTabs,
                        enableAnnotation,
                        showAnnotation,
                        enableAuthorization,
                        appVersionNum,
                        id = "DiagnosticsExplorer") {

  ns <- shiny::NS(id)
  appInformationText <- getAppInfo(appVersionNum)

  if (enableAnnotation & showAnnotation) {
    headerContent <- tags$li(
      if (enableAuthorization) {
        shiny::uiOutput(outputId = ns("signInButton"))
      },
      shiny::conditionalPanel(
        "output.postAnnoataionEnabled == true",
        ns = ns,
        shiny::uiOutput(outputId = ns("userNameLabel"),
                        style = "color:white;font-weight:bold;padding-right:30px")
      ),
      class = "dropdown",
      style = "margin-top: 8px !important; margin-right : 5px !important"
    )
  } else {
    headerContent <- tags$li(
      class = "dropdown",
      style = "margin-top: 8px !important; margin-right : 5px !important"
    )
  }

  header <-
    shinydashboard::dashboardHeader(title = "Cohort Diagnostics", headerContent)

  sidebarMenu <-
    shinydashboard::sidebarMenu(
      id = ns("tabs"),
      if ("cohort" %in% enabledTabs) {
        shinydashboard::menuItem(text = "Cohort Definition", tabName = "cohortDefinition", icon = shiny::icon("code"))
      },
      if ("includedSourceConcept" %in% enabledTabs) {
        shinydashboard::menuItem(text = "Concepts in Data Source", tabName = "conceptsInDataSource", icon = shiny::icon("table"))
      },
      if ("orphanConcept" %in% enabledTabs) {
        shinydashboard::menuItem(text = "Orphan Concepts", tabName = "orphanConcepts", icon = shiny::icon("notes-medical"))
      },
      if ("cohortCount" %in% enabledTabs) {
        shinydashboard::menuItem(text = "Cohort Counts", tabName = "cohortCounts", icon = shiny::icon("bars"))
      },
      if ("incidenceRate" %in% enabledTabs) {
        shinydashboard::menuItem(text = "Incidence Rate", tabName = "incidenceRate", icon = shiny::icon("plus"))
      },
      if ("temporalCovariateValue" %in% enabledTabs) {
        shinydashboard::menuItem(text = "Time Distributions", tabName = "timeDistribution", icon = shiny::icon("clock"))
      },
      if ("indexEventBreakdown" %in% enabledTabs) {
        shinydashboard::menuItem(text = "Index Event Breakdown", tabName = "indexEventBreakdown", icon = shiny::icon("hospital"))
      },
      if ("visitContext" %in% enabledTabs) {
        shinydashboard::menuItem(text = "Visit Context", tabName = "visitContext", icon = shiny::icon("building"))
      },
      if ("relationship" %in% enabledTabs) {
        shinydashboard::menuItem(text = "Cohort Overlap", tabName = "cohortOverlap", icon = shiny::icon("circle"))
      },
      if ("temporalCovariateValue" %in% enabledTabs) {
        shinydashboard::menuItem(text = "Cohort Characterization", tabName = "cohortCharacterization", icon = shiny::icon("user"))
      },
      if ("temporalCovariateValue" %in% enabledTabs) {
        shinydashboard::menuItem(text = "Compare Characterization", tabName = "compareCohortCharacterization", icon = shiny::icon("users"))
      },
      shinydashboard::menuItem(text = "Meta data", tabName = "databaseInformation", icon = shiny::icon("gear", verify_fa = FALSE)),
      # Conditional dropdown boxes in the side bar ------------------------------------------------------
      uiControls(ns, enabledTabs)
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
      if ("aboutText" %in% enabledTabs) {
        HTML(aboutText)
      }
    ),
    shinydashboard::tabItem(
      tabName = "cohortDefinition",
      cohortDefinitionsView(ns("cohortDefinitions"))
    ),
    shinydashboard::tabItem(
      tabName = "cohortCounts",
      cohortCountsView(ns("cohortCounts")),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationUi(ns("cohortCountsAnnotation"))
        )
      }
    ),
    shinydashboard::tabItem(
      tabName = "incidenceRate",
      incidenceRatesView(ns("incidenceRates"))
    ),
    shinydashboard::tabItem(
      tabName = "timeDistribution",
      timeDistributionsView(ns("timeDistributions")),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationUi(ns("timeDistributionAnnotation"))
        )
      }

    ),
    shinydashboard::tabItem(
      tabName = "conceptsInDataSource",
      conceptsInDataSourceView(ns("conceptsInDataSource")),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationUi(ns("conceptsInDataSourceAnnotation"))
        )
      }
    ),
    shinydashboard::tabItem(
      tabName = "orphanConcepts",
      orpahanConceptsView(ns("orphanConcepts")),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationUi(ns("orphanConceptsAnnotation"))
        )
      }
    ),
    shinydashboard::tabItem(
      tabName = "indexEventBreakdown",
      indexEventBreakdownView(ns("indexEvents")),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationUi(ns("indexEventBreakdownAnnotation"))
        )
      }
    ),
    shinydashboard::tabItem(
      tabName = "visitContext",
      visitContextView(ns("visitContext")),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationUi(ns("visitContextAnnotation"))
        )
      }
    ),
    shinydashboard::tabItem(
      tabName = "cohortOverlap",
      cohortOverlapView(ns("cohortOverlap")),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationUi(ns("cohortOverlapAnnotation"))
        )
      }
    ),
    shinydashboard::tabItem(
      tabName = "cohortCharacterization",
      characterizationView(ns("characterization")),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationUi(ns("cohortCharacterization"))
        )
      }
    ),
    shinydashboard::tabItem(
      tabName = "compareCohortCharacterization",
      compareCohortCharacterizationView(ns("compareCohortCharacterization")),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationUi(ns("compareTemporalCharacterizationAnnotation"))
        )
      }
    ),
    shinydashboard::tabItem(
      tabName = "databaseInformation",
      databaseInformationView(ns("databaseInformation")),
    )
  )


  # body
  body <- shinydashboard::dashboardBody(
    bodyTabItems,
    htmltools::withTags(
      div(
        style = "margin-left : 0px",
        h6(appInformationText)
      )
    )
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

tabularUi <- function(enabledTabs,
                      id = "DiagnosticsExplorer") {
  ns <- shiny::NS(id)
  ui <-
      shiny::fluidPage(
        shinydashboard::box(uiControls(ns, enabledTabs), width = 12),
        shiny::tabsetPanel(
          # shiny::tabPanel("About", shiny::HTML(aboutText)),
          if ("cohort" %in% enabledTabs) {
            shiny::tabPanel("Cohort Definitions", cohortDefinitionsView(ns("cohortDefinitions")), value = "cohortDefinition")
          },
          if ("includedSourceConcept" %in% enabledTabs) {
            shiny::tabPanel("Concepts in Data Source", conceptsInDataSourceView(ns("conceptsInDataSource")), value = "conceptsInDataSource")
          },
          if ("orphanConcept" %in% enabledTabs) {
            shiny::tabPanel("Orphan Concepts", orpahanConceptsView(ns("orphanConcepts")), value = "orphanConcept")
          },
          if ("cohortCount" %in% enabledTabs) {
            shiny::tabPanel("Cohort counts", cohortCountsView(ns("cohortCounts")), value = "cohortCounts")
          },
          if ("incidenceRate" %in% enabledTabs) {
            shiny::tabPanel("Incidence Rates", incidenceRatesView(ns("incidenceRates")), value = "incidenceRate")
          },
          if ("indexEventBreakdown" %in% enabledTabs) {
            shiny::tabPanel("Index Events", indexEventBreakdownView(ns("indexEvents")), value = "indexEventBreakdown")
          },
          if ("visitContext" %in% enabledTabs) {
            shiny::tabPanel("Visit Context", visitContextView(ns("visitContext")), value = "visitContext")
          },
          if ("relationship" %in% enabledTabs) {
            shiny::tabPanel("Cohort Overlap", cohortOverlapView(ns("cohortOverlap")), value = "cohortOverlap")
          },
          if ("temporalCovariateValue" %in% enabledTabs) {
            shiny::tabPanel("Time Distributions", timeDistributionsView(ns("timeDistributions")), value = "timeDistribution")
          },
          if ("temporalCovariateValue" %in% enabledTabs) {
            shiny::tabPanel("Characterization", characterizationView(ns("characterization")), value = "characterization")
          },
          if ("temporalCovariateValue" %in% enabledTabs) {
            shiny::tabPanel("Compare Characterization", compareCohortCharacterizationView(ns("compareCohortCharacterization")),
                            value = "compareTemporalCharacterization")
          },
          shiny::tabPanel("Database Information", databaseInformationView(ns("databaseInformation")),
                          value = "databaseInformation"),
          type = "pills",
          id = ns("tabs")
        ),
        width = "100%"
      )
  return(ui)
}
