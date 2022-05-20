ns <- shiny::NS("DiagnosticsExplorer")

appInformationText <- paste("Powered by OHDSI Cohort Diagnostics application", paste0(appVersionNum, "."))
appInformationText <- paste0(
  appInformationText,
  "Application was last initated on ",
  lubridate::now(tzone = "EST"),
  " EST. Cohort Diagnostics website is at https://ohdsi.github.io/CohortDiagnostics/"
)

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

choicesFordatabaseOrVocabularySchema <- database$databaseIdWithVocabularyVersion

if (enableAnnotation & showAnnotation) {
  headerContent <- tags$li(
    if (enableAuthorization) {
      shiny::conditionalPanel(
        "!output.postAnnoataionEnabled",
        ns = ns,
        shiny::actionButton(
          inputId = ns("annotationUserPopUp"),
          label = "Sign in"
        )
      )
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
      shinydashboard::menuItem(text = "Cohort Definition", tabName = "cohortDefinition")
    },
    if ("includedSourceConcept" %in% enabledTabs) {
      addInfo(
        item = shinydashboard::menuItem(text = "Concepts in Data Source", tabName = "conceptsInDataSource"),
        infoId = "conceptsInDataSourceInfo"
      )
    },
    if ("orphanConcept" %in% enabledTabs) {
      addInfo(
        item = shinydashboard::menuItem(text = "Orphan Concepts", tabName = "orphanConcepts"),
        infoId = "orphanConceptsInfo"
      )
    },
    if ("cohortCount" %in% enabledTabs) {
      addInfo(
        item = shinydashboard::menuItem(text = "Cohort Counts", tabName = "cohortCounts"),
        infoId = "cohortCountsInfo"
      )
    },
    if ("incidenceRate" %in% enabledTabs) {
      addInfo(
        item = shinydashboard::menuItem(text = "Incidence Rate", tabName = "incidenceRate"),
        infoId = "incidenceRateInfo"
      )
    },
    if ("temporalCovariateValue" %in% enabledTabs) {
      addInfo(
        item = shinydashboard::menuItem(text = "Time Distributions", tabName = "timeDistribution"),
        infoId = "timeDistributionInfo"
      )
    },
    if ("inclusionRuleStats" %in% enabledTabs) {
      addInfo(
        item = shinydashboard::menuItem(text = "Inclusion Rule Statistics", tabName = "inclusionRuleStats"),
        infoId = "inclusionRuleStatsInfo"
      )
    },
    if ("indexEventBreakdown" %in% enabledTabs) {
      addInfo(
        item = shinydashboard::menuItem(text = "Index Event Breakdown", tabName = "indexEventBreakdown"),
        infoId = "indexEventBreakdownInfo"
      )
    },
    if ("visitContext" %in% enabledTabs) {
      addInfo(
        item = shinydashboard::menuItem(text = "Visit Context", tabName = "visitContext"),
        infoId = "visitContextInfo"
      )
    },
    if ("relationship" %in% enabledTabs) {
      addInfo(
        shinydashboard::menuItem(text = "Cohort Overlap", tabName = "cohortOverlap"),
        infoId = "cohortOverlapInfo"
      )
    },
    if ("temporalCovariateValue" %in% enabledTabs) {
      addInfo(
        shinydashboard::menuItem(text = "Cohort Characterization", tabName = "cohortCharacterization"),
        infoId = "cohortCharacterizationInfo"
      )
    },
    if ("temporalCovariateValue" %in% enabledTabs) {
      addInfo(
        shinydashboard::menuItem(text = "Temporal Characterization", tabName = "temporalCharacterization"),
        infoId = "temporalCharacterizationInfo"
      )
    },
    if ("temporalCovariateValue" %in% enabledTabs) {
      addInfo(
        item = shinydashboard::menuItem(text = "Compare Cohort Char.", tabName = "compareCohortCharacterization"),
        infoId = "compareCohortCharacterizationInfo"
      )
    },
    if ("temporalCovariateValue" %in% enabledTabs) {
      addInfo(
        shinydashboard::menuItem(text = "Compare Temporal Char.", tabName = "compareTemporalCharacterization"),
        infoId = "compareTemporalCharacterizationInfo"
      )
    },
    shinydashboard::menuItem(text = "Meta data", tabName = "databaseInformation"),
    # Conditional dropdown boxes in the side bar ------------------------------------------------------
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
      input.tabs != 'cohortOverlap'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("database"),
        label = "Database",
        choices = database$databaseId %>% unique(),
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
      input.tabs == 'conceptsInDataSource' |
      input.tabs == 'orphanConcepts' |
      input.tabs == 'inclusionRuleStats' |
      input.tabs == 'visitContext' |
      input.tabs == 'cohortOverlap'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("databases"),
        label = "Database",
        choices = database$databaseId %>% unique(),
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
    if ("temporalCovariateValue" %in% enabledTabs) {
      shiny::conditionalPanel(
        condition = "input.tabs=='temporalCharacterization' | input.tabs =='compareTemporalCharacterization'",
        ns = ns,
        shinyWidgets::pickerInput(
          inputId = ns("timeIdChoices"),
          label = "Temporal Choice",
          choices = temporalCharacterizationTimeIdChoices$temporalChoices,
          multiple = TRUE,
          choicesOpt = list(style = rep_len("color: black;", 999)),
          selected = temporalCharacterizationTimeIdChoices %>%
            dplyr::filter(.data$primaryTimeId == 1) %>%
            dplyr::filter(.data$isTemporal == 1) %>%
            dplyr::arrange(.data$sequence) %>%
            dplyr::pull("temporalChoices"),
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
      condition = "input.tabs == 'compareCohortCharacterization'|
        input.tabs == 'compareTemporalCharacterization'",
      ns = ns,
      shinyWidgets::pickerInput(
        inputId = ns("comparatorCohort"),
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
    tabName = "inclusionRuleStats",
    inclusionRulesView(ns("inclusionRules")),
    column(
      12,
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationUi(ns("inclusionRuleStatsAnnotation"))
        )
      }
    )
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
    tabName = "temporalCharacterization",
    temporalCharacterizationView(ns("temporalCharacterization")),
    if (showAnnotation) {
      column(
        12,
        tags$br(),
        annotationUi(ns("temporalCharacterizationAnnotation"))
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
        annotationUi(ns("compareCohortCharacterizationAnnotation"))
      )
    }
  ),
  shinydashboard::tabItem(
    tabName = "compareTemporalCharacterization",
    compareCohortCharacterizationView(ns("compareTemporalCohortCharacterization")),
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
