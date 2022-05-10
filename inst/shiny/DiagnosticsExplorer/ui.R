
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
    status = "warning",
    width = "100%",
    tags$div(
      style = "max-height: 100px; overflow-y: auto",
      shiny::uiOutput(outputId = outputId)
    )
  )
}



cohortReferenceWithDatabaseId <- function(cohortOutputId, databaseOutputId) {
  shinydashboard::box(
    status = "warning",
    width = "100%",
    tags$div(
      style = "max-height: 100px; overflow-y: auto",
      tags$table(
        width = "100%",
        tags$tr(
          tags$td(
            width = "70%",
            tags$b("Cohorts :"),
            shiny::uiOutput(outputId = cohortOutputId)
          ),
          tags$td(
            style = "align: right !important;", width = "30%",
            tags$b("Database :"),
            shiny::uiOutput(outputId = databaseOutputId)
          )
        )
      )
    )
  )
}

choicesFordatabaseOrVocabularySchema <- database$databaseIdWithVocabularyVersion

if (enableAnnotation) {
  headerContent <- tags$li(
    shiny::conditionalPanel(
      "output.postAnnotationEnabled == false",
      shiny::actionButton(
        inputId = "annotationUserPopUp",
        label = "Sign in"
      )
    ),
    shiny::conditionalPanel(
      "output.postAnnotationEnabled == true",
      shiny::uiOutput(outputId = "userNameLabel", style = "color:white;font-weight:bold;padding-right:30px")
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
    id = "tabs",
    if (exists("cohort")) {
      shinydashboard::menuItem(text = "Cohort Definition", tabName = "cohortDefinition")
    },
    if (exists("includedSourceConcept")) {
      addInfo(
        item = shinydashboard::menuItem(text = "Concepts in Data Source", tabName = "conceptsInDataSource"),
        infoId = "conceptsInDataSourceInfo"
      )
    },
    if (exists("orphanConcept")) {
      addInfo(
        item = shinydashboard::menuItem(text = "Orphan Concepts", tabName = "orphanConcepts"),
        infoId = "orphanConceptsInfo"
      )
    },
    if (exists("cohortCount")) {
      addInfo(
        item = shinydashboard::menuItem(text = "Cohort Counts", tabName = "cohortCounts"),
        infoId = "cohortCountsInfo"
      )
    },
    if (exists("incidenceRate")) {
      addInfo(
        item = shinydashboard::menuItem(text = "Incidence Rate", tabName = "incidenceRate"),
        infoId = "incidenceRateInfo"
      )
    },
    if (exists("temporalCovariateValue")) {
      addInfo(
        item = shinydashboard::menuItem(text = "Time Distributions", tabName = "timeDistribution"),
        infoId = "timeDistributionInfo"
      )
    },
    if (exists("inclusionRuleStats")) {
      addInfo(
        item = shinydashboard::menuItem(text = "Inclusion Rule Statistics", tabName = "inclusionRuleStats"),
        infoId = "inclusionRuleStatsInfo"
      )
    },
    if (exists("indexEventBreakdown")) {
      addInfo(
        item = shinydashboard::menuItem(text = "Index Event Breakdown", tabName = "indexEventBreakdown"),
        infoId = "indexEventBreakdownInfo"
      )
    },
    if (exists("visitContext")) {
      addInfo(
        item = shinydashboard::menuItem(text = "Visit Context", tabName = "visitContext"),
        infoId = "visitContextInfo"
      )
    },
    if (exists("relationship")) {
      addInfo(
        shinydashboard::menuItem(text = "Cohort Overlap", tabName = "cohortOverlap"),
        infoId = "cohortOverlapInfo"
      )
    },
    if (exists("temporalCovariateValue")) {
      addInfo(
        shinydashboard::menuItem(text = "Cohort Characterization", tabName = "cohortCharacterization"),
        infoId = "cohortCharacterizationInfo"
      )
    },
    if (exists("temporalCovariateValue")) {
      addInfo(
        shinydashboard::menuItem(text = "Temporal Characterization", tabName = "temporalCharacterization"),
        infoId = "temporalCharacterizationInfo"
      )
    },
    if (exists("temporalCovariateValue")) {
      addInfo(
        item = shinydashboard::menuItem(text = "Compare Cohort Char.", tabName = "compareCohortCharacterization"),
        infoId = "compareCohortCharacterizationInfo"
      )
    },
    if (exists("temporalCovariateValue")) {
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
      input.tabs == 'conceptsInDataSource' |
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
      shinyWidgets::pickerInput(
        inputId = "targetCohort",
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
      input.tabs == 'conceptsInDataSource' |
      input.tabs == 'orphanConcepts'",
      shinyWidgets::pickerInput(
        inputId = "conceptSetsSelected",
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
    if (exists("aboutText")) {
      HTML(aboutText)
    }
  ),
  shinydashboard::tabItem(
    tabName = "cohortDefinition",
    cohortDefinitionsView("cohortDefinitions")
  ),
  shinydashboard::tabItem(
    tabName = "cohortCounts",
    cohortCountsView("cohortCounts"),
    if (showAnnotation) {
      column(
        12,
        tags$br(),
        annotationFunction("cohortCounts")
      )
    }
  ),
  shinydashboard::tabItem(
    tabName = "incidenceRate",
    incidenceRatesView("incidenceRates")
  ),
  shinydashboard::tabItem(
    tabName = "timeDistribution",
    timeDistributionsView("timeDistributions"),
    if (showAnnotation) {
      column(
        12,
        tags$br(),
        annotationFunction("timeDistribution")
      )
    }

  ),
  shinydashboard::tabItem(
    tabName = "conceptsInDataSource",
    conceptsInDataSourceView("conceptsInDataSource"),
    if (showAnnotation) {
      column(
        12,
        tags$br(),
        annotationFunction("conceptsInDataSource")
      )
    }
  ),
  shinydashboard::tabItem(
    tabName = "orphanConcepts",
    cohortReference("orphanConceptsSelectedCohort"),
    shinydashboard::box(
      title = NULL,
      width = NULL,
      htmltools::withTags(
        table(
          width = "100%",
          tr(
            td(
              shiny::radioButtons(
                inputId = "orphanConceptsType",
                label = "Filters",
                choices = c("All", "Standard Only", "Non Standard Only"),
                selected = "All",
                inline = TRUE
              )
            ),
            td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
            td(
              shiny::radioButtons(
                inputId = "orphanConceptsColumFilterType",
                label = "Display",
                choices = c("All", "Persons", "Records"),
                selected = "All",
                inline = TRUE
              )
            ),
            td()
          )
        )
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(outputId = "orphanConceptsTable")),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("orphanConcepts")
        )
      }
    )
  ),
  shinydashboard::tabItem(
    tabName = "inclusionRuleStats",
    inclusionRulesView("inclusionRules"),
    column(
      12,
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("inclusionRuleStats")
        )
      }
    )
  ),
  shinydashboard::tabItem(
    tabName = "indexEventBreakdown",
    indexEventBreakdownView("indexEvents"),
    if (showAnnotation) {
      column(
        12,
        tags$br(),
        annotationFunction("indexEventBreakdown")
      )
    }
  ),
  shinydashboard::tabItem(
    tabName = "visitContext",
    visitContextView("visitContext"),
    if (showAnnotation) {
      column(
        12,
        tags$br(),
        annotationFunction("visitContext")
      )
    }
  ),
  shinydashboard::tabItem(
    tabName = "cohortOverlap",
    cohortOverlapView("cohortOverlap"),
    if (showAnnotation) {
      column(
        12,
        tags$br(),
        annotationFunction("cohortOverlap")
      )
    }

  ),
  shinydashboard::tabItem(
    tabName = "cohortCharacterization",
    characterizationView("characterization"),
    if (showAnnotation) {
      column(
        12,
        tags$br(),
        annotationFunction("cohortCharacterization")
      )
    }
  ),
  shinydashboard::tabItem(
    tabName = "temporalCharacterization",
    temporalCharacterizationView("temporalCharacterization"),
    if (showAnnotation) {
      column(
        12,
        tags$br(),
        annotationFunction("temporalCharacterization")
      )
    }
  ),
  shinydashboard::tabItem(
    tabName = "compareCohortCharacterization",
    compareCohortCharacterizationView("compareCohortCharacterization"),
    if (showAnnotation) {
      column(
        12,
        tags$br(),
        annotationFunction("compareCohortCharacterization")
      )
    }
  ),
  shinydashboard::tabItem(
    tabName = "compareTemporalCharacterization",
    compareCohortCharacterizationView("compareTemporalCohortCharacterization"),
    if (showAnnotation) {
      column(
        12,
        tags$br(),
        annotationFunction("compareTemporalCharacterization")
      )
    }
  ),
  shinydashboard::tabItem(
    tabName = "databaseInformation",
    databaseInformationView("databaseInformation"),
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
