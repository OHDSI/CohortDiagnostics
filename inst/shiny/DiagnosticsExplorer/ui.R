
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
    if (exists("cohortOverlap")) {
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
    shinydashboard::box(
      width = NULL,
      status = "primary",
      htmltools::withTags(
        table(
          width = "100%",
          tr(
            td(
              align = "left",
              h4("Cohort Definition")
            ),
            td(
              align = "right",
              button("Download as CSV", onclick = "Reactable.downloadDataCSV('cohortDefinitionTable')"),
              shiny::downloadButton(
                outputId = "exportAllCohortDetails",
                label = "Export all cohort",
                icon = shiny::icon("file-export"),
                style = "margin-top: 5px; margin-bottom: 5px;"
              )
            )
          )
        )
      ),
      shiny::column(
        12,
        reactable::reactableOutput(outputId = "cohortDefinitionTable")
      ),
      shiny::column(
        12,
        conditionalPanel(
          "output.cohortDefinitionRowIsSelected == true",
          shiny::tabsetPanel(
            type = "tab",
            shiny::tabPanel(
              title = "Details",
              shiny::htmlOutput("cohortDetailsText")
            ),
            shiny::tabPanel(
              title = "Cohort Count",
              tags$br(),
              htmltools::withTags(table(
                width = "100%",
                tr(
                  td(
                    align = "right",
                    button("Download as CSV", onclick = "Reactable.downloadDataCSV('cohortDefinitionCohortCountTable')")
                  )
                )
              )),
              reactable::reactableOutput(outputId = "cohortDefinitionCohortCountTable")
            ),
            shiny::tabPanel(
              title = "Cohort definition",
              copyToClipboardButton(
                toCopyId = "cohortDefinitionText",
                style = "margin-top: 5px; margin-bottom: 5px;"
              ),
              shinycssloaders::withSpinner(shiny::htmlOutput("cohortDefinitionText"))
            ),
            shiny::tabPanel(
              title = "Concept Sets",
              reactable::reactableOutput(outputId = "conceptsetExpressionsInCohort"),
              shiny::conditionalPanel(
                condition = "output.cohortDefinitionConceptSetExpressionRowIsSelected == true",
                tags$table(
                  tags$tr(
                    tags$td(
                      shiny::radioButtons(
                        inputId = "conceptSetsType",
                        label = "",
                        choices = c(
                          "Concept Set Expression",
                          "Resolved",
                          "Orphan concepts",
                          "Json"
                        ),
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
                    tags$td(shiny::htmlOutput("subjectCountInCohortConceptSet")),
                    tags$td(shiny::htmlOutput("recordCountInCohortConceptSet")),
                    tags$td(
                      shiny::conditionalPanel(
                        condition = "input.conceptSetsType == 'Resolved' ||
                                                                input.conceptSetsType == 'Orphan concepts'",
                        shiny::checkboxInput(
                          inputId = "withRecordCount",
                          label = "With Record Count",
                          value = TRUE
                        )
                      )
                    )
                  )
                )
              ),
              shiny::conditionalPanel(
                condition = "output.cohortDefinitionConceptSetExpressionRowIsSelected == true &
                input.conceptSetsType != 'Resolved' &
                input.conceptSetsType != 'Json' &
                input.conceptSetsType != 'Orphan concepts'",
                htmltools::withTags(table(
                  width = "100%",
                  tr(
                    td(
                      align = "right",
                      button("Download as CSV", onclick = "Reactable.downloadDataCSV('cohortDefinitionConceptSetDetailsTable')")
                    )
                  )
                )),
                reactable::reactableOutput(outputId = "cohortDefinitionConceptSetDetailsTable")
              ),
              shiny::conditionalPanel(
                condition = "input.conceptSetsType == 'Resolved'",
                htmltools::withTags(table(
                  width = "100%",
                  tr(
                    td(
                      align = "right",
                      button("Download as CSV", onclick = "Reactable.downloadDataCSV('cohortDefinitionResolvedConceptsTable')")
                    )
                  )
                )),
                reactable::reactableOutput(outputId = "cohortDefinitionResolvedConceptsTable")
              ),
              shiny::conditionalPanel(
                condition = "output.cohortDefinitionResolvedRowIsSelected == true && input.conceptSetsType == 'Resolved'",
                htmltools::withTags(table(
                  width = "100%",
                  tr(
                    td(
                      align = "right",
                      button("Download as CSV", onclick = "Reactable.downloadDataCSV('cohortDefinitionResolvedTableSelectedConceptIdMappedConcepts')")
                    )
                  )
                )),
                shinydashboard::box(
                  title = "Mapped Concepts",
                  width = NULL,
                  reactable::reactableOutput(outputId = "cohortDefinitionResolvedTableSelectedConceptIdMappedConcepts")
                )
              ),
              shiny::conditionalPanel(
                condition = "input.conceptSetsType == 'Orphan concepts'",
                htmltools::withTags(table(
                  width = "100%",
                  tr(
                    td(
                      align = "right",
                      button("Download as CSV", onclick = "Reactable.downloadDataCSV('cohortDefinitionOrphanConceptTable')")
                    )
                  )
                )),
                reactable::reactableOutput(outputId = "cohortDefinitionOrphanConceptTable")
              ),
              shiny::conditionalPanel(
                condition = "input.conceptSetsType == 'Json'",
                copyToClipboardButton(
                  toCopyId = "cohortConceptsetExpressionJson",
                  style = "margin-top: 5px; margin-bottom: 5px;"
                ),
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
              tags$head(tags$style(
                "#cohortDefinitionJson { max-height:400px};"
              ))
            ),
            shiny::tabPanel(
              title = "SQL",
              copyToClipboardButton("cohortDefinitionSql", style = "margin-top: 5px; margin-bottom: 5px;"),
              shiny::verbatimTextOutput("cohortDefinitionSql"),
              tags$head(tags$style(
                "#cohortDefinitionSql { max-height:400px};"
              ))
            )
          )
        )
      ),
    )
  ),
  shinydashboard::tabItem(
    tabName = "cohortCounts",
    cohortReference("cohortCountsSelectedCohorts"),
    shinydashboard::box(
      width = NULL,
      title = NULL,
      htmltools::withTags(
        table(
          width = "100%",
          tr(
            td(
              shiny::radioButtons(
                inputId = "cohortCountsTableColumnFilter",
                label = "Display",
                choices = c("Both", "Persons", "Records"),
                selected = "Both",
                inline = TRUE
              )
            ),
            td(
              align = "right",
              button("Download as CSV", onclick = "Reactable.downloadDataCSV('cohortCountsTable')")
            )
          )
        )
      ),
      reactable::reactableOutput(outputId = "cohortCountsTable"),
      shiny::conditionalPanel(
        condition = "output.cohortCountRowIsSelected == true",
        tags$br(),
        reactable::reactableOutput("InclusionRuleStatForCohortSeletedTable", width = NULL)
      ),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("cohortCounts")
        )
      }
    )
  ),
  shinydashboard::tabItem(
    tabName = "incidenceRate",
    cohortReference("incidenceRateSelectedCohorts"),
    shinydashboard::box(
      title = "Incidence Rate",
      width = NULL,
      status = "primary",
      htmltools::withTags(
        table(
          style = "width: 100%",
          tr(
            td(
              valign = "bottom",
              shiny::checkboxGroupInput(
                inputId = "irStratification",
                label = "Stratify by",
                choices = c("Age", "Sex", "Calendar Year"),
                selected = c("Age", "Sex", "Calendar Year"),
                inline = TRUE
              )
            ),
            td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;")),
            td(
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
                  dragRange = TRUE, width = 400,
                  step = 1,
                  sep = "",
                )
              )
            ),
            td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;")),
            td(
              valign = "bottom",
              style = "text-align: right",
              shiny::checkboxInput("irYscaleFixed", "Use same y-scale across databases")
            )
          )
        )
      ),
      htmltools::withTags(
        table(
          width = "100%",
          tr(
            td(
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
            td(
              shiny::conditionalPanel(
                condition = "input.irStratification.indexOf('Sex') > -1",
                shinyWidgets::pickerInput(
                  inputId = "incidenceRateGenderFilter",
                  label = "Filter By Sex",
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
            td(
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
            td(
              shiny::numericInput(
                inputId = "minPersonYear",
                label = "Minimum person years",
                value = 1000,
                min = 0
              )
            ),
            td(
              shiny::numericInput(
                inputId = "minSubjetCount",
                label = "Minimum subject count",
                value = NULL
              )
            ),
            td(
              align = "right",
              shiny::downloadButton(
                "saveIncidenceRatePlot",
                label = "",
                icon = shiny::icon("download"),
                style = "margin-top: 5px; margin-bottom: 5px;"
              )
            )
          )
        )
      ),
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
    cohortReference("timeDistributionSelectedCohorts"),
    shinydashboard::box(
      title = "Time Distributions",
      width = NULL,
      status = "primary",
      shiny::radioButtons(
        inputId = "timeDistributionType",
        label = "",
        choices = c("Table", "Plot"),
        selected = "Plot",
        inline = TRUE
      ),
      shiny::conditionalPanel(
        condition = "input.timeDistributionType=='Table'",
        tags$table(
          width = "100%",
          tags$tr(tags$td(
            align = "right",
            tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('timeDistributionTable')")
          ))
        ),
        reactable::reactableOutput(outputId = "timeDistributionTable")
      ),
      shiny::conditionalPanel(
        condition = "input.timeDistributionType=='Plot'",
        tags$br(),
        ggiraph::ggiraphOutput("timeDistributionPlot", width = "100%", height = "100%")
      ),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("timeDistribution")
        )
      }
    )
  ),
  shinydashboard::tabItem(
    tabName = "conceptsInDataSource",
    cohortReference("conceptsInDataSourceSelectedCohort"),
    shinydashboard::box(
      title = "Concepts in Data Source",
      width = NULL,
      htmltools::withTags(
        table(
          width = "100%",
          tr(
            td(
              shiny::radioButtons(
                inputId = "includedType",
                label = "",
                choices = c("Source fields", "Standard fields"),
                selected = "Standard fields",
                inline = TRUE
              )
            ),
            td(
              shiny::radioButtons(
                inputId = "conceptsInDataSourceTableColumnFilter",
                label = "",
                choices = c("Both", "Persons", "Records"),
                #
                selected = "Persons",
                inline = TRUE
              )
            ),
            td(
              align = "right",
              button("Download as CSV", onclick = "Reactable.downloadDataCSV('conceptsInDataSourceTable')")
            )
          )
        )
      ),
      reactable::reactableOutput(outputId = "conceptsInDataSourceTable"),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("conceptsInDataSource")
        )
      }
    )
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
            td(
              button("Download as CSV", onclick = "Reactable.downloadDataCSV('orphanConceptsTable')")
            )
          )
        )
      ),
      reactable::reactableOutput(outputId = "orphanConceptsTable"),
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
    cohortReference("inclusionRuleStatSelectedCohort"),
    shinydashboard::box(
      title = NULL,
      width = NULL,
      htmltools::withTags(
        table(
          width = "100%",
          tr(
            td(
              align = "left",
              shiny::radioButtons(
                inputId = "inclusionRuleTableFilters",
                label = "Inclusion Rule Events",
                choices = c("All", "Meet", "Gain", "Remain", "Total"),
                selected = "All",
                inline = TRUE
              )
            ),
            td(
              align = "right",
              button("Download as CSV", onclick = "Reactable.downloadDataCSV('inclusionRuleTable')")
            )
          )
        )
      ),
      reactable::reactableOutput(outputId = "inclusionRuleTable"),
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
    )
  ),
  shinydashboard::tabItem(
    tabName = "indexEventBreakdown",
    cohortReference("indexEventBreakdownSelectedCohort"),
    shinydashboard::box(
      width = NULL,
      title = NULL,
      htmltools::withTags(
        table(
          width = "100%",
          tr(
            td(
              shiny::radioButtons(
                inputId = "indexEventBreakdownTableRadioButton",
                label = "",
                choices = c("All", "Standard concepts", "Non Standard Concepts"),
                selected = "All",
                inline = TRUE
              )
            ),
            td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
            td(
              shiny::radioButtons(
                inputId = "indexEventBreakdownTableFilter",
                label = "Display",
                choices = c("Both", "Records", "Persons"),
                selected = "Persons",
                inline = TRUE
              )
            ),
            td(
              shiny::checkboxInput(
                inputId = "indexEventBreakDownShowAsPercent",
                label = "Show as percent"
              )
            ),
            td(
              align = "right",
              tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('breakdownTable')")
            )
          )
        )
      ),
      reactable::reactableOutput(outputId = "breakdownTable"),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("indexEventBreakdown")
        )
      }
    )
  ),
  shinydashboard::tabItem(
    tabName = "visitContext",
    cohortReference("visitContextSelectedCohort"),
    shinydashboard::box(
      width = NULL,
      title = NULL,
      tags$table(
        width = "100%",
        tags$tr(
          tags$td(
            shiny::radioButtons(
              inputId = "visitContextTableFilters",
              label = "Display",
              choices = c("All", "Before", "During", "Simultaneous", "After"),
              selected = "All",
              inline = TRUE
            )
          ),
          tags$td(
            shiny::radioButtons(
              inputId = "visitContextPersonOrRecords",
              label = "Display",
              choices = c("Persons", "Records"),
              selected = "Persons",
              inline = TRUE
            )
          ),
          tags$td(
            align = "right",
            tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('visitContextTable')")
          )
        )
      ),
      reactable::reactableOutput(outputId = "visitContextTable"),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("visitContext")
        )
      }
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
      ggiraph::ggiraphOutput("overlapPlot", width = "100%", height = "100%"),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("cohortOverlap")
        )
      }
    )
  ),
  shinydashboard::tabItem(
    tabName = "cohortCharacterization",
    cohortReference("characterizationSelectedCohort"),
    shinydashboard::box(
      width = NULL,
      title = NULL,
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
            shiny::conditionalPanel(
              condition = "input.charType == 'Raw'",
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
                    inputId = "characterizationDomainIdFilter",
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
                    inputId = "characterizationProportionOrContinuous",
                    label = "",
                    choices = c("All", "Proportion", "Continuous"),
                    selected = "Proportion",
                    inline = TRUE
                  )
                )
              ))
            )
          )
        ),
        tags$tr(
          tags$td(
            colspan = 2,
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
        )
      ),
      tags$table(
        width = "100%",
        tags$tr(
          tags$td(
            align = "right",
            tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('characterizationTable')")
          )
        )
      ),
      shinycssloaders::withSpinner(
        reactable::reactableOutput(outputId = "characterizationTable")
      ),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("cohortCharacterization")
        )
      }
    )
  ),
  shinydashboard::tabItem(
    tabName = "temporalCharacterization",
    cohortReferenceWithDatabaseId("temporalCharacterizationSelectedCohort", "temporalCharacterizationSelectedDatabase"),
    shinydashboard::box(
      width = NULL,
      title = NULL,
      tags$table(tags$tr(
        tags$td(
          shinyWidgets::pickerInput(
            inputId = "temporalCharacterizationAnalysisNameFilter",
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
            inputId = "temporalcharacterizationDomainIdFilter",
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
            inputId = "temporalProportionOrContinuous",
            label = "",
            choices = c("All", "Proportion", "Continuous"),
            selected = "Proportion",
            inline = TRUE
          )
        )
      )),
      tags$table(
        width = "100%",
        tags$tr(
          tags$td(
            align = "right",
            tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('temporalCharacterizationTable')")
          )
        )
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput("temporalCharacterizationTable")),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("temporalCharacterization")
        )
      }
    )
  ),
  shinydashboard::tabItem(
    tabName = "compareCohortCharacterization",
    cohortReferenceWithDatabaseId("cohortCharCompareSelectedCohort", "cohortCharCompareSelectedDatabase"),
    shinydashboard::box(
      width = NULL,
      title = NULL,
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
      shiny::conditionalPanel(
        condition = "input.charCompareType == 'Raw table' | input.charCompareType=='Plot'",
        tags$table(tags$tr(
          tags$td(
            shinyWidgets::pickerInput(
              inputId = "compareCohortCharacterizationAnalysisNameFilter",
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
              inputId = "compareCohortcharacterizationDomainIdFilter",
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
              inputId = "compareCharacterizationProportionOrContinuous",
              label = "",
              choices = c("All", "Proportion", "Continuous"),
              selected = "Proportion",
              inline = TRUE
            )
          )
        ))
      ),
      shiny::conditionalPanel(
        condition = "input.charCompareType=='Pretty table' | input.charCompareType=='Raw table'",
        tags$table(
          width = "100%",
          tags$tr(
            tags$td(
              align = "right",
              tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('compareCohortCharacterizationTable')")
            )
          )
        ),
        shinycssloaders::withSpinner(
          reactable::reactableOutput("compareCohortCharacterizationTable")
        )
      ),
      shiny::conditionalPanel(
        condition = "input.charCompareType=='Plot'",
        shinydashboard::box(
          title = "Compare Cohort Characterization",
          width = NULL,
          status = "primary",
          shiny::htmlOutput("compareCohortCharacterizationSelectedCohort"),
          shinycssloaders::withSpinner(
            ggiraph::ggiraphOutput(
              outputId = "compareCohortCharacterizationBalancePlot",
              width = "100%",
              height = "100%"
            )
          )
        )
      ),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("compareCohortCharacterization")
        )
      }
    )
  ),
  shinydashboard::tabItem(
    tabName = "compareTemporalCharacterization",
    cohortReferenceWithDatabaseId(cohortOutputId = "temporalCharCompareSelectedCohort", databaseOutputId = "temporalCharCompareSelectedDatabase"),
    shinydashboard::box(
      width = NULL,
      title = NULL,
      tags$table(
        tags$tr(
          tags$td(
            shiny::radioButtons(
              inputId = "temporalCharacterizationType",
              label = "",
              choices = c("Raw table", "Plot"),
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
      shiny::conditionalPanel(
        condition = "input.temporalCharacterizationType == 'Raw table' | input.temporalCharacterizationType=='Plot'",
        tags$table(tags$tr(
          tags$td(
            shinyWidgets::pickerInput(
              inputId = "temporalCompareAnalysisNameFilter",
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
              inputId = "temporalCompareDomainNameFilter",
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
              inputId = "temporalCompareCharacterizationProportionOrContinuous",
              label = "Filter to:",
              choices = c("All", "Proportion", "Continuous"),
              selected = "Proportion",
              inline = TRUE
            )
          )
        ))
      ),
      shiny::conditionalPanel(
        condition = "input.temporalCharacterizationType=='Pretty table' |
                            input.temporalCharacterizationType=='Raw table'",
        tags$table(
          width = "100%",
          tags$tr(
            tags$td(
              align = "right",
              tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('temporalCharacterizationCompareTable')")
            )
          )
        ),
        shinycssloaders::withSpinner(
          reactable::reactableOutput(outputId = "temporalCharacterizationCompareTable")
        )
      ),
      shiny::conditionalPanel(
        condition = "input.temporalCharacterizationType=='Plot'",
        shinydashboard::box(
          title = "Compare Temporal Characterization",
          width = NULL,
          status = "primary",
          shinycssloaders::withSpinner(
            ggiraph::ggiraphOutput(
              outputId = "temporalCharacterizationComparePlot",
              width = "100%",
              height = "100%"
            )
          )
        )
      ),
      if (showAnnotation) {
        column(
          12,
          tags$br(),
          annotationFunction("compareTemporalCharacterization")
        )
      }
    )
  ),
  shinydashboard::tabItem(
    tabName = "databaseInformation",
    shinydashboard::box(
      width = NULL,
      title = NULL,
      shiny::tabsetPanel(
        id = "metadataInformationTabsetPanel",
        shiny::tabPanel(
          title = "Data source",
          value = "datasourceTabPanel",
          tags$br(),
          htmltools::withTags(table(
            width = "100%",
            tr(
              td(
                align = "right",
                button("Download as CSV", onclick = "Reactable.downloadDataCSV('databaseInformationTable')")
              )
            )
          )),
          tags$br(),
          reactable::reactableOutput(outputId = "databaseInformationTable")
        ),
        shiny::tabPanel(
          title = "Meta data information",
          value = "metaDataInformationTabPanel",
          tags$br(),
          shinydashboard::box(
            title = shiny::htmlOutput(outputId = "metadataInfoTitle"),
            collapsible = TRUE,
            width = NULL,
            collapsed = FALSE,
            shiny::htmlOutput(outputId = "metadataInfoDetailsText"),
            shinydashboard::box(
              title = NULL,
              collapsible = TRUE,
              width = NULL,
              collapsed = FALSE,
              tags$button("Download as CSV", onclick = "Reactable.downloadDataCSV('packageDependencySnapShotTable')"),
              reactable::reactableOutput(outputId = "packageDependencySnapShotTable")
            ),
            shinydashboard::box(
              title = NULL,
              collapsible = TRUE,
              width = NULL,
              collapsed = FALSE,
              shiny::verbatimTextOutput(outputId = "argumentsAtDiagnosticsInitiationJson"),
              tags$head(
                tags$style("#argumentsAtDiagnosticsInitiationJson { max-height:400px};")
              )
            )
          )
        )
      )
    )
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
