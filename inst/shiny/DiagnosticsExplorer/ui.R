header <-
  shinydashboard::dashboardHeader(title = "Cohort Diagnostics")

sidebarMenu <-
  shinydashboard::sidebarMenu(
    id = "tabs",
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
    if (exists("timeSeries"))
      addInfo(
        item = shinydashboard::menuItem(text = "Time Series", tabName = "timeSeries"),
        infoId = "timeSeriesInfo"
      ),
    if (exists("timeDistribution"))
      addInfo(
        item = shinydashboard::menuItem(text = "Time Distributions", tabName = "timeDistribution"),
        infoId = "timeDistributionInfo"
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
    if (exists("cohortRelationships"))
      addInfo(
        shinydashboard::menuItem(text = "Cohort Overlap", tabName = "cohortOverlap"),
        infoId = "cohortOverlapInfo"
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
    if (exists("covariateValue"))
      addInfo(
        item = shinydashboard::menuItem(text = "Compare Cohort Char.", tabName = "compareCohortCharacterization"),
        infoId = "compareCohortCharacterizationInfo"
      ),
    if (exists("temporalCovariateValue"))
      addInfo(
        shinydashboard::menuItem(text = "Compare Temporal Char.", tabName = "compareTemporalCharacterization"),
        infoId = "compareTemporalCharacterizationInfo"
      ),
    shinydashboard::menuItem(text = "Meta data", tabName = "databaseInformation"),
    # Conditional dropdown boxes in the side bar ------------------------------------------------------
    shiny::conditionalPanel(
      condition = "input.tabs !='incidenceRate' &
      input.tabs != 'timeDistribution' &
      input.tabs != 'timeSeries' &
      input.tabs != 'cohortCharacterization' &
      input.tabs != 'cohortCounts' &
      input.tabs != 'indexEventBreakdown' &
      input.tabs != 'cohortDefinition' &
      input.tabs != 'visitContext' &
      input.tabs != 'cohortOverlap' &&
      input.tabs != 'compareCohortCharacterization'",
      shinyWidgets::pickerInput(
        inputId = "selectedDatabaseId",
        label = "Datasource",
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
      input.tabs == 'timeSeries' |
      input.tabs =='cohortCharacterization' |
      input.tabs == 'cohortCounts' |
      input.tabs == 'indexEventBreakdown' |
      input.tabs == 'visitContext' |
      input.tabs == 'cohortOverlap' |
      input.targetConceptSetsType == 'Resolved' |
      input.targetConceptSetsType == 'Excluded' |
      input.targetConceptSetsType == 'Orphan concepts' |
      input.targetCohortDefinitionTabSetPanel == 'targetCohortdefinitionInclusionRuleTabPanel' |
      input.tabs == 'compareCohortCharacterization'",
      shinyWidgets::pickerInput(
        inputId = "selectedDatabaseIds",
        label = "Datasource",
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
          choices = temporalCovariateChoices$choices,
          multiple = TRUE,
          choicesOpt = list(style = rep_len("color: black;", 999)),
          selected = temporalCovariateChoices %>%
            dplyr::filter(stringr::str_detect(string = .data$choices,
                                              pattern = 'Start -365 to end -31|Start -30 to end -1|Start 0 to end 0|Start 1 to end 30|Start 31 to end 365')) %>% 
            dplyr::filter(.data$timeId %in% (
              c(
                min(temporalCovariateChoices$timeId),
                temporalCovariateChoices %>%
                  dplyr::pull(.data$timeId)
              ) %>%
                unique() %>%
                sort()
            )) %>%
            dplyr::pull(.data$choices),
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
      input.tabs != 'timeSeries' &
      input.tabs != 'cohortCounts' &
      input.tabs != 'cohortOverlap'&
      input.tabs != 'incidenceRate' &
      input.tabs != 'timeDistribution'",
      shinyWidgets::pickerInput(
        inputId = "selectedCompoundCohortName",
        label = "cohort",
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
      input.tabs == 'timeSeries' |
      input.tabs == 'cohortOverlap' |
      input.tabs == 'incidenceRate' |
      input.tabs == 'timeDistribution'",
      shinyWidgets::pickerInput(
        inputId = "selectedCompoundCohortNames",
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
      condition = "input.tabs == 'cohortOverlap'",
      shinyWidgets::pickerInput(
        inputId = "selectedComparatorCompoundCohortNames",
        label = "Comparators",
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
      condition = "input.tabs == 'compareCohortCharacterization' |
        input.tabs == 'compareTemporalCharacterization' |
        input.tabs == 'cohortDefinition'",
      shinyWidgets::pickerInput(
        inputId = "selectedComparatorCompoundCohortName",
        label = "Comparator",
        choices = c(""),
        multiple = TRUE,
        choicesOpt = list(style = rep_len("color: black;", 999)),
        options = shinyWidgets::pickerOptions(
          maxOptions = 1,
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
      input.tabs == 'indexEventBreakdown' |
      input.tabs == 'compareTemporalCharacterization'",
      shinyWidgets::pickerInput(
        inputId = "conceptSetsSelectedCohortLeft",
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

#Side bar code
sidebar <-
  shinydashboard::dashboardSidebar(sidebarMenu, 
                                   width = NULL, 
                                   collapsed = FALSE
  )

# Body - items in tabs --------------------------------------------------
bodyTabItems <- shinydashboard::tabItems(
  shinydashboard::tabItem(tabName = "about",
                          if (exists("aboutText"))
                            HTML(aboutText)),
  shinydashboard::tabItem(
    tabName = "cohortDefinition",
    shinydashboard::box(
      width = NULL,
      status = "primary",
      shinydashboard::box(
        title = "Cohort Definition",
        status = NULL,
        width = NULL,
        solidHeader = FALSE,
        collapsible = TRUE,
        collapsed = TRUE,
        # column(6,tags$h4("Cohort Definition")),
        column(12,
               tags$table(width = "100%",
                          tags$tr(
                            tags$td(
                              align = "right",
                              shiny::downloadButton(
                                outputId = "downloadAllCohortDetails",
                                label = NULL,
                                icon = shiny::icon("download"),
                                style = "margin-top: 5px; margin-bottom: 5px;"
                              )
                            )
                          ))),
        DT::dataTableOutput(outputId = "cohortDefinitionTable")
      ),
      shiny::uiOutput(outputId = "dynamicUIGenerationForCohortSelectedTarget"),
      shiny::uiOutput(outputId = "dynamicUIGenerationForCohortSelectedComparator"),
      tags$br(),
      shiny::column(12,
                    shiny::conditionalPanel(
                      condition = "output.cohortDefinitionSelectedRowCount >= 1 &
                       input.targetConceptSetsType != 'Concept Set Expression' &
                       input.targetConceptSetsType != 'Concept Set Json' &
                       input.targetCohortDefinitionTabSetPanel == 'targetCohortDefinitionConceptSetTabPanel'",
                      shiny::uiOutput(outputId = "dynamicUIForRelationshipAndComparisonTable")
                    )
      )
    )
  ),
  shinydashboard::tabItem(
    tabName = "cohortCounts",
    createShinyBoxFromOutputId("cohortCountsSelectedCohorts"),
    tags$table(width = "100%", 
               tags$tr(
                 tags$td(
                   shiny::radioButtons(
                     inputId = "cohortCountsTableColumnFilter",
                     label = "Display",
                     choices = c("Both", "Subjects Only", "Records Only"), 
                     selected = "Both",
                     inline = TRUE
                   )
                 ),
                 tags$td(align = "right",
                         shiny::downloadButton(
                           "saveCohortCountsTable",
                           label = "",
                           icon = shiny::icon("download"),
                           style = "margin-top: 5px; margin-bottom: 5px;"
                         )
                 )
               )
    ),
    DT::dataTableOutput("cohortCountsTable"),
    tags$br(),
    shinydashboard::box(
      title = "Notes",
      status = NULL,
      width = NULL,
      solidHeader = TRUE,
      collapsible = TRUE,
      collapsed = TRUE,
      shiny::uiOutput(outputId = "cohortCountsCategories")
    ),
    shiny::conditionalPanel(
      condition = "output.doesSelectedRowInCohortCountTableHaveCohortId == true",
      tags$br(),
      tags$h3("Inclusion Rules"),
      DT::dataTableOutput("inclusionRuleStatisticsForCohortSeletedTable")
    )
  ),
  shinydashboard::tabItem(
    tabName = "incidenceRate",
    createShinyBoxFromOutputId("incidenceRateSelectedCohorts"),
    shinydashboard::box(
      title = "Incidence Rate",
      width = NULL,
      status = "primary",
      tags$table(
        style = "width: 100%",
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
            style = "width:30% !important;margin-top:10px;",
            shiny::conditionalPanel(
              condition = "input.irYscaleFixed",
              shiny::sliderInput(
                inputId = "YscaleMinAndMax",
                label = "Limit y-scale range to:",
                min = c(0),
                max = c(0),
                value = c(0, 0),
                dragRange = TRUE,
                width = 400,
                step = 1,
                sep = "",
              )
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
          style = "width: 100%",
          tags$td(
            shiny::conditionalPanel(
              condition = "input.irStratification.indexOf('Age') > -1",
              shinyWidgets::pickerInput(
                inputId = "incidenceRateAgeFilter",
                label = "Filter By Age",
                width = 300,
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
          tags$td(
            shiny::conditionalPanel(
              condition = "input.irStratification.indexOf('Gender') > -1",
              shinyWidgets::pickerInput(
                inputId = "incidenceRateGenderFilter",
                label = "Filter By Gender",
                width = 300,
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
          tags$td(
            style = "width: 30%",
            shiny::conditionalPanel(
              condition = "input.irStratification.indexOf('Calendar Year') > -1",
              shiny::sliderInput(
                inputId = "incidenceRateCalendarFilter",
                label = "Filter By Calendar Year",
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
          tags$td(
            shiny::numericInput(
              inputId = "minPersonYear",
              label = "Minimum person years",
              value = 1000,
              min = 0
            )
          ),
          tags$td(
            shiny::numericInput(
              inputId = "minSubjetCount",
              label = "Minimum subject count",
              value = NULL
            )
          ),
          tags$td(tags$table(width = "100%",
                             tags$tr(
                               tags$td(
                                 align = "right",
                                 shiny::downloadButton(
                                   "saveIncidenceRateData",
                                   label = "",
                                   icon = shiny::icon("download"),
                                   style = "margin-top: 5px; margin-bottom: 5px;"
                                 )
                               )
                             )),)
        ),
        tags$tr(
          tags$td(colspan = 6, align = 'right',
            shiny::actionButton(
              inputId = "renderIncidentRatePlot",
              label = "Render Plot"
            )
          )
        )
      ),
      ggiraph::ggiraphOutput(
        outputId = "incidenceRatePlot",
        width = "100%",
        height = "100%"
      )
    )
  ),
  shinydashboard::tabItem(
    tabName = "timeSeries",
    createShinyBoxFromOutputId("timeSeriesSelectedCohorts"),
    tags$table(
      tags$tr(
        tags$td(
          shiny::radioButtons(
            inputId = "timeSeriesAggregationPeriodSelection",
            label = "Aggregation period:",
            choices = c("Monthly","Yearly"),
            selected = "Monthly",
            inline = TRUE
          )
        ),
        tags$td(
          shinyWidgets::pickerInput(
            inputId = "timeSeriesTypeFilter",
            label = "Time series Type",
            choices = c(""),
            width = 200,
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
        tags$td(
          shiny::sliderInput(
            inputId = "timeSeriesPeriodRangeFilter",
            label = "Filter By Period Begin",
            min = c(0),
            max = c(0),
            value = c(0, 0),
            dragRange = TRUE,
            pre = "Year ",
            step = 1,
            sep = ""
          )
        ),
        tags$td(),
        tags$td(),
        tags$td(tags$b("Series Type Description :"),
          shiny::uiOutput(outputId = "timeSeriesTypeLong")
        )
      )
    ),
    shinydashboard::box(
      title = "Time Series",
      width = NULL,
      status = "primary",
      solidHeader = TRUE,
      
      shiny::column(
        3,
        shiny::radioButtons(
          inputId = "timeSeriesType",
          label = "",
          choices = c("Table", "Plot"),
          selected = "Table",
          inline = TRUE
        )
      ),
      shiny::column(
        6,
        shiny::conditionalPanel(
          condition = "input.timeSeriesType=='Plot'",
          shinyWidgets::pickerInput(
            inputId = "timeSeriesPlotFilters",
            label = "Filter By:",
            width = 300,
            choices = c("Records",
                        "Subjects",
                        "Person Days",
                        "Records Start", 
                        "Subjects Start", 
                        "Records End", 
                        "Subjects End"),
            selected = c("Subjects"),
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
        ),
      ),
      shiny::conditionalPanel(
        condition = "input.timeSeriesType=='Table'",
        tags$table(width = "100%",
                   tags$tr(
                     tags$td(
                       align = "right",
                       shiny::downloadButton(
                         "saveTimeSeriesTable",
                         label = "",
                         icon = shiny::icon("download"),
                         style = "margin-top: 5px; margin-bottom: 5px;"
                       )
                     )
                   )),
        DT::dataTableOutput("fixedTimeSeriesTable")
      ),
      shiny::conditionalPanel(
        condition = "input.timeSeriesType=='Plot'",
       shiny::column(12,
          plotly::plotlyOutput("fixedTimeSeriesPlot",height = "auto"),
          tags$head(
            tags$style("#fixedTimeSeriesPlot { width: '90vw' !important};")
          )
        )
      )
    )
  ),
  shinydashboard::tabItem(
    tabName = "timeDistribution",
    createShinyBoxFromOutputId("timeDistSelectedCohorts"),
    shinydashboard::box(
      width = NULL,
      status = "primary",
      tags$br(),
      tags$h4("Time distribution"),
      plotly::plotlyOutput("timeDistributionPlot", height = "auto"),
      tags$head(
        tags$style("#timeDistributionPlot { width: '90vw' !important};")
      ),
      tags$table(width = "100%", 
                 tags$tr(
                   tags$td(align = "right",
                           shiny::downloadButton(
                             "saveTimeDistributionTable",
                             label = "",
                             icon = shiny::icon("download"),
                             style = "margin-top: 5px; margin-bottom: 5px;"
                           )
                   )
                 )
      ),
      DT::dataTableOutput("timeDistributionTable")
    )
  ),
  shinydashboard::tabItem(
    tabName = "indexEventBreakdown",
    createShinyBoxFromOutputId("indexEventBreakdownSelectedCohort"),
    tags$table(width = '100%',
      tags$tr(
        tags$td(
          shiny::radioButtons(
            inputId = "indexEventBreakdownTableRadioButton",
            label = "",
            choices = c("All", "Standard concepts", "Non Standard Concepts"),
            selected = "All",
            inline = TRUE
          )
        ),
        tags$td(HTML("&nbsp;&nbsp;&nbsp;")),
        tags$td(
          shiny::radioButtons(
            inputId = "indexEventBreakdownTableFilter",
            label = "Display",
            choices = c("Both", "Records", "Persons"), 
            selected = "Persons",
            inline = TRUE
          )
        ),
        tags$td(HTML("&nbsp;&nbsp;&nbsp;")),
        tags$td(
          shiny::radioButtons(
            inputId = "indexEventBreakdownValueFilter",
            label = "Display Value Type",
            choices = c("Absolute", "Percentage"), 
            selected = "Absolute",
            inline = TRUE
          )
        )
      )
    ),
    tags$table(width = "100%", 
               tags$tr(
                 tags$td(align = "right",
                         shiny::downloadButton(
                           "saveBreakdownTable",
                           label = "",
                           icon = shiny::icon("download"),
                           style = "margin-top: 5px; margin-bottom: 5px;"
                         )
                 )
               )
    ),
    DT::dataTableOutput(outputId = "indexEventBreakdownTable"),
    tags$br(),
    shiny::column(12,
                  shiny::conditionalPanel(
                    condition = "true",
                    shiny::uiOutput(outputId = "dynamicUIForRelationshipAndTemeSeriesForIndexEvent")
                  )
    )
  ),
  shinydashboard::tabItem(
    tabName = "visitContext",
    createShinyBoxFromOutputId("visitContextSelectedCohort"),
    shiny::conditionalPanel(
      condition = "output.doesVisitContextContainData == true",
      tags$table(width = '100%',
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
              inputId = "visitContextValueFilter",
              label = "Display Value Type",
              choices = c("Absolute", "Percentage"), 
              selected = "Absolute",
              inline = TRUE
            )
          ),
          tags$td(
            shiny::radioButtons(
              inputId = "visitContextPersonOrRecords",
              label = "Display",
              choices = c("Person", "Record"),
              selected = "Person",
              inline = TRUE
            )
          ),
          tags$td(
            align = "right",
            shiny::downloadButton(
              "saveVisitContextTable",
              label = "",
              icon = shiny::icon("download"),
              style = "margin-top: 5px; margin-bottom: 5px;"
            )
          )
        )
      )
    ),
    DT::dataTableOutput(outputId = "visitContextTable")
  ),
  shinydashboard::tabItem(
    tabName = "cohortOverlap",
    createShinyBoxFromOutputId("cohortOverlapSelectedCohort"),
    shinydashboard::box(
      title = "Cohort Overlap (Subjects)",
      width = NULL,
      status = "primary",
      shiny::tabsetPanel(
        type = "tab",
        id = "cohortOverlapTab",
        shiny::tabPanel(
          title = "Plot",
          value = "cohortOverlapPlotTab",
          tags$table(width = "100%", 
                     tags$tr(
                       tags$td(
                         shiny::radioButtons(
                           inputId = "overlapPlotType",
                           label = "",
                           choices = c("Percentages", "Counts"),
                           selected = "Percentages",
                           inline = TRUE
                         )
                       ),
                       
                     )
          ),
          plotly::plotlyOutput("overlapPlot", height = "auto")
          ),
        shiny::tabPanel(
          title = "Raw Table",
          value = "cohortOverlapTableTab",
          tags$table(width = "100%",
                     tags$tr(
                       tags$td(
                         align = "right",
                         shiny::downloadButton(
                           "saveCohortOverlapTable",
                           label = "",
                           icon = shiny::icon("download"),
                           style = "margin-top: 5px; margin-bottom: 5px;"
                         )
                       )
                     )),
          DT::dataTableOutput(outputId = "cohortOverlapTable")
        ))
     
    )
  ),
  shinydashboard::tabItem(
    tabName = "cohortCharacterization",
    createShinyBoxFromOutputId("characterizationSelectedCohort"),
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
          shiny::conditionalPanel(condition = "input.charType == 'Raw'",
                                  tags$table(tags$tr(
                                    tags$td(
                                      shinyWidgets::pickerInput(
                                        inputId = "characterizationAnalysisNameOptions",
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
                                        inputId = "characterizationDomainNameOptions",
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
                                        inputId = "charProportionOrContinuous",
                                        label = "",
                                        choices = c("All", "Proportion", "Continuous"),
                                        selected = "Proportion",
                                        inline = TRUE
                                      )
                                    )
                                  )))
        )
      ),
      tags$tr(
        tags$td(colspan = 2,
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
      )),
    tags$table(width = "100%", 
               tags$tr(
                 tags$td(align = "right",
                         shiny::downloadButton(
                           outputId = "saveCohortCharacterizationTable",
                           label = "",
                           icon = shiny::icon("download"),
                           style = "margin-top: 5px; margin-bottom: 5px;"
                         )
                 )
               )
    ),
    DT::dataTableOutput(outputId = "characterizationTable")
  ),
  shinydashboard::tabItem(
    tabName = "temporalCharacterization",
    createShinyBoxWithSplitForTwoOutputIds(leftOutputId = "temporalCharacterizationSelectedCohort",
                                           leftOutputLabel = "Cohort",
                                           rightOutputId = "temporalCharacterizationSelectedDatabase",
                                           rightOutputLabel = "Datasource",
                                           leftUnits = 70),
    tags$table(tags$tr(
      tags$td(
        shinyWidgets::pickerInput(
          inputId = "temporalCharacterizationAnalysisNameOptions",
          label = "Analysis name",
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
      ),
      tags$td(
        shinyWidgets::pickerInput(
          inputId = "temporalCharacterizationDomainNameOptions",
          label = "Domain name",
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
      ),
      tags$td(
        shiny::radioButtons(
          inputId = "temporalCharacterizationOutputTypeProportionOrContinuous",
          label = "",
          choices = c("All", "Proportion", "Continuous"),
          selected = "Proportion",
          inline = TRUE
        )
      )
    )),
    tags$table(width = "100%", 
               tags$tr(
                 tags$td(align = "right",
                         shiny::downloadButton(
                           outputId = "saveTemporalCharacterizationTable",
                           label = "",
                           icon = shiny::icon("download"),
                           style = "margin-top: 5px; margin-bottom: 5px;"
                         )
                 )
               )
    ),
    DT::dataTableOutput("temporalCharacterizationTable")
  ),
  shinydashboard::tabItem(
    tabName = "compareCohortCharacterization",
    createShinyBoxWithSplitForTwoOutputIds(leftOutputId = "cohortCharCompareSelectedCohort", 
                                           leftOutputLabel = "Cohort",
                                           rightOutputId = "cohortCharCompareSelectedDatabase",
                                           rightOutputLabel = "Datasource",
                                           leftUnits = 70),
    tags$table(
      tags$tr(
        tags$td(
          shiny::radioButtons(
            inputId = "characterizationCompareMethod",
            label = "",
            choices = c("Pretty table", "Raw table", "Plot"),
            selected = "Plot",
            inline = TRUE
          ),
        ),
        tags$td(
          shiny::conditionalPanel(
            condition = "input.characterizationCompareMethod == 'Raw table'",
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
    
    shiny::conditionalPanel(condition = "input.characterizationCompareMethod == 'Raw table' | input.characterizationCompareMethod=='Plot'",
                            tags$table(tags$tr(
                              tags$td(
                                shinyWidgets::pickerInput(
                                  inputId = "compareCharacterizationAnalysisNameFilter",
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
                                shiny::radioButtons(
                                  inputId = "compareCharacterizationProportionOrContinous",
                                  label = "",
                                  choices = c("All", "Proportion", "Continuous"),
                                  selected = "Proportion",
                                  inline = TRUE
                                )
                              )
                            ))),
    shiny::conditionalPanel(condition = "input.characterizationCompareMethod=='Pretty table' | input.characterizationCompareMethod=='Raw table'",
                            tags$table(width = "100%", 
                                       tags$tr(
                                         tags$td(align = "right",
                                                 shiny::downloadButton(
                                                   outputId = "saveCompareCohortCharacterizationTable",
                                                   label = "",
                                                   icon = shiny::icon("download"),
                                                   style = "margin-top: 5px; margin-bottom: 5px;"
                                                 )
                                         )
                                       )
                            ),
                            # tags$td(
                            #   shiny::radioButtons(
                            #     inputId = "compareCharacterizationProportionOrContinous",
                            #     label = "",
                            #     choices = c("All", "Proportion", "Continuous"),
                            #     selected = "Proportion",
                            #     inline = TRUE
                            #   )
                            # ),
                            DT::dataTableOutput("compareCharacterizationTable")),
    shiny::conditionalPanel(
      condition = "input.characterizationCompareMethod=='Plot'",
      shinydashboard::box(
        title = "Compare Cohort Characterization",
        width = NULL,
        status = "primary",
        shiny::htmlOutput("compareCohortCharacterizationSelectedCohort"),
        plotly::plotlyOutput(
          outputId = "compareCharacterizationPlot",
          width = "100%",
          height = "100%"
        )
      )
    )
  ),
  shinydashboard::tabItem(
    tabName = "compareTemporalCharacterization",
    createShinyBoxWithSplitForTwoOutputIds(leftOutputId = "temporalCharCompareSelectedCohort", 
                                           leftOutputLabel = "Cohort",
                                           rightOutputId = "temporalCharCompareSelectedDatabase",
                                           rightOutputLabel = "Datasource",
                                           leftUnits = 70),
    tags$table(
      tags$tr(
        tags$td(
          shiny::radioButtons(
            inputId = "temporalCharacterizationType",
            label = "",
            choices = c("Raw table", "Plot"),
            #"Pretty table", removed pretty option for compare temporal characterization
            # Pretty table can be put back in - we will need a different Table1Specs for temporal characterization
            selected = "Plot",
            inline = TRUE
          )
        ),
        tags$td(
          shiny::conditionalPanel(
            condition = "input.temporalCharacterizationType == 'Plot'",
            shiny::sliderInput(
              inputId = "temporalCharacterizationXMeanFilter",
              label = "Filter X-axis",
              min = c(0.0),
              max = c(1.0),
              value = c(0.0, 1.0),
              dragRange = TRUE,
              pre = "Mean ",
              step = 0.1,
              sep = ""
            )
          )
        ),
        tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
        tags$td(
          shiny::conditionalPanel(
            condition = "input.temporalCharacterizationType == 'Plot'",
            shiny::sliderInput(
              inputId = "temporalCharacterizationYMeanFilter",
              label = "Filter Y-axis",
              min = c(0.0),
              max = c(1.0),
              value = c(0.0, 1.0),
              dragRange = TRUE,
              pre = "Mean ",
              step = 0.1,
              sep = ""
            )
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
    shiny::conditionalPanel(condition = "input.temporalCharacterizationType == 'Raw table' | input.temporalCharacterizationType=='Plot'",
                            tags$table(tags$tr(
                              tags$td(
                                shinyWidgets::pickerInput(
                                  inputId = "compareTemporalCharacterizationAnalysisNameFilter",
                                  label = "Analysis name",
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
                              ),
                              tags$td(
                                shinyWidgets::pickerInput(
                                  inputId = "compareTemporalCharacterizationDomainNameFilter",
                                  label = "Domain name",
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
                              ),
                              tags$td(
                                shiny::radioButtons(
                                  inputId = "temporalCharacterProportionOrContinuous",
                                  label = "Filter to:",
                                  choices = c("All", "Proportion", "Continuous"),
                                  selected = "Proportion",
                                  inline = TRUE
                                )
                              )
                            ))),
    shiny::conditionalPanel(
      condition = "input.temporalCharacterizationType=='Pretty table' |
                            input.temporalCharacterizationType=='Raw table'",
      tags$table(width = "100%", 
                 tags$tr(
                   tags$td(align = "right",
                           shiny::downloadButton(
                             outputId = "saveCompareTemporalCharacterizationTable",
                             label = "",
                             icon = shiny::icon("download"),
                             style = "margin-top: 5px; margin-bottom: 5px;"
                           )
                   )
                 )
      ),
      DT::dataTableOutput(outputId = "compareTemporalCharacterizationTable")
    ),
    shiny::conditionalPanel(
      condition = "input.temporalCharacterizationType=='Plot'",
      shinydashboard::box(
        title = "Compare Temporal Characterization",
        width = NULL,
        status = "primary",
        plotly::plotlyOutput(
          outputId = "compareTemporalCharacterizationPlot",
          width = "100%",
          height = "auto"
        ),
        tags$head(
          tags$style("#compareTemporalCharacterizationPlot { width: '90vw' !important};")
        )
      )
    )
  ),
  shinydashboard::tabItem(tabName = "databaseInformation",
                          shiny::tabsetPanel(
                            id = "metadataInformation",
                            shiny::tabPanel(
                              title = "Data source",
                              tags$br(),
                              DT::dataTableOutput("databaseInformationTable")
                            ),
                            shiny::tabPanel(
                              title = "Meta data information",
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
                                  DT::dataTableOutput("packageDependencySnapShotTable")
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
                                  # DT::dataTableOutput("argumentsAtDiagnosticsInitiationJson")
                                )
                              ) 
                            )
                          )
                     )
)


#body
body <- shinydashboard::dashboardBody(bodyTabItems,
                                      htmltools::withTags(
                                        div(style = "margin-left : 0px",
                                            h6(appInformationText)
                                        )))


#main
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
