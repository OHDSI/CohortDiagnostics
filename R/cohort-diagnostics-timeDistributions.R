# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of PatientLevelPrediction
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Returns data from time_distribution table of Cohort Diagnostics results data model
getTimeDistributionResult <- function(dataSource,
                                      cohortIds,
                                      databaseIds,
                                      databaseTable) {
  data <- queryResultCovariateValue(
    dataSource = dataSource,
    cohortIds = cohortIds,
    databaseIds = databaseIds,
    analysisIds = c(8, 9, 10),
    temporalCovariateValue = FALSE,
    temporalCovariateValueDist = TRUE
  )
  if (!hasData(data)) {
    return(NULL)
  }
  temporalCovariateValueDist <- data$temporalCovariateValueDist
  if (!hasData(temporalCovariateValueDist)) {
    return(NULL)
  }
  data <- temporalCovariateValueDist %>%
    dplyr::inner_join(data$temporalCovariateRef,
                      by = "covariateId"
    ) %>%
    dplyr::inner_join(data$temporalAnalysisRef,
                      by = "analysisId"
    ) %>%
    dplyr::inner_join(databaseTable,
                      by = "databaseId"
    ) %>%
    dplyr::rename(
      "timeMetric" = "covariateName",
      "averageValue" = "mean",
      "standardDeviation" = "sd"
    ) %>%
    dplyr::select(
      "cohortId",
      "databaseId",
      "databaseName",
      "timeMetric",
      "averageValue",
      "standardDeviation",
      "minValue",
      "p10Value",
      "p25Value",
      "medianValue",
      "p75Value",
      "p90Value",
      "maxValue"
    )
  return(data)
}

plotTimeDistribution <- function(data, shortNameRef = NULL, showMax = FALSE) {
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(
    x = data,
    any.missing = FALSE,
    min.rows = 1,
    min.cols = 5,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertNames(
    x = colnames(data),
    must.include = c(
      "minValue",
      "p25Value",
      "medianValue",
      "p75Value",
      "maxValue"
    ),
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)

  plotData <-
    addShortName(data = data, shortNameRef = shortNameRef)

  plotData$tooltip <- c(
    paste0(
      plotData$cohortName,
      "\n\nDatabase = ",
      plotData$databaseName,
      "\n\nP10 = ",
      scales::comma(plotData$p10Value, accuracy = 1),
      "  P25 = ",
      scales::comma(plotData$p25Value, accuracy = 1),
      "  Median = ",
      scales::comma(plotData$medianValue, accuracy = 1),
      "  P75 = ",
      scales::comma(plotData$p75Value, accuracy = 1),
      "  P90 = ",
      scales::comma(plotData$p90Value, accuracy = 1),

      "\n\nMin = ",
      scales::comma(plotData$minValue, accuracy = 1),
      "  Max = ",
      scales::comma(plotData$maxValue, accuracy = 1),
      "  Mean = ",
      scales::comma(x = plotData$averageValue, accuracy = 0.01)
    )
  )

  # Fixed colour reference
  nCohorts <- plotData$shortName %>% unique() %>% length()
  colorPallet <- RColorBrewer::brewer.pal(max(c(3, nCohorts)), "Set3")

  sortShortName <- plotData %>%
    dplyr::select("shortName") %>%
    dplyr::distinct() %>%
    dplyr::arrange(-as.integer(sub(
      pattern = "^C", "", x = .data$shortName
    )))

  plotData <- plotData %>%
    dplyr::arrange(
      shortName = factor(.data$shortName, levels = sortShortName$shortName),
      .data$shortName
    )

  plotData$shortName <- factor(plotData$shortName,
                               levels = sortShortName$shortName
  )

  ncols <- plotData$timeMetric %>% unique() %>% length()
  nrows <- plotData$databaseName %>% unique() %>% length()
  subplots <- list()

  for (db in plotData$databaseName %>% unique()) {
    for (tm in plotData$timeMetric %>% unique()) {
      subset <- plotData %>%
        dplyr::filter(.data$timeMetric == tm, .data$databaseName == db)
      subplots[[length(subplots) + 1]] <- subset %>%
        plotly::plot_ly() %>%
        plotly::add_boxplot(y = ~shortName,
                            whiskerwidth = 0.2,
                            color = ~shortName,
                            colors = colorPallet,
                            type = "box",
                            hoverlabel = list(bgcolor = "#000"),
                            line = list(color = 'rgb(0,0,0)', width = 1.5),
                            hoveron = "points",
                            q1 = ~p25Value,
                            q3 = ~p75Value,
                            median = ~medianValue,
                            mean = ~averageValue,
                            upperfence = ~p90Value,
                            lowerfence = ~p10Value) %>%
        plotly::add_markers(y = ~shortName,
                            color = ~shortName,
                            text = ~tooltip,
                            opacity = 0.99,
                            size = 100,
                            x = ~medianValue) %>%
        plotly::layout(plot_bgcolor = '#e5ecf6',
                       xaxis = list(
                         title = "time in days",
                         zerolinecolor = '#ffff',
                         zerolinewidth = 2,
                         gridcolor = 'ffff'),
                       yaxis = list(
                         title = addTextBreaks(
                           text = db, 
                           length = 25 # change this based on plot height?
                         ),
                         showTitle = FALSE,
                         zerolinecolor = '#ffff',
                         zerolinewidth = 2,
                         gridcolor = 'ffff'))

      if (showMax) {
        subplots[[length(subplots)]] <- subplots[[length(subplots)]] %>%
          plotly::add_markers(y = ~shortName,
                              color = ~shortName,
                              text = ~tooltip,
                              size = 1,
                              x = ~maxValue)
      }
    }
  }

  # Add titles for each subplot
  topAnnotations <- list()
  for (tm in plotData$timeMetric %>% unique()) {
    # Trial and error...
    xTitlePos <- (length(topAnnotations) / ncols) + 1 / ncols * 0.5
    topAnnotations[[length(topAnnotations) + 1]] <- list(text = tm,
                                                         showarrow = FALSE,
                                                         x = xTitlePos,
                                                         y = 1.0,
                                                         xref = "paper",
                                                         yref = "paper",
                                                         xanchor = "center",
                                                         yanchor = "bottom")
  }

  plt <- plotly::subplot(subplots, nrows = nrows, shareY = TRUE, shareX = TRUE) %>%
    plotly::layout(annotations = topAnnotations, showlegend = F)

  return(plt)
}

#' timeDistributions view
#' @description
#' Use for customizing UI
#'
#' @param id    Namespace Id - use namespaced id ns("imeDistributions") inside diagnosticsExplorer module
#' @family CohortDiagnostics
#' @export
timeDistributionsView <- function(id) {
  ns <- shiny::NS(id)
  selectableCols <- c(
    "Average",
    "SD",
    "Min",
    "P10",
    "P25",
    "Median",
    "P75",
    "P90",
    "Max"
  )

  selectableTimeMeasures <- c(
    "observation time (days) prior to index",
    "observation time (days) after index",
    "time (days) between cohort start and end"
  )

  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Time Distributions",
      width = "100%",
      shiny::htmlTemplate(system.file("cohort-diagnostics-www", "timeDistribution.html", package = utils::packageName()))
    ),
    shinydashboard::box(
      status = "warning",
      width = "100%",
      shiny::tags$div(
        style = "max-height: 100px; overflow-y: auto",
        shiny::uiOutput(outputId = ns("selectedCohorts"))
      )
    ),
    shinydashboard::box(
      title = "Time Distributions",
      width = NULL,
      status = "primary",

      shiny::fluidRow(
        shiny::column(
          width = 2,
          shiny::radioButtons(
            inputId = ns("timeDistributionType"),
            label = "",
            choices = c("Table", "Plot"),
            selected = "Plot",
            inline = TRUE
          )
        ),
        shiny::column(
          width = 5,
          shinyWidgets::pickerInput(
            label = "View Time Measures",
            inputId = ns("selecatableTimeMeasures"),
            multiple = TRUE,
            selected = selectableTimeMeasures,
            choices = selectableTimeMeasures,
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
        shiny::column(
          width = 5,
          shiny::conditionalPanel(
            condition = "input.timeDistributionType=='Table'",
            ns = ns,
            shinyWidgets::pickerInput(
              label = "View Columns",
              inputId = ns("selecatableCols"),
              multiple = TRUE,
              selected = selectableCols,
              choices = selectableCols,
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
        )
      ),
      shiny::conditionalPanel(
        condition = "input.timeDistributionType=='Table'",
        ns = ns,
        shinycssloaders::withSpinner(reactable::reactableOutput(outputId = ns("timeDistributionTable"))),
        reactableCsvDownloadButton(ns, "timeDistributionTable")
      ),
      shiny::conditionalPanel(
        condition = "input.timeDistributionType=='Plot'",
        ns = ns,
        shiny::fluidRow(
          shiny::column(
            width = 3,
            shiny::checkboxInput(ns("showMaxValues"), label = "Show max values", value = FALSE)
          ),
          shiny::column(
            width = 3,
            shiny::numericInput(
              inputId = ns("plotRowHeight"),
              label = "Plot row height (pixels)",
              max = 2000,
              value = 400,
              min = 200
            )
          )
        ),
        shiny::tags$br(),
        shiny::uiOutput(ns("plotArea"))
      )
    )
  )
}

timeDistributionsModule <- function(id,
                                    dataSource,
                                    selectedCohorts,
                                    selectedDatabaseIds,
                                    cohortIds,
                                    cohortTable = dataSource$cohortTable,
                                    databaseTable = dataSource$dbTable) {
  shiny::moduleServer(id, function(input, output, session) {
    ns <- session$ns
    output$selectedCohorts <- shiny::renderUI({ selectedCohorts() })

    # Time distribution -----
    ## timeDistributionData -----
    timeDistributionData <- shiny::reactive({
      shiny::validate(shiny::need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      shiny::validate(shiny::need(length(cohortIds()) > 0, "No cohorts chosen"))

      data <- getTimeDistributionResult(
        dataSource = dataSource,
        cohortIds = cohortIds(),
        databaseIds = selectedDatabaseIds(),
        databaseTable = databaseTable
      )

      if (hasData(data)) {
        data <- data %>% dplyr::filter(.data$timeMetric %in% input$selecatableTimeMeasures)
      }

      return(data)
    })


    output$plotArea <- shiny::renderUI({
      rowHeight <- ifelse(is.null(input$plotRowHeight) | is.na(input$plotRowHeight), 400, input$plotRowHeight)
      plotHeight <- rowHeight * length(selectedDatabaseIds())
      shiny::div(
        id = ns("tsPlotContainer"),
        shinycssloaders::withSpinner(plotly::plotlyOutput(ns("timeDistributionPlot"),
                                                          width = "100%",
                                                          height = sprintf("%spx", plotHeight + 50))),
        height = sprintf("%spx", plotHeight + 50)
      )
    })


    ## output: timeDistributionPlot -----
    output$timeDistributionPlot <- plotly::renderPlotly(expr = {
      data <- timeDistributionData() %>%
        dplyr::inner_join(cohortTable %>% dplyr::select("cohortName", "cohortId"), by = "cohortId")
      shiny::validate(shiny::need(hasData(data), "No data for this combination"))
      plot <- plotTimeDistribution(data = data, shortNameRef = cohortTable, showMax = isTRUE(input$showMaxValues))

      # Note that this code is only used because renderUI/ uiOutput didn't seem to update with plotly
      plotHeight <- 300 * length(selectedDatabaseIds())
      session$sendCustomMessage(ns("tsPlotHeight"), sprintf("%spx", plotHeight))

      return(plot)
    })

    ## output: timeDistributionTable -----
    output$timeDistributionTable <- reactable::renderReactable(expr = {
      data <- timeDistributionData()
      shiny::validate(shiny::need(hasData(data), "No data for this combination"))

      data <- data %>%
        dplyr::inner_join(cohortTable %>% dplyr::select("cohortName", "cohortId"), by = "cohortId") %>%
        dplyr::arrange(.data$databaseId, .data$cohortId) %>%
        dplyr::select(
          "cohortId",
          "Database" = "databaseName",
          "Cohort" = "cohortName",
          "TimeMeasure" = "timeMetric",
          "Average" = "averageValue",
          "SD" = "standardDeviation",
          "Min" = "minValue",
          "P10" = "p10Value",
          "P25" = "p25Value",
          "Median" = "medianValue",
          "P75" = "p75Value",
          "P90" = "p90Value",
          "Max" = "maxValue"
        ) %>%
        dplyr::select(dplyr::all_of(c("Database", "cohortId", "Cohort", "TimeMeasure", input$selecatableCols)))

      shiny::validate(shiny::need(hasData(data), "No data for this combination"))

      keyColumns <- c(
        "Database",
        "cohortId",
        "Cohort",
        "TimeMeasure"
      )
      dataColumns <- input$selecatableCols

      table <- getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns
      )
      return(table)
    })
  })
}
