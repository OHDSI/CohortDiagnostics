plotTimeDistribution <- function(data, shortNameRef = NULL) {
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
      plotData$shortName,
      "\nDatabase = ",
      plotData$databaseId,
      "\nMin = ",
      scales::comma(plotData$minValue, accuracy = 1),
      "\nP25 = ",
      scales::comma(plotData$p25Value, accuracy = 1),
      "\nMedian = ",
      scales::comma(plotData$medianValue, accuracy = 1),
      "\nP75 = ",
      scales::comma(plotData$p75Value, accuracy = 1),
      "\nMax = ",
      scales::comma(plotData$maxValue, accuracy = 1),
      "\nTime Measure = ",
      plotData$timeMetric,
      "\nAverage = ",
      scales::comma(x = plotData$averageValue, accuracy = 0.01)
    )
  )

  sortShortName <- plotData %>%
    dplyr::select(shortName) %>%
    dplyr::distinct() %>%
    dplyr::arrange(-as.integer(sub(
      pattern = "^C", "", x = shortName
    )))

  plotData <- plotData %>%
    dplyr::arrange(
      shortName = factor(shortName, levels = sortShortName$shortName),
      shortName
    )

  plotData$shortName <- factor(plotData$shortName,
                               levels = sortShortName$shortName
  )

  plot <- ggplot2::ggplot(data = plotData) +
    ggplot2::aes(
      x = shortName,
      ymin = minValue,
      lower = p25Value,
      middle = medianValue,
      upper = p75Value,
      ymax = maxValue,
      average = averageValue
    ) +
    ggplot2::geom_errorbar(size = 0.5) +
    ggiraph::geom_boxplot_interactive(
      ggplot2::aes(tooltip = tooltip),
      stat = "identity",
      fill = rgb(0, 0, 0.8, alpha = 0.25),
      size = 0.2
    ) +
    ggplot2::facet_grid(databaseName ~ timeMetric, scales = "free") +
    ggplot2::coord_flip() +
    ggplot2::theme(
      panel.grid.major.y = ggplot2::element_blank(),
      panel.grid.minor.y = ggplot2::element_blank(),
      axis.title.y = ggplot2::element_blank(),
      axis.ticks.y = ggplot2::element_blank(),
      strip.background = ggplot2::element_blank(),
      strip.text.y = ggplot2::element_text(size = 5)
    )
  height <-
    1.5 + 0.4 * nrow(dplyr::distinct(plotData, databaseId, shortName))
  plot <- ggiraph::girafe(
    ggobj = plot,
    options = list(
      ggiraph::opts_sizing(width = .7),
      ggiraph::opts_zoom(max = 5)
    ),
    width_svg = 12,
    height_svg = height
  )
}

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
      shiny::htmlTemplate(file.path("html", "timeDistribution.html"))
    ),
    shinydashboard::box(
      status = "warning",
      width = "100%",
      tags$div(
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
        csvDownloadButton(ns, "timeDistributionTable")
      ),
      shiny::conditionalPanel(
        condition = "input.timeDistributionType=='Plot'",
        ns = ns,
        tags$br(),
        shinycssloaders::withSpinner(ggiraph::ggiraphOutput(ns("timeDistributionPlot"), width = "100%", height = "100%"))
      )
    )
  )
}

timeDistributionsModule <- function(id,
                                    dataSource,
                                    selectedCohorts,
                                    selectedDatabaseIds,
                                    cohortIds,
                                    cohortTable,
                                    databaseTable) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI({ selectedCohorts() })

    # Time distribution -----
    ## timeDistributionData -----
    timeDistributionData <- shiny::reactive({
      validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      validate(need(length(cohortIds()) > 0, "No cohorts chosen"))

      data <- getTimeDistributionResult(
        dataSource = dataSource,
        cohortIds = cohortIds(),
        databaseIds = selectedDatabaseIds(),
        databaseTable = databaseTable
      )

      if (hasData(data)) {
        data <- data %>% dplyr::filter(timeMetric %in% input$selecatableTimeMeasures)
      }

      return(data)
    })

    ## output: timeDistributionPlot -----
    output$timeDistributionPlot <- ggiraph::renderggiraph(expr = {
      data <- timeDistributionData()
      validate(need(hasData(data), "No data for this combination"))
      plot <- plotTimeDistribution(data = data, shortNameRef = cohortTable)
      return(plot)
    })

    ## output: timeDistributionTable -----
    output$timeDistributionTable <- reactable::renderReactable(expr = {
      data <- timeDistributionData()
      validate(need(hasData(data), "No data for this combination"))

      data <- data %>%
        dplyr::inner_join(cohortTable %>% dplyr::select(cohortName, cohortId), by = "cohortId") %>%
        dplyr::arrange(databaseId, cohortId) %>%
        dplyr::select(
          cohortId,
          Database = databaseName,
          Cohort = cohortName,
          TimeMeasure = timeMetric,
          Average = averageValue,
          SD = standardDeviation,
          Min = minValue,
          P10 = p10Value,
          P25 = p25Value,
          Median = medianValue,
          P75 = p75Value,
          P90 = p90Value,
          Max = maxValue
        ) %>%
        dplyr::select(all_of(c("Database", "cohortId", "Cohort", "TimeMeasure", input$selecatableCols)))

      validate(need(hasData(data), "No data for this combination"))

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
