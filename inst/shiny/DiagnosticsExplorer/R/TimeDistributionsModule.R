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
    dplyr::select(.data$shortName) %>%
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

  plot <- ggplot2::ggplot(data = plotData) +
    ggplot2::aes(
      x = .data$shortName,
      ymin = .data$minValue,
      lower = .data$p25Value,
      middle = .data$medianValue,
      upper = .data$p75Value,
      ymax = .data$maxValue,
      average = .data$averageValue
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
    1.5 + 0.4 * nrow(dplyr::distinct(plotData, .data$databaseId, .data$shortName))
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
  shiny::tagList(
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
      shiny::radioButtons(
        inputId = ns("timeDistributionType"),
        label = "",
        choices = c("Table", "Plot"),
        selected = "Plot",
        inline = TRUE
      ),
      shiny::conditionalPanel(
        condition = "input.timeDistributionType=='Table'",
        ns = ns,
        tags$table(
          width = "100%",
          tags$tr(tags$td(
            align = "right",
          ))
        ),
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
    timeDistributionData <- reactive({
      validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
      data <- getTimeDistributionResult(
        dataSource = dataSource,
        cohortIds = cohortIds(),
        databaseIds = selectedDatabaseIds(),
        databaseTable = databaseTable
      )
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
        addShortName(cohortTable) %>%
        dplyr::arrange(.data$databaseId, .data$cohortId) %>%
        dplyr::mutate(
          databaseName = as.factor(.data$databaseName)
        ) %>%
        dplyr::select(
          Database = .data$databaseName,
          Cohort = .data$shortName,
          TimeMeasure = .data$timeMetric,
          Average = .data$averageValue,
          SD = .data$standardDeviation,
          Min = .data$minValue,
          P10 = .data$p10Value,
          P25 = .data$p25Value,
          Median = .data$medianValue,
          P75 = .data$p75Value,
          P90 = .data$p90Value,
          Max = .data$maxValue
        )

      validate(need(hasData(data), "No data for this combination"))

      keyColumns <- c(
        "Database",
        "Cohort",
        "TimeMeasure"
      )
      dataColumns <- c(
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

      table <- getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns
      )
      return(table)
    })
  })
}