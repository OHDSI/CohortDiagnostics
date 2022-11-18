### cohort overlap plot ##############
plotCohortOverlap <- function(data,
                              shortNameRef = NULL,
                              yAxis = "Percentages") {
  data <- data %>%
    addShortName(
      shortNameRef = shortNameRef,
      cohortIdColumn = "targetCohortId",
      shortNameColumn = "targetShortName"
    ) %>%
    addShortName(
      shortNameRef = shortNameRef,
      cohortIdColumn = "comparatorCohortId",
      shortNameColumn = "comparatorShortName"
    )

  plotData <- data %>%
    dplyr::mutate(
      absTOnlySubjects = abs(tOnlySubjects),
      absCOnlySubjects = abs(cOnlySubjects),
      absBothSubjects = abs(bothSubjects),
      absEitherSubjects = abs(eitherSubjects),
      signTOnlySubjects = dplyr::case_when(tOnlySubjects < 0 ~ "<", TRUE ~ ""),
      signCOnlySubjects = dplyr::case_when(cOnlySubjects < 0 ~ "<", TRUE ~ ""),
      signBothSubjects = dplyr::case_when(bothSubjects < 0 ~ "<", TRUE ~ "")
    ) %>%
    dplyr::mutate(
      tOnlyString = paste0(
        signTOnlySubjects,
        scales::comma(absTOnlySubjects, accuracy = 1),
        " (",
        signTOnlySubjects,
        scales::percent(absTOnlySubjects /
                          absEitherSubjects,
                        accuracy = 1
        ),
        ")"
      ),
      cOnlyString = paste0(
        signCOnlySubjects,
        scales::comma(absCOnlySubjects, accuracy = 1),
        " (",
        signCOnlySubjects,
        scales::percent(absCOnlySubjects /
                          absEitherSubjects,
                        accuracy = 1
        ),
        ")"
      ),
      bothString = paste0(
        signBothSubjects,
        scales::comma(absBothSubjects, accuracy = 1),
        " (",
        signBothSubjects,
        scales::percent(absBothSubjects /
                          absEitherSubjects,
                        accuracy = 1
        ),
        ")"
      )
    ) %>%
    dplyr::mutate(
      tooltip = paste0(
        "Database: ",
        databaseName,
        "\n",
        "\n",
        targetShortName,
        " only: ",
        tOnlyString,
        "\nBoth: ",
        bothString,
        "\n",
        comparatorShortName,
        " only: ",
        cOnlyString
      )
    ) %>%
    dplyr::select(
      targetShortName,
      comparatorShortName,
      databaseId,
      databaseName,
      absTOnlySubjects,
      absCOnlySubjects,
      absBothSubjects,
      tooltip
    ) %>%
    tidyr::pivot_longer(
      cols = c(
        "absTOnlySubjects",
        "absCOnlySubjects",
        "absBothSubjects"
      ),
      names_to = "subjectsIn",
      values_to = "value"
    ) %>%
    dplyr::mutate(
      subjectsIn = dplyr::recode(
        subjectsIn,
        absTOnlySubjects = "Left cohort only",
        absBothSubjects = "Both cohorts",
        absCOnlySubjects = "Right cohort only"
      )
    )

  plotData$subjectsIn <-
    factor(
      plotData$subjectsIn,
      levels = c("Right cohort only", "Both cohorts", "Left cohort only")
    )

  if (yAxis == "Percentages") {
    position <- "fill"
  } else {
    position <- "stack"
  }

  sortTargetShortName <- plotData %>%
    dplyr::select(targetShortName) %>%
    dplyr::distinct() %>%
    dplyr::arrange(-as.integer(sub(
      pattern = "^C", "", x = targetShortName
    )))

  sortComparatorShortName <- plotData %>%
    dplyr::select(comparatorShortName) %>%
    dplyr::distinct() %>%
    dplyr::arrange(as.integer(sub(
      pattern = "^C", "", x = comparatorShortName
    )))

  plotData <- plotData %>%
    dplyr::arrange(
      targetShortName = factor(targetShortName, levels = sortTargetShortName$targetShortName),
      targetShortName
    ) %>%
    dplyr::arrange(
      comparatorShortName = factor(comparatorShortName, levels = sortComparatorShortName$comparatorShortName),
      comparatorShortName
    )

  plotData$targetShortName <- factor(plotData$targetShortName,
                                     levels = sortTargetShortName$targetShortName
  )

  plotData$comparatorShortName <-
    factor(plotData$comparatorShortName,
           levels = sortComparatorShortName$comparatorShortName
    )

  plot <- ggplot2::ggplot(data = plotData) +
    ggplot2::aes(
      fill = subjectsIn,
      y = targetShortName,
      x = value,
      tooltip = tooltip,
      group = subjectsIn
    ) +
    ggplot2::ylab(label = "") +
    ggplot2::xlab(label = "") +
    ggplot2::scale_fill_manual("Subjects in", values = c(rgb(0.8, 0.2, 0.2), rgb(0.3, 0.2, 0.4), rgb(0.4, 0.4, 0.9))) +
    ggplot2::facet_grid(comparatorShortName ~ databaseName) +
    ggplot2::theme(
      panel.background = ggplot2::element_blank(),
      strip.background = ggplot2::element_blank(),
      panel.grid.major.x = ggplot2::element_line(color = "gray"),
      axis.ticks.y = ggplot2::element_blank(),
      panel.spacing = ggplot2::unit(2, "lines")
    ) +
    ggiraph::geom_bar_interactive(
      position = position,
      alpha = 0.6,
      stat = "identity"
    )
  if (yAxis == "Percentages") {
    plot <- plot + ggplot2::scale_x_continuous(labels = scales::percent)
  } else {
    plot <-
      plot + ggplot2::scale_x_continuous(labels = scales::comma, n.breaks = 3)
  }
  width <- length(unique(plotData$databaseId))
  height <-
    nrow(
      plotData %>%
        dplyr::select(targetShortName, comparatorShortName) %>%
        dplyr::distinct()
    )
  plot <- ggiraph::girafe(
    ggobj = plot,
    options = list(ggiraph::opts_sizing(rescale = TRUE)),
    width_svg = max(12, 2 * width),
    height_svg = max(2, 0.5 * height)
  )
  return(plot)
}


#' Cohort Overlap View
#'
cohortOverlapView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Cohort Overlap (subjects)",
      width = "100%",
      shiny::htmlTemplate(file.path("html", "cohortOverlap.html"))
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
      width = NULL,
      status = "primary",

      shiny::tabsetPanel(
        type = "pills",
        shiny::tabPanel(
          title = "Plot",
          shiny::radioButtons(
            inputId = ns("overlapPlotType"),
            label = "",
            choices = c("Percentages", "Counts"),
            selected = "Percentages",
            inline = TRUE
          ),
          shinycssloaders::withSpinner(ggiraph::ggiraphOutput(ns("overlapPlot"), width = "100%", height = "100%"))
        ),

        shiny::tabPanel(
          title = "Table",
          shiny::fluidRow(
            shiny::column(
              width = 3,
              shiny::checkboxInput(
                inputId = ns("showAsPercentage"),
                label = "Show As Percentage",
                value = TRUE
              )
            ),
            shiny::column(
              width = 3,
              shiny::checkboxInput(
                inputId = ns("showCohortIds"),
                label = "Show Cohort Ids",
                value = TRUE
              )
            )
          ),
          shinycssloaders::withSpinner(
            reactable::reactableOutput(ns("overlapTable"))
          )
        )
      )
    )
  )
}

#' Cohort Overlap Module
#'
#' @requiredPackage shiny
#' @requiredPackage shinydashboard
#' @requiredPackage shinycssloaders
#' @requiredPackage ggiraph
#'
cohortOverlapModule <- function(id,
                                dataSource,
                                selectedCohorts,
                                selectedDatabaseIds,
                                targetCohortId,
                                cohortIds,
                                cohortTable) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI({ selectedCohorts() })

    # Cohort Overlap ------------------------
    cohortOverlapData <- reactive({
      validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      validate(need(length(cohortIds()) > 1, "Please select at least two cohorts."))
      combisOfTargetComparator <- t(utils::combn(cohortIds(), 2)) %>%
        as.data.frame() %>%
        dplyr::tibble()
      colnames(combisOfTargetComparator) <- c("targetCohortId", "comparatorCohortId")


      data <- getResultsCohortOverlap(
        dataSource = dataSource,
        targetCohortIds = combisOfTargetComparator$targetCohortId,
        comparatorCohortIds = combisOfTargetComparator$comparatorCohortId,
        databaseIds = selectedDatabaseIds()
      )
      validate(need(
        !is.null(data),
        paste0("No cohort overlap data for this combination")
      ))
      validate(need(
        nrow(data) > 0,
        paste0("No cohort overlap data for this combination.")
      ))
      return(data)
    })

    output$overlapPlot <- ggiraph::renderggiraph(expr = {
      validate(need(
        length(cohortIds()) > 0,
        paste0("Please select Target Cohort(s)")
      ))

      data <- cohortOverlapData()
      validate(need(
        !is.null(data),
        paste0("No cohort overlap data for this combination")
      ))
      validate(need(
        nrow(data) > 0,
        paste0("No cohort overlap data for this combination.")
      ))

      validate(need(
        !all(is.na(data$eitherSubjects)),
        paste0("No cohort overlap data for this combination.")
      ))

      plot <- plotCohortOverlap(
        data = data,
        shortNameRef = cohortTable,
        yAxis = input$overlapPlotType
      )
      return(plot)
    })


    output$overlapTable <- reactable::renderReactable({
      data <- cohortOverlapData()
      validate(need(
        !is.null(data),
        paste0("No cohort overlap data for this combination")
      ))

      data <- data %>%
        dplyr::inner_join(cohortTable %>% dplyr::select(cohortId,
                                                        targetCohortName = cohortName),
                          by = c("targetCohortId" = "cohortId")) %>%
        dplyr::inner_join(cohortTable %>% dplyr::select(cohortId,
                                                        comparatorCohortName = cohortName),
                          by = c("comparatorCohortId" = "cohortId")) %>%
        dplyr::select(
          databaseName,
          targetCohortId,
          targetCohortName,
          comparatorCohortId,
          comparatorCohortName,
          tOnly = tOnlySubjects,
          cOnly = cOnlySubjects,
          both = bothSubjects,
          totalSubjects = eitherSubjects
        )

      if (input$showCohortIds) {
        data <- data %>% dplyr::mutate(
          targetCohortName = paste0("C", targetCohortId, " - ", targetCohortName),
          comparatorCohortName = paste0("C", comparatorCohortId, " - ", comparatorCohortName)
        )
      }

      data <- data %>% dplyr::select(-targetCohortId, -comparatorCohortId)

      if (input$showAsPercentage) {
        data$tOnly <- data$tOnly / data$totalSubjects
        data$cOnly <- data$cOnly / data$totalSubjects
        data$both <- data$both / data$totalSubjects
      }

      styleFunc <- function(value) {
        color <- '#fff'
        if (input$showAsPercentage) {
          if (is.numeric(value)) {
            value <- ifelse(is.na(value), 0, value)
            color <- pallete(value)
          }
        }
        list(background = color)
      }

      valueColDef <- reactable::colDef(
        cell = formatDataCellValueInDisplayTable(input$showAsPercentage),
        style = styleFunc,
        width = 80
      )
      colnames(data) <- SqlRender::camelCaseToTitleCase(colnames(data))
      reactable::reactable(
        data = data,
        columns = list(
          "T Only" = valueColDef,
          "C Only" = valueColDef,
          "Both" = valueColDef,
          "Target Cohort Name" = reactable::colDef(minWidth = 300),
          "Comparator Cohort Name" = reactable::colDef(minWidth = 300),
          "Total Subjects" = reactable::colDef(cell = formatDataCellValueInDisplayTable(FALSE))
        ),
        sortable = TRUE,
        groupBy = c("Target Cohort Name", "Comparator Cohort Name"),
        resizable = TRUE,
        filterable = TRUE,
        searchable = TRUE,
        pagination = TRUE,
        showPagination = TRUE,
        showPageInfo = TRUE,
        highlight = TRUE,
        striped = TRUE,
        compact = TRUE,
        wrap = TRUE,
        showSortIcon = TRUE,
        showSortable = TRUE,
        fullWidth = TRUE,
        bordered = TRUE,
        onClick = "select",
        showPageSizeOptions = TRUE,
        pageSizeOptions = c(10, 20, 50, 100, 1000),
        defaultPageSize = 20,
        theme = reactable::reactableTheme(
          rowSelectedStyle = list(backgroundColor = "#eee", boxShadow = "inset 2px 0 0 0 #ffa62d")
        )
      )
    })
  })
}
