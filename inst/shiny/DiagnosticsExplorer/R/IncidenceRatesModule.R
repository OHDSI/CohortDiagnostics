plotIncidenceRate <- function(data,
                              cohortTable = NULL,
                              stratifyByAgeGroup = TRUE,
                              stratifyByGender = TRUE,
                              stratifyByCalendarYear = TRUE,
                              yscaleFixed = FALSE) {
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(
    x = data,
    any.missing = TRUE,
    min.rows = 1,
    min.cols = 5,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = stratifyByAgeGroup,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = stratifyByGender,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = stratifyByCalendarYear,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = yscaleFixed,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertDouble(
    x = data$incidenceRate,
    lower = 0,
    any.missing = FALSE,
    null.ok = FALSE,
    min.len = 1,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  checkmate::assertDouble(
    x = data$incidenceRate,
    lower = 0,
    any.missing = FALSE,
    null.ok = FALSE,
    min.len = 1,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)

  cohortNames <- cohortTable %>% dplyr::select(cohortId,
                                               cohortName)

  plotData <- data %>%
    dplyr::inner_join(cohortNames, by = "cohortId",) %>%
    addShortName(cohortTable) %>%
    dplyr::mutate(incidenceRate = round(incidenceRate, digits = 3))
  plotData <- plotData %>%
    dplyr::mutate(
      strataGender = !is.na(gender),
      strataAgeGroup = !is.na(ageGroup),
      strataCalendarYear = !is.na(calendarYear)
    ) %>%
    dplyr::filter(
      strataGender %in% !!stratifyByGender &
        strataAgeGroup %in% !!stratifyByAgeGroup &
        strataCalendarYear %in% !!stratifyByCalendarYear
    ) %>%
    dplyr::select(-dplyr::starts_with("strata"))

  aesthetics <- list(y = "incidenceRate")
  if (stratifyByCalendarYear) {
    aesthetics$x <- "calendarYear"
    xLabel <- "Calender year"
    showX <- TRUE
    if (stratifyByGender) {
      aesthetics$group <- "gender"
      aesthetics$color <- "gender"
    }
    plotType <- "line"
  } else {
    xLabel <- ""
    if (stratifyByGender) {
      aesthetics$x <- "gender"
      aesthetics$color <- "gender"
      aesthetics$fill <- "gender"
      showX <- TRUE
    } else if (stratifyByAgeGroup) {
      aesthetics$x <- "ageGroup"
      showX <- TRUE
    } else {
      aesthetics$x <- 1
      showX <- FALSE
    }
    plotType <- "bar"
  }


  sortShortName <- plotData %>%
    dplyr::select(shortName) %>%
    dplyr::distinct() %>%
    dplyr::arrange(as.integer(sub(
      pattern = "^C", "", x = shortName
    )))

  plotData <- plotData %>%
    dplyr::arrange(
      shortName = factor(shortName, levels = sortShortName$shortName),
      shortName
    )


  plotData$shortName <- factor(plotData$shortName,
                               levels = sortShortName$shortName)

  if (stratifyByAgeGroup) {
    sortAgeGroup <- plotData %>%
      dplyr::select(ageGroup) %>%
      dplyr::distinct() %>%
      dplyr::arrange(as.integer(sub(
        pattern = "-.+$", "", x = ageGroup
      )))

    plotData <- plotData %>%
      dplyr::arrange(
        ageGroup = factor(ageGroup, levels = sortAgeGroup$ageGroup),
        ageGroup
      )

    plotData$ageGroup <- factor(plotData$ageGroup,
                                levels = sortAgeGroup$ageGroup
    )
  }

  plotData$tooltip <- c(
    paste0(
      plotData$cohortName,
      "\n",
      plotData$databaseName,
      "\nIncidence Rate = ",
      scales::comma(plotData$incidenceRate, accuracy = 0.01),
      "/per 1k PY",
      "\nIncidence Proportion = ",
      scales::percent(plotData$cohortCount / plotData$cohortSubjects, accuracy = 0.1),
      "\nPerson years = ",
      scales::comma(plotData$personYears, accuracy = 0.01),
      "\nCohort count = ",
      scales::comma(plotData$cohortSubjects, accuracy = 1),
      "\nCount = ",
      paste0(scales::comma(plotData$cohortCount, accuracy = 1))
    )
  )

  if (stratifyByAgeGroup) {
    plotData$tooltip <-
      c(paste0(plotData$tooltip, "\nAge Group = ", plotData$ageGroup))
  }

  if (stratifyByGender) {
    plotData$tooltip <-
      c(paste0(plotData$tooltip, "\nSex = ", plotData$gender))
  }

  if (stratifyByCalendarYear) {
    plotData$tooltip <-
      c(paste0(plotData$tooltip, "\nYear = ", plotData$calendarYear))
  }

  if (stratifyByGender) {
    # Make sure colors are consistent, no matter which genders are included:
    genders <- c("Female", "Male", "No matching concept")
    # Code used to generate palette:
    # writeLines(paste(RColorBrewer::brewer.pal(n = 2, name = "Dark2"), collapse = "\", \""))
    colors <- c("#D95F02", "#1B9E77", "#444444")
    colors <- colors[genders %in% unique(plotData$gender)]
    plotData$gender <- factor(plotData$gender, levels = genders)
  }


  plot <-
    ggplot2::ggplot(data = plotData, do.call(ggplot2::aes_string, aesthetics)) +
      ggplot2::xlab(xLabel) +
      ggplot2::ylab("Incidence Rate (/1,000 person years)") +
      ggplot2::scale_y_continuous(expand = c(0, 0))

  if (stratifyByCalendarYear) {
    distinctCalenderYear <- plotData$calendarYear %>%
      unique() %>%
      sort()
    if (all(!is.na(distinctCalenderYear))) {
      if (length(distinctCalenderYear) >= 8) {
        plot <-
          plot + ggplot2::scale_x_continuous(n.breaks = 8, labels = round)
      } else {
        plot <-
          plot + ggplot2::scale_x_continuous(breaks = distinctCalenderYear)
      }
    }
  }


  plot <- plot + ggplot2::theme(
    legend.position = "top",
    legend.title = ggplot2::element_blank(),
    axis.text.x = if (showX) {
      ggplot2::element_text(angle = 90, vjust = 0.5)
    } else {
      ggplot2::element_blank()
    }
  )

  if (plotType == "line") {
    plot <- plot +
      ggiraph::geom_line_interactive(ggplot2::aes(), size = 1, alpha = 0.6) +
      ggiraph::geom_point_interactive(ggplot2::aes(tooltip = tooltip),
                                      size = 2,
                                      alpha = 0.6
      )
  } else {
    plot <-
      plot + ggiraph::geom_col_interactive(ggplot2::aes(tooltip = tooltip), alpha = 0.6)
  }
  if (stratifyByGender) {
    plot <- plot + ggplot2::scale_color_manual(values = colors)
    plot <- plot + ggplot2::scale_fill_manual(values = colors)
  }
  # databaseId field only present when called in Shiny app:
  if (!is.null(data$databaseId) && length(data$databaseId) > 1) {
    if (yscaleFixed) {
      scales <- "fixed"
    } else {
      scales <- "free_y"
    }
    if (stratifyByGender | stratifyByCalendarYear) {
      if (stratifyByAgeGroup) {
        plot <-
          plot + ggh4x::facet_nested(databaseName + shortName ~ plotData$ageGroup, scales = scales)
      } else {
        plot <-
          plot + ggh4x::facet_nested(databaseName + shortName ~ ., scales = scales)
      }
    } else {
      plot <-
        plot + ggh4x::facet_nested(databaseName + shortName ~ ., scales = scales)
    }
    # spacing <- rep(c(1, rep(0.5, length(unique(plotData$shortName)) - 1)), length(unique(plotData$databaseId)))[-1]
    spacing <- plotData %>%
      dplyr::distinct(databaseId, shortName) %>%
      dplyr::arrange(databaseId) %>%
      dplyr::group_by(databaseId) %>%
      dplyr::summarise(count = dplyr::n(), .groups = "keep") %>%
      dplyr::ungroup()
    spacing <-
      unlist(sapply(spacing$count, function(x) {
        c(1, rep(0.5, x - 1))
      }))[-1]

    if (length(spacing) > 0) {
      plot <-
        plot + ggplot2::theme(
          panel.spacing.y = ggplot2::unit(spacing, "lines"),
          strip.background = ggplot2::element_blank()
        )
    } else {
      plot <-
        plot + ggplot2::theme(strip.background = ggplot2::element_blank())
    }
  } else {
    if (stratifyByAgeGroup) {
      plot <- plot + ggplot2::facet_grid(~ageGroup)
    }
  }
  height <-
    1.5 + 1 * nrow(dplyr::distinct(plotData, databaseId, shortName))
  plot <- ggiraph::girafe(
    ggobj = plot,
    options = list(
      ggiraph::opts_sizing(width = .7),
      ggiraph::opts_zoom(max = 5)
    ),
    width_svg = 15,
    height_svg = height
  )
  return(plot)
}

incidenceRatesView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Incidence Rates",
      width = "100%",
      shiny::htmlTemplate(file.path("html", "incidenceRate.html"))
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

      shiny::fluidRow(
        shiny::column(
          width = 4,
          shiny::checkboxGroupInput(
            inputId = ns("irStratification"),
            label = "Stratify by",
            choices = c("Age", "Sex", "Calendar Year"),
            selected = c("Age", "Sex", "Calendar Year"),
            inline = TRUE
          )
        ),
        shiny::column(
          width = 3,
          tags$br(),
          shiny::checkboxInput(
            inputId = ns("irYscaleFixed"),
            label = "Use same y-scale across databases"),
        ),
        shiny::column(
          width = 5,
          shiny::conditionalPanel(
            condition = "input.irYscaleFixed",
            ns = ns,
            shiny::sliderInput(
              inputId = ns("YscaleMinAndMax"),
              label = "Limit y-scale range to:",
              min = c(0),
              max = c(0),
              value = c(0, 0),
              dragRange = TRUE, width = 400,
              step = 1,
              sep = "",
            )
          )
        )
      ),
      shiny::fluidRow(
        shiny::conditionalPanel(
          condition = "input.irStratification.indexOf('Age') > -1",
          ns = ns,
          shiny::column(
            width = 6,
            shinyWidgets::pickerInput(
              inputId = ns("incidenceRateAgeFilter"),
              label = "Filter By Age",
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
        shiny::conditionalPanel(
          condition = "input.irStratification.indexOf('Sex') > -1",
          ns = ns,
          shiny::column(
            width = 6,
            shinyWidgets::pickerInput(
              inputId = ns("incidenceRateGenderFilter"),
              label = "Filter By Sex",
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
        )
      ),

      shiny::fluidRow(
         shiny::column(
          width = 3,
          shiny::numericInput(
            inputId = ns("minPersonYear"),
            label = "Minimum person years",
            value = 1000,
            min = 0
          )
        ),
        shiny::column(
          width = 3,
          shiny::numericInput(
            inputId = ns("minSubjetCount"),
            label = "Minimum subject count",
            value = NULL
          )
        ),
        shiny::column(
          width = 6,
          shiny::conditionalPanel(
            condition = "input.irStratification.indexOf('Calendar Year') > -1",
            ns = ns,
            shiny::sliderInput(
              inputId = ns("incidenceRateCalenderFilter"),
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
        )
      ),
      shiny::actionButton(inputId = ns("generatePlot"), label = "Generate Plot")
    ),
    shiny::conditionalPanel(
      ns = ns,
      condition = "input.generatePlot > 0",
      shinydashboard::box(
        width = NULL,
        shiny::htmlOutput(outputId = ns("hoverInfoIr")),
        shinycssloaders::withSpinner(
          ggiraph::ggiraphOutput(
            outputId = ns("incidenceRatePlot"),
            width = "100%",
            height = "100%"
          )
        )
      )
    )
  )
}

incidenceRatesModule <- function(id,
                                 dataSource,
                                 selectedCohorts,
                                 selectedDatabaseIds,
                                 cohortIds,
                                 cohortTable) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    irRanges <- getIncidenceRateRanges(dataSource)
    output$selectedCohorts <- shiny::renderUI({ selectedCohorts() })

    # Incidence rate ---------------------------

    incidenceRateData <- reactive({
      validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
      stratifyByAge <- "Age" %in% input$irStratification
      stratifyByGender <- "Sex" %in% input$irStratification
      stratifyByCalendarYear <-
        "Calendar Year" %in% input$irStratification
      if (length(cohortIds()) > 0) {
        data <- getIncidenceRateResult(
          dataSource = dataSource,
          cohortIds = cohortIds(),
          databaseIds = selectedDatabaseIds(),
          stratifyByGender = stratifyByGender,
          stratifyByAgeGroup = stratifyByAge,
          stratifyByCalendarYear = stratifyByCalendarYear,
          minPersonYears = input$minPersonYear,
          minSubjectCount = input$minSubjetCount
        ) %>%
          dplyr::mutate(incidenceRate = dplyr::case_when(
            incidenceRate < 0 ~ 0,
            TRUE ~ incidenceRate
          ))
      } else {
        data <- NULL
      }
      return(data)
    })

    shiny::observe({
      ageFilter <- irRanges$ageGroups %>%
        dplyr::filter(ageGroup != " ", ageGroup != "NA", !is.na(ageGroup)) %>%
        dplyr::distinct() %>%
        dplyr::arrange(as.integer(sub(
          pattern = "-.+$", "", x = ageGroup
        )))

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "incidenceRateAgeFilter",
        selected = ageFilter$ageGroup,
        choices = ageFilter$ageGroup,
        choicesOpt = list(style = rep_len("color: black;", 999))
      )

    })

    shiny::observe({
      genderFilter <- irRanges$gender %>%
        dplyr::select(gender) %>%
        dplyr::filter(
          gender != "NA",
          gender != " ",
          !is.na(gender),
          !is.null(gender)
        ) %>%
        dplyr::distinct() %>%
        dplyr::arrange(gender)

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "incidenceRateGenderFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = genderFilter$gender,
        selected = genderFilter$gender
      )

    })

    shiny::observe({
      calenderFilter <- irRanges$calendarYear %>%
        dplyr::select(calendarYear) %>%
        dplyr::filter(
          calendarYear != " ",
          calendarYear != "NA",
          !is.na(calendarYear)
        ) %>%
        dplyr::distinct(calendarYear) %>%
        dplyr::arrange(calendarYear)

      minValue <- min(calenderFilter$calendarYear)

      maxValue <- max(calenderFilter$calendarYear)

      shiny::updateSliderInput(
        session = session,
        inputId = "incidenceRateCalenderFilter",
        min = minValue,
        max = maxValue,
        value = c(2010, maxValue)
      )
    })

    shiny::observe({
      minIncidenceRateValue <- round(min(irRanges$incidenceRate$minIr), digits = 2)
      maxIncidenceRateValue <- round(max(irRanges$incidenceRate$maxIr), digits = 2)
      shiny::updateSliderInput(
        session = session,
        inputId = "YscaleMinAndMax",
        min = 0,
        max = maxIncidenceRateValue,
        value = c(minIncidenceRateValue, maxIncidenceRateValue),
        step = round((maxIncidenceRateValue - minIncidenceRateValue) / 5, digits = 2)
      )
    })

    incidenceRateCalenderFilter <- shiny::reactive({
      calenderFilter <- incidenceRateData() %>%
        dplyr::select(calendarYear) %>%
        dplyr::filter(
          calendarYear != "NA",
          !is.na(calendarYear)
        ) %>%
        dplyr::distinct(calendarYear) %>%
        dplyr::arrange(calendarYear)
      calenderFilter <-
        calenderFilter[calenderFilter$calendarYear >= input$incidenceRateCalenderFilter[1] &
                         calenderFilter$calendarYear <= input$incidenceRateCalenderFilter[2], , drop = FALSE] %>%
          dplyr::pull(calendarYear)
      return(calenderFilter)
    })


    incidenceRateYScaleFilter <- shiny::reactive({
      incidenceRateFilter <- incidenceRateData() %>%
        dplyr::select(incidenceRate) %>%
        dplyr::filter(
          incidenceRate != "NA",
          !is.na(incidenceRate)
        ) %>%
        dplyr::distinct(incidenceRate) %>%
        dplyr::arrange(incidenceRate)
      incidenceRateFilter <-
        incidenceRateFilter[incidenceRateFilter$incidenceRate >= input$YscaleMinAndMax[1] &
                              incidenceRateFilter$incidenceRate <= input$YscaleMinAndMax[2], , drop = FALSE] %>%
          dplyr::pull(incidenceRate)
      return(incidenceRateFilter)
    })

    getIrPlot <- shiny::eventReactive(input$generatePlot, {
      validate(need(length(selectedDatabaseIds()) > 0, "No data sources chosen"))
      validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
      stratifyByAge <- "Age" %in% input$irStratification
      stratifyByGender <- "Sex" %in% input$irStratification
      stratifyByCalendarYear <-
        "Calendar Year" %in% input$irStratification
      shiny::withProgress(
        message = paste(
          "Building incidence rate plot data for ",
          length(cohortIds()),
          " cohorts and ",
          length(selectedDatabaseIds()),
          " databases"
        ),
      {
        data <- incidenceRateData()

        validate(need(all(!is.null(data), nrow(data) > 0), paste0("No data for this combination")))

        if (stratifyByAge && !"All" %in% input$incidenceRateAgeFilter) {
          data <- data %>%
            dplyr::filter(ageGroup %in% input$incidenceRateAgeFilter)
        }
        if (stratifyByGender &&
          !"All" %in% input$incidenceRateGenderFilter) {
          data <- data %>%
            dplyr::filter(gender %in% input$incidenceRateGenderFilter)
        }
        if (stratifyByCalendarYear) {
          data <- data %>%
            dplyr::filter(calendarYear %in% incidenceRateCalenderFilter())
        }
        if (input$irYscaleFixed) {
          data <- data %>%
            dplyr::filter(incidenceRate %in% incidenceRateYScaleFilter())
        }
        if (all(!is.null(data), nrow(data) > 0)) {
          plot <- plotIncidenceRate(
            data = data,
            cohortTable = cohortTable,
            stratifyByAgeGroup = stratifyByAge,
            stratifyByGender = stratifyByGender,
            stratifyByCalendarYear = stratifyByCalendarYear,
            yscaleFixed = input$irYscaleFixed
          )
          return(plot)
        }
      },
        detail = "Please Wait"
      )

    })

    output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
      getIrPlot()
    })

  })
}
