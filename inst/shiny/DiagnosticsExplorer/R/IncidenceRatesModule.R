
plotIncidenceRate <- function(data,
                              shortNameRef = NULL,
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
  
  plotData <- data %>%
    addShortName(shortNameRef) %>%
    dplyr::mutate(incidenceRate = round(.data$incidenceRate, digits = 3))
  plotData <- plotData %>%
    dplyr::mutate(
      strataGender = !is.na(.data$gender),
      strataAgeGroup = !is.na(.data$ageGroup),
      strataCalendarYear = !is.na(.data$calendarYear)
    ) %>%
    dplyr::filter(
      .data$strataGender %in% !!stratifyByGender &
        .data$strataAgeGroup %in% !!stratifyByAgeGroup &
        .data$strataCalendarYear %in% !!stratifyByCalendarYear
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
    dplyr::select(.data$shortName) %>%
    dplyr::distinct() %>%
    dplyr::arrange(as.integer(sub(
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

  if (stratifyByAgeGroup) {
    sortAgeGroup <- plotData %>%
      dplyr::select(.data$ageGroup) %>%
      dplyr::distinct() %>%
      dplyr::arrange(as.integer(sub(
        pattern = "-.+$", "", x = .data$ageGroup
      )))

    plotData <- plotData %>%
      dplyr::arrange(
        ageGroup = factor(.data$ageGroup, levels = sortAgeGroup$ageGroup),
        .data$ageGroup
      )

    plotData$ageGroup <- factor(plotData$ageGroup,
      levels = sortAgeGroup$ageGroup
    )
  }

  plotData$tooltip <- c(
    paste0(
      plotData$shortName,
      " ",
      plotData$databaseName,
      " ",
      plotData$databaseId,
      " ",
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
          plot + facet_nested(databaseId + shortName ~ plotData$ageGroup, scales = scales)
      } else {
        plot <-
          plot + facet_nested(databaseId + shortName ~ ., scales = scales)
      }
    } else {
      plot <-
        plot + facet_nested(databaseId + shortName ~ ., scales = scales)
    }
    # spacing <- rep(c(1, rep(0.5, length(unique(plotData$shortName)) - 1)), length(unique(plotData$databaseId)))[-1]
    spacing <- plotData %>%
      dplyr::distinct(.data$databaseId, .data$shortName) %>%
      dplyr::arrange(.data$databaseId) %>%
      dplyr::group_by(.data$databaseId) %>%
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
    1.5 + 1 * nrow(dplyr::distinct(plotData, .data$databaseId, .data$shortName))
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
      status = "warning",
      width = "100%",
      tags$div(
        style = "max-height: 100px; overflow-y: auto",
        shiny::uiOutput(outputId = ns("selectedCohorts"))
      )
    ),
    shinydashboard::box(
      title = "Incidence Rate",
      width = NULL,
      status = "primary",
      htmltools::withTags(
        tags$table(
          style = "width: 100%",
          tags$tr(
            tags$td(
              valign = "bottom",
              shiny::checkboxGroupInput(
                inputId = ns("irStratification"),
                label = "Stratify by",
                choices = c("Age", "Sex", "Calendar Year"),
                selected = c("Age", "Sex", "Calendar Year"),
                inline = TRUE
              )
            ),
            tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;")),
            tags$td(
              valign = "bottom",
              style = "width:30% !important;margin-top:10px;",
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
            ),
            tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;")),
            tags$td(
              valign = "bottom",
              style = "text-align: right",
              shiny::checkboxInput(ns("irYscaleFixed"), "Use same y-scale across databases")
            )
          )
        )
      ),
      htmltools::withTags(
        tags$table(
          width = "100%",
          tags$tr(
            tags$td(
              shiny::conditionalPanel(
                condition = "input.irStratification.indexOf('Age') > -1",
                ns = ns,
                shinyWidgets::pickerInput(
                  inputId = ns("incidenceRateAgeFilter"),
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
            tags$td(
              shiny::conditionalPanel(
                condition = "input.irStratification.indexOf('Sex') > -1",
                ns = ns,
                shinyWidgets::pickerInput(
                  inputId = ns("incidenceRateGenderFilter"),
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
            tags$td(
              style = "width:30% !important",
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
            ),
            tags$td(
              shiny::numericInput(
                inputId = ns("minPersonYear"),
                label = "Minimum person years",
                value = 1000,
                min = 0
              )
            ),
            tags$td(
              shiny::numericInput(
                inputId = ns("minSubjetCount"),
                label = "Minimum subject count",
                value = NULL
              )
            ),
            tags$td(
              align = "right",
              shiny::downloadButton(
                ns("saveIncidenceRatePlot"),
                label = "",
                icon = shiny::icon("download"),
                style = "margin-top: 5px; margin-bottom: 5px;"
              )
            )
          )
        )
      ),
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
}

incidenceRatesModule <- function(id,
                                 dataSource,
                                 selectedCohorts,
                                 selectedDatabaseIds,
                                 cohortIds,
                                 cohortTable) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
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
            .data$incidenceRate < 0 ~ 0,
            TRUE ~ .data$incidenceRate
          ))
      } else {
        data <- NULL
      }
      return(data)
    })

    shiny::observe({
      if (!is.null(incidenceRateData()) &&
        nrow(incidenceRateData()) > 0) {
        ageFilter <- incidenceRateData() %>%
          dplyr::select(.data$ageGroup) %>%
          dplyr::filter(.data$ageGroup != "NA", !is.na(.data$ageGroup)) %>%
          dplyr::distinct() %>%
          dplyr::mutate(lowerValue = as.integer(sub(
            pattern = "-.+$", "", x = .data$ageGroup
          ))) %>% 
          dplyr::arrange(.data$lowerValue)

        shinyWidgets::updatePickerInput(
          session = session,
          inputId = "incidenceRateAgeFilter",
          selected = ageFilter$ageGroup,
          choices = ageFilter %>% 
            dplyr::filter(.data$lowerValue >= 0) %>% 
            dplyr::filter(.data$lowerValue < 80) %>% 
            dplyr::pull(.data$ageGroup),
          choicesOpt = list(style = rep_len("color: black;", 999))
        )
      }
    })

    shiny::observe({
      if (!is.null(incidenceRateData()) &&
        nrow(incidenceRateData()) > 0) {
        genderFilter <- incidenceRateData() %>%
          dplyr::select(.data$gender) %>%
          dplyr::filter(
            .data$gender != "NA",
            !is.na(.data$gender)
          ) %>%
          dplyr::distinct() %>%
          dplyr::arrange(.data$gender)
        
        shinyWidgets::updatePickerInput(
          session = session,
          inputId = "incidenceRateGenderFilter",
          choicesOpt = list(style = rep_len("color: black;", 999)),
          choices = genderFilter$gender,
          selected = c('Female', 'Male')
        )
      }
    })

    shiny::observe({
      if (!is.null(incidenceRateData()) &&
        nrow(incidenceRateData()) > 0) {
        calenderFilter <- incidenceRateData() %>%
          dplyr::select(.data$calendarYear) %>%
          dplyr::filter(
            .data$calendarYear != "NA",
            !is.na(.data$calendarYear)
          ) %>%
          dplyr::distinct(.data$calendarYear) %>%
          dplyr::arrange(.data$calendarYear)

        minValue <- 0
        maxValue <- 99999
        if (hasData(calenderFilter$calendarYear)) {
          minValue <- min(calenderFilter$calendarYear)
        }
        if (hasData(calenderFilter$calendarYear)) {
          maxValue <- max(calenderFilter$calendarYear)
        }

        shiny::updateSliderInput(
          session = session,
          inputId = "incidenceRateCalenderFilter",
          min = minValue,
          max = maxValue,
          value = c(2010, maxValue)
        )

        minIncidenceRateValue <- round(min(incidenceRateData()$incidenceRate), digits = 2)

        maxIncidenceRateValue <- round(max(incidenceRateData()$incidenceRate), digits = 2)

        shiny::updateSliderInput(
          session = session,
          inputId = "YscaleMinAndMax",
          min = 0,
          max = maxIncidenceRateValue,
          value = c(minIncidenceRateValue, maxIncidenceRateValue),
          step = round((maxIncidenceRateValue - minIncidenceRateValue) / 5, digits = 2)
        )
      }
    })

    incidenceRateCalenderFilter <- shiny::reactive({
      calenderFilter <- incidenceRateData() %>%
        dplyr::select(.data$calendarYear) %>%
        dplyr::filter(
          .data$calendarYear != "NA",
          !is.na(.data$calendarYear)
        ) %>%
        dplyr::distinct(.data$calendarYear) %>%
        dplyr::arrange(.data$calendarYear)
      calenderFilter <-
        calenderFilter[calenderFilter$calendarYear >= input$incidenceRateCalenderFilter[1] &
                         calenderFilter$calendarYear <= input$incidenceRateCalenderFilter[2], , drop = FALSE] %>%
          dplyr::pull(.data$calendarYear)
      return(calenderFilter)
    })


    incidenceRateYScaleFilter <- shiny::reactive({
      incidenceRateFilter <- incidenceRateData() %>%
        dplyr::select(.data$incidenceRate) %>%
        dplyr::filter(
          .data$incidenceRate != "NA",
          !is.na(.data$incidenceRate)
        ) %>%
        dplyr::distinct(.data$incidenceRate) %>%
        dplyr::arrange(.data$incidenceRate)
      incidenceRateFilter <-
        incidenceRateFilter[incidenceRateFilter$incidenceRate >= input$YscaleMinAndMax[1] &
                              incidenceRateFilter$incidenceRate <= input$YscaleMinAndMax[2], , drop = FALSE] %>%
          dplyr::pull(.data$incidenceRate)
      return(incidenceRateFilter)
    })

    output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
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
            dplyr::filter(.data$ageGroup %in% input$incidenceRateAgeFilter)
        }
        if (stratifyByGender &&
          !"All" %in% input$incidenceRateGenderFilter) {
          data <- data %>%
            dplyr::filter(.data$gender %in% input$incidenceRateGenderFilter)
        }
        if (stratifyByCalendarYear) {
          data <- data %>%
            dplyr::filter(.data$calendarYear %in% incidenceRateCalenderFilter())
        }
        if (input$irYscaleFixed) {
          data <- data %>%
            dplyr::filter(.data$incidenceRate %in% incidenceRateYScaleFilter())
        }
        if (all(!is.null(data), nrow(data) > 0)) {
          plot <- plotIncidenceRate(
            data = data,
            shortNameRef = cohortTable,
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

  })
}