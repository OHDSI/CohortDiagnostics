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
          dplyr::arrange(as.integer(sub(
            pattern = "-.+$", "", x = .data$ageGroup
          )))

        shinyWidgets::updatePickerInput(
          session = session,
          inputId = "incidenceRateAgeFilter",
          selected = ageFilter$ageGroup,
          choices = ageFilter$ageGroup,
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
          selected = genderFilter$gender
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

        minValue <- min(calenderFilter$calendarYear)

        maxValue <- max(calenderFilter$calendarYear)

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