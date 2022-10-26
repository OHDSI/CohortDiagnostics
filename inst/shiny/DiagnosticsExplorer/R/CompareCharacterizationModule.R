plotTemporalCompareStandardizedDifference <- function(balance,
                                                      shortNameRef = NULL,
                                                      xLimitMin = 0,
                                                      xLimitMax = 1,
                                                      yLimitMin = 0,
                                                      yLimitMax = 1,
                                                      domain = "all") {
  domains <-
    c(
      "Condition",
      "Device",
      "Drug",
      "Measurement",
      "Observation",
      "Procedure",
      "Demographics"
    )

  balance$domainId[!balance$domainId %in% domains] <- "Other"
  if (domain != "all") {
    balance <- balance %>%
      dplyr::filter(.data$domainId == !!domain)
  }
  validate(need((nrow(balance) > 0), paste0("No data for selected combination.")))

  # Can't make sense of plot with > 1000 dots anyway, so remove
  # anything with small mean in both target and comparator:
  if (nrow(balance) > 1000) {
    balance <- balance %>%
      dplyr::filter(.data$mean1 > 0.01 | .data$mean2 > 0.01)
  }
  if (nrow(balance) > 1000) {
    balance <- balance %>%
      dplyr::filter(.data$sumValue1 > 0 & .data$sumValue2 > 0)
  }

  balance <- balance %>%
    addShortName(
      shortNameRef = shortNameRef,
      cohortIdColumn = "cohortId1",
      shortNameColumn = "targetCohort"
    ) %>%
    addShortName(
      shortNameRef = shortNameRef,
      cohortIdColumn = "cohortId2",
      shortNameColumn = "comparatorCohort"
    )

  # ggiraph::geom_point_interactive(ggplot2::aes(tooltip = tooltip), size = 3, alpha = 0.6)
  balance$tooltip <-
    c(
      paste0(
        "Covariate Name: ",
        balance$covariateName,
        "\nDomain: ",
        balance$domainId,
        "\nAnalysis: ",
        balance$analysisName,
        "\nY ",
        balance$comparatorCohort,
        ": ",
        scales::comma(balance$mean2, accuracy = 0.01),
        "\nX ",
        balance$targetCohort,
        ": ",
        scales::comma(balance$mean1, accuracy = 0.01),
        "\nStd diff.:",
        scales::comma(balance$stdDiff, accuracy = 0.01)
      )
    )

  # Code used to generate palette:
  # writeLines(paste(RColorBrewer::brewer.pal(n = length(domains), name = "Dark2"), collapse = "\", \""))

  # Make sure colors are consistent, no matter which domains are included:
  colors <-
    c(
      "#1B9E77",
      "#D95F02",
      "#7570B3",
      "#E7298A",
      "#66A61E",
      "#E6AB02",
      "#444444"
    )
  colors <-
    colors[c(domains, "Other") %in% unique(balance$domainId)]

  balance$domainId <-
    factor(balance$domainId, levels = c(domains, "Other"))

  # targetLabel <- paste(strwrap(targetLabel, width = 50), collapse = "\n")
  # comparatorLabel <- paste(strwrap(comparatorLabel, width = 50), collapse = "\n")

  xCohort <- balance %>%
    dplyr::distinct(balance$targetCohort) %>%
    dplyr::pull()
  yCohort <- balance %>%
    dplyr::distinct(balance$comparatorCohort) %>%
    dplyr::pull()

  if (nrow(balance) == 0) {
    return(NULL)
  }

  plot <-
    ggplot2::ggplot(
      balance,
      ggplot2::aes(
        x = .data$mean1,
        y = .data$mean2,
        color = .data$domainId
      )
    ) +
      ggiraph::geom_point_interactive(
        ggplot2::aes(tooltip = .data$tooltip),
        size = 3,
        shape = 16,
        alpha = 0.5
      ) +
      ggplot2::geom_abline(
        slope = 1,
        intercept = 0,
        linetype = "dashed"
      ) +
      ggplot2::geom_hline(yintercept = 0) +
      ggplot2::geom_vline(xintercept = 0) +
      # ggplot2::scale_x_continuous("Mean") +
      # ggplot2::scale_y_continuous("Mean") +
      ggplot2::xlab(paste("Covariate Mean in Target Cohort")) +
      ggplot2::ylab(paste("Covariate Mean in Comparator Cohort")) +
      ggplot2::scale_color_manual("Domain", values = colors) +
      ggplot2::facet_grid(cols = ggplot2::vars(temporalChoices)) + # need to facet by 'startDay' that way it is arranged in numeric order.
      # but labels should be based on choices
      # ggplot2::facet_wrap(~temporalChoices) +
      ggplot2::theme(
        strip.background = ggplot2::element_blank(),
        panel.spacing = ggplot2::unit(2, "lines")
      ) +
      ggplot2::xlim(xLimitMin, xLimitMax) +
      ggplot2::ylim(yLimitMin, yLimitMax)

  numberOfTimeIds <- balance$timeId %>%
    unique() %>%
    length()

  plot <- ggiraph::girafe(
    ggobj = plot,
    options = list(ggiraph::opts_sizing(rescale = TRUE)),
    width_svg = max(8, 3 * numberOfTimeIds),
    height_svg = 3
  )
  return(plot)
}

compareCohortCharacterizationView <- function(id, title = "Compare cohort characterization") {
  ns <- shiny::NS(id)

  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Compare Cohort Characterization",
      width = "100%",
      shiny::htmlTemplate(file.path("html", "compareCohortCharacterization.html"))
    ),
    shinydashboard::box(
      width = NULL,
      title = title,
      shiny::fluidRow(
        shiny::column(
          width = 3,
          shinyWidgets::pickerInput(
            inputId = ns("targetCohort"),
            label = "Target Cohort",
            choices = NULL,
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
        shiny::column(
          width = 3,
          shinyWidgets::pickerInput(
            inputId = ns("targetDatabase"),
            label = "Target Database",
            choices = NULL,
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
        shiny::column(
          width = 3,
          shinyWidgets::pickerInput(
            inputId = ns("comparatorCohort"),
            label = "Comparator Cohort",
            choices = NULL,
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
        shiny::column(
          width = 3,
          shinyWidgets::pickerInput(
            inputId = ns("comparatorDatabase"),
            label = "Comparator Database",
            choices = NULL,
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
      ),
      shiny::fluidRow(
        shiny::column(
          width = 4,
          shinyWidgets::pickerInput(
            inputId = ns("timeIdChoices"),
            label = "Temporal Window (s)",
            choices = NULL,
            multiple = TRUE,
            choicesOpt = list(style = rep_len("color: black;", 999)),
            selected = NULL,
            options = shinyWidgets::pickerOptions(
              actionsBox = TRUE,
              liveSearch = TRUE,
              maxOptions = 5, # Selecting even this many will be slow
              size = 10,
              liveSearchStyle = "contains",
              liveSearchPlaceholder = "Type here to search",
              virtualScroll = 50
            )
          )
        ),
        shiny::column(
          width = 4,
          shinyWidgets::pickerInput(
            inputId = ns("analysisNameFilter"),
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
        shiny::column(
          width = 4,
          shinyWidgets::pickerInput(
            inputId = ns("domainIdFilter"),
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
      ),
      shiny::fluidRow(
        shiny::column(
          width = 3,
          shiny::numericInput(
            inputId = ns("minMeanFilterVal"),
            label = "Min Covariate Mean",
            value = 0.005,
            min = 0.0,
            max = 0.9,
            step = 0.005
          )
        )
      ),
      shiny::actionButton(label = "Generate Report", inputId = ns("generatePlot"))
    ),
    shiny::conditionalPanel(
      condition = "input.generatePlot != 0",
      ns = ns,
      shiny::uiOutput(ns("selectionsPlot")),
      shinydashboard::box(
        width = NULL,
        status = "primary",
        shiny::tabsetPanel(
          type = "pills",
          shiny::tabPanel(
            title = "Plot",
            shinycssloaders::withSpinner(
              ggiraph::ggiraphOutput(
                outputId = ns("compareCohortCharacterizationBalancePlot"),
                width = "100%",
                height = "100%"
              )
            )
          ),
          shiny::tabPanel(
            title = "Raw Table",
            shiny::fluidRow(
              shiny::column(
                width = 3,
                shiny::radioButtons(
                  inputId = ns("proportionOrContinuous"),
                  label = "Covariate Type",
                  choices = c("All", "Proportion", "Continuous"),
                  selected = "Proportion",
                  inline = TRUE
                )
              ),
              shiny::column(
                width = 3,
                shiny::radioButtons(
                  inputId = ns("compareCharacterizationColumnFilters"),
                  label = "Display values",
                  choices = c("Mean", "Mean and Standard Deviation"),
                  selected = "Mean",
                  inline = TRUE
                ),
                shiny::checkboxInput(
                  inputId = ns("showOnlyMutualCovariates"),
                  label = "Show only covariates found in target and comparator",
                  value = FALSE
                )
              ),
              shiny::column(
                width = 4,
                shinyWidgets::pickerInput(
                  inputId = ns("timeIdChoicesSingle"),
                  label = "Temporal Window",
                  choices = NULL,
                  multiple = FALSE,
                  choicesOpt = list(style = rep_len("color: black;", 999)),
                  selected = NULL,
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
            ),
            shinycssloaders::withSpinner(
              reactable::reactableOutput(ns("compareCohortCharacterizationTable")),
            ),
            csvDownloadButton(ns, "compareCohortCharacterizationTable")
          )
        )
      )
    )
  )
}


compareCohortCharacterizationModule <- function(id,
                                                dataSource,
                                                cohortTable,
                                                databaseTable,
                                                conceptSets,
                                                temporalAnalysisRef,
                                                analysisNameOptions,
                                                domainIdOptions,
                                                temporalChoices) {


  shiny::moduleServer(id, function(input, output, session) {
    # Temporal choices (e.g. -30d - 0d ) are dynamic to execution
    timeIdOptions <- getResultsTemporalTimeRef(dataSource = dataSource) %>%
      dplyr::arrange(.data$sequence)
    shiny::observe({
      # Default time windows
      selectedTimeWindows <- timeIdOptions %>%
        dplyr::filter(.data$primaryTimeId == 1) %>%
        dplyr::filter(.data$isTemporal == 1) %>%
        dplyr::arrange(.data$sequence) %>%
        dplyr::pull("temporalChoices")

      shinyWidgets::updatePickerInput(session,
                                      inputId = "timeIdChoices",
                                      choices = timeIdOptions$temporalChoices,
                                      selected = selectedTimeWindows)

      shinyWidgets::updatePickerInput(session,
                                      inputId = "timeIdChoicesSingle",
                                      choices = timeIdOptions$temporalChoices)

      cohortChoices <- cohortTable$cohortId
      names(cohortChoices) <- cohortTable$cohortName
      shinyWidgets::updatePickerInput(session,
                                      inputId = "targetCohort",
                                      choices = cohortChoices)

      shinyWidgets::updatePickerInput(session,
                                      inputId = "comparatorCohort",
                                      choices = cohortChoices)


      databaseChoices <- databaseTable$databaseId
      names(databaseChoices) <- databaseTable$databaseName
      shinyWidgets::updatePickerInput(session,
                                      inputId = "targetDatabase",
                                      choices = databaseChoices)

      shinyWidgets::updatePickerInput(session,
                                      inputId = "comparatorDatabase",
                                      choices = databaseChoices)

    })

    selectedTimeIds <- shiny::reactive({
      timeIdOptions %>%
        dplyr::filter(.data$temporalChoices %in% input$timeIdChoices) %>%
        dplyr::select(.data$timeId) %>%
        dplyr::pull()
    })

    selectedTimeIdsSingle <- shiny::reactive({
      timeIdOptions %>%
        dplyr::filter(.data$temporalChoices %in% input$timeIdChoicesSingle) %>%
        dplyr::select(.data$timeId) %>%
        dplyr::pull()
    })

    targetCohortId <- shiny::reactive({
      as.integer(input$targetCohort)
    })

    comparatorCohortId <- shiny::reactive({
      as.integer(input$comparatorCohort)
    })

    selectedDatabaseIds <- shiny::reactive({
      c(input$targetDatabase, input$comparatorDatabase)
    })
    temporalCharacterizationOutput <-
      shiny::reactive(x = {

        data <- getCharacterizationOutput(
          dataSource = dataSource,
          cohortIds = c(targetCohortId(), comparatorCohortId()),
          databaseIds = selectedDatabaseIds(),
          temporalCovariateValueDist = FALSE,
          meanThreshold = input$minMeanFilterVal
        )

        return(data)
      })

    compareCharacterizationOutput <-
      shiny::reactive(x = {
        data <- temporalCharacterizationOutput()
        if (!hasData(data)) {
          return(NULL)
        }
        return(data)
      })


    # Compare cohort characterization --------------------------------------------
    ### analysisNameFilter -----
    shiny::observe({
      characterizationAnalysisOptionsUniverse <- NULL
      charcterizationAnalysisOptionsSelected <- NULL

      if (hasData(temporalAnalysisRef)) {
        characterizationAnalysisOptionsUniverse <- analysisNameOptions
        charcterizationAnalysisOptionsSelected <-
          temporalAnalysisRef %>%
            dplyr::pull(.data$analysisName) %>%
            unique()
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "analysisNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = characterizationAnalysisOptionsUniverse,
        selected = charcterizationAnalysisOptionsSelected
      )
    })


    ### domainIdFilter -----
    shiny::observe({
      characterizationDomainOptionsUniverse <- NULL
      charcterizationDomainOptionsSelected <- NULL

      if (hasData(temporalAnalysisRef)) {
        characterizationDomainOptionsUniverse <- domainIdOptions
        charcterizationDomainOptionsSelected <-
          temporalAnalysisRef %>%
            dplyr::pull(.data$domainId) %>%
            unique()
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "domainIdFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = characterizationDomainOptionsUniverse,
        selected = charcterizationDomainOptionsSelected
      )
    })

    ## compareCohortCharacterizationDataFiltered ------------
    compareCohortCharacterizationDataFiltered <- shiny::reactive({
      validate(need(length(targetCohortId()) == 1, "One target cohort must be selected"))
      validate(need(
        length(comparatorCohortId()) == 1,
        "One comparator cohort must be selected"
      ))
      validate(
        need(
          (targetCohortId() != comparatorCohortId()) | (input$comparatorDatabase != input$targetDatabase),
          "Target and comparator cohorts/database cannot be the same"
        )
      )

      data <- compareCharacterizationOutput()
      if (!hasData(data)) {
        return(NULL)
      }

      data <- data$covariateValue
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::filter(.data$cohortId %in% c(targetCohortId(), comparatorCohortId())) %>%
        dplyr::filter(.data$databaseId %in% selectedDatabaseIds())

      data <- data %>%
        dplyr::filter(.data$analysisName %in% input$analysisNameFilter) %>%
        dplyr::filter(.data$domainId %in% input$domainIdFilter)

      if (!hasData(data)) {
        return(NULL)
      }
      return(data)
    })

    ## compareCohortCharacterizationBalanceData ----------------------------------------
    compareCohortCharacterizationBalanceData <- shiny::reactive({
      data <- compareCohortCharacterizationDataFiltered()
      if (!hasData(data)) {
        return(NULL)
      }
      covs1 <- data %>%
        dplyr::filter(.data$cohortId == targetCohortId(),
                      .data$databaseId == input$targetDatabase)
      if (!hasData(covs1)) {
        return(NULL)
      }
      covs2 <- data %>%
        dplyr::filter(.data$cohortId == comparatorCohortId(),
                      .data$databaseId == input$comparatorDatabase)
      if (!hasData(covs2)) {
        return(NULL)
      }

      return(compareCohortCharacteristics(covs1, covs2))
    })

    rawTableBaseData <- shiny::eventReactive(input$generatePlot, {
      data <- compareCohortCharacterizationBalanceData()
      if (!hasData(data)) {
        return(NULL)
      }
      return(data)
    })


    ## compareCohortCharacterizationRawTable ----------------------------------------
    compareCohortCharacterizationRawTable <- shiny::reactive({
      data <- rawTableBaseData()
      validate(need(hasData(data), "No data available for selected combination."))
      distinctTemporalChoices <- unique(temporalChoices$temporalChoices)
      sortedTemporalChoices <- data %>%
        dplyr::arrange(factor(.data$temporalChoices, levels = distinctTemporalChoices)) %>%
        dplyr::distinct(.data$temporalChoices) %>%
        dplyr::pull(.data$temporalChoices)

      data <- data %>%
        dplyr::arrange(factor(.data$temporalChoices, levels = sortedTemporalChoices))

      data <- data %>%
        dplyr::filter(.data$timeId == selectedTimeIdsSingle())

      showAsPercent <- FALSE
      if (input$proportionOrContinuous == "Proportion") {
        showAsPercent <- TRUE
        data <- data %>%
          dplyr::filter(.data$isBinary == "Y")
      } else if (input$proportionOrContinuous == "Continuous") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "N")
      }

      data <- data %>%
        dplyr::rename(
          "target" = .data$mean1,
          "sdT" = .data$sd1,
          "comparator" = .data$mean2,
          "sdC" = .data$sd2,
          "StdDiff" = .data$absStdDiff
        )

      if (input$compareCharacterizationColumnFilters == "Mean and Standard Deviation") {
        data <- data %>%
          dplyr::select(.data$covariateName,
                        .data$analysisName,
                        .data$conceptId,
                        .data$target,
                        .data$sdT,
                        .data$comparator,
                        .data$sdC,
                        .data$StdDiff)
      } else {
        data <- data %>%
          dplyr::select(.data$covariateName,
                        .data$analysisName,
                        .data$conceptId,
                        .data$target,
                        .data$comparator,
                        .data$StdDiff)
      }

      # Covariates where stdDiff is NA or NULL
      if (input$showOnlyMutualCovariates) {
        data <- data %>% dplyr::filter(!is.na(.data$StdDiff),
                                       !is.null(.data$StdDiff))
      }

      reactable::reactable(
        data = data,
        columns = list(
          target = reactable::colDef(
            cell = formatDataCellValueInDisplayTable(showDataAsPercent = showAsPercent),
            na = ""
          ),
          comparator = reactable::colDef(
            cell = formatDataCellValueInDisplayTable(showDataAsPercent = showAsPercent),
            na = ""
          ),
          StdDiff = reactable::colDef(
            cell = function(value) {
              return(round(value,2))
            },
            style = function(value) {
              color <- '#fff'
              if (is.numeric(value) & hasData(data$StdDiff)) {
                value <- ifelse(is.na(value), min(data$StdDiff, na.rm = TRUE), value)
                normalized <- (value - min(data$StdDiff, na.rm = TRUE)) / (max(data$StdDiff, na.rm = TRUE) - min(data$StdDiff, na.rm = TRUE))
                color <- pallete(normalized)
              }
              list(background = color)
            },
            na = ""
          ),
          covariateName = reactable::colDef(name = "Covariate Name", minWidth = 500),
          analysisName = reactable::colDef(name = "Analysis Name"),
          conceptId = reactable::colDef(name = "Concept Id")
        ),
        sortable = TRUE,
        resizable = TRUE,
        filterable = TRUE,
        searchable = TRUE,
        pagination = TRUE,
        showPagination = TRUE,
        showPageInfo = TRUE,
        highlight = TRUE,
        striped = TRUE,
        compact = TRUE,
        wrap = FALSE,
        showSortIcon = TRUE,
        showSortable = TRUE,
        fullWidth = TRUE,
        bordered = TRUE,
        showPageSizeOptions = TRUE,
        pageSizeOptions = c(10, 20, 50, 100, 1000),
        defaultPageSize = 100,
        selection = NULL,
        theme = reactable::reactableTheme(
          rowSelectedStyle = list(backgroundColor = "#eee", boxShadow = "inset 2px 0 0 0 #ffa62d")
        )
      )

    })

    selectionsOutput <- shiny::reactive({

      target <- paste(cohortTable %>%
                        dplyr::filter(.data$cohortId == targetCohortId()) %>%
                        dplyr::select(.data$cohortName) %>%
                        dplyr::pull(),
                      collapse = ", ")
      comparator <- paste(cohortTable %>%
                            dplyr::filter(.data$cohortId == comparatorCohortId()) %>%
                            dplyr::select(.data$cohortName) %>%
                            dplyr::pull(),
                          collapse = ", ")


      shinydashboard::box(
        status = "warning",
        width = "100%",
        shiny::fluidRow(
          shiny::column(
            width = 7,
            tags$b("Target Cohort :"), paste0(target, " C", targetCohortId()),
            tags$br(),
            tags$b("Comparator Cohort :"), paste0(comparator, " C", comparatorCohortId())
          ),
          shiny::column(
            width = 5,
            tags$b("Target Database :"),
            paste(databaseTable %>%
                    dplyr::filter(.data$databaseId == input$targetDatabase) %>%
                    dplyr::select(.data$databaseName) %>%
                    dplyr::pull(),
                  collapse = ", "),
            tags$br(),
            tags$b("Comparator Database :"),
            paste(databaseTable %>%
                    dplyr::filter(.data$databaseId == input$comparatorDatabase) %>%
                    dplyr::select(.data$databaseName) %>%
                    dplyr::pull(),
                  collapse = ", ")
          )
        )
      )
    })

    generateTable <- shiny::reactive({
      data <- compareCohortCharacterizationRawTable()
      validate(need(hasData(data), "No data for selected combination"))
      return(data)
    })

    ## output: compareCohortCharacterizationTable ----------------------------------------
    output$compareCohortCharacterizationTable <- reactable::renderReactable(expr = {
      generateTable()
    })

    generatePlot <- shiny::eventReactive(input$generatePlot, {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = "Getting plot data",
        value = 0
      )

      data <- compareCohortCharacterizationBalanceData()
      validate(need(
        hasData(data),
        "No data available for selected combination."
      ))

      progress$set(
        message = "Plotting results",
        value = 50
      )
      distinctTemporalChoices <- unique(temporalChoices$temporalChoices)

      data <- data %>%
        dplyr::filter(.data$timeId %in% selectedTimeIds(),
                      !is.na(.data$stdDiff)) %>%
        dplyr::arrange(factor(.data$temporalChoices, levels = distinctTemporalChoices)) %>%
        dplyr::mutate(temporalChoices = factor(.data$temporalChoices, levels = unique(.data$temporalChoices)))

      plot <-
        plotTemporalCompareStandardizedDifference(
          balance = data,
          shortNameRef = cohortTable,
          xLimitMin = 0,
          xLimitMax = 1,
          yLimitMin = 0,
          yLimitMax = 1
        )

      progress$set(
        message = "Returning data",
        value = 90
      )
      validate(need(
        !is.null(plot),
        "No plot available for selected combination."
      ))
      return(plot)
    })

    selectionsOutputPlot <- shiny::eventReactive(input$generatePlot, {
      selectionsOutput()
    })

    output$selectionsPlot <- shiny::renderUI({
      selectionsOutputPlot()
    })

    ## output: compareCohortCharacterizationBalancePlot ----------------------------------------
    output$compareCohortCharacterizationBalancePlot <-
      ggiraph::renderggiraph(expr = {
        generatePlot()
      })
  })
}
