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
      ggplot2::xlab(paste("Covariate Mean in ", xCohort)) +
      ggplot2::ylab(paste("Covariate Mean in ", yCohort)) +
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

compareCohortCharacterizationView <- function(id) {
  ns <- shiny::NS(id)

  shiny::tagList(
    shinydashboard::box(
      width = NULL,
      title = NULL,
      shiny::conditionalPanel(
        condition = "output.showTemporalChoices == 'TRUE' & input.charCompareType == 'Plot'",
        ns = ns,
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
        ),
      ),
      shiny::conditionalPanel(
        condition = "input.charCompareType != 'Plot'",
        ns = ns,
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
        ),
      ),

      shiny::radioButtons(
        inputId = ns("charCompareType"),
        label = "Output type",
        choices = c("Pretty table", "Raw table", "Plot"),
        selected = "Plot",
        inline = TRUE
      ),
      shiny::conditionalPanel(
        condition = "input.charCompareType == 'Raw table' | input.charCompareType=='Plot'",
        ns = ns,
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
        ),
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
        ),

        shiny::conditionalPanel(
          condition = "input.charCompareType=='Raw table'",
          ns = ns,
          shiny::radioButtons(
            inputId = ns("compareCharacterizationColumnFilters"),
            label = "Display values",
            choices = c("Mean", "Mean and Standard Deviation"),
            selected = "Mean",
            inline = TRUE
          )
        ),

        shiny::radioButtons(
          inputId = ns("proportionOrContinuous"),
          label = "Covariate Type",
          choices = c("All", "Proportion", "Continuous"),
          selected = "Proportion",
          inline = TRUE
        )
      ),
      shiny::conditionalPanel(
        condition = "input.charCompareType=='Raw table'",
        ns = ns,
        shiny::actionButton(label = "Generate Table", inputId = ns("generateTable"))
      ),
      shiny::conditionalPanel(
        condition = "input.charCompareType=='Pretty table'",
        ns = ns,
        shiny::actionButton(label = "Generate Table", inputId = ns("generatePrettyTable"))
      ),
      shiny::conditionalPanel(
        condition = "input.charCompareType=='Plot'",
        ns = ns,
        shiny::actionButton(label = "Generate Plot", inputId = ns("generatePlot"))
      )
    ),
    shiny::conditionalPanel(
      condition = "input.generateTable != 0 & input.charCompareType=='Raw table'",
      ns = ns,
      shiny::uiOutput(ns("selectionsRawTable")),
      shinydashboard::box(
        width = NULL,
        title = NULL,
        shinycssloaders::withSpinner(
          reactable::reactableOutput(ns("compareCohortCharacterizationTable")),
        ),
        csvDownloadButton(ns, "compareCohortCharacterizationTable")
      )
    ),
    shiny::conditionalPanel(
      condition = "input.generatePrettyTable != 0 & input.charCompareType=='Pretty table'",
      ns = ns,
      shiny::uiOutput(ns("selectionsPrettyTable")),
      shinydashboard::box(
        width = NULL,
        title = NULL,
        shinycssloaders::withSpinner(
          reactable::reactableOutput(ns("compareCohortCharacterizationPrettyTable")),
        ),
        csvDownloadButton(ns, "compareCohortCharacterizationPrettyTable")
      )
    ),
    shiny::conditionalPanel(
      condition = "input.generatePlot != 0 & input.charCompareType=='Plot'",
      ns = ns,
      shiny::uiOutput(ns("selectionsPlot")),
      shinydashboard::box(
        title = "Compare Cohort Characterization",
        width = NULL,
        status = "primary",
        shiny::htmlOutput(ns("compareCohortCharacterizationSelectedCohort")),
        shinycssloaders::withSpinner(
          ggiraph::ggiraphOutput(
            outputId = ns("compareCohortCharacterizationBalancePlot"),
            width = "100%",
            height = "100%"
          )
        )
      )
    )
  )
}


compareCohortCharacterizationModule <- function(id,
                                                dataSource,
                                                selectedCohort,
                                                selectedDatabaseIds,
                                                targetCohortId,
                                                comparatorCohortId,
                                                selectedComparatorCohort,
                                                selectedConceptSets,
                                                getFilteredConceptIds,
                                                cohortTable,
                                                databaseTable,
                                                temporalAnalysisRef,
                                                showTemporalChoices,
                                                analysisNameOptions,
                                                domainIdOptions,
                                                temporalChoices,
                                                prettyTable1Specifications) {


  shiny::moduleServer(id, function(input, output, session) {

    output$showTemporalChoices <- shiny::renderText({ showTemporalChoices })
    shiny::outputOptions(output, "showTemporalChoices", suspendWhenHidden = FALSE)
    if (showTemporalChoices) {
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

      })
      selectedTimeIds <- shiny::reactive({
        timeIdOptions %>%
          dplyr::filter(.data$temporalChoices %in% input$timeIdChoices) %>%
          dplyr::select(.data$timeId) %>%
          dplyr::pull()
      })
    } else {
      timeIdOptions <- getResultsTemporalTimeRef(dataSource = dataSource) %>%
        dplyr::arrange(.data$sequence) %>%
        dplyr::filter(.data$isTemporal == 0) %>%
        dplyr::filter(.data$primaryTimeId == 1) %>%
        dplyr::arrange(.data$sequence)

      selectedTimeIds <- shiny::reactive({
        timeIdOptions %>%
          dplyr::select(.data$timeId) %>%
          dplyr::pull()
      })

      shinyWidgets::updatePickerInput(session,
                                      inputId = "timeIdChoicesSingle",
                                      choices = timeIdOptions$temporalChoices)
    }

    selectedTimeIdsSingle <- shiny::reactive({
      timeIdOptions %>%
        dplyr::filter(.data$temporalChoices %in% input$timeIdChoicesSingle) %>%
        dplyr::select(.data$timeId) %>%
        dplyr::pull()
    })

    temporalCharacterizationOutput <-
      shiny::reactive(x = {
        data <- getCharacterizationOutput(
          dataSource = dataSource,
          cohortIds = c(targetCohortId(), comparatorCohortId()),
          databaseIds = selectedDatabaseIds(),
          temporalCovariateValueDist = FALSE
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
      validate(need(length(selectedDatabaseIds()) == 1, "One data source must be selected"))
      validate(need(length(targetCohortId()) == 1, "One target cohort must be selected"))
      validate(need(
        length(comparatorCohortId()) == 1,
        "One comparator cohort must be selected"
      ))
      validate(
        need(
          targetCohortId() != comparatorCohortId(),
          "Target and comparator cohorts cannot be the same"
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

      if (input$charCompareType == "Raw") {
        if (input$proportionOrContinuous == "Proportion") {
          data <- data %>%
            dplyr::filter(.data$isBinary == "Y")
        } else if (input$proportionOrContinuous == "Continuous") {
          data <- data %>%
            dplyr::filter(.data$isBinary == "N")
        }
      }

      data <- data %>%
        dplyr::filter(.data$analysisName %in% input$analysisNameFilter) %>%
        dplyr::filter(.data$domainId %in% input$domainIdFilter)

      if (hasData(selectedConceptSets())) {
        if (hasData(getFilteredConceptIds())) {
          data <- data %>%
            dplyr::filter(.data$conceptId %in% getFilteredConceptIds())
        }
      }
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
        dplyr::filter(.data$cohortId %in% c(targetCohortId()))
      if (!hasData(covs1)) {
        return(NULL)
      }
      covs2 <- data %>%
        dplyr::filter(.data$cohortId %in% c(comparatorCohortId()))
      if (!hasData(covs2)) {
        return(NULL)
      }

      balance <- compareCohortCharacteristics(covs1, covs2)
      return(balance)
    })

    ## compareCohortCharacterizationPrettyTable ----------------------------------------
    compareCohortCharacterizationPrettyTable <- shiny::reactive(x = {
      if (!input$charCompareType == "Pretty table") {
        return(NULL)
      }
      data <- compareCohortCharacterizationBalanceData()
      if (!hasData(data)) {
        return(NULL)
      }

      data <- data %>%
        dplyr::filter(.data$timeId %in% selectedTimeIdsSingle())

      data1 <- data %>%
        dplyr::rename(
          "cohortId" = .data$cohortId1,
          "mean" = .data$mean1,
          "sumValue" = .data$sumValue1
        ) %>%
        dplyr::select(
          .data$cohortId,
          .data$databaseId,
          .data$analysisId,
          .data$covariateId,
          .data$covariateName,
          .data$mean
        ) %>%
        dplyr::rename(sumValue = .data$mean)

      data2 <- data %>%
        dplyr::rename(
          "cohortId" = .data$cohortId2,
          "mean" = .data$mean2,
          "sumValue" = .data$sumValue2
        ) %>%
        dplyr::select(
          .data$cohortId,
          .data$databaseId,
          .data$analysisId,
          .data$covariateId,
          .data$covariateName,
          .data$mean
        ) %>%
        dplyr::rename(sumValue = .data$mean)


      data1 <-
        prepareTable1(
          covariates = data1,
          prettyTable1Specifications = prettyTable1Specifications,
          cohort = cohort
        )

      data2 <-
        prepareTable1(
          covariates = data2,
          prettyTable1Specifications = prettyTable1Specifications,
          cohort = cohort
        )

      data <- data1 %>%
        dplyr::full_join(data2,
                         by = c(
                           "characteristic",
                           "sequence",
                           "databaseId"
                         )
        ) %>%
        dplyr::arrange(.data$databaseId, .data$sequence) %>%
        dplyr::select(-.data$databaseId)

      if (!hasData(data)) {
        return(NULL)
      }
      keyColumns <- c("characteristic")
      dataColumns <- intersect(
        x = colnames(data),
        y = cohort$shortName
      )

      table <- getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns,
        showDataAsPercent = TRUE
      )
      return(table)
    })

    ## compareCohortCharacterizationRawTable ----------------------------------------
    compareCohortCharacterizationRawTable <- shiny::reactive(x = {
      if (!input$charCompareType == "Raw table") {
        return(NULL)
      }
      data <- compareCohortCharacterizationBalanceData()
      if (!hasData(data)) {
        return(NULL)
      }

      distinctTemporalChoices <- unique(temporalChoices$temporalChoices)
      sortedTemporalChoices <- data %>%
        dplyr::arrange(factor(.data$temporalChoices, levels = distinctTemporalChoices)) %>%
        dplyr::distinct(.data$temporalChoices) %>%
        dplyr::pull(.data$temporalChoices)

      data <- data %>%
        dplyr::arrange(factor(.data$temporalChoices, levels = sortedTemporalChoices))

      data <- data %>%
        dplyr::filter(.data$timeId %in% selectedTimeIdsSingle())

      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = "Post processing: Rendering table",
        value = 0
      )
      data <- data %>%
        dplyr::rename(
          "target" = mean1,
          "sdT" = sd1,
          "comparator" = mean2,
          "sdC" = sd2,
          "StdDiff" = absStdDiff
        )

      keyColumnFields <-
        c("covariateName", "analysisName", "conceptId")

      showDataAsPercent <- FALSE
      if (input$compareCharacterizationColumnFilters == "Mean and Standard Deviation") {
        dataColumnFields <-
          c(
            "target",
            "sdT",
            "comparator",
            "sdC",
            "StdDiff"
          )
      } else {
        dataColumnFields <- c("target", "comparator", "StdDiff")
        if (input$proportionOrContinuous == "Proportion") {
          showDataAsPercent <- TRUE
        }
      }
      getDisplayTableGroupedByDatabaseId(
        data = data,
        cohort = cohortTable,
        databaseTable = databaseTable,
        headerCount = NULL,
        keyColumns = keyColumnFields,
        countLocation = 1,
        dataColumns = dataColumnFields,
        maxCount = 1,
        showDataAsPercent = showDataAsPercent,
        excludedColumnFromPercentage = "StdDiff",
        sort = TRUE,
        isTemporal = TRUE,
        pageSize = 100
      )
    })

    selectionsOutput <- shiny::reactive({
      shinydashboard::box(
        status = "warning",
        width = "100%",
        shiny::fluidRow(
          shiny::column(
            width = 9,
            tags$b("Cohorts :"),
            paste(cohortTable %>%
                    dplyr::filter(.data$cohortId %in% c(targetCohortId(), comparatorCohortId()) %>%
                                    dplyr::select(.data$cohortName) %>%
                                    dplyr::pull(),
                                  collapse = ", ")
            ),
            shiny::column(
              width = 3,
              tags$b("Database :"),
              paste(databaseTable %>%
                      dplyr::filter(.data$databaseId %in% selectedDatabaseIds()) %>%
                      dplyr::select(.data$databaseName) %>%
                      dplyr::pull(),
                    collapse = ", ")
            )
          )
        )
      )
    })

    selectionsOutputRaw <- shiny::eventReactive(input$generateTable, {
      selectionsOutput()
    })

    output$selectionsRawTable <- shiny::renderUI({
      selectionsOutputRaw()
    })

    generateTable <- shiny::eventReactive(input$generateTable, {
      data <- compareCohortCharacterizationRawTable()
      validate(need(hasData(data), "No data for selected combination"))
      return(data)
    })

    ## output: compareCohortCharacterizationTable ----------------------------------------
    output$compareCohortCharacterizationTable <- reactable::renderReactable(expr = {
      generateTable()
    })

    generatePrettyTable <- shiny::eventReactive(input$generatePrettyTable, {
      data <- compareCohortCharacterizationPrettyTable()
      validate(need(hasData(data), "No data for selected combination"))
      return(data)
    })

    selectionsOutputPretty <- shiny::eventReactive(input$generatePrettyTable, {
      selectionsOutput()
    })

    output$selectionsPrettyTable <- shiny::renderUI({
      selectionsOutputPretty()
    })

    ## output: compareCohortCharacterizationTable ----------------------------------------
    output$compareCohortCharacterizationPrettyTable <- reactable::renderReactable(expr = {
      generatePrettyTable()
    })

    generatePlot <- shiny::eventReactive(input$generatePlot, {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = "Getting plot data",
        value = 0
      )

      if (!input$charCompareType == "Plot") {
        return(NULL)
      }
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
        dplyr::filter(.data$timeId %in% selectedTimeIds()) %>%
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