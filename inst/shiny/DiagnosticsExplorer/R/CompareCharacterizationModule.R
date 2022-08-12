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
      status = "warning",
      width = "100%",
      tags$div(
        style = "max-height: 100px; overflow-y: auto",
        tags$table(
          width = "100%",
          tags$tr(
            tags$td(
              width = "70%",
              tags$b("Cohorts :"),
              shiny::uiOutput(outputId = ns("selectedCohorts"))
            ),
            tags$td(
              style = "align: right !important;", width = "30%",
              tags$b("Database :"),
              shiny::uiOutput(outputId = ns("selectedDatabases"))
            )
          )
        )
      )
    ),
    shinydashboard::box(
      width = NULL,
      title = NULL,
      tags$table(
        tags$tr(
          tags$td(
            shiny::radioButtons(
              inputId = ns("charCompareType"),
              label = "",
              choices = c("Pretty table", "Raw table", "Plot"),
              selected = "Plot",
              inline = TRUE
            ),
          ),
          tags$td(
            shiny::checkboxInput(
              inputId = ns("compareCharacterizationFilterLowValues"),
              label = "Filter low values",
              value = TRUE
            )
          ),
          tags$td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;")),
          tags$td(
            shiny::conditionalPanel(
              condition = "input.charCompareType == 'Raw table'",
              ns = ns,
              shiny::radioButtons(
                inputId = ns("compareCharacterizationColumnFilters"),
                label = "Display",
                choices = c("Mean and Standard Deviation", "Mean only"),
                selected = "Mean only",
                inline = TRUE
              )
            )
          )
        )
      ),
      shiny::conditionalPanel(
        condition = "input.charCompareType == 'Raw table' | input.charCompareType=='Plot'",
        ns = ns,
        tags$table(tags$tr(
          tags$td(
            shinyWidgets::pickerInput(
              inputId = ns("compareCohortCharacterizationAnalysisNameFilter"),
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
            shinyWidgets::pickerInput(
              inputId = ns("compareCohortcharacterizationDomainIdFilter"),
              label = "Domain name",
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
              inputId = ns("compareCharacterizationProportionOrContinuous"),
              label = "",
              choices = c("All", "Proportion", "Continuous"),
              selected = "Proportion",
              inline = TRUE
            )
          )
        ))
      ),
      shiny::conditionalPanel(
        condition = "input.charCompareType=='Pretty table' | input.charCompareType=='Raw table'",
        ns = ns,
        tags$table(
          width = "100%",
          tags$tr(
            tags$td(
              align = "right",
            )
          )
        ),
        shinycssloaders::withSpinner(
          reactable::reactableOutput(ns("compareCohortCharacterizationTable")),
        ),
        csvDownloadButton(ns, "compareCohortCharacterizationTable")
      ),
      shiny::conditionalPanel(
        condition = "input.charCompareType=='Plot'",
        ns = ns,
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
                                                selectedTimeIds,
                                                characterizationOutputMenu,
                                                getFilteredConceptIds,
                                                cohortTable,
                                                databaseTable,
                                                temporalAnalysisRef,
                                                analysisIdInCohortCharacterization,
                                                analysisNameOptions,
                                                domainIdOptions,
                                                characterizationTimeIdChoices,
                                                temporalChoices,
                                                prettyTable1Specifications) {

  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI({
      htmltools::withTags(table(
        tr(td(
          selectedCohort()
        )),
        tr(td(
          selectedComparatorCohort()
        ))
      ))
    })

    output$selectedDatabases <- shiny::renderUI({
      paste(databaseTable %>%
              dplyr::filter(.data$databaseId %in% selectedDatabaseIds()) %>% dplyr::select(.data$databaseName),
            collapse = ", ")
    })

    # Compare cohort characterization --------------------------------------------
    ### compareCohortCharacterizationAnalysisNameFilter -----
    shiny::observe({
      characterizationAnalysisOptionsUniverse <- NULL
      charcterizationAnalysisOptionsSelected <- NULL

      if (hasData(temporalAnalysisRef)) {
        characterizationAnalysisOptionsUniverse <- analysisNameOptions
        charcterizationAnalysisOptionsSelected <-
          temporalAnalysisRef %>%
          dplyr::filter(.data$domainId %in% c('Condition')) %>%
          dplyr::filter(.data$isBinary == 'Y') %>%
          dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
          dplyr::pull(.data$analysisName) %>%
          unique()
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "compareCohortCharacterizationAnalysisNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = characterizationAnalysisOptionsUniverse,
        selected = charcterizationAnalysisOptionsSelected
      )
    })


    ### compareCohortcharacterizationDomainIdFilter -----
    shiny::observe({
      characterizationDomainOptionsUniverse <- NULL
      charcterizationDomainOptionsSelected <- NULL

      if (hasData(temporalAnalysisRef)) {
        characterizationDomainOptionsUniverse <- domainIdOptions
        charcterizationDomainOptionsSelected <-
          temporalAnalysisRef %>%
          dplyr::filter(.data$domainId %in% c('Condition')) %>%
          dplyr::filter(.data$isBinary == 'Y') %>%
          dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
          dplyr::pull(.data$domainId) %>%
          unique()
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "compareCohortcharacterizationDomainIdFilter",
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

      data <- characterizationOutputMenu()
      if (!hasData(data)) {
        return(NULL)
      }

      data <- data$covariateValue
      if (!hasData(data)) {
        return(NULL)
      }
      
      if (isTRUE(input$compareCharacterizationFilterLowValues)) {
        data <- data %>% 
          dplyr::filter(.data$mean > 0.01)
      }
      
      data <- data %>%
        dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
        dplyr::filter(.data$timeId %in% selectedTimeIds()) %>%
        dplyr::filter(.data$cohortId %in% c(targetCohortId(), comparatorCohortId())) %>%
        dplyr::filter(.data$databaseId %in% selectedDatabaseIds())

      if (input$charCompareType == "Raw") {
        if (input$compareCharacterizationProportionOrContinuous == "Proportion") {
          data <- data %>%
            dplyr::filter(.data$isBinary == "Y")
        } else if (input$compareCharacterizationProportionOrContinuous == "Continuous") {
          data <- data %>%
            dplyr::filter(.data$isBinary == "N")
        }
      }

      if (input$compareCharacterizationProportionOrContinuous == "Proportion") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "Y")
      } else if (input$compareCharacterizationProportionOrContinuous == "Continuous") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "N")
      }

      data <- data %>%
        dplyr::filter(.data$analysisName %in% input$compareCohortCharacterizationAnalysisNameFilter) %>%
        dplyr::filter(.data$domainId %in% input$compareCohortcharacterizationDomainIdFilter)

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

      showDataAsPercent <- TRUE

      if (showDataAsPercent) {
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
      } else {
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
            .data$sumValue
          )
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
            .data$sumValue
          )
      }

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
        showDataAsPercent = showDataAsPercent
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
        if (input$compareCharacterizationProportionOrContinuous == "Proportion") {
          showDataAsPercent <- TRUE
        }
      }
      countLocation <- 1

      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(
          data = data,
          string = dataColumnFields
        )

      getDisplayTableGroupedByDatabaseId(
        data = data,
        cohort = cohortTable,
        databaseTable = databaseTable,
        headerCount = NULL,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showDataAsPercent = showDataAsPercent,
        excludedColumnFromPercentage = "StdDiff",
        sort = TRUE,
        isTemporal = TRUE,
        pageSize = 100
      )
    })

    ## output: compareCohortCharacterizationTable ----------------------------------------
    output$compareCohortCharacterizationTable <- reactable::renderReactable(expr = {
      if (input$charCompareType == "Pretty table") {
        data <- compareCohortCharacterizationPrettyTable()
        validate(need(hasData(data), "No data for selected combination"))
        return(data)
      } else if (input$charCompareType == "Raw table") {
        data <- compareCohortCharacterizationRawTable()
        validate(need(hasData(data), "No data for selected combination"))
        return(data)
      }
    })

    ## output: compareCohortCharacterizationBalancePlot ----------------------------------------
    output$compareCohortCharacterizationBalancePlot <-
      ggiraph::renderggiraph(expr = {
        if (!input$charCompareType == "Plot") {
          return(NULL)
        }
        data <- compareCohortCharacterizationBalanceData()
        validate(need(
          hasData(data),
          "No data available for selected combination."
        ))

        distinctTemporalChoices <- unique(temporalChoices$temporalChoices)
        data <- data %>%
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
        validate(need(
          !is.null(plot),
          "No plot available for selected combination."
        ))
        return(plot)
      })
  })
}