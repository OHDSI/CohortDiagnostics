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

compareCohortCharacteristics <-
  function(characteristics1, characteristics2) {
    characteristics1Renamed <- characteristics1 %>%
      dplyr::rename(
        "sumValue1" = "sumValue",
        "mean1" = "mean",
        "sd1" = "sd",
        "cohortId1" = "cohortId"
      )
    cohortId1Value <- characteristics1Renamed$cohortId1 %>% unique()
    if (length(cohortId1Value) > 1) {
      stop("Can only compare one target cohort id to one comparator cohort id")
    }

    characteristics2Renamed <- characteristics2 %>%
      dplyr::rename(
        "sumValue2" = "sumValue",
        "mean2" = "mean",
        "sd2" = "sd",
        "cohortId2" = "cohortId"
      )
    cohortId2Value <- characteristics2Renamed$cohortId2 %>% unique()
    if (length(cohortId2Value) > 1) {
      stop("Can only compare one target cohort id to one comparator cohort id")
    }

    characteristics <- characteristics1Renamed %>%
      dplyr::full_join(
        characteristics2Renamed,
        na_matches = c("na"),
        by = c(
          "timeId",
          "startDay",
          "endDay",
          "temporalChoices",
          "analysisId",
          "covariateId",
          "covariateName",
          "isBinary",
          "conceptId",
          "analysisName",
          "domainId"
        )
      ) %>%
      dplyr::mutate(
        mean2 = ifelse(is.na(.data$mean2), 0, .data$mean2),
        sd2 = ifelse(is.na(.data$sd2), 0, .data$sd2),
        sd1 = ifelse(is.na(.data$sd1), 0, .data$sd1),
        mean1 = ifelse(is.na(.data$mean1), 0, .data$mean1),
      ) %>%
      dplyr::mutate(
        sdd = sqrt(.data$sd1^2 + .data$sd2^2)
      )

    characteristics$stdDiff <- (characteristics$mean1 - characteristics$mean2) / characteristics$sdd

    characteristics <- characteristics %>%
      dplyr::arrange(-abs(.data$stdDiff)) %>%
      dplyr::mutate(stdDiff = dplyr::na_if(.data$stdDiff, 0)) %>%
      dplyr::mutate(
        absStdDiff = abs(.data$stdDiff),
        cohortId1 = !!cohortId1Value,
        cohortId2 = !!cohortId2Value,
      )

    return(characteristics)
  }


plotTemporalCompareStandardizedDifference <- function(balance,
                                                      shortNameRef = NULL,
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
  shiny::validate(shiny::need((nrow(balance) > 0), paste0("No data for selected combination.")))

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

  balance$domainId <-
    factor(balance$domainId, levels = c(domains, "Other"))

  if (nrow(balance) == 0) {
    return(NULL)
  }

  subplots <- list()
  annotations <- list()
  titles <- balance$temporalChoices %>% unique()
  nsuplots <- length(titles)
  for (timeChoice in titles) {
    dt <- balance %>% dplyr::filter(.data$temporalChoices == timeChoice)

    # Read as - display the plot title 50% along the way of this sub plot - x pos will vary depending on n plots
    xTitlePos <- (length(annotations) / nsuplots) + 1 / nsuplots * 0.5
    annotations[[length(annotations) + 1]] <- list(text = timeChoice,
                                                   showarrow = FALSE,
                                                   x = xTitlePos,
                                                   y = 1.0,
                                                   xref = "paper",
                                                   yref = "paper",
                                                   xanchor = "center",
                                                   yanchor = "bottom")

    subplots[[length(subplots) + 1]] <- plotly::plot_ly(
      data = dt,
      type = 'scatter',
      mode = 'markers',
      x = ~mean1,
      y = ~mean2,
      color = ~domainId,
      colors = colors,
      text = ~tooltip,
      marker = list(opacity = 0.7),
      hovertemplate = "%{text}"
    ) %>%
      plotly::layout(
        plot_bgcolor = '#e5ecf6',
        xaxis = list(tickformat = ".0%", zerolinecolor = '#fff',
                     zerolinewidth = 2,
                     title = "",
                     gridcolor = '#fff'),
        yaxis = list(tickformat = ".0%", zerolinecolor = '#fff',
                     zerolinewidth = 2,
                     gridcolor = '#fff'),
        showlegend = F,
        shapes = list(list(
          type = "line",
          x0 = 0,
          x1 = 1,
          xref = "x",
          y0 = 0,
          y1 = 1,
          yref = "y",
          line = list(color = "black", dash = "dot")
        ))
      )
  }
  annotations[[length(annotations) + 1]] <- list(text = "Prevalance in Target Cohort", font = list(size = 13),
                                                 y = -0.1,
                                                 x = 0.5,
                                                 xref = "paper",
                                                 yref = "paper",
                                                 xanchor = "center",
                                                 yanchor = "bottom",
                                                 showarrow = FALSE)


  plotly::subplot(subplots, nrows = 1, shareX = FALSE, shareY = FALSE) %>%
    plotly::layout(annotations = annotations,
                   xaxis = list(title = ""),
                   yaxis = list(title = "Prevalence in Comparator Cohort"))
}


#' compare characterization view
#' @description
#' Use for customizing UI
#'
#' @param id    Namespace Id - use namespaced id ns("compareCohortCharacterization") inside diagnosticsExplorer module
#' @param title     Optional string title field
#' @family CohortDiagnostics
#' @export
compareCohortCharacterizationView <- function(id, title = "Compare cohort characterization") {
  ns <- shiny::NS(id)

  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Compare Cohort Characterization",
      width = "100%",
      shiny::htmlTemplate(system.file("cohort-diagnostics-www", "compareCohortCharacterization.html", package = utils::packageName()))
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
              plotly::plotlyOutput(
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
            reactableCsvDownloadButton(ns, "compareCohortCharacterizationTable")
          )
        )
      )
    )
  )
}

getCharacterizationOutput <- function(dataSource,
                                      cohortIds,
                                      analysisIds = NULL,
                                      databaseIds,
                                      startDay = NULL,
                                      endDay = NULL,
                                      temporalCovariateValue = TRUE,
                                      temporalCovariateValueDist = TRUE,
                                      meanThreshold = 0.005) {
  temporalChoices <-
    getResultsTemporalTimeRef(dataSource = dataSource)

  covariateValue <- queryResultCovariateValue(
    dataSource = dataSource,
    cohortIds = cohortIds,
    analysisIds = analysisIds,
    databaseIds = databaseIds,
    startDay = startDay,
    endDay = endDay,
    temporalCovariateValue = temporalCovariateValue,
    temporalCovariateValueDist = temporalCovariateValueDist,
    meanThreshold = meanThreshold
  )

  postProcessCharacterizationValue <- function(data) {
    if ("timeId" %in% colnames(data$temporalCovariateValue)) {
      data$temporalCovariateValue$timeId <- NULL
    }
    resultCovariateValue <- data$temporalCovariateValue %>%
      dplyr::arrange(
        .data$cohortId,
        .data$databaseId,
        .data$covariateId
      ) %>%
      dplyr::inner_join(data$temporalCovariateRef,
                        by = "covariateId"
      ) %>%
      dplyr::inner_join(data$temporalAnalysisRef,
                        by = "analysisId"
      ) %>%
      dplyr::left_join(
        temporalChoices %>%
          dplyr::select(
            "startDay",
            "endDay",
            "timeId",
            "temporalChoices"
          ),
        by = c("startDay", "endDay")
      ) %>%
      dplyr::relocate(
        "cohortId",
        "databaseId",
        "timeId",
        "startDay",
        "endDay",
        "temporalChoices",
        "analysisId",
        "covariateId",
        "covariateName",
        "isBinary"
      )

    if ("missingMeansZero" %in% colnames(resultCovariateValue)) {
      resultCovariateValue <- resultCovariateValue %>%
        dplyr::mutate(mean = dplyr::if_else(
          is.na(.data$mean) &
            !is.na(.data$missingMeansZero) &
            .data$missingMeansZero == "Y",
          0,
          .data$mean
        )) %>%
        dplyr::select(-"missingMeansZero")
    }
    resultCovariateValue <- resultCovariateValue %>%
      dplyr::mutate(
        covariateName = stringr::str_replace_all(
          string = .data$covariateName,
          pattern = "^.*: ",
          replacement = ""
        )
      ) %>%
      dplyr::mutate(covariateName = stringr::str_to_sentence(string = .data$covariateName))

    if (!hasData(resultCovariateValue)) {
      return(NULL)
    }
    return(resultCovariateValue)
  }

  resultCovariateValue <- NULL
  if ("temporalCovariateValue" %in% names(covariateValue) &&
    hasData(covariateValue$temporalCovariateValue)) {
    resultCovariateValue <-
      postProcessCharacterizationValue(data = covariateValue)
  }

  resultCovariateValueDist <- NULL

  temporalCovariateValue <- NULL
  temporalCovariateValueDist <- NULL

  if (hasData(resultCovariateValue)) {
    temporalCovariateValue <- dplyr::bind_rows(
      temporalCovariateValue,
      resultCovariateValue
    )
  }

  if (hasData(resultCovariateValueDist)) {
    temporalCovariateValueDist <-
      dplyr::bind_rows(
        temporalCovariateValueDist,
        resultCovariateValueDist
      )
  }

  return(
    list(
      covariateValue = temporalCovariateValue,
      covariateValueDist = temporalCovariateValueDist
    )
  )
}


compareCohortCharacterizationModule <- function(id,
                                                dataSource,
                                                cohortTable = dataSource$cohortTable,
                                                databaseTable = dataSource$dbTable,
                                                temporalAnalysisRef = dataSource$temporalAnalysisRef,
                                                analysisNameOptions = dataSource$analysisNameOptions,
                                                domainIdOptions = dataSource$domainIdOptions,
                                                temporalChoices = dataSource$temporalChoices) {


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
        dplyr::select("timeId") %>%
        dplyr::pull()
    })

    selectedTimeIdsSingle <- shiny::reactive({
      timeIdOptions %>%
        dplyr::filter(.data$temporalChoices %in% input$timeIdChoicesSingle) %>%
        dplyr::select("timeId") %>%
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
            dplyr::pull("analysisName") %>%
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
            dplyr::pull("domainId") %>%
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
      shiny::validate(shiny::need(length(targetCohortId()) == 1, "One target cohort must be selected"))
      shiny::validate(shiny::need(
        length(comparatorCohortId()) == 1,
        "One comparator cohort must be selected"
      ))
      shiny::validate(
        shiny::need(
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
      shiny::validate(shiny::need(hasData(data), "No data available for selected combination."))
      distinctTemporalChoices <- unique(temporalChoices$temporalChoices)
      sortedTemporalChoices <- data %>%
        dplyr::arrange(factor(.data$temporalChoices, levels = distinctTemporalChoices)) %>%
        dplyr::distinct(.data$temporalChoices) %>%
        dplyr::pull("temporalChoices")

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
          "target" = "mean1",
          "sdT" = "sd1",
          "comparator" = "mean2",
          "sdC" = "sd2",
          "StdDiff" = "absStdDiff"
        )

      if (input$compareCharacterizationColumnFilters == "Mean and Standard Deviation") {
        data <- data %>%
          dplyr::select("covariateName",
                        "analysisName",
                        "conceptId",
                        "target",
                        "sdT",
                        "comparator",
                        "sdC",
                        "StdDiff")
      } else {
        data <- data %>%
          dplyr::select("covariateName",
                        "analysisName",
                        "conceptId",
                        "target",
                        "comparator",
                        "StdDiff")
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
              return(round(value, 2))
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
                        dplyr::select("cohortName") %>%
                        dplyr::pull(),
                      collapse = ", ")
      comparator <- paste(cohortTable %>%
                            dplyr::filter(.data$cohortId == comparatorCohortId()) %>%
                            dplyr::select("cohortName") %>%
                            dplyr::pull(),
                          collapse = ", ")


      shinydashboard::box(
        status = "warning",
        width = "100%",
        shiny::fluidRow(
          shiny::column(
            width = 7,
            shiny::tags$b("Target Cohort :"), paste0(target, " C", targetCohortId()),
            shiny::tags$br(),
            shiny::tags$b("Comparator Cohort :"), paste0(comparator, " C", comparatorCohortId())
          ),
          shiny::column(
            width = 5,
            shiny::tags$b("Target Database :"),
            paste(databaseTable %>%
                    dplyr::filter(.data$databaseId == input$targetDatabase) %>%
                    dplyr::select("databaseName") %>%
                    dplyr::pull(),
                  collapse = ", "),
            shiny::tags$br(),
            shiny::tags$b("Comparator Database :"),
            paste(databaseTable %>%
                    dplyr::filter(.data$databaseId == input$comparatorDatabase) %>%
                    dplyr::select("databaseName") %>%
                    dplyr::pull(),
                  collapse = ", ")
          )
        )
      )
    })

    generateTable <- shiny::reactive({
      data <- compareCohortCharacterizationRawTable()
      shiny::validate(shiny::need(hasData(data), "No data for selected combination"))
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
      shiny::validate(shiny::need(
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
          shortNameRef = cohortTable)

      progress$set(
        message = "Returning data",
        value = 90
      )
      shiny::validate(shiny::need(
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
      plotly::renderPlotly(expr = {
        generatePlot()
      })
  })
}
