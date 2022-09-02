temporalCharacterizationView <- function(id) {
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
      shiny::radioButtons(
        inputId = ns("proportionOrContinuous"),
        label = "View Covariate Type(s)",
        choices = c("All", "Proportion", "Continuous"),
        selected = "Proportion",
        inline = TRUE
      ),
      shiny::actionButton(label = "Get Temporal covariate data", inputId = ns("generateReport")),

      shiny::conditionalPanel("input.generateReport != 0",
                              ns = ns,
                              shiny::tagList(
                                shinycssloaders::withSpinner(reactable::reactableOutput(ns("temporalCharacterizationTable"))),
                                csvDownloadButton(ns, "temporalCharacterizationTable")
                              ))
    )

  )
}


temporalCharacterizationModule <- function(id,
                                           dataSource,
                                           databaseTable,
                                           selectedCohort,
                                           selectedDatabaseIds,
                                           targetCohortId,
                                           temporalAnalysisRef,
                                           analysisNameOptions,
                                           getResolvedAndMappedConceptIdsForFilters,
                                           selectedConceptSets,
                                           domainIdOptions,
                                           temporalCharacterizationTimeIdChoices) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI(selectedCohort())
    output$selectedDatabases <- shiny::renderUI({
      paste(databaseTable %>%
              dplyr::filter(.data$databaseId %in% selectedDatabaseIds()) %>%
              dplyr::select(.data$databaseName),
            collapse = ", ")
    })

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

    })
    selectedTemporalTimeIds <- shiny::reactive({
      timeIdOptions %>%
        dplyr::filter(.data$temporalChoices %in% input$timeIdChoices) %>%
        dplyr::select(.data$timeId) %>%
        dplyr::pull()
    })

    # Temporal characterization ------------
    characterizationOutput <- shiny::reactive(x = {
      data <- getCharacterizationOutput(
        dataSource = dataSource,
        cohortIds = targetCohortId(),
        databaseIds = selectedDatabaseIds(),
        temporalCovariateValueDist = FALSE
      )
      return(data)
    })


    ### analysisNameFilter ----
    shiny::observe({
      analysisOptions <- NULL
      selectedAnalysisOptions <- NULL

      if (hasData(temporalAnalysisRef)) {
        analysisOptions <-
          analysisNameOptions
        selectedAnalysisOptions <-
          temporalAnalysisRef %>%
            dplyr::filter(.data$analysisId %in% analysisIdInTemporalCharacterization) %>%
            dplyr::pull(.data$analysisName) %>%
            unique()
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "analysisNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = analysisOptions,
        selected = selectedAnalysisOptions
      )
    })

    ### domainIdFilter ----
    shiny::observe({
      domainOptions <- NULL
      domainOptionsSelected <- NULL

      if (hasData(temporalAnalysisRef)) {
        domainOptions <-
          domainIdOptions
        domainOptionsSelected <-
          temporalAnalysisRef %>%
            dplyr::pull(.data$domainId) %>%
            unique()
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "domainIdFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = domainOptions,
        selected = domainOptionsSelected
      )
    })

    ## temporalCohortCharacterizationDataFiltered ------------
    temporalCharacterizationDataFilt <- shiny::reactive({
      validate(need(length(selectedDatabaseIds()) == 1, "One data source must be selected"))
      validate(need(length(targetCohortId()) == 1, "One target cohort must be selected"))
      if (!hasData(selectedTemporalTimeIds())) {
        return(NULL)
      }

      data <-
        characterizationOutput()
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data$covariateValue
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::filter(.data$timeId %in% selectedTemporalTimeIds()) %>%
        dplyr::filter(.data$cohortId %in% c(targetCohortId())) %>%
        dplyr::filter(.data$databaseId %in% c(selectedDatabaseIds()))

      if (input$proportionOrContinuous == "Proportion") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "Y")
      } else if (input$proportionOrContinuous == "Continuous") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "N")
      }

      data <- data %>%
        dplyr::filter(.data$analysisName %in% input$analysisNameFilter) %>%
        dplyr::filter(.data$domainId %in% input$domainIdFilter)

      if (hasData(selectedConceptSets())) {
        mappedConcepts <- getResolvedAndMappedConceptIdsForFilters()
        if (hasData()) {
          data <- data %>%
            dplyr::filter(.data$conceptId %in% mappedConcepts)
        }
      }
      if (!hasData(data)) {
        return(NULL)
      }
      return(data)
    })

    ## temporalCharacterizationRawTable ----
    temporalCharacterizationRawTable <- shiny::eventReactive(input$generateReport, {
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = paste0(
          "Retrieving characterization output for cohort id ",
          targetCohortId(),
          " cohorts and ",
          length(selectedDatabaseIds()),
          " data sources."
        ),
        value = 0
      )

      data <- temporalCharacterizationDataFilt()

      validate(need(
        hasData(data),
        "No temporal characterization data"
      ))
      progress$set(
        message = "Post processing: filtering table",
        value = 50
      )

      temporalChoices <- temporalCharacterizationTimeIdChoices %>%
        dplyr::filter(.data$timeId %in% c(data$timeId %>% unique())) %>%
        dplyr::pull(.data$temporalChoices) %>%
        unique()

      keyColumns <- c("covariateName", "analysisName", "conceptId")
      data <- data %>%
        dplyr::select(
          .data$covariateName,
          .data$analysisName,
          .data$temporalChoices,
          .data$conceptId,
          .data$mean,
          .data$sd
        ) %>%
        tidyr::pivot_wider(
          id_cols = dplyr::all_of(keyColumns),
          names_from = "temporalChoices",
          values_from = "mean",
          names_sep = "_"
        ) %>%
        dplyr::relocate(dplyr::all_of(c(keyColumns, temporalChoices))) %>%
        dplyr::arrange(dplyr::desc(dplyr::across(dplyr::starts_with("T ("))))

      if (any(stringr::str_detect(
        string = colnames(data),
        pattern = stringr::fixed("T (0")
      ))) {
        data <- data %>%
          dplyr::arrange(dplyr::desc(dplyr::across(dplyr::starts_with("T (0"))))
      }

      dataColumns <- c(temporalChoices)

      showDataAsPercent <- FALSE
      if (input$proportionOrContinuous == "Proportion") {
        showDataAsPercent <- TRUE
      }

      progress$set(
        message = "Rendering table",
        value = 80
      )

      getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns,
        showDataAsPercent = showDataAsPercent,
        pageSize = 100
      )
    })

    output$temporalCharacterizationTable <-
      reactable::renderReactable(expr = {
        progress <- shiny::Progress$new()
        on.exit(progress$close())
        progress$set(
          message = paste0("Returning result"),
          value = 90
        )

        temporalCharacterizationRawTable()
      })
  })
}