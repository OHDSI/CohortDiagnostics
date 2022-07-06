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
      tags$table(tags$tr(
        tags$td(
          shinyWidgets::pickerInput(
            inputId = ns("temporalCharacterizationAnalysisNameFilter"),
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
            inputId = ns("temporalcharacterizationDomainIdFilter"),
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
            inputId = ns("temporalProportionOrContinuous"),
            label = "",
            choices = c("All", "Proportion", "Continuous"),
            selected = "Proportion",
            inline = TRUE
          )
        )
      )),
      tags$table(
        width = "100%",
        tags$tr(
          tags$td(
            align = "right",
          )
        )
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(ns("temporalCharacterizationTable"))),
    )
  )
}


temporalCharacterizationModule <- function(id,
                                           dataSource,
                                           selectedCohort,
                                           selectedDatabaseIds,
                                           targetCohortId,
                                           temporalAnalysisRef,
                                           analysisNameOptions,
                                           selectedTemporalTimeIds,
                                           getResolvedAndMappedConceptIdsForFilters,
                                           selectedConceptSets,
                                           analysisIdInTemporalCharacterization,
                                           domainIdOptions,
                                           temporalCharacterizationTimeIdChoices,
                                           characterizationOutputForCharacterizationMenu) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    output$selectedCohorts <- shiny::renderUI(selectedCohort())
    output$selectedDatabases <- shiny::renderUI(selectedDatabaseIds())

    # Temporal characterization ------------

    ### temporalCharacterizationAnalysisNameFilter ----
    shiny::observe({
      temporalCharacterizationAnalysisOptionsUniverse <- NULL
      temporalCharcterizationAnalysisOptionsSelected <- NULL

      if (hasData(temporalAnalysisRef)) {
        temporalCharacterizationAnalysisOptionsUniverse <-
          analysisNameOptions
        temporalCharcterizationAnalysisOptionsSelected <-
          temporalAnalysisRef %>%
            dplyr::filter(.data$analysisId %in% analysisIdInTemporalCharacterization) %>%
            dplyr::pull(.data$analysisName) %>%
            unique()
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "temporalCharacterizationAnalysisNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = temporalCharacterizationAnalysisOptionsUniverse,
        selected = temporalCharcterizationAnalysisOptionsSelected
      )
    })

    ### temporalcharacterizationDomainIdFilter ----
    shiny::observe({
      temporalCharacterizationDomainOptionsUniverse <- NULL
      temporalCharcterizationDomainOptionsSelected <- NULL

      if (hasData(temporalAnalysisRef)) {
        temporalCharacterizationDomainOptionsUniverse <-
          domainIdOptions
        temporalCharcterizationDomainOptionsSelected <-
          temporalAnalysisRef %>%
            dplyr::filter(.data$analysisId %in% analysisIdInTemporalCharacterization) %>%
            dplyr::pull(.data$domainId) %>%
            unique()
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "temporalcharacterizationDomainIdFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = temporalCharacterizationDomainOptionsUniverse,
        selected = temporalCharcterizationDomainOptionsSelected
      )
    })

    ## temporalCohortCharacterizationDataFiltered ------------
    temporalCohortCharacterizationDataFiltered <- shiny::reactive({
      validate(need(length(selectedDatabaseIds()) == 1, "One data source must be selected"))
      validate(need(length(targetCohortId()) == 1, "One target cohort must be selected"))
      if (!hasData(selectedTemporalTimeIds())) {
        return(NULL)
      }

      data <-
        characterizationOutputForCharacterizationMenu()
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data$covariateValue
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data %>%
        dplyr::filter(.data$analysisId %in% analysisIdInTemporalCharacterization) %>%
        dplyr::filter(.data$timeId %in% selectedTemporalTimeIds()) %>%
        dplyr::filter(.data$cohortId %in% c(targetCohortId())) %>%
        dplyr::filter(.data$databaseId %in% c(selectedDatabaseIds()))

      if (input$temporalProportionOrContinuous == "Proportion") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "Y")
      } else if (input$temporalProportionOrContinuous == "Continuous") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "N")
      }

      data <- data %>%
        dplyr::filter(.data$analysisName %in% input$temporalCharacterizationAnalysisNameFilter) %>%
        dplyr::filter(.data$domainId %in% input$temporalcharacterizationDomainIdFilter)

      if (hasData(selectedConceptSets())) {
        if (hasData(getResolvedAndMappedConceptIdsForFilters())) {
          data <- data %>%
            dplyr::filter(.data$conceptId %in% getResolvedAndMappedConceptIdsForFilters())
        }
      }
      if (!hasData(data)) {
        return(NULL)
      }
      return(data)
    })

    ## temporalCharacterizationRawTable ----
    temporalCharacterizationRawTable <- shiny::reactive(x = {
      data <- temporalCohortCharacterizationDataFiltered()

      validate(need(
        hasData(data),
        "No temporal characterization data"
      ))
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = "Post processing: Rendering table",
        value = 0
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
      if (input$temporalProportionOrContinuous == "Proportion") {
        showDataAsPercent <- TRUE
      }

      getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns,
        showDataAsPercent = showDataAsPercent,
        pageSize = 100
      )
    })

    ## Output: temporalCharacterizationTable ------------------------
    output$temporalCharacterizationTable <-
      reactable::renderReactable(expr = {
        temporalCharacterizationRawTable()
      })

  })
}