characterizationView <- function(id) {
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
      width = NULL,
      title = NULL,
      tags$table(
        tags$tr(
          tags$td(
            shiny::radioButtons(
              inputId = ns("charType"),
              label = "",
              choices = c("Pretty", "Raw"),
              selected = "Pretty",
              inline = TRUE
            )
          ),
          tags$td(
            shiny::conditionalPanel(
              condition = "input.charType == 'Raw'",
              ns = ns,
              tags$table(tags$tr(
                tags$td(
                  shinyWidgets::pickerInput(
                    inputId = ns("characterizationAnalysisNameFilter"),
                    label = "Analysis name",
                    choices = c(""),
                    selected = c(""),
                    inline = TRUE,
                    multiple = TRUE,
                    width = 300,
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
                    inputId = ns("characterizationDomainIdFilter"),
                    label = "Domain name",
                    choices = c(""),
                    selected = c(""),
                    inline = TRUE,
                    multiple = TRUE,
                    width = 300,
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
                    inputId = ns("characterizationProportionOrContinuous"),
                    label = "",
                    choices = c("All", "Proportion", "Continuous"),
                    selected = "Proportion",
                    inline = TRUE
                  )
                ),
                tags$td(
                  shiny::checkboxInput(
                    inputId = ns("characterizationFilterLowValues"),
                    label = "Filter low values",
                    value = TRUE
                  )
                )
              ))
            )
          )
        ),
        tags$tr(
          tags$td(
            colspan = 2,
            shiny::conditionalPanel(
              condition = "input.charType == 'Raw'",
              ns = ns,
              shiny::radioButtons(
                inputId = ns("characterizationColumnFilters"),
                label = "Display",
                choices = c("Mean and Standard Deviation", "Mean only"),
                selected = "Mean only",
                inline = TRUE
              )
            )
          )
        )
      ),
      tags$table(
        width = "100%",
        tags$tr(
          tags$td(
            align = "right",
          )
        )
      ),
      shinycssloaders::withSpinner(
        reactable::reactableOutput(outputId = ns("characterizationTable"))
      ),
      csvDownloadButton(ns, "characterizationTable")
    )
  )
}


characterizationModule <- function(id,
                                   dataSource,
                                   cohortTable,
                                   databaseTable,
                                   selectedCohort,
                                   selectedDatabaseIds,
                                   targetCohortId,
                                   temporalAnalysisRef,
                                   analysisNameOptions,
                                   analysisIdInCohortCharacterization,
                                   getConceptIdsToFilterCharacterizationOutput,
                                   selectedConceptSets,
                                   characterizationMenuOutput,
                                   characterizationTimeIdChoices) {

  shiny::moduleServer(id, function(input, output, session) {


    output$selectedCohorts <- shiny::renderUI(selectedCohort())
    # Cohort Characterization -------------------------------------------------

    #### characterizationAnalysisNameFilter ----
    shiny::observe({
      characterizationAnalysisOptionsUniverse <- NULL
      charcterizationAnalysisOptionsSelected <- NULL
      
      if (hasData(temporalAnalysisRef)) {
        characterizationAnalysisOptionsUniverse <- analysisNameOptions
        charcterizationAnalysisOptionsSelected <- temporalAnalysisRef %>%
          dplyr::filter(.data$domainId %in% c('Condition', 'Cohort')) %>% 
          dplyr::filter(.data$isBinary == 'Y') %>% 
          dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
          dplyr::pull(.data$analysisName) %>%
          unique()
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "characterizationAnalysisNameFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = characterizationAnalysisOptionsUniverse,
        selected = charcterizationAnalysisOptionsSelected
      )
    })

    ### characterizationDomainNameFilter ----
    shiny::observe({
      characterizationDomainOptionsUniverse <- NULL
      charcterizationDomainOptionsSelected <- NULL
      
      if (hasData(temporalAnalysisRef)) {
        characterizationDomainOptionsUniverse <- domainIdOptions
        charcterizationDomainOptionsSelected <- temporalAnalysisRef %>%
          dplyr::filter(.data$domainId %in% c('Condition', 'Cohort')) %>% 
          dplyr::filter(.data$isBinary == 'Y') %>% 
          dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
          dplyr::pull(.data$domainId) %>%
          unique()
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "characterizationDomainIdFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = characterizationDomainOptionsUniverse,
        selected = charcterizationDomainOptionsSelected
      )
      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "characterizationDomainIdFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = characterizationDomainOptionsUniverse,
        selected = charcterizationDomainOptionsSelected
      )
    })

    ## cohortCharacterizationDataFiltered ----
    cohortCharacterizationDataFiltered <- shiny::reactive(x = {
      validate(need(length(selectedDatabaseIds()) > 0, "Atleast one data source must be selected"))
      validate(need(length(targetCohortId()) == 1, "One target cohort must be selected"))

      data <-
        characterizationMenuOutput()

      if (!hasData(data)) {
        return(NULL)
      }
      data <- data$covariateValue
      if (!hasData(data)) {
        return(NULL)
      }
      
      if (isTRUE(input$characterizationFilterLowValues)) {
        data <- data %>% 
          dplyr::filter(.data$mean > 0.01)
      }

      data <- data %>%
        dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
        dplyr::filter(.data$timeId %in% c(characterizationTimeIdChoices$timeId %>% unique())) %>%
        dplyr::filter(.data$cohortId %in% c(targetCohortId())) %>%
        dplyr::filter(.data$databaseId %in% c(selectedDatabaseIds()))

      if (input$charType == "Raw") {
        if (input$characterizationProportionOrContinuous == "Proportion") {
          data <- data %>%
            dplyr::filter(.data$isBinary == "Y")
        } else if (input$characterizationProportionOrContinuous == "Continuous") {
          data <- data %>%
            dplyr::filter(.data$isBinary == "N")
        }
      }

      if (input$characterizationProportionOrContinuous == "Proportion") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "Y")
      } else if (input$characterizationProportionOrContinuous == "Continuous") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "N")
      }

      data <- data %>%
        dplyr::filter(.data$analysisName %in% input$characterizationAnalysisNameFilter)

      data <- data %>%
        dplyr::filter(.data$domainId %in% input$characterizationDomainIdFilter)

      if (hasData(selectedConceptSets())) {
        if (hasData(getConceptIdsToFilterCharacterizationOutput())) {
          data <- data %>%
            dplyr::filter(.data$conceptId %in% getConceptIdsToFilterCharacterizationOutput())
        }
      }
      if (!hasData(data)) {
        return(NULL)
      }
      return(data)
    })

    ## cohortCharacterizationPrettyTable ----
    cohortCharacterizationPrettyTable <- shiny::reactive(x = {
      validate(need(length(selectedDatabaseIds()) > 0, "Atleast one data source must be selected"))
      validate(need(length(targetCohortId()) == 1, "One target cohort must be selected"))
      data <-
        characterizationMenuOutput()
      if (!hasData(data)) {
        return(NULL)
      }
      data <- data$covariateValue
      if (!hasData(data)) {
        return(NULL)
      }

      data <- data %>%
        dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
        dplyr::filter(.data$timeId %in% c(characterizationTimeIdChoices$timeId %>% unique(), NA)) %>%
        dplyr::filter(.data$cohortId %in% c(targetCohortId())) %>%
        dplyr::filter(.data$databaseId %in% c(selectedDatabaseIds()))
      if (!hasData(data)) {
        return(NULL)
      }

      showDataAsPercent <-
        TRUE ## showDataAsPercent set based on UI selection - proportion)

      if (showDataAsPercent) {
        data <- data %>%
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
        data <- data %>%
          dplyr::select(
            .data$cohortId,
            .data$databaseId,
            .data$analysisId,
            .data$covariateId,
            .data$covariateName,
            .data$sumValue
          )
      }

      table <- data %>%
        prepareTable1(
          prettyTable1Specifications = prettyTable1Specifications,
          cohort = cohort
        )
      if (!hasData(table)) {
        return(NULL)
      }
      keyColumnFields <- c("characteristic")
      dataColumnFields <- intersect(
        x = colnames(table),
        y = cohort$shortName
      )


      countLocation <- 1
      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource = dataSource,
          databaseIds = selectedDatabaseIds(),
          cohortIds = targetCohortId(),
          source = "cohort",
          fields = "Persons"
        )
      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(
          data = table,
          string = dataColumnFields
        )
      displayTable <- getDisplayTableGroupedByDatabaseId(
        data = table,
        cohort = cohortTable,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showDataAsPercent = showDataAsPercent,
        sort = FALSE,
        pageSize = 100
      )
      return(displayTable)
    })

    ## cohortCharacterizationRawTable ----
    cohortCharacterizationRawTable <- shiny::reactive(x = {
      data <- cohortCharacterizationDataFiltered()
      if (!hasData(data)) {
        return(NULL)
      }
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = "Post processing: Rendering table",
        value = 0
      )
      data <- data %>%
        dplyr::select(
          .data$covariateName,
          .data$analysisName,
          .data$startDay,
          .data$endDay,
          .data$conceptId,
          .data$mean,
          .data$sd,
          .data$cohortId,
          .data$databaseId,
          .data$temporalChoices
        )

      keyColumnFields <-
        c("covariateName", "analysisName", "temporalChoices", "conceptId")

      showDataAsPercent <- FALSE
      if (input$characterizationColumnFilters == "Mean and Standard Deviation") {
        dataColumnFields <- c("mean", "sd")
      } else {
        dataColumnFields <- c("mean")
        if (input$characterizationProportionOrContinuous == "Proportion") {
          showDataAsPercent <- TRUE
        }
      }
      countLocation <- 1

      countsForHeader <-
        getDisplayTableHeaderCount(
          dataSource = dataSource,
          databaseIds = selectedDatabaseIds(),
          cohortIds = targetCohortId(),
          source = "cohort",
          fields = "Persons"
        )

      maxCountValue <-
        getMaxValueForStringMatchedColumnsInDataFrame(
          data = data,
          string = dataColumnFields
        )

      getDisplayTableGroupedByDatabaseId(
        data = data,
        cohort = cohortTable,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showDataAsPercent = showDataAsPercent,
        sort = TRUE,
        pageSize = 100
      )
    })

    ## Output: characterizationTable ----
    output$characterizationTable <- reactable::renderReactable(expr = {
      if (input$charType == "Pretty") {
        data <- cohortCharacterizationPrettyTable()
        validate(need(hasData(data), "No data for selected combination"))
        return(data)
      } else {
        data <- cohortCharacterizationRawTable()
        validate(need(hasData(data), "No data for selected combination"))
        return(data)
      }
    })

  })
}
