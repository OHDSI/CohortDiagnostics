characterizationView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Cohort Characterization",
      width = "100%",
      shiny::htmlTemplate(file.path("html", "cohortCharacterization.html"))
    ),
    shinydashboard::box(
      width = NULL,
      shiny::radioButtons(
        inputId = ns("charType"),
        label = "Table type",
        choices = c("Pretty", "Raw"),
        selected = "Pretty",
        inline = TRUE
      ),

      shiny::conditionalPanel(
        condition = "input.charType == 'Raw'",
        ns = ns,
        shiny::fluidRow(
          shiny::column(
            width = 6,
            shinyWidgets::pickerInput(
              inputId = ns("characterizationAnalysisNameFilter"),
              label = "Analysis name",
              choices = c(""),
              selected = c(""),
              inline = TRUE,
              multiple = TRUE,
              width = "100%",
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
            width = 6,
            shinyWidgets::pickerInput(
              inputId = ns("characterizationDomainIdFilter"),
              label = "Domain name",
              choices = c(""),
              selected = c(""),
              inline = TRUE,
              multiple = TRUE,
              width = "100%",
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
          )
        ),
        shiny::fluidRow(
          shiny::column(
            width = 6,
            shiny::radioButtons(
              inputId = ns("characterizationProportionOrContinuous"),
              label = "Covariate type(s)",
              choices = c("All", "Proportion", "Continuous"),
              selected = "Proportion",
              inline = TRUE
            )
          ),
          shiny::column(
            width = 6,
            shiny::radioButtons(
              inputId = ns("characterizationColumnFilters"),
              label = "Display",
              choices = c("Mean and Standard Deviation", "Mean only"),
              selected = "Mean only",
              inline = TRUE
            )
          )
        )
      ),
      shiny::actionButton(label = "Generate Table", inputId = ns("generateReport"))
    ),
    shiny::conditionalPanel(
      condition = "input.generateReport > 0",
      ns = ns,
      shiny::uiOutput(outputId = ns("selections")),
      shinydashboard::box(
        width = NULL,
        shinycssloaders::withSpinner(
          reactable::reactableOutput(outputId = ns("characterizationTable"))
        ),
        csvDownloadButton(ns, "characterizationTable")
      )
    )
  )
}


characterizationModule <- function(id,
                                   dataSource,
                                   cohortTable,
                                   databaseTable,
                                   selectedDatabaseIds,
                                   targetCohortId,
                                   temporalAnalysisRef,
                                   analysisNameOptions,
                                   analysisIdInCohortCharacterization,
                                   getResolvedAndMappedConceptIdsForFilters,
                                   selectedConceptSets,
                                   characterizationTimeIdChoices) {

  shiny::moduleServer(id, function(input, output, session) {

    selectionsOutput <- shiny::eventReactive(input$generateReport, {
      shinydashboard::box(
        status = "warning",
        width = "100%",
        shiny::fluidRow(
          shiny::column(
            width = 9,
            tags$b("Cohort :"),
            paste(cohortTable %>%
                    dplyr::filter(.data$cohortId %in% targetCohortId()) %>%
                    dplyr::select(.data$cohortName) %>%
                    dplyr::pull(),
                  collapse = ", ")
          ),
          shiny::column(
            width = 3,
            tags$b("Database(s) :"),
            paste(databaseTable %>%
                    dplyr::filter(.data$databaseId %in% selectedDatabaseIds()) %>%
                    dplyr::select(.data$databaseName) %>%
                    dplyr::pull(),
                  collapse = ", ")
          )
        )
      )
    })

    output$selections <- shiny::renderUI(selectionsOutput())
    # Cohort Characterization -------------------------------------------------

    # Temporal characterization ------------
    characterizationOutput <- shiny::eventReactive(input$generateReport, {
      validate(need(length(selectedDatabaseIds()) > 0, "At least one data source must be selected"))
      validate(need(length(targetCohortId()) == 1, "One target cohort must be selected"))

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
        value = 20
      )
      data <- getCharacterizationOutput(
        dataSource = dataSource,
        cohortIds = targetCohortId(),
        databaseIds = selectedDatabaseIds(),
        temporalCovariateValueDist = FALSE
      )
      return(data)
    })
    #### characterizationAnalysisNameFilter ----
    shiny::observe({
      characterizationAnalysisOptionsUniverse <- NULL
      charcterizationAnalysisOptionsSelected <- NULL

      if (hasData(temporalAnalysisRef)) {
        characterizationAnalysisOptionsUniverse <- analysisNameOptions
        charcterizationAnalysisOptionsSelected <- temporalAnalysisRef %>%
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
    cohortCharacterizationDataFiltered <- shiny::reactive({

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
        dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
        dplyr::filter(.data$timeId %in% c(characterizationTimeIdChoices$timeId %>% unique()))

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

    ## cohortCharacterizationPrettyTable ----
    cohortCharacterizationPrettyTable <- shiny::reactive({
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
        dplyr::filter(.data$analysisId %in% analysisIdInCohortCharacterization) %>%
        dplyr::filter(.data$timeId %in% c(characterizationTimeIdChoices$timeId %>% unique(), NA))
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
          cohort = cohortTable
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
          databaseIds = data$databaseId %>% unique(),
          cohortIds = data$cohortId %>% unique(),
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
    cohortCharacterizationRawTable <- shiny::reactive({
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
          databaseIds = data$databaseId %>% unique(),
          cohortIds = data$cohortId %>% unique(),
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
