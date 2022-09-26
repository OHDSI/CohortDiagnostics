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
      shiny::fluidRow(
        shiny::column(
          width = 4,
          shinyWidgets::pickerInput(
            inputId = ns("targetCohort"),
            label = "Select Cohort",
            choices = NULL,
            options = shinyWidgets::pickerOptions(
              actionsBox = TRUE,
              liveSearch = TRUE,
              size = 10,
              maxOptions = 5, # Selecting even this many will be slow
              liveSearchStyle = "contains",
              liveSearchPlaceholder = "Type here to search",
              virtualScroll = 50
            )
          )
        ),
        shiny::column(
          width = 4,
          shinyWidgets::pickerInput(
            inputId = ns("targetDatabase"),
            label = "Select Database (s)",
            choices = NULL,
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
            inputId = ns("selectedConceptSet"),
            label = "Select Concept Set",
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
      shiny::conditionalPanel(
        condition = "input.charType == 'Raw'",
        ns = ns,
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
            width = 4,
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
        )
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.charType == 'Pretty'",
        shiny::actionButton(label = "Generate Table", inputId = ns("generateReport"))
      ),
      shiny::conditionalPanel(
        ns = ns,
        condition = "input.charType == 'Raw'",
        shiny::actionButton(label = "Generate Table", inputId = ns("generateRaw"))
      ),
    ),
    shiny::conditionalPanel(
      condition = "input.generateReport > 0 && input.charType == 'Pretty'",
      ns = ns,
      shiny::uiOutput(outputId = ns("selections")),
      shinydashboard::box(
        width = NULL,
        shinycssloaders::withSpinner(
          reactable::reactableOutput(outputId = ns("characterizationTable"))
        ),
        csvDownloadButton(ns, "characterizationTable")
      )
    ),
    shiny::conditionalPanel(
      condition = "input.generateRaw > 0 && input.charType == 'Raw'",
      ns = ns,
      shiny::uiOutput(outputId = ns("selectionsRaw")),
      shinydashboard::box(
        width = NULL,
        shiny::fluidRow(
          shiny::column(
            width = 6,
            shiny::radioButtons(
              inputId = ns("proportionOrContinuous"),
              label = "Covariate type(s)",
              choices = c("All", "Proportion", "Continuous"),
              selected = "All",
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
        ),
        shiny::tabsetPanel(
          type = "pills",
          shiny::tabPanel(
            title = "Group by Database",
            shinycssloaders::withSpinner(
              reactable::reactableOutput(outputId = ns("characterizationTableRaw"))
            ),
            csvDownloadButton(ns, "characterizationTableRaw")
          ),
          shiny::tabPanel(
            title = "Group by Time ID",
            shinycssloaders::withSpinner(
              reactable::reactableOutput(outputId = ns("characterizationTableRawGroupedByTime"))
            ),
            csvDownloadButton(ns, "characterizationTableRawGroupedByTime")
          )
        )
      )
    )
  )
}


characterizationModule <- function(id,
                                   dataSource,
                                   cohortTable,
                                   databaseTable,
                                   temporalAnalysisRef,
                                   analysisNameOptions,
                                   domainIdOptions,
                                   analysisIdInCohortCharacterization,
                                   getResolvedAndMappedConceptIdsForFilters,
                                   selectedConceptSets,
                                   characterizationTimeIdChoices,
                                   table1SpecPath = "data/Table1SpecsLong.csv") {
  prettyTable1Specifications <- readr::read_csv(
    file = table1SpecPath,
    col_types = readr::cols(),
    guess_max = min(1e7),
    lazy = FALSE
  )
  shiny::moduleServer(id, function(input, output, session) {

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

      cohortChoices <- cohortTable$cohortId
      names(cohortChoices) <- cohortTable$cohortName
      shinyWidgets::updatePickerInput(session,
                                      inputId = "targetCohort",
                                      choices = cohortChoices)


      databaseChoices <- databaseTable$databaseId
      names(databaseChoices) <- databaseTable$databaseName
      shinyWidgets::updatePickerInput(session,
                                      inputId = "targetDatabase",
                                      selected = databaseChoices[1],
                                      choices = databaseChoices)
    })

    selectedTimeIds <- shiny::reactive({
      timeIdOptions %>%
        dplyr::filter(.data$temporalChoices %in% input$timeIdChoices) %>%
        dplyr::select(.data$timeId) %>%
        dplyr::pull()
    })

    selectedDatabaseIds <- shiny::reactive(input$targetDatabase)
    targetCohortId <- shiny::reactive(input$targetCohort)


    selectionsPanel <- shiny::reactive({
      shinydashboard::box(
        status = "warning",
        width = "100%",
        shiny::fluidRow(
          shiny::column(
            width = 4,
            tags$b("Cohort :"),
            paste(cohortTable %>%
                    dplyr::filter(.data$cohortId %in% targetCohortId()) %>%
                    dplyr::select(.data$cohortName) %>%
                    dplyr::pull(),
                  collapse = ", ")
          ),
          shiny::column(
            width = 8,
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

    selectionsOutput <- shiny::eventReactive(input$generateReport, {
      selectionsPanel()
    })

    selectionsOutputRaw <- shiny::eventReactive(input$generateRaw, {
      selectionsPanel()
    })

    output$selections <- shiny::renderUI(selectionsOutput())
    output$selectionsRaw <- shiny::renderUI(selectionsOutputRaw())
    # Cohort Characterization -------------------------------------------------

    # Temporal characterization ------------
    characterizationOutput <- shiny::reactive({
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

    ## cohortCharacterizationPrettyTable ----
    cohortCharacterizationPrettyTable <- shiny::eventReactive(input$generateReport, {
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
        y = cohortTable$shortName
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
        showDataAsPercent = TRUE,
        sort = FALSE,
        pageSize = 100
      )
      return(displayTable)
    })

    ## Output: characterizationTable ----
    output$characterizationTable <- reactable::renderReactable(expr = {
      data <- cohortCharacterizationPrettyTable()
      validate(need(hasData(data), "No data for selected combination"))
      return(data)
    })


    ## cohortCharacterizationDataFiltered ----
    cohortCharacterizationDataFiltered <- shiny::eventReactive(input$generateRaw, {
      data <- characterizationOutput()

      if (!hasData(data)) {
        return(NULL)
      }
      data <- data$covariateValue
      if (!hasData(data)) {
        return(NULL)
      }

      data <- data %>%
        dplyr::filter(.data$timeId %in% selectedTimeIds()) %>%
        dplyr::filter(.data$analysisName %in% input$characterizationAnalysisNameFilter) %>%
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

    rawTableReactable <- shiny::reactive({
      data <- cohortCharacterizationDataFiltered()
      if (is.null(data)) {
        return(NULL)
      }

      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = "Post processing: Rendering table",
        value = 0
      )

      keyColumnFields <-
        c("covariateName", "analysisName", "temporalChoices", "conceptId")

      if (input$characterizationColumnFilters == "Mean and Standard Deviation") {
        dataColumnFields <- c("mean", "sd")
      } else {
        dataColumnFields <- c("mean")
      }
      countLocation <- 1


      if (!hasData(data)) {
        return(NULL)
      }

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

      data <- data %>%
        dplyr::select(
          .data$covariateName,
          .data$analysisName,
          .data$startDay,
          .data$endDay,
          .data$conceptId,
          .data$isBinary,
          .data$mean,
          .data$sd,
          .data$cohortId,
          .data$databaseId,
          .data$temporalChoices
        )

      if (input$proportionOrContinuous == "Proportion") {
        data <- data %>%
          dplyr::filter(.data$isBinary == "Y") %>%
          dplyr::select(-.data$isBinary)
      } else if (input$proportionOrContinuous == "Continuous") {
        data <- data %>% dplyr::filter(.data$isBinary == "N") %>%
          dplyr::select(-.data$isBinary)
      }

      getDisplayTableGroupedByDatabaseId(
        data = data,
        cohort = cohortTable,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        maxCount = maxCountValue,
        showDataAsPercent = FALSE,
        sort = TRUE,
        pageSize = 100
      )
    })

    output$characterizationTableRaw <- reactable::renderReactable(expr = {
      data <- rawTableReactable()
      validate(need(hasData(data), "No data for selected combination"))
      return(data)
    })


    rawTableTimeIdReactable <- shiny::reactive({
      data <- cohortCharacterizationDataFiltered()
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(
        message = "Post processing: Rendering table",
        value = 0
      )

      temporalChoices <- data$temporalChoices %>% unique()

      data <-
        data %>% dplyr::inner_join(databaseTable %>%
                                     dplyr::select(.data$databaseId, .data$databaseName),
                                   by = "databaseId")
      keyColumns <- c("covariateName", "analysisName", "conceptId", "databaseName")
      data <- data %>%
        dplyr::select(
          .data$covariateName,
          .data$analysisName,
          .data$databaseName,
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
      dataColumns <- temporalChoices
      progress$set(
        message = "Rendering table",
        value = 80
      )

      getDisplayTableSimple(
        data = data,
        keyColumns = keyColumns,
        dataColumns = dataColumns,
        showDataAsPercent = FALSE,
        pageSize = 100
      )
    })

    output$characterizationTableRawGroupedByTime <- reactable::renderReactable(expr = {
      data <- rawTableTimeIdReactable()
      validate(need(hasData(data), "No data for selected combination"))
      return(data)
    })
  })
}
