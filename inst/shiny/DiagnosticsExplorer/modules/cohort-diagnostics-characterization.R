# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of OhdsiShinyModules
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

#' characterization
#' @description
#' Use for customizing UI
#'
#' @param id    Namespace Id - use namespaced id ns("characterization") inside diagnosticsExplorer module
#' @family CohortDiagnostics
#' @export
cohortDiagCharacterizationView <- function(id) {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      collapsible = TRUE,
      collapsed = TRUE,
      title = "Cohort Characterization",
      width = "100%",
      shiny::htmlTemplate(file.path(cdWwwPath, "cohortCharacterization.html"))
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
          width = 5,
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
          width = 5,
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
              maxOptions = 5, # Selecting even this many will be slow
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
              inputId = ns("selectedRawAnalysisIds"),
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
        ),
        shiny::fluidRow(
          shiny::column(
            width = 4,
            shiny::radioButtons(
              inputId = ns("proportionOrContinuous"),
              label = "Covariate type(s)",
              choices = c("All", "Proportion", "Continuous"),
              selected = "All",
              inline = TRUE
            ),
            shiny::p("Percentage displayed where only proportional data is selected")
          ),
          shiny::column(
            width = 4,
            shiny::radioButtons(
              inputId = ns("characterizationColumnFilters"),
              label = "Display",
              choices = c("Mean and Standard Deviation", "Mean only"),
              selected = "Mean only",
              inline = TRUE
            )
          ),
          shiny::column(
            width = 4,
            shinyWidgets::pickerInput(
              inputId = ns("selectedConceptSet"),
              label = "Subset to Concept Set",
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
        reactableCsvDownloadButton(ns, "characterizationTable")
      )
    ),
    shiny::conditionalPanel(
      condition = "input.generateRaw > 0 && input.charType == 'Raw'",
      ns = ns,
      shiny::uiOutput(outputId = ns("selectionsRaw")),
      shinydashboard::box(
        width = NULL,
        shiny::tabsetPanel(
          type = "pills",
          shiny::tabPanel(
            title = "Group by Database",
            shiny::fluidRow(
              shiny::column(width = 6,
                            shiny::selectInput(inputId = ns("sortByRaw"),
                                               label = "Sort By",
                                               choices = NULL)
              ),
              shiny::column(width = 2,
                            shiny::radioButtons(inputId = ns("shortByRawAsc"),
                                                choices = c(ascending = "ASC", descending = "DESC"),
                                                selected = "DESC",
                                                label = "order")
              ),
              shiny::column(width = 4,
                            shiny::textInput(inputId = ns("generalSearchString"),
                                             label = "",
                                             placeholder = "Search covariates")
              ),
            ),
            largeTableView(id = ns("rawCharTbl"), selectedPageSize = 100)
          ),
          shiny::tabPanel(
            title = "Group by Time Windows",
            shiny::fluidRow(
              shiny::column(width = 6,
                            shiny::selectInput(inputId = ns("sortByRawTemporal"),
                                               label = "Sort By",
                                               choices = NULL)
              ),
              shiny::column(width = 2,
                            shiny::radioButtons(inputId = ns("shortByRawAscTemporal"),
                                                choices = c(ascending = "ASC", descending = "DESC"),
                                                selected = "DESC",
                                                label = "order")
              ),
              shiny::column(width = 4,
                            shiny::textInput(inputId = ns("generalSearchStringTemporal"),
                                             label = "",
                                             placeholder = "Search covariates")
              ),
            ),
            largeTableView(id = ns("rawCharTblTemporal"), selectedPageSize = 100)
          )
        )
      )
    )
  )
}

prepareTable1 <- function(covariates,
                          prettyTable1Specifications,
                          cohort) {
  if (!all(
    is.data.frame(prettyTable1Specifications),
    nrow(prettyTable1Specifications) > 0
  )) {
    return(NULL)
  }
  keyColumns <- prettyTable1Specifications %>%
    dplyr::select(
      "labelOrder",
      "label",
      "covariateId",
      "analysisId",
      "sequence"
    ) %>%
    dplyr::distinct() %>%
    dplyr::left_join(
      covariates %>%
        dplyr::select(
          "covariateId",
          "covariateName"
        ) %>%
        dplyr::distinct(),
      by = c("covariateId")
    ) %>%
    dplyr::filter(!is.na(.data$covariateName)) %>%
    tidyr::crossing(
      covariates %>%
        dplyr::select(
          "cohortId",
          "databaseId"
        ) %>%
        dplyr::distinct()
    ) %>%
    dplyr::arrange(
      .data$cohortId,
      .data$databaseId,
      .data$analysisId,
      .data$covariateId
    ) %>%
    dplyr::mutate(
      covariateName = stringr::str_replace( # replace with gsub
        string = .data$covariateName,
        pattern = "black or african american",
        replacement = "Black or African American"
      )
    ) %>%
    dplyr::mutate(
      covariateName = stringr::str_replace(
        string = .data$covariateName,
        pattern = "white",
        replacement = "White"
      )
    ) %>%
    dplyr::mutate(
      covariateName = stringr::str_replace(
        string = .data$covariateName,
        pattern = "asian",
        replacement = "Asian"
      )
    )

  covariates <- keyColumns %>%
    dplyr::left_join(
      covariates %>%
        dplyr::select(-"covariateName"),
      by = c(
        "cohortId",
        "databaseId",
        "covariateId",
        "analysisId"
      )
    ) %>%
    dplyr::filter(!is.na(.data$covariateName))

  space <- "&nbsp;"

  # labels
  tableHeaders <-
    covariates %>%
      dplyr::select(
        "cohortId",
        "databaseId",
        "label",
        "labelOrder",
        "sequence"
      ) %>%
      dplyr::distinct() %>%
      dplyr::group_by(
        .data$cohortId,
        .data$databaseId,
        .data$label,
        .data$labelOrder
      ) %>%
      dplyr::summarise(
        sequence = min(.data$sequence),
        .groups = "keep"
      ) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(
        characteristic = paste0(
          "<strong>",
          .data$label,
          "</strong>"
        ),
        header = 1
      ) %>%
      dplyr::select(
        "cohortId",
        "databaseId",
        "sequence",
        "header",
        "labelOrder",
        "characteristic"
      ) %>%
      dplyr::distinct()

  tableValues <-
    covariates %>%
      dplyr::mutate(
        characteristic = paste0(
          space,
          space,
          space,
          space,
          .data$covariateName
        ),
        header = 0,
        valueCount = .data$sumValue
      ) %>%
      dplyr::select(
        "cohortId",
        "databaseId",
        "covariateId",
        "analysisId",
        "sequence",
        "header",
        "labelOrder",
        "characteristic",
        "valueCount"
      )

  table <- dplyr::bind_rows(tableHeaders, tableValues) %>%
    dplyr::mutate(sequence = .data$sequence - .data$header) %>%
    dplyr::arrange(.data$sequence) %>%
    dplyr::select(
      "cohortId",
      "databaseId",
      "sequence",
      "characteristic",
      "valueCount"
    ) %>%
    dplyr::rename(count = "valueCount") %>%
    dplyr::inner_join(cohort %>%
                        dplyr::select(
                          "cohortId",
                          "shortName"
                        ),
                      by = "cohortId"
    ) %>%
    dplyr::group_by(
      .data$databaseId,
      .data$characteristic,
      .data$shortName
    ) %>%
    dplyr::summarise(
      sequence = min(.data$sequence),
      count = min(.data$count),
      .groups = "keep"
    ) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(
      id_cols = c(
        "databaseId",
        "characteristic",
        "sequence"
      ),
      values_from = "count",
      names_from = "shortName"
    ) %>%
    dplyr::arrange(.data$sequence)


  if (nrow(table) == 0) {
    return(NULL)
  }
  return(table)
}

cohortDiagCharacterizationModule <- function(
  id,
  dataSource,
  cohortTable = dataSource$cohortTable,
  databaseTable = dataSource$dbTable,
  temporalAnalysisRef = dataSource$temporalAnalysisRef,
  domainIdOptions = dataSource$domainIdOptions,
  characterizationTimeIdChoices = dataSource$characterizationTimeIdChoices,
  table1SpecPath = system.file("cohort-diagnostics-ref", "Table1SpecsLong.csv",
                               package = "CohortDiagnostics")
) {
  prettyTable1Specifications <- readr::read_csv(
    file = table1SpecPath,
    col_types = readr::cols(),
    guess_max = min(1e7),
    lazy = FALSE
  )

  # Analysis IDs for pretty table
  prettyTableAnalysisIds <- c(
    1, 3, 4, 5, 6, 7,
    203, 403, 501, 703,
    801, 901, 903, 904,
    -301, -201
  )

  shiny::moduleServer(id, function(input, output, session) {

    timeIdOptions <- getResultsTemporalTimeRef(dataSource = dataSource) %>%
      dplyr::arrange(.data$sequence)

    selectedTimeIds <- shiny::reactive({
      timeIdOptions %>%
        dplyr::filter(.data$temporalChoices %in% input$timeIdChoices) %>%
        dplyr::select("timeId") %>%
        dplyr::pull()
    })

    selectedDatabaseIds <- shiny::reactive(input$targetDatabase)
    targetCohortId <- shiny::reactive(input$targetCohort)

    getCohortConceptSets <- shiny::reactive({
      if (!hasData(input$targetCohort) | nrow(dataSource$conceptSets) == 0) {
        return(NULL)
      }

      dataSource$conceptSets %>%
        dplyr::filter(.data$cohortId == input$targetCohort) %>%
        dplyr::mutate(name = .data$conceptSetName, id = .data$conceptSetId) %>%
        dplyr::select("id", "name")
    })

    shiny::observe({
      # Default time windows
      selectedTimeWindows <- timeIdOptions %>%
        dplyr::distinct() %>%
        dplyr::filter(.data$primaryTimeId == 1) %>%
        dplyr::filter(.data$isTemporal == 1) %>%
        dplyr::arrange(.data$sequence) %>%
        dplyr::pull("temporalChoices")

      shinyWidgets::updatePickerInput(session,
                                      inputId = "timeIdChoices",
                                      choices = timeIdOptions$temporalChoices %>% unique(),
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

    conceptSetIds <- shiny::reactive({
      
      if (!hasData(input$selectedConceptSet) || input$selectedConceptSet == "") {
        return(NULL)
      }
    
      input$selectedConceptSet
    })

    getResolvedConcepts <- shiny::reactive({
      output <- resolvedConceptSet(
        dataSource = dataSource,
        databaseIds = selectedDatabaseIds(),
        cohortId = targetCohortId()
      )
      if (!hasData(output)) {
        return(NULL)
      }
      return(output)
    })

    ### getMappedConceptsReactive ----
    getMappedConcepts <- shiny::reactive({
      progress <- shiny::Progress$new()
      on.exit(progress$close())
      progress$set(message = "Getting concepts mapped to concept ids resolved by concept set expression (may take time)", value = 0)
      output <- mappedConceptSet(dataSource = dataSource,
                                 databaseIds = selectedDatabaseIds(),
                                 cohortId = targetCohortId())
      if (!hasData(output)) {
        return(NULL)
      }
      return(output)
    })

    getFilteredConceptIds <- shiny::reactive({
      shiny::validate(shiny::need(hasData(selectedDatabaseIds()), "No data sources chosen"))
      shiny::validate(shiny::need(hasData(targetCohortId()), "No cohort chosen"))

      if (!hasData(conceptSetIds()))
        return(NULL)

      resolved <- getResolvedConcepts()
      mapped <- getMappedConcepts()
      output <- c()
      if (hasData(resolved)) {
        resolved <- resolved %>%
          dplyr::filter(.data$databaseId %in% selectedDatabaseIds()) %>%
          dplyr::filter(.data$cohortId %in% targetCohortId()) %>%
          dplyr::filter(.data$conceptSetId %in% conceptSetIds())
        output <- c(output, resolved$conceptId) %>% unique()
      }
      if (hasData(mapped)) {
        mapped <- mapped %>%
          dplyr::filter(.data$databaseId %in% selectedDatabaseIds()) %>%
          dplyr::filter(.data$cohortId %in% targetCohortId()) %>%
          dplyr::filter(.data$conceptSetId %in% conceptSetIds())
        output <- c(output, mapped$conceptId) %>% unique()
      }

      if (hasData(output)) {
        return(output)
      } else {
        return(NULL)
      }
    })

    selectedConceptSets <- shiny::reactive(input$selectedConceptSet)

    selectionsPanel <- shiny::reactive({
      shinydashboard::box(
        status = "warning",
        width = "100%",
        shiny::fluidRow(
          shiny::column(
            width = 4,
            shiny::tags$b("Cohort :"),
            paste(cohortTable %>%
                    dplyr::filter(.data$cohortId %in% targetCohortId()) %>%
                    dplyr::select("cohortName") %>%
                    dplyr::pull(),
                  collapse = ", ")
          ),
          shiny::column(
            width = 8,
            shiny::tags$b("Database(s) :"),
            paste(databaseTable %>%
                    dplyr::filter(.data$databaseId %in% selectedDatabaseIds()) %>%
                    dplyr::select("databaseName") %>%
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


    #### selectedRawAnalysisIds ----
    shiny::observe({
      analysisIdOptions <- NULL
      selectectAnalysisIdOptions <- NULL

      if (hasData(temporalAnalysisRef)) {
        analysisRefs <- temporalAnalysisRef %>%
          dplyr::select("analysisName", "analysisId") %>%
          dplyr::distinct() %>%
          dplyr::arrange(.data$analysisName)

        analysisIdOptions <- analysisRefs$analysisId
        names(analysisIdOptions) <- analysisRefs$analysisName
        selectectAnalysisIdOptions <- temporalAnalysisRef$analysisId
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "selectedRawAnalysisIds",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = analysisIdOptions,
        selected = selectectAnalysisIdOptions
      )
    })

    ### characterizationDomainNameFilter ----
    shiny::observe({
      characterizationDomainOptionsUniverse <- NULL
      charcterizationDomainOptionsSelected <- NULL

      if (hasData(temporalAnalysisRef)) {
        characterizationDomainOptionsUniverse <- domainIdOptions
        charcterizationDomainOptionsSelected <- temporalAnalysisRef %>%
          dplyr::pull("domainId") %>%
          unique()
      }

      shinyWidgets::updatePickerInput(
        session = session,
        inputId = "characterizationDomainIdFilter",
        choicesOpt = list(style = rep_len("color: black;", 999)),
        choices = characterizationDomainOptionsUniverse,
        selected = charcterizationDomainOptionsSelected
      )
    })

    getPrettyCharacterizationData <- shiny::reactive({
      data <- dataSource$connectionHandler$queryDb(
        sql = "SELECT tcv.*, ref.analysis_id, ref.covariate_name
                FROM @schema.@table_name tcv
                INNER JOIN @schema.@ref_table_name ref ON ref.covariate_id = tcv.covariate_id
                WHERE ref.covariate_id IS NOT NULL
                {@analysis_ids != \"\"} ? { AND ref.analysis_id IN (@analysis_ids)}
                {@cohort_id != \"\"} ? { AND tcv.cohort_id IN (@cohort_id)}
                {@time_id != \"\"} ? { AND (time_id IN (@time_id) OR time_id IS NULL OR time_id = 0)}
                {@use_database_id} ? { AND database_id IN (@database_id)}
                {@filter_mean_threshold != \"\"} ? { AND tcv.mean > @filter_mean_threshold};",
        snakeCaseToCamelCase = TRUE,
        analysis_ids = prettyTableAnalysisIds,
        time_id = characterizationTimeIdChoices$timeId,
        use_database_id = !is.null(selectedDatabaseIds()),
        database_id = quoteLiterals(selectedDatabaseIds()),
        table_name = dataSource$prefixTable("temporal_covariate_value"),
        ref_table_name = dataSource$prefixTable("temporal_covariate_ref"),
        cohort_id = targetCohortId(),
        schema = dataSource$schema,
        filter_mean_threshold = 0.0
      ) %>%
        dplyr::tibble() %>%
        tidyr::replace_na(replace = list(timeId = -1))
      data
    })

    ## cohortCharacterizationPrettyTable ----
    cohortCharacterizationPrettyTable <- shiny::eventReactive(input$generateReport, {
      data <- getPrettyCharacterizationData()
      if (!hasData(data)) {
        return(NULL)
      }

      if (!hasData(data)) {
        return(NULL)
      }

      data <- data %>%
        dplyr::select(
          "cohortId",
          "databaseId",
          "analysisId",
          "covariateId",
          "covariateName",
          "mean"
        ) %>%
        dplyr::rename(sumValue = "mean")


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

      displayTable <- getDisplayTableGroupedByDatabaseId(
        data = table,
        databaseTable = databaseTable,
        headerCount = countsForHeader,
        keyColumns = keyColumnFields,
        countLocation = countLocation,
        dataColumns = dataColumnFields,
        showDataAsPercent = TRUE,
        sort = FALSE,
        pageSize = 100
      )
      return(displayTable)
    })

    ## Output: characterizationTable ----
    output$characterizationTable <- reactable::renderReactable(expr = {
      data <- cohortCharacterizationPrettyTable()
      shiny::validate(shiny::need(hasData(data), "No data for selected combination"))
      return(data)
    })

    # Params when user presses button
    inputButtonParams <- shiny::eventReactive(input$generateRaw, {
      conceptIds <- getFilteredConceptIds()
      if (is.null(conceptIds) || is.na(conceptIds)) {
        conceptIds <- ""
      }

      binary <- ""
      if (input$proportionOrContinuous == "Proportion") {
        binary <- "y"
      } else if (input$proportionOrContinuous == "Continuous") {
        binary <- "n"
      }

      list(
        analysis_ids = input$selectedRawAnalysisIds %>% unique(),
        time_id = selectedTimeIds() %>% unique(),
        domain_ids = quoteLiterals(input$characterizationDomainIdFilter %>% unique()),
        table_prefix = dataSource$cdTablePrefix,
        cohort_id = targetCohortId(),
        results_database_schema = dataSource$schema,
        database_id = quoteLiterals(selectedDatabaseIds()),
        is_binary = binary,
        concept_ids = conceptIds,
        time_invariant_search = -1 %in% selectedTimeIds()
      )
    })

    getSearchStr <- shiny::reactive({
      if (input$generalSearchString == "" ||
        is.na(input$generalSearchString) ||
        is.null(input$generalSearchString))
        return('')

      return(input$generalSearchString)
    })

    getOrderbyCol <- shiny::reactive({
      input$sortByRaw
    })

    # params with default reactive behaviour
    inputParamsRaw <- shiny::reactive({
      params <- inputButtonParams()
      params$search_str <- getSearchStr()
      params$order_by_col <- getOrderbyCol()
      params$order_desc <- input$shortByRawAsc == "DESC"
      params$use_database_id <- FALSE
      return(params)
    })

    getSearchStrTemporal <- shiny::reactive({
      if (input$generalSearchStringTemporal == "" ||
        is.na(input$generalSearchStringTemporal) ||
        is.null(input$generalSearchStringTemporal))
        return('')

      return(input$generalSearchStringTemporal)
    })

    getOrderbyColTemporal <- shiny::reactive({
      input$sortByRawTemporal
    })


    # params with default reactive behaviour
    inputParamsRawTemporal <- shiny::reactive({
      params <- inputButtonParams()
      params$search_str <- getSearchStrTemporal()
      params$order_by_col <- getOrderbyColTemporal()
      params$order_desc <- input$shortByRawAscTemporal == "DESC"
      params$time_id <- ""
      params$use_database_id <- TRUE
      params$database_table <- dataSource$databaseTable
      params$database_table_prefix <- dataSource$databaseTablePrefix
      params$database_name <- ifelse(tolower(dataSource$databaseTable) == 'database_meta_data', 'cdm_source_name', 'database_name')
      return(params)
    })

    # Set real query from dynamic data
    # The following is a lot of dynamically generated sql to create a pivoted table to allow
    # Side by side  view af covariate means
    shiny::observeEvent(input$generateRaw, {
      databaseIds <- selectedDatabaseIds()
      dbId1 <- databaseIds[1]
      if (!is.null(dbId1)) {
        columnDefinitions <- list(
          covariateName = reactable::colDef(name = "Covariate Name", minWidth = 200),
          analysisName = reactable::colDef(name = "Analysis Name"),
          temporalChoices = reactable::colDef(name = "Temporal Choices"),
          conceptId = reactable::colDef(name = "Concept Id"),
          isBinary = reactable::colDef(show = FALSE)
        )

        columnGroups <- list()

        sortChoices <- list(
          "Concept Id" = "tcr.concept_id",
          "Analysis Name" = "tar.analysis_name",
          "Covariate Name" = "tcr.covariate_name",
          "Temporal Choices" = "ttr.time_id"
        )

        for (i in 1:length(databaseIds)) {
          dbi <- databaseIds[i]
          columnIdent <- paste0("mean", i)
          columnDefinitions[[columnIdent]] <- reactable::colDef(name = "Mean",
                                                                cell = formatCellByBinaryType())


          columnIdentSd <- paste0("sd", i)
          columnDefinitions[[columnIdentSd]] <- reactable::colDef(name = "sd",
                                                                  show = input$characterizationColumnFilters == "Mean and Standard Deviation",
                                                                  cell = formatDataCellValueInDisplayTable(showDataAsPercent = FALSE))
          groupCols <- c(columnIdent)
          if (input$characterizationColumnFilters == "Mean and Standard Deviation")
            groupCols <- c(columnIdent, columnIdentSd)


          databaseName <- databaseTable %>%
            dplyr::filter(.data$databaseId == dbi) %>%
            dplyr::select("databaseName") %>%
            dplyr::pull()

          columnGroups[[length(columnGroups) + 1]] <- reactable::colGroup(name = databaseName,
                                                                          columns = groupCols,
                                                                          align = "center")

          sortChoices[[paste(databaseName, "mean")]] <- paste0("mean", i)
        }
        shiny::updateSelectInput(inputId = "sortByRaw", choices = sortChoices, selected = "mean1")

        sql <- "
          SELECT @select_stament

          FROM @results_database_schema.@table_prefixtemporal_covariate_ref tcr
          INNER JOIN @results_database_schema.@table_prefixtemporal_analysis_ref tar ON tar.analysis_id = tcr.analysis_id
          LEFT JOIN @results_database_schema.@table_prefixtemporal_covariate_value tcv ON (
            tcr.covariate_id = tcv.covariate_id
          )
          LEFT JOIN @results_database_schema.@table_prefixtemporal_time_ref ttr ON ttr.time_id = tcv.time_id
          WHERE tcr.covariate_id IS NOT NULL
          "

        selectSt <- "
          tcr.covariate_name,
          tar.analysis_name,
          CASE
            WHEN ttr.start_day IS NULL THEN 'Time Invariant'
            ELSE CONCAT('T (', ttr.start_day, 'd to ', ttr.end_day, 'd)')
          END as temporal_choices,
          tcr.concept_id,
          is_binary,
          "

        selectTemplate <-
          "
           MAX(CASE WHEN tcv.database_id = '@db_id_i' THEN tcv.mean END) AS mean@i,
           MAX(CASE WHEN tcv.database_id = '@db_id_i' THEN tcv.sd END) AS sd@i"

        havingTemplate <- " MAX(CASE WHEN tcv.database_id = '@db_id_i' THEN tcv.mean END) IS NOT NULL"

        tplSql <- c()
        havingSql <- c()
        # Select casees for each db
        for (i in 1:length(databaseIds)) {
          dbIdi <- databaseIds[i]
          tplSql <- c(tplSql, SqlRender::render(selectTemplate, i = i, db_id_i = dbIdi))
          havingSql <- c(havingSql, SqlRender::render(havingTemplate, db_id_i = dbIdi))
        }

        tplSql <- paste(tplSql, collapse = ",\n")

        selectSt <- paste(selectSt, tplSql)

        paramSql <-
          "
          {DEFAULT @order_by_col = tcr.covariate_name}
          {DEFAULT @order_desc = TRUE}
          {@analysis_ids != \"\"} ? { AND tcr.analysis_id IN (@analysis_ids)}
          {@domain_ids != \"\"} ? { AND tar.domain_id IN (@domain_ids)}
          {@cohort_id != \"\"} ? { AND tcv.cohort_id IN (@cohort_id)}
          {@time_id != \"\"} ? { AND (ttr.time_id IN (@time_id) {@time_invariant_search} ? {OR ttr.time_id IS NULL OR ttr.time_id = 0})}
          {@use_database_id} ? { AND tcv.database_id IN (@database_id)}
          {@is_binary != ''} ? {AND  lower(is_binary) = '@is_binary'}
          {@concept_ids != ''} ? {AND  tcr.concept_id IN (@concept_ids)}
          {@search_str != ''} ? {AND lower(CONCAT(tcr.covariate_name, tar.analysis_name, tcr.concept_id)) LIKE lower('%@search_str%')}
        "

        grpSql <- SqlRender::render("\n\tGROUP BY tcr.covariate_name, tar.analysis_name, tcr.concept_id, is_binary, ttr.start_day, ttr.end_day
        HAVING @having_clasuse
        ", having_clasuse = paste(havingSql, collapse = " OR\n"))

        orderClause <- "{@order_by_col != ''} ? {ORDER BY @order_by_col {@order_desc} ? {DESC} : {ASC}}"

        baseQuery <- SqlRender::render(sql,
                                       select_stament = selectSt,
                                       warnOnMissingParameters = FALSE)


        baseQuery <- paste(baseQuery, paramSql, grpSql, orderClause)
        ldt <- LargeDataTable$new(connectionHandler = dataSource$connectionHandler,
                                  baseQuery = baseQuery)

        largeTableServer(id = "rawCharTbl",
                         ldt,
                         inputParams = inputParamsRaw,
                         columns = columnDefinitions,
                         columnGroups = columnGroups)
      }

      timeIds <- selectedTimeIds()
      if (length(timeIds) > 0) {

        columnDefinitionsT <- list(
          covariateName = reactable::colDef(name = "Covariate Name", minWidth = 200),
          analysisName = reactable::colDef(name = "Analysis Name"),
          temporalChoices = reactable::colDef(name = "Temporal Choices"),
          conceptId = reactable::colDef(name = "Concept Id"),
          isBinary = reactable::colDef(show = FALSE)
        )

        columnGroupsT <- list()

        sortChoices <- list(
          "Concept Id" = "tcr.concept_id",
          "Analysis Name" = "tar.analysis_name",
          "Covariate Name" = "tcr.covariate_name",
          "Database" = "db.database_name"
        )

        for (i in 1:length(timeIds)) {
          timeIdi <- timeIds[i]
          columnIdent <- paste0("mean", i)
          columnDefinitionsT[[columnIdent]] <- reactable::colDef(name = "Mean",
                                                                 cell = formatCellByBinaryType())


          columnIdentSd <- paste0("sd", i)
          columnDefinitionsT[[columnIdentSd]] <- reactable::colDef(name = "sd",
                                                                   show = input$characterizationColumnFilters == "Mean and Standard Deviation",
                                                                   cell = formatDataCellValueInDisplayTable(showDataAsPercent = FALSE))
          groupCols <- c(columnIdent)
          if (input$characterizationColumnFilters == "Mean and Standard Deviation")
            groupCols <- c(columnIdent, columnIdentSd)


          temporalChoiceName <- timeIdOptions %>%
            dplyr::distinct() %>%
            dplyr::filter(.data$timeId == timeIdi) %>%
            dplyr::pull("temporalChoices")

          columnGroupsT[[length(columnGroupsT) + 1]] <- reactable::colGroup(name = temporalChoiceName,
                                                                            columns = groupCols,
                                                                            align = "center")

          sortChoices[[paste(temporalChoiceName, "mean")]] <- paste0("mean", i)
        }

        shiny::updateSelectInput(inputId = "sortByRawTemporal", choices = sortChoices, selected = "mean1")

        sqlt <- "
          SELECT @select_stament

          FROM @results_database_schema.@table_prefixtemporal_covariate_ref tcr
          INNER JOIN @results_database_schema.@table_prefixtemporal_analysis_ref tar ON tar.analysis_id = tcr.analysis_id
          INNER JOIN @results_database_schema.@table_prefixtemporal_covariate_value tcv ON tcr.covariate_id = tcv.covariate_id
          INNER JOIN @results_database_schema.@database_table_prefix@database_table db ON db.database_id = tcv.database_id
          WHERE tcr.covariate_id IS NOT NULL
        "

        selectSt <- "db.@database_name as database_name,
            tcr.covariate_name,
            tar.analysis_name,
            is_binary,
            tcr.concept_id,"

        selectTemplate <-
          "MAX(CASE WHEN tcv.time_id = @time_id {@time_id == -1} ? {OR tcv.time_id = 0 OR tcv.time_id IS NULL} THEN tcv.mean END) AS mean@i,
           MAX(CASE WHEN tcv.time_id = @time_id {@time_id == -1} ? {OR tcv.time_id = 0 OR tcv.time_id IS NULL} THEN tcv.sd END) AS sd@i"

        havingTemplate <-
          "MAX(CASE WHEN tcv.time_id = @time_id {@time_id == -1} ? {OR tcv.time_id = 0 OR tcv.time_id IS NULL} THEN tcv.mean END) IS NOT NULL"

        havingSql <- c()
        tplSql <- c()
        # Select casees for each db
        for (i in 1:length(timeIds)) {
          timeId <- timeIds[i]
          tplSql <- c(tplSql, SqlRender::render(
            sql = selectTemplate, 
            i = i, 
            time_id = timeId)
            )
          havingSql <- c(havingSql, SqlRender::render(
            sql = havingTemplate, 
            time_id = timeId
            )
            )
        }

        tplSql <- paste(tplSql, collapse = ", \n")
        groupClause <- SqlRender::render("
        GROUP BY db.@database_name, tcr.covariate_name, tar.analysis_name, tcr.concept_id, is_binary
        HAVING @having_clasuse
        ", having_clasuse = paste(havingSql, collapse = " OR\n"))

        orderClause <- "{@order_by_col != ''} ? {ORDER BY @order_by_col {@order_desc} ? {DESC} : {ASC}}"

        baseQueryTemporal <- SqlRender::render(sqlt,
                                               select_stament = paste(selectSt, tplSql),
                                               warnOnMissingParameters = FALSE)

        baseQueryTemporal <- paste(baseQueryTemporal, paramSql, groupClause, orderClause)
        ldtTemporal <- LargeDataTable$new(connectionHandler = dataSource$connectionHandler,
                                          baseQuery = baseQueryTemporal)

        ## do the same for the temporal table
        rawCharTblTemporal <- largeTableServer(id = "rawCharTblTemporal",
                                               ldtTemporal,
                                               inputParams = inputParamsRawTemporal,
                                               columns = columnDefinitionsT,
                                               columnGroups = columnGroupsT)
      }
    })
  })
}
