shiny::shinyServer(function(input, output, session) {
  cohortId <- shiny::reactive(x = {
    return(cohort$cohortId[cohort$compoundName == input$cohort])
  })
  
  cohortIds <- shiny::reactive(x = {
    return(cohort$cohortId[cohort$compoundName  %in% input$cohorts])
  })
  
  timeId <- shiny::reactive(x = {
    return(
      temporalCovariateChoices %>%
        dplyr::filter(choices %in% input$timeIdChoices) %>%
        dplyr::pull(timeId)
    )
  })
  
  phenotypeId <- shiny::reactive(x = {
    return(phenotypeDescription$phenotypeId[phenotypeDescription$phenotypeName == input$phenotypes])
  })
  
  if (exists(x = "phenotypeDescription")) {
    shiny::observe(x = {
      idx <- which(phenotypeDescription$phenotypeName == input$phenotypes)
      shiny::isolate({
        proxy <- DT::dataTableProxy(
          outputId = "phenoTypeDescriptionTable",
          session = session,
          deferUntilFlush = FALSE
        )
        DT::selectRows(proxy, idx)
        DT::selectPage(
          proxy,
          which(input$phenoTypeDescriptionTable_rows_all == idx) %/%
            input$phenoTypeDescriptionTable_state$length + 1
        )
      })
    })
  }
  
  cohortSubset <- shiny::reactive(x = {
    if (exists(x = "phenotypeDescription")) {
      return(
        cohort %>%
          dplyr::filter(.data$phenotypeId == phenotypeId()) %>%
          dplyr::arrange(.data$cohortId)
      )
    } else {
      return(cohort %>%
               dplyr::arrange(.data$cohortId))
    }
  })
  
  phenotypeSubset <- shiny::reactive(x = {
    if (exists(x = "phenotypeDescription")) {
      return(phenotypeDescription %>%
               dplyr::arrange(.data$phenotypeId))
    }
  })
  
  shiny::observe(x = {
    subset <-
      unique(conceptSets$conceptSetName[conceptSets$cohortId == cohortId()]) %>% sort()
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "conceptSet",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset
    )
  })
  
  shiny::observe(x = {
    subset <- cohortSubset()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "cohort",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset
    )
  })
  
  shiny::observe(x = {
    subset <- cohortSubset()$compoundName
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "cohorts",
      choicesOpt = list(style = rep_len("color: black;", 999)),
      choices = subset,
      selected = c(subset[1], subset[2])
    )
  })
  
  # Phenotype Description ------------------------------------------------------------------------------
  output$phenoTypeDescriptionTable <- DT::renderDT(expr = {
    data <- phenotypeDescription %>%
      dplyr::select(
        .data$phenotypeId,
        .data$phenotypeName,
        .data$overview,
        .data$cohortDefinitions
      )
    dataTable <- standardDataTable(data = data)
    return(dataTable)
  }, server = TRUE)
  
  selectedPhenotypeDescriptionRow <- reactive({
    idx <- input$phenoTypeDescriptionTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      row <- phenotypeDescription[idx, ]
      return(row)
    }
  })
  
  output$phenotypeRowIsSelected <- reactive({
    return(!is.null(selectedPhenotypeDescriptionRow()))
  })
  outputOptions(x = output,
                name = "phenotypeRowIsSelected",
                suspendWhenHidden = FALSE)
  
  output$phenotypeDescriptionText <- shiny::renderUI(expr = {
    row <- selectedPhenotypeDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      text <-  row$clinicalDescription
      
      referentConcept <-
        getConceptDetails(dataSource, row$phenotypeId / 1000)
      if (nrow(referentConcept) > 0) {
        text <-
          paste(
            sprintf(
              "<strong>Referent concept: </strong>%s (concept ID: %s)<br/><br/>",
              referentConcept$conceptName,
              referentConcept$conceptId
            ),
            text
          )
      }
      shiny::HTML(text)
    }
  })
  
  output$phenotypeLiteratureReviewText <- shiny::renderUI(expr = {
    row <- selectedPhenotypeDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      files <-
        listFilesInGitHub(phenotypeId = row$phenotypeId,
                          subFolder = "literature")
      if (nrow(files) == 0) {
        return("Nothing here (yet)")
      } else {
        return(HTML(paste(files$html, sep = "<br/>")))
      }
    }
  })
  
  output$phenotypeEvaluationText <- shiny::renderUI(expr = {
    row <- selectedPhenotypeDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      files <-
        listFilesInGitHub(phenotypeId = row$phenotypeId,
                          subFolder = "evaluation")
      if (nrow(files) == 0) {
        return("Nothing here (yet)")
      } else {
        return(HTML(paste(files$html, sep = "<br/>")))
      }
    }
  })
  
  output$phenotypeNotesText <- shiny::renderUI(expr = {
    row <- selectedPhenotypeDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      files <-
        listFilesInGitHub(phenotypeId = row$phenotypeId,
                          subFolder = "notes")
      if (nrow(files) == 0) {
        return("Nothing here (yet)")
      } else {
        return(HTML(paste(files$html, sep = "<br/>")))
      }
    }
  })
  
  observeEvent(input$selectPhenotypeButton, {
    shinyWidgets::updatePickerInput(
      session = session,
      inputId = "phenotypes",
      selected = selectedPhenotypeDescriptionRow()$phenotypeName
    )
  })
  
  # Cohort Definition ---------------------------------------------------------
  output$cohortDefinitionTable <- DT::renderDT(expr = {
    data <- cohortSubset() %>%
      dplyr::select(.data$cohortId, .data$cohortName)
    dataTable <-
      standardDataTable(data = data, selectionMode = 'multiple')
    return(dataTable)
  }, server = TRUE)
  
  selectedCohortDefinitionRow <- reactive({
    idx <- input$cohortDefinitionTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      subset <- cohortSubset()
      if (nrow(subset) == 0) {
        return(NULL)
      }
      row <- subset[idx[1],]
      return(row)
    }
  })
  
  compareCohortDefinitionRow <- reactive({
    idx <- input$cohortDefinitionTable_rows_selected
    if (is.null(idx) && length(idx) != 2) {
      return(NULL)
    } else {
      subset <- cohortSubset()
      if (nrow(subset) == 0) {
        return(NULL)
      }
      row <- subset[idx[2],]
      return(row)
    }
  })
  
  observeEvent(length(input$cohortDefinitionTable_rows_selected) != 2, {
    shinyWidgets::updatePickerInput(session = session,
                                    inputId = "isCompare",
                                    selected = "No Comparision")
  })
  
  output$cohortDefinitionRowIsSelected <- reactive({
    return(length(input$cohortDefinitionTable_rows_selected))
  })
  
  
  outputOptions(x = output,
                name = "cohortDefinitionRowIsSelected",
                suspendWhenHidden = FALSE)
  
  output$cohortDetailsText <- shiny::renderUI(expr = {
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      # for(i in 1:nrow(row))
      # {
      tags$table(
        style = "margin-top: 5px;",
        tags$tr(
          tags$td(tags$strong("Cohort ID: ")),
          tags$td(HTML("&nbsp;&nbsp;")),
          tags$td(row$cohortId)
        ),
        tags$tr(
          tags$td(tags$strong("Cohort Name: ")),
          tags$td(HTML("&nbsp;&nbsp;")),
          tags$td(row$cohortName)
        ),
        tags$tr(
          tags$td(tags$strong("Logic: ")),
          tags$td(HTML("&nbsp;&nbsp;")),
          tags$td(row$logicDescription)
        )
      )
      # }
    }
  })
  
  output$cohortDefinitionDetails <- shiny::renderUI(expr = {
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      cohortExtra %>%
        dplyr::filter(.data$cohortId == row$cohortId) %>%
        dplyr::pull(.data$html) %>%
        shiny::HTML()
    }
  })
  
  output$cohortDefinitionJson <- shiny::renderText({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$json
    }
  })
  
  output$cohortDefinitionSql <- shiny::renderText({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$sql
    }
  })
  
  cohortDefinitionConceptSets <- reactive({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    }
    if (is(dataSource, "environment") ||
        input$conceptSetsType == "Concept Set Expression") {
      expression <- RJSONIO::fromJSON(row$json)
      if (is.null(expression$ConceptSets)) {
        return(NULL)
      }
      
      doItem <- function(item) {
        row <- dplyr::as_tibble(item$concept)
        colnames(row) <- snakeCaseToCamelCase(colnames(row))
        row$isExcluded <- item$isExcluded
        row$includeDescendants <- item$includeDescendants
        row$includeMapped <- item$includeMapped
        return(row)
      }
      doConceptSet <- function(conceptSet) {
        rows <- lapply(conceptSet$expression$items, doItem) %>%
          dplyr::bind_rows()
        rows$conceptSetName <- rep(conceptSet$name, nrow(rows))
        return(rows)
      }
      data <- lapply(expression$ConceptSet, doConceptSet) %>%
        dplyr::bind_rows()
      data <- data %>%
        dplyr::select(
          .data$conceptSetName,
          .data$conceptId,
          .data$conceptCode,
          .data$conceptName,
          .data$domainId,
          .data$standardConcept,
          .data$isExcluded,
          .data$includeDescendants,
          .data$includeMapped
        ) %>%
        dplyr::arrange(.data$conceptSetName, .data$conceptId)
      colnames(data) <- camelCaseToTitleCase(colnames(data))
    } else {
      subset <- conceptSets %>%
        dplyr::filter(.data$cohortId == row$cohortId)
      if (nrow(subset) == 0) {
        return(NULL)
      }
      source <-
        (input$conceptSetsType == "Included Source Concepts")
      data <-
        resolveConceptSet(dataSource = dataSource, subset, source = source)
      data <- data %>%
        dplyr::inner_join(subset, by = "conceptSetId") %>%
        dplyr::select(
          .data$conceptSetName,
          .data$conceptId,
          .data$conceptCode,
          .data$conceptName,
          .data$conceptClassId,
          .data$domainId,
          .data$vocabularyId,
          .data$standardConcept
        ) %>%
        dplyr::arrange(.data$conceptSetName, .data$conceptId)
      colnames(data) <- camelCaseToTitleCase(colnames(data))
    }
    return(data)
    
  })
  
  output$cohortDefinitionConceptSetsTable <-
    DT::renderDT(expr = {
      data <- cohortDefinitionConceptSets()
      if (is.null(data)) {
        return(NULL)
      }
      dataTable <- standardDataTable(data = data)
      return(dataTable)
    })
  
  output$compareCohortDetailsText <- shiny::renderUI(expr = {
    row <- compareCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      # for(i in 1:nrow(row))
      # {
      tags$table(
        style = "margin-top: 5px;",
        tags$tr(
          tags$td(tags$strong("Cohort ID: ")),
          tags$td(HTML("&nbsp;&nbsp;")),
          tags$td(row$cohortId)
        ),
        tags$tr(
          tags$td(tags$strong("Cohort Name: ")),
          tags$td(HTML("&nbsp;&nbsp;")),
          tags$td(row$cohortName)
        ),
        tags$tr(
          tags$td(tags$strong("Logic: ")),
          tags$td(HTML("&nbsp;&nbsp;")),
          tags$td(row$logicDescription)
        )
      )
      # }
    }
  })
  
  output$compareCohortDefinitionDetails <- shiny::renderUI(expr = {
    row <- compareCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      cohortExtra %>%
        dplyr::filter(.data$cohortId == row$cohortId) %>%
        dplyr::pull(.data$html) %>%
        shiny::HTML()
    }
  })
  
  output$compareCohortDefinitionJson <- shiny::renderText({
    row <- compareCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$json
    }
  })
  
  output$compareCohortDefinitionSql <- shiny::renderText({
    row <- compareCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$sql
    }
  })
  
  compareCohortDefinitionConceptSets <- reactive({
    row <- compareCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    }
    if (is(dataSource, "environment") ||
        input$compareConceptSetsType == "Concept Set Expression") {
      expression <- RJSONIO::fromJSON(row$json)
      if (is.null(expression$ConceptSets)) {
        return(NULL)
      }
      
      doItem <- function(item) {
        row <- dplyr::as_tibble(item$concept)
        colnames(row) <- snakeCaseToCamelCase(colnames(row))
        row$isExcluded <- item$isExcluded
        row$includeDescendants <- item$includeDescendants
        row$includeMapped <- item$includeMapped
        return(row)
      }
      doConceptSet <- function(conceptSet) {
        rows <- lapply(conceptSet$expression$items, doItem) %>%
          dplyr::bind_rows()
        rows$conceptSetName <- rep(conceptSet$name, nrow(rows))
        return(rows)
      }
      data <- lapply(expression$ConceptSet, doConceptSet) %>%
        dplyr::bind_rows()
      data <- data %>%
        dplyr::select(
          .data$conceptSetName,
          .data$conceptId,
          .data$conceptCode,
          .data$conceptName,
          .data$domainId,
          .data$standardConcept,
          .data$isExcluded,
          .data$includeDescendants,
          .data$includeMapped
        ) %>%
        dplyr::arrange(.data$conceptSetName, .data$conceptId)
      colnames(data) <- camelCaseToTitleCase(colnames(data))
    } else {
      subset <- conceptSets %>%
        dplyr::filter(.data$cohortId == row$cohortId)
      if (nrow(subset) == 0) {
        return(NULL)
      }
      source <-
        (input$conceptSetsType == "Included Source Concepts")
      data <-
        resolveConceptSet(dataSource = dataSource, subset, source = source)
      data <- data %>%
        dplyr::inner_join(subset, by = "conceptSetId") %>%
        dplyr::select(
          .data$conceptSetName,
          .data$conceptId,
          .data$conceptCode,
          .data$conceptName,
          .data$conceptClassId,
          .data$domainId,
          .data$vocabularyId,
          .data$standardConcept
        ) %>%
        dplyr::arrange(.data$conceptSetName, .data$conceptId)
      colnames(data) <- camelCaseToTitleCase(colnames(data))
    }
    return(data)
    
  })
  
  output$compareCohortDefinitionConceptSetsTable <-
    DT::renderDT(expr = {
      data <- compareCohortDefinitionConceptSets()
      if (is.null(data)) {
        return(NULL)
      }
      dataTable <- standardDataTable(data)
      return(dataTable)
    })
  
  output$compareSaveConceptSetButton <- downloadHandler(
    filename = function() {
      paste("conceptSet-", Sys.Date(), ".csv", sep = "")
    },
    content = function(file) {
      data <- compareCohortDefinitionConceptSets()
      write.csv(data, file)
    }
  )
  
  output$detailsDiff <- diffr::renderDiffr({
    row1 <- selectedCohortDefinitionRow()
    row2 <- compareCohortDefinitionRow()
    file1 <- tempfile()
    writeLines(row1$logicDescription, con = file1)
    file2 <- tempfile()
    writeLines(row2$logicDescription, con = file2)
    detailsDiffOutput <- diffr::diffr(
      file1,
      file2,
      wordWrap = TRUE,
      before = row1$compoundName,
      after = row2$compoundName
    )
    unlink(file1)
    unlink(file2)
    return(detailsDiffOutput)
  })
  
  output$concesptSetDiff <- DT::renderDT(expr = {
    data1 <- cohortDefinitionConceptSets()
    data2 <- compareCohortDefinitionConceptSets()
    data <-
      data1 %>% dplyr::semi_join(data2,
                                 by = c(
                                   "Concept Id",
                                   "Is Excluded",
                                   "Include Descendants",
                                   "Include Mapped"
                                 ))
    if (is.null(data)) {
      return(NULL)
    }
    dataTable <- standardDataTable(data)
    return(dataTable)
  })
  
  output$jsonDiff <- diffr::renderDiffr({
    row1 <- selectedCohortDefinitionRow()
    row2 <- compareCohortDefinitionRow()
    file1 <- tempfile()
    writeLines(row1$json, con = file1)
    file2 <- tempfile()
    writeLines(row2$json, con = file2)
    jsonDiffOutput <- diffr::diffr(
      file1,
      file2,
      wordWrap = TRUE,
      before = row1$compoundName,
      after = row2$compoundName
    )
    unlink(file1)
    unlink(file2)
    return(jsonDiffOutput)
  })
  
  output$sqlDiff <- diffr::renderDiffr({
    row1 <- selectedCohortDefinitionRow()
    row2 <- compareCohortDefinitionRow()
    file1 <- tempfile()
    writeLines(row1$sql, con = file1)
    file2 <- tempfile()
    writeLines(row2$sql, con = file2)
    sqlDiffOutput <- diffr::diffr(
      file1,
      file2,
      wordWrap = FALSE,
      before = row1$compoundName,
      after = row2$compoundName,
      width = "100%"
    )
    unlink(file1)
    unlink(file2)
    return(sqlDiffOutput)
  })
  
  
  # Cohort Counts ---------------------------------------------------------------------------
  output$cohortCountsTable <- DT::renderDT(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getCohortCountResult(
      dataSource = dataSource,
      databaseIds = input$databases,
      cohortIds = cohortIds()
    )
    data <- addMetaDataInformationToResults(data)
    dataTable <- standardDataTable(data = data)
    return(dataTable)
  }, server = TRUE)
  
  
  
  
  # Incidence rate --------------------------------------------------------------------------------
  incidenceRateDataFromRemote <- shiny::reactive(x = {
    if (length(cohortIds()) > 0) {
      data <- getIncidenceRateResult(
        dataSource = dataSource,
        cohortIds = cohortIds(),
        minPersonYears = 0
      )
    } else {
      data <- tidyr::tibble()
    }
    return(data)
  })
  
  shiny::observe(x = {
    if (nrow(incidenceRateDataFromRemote()) > 0) {
    ageFilter <- incidenceRateDataFromRemote()$ageGroup %>% unique()
    ageFilter <- ageFilter[!ageFilter == 'All']
      shiny::updateSelectizeInput(
        session = session,
        inputId = "incidenceRateAgeFilter",
        label = "Age",
        selected = ageFilter,
        choices = ageFilter,
        server = TRUE
      )
      genderFilter <- incidenceRateDataFromRemote()$gender %>% unique()
      genderFilter <- genderFilter[!genderFilter == 'All']
      shiny::updateSelectizeInput(
        session = session,
        inputId = "incidenceRateGenderFilter",
        label = "Gender",
        selected = genderFilter,
        choices = genderFilter,
        server = TRUE
      )
      calendarFilter <- incidenceRateDataFromRemote()$calendarYear %>% unique()
      calendarFilter <- calendarFilter[!calendarFilter == 'All']
      shiny::updateSelectizeInput(
        session = session,
        inputId = "incidenceRateCalenderFilter",
        label = "Calendar Year",
        selected = calendarFilter,
        choices = calendarFilter,
        server = TRUE
      )
    }
  })
  
  incidenceRateDataFiltered <- reactive({
    if (all(is.null(input$incidenceRateGenderFilter),
            is.null(input$incidenceRateAgeFilter),
            is.null(input$incidenceRateCalenderFilter))) {
      return(dplyr::tibble())
    }
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    
    data <- incidenceRateDataFromRemote() %>%
      dplyr::filter(.data$databaseId %in% input$databases)
    
    if (stratifyByAge) {
      data <- data %>% 
        dplyr::filter(.data$ageGroup %in% input$incidenceRateAgeFilter)
    }
    if (stratifyByCalendarYear) {
      data <- data %>% 
        dplyr::filter(.data$calendarYear %in% input$incidenceRateCalenderFilter)
    }
    if (stratifyByGender) {
      data <- data %>% 
        dplyr::filter(.data$gender %in% input$incidenceRateGenderFilter)
    }
    return(data)
  })
  
  
  output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
    data <- incidenceRateDataFiltered()
    validate(need(nrow(data) > 0, paste0("No data for this combination")))
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <-
      "Calendar Year" %in% input$irStratification
    plot <- plotIncidenceRate(
      data = data,
      shortNameRef = cohort,
      stratifyByAgeGroup = stratifyByAge,
      stratifyByGender = stratifyByGender,
      stratifyByCalendarYear = stratifyByCalendarYear,
      yscaleFixed = input$irYscaleFixed,
      minPersonYears = 1000
    )
    return(plot)
  })
  
  output$incidenceRateTable <- DT::renderDT(expr = {
    data <- incidenceRateDataFiltered()
    if (nrow(data) > 0) {
      data <- addMetaDataInformationToResults(data)
      colnames(data) <-
        colnames(data) %>% stringr::str_replace_all(string = .,
                                                    pattern = "Value",
                                                    replacement = "")
    }
    table <- standardDataTable(data)
    return(table)
  }, server = TRUE)
  
  # Time distribution -----------------------------------------------------------------------------
  timeDist <- reactive({
    data <- getTimeDistributionResult(
      dataSource = dataSource,
      cohortIds = cohortIds(),
      databaseIds = input$databases
    )
    return(data)
  })
  
  output$timeDisPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    data <- timeDist()
    validate(need(nrow(data) > 0, paste0("No data for this combination")))
    
    plot <- plotTimeDistribution(data = data, shortNameRef = cohort)
    return(plot)
  })
  
  output$timeDistTable <- DT::renderDT(expr = {
    data <- timeDist()
    data <- data %>% dplyr::relocate(.data$timeMetric)
    data <- addMetaDataInformationToResults(data)
    colnames(data) <-
      colnames(data) %>% stringr::str_replace_all(string = .,
                                                  pattern = "Value",
                                                  replacement = "")
    table <- standardDataTable(data)
    return(table)
  }, server = TRUE)
  
  # included concepts table --------------------------------------------------------------------------
  # output$includedConceptsTable <- DT::renderDT(expr = {
  #   validate(need(length(input$databases) > 0, "No data sources chosen"))
  #   data <- getIncludedConceptResult(
  #     dataSource = dataSource,
  #     cohortId = cohortId(),
  #     databaseIds = input$databases
  #   )
  #   data <- data %>%
  #     dplyr::filter(.data$conceptSetName == input$conceptSet)
  #   if (nrow(data) == 0) {
  #     return(dplyr::tibble("No data available for selected databases and cohorts"))
  #   }
  #
  #   databaseIds <- unique(data$databaseId)
  #
  #   if (!all(input$databases %in% databaseIds)) {
  #     return(dplyr::tibble(
  #       Note = paste0(
  #         "There is no data for the databases:\n",
  #         paste0(setdiff(input$databases, databaseIds),
  #                collapse = ",\n "),
  #         ".\n Please unselect them."
  #       )
  #     ))
  #   }
  #
  #   maxCount <- max(data$conceptCount, na.rm = TRUE)
  #
  #   if (input$includedType == "Source Concepts") {
  #     table <- data %>%
  #       dplyr::select(
  #         .data$databaseId,
  #         .data$sourceConceptId,
  #         .data$conceptSubjects,
  #         .data$conceptCount
  #       ) %>%
  #       dplyr::arrange(.data$databaseId) %>%
  #       tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>%
  #       dplyr::mutate(name = paste0(
  #         databaseId,
  #         "_",
  #         stringr::str_replace(
  #           string = .data$name,
  #           pattern = 'concept',
  #           replacement = ''
  #         )
  #       )) %>%
  #       tidyr::pivot_wider(
  #         id_cols = c(.data$sourceConceptId),
  #         names_from = .data$name,
  #         values_from = .data$value
  #       ) %>%
  #       dplyr::inner_join(
  #         data %>%
  #           dplyr::select(
  #             .data$sourceConceptId,
  #             .data$sourceConceptName,
  #             .data$sourceVocabularyId,
  #             .data$sourceConceptCode
  #           ) %>%
  #           dplyr::distinct(),
  #         by = "sourceConceptId"
  #       ) %>%
  #       dplyr::relocate(
  #         .data$sourceConceptId,
  #         .data$sourceConceptName,
  #         .data$sourceVocabularyId,
  #         .data$sourceConceptCode
  #       )
  #
  #     if (nrow(table) == 0) {
  #       return(dplyr::tibble(
  #         Note = paste0("No data available for selected databases and cohorts")
  #       ))
  #     }
  #     table <- table[order(-table[, 5]), ]
  #     dataTable <- standardDataTable(table)
  #   } else {
  #     table <- data %>%
  #       dplyr::select(
  #         .data$databaseId,
  #         .data$conceptId,
  #         .data$conceptSubjects,
  #         .data$conceptCount
  #       ) %>%
  #       dplyr::group_by(.data$databaseId,
  #                       .data$conceptId) %>%
  #       dplyr::summarise(
  #         conceptSubjects = sum(.data$conceptSubjects),
  #         conceptCount = sum(.data$conceptCount)
  #       ) %>%
  #       dplyr::ungroup() %>%
  #       dplyr::arrange(.data$databaseId) %>%
  #       tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>%
  #       dplyr::mutate(name = paste0(
  #         databaseId,
  #         "_",
  #         stringr::str_replace(
  #           string = .data$name,
  #           pattern = "concept",
  #           replacement = ""
  #         )
  #       )) %>%
  #       tidyr::pivot_wider(
  #         id_cols = c(.data$conceptId),
  #         names_from = .data$name,
  #         values_from = .data$value
  #       ) %>%
  #       dplyr::inner_join(
  #         data %>%
  #           dplyr::select(.data$conceptId,
  #                         .data$conceptName,
  #                         .data$vocabularyId) %>%
  #           dplyr::distinct(),
  #         by = "conceptId"
  #       ) %>%
  #       dplyr::relocate(.data$conceptId, .data$conceptName, .data$vocabularyId)
  #
  #     if (nrow(table) == 0) {
  #       return(dplyr::tibble(
  #         Note = paste0('No data available for selected databases and cohorts')
  #       ))
  #     }
  #     table <- table[order(-table[, 4]), ]
  #     dataTable <- standardDataTable(table)
  #   }
  #   return(dataTable)
  # }, server = TRUE)
  
  # orphan concepts table -------------------------------------------------------------------------
  # output$orphanConceptsTable <- DT::renderDT(expr = {
  #   validate(need(length(input$databases) > 0, "No data sources chosen"))
  #
  #   data <- getOrphanConceptResult(
  #     dataSource = dataSource,
  #     cohortId = cohortId(),
  #     databaseIds = input$databases
  #   )
  #   data <- data %>%
  #     dplyr::filter(.data$conceptSetName == input$conceptSet)
  #
  #   if (nrow(data) == 0) {
  #     return(dplyr::tibble(Note = paste0(
  #       "There is no data for the selected combination."
  #     )))
  #   }
  #   databaseIds <- unique(data$databaseId)
  #
  #   if (!all(input$databases %in% databaseIds)) {
  #     return(dplyr::tibble(
  #       Note = paste0(
  #         "There is no data for the databases:\n",
  #         paste0(setdiff(input$databases, databaseIds),
  #                collapse = ",\n "),
  #         ".\n Please unselect them."
  #       )
  #     ))
  #   }
  #
  #   maxCount <- max(data$conceptCount, na.rm = TRUE)
  #
  #   table <- data %>%
  #     dplyr::select(.data$databaseId,
  #                   .data$conceptId,
  #                   .data$conceptSubjects,
  #                   .data$conceptCount) %>%
  #     dplyr::group_by(.data$databaseId,
  #                     .data$conceptId) %>%
  #     dplyr::summarise(
  #       conceptSubjects = sum(.data$conceptSubjects),
  #       conceptCount = sum(.data$conceptCount)
  #     ) %>%
  #     dplyr::ungroup() %>%
  #     dplyr::arrange(.data$databaseId) %>%
  #     tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>%
  #     dplyr::mutate(name = paste0(
  #       databaseId,
  #       "_",
  #       stringr::str_replace(
  #         string = .data$name,
  #         pattern = "concept",
  #         replacement = ""
  #       )
  #     )) %>%
  #     tidyr::pivot_wider(
  #       id_cols = c(.data$conceptId),
  #       names_from = .data$name,
  #       values_from = .data$value
  #     ) %>%
  #     dplyr::inner_join(
  #       data %>%
  #         dplyr::select(
  #           .data$conceptId,
  #           .data$conceptName,
  #           .data$vocabularyId,
  #           .data$conceptCode
  #         ) %>%
  #         dplyr::distinct(),
  #       by = "conceptId"
  #     ) %>%
  #     dplyr::relocate(.data$conceptId,
  #                     .data$conceptName,
  #                     .data$vocabularyId,
  #                     .data$conceptCode)
  #
  #   if (nrow(table) == 0) {
  #     return(dplyr::tibble(
  #       Note = paste0('No data available for selected databases and cohorts')
  #     ))
  #   }
  #
  #   table <- table[order(-table[, 5]), ]
  #   dataTable <- standardDataTable(table)
  #   return(table)
  # }, server = TRUE)
  
  # Concept set diagnostics ---------------------------------------------------------------------
  output$conceptSetDiagnosticsTable <- DT::renderDT(expr = {
    conceptSetSql <- conceptSets %>%
      dplyr::filter(.data$cohortId == cohortId() &
                      .data$conceptSetName == input$conceptSet) %>%
      dplyr::pull(.data$conceptSetSql)
    if (length(conceptSetSql) == 0) {
      # Happens when switching cohorts: first the cohortId changes, and later the conceptSetName changes
      return(NULL)
    }
    standard <-
      (input$conceptSetDiagnosticsType == "Standard Concepts")
    data <- getRecommendedConcepts(
      dataSource = dataSource,
      conceptSetSql = conceptSetSql,
      standard = standard
    ) %>%
      dplyr::relocate(.data$conceptInSet)
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0(
        'No data available for selected concept set'
      )))
    }
    if (standard) {
      data <- data %>%
        dplyr::arrange(
          dplyr::desc(.data$descendantDatabaseCount),
          dplyr::desc(.data$descendantRecordCount)
        ) %>%
        dplyr::rename(
          rc = .data$recordCount,
          dc = .data$databaseCount,
          drc = .data$descendantRecordCount,
          ddc = .data$descendantDatabaseCount
        )
    } else {
      data <- data %>%
        dplyr::arrange(dplyr::desc(.data$databaseCount),
                       dplyr::desc(.data$recordCount)) %>%
        dplyr::rename(rc = .data$recordCount,
                      dc = .data$databaseCount)
    }
    table <- standardDataTable(data)
    return(table)
  })
  
  # Inclusion rules table -----------------------------------------------------------------------
  output$inclusionRuleTable <- DT::renderDT(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    data <- getInclusionRuleStats(
      dataSource = dataSource,
      cohortIds = cohortIds(),
      databaseIds = input$databases
    )
    data <- addMetaDataInformationToResults(data)
    table <- standardDataTable(data)
    return(table)
  }, server = TRUE)
  
  
  # Index event breakdown ----------------------------------------------------------------
  output$breakdownTable <- DT::renderDT(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen chosen"))
    data <- getIndexEventBreakdown(
      dataSource = dataSource,
      cohortIds = cohortIds(),
      databaseIds = input$databases
    )
    data <- addMetaDataInformationToResults(data)
    dataTable <- standardDataTable(data)
    return(dataTable)
  }, server = TRUE)
  
  
  # Visit Context ---------------------------------------------------------------------------------------------
  output$visitContextTable <- DT::renderDT(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getVisitContextResults(
      dataSource = dataSource,
      cohortIds = cohortIds(),
      databaseIds = input$databases
    )
    data <- addMetaDataInformationToResults(data)
    table <- standardDataTable(data)
  }, server = TRUE)
  
  
  # Characterization -----------------------------------------------------------------
  characterizationData <- shiny::reactive(x = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getCovariateValueResult(
      dataSource = dataSource,
      # analysisIds = analysisIds,
      cohortIds = cohortIds(),
      databaseIds = input$databases,
      isTemporal = FALSE
    )
  })
  
  # Characterization --------------------------------------------------
  output$characterizationTable <- DT::renderDT(expr = {
    data <- characterizationData()
    if (input$charType == "Pretty") {
      analysisIds <- prettyAnalysisIds
      table <- data %>%
        prepareTable1() %>%
        dplyr::rename(percent = .data$value)
      characteristics <- table %>%
        dplyr::select(.data$characteristic,
                      .data$position,
                      .data$header,
                      .data$sortOrder) %>%
        dplyr::distinct() %>%
        dplyr::group_by(.data$characteristic, .data$position, .data$header) %>%
        dplyr::summarise(sortOrder = max(.data$sortOrder)) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(.data$position, desc(.data$header)) %>%
        dplyr::mutate(sortOrder = dplyr::row_number()) %>%
        dplyr::distinct()
      
      characteristics <- dplyr::bind_rows(
        tidyr::crossing(
          characteristics %>%
            dplyr::filter(.data$header == 1),
          dplyr::tibble(cohortId = cohortIds()),
          dplyr::tibble(databaseId = input$databases)
        ),
        characteristics %>%
          dplyr::filter(.data$header == 0) %>%
          tidyr::crossing(dplyr::tibble(databaseId = input$databases)) %>%
          tidyr::crossing(dplyr::tibble(cohortId = cohortIds()))
      )
      data <- characteristics %>%
        dplyr::left_join(
          table %>%
            dplyr::select(-.data$sortOrder),
          by = c(
            "databaseId",
            "cohortId",
            "characteristic",
            "position",
            "header"
          )
        )  %>%
        dplyr::arrange(.data$databaseId, .data$cohortId, .data$sortOrder) %>%
        dplyr::select(-.data$position, -.data$header) %>%
        dplyr::relocate(.data$sortOrder, .after = dplyr::last_col())
    }
    data <- addMetaDataInformationToResults(data)
    table <- standardDataTable(data = data)
    return(table)
  })
  
  # covariateIdArray <- reactiveVal()
  # covariateIdArray(c())
  # observeEvent(input$rows, {
  #   if (input$rows[[2]] %in% covariateIdArray())
  #     covariateIdArray(covariateIdArray()[covariateIdArray() %in% input$rows[[2]] == FALSE])
  #   else
  #     covariateIdArray(c(covariateIdArray(), input$rows[[2]]))
  # })
  
  # Temporal characterization -----------------------------------------------------------------
  temporalCharacterization <- shiny::reactive(x = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    validate(need(length(timeId()) > 0, "No time periods selected"))
    data <- getCovariateValueResult(
      dataSource = dataSource,
      cohortIds = cohortIds(),
      databaseIds = input$databases,
      timeIds = timeId(),
      isTemporal = TRUE
    )
  })
  
  # Table inside the table radio button
  output$temporalCharacterizationTable <-
    DT::renderDT(expr = {
      data <- temporalCharacterization()
      table <- addMetaDataInformationToResults(data)
      table <- standardDataTable(data = table)
      return(table)
    }, server = TRUE)
  
  # reactive function to filter characterization data based on timeId or domainId
  filterByTimeIdAndDomainId <- reactive({
    data <- temporalCharacterization()
    #filter data by timeId
    # if (input$timeIdChoicesFilter != 'All') {
    #   data <- data %>%
    #     dplyr::filter(
    #       .data$timeId %in% (
    #         temporalCovariateChoices %>%
    #           dplyr::filter(choices %in% input$timeIdChoicesFilter) %>%
    #           dplyr::pull(.data$timeId)
    #       )
    #     )
    # }
    #filter data by domain
    # domains <- c("condition", "device", "drug", "measurement", "observation", "procedure")
    data$domain <-
      tolower(stringr::str_extract(data$covariateName, "[a-z]+"))
    data$domain[!data$domain %in% input$temporalDomainId] <- "other"
    if (input$temporalDomainId != "all") {
      data <- data %>%
        dplyr::filter(.data$domain == !!input$temporalDomainId)
    }
    return(data)
  })
  
  # Temporal characterization table that is shown on selecting the plot radio button
  output$temporalCharacterizationCovariateTable <-
    DT::renderDT(expr = {
      data <- filterByTimeIdAndDomainId()
      data <-
        compareTemporalCohortCharacteristics(characteristics1 = data,
                                             characteristics2 = data)
      if (nrow(data) == 0) {
        return(dplyr::tibble(Note = "No data for the selected combination."))
      }
      if (nrow(data) > 1000) {
        data <- data %>%
          dplyr::filter(.data$mean1 > 0.01 | .data$mean2 > 0.01)
      }
      data <- data %>%
        dplyr::select(.data$covariateName)
      options = list(
        pageLength = 10,
        searching = TRUE,
        searchHighlight = TRUE,
        scrollX = TRUE,
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        stateSave = TRUE,
        dom = 'tip',
        columnDefs = list(truncateStringDef(0, 40))
      )
      table <- DT::datatable(
        data,
        options = options,
        rownames = FALSE,
        colnames = colnames(table) %>%
          camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      return(table)
    })
  
  # Temporal characterization table that shows the covariates selected by lasso method
  output$temporalCharacterizationCovariateLassoTable <-
    DT::renderDT(expr = {
      data <- temporalCharacterization()
      if (nrow(data) > 1000) {
        data <- data %>%
          dplyr::filter(.data$mean > 0.01)
      }
      if (nrow(data) == 0) {
        return(dplyr::tibble(Note = "No data for the selected combination."))
      }
      table <- data %>%
        dplyr::select(.data$covariateName)
      table <- table[selectedPlotPoints(),]
      options = list(
        pageLength = 10,
        searching = TRUE,
        searchHighlight = TRUE,
        scrollX = TRUE,
        lengthChange = TRUE,
        ordering = TRUE,
        paging = TRUE,
        stateSave = TRUE,
        dom = 'tip',
        columnDefs = list(truncateStringDef(0, 40))
      )
      
      table <- DT::datatable(
        table,
        options = options,
        rownames = FALSE,
        colnames = colnames(table) %>%
          camelCaseToTitleCase(),
        escape = FALSE,
        filter = "top",
        class = "stripe nowrap compact"
      )
      return(table)
    })
  
  selectedtemporalCharacterizationCovariateRow <- reactive({
    # _row_selected is an inbuilt property of DT that provides the index of selected row.
    idx <-
      input$temporalCharacterizationCovariateTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      return(idx)
    }
  })
  
  filteredTemporalCovariateName <- reactiveVal()
  filteredTemporalCovariateName(c())
  # collect the user selected covariate names
  observeEvent(input$temporalCharacterizationCovariateTable_state, {
    if (input$temporalCharacterizationCovariateTable_state$columns[[1]]$search != "")
      filteredTemporalCovariateName(input$temporalCharacterizationCovariateTable_state$columns[[1]]$search$search)
    else
      filteredTemporalCovariateName(c())
  })
  
  selectedPlotPoints <- reactiveVal()
  selectedPlotPoints(c())
  # observe the selection of covariates inside the plot
  observeEvent(input$compareTemporalCharacterizationPlot_selected, {
    selectedPlotPoints(input$compareTemporalCharacterizationPlot_selected)
  })
  
  output$compareTemporalCharacterizationPlot <-
    ggiraph::renderggiraph(expr = {
      data <- filterByTimeIdAndDomainId()
      if (nrow(data) == 0) {
        return(dplyr::tibble(Note = "No data for the selected combination."))
      }
      data <-
        compareTemporalCohortCharacteristics(characteristics1 = data,
                                             characteristics2 = data)
      if (!is.null(selectedtemporalCharacterizationCovariateRow())) {
        data <- data[selectedtemporalCharacterizationCovariateRow(), ]
      }
      else if (!is.null(filteredTemporalCovariateName())) {
        data <- data %>%
          dplyr::filter(grepl(filteredTemporalCovariateName(), .data$covariateName))
      }
      
      if (nrow(data) > 1000) {
        data <- data %>%
          dplyr::filter(.data$mean1 > 0.01 | .data$mean2 > 0.01)
      }
      plot <- plotTemporalCohortComparison(
        balance = data,
        shortNameRef = cohort,
        domain = input$temporalDomainId
      )
      return(plot)
    })
  
  output$compareTemporalCharacterizationLassoPlot <-
    ggiraph::renderggiraph(expr = {
      data <- filterByTimeIdAndDomainId()
      if (nrow(data) == 0) {
        return(dplyr::tibble(Note = "No data for the selected combination."))
      }
      data <-
        compareTemporalCohortCharacteristics(characteristics1 = data,
                                             characteristics2 = data)
      if (nrow(data) > 1000) {
        data <- data %>%
          dplyr::filter(.data$mean1 > 0.01 | .data$mean2 > 0.01)
      }
      data <- data[selectedPlotPoints(), ]
      plot <- plotTemporalLassoCohortComparison(
        balance = data,
        shortNameRef = cohort,
        domain = input$temporalDomainId
      )
      return(plot)
    })
  
  
  #Cohort Overlap ------------------------
  cohortOverlap <- reactive({
    combisOfTargetComparator <-
      tidyr::crossing(targetCohortId = cohortIds(),
                      comparatorCohortId = cohortIds()) %>%
      dplyr::filter(!.data$targetCohortId == .data$comparatorCohortId) %>%
      dplyr::distinct()
    validate(need(
      nrow(combisOfTargetComparator) > 0,
      paste0("Please select at least two cohorts.")
    ))
    
    data <- getCohortOverlapResult(
      dataSource = dataSource,
      targetCohortIds = combisOfTargetComparator$targetCohortId,
      comparatorCohortIds = combisOfTargetComparator$comparatorCohortId,
      databaseIds = input$databases
    )
  })
  
  output$overlapPlot <- ggiraph::renderggiraph(expr = {
    validate(need(
      length(cohortIds()) > 0,
      paste0("Please select Target Cohort(s)")
    ))
    
    data <- cohortOverlap()
    validate(need(
      !is.null(data),
      paste0("No cohort overlap data for this combination")
    ))
    
    plot <- plotCohortOverlap(
      data = data,
      shortNameRef = cohort,
      yAxis = input$overlapPlotType
    )
    return(plot)
  })
  
  # Compare cohort characteristics --------------------------------------------
  
  computeBalance <- shiny::reactive(x = {
    validate(need((length(cohortIds()) != 1),
                  paste0("Please select atleast two different cohorts.")
    ))
    validate(need((length(input$databases) >= 1),
                  paste0("Please select atleast one datasource.")
    ))
    covs1 <- getCovariateValueResult(
      dataSource = dataSource,
      cohortIds = cohortIds(),
      databaseIds = input$databases,
      isTemporal = FALSE
    )
    balance <- compareCohortCharacteristics(covs1, covs1) %>%
      dplyr::mutate(absStdDiff = abs(.data$stdDiff))
    return(balance)
  })
  
  output$charCompareTable <- DT::renderDT(expr = {
    balance <- computeBalance()
    if (nrow(balance) == 0) {
      return(dplyr::tibble(Note = "No data for the selected combination."))
    }
    
    if (input$charCompareType == "Pretty table") {
      table <- prepareTable1Comp(balance)
      if (nrow(table) > 0) {
        table <- table %>%
          dplyr::arrange(.data$sortOrder) %>%
          dplyr::select(-.data$sortOrder) %>%
          addShortName(cohort,
                       cohortIdColumn = "cohortId1",
                       shortNameColumn = "shortName1") %>%
          addShortName(cohort,
                       cohortIdColumn = "cohortId2",
                       shortNameColumn = "shortName2") %>%
          dplyr::relocate(.data$shortName1, .data$shortName2) %>%
          dplyr::select(-.data$cohortId1, -.data$cohortId2)
      } else {
        return(dplyr::tibble(Note = "No data for covariates that are part of pretty table."))
      }
      table <- standardDataTable(table)
    } else {
      table <- balance %>%
        dplyr::select(
          .data$cohortId1,
          .data$cohortId2,
          .data$covariateName,
          .data$conceptId,
          .data$mean1,
          .data$sd1,
          .data$mean2,
          .data$sd2,
          .data$stdDiff
        ) %>%
        addShortName(cohort,
                     cohortIdColumn = "cohortId1",
                     shortNameColumn = "shortName1") %>%
        addShortName(cohort,
                     cohortIdColumn = "cohortId2",
                     shortNameColumn = "shortName2") %>%
        dplyr::relocate(.data$shortName1, .data$shortName2) %>%
        dplyr::select(-.data$cohortId1, -.data$cohortId2) %>%
        dplyr::arrange(desc(abs(.data$stdDiff)))
      table <- standardDataTable(data = table)
    }
    return(table)
  }, server = TRUE)
  
  output$charComparePlot <- ggiraph::renderggiraph(expr = {
    data <- computeBalance()
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = "No data for the selected combination."))
    }
    plot <-
      plotCohortComparisonStandardizedDifference(
        balance = data,
        shortNameRef = cohort,
        domain = input$domainId
      )
    return(plot)
  })
  
  output$databaseInformationTable <- DT::renderDT(expr = {
    table <- database[, c("databaseId", "databaseName", "description")]
    table <- standardDataTable(table)
    return(table)
  }, server = TRUE)
  
  
  
  shiny::observeEvent(input$cohortCountsInfo, {
    showInfoBox(title = "Cohort Counts", htmlFileName = "html/cohortCounts.html")
  })
  
  shiny::observeEvent(input$incidenceRateInfo, {
    showInfoBox(title = "Incidence Rate", htmlFileName = "html/incidenceRate.html")
  })
  
  shiny::observeEvent(input$timeDistributionInfo, {
    showInfoBox(title = "Time Distributions", htmlFileName = "html/timeDistribution.html")
  })
  
  shiny::observeEvent(input$includedConceptsInfo, {
    showInfoBox(title = "Included (Source) Concepts",
                htmlFileName = "html/includedConcepts.html")
  })
  
  shiny::observeEvent(input$orphanConceptsInfo, {
    showInfoBox(title = "Orphan (Source) Concepts", htmlFileName = "html/orphanConcepts.html")
  })
  
  shiny::observeEvent(input$conceptSetDiagnosticsInfo, {
    showInfoBox(title = "Concept Set Diagnostics",
                htmlFileName = "html/conceptSetDiagnostics.html")
  })
  
  shiny::observeEvent(input$inclusionRuleStatsInfo, {
    showInfoBox(title = "Inclusion Rule Statistics",
                htmlFileName = "html/inclusionRuleStats.html")
  })
  
  shiny::observeEvent(input$indexEventBreakdownInfo, {
    showInfoBox(title = "Index Event Breakdown", htmlFileName = "html/indexEventBreakdown.html")
  })
  
  shiny::observeEvent(input$visitContextInfo, {
    showInfoBox(title = "Visit Context",
                htmlFileName = "html/visitContext.html")
  })
  
  shiny::observeEvent(input$cohortCharacterizationInfo, {
    showInfoBox(title = "Cohort Characterization",
                htmlFileName = "html/cohortCharacterization.html")
  })
  
  shiny::observeEvent(input$temporalCharacterizationInfo, {
    showInfoBox(title = "Temporal Characterization",
                htmlFileName = "html/temporalCharacterization.html")
  })
  
  shiny::observeEvent(input$cohortOverlapInfo, {
    showInfoBox(title = "Cohort Overlap", htmlFileName = "html/cohortOverlap.html")
  })
  
  shiny::observeEvent(input$compareCohortCharacterizationInfo, {
    showInfoBox(title = "Compare Cohort Characteristics",
                htmlFileName = "html/compareCohortCharacterization.html")
  })
  
  # Cohort labels --------------------------------------------------------------------------------------------
  targetCohortCount <- shiny::reactive(x = {
    targetCohortWithCount <-
      getCohortCountResult(
        dataSource = dataSource,
        cohortIds = cohortId(),
        databaseIds = input$database
      ) %>%
      dplyr::left_join(y = cohort, by = "cohortId") %>%
      dplyr::arrange(.data$cohortName)
    return(targetCohortWithCount)
  })
  
  targetCohortCountHtml <- shiny::reactive(x = {
    targetCohortCount <- targetCohortCount()
    
    return(htmltools::withTags(div(
      h5(
        "Target: ",
        targetCohortCount$cohortName,
        " ( n = ",
        scales::comma(x = targetCohortCount$cohortSubjects),
        " )"
      )
    )))
  })
  
  selectedCohorts <- shiny::reactive(x = {
    cohorts <- cohortSubset() %>%
      dplyr::filter(.data$cohortId %in% cohortIds()) %>%
      dplyr::arrange(.data$cohortId) %>%
      dplyr::select(.data$shortName, .data$cohortName)
    return(apply(cohorts, 1, function(x)
      tags$tr(lapply(x, tags$td))))
  })
  
  output$cohortCountsSelectedCohort <-
    shiny::renderUI(expr = {
      selectedCohorts()
    })
  output$indexEventBreakdownSelectedCohort <-
    shiny::renderUI(expr = {
      selectedCohorts()
    })
  output$characterizationSelectedCohort <-
    shiny::renderUI(expr = {
      selectedCohorts()
    })
  output$temporalCharacterizationSelectedCohort <-
    shiny::renderUI(expr = {
      selectedCohorts()
    })
  output$inclusionRuleStatSelectedCohort <-
    shiny::renderUI(expr = {
      selectedCohorts()
    })
  output$cohortOverlapSelectedCohort <-
    shiny::renderUI(expr = {
      selectedCohorts()
    })
  output$incidenceRateSelectedCohort <-
    shiny::renderUI(expr = {
      selectedCohorts()
    })
  output$timeDistSelectedCohort <-
    shiny::renderUI(expr = {
      selectedCohorts()
    })
  output$visitContextSelectedCohort <-
    shiny::renderUI(expr = {
      selectedCohorts()
    })
  output$cohortCharCompareSelectedCohort <-
    shiny::renderUI(expr = {
      selectedCohorts()
    })
  #Download
  # download_box <- function(exportname, plot){
  #   downloadHandler(
  #     filename = function() {
  #       paste(exportname, Sys.Date(), ".png", sep = "")
  #     },
  #     content = function(file) {
  #       ggplot2::ggsave(file, plot = plot, device = "png", width = 9, height = 7, dpi = 400)
  #     }
  #   )
  # }
  
})
