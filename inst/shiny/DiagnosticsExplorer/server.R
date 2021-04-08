library(magrittr)

source("R/DisplayFunctions.R")
source("R/Tables.R")
source("R/Plots.R")
source("R/Results.R")
source("R/ConceptRecommender.R")
source("R/GitHubScraper.R")

shiny::shinyServer(function(input, output, session) {
  
  cohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$compoundName == input$cohort])
  })
  
  cohortIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {list(input$cohorts_open,
                                        input$tabs)
  },handlerExpr = {
    if (isFALSE(input$cohorts_open) || !is.null(input$tabs)) {
      selectedCohortIds <- cohort$cohortId[cohort$compoundName  %in% input$cohorts]
      cohortIds(selectedCohortIds)
    }
  })
  
  comparatorCohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$compoundName == input$comparatorCohort])
  })
  
  timeId <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {list(input$timeIdChoices_open,
                                        input$tabs)
  },handlerExpr = {
    if (isFALSE(input$timeIdChoices_open) || !is.null(input$tabs)) {
      selectedTimeIds <- temporalCovariateChoices %>%
        dplyr::filter(choices %in% input$timeIdChoices) %>%
        dplyr::pull(timeId)
      timeId(selectedTimeIds)
    }
  })
  
  databaseIds <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {list(input$databases_open,
                                        input$tabs)
  },handlerExpr = {
    if (isFALSE(input$databases_open) || !is.null(input$tabs)) {
      selectedDatabaseIds <- input$databases
      databaseIds(selectedDatabaseIds)
    }
  })
  
  cohortSubset <- shiny::reactive({
      return(cohort %>%
               dplyr::arrange(.data$cohortId))
  })
  
  shiny::observe({
    if (!is.null(conceptSets) && nrow(conceptSets) > 0) {
    subset <- unique(conceptSets$conceptSetName[conceptSets$cohortId == cohortId()]) %>% sort()
    shinyWidgets::updatePickerInput(session = session,
                                    inputId = "conceptSet",
                                    choicesOpt = list(style = rep_len("color: black;", 999)),
                                    choices = subset)
    }
  })
  
  shiny::observe({
    subset <- cohortSubset()$compoundName
    shinyWidgets::updatePickerInput(session = session,
                                    inputId = "cohort",
                                    choicesOpt = list(style = rep_len("color: black;", 999)),
                                    choices = subset)
  })
  
  shiny::observe({
    subset <- cohortSubset()$compoundName
    shinyWidgets::updatePickerInput(session = session,
                                    inputId = "cohorts",
                                    choicesOpt = list(style = rep_len("color: black;", 999)),
                                    choices = subset,
                                    selected = c(subset[1], subset[2]))
  })
  
  shiny::observe({
    subset <- cohortSubset()$compoundName
    shinyWidgets::updatePickerInput(session = session,
                                    inputId = "comparatorCohort",
                                    choicesOpt = list(style = rep_len("color: black;", 999)),
                                    choices = subset,
                                    selected = subset[2])
  })
  
  # Cohort Definition ---------------------------------------------------------
  output$cohortDefinitionTable <- DT::renderDataTable(expr = {
    data <- cohortSubset() %>%
      dplyr::select(cohort = .data$shortName, .data$cohortId, .data$cohortName) %>%
      dplyr::mutate(cohort = as.factor(.data$cohort))
    
    options = list(pageLength = 10,
                   lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                   searching = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE,
                   searchHighlight = TRUE)
    
    dataTable <- DT::datatable(data,
                               options = options,
                               rownames = FALSE,
                               colnames = colnames(data) %>% camelCaseToTitleCase(),
                               escape = FALSE,
                               filter = "top",
                               selection = list(mode = "single", target = "row"),
                               class = "stripe compact")
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
  
  output$cohortDefinitionRowIsSelected <- reactive({
    return(!is.null(selectedCohortDefinitionRow()))
  })
  
  outputOptions(output, "cohortDefinitionRowIsSelected", suspendWhenHidden = FALSE)
  
  output$cohortDetailsText <- shiny::renderUI({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      tags$table(style = "margin-top: 5px;",
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
    if (is(dataSource, "environment") || input$conceptSetsType == "Concept Set Expression") {
      expression <- RJSONIO::fromJSON(row$json, digits = 23)
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
        dplyr::select(.data$conceptSetName,
                      .data$conceptId, 
                      .data$conceptCode, 
                      .data$conceptName,
                      .data$domainId,
                      .data$standardConcept,
                      .data$isExcluded,
                      .data$includeDescendants,
                      .data$includeMapped) %>%
        dplyr::arrange(.data$conceptSetName, .data$conceptId)
      data$conceptSetName <- as.factor(data$conceptSetName)
      data$domainId <- as.factor(data$domainId)
      data$standardConcept <- as.factor(data$standardConcept)
      colnames(data) <- camelCaseToTitleCase(colnames(data))
    } else {
      subset <- conceptSets %>%
        dplyr::filter(.data$cohortId == row$cohortId)
      if (nrow(subset) == 0) {
        return(NULL)
      } 
      source <- (input$conceptSetsType == "Included Source Concepts")
      data <- resolveConceptSet(dataSource = dataSource, subset, source = source)
      data <- data %>%
        dplyr::inner_join(subset, by = "conceptSetId") %>%
        dplyr::select(.data$conceptSetName,
                      .data$conceptId, 
                      .data$conceptCode, 
                      .data$conceptName,
                      .data$conceptClassId,
                      .data$domainId,
                      .data$vocabularyId,
                      .data$standardConcept) %>%
        dplyr::arrange(.data$conceptSetName, .data$conceptId)
      data$conceptSetName <- as.factor(data$conceptSetName)
      data$conceptClassId <- as.factor(data$conceptClassId)
      data$domainId <- as.factor(data$domainId)
      data$vocabularyId <- as.factor(data$vocabularyId)
      data$standardConcept <- as.factor(data$standardConcept)
      colnames(data) <- camelCaseToTitleCase(colnames(data))
    }
    return(data)
    
  })
  
  output$cohortDefinitionConceptSetsTable <- DT::renderDataTable(expr = {
    
    data <- cohortDefinitionConceptSets()
    if (is.null(data)) {
      return(NULL)
    } 
    
    options = list(pageLength = 100,
                   lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE)
    
    dataTable <- DT::datatable(data,
                               options = options,
                               rownames = FALSE,
                               escape = FALSE,
                               filter = "top",
                               class = "stripe nowrap compact")
    return(dataTable)
    
  })
  
  output$saveConceptSetButton <- downloadHandler(
    filename = function() {
      paste("conceptSet-", Sys.Date(), ".csv", sep="")
    },
    content = function(file) {
      data <- cohortDefinitionConceptSets()
      write.csv(data, file)
    }
  )
  
  # Cohort Counts --------------------------------------------------------------------------- 
  output$cohortCountsTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getCohortCountResult(dataSource = dataSource,
                                 databaseIds = databaseIds(),
                                 cohortIds = cohortIds()) %>% 
      addShortName(cohort) %>%
      dplyr::select(.data$databaseId, 
                    .data$shortName, 
                    .data$cohortSubjects, 
                    .data$cohortEntries,
                    .data$cohortId) %>% 
      dplyr::rename(cohort = .data$shortName) %>%
      dplyr::mutate(cohort = as.factor(.data$cohort))
    
    if (nrow(data) == 0) {
      return(tidyr::tibble("There is no data on any cohort"))
    }
    
    # instead maybe we can just convert this to a warning message in header.
    if (!isTRUE(all.equal(data$databaseId %>% unique %>% sort(),
                          databaseIds() %>% unique() %>% sort()))) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(databaseIds(), 
                                                        data$databaseId %>% unique()), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    table <- dplyr::full_join(
      data %>% 
        dplyr::select(.data$cohort, .data$databaseId, 
                      .data$cohortSubjects) %>% 
        dplyr::mutate(columnName = paste0(.data$databaseId, "_subjects")) %>% 
        dplyr::arrange(.data$cohort, .data$databaseId) %>% 
        tidyr::pivot_wider(id_cols = .data$cohort,
                           names_from = columnName,
                           values_from = .data$cohortSubjects),
      data %>% 
        dplyr::select(.data$cohort, .data$databaseId, 
                      .data$cohortEntries) %>% 
        dplyr::mutate(columnName = paste0(.data$databaseId, "_entries")) %>% 
        dplyr::arrange(.data$cohort, .data$databaseId) %>% 
        tidyr::pivot_wider(id_cols = .data$cohort,
                           names_from = columnName,
                           values_from = .data$cohortEntries),
      by = c("cohort"))
    table <- table %>% 
      dplyr::select(order(colnames(table))) %>% 
      dplyr::relocate(.data$cohort) %>%
      dplyr::arrange(.data$cohort) 
    
    databaseIds <- sort(unique(data$databaseId))
    
    sketch <- htmltools::withTags(table(
      class = "display",
      thead(
        tr(
          th(rowspan = 2, "Cohort"),
          lapply(databaseIds, th, colspan = 2, class = "dt-center")
        ),
        tr(
          lapply(rep(c("Entries", "Subjects"), length(databaseIds)), th)
        )
      )
    ))
    
    options = list(pageLength = 100,
                   lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   columnDefs = list(minCellCountDef(1:(2*length(databaseIds)))))
    
    dataTable <- DT::datatable(table,
                               options = options,
                               rownames = FALSE,
                               container = sketch, 
                               escape = FALSE,
                               filter = "top",
                               class = "stripe nowrap compact")
    for (i in 1:length(databaseIds)) {
      dataTable <- DT::formatStyle(table = dataTable,
                                   columns = i*2,
                                   background = DT::styleColorBar(c(0, max(table[, i*2], na.rm = TRUE)), "lightblue"),
                                   backgroundSize = "98% 88%",
                                   backgroundRepeat = "no-repeat",
                                   backgroundPosition = "center")
      dataTable <- DT::formatStyle(table = dataTable,
                                   columns = i*2 + 1,
                                   background = DT::styleColorBar(c(0, max(table[, i*2 + 1], na.rm = TRUE)), "#ffd699"),
                                   backgroundSize = "98% 88%",
                                   backgroundRepeat = "no-repeat",
                                   backgroundPosition = "center")
    }
    return(dataTable)
  }, server = TRUE)
  
  # Incidence rate --------------------------------------------------------------------------------
  
  incidenceRateData <- reactive({
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
    if (length(cohortIds()) > 0) {
      data <- getIncidenceRateResult(dataSource = dataSource,
                                     cohortIds = cohortIds(), 
                                     databaseIds = databaseIds(), 
                                     stratifyByGender =  stratifyByGender,
                                     stratifyByAgeGroup =  stratifyByAge,
                                     stratifyByCalendarYear =  stratifyByCalendarYear,
                                     minPersonYears = 1000) %>% 
        dplyr::mutate(incidenceRate = dplyr::case_when(.data$incidenceRate < 0 ~ 0, 
                                                       TRUE ~ .data$incidenceRate))
    } else {
      data <- tidyr::tibble()
    }
    return(data)
  })
  
  shiny::observe({
    if (nrow(incidenceRateData()) > 0) {
      ageFilter <- incidenceRateData() %>% 
        dplyr::select(.data$ageGroup) %>%
        dplyr::filter(.data$ageGroup != "NA",!is.na(.data$ageGroup)) %>%
        dplyr::distinct() %>%
        dplyr::arrange(as.integer(sub(pattern = '-.+$','',x = .data$ageGroup)))
      
      shinyWidgets::updatePickerInput(session = session,
                                      inputId = "incidenceRateAgeFilter",
                                      selected = ageFilter$ageGroup,
                                      choices = ageFilter$ageGroup,
                                      choicesOpt = list(style = rep_len("color: black;", 999)))
    }
  })
  
  shiny::observe({
    if (nrow(incidenceRateData()) > 0) {
      genderFilter <- incidenceRateData() %>% 
        dplyr::select(.data$gender) %>% 
        dplyr::filter(.data$gender != "NA",
                      !is.na(.data$gender)) %>%
        dplyr::distinct() %>% 
        dplyr::arrange(.data$gender)
      
      shinyWidgets::updatePickerInput(session = session,
                                      inputId = "incidenceRateGenderFilter",
                                      choicesOpt = list(style = rep_len("color: black;", 999)),
                                      choices = genderFilter$gender,
                                      selected = genderFilter$gender)
    }
  })
  
  shiny::observe({
    if (nrow(incidenceRateData()) > 0) {
      calenderFilter <- incidenceRateData() %>% 
        dplyr::select(.data$calendarYear) %>% 
        dplyr::filter(.data$calendarYear != "NA",
                      !is.na(.data$calendarYear)) %>%
        dplyr::distinct(.data$calendarYear) %>% 
        dplyr::arrange(.data$calendarYear)
      
      shinyWidgets::updatePickerInput(session = session,
                                      inputId = "incidenceRateCalenderFilter",
                                      choicesOpt = list(style = rep_len("color: black;", 999)),
                                      choices = calenderFilter$calendarYear,
                                      selected = calenderFilter$calendarYear)
    }
  })
  
  
  incidenceRateAgeFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {list(input$incidenceRateAgeFilter_open,
                                        input$tabs)
  },handlerExpr = {
    if (isFALSE(input$incidenceRateAgeFilter_open) || !is.null(input$tabs)) {
      selectedIncidenceRateAgeFilter <- input$incidenceRateAgeFilter
      incidenceRateAgeFilter(selectedIncidenceRateAgeFilter)
    }
  })
  
  incidenceRateGenderFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {list(input$incidenceRateGenderFilter_open,
                                        input$tabs)
  },handlerExpr = {
    if (isFALSE(input$incidenceRateGenderFilter_open) || !is.null(input$tabs)) {
      selectedIncidenceRateGenderFilter <- input$incidenceRateGenderFilter
      incidenceRateGenderFilter(selectedIncidenceRateGenderFilter)
    }
  })
  
  incidenceRateCalenderFilter <- reactiveVal(NULL)
  shiny::observeEvent(eventExpr = {list(input$incidenceRateCalenderFilter_open,
                                        input$tabs)
  },handlerExpr = {
    if (isFALSE(input$incidenceRateCalenderFilter_open) || !is.null(input$tabs)) {
      selectedIncidenceRateCalenderFilter <- input$incidenceRateCalenderFilter
      incidenceRateCalenderFilter(selectedIncidenceRateCalenderFilter)
    }
  })
  
  output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
    data <- incidenceRateData()
    
    if (stratifyByAge && !"All" %in% incidenceRateAgeFilter()) {
      data <- data %>% 
        dplyr::filter(.data$ageGroup %in% incidenceRateAgeFilter())}
    if (stratifyByGender && !"All" %in% incidenceRateGenderFilter()) {
      data <- data %>% 
        dplyr::filter(.data$gender %in% incidenceRateGenderFilter())}
    if (stratifyByCalendarYear && !"All" %in% incidenceRateCalenderFilter()) {
      data <- data %>% 
        dplyr::filter(.data$calendarYear %in% incidenceRateCalenderFilter())}
    
    validate(need(nrow(data) > 0, paste0("No data for this combination")))
    
    plot <- plotIncidenceRate(data = data,
                              shortNameRef = cohort,
                              stratifyByAgeGroup = stratifyByAge,
                              stratifyByGender = stratifyByGender,
                              stratifyByCalendarYear = stratifyByCalendarYear,
                              yscaleFixed = input$irYscaleFixed)
    return(plot)
  })
  
  # Time distribution -----------------------------------------------------------------------------
  timeDist <- reactive({
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getTimeDistributionResult(dataSource = dataSource,
                                      cohortIds = cohortIds(), 
                                      databaseIds = databaseIds())
    return(data)
  })
  
  output$timeDisPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    data <- timeDist()
    validate(need(nrow(data) > 0, paste0("No data for this combination")))
    
    plot <- plotTimeDistribution(data = data, shortNameRef = cohort)
    return(plot)
  })
  
  output$timeDistTable <- DT::renderDataTable(expr = {
    data <- timeDist()  %>%
      addShortName(cohort) %>%
      dplyr::arrange(.data$databaseId, .data$cohortId) %>%
      dplyr::mutate(shortName = as.factor(.data$shortName),
                    databaseId = as.factor(.data$databaseId)) %>%
      dplyr::select(Database = .data$databaseId,
                    Cohort = .data$shortName,
                    TimeMeasure = .data$timeMetric, 
                    Average = .data$averageValue, 
                    SD = .data$standardDeviation, 
                    Min = .data$minValue, 
                    P10 = .data$p10Value, 
                    P25 = .data$p25Value, 
                    Median = .data$medianValue, 
                    P75 = .data$p75Value, 
                    P90 = .data$p90Value, 
                    Max = .data$maxValue) 
    
    
    if (is.null(data) || nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    
    options = list(pageLength = 100,
                   lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE,
                   columnDefs = list(minCellCountDef(3)))
    table <- DT::datatable(data,
                           options = options,
                           rownames = FALSE,
                           filter = "top",
                           class = "stripe nowrap compact")
    table <- DT::formatRound(table, c("Average", "SD"), digits = 2)
    table <- DT::formatRound(table, c("Min", "P10", "P25", "Median", "P75", "P90", "Max"), digits = 0)
    return(table)
  }, server = TRUE)
  
  # included concepts table --------------------------------------------------------------------------
  output$includedConceptsTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    
    data <- getIncludedConceptResult(dataSource = dataSource,
                                     cohortId = cohortId(),
                                     databaseIds = databaseIds())
    data <- data %>%
      dplyr::filter(.data$conceptSetName == input$conceptSet)
    if (nrow(data) == 0) {
      return(dplyr::tibble("No data available for selected databases and cohorts"))
    }
    
    databaseIds <- unique(data$databaseId)
    
    if (!all(databaseIds() %in% databaseIds)) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(databaseIds(), databaseIds), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    
    if (input$includedType == "Source Concepts") {
      table <- data %>%
        dplyr::select(.data$databaseId, 
                      .data$sourceConceptId,
                      .data$conceptSubjects,
                      .data$conceptCount) %>%
        dplyr::arrange(.data$databaseId) %>% 
        tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>% 
        dplyr::mutate(name = paste0(databaseId, "_",
                                    stringr::str_replace(string = .data$name, 
                                                         pattern = 'concept', 
                                                         replacement = ''))) %>% 
        tidyr::pivot_wider(id_cols = c(.data$sourceConceptId),
                           names_from = .data$name,
                           values_from = .data$value) %>%
        dplyr::inner_join(data %>%
                            dplyr::select(.data$sourceConceptId,
                                          .data$sourceConceptName,
                                          .data$sourceVocabularyId,
                                          .data$sourceConceptCode) %>%
                            dplyr::distinct(),
                          by = "sourceConceptId") %>%
        dplyr::relocate(.data$sourceConceptId, 
                        .data$sourceConceptName, 
                        .data$sourceVocabularyId,
                        .data$sourceConceptCode)
      
      if (nrow(table) == 0) {
        return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
      }
      table <- table[order(-table[, 5]), ]
      
      sketch <- htmltools::withTags(table(
        class = "display",
        thead(
          tr(
            th(rowspan = 2, 'Concept ID'),
            th(rowspan = 2, 'Concept Name'),
            th(rowspan = 2, 'Vocabulary ID'),
            th(rowspan = 2, 'Concept Code'),
            lapply(databaseIds, th, colspan = 2, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Subjects", "Count"), length(databaseIds)), th)
          )
        )
      ))
      
      options = list(pageLength = 100,
                     lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                     searching = TRUE,
                     scrollX = TRUE,
                     lengthChange = TRUE,
                     searchHighlight = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(truncateStringDef(1, 100),
                                       minCellCountDef(3 + (1:(length(databaseIds) * 2)))))
      
      dataTable <- DT::datatable(table,
                                 colnames = colnames(table),
                                 options = options,
                                 rownames = FALSE, 
                                 container = sketch,
                                 escape = FALSE,
                                 filter = "top",
                                 class = "stripe nowrap compact")
      
      dataTable <- DT::formatStyle(table = dataTable,
                                   columns =  4 + (1:(length(databaseIds) * 2)),
                                   background = DT::styleColorBar(c(0, maxCount), "lightblue"),
                                   backgroundSize = "98% 88%",
                                   backgroundRepeat = "no-repeat",
                                   backgroundPosition = "center")
    } else {
      table <- data %>%
        dplyr::select(.data$databaseId, 
                      .data$conceptId,
                      .data$conceptSubjects,
                      .data$conceptCount) %>%
        dplyr::group_by(.data$databaseId, 
                        .data$conceptId) %>%
        dplyr::summarise(conceptSubjects = sum(.data$conceptSubjects),
                         conceptCount = sum(.data$conceptCount)) %>%
        dplyr::ungroup() %>%
        dplyr::arrange(.data$databaseId) %>% 
        tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>% 
        dplyr::mutate(name = paste0(databaseId, "_",
                                    stringr::str_replace(string = .data$name, 
                                                         pattern = "concept", 
                                                         replacement = ""))) %>% 
        tidyr::pivot_wider(id_cols = c(.data$conceptId),
                           names_from = .data$name,
                           values_from = .data$value) %>%
        dplyr::inner_join(data %>%
                            dplyr::select(.data$conceptId,
                                          .data$conceptName,
                                          .data$vocabularyId) %>%
                            dplyr::distinct(),
                          by = "conceptId") %>%
        dplyr::relocate(.data$conceptId, .data$conceptName, .data$vocabularyId)
      
      if (nrow(table) == 0) {
        return(dplyr::tibble(Note = paste0('No data available for selected databases and cohorts')))
      }
      
      table <- table[order(-table[, 4]), ]
      
      sketch <- htmltools::withTags(table(
        class = "display",
        thead(
          tr(
            th(rowspan = 2, "Concept ID"),
            th(rowspan = 2, "Concept Name"),
            th(rowspan = 2, "Vocabulary ID"),
            lapply(databaseIds, th, colspan = 2, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Subjects", "Counts"), length(databaseIds)), th)
          )
        )
      ))
      
      options = list(pageLength = 100,
                     lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                     searching = TRUE,
                     scrollX = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(truncateStringDef(1, 100),
                                       minCellCountDef(2 + (1:(length(databaseIds) * 2)))))
      
      dataTable <- DT::datatable(table,
                                 options = options,
                                 colnames = colnames(table),
                                 rownames = FALSE,
                                 container = sketch,
                                 escape = FALSE,
                                 filter = "top",
                                 class = "stripe nowrap compact")
      
      dataTable <- DT::formatStyle(table = dataTable,
                                   columns =  3 + (1:(length(databaseIds)*2)),
                                   background = DT::styleColorBar(c(0, maxCount), "lightblue"),
                                   backgroundSize = "98% 88%",
                                   backgroundRepeat = "no-repeat",
                                   backgroundPosition = "center")
    }
    return(dataTable)
  }, server = TRUE)
  
  # orphan concepts table -------------------------------------------------------------------------
  output$orphanConceptsTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    
    data <- getOrphanConceptResult(dataSource = dataSource,
                                   cohortId = cohortId(),
                                   databaseIds = databaseIds())
    data <- data %>%
      dplyr::filter(.data$conceptSetName == input$conceptSet)
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("There is no data for the selected combination.")))
    }
    databaseIds <- unique(data$databaseId)
    
    if (!all(databaseIds() %in% databaseIds)) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(databaseIds(), databaseIds), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    
    table <- data %>%
      dplyr::select(.data$databaseId, 
                    .data$conceptId,
                    .data$conceptSubjects,
                    .data$conceptCount) %>%
      dplyr::group_by(.data$databaseId, 
                      .data$conceptId) %>%
      dplyr::summarise(conceptSubjects = sum(.data$conceptSubjects),
                       conceptCount = sum(.data$conceptCount)) %>%
      dplyr::ungroup() %>%
      dplyr::arrange(.data$databaseId) %>% 
      tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>% 
      dplyr::mutate(name = paste0(databaseId, "_",
                                  stringr::str_replace(string = .data$name, 
                                                       pattern = "concept", 
                                                       replacement = ""))) %>% 
      tidyr::pivot_wider(id_cols = c(.data$conceptId),
                         names_from = .data$name,
                         values_from = .data$value) %>%
      dplyr::inner_join(data %>%
                          dplyr::select(.data$conceptId,
                                        .data$conceptName,
                                        .data$vocabularyId,
                                        .data$conceptCode) %>%
                          dplyr::distinct(),
                        by = "conceptId") %>%
      dplyr::relocate(.data$conceptId, .data$conceptName, .data$vocabularyId, .data$conceptCode)
    
    if (nrow(table) == 0) {
      return(dplyr::tibble(Note = paste0('No data available for selected databases and cohorts')))
    }
    
    table <- table[order(-table[, 5]), ]
    
    
    sketch <- htmltools::withTags(table(
      class = "display",
      thead(
        tr(
          th(rowspan = 2, "Concept ID"),
          th(rowspan = 2, "Concept Name"),
          th(rowspan = 2, "Vocabulary ID"),
          th(rowspan = 2, "Concept Code"),
          lapply(databaseIds, th, colspan = 2, class = "dt-center")
        ),
        tr(
          lapply(rep(c("Subjects", "Counts"), length(databaseIds)), th)
        )
      )
    ))
    
    options = list(pageLength = 100,
                   lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                   searching = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(truncateStringDef(1, 100),
                                     minCellCountDef(3 + (1:(length(databaseIds) * 2)))))
    
    table <- DT::datatable(table,
                           options = options,
                           colnames = colnames(table),
                           rownames = FALSE,
                           container = sketch,
                           escape = FALSE,
                           filter = "top",
                           class = "stripe nowrap compact")
    
    table <- DT::formatStyle(table = table,
                             columns =  4 + (1:(length(databaseIds)*2)),
                             background = DT::styleColorBar(c(0, maxCount), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  # Concept set diagnostics ---------------------------------------------------------------------
  output$conceptSetDiagnosticsTable <- DT::renderDataTable(expr = {
    conceptSetSql <- conceptSets %>%
      dplyr::filter(.data$cohortId == cohortId() & .data$conceptSetName == input$conceptSet) %>%
      dplyr::pull(.data$conceptSetSql)
    if (length(conceptSetSql) == 0) {
      # Happens when switching cohorts: first the cohortId changes, and later the conceptSetName changes
      return(NULL)
    }
    standard <- (input$conceptSetDiagnosticsType == "Standard Concepts")
    data <- getRecommendedConcepts(dataSource = dataSource, 
                                   conceptSetSql = conceptSetSql,
                                   standard = standard) %>%
      dplyr::mutate(conceptInSet = as.factor(.data$conceptInSet),
                    domainId = as.factor(.data$domainId),
                    vocabularyId = as.factor(.data$vocabularyId)) %>% 
      dplyr::relocate(.data$conceptInSet) 
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0('No data available for selected concept set')))
    }
    if (standard) {
      maxRecords <- max(c(data$recordCount, data$descendantRecordCount))
      maxDbs <- max(c(data$databaseCount, data$descendantDatabaseCount))
      data <- data %>%
        dplyr::arrange(dplyr::desc(.data$descendantDatabaseCount), dplyr::desc(.data$descendantRecordCount))
    } else {
      maxRecords <- max(c(data$recordCount))
      maxDbs <- max(c(data$databaseCount))
      data <- data %>%
        dplyr::arrange(dplyr::desc(.data$databaseCount), dplyr::desc(.data$recordCount))
    }
    options <- list(pageLength = 100,
                    lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                    searching = TRUE,
                    scrollX = TRUE,
                    lengthChange = TRUE,
                    ordering = TRUE,
                    paging = TRUE,
                    columnDefs = list(truncateStringDef(2, 50),
                                      minCellCountDef(6:ifelse(standard, 9, 7))))
    
    sketch <- htmltools::withTags(table(
      class = "display",
      thead(
        tr(
          th(rowspan = 2, "Concept in Set"),
          th(rowspan = 2, "Concept ID"),
          th(rowspan = 2, "Concept Name"),
          th(rowspan = 2, "Vocabulary ID"),
          th(rowspan = 2, "Domain Id"),
          th(rowspan = 2, "Standard Concept"),
          th(colspan = 2, "Without Descendants", class = "dt-center"),
          if (standard) th(colspan = 2, "With Descendants", class = "dt-center"),
        ),
        tr(
          lapply(rep(c("Records", "Databases"), ifelse(standard, 2, 1)), th)
        )
      )
    ))
    
    table <- DT::datatable(data = data,
                           rownames = FALSE,
                           container = sketch,
                           filter = 'top',
                           options = options)
    table <- DT::formatStyle(table,
                             columns = 1, 
                             color = DT::JS("value == 'Included' ? 'green' : value == 'Not included - parent' ? 'orange' : value == 'Not included - descendant' ? 'orange' : 'red'"))
    table <- DT::formatStyle(table = table,
                             columns =  c(7, if (standard) 9 ),
                             background = DT::styleColorBar(c(0, maxRecords), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    table <- DT::formatStyle(table = table,
                             columns =  c(8, if (standard) 10),
                             background = DT::styleColorBar(c(0, maxDbs), "#ffd699"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  })
  
  # Inclusion rules table -----------------------------------------------------------------------
  output$inclusionRuleTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    table <- getInclusionRuleStats(dataSource = dataSource,
                                   cohortIds = cohortId(),
                                   databaseIds = databaseIds()) 
    if (nrow(table) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    
    databaseIds <- unique(table$databaseId)
    
    if (!all(databaseIds() %in% databaseIds)) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(databaseIds(), databaseIds), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    table <- table %>% 
      tidyr::pivot_longer(cols = c(.data$meetSubjects, .data$gainSubjects, 
                                   .data$totalSubjects, .data$remainSubjects)) %>% 
      dplyr::mutate(name = paste0(databaseId, "_", .data$name)) %>% 
      tidyr::pivot_wider(id_cols = c(.data$cohortId, .data$ruleSequenceId, .data$ruleName),
                         names_from = .data$name,
                         values_from = .data$value) %>%
      dplyr::select(-.data$cohortId)
    
    sketch <- htmltools::withTags(table(
      class = "display",
      thead(
        tr(
          th(rowspan = 2, "Rule Sequence ID"),
          th(rowspan = 2, "Rule Name"),
          lapply(databaseIds, th, colspan = 4, class = "dt-center")
        ),
        tr(
          lapply(rep(c("Meet", "Gain", "Remain", "Total"), length(databaseIds)), th)
        )
      )
    ))
    
    options = list(pageLength = 100,
                   lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(minCellCountDef(1 + (1:(length(databaseIds) * 4)))))
    
    table <- DT::datatable(table,
                           options = options,
                           colnames = colnames(table) %>% camelCaseToTitleCase(),
                           rownames = FALSE,
                           container = sketch,
                           escape = FALSE,
                           filter = "top",
                           class = "stripe nowrap compact")
    
    # table <- DT::formatStyle(table = table,
    #                          columns = 2 + (1:(length(databaseIds) * 4)),
    #                          background = DT::styleColorBar(lims, "lightblue"),
    #                          backgroundSize = "98% 88%",
    #                          backgroundRepeat = "no-repeat",
    #                          backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  
  # Index event breakdown ----------------------------------------------------------------
  output$breakdownTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen chosen"))
    data <- getIndexEventBreakdown(dataSource = dataSource,
                                   cohortIds = cohortId(),
                                   databaseIds = databaseIds())
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    databaseIds <- unique(data$databaseId)
    data <- data %>% 
      dplyr::arrange(.data$databaseId) %>% 
      tidyr::pivot_wider(id_cols = c("conceptId", "conceptName", "domainId", 
                                     "vocabularyId", "standardConcept", 
                                     "domainTable", "domainField"),
                         names_from = "databaseId", 
                         values_from = c("conceptCount", "subjectCount"))
    
    data <- data[order(-data[8]), ]
    
    sketch <- htmltools::withTags(table(
      class = "display",
      thead(
        tr(
          th(rowspan = 2, "Concept Id"),
          th(rowspan = 2, "Concept Name"),
          th(rowspan = 2, "Domain Id"),
          th(rowspan = 2, "Vocabulary Id"),
          th(rowspan = 2, "Standard Concept"),
          th(rowspan = 2, "Domain Table"),
          th(rowspan = 2, "Domain Field"),
          lapply(databaseIds, th, colspan = 2, class = "dt-center")
        ),
        tr(
          lapply(rep(c("Concept Count", "Subject Count"), length(databaseIds)), th)
        )
      )
    ))
    
    options = list(pageLength = 100,
                   lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(minCellCountDef(1 + 1:(length(databaseIds) * 2))))
    dataTable <- DT::datatable(data,
                               options = options,
                               rownames = FALSE,
                               container = sketch,
                               colnames = colnames(data) %>% 
                                 camelCaseToTitleCase(),
                               escape = FALSE,
                               filter = "top",
                               class = "stripe nowrap compact")
    dataTable <- DT::formatStyle(table = dataTable,
                                 columns = 7 + 1:(length(databaseIds) * 2),
                                 background = DT::styleColorBar(c(0, maxCount), "lightblue"),
                                 backgroundSize = "98% 88%",
                                 backgroundRepeat = "no-repeat",
                                 backgroundPosition = "center")
    return(dataTable)
  }, server = TRUE)
  
  # Visit Context ---------------------------------------------------------------------------------------------
  output$visitContextTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    data <- getVisitContextResults(dataSource = dataSource,
                                   cohortIds = cohortId(), 
                                   databaseIds = databaseIds())
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohort")))
    }
    
    databaseIds <- sort(unique(data$databaseId))
    
    if (!all(databaseIds() %in% databaseIds)) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(databaseIds(), databaseIds), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    
    maxSubjects <- max(data$subjects)
    visitContextReference <-  expand.grid(visitContext = c("Before", "During visit", "On visit start", "After"), 
                                          visitConceptName = unique(data$visitConceptName),
                                          databaseId = databaseIds) %>% 
      tidyr::tibble()
    
    table <- visitContextReference %>% 
      dplyr::left_join(data, by = c("visitConceptName", "visitContext", "databaseId")) %>% 
      dplyr::select(.data$visitConceptName, .data$visitContext, .data$subjects, .data$databaseId) %>% 
      dplyr::mutate(visitContext = paste0(.data$databaseId, "_", .data$visitContext)) %>% 
      dplyr::select(-.data$databaseId) %>%
      dplyr::arrange(.data$visitConceptName) %>% 
      tidyr::pivot_wider(id_cols = c(.data$visitConceptName),
                         names_from = .data$visitContext,
                         values_from = .data$subjects) %>% 
      dplyr::relocate(.data$visitConceptName) %>%
      dplyr::mutate(visitConceptName = as.factor(visitConceptName))
    
    sketch <- htmltools::withTags(table(
      class = "display",
      thead(
        tr(
          th(rowspan = 2, "Visit"),
          lapply(databaseIds, th, colspan = 4, class = "dt-center")
        ),
        tr(
          lapply(rep(c("Visits Before", "Visits Ongoing", "Starting Simultaneous", "Visits After"), length(databaseIds)), th)
        )
      )
    ))
    
    options = list(pageLength = 100,
                   lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(truncateStringDef(0, 30),
                                     minCellCountDef(1:(length(databaseIds) * 4))))
    
    table <- DT::datatable(table,
                           options = options,
                           colnames = colnames(table) %>% 
                             camelCaseToTitleCase(),
                           rownames = FALSE,
                           container = sketch,
                           escape = TRUE,
                           filter = "top")
    
    table <- DT::formatStyle(table = table,
                             columns = 1:(length(databaseIds) * 4),
                             background = DT::styleColorBar(c(0, maxSubjects), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    
  }, server = TRUE)
  
  # Characterization --------------------------------------------------
  output$characterizationTable <- DT::renderDataTable(expr = {
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    if (input$charType == "Pretty") {
      analysisIds <- prettyAnalysisIds
    } else {
      analysisIds <- NULL
    }
    data <- getCovariateValueResult(dataSource = dataSource,
                                    analysisIds = analysisIds,
                                    cohortIds = cohortId(),
                                    databaseIds = databaseIds(),
                                    isTemporal = FALSE)
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    
    databaseIds <- sort(unique(data$databaseId))
    if (!all(databaseIds() %in% databaseIds)) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(databaseIds(), databaseIds), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    if (input$charType == "Pretty") {
      countData <- getCohortCountResult(dataSource = dataSource,
                                        databaseIds = databaseIds(),
                                        cohortIds = cohortId()) %>%
        dplyr::arrange(.data$databaseId)
      
      table <- data %>% 
        prepareTable1()
      if (nrow(table) == 0) {
        return(dplyr::tibble(Note = "There is no data to return."))       
      }
      
      characteristics <- table %>% 
        dplyr::select(.data$characteristic, .data$position, 
                      .data$header, .data$sortOrder) %>% 
        dplyr::distinct() %>% 
        dplyr::group_by(.data$characteristic, .data$position, .data$header) %>% 
        dplyr::summarise(sortOrder = max(.data$sortOrder)) %>% 
        dplyr::ungroup() %>% 
        dplyr::arrange(.data$position, desc(.data$header)) %>% 
        dplyr::mutate(sortOrder = dplyr::row_number()) %>%
        dplyr::distinct() 
      
      characteristics <- dplyr::bind_rows(characteristics %>% 
                                            dplyr::filter(.data$header == 1) %>% 
                                            dplyr::mutate(cohortId = sort(cohortId())[[1]], 
                                                          databaseId = sort(databaseIds[[1]])),
                                          characteristics %>% 
                                            dplyr::filter(.data$header == 0) %>% 
                                            tidyr::crossing(dplyr::tibble(databaseId = databaseIds)) %>% 
                                            tidyr::crossing(dplyr::tibble(cohortId = cohortId()))) %>% 
        dplyr::arrange(.data$sortOrder, .data$databaseId, .data$cohortId)
      
      table <- characteristics %>% 
        dplyr::left_join(table %>% 
                           dplyr::select(-.data$sortOrder),
                         by = c("characteristic", "position", "header", "databaseId", "cohortId"))  %>% 
        dplyr::arrange(.data$sortOrder) %>% 
        tidyr::pivot_wider(id_cols = c("cohortId", "characteristic"), 
                           names_from = "databaseId",
                           values_from = "value" ,
                           names_sep = "_",
                           names_prefix = "Value_")
      table <- table %>%
        dplyr::relocate(.data$characteristic) %>% 
        dplyr::select(-.data$cohortId)
      
      options = list(pageLength = 100,
                     lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                     searching = TRUE,
                     scrollX = TRUE,
                     scrollY = TRUE,
                     lengthChange = TRUE,
                     ordering = FALSE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       minCellPercentDef(1:length(databaseIds))
                     ))
      sketch <- htmltools::withTags(table(
        class = "display",
        thead(
          tr(
            th(rowspan = 2, "Covariate Name"),
            lapply(databaseIds, th, colspan = 1, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Proportion"), 
                       length(databaseIds)), th)
          )
        )
      ))
      table <- DT::datatable(table,
                             options = options,
                             rownames = FALSE,
                             container = sketch, 
                             escape = FALSE,
                             filter = "top",
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns = 1 + 1:length(databaseIds),
                               background = DT::styleColorBar(c(0,1), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    } else {
      data <- data %>% 
        dplyr::arrange(.data$databaseId, .data$cohortId) %>% 
        tidyr::pivot_longer(cols = c(.data$mean, .data$sd)) %>% 
        dplyr::mutate(name = paste0(databaseId, "_", .data$name)) %>% 
        tidyr::pivot_wider(id_cols = c(.data$cohortId, .data$covariateId),
                           names_from = .data$name,
                           values_from = .data$value) %>%
        dplyr::inner_join(data %>% dplyr::select(.data$covariateId, 
                                                 .data$covariateName, 
                                                 .data$conceptId) %>% 
                            dplyr::distinct(),
                          by = "covariateId") %>%
        dplyr::select(-.data$covariateId) %>%
        dplyr::select(-.data$cohortId) %>% 
        dplyr::relocate(.data$covariateName, .data$conceptId)
      
      data <- data[order(-data[3]), ]
      
      options = list(pageLength = 1000,
                     lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                     searching = TRUE,
                     searchHighlight = TRUE,
                     scrollX = TRUE,
                     scrollY = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(1, 150),
                       minCellRealDef(1 + 1:(length(databaseIds)*2), digits = 3)))
      sketch <- htmltools::withTags(table(
        class = "display",
        thead(
          tr(
            th(rowspan = 2, "Covariate Name"),
            th(rowspan = 2, "Concept Id"),
            lapply(databaseIds, th, colspan = 2, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Mean", "SD"), length(databaseIds)), th)
          )
        )
      ))
      table <- DT::datatable(data,
                             options = options,
                             rownames = FALSE,
                             container = sketch, 
                             escape = FALSE,
                             filter = "top",
                             class = "stripe nowrap compact")
      table <- DT::formatStyle(table = table,
                               columns = (2 + (1:length(databaseIds) * 2)),
                               background = DT::styleColorBar(c(0,1), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    }
    return(table)
  })
  
  # Temporal characterization -----------------------------------------------------------------
  temporalCharacterization <- shiny::reactive({
    validate(need(length(timeId()) > 0, "No time periods selected"))
    validate(need(length(input$database) > 0, "No data sources chosen"))
    validate(need(length(cohortId()) > 0, "No cohorts chosen"))
    data <- getCovariateValueResult(dataSource = dataSource,
                                    cohortIds = cohortId(),
                                    databaseIds = input$database,
                                    timeIds = timeId(),
                                    isTemporal = TRUE) %>% 
      dplyr::select(-.data$cohortId, -.data$databaseId, -.data$covariateId)
  })
  
  output$temporalCharacterizationTable <- DT::renderDataTable(expr = {
    data <- temporalCharacterization()
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    
    table <- data %>% 
      dplyr::inner_join(temporalCovariateChoices, by = "timeId") %>% 
      dplyr::arrange(.data$timeId)  %>% 
      tidyr::pivot_wider(id_cols = c("covariateName", "conceptId"), 
                         names_from = "choices",
                         values_from = "mean" ,
                         names_sep = "_") %>% 
      dplyr::relocate(.data$covariateName) %>%
      dplyr::arrange( dplyr::desc(dplyr::across(dplyr::starts_with('Start'))))
    
    temporalCovariateChoicesSelected <- temporalCovariateChoices %>% 
      dplyr::filter(.data$timeId %in% c(timeId())) %>% 
      dplyr::arrange(.data$timeId)
    
    options = list(pageLength = 1000,
                   lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(
                     truncateStringDef(1, 40),
                     minCellPercentDef(1 + 1:(length(temporalCovariateChoicesSelected$choices)))))
    
    table <- DT::datatable(table,
                           options = options,
                           rownames = FALSE,
                           colnames = colnames(table) %>% 
                             camelCaseToTitleCase(),
                           escape = FALSE,
                           filter = "top",
                           class = "stripe nowrap compact",
                           callback =  DT::JS("table.on('click.dt', 'td', function() {
                                            var row_=table.row(this).data();
                                            var data = [row_];
                                            Shiny.onInputChange('rows',data );});"))
    table <- DT::formatStyle(table = table,
                             columns = (2 + (1:length(temporalCovariateChoicesSelected$choices))), #0 index
                             background = DT::styleColorBar(c(0,1), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  #Cohort Overlap ------------------------
  cohortOverlap <- reactive({
    validate(need(length(databaseIds()) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    combisOfTargetComparator <- tidyr::crossing(targetCohortId = cohortIds(),
                                                comparatorCohortId = cohortIds()) %>% 
      dplyr::filter(!.data$targetCohortId == .data$comparatorCohortId) %>% 
      dplyr::distinct()
    validate(need(nrow(combisOfTargetComparator) > 0, paste0("Please select at least two cohorts.")))
    
    data <- getCohortOverlapResult(dataSource = dataSource, 
                                   targetCohortIds = combisOfTargetComparator$targetCohortId, 
                                   comparatorCohortIds = combisOfTargetComparator$comparatorCohortId, 
                                   databaseIds = databaseIds())
  })
  
  output$overlapPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(cohortIds()) > 0, paste0("Please select Target Cohort(s)")))
    
    data <- cohortOverlap()
    validate(need(!is.null(data), paste0("No cohort overlap data for this combination")))
    
    plot <- plotCohortOverlap(data = data,
                              shortNameRef = cohort,
                              yAxis = input$overlapPlotType)
    return(plot)
  })
  
  # Compare cohort characteristics --------------------------------------------
  
  computeBalance <- shiny::reactive({
    validate(need((length(cohortId()) > 0), paste0("Please select cohort.")))
    validate(need((length(comparatorCohortId()) > 0), paste0("Please select comparator cohort.")))
    validate(need((comparatorCohortId() != cohortId()), paste0("Please select different cohort and comarator.")))
    validate(need((length(input$database) > 0), paste0("Please select atleast one datasource.")))
    covs1 <- getCovariateValueResult(dataSource = dataSource,
                                     cohortIds = cohortId(),
                                     databaseIds = input$database,
                                     isTemporal = FALSE)
    
    covs2 <- getCovariateValueResult(dataSource = dataSource,
                                     cohortIds = comparatorCohortId(),
                                     databaseIds = input$database,
                                     isTemporal = FALSE)
    
    balance <- compareCohortCharacteristics(covs1, covs2) %>%
      dplyr::mutate(absStdDiff = abs(.data$stdDiff))
    return(balance)
  })
  
  output$charCompareTable <- DT::renderDataTable(expr = {
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
          dplyr::select(-.data$cohortId1, -.data$cohortId2)
      } else {
        return(dplyr::tibble(Note = "No data for covariates that are part of pretty table."))
      }
      
      options = list(pageLength = 100,
                     lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                     searching = TRUE,
                     scrollX = TRUE,
                     searchHighlight = TRUE,
                     lengthChange = TRUE,
                     ordering = FALSE,
                     paging = TRUE,
                     columnDefs = list(minCellPercentDef(1:2)))
      
      table <- DT::datatable(table,
                             options = options,
                             rownames = FALSE,
                             colnames = c("Characteristic", "Target", "Comparator","Std. Diff."),
                             escape = FALSE,
                             filter = "top",
                             class = "stripe nowrap compact")
      table <- DT::formatStyle(table = table,
                               columns = 2:4,
                               background = DT::styleColorBar(c(0,1), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
      table <- DT::formatStyle(table = table,
                               columns = 4,
                               background = styleAbsColorBar(1, "lightblue", "pink"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
      table <- DT::formatRound(table, 4, digits = 2)
    } else {
      table <- balance %>% 
        dplyr::select(.data$cohortId1,
                      .data$cohortId2,
                      .data$covariateName, 
                      .data$conceptId, 
                      .data$mean1, 
                      .data$sd1, 
                      .data$mean2, 
                      .data$sd2, 
                      .data$stdDiff) %>%  
        dplyr::select(-.data$cohortId1, -.data$cohortId2) %>% 
        dplyr::arrange(desc(abs(.data$stdDiff)))
      
      options = list(pageLength = 100,
                     lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                     searching = TRUE,
                     searchHighlight = TRUE,
                     scrollX = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       minCellRealDef(2:6, digits = 2)))
      
      table <- DT::datatable(table,
                             options = options,
                             rownames = FALSE,
                             colnames = c("Covariate Name", 
                                          "Concept ID", 
                                          "Mean Target", 
                                          "SD Target", 
                                          "Mean Comparator", 
                                          "SD Comparator", 
                                          "StdDiff"),
                             escape = FALSE,
                             filter = "top",
                             class = "stripe nowrap compact")
      table <- DT::formatStyle(table = table,
                               columns = c(3, 5),
                               background = DT::styleColorBar(c(0,1), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
      table <- DT::formatStyle(table = table,
                               columns = 7,
                               background = styleAbsColorBar(1, "lightblue", "pink"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    }
    return(table)
  }, server = TRUE)
  
  output$charComparePlot <- ggiraph::renderggiraph(expr = {
    data <- computeBalance()
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = "No data for the selected combination."))
    }
    plot <- plotCohortComparisonStandardizedDifference(balance = data,
                                                       shortNameRef = cohort,
                                                       domain = input$domainId)
    return(plot)
  })
  
  output$databaseInformationTable <- DT::renderDataTable(expr = {
    table <- database[, c("databaseId", "databaseName", "description")]
    options = list(pageLength = 100,
                   lengthMenu = list(c(10, 100, 1000, -1), c("10", "100", "1000", "All")),
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   searchHighlight = TRUE,
                   columnDefs = list(list(width = "30%", targets = 1),
                                     list(width = "60%", targets = 2))
    )
    table <- DT::datatable(table,
                           options = options,
                           colnames = c("ID", "Name", "Description"),
                           rownames = FALSE,
                           class = "stripe compact")
    return(table)
  }, server = TRUE)
  
  # Infoboxes ------------------------------------------------------------------------
  showInfoBox <- function(title, htmlFileName) {
    shiny::showModal(shiny::modalDialog(
      title = title,
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(readChar(htmlFileName, file.info(htmlFileName)$size) )
    ))
  }
  
  shiny::observeEvent(input$cohortCountsInfo, {
    showInfoBox("Cohort Counts", "html/cohortCounts.html")
  })
  
  shiny::observeEvent(input$incidenceRateInfo, {
    showInfoBox("Incidence Rate", "html/incidenceRate.html")
  })
  
  shiny::observeEvent(input$timeDistributionInfo, {
    showInfoBox("Time Distributions", "html/timeDistribution.html")
  })
  
  shiny::observeEvent(input$includedConceptsInfo, {
    showInfoBox("Concepts in data source", "html/conceptsInDataSource.html")
  })
  
  shiny::observeEvent(input$orphanConceptsInfo, {
    showInfoBox("Orphan (Source) Concepts", "html/orphanConcepts.html")
  })
  
  shiny::observeEvent(input$conceptSetDiagnosticsInfo, {
    showInfoBox("Concept Set Diagnostics", "html/conceptSetDiagnostics.html")
  })
  
  shiny::observeEvent(input$inclusionRuleStatsInfo, {
    showInfoBox("Inclusion Rule Statistics", "html/inclusionRuleStats.html")
  })
  
  shiny::observeEvent(input$indexEventBreakdownInfo, {
    showInfoBox("Index Event Breakdown", "html/indexEventBreakdown.html")
  })
  
  shiny::observeEvent(input$visitContextInfo, {
    showInfoBox("Visit Context", "html/visitContext.html")
  })
  
  shiny::observeEvent(input$cohortCharacterizationInfo, {
    showInfoBox("Cohort Characterization", "html/cohortCharacterization.html")
  })
  
  shiny::observeEvent(input$temporalCharacterizationInfo, {
    showInfoBox("Temporal Characterization", "html/temporalCharacterization.html")
  })
  
  shiny::observeEvent(input$cohortOverlapInfo, {
    showInfoBox("Cohort Overlap", "html/cohortOverlap.html")
  })
  
  shiny::observeEvent(input$compareCohortCharacterizationInfo, {
    showInfoBox("Compare Cohort Characteristics", "html/compareCohortCharacterization.html")
  })
  
  # Cohort labels --------------------------------------------------------------------------------------------
  targetCohortCount <- shiny::reactive({
    targetCohortWithCount <- getCohortCountResult(dataSource = dataSource,
                                                  cohortIds = cohortId(),
                                                  databaseIds = input$database) %>% 
      dplyr::left_join(y = cohort, by = "cohortId") %>% 
      dplyr::arrange(.data$cohortName)
    return(targetCohortWithCount)
  }) 
  
  targetCohortCountHtml <- shiny::reactive({
    targetCohortCount <- targetCohortCount()
    
    return(htmltools::withTags(
      div(
        h5("Target: ", targetCohortCount$cohortName, " ( n = ", scales::comma(x = targetCohortCount$cohortSubjects), " )")
      )
    )
    )
  })
  
  selectedCohorts <- shiny::reactive({
    cohorts <- cohortSubset() %>%
      dplyr::filter(.data$cohortId %in% cohortIds()) %>%
      dplyr::arrange(.data$cohortId) %>%
      dplyr::select(.data$shortName, .data$cohortName)
    return(apply(cohorts, 1, function(x) tags$tr(lapply(x, tags$td))))
  })
  
  selectedCohort <- shiny::reactive({
    return(input$cohort)
  })
  
  selectedComparatorCohort <- shiny::reactive({
    return(input$comparatorCohort)
  })
  
  output$cohortCountsSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$indexEventBreakdownSelectedCohort <- shiny::renderUI({selectedCohort()})
  output$characterizationSelectedCohort <- shiny::renderUI({selectedCohort()})
  output$inclusionRuleStatSelectedCohort <- shiny::renderUI({selectedCohort()})
  output$cohortOverlapSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$incidenceRateSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$timeDistSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$visitContextSelectedCohort <- shiny::renderUI({selectedCohort()})
  output$temporalCharacterizationSelectedCohort <- shiny::renderUI({selectedCohort()})
  output$cohortCharCompareSelectedCohort <- shiny::renderUI({
    htmltools::withTags(
      table(
        tr(
          td(selectedCohort())
        ),
        tr(
          td(selectedComparatorCohort())
        )
      )
    ) 
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
