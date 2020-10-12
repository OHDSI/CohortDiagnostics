library(magrittr)

source("R/DisplayFunctions.R")
source("R/Tables.R")
source("R/Plots.R")
source("R/Results.R")

shiny::shinyServer(function(input, output, session) {
  
  cohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$cohortName == input$cohort])
  })
  
  comparatorCohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$cohortName == input$comparator])
  })
  
  cohortIds <- shiny::reactive({
    return(cohort$cohortId[cohort$cohortName  %in% input$cohorts])
  })
  
  timeId <- shiny::reactive({
    return(temporalCovariateChoices %>%
             dplyr::filter(choices %in% input$timeIdChoices) %>%
             dplyr::pull(timeId))
  })
  
  phenotypeId <- shiny::reactive({
    return(phenotypeDescription$phenotypeId[phenotypeDescription$phenotypeName %in% input$phenotypes])
  })
  
  cohortSubset <- shiny::reactive({
    if (exists("phenotypeDescription")) {
      return(cohort %>%
               dplyr::filter(.data$phenotypeId == phenotypeId()) %>%
               dplyr::arrange(.data$cohortId))
    } else {
      return(cohort %>%
               dplyr::arrange(.data$cohortId))
    }
  })
  
  phenotypeSubset <- shiny::reactive({
    if (exists("phenotypeDescription")) {
      return(phenotypeDescription %>% 
               dplyr::arrange(.data$phenotypeId))
    }
  })
  
  shiny::observe({
    subset <- unique(conceptSets$conceptSetName[conceptSets$cohortId == cohortId()]) %>% sort()
    shinyWidgets::updatePickerInput(session = session,
                                    inputId = "conceptSet",
                                    choicesOpt = list(style = rep_len("color: black;", 999)),
                                    choices = subset)
  })
  
  shiny::observe({
    subset <- cohortSubset()$cohortName
    shinyWidgets::updatePickerInput(session = session,
                                    inputId = "cohort",
                                    choicesOpt = list(style = rep_len("color: black;", 999)),
                                    choices = subset)
  })
  
  shiny::observe({
    subset <- cohortSubset()$cohortName
    shinyWidgets::updatePickerInput(session = session,
                                    inputId = "cohorts",
                                    choicesOpt = list(style = rep_len("color: black;", 999)),
                                    choices = subset,
                                    selected = c(subset[1], subset[2]))
  })
  
  extractedData <- reactiveVal()
  extractedData(c())
  
  # Phenotype Description ------------------------------------------------------------------------------
  output$phenoTypeDescriptionTable <- DT::renderDataTable(expr = {
    data <- phenotypeDescription %>% 
      dplyr::mutate(dplyr::across(.cols = dplyr::everything(), tidyr::replace_na, "")) %>% 
      dplyr::mutate(literatureReview = dplyr::case_when(!.data$literatureReview %in% c("","0") ~ 
                                                          paste0("<a href='", .data$literatureReview, "' target='_blank'>", "Link", "</a>"),
                                                        TRUE ~ "Ongoing")) %>%
      dplyr::mutate(phenotypeId = paste0("<a href='", paste0(conceptBaseUrl, .data$referentConceptId), "' target='_blank'>", .data$phenotypeId, "</a>")) %>% 
      dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                   pattern = "Overview:", 
                                                                   replacement = "<strong>Overview:</strong>"))  %>% 
      dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                   pattern = "Assessment:", 
                                                                   replacement = "<br/> <strong>Assessment:</strong>")) %>% 
      dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                   pattern = "Presentation:", 
                                                                   replacement = "<br/> <strong>Presentation: </strong>")) %>% 
      dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription,
                                                                   pattern = "Plan:",
                                                                   replacement = "<br/> <strong>Plan: </strong>")) %>% 
      dplyr::select(-.data$referentConceptId)
    
    options = list(pageLength = 10,
                   searching = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE,
                   searchHighlight = TRUE)
    
    dataTable <- DT::datatable(data,
                               options = options,
                               rownames = FALSE,
                               colnames = colnames(data) %>% 
                                 camelCaseToTitleCase(),
                               escape = FALSE,
                               filter = c("bottom"),
                               selection = list(mode = "single", target = "row"),
                               class = "stripe compact")
    return(dataTable)
  }, server = TRUE)
  
  # Cohort Description ---------------------------------------------------------
  output$cohortDescriptionTable <- DT::renderDataTable(expr = {
    data <- cohortSubset() %>%
      dplyr::select(.data$phenotypeId, .data$cohortId, 
                    .data$cohortName, .data$logicDescription) 
    
    options = list(pageLength = 10,
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
                               filter = c("bottom"),
                               selection = list(mode = "single", target = "row"),
                               class = "stripe compact")
    return(dataTable)
  }, server = TRUE)
  
  selectedCohortDescriptionRow <- reactive({
    idx <- input$cohortDescriptionTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      subset <- cohortSubset()
      if (nrow(subset) == 0) {
        return(NULL)
      }
      row <- subset[idx, ]
      return(row)
    }
  })
  
  output$cohortDescriptionRowIsSelected <- reactive({
    return(!is.null(selectedCohortDescriptionRow()))
  })
  outputOptions(output, "cohortDescriptionRowIsSelected", suspendWhenHidden = FALSE)
  
  output$cohortDetailsText <- shiny::renderUI({
    row <- selectedCohortDescriptionRow()
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
  
  output$cohortDescriptionDefinition <- shiny::renderUI({
    row <- selectedCohortDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      cohortExtra %>%
        dplyr::filter(.data$cohortId == row$cohortId) %>%
        dplyr::pull(.data$html) %>%
        shiny::HTML()
    }
  })
  
  output$cohortDescriptionJson <- shiny::renderText({
    row <- selectedCohortDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$json
    }
  })
  
  output$cohortDescriptionSql <- shiny::renderText({
    row <- selectedCohortDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row$sql
    }
  })
  
  output$cohortDescriptionConceptSetsTable <- DT::renderDataTable(expr = {
    row <- selectedCohortDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } 
    
    if (is(dataSource, "environment") || input$conceptSetsType == "Concept Set Expression") {
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
    
    options = list(pageLength = 10,
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
                               filter = c("top"),
                               class = "stripe nowrap compact")
    return(dataTable)
    
  })
  
  selectedPhenotypeDescriptionRow <- reactive({
    idx <- input$phenoTypeDescriptionTable_rows_selected
    if (is.null(idx)) {
      return(NULL)
    } else {
      subset <- phenotypeSubset()
      if (nrow(subset) == 0) {
        return(NULL)
      }
      row <- subset[idx, ]
      return(row)
    }
  })
  
  output$phenotypeRowIsSelected <- reactive({
    return(!is.null(selectedPhenotypeDescriptionRow()))
  })
  outputOptions(output, "phenotypeRowIsSelected", suspendWhenHidden = FALSE)
  
  output$phenotypeDescriptionText <- shiny::renderUI({
    row <- selectedPhenotypeDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      row <- row %>% 
        dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                     pattern = "Overview:", 
                                                                     replacement = "<strong>Overview:</strong>"))  %>% 
        dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                     pattern = "Assessment:", 
                                                                     replacement = "<br/> <strong>Assessment:</strong>")) %>% 
        dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                     pattern = "Presentation:", 
                                                                     replacement = "<br/> <strong>Presentation: </strong>")) %>% 
        dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription,
                                                                     pattern = "Plan:",
                                                                     replacement = "<br/> <strong>Plan: </strong>"))
      
      HTML(row$clinicalDescription)
    }
  })
  
  output$phenotypeLiteratureReviewText <- shiny::renderUI({
    row <- selectedPhenotypeDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      tags$p(row$literatureReview)
    }
  })
  
  output$phenotypeNotesText <- shiny::renderUI({
    row <- selectedPhenotypeDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      tags$p(row$phenotypeNotes)
    }
  })
  
  # Cohort Counts ---------------------------------------------------------------------------
  output$cohortCountsTable <- DT::renderDataTable(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    
    data <- getCohortCountResult(dataSource = dataSource,
                                 databaseIds = input$databases) %>%
      dplyr::left_join(cohort, by = "cohortId") %>% 
      dplyr::filter(.data$phenotypeId == phenotypeId()) %>% 
      dplyr::select(.data$cohortId, 
                    .data$cohortName, 
                    .data$databaseId, 
                    .data$webApiCohortId,
                    .data$cohortSubjects, 
                    .data$cohortEntries)
    
    if (nrow(data) == 0) {
      return(tidyr::tibble("There is no data on any cohort"))
    }
    
    if (!isTRUE(all.equal(data$databaseId %>% unique %>% sort(),
                          input$databases %>% unique() %>% sort()))) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases, 
                                                        data$databaseId %>% unique()), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    table <- dplyr::full_join(
      data %>% 
        dplyr::select(.data$cohortId, .data$databaseId, 
                      .data$cohortSubjects, .data$cohortName) %>% 
        dplyr::mutate(columnName = paste0(.data$databaseId, "_subjects")) %>% 
        tidyr::pivot_wider(id_cols = c(.data$cohortId, .data$cohortName),
                           names_from = columnName,
                           values_from = .data$cohortSubjects),
      data %>% 
        dplyr::select(.data$cohortId, .data$databaseId, 
                      .data$cohortEntries, .data$cohortName) %>% 
        dplyr::mutate(columnName = paste0(.data$databaseId, "_entries")) %>% 
        tidyr::pivot_wider(id_cols = c(.data$cohortId, .data$cohortName),
                           names_from = columnName,
                           values_from = .data$cohortEntries),
      by = c("cohortId", "cohortName"))
    table <- table %>% 
      dplyr::select(order(colnames(table))) %>% 
      dplyr::relocate(.data$cohortId)
    
    table <- data %>% 
      dplyr::select(.data$cohortId, .data$cohortName, .data$webApiCohortId) %>% 
      dplyr::distinct() %>% 
      dplyr::inner_join(table, by = c("cohortId", "cohortName")) %>%
      dplyr::arrange(.data$cohortName)
    
    
    if (!is.null(cohortBaseUrl)) {
      table <- table %>%
        dplyr::mutate(url = paste0(cohortBaseUrl, .data$webApiCohortId),
                      cohortName = paste0("<a href='", 
                                          .data$url, 
                                          "' target='_blank'>", 
                                          .data$cohortName, 
                                          "</a>")) %>%
        dplyr::select(-.data$url)
    }
    table <- table %>%
      dplyr::select(-.data$cohortId, -.data$webApiCohortId) 
    
    databaseIds <- unique(data$databaseId)
    
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
    
    options = list(pageLength = 10,
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
                               filter = c("bottom"),
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
  
  incidenceRate <- reactive({
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
    data <- getIncidenceRateResult(dataSource = dataSource,
                                   cohortIds = cohortIds(), 
                                   databaseIds = input$databases, 
                                   stratifyByGender =  stratifyByGender,
                                   stratifyByAgeGroup =  stratifyByAge,
                                   stratifyByCalendarYear =  stratifyByCalendarYear,
                                   minPersonYears = 1000) %>% 
      dplyr::mutate(incidenceRate = dplyr::case_when(.data$incidenceRate < 0 ~ 0, 
                                                     TRUE ~ .data$incidenceRate))
  })
  
  output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
    extractedData(incidenceRate())
    
    validate(need(!is.null(extractedData()), paste0("No data for this combination")),
             need(nrow(extractedData()) > 0, paste0("No data for this combination")))
    
    plot <- plotIncidenceRate(data = extractedData(),
                              cohortIds = NULL,
                              databaseIds = NULL,
                              stratifyByAgeGroup = stratifyByAge,
                              stratifyByGender = stratifyByGender,
                              stratifyByCalendarYear  = stratifyByCalendarYear,
                              yscaleFixed =   input$irYscaleFixed)
    return(plot)
  })
  
  timeDist <- reactive({
    data <- getTimeDistributionResult(dataSource = dataSource,
                                      cohortIds = cohortIds(), 
                                      databaseIds = input$databases)
  })
  
  output$timeDisPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    extractedData(timeDist())
    validate(need(!is.null(extractedData()), paste0('No data for this combination')),
             need(nrow(extractedData()) > 0, paste0('No data for this combination')))
    
    plot <- plotTimeDistribution(data = extractedData(),
                                 cohortIds = cohortIds(),
                                 databaseIds = input$databases)
    return(plot)
  })
  
  output$timeDistTable <- DT::renderDataTable(expr = {
    
    table <- getTimeDistributionResult(dataSource = dataSource,
                                       cohortIds = cohortId(), 
                                       databaseIds = input$databases)
    
    if (is.null(table)) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    
    options = list(pageLength = 9,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE,
                   columnDefs = list(minCellCountDef(3)))
    table <- DT::datatable(table,
                           options = options,
                           rownames = FALSE,
                           colnames = colnames(table) %>% 
                             camelCaseToTitleCase(),
                           filter = c("bottom"),
                           class = "stripe nowrap compact")
    table <- DT::formatRound(table, c("Average", "SD"), digits = 2)
    table <- DT::formatRound(table, c("Min", "P10", "P25", "Median", "P75", "P90", "Max"), digits = 0)
    return(table)
  }, server = TRUE)
  
  
  output$includedConceptsTable <- DT::renderDataTable(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    
    data <- getIncludedConceptResult(dataSource = dataSource,
                                     cohortId = cohortId(),
                                     databaseIds = input$databases)
    data <- data %>%
      dplyr::filter(.data$conceptSetName == input$conceptSet)
    if (nrow(data) == 0) {
      return(dplyr::tibble("No data available for selected databases and cohorts"))
    }
    
    databaseIds <- unique(data$databaseId)
    
    if (!all(input$databases %in% databaseIds)) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases, databaseIds), 
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
      
      options = list(pageLength = 10,
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
                                 filter = c("bottom"),
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
      
      options = list(pageLength = 10,
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
                                 filter = c("bottom"),
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
  
  output$orphanConceptsTable <- DT::renderDataTable(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    
    data <- getOrphanConceptResult(dataSource = dataSource,
                                   cohortId = cohortId(),
                                   databaseIds = input$databases)
    data <- data %>%
      dplyr::filter(.data$conceptSetName == input$conceptSet)
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("There is no data for the selected combination.")))
    }
    databaseIds <- unique(data$databaseId)
    
    if (!all(input$databases %in% databaseIds)) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases, databaseIds), 
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
    
    options = list(pageLength = 10,
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
                           filter = c("bottom"),
                           class = "stripe nowrap compact")
    
    table <- DT::formatStyle(table = table,
                             columns =  4 + (1:(length(databaseIds)*2)),
                             background = DT::styleColorBar(c(0, maxCount), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  output$inclusionRuleTable <- DT::renderDataTable(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    table <- getInclusionRuleStats(dataSource = dataSource,
                                   cohortIds = cohortId(),
                                   databaseIds = input$databases) 
    if (nrow(table) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    
    databaseIds <- unique(table$databaseId)
    
    if (!all(input$databases %in% databaseIds)) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases, databaseIds), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    table <- table %>% 
      tidyr::pivot_longer(cols = c(.data$meetSubjects, .data$gainSubjects, 
                                   .data$totalSubjects, .data$remainSubjects)) %>% 
      dplyr::group_by(.data$ruleSequenceId, .data$databaseId, .data$name, .data$ruleName) %>% 
      dplyr::summarise(value = sum(.data$value)) %>% 
      dplyr::mutate(name = paste0(databaseId, "_", .data$name)) %>% 
      tidyr::pivot_wider(id_cols = c(.data$ruleSequenceId, .data$ruleName),
                         names_from = .data$name,
                         values_from = .data$value)
    
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
    
    options = list(pageLength = 10,
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
                           filter = c("bottom"),
                           class = "stripe nowrap compact")
    
    # table <- DT::formatStyle(table = table,
    #                          columns = 2 + (1:(length(databaseIds) * 4)),
    #                          background = DT::styleColorBar(lims, "lightblue"),
    #                          backgroundSize = "98% 88%",
    #                          backgroundRepeat = "no-repeat",
    #                          backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  output$breakdownTable <- DT::renderDataTable(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    data <- getIndexEventBreakdown(dataSource = dataSource,
                                   cohortIds = cohortId(),
                                   databaseIds = input$databases)
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    databaseIds <- unique(data$databaseId) %>% sort()
    data <- data %>%
      dplyr::select(.data$conceptId, .data$conceptName, .data$databaseId, .data$conceptCount)
    table <- data[data$databaseId == databaseIds[1], ]
    table$databaseId <- NULL
    colnames(table)[3] <- databaseIds[1]
    if (length(databaseIds) > 1) {
      for (i in 2:length(databaseIds)) {
        temp <- data[data$databaseId == databaseIds[i],]
        temp$databaseId <- NULL        
        colnames(temp)[3] <- databaseIds[i]
        table <- merge(table, temp, all = TRUE)
      }
    }
    table <- table[order(-table[, 3]), ]
    colnames(table)[1:2] <- c("Concept ID", "Name")
    options = list(pageLength = 10,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(minCellCountDef(3:ncol(table) - 1)))
    dataTable <- DT::datatable(table,
                               options = options,
                               rownames = FALSE,
                               escape = FALSE,
                               filter = c("bottom"),
                               class = "stripe nowrap compact")
    dataTable <- DT::formatStyle(table = dataTable,
                                 columns = 3:ncol(table),
                                 background = DT::styleColorBar(c(0, maxCount), "lightblue"),
                                 backgroundSize = "98% 88%",
                                 backgroundRepeat = "no-repeat",
                                 backgroundPosition = "center")
    return(dataTable)
  }, server = TRUE)
  
  output$visitContextTable <- DT::renderDataTable(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    data <- getVisitContextResults(dataSource = dataSource,
                                   cohortIds = cohortId(), 
                                   databaseIds = input$databases)
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohort")))
    }
    
    databaseIds <- sort(unique(data$databaseId))
    
    if (!all(input$databases %in% databaseIds)) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases, databaseIds), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    maxSubjects <- max(data$subjects)
    visitContextReference <-  expand.grid(visitContext = c("Before", "During visit", "On visit start", "After"), 
                                          visitConceptName = unique(data$visitConceptName),
                                          databaseId = databaseIds)
    
    table <- visitContextReference %>% 
      dplyr::left_join(data, by = c("visitConceptName", "visitContext", "databaseId")) %>% 
      dplyr::select(.data$visitConceptName, .data$visitContext, .data$subjects, .data$databaseId) %>% 
      dplyr::mutate(visitContext = paste0(.data$databaseId, "_", .data$visitContext)) %>% 
      dplyr::select(-.data$databaseId) %>%
      tidyr::pivot_wider(id_cols = c(.data$visitConceptName),
                         names_from = .data$visitContext,
                         values_from = .data$subjects)
    
    sketch <- htmltools::withTags(table(
      class = "display",
      thead(
        tr(
          th(rowspan = 2, "Visit"),
          lapply(databaseIds, th, colspan = 4, class = "dt-center")
        ),
        tr(
          lapply(rep(c("Visits Before", "Visits Ongoing", "Starting Simultateous", "Visits After"), length(databaseIds)), th)
        )
      )
    ))
    
    options = list(pageLength = 10,
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
                           colnames = colnames(table) %>% camelCaseToTitleCase(),
                           rownames = FALSE,
                           container = sketch,
                           escape = TRUE,
                           filter = c("bottom"))
    
    table <- DT::formatStyle(table = table,
                             columns = 1:(length(databaseIds) * 4) + 1,
                             background = DT::styleColorBar(c(0, maxSubjects), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    
  }, server = TRUE)
  
  characterizationTable <- shiny::reactive({
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    if (input$charType == "Pretty") {
      analysisIds <- prettyAnalysisIds
    } else {
      analysisIds <- NULL
    }
    data <- getCovariateValueResult(dataSource = dataSource,
                                    analysisIds = analysisIds,
                                    cohortIds = cohortIds(),
                                    databaseIds = input$databases,
                                    isTemporal = FALSE)
    if ('shortName' %in% colnames(cohort)) {
      data <- data %>%  dplyr::inner_join(cohort %>% 
                                            dplyr::filter(.data$phenotypeId == phenotypeId()) %>%
                                            dplyr::select(.data$cohortId, .data$shortName, .data$cohortName))
    } else {
      data <- data %>%  dplyr::inner_join(cohort %>% 
                                            dplyr::filter(.data$phenotypeId == phenotypeId()) %>%
                                            dplyr::select(.data$cohortId, .data$cohortName) %>%
                                            dplyr::distinct() %>% 
                                            dplyr::mutate(shortName = paste0('C', dplyr::row_number())))
    }
  })
  
  output$characterizationTable <- DT::renderDataTable({
    extractedData(characterizationTable())
    if (nrow(extractedData()) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    
    databaseIds <- sort(unique(extractedData()$databaseId))
    if (!all(input$databases %in% databaseIds)) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases, databaseIds), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    if (input$charType == "Pretty") {
      countData <- getCohortCountResult(dataSource = dataSource,
                                        databaseIds = input$databases,
                                        cohortIds = cohortIds()) %>%
        dplyr::arrange(.data$databaseId)
      
      table <- extractedData() %>% 
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
                                            dplyr::mutate(cohortId = sort(cohortIds())[[1]], 
                                                          databaseId = sort(databaseIds[[1]])),
                                          characteristics %>% 
                                            dplyr::filter(.data$header == 0) %>% 
                                            tidyr::crossing(dplyr::tibble(databaseId = databaseIds)) %>% 
                                            tidyr::crossing(dplyr::tibble(cohortId = cohortIds()))) %>% 
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
      if ('shortName' %in% colnames(cohort)) {
        table <- table %>%  dplyr::inner_join(cohort %>% 
                                                dplyr::filter(.data$phenotypeId == phenotypeId()) %>%
                                                dplyr::select(.data$cohortId, .data$shortName))
      } else {
        table <- table %>%  dplyr::inner_join(cohort %>% 
                                                dplyr::filter(.data$phenotypeId == phenotypeId()) %>%
                                                dplyr::select(.data$cohortId) %>%
                                                dplyr::distinct() %>% 
                                                dplyr::mutate(shortName = paste0('C', dplyr::row_number())))
      }
      table <- table %>%
        dplyr::relocate(.data$shortName, .data$characteristic) %>% 
        dplyr::select(-.data$cohortId)
      
      options = list(pageLength = 100,
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
            th(rowspan = 2, "Cohorts"),
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
                             filter = c("bottom"),
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns = 3 + (1:length(databaseIds)),
                               background = DT::styleColorBar(c(0,1), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    } else {
      data <- extractedData() %>% 
        dplyr::arrange(.data$databaseId, .data$cohortId) %>% 
        tidyr::pivot_longer(cols = c(.data$mean, .data$sd)) %>% 
        dplyr::mutate(name = paste0(databaseId, "_", .data$name)) %>% 
        tidyr::pivot_wider(id_cols = c(.data$cohortId, .data$covariateId),
                           names_from = .data$name,
                           values_from = .data$value) %>%
        dplyr::inner_join(extractedData() %>% dplyr::select(.data$covariateId, 
                                                            .data$covariateName, 
                                                            .data$conceptId) %>% 
                            dplyr::distinct(),
                          by = "covariateId") %>%
        dplyr::select(-.data$covariateId) %>% 
        dplyr::relocate("cohortId", "covariateName", "conceptId") %>% 
        dplyr::distinct()
      
      if ('shortName' %in% colnames(cohort)) {
        data <- data %>%  dplyr::inner_join(cohort %>% 
                                              dplyr::filter(.data$phenotypeId == phenotypeId()) %>%
                                              dplyr::select(.data$cohortId, .data$shortName))
      } else {
        data <- data %>%  dplyr::inner_join(cohort %>% 
                                              dplyr::filter(.data$phenotypeId == phenotypeId()) %>%
                                              dplyr::select(.data$cohortId) %>%
                                              dplyr::distinct() %>% 
                                              dplyr::mutate(shortName = paste0('C', dplyr::row_number())))
      }
      
      data <- data %>%
        dplyr::select(-.data$cohortId) %>% 
        dplyr::relocate(.data$shortName, .data$covariateName, .data$conceptId)
      data <- data[order(-data[4]), ]
      
      options = list(pageLength = 100,
                     searching = TRUE,
                     searchHighlight = TRUE,
                     scrollX = TRUE,
                     scrollY = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       minCellRealDef(2:(2 + length(databaseIds)*2), digits = 3)))
      sketch <- htmltools::withTags(table(
        class = "display",
        thead(
          tr(
            th(rowspan = 2, "Cohorts"),
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
                             filter = c("bottom"),
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
  
  covariateIdArray <- reactiveVal()
  covariateIdArray(c())
  observeEvent(input$rows, {
    if (input$rows[[2]] %in% covariateIdArray())
      covariateIdArray(covariateIdArray()[covariateIdArray() %in% input$rows[[2]] == FALSE])
    else
      covariateIdArray(c(covariateIdArray(),input$rows[[2]]))
  })
  
  temporalCharacterizationTable <- shiny::reactive({
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    validate(need(length(timeId()) > 0, "No time periods selected"))
    
    data <- getCovariateValueResult(dataSource = dataSource,
                                    cohortIds = cohortIds(),
                                    databaseIds = input$databases,
                                    timeIds = timeId(),
                                    isTemporal = TRUE)
    
    if ('shortName' %in% colnames(cohort)) {
      data <- data %>%  
        dplyr::left_join(cohort %>%
                           dplyr::filter(.data$phenotypeId == phenotypeId()) %>%
                           dplyr::select(.data$cohortId, .data$cohortName, .data$shortName) %>%
                           dplyr::distinct()) %>% 
        dplyr::relocate(.data$shortName, .data$databaseId, .data$covariateName, .data$covariateId)
    } else {
      data <- data %>%  
        dplyr::left_join(cohort %>% 
                           dplyr::filter(.data$phenotypeId == phenotypeId()) %>%
                           dplyr::select(.data$cohortId, .data$cohortName) %>%
                           dplyr::distinct() %>% 
                           dplyr::mutate(shortName = paste0('C', dplyr::row_number()))) %>%
        dplyr::relocate(.data$shortName, .data$databaseId, .data$covariateName, .data$covariateId)
    }
  })
  
  output$temporalCharacterizationTable <- DT::renderDataTable(expr = {
    extractedData(temporalCharacterizationTable())
    if (nrow(extractedData()) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    
    table <- extractedData() %>% 
      dplyr::inner_join(temporalCovariateChoices, by = "timeId") %>% 
      dplyr::arrange(.data$timeId)  %>% 
      tidyr::pivot_wider(id_cols = c("cohortId", "shortName", "databaseId", "covariateId", "covariateName", "conceptId"), 
                         names_from = "choices",
                         values_from = "mean" ,
                         names_sep = "_") %>% 
      dplyr::relocate(.data$cohortId, .data$shortName, .data$databaseId, .data$covariateName, .data$covariateId) %>% 
      dplyr::rename(cohort = .data$shortName) %>% 
      dplyr::select(-.data$conceptId, -.data$cohortId) %>% 
      dplyr::arrange(.data$cohort, .data$databaseId, dplyr::desc(dplyr::across(dplyr::starts_with('Start'))))
    
    temporalCovariateChoicesSelected <- temporalCovariateChoices %>% 
      dplyr::filter(.data$timeId %in% c(timeId())) %>% 
      dplyr::arrange(.data$timeId)
    
    options = list(pageLength = 10,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(
                     truncateStringDef(2, 40),
                     minCellPercentDef( 3 + 1:(length(temporalCovariateChoicesSelected$choices)))))
    
    table <- DT::datatable(table,
                           options = options,
                           rownames = FALSE,
                           colnames = colnames(table) %>% 
                             camelCaseToTitleCase(),
                           escape = FALSE,
                           filter = c("bottom"),
                           class = "stripe nowrap compact",
                           callback =  DT::JS("table.on('click.dt', 'td', function() {
                                            var row_=table.row(this).data();
                                            var data = [row_];
                                            Shiny.onInputChange('rows',data );});"))
    table <- DT::formatStyle(table = table,
                             columns = (4 + (1:length(temporalCovariateChoicesSelected$choices))), #0 index
                             background = DT::styleColorBar(c(0,1), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  cohortOverlap <- reactive({
    combisOfTargetComparator <- tidyr::crossing(targetCohortId = cohortIds(),
                                                comparatorCohortId = cohortIds()) %>% 
      dplyr::filter(!.data$targetCohortId == .data$comparatorCohortId) %>% 
      dplyr::distinct()
    validate(need(nrow(combisOfTargetComparator) > 0, paste0("Please select atleast two cohorts.")))
    
    data <- getCohortOverlapResult(dataSource = dataSource, 
                                   targetCohortIds = combisOfTargetComparator$targetCohortId, 
                                   comparatorCohortIds = combisOfTargetComparator$comparatorCohortId, 
                                   databaseIds = input$databases)
  })
  
  output$overlapPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(cohortIds()) > 0, paste0("Please select Target Cohort(s)")))
    
    extractedData(cohortOverlap())
    validate(need(!is.null(extractedData()), paste0("No cohort overlap data for this combination")))
    
    plot <- plotCohortOverlap(data = extractedData(),
                              yAxis = input$overlapPlotType,
                              cohortIdLength = length(cohortIds()))
    return(plot)
  })
  
  computeBalance <- shiny::reactive({
    covs1 <- getCovariateValueResult(dataSource = dataSource,
                                     cohortIds = cohortIds(),
                                     databaseIds = input$databases,
                                     isTemporal = FALSE)
    covs2 <- getCovariateValueResult(dataSource = dataSource,
                                     cohortIds = cohortIds(),
                                     databaseIds = input$databases,
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
          dplyr::select(-.data$sortOrder)
      } else {
        return(dplyr::tibble(Note = "No data for covariates that are part of pretty table."))
      }
      
      options = list(pageLength = 100,
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
                             colnames = c("Characteristic", "Target", "Comparator","StdDiff"),
                             escape = FALSE,
                             filter = c("bottom"),
                             class = "stripe nowrap compact")
      table <- DT::formatStyle(table = table,
                               columns = 2:3,
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
        dplyr::select(.data$covariateName, .data$conceptId, .data$mean1, .data$sd1, .data$mean2, .data$sd2, .data$stdDiff)
      
      table <- table[order(-abs(table[7])), ]
      
      options = list(pageLength = 100,
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
                             colnames = c("Covariate Name", "Concept ID", "Mean Target", "SD Target", "Mean Comparator", "SD Comparator", "StdDiff"),
                             escape = FALSE,
                             filter = c("bottom"),
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
  
  cohortCompare <- shiny::reactive({
    balance <- computeBalance()
    
    balance <- balance %>%
      replace(is.na(.), 0) %>% 
      dplyr::filter(.data$cohortId1 != 0) %>% 
      dplyr::filter(.data$cohortId2 != 0)
    
    balance <- balance %>%
      dplyr::inner_join(
        balance %>%
          dplyr::select(.data$cohortId1,
                        .data$cohortId2) %>%
          dplyr::distinct() %>%
          dplyr::arrange(.data$cohortId1,
                         .data$cohortId2) %>%
          dplyr::mutate(comparisonGroup = dplyr::row_number())) %>%
      dplyr::relocate(.data$comparisonGroup)
    
    combis <- dplyr::bind_rows(balance %>% 
                                 dplyr::inner_join(cohort %>% 
                                                     dplyr::select(.data$cohortId, .data$shortName),
                                                   by = c('cohortId1' = 'cohortId')) %>% 
                                 dplyr::select(.data$cohortId1) %>% 
                                 dplyr::distinct() %>% 
                                 dplyr::arrange(.data$cohortId1) %>% 
                                 dplyr::rename(cohortId = .data$cohortId1),
                               balance %>% 
                                 dplyr::inner_join(cohort %>% 
                                                     dplyr::select(.data$cohortId, .data$shortName),
                                                   by = c('cohortId2' = 'cohortId')) %>% 
                                 dplyr::select(.data$cohortId2) %>% 
                                 dplyr::distinct() %>% 
                                 dplyr::arrange(.data$cohortId2) %>% 
                                 dplyr::rename(cohortId = .data$cohortId2)) %>% 
      dplyr::inner_join(y = cohort %>% 
                          dplyr::select(.data$cohortId, .data$cohortName,.data$shortName))
    
    balance <- balance %>% 
      dplyr::inner_join(y = combis %>% 
                          dplyr::filter(stringr::str_detect(string = .data$shortName,
                                                            pattern = 'C')) %>% 
                          dplyr::rename(targetCohortId = .data$cohortId,
                                        targetCohortName = .data$cohortName,
                                        targetCohortShortName = .data$shortName),
                        by = c("cohortId1" = "targetCohortId")) %>% 
      dplyr::inner_join(y = combis %>% 
                          dplyr::filter(stringr::str_detect(string = .data$shortName,
                                                            pattern = 'C')) %>% 
                          dplyr::rename(comparatorCohortId = .data$cohortId,
                                        comparatorCohortName = .data$cohortName,
                                        comparatorCohortShortName = .data$shortName),
                        by = c("cohortId2" = "comparatorCohortId")) %>% 
      dplyr::inner_join(y = cohort %>% 
                          dplyr::select(.data$cohortName,.data$cohortId), by = c("cohortId1" = "cohortId"))
    
    return(balance)
  })
  
  output$charComparePlot <- ggiraph::renderggiraph(expr = {
    validate(need((length(cohortIds()) != 1), paste0("Please select atleast two different cohorts.")))
    extractedData(cohortCompare())
    if (nrow(extractedData()) == 0) {
      return(dplyr::tibble(Note = "No data for the selected combination."))
    }
    plot <- plotCohortComparisonStandardizedDifference(balance = extractedData(),
                                                       domain = input$domainId,
                                                       targetLabel = paste0("Mean in Target (", input$cohort, ")"),
                                                       comparatorLabel = paste0("Mean in Comparator (", input$comparator, ")"))
    return(plot)
  })
  
  # output$databaseInformationPanel <- renderUI({
  #   row <- database[database$databaseId == input$database, ]
  #   text <- div(tags$p(tags$h3("ID"), wellPanel(row$databaseId)),
  #               tags$p(tags$h3("Name"), wellPanel(row$databaseName)),
  #               tags$p(tags$h3("Description"), wellPanel(row$description)))
  #   return(text)
  # })
  
  output$databaseInformationTable <- DT::renderDataTable(expr = {
    
    table <- database[, c("databaseId", "databaseName", "description")]
    options = list(pageLength = 20,
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
    showInfoBox("Included (Source) Concepts", "html/includedConcepts.html")
  })
  
  shiny::observeEvent(input$orphanConceptsInfo, {
    showInfoBox("Orphan (Source) Concepts", "html/orphanConcepts.html")
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
    cohorts <- extractedData() %>%
      dplyr::distinct(.data$cohortName) %>% 
      dplyr::arrange(.data$cohortName)
    
    html <- htmltools::withTags(
      div(table(
        tr(
          td(
            HTML(paste(paste(cohorts$cohortName), collapse = "</br>"))
          )
        )
      )
      ))
    return(html)
  })
  
  output$characterizationSelectedCohort <- shiny::renderUI({
    return(selectedCohorts())
  })
  
  output$temporalCharacterizationSelectedCohort <- shiny::renderUI({
    return(selectedCohorts())
  })
  
  output$inclusionRuleStatSelectedCohort <- shiny::renderUI({
    return(targetCohortCountHtml())
  })
  
  # output$compareCohortCharacterizationSelectedCohort <- shiny::renderUI({
  #   html <- selectedCohorts()
  #   return(html)
  # })
  
  output$cohortOverlapSelectedCohort <- shiny::renderUI({
    html <- selectedCohorts()
    return(html)
  })
  
  output$incidenceRateSelectedCohort <- shiny::renderUI({
    html <- selectedCohorts()
    return(html)
  })
  
  output$timeDistSelectedCohort <- shiny::renderUI({
    html <- selectedCohorts()
    return(html)
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
