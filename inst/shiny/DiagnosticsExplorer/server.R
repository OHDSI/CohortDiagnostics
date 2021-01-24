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
  
  cohortIds <- shiny::reactive({
    return(cohort$cohortId[cohort$compoundName  %in% input$cohorts])
  })
  
  timeId <- shiny::reactive({
    return(temporalCovariateChoices %>%
             dplyr::filter(choices %in% input$timeIdChoices) %>%
             dplyr::pull(timeId))
  })
  
  phenotypeId <- shiny::reactive({
    return(phenotypeDescription$phenotypeId[phenotypeDescription$phenotypeName == input$phenotypes])
  })
  
  if (exists("phenotypeDescription")) {
    shiny::observe({
      idx <- which(phenotypeDescription$phenotypeName == input$phenotypes)
      isolate({
        proxy <- DT::dataTableProxy(outputId = "phenoTypeDescriptionTable",
                                    session = session,
                                    deferUntilFlush = FALSE)
        DT::selectRows(proxy, idx)
        DT::selectPage(proxy, which(input$phenoTypeDescriptionTable_rows_all == idx) %/%
                         input$phenoTypeDescriptionTable_state$length + 1)
      })
    })
  }
  
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
  
  # Phenotype Description ------------------------------------------------------------------------------
  output$phenoTypeDescriptionTable <- DT::renderDataTable(expr = {
    data <- phenotypeDescription %>%
      dplyr::mutate(name = sprintf("%s <div style=\"display: none\">%s</div>", .data$phenotypeName, .data$searchTermString)) %>%
      dplyr::select(.data$phenotypeId, .data$name, .data$overview, .data$cohortDefinitions)
    
    options = list(pageLength = 5,
                   lengthMenu = c(5, 10, 15, 20, 100, 500, 1000),
                   searching = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE,
                   searchHighlight = TRUE,
                   stateSave = TRUE)
    
    dataTable <- DT::datatable(data,
                               options = options,
                               rownames = FALSE,
                               colnames = colnames(data) %>% 
                                 camelCaseToTitleCase(),
                               escape = FALSE,
                               filter = "top",
                               selection = list(mode = "single", target = "row"),
                               class = "stripe compact")
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
  outputOptions(output, "phenotypeRowIsSelected", suspendWhenHidden = FALSE)
  
  output$phenotypeDescriptionText <- shiny::renderUI({
    row <- selectedPhenotypeDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      text <-  row$clinicalDescription
      
      referentConcept <- getConceptDetails(dataSource, row$phenotypeId/1000)
      if (nrow(referentConcept) > 0) {
        text <- paste(sprintf("<strong>Referent concept: </strong>%s (concept ID: %s)<br/><br/>",
                              referentConcept$conceptName,
                              referentConcept$conceptId),
                      text)
      }
      shiny::HTML(text)
    }
  })
  
  output$phenotypeLiteratureReviewText <- shiny::renderUI({
    row <- selectedPhenotypeDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      files <- listFilesInGitHub(phenotypeId = row$phenotypeId, subFolder = "literature")
      if (nrow(files) == 0) {
        return("Nothing here (yet)")
      } else {
        return(HTML(paste(files$html, sep = "<br/>")))
      }
    }
  })
  
  output$phenotypeEvaluationText <- shiny::renderUI({
    row <- selectedPhenotypeDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      files <- listFilesInGitHub(phenotypeId = row$phenotypeId, subFolder = "evaluation")
      if (nrow(files) == 0) {
        return("Nothing here (yet)")
      } else {
        return(HTML(paste(files$html, sep = "<br/>")))
      }
    }
  })
  
  output$phenotypeNotesText <- shiny::renderUI({
    row <- selectedPhenotypeDescriptionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      files <- listFilesInGitHub(phenotypeId = row$phenotypeId, subFolder = "notes")
      if (nrow(files) == 0) {
        return("Nothing here (yet)")
      } else {
        return(HTML(paste(files$html, sep = "<br/>")))
      }
    }
  })
  
  observeEvent(input$selectPhenotypeButton, {
    shinyWidgets::updatePickerInput(session = session, 
                                    inputId = "phenotypes", 
                                    selected = selectedPhenotypeDescriptionRow()$phenotypeName)
  })
  
  # Cohort Definition ---------------------------------------------------------
  output$cohortDefinitionTable <- DT::renderDataTable(expr = {
    data <- cohortSubset() %>%
      dplyr::select(cohort = .data$shortName, .data$cohortId, .data$cohortName) %>%
      dplyr::mutate(cohort = as.factor(.data$cohort))
    
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
                               filter = "top",
                               selection = list(mode = "multiple", target = "row"),
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
  
  
  # output$cohortDefinitionRowIsSelected <- reactive({
  #   return(!is.null(selectedCohortDefinitionRow()))
  # })
  
  observeEvent(length(input$cohortDefinitionTable_rows_selected) != 2, {
    shinyWidgets::updatePickerInput(session = session, 
                                    inputId = "isCompare", 
                                    selected = "No Comparision")
  })
  
  output$cohortDefinitionRowIsSelected <- reactive({
    return(length(input$cohortDefinitionTable_rows_selected))
  })
  
  
  outputOptions(output, "cohortDefinitionRowIsSelected", suspendWhenHidden = FALSE)
  
  output$cohortDetailsText <- shiny::renderUI({
    row <- selectedCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      # for(i in 1:nrow(row))
      # {
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
      # }
    }
  })
  
  output$cohortDefinitionDetails <- shiny::renderUI({
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
    return(data)
    
  })
  
  output$cohortDefinitionConceptSetsTable <- DT::renderDataTable(expr = {
    
    data <- cohortDefinitionConceptSets()
    if (is.null(data)) {
      return(NULL)
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
  
  output$compareCohortDetailsText <- shiny::renderUI({
    row <- compareCohortDefinitionRow()
    if (is.null(row)) {
      return(NULL)
    } else {
      # for(i in 1:nrow(row))
      # {
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
      # }
    }
  })
  
  output$compareCohortDefinitionDetails <- shiny::renderUI({
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
    if (is(dataSource, "environment") || input$compareConceptSetsType == "Concept Set Expression") {
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
    return(data)
    
  })
  
  output$compareCohortDefinitionConceptSetsTable <- DT::renderDataTable(expr = {
    
    data <- compareCohortDefinitionConceptSets()
    if (is.null(data)) {
      return(NULL)
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
                               filter = "top",
                               class = "stripe nowrap compact")
    return(dataTable)
    
  })
  
  output$compareSaveConceptSetButton <- downloadHandler(
    filename = function() {
      paste("conceptSet-", Sys.Date(), ".csv", sep="")
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
    detailsDiffOutput <- diffr::diffr(file1, file2, wordWrap = TRUE,
                                   before = row1$compoundName, after = row2$compoundName)
    unlink(file1)
    unlink(file2)
    return(detailsDiffOutput)
  })
  
  output$concesptSetDiff <- DT::renderDataTable(expr = {
    
    data1 <- cohortDefinitionConceptSets()
    data2 <- compareCohortDefinitionConceptSets()
    data <- data1 %>% dplyr::semi_join(data2, by = c("Concept Id","Is Excluded","Include Descendants","Include Mapped"))
    if (is.null(data)) {
      return(NULL)
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
                               filter = "top",
                               class = "stripe nowrap compact")
    return(dataTable)
    
  })
  
  output$jsonDiff <- diffr::renderDiffr({
    row1 <- selectedCohortDefinitionRow()
    row2 <- compareCohortDefinitionRow()
    file1 <- tempfile()
    writeLines(row1$json, con = file1)
    file2 <- tempfile()
    writeLines(row2$json, con = file2)
    jsonDiffOutput <- diffr::diffr(file1, file2, wordWrap = TRUE,
          before = row1$compoundName, after = row2$compoundName)
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
    sqlDiffOutput <- diffr::diffr(file1, file2, wordWrap = FALSE,
                 before = row1$compoundName, after = row2$compoundName,width = "100%")
    unlink(file1)
    unlink(file2)
    return(sqlDiffOutput)
  })
  
  
  # Cohort Counts --------------------------------------------------------------------------- 
  output$cohortCountsTable <- DT::renderDataTable(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getCohortCountResult(dataSource = dataSource,
                                 databaseIds = input$databases,
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
                          input$databases %>% unique() %>% sort()))) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases, 
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
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
    if (length(cohortIds()) > 0) {
      data <- getIncidenceRateResult(dataSource = dataSource,
                                     cohortIds = cohortIds(), 
                                     databaseIds = input$databases, 
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
  
  output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
    data <- incidenceRateData()
    
    if (stratifyByAge && !"All" %in% input$incidenceRateAgeFilter) {
      data <- data %>% 
        dplyr::filter(.data$ageGroup %in% input$incidenceRateAgeFilter)}
    if (stratifyByGender && !"All" %in% input$incidenceRateGenderFilter) {
      data <- data %>% 
        dplyr::filter(.data$gender %in% input$incidenceRateGenderFilter)}
    if (stratifyByCalendarYear && !"All" %in% input$incidenceRateCalenderFilter) {
      data <- data %>% 
        dplyr::filter(.data$calendarYear %in% input$incidenceRateCalenderFilter)}
    
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
    data <- getTimeDistributionResult(dataSource = dataSource,
                                      cohortIds = cohortIds(), 
                                      databaseIds = input$databases)
    return(data)
  })
  
  output$timeDisPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
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
    
    options = list(pageLength = 9,
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
    options <- list(pageLength = 10,
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
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    table <- getInclusionRuleStats(dataSource = dataSource,
                                   cohortIds = cohortIds(),
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
      dplyr::mutate(name = paste0(databaseId, "_", .data$name)) %>% 
      tidyr::pivot_wider(id_cols = c(.data$cohortId, .data$ruleSequenceId, .data$ruleName),
                         names_from = .data$name,
                         values_from = .data$value) %>%
      addShortName(cohort) %>%
      dplyr::relocate(.data$shortName) %>%
      dplyr::mutate(shortName = as.factor(.data$shortName))
    
    sketch <- htmltools::withTags(table(
      class = "display",
      thead(
        tr(
          th(rowspan = 2, "Cohort"),
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
                   columnDefs = list(minCellCountDef(2 + (1:(length(databaseIds) * 4)))))
    
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
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen chosen"))
    data <- getIndexEventBreakdown(dataSource = dataSource,
                                   cohortIds = cohortIds(),
                                   databaseIds = input$databases) %>% 
      addShortName(cohort)
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    maxCount <- max(data$conceptCount, na.rm = TRUE)
    data <- data %>% 
      dplyr::select(.data$databaseId,
                    .data$shortName, 
                    .data$conceptId, 
                    .data$conceptName,
                    .data$conceptCount) %>% 
      dplyr::arrange(.data$shortName, .data$databaseId) %>% 
      tidyr::pivot_wider(id_cols = c("shortName", "conceptId", "conceptName"),
                         names_from = "databaseId", 
                         values_from = "conceptCount") %>% 
      dplyr::rename(cohort = .data$shortName) %>%
      dplyr::mutate(cohort = as.factor(.data$cohort))
    
    data <- data[order(-data[4]), ]
    
    options = list(pageLength = 10,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(minCellCountDef(3:ncol(data) - 1)))
    dataTable <- DT::datatable(data,
                               options = options,
                               rownames = FALSE,
                               escape = FALSE,
                               filter = "top",
                               class = "stripe nowrap compact")
    dataTable <- DT::formatStyle(table = dataTable,
                                 columns = 4:ncol(data),
                                 background = DT::styleColorBar(c(0, maxCount), "lightblue"),
                                 backgroundSize = "98% 88%",
                                 backgroundRepeat = "no-repeat",
                                 backgroundPosition = "center")
    return(dataTable)
  }, server = TRUE)
  
  # Visit Context ---------------------------------------------------------------------------------------------
  output$visitContextTable <- DT::renderDataTable(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    data <- getVisitContextResults(dataSource = dataSource,
                                   cohortIds = cohortIds(), 
                                   databaseIds = input$databases) %>%  
      addShortName(cohort)
    
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
                                          databaseId = databaseIds) %>% 
      tidyr::tibble()
    
    table <- visitContextReference %>% 
      dplyr::left_join(data, by = c("visitConceptName", "visitContext", "databaseId")) %>% 
      dplyr::select(.data$visitConceptName, .data$visitContext, .data$subjects, .data$databaseId, .data$shortName) %>% 
      dplyr::mutate(visitContext = paste0(.data$databaseId, "_", .data$visitContext)) %>% 
      dplyr::select(-.data$databaseId) %>%
      dplyr::arrange(.data$shortName, .data$visitConceptName) %>% 
      tidyr::pivot_wider(id_cols = c( .data$shortName, .data$visitConceptName),
                         names_from = .data$visitContext,
                         values_from = .data$subjects) %>% 
      dplyr::relocate(.data$shortName, .data$visitConceptName) %>% 
      dplyr::rename(cohort = .data$shortName) %>% 
      dplyr::filter(!is.na(.data$cohort)) %>%
      dplyr::mutate(cohort = as.factor(cohort),
                    visitConceptName = as.factor(visitConceptName))
    
    sketch <- htmltools::withTags(table(
      class = "display",
      thead(
        tr(
          th(rowspan = 2, "Cohorts"),
          th(rowspan = 2, "Visit"),
          lapply(databaseIds, th, colspan = 4, class = "dt-center")
        ),
        tr(
          lapply(rep(c("Visits Before", "Visits Ongoing", "Starting Simultaneous", "Visits After"), length(databaseIds)), th)
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
                                     minCellCountDef(1 + 1:(length(databaseIds) * 4))))
    
    table <- DT::datatable(table,
                           options = options,
                           colnames = colnames(table) %>% 
                             camelCaseToTitleCase(),
                           rownames = FALSE,
                           container = sketch,
                           escape = TRUE,
                           filter = "top")
    
    table <- DT::formatStyle(table = table,
                             columns = 1:(length(databaseIds) * 4) + 1,
                             background = DT::styleColorBar(c(0, maxSubjects), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    
  }, server = TRUE)
  
  # Characterization --------------------------------------------------
  output$characterizationTable <- DT::renderDataTable(expr = {
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
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    
    databaseIds <- sort(unique(data$databaseId))
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
                           names_prefix = "Value_") %>%
        addShortName(cohort)
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
                       minCellPercentDef(1 + 1:length(databaseIds))
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
                             filter = "top",
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns = 2 + (1:length(databaseIds)),
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
        addShortName(cohort) %>%
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
                       truncateStringDef(1, 150),
                       minCellRealDef(2 + 1:(length(databaseIds)*2), digits = 3)))
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
  
  covariateIdArray <- reactiveVal()
  covariateIdArray(c())
  observeEvent(input$rows, {
    if (input$rows[[2]] %in% covariateIdArray())
      covariateIdArray(covariateIdArray()[covariateIdArray() %in% input$rows[[2]] == FALSE])
    else
      covariateIdArray(c(covariateIdArray(),input$rows[[2]]))
  })
  
  # Temporal characterization -----------------------------------------------------------------
  temporalCharacterization <- shiny::reactive({
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(cohortIds()) > 0, "No cohorts chosen"))
    validate(need(length(timeId()) > 0, "No time periods selected"))
    data <- getCovariateValueResult(dataSource = dataSource,
                                    cohortIds = cohortIds(),
                                    databaseIds = input$databases,
                                    timeIds = timeId(),
                                    isTemporal = TRUE)
  })
  
  # Table inside the table radio button
  output$temporalCharacterizationTable <- DT::renderDataTable(expr = {
    data <- temporalCharacterization()
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    
    table <- data %>% 
      dplyr::inner_join(temporalCovariateChoices, by = "timeId") %>% 
      dplyr::arrange(.data$timeId)  %>% 
      tidyr::pivot_wider(id_cols = c("cohortId", "databaseId", "covariateId", "covariateName", "conceptId"), 
                         names_from = "choices",
                         values_from = "mean" ,
                         names_sep = "_") %>% 
      addShortName(cohort) %>%
      dplyr::relocate(.data$databaseId, .data$shortName, .data$covariateName, .data$covariateId) %>%
      dplyr::rename(cohort = .data$shortName) %>% 
      dplyr::select(-.data$conceptId, -.data$cohortId) %>% 
      dplyr::arrange(.data$databaseId, .data$cohort, dplyr::desc(dplyr::across(dplyr::starts_with('Start')))) %>%
      dplyr::mutate(cohort = as.factor(.data$cohort),
                    databaseId = as.factor(.data$databaseId))
    
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
                           filter = "top",
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
  
  # reactive function to filter characterization data based on timeId or domainId
  filterByTimeIdAndDomainId <- reactive({
    data <- temporalCharacterization() 
    #filter data by timeId
    if (input$timeIdChoicesFilter != 'All') {
      data <- data %>% 
        dplyr::filter(.data$timeId %in% (temporalCovariateChoices %>% 
                                           dplyr::filter(choices %in% input$timeIdChoicesFilter) %>% 
                                           dplyr::pull(.data$timeId)))
    }
    #filter data by domain
    # domains <- c("condition", "device", "drug", "measurement", "observation", "procedure")
    data$domain <- tolower(stringr::str_extract(data$covariateName, "[a-z]+"))
    data$domain[!data$domain %in% input$temporalDomainId] <- "other"
    if (input$temporalDomainId != "all") {
      data <- data %>%
        dplyr::filter(.data$domain == !!input$temporalDomainId)
    }
    return(data)
  })
  
  # Temporal characterization table that is shown on selecting the plot radio button
  output$temporalCharacterizationCovariateTable <- DT::renderDataTable(expr = {
    data <- filterByTimeIdAndDomainId()
    data <- compareTemporalCohortCharacteristics(characteristics1 = data, characteristics2 = data)
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = "No data for the selected combination."))
    }
    if (nrow(data) > 1000) {
      data <- data %>%
        dplyr::filter(.data$mean1 > 0.01 | .data$mean2 > 0.01)
    }
    data <- data %>% 
      dplyr::select(.data$covariateName)
    options = list(pageLength = 10,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   stateSave = TRUE,
                   dom = 'tip', 
                   columnDefs = list(truncateStringDef(0,40)))
    table <- DT::datatable(data,
                           options = options,
                           rownames = FALSE,
                           colnames = colnames(table) %>% 
                             camelCaseToTitleCase(),
                           escape = FALSE,
                           filter = "top",
                           class = "stripe nowrap compact")
    return(table)
  })
  
  # Temporal characterization table that shows the covariates selected by lasso method
  output$temporalCharacterizationCovariateLassoTable <- DT::renderDataTable(expr = {
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
    options = list(pageLength = 10,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   stateSave = TRUE,
                   dom = 'tip', 
                   columnDefs = list(truncateStringDef(0,40)))
    
    table <- DT::datatable(table,
                           options = options,
                           rownames = FALSE,
                           colnames = colnames(table) %>% 
                             camelCaseToTitleCase(),
                           escape = FALSE,
                           filter = "top",
                           class = "stripe nowrap compact")
    return(table)
  })
  
  selectedtemporalCharacterizationCovariateRow <- reactive({
    # _row_selected is an inbuilt property of DT that provides the index of selected row.
    idx <- input$temporalCharacterizationCovariateTable_rows_selected
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
  
  output$compareTemporalCharacterizationPlot <- ggiraph::renderggiraph(expr = {
    data <- filterByTimeIdAndDomainId()
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = "No data for the selected combination."))
    }
    data <- compareTemporalCohortCharacteristics(characteristics1 = data, characteristics2 = data)
    if (!is.null(selectedtemporalCharacterizationCovariateRow())) {
      data <- data[selectedtemporalCharacterizationCovariateRow(), ]
    }
    else if (!is.null(filteredTemporalCovariateName())) {
      data <- data %>% 
        dplyr::filter(grepl(filteredTemporalCovariateName(),.data$covariateName))
    }
    
    if (nrow(data) > 1000) {
      data <- data %>%
        dplyr::filter(.data$mean1 > 0.01 | .data$mean2 > 0.01)
    }
    plot <- plotTemporalCohortComparison(balance = data,
                                         shortNameRef = cohort,
                                         domain = input$temporalDomainId)
    return(plot)
  })
  
  output$compareTemporalCharacterizationLassoPlot <- ggiraph::renderggiraph(expr = {
    data <- filterByTimeIdAndDomainId()
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = "No data for the selected combination."))
    }
    data <- compareTemporalCohortCharacteristics(characteristics1 = data, characteristics2 = data)
    if (nrow(data) > 1000) {
      data <- data %>%
        dplyr::filter(.data$mean1 > 0.01 | .data$mean2 > 0.01)
    }
    data <- data[selectedPlotPoints(), ]
    plot <- plotTemporalLassoCohortComparison(balance = data,
                                              shortNameRef = cohort,
                                              domain = input$temporalDomainId)
    return(plot)
  })
  
  
  #Cohort Overlap ------------------------
  cohortOverlap <- reactive({
    combisOfTargetComparator <- tidyr::crossing(targetCohortId = cohortIds(),
                                                comparatorCohortId = cohortIds()) %>% 
      dplyr::filter(!.data$targetCohortId == .data$comparatorCohortId) %>% 
      dplyr::distinct()
    validate(need(nrow(combisOfTargetComparator) > 0, paste0("Please select at least two cohorts.")))
    
    data <- getCohortOverlapResult(dataSource = dataSource, 
                                   targetCohortIds = combisOfTargetComparator$targetCohortId, 
                                   comparatorCohortIds = combisOfTargetComparator$comparatorCohortId, 
                                   databaseIds = input$databases)
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
    validate(need((length(cohortIds()) != 1), paste0("Please select atleast two different cohorts.")))
    validate(need((length(input$databases) >= 1), paste0("Please select atleast one datasource.")))
    covs1 <- getCovariateValueResult(dataSource = dataSource,
                                     cohortIds = cohortIds(),
                                     databaseIds = input$databases,
                                     isTemporal = FALSE)
    balance <- compareCohortCharacteristics(covs1, covs1) %>%
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
          addShortName(cohort, cohortIdColumn = "cohortId1", shortNameColumn = "shortName1") %>%
          addShortName(cohort, cohortIdColumn = "cohortId2", shortNameColumn = "shortName2") %>%
          dplyr::relocate(.data$shortName1, .data$shortName2) %>% 
          dplyr::select(-.data$cohortId1, -.data$cohortId2)
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
                             colnames = c("Target", "Comparator", "Characteristic", "Target", "Comparator","Std. Diff."),
                             escape = FALSE,
                             filter = "top",
                             class = "stripe nowrap compact")
      table <- DT::formatStyle(table = table,
                               columns = 4:5,
                               background = DT::styleColorBar(c(0,1), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
      table <- DT::formatStyle(table = table,
                               columns = 6,
                               background = styleAbsColorBar(1, "lightblue", "pink"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
      table <- DT::formatRound(table, 6, digits = 2)
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
        addShortName(cohort, cohortIdColumn = "cohortId1", shortNameColumn = "shortName1") %>%
        addShortName(cohort, cohortIdColumn = "cohortId2", shortNameColumn = "shortName2") %>%
        dplyr::relocate(.data$shortName1, .data$shortName2) %>% 
        dplyr::select(-.data$cohortId1, -.data$cohortId2) %>% 
        dplyr::arrange(desc(abs(.data$stdDiff)))
      
      options = list(pageLength = 100,
                     searching = TRUE,
                     searchHighlight = TRUE,
                     scrollX = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       minCellRealDef(4:8, digits = 2)))
      
      table <- DT::datatable(table,
                             options = options,
                             rownames = FALSE,
                             colnames = c("Target", 
                                          "Comparator", 
                                          "Covariate Name", 
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
                               columns = c(5, 7),
                               background = DT::styleColorBar(c(0,1), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
      table <- DT::formatStyle(table = table,
                               columns = 9,
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
    showInfoBox("Included (Source) Concepts", "html/includedConcepts.html")
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
  
  output$cohortCountsSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$indexEventBreakdownSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$characterizationSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$temporalCharacterizationSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$inclusionRuleStatSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$cohortOverlapSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$incidenceRateSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$timeDistSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$visitContextSelectedCohort <- shiny::renderUI({selectedCohorts()})
  output$cohortCharCompareSelectedCohort <- shiny::renderUI({selectedCohorts()})
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
