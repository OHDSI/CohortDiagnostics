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
  
  comparatorCohortIds <- shiny::reactive({
    return(cohort$cohortId[cohort$cohortName %in% input$comparators])
  })
  
  timeId <- shiny::reactive({
    return(temporalCovariateChoices %>%
             dplyr::filter(choices %in% input$timeIdChoices) %>%
             dplyr::pull(timeId))
  })
  
  shiny::observe({
    subset <- unique(conceptSets$conceptSetName[conceptSets$cohortId == cohortId()]) %>% sort()
    shinyWidgets::updatePickerInput(session = session,
                                    inputId = "conceptSet",
                                    choicesOpt = list(style = rep_len("color: black;", 999)),
                                    choices = subset)
  })
  
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
                               class = "stripe compact")
    return(dataTable)
  }, server = TRUE)
  
  output$cohortDescriptionTable <- DT::renderDataTable(expr = {
    data <- cohort %>% 
      dplyr::left_join(y = phenotypeDescription) %>% 
      dplyr::mutate(cohortName = paste0("<a href='", 
                                        paste0(cohortBaseUrl, 
                                               .data$webApiCohortId),
                                        "' target='_blank'>", 
                                        paste0(.data$cohortName), "</a>")) %>% 
      dplyr::select(.data$phenotypeId, .data$cohortId, 
                    .data$cohortName, .data$logicDescription) %>% 
      dplyr::arrange(.data$phenotypeId, .data$cohortId, 
                     .data$cohortName)
    
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
                               class = "stripe compact")
    return(dataTable)
  }, server = TRUE)
  
  output$cohortCountsTable <- DT::renderDataTable(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    
    data <- getCohortCountResult(dataSource = dataSource,
                                 databaseIds = input$databases) %>%
      dplyr::left_join(cohort, by = "cohortId") %>% 
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
  
  output$incidenceRatePlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
    
    data <- getIncidenceRateResult(dataSource = dataSource,
                                   cohortIds = cohortId(), 
                                   databaseIds = input$databases, 
                                   stratifyByGender =  stratifyByGender,
                                   stratifyByAgeGroup =  stratifyByAge,
                                   stratifyByCalendarYear =  stratifyByCalendarYear,
                                   minPersonYears = 1000) %>% 
      dplyr::mutate(incidenceRate = dplyr::case_when(.data$incidenceRate < 0 ~ 0, 
                                                     TRUE ~ .data$incidenceRate))
    
    validate(need(!is.null(data), paste0("No data for this combination")),
             need(nrow(data) > 0, paste0("No data for this combination")))
    
    plot <- plotIncidenceRate(data = data,
                              cohortIds = NULL,
                              databaseIds = NULL,
                              stratifyByAgeGroup = stratifyByAge,
                              stratifyByGender = stratifyByGender,
                              stratifyByCalendarYear  = stratifyByCalendarYear,
                              yscaleFixed =   input$irYscaleFixed)
    return(plot)
  })
  
  output$timeDisPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    data <- getTimeDistributionResult(dataSource = dataSource,
                                      cohortIds = cohortId(), 
                                      databaseIds = input$databases)
    validate(need(!is.null(data), paste0('No data for this combination')),
             need(nrow(data) > 0, paste0('No data for this combination')))
    
    plot <- plotTimeDistribution(data = data,
                                 cohortIds = cohortId(),
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
  
  output$characterizationTable <- DT::renderDataTable({
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    if (input$charType == "Pretty") {
      analysisIds <- prettyAnalysisIds
    } else {
      analysisIds <- NULL
    }
    data <- getCovariateValueResult(dataSource = dataSource,
                                    analysisIds = analysisIds,
                                    cohortIds = cohortId(),
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
                                        cohortIds = cohortId()) %>%
        dplyr::arrange(.data$databaseId)
      
      table <- list()
      characteristics <- list()
      for (i in 1:length(databaseIds)) {
        temp <- data %>% 
          dplyr::filter(.data$databaseId == databaseIds[i]) %>% 
          prepareTable1()
        if (nrow(temp) > 0) {
          table[[i]] <- temp %>% 
            dplyr::mutate(databaseId = databaseIds[i])
          characteristics[[i]] <- table[[i]] %>% 
            dplyr::select(.data$characteristic, .data$position, 
                          .data$header, .data$sortOrder)
        } else {
          return(dplyr::tibble(Note = paste0(databaseIds[i], " does not have covariates that are part of pretty table. Please unselect.")))
        }
      }
      characteristics <- dplyr::bind_rows(characteristics) %>% 
        dplyr::distinct() %>% 
        dplyr::group_by(.data$characteristic, .data$position, .data$header) %>% 
        dplyr::summarise(sortOrder = max(.data$sortOrder)) %>% 
        dplyr::ungroup() %>% 
        dplyr::arrange(.data$position, desc(.data$header)) %>% 
        dplyr::mutate(sortOrder = dplyr::row_number()) %>%
        dplyr::distinct() %>% 
        tidyr::crossing(dplyr::tibble(databaseId = input$databases)) %>% 
        dplyr::arrange(.data$databaseId, .data$sortOrder)
      
      table <- characteristics %>% 
        dplyr::left_join(dplyr::bind_rows(table) %>% 
                           dplyr::select(-.data$sortOrder),
                         by = c("characteristic", "position", "header", "databaseId"))  %>% 
        dplyr::arrange(.data$sortOrder) %>% 
        tidyr::pivot_wider(id_cols = "characteristic", 
                           names_from = "databaseId",
                           values_from = "value" ,
                           names_sep = "_",
                           names_prefix = "Value_")
      
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
            th(rowspan = 3, "Covariate Name"),
            lapply(databaseIds, th, colspan = 1, class = "dt-center")
          ),
          tr(
            lapply(paste0("(n = ", 
                          format(countData$cohortSubjects, big.mark = ","), ")"), 
                   th, 
                   colspan = 1, 
                   class = "dt-center no-padding")
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
                               columns = 1 + (1:length(databaseIds)),
                               background = DT::styleColorBar(c(0,1), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    } else {
      data <- data %>% 
        dplyr::arrange(.data$databaseId) %>% 
        tidyr::pivot_longer(cols = c(.data$mean, .data$sd)) %>% 
        dplyr::mutate(name = paste0(databaseId, "_", .data$name)) %>% 
        tidyr::pivot_wider(id_cols = c(.data$covariateId),
                           names_from = .data$name,
                           values_from = .data$value) %>%
        dplyr::inner_join(data %>% dplyr::select(.data$covariateId, 
                                                 .data$covariateName, 
                                                 .data$conceptId) %>% 
                            dplyr::distinct(),
                          by = "covariateId") %>%
        dplyr::select(-covariateId) %>% 
        dplyr::relocate("covariateName", "conceptId") %>% 
        dplyr::distinct()
      
      data <- data[order(-data[3]), ]
      
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
                       minCellRealDef(2:(1 + length(databaseIds)*2), digits = 3)))
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
                             filter = c("bottom"),
                             class = "stripe nowrap compact")
      table <- DT::formatStyle(table = table,
                               columns = (1 + 2*(1:length(databaseIds))),
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
  
  output$temporalCharacterizationTable <- DT::renderDataTable(expr = {
    validate(need(length(input$databases) > 0, "No data sources chosen"))
    validate(need(length(timeId()) > 0, "No time periods selected"))
    
    data <- getCovariateValueResult(dataSource = dataSource,
                                    cohortIds = cohortId(),
                                    databaseIds = input$database,
                                    timeIds = timeId(),
                                    isTemporal = TRUE) 
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0("No data available for selected databases and cohorts")))
    }
    
    table <- data %>% 
      dplyr::inner_join(temporalCovariateChoices, by = "timeId") %>% 
      dplyr::arrange(.data$timeId)  %>% 
      tidyr::pivot_wider(id_cols = c("covariateId", "covariateName", "conceptId"), 
                         names_from = "choices",
                         values_from = "mean" ,
                         names_sep = "_"
      ) %>% 
      dplyr::select(-.data$conceptId) %>% 
      dplyr::relocate(.data$covariateName, .data$covariateId) 
    
    table <- table[order(-table[3]), ]
    
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
                     truncateStringDef(0, 150),
                     minCellPercentDef(1:(length(temporalCovariateChoicesSelected$choices)) + 1)))
    
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
                             columns = (2 + (1:length(temporalCovariateChoicesSelected$choices))), #0 index
                             background = DT::styleColorBar(c(0,1), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  cohortOverlap <- reactive({
    combisOfTargetComparator <- tidyr::crossing(targetCohortId = cohortIds(),
                                                comparatorCohortId = comparatorCohortIds()) %>% 
      dplyr::filter(!.data$targetCohortId == .data$comparatorCohortId) %>% 
      dplyr::distinct()
    validate(need(nrow(combisOfTargetComparator) > 0, paste0("Target cohort and comparator cannot be the same")))
    
    data <- getCohortOverlapResult(dataSource = dataSource, 
                                   targetCohortIds = combisOfTargetComparator$targetCohortId, 
                                   comparatorCohortIds = combisOfTargetComparator$comparatorCohortId, 
                                   databaseIds = input$databases)
  })
  
  output$overlapPlot <- ggiraph::renderggiraph(expr = {
    validate(need(length(cohortIds()) > 0, paste0("Please select Target Cohort(s)")))
    validate(need(length(comparatorCohortIds()) > 0, paste0("Please select Comparator Cohort(s)")))
    data <- cohortOverlap()
    validate(need(!is.null(data), paste0("No cohort overlap data for this combination")))
    
    plot <- plotCohortOverlap(data = data,
                              yAxis = input$overlapPlotType)
    return(plot)
  })
  
  computeBalance <- shiny::reactive({
    if (cohortId() == comparatorCohortId()) {
      return(dplyr::tibble())
    }
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
    if (cohortId() == comparatorCohortId()) {
      return(dplyr::tibble(Note = "Cohort and Target are the same. Nothing to compare"))
    } 
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
  
  output$charComparePlot <- ggiraph::renderggiraph(expr = {
    if (cohortId() == comparatorCohortId()) {
      return(dplyr::tibble(Note = "Cohort and Target are the same. Nothing to compare"))
    } 
    balance <- computeBalance()
    if (nrow(balance) == 0) {
      return(dplyr::tibble(Note = "No data for the selected combination."))
    }
    
    
    plot <- plotCohortComparisonStandardizedDifference(balance = balance,
                                                       domain = input$domainId,
                                                       targetLabel = paste0("Mean in Target (", input$cohort, ")"),
                                                       comparatorLabel = paste0("Mean in Comparator (", input$comparator, ")"))
    return(plot)
  })
  
  output$hoverInfocohortComparePlot <- shiny::renderUI({
    balance <- computeBalance() %>% 
      tidyr::replace_na(mean1 = 0, mean2 = 0)
    if (nrow(balance) == 0) {
      return(NULL)
    } else {
      hover <- input$plotHoverCharCompare
      point <- shiny::nearPoints(df = balance, 
                                 coordinfo = hover, 
                                 threshold = 5, 
                                 maxpoints = 1, 
                                 addDist = TRUE)
      if (nrow(point) == 0) {
        return(NULL)
      }
      text <- paste(point$covariateName, 
                    "",
                    sprintf("<b>Mean Target: </b> %0.2f", point$mean1),
                    sprintf("<b>Mean Comparator: </b> %0.2f", point$mean2), 
                    sprintf("<b>Std diff.: </b> %0.2f", point$stdDiff), 
                    sep = "<br/>")
      left_px <- hover$coords_css$x
      top_px <- hover$coords_css$y
      if (hover$x > 0.5) {
        xOffset <- -505
      } else {
        xOffset <- 5
      }
      style <- paste0("position:absolute; z-index:100; background-color: rgba(245, 245, 245, 0.85); ",
                      "left:",
                      left_px + xOffset,
                      "px; top:",
                      top_px - 150,
                      "px; width:500px;")
      div(
        style = "position: relative; width: 0; height: 0",
        wellPanel(
          style = style,
          p(HTML(text))
        )
      )
    }
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
  
  output$temporalCharacterizationSelectedDataBase <- shiny::renderText(input$database)
  
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
  
  selectedCohortCounts <- shiny::reactive({
    targetCohortWithCount <- targetCohortCount()
    
    comparatorCohortWithCount <- getCohortCountResult(dataSource = dataSource,
                                                      cohortIds = comparatorCohortId(),
                                                      databaseIds = input$database) %>% 
      dplyr::left_join(y = cohort, by = "cohortId")
    
    return(htmltools::withTags(
      div(table(
        tr(
          td(
            h5("Target: ", targetCohortWithCount$cohortName, " ( n = ", scales::comma(targetCohortWithCount$cohortSubjects), " )")
          ),
          td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;")),
          td(
            h5("Comparator : ", comparatorCohortWithCount$cohortName, " ( n = ",scales::comma(comparatorCohortWithCount$cohortSubjects), " )")
          )
        )
      ) 
      )))
  })
  
  output$temporalCharacterizationSelectedCohort <- shiny::renderUI({
    return(targetCohortCountHtml())
  })
  
  output$inclusionRuleStatSelectedCohort <- shiny::renderUI({
    return(targetCohortCountHtml())
  })
  
  output$compareCohortCharacterizationSelectedCohort <- shiny::renderUI({
    return(selectedCohortCounts())
  })
  
  output$cohortOverlapSelectedCohort <- shiny::renderUI({
    data <- cohortOverlap()
    targetCohorts <- data %>%
      dplyr::distinct(.data$targetShortName, .data$targetCohortName) %>%
      dplyr::arrange(.data$targetShortName)
    comparatorCohorts <- data %>%
      dplyr::distinct(.data$comparatorShortName, .data$comparatorCohortName) %>%
      dplyr::arrange(.data$comparatorShortName)
    
    html <- htmltools::withTags(
      div(table(
        tr(
          td(
            HTML(paste(paste(targetCohorts$targetShortName, targetCohorts$targetCohortName, sep = ": "), collapse = "</br>"))
          ),
          td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;")),
          td(
            HTML(paste(paste(comparatorCohorts$comparatorShortName, comparatorCohorts$comparatorCohortName, paste = ": "), collapse = "</br>"))
          )
        )
      )
      ))
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
