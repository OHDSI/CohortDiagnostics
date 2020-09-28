library(magrittr)

source("R/Tables.R")
source("R/Other.R")
source("R/Plots.R")
source("R/Results.R")

truncateStringDef <- function(columns, maxChars) {
  list(
    targets = columns,
    render = DT::JS(sprintf("function(data, type, row, meta) {\n
      return type === 'display' && data != null && data.length > %s ?\n
        '<span title=\"' + data + '\">' + data.substr(0, %s) + '...</span>' : data;\n
     }", maxChars, maxChars))
  )
}

minCellCountDef <- function(columns) {
  list(
    targets = columns,
    render = DT::JS("function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return data.toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
    return '<' + Math.abs(data).toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
  }")
  )
}

minCellPercentDef <- function(columns) {
  list(
    targets = columns,
    render = DT::JS("function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return (100 * data).toFixed(1).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
    return '<' + Math.abs(100 * data).toFixed(1).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
  }")
  )
}

minCellRealDef <- function(columns, digits = 1) {
  list(
    targets = columns,
    render = DT::JS(sprintf("function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return data.toFixed(%s).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
    return '<' + Math.abs(data).toFixed(%s).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
  }", digits, digits))
  )
}

styleAbsColorBar <- function(maxValue, colorPositive, colorNegative, angle = 90) {
  DT::JS(sprintf("isNaN(parseFloat(value))? '' : 'linear-gradient(%fdeg, transparent ' + (%f - Math.abs(value))/%f * 100 + '%%, ' + (value > 0 ? '%s ' : '%s ') + (%f - Math.abs(value))/%f * 100 + '%%)'", 
                 angle, maxValue, maxValue, colorPositive, colorNegative, maxValue, maxValue))
}

camelCaseToTitleCase <- function(string) {
  string <- gsub("([A-Z])", " \\1", string)
  string <- gsub("([a-z])([0-9])", "\\1 \\2", string)
  substr(string, 1, 1) <- toupper(substr(string, 1, 1))
  return(string)
}

shiny::shinyServer(function(input, output, session) {
  
  cohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$cohortName == input$cohort])
  })
  
  comparatorCohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$cohortName == input$comparator])
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
      dplyr::mutate(dplyr::across(.cols = dplyr::everything(), tidyr::replace_na, '')) %>% 
      dplyr::mutate(literatureReview = dplyr::case_when(!.data$literatureReview %in% c('','0') ~ 
                                                          paste0("<a href='", .data$literatureReview, "' target='_blank'>", "Link", "</a>"),
                                                        TRUE ~ 'Ongoing')) %>%
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
      dplyr::mutate(webApiCohortId = as.integer(.data$webApiCohortId)) %>% #this is temporary - we need to standardize this 
      dplyr::left_join(y = phenotypeDescription) %>% 
      dplyr::mutate(cohortName = paste0("<a href='", paste0(cohortBaseUrl, .data$webApiCohortId),"' target='_blank'>", paste0(.data$cohortName), "</a>")) %>% 
      dplyr::select(.data$phenotypeId, .data$cohortId, .data$cohortName, .data$logicDescription) %>% 
      dplyr::arrange(.data$phenotypeId, .data$cohortId, .data$cohortName)
    
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
    data <- cohortCount %>%
      dplyr::filter(.data$databaseId %in% input$databases) %>% 
      dplyr::left_join(cohort, by = "cohortId") %>% 
      dplyr::select(.data$cohortId, 
                    .data$cohortName, .data$databaseId, 
                    .data$webApiCohortId,
                    .data$cohortSubjects, .data$cohortEntries)
    
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
      dplyr::mutate(url = paste0(cohortBaseUrl, .data$webApiCohortId),
                    cohortName = paste0("<a href='", 
                                        .data$url, 
                                        "' target='_blank'>", 
                                        .data$cohortName, 
                                        "</a>")
      ) %>% 
      dplyr::select(-.data$cohortId, -.data$url, -.data$webApiCohortId) %>%
      dplyr::arrange(.data$cohortName)
    
    databaseIds <- cohortCount %>%
      dplyr::filter(.data$databaseId %in% input$databases) %>% 
      dplyr::select(.data$databaseId) %>% 
      dplyr::distinct() %>% 
      dplyr::arrange() %>% 
      dplyr::pull(.data$databaseId)
    
    sketch <- htmltools::withTags(table(
      class = 'display',
      thead(
        tr(
          th(rowspan = 2, 'Cohort'),
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
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
    
    data <- getIncidenceRateResult(connection = NULL,
                                   connectionDetails = NULL,
                                   cohortIds = cohortId(), 
                                   databaseIds = input$databases, 
                                   stratifyByGender =  stratifyByGender,
                                   stratifyByAgeGroup =  stratifyByAge,
                                   stratifyByCalendarYear =  stratifyByCalendarYear,
                                   minPersonYears = 1000,
                                   resultsDatabaseSchema = NULL) %>% 
      dplyr::mutate(incidenceRate = dplyr::case_when(.data$incidenceRate < 0 ~ 0, 
                                                     TRUE ~ .data$incidenceRate))
    
    validate(
      need(!is.null(data), paste0('No incident rate data for this combination')))
    
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
    data <- getTimeDistributionResult(cohortIds = cohortId(), databaseIds = input$databases)
    validate(
      need(!is.null(data), paste0('No time distribution data for this combination')))
    
    plot <- plotTimeDistribution(data = data,
                                 cohortIds = cohortId(),
                                 databaseIds = input$databases)
    return(plot)
  })
  
  output$timeDistTable <- DT::renderDataTable(expr = {
    
    table <- getTimeDistributionResult(cohortIds = cohortId(), 
                                       databaseIds = input$databases)
    
    if (is.null(table)) {
      return(dplyr::tibble(Note = paste0('No data available for selected databases and cohorts')))
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
                           filter = c('bottom'),
                           class = "stripe nowrap compact")
    table <- DT::formatRound(table, c("Average", "SD"), digits = 2)
    table <- DT::formatRound(table, c("Min", "P10", "P25", "Median", "P75", "P90", "Max"), digits = 0)
    return(table)
  }, server = TRUE)
  
  output$includedConceptsTable <- DT::renderDataTable(expr = {
    data <- includedSourceConcept %>% 
      dplyr::inner_join(conceptSets, by = c("cohortId", "conceptSetId")) %>% 
      dplyr::filter(.data$cohortId == cohortId() &
                      .data$conceptSetName == input$conceptSet &
                      .data$databaseId %in% input$databases) %>% 
      dplyr::select(-.data$cohortId)
    
    if (nrow(data) == 0) {
      return(dplyr::tibble('No data available for selected databases and cohorts'))
    }
    
    databaseIds <- includedSourceConcept %>%
      dplyr::filter(.data$databaseId %in% input$databases) %>% 
      dplyr::select(.data$databaseId) %>% 
      dplyr::distinct() %>% 
      dplyr::arrange() %>% 
      dplyr::pull(.data$databaseId)
    
    if (!isTRUE(all.equal(databaseIds %>% sort(),
                          input$databases %>% unique() %>% sort()))) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases %>% sort(), 
                                                        databaseIds %>% sort()), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    maxConceptSubjects <- max(data$conceptSubjects, na.rm = TRUE)
    
    if (input$includedType == "Source Concepts") {
      table <- data %>%
        dplyr::group_by(.data$databaseId, .data$conceptSetId, 
                        .data$sourceConceptId) %>%
        dplyr::summarise(conceptSubjects = sum(abs(.data$conceptSubjects )) * 
                           min(.data$conceptSubjects)/abs(min(.data$conceptSubjects)),
                         conceptCount = sum(abs(.data$conceptCount)) *
                           min(.data$conceptCount)/abs(min(.data$conceptCount))) %>%
        dplyr::ungroup() %>% 
        dplyr::rename(conceptId = .data$sourceConceptId) %>% 
        dplyr::arrange(.data$databaseId, .data$conceptId) %>% 
        tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>% 
        dplyr::mutate(name = paste0(databaseId, "_",
                                    stringr::str_replace(string = .data$name, 
                                                         pattern = 'concept', 
                                                         replacement = ''))) %>% 
        tidyr::pivot_wider(id_cols = c(.data$conceptId),
                           names_from = .data$name,
                           values_from = .data$value) %>% 
        dplyr::inner_join(concept %>% 
                            dplyr::select(.data$conceptId, 
                                          .data$conceptName, 
                                          .data$vocabularyId),
                          by = "conceptId") %>% 
        dplyr::select(order(colnames(.))) %>% 
        dplyr::relocate(.data$conceptId, .data$conceptName, .data$vocabularyId)
      
      if (nrow(table) == 0) {
        return(dplyr::tibble(Note = paste0('No data available for selected databases and cohorts')))
      }
      
      table <- table[order(-table[, 4]), ]
      
      sketch <- htmltools::withTags(table(
        class = 'display',
        thead(
          tr(
            th(rowspan = 2, 'Concept Id'),
            th(rowspan = 2, 'Concept Name'),
            th(rowspan = 2, 'Vocabulary Id'),
            lapply(databaseIds, th, colspan = 2, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Counts", "Subjects"), length(databaseIds)), th)
          )
        )
      ))
      
      options = list(pageLength = 10,
                     searching = TRUE,
                     scrollX = TRUE,
                     lengthChange = TRUE,
                     searchHighlight = TRUE,
                     ordering = TRUE,
                     paging = TRUE)
      #                ,
      #                columnDefs = list(truncateStringDef(1, 100),
      #                                  minCellCountDef(2 + (1:(length(databaseIds) * 2)))))
      
      table <- DT::datatable(table,
                             colnames = colnames(table),
                             options = options,
                             rownames = FALSE, 
                             # container = sketch,
                             escape = FALSE,
                             filter = c('bottom'),
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns =  3 + (1:(length(databaseIds)*2)),
                               background = DT::styleColorBar(c(0,maxConceptSubjects), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    } else {
      table <- data %>%
        dplyr::group_by(.data$databaseId, .data$conceptSetId, 
                        .data$conceptId) %>%
        dplyr::summarise(conceptSubjects = sum(abs(.data$conceptSubjects )) * 
                           min(.data$conceptSubjects)/abs(min(.data$conceptSubjects)),
                         conceptCount = sum(abs(.data$conceptCount)) *
                           min(.data$conceptCount)/abs(min(.data$conceptCount))) %>%
        dplyr::ungroup() %>% 
        dplyr::arrange(.data$databaseId) %>% 
        tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>% 
        dplyr::mutate(name = paste0(databaseId, "_",
                                    stringr::str_replace(string = .data$name, 
                                                         pattern = 'concept', 
                                                         replacement = ''))) %>% 
        tidyr::pivot_wider(id_cols = c(.data$conceptId),
                           names_from = .data$name,
                           values_from = .data$value) %>% 
        dplyr::inner_join(concept %>% 
                            dplyr::select(.data$conceptId, 
                                          .data$conceptName, 
                                          .data$vocabularyId),
                          by = "conceptId") %>% 
        dplyr::select(order(colnames(.))) %>% 
        dplyr::relocate(.data$conceptId, 
                        .data$conceptName, 
                        .data$vocabularyId)
      
      sketch <- htmltools::withTags(table(
        class = 'display',
        thead(
          tr(
            th(rowspan = 2, 'Concept Id'),
            th(rowspan = 2, 'Concept Name'),
            th(rowspan = 2, 'Vocabulary Id'),
            lapply(databaseIds, th, colspan = 2, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Counts", "Subjects"), length(databaseIds)), th)
          )
        )
      ))
      
      options = list(pageLength = 10,
                     searching = TRUE,
                     scrollX = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE)
      # ,
      # columnDefs = list(truncateStringDef(1, 100),
      #                   minCellCountDef(2 + (1:(databaseIds * 2)))))
      
      table <- DT::datatable(table,
                             options = options,
                             colnames = colnames(table),
                             rownames = FALSE,
                             # container = sketch,
                             escape = FALSE,
                             filter = c('bottom'),
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns =  3 + (1:(length(databaseIds)*2)),
                               background = DT::styleColorBar(c(0, maxConceptSubjects), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    }
    return(table)
  }, server = TRUE)
  
  output$orphanConceptsTable <- DT::renderDataTable(expr = {
    data <- orphanConcept %>% 
      dplyr::inner_join(conceptSets) %>% 
      dplyr::filter(.data$cohortId == cohortId() &
                      .data$conceptSetName == input$conceptSet &
                      .data$databaseId %in% input$databases) %>% 
      dplyr::select(-.data$cohortId)
    
    databaseIds <- orphanConcept %>%
      dplyr::filter(.data$databaseId %in% input$databases) %>% 
      dplyr::select(.data$databaseId) %>% 
      dplyr::distinct() %>% 
      dplyr::arrange() %>% 
      dplyr::pull(.data$databaseId)
    
    if (!isTRUE(all.equal(databaseIds %>% sort(),
                          input$databases %>% unique() %>% sort()))) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases %>% sort(), 
                                                        databaseIds %>% sort()), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    maxConceptCount <- max(data$conceptCount, na.rm = TRUE)
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0('No data available for selected databases and cohorts')))
    }
    
    table <- data %>% 
      tidyr::pivot_longer(cols = c(.data$conceptSubjects, .data$conceptCount)) %>% 
      dplyr::group_by(.data$conceptId, .data$databaseId, 
                      .data$name, .data$conceptSetId) %>% 
      dplyr::summarise(value = sum(.data$value)) %>% 
      dplyr::mutate(name = paste0(databaseId, "_",
                                  stringr::str_replace(string = .data$name, 
                                                       pattern = 'concept', 
                                                       replacement = ''))) %>% 
      tidyr::pivot_wider(id_cols = c(.data$conceptId),
                         names_from = .data$name,
                         values_from = .data$value) %>% 
      dplyr::inner_join(concept %>% 
                          dplyr::select(.data$conceptId, 
                                        .data$conceptName, 
                                        .data$vocabularyId),
                        by = "conceptId") %>% 
      dplyr::select(order(colnames(.))) %>% 
      dplyr::relocate(.data$conceptId, .data$conceptName, .data$vocabularyId)
    
    table <- table[order(-table[, 4]),]
    
    sketch <- htmltools::withTags(table(
      class = 'display',
      thead(
        tr(
          th(rowspan = 2, 'Concept Id'),
          th(rowspan = 2, 'Concept Name'),
          th(rowspan = 2, 'Vocabulary Id'),
          lapply(databaseIds, th, colspan = 2, class = "dt-center")
        ),
        tr(
          lapply(rep(c("Counts", "Subjects"), length(databaseIds)), th)
        )
      )
    ))
    
    options = list(pageLength = 10,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE)
    # ,
    # columnDefs = list(minCellCountDef(2 + (1:(length(databaseIds) * 2)))))
    table <- DT::datatable(table,
                           options = options,
                           colnames = colnames(table),
                           rownames = FALSE,
                           # container = sketch,
                           escape = FALSE,
                           filter = c('bottom'),
                           class = "stripe nowrap compact")
    table <- DT::formatStyle(table = table,
                             columns = 3 + (1:(length(databaseIds) * 2)),
                             background = DT::styleColorBar(c(0, maxConceptCount), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  output$inclusionRuleTable <- DT::renderDataTable(expr = {
    table <- inclusionRuleStats %>% 
      dplyr::filter(.data$cohortId == cohortId() &
                      .data$databaseId == input$database) %>% 
      dplyr::select(.data$ruleSequenceId, .data$ruleName, 
                    .data$meetSubjects, .data$gainSubjects, 
                    .data$remainSubjects, .data$totalSubjects, .data$databaseId) %>% 
      dplyr::arrange(.data$ruleSequenceId)
    
    if (nrow(table) == 0) {
      return(dplyr::tibble(Note = paste0('No data available for selected databases and cohorts')))
    }
    
    databaseIds <- inclusionRuleStats %>%
      dplyr::filter(.data$databaseId %in% input$databases) %>% 
      dplyr::select(.data$databaseId) %>% 
      dplyr::distinct() %>% 
      dplyr::arrange() %>% 
      dplyr::pull(.data$databaseId)
    
    if (!isTRUE(all.equal(databaseIds %>% sort(),
                          input$databases %>% unique() %>% sort()))) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases %>% sort(), 
                                                        databaseIds %>% sort()), 
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
      class = 'display',
      thead(
        tr(
          th(rowspan = 2, 'Rule Sequence Id'),
          th(rowspan = 2, 'Rule Name'),
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
                   paging = TRUE)
    # ,
    # columnDefs = list(minCellCountDef(1 + (1:(databaseIds * 4)))))
    
    table <- DT::datatable(table,
                           options = options,
                           colnames = colnames(table) %>% camelCaseToTitleCase(),
                           rownames = FALSE,
                           # container = sketch,
                           escape = FALSE,
                           filter = c('bottom'),
                           class = "stripe nowrap compact")
    table <- DT::formatStyle(table = table,
                             columns = 2 + (1:(databaseIds * 4)),
                             #background = DT::styleColorBar(lims, "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  output$breakdownTable <- DT::renderDataTable(expr = {
    data <- indexEventBreakdown %>%
      dplyr::filter(.data$cohortId == cohortId() & 
                      .data$databaseId %in% input$databases) %>%
      dplyr::select(-.data$cohortId) %>% 
      dplyr::inner_join(concept, by = "conceptId") %>% 
      dplyr::select(.data$conceptId, .data$conceptName,
                    .data$databaseId, .data$conceptCount)
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0('No data available for selected databases and cohorts')))
    }
    
    databaseIds <- unique(data$databaseId) %>% sort()
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
                               filter = c('bottom'),
                               class = "stripe nowrap compact")
    for (col in 3:ncol(table)) {
      dataTable <- DT::formatStyle(table = dataTable,
                                   columns = col,
                                   background = DT::styleColorBar(c(0, max(table[, col], na.rm = TRUE)), "lightblue"),
                                   backgroundSize = "98% 88%",
                                   backgroundRepeat = "no-repeat",
                                   backgroundPosition = "center")
    }
    return(dataTable)
  }, server = TRUE)
  
  output$visitContextTable <- DT::renderDataTable(expr = {
    data <- visitContext %>% 
      dplyr::filter(.data$cohortId == cohortId() & 
                      .data$databaseId %in% input$databases)
    
    if (nrow(data) == 0) {
      return(dplyr::tibble(Note = paste0('No data available for selected databases and cohort')))
    }
    
    databaseIds <- visitContext %>%
      dplyr::filter(.data$databaseId %in% input$databases) %>% 
      dplyr::select(.data$databaseId) %>% 
      dplyr::distinct() %>% 
      dplyr::arrange() %>% 
      dplyr::pull(.data$databaseId)
    
    if (!isTRUE(all.equal(databaseIds %>% sort(),
                          input$databases %>% unique() %>% sort()))) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases, 
                                                        databaseIds), 
                                                collapse = ", \n"), 
                                         ".\n Please unselect them.")))
    }
    
    visitContextReferene <-  tidyr::crossing(dplyr::tibble(visitContext = visitContext$visitContext %>% 
                                                             unique()),
                                             dplyr::tibble(databaseId = databaseIds))
    visitContextReferene <- visitContextReferene %>% 
      dplyr::mutate(visitContext = replace(visitContext, 1:4, c("Before", "During visit", "On visit start", "After")))
    
    table <- visitContextReferene %>% 
      dplyr::left_join(data) %>% 
      dplyr::left_join(concept, by = c("visitConceptId" = "conceptId")) %>% 
      dplyr::select(.data$conceptName, .data$visitContext, .data$subjects, .data$databaseId) %>% 
      dplyr::mutate(visitContext = paste0(.data$databaseId, "_", .data$visitContext)) %>% 
      tidyr::pivot_wider(id_cols = c(.data$conceptName),
                         names_from = .data$visitContext,
                         values_from = .data$subjects)
    
    sketch <- htmltools::withTags(table(
      class = 'display',
      thead(
        tr(
          th(rowspan = 2, 'Visit'),
          lapply(databaseIds, th, colspan = 4, class = "dt-center")
        ),
        tr(
          lapply(rep(c("Before ", "During Visit","On Visit Start", "After"), length(databaseIds)), th)
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
                   columnDefs = list(truncateStringDef(1, 100),
                                     minCellCountDef(1:(length(databaseIds) * 4))))
    
    table <- DT::datatable(table,
                           options = options,
                           colnames = colnames(table) %>% camelCaseToTitleCase(),
                           rownames = FALSE,
                           container = sketch,
                           escape = TRUE,
                           filter = c('bottom'))
    
    
  }, server = TRUE)
  
  output$characterizationTable <- DT::renderDataTable(expr = {
    data <- covariateValue %>% 
      dplyr::filter(.data$cohortId == cohortId() & 
                      .data$databaseId %in% input$databases) %>% 
      dplyr::select(-cohortId)
    
    dataCounts <- data %>% 
      dplyr::select(.data$databaseId) %>% 
      dplyr::distinct() %>% 
      dplyr::left_join(y = (cohortCount %>% 
                              dplyr::filter(.data$cohortId == cohortId()) %>% 
                              dplyr::select(-cohortId))) %>% 
      dplyr::arrange(.data$databaseId)
    
    if (nrow(dataCounts) == 0) {
      return(dplyr::tibble(Note = paste0('No data available for selected databases and cohorts')))
    }
    
    if (!isTRUE(all.equal(dataCounts$databaseId %>% unique %>% sort(),
                  input$databases %>% unique() %>% sort()))) {
      return(dplyr::tibble(Note = paste0("There is no data for the databases:\n",
                                         paste0(setdiff(input$databases, 
                                                        dataCounts$databaseId), 
                                                collapse = ",\n "), 
                                         ".\n Please unselect them.")))
    }
    
    dataCountsWithSubjectCountBelowThreshold <- dataCounts %>% 
      dplyr::filter(.data$cohortSubjects < thresholdCohortSubjects)
    
    if (nrow(dataCountsWithSubjectCountBelowThreshold) > 0) {
      return(dataCountsWithSubjectCountBelowThreshold %>% 
               dplyr::mutate(threshold = thresholdCohortSubjects) %>% 
               dplyr::rename('These datasource(s) have less than threshold value. Please unselect the following in the Database selection option:'  = 
                               .data$databaseId))
    }
    
    if (input$charType == "Pretty") {
      data <- data %>% 
        dplyr::inner_join(y = covariateRef) %>% 
        dplyr::distinct()
      table <- list()
      characteristics <- list()
      for (j in (1:nrow(dataCounts))) {
        dataCount <- dataCounts[j,]
        temp <- data %>% 
          dplyr::filter(.data$databaseId == dataCount$databaseId) %>% 
          prepareTable1()
        table[[j]] <- temp
        if (nrow(table[[j]]) > 0) {
          table[[j]] <- table[[j]] %>% 
            dplyr::mutate(databaseId = dataCount$databaseId)
          characteristics[[j]] <- table[[j]] %>% 
            dplyr::select(.data$characteristic, .data$position, 
                          .data$header, .data$sortOrder)
        } else {
          return(dplyr::tibble(Note = paste0(dataCount$databaseId, ' does not have covariates that are part of pretty table. Please unselect.')))
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
                           dplyr::select(-.data$sortOrder))  %>% 
        dplyr::arrange(.data$sortOrder) %>% 
        tidyr::pivot_wider(id_cols = 'characteristic', 
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
                       minCellPercentDef(1:nrow(dataCounts))
                     ))
      sketch <- htmltools::withTags(table(
        class = 'display',
        thead(
          tr(
            th(rowspan = 3, 'Covariate Name'),
            lapply(dataCounts$databaseId, th, colspan = 1, class = "dt-center")
          ),
          tr(
            lapply(paste0("(n = ", 
                          format(dataCounts$cohortSubjects, big.mark = ","), ")"), 
                   th, 
                   colspan = 1, 
                   class = "dt-center no-padding")
          ),
          tr(
            lapply(rep(c("Proportion"), 
                       length(dataCounts$databaseId)), th)
          )
        )
      ))
      table <- DT::datatable(table,
                             options = options,
                             rownames = FALSE,
                             container = sketch, 
                             escape = FALSE,
                             filter = c('bottom'),
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns = 1 + (1:nrow(dataCounts)),
                               background = DT::styleColorBar(c(0,1), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    } else {
      data <- data %>% 
        dplyr::mutate(databaseId = stringr::str_replace_all(string = .data$databaseId, pattern = "_", replacement = " ")) %>% 
        tidyr::pivot_wider(id_cols = 'covariateId', 
                           names_from = "databaseId",
                           values_from = "mean" ,
                           names_sep = "_"
        ) %>%  
        dplyr::left_join(y = covariateRef %>% 
                           dplyr::select(.data$covariateId, 
                                         .data$covariateName, 
                                         .data$conceptId) %>% 
                           dplyr::distinct()) %>%
        dplyr::select(-covariateId) %>% 
        dplyr::relocate("covariateName", "conceptId") %>% 
        dplyr::arrange(.data$covariateName) %>% 
        dplyr::distinct()
      
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
                       minCellPercentDef(1:(length(dataCounts$databaseId)) + 1)))
      # sketch <- htmltools::withTags(table(
      #  class = 'display',
      #  thead(
      #    tr(
      #      th(rowspan = 2, 'Covariate Name'),
      #      th(rowspan = 2, 'Concept Id'),
      #      lapply(dataCounts$databaseId, th, colspan = 2, class = "dt-center")
      #    ),
      #    tr(
      #      lapply(rep(c("Proportion"), nrow(dataCounts)), th)
      #    )
      #  )
      # ))
      table <- DT::datatable(data,
                             options = options,
                             rownames = FALSE,
                             #   container = sketch, 
                             escape = FALSE,
                             filter = c('bottom'),
                             class = "stripe nowrap compact")
      table <- DT::formatStyle(table = table,
                               columns = (2 + (1:length(dataCounts$databaseId))),
                               background = DT::styleColorBar(c(0,1), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    }
    return(table)
  }, server = TRUE)
  
  covariateIdArray <- reactiveVal()
  covariateIdArray(c())
  observeEvent(input$rows, {
    if (input$rows[[2]] %in% covariateIdArray())
      covariateIdArray(covariateIdArray()[covariateIdArray() %in% input$rows[[2]] == FALSE])
    else
      covariateIdArray(c(covariateIdArray(),input$rows[[2]]))
  })
  
  # covariateTimeSeriesPlot <- shiny::reactive({
  #   data <- temporalCovariateValue %>% 
  #     dplyr::filter(.data$cohortId == cohortId() & 
  #                     .data$databaseId == input$database & 
  #                     .data$timeId > 5 & 
  #                     .data$covariateId %in% covariateIdArray()) %>% 
  #     dplyr::inner_join(temporalTimeRef) %>% 
  #     dplyr::inner_join(temporalCovariateRef)
  #   
  #   dataTS <- tsibble::as_tsibble(x = data %>% 
  #                                   dplyr::select(.data$startDay,
  #                                                 .data$endDay,
  #                                                 .data$timeId,
  #                                                 .data$covariateId,
  #                                                 .data$covariateName,
  #                                                 .data$mean) %>% 
  #                                   dplyr::arrange(.data$timeId),
  #                                 key = c(.data$covariateName), 
  #                                 index = .data$timeId,
  #                                 regular = TRUE,
  #                                 validate = TRUE)
  #   
  #   # dcmp <- dataTS %>%
  #   #   fabletools::model(.data = feasts::STL(.data$mea ~ season(window = Inf)))
  #   # 
  #   # plot <- feasts::gg_season(data = dataTS)
  #   
  #   
  #   
  #   
  #   plot <- dataTS %>%
  #     ggplot2::autoplot(.data$mean) +
  #     ggplot2::xlab("Start Day") +
  #     ggiraph::geom_point_interactive(
  #       ggplot2::aes(tooltip = .data$covariateName), size = 2) +
  #     ggplot2::theme(legend.position = "none")
  # 
  #   plot <- ggiraph::girafe(ggobj = plot,width_svg = 10, height_svg = 4, options = list(
  #     ggiraph::opts_sizing(width = .7),
  #     ggiraph::opts_zoom(max = 5))
  #   )
  #   return(plot)
  # })
  
  # output$covariateTimeSeriesPlot <- ggiraph::renderggiraph(expr = {
  #   return(covariateTimeSeriesPlot())
  # })
  
  output$temporalCharacterizationTable <- DT::renderDataTable(expr = {
    table <- temporalCovariateValue %>% 
      dplyr::filter(.data$cohortId == cohortId(),
                    .data$databaseId == input$database,
                    .data$timeId %in% c(timeId())) %>% 
      dplyr::select(-cohortId) %>% 
      dplyr::mutate(databaseId = stringr::str_replace_all(string = .data$databaseId, 
                                                          pattern = "_", 
                                                          replacement = " ")) %>% 
      dplyr::left_join(y = temporalCovariateChoices) %>% 
      dplyr::left_join(y = temporalCovariateRef)  %>%
      dplyr::arrange(.data$timeId)  %>% 
      tidyr::pivot_wider(id_cols = c('covariateId', 'covariateName', 'conceptId'), 
                         names_from = "choices",
                         values_from = "mean" ,
                         names_sep = "_"
      ) %>% 
      dplyr::select(-.data$conceptId) %>% 
      dplyr::relocate(.data$covariateName, .data$covariateId) %>% 
      dplyr::arrange(.data$covariateName)
    
    if (nrow(table) == 0) {
      return(dplyr::tibble(Note = paste0('No data available for selected databases and cohorts')))
    }
    
    temporalCovariateChoicesSelected <- temporalCovariateChoices %>% 
      dplyr::filter(.data$timeId %in% c(timeId())) 
    
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
                           filter = c('bottom'),
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
  
  output$overlapTable <- DT::renderDataTable(expr = {
    data <- getCohortOverlapResult(targetCohortIds = cohortId(), 
                                   comparatorCohortIds = comparatorCohortId(), 
                                   databaseIds = input$database)
    
    if (is.null(data)) {
      return(dplyr::tibble(' ' = paste0('No data available for selected databases and cohorts and comaprator')))
    }
    table <- data.frame(row.names = c("Subject in either cohort",
                                      "Subject in both cohort",
                                      "Subject in target not in comparator",
                                      "Subject in comparator not in target",
                                      "Subject in target before comparator",
                                      "Subject in comparator before target",
                                      "Subject in target and comparator on same day"),
                        Value = c(data$eitherSubjects,
                                  data$bothSubjects,
                                  data$tOnlySubjects,
                                  data$cOnlySubjects,
                                  data$tBeforeCSubjects,
                                  data$cBeforeTSubjects,
                                  data$sameDaySubjects))
    if (!is.null(data$tInCSubjects)) {
      table <- rbind(table,
                     data.frame(row.names = c("Subject having target start during comparator",
                                              "Subject having comparator start during target"),
                                Value = c(data$tInCSubjects,
                                          data$cInTSubjects)))
    }
    table$Value[is.na(table$Value)] <- 0
    options = list(pageLength = 7,
                   searching = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   searchHighlight = TRUE,
                   ordering = FALSE,
                   paging = FALSE,
                   info = FALSE,
                   columnDefs = list(minCellCountDef(1)))
    table <- DT::datatable(table,
                           options = options,
                           rownames = TRUE,
                           filter = c('bottom'),
                           class = "stripe nowrap compact")
    return(table)
  }, server = TRUE)
  
  overLapPlot <- shiny::reactive({
    data <- getCohortOverlapResult(targetCohortIds = cohortId(), 
                                   comparatorCohortIds = comparatorCohortId(), 
                                   databaseIds = input$database)
    validate(
      need(!(cohortId() == comparatorCohortId()), paste0('Target cohort and comparator cannot be the same')))
    
    validate(
      need(!is.null(data), paste0('No cohort overlap data for this combination')))
    plot <- plotCohortOverlapVennDiagram(data = data,
                                         targetCohortIds = cohortId(),
                                         comparatorCohortIds = comparatorCohortId(),
                                         databaseIds = input$database)
    
    return(plot)
  })
  
  output$overlapPlot <- shiny::renderPlot(expr = {
    return(overLapPlot())
  }, res = 100)
  
  computeBalance <- shiny::reactive({
    if (cohortId() == comparatorCohortId()) {
      return(dplyr::tibble())
    }
    covs1 <- covariateValue %>% 
      dplyr::filter(.data$cohortId == cohortId(),
                    .data$databaseId == input$database)
    covs2 <- covariateValue %>% 
      dplyr::filter(.data$cohortId == comparatorCohortId(),
                    .data$databaseId == input$database)
    covs1 <- dplyr::left_join(x = covs1, y = covariateRef)
    covs2 <- dplyr::left_join(x = covs2, y = covariateRef)
    balance <- compareCohortCharacteristics(covs1, covs2) %>%
      dplyr::mutate(absStdDiff = abs(.data$stdDiff))
    return(balance)
  })
  
  output$charCompareTable <- DT::renderDataTable(expr = {
    balance <- computeBalance()
    if (nrow(balance) == 0) {
      if (cohortId() == comparatorCohortId()) {
        return(dplyr::tibble(Note = "Cohort and Target are the same. Nothing to compare"))
      } else {
        return(dplyr::tibble(Note = "No data for the selected combination."))
      }
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
                             filter = c('bottom'),
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
        dplyr::arrange(.data$covariateName) %>% 
        dplyr::select(.data$covariateName, .data$mean1, .data$mean2, .data$stdDiff, .data$conceptId) %>% 
        dplyr::mutate(stdDiff = round(x = .data$stdDiff, digits = 3)) %>% 
        dplyr::rename_with(.fn = ~ stringr::str_replace(string = ., pattern = 'mean1', replacement = 'Target')) %>% 
        dplyr::rename_with(.fn = ~ stringr::str_replace(string = ., pattern = 'mean2', replacement = 'Comparator')) %>% 
        dplyr::rename_with(.fn = camelCaseToTitleCase)
      
      options = list(pageLength = 100,
                     searching = TRUE,
                     searchHighlight = TRUE,
                     scrollX = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       minCellPercentDef(1:2)))
      
      table <- DT::datatable(table,
                             options = options,
                             rownames = FALSE,
                             escape = FALSE,
                             filter = c('bottom'),
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
    }
    return(table)
  }, server = TRUE)
  
  output$charComparePlot <- ggiraph::renderggiraph(expr = {
    data <- compareCovariateValueResult(connection = NULL, 
                                        connectionDetails = NULL,
                                        targetCohortIds = cohortId(), 
                                        comparatorCohortIds = comparatorCohortId(),
                                        databaseIds = input$database,
                                        minProportion = 0.01,
                                        maxProportion = 1,
                                        isTemporal = FALSE,
                                        timeIds = NULL,
                                        resultsDatabaseSchema = NULL,
                                        domain = input$domain)
    validate(
      need(!is.null(data), paste0('No cohort compare data for this combination')))
    
    validate(
      need(!(cohortId() == comparatorCohortId()), paste0('Target cohort and comparator cannot be the same')))
    
    cohortReference <- getCohortReference()
    covariateReference <- getCovariateReference(isTemporal = FALSE)
    plot <- plotCohortComparisonStandardizedDifference(data = data, 
                                                       targetCohortIds = cohortId(), 
                                                       comparatorCohortIds = comparatorCohortId(),
                                                       cohortReference = cohortReference,
                                                       covariateReference = covariateReference,
                                                       concept = NULL,
                                                       databaseIds = input$database)
    
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
                   columnDefs = list(list(width = '30%', targets = 1),
                                     list(width = '60%', targets = 2))
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
    targetCohortWithCount <- cohortCount %>% 
      dplyr::filter(.data$cohortId == cohortId(),
                    .data$databaseId == input$database) %>% 
      dplyr::left_join(y = cohort) %>% 
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
    
    comparatorCohortWithCount <- cohortCount %>% 
      dplyr::filter(.data$cohortId == comparatorCohortId(),
                    .data$databaseId == input$database) %>%
      dplyr::left_join(y = cohort)
    
    return(htmltools::withTags(
      div(table(
        tr(
          td(
            h5("Target: ", targetCohortWithCount$cohortName, " ( n = ", scales::comma(targetCohortWithCount$cohortSubjects), " )"),
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
    return(selectedCohortCounts())
  })
  
  #Download
  download_box <- function(exportname, plot){
    downloadHandler(
      filename = function() {
        paste(exportname, Sys.Date(), ".png", sep = "")
      },
      content = function(file) {
        ggplot2::ggsave(file, plot = plot, device = "png", width = 9, height = 7, dpi = 400)
      }
    )
  }
  
  output$downloadOverlapPlot <- download_box("OverlapPlot", overLapPlot())
})
