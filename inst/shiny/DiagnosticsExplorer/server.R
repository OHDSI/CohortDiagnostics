library(magrittr)

source("R/Tables.R")
source("R/Other.R")

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

shiny::shinyServer(function(input, output, session) {
  
  cohortId <- shiny::reactive({
    return(cohorts$cohortId[cohorts$cohortName == input$cohort])
  })
  
  comparatorCohortId <- shiny::reactive({
    return(cohorts$cohortId[cohorts$cohortName == input$comparator])
  })
  
  timeId <- shiny::reactive({
    return(temporalCovariateChoices %>%
             dplyr::filter(choices %in% input$timeIdChoices) %>%
             dplyr::pull(timeId))
  })
  
  cohortBaseUrl2 <- shiny::reactive({
    return(input$cohortBaseUrl2)
  })
  
  cohortBaseUrl <- shiny::reactive({
    return(input$cohortBaseUrl)
  })
  
  conceptIdBaseUrl <- shiny::reactive({
    return(input$conceptIdBaseUrl)
  })
  
  shiny::observe({
    subset <- unique(conceptSet$conceptSetName[conceptSet$cohortId == cohortId()]) %>% sort()
    shinyWidgets::updatePickerInput(session = session,
                                    inputId = "conceptSet",
                                    choices = subset)
  })
  
  output$phenoTypeDescriptionTable <- DT::renderDataTable(expr = {
    data <- phenotypeDescription %>% 
      dplyr::mutate(literatureReview = dplyr::case_when(!.data$literatureReview %in% c('','0') ~ 
                                                          paste0("<a href='", .data$literatureReview, "' target='_blank'>", "Link", "</a>"),
                                                        TRUE ~ 'Ongoing')) %>%
      dplyr::mutate(referentConceptId = paste0("<a href='", paste0(conceptIdBaseUrl(), .data$referentConceptId), "' target='_blank'>", .data$referentConceptId, "</a>")) %>% 
      dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                   pattern = "Overview:", 
                                                                   replacement = "<strong>Overview:</strong>")) %>% 
      dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription, 
                                                                   pattern = "Presentation:", 
                                                                   replacement = "<br/> <strong>Presentation: </strong>")) %>% 
      dplyr::mutate(clinicalDescription = stringr::str_replace_all(string = .data$clinicalDescription,
                                                                   pattern = "Plan:",
                                                                   replacement = "<br/> <strong>Plan: </strong>"))
    
    options = list(pageLength = 20,
                   searching = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE,
                   searchHighlight = TRUE)
    
    dataTable <- DT::datatable(data,
                               options = options,
                               rownames = FALSE,
                               colnames = colnames(data) %>% SqlRender::camelCaseToTitleCase(),
                               escape = FALSE,
                               filter = c("bottom"),
                               class = "stripe compact")
    return(dataTable)
  }, server = TRUE)
  
  output$cohortDescriptionTable <- DT::renderDataTable(expr = {
    data <- cohort %>% 
      dplyr::mutate(webApiCohortId = as.integer(.data$webApiCohortId)) %>% #this is temporary - we need to standardize this 
      dplyr::left_join(y = phenotypeDescription) %>% 
      dplyr::mutate(cohortName = paste0("<a href='", paste0(cohortBaseUrl(), .data$webApiCohortId),"' target='_blank'>", paste0(.data$cohortName), "</a>")) %>% 
      dplyr::select(.data$phenotypeId, .data$cohortId, .data$cohortName, .data$logicDescription) %>% 
      dplyr::arrange(.data$phenotypeId, .data$cohortId, .data$cohortName)
    
    options = list(pageLength = 20,
                   searching = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE,
                   searchHighlight = TRUE)
    
    dataTable <- DT::datatable(data,
                               options = options,
                               rownames = FALSE,
                               colnames = colnames(data) %>% SqlRender::camelCaseToTitleCase(),
                               escape = FALSE,
                               filter = c("bottom"),
                               class = "stripe compact")
    return(dataTable)
  }, server = TRUE)
  
  output$cohortCountsTable <- DT::renderDataTable(expr = {
    data <- cohortCount %>%
      dplyr::filter(.data$databaseId %in% input$databases) %>% 
      dplyr::left_join(cohort) %>% 
      dplyr::mutate(phenotypeId = .data$referentConceptId * 1000) %>% 
      dplyr::select(.data$phenotypeId, .data$cohortId, 
                    .data$cohortName, .data$databaseId, 
                    .data$logicDescription, .data$webApiCohortId,
                    .data$cohortSubjects, .data$cohortEntries)
    
    if (nrow(data) == 0) {
      return(NULL)
    }
    databaseIds <- data %>% 
      dplyr::select(.data$databaseId) %>% 
      dplyr::distinct() %>% 
      dplyr::arrange() %>% 
      dplyr::pull(.data$databaseId)
    
    table <- dplyr::full_join(
      data %>% 
        dplyr::select(.data$cohortId, .data$databaseId, 
                      .data$cohortSubjects) %>% 
        dplyr::mutate(columnName = paste0(.data$databaseId, "_subjects")) %>% 
        tidyr::pivot_wider(id_cols = c(.data$cohortId),
                           names_from = columnName,
                           values_from = .data$cohortSubjects,
                           values_fill = 0
        ),
      data %>% 
        dplyr::select(.data$cohortId, .data$databaseId, 
                      .data$cohortEntries) %>% 
        dplyr::mutate(columnName = paste0(.data$databaseId, "_entries")) %>% 
        tidyr::pivot_wider(id_cols = c(.data$cohortId),
                           names_from = columnName,
                           values_from = .data$cohortEntries,
                           values_fill = 0
        )
    )
    table <- table %>% 
      dplyr::select(order(colnames(table))) %>% 
      dplyr::relocate(.data$cohortId)
    
    table <- data %>% 
      dplyr::select(.data$cohortId, .data$cohortName) %>% 
      dplyr::distinct() %>% 
      dplyr::inner_join(table) %>% 
      dplyr::mutate(url = paste0(cohortBaseUrl2(), table$cohortId),
                    cohortName = paste0("<a href='", 
                                        .data$url, 
                                        "' target='_blank'>", 
                                        .data$cohortName, 
                                        "</a>")
      ) %>% 
      dplyr::select(-.data$cohortId, -.data$url) %>%
      dplyr::arrange(.data$cohortName)
    
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
    
    options = list(pageLength = 20,
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
  
  # filteredIncidenceRates <- shiny::reactive({
    # data <- incidenceRate[incidenceRate$cohortId == cohortId() & 
    #                         incidenceRate$databaseId %in% input$databases, ]
    # 
    # data <- data[data$incidenceRate > 0, ]
    # if (nrow(data) == 0) {
    #   return(NULL)
    # }
    # stratifyByAge <- "Age" %in% input$irStratification
    # stratifyByGender <- "Gender" %in% input$irStratification
    # stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
    # minPersonYears = 1000
    # 
    # idx <- rep(TRUE, nrow(data))
    # if (stratifyByAge) {
    #   idx <- idx & !is.na(data$ageGroup)
    # } else {
    #   idx <- idx & is.na(data$ageGroup)
    # }
    # if (stratifyByGender) {
    #   idx <- idx & !is.na(data$gender)
    # } else {
    #   idx <- idx & is.na(data$gender)
    # }
    # if (stratifyByCalendarYear) {
    #   idx <- idx & !is.na(data$calendarYear)
    # } else {
    #   idx <- idx & is.na(data$calendarYear)
    # }
    # data <- data[idx, ]
    # data <- data[data$cohortCount > 0, ]
    # data <- data[data$personYears > minPersonYears, ]
    # data$gender <- as.factor(data$gender)
    # data$calendarYear <- as.numeric(as.character(data$calendarYear))
    # ageGroups <- unique(data$ageGroup)
    # ageGroups <- ageGroups[order(as.numeric(gsub("-.*", "", ageGroups)))]
    # data$ageGroup <- factor(data$ageGroup, levels = ageGroups)
    # data <- data[data$incidenceRate > 0, ]
    # data$dummy <- 0
    # if (nrow(data) == 0) {
    #   return(NULL)
    # } else {
    #   return(data)
    # }
  # })
  
  output$incidenceRatePlot <- plotly::renderPlotly(expr = {
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
    
    data <- CohortDiagnostics::getIncidenceRateResult(connection = NULL,
                                                      connectionDetails = NULL,
                                                      cohortIds = cohortId(), 
                                                      databaseIds = input$databases, 
                                                      stratifyByGender =  stratifyByGender,
                                                      stratifyByAgeGroup =  stratifyByAge,
                                                      stratifyByCalendarYear =  stratifyByCalendarYear,
                                                      minPersonYears = 1000,
                                                      resultsDatabaseSchema = NULL)
    
    if (is.null(data)) {
      return(NULL)
    }
    
    plot <- CohortDiagnostics::plotIncidenceRate(data = data,
                                                 cohortIds = NULL,
                                                 databaseIds = NULL,
                                                 stratifyByAgeGroup = stratifyByAge,
                                                 stratifyByGender = stratifyByGender,
                                                 stratifyByCalendarYear  = stratifyByCalendarYear,
                                                 yscaleFixed =   input$irYscaleFixed)
    return(plot)
  })
  
  # output$hoverInfoIr <- shiny::renderUI({
    # data <- filteredIncidenceRates()
    # if (is.null(data)) {
    #   return(NULL)
    # }else {
    #   hover <- input$plotHoverIr
    #   point <- nearPoints(data, hover, threshold = 5, maxpoints = 1, addDist = TRUE)
    #   if (nrow(point) == 0) {
    #     return(NULL)
    #   }
    #   left_px <- hover$coords_css$x
    #   top_px <- hover$coords_css$y
    #   
    #   text <- gsub("-", "<", sprintf("<b>Incidence rate: </b> %0.3f per 1,000 patient years", point$incidenceRate))
    #   text <- paste(text, sprintf("<b>Cohort count (numerator): </b> %s",  format(point$cohortCount, scientific = FALSE, big.mark = ",")), sep = "<br/>")
    #   text <- paste(text, sprintf("<b>Person time (denominator): </b> %s years", format(round(point$personYears), scientific = FALSE, big.mark = ",")), sep = "<br/>")
    #   text <- paste(text, "", sep = "<br/>")
    #   
    #   if (!is.na(point$ageGroup)) {
    #     text <- paste(text, sprintf("<b>Age group: </b> %s years", point$ageGroup), sep = "<br/>")
    #     top_px <- top_px - 15
    #   }
    #   if (!is.na(point$gender)) {
    #     text <- paste(text, sprintf("<b>Gender: </b> %s", point$gender), sep = "<br/>")
    #     top_px <- top_px - 15
    #   }
    #   if (!is.na(point$calendarYear)) {
    #     text <- paste(text, sprintf("<b>Calendar year: </b> %s", point$calendarYear), sep = "<br/>")
    #     top_px <- top_px - 15
    #   }
    #   text <- paste(text, sprintf("<b>Database: </b> %s", point$databaseId), sep = "<br/>")
    #   style <- paste0("position:absolute; z-index:100; background-color: rgba(245, 245, 245, 0.85); ",
    #                   "left:",
    #                   left_px - 200,
    #                   "px; top:",
    #                   top_px - 170,
    #                   "px; width:400px;")
    #   div(
    #     style = "position: relative; width: 0; height: 0",
    #     wellPanel(
    #       style = style,
    #       p(HTML(text))
    #     )
    #   )
    # }
  # }) 
  
  timeDisPlotDownload <- shiny::reactive({
    data <- CohortDiagnostics::getTimeDistributionResult(cohortIds = cohortId(), databaseIds = input$databases)
    
    if (is.null(data)) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
    }
    
    plot <- CohortDiagnostics::plotTimeDistribution(data = data,
                                                    cohortIds = cohortId(),
                                                    databaseIds = input$databases)
    
    return(plot)
  })

  output$timeDisPlot <- shiny::renderPlot(expr = {
    return(timeDisPlotDownload())
  }, res = 100)
  
  output$timeDistTable <- DT::renderDataTable(expr = {

    table <- CohortDiagnostics::getTimeDistributionResult(cohortIds = cohortId(), databaseIds = input$databases)

    if (is.null(table)) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
    }
    
    options = list(pageLength = 20,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE)
    table <- DT::datatable(table,
                           options = options,
                           rownames = FALSE,
                           colnames = colnames(table) %>% SqlRender::camelCaseToTitleCase(),
                           filter = c('bottom'),
                           class = "stripe nowrap compact")
    table <- DT::formatRound(table, c("Average", "SD"), digits = 2)
    table <- DT::formatRound(table, c("Min", "P10", "P25", "Median", "P75", "P90", "Max"), digits = 0)

    return(table)
  }, server = TRUE)
  
  output$includedConceptsTable <- DT::renderDataTable(expr = {
    data <- includedSourceConcept %>% 
      dplyr::left_join(concept) %>% 
      dplyr::left_join(conceptSets) %>%
      dplyr::filter(.data$cohortId == cohortId() &
                      .data$conceptSetName == input$conceptSet &
                      .data$databaseId %in% input$databases)
    
    maxConceptSubjects <- max(data$conceptSubjects)
    
    if (input$includedType == "Source Concepts") {
      table <- data %>%
        dplyr::select(.data$sourceConceptId, .data$vocabularyId, 
                      .data$conceptCode, .data$conceptName, 
                      .data$conceptSubjects, .data$databaseId) %>% 
        dplyr::group_by(.data$sourceConceptId, .data$vocabularyId, 
                        .data$conceptCode, .data$conceptName, 
                        .data$databaseId) %>%
        dplyr::summarise(conceptSubjects = sum(.data$conceptSubjects)) %>% #this logic needs to be confirmed
        dplyr::ungroup() %>% 
        dplyr::arrange(.data$databaseId) %>% 
        dplyr::rename(conceptId = .data$sourceConceptId)
        if (nrow(data) == 0) {
          return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
        }
      # colnames(table) <- SqlRender::camelCaseToTitleCase(colnames(table))
      table <- table %>% 
        tidyr::pivot_wider(id_cols = c(.data$conceptId, .data$vocabularyId, 
                                       .data$conceptCode, .data$conceptName),
                           names_from = .data$databaseId,
                           values_from = .data$conceptSubjects,
                           values_fill = 0)
      
      table[table < 0] <- 0
      
      options = list(pageLength = 999,
                     searching = TRUE,
                     scrollX = TRUE,
                     lengthChange = FALSE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       list(minCellCountDef(0))
                     ))
      
      table <- DT::datatable(table,
                             colnames = colnames(table),
                             options = options,
                             rownames = FALSE, 
                             escape = FALSE,
                             filter = c('bottom'),
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns =  4 + (1:length(input$databases)),
                               background = DT::styleColorBar(c(0,maxConceptSubjects), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    } else {
      table <- data %>% 
        dplyr::mutate(absConceptSubjects = abs(.data$conceptSubjects)) %>% 
        dplyr::group_by(.data$conceptId, .data$conceptName, .data$databaseId) %>% 
        dplyr::summarise(absConceptSubjects = sum(.data$absConceptSubjects)) %>% 
        dplyr::arrange(.data$databaseId) %>% 
        tidyr::pivot_wider(id_cols = c(.data$conceptId, .data$conceptName),
                           names_from = .data$databaseId,
                           values_from = .data$absConceptSubjects,
                           values_fill = 0)
      table[table < 0] <- 0
      
      options = list(pageLength = 999,
                     searching = FALSE,
                     scrollX = TRUE,
                     lengthChange = FALSE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       list(minCellCountDef(0))
                     ))
      
      table <- DT::datatable(table,
                             options = options,
                             colnames = colnames(table),
                             rownames = FALSE,
                             escape = FALSE,
                             filter = c('bottom'),
                             class = "stripe nowrap compact")
      
      table <- DT::formatStyle(table = table,
                               columns =  2 + (1:length(input$databases)),
                               background = DT::styleColorBar(c(0, maxConceptSubjects), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    }
    return(table)
  }, server = TRUE)
  
  output$orphanConceptsTable <- DT::renderDataTable(expr = {
    table <- orphanConcept %>%
      dplyr::inner_join(concept) %>% 
      dplyr::left_join(conceptSets) %>% 
      dplyr::filter(.data$cohortId == cohortId() &
                      .data$conceptSetName == input$conceptSet &
                      .data$databaseId %in% input$databases) %>% 
      dplyr::select(.data$conceptId, .data$standardConcept, 
                    .data$vocabularyId, .data$conceptCode, 
                    .data$conceptName, .data$conceptCount, 
                    .data$databaseId) %>% 
      dplyr::arrange(conceptCount) %>% 
      #temporary solution as described here https://github.com/OHDSI/CohortDiagnostics/issues/162
      dplyr::group_by(.data$conceptId) %>% 
      dplyr::slice(1) %>% 
      dplyr::ungroup()
    # colnames(table) <- SqlRender::camelCaseToTitleCase(string = colnames(table))
    if (nrow(table) == 0) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
    }
    table <- table %>% 
      tidyr::pivot_wider(id_cols = c(.data$conceptId, .data$standardConcept, 
                                     .data$vocabularyId, .data$conceptCode, 
                                     .data$conceptName),
                         names_from = .data$databaseId,
                         values_from = .data$conceptCount,
                         values_fill = -10)
    
    maxConceptCount <- max(table$conceptCount)
    table[table < 0] <- 0
    
    options = list(pageLength = 20,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(minCellCountDef(0)))
    table <- DT::datatable(table,
                           options = options,
                           colnames = colnames(table),
                           rownames = FALSE,
                           escape = FALSE,
                           filter = c('bottom'),
                           class = "stripe nowrap compact")
    table <- DT::formatStyle(table = table,
                             columns = 5 + (1:length(input$databases)),
                             background = DT::styleColorBar(c(0,maxConceptCount), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  output$inclusionRuleTable <- DT::renderDataTable(expr = {
    table <- inclusionRuleStats %>% 
      dplyr::filter(.data$cohortId == cohortId() &
                      .data$databaseId == input$database) %>% 
      dplyr::arrange(.data$ruleSequence)
    
    if (nrow(table) == 0) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
    }
    
    options = list(pageLength = 20,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(minCellCountDef(2:4)))
    table <- DT::datatable(table,
                           options = options,
                           colnames = colnames(table) %>% 
                             SqlRender::camelCaseToTitleCase(),
                           rownames = FALSE,
                           escape = FALSE,
                           filter = c('bottom'),
                           class = "stripe nowrap compact")
    table <- DT::formatStyle(table = table,
                             columns = 5,
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
      dplyr::left_join(concept)
    
    if (nrow(data) == 0) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
    }
    
    data <- data[, c("conceptId", "conceptName", "conceptCount", "databaseId" )]
    databaseIds <- unique(data$databaseId)
    table <- data[data$databaseId == databaseIds[1], ]
    table$databaseId <- NULL
    colnames(table)[3] <- paste(databaseIds[1], "Count")
    if (length(databaseIds) > 1) {
      for (i in 2:length(databaseIds)) {
        temp <- data[data$databaseId == databaseIds[i],]
        temp$databaseId <- NULL        
        colnames(temp)[3] <- paste(databaseIds[i], "Count")
        table <- merge(table, temp, all = TRUE)
      }
    }
    table <- table[order(-table[,3]), ]
    colnames(table)[1:2] <- c("Concept ID", "Name")
    options = list(pageLength = 20,
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
  
  output$characterizationTable <- DT::renderDataTable(expr = {
    data <- covariateValue %>% 
      dplyr::filter(.data$cohortId == cohortId() & 
                      .data$databaseId %in% input$databases) %>% 
      dplyr::select(-cohortId)
    
    dataCounts <- data %>% 
      dplyr::select(databaseId) %>% 
      dplyr::distinct() %>% 
      dplyr::left_join(y = (cohortCount %>% 
                              dplyr::filter(.data$cohortId == cohortId()) %>% 
                              dplyr::select(-cohortId))) %>% 
      dplyr::arrange(.data$databaseId)
    if (nrow(dataCounts) == 0) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
    }
    if (input$charType == "Pretty") {
      data <- data %>% 
        dplyr::left_join(y = covariateRef) %>% 
        dplyr::distinct()
      table <- list()
      for (j in (1:nrow(dataCounts))) {
        dataCount <- dataCounts %>% 
          dplyr::slice(j)
        temp <- data %>% 
          dplyr::filter(.data$databaseId == dataCount$databaseId) %>% 
          prepareTable1() %>% 
          dplyr::mutate(databaseId = dataCount$databaseId)
        table[[j]] <- temp
      }
      table <- dplyr::bind_rows(table) %>% 
        tidyr::pivot_wider(id_cols = 'characteristic', 
                           names_from = "databaseId",
                           values_from = "value" ,
                           names_sep = "_",
                           values_fill = 0,
                           names_prefix = "Value_"
        )
      options = list(pageLength = 999,
                     searching = FALSE,
                     scrollX = TRUE,
                     lengthChange = FALSE,
                     ordering = FALSE,
                     paging = FALSE,
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
                           names_sep = "_",
                           values_fill = 0
        ) %>%  
        dplyr::left_join(y = covariateRef %>% dplyr::select(.data$covariateId, .data$covariateName, .data$conceptId) %>% dplyr::distinct()) %>%
        dplyr::select(-covariateId) %>% 
        dplyr::relocate("covariateName", "conceptId") %>% 
        dplyr::arrange(.data$covariateName) %>% 
        dplyr::distinct()
      
      options = list(pageLength = 20,
                     searching = TRUE,
                     searchHighlight = TRUE,
                     scrollX = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       minCellPercentDef(1:(length(dataCounts$databaseId)) + 1)
                     )
      )
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
  
  output$temporalCharacterizationTable <- DT::renderDataTable(expr = {
    temporalCovariateRef <- temporalCovariateRef %>% 
      #temporary solution as described here https://github.com/OHDSI/CohortDiagnostics/issues/162
      dplyr::select(.data$covariateId, .data$conceptId, .data$covariateName) %>% 
      dplyr::group_by(.data$covariateId) %>% 
      dplyr::slice(1) %>% 
      dplyr::distinct()
    
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
                         names_sep = "_",
                         values_fill = 0
      ) %>% 
      dplyr::select(-.data$covariateId) %>% 
      dplyr::relocate(.data$covariateName, .data$conceptId) %>% 
      dplyr::arrange(.data$covariateName)
    
    if (nrow(table) == 0) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
    }
    
    temporalCovariateChoicesSelected <- temporalCovariateChoices %>% 
      dplyr::filter(.data$timeId %in% c(timeId())) 
    
    options = list(pageLength = 20,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(
                     truncateStringDef(0, 150),
                     minCellPercentDef(1:(length(temporalCovariateChoicesSelected$choices)) + 1)
                   )
    )
    
    table <- DT::datatable(table,
                           options = options,
                           rownames = FALSE,
                           colnames = colnames(table) %>% SqlRender::camelCaseToTitleCase(),
                           escape = FALSE,
                           filter = c('bottom'),
                           class = "stripe nowrap compact")
    table <- DT::formatStyle(table = table,
                             columns = (2 + (1:length(temporalCovariateChoicesSelected$choices))), #0 index
                             background = DT::styleColorBar(c(0,1), "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  computeTemporalCharacterizationBalance <- shiny::reactive({
    if (length(input$timeIdChoices) != 2 ) {
      return(tidyr::tibble())
    }
    covs1 <- temporalCovariateValue %>% 
      dplyr::filter(.data$timeId == timeId()[1],
                    .data$databaseId == input$database)
    covs2 <- temporalCovariateValue %>% 
      dplyr::filter(.data$timeId == timeId()[2],
                    .data$databaseId == input$database)
    covs1 <- dplyr::left_join(x = covs1, y = temporalCovariateRef)
    covs2 <- dplyr::left_join(x = covs2, y = temporalCovariateRef)
    balance <- compareTemporalCharacterization(covs1, covs2) %>%
      dplyr::mutate(absStdDiff = abs(.data$stdDiff))
    return(balance)
  })
  
  temporalCharacterizationPlot <- shiny::reactive({
    balance <- computeTemporalCharacterizationBalance()
    if (nrow(balance) == 0) {
      return(NULL)
    }
    balance$mean1[is.na(balance$mean1)] <- 0
    balance$mean2[is.na(balance$mean2)] <- 0
    plot <- ggplot2::ggplot(balance, ggplot2::aes(x = mean1, y = mean2, color = absStdDiff)) +
      ggplot2::geom_point(alpha = 0.3, shape = 16, size = 2) +
      ggplot2::geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
      ggplot2::geom_hline(yintercept = 0) +
      ggplot2::geom_vline(xintercept = 0) +             
      ggplot2::scale_x_continuous(input$timeIdChoices[[2]], limits = c(0, 1)) +
      ggplot2::scale_y_continuous(input$timeIdChoices[[1]], limits = c(0, 1)) +
      ggplot2::scale_color_gradient("Absolute\nStd. Diff.", low = "blue", high = "red", space = "Lab", na.value = "red")
    return(plot)
  })
  
  output$temporalCharacterizationPlot <- shiny::renderPlot(expr = {
    return(temporalCharacterizationPlot())
  }, res = 100)
  
  output$temporalCharacterizationPlotHover <- shiny::renderUI({
    balance <- computeTemporalCharacterizationBalance()
    balance$mean1[is.na(balance$mean1)] <- 0
    balance$mean2[is.na(balance$mean2)] <- 0
    if (nrow(balance) == 0) {
      return(NULL)
    } else {
      hover <- input$temporalCharacterizationPlotHoverInfo
      point <- nearPoints(balance, hover, threshold = 5, maxpoints = 1, addDist = TRUE)
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
  
  output$overlapTable <- DT::renderDataTable(expr = {
    data <- CohortDiagnostics::getCohortOverlapResult(targetCohortIds = cohortId(), 
                                                      comparatorCohortIds = comparatorCohortId(), 
                                                      databaseIds = input$database)
    
    if (is.null(data)) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts and comaprator')))
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
                   searching = FALSE,
                   scrollX = TRUE,
                   lengthChange = FALSE,
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
    data <- CohortDiagnostics::getCohortOverlapResult(targetCohortIds = cohortId(), comparatorCohortIds = comparatorCohortId(), databaseIds = input$database)
    
    if (is.null(data)) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts and comaprator')))
    }
    
    plot <- CohortDiagnostics::plotCohortOverlapVennDiagram(data = data,
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
      return(tidyr::tibble())
    }
    covs1 <- covariateValue %>% 
      dplyr::filter(.data$cohortId == cohortId(),
                    .data$databaseId == input$database)
    covs2 <- covariateValue %>% 
      dplyr::filter(.data$cohortId == comparatorCohortId(),
                    .data$databaseId == input$database)
    covs1 <- dplyr::left_join(x = covs1, y = covariate)
    covs2 <- dplyr::left_join(x = covs2, y = covariate)
    balance <- compareCohortCharacteristics(covs1, covs2) %>%
      dplyr::mutate(absStdDiff = abs(.data$stdDiff))
    return(balance)
  })
  
  output$charCompareTable <- DT::renderDataTable(expr = {
    balance <- computeBalance()
    if (nrow(balance) == 0) {
      return(NULL)
    }
    
    if (input$charCompareType == "Pretty table") {
      table <- prepareTable1Comp(balance) %>% 
        dplyr::arrange(.data$sortOrder) %>% 
        dplyr::select(-.data$sortOrder)
      
      options = list(pageLength = 999,
                     searching = FALSE,
                     scrollX = TRUE,
                     lengthChange = FALSE,
                     ordering = FALSE,
                     paging = FALSE,
                     columnDefs = list(minCellPercentDef(1:2))
      )
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
        dplyr::rename_with(.fn = SqlRender::camelCaseToTitleCase)
      
      options = list(pageLength = 20,
                     searching = TRUE,
                     searchHighlight = TRUE,
                     scrollX = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       minCellPercentDef(1:2)
                     )
      )
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
  
  output$charComparePlot <- plotly::renderPlotly(expr = {
    
    data <- CohortDiagnostics::compareCovariateValueResult(connection = NULL, 
                                                           connectionDetails = NULL,
                                                           targetCohortIds = cohortId(), 
                                                           comparatorCohortIds = comparatorCohortId(),
                                                           databaseIds = input$databases,
                                                           minProportion = 0.01,
                                                           maxProportion = 1,
                                                           isTemporal = FALSE,
                                                           timeIds = NULL,
                                                           resultsDatabaseSchema = NULL)
    
    
    if (is.null(data)) {
      return(NULL)
    }

    cohortReference <- CohortDiagnostics::getCohortReference()
    
    covariateReference <- CohortDiagnostics::getCovariateReference(isTemporal = FALSE)
      
    plot <- CohortDiagnostics::plotCohortComparisonStandardizedDifference(data = data, 
                                                                          targetCohortIds = cohortId(), 
                                                                          comparatorCohortIds = comparatorCohortId(),
                                                                          cohortReference = cohortReference,
                                                                          covariateReference = covariateReference,
                                                                          concept = NULL,
                                                                          databaseIds = input$databases)
    
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
                   lengthChange = FALSE,
                   ordering = TRUE,
                   paging = FALSE,
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
  
  output$incidentRateSelectedCohort <- shiny::renderUI({
    return(targetCohortCountHtml())
  })
  
  output$timeDistributionSelectedCohort <- shiny::renderUI({
    return(targetCohortCountHtml())
  })
  
  output$includeConceptsSelectedCohort <- shiny::renderUI({
    return(targetCohortCountHtml())
  })
  
  output$orphanConceptSelectedCohort <- shiny::renderUI({
    return(targetCohortCountHtml())
  })
  
  output$inclusionRuleStatSelectedCohort <- shiny::renderUI({
    return(targetCohortCountHtml())
  })
  
  output$indexEventBreakdownSelectedCohort <- shiny::renderUI({
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
  
  output$timeDistributionPlot <- download_box("TimeDistribution", timeDisPlotDownload())
  output$downloadCompareCohortPlot <- download_box("CompareCohort", downloadCohortComparePlot())
  output$downloadOverlapPlot <- download_box("OverlapPlot", overLapPlot())
  output$downloadTemporalCharacterizationPlot <- download_box("Temporal characterization plot", temporalCharacterizationPlot())
})
