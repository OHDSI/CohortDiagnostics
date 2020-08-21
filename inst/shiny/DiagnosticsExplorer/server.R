library(magrittr)

source("R/Plots.R")
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
    return(cohort$cohortId[cohort$cohortFullName == input$cohort])
  })
  
  comparatorCohortId <- shiny::reactive({
    return(cohort$cohortId[cohort$cohortFullName == input$comparator])
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
    subset <- unique(conceptSets$conceptSetName[conceptSets$cohortId == cohortId()]) %>% sort()
    shinyWidgets::updatePickerInput(session = session,
                                    inputId = "conceptSet",
                                    choices = subset)
  })
  
  output$phenoTypeDescriptionTable <- DT::renderDataTable(expr = {
    data <- phenotypeDescription %>% 
      dplyr::mutate(literatureReview = dplyr::case_when(!.data$literatureReview %in% c('','0') ~ 
                                                          paste0("<a href='", .data$literatureReview, "' target='_blank'>", "Link", "</a>"),
                                                        TRUE ~ 'Ongoing')) %>%
      dplyr::mutate(referentConceptId = paste0("<a href='", paste0(conceptIdBaseUrl(), .data$referentConceptId), "' target='_blank'>", .data$referentConceptId, "</a>")) 
    
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
    data <- cohortDescription %>% 
      dplyr::mutate(atlasId = as.integer(.data$atlasId)) %>% #this is temporary - we need to standardize this 
      dplyr::left_join(y = phenotypeDescription) %>% 
      dplyr::left_join(y = cohort, by = c('atlasId' = 'cohortId')) %>% #this is temporary - we need to standardize this 
      dplyr::mutate(cohortFullName = paste0("<a href='", paste0(cohortBaseUrl(), .data$atlasId),"' target='_blank'>", paste0(.data$cohortDefinitionName), "</a>")) %>% 
      dplyr::select(phenotypeId, phenotypeName, cohortDefinitionId, cohortFullName, logicDescription, cohortDefinitionNotes) %>% 
      dplyr::arrange(phenotypeId, phenotypeName, cohortDefinitionId, cohortFullName)
    
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
    data <- cohortCount[cohortCount$databaseId %in% input$databases, ]
    if (nrow(data) == 0) {
      return(NULL)
    }
    databaseIds <- unique(data$databaseId) %>% sort()
    table <- data[data$databaseId == databaseIds[1], c("cohortId", "cohortEntries", "cohortSubjects")]
    colnames(table)[2:3] <- paste(colnames(table)[2:3], databaseIds[1], sep = "_")
    if (length(databaseIds) > 1) {
      for (i in 2:length(databaseIds)) {
        temp <- data[data$databaseId == databaseIds[i], c("cohortId", "cohortEntries", "cohortSubjects")]
        colnames(temp)[2:3] <- paste(colnames(temp)[2:3], databaseIds[i], sep = "_")
        table <- merge(table, temp, all = TRUE)
      }
    }
    table <- merge(cohort, table, all.x = TRUE)
    table$url <- paste0(cohortBaseUrl2(), table$cohortId)
    table$cohortFullName <- paste0("<a href='", table$url, "' target='_blank'>", table$cohortFullName, "</a>")
    table$cohortId <- NULL
    table$cohortName <- NULL
    table$url <- NULL
    table <- table %>% 
      dplyr::arrange(.data$cohortFullName)
    
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
  
  filteredIncidenceRates <- shiny::reactive({
    data <- incidenceRate[incidenceRate$cohortId == cohortId() & 
                            incidenceRate$databaseId %in% input$databases, ]
    data <- data[data$incidenceRate > 0, ]
    if (nrow(data) == 0) {
      return(NULL)
    }
    stratifyByAge <- "Age" %in% input$irStratification
    stratifyByGender <- "Gender" %in% input$irStratification
    stratifyByCalendarYear <- "Calendar Year" %in% input$irStratification
    minPersonYears = 1000
    
    idx <- rep(TRUE, nrow(data))
    if (stratifyByAge) {
      idx <- idx & !is.na(data$ageGroup)
    } else {
      idx <- idx & is.na(data$ageGroup)
    }
    if (stratifyByGender) {
      idx <- idx & !is.na(data$gender)
    } else {
      idx <- idx & is.na(data$gender)
    }
    if (stratifyByCalendarYear) {
      idx <- idx & !is.na(data$calendarYear)
    } else {
      idx <- idx & is.na(data$calendarYear)
    }
    data <- data[idx, ]
    data <- data[data$cohortCount > 0, ]
    data <- data[data$personYears > minPersonYears, ]
    data$gender <- as.factor(data$gender)
    data$calendarYear <- as.numeric(as.character(data$calendarYear))
    ageGroups <- unique(data$ageGroup)
    ageGroups <- ageGroups[order(as.numeric(gsub("-.*", "", ageGroups)))]
    data$ageGroup <- factor(data$ageGroup, levels = ageGroups)
    data <- data[data$incidenceRate > 0, ]
    data$dummy <- 0
    if (nrow(data) == 0) {
      return(NULL)
    } else {
      return(data)
    }
  })
  
  incidentRatePlotDownload <- shiny::reactive({
    data <- filteredIncidenceRates()
    if (is.null(data)) {
      return(NULL)
    }
    plot <- plotincidenceRate(data = data,
                              stratifyByAge = "Age" %in% input$irStratification,
                              stratifyByGender = "Gender" %in% input$irStratification,
                              stratifyByCalendarYear = "Calendar Year" %in% input$irStratification,
                              yscaleFixed = input$irYscaleFixed)
    return(plot)
  })
  
  output$incidenceRatePlot <- shiny::renderPlot(expr = {
    return(incidentRatePlotDownload())
  }, res = 100)
  
  output$hoverInfoIr <- shiny::renderUI({
    data <- filteredIncidenceRates()
    if (is.null(data)) {
      return(NULL)
    }else {
      hover <- input$plotHoverIr
      point <- nearPoints(data, hover, threshold = 5, maxpoints = 1, addDist = TRUE)
      if (nrow(point) == 0) {
        return(NULL)
      }
      left_px <- hover$coords_css$x
      top_px <- hover$coords_css$y
      
      text <- gsub("-", "<", sprintf("<b>Incidence rate: </b> %0.3f per 1,000 patient years", point$incidenceRate))
      text <- paste(text, sprintf("<b>Cohort count (numerator): </b> %s",  format(point$cohortCount, scientific = FALSE, big.mark = ",")), sep = "<br/>")
      text <- paste(text, sprintf("<b>Person time (denominator): </b> %s years", format(round(point$personYears), scientific = FALSE, big.mark = ",")), sep = "<br/>")
      text <- paste(text, "", sep = "<br/>")
      
      if (!is.na(point$ageGroup)) {
        text <- paste(text, sprintf("<b>Age group: </b> %s years", point$ageGroup), sep = "<br/>")
        top_px <- top_px - 15
      }
      if (!is.na(point$gender)) {
        text <- paste(text, sprintf("<b>Gender: </b> %s", point$gender), sep = "<br/>")
        top_px <- top_px - 15
      }
      if (!is.na(point$calendarYear)) {
        text <- paste(text, sprintf("<b>Calendar year: </b> %s", point$calendarYear), sep = "<br/>")
        top_px <- top_px - 15
      }
      text <- paste(text, sprintf("<b>Database: </b> %s", point$databaseId), sep = "<br/>")
      style <- paste0("position:absolute; z-index:100; background-color: rgba(245, 245, 245, 0.85); ",
                      "left:",
                      left_px - 200,
                      "px; top:",
                      top_px - 170,
                      "px; width:400px;")
      div(
        style = "position: relative; width: 0; height: 0",
        wellPanel(
          style = style,
          p(HTML(text))
        )
      )
    }
  }) 
  
  timeDisPlotDownload <- shiny::reactive({
    data <- timeDistribution[timeDistribution$cohortId == cohortId() & 
                               timeDistribution$databaseId %in% input$databases, ]
    if (nrow(data) == 0) {
      return(NULL)
    }
    data$x <- 1
    plot <- ggplot2::ggplot(data, ggplot2::aes(x = x,
                                               ymin = minValue,
                                               lower = p25Value,
                                               middle = medianValue,
                                               upper = p75Value,
                                               ymax = maxValue)) +
      ggplot2::geom_errorbar(ggplot2::aes(ymin = minValue, ymax = minValue), size = 1) +
      ggplot2::geom_errorbar(ggplot2::aes(ymin = maxValue, ymax = maxValue), size = 1) +
      ggplot2::geom_boxplot(stat = "identity", fill = rgb(0, 0, 0.8, alpha = 0.25), size = 1) +
      ggplot2::facet_grid(databaseId~timeMetric, scale = "free") +
      ggplot2::coord_flip() +
      ggplot2::theme(panel.grid.major.y = ggplot2::element_blank(),
                     panel.grid.minor.y = ggplot2::element_blank(),
                     axis.title.y = ggplot2::element_blank(),
                     axis.ticks.y = ggplot2::element_blank(),
                     axis.text.y = ggplot2::element_blank())
    return(plot)
    
  })
  
  output$timeDisPlot <- shiny::renderPlot(expr = {
    return(timeDisPlotDownload())
  }, res = 100)
  
  output$timeDistTable <- DT::renderDataTable(expr = {
    data <- timeDistribution[timeDistribution$cohortId == cohortId() & 
                               timeDistribution$databaseId %in% input$databases, ]
    if (nrow(data) == 0) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
    }
    columns <- c("timeMetric", "averageValue", "standardDeviation", "minValue", "p10Value", "p25Value", "medianValue", "p75Value", "p90Value", "maxValue")
    headers <- c("Time Measure", "Average", "SD", "Min", "P10", "P25", "Median", "P75", "P90", "Max")
    if (length(unique(data$databaseId)) > 1) {
      columns <- c("databaseId", columns)
      headers <- c("Database", headers)
    }
    table <- data[, columns]
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
                           colnames = headers,
                           filter = c('bottom'),
                           class = "stripe nowrap compact")
    table <- DT::formatRound(table, c("averageValue", "standardDeviation"), digits = 2)
    table <- DT::formatRound(table, c("minValue", "p10Value", "p25Value", "medianValue", "p75Value", "p90Value", "maxValue"), digits = 0)
    return(table)
  }, server = TRUE)
  
  output$includedConceptsTable <- DT::renderDataTable(expr = {
    data <- includedSourceConcept %>% 
      dplyr::filter(.data$cohortId == cohortId() &
                      .data$conceptSetName == input$conceptSet &
                      .data$databaseId %in% input$databases)
    
    if (nrow(data) == 0) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
    }
    
    
    maxConceptSubjects <- max(data$conceptSubjects)
    
    if (input$includedType == "Source Concepts") {
      table <- data %>% 
        dplyr::select(.data$sourceConceptId, .data$sourceVocabularyId, .data$conceptCode, .data$sourceConceptName, .data$conceptSubjects, .data$databaseId) %>% 
        dplyr::group_by(.data$sourceConceptId, .data$sourceVocabularyId, .data$conceptCode, .data$sourceConceptName, .data$databaseId) %>%
        dplyr::summarise(conceptSubjects = sum(.data$conceptSubjects)) %>% #this logic needs to be confirmed
        dplyr::ungroup() %>%
        dplyr::rename(conceptId = "sourceConceptId", vocabularyId = "sourceVocabularyId", conceptName = "sourceConceptName" ) %>% 
        dplyr::arrange(.data$databaseId) %>% 
        tidyr::pivot_wider(id_cols = c("conceptId", "vocabularyId", "conceptCode", "conceptName" ),
                           names_from = "databaseId",
                           values_from = "conceptSubjects",
                           names_sep = "_",
                           values_fill = 0)
      
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
                             colnames = colnames(table) %>% SqlRender::camelCaseToTitleCase(),
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
        tidyr::pivot_wider(id_cols = c("conceptId", "conceptName"),
                           names_from = "databaseId",
                           values_from = "absConceptSubjects",
                           names_sep = "_",
                           values_fill = 0)
      
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
                             colnames = colnames(table) %>% SqlRender::camelCaseToTitleCase(),
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
      dplyr::filter(.data$cohortId == cohortId() &
                      .data$conceptSetName == input$conceptSet &
                      .data$databaseId %in% input$databases)
    
    if (nrow(table) == 0) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
    }
    
    maxConceptCount <- max(table$conceptCount)
    
    table <- table %>% 
      dplyr::select(.data$conceptId, .data$standardConcept, .data$vocabularyId, .data$conceptCode, .data$conceptName, .data$conceptCount, .data$databaseId) %>% 
      dplyr::arrange(conceptCount) %>% 
      dplyr::rename(conceptId = "conceptId", standard = "standardConcept", Vocabulary = "vocabularyId", code = "conceptCode", Name = "conceptName") %>% 
      tidyr::pivot_wider(id_cols = c("conceptId", "standard", "Vocabulary", "code", "Name"),
                         names_from = "databaseId",
                         values_from = "conceptCount",
                         names_sep = "_",
                         values_fill = 0)
    
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
                           colnames = colnames(table) %>% SqlRender::camelCaseToTitleCase(),
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
    table <- inclusionRuleStats[inclusionRuleStats$cohortId == cohortId() & inclusionRuleStats$databaseId == input$database, ]
    table <- table[order(table$ruleSequenceId), ]
    table$cohortId <- NULL
    table$databaseId <- NULL
    if (nrow(table) == 0) {
      return(tidyr::tibble(' ' = paste0('No data available for selected databases and cohorts')))
    }
    lims <- c(0, max(table$remainSubjects))
    table <- table[, c("ruleSequenceId", "ruleName", "meetSubjects", "gainSubjects", "totalSubjects", "remainSubjects")]
    colnames(table) <- c("Sequence", "Name", "Meet", "Gain", "Total", "Remain")
    options = list(pageLength = 20,
                   searching = TRUE,
                   searchHighlight = TRUE,
                   scrollX = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(minCellCountDef(2:5)))
    table <- DT::datatable(table,
                           options = options,
                           rownames = FALSE,
                           escape = FALSE,
                           filter = c('bottom'),
                           class = "stripe nowrap compact")
    table <- DT::formatStyle(table = table,
                             columns = 6,
                             background = DT::styleColorBar(lims, "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  }, server = TRUE)
  
  output$breakdownTable <- DT::renderDataTable(expr = {
    data <- indexEventBreakdown[indexEventBreakdown$cohortId == cohortId() & 
                                  indexEventBreakdown$databaseId %in% input$databases, ]
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
        dplyr::left_join(y = covariate) %>% 
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
            lapply(paste0("(n = ", format(dataCounts$cohortSubjects, big.mark = ","), ")"), th, colspan = 1, class = "dt-center no-padding")
          ),
          tr(
            lapply(rep(c("Proportion"), length(dataCounts$databaseId)), th)
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
        dplyr::left_join(y = covariate %>% dplyr::select(.data$covariateId, .data$covariateName, .data$conceptId) %>% dplyr::distinct()) %>%
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
    temporalCovariateRef <- temporalCovariate %>% 
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
      dplyr::mutate(databaseId = stringr::str_replace_all(string = .data$databaseId, pattern = "_", replacement = " ")) %>% 
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
    covs1 <- dplyr::left_join(x = covs1, y = temporalCovariate)
    covs2 <- dplyr::left_join(x = covs2, y = temporalCovariate)
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
    data <- cohortOverlap[cohortOverlap$targetCohortId == cohortId() & 
                            cohortOverlap$comparatorCohortId == comparatorCohortId() &
                            cohortOverlap$databaseId == input$database, ]
    if (nrow(data) == 0) {
      return(NULL)
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
    data <- cohortOverlap[cohortOverlap$targetCohortId == cohortId() & 
                            cohortOverlap$comparatorCohortId == comparatorCohortId() &
                            cohortOverlap$databaseId == input$database, ]
    if (nrow(data) == 0) {
      return(NULL)
    }
    plot <- VennDiagram::draw.pairwise.venn(area1 = abs(data$eitherSubjects) - abs(data$cOnlySubjects),
                                            area2 = abs(data$eitherSubjects) - abs(data$tOnlySubjects),
                                            cross.area = abs(data$bothSubjects),
                                            category = c("Target", "Comparator"), 
                                            col = c(rgb(0.8, 0, 0), rgb(0, 0, 0.8)),
                                            fill = c(rgb(0.8, 0, 0), rgb(0, 0, 0.8)),
                                            alpha = 0.2,
                                            fontfamily = rep("sans", 3),
                                            cat.fontfamily = rep("sans", 2),
                                            margin = 0.01,
                                            ind = FALSE)
    # Borrowed from https://stackoverflow.com/questions/37239128/how-to-put-comma-in-large-number-of-venndiagram
    idx <- sapply(plot, function(i) grepl("text", i$name))
    for (i in 1:3) {
      plot[idx][[i]]$label <- format(as.numeric(plot[idx][[i]]$label), big.mark = ",", scientific = FALSE)
    }
    grid::grid.draw(plot)
    
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
  
  downloadCohortComparePlot <- shiny::reactive({
    balance <- computeBalance()
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
      ggplot2::scale_x_continuous(input$cohort, limits = c(0, 1)) +
      ggplot2::scale_y_continuous(input$comparator, limits = c(0, 1)) +
      ggplot2::scale_color_gradient("Absolute\nStd. Diff.", low = "blue", high = "red", space = "Lab", na.value = "red")
    return(plot)
  })
  
  output$charComparePlot <- shiny::renderPlot(expr = {
    return(downloadCohortComparePlot())
  }, res = 100)
  
  output$hoverInfoCharComparePlot <- shiny::renderUI({
    balance <- computeBalance()
    balance$mean1[is.na(balance$mean1)] <- 0
    balance$mean2[is.na(balance$mean2)] <- 0
    if (nrow(balance) == 0) {
      return(NULL)
    } else {
      hover <- input$plotHoverCharCompare
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
      dplyr::arrange(.data$cohortFullName)
    return(targetCohortWithCount)
  }) 
  
  targetCohortCountHtml <- shiny::reactive({
    targetCohortCount <- targetCohortCount()
    
    return(htmltools::withTags(
      div(
        h5("Target: ", targetCohortCount$cohortFullName, " ( n = ", scales::comma(x = targetCohortCount$cohortSubjects), " )")
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
            h5("Target: ", targetCohortWithCount$cohortFullName, " ( n = ", scales::comma(targetCohortWithCount$cohortSubjects), " )"),
          ),
          td(HTML("&nbsp;&nbsp;&nbsp;&nbsp;")),
          td(
            h5("Comparator : ", comparatorCohortWithCount$cohortFullName, " ( n = ",scales::comma(comparatorCohortWithCount$cohortSubjects), " )")
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
  
  output$downloadIncidentRatePlot <- download_box("IncidentRate", incidentRatePlotDownload())
  output$timeDistributionPlot <- download_box("TimeDistribution", timeDisPlotDownload())
  output$downloadCompareCohortPlot <- download_box("CompareCohort", downloadCohortComparePlot())
  output$downloadOverlapPlot <- download_box("OverlapPlot", overLapPlot())
  output$downloadTemporalCharacterizationPlot <- download_box("Temporal characterization plot", temporalCharacterizationPlot())
})
