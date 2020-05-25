library(shiny)
library(shinydashboard)
library(DT)
source("PlotsAndTables.R")

truncateStringDef <- function(columns, maxChars) {
  list(
    targets = columns,
    render = JS(sprintf("function(data, type, row, meta) {\n
      return type === 'display' && data != null && data.length > %s ?\n
        '<span title=\"' + data + '\">' + data.substr(0, %s) + '...</span>' : data;\n
     }", maxChars, maxChars))
  )
}

minCellCountDef <- function(columns) {
  list(
    targets = columns,
    render = JS("function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return data.toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
    return '<' + Math.abs(data).toString().replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
  }")
  )
}

minCellPercentDef <- function(columns) {
  list(
    targets = columns,
    render = JS("function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return (100 * data).toFixed(1).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
    return '<' + Math.abs(100 * data).toFixed(1).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,') + '%';
  }")
  )
}

minCellRealDef <- function(columns, digits = 1) {
  list(
    targets = columns,
    render = JS(sprintf("function(data, type) {
    if (type !== 'display' || isNaN(parseFloat(data))) return data;
    if (data >= 0) return data.toFixed(%s).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
    return '<' + Math.abs(data).toFixed(%s).replace(/(\\d)(?=(\\d{3})+(?!\\d))/g, '$1,');
  }", digits, digits))
  )
}

styleAbsColorBar <- function(maxValue, colorPositive, colorNegative, angle = 90) {
  JS(sprintf("isNaN(parseFloat(value))? '' : 'linear-gradient(%fdeg, transparent ' + (%f - Math.abs(value))/%f * 100 + '%%, ' + (value > 0 ? '%s ' : '%s ') + (%f - Math.abs(value))/%f * 100 + '%%)'", 
             angle, maxValue, maxValue, colorPositive, colorNegative, maxValue, maxValue))
}

shinyServer(function(input, output, session) {
  
  cohortId <- reactive({
    return(cohort$cohortId[cohort$cohortFullName == input$cohort])
  })
  
  comparatorCohortId <- reactive({
    return(cohort$cohortId[cohort$cohortFullName == input$comparator])
  })
  
  observe({
    subset <- unique(conceptSets$conceptSetName[conceptSets$cohortId == cohortId()])
    updateSelectInput(session = session,
                      inputId = "conceptSet",
                      choices = subset)
  })
  
  output$cohortCountsTable <- renderDataTable({
    data <- cohortCount[cohortCount$databaseId %in% input$databases, ]
    if (nrow(data) == 0) {
      return(NULL)
    }
    databaseIds <- unique(data$databaseId)
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
    table$cohortId <- NULL
    table$cohortName <- NULL
    
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
    
    options = list(pageLength = 25,
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE,
                   columnDefs = list(minCellCountDef(1:(2*length(databaseIds)))))
    
    dataTable <- datatable(table,
                           options = options,
                           rownames = FALSE,
                           container = sketch, 
                           escape = FALSE,
                           class = "stripe nowrap compact")
    for (i in 1:length(databaseIds)) {
      dataTable <- formatStyle(table = dataTable,
                               columns = i*2,
                               background = styleColorBar(c(0, max(table[, i*2], na.rm = TRUE)), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
      dataTable <- formatStyle(table = dataTable,
                               columns = i*2 + 1,
                               background = styleColorBar(c(0, max(table[, i*2 + 1], na.rm = TRUE)), "#ffd699"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    }
    return(dataTable)
  })
  
  filteredIncidenceRates <- reactive({
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
  
  output$incidenceRatePlot <- renderPlot({
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
  }, res = 100)
  
  output$hoverInfoIr <- renderUI({
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
  
  output$timeDisPlot <- renderPlot({
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
  }, res = 100)
  
  output$timeDistTable <- renderDataTable({
    data <- timeDistribution[timeDistribution$cohortId == cohortId() & 
                               timeDistribution$databaseId %in% input$databases, ]
    if (nrow(data) == 0) {
      return(NULL)
    }
    columns <- c("timeMetric", "averageValue", "standardDeviation", "minValue", "p10Value", "p25Value", "medianValue", "p75Value", "p90Value", "maxValue")
    headers <- c("Time Measure", "Average", "SD", "Min", "P10", "P25", "Median", "P75", "P90", "Max")
    if (length(unique(data$databaseId)) > 1) {
      columns <- c("databaseId", columns)
      headers <- c("Database", headers)
    }
    table <- data[, columns]
    options = list(pageLength = 25,
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   info = TRUE)
    table <- datatable(table,
                       options = options,
                       rownames = FALSE,
                       colnames = headers,
                       class = "stripe nowrap compact")
    table <- formatRound(table, c("averageValue", "standardDeviation"), digits = 2)
    table <- formatRound(table, c("minValue", "p10Value", "p25Value", "medianValue", "p75Value", "p90Value", "maxValue"), digits = 0)
    return(table)
  })
  
  output$includedConceptsTable <- renderDataTable({
    table <- includedSourceConcept[includedSourceConcept$cohortId == cohortId() &
                                     includedSourceConcept$conceptSetName == input$conceptSet & 
                                     includedSourceConcept$databaseId == input$database, ]
    if (input$includedType == "Source Concepts") {
      table <- table[, c("conceptSubjects", "sourceConceptId", "sourceVocabularyId", "conceptCode", "sourceConceptName")]
      table <- table[!is.na(table$sourceConceptName), ]
      table <- table[order(-table$conceptSubjects), ]
      colnames(table) <- c("Subjects", "Concept ID", "Vocabulary", "Code", "Name")
    } else {
      table$absConceptSubjects <- abs(table$conceptSubjects)
      table <- aggregate(absConceptSubjects ~ conceptId + conceptName, data = table, sum)
      table <- table[order(-table$absConceptSubjects), ]
      table <- table[, c("absConceptSubjects", "conceptId", "conceptName")]
      colnames(table) <- c("Subjects", "Concept ID", "Concept Name")
    }
    lims <- c(0, max(table$Subjects))
    options = list(pageLength = 25,
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(minCellCountDef(0)))
    table <- datatable(table,
                       options = options,
                       rownames = FALSE,
                       escape = FALSE,
                       class = "stripe nowrap compact")
    table <- formatStyle(table = table,
                         columns = 1,
                         background = styleColorBar(lims, "lightblue"),
                         backgroundSize = "98% 88%",
                         backgroundRepeat = "no-repeat",
                         backgroundPosition = "center")
    return(table)
  })
  
  output$orphanConceptsTable <- renderDataTable({
    table <- orphanConcept[orphanConcept$cohortId == cohortId() &
                             orphanConcept$conceptSetName == input$conceptSet & 
                             orphanConcept$databaseId == input$database, ]
    if (nrow(table) == 0) {
      return(NULL)
    }
    table <- table[, c("conceptCount", "conceptId", "standardConcept", "vocabularyId", "conceptCode", "conceptName")]
    table <- table[order(-table$conceptCount), ]
    colnames(table) <- c("Count", "Concept ID", "Standard", "Vocabulary", "Code", "Name")
    lims <- c(0, max(table$Count))
    options = list(pageLength = 25,
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(minCellCountDef(0)))
    table <- datatable(table,
                       options = options,
                       rownames = FALSE,
                       escape = FALSE,
                       class = "stripe nowrap compact")
    table <- formatStyle(table = table,
                         columns = 1,
                         background = styleColorBar(lims, "lightblue"),
                         backgroundSize = "98% 88%",
                         backgroundRepeat = "no-repeat",
                         backgroundPosition = "center")
    return(table)
  })
  
  output$inclusionRuleTable <- renderDataTable({
    table <- inclusionRuleStats[inclusionRuleStats$cohortId == cohortId() & inclusionRuleStats$databaseId == input$database, ]
    if (nrow(table) == 0) {
      return(NULL)
    }
    table <- table[order(table$ruleSequenceId), ]
    table$cohortId <- NULL
    table$databaseId <- NULL
    lims <- c(0, max(table$remainSubjects))
    table <- table[, c("ruleSequenceId", "ruleName", "meetSubjects", "gainSubjects", "totalSubjects", "remainSubjects")]
    colnames(table) <- c("Sequence", "Name", "Meet", "Gain", "Total", "Remain")
    options = list(pageLength = 25,
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(minCellCountDef(2:5)))
    table <- datatable(table,
                       options = options,
                       rownames = FALSE,
                       escape = FALSE,
                       class = "stripe nowrap compact")
    table <- formatStyle(table = table,
                         columns = 6,
                         background = styleColorBar(lims, "lightblue"),
                         backgroundSize = "98% 88%",
                         backgroundRepeat = "no-repeat",
                         backgroundPosition = "center")
    return(table)
  })
  
  output$breakdownTable <- renderDataTable({
    data <- indexEventBreakdown[indexEventBreakdown$cohortId == cohortId() & 
                                   indexEventBreakdown$databaseId %in% input$databases, ]
    if (nrow(data) == 0) {
      return(NULL)
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
    options = list(pageLength = 25,
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE,
                   columnDefs = list(minCellCountDef(3:ncol(table) - 1)))
    dataTable <- datatable(table,
                           options = options,
                           rownames = FALSE,
                           escape = FALSE,
                           class = "stripe nowrap compact")
    for (col in 3:ncol(table)) {
      dataTable <- formatStyle(table = dataTable,
                               columns = col,
                               background = styleColorBar(c(0, max(table[, col], na.rm = TRUE)), "lightblue"),
                               backgroundSize = "98% 88%",
                               backgroundRepeat = "no-repeat",
                               backgroundPosition = "center")
    }
    return(dataTable)
  })
  
  output$characterizationTable <- renderDataTable({
    data <- covariateValue[covariateValue$cohortId == cohortId() & covariateValue$databaseId %in% input$databases, ]
    data$cohortId <- NULL
    databaseIds <- unique(data$databaseId)
    
    if (input$charType == "Pretty") {
      data <- merge(data, covariate)
      table <- data[data$databaseId == databaseIds[1], ]
      table <- prepareTable1(table)
      colnames(table)[2] <- paste(colnames(table)[2], databaseIds[1], sep = "_")
      table$order <- 1:nrow(table)
      if (length(databaseIds) > 1) {
        for (i in 2:length(databaseIds)) {
          temp <- data[data$databaseId == databaseIds[i],]
          temp <- prepareTable1(temp)
          colnames(temp)[2] <- paste(colnames(temp)[2], databaseIds[i], sep = "_")
          table <- merge(table, temp, all.x = TRUE)
        }
      }
      table <- table[order(table$order), ]
      table$order <- NULL
      options = list(pageLength = 999,
                     searching = FALSE,
                     lengthChange = FALSE,
                     ordering = FALSE,
                     paging = FALSE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       minCellPercentDef(1:length(databaseIds))
                     ))
      sketch <- htmltools::withTags(table(
        class = 'display',
        thead(
          tr(
            th(rowspan = 2, 'Covariate Name'),
            lapply(databaseIds, th, colspan = 1, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Proportion"), length(databaseIds)), th)
          )
        )
      ))
      table <- datatable(table,
                         options = options,
                         rownames = FALSE,
                         container = sketch, 
                         escape = FALSE,
                         class = "stripe nowrap compact")
      
      table <- formatStyle(table = table,
                           columns = 1 + (1:length(databaseIds)),
                           background = styleColorBar(c(0,1), "lightblue"),
                           backgroundSize = "98% 88%",
                           backgroundRepeat = "no-repeat",
                           backgroundPosition = "center")
    } else {
      table <- data[data$databaseId == databaseIds[1], c("covariateId", "mean", "sd")]
      colnames(table)[2:3] <- paste(colnames(table)[2:3], databaseIds[1], sep = "_")
      if (length(databaseIds) > 1) {
        for (i in 2:length(databaseIds)) {
          temp <- data[data$databaseId == databaseIds[i], c("covariateId", "mean", "sd")]
          colnames(temp)[2:3] <- paste(colnames(temp)[2:3], databaseIds[i], sep = "_")
          table <- merge(table, temp, all = TRUE)
        }
      }
      table <- merge(covariate, table)    
      table$covariateAnalysisId <- NULL
      table$covariateId <- NULL
      table <- table[order(table$covariateName), ]
      options = list(pageLength = 25,
                     searching = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       minCellRealDef(1:(2*length(databaseIds)))
                     )
      )
      sketch <- htmltools::withTags(table(
        class = 'display',
        thead(
          tr(
            th(rowspan = 2, 'Covariate Name'),
            lapply(databaseIds, th, colspan = 2, class = "dt-center")
          ),
          tr(
            lapply(rep(c("Mean", "SD"), length(databaseIds)), th)
          )
        )
      ))
      table <- datatable(table,
                         options = options,
                         rownames = FALSE,
                         container = sketch, 
                         escape = FALSE,
                         class = "stripe nowrap compact")
      table <- formatStyle(table = table,
                           columns = 2*(1:length(databaseIds)),
                           background = styleColorBar(c(0,1), "lightblue"),
                           backgroundSize = "98% 88%",
                           backgroundRepeat = "no-repeat",
                           backgroundPosition = "center")
    }
    return(table)
  })
  
  output$overlapTable <- renderDataTable({
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
                   lengthChange = FALSE,
                   ordering = FALSE,
                   paging = FALSE,
                   info = FALSE,
                   columnDefs = list(minCellCountDef(1)))
    table <- datatable(table,
                       options = options,
                       rownames = TRUE,
                       class = "stripe nowrap compact")
    return(table)
  })
  
  output$overlapPlot <- renderPlot({
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
  }, res = 100)
  
  computeBalance <- reactive({
    if (cohortId() == comparatorCohortId()) {
      return(data.frame())
    }
    covs1 <- covariateValue[covariateValue$cohortId == cohortId() & covariateValue$databaseId == input$database, ]
    covs2 <- covariateValue[covariateValue$cohortId == comparatorCohortId() & covariateValue$databaseId == input$database, ]
    covs1 <- merge(covs1, covariate)
    covs2 <- merge(covs2, covariate)
    balance <- compareCohortCharacteristics(covs1, covs2)
    balance$absStdDiff <- abs(balance$stdDiff)
    return(balance)
  })
  
  output$charCompareTable <- renderDataTable({
    balance <- computeBalance()
    if (nrow(balance) == 0) {
      return(NULL)
    }

    if (input$charCompareType == "Pretty table") {
      balance <- merge(balance, covariate[, c("covariateId", "covariateAnalysisId")])
      table <- prepareTable1Comp(balance)
      options = list(pageLength = 999,
                     searching = FALSE,
                     lengthChange = FALSE,
                     ordering = FALSE,
                     paging = FALSE,
                     columnDefs = list(minCellPercentDef(1:2))
      )
      table <- datatable(table,
                         options = options,
                         rownames = FALSE,
                         escape = FALSE,
                         class = "stripe nowrap compact")
      table <- formatStyle(table = table,
                           columns = 2:3,
                           background = styleColorBar(c(0,1), "lightblue"),
                           backgroundSize = "98% 88%",
                           backgroundRepeat = "no-repeat",
                           backgroundPosition = "center")
      table <- formatStyle(table = table,
                           columns = 4,
                           background = styleAbsColorBar(1, "lightblue", "pink"),
                           backgroundSize = "98% 88%",
                           backgroundRepeat = "no-repeat",
                           backgroundPosition = "center")
      table <- formatRound(table, 4, digits = 2)
    } else {
      table <- balance
      table <- table[order(table$covariateName), ]
      table <- table[, c("covariateName", "mean1", "sd1", "mean2", "sd2", "stdDiff")]
      colnames(table) <- c("Covariate name", "Mean Target", "SD Target", "Mean Comparator", "SD Comparator", "StdDiff")
      
      options = list(pageLength = 25,
                     searching = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       truncateStringDef(0, 150),
                       minCellRealDef(c(1,3), 2)
                     )
      )
      table <- datatable(table,
                         options = options,
                         rownames = FALSE,
                         escape = FALSE,
                         class = "stripe nowrap compact")
      table <- formatStyle(table = table,
                           columns = c(2,4),
                           background = styleColorBar(c(0,1), "lightblue"),
                           backgroundSize = "98% 88%",
                           backgroundRepeat = "no-repeat",
                           backgroundPosition = "center")
      table <- formatStyle(table = table,
                           columns = 6,
                           background = styleAbsColorBar(1, "lightblue", "pink"),
                           backgroundSize = "98% 88%",
                           backgroundRepeat = "no-repeat",
                           backgroundPosition = "center")
      table <- formatRound(table, c(3, 5, 6), digits = 2)
    }
    return(table)
  })
  
  output$charComparePlot <- renderPlot({
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
      ggplot2::scale_x_continuous("Mean Target", limits = c(0, 1)) +
      ggplot2::scale_y_continuous("Mean Comparator", limits = c(0, 1)) +
      ggplot2::scale_color_gradient("Absolute\nStd. Diff.", low = "blue", high = "red", space = "Lab", na.value = "red")
    return(plot)
  }, res = 100)
  
  output$hoverInfoCharComparePlot <- renderUI({
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
  
  output$databaseInformationTable <- renderDataTable({

    table <- database[, c("databaseId", "databaseName", "description")]
    options = list(pageLength = 25,
                   searching = TRUE,
                   lengthChange = FALSE,
                   ordering = TRUE,
                   paging = FALSE,
                   columnDefs = list(list(width = '30%', targets = 1),
                                     list(width = '60%', targets = 2))
    )
    table <- datatable(table,
                       options = options,
                       colnames = c("ID", "Name", "Description"),
                       rownames = FALSE,
                       class = "stripe compact")
    return(table)
  })

  showInfoBox <- function(title, htmlFileName) {
    showModal(modalDialog(
      title = title,
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      HTML(readChar(htmlFileName, file.info(htmlFileName)$size) )
    ))
  }
  
  observeEvent(input$cohortCountsInfo, {
    showInfoBox("Cohort Counts", "html/cohortCounts.html")
  })
  
  observeEvent(input$incidenceRateInfo, {
    showInfoBox("Incidence Rate", "html/incidenceRate.html")
  })

  observeEvent(input$timeDistributionInfo, {
    showInfoBox("Time Distributions", "html/timeDistribution.html")
  })
  
  observeEvent(input$includedConceptsInfo, {
    showInfoBox("Included (Source) Concepts", "html/includedConcepts.html")
  })
  
  observeEvent(input$orphanConceptsInfo, {
    showInfoBox("Orphan (Source) Concepts", "html/orphanConcepts.html")
  })
  
  observeEvent(input$inclusionRuleStatsInfo, {
    showInfoBox("Inclusion Rule Statistics", "html/inclusionRuleStats.html")
  })
  
  observeEvent(input$indexEventBreakdownInfo, {
    showInfoBox("Index Event Breakdown", "html/indexEventBreakdown.html")
  })
  
  observeEvent(input$cohortCharacterizationInfo, {
    showInfoBox("Cohort Characterization", "html/cohortCharacterization.html")
  })
  
  observeEvent(input$cohortOverlapInfo, {
    showInfoBox("Cohort Overlap", "html/cohortOverlap.html")
  })
  
  observeEvent(input$compareCohortCharacterizationInfo, {
    showInfoBox("Compare Cohort Characteristics", "html/compareCohortCharacterization.html")
  })
})
