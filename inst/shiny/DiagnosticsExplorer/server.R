library(shiny)
library(shinydashboard)
library(DT)
source("PlotsAndTables.R")

truncScript <- "function(data, type, row, meta) {\n
      return type === 'display' && data != null && data.length > %s ?\n
        '<span title=\"' + data + '\">' + data.substr(0, %s) + '...</span>' : data;\n
     }"

shinyServer(function(input, output, session) {
  
  cohortId <- reactive({
    return(cohort$cohortId[cohort$cohortName == input$cohort])
  })
  
  comparatorCohortId <- reactive({
    return(cohort$cohortId[cohort$cohortName == input$comparator])
  })
  
  observe({
    subset <- unique(conceptSets$conceptSetName[conceptSets$cohortId == cohortId()])
    updateSelectInput(session = session,
                      inputId = "conceptSet",
                      choices = subset)
  })
  
  output$incidenceProportionPlot <- renderPlot({
    data <- incidenceProportion[incidenceProportion$cohortId == cohortId() & 
                                  incidenceProportion$databaseId %in% input$databases, ]
    if (nrow(data) == 0) {
      return(NULL)
    }
    plot <- CohortDiagnostics::plotIncidenceProportion(data)
    return(plot)
  }, res = 100)
  
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
      ggplot2::facet_grid(databaseId~covariateName, scale = "free") +
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
    columns <- c("covariateName", "averageValue", "standardDeviation", "minValue", "p10Value", "p25Value", "medianValue", "p75Value", "p90Value", "maxValue")
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
      table <- table[, c("conceptSubjects", "sourceVocabularyId", "conceptCode", "sourceConceptName")]
      table <- table[order(-table$conceptSubjects), ]
      colnames(table) <- c("Subjects", "Vocabulary", "Code", "Name")
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
                   paging = TRUE)
    table <- datatable(table,
                       options = options,
                       rownames = FALSE,
                       escape = FALSE,
                       class = "stripe nowrap compact")
    table <- formatRound(table, "Subjects", digits = 0)
    table <- DT::formatStyle(table = table,
                             columns = 1,
                             background = DT::styleColorBar(lims, "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  })
  
  output$orphanConceptsTable <- renderDataTable({
    table <- orphanConcept[orphanConcept$cohortId == cohortId() &
                             orphanConcept$conceptSetName == input$conceptSet & 
                             orphanConcept$databaseId == input$database, ]
    table <- table[, c("conceptCount", "standardConcept", "vocabularyId", "conceptCode", "conceptName")]
    table <- table[order(-table$conceptCount), ]
    colnames(table) <- c("Count", "Standard", "Vocabulary", "Code", "Name")
    lims <- c(0, max(table$Count))
    options = list(pageLength = 25,
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE)
    table <- datatable(table,
                       options = options,
                       rownames = FALSE,
                       escape = FALSE,
                       class = "stripe nowrap compact")
    table <- formatRound(table, "Count", digits = 0)
    table <- DT::formatStyle(table = table,
                             columns = 1,
                             background = DT::styleColorBar(lims, "lightblue"),
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
    colnames(table) <- c("Sequence", "Name", "Meet", "Gain", "Total", "Remain")
    options = list(pageLength = 25,
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE)
    table <- datatable(table,
                       options = options,
                       rownames = FALSE,
                       escape = FALSE,
                       class = "stripe nowrap compact")
    table <- formatRound(table, c("Meet", "Gain", "Total", "Remain"), digits = 0)
    return(table)
  })
  
  output$breakdownTable <- renderDataTable({
    table <- indexEventBreakdown[indexEventBreakdown$cohortId == cohortId() & indexEventBreakdown$databaseId == input$database, ]
    table <- table[order(-table$conceptCount), ]
    table <- table[, c("conceptCount", "conceptId", "conceptName")]
    colnames(table) <- c("Count", "Concept ID", "Name")
    lims <- c(0, max(table$Count))
    options = list(pageLength = 25,
                   searching = TRUE,
                   lengthChange = TRUE,
                   ordering = TRUE,
                   paging = TRUE)
    table <- datatable(table,
                       options = options,
                       rownames = FALSE,
                       escape = FALSE,
                       class = "stripe nowrap compact")
    table <- formatRound(table, "Count", digits = 0)
    table <- DT::formatStyle(table = table,
                             columns = 1,
                             background = DT::styleColorBar(lims, "lightblue"),
                             backgroundSize = "98% 88%",
                             backgroundRepeat = "no-repeat",
                             backgroundPosition = "center")
    return(table)
  })
  
  output$characterizationTable <- renderDataTable({
    table <- covariateValue[covariateValue$cohortId == cohortId() & covariateValue$databaseId == input$database, ]
    table <- merge(table, covariate)
    table$cohortId <- NULL
    table$databaseId <- NULL
    
    if (input$charType == "Pretty") {
      table <- prepareTable1(table, output = "HTML")
      options = list(pageLength = 999,
                     searching = FALSE,
                     lengthChange = FALSE,
                     ordering = FALSE,
                     paging = FALSE,
                     columnDefs = list(
                       list(
                         targets = 0,
                         render = JS(sprintf(truncScript, 150, 150))
                       )
                     ))
    } else {
      table <- table[order(table$covariateName), ]
      table <- table[, c("covariateName", "mean", "sd")]
      table$mean <- round(table$mean, 3)
      table$sd <- round(table$sd, 3)
      colnames(table) <- c("Covariate name", "Mean", "SD")
      
      options = list(pageLength = 25,
                     searching = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       list(
                         targets = 0,
                         render = JS(sprintf(truncScript, 150, 150))
                       )
                     )
      )
    }
    table <- datatable(table,
                       options = options,
                       rownames = FALSE,
                       escape = FALSE,
                       class = "stripe nowrap compact")
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
    options = list(pageLength = 7,
                   searching = FALSE,
                   lengthChange = FALSE,
                   ordering = FALSE,
                   paging = FALSE,
                   info = FALSE)
    table <- datatable(table,
                       options = options,
                       rownames = TRUE,
                       class = "stripe nowrap compact")
    table <- formatRound(table, "Value", digits = 0)
    return(table)
  })
  
  output$overlapPlot <- renderPlot({
    data <- cohortOverlap[cohortOverlap$targetCohortId == cohortId() & 
                            cohortOverlap$comparatorCohortId == comparatorCohortId() &
                            cohortOverlap$databaseId == input$database, ]
    if (nrow(data) == 0) {
      return(NULL)
    }
    VennDiagram::draw.pairwise.venn(area1 = data$tOnlySubjects + data$bothSubjects,
                                    area2 = data$cOnlySubjects + data$bothSubjects,
                                    cross.area = data$bothSubjects,
                                    category = c("Target", "Comparator"), 
                                    col = c(rgb(0.8, 0, 0), rgb(0, 0, 0.8)),
                                    fill = c(rgb(0.8, 0, 0), rgb(0, 0, 0.8)),
                                    alpha = 0.2,
                                    fontfamily = rep("sans", 3),
                                    cat.fontfamily = rep("sans", 2),
                                    margin = 0.01)
  }, res = 100)
  
  output$charCompareTable <- renderDataTable({
    covs1 <- covariateValue[covariateValue$cohortId == cohortId() & covariateValue$databaseId == input$database, ]
    covs2 <- covariateValue[covariateValue$cohortId == comparatorCohortId() & covariateValue$databaseId == input$database, ]
    covs1 <- merge(covs1, covariate)
    covs2 <- merge(covs2, covariate)
    balance <- CohortDiagnostics::compareCohortCharacteristics(covs1, covs2)
    
    
    
    if (input$charCompareType == "Pretty") {
      balance <- merge(balance, covariate[, c("covariateId", "covariateAnalysisId")])
      table <- prepareTable1Comp(balance, output = "HTML")
      options = list(pageLength = 999,
                     searching = FALSE,
                     lengthChange = FALSE,
                     ordering = FALSE,
                     paging = FALSE)
    } else {
      table <- balance
      
      lens <- sapply(table$covariateName, function(x) tryCatch(nchar(x), error = function(e) 0), USE.NAMES = FALSE)
      table <- table[order(table$covariateName), ]
      table <- table[, c("covariateName", "mean1", "sd1", "mean2", "sd2", "stdDiff")]
      table$mean1 <- round(table$mean1, 3)
      table$sd1 <- round(table$sd1, 3)
      table$mean2 <- round(table$mean2, 3)
      table$sd2 <- round(table$sd2, 3)
      table$stdDiff <- round(table$stdDiff, 3)
      colnames(table) <- c("Covariate name", "Mean T", "SD T", "Mean C", "SD C", "StdDiff")
      
      options = list(pageLength = 25,
                     searching = TRUE,
                     lengthChange = TRUE,
                     ordering = TRUE,
                     paging = TRUE,
                     columnDefs = list(
                       list(
                         targets = 0,
                         render = JS(sprintf(truncScript, 150, 150))
                       )
                     )
      )
    }
    table <- datatable(table,
                       options = options,
                       rownames = FALSE,
                       escape = FALSE,
                       class = "stripe nowrap compact")
    return(table)
  })
})
