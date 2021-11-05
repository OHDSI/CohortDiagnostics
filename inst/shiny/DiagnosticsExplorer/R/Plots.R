#!!!!!! make plotTimeSeries generic enough that it maybe used every where there is time series
###!! given an input tsibble, it should be smart enough to plot
# library(plotly)
plotTimeSeriesFromTsibble <-
  function(tsibbleData,
           plotFilters,
           indexAggregationType = "Monthly",
           timeSeriesPeriodRangeFilter = c(2010, 2021)) {
    if (is.null(data)) {
      return(NULL)
    }
    
    distinctCohortShortName <- c()
    for (i in 1:length(tsibbleData)) {
      data  <- tsibbleData[[i]]$cohortShortName  %>% unique()
      distinctCohortShortName <-
        union(distinctCohortShortName, data)
    }
    distinctCohortCompoundName <- cohort %>% 
      dplyr::filter(.data$shortName %in% distinctCohortShortName) %>% 
      dplyr::mutate(compoundName = ifelse(stringr::str_length(.data$compoundName) > 40, 
                                          paste0(substr(.data$compoundName,0,40),"\n",substr(.data$compoundName,40,stringr::str_length(.data$compoundName))),
                                          .data$compoundName)) %>% 
      dplyr::pull(.data$compoundName) %>% 
      paste(collapse = "\n")
    
    distinctDatabaseShortName <- c()
    for (i in 1:length(tsibbleData)) {
      tsibbleData[[i]] <- tsibbleData[[i]] %>% 
        dplyr::inner_join(database %>% 
                            dplyr::select(.data$databaseId, .data$shortName) %>% 
                            dplyr::rename("databaseShortName" = .data$shortName), by = "databaseId")
      data <- tsibbleData[[i]] %>% 
        dplyr::pull(.data$databaseShortName) %>% 
        unique()
      distinctDatabaseShortName <- union(distinctDatabaseShortName, data)
    }
    distinctDatabaseCompoundName <- database %>% 
      dplyr::filter(.data$shortName %in% distinctDatabaseShortName) %>% 
      dplyr::pull(.data$compoundName) %>% 
      paste(collapse = "\n")
    
    cohortPlots <- list()
    noOfPlotRows <-
      length(distinctCohortShortName) * length(plotFilters)
    for (i in 1:length(distinctCohortShortName)) {
      filterPlots <- list()
      for (j in 1:length(plotFilters)) {
        data <-
          tsibbleData[[j]] %>% dplyr::filter(.data$cohortShortName == distinctCohortShortName[i])
        databasePlots <- list()
        for (k in 1:length(distinctDatabaseShortName)) {
            
          databasePlots[[k]] <- plotTs(
            data = data %>% dplyr::filter(.data$databaseShortName == distinctDatabaseShortName[k]),
            plotHeight =  1000,
            xAxisMin = as.Date(paste0(timeSeriesPeriodRangeFilter[1], "-01-01")),
            xAxisMax = as.Date(paste0(timeSeriesPeriodRangeFilter[2], "-12-31")),
            valueType = plotFilters[[j]]
          )
          if (j == 1 && i == 1) {
            databasePlots[[k]] <- databasePlots[[k]] %>%
              plotly::layout(
                annotations = list(
                  x = 0.5 ,
                  y = 1.2,
                  text = distinctDatabaseShortName[[k]],
                  showarrow = F,
                  xref = 'paper',
                  yref = 'paper',
                  xanchor = 'center',
                  yanchor = 'middle'
                )
              )
          }
          if (j != length(plotFilters) || i != length(distinctCohortShortName)) {
            databasePlots[[k]] <-
              databasePlots[[k]] %>% plotly::layout(xaxis = list(showticklabels = FALSE))
          }
          
          # databasePlots <-
          #   lapply(distinctDatabaseId, function(singleDatabaseId) {
          #     filteredData <-
          #       data %>% dplyr::filter(.data$databaseId == singleDatabaseId)
          #     plot <-
          #       plotly::plot_ly(
          #         filteredData,
          #         x = ~ periodBegin,
          #         y = as.formula(paste0("~", timeSeriesStatistics[k])),
          #         height = 150 * noOfPlotRows
          #       ) %>%
          #       plotly::add_lines(
          #         name = singleDatabaseId,
          #         text = ~ paste(
          #           "Statistics = ",
          #           timeSeriesStatistics[k],
          #           "\nDatabase ID = ",
          #           .data$databaseId,
          #           "\nCohort = ",
          #           .data$cohortShortName
          #         )
          #       ) %>%
          #       plotly::layout(showlegend = FALSE, xaxis = list(range = c(
          #         as.Date(paste(
          #           timeSeriesPeriodRangeFilter[1], 1, 1, sep = "-"
          #         )),
          #         as.Date(paste(
          #           timeSeriesPeriodRangeFilter[1], 1, 1, sep = "-"
          #         ))
          #       )))
          #     if (i == 1 && j == 1 && k == 1) {
          #       plot <- plot %>%
          #         plotly::layout(
          #           annotations = list(
          #             x = 0.5 ,
          #             y = 1.2,
          #             text = singleDatabaseId,
          #             showarrow = F,
          #             xref = 'paper',
          #             yref = 'paper'
          #           )
          #         )
          #     }
          #     # X axis should be shown only for last row plots
          #     if (i != length(distinctCohortShortName) ||
          #         j != length(plotFilters) ||
          #         k != length(timeSeriesStatistics)) {
          #       plot <-
          #         plot %>% plotly::layout(xaxis = list(showticklabels = FALSE))
          #     }
          #     return(plot)
          #   })
          # statisticsPlot <-
          #   plotly::subplot(databasePlots, shareY = TRUE, titleX = FALSE) %>%
          #   plotly::layout(
          #     annotations = list(
          #       x = -0.05,
          #       y = 0.5,
          #       text = camelCaseToTitleCase(timeSeriesStatistics[k]),
          #       showarrow = FALSE,
          #       xref = "paper",
          #       yref = "paper",
          #       textangle = -90
          #     )
          #   )
          # 
          # statisticsPlots[[k]] <- statisticsPlot
        }
        filterPlots[[j]] <-
          plotly::subplot(databasePlots,shareY = TRUE) %>%
          plotly::layout(
            annotations = list(
              x = 0.1,
              y = 1.0,
              text = camelCaseToTitleCase(plotFilters[j]),
              showarrow = FALSE,
              xref = "paper",
              yref = "paper",
              xanchor = 'center',
              yanchor = 'middle'
            )
          )
        
      }
      cohortPlots[[i]] <-
        plotly::subplot(filterPlots, nrows = length(filterPlots)) %>%
        plotly::layout(
          annotations = list(
            x = -0.07,
            y = 0.5,
            text = camelCaseToTitleCase(distinctCohortShortName[i]),
            showarrow = FALSE,
            xref = "paper",
            yref = "paper",
            xanchor = 'center',
            yanchor = 'middle',
            textangle = -90
          )
        )
    }
    m <- list(
      l = 50,
      r = 0,
      b = 200,
      t = 50,
      pad = 4
    )
    finalPlot <-
      plotly::subplot(cohortPlots, nrows = length(cohortPlots)) %>%
      plotly::layout(autosize = T, 
                     margin = m,
                     annotations = list(
                       x = c(0.3,0.7) ,
                       y = c(-0.15,-0.15),
                       align = "left",
                       text = c(paste0("<b>Cohorts :</b>\n",distinctCohortCompoundName),paste0("<b>Datasouce :</b>\n",distinctDatabaseCompoundName)),
                       showarrow = F,
                       xref = 'paper',
                       yref = 'paper',
                       xanchor = 'center',
                       yanchor = 'middle'
                     ))
    
    # # Using Plotly
    
    #  for (i in 1:length(timeSeriesStatistics)) {
    #    cohortPlots <- list()
    #    for (j in 1:length(distinctCohortShortName)) {
    #      data <- tsibbleData %>% dplyr::filter(.data$cohortShortName == distinctCohortShortName[j])
    #      databasePlots <- lapply(distinctDatabaseId, function(var3) {
    #        filteredData <- data %>% dplyr::filter(.data$databaseId == var3)
    #        plot <- plotly::plot_ly(filteredData, x = ~periodBegin, y = as.formula(paste0("~", timeSeriesStatistics[i]))) %>%
    #          plotly::add_lines(name = var3, text = ~paste("\nStatistics = ",timeSeriesStatistics[i]) ) %>%
    #          plotly::layout(showlegend = FALSE)
    #        if (i == 1 && j == 1) {
    #          plot <- plot %>%
    #            plotly::layout(annotations = list(x = 0.5 , y = 1.1, text = var3, showarrow = F,
    #                                      xref = 'paper', yref = 'paper'))
    #        }
    #        # X axis should be shown only for last row plots
    #       if (i != length(timeSeriesStatistics) || j != length(distinctCohortShortName)) {
    #          plot <- plot %>% plotly::layout(xaxis = list(showticklabels = FALSE))
    #        }
    #        return(plot)
    #      })
    #      cohortPlot <- plotly::subplot(databasePlots, shareY = TRUE, titleX = FALSE) %>%
    #        plotly::layout(annotations = list(x = 0.0,
    #                                          y = 0.5,
    #                                          text = distinctCohortShortName[j],
    #                                          showarrow = FALSE,
    #                                          xref = "paper",
    #                                          yref = "paper",
    #                                          textangle = -90,
    #                                          bordercolor = "rgb(200, 0, 25)"))
    #
    #      cohortPlots[[j]] <- cohortPlot
    #    }
    #    plots[[i]] <- plotly::subplot(cohortPlots,nrows = length(cohortPlots)) %>%
    #      plotly::layout(annotations = list(x = -0.04,
    #                                        y = 0.5,
    #                                        text = timeSeriesStatistics[i],
    #                                        showarrow = FALSE,
    #                                        xref = "paper",
    #                                        yref = "paper",
    #                                       textangle = -90,
    #                                       bordercolor = "rgb(150, 150, 150)"))
    #
    #      # plotly::layout(annotations = list(x = -0.05 , y = 0.5, text = timeSeriesStatistics[i], showarrow = F,
    #      #                                   xref = 'paper', yref = 'paper'))
    #  }
    # finalPlot <- plotly::subplot(plots,nrows = length(plots))
    
    
    
    # USING ggiraph and ggplot2
    # tsibbleData1 <- tsibbleData
    # if aggregationPeriod is 'Year' then STL will not return 'season_year'
    # if (indexAggregationType == "Yearly") {
    #   pivotBy <- c("Total", "trend", "remainder")
    # } else {
    #   pivotBy <- c("Total", "trend", "season_year", "remainder")
    # }
    #
    # data <- tsibbleData %>%
    #   tidyr::pivot_longer(cols = pivotBy ,
    #                       names_to = "fieldName",
    #                       values_to = "fieldValues")
    
    # aesthetics <-
    #   list(
    #     x = "periodBegin",
    #     y = "fieldValues",
    #     group = "fieldName",
    #     color = "fieldName",
    #     text = "tooltip"
    #   )
    # #!!!!!!!!!!!!! dummy name if no cohort
    # if (is.null(data$cohortShortName)) {
    #   data$cohortShortName <- "cohort"
    # }
    #
    # data$tooltip <- c(
    #   paste0(
    #     # data$fieldName,
    #     # " = ",
    #     # data$fieldValues,
    #     # "\nPeriod Begin = ",
    #     # data$periodBegin,
    #     "Database ID = ",
    #     data$databaseId
    #     ,
    #     "\nCohort = ",
    #     data$cohortShortName
    #   )
    # )
    #
    # # Filtering by Decomposition plot category
    # data <- data[data$fieldName %in% timeSeriesStatistics, ]
    #
    # distinctDatabaseId <- data$databaseId %>% unique()
    # distinctCohortShortName <- data$cohortShortName %>% unique()
    # xAxisFontSize <- 18
    # if (length(distinctDatabaseId)* length(distinctCohortShortName) >= 30) {
    #   xAxisFontSize <- 12
    # }
    #
    # plot <-
    #   ggplot2::ggplot(data = data, do.call(ggplot2::aes_string, aesthetics)) +
    #   ggplot2::theme_bw() +
    #   # ggplot2::geom_line()+
    #   # ggplot2::geom_point()+
    #   ggiraph::geom_line_interactive(ggplot2::aes(), size = 0.5, alpha = 0.6) +
    #   ggiraph::geom_point_interactive(ggplot2::aes(tooltip = tooltip),
    #                                   size = 0.7,
    #                                   alpha = 0.9) +
    #   ggplot2::labs(x = "Period Begin", y = yAxisLabel) +
    #   ggplot2::scale_y_continuous(labels = scales::comma) +
    #   ggplot2::theme(legend.position = "none") +
    #   facet_nested(factor(
    #     fieldName,
    #     levels = c("Total", "trend", "season_year", "remainder")
    #   ) ~ databaseId + cohortShortName,
    #   scales = "free_y") +
    #   ggplot2::theme(
    #     strip.text = ggplot2::element_text(size = 18),
    #     axis.text.y = ggplot2::element_text(size = 18),
    #     plot.title = ggplot2::element_text(size = 20),
    #     plot.subtitle =  ggplot2::element_text(size = 20),
    #     axis.title = ggplot2::element_text(size = 18),
    #     axis.text.x = ggplot2::element_text(angle = 90, vjust = 0.5,size = xAxisFontSize),
    #     panel.border = ggplot2::element_blank(),
    #     strip.placement = "outside",
    #     strip.background.x = ggplot2::element_rect(color = "black",fill = "white"),
    #     strip.background.y = ggplot2::element_blank()
    #   )
    #
    # spacing <- data %>%
    #   dplyr::distinct(.data$databaseId, .data$shortName) %>%
    #   dplyr::arrange(.data$databaseId) %>%
    #   dplyr::group_by(.data$databaseId) %>%
    #   dplyr::summarise(count = dplyr::n()) %>%
    #   dplyr::ungroup()
    # spacing <-
    #   unlist(sapply(spacing$count, function(x)
    #     c(1, rep(0.5, x - 1))))[-1]
    #
    # if (length(spacing) > 0) {
    #   plot <-
    #     plot + ggplot2::theme(
    #       panel.spacing.y = ggplot2::unit(spacing[1], "lines")
    #     )
    # }
    
    return(finalPlot)
  }

#make plotTs a separate standalone function that takes dataframe with required fields databaaseId, periodDate, Total and trend - and plots it
#!!!!!return a plot with overlay of Total and trend - currently only showing Total
#!!!! should be used everywhere else
plotTs <- function(data,
                   plotHeight,
                   xAxisMin,
                   xAxisMax,
                   valueType = "records") {
  plot <-
    plotly::plot_ly(
      data = data,
      height = plotHeight
    ) %>%
    plotly::add_markers(x = ~ periodDate,
                      y = ~ Total,
                      alpha = 0.1, 
                      size = I(20),
                      color = I("grey"),
                      name = "Total",
                      text = ~ paste("Database ID = ",.data$databaseId,
                                     "\nvalueType = ",valueType )) %>%
    plotly::add_trace(x = ~ periodDate,
                      y = ~ trend,
                      mode = "line",
                      type = "scatter",
                      name = "Trends",
                      text = ~ paste("Database ID = ",.data$databaseId,
                                     "\nvalueType = ",valueType )) %>%
    plotly::layout(showlegend = FALSE,
                   xaxis = list(tickangle = -90, range = c(xAxisMin, xAxisMax), showspikes = showPlotSpikes),
                   yaxis = list(tickformat = ifelse((valueType == "recordsProportion" || valueType == "personsProportion"),".2%",",d"), showspikes = showPlotSpikes))
  
  
  
  return(plot)
}


plotTimeSeriesForCohortDefinitionFromTsibble <-
  function(stlModeledTsibbleData,
           timeSeriesPeriodRangeFilter = c(2010, 2021),
           conceptId = NULL,
           conceptName = NULL,
           conceptSynonym = NULL) {
    if (is.null(stlModeledTsibbleData)) {
      return(NULL)
    }
    yAxisValues <- names(stlModeledTsibbleData)
    distinctDatabaseShortName <- c()
    distinctDatabaseCompoundName <- c()
    for (i in 1:length(yAxisValues)) {
      if ("dcmp_ts" %in% class(stlModeledTsibbleData[[i]])) {
        stlModeledTsibbleData[[i]] <- stlModeledTsibbleData[[i]] %>% 
          dplyr::inner_join(database %>% 
                              dplyr::select(.data$databaseId, .data$shortName) %>% 
                              dplyr::rename("databaseShortName" = .data$shortName), by = "databaseId") %>% 
          dplyr::mutate(databaseCompoundName = paste(.data$databaseShortName," - ",.data$databaseId))
        distinctDatabaseCompoundName <- c(distinctDatabaseCompoundName,
                                       stlModeledTsibbleData[[i]]$databaseCompoundName) %>% unique()
        distinctDatabaseShortName <- c(distinctDatabaseShortName,
                                stlModeledTsibbleData[[i]]$databaseShortName) %>% unique()
      }
    }
    distinctDatabaseCompoundNameString = paste(distinctDatabaseCompoundName,collapse = ";")
    
    noOfPlotRows <- length(yAxisValues)
    yAxisValuesPlots <- list()
    for (j in 1:length(yAxisValues)) {
      data <- stlModeledTsibbleData[[j]]
      databasePlots <- list()
      for (l in (1:length(distinctDatabaseShortName))) {
        databasePlots[[l]] <- plotTs(
          data = data %>%
            dplyr::filter(.data$databaseShortName %in% distinctDatabaseShortName[[l]]),
          plotHeight =  200 * noOfPlotRows,
          xAxisMin = as.Date(paste0(timeSeriesPeriodRangeFilter[1], "-01-01")),
          xAxisMax = as.Date(paste0(timeSeriesPeriodRangeFilter[2], "-12-31")),
          valueType = yAxisValues[[j]]
        ) 
        
        if (j == 1) {
          databasePlots[[l]] <- databasePlots[[l]] %>%
            plotly::layout(
              annotations = list(
                x = 0.5 ,
                y = 1.2,
                text = distinctDatabaseShortName[[l]],
                showarrow = F,
                xref = 'paper',
                yref = 'paper',
                xanchor = 'center',
                yanchor = 'middle'
              )
            )
        }
        if (j != length(yAxisValues)) {
          databasePlots[[l]] <-
            databasePlots[[l]] %>% plotly::layout(xaxis = list(showticklabels = FALSE))
        }
       
      }
      yAxisValuesPlots[[yAxisValues[[j]]]] <- plotly::subplot(databasePlots, shareY = TRUE, titleX = FALSE) %>% 
        plotly::layout(
          annotations = list(
            x = 0.1,
            y = 1.0,
            text = camelCaseToTitleCase(yAxisValues[[j]]),
            showarrow = FALSE,
            xref = "paper",
            yref = "paper",
            xanchor = 'center',
            yanchor = 'middle'
          )
        )
    }
    m <- list(
      l = 80,
      r = 0,
      b = 100,
      t = 50,
      pad = 4
    )
    finalPlot <-
      plotly::subplot(yAxisValuesPlots, nrows = length(yAxisValuesPlots)) %>%
      plotly::layout(autosize = T,
                     margin = m,
                     annotations = list(
                       x = 0.5 ,
                       y = -0.11,
                       text = paste0("<b>Concept: </b>",conceptName," (",conceptId,")","\n<b>Datasource: </b>",distinctDatabaseCompoundNameString),
                       showarrow = F,
                       xref = 'paper',
                       yref = 'paper',
                       xanchor = 'center',
                       yanchor = 'middle'
                     ))
    
    #!!! add database Id to x-axis 
    #!!! show both Total and trend in plotTs
    #!!! show y - labels -- value of yAxisValues
    return(finalPlot)
  }


plotTimeDistribution <- function(data, shortNameRef = NULL) {
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(
    x = data,
    any.missing = FALSE,
    min.rows = 1,
    min.cols = 5,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertNames(
    x = colnames(data),
    must.include = c(
      "minValue",
      "p25Value",
      "medianValue",
      "p75Value",
      "maxValue"
    ),
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  
  initialColor <- colorReference %>% 
    dplyr::filter(.data$type == "database",.data$name == "database") %>% 
    dplyr::pull(.data$value)
  
  selectedColors <- colorRampPalette(c("#000000",initialColor, "#FFFFFF"))(length(data$databaseId %>% unique()) + 2) %>% 
    head(-1) %>% 
    tail(-1)
  

  plotData <- data %>% 
    dplyr::inner_join(cohort %>% 
                        dplyr::select(.data$cohortId, .data$shortName),
                      by = "cohortId") %>% 
    dplyr::inner_join(
      database %>%
        dplyr::select(.data$databaseId, .data$shortName) %>%
        dplyr::rename("databaseShortName" = .data$shortName),
      by = "databaseId"
    ) %>% 
    dplyr::inner_join(
      data %>% 
        dplyr::select(.data$databaseId) %>% 
        unique() %>% 
        dplyr::mutate(color = selectedColors), by = "databaseId"
    )
    
  
  sortShortName <- plotData %>%
    dplyr::select(.data$shortName) %>%
    dplyr::distinct() %>%
    dplyr::arrange(as.integer(sub(
      pattern = '^C', '', x = .data$shortName
    )))
  
  plotData <- plotData %>%
    dplyr::arrange(shortName = factor(.data$shortName, levels = sortShortName$shortName),
                   .data$shortName)
  xAxisMin <- plotData$minValue %>% min()
  xAxisMax <- plotData$maxValue %>% max()
  
  plotData$shortName <- factor(plotData$shortName,
                               levels = sortShortName$shortName)
  
  distinctDatabaseShortName <- plotData$databaseShortName %>% unique()
  distinctTimeMetric <- c("observation time (days) prior to index", "time (days) between cohort start and end","observation time (days) after index")
  
  distinctCohortCompoundName <- cohort %>% 
    dplyr::filter(.data$shortName %in% sortShortName$shortName) %>% 
    dplyr::pull(.data$compoundName) %>% 
    paste(collapse = "\n")
  
  distinctDatabaseCompoundName <- database %>% 
    dplyr::filter(.data$shortName %in% distinctDatabaseShortName) %>% 
    dplyr::pull(.data$compoundName) %>% 
    paste(collapse = "\n")
  
 # plotHeight <- 200 + length(distinctDatabaseShortName) * length(sortShortName$shortName) * 100
  plotHeight <- 800
  databasePlots <- list()
  for (i in 1:length(distinctDatabaseShortName)) {
    filteredDataByDatabase <- plotData %>%
      dplyr::filter(.data$databaseShortName == distinctDatabaseShortName[i])
    timeMetricPlots  <- list()
    for (j in 1:length(distinctTimeMetric)) {
      filteredDataByTimeMetric <- filteredDataByDatabase %>%
        dplyr::filter(.data$timeMetric == distinctTimeMetric[j])
      cohortPlots <- plotly::plot_ly(height = plotHeight)
      for (k in 1:length(sortShortName$shortName)) {
        rowData <-  filteredDataByTimeMetric %>%
          dplyr::filter((.data$shortName == sortShortName$shortName[k]))
        selectedRowdata <- c(
            ifelse(length(rowData$minValue) > 0,rowData$minValue,''),
            ifelse(length(rowData$p25Value) > 0,rowData$p25Value,''),
            ifelse(length(rowData$medianValue) > 0,rowData$medianValue,''),
            ifelse(length(rowData$p75Value) > 0,rowData$p75Value,''),
            ifelse(length(rowData$maxValue) > 0,rowData$maxValue,'')
          )
        cohortPlots <- cohortPlots %>% 
          plotly::add_boxplot(x = selectedRowdata,
                              name = sortShortName$shortName[k],
                              color = I(rowData$color),
                              boxpoints = FALSE,
                              text = ~paste0(
                                rowData$shortName,
                                "\nDatabase = ",
                                rowData$databaseId,
                                "\nMin = ",
                                scales::comma(rowData$minValue, accuracy = 1),
                                "\nP25 = ",
                                scales::comma(rowData$p25Value, accuracy = 1),
                                "\nMedian = ",
                                scales::comma(rowData$medianValue, accuracy = 1),
                                "\nP75 = ",
                                scales::comma(rowData$p75Value, accuracy = 1),
                                "\nMax = ",
                                scales::comma(rowData$maxValue, accuracy = 1),
                                "\nTime Measure = ",
                                rowData$timeMetric,
                                "\nAverage = ",
                                scales::comma(x = rowData$averageValue, accuracy = 0.01)
                              ))
      }
      
      cohortPlots <- cohortPlots %>%
        plotly::layout(
        showlegend = FALSE,
        xaxis = list(range = c(xAxisMin, xAxisMax), tickformat = ",d", showspikes = showPlotSpikes),
        yaxis = list(showspikes = showPlotSpikes)
      )
      
      if (i != length(distinctDatabaseShortName) && length(distinctDatabaseShortName) < 3) {
        cohortPlots <-
          cohortPlots %>% plotly::layout(xaxis = list(showticklabels = FALSE))
      }
     
      if (i == 1) {
        cohortPlots <- cohortPlots %>%
          plotly::layout(
            annotations = list(
              x = 0.5 + (j - (length(
                distinctTimeMetric
              ) + 1) / 2) * 0.3,
              y = 1.1,
              text = camelCaseToTitleCase(distinctTimeMetric[[j]]),
              showarrow = FALSE,
              xref = "paper",
              yref = "paper"
            )
          )
      }
      timeMetricPlots[[j]] <- cohortPlots
    }
    databasePlots[[i]] <-
      plotly::subplot(timeMetricPlots) %>%
      plotly::layout(
        annotations = list(
          x = 1.02,
          y = 0.5,
          text = camelCaseToTitleCase(distinctDatabaseShortName[i]),
          showarrow = FALSE,
          xref = "paper",
          yref = "paper",
          xanchor = "right",
          yanchor = "middle",
          textangle = 90,
          font = list(size = 18)
        )
      )
  }
  
  m <- list(
    l = 50,
    r = 50,
    b = 200,
    t = 70
  )
  finalPlot <-
    plotly::subplot(databasePlots, nrows = length(databasePlots), margin = 0.008) %>% 
    plotly::layout(margin = m,
                   annotations = list(
                     x = c(0.3,0.7) ,
                     y = c(-0.2,-0.2),
                     text = c(paste0("<b>Cohorts :</b>\n",distinctCohortCompoundName),paste0("<b>DataSource :</b>\n",distinctDatabaseCompoundName)),
                     showarrow = F,
                     xref = 'paper',
                     yref = 'paper',
                     align = 'left',
                     xanchor = 'center',
                     yanchor = 'middle'
                   ))
  
  return(finalPlot)
}




plotIndexEventBreakdown <- function(data, yAxis = "conceptCount") {
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(
    x = data,
    any.missing = FALSE,
    min.rows = 1,
    min.cols = 5,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  
  # initialColor <- colorReference %>% 
  #   dplyr::filter(.data$type == "database",.data$name == "database") %>% 
  #   dplyr::pull(.data$value)
  # 
  # selectedColors <- colorRampPalette(c("#000000",initialColor, "#FFFFFF"))(length(data$databaseId %>% unique()) + 2) %>% 
  #   head(-1) %>% 
  #   tail(-1)
  
  data <- data %>% 
    dplyr::mutate(count = .data[[yAxis]]) %>% 
    dplyr::select(-.data$conceptCount,-.data$subjectCount)
  
  
  plotData <- data %>% 
    dplyr::inner_join(cohort %>% 
                        dplyr::select(.data$cohortId, .data$shortName, .data$compoundName),
                      by = "cohortId") %>% 
    dplyr::inner_join(
      database %>%
        dplyr::select(.data$databaseId, .data$shortName, .data$compoundName) %>%
        dplyr::rename("databaseShortName" = .data$shortName, "databaseCompoundName" = .data$compoundName),
      by = "databaseId"
    ) 
  
  distinctCohortShortName <- plotData$shortName %>% unique()
  distinctDatabaseShortName <- plotData$databaseShortName %>%  unique()
  disctinctConceptIds <- plotData$conceptId %>%  unique()
  distinctCohortCompoundName <- plotData$compoundName %>% unique()
  distinctDatabaseCompoundName <- plotData$databaseCompoundName %>%  unique()
  
  # plotHeight <- 200 + length(distinctDatabaseShortName) * length(sortShortName$shortName) * 100
  plotHeight <- 800

  cohortPlots  <- list()
  for (i in 1:length(distinctCohortShortName)) {
    filteredDataByCohort <- plotData %>%
      dplyr::filter(.data$shortName == distinctCohortShortName[i])
      
    databasePlots <- list()
    for (j in 1:length(distinctDatabaseShortName)) {
      filteredDataByDatabase <- filteredDataByCohort %>%
        dplyr::filter(.data$databaseShortName == distinctDatabaseShortName[j])
      
      databasePlot <-  
          plotly::plot_ly(
            filteredDataByDatabase,
            x = ~daysRelativeIndex,
            y = ~count,
            name = ~conceptId,
            type = 'scatter',
            mode = 'lines',
            text = ~paste0(
              "Cohort =",
              compoundName,
              "\nDatabase = ",
              databaseId,
              "\nDays Relative Index = ",
              daysRelativeIndex,
              "\nCount = ",
              count
            ),
            showlegend = FALSE,
            height = plotHeight
          ) %>% 
          plotly::layout(
            legend = list(orientation = "h",   # show entries horizontally
                          xanchor = "center",  # use center of legend as anchor
                          x = 0.5),
            xaxis = list(range = c(-30, 30), tickformat = ",d", showspikes = showPlotSpikes),
            yaxis = list(showspikes = showPlotSpikes, type = "log")
          )
        
      
      if (i == 1) {
        databasePlot <- databasePlot %>%
          plotly::layout(
            annotations = list(
              x = 0.5,
              y = 1.05,
              text = camelCaseToTitleCase(distinctDatabaseShortName[[j]]),
              showarrow = FALSE,
              xref = "paper",
              yref = "paper",
              xanchor = "center",
              yanchor = "middle"
            )
          )
      }
      databasePlots[[j]] <- databasePlot
    }
    cohortPlots[[i]] <-
      plotly::subplot(databasePlots) %>%
      plotly::layout(
        annotations = list(
          x = -0.02,
          y = 0.5,
          text = camelCaseToTitleCase(distinctCohortShortName[i]),
          showarrow = FALSE,
          xref = "paper",
          yref = "paper",
          xanchor = "right",
          yanchor = "middle",
          textangle = -90
        )
      )
  }
  
  m <- list(
    l = 100,
    r = 50,
    b = 200,
    t = 70
  )
  finalPlot <-
    plotly::subplot(cohortPlots,nrows = length(cohortPlots), margin = m) %>% 
    plotly::layout(margin = m,
                   annotations = list(
                     x = c(0.3,0.7) ,
                     y = c(-0.1,-0.1),
                     text = c(paste0("<b>Cohorts :</b>\n",paste(distinctCohortCompoundName,collapse = "\n")),paste0("<b>DataSource :</b>\n",paste(distinctDatabaseCompoundName,collapse = "\n"))),
                     showarrow = F,
                     xref = 'paper',
                     yref = 'paper',
                     align = 'left',
                     xanchor = 'center',
                     yanchor = 'middle'
                   ))
  
  return(finalPlot)
}



plotIncidenceRate <- function(data,
                              cohortCount,
                              shortNameRef = NULL,
                              stratifyByAgeGroup = TRUE,
                              stratifyByGender = TRUE,
                              stratifyByCalendarYear = TRUE,
                              yscaleFixed = FALSE) {
  errorMessage <- checkmate::makeAssertCollection()
  
  if (!doesObjectHaveData(data)) {
    return(NULL)
  }
  checkmate::assertTibble(
    x = data,
    any.missing = TRUE,
    min.rows = 1,
    min.cols = 5,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = stratifyByAgeGroup,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = stratifyByGender,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = stratifyByCalendarYear,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertLogical(
    x = yscaleFixed,
    any.missing = FALSE,
    min.len = 1,
    max.len = 1,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::assertDouble(
    x = data$incidenceRate,
    lower = 0,
    any.missing = FALSE,
    null.ok = FALSE,
    min.len = 1,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  checkmate::assertDouble(
    x = data$incidenceRate,
    lower = 0,
    any.missing = FALSE,
    null.ok = FALSE,
    min.len = 1,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  
  plotData <- data %>%
    dplyr::left_join(cohortCount, by = c('cohortId', 'databaseId')) %>%
    dplyr::inner_join(cohort %>% 
                        dplyr::select(.data$cohortId, .data$shortName),
                      by = "cohortId") %>% 
    dplyr::rename("shortName" = .data$shortName) %>%
    dplyr::mutate(incidenceRate = round(.data$incidenceRate, digits = 3)) %>%
    # dplyr::mutate(
    #   strataGender = !is.na(.data$gender),
    #   strataAgeGroup = !is.na(.data$ageGroup),
    #   strataCalendarYear = !is.na(.data$calendarYear)
    # ) %>%
    # dplyr::filter(
    #   .data$strataGender %in% !!stratifyByGender &
    #     .data$strataAgeGroup %in% !!stratifyByAgeGroup &
    #     .data$strataCalendarYear %in% !!stratifyByCalendarYear
    # ) %>%
    dplyr::select(-dplyr::starts_with("strata"))
  
  if (stratifyByCalendarYear) {
    plotData <- plotData %>%
      dplyr::mutate(calendarYear = as.integer(.data$calendarYear))
  }
  
  aesthetics <- list(y = "incidenceRate")
  if (stratifyByCalendarYear) {
    aesthetics$x <- "calendarYear"
    xLabel <- "Calender year"
    showX <- TRUE
    if (stratifyByGender) {
      aesthetics$group <- "gender"
      aesthetics$color <- "gender"
    }
    plotType <- "line"
  } else {
    xLabel <- ""
    if (stratifyByGender) {
      aesthetics$x <- "gender"
      aesthetics$color <- "gender"
      aesthetics$fill <- "gender"
      showX <- TRUE
    } else if (stratifyByAgeGroup) {
      aesthetics$x <- "ageGroup"
      showX <- TRUE
    }
    else {
      aesthetics$x <- 1
      showX <- FALSE
    }
    plotType <- "bar"
  }
  
  sortShortName <- plotData %>%
    dplyr::select(.data$shortName) %>%
    dplyr::distinct() %>%
    dplyr::arrange(as.integer(sub(
      pattern = '^C', '', x = .data$shortName
    )))
  
  plotData <- plotData %>%
    dplyr::arrange(shortName = factor(.data$shortName, levels = sortShortName$shortName),
                   .data$shortName)
  
  plotData$shortName <- factor(plotData$shortName,
                               levels = sortShortName$shortName)
  
  if (stratifyByAgeGroup) {
    sortAgeGroup <- plotData %>%
      dplyr::select(.data$ageGroup) %>%
      dplyr::distinct() %>%
      dplyr::arrange(as.integer(sub(
        pattern = '-.+$', '', x = .data$ageGroup
      )))
    
    plotData <- plotData %>%
      dplyr::arrange(ageGroup = factor(.data$ageGroup, levels = sortAgeGroup$ageGroup),
                     .data$ageGroup)
    
    plotData$ageGroup <- factor(plotData$ageGroup,
                                levels = sortAgeGroup$ageGroup)
  }
  
  plotData$tooltip <- c(
    paste0(
      plotData$shortName,
      " ",
      plotData$databaseId,
      "\nIncidence Rate = ",
      scales::comma(plotData$incidenceRate, accuracy = 0.01),
      "/per 1k PY",
      "\nIncidence Proportion = ",
      scales::percent(plotData$cohortCount / plotData$cohortSubjects, accuracy = 0.1),
      "\nPerson years = ",
      scales::comma(plotData$personYears, accuracy = 0.01),
      "\nCohort count = ",
      scales::comma(plotData$cohortSubjects, accuracy = 1),
      "\nCount = ",
      paste0(scales::comma(plotData$cohortCount, accuracy = 1))
    )
  )
  
  if (stratifyByAgeGroup) {
    plotData$tooltip <-
      c(paste0(plotData$tooltip, "\nAge Group = ", plotData$ageGroup))
  }
  
  if (stratifyByGender) {
    plotData$tooltip <-
      c(paste0(plotData$tooltip, "\nGender = ", plotData$gender))
  }
  
  if (stratifyByCalendarYear) {
    plotData$tooltip <-
      c(paste0(plotData$tooltip, "\nYear = ", plotData$calendarYear))
  }
  
  if (stratifyByGender) {
    # Make sure colors are consistent, no matter which genders are included:
    
    genders <- c("Female", "Male", "No matching concept")
    # Code used to generate palette:
    # writeLines(paste(RColorBrewer::brewer.pal(n = 2, name = "Dark2"), collapse = "\", \""))
    colors <- c("#D95F02", "#1B9E77", "#444444")
    colors <- colors[genders %in% unique(plotData$gender)]
    plotData$gender <- factor(plotData$gender, levels = genders)
  }
  
  plot <-
    ggplot2::ggplot(data = plotData, do.call(ggplot2::aes_string, aesthetics)) +
    ggplot2::xlab(xLabel) +
    ggplot2::ylab("Incidence Rate (/1,000 person years)") +
    ggplot2::scale_y_continuous(expand = c(0, 0))
  
  if (stratifyByCalendarYear) {
    distinctCalenderYear <- plotData$calendarYear %>%
      unique() %>%
      sort()
    if (all(!is.na(distinctCalenderYear))) {
      if (length(distinctCalenderYear) >= 8) {
        plot <-
          plot + ggplot2::scale_x_continuous(n.breaks = 8, labels = round)
      } else {
        plot <-
          plot + ggplot2::scale_x_continuous(breaks = distinctCalenderYear)
      }
    }
  }
  
  plot <- plot + ggplot2::theme(
    legend.position = "top",
    legend.title = ggplot2::element_blank(),
    axis.text.x = if (showX)
      ggplot2::element_text(angle = 90, vjust = 0.5)
    else
      ggplot2::element_blank()
  )
  
  if (plotType == "line") {
    plot <- plot +
      ggiraph::geom_line_interactive(ggplot2::aes(), size = 1, alpha = 0.6) +
      ggiraph::geom_point_interactive(ggplot2::aes(tooltip = tooltip),
                                      size = 2,
                                      alpha = 0.6)
  } else {
    plot <-
      plot +   ggiraph::geom_col_interactive(ggplot2::aes(tooltip = tooltip), alpha = 0.6)
  }
  if (stratifyByGender) {
    plot <- plot + ggplot2::scale_color_manual(values = colors)
    plot <- plot + ggplot2::scale_fill_manual(values = colors)
  }
  # databaseId field only present when called in Shiny app:
  if (!is.null(data$databaseId) && length(data$databaseId) > 1) {
    if (yscaleFixed) {
      scales <- "fixed"
    } else {
      scales <- "free_y"
    }
    if (stratifyByGender | stratifyByCalendarYear) {
      if (stratifyByAgeGroup) {
        plot <-
          plot + facet_nested(databaseId + shortName ~ plotData$ageGroup, scales = scales)
      } else {
        plot <-
          plot + facet_nested(databaseId + shortName ~ ., scales = scales)
      }
    } else {
      plot <-
        plot + facet_nested(databaseId + shortName ~ ., scales = scales)
    }
    # spacing <- rep(c(1, rep(0.5, length(unique(plotData$shortName)) - 1)), length(unique(plotData$databaseId)))[-1]
    spacing <- plotData %>%
      dplyr::distinct(.data$databaseId, .data$shortName) %>%
      dplyr::arrange(.data$databaseId) %>%
      dplyr::group_by(.data$databaseId) %>%
      dplyr::summarise(count = dplyr::n()) %>%
      dplyr::ungroup()
    spacing <-
      unlist(sapply(spacing$count, function(x)
        c(1, rep(0.5, x - 1))))[-1]
    
    if (length(spacing) > 0) {
      plot <-
        plot + ggplot2::theme(
          panel.spacing.y = ggplot2::unit(spacing, "lines"),
          strip.background = ggplot2::element_blank()
        )
    } else {
      plot <-
        plot + ggplot2::theme(strip.background = ggplot2::element_blank())
    }
  } else {
    if (stratifyByAgeGroup) {
      plot <- plot + ggplot2::facet_grid(~ ageGroup)
    }
  }
  height <-
    1.5 + 1 * nrow(dplyr::distinct(plotData, .data$databaseId, .data$shortName))
  plot <- ggiraph::girafe(
    ggobj = plot,
    options = list(ggiraph::opts_sizing(width = .7),
                   ggiraph::opts_zoom(max = 5)),
    width_svg = 15,
    height_svg = height
  )
  return(plot)
}




plotCalendarIncidence <- function(data,
                                  cohortCount = cohortCount,
                                  shortNameRef = cohort,
                                  yscaleFixed = FALSE) {
  plotData <- data %>%
    dplyr::left_join(cohortCount, by = c('cohortId', 'databaseId')) %>%
    dplyr::inner_join(cohort %>% 
                        dplyr::select(.data$cohortId, .data$shortName),
                      by = .data$cohortId) %>% 
    dplyr::rename("shortName" = .data$shortName)
  aesthetics <- list(x = "calendarMonth", y = "countValue")
  plotType <- "line"
  sortShortName <- plotData %>%
    dplyr::select(.data$shortName) %>%
    dplyr::distinct() %>%
    dplyr::arrange(as.integer(sub(
      pattern = '^C', '', x = .data$shortName
    )))
  
  plotData <- plotData %>%
    dplyr::arrange(shortName = factor(.data$shortName, levels = sortShortName$shortName),
                   .data$shortName)
  
  plotData$shortName <- factor(plotData$shortName,
                               levels = sortShortName$shortName)
  
  plotData$tooltip <- c(
    paste0(
      plotData$shortName,
      " ",
      plotData$databaseId,
      "\nCount Value = ",
      plotData$countValue,
      "\nCalendar Month = ",
      plotData$calendarMonth,
      "\nPeriod Type = ",
      plotData$periodType,
      "\nCohort Subjects = ",
      plotData$cohortSubjects
    )
  )
  
  plot <-
    ggplot2::ggplot(data = plotData, do.call(ggplot2::aes_string, aesthetics)) +
    ggplot2::xlab("Calendar Month") +
    ggplot2::ylab("Count Value") +
    ggplot2::scale_y_continuous(labels = scales::comma)
  
  plot <- plot + ggplot2::theme(
    legend.position = "top",
    legend.title = ggplot2::element_blank(),
    axis.text.x = ggplot2::element_text(
      size = 12,
      angle = 90,
      vjust = 0.5
    ),
    axis.text.y = ggplot2::element_text(size = 12)
  ) +
    ggiraph::geom_line_interactive(ggplot2::aes(), size = 1, alpha = 0.6) +
    ggiraph::geom_point_interactive(ggplot2::aes(tooltip = tooltip),
                                    size = 2,
                                    alpha = 0.6)
  
  if (!is.null(plotData$databaseId) &&
      length(plotData$databaseId) > 1) {
    if (yscaleFixed) {
      scales <- "fixed"
    } else {
      scales <- "free_y"
    }
    # databaseId <- unique(plotData$databaseId)
    # shortName <- unique(plotData$shortName)
    # periodType <- unique(plotData$periodType)
    # plot + ggplot2::facet_wrap(databaseId + shortName ~ periodType,scales = scales)
    plot <-
      plot + ggplot2::facet_grid(databaseId + shortName ~  periodType,
                                 scales = scales,
                                 space = scales) +
      ggplot2::theme(
        strip.text.x = ggplot2::element_text(size = 10),
        strip.text.y = ggplot2::element_text(size = 10)
      )
  }
  height <-
    1.5 + 2 * nrow(dplyr::distinct(plotData, .data$databaseId, .data$shortName))
  plot <- ggiraph::girafe(
    ggobj = plot,
    options = list(ggiraph::opts_sizing(width = .7),
                   ggiraph::opts_zoom(max = 5)),
    width_svg = 15,
    height_svg = height
  )
  return(plot)
}

plotCompareCohortCharacterization <- function(balance,
                                              isTemporal = FALSE) {
  # Can't make sense of plot with > 1000 dots anyway, so remove
  # anything with small mean in both target and comparator:
  # removing covariates where the value of mean1 and mean2 are similar
  if (!isTemporal) {
    numberOfDatabaseId <- balance$databaseId %>% unique() %>% sort()
    dataAll <- list()
    for (i in (1:length(numberOfDatabaseId))) {
      dataAll[[i]] <- balance %>% 
        dplyr::filter(.data$databaseId %in% c(numberOfDatabaseId[[i]]))
      if (nrow(dataAll[[i]]) > 1000) {
        dataAll[[i]] <- dataAll[[i]] %>%
          dplyr::filter((.data$mean1 > 0.01 | .data$mean2 > 0.01) &
                          (.data$mean1/.data$mean2 > 0.9 |
                          .data$mean1/.data$mean2 < 1.1))
      }
      if (nrow(dataAll[[i]]) > 1000) {
        dataAll[[i]] <- dataAll[[i]] %>%
          dplyr::filter((.data$mean1 > 0.02 | .data$mean2 > 0.02) &
                          (.data$mean1/.data$mean2 > 0.9 |
                             .data$mean1/.data$mean2 < 1.1))
      }
      if (nrow(dataAll[[i]]) > 1000) {
        dataAll[[i]] <- dataAll[[i]] %>%
          dplyr::filter((.data$mean1 > 0.03 | .data$mean2 > 0.03) &
                          (.data$mean1/.data$mean2 > 0.9 |
                             .data$mean1/.data$mean2 < 1.1))
      }
      if (nrow(dataAll[[i]]) > 1000) {
        dataAll[[i]] <- dataAll[[i]] %>%
          dplyr::filter((.data$mean1 > 0.04 | .data$mean2 > 0.04) &
                          (.data$mean1/.data$mean2 > 0.9 |
                             .data$mean1/.data$mean2 < 1.1))
      }
      if (nrow(dataAll[[i]]) > 1000) {
        dataAll[[i]] <- dataAll[[i]] %>%
          dplyr::filter((.data$mean1 > 0.05 | .data$mean2 > 0.05) &
                          (.data$mean1/.data$mean2 > 0.9 |
                             .data$mean1/.data$mean2 < 1.1))
      }
    }
  } else if (isTemporal) {
    numberOfTimeId <- balance$timeId %>% unique() %>% sort()
    dataAll <- list()
    for (i in (1:length(numberOfTimeId))) {
      dataAll[[i]] <- balance %>% 
        dplyr::filter(.data$timeId %in% c(numberOfTimeId[[i]]))
      if (nrow(dataAll[[i]]) > 1000) {
        dataAll[[i]] <- dataAll[[i]] %>%
          dplyr::filter((.data$mean1 > 0.01 | .data$mean2 > 0.01) &
                          (.data$mean1/.data$mean2 > 0.9 |
                             .data$mean1/.data$mean2 < 1.1))
      }
      if (nrow(dataAll[[i]]) > 1000) {
        dataAll[[i]] <- dataAll[[i]] %>%
          dplyr::filter((.data$mean1 > 0.02 | .data$mean2 > 0.02) &
                          (.data$mean1/.data$mean2 > 0.9 |
                             .data$mean1/.data$mean2 < 1.1))
      }
      if (nrow(dataAll[[i]]) > 1000) {
        dataAll[[i]] <- dataAll[[i]] %>%
          dplyr::filter((.data$mean1 > 0.03 | .data$mean2 > 0.03) &
                          (.data$mean1/.data$mean2 > 0.9 |
                             .data$mean1/.data$mean2 < 1.1))
      }
      if (nrow(dataAll[[i]]) > 1000) {
        dataAll[[i]] <- dataAll[[i]] %>%
          dplyr::filter((.data$mean1 > 0.04 | .data$mean2 > 0.04) &
                          (.data$mean1/.data$mean2 > 0.9 |
                             .data$mean1/.data$mean2 < 1.1))
      }
      if (nrow(dataAll[[i]]) > 1000) {
        dataAll[[i]] <- dataAll[[i]] %>%
          dplyr::filter((.data$mean1 > 0.05 | .data$mean2 > 0.05) &
                          (.data$mean1/.data$mean2 > 0.9 |
                             .data$mean1/.data$mean2 < 1.1))
      }
    }
    balance <- dplyr::bind_rows(dataAll)
  }
  
  #enhance data
  balance <- balance %>%
    dplyr::mutate(domain = dplyr::case_when(
      !.data$domainId %in% c(
        "Condition",
        "Device",
        "Drug",
        "Measurement",
        "Observation",
        "Procedure",
        "Cohort"
      ) ~ 'Other',
      TRUE ~ .data$domainId
    )) %>%
    dplyr::inner_join(
      cohort %>%
        dplyr::select(.data$cohortId, .data$shortName),
      by = c("cohortId1" = "cohortId")
    ) %>%
    dplyr::rename("targetCohort" = .data$shortName) %>%
    dplyr::inner_join(
      cohort %>%
        dplyr::select(.data$cohortId, .data$shortName),
      by = c("cohortId2" = "cohortId")
    ) %>%
    dplyr::rename("comparatorCohort" = .data$shortName) %>%
    dplyr::mutate(
      tooltip =
        paste0(
          "Database Id:",
          .data$databaseId,
          "\nCovariate Name: ",
          .data$covariateName,
          "\nDomain: ",
          .data$domainId,
          "\nAnalysis: ",
          .data$analysisNameLong,
          "\nY ",
          .data$targetCohort,
          ": ",
          scales::percent(.data$mean1, accuracy = 0.1),
          "\nX ",
          .data$comparatorCohort,
          ": ",
          scales::percent(.data$mean2, accuracy = 0.1),
          "\nStd diff.: ",
          scales::percent(.data$stdDiff, accuracy = 0.1),
          "\nTime: ",
          paste0("Start ", .data$startDay, " to end ", .data$endDay)
        )
    ) %>%
    dplyr::inner_join(
      database %>%
        dplyr::select(.data$databaseId, .data$shortName) %>%
        dplyr::rename("databaseShortName" = .data$shortName),
      by = "databaseId"
    )
  
  #to sort the loop by database short name
  distinctDatabaseShortName <- database %>%
    dplyr::arrange(.data$id) %>%
    dplyr::filter(.data$databaseId %in% c(balance$databaseId %>% unique())) %>%
    dplyr::pull(.data$shortName)
  
  distinctTimeIdChoices <- temporalCovariateChoices %>% 
    dplyr::filter(.data$timeId %in% c(balance$timeId %>% unique())) %>%
    dplyr::pull(.data$choices)
  
  if (!isTemporal) {
    itemsToLoopOver <- distinctDatabaseShortName
  } else {
    itemsToLoopOver <- distinctTimeIdChoices
  }
  
  plotsArrary <- list()
  for (i in 1:length(itemsToLoopOver)) {
    if (!isTemporal) {
      data <- balance %>%
        dplyr::filter(.data$databaseShortName == itemsToLoopOver[i]) 
    } else if (isTemporal) {
      data <- balance %>%
        dplyr::filter(.data$timeId %in% c(temporalCovariateChoices %>% 
                                            dplyr::filter(.data$choices %in% c(itemsToLoopOver[i])) %>% 
                                            dplyr::pull(.data$timeId)))
    }
    data <- data %>%
      dplyr::inner_join(
        colorReference %>%
          dplyr::filter(.data$type == "domain") %>%
          dplyr::mutate(domain = .data$name, colors = .data$value) %>%
          dplyr::select(.data$domain, .data$colors),
        by = "domain"
      ) %>% dplyr::arrange(.data$domain)
    
    colors <- data$colors %>% unique()
    
    plotsArrary[[i]] <-
      plotly::plot_ly(
        data = data,
        x = ~ mean2,
        y = ~ mean1,
        text = ~ tooltip,
        type = 'scatter',
        height = 650,
        hoverinfo = 'text',
        mode = "markers",
        color = ~ domain,
        colors = colors,
        opacity = 0.8,
        showlegend = ifelse(i == 1, T, F),
        marker = list(
          size = 6,
          line = list(color = 'rgb(255,255,255)', width = 1)
        )
      ) %>%
      plotly::layout(
        xaxis = list(title = '', range = c(0, 1), showspikes = showPlotSpikes),
        yaxis = list(title = '', ange = c(0, 1), showspikes = showPlotSpikes),
        annotations = list(
          x = 0.5,
          y = 1.02,
          text = camelCaseToTitleCase(itemsToLoopOver[i]),
          showarrow = FALSE,
          xref = "paper",
          yref = "paper",
          xanchor = "center",
          yanchor = "middle",
          font = list(size = 18)
        )
      ) %>%
      plotly::add_segments(
        x = 0,
        y = 0,
        xend = 1,
        yend = 1,
        showlegend = F,
        line = list(
          width = 0.5,
          color = "rgb(160,160,160)",
          dash = "dash"
        )
      )
  }
  
  databaseArray  <- c()
  for (i in 1:length(distinctDatabaseShortName)) {
    databaseArray  <- paste0("    ",
                             database %>%
                               dplyr::filter(.data$shortName == 
                                               distinctDatabaseShortName[i]) %>%
                               dplyr::pull(.data$compoundName))
  }
  marginValues <- list(l = 100,
                       r = 0,
                       b = 200,
                       t = 50)
  
  targetName <- balance %>%
    dplyr::select(.data$cohortId1) %>%
    dplyr::mutate(cohortId = .data$cohortId1) %>%
    dplyr::inner_join(cohort, by = "cohortId") %>%
    dplyr::pull(.data$cohortName) %>% unique()
  
  comparatorName <- balance %>%
    dplyr::select(.data$cohortId2) %>%
    dplyr::mutate(cohortId = .data$cohortId2) %>%
    dplyr::inner_join(cohort, by = "cohortId") %>%
    dplyr::pull(.data$cohortName) %>% unique()
  
  #plot headers
  xLabelMain <- balance %>%
    dplyr::distinct(.data$comparatorCohort) %>%
    dplyr::mutate(comparatorCohort = paste0("Comparator (", .data$comparatorCohort, ")")) %>%
    dplyr::pull(.data$comparatorCohort)
  yLabelMain <- balance %>%
    dplyr::distinct(.data$targetCohort) %>%
    dplyr::mutate(targetCohort = paste0("Target (", .data$targetCohort, ")")) %>%
    dplyr::pull(.data$targetCohort)
  
  plot <- plotly::subplot(
    plotsArrary,
    nrows = ceiling(length(plotsArrary) / 5),
    shareX = TRUE,
    shareY = TRUE
  ) %>%
    plotly::layout(
      margin = marginValues,
      scene = list(aspectration = list(x = 1, y = 1)),
      annotations = list(
        x = c(-0.05, 0.5, 0.3, 0.7),
        y = c(0.5, -0.1, -0.4, -0.4),
        text = c(
          yLabelMain,
          xLabelMain,
          paste0(
           
            paste0("<b>Comparator: </b>",
                   comparatorName, " (", balance %>%
                     dplyr::distinct(.data$comparatorCohort),")"),
            "\n",
            "<b>Target: </b>",
            targetName, " (",
            balance %>%
              dplyr::distinct(.data$targetCohort), ")"
          ),
          paste0("<b>Databases: </b>\n", paste0(databaseArray , collapse = "\n"))
        ),
        showarrow = FALSE,
        xref = "paper",
        yref = "paper",
        xanchor = "center",
        yanchor = "bottom",
        align = "left",
        font = list(size = 12),
        textangle = c(-90, 0, 0, 0)
      )
    )
  return(plot)
}


# plotTemporalCompareStandardizedDifference3D <- function(balance,
#                                                         shortNameRef = NULL,
#                                                         domain = "all") {
#   domains <-
#     c("Condition",
#       "Device",
#       "Drug",
#       "Measurement",
#       "Observation",
#       "Procedure",
#       "Cohort")
#   balance$domain <- balance$domainId
#   balance$domain[!balance$domain %in% domains] <- "other"
#   if (domain != "all") {
#     balance <- balance %>%
#       dplyr::filter(.data$domain == !!domain)
#   }
#   
#   validate(need((nrow(balance) > 0), paste0("No data for selected combination.")))
#   
#   # Can't make sense of plot with > 1000 dots anyway, so remove
#   # anything with small mean in both target and comparator:
#   if (nrow(balance) > 1000) {
#     balance <- balance %>%
#       dplyr::filter(.data$mean1 > 0.01 | .data$mean2 > 0.01)
#   }
#   balance <- balance %>%
#     addShortName(
#       shortNameRef = shortNameRef,
#       cohortIdColumn = "cohortId1",
#       shortNameColumn = "targetCohort"
#     ) %>%
#     addShortName(
#       shortNameRef = shortNameRef,
#       cohortIdColumn = "cohortId2",
#       shortNameColumn = "comparatorCohort"
#     )
#   
#   # ggiraph::geom_point_interactive(ggplot2::aes(tooltip = tooltip), size = 3, alpha = 0.6)
#   balance$tooltip <-
#     c(
#       paste0(
#         "Covariate Name: ",
#         balance$covariateName,
#         "\nDomain: ",
#         balance$domainId,
#         "\nAnalysis: ",
#         balance$analysisName,
#         "\n Target (",
#         balance$targetCohort,
#         ") : ",
#         scales::comma(balance$mean1, accuracy = 0.01),
#         "\n Comparator (",
#         balance$comparatorCohort,
#         ") : ",
#         scales::comma(balance$mean2, accuracy = 0.01),
#         "\nStd diff.: ",
#         scales::comma(balance$stdDiff, accuracy = 0.01),
#         "\nTime : ",
#         balance$choices
#       )
#     )
#   balance <- balance %>% 
#     dplyr::inner_join(
#       read.csv('colorReference.csv') %>% 
#         dplyr::filter(.data$type == "domain") %>% 
#         dplyr::mutate(domain = .data$name, colors = .data$value) %>% 
#         dplyr::select(.data$domain,.data$colors),
#       by = "domain"
#     )
#   
#   xCohort <- balance %>%
#     dplyr::distinct(balance$targetCohort) %>%
#     dplyr::pull()
#   yCohort <- balance %>%
#     dplyr::distinct(balance$comparatorCohort) %>%
#     dplyr::pull()
#   
#   targetName <- balance %>% 
#     dplyr::select(.data$cohortId1) %>% 
#     dplyr::mutate(cohortId = .data$cohortId1) %>% 
#     dplyr::inner_join(cohort, by = "cohortId") %>% 
#     dplyr::pull(.data$cohortName) %>% unique()
#   
#   comparatorName <- balance %>% 
#     dplyr::select(.data$cohortId2) %>% 
#     dplyr::mutate(cohortId = .data$cohortId2) %>% 
#     dplyr::inner_join(cohort, by = "cohortId") %>% 
#     dplyr::pull(.data$cohortName) %>% unique()
#   
#   selectedDatabaseId <- balance$databaseId %>% unique()
#   
#   balance$tempChoices <- ''
#   for (i in 1 : nrow(balance)) {
#     balance$tempChoices[i] <- as.integer(strsplit(balance$choices[i], " ")[[1]][2])
#   }
#   balance <- balance %>% 
#     dplyr::arrange(.data$tempChoices) %>% 
#     dplyr::select(-.data$tempChoices)
#   distinctChoices <- balance$choices %>%  unique()
#   
#   plot <- plotly::plot_ly(balance,  x = ~ mean1, y = ~mean2, z = ~choices  , color = ~ domain, colors = ~colors,
#                           hoverinfo = 'text',
#                           opacity = 0.5,
#                           text = ~ tooltip)  %>% 
#     plotly::add_markers() %>% 
#     plotly::layout(scene = list(xaxis = list(title = paste0('Cohort (',xCohort,')')),
#                                 yaxis = list(title = paste0('Comparator (',yCohort,')')),
#                                 zaxis = list(title = '')
#     ),title = list(text=paste("target :",targetName,"\n",
#                               "comparator :", comparatorName,"\n",
#                               "Database :", selectedDatabaseId),
#                    x=0.5, y=0.1,font=list(size=10))
#     )
#   return(plot)
# }











### cohort overlap plot ##############

plotCohortOverlap <- function(data,
                              shortNameRef = NULL,
                              yAxis = "Percentages") {
  
  data <- data %>%
    dplyr::inner_join(cohort %>% 
                        dplyr::select(.data$cohortId, .data$shortName),
                      by = c("targetCohortId" = "cohortId")) %>% 
    dplyr::rename("targetShortName" = .data$shortName) %>% 
    dplyr::inner_join(cohort %>% 
                        dplyr::select(.data$cohortId, .data$shortName),
                      by = c("comparatorCohortId" = "cohortId")) %>% 
    dplyr::rename("comparatorShortName" = .data$shortName)
   
  targetCohortCompoundName <- data  %>% 
    dplyr::inner_join(cohort %>% 
                        dplyr::mutate(targetCohortId = .data$cohortId) %>% 
                        dplyr::select(.data$targetCohortId,.data$compoundName),
                      by = "targetCohortId") %>% 
    dplyr::mutate(compoundName = ifelse(stringr::str_length(.data$compoundName) > 40, 
                                        paste0(substr(.data$compoundName,0,40),"\n",substr(.data$compoundName,40,stringr::str_length(.data$compoundName))),
                                        .data$compoundName)) %>% 
    dplyr::pull(.data$compoundName) %>% unique() %>% paste(collapse = "\n")
  # targetCohortCompoundName <- paste("<b>Target Cohorts</b> :",paste(targetCohortCompoundName,collapse = ","))
  
  comparatorCohortCompoundName <- data  %>% 
    dplyr::inner_join(cohort %>% 
                        dplyr::mutate(comparatorCohortId = .data$cohortId) %>% 
                        dplyr::select(.data$comparatorCohortId,.data$compoundName),
                      by = "comparatorCohortId") %>% 
    dplyr::mutate(compoundName = ifelse(stringr::str_length(.data$compoundName) > 40, 
                                        paste0(substr(.data$compoundName,0,40),"\n",substr(.data$compoundName,40,stringr::str_length(.data$compoundName))),
                                        .data$compoundName)) %>% 
    dplyr::pull(.data$compoundName) %>% unique() %>% paste(collapse = "\n")
  # comparatorCohortCompoundName <- paste("<b>Comparator Cohorts</b> :",paste(comparatorCohortCompoundName,collapse = ","))                    
  
  plotData <- data %>%
    dplyr::mutate(
      absTOnlySubjects = abs(.data$tOnlySubjects),
      absCOnlySubjects = abs(.data$cOnlySubjects),
      absBothSubjects = abs(.data$bothSubjects),
      absEitherSubjects = abs(.data$eitherSubjects),
      signTOnlySubjects = dplyr::case_when(.data$tOnlySubjects < 0 ~ '<', TRUE ~ ''),
      signCOnlySubjects = dplyr::case_when(.data$cOnlySubjects < 0 ~ '<', TRUE ~ ''),
      signBothSubjects = dplyr::case_when(.data$bothSubjects < 0 ~ '<', TRUE ~ '')
    ) %>%
    dplyr::mutate(
      tOnlyString = paste0(
        .data$signTOnlySubjects,
        scales::comma(.data$absTOnlySubjects, accuracy = 1),
        " (",
        .data$signTOnlySubjects,
        scales::percent(.data$absTOnlySubjects /
                          .data$absEitherSubjects,
                        accuracy = 1),
        ")"
      ),
      cOnlyString = paste0(
        .data$signCOnlySubjects,
        scales::comma(.data$absCOnlySubjects, accuracy = 1),
        " (",
        .data$signCOnlySubjects,
        scales::percent(.data$absCOnlySubjects /
                          .data$absEitherSubjects,
                        accuracy = 1),
        ")"
      ),
      bothString = paste0(
        .data$signBothSubjects,
        scales::comma(.data$absBothSubjects, accuracy = 1),
        " (",
        .data$signBothSubjects,
        scales::percent(.data$absBothSubjects /
                          .data$absEitherSubjects,
                        accuracy = 1),
        ")"
      )
    )  %>%
    dplyr::mutate(
      tooltip = paste0(
        "Source: ",
        .data$databaseId,
        "\n",
        "\n",
        .data$targetShortName,
        ' only: ',
        .data$tOnlyString,
        "\nBoth: ",
        .data$bothString,
        "\n",
        .data$comparatorShortName,
        ' only: ',
        .data$cOnlyString
      )
    ) %>%
    dplyr::select(
      .data$targetShortName,
      .data$comparatorShortName,
      .data$databaseId,
      .data$absTOnlySubjects,
      .data$absCOnlySubjects,
      .data$absBothSubjects,
      .data$tooltip
    ) %>%
    tidyr::pivot_longer(
      cols = c("absTOnlySubjects",
               "absCOnlySubjects",
               "absBothSubjects"),
      names_to = "subjectsIn",
      values_to = "value"
    ) %>%
    dplyr::mutate(
      subjectsIn = dplyr::recode(
        .data$subjectsIn,
        absTOnlySubjects = "Target cohort only",
        absBothSubjects = "Both cohorts",
        absCOnlySubjects = "Comparator cohort only"
      )
    )
  
  plotDataSummary <- plotData %>%
    dplyr::select(
      .data$targetShortName,
      .data$comparatorShortName,
      .data$databaseId,
      .data$value
    ) %>%
    dplyr::group_by(
      .data$targetShortName,
      .data$comparatorShortName,
      .data$databaseId
    ) %>%
    dplyr::summarize(totalSubjects = sum(.data$value), .groups = "keep")
  
  plotData <- plotData %>%
    dplyr::inner_join(plotDataSummary,
      by = c("targetShortName",
             "comparatorShortName",
             "databaseId")
    ) %>% 
    dplyr::mutate(percent = round(.data$value /
                                              .data$totalSubjects,
                                            digits = 5)) 
  
  plotData$subjectsIn <-
    factor(
      plotData$subjectsIn,
      levels = c("Target cohort only", "Both cohorts", "Comparator cohort only")
    )
  
  if (yAxis == "Percentages") {
    xAxisTickFormat <- "%"
    plotData <- plotData %>% 
      dplyr::mutate(xAxisValues = .data$percent) 
    xAxisMax <- 1
  } else {
    xAxisTickFormat <- ""
    plotData <- plotData %>% 
      dplyr::mutate(xAxisValues = .data$value)
    xAxisMax <- max(plotData$totalSubjects) 
  }
  
  
  sortTargetShortName <- plotData %>%
    dplyr::select(.data$targetShortName) %>%
    dplyr::distinct() %>%
    dplyr::arrange(as.integer(sub(
      pattern = '^C', '', x = .data$targetShortName
    )))
  
  sortComparatorShortName <- plotData %>%
    dplyr::select(.data$comparatorShortName) %>%
    dplyr::distinct() %>%
    dplyr::arrange(as.integer(sub(
      pattern = '^C', '', x = .data$comparatorShortName
    )))
  
  
  
  plotData <- plotData %>%
    dplyr::arrange(
      targetShortName = factor(.data$targetShortName, levels = sortTargetShortName$targetShortName),
      .data$targetShortName
    ) %>%
    dplyr::arrange(
      comparatorShortName = factor(.data$comparatorShortName, levels = sortComparatorShortName$comparatorShortName),
      .data$comparatorShortName
    ) %>% 
    dplyr::inner_join(database %>% 
                        dplyr::select(.data$databaseId, .data$shortName) %>% 
                        dplyr::rename("databaseShortName" = .data$shortName), by = "databaseId") %>% 
    dplyr::mutate(databaseCompoundName = paste(.data$databaseShortName,": ",.data$databaseId))
  
  distinctComparatorShortName <- plotData$comparatorShortName %>% unique()
  # distinctTargetShortName <- plotData$targetShortName %>% unique()
  distinctDatabaseShortName <- plotData$databaseShortName %>% unique()
  distinctDatabaseCompoundName <- paste(plotData$databaseCompoundName %>% unique(),collapse = ",")
  databasePlots <- list()
  for (i in 1:length(distinctDatabaseShortName)) {
    plotDataFilteredByDatabaseId <- plotData %>% 
      dplyr::filter(.data$databaseShortName == distinctDatabaseShortName[i])
    comparatorPlots <- list()
    for (j in 1:length(distinctComparatorShortName)) {
      plotDataFilteredByComparator <- plotDataFilteredByDatabaseId %>% 
        dplyr::filter(.data$comparatorShortName == distinctComparatorShortName[j])
      
      if (i == 1 && j == 1) {
        showLegend <- TRUE
      } else {
        showLegend <- FALSE
      }
     
      if (j == length(distinctComparatorShortName)) {
        xAxisTickLabels <- TRUE
      } else {
        xAxisTickLabels <- FALSE
      }
      
      if (i == 1) {
        yAxisTickLabels <- TRUE
      } else {
        yAxisTickLabels <- FALSE
      }
      if (nrow(plotDataFilteredByComparator) > 0) {
        distinctTargetShortName <- plotDataFilteredByComparator$targetShortName %>% unique()
        annotationStartValue <- round(1 / (length(distinctTargetShortName) * 2), digits = 3)
        annotationEndValue <- 0.999
        annotationInterval <- annotationStartValue * 2
      } else {
        distinctTargetShortName <- c()
        annotationStartValue <- 0
        annotationEndValue <- 0
        annotationInterval <- 0
      }
     
      comparatorPlots[[j]] <- plotly::plot_ly(plotDataFilteredByComparator,
                                              x = ~xAxisValues, y = ~targetShortName, type = 'bar',
                                              name = ~subjectsIn, text = ~tooltip,
                                              color = ~subjectsIn, colors = c( rgb(0.4, 0.4, 0.9), rgb(0.3, 0.2, 0.4),rgb(0.8, 0.2, 0.2)),
                                              showlegend = showLegend, height = 800) %>%
        plotly::layout(barmode = 'stack',
                       legend = list(orientation = "h",x = 0.4,y = -0.1),
                       xaxis = list(range = c(0, xAxisMax),
                                    showticklabels = xAxisTickLabels,
                                    tickformat = xAxisTickFormat,
                                    showspikes = showPlotSpikes),
                       yaxis = list(showticklabels = yAxisTickLabels, showspikes = showPlotSpikes),
                       annotations = list(
                         x = rep(1 + length(distinctDatabaseShortName) * 0.01,length(distinctTargetShortName)) ,
                         y = seq(annotationStartValue, annotationEndValue, annotationInterval),
                         text = rep(ifelse(i == length(distinctDatabaseShortName),distinctComparatorShortName[j],""),length(distinctTargetShortName)),
                         showarrow = F,
                         xanchor = "center",
                         yanchor = "middle",
                         xref = 'paper',
                         yref = 'paper',
                         font = list(size = 14)
                       ))
    }
    databasePlots[[i]] <- plotly::subplot(comparatorPlots,nrows = length(comparatorPlots)) %>% 
      plotly::layout(annotations = list(
                       x = 0.5 ,
                       y = 1.05,
                       text = distinctDatabaseShortName[i],
                       showarrow = F,
                       xanchor = "center",
                       yanchor = "middle",
                       xref = 'paper',
                       yref = 'paper',
                       font = list(size = 14)
                     ))
  }
  m <- list(
    l = 50,
    r = 50,
    b = 250,
    t = 50
  )
  plot <- plotly::subplot(databasePlots) %>% 
    plotly::layout(annotations = list(
      x = c(0.2,0.5,0.8) ,
      y = c(-0.3,-0.3,-0.3),
      text = c(paste0("<b>Cohort : </b>\n",targetCohortCompoundName),paste0("<b>Comparator :</b>\n",comparatorCohortCompoundName),paste0("<b>Datasource :</b>\n",distinctDatabaseCompoundName)),
      showarrow = F,
      xanchor = "center",
      yanchor = "middle",
      xref = 'paper',
      yref = 'paper',
      align = 'left'
      # font = list(size = 14)
    ),margin = m)
  
  return(plot)
}


### cohort overlap plot ##############

plotCohortOverlapPie <- function(data,
                              shortNameRef = NULL) {
  data <- data %>%
    dplyr::inner_join(cohort %>% 
                        dplyr::select(.data$cohortId, .data$shortName),
                      by = c("targetCohortId" = "cohortId")) %>% 
    dplyr::rename("targetShortName" = .data$shortName) %>% 
    dplyr::inner_join(cohort %>% 
                        dplyr::select(.data$cohortId, .data$shortName),
                      by = c("comparatorCohortId" = "cohortId")) %>% 
    dplyr::rename("comparatorShortName" = .data$shortName)
   
  data <- data %>% dplyr::inner_join(database %>% 
                                       dplyr::select(.data$databaseId, .data$shortName) %>% 
                                       dplyr::rename("databaseShortName" = .data$shortName), by = "databaseId") 
  
  targetCohortCompoundName <- data  %>%
    dplyr::inner_join(cohort %>%
                        dplyr::mutate(targetCohortId = .data$cohortId) %>%
                        dplyr::select(.data$targetCohortId,.data$compoundName),
                      by = "targetCohortId") %>%
    dplyr::pull(.data$compoundName) %>% unique()
  targetCohortCompoundName <- paste("<b>Target Cohort :</b>",paste(targetCohortCompoundName,collapse = ","))

  comparatorCohortCompoundName <- data  %>%
    dplyr::inner_join(cohort %>%
                        dplyr::mutate(comparatorCohortId = .data$cohortId) %>%
                        dplyr::select(.data$comparatorCohortId,.data$compoundName),
                      by = "comparatorCohortId") %>%
    dplyr::pull(.data$compoundName) %>% unique()
  comparatorCohortCompoundName <- paste("<b>Comparator Cohort :</b>",paste(comparatorCohortCompoundName,collapse = ","))
  
  plotData <- data %>% 
    dplyr::mutate("Started_During" = abs(.data$cInTSubjects - .data$cStartOnTStart - .data$cStartOnTEnd)) %>% 
    dplyr::select(.data$cStartBeforeTStart,.data$cStartOnTStart,.data$Started_During,.data$cStartOnTEnd,.data$cStartAfterTEnd,.data$databaseId,.data$databaseShortName) %>% 
    dplyr::rename("Started before" = .data$cStartBeforeTStart,
                  "Started on" = .data$cStartOnTStart,
                  "Started during" = .data$Started_During,
                  "Started at the end of" = .data$cStartOnTEnd,
                  "Started after" = .data$cStartAfterTEnd) %>%
    dplyr::mutate(database = paste0(.data$databaseShortName ," : ",.data$databaseId)) %>% 
    tidyr::pivot_longer(
      cols = c("Started before",
               "Started on",
               "Started during",
               "Started at the end of",
               "Started after"),
      names_to = "subjectsIn",
      values_to = "value"
    )
  distinctDatabaseShortName <- plotData$databaseShortName %>%  unique()
  databaseString <- paste(plotData$database %>% unique(),collapse = ", ")
  
  lightColors <- colorRampPalette(c( rgb(0.3,0.2,0.4), rgb(1,1,1)))(4) %>% 
    head(-1) %>% 
    tail(-1)
  
  darkColors <- colorRampPalette(c( rgb(0.3,0.2,0.4), rgb(0,0,0)))(4) %>% 
    head(-1) 
  colors <- c(rev(lightColors),darkColors)
  subjects <- c("Started before",
                "Started on",
                "Started during",
                "Started at the end of",
                "Started after")
  
  plot <- plotly::plot_ly()
  for (i in 1:length(distinctDatabaseShortName)) {
    filteredData <- plotData %>% 
      dplyr::filter(.data$databaseShortName == distinctDatabaseShortName[i]) %>% 
      dplyr::arrange(dplyr::desc(.data$value)) %>% 
      dplyr::mutate(percentage = scales::label_percent()(.data$value/sum(.data$value))) %>% 
      dplyr::mutate(ValuesWithComma = scales::label_comma()(.data$value))
    
    plot <- plot %>% 
      plotly::add_pie(data = filteredData, 
                      labels = ~subjectsIn, 
                      values = ~value,
                      title = paste0(distinctDatabaseShortName[i],"\nn = ",scales::label_comma()(sum(filteredData$value))),  
                      name = distinctDatabaseShortName[i],
                      hovertemplate = ~paste(distinctDatabaseShortName[i],": ",databaseId,'<br>',subjectsIn,': ',ValuesWithComma,"(",percentage,")"),
                      domain = list(row = 0, column = i - 1),
                      marker = list(colors = colors),
                      showlegend = ifelse(i == length(distinctDatabaseShortName),T,F))
  }
  m <- list(
    l = 0,
    r = 0,
    b = 150,
    t = 50
  )
  plot <- plot %>% 
    plotly::layout(
      grid = list(rows = 1, columns = length(distinctDatabaseShortName)),
      showlegend = T,
      legend = list(orientation = "h",   # show entries horizontally
                    xanchor = "center",  # use center of legend as anchor
                    x = 0.5,
                    y = -0.15),
      annotations = list(
      x = c(0.3,0.7) ,
      y = c(-0.5,-0.5),
      text = c(paste0(targetCohortCompoundName,"\n",comparatorCohortCompoundName), paste0("<b>Datasource :</b>\n",databaseString)),
      showarrow = F,
      xanchor = "center",
      yanchor = "bottom",
      xref = 'paper',
      yref = 'paper',
      align = 'left',
      font = list(size = 11)
    ),margin = m)
  return(plot)
}


plotTsStlDecomposition <- function(data,
                                   field) {
  if (!field %in% colnames(data)) {
    return(NULL)
  }
  data[["value"]] <- abs(data[[field]])
  ts <- data %>%
    dplyr::select("value") %>%
    tsibble::fill_gaps(value = 0)
  tsModel <- ts %>%
    fabletools::model(feasts::STL(value))
  
  plot <- fabletools::components(tsModel) %>%
    feasts::autoplot()
  
  return(plot)
  
}
