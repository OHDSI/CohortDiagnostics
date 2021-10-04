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
    
    distinctDatabaseId <- c()
    for (i in 1:length(tsibbleData)) {
      data  <- tsibbleData[[i]]$databaseId %>% unique()
      distinctDatabaseId <- union(distinctDatabaseId, data)
    }
    
    cohortPlots <- list()
    noOfPlotRows <-
      length(distinctCohortShortName) * length(plotFilters)
    for (i in 1:length(distinctCohortShortName)) {
      filterPlots <- list()
      for (j in 1:length(plotFilters)) {
        data <-
          tsibbleData[[j]] %>% dplyr::filter(.data$cohortShortName == distinctCohortShortName[i])
        databasePlots <- list()
        for (k in 1:length(distinctDatabaseId)) {
            
          databasePlots[[k]] <- plotTs(
            data = data %>% dplyr::filter(.data$databaseId == distinctDatabaseId[k]),
            plotHeight =  200 * noOfPlotRows,
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
                  text = distinctDatabaseId[[k]],
                  showarrow = F,
                  xref = 'paper',
                  yref = 'paper'
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
              x = -0.07,
              y = 0.5,
              text = camelCaseToTitleCase(plotFilters[j]),
              showarrow = FALSE,
              xref = "paper",
              yref = "paper",
              textangle = -90
            )
          )
        
      }
      cohortPlots[[i]] <-
        plotly::subplot(filterPlots, nrows = length(filterPlots)) %>%
        plotly::layout(
          annotations = list(
            x = -0.09,
            y = 0.5,
            text = camelCaseToTitleCase(distinctCohortShortName[i]),
            showarrow = FALSE,
            xref = "paper",
            yref = "paper",
            textangle = -90
          )
        )
    }
    m <- list(
      l = 150,
      r = 0,
      b = 0,
      t = 50,
      pad = 4
    )
    finalPlot <-
      plotly::subplot(cohortPlots, nrows = length(cohortPlots)) %>%
      plotly::layout(autosize = T, margin = m)
    
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
    #   dplyr::distinct(.data$databaseId, .data$cohortShortName) %>%
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
                   valueType = "Records") {
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
                      text = ~ paste("Statistics = TOTAL",
                                     "\nDatabase ID = ",.data$databaseId,
                                     "\nvalueType = ",valueType )) %>%
    plotly::add_trace(x = ~ periodDate,
                      y = ~ trend,
                      mode = "line",
                      type = "scatter",
                      name = "Trends",
                      text = ~ paste("Statistics = TREND",
                                     "\nDatabase ID = ",.data$databaseId,
                                     "\nvalueType = ",valueType )) %>%
    plotly::layout(showlegend = FALSE,
                   xaxis = list(range = c(xAxisMin, xAxisMax)))
  
  
  
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
    distinctDatabaseId <- c()
    for (i in 1:length(yAxisValues)) {
      if ("dcmp_ts" %in% class(stlModeledTsibbleData[[i]])) {
        distinctDatabaseId <- c(distinctDatabaseId,
                                stlModeledTsibbleData[[i]]$databaseId) %>% unique()
      }
    }
    
    cohortPlot <- list()
    noOfPlotRows <- length(yAxisValues)
    yAxisValuesPlots <- list()
    for (j in 1:length(yAxisValues)) {
      data <- stlModeledTsibbleData[[j]]
      databasePlots <- list()
      for (l in (1:length(distinctDatabaseId))) {
        databasePlots[[l]] <- plotTs(
          data = data %>%
            dplyr::filter(.data$databaseId %in% distinctDatabaseId[[l]]),
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
                text = distinctDatabaseId[[l]],
                showarrow = F,
                xref = 'paper',
                yref = 'paper'
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
            x = -0.04,
            y = 0.5,
            text = camelCaseToTitleCase(yAxisValues[[j]]),
            showarrow = FALSE,
            xref = "paper",
            yref = "paper",
            textangle = -90
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
                       y = -0.25,
                       text = paste0(conceptName," (",conceptId,")","\n",conceptSynonym),
                       showarrow = F,
                       xref = 'paper',
                       yref = 'paper'
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
  
  initialColor <- read.csv(paste0(getwd(),"/colorReference.csv")) %>% 
    dplyr::filter(.data$type == "database",.data$name == "database") %>% 
    dplyr::pull(.data$value)
  
  colorReference <- data %>% 
    dplyr::select(.data$databaseId) %>% 
    unique()

  lightColors <- colorRampPalette(c(initialColor, "#000000"))(ceiling(nrow(colorReference)/2) + 1) %>% 
    head(-1) %>% 
    tail(-1)
  
  darkColors <- colorRampPalette(c(initialColor, "#000000"))(floor(nrow(colorReference)/2) + 2) %>% 
    head(-1) 
  
  colorReference <- colorReference %>% 
    dplyr::mutate(color = c(lightColors,darkColors))
 
  plotData <-
    addShortName(data = data, shortNameRef = shortNameRef)  %>% 
    addDatabaseShortName(shortNameRef = database) %>% 
    dplyr::inner_join(colorReference, by = "databaseId")
    
  
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
   
 plotHeight <- length(distinctDatabaseShortName) * length(sortShortName$shortName) * 100
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
        xaxis = list(range = c(xAxisMin, xAxisMax), tickformat = ",d")
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
    b = 0,
    t = 50
  )
  finalPlot <-
    plotly::subplot(databasePlots, nrows = length(databasePlots), margin = 0.008) %>% 
    plotly::layout(margin = m)
  
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
    addShortName(shortNameRef) %>%
    dplyr::mutate(incidenceRate = round(.data$incidenceRate, digits = 3)) %>%
    dplyr::mutate(
      strataGender = !is.na(.data$gender),
      strataAgeGroup = !is.na(.data$ageGroup),
      strataCalendarYear = !is.na(.data$calendarYear)
    ) %>%
    dplyr::filter(
      .data$strataGender %in% !!stratifyByGender &
        .data$strataAgeGroup %in% !!stratifyByAgeGroup &
        .data$strataCalendarYear %in% !!stratifyByCalendarYear
    ) %>%
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
    addShortName(shortNameRef)
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

plotCohortComparisonStandardizedDifference <- function(balance,
                                                       shortNameRef = NULL,
                                                       domain = "all") {
  domains <-
    c("Condition",
      "Device",
      "Drug",
      "Measurement",
      "Observation",
      "Procedure",
      "Cohort")
  balance$domain <- balance$domainId
  balance$domain[!balance$domain %in% domains] <- "Other"

  
  if (domain != "all") {
    balance <- balance %>%
      dplyr::filter(.data$domain == !!domain)
  }
  
  # Can't make sense of plot with > 1000 dots anyway, so remove
  # anything with small mean in both target and comparator:
  if (nrow(balance) > 1000) {
    balance <- balance %>%
      dplyr::filter(.data$mean1 > 0.01 | .data$mean2 > 0.01)
  }
  
  balance <- balance %>%
    addShortName(
      shortNameRef = shortNameRef,
      cohortIdColumn = "cohortId1",
      shortNameColumn = "targetCohort"
    ) %>%
    addShortName(
      shortNameRef = shortNameRef,
      cohortIdColumn = "cohortId2",
      shortNameColumn = "comparatorCohort"
    )
  
  # ggiraph::geom_point_interactive(ggplot2::aes(tooltip = tooltip), size = 3, alpha = 0.6)
  balance$tooltip <-
    c(
      paste0(
        "Covariate Name: ",
        balance$covariateName,
        "\nDomain: ",
        balance$domainId,
        "\nAnalysis: ",
        balance$analysisNameLong,
        "\nY ",
        balance$comparatorCohort,
        ": ",
        scales::comma(balance$mean2, accuracy = 0.01),
        "\nX ",
        balance$targetCohort,
        ": ",
        scales::comma(balance$mean1, accuracy = 0.01),
        "\nStd diff.: ",
        scales::comma(balance$stdDiff, accuracy = 0.01),
        "\nTime: ",
        paste0("Start ", balance$startDay, " to end ", balance$endDay)
      )
    )
  
  # Code used to generate palette:
  # writeLines(paste(RColorBrewer::brewer.pal(n = length(domains), name = "Dark2"), collapse = "\", \""))
  
  balance <- balance %>% 
    dplyr::inner_join(
      read.csv('colorReference.csv') %>% 
        dplyr::filter(.data$type == "domain") %>% 
        dplyr::mutate(domain = .data$name, colors = .data$value) %>% 
        dplyr::select(.data$domain,.data$colors),
      by = "domain"
    )
  
  
  xCohort <- balance %>%
    dplyr::distinct(balance$targetCohort) %>%
    dplyr::pull()
  yCohort <- balance %>%
    dplyr::distinct(balance$comparatorCohort) %>%
    dplyr::pull()
  
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
  
  
  balance <- balance %>% 
    addDatabaseShortName(shortNameRef = database)
  
  distinctDatabaseShortName <- balance$databaseShortName %>% unique()
  
 
  
  databasePlots <- list()
  for (i in 1:length(distinctDatabaseShortName)) {
    data <- balance %>% 
      dplyr::filter(.data$databaseShortName == distinctDatabaseShortName[i])
    databasePlots[[i]] <- plotly::plot_ly(balance, x = ~mean1, y = ~mean2, text = ~tooltip, type = 'scatter',height = 650,hoverinfo = 'text',
                            mode = "markers", color = ~domain, colors = ~colors, opacity = 0.4,showlegend = ifelse(i == 1, T, F), 
                            marker = list(size = 12, line = list(color = 'rgb(255,255,255)', width = 1))) %>% 
      plotly::layout(
        xaxis = list(range = c(0, 1)),
        yaxis = list(range = c(0, 1)),
        annotations = list(
          x = c(-0.06, 0.5, 0.5),
          y = c(0.5, -0.06, 1.02),
          text = c(
            ifelse(i == 1, paste("Covariate Mean in ", xCohort), ""),
            paste("Covariate Mean in ", yCohort),
            camelCaseToTitleCase(distinctDatabaseShortName[i])
          ),
          showarrow = FALSE,
          xref = "paper",
          yref = "paper",
          xanchor = "center",
          yanchor = "middle",
          textangle = c(-90, 0, 0),
          font = list(size = 18)
        )
      ) %>% 
      plotly::add_segments(x = 0, y = 0, xend = 1, yend = 1,showlegend = F,
                           line = list(width = 0.5, color = "rgb(160,160,160)", dash = "dash"))
  }
  
  
  databaseArray <- c()
  for (i in 1:length(distinctDatabaseShortName)){
    databaseId <- balance %>%  dplyr::filter(.data$databaseShortName == distinctDatabaseShortName[i]) %>% 
      dplyr::pull(.data$databaseId) %>% unique()
    databaseArray <- c(databaseArray, paste(distinctDatabaseShortName[i]," : ", databaseId))
  }
  databaseString  <- paste(databaseArray, collapse = ", ")
  
  m <- list(
    l = 100,
    r = 0,
    b = 200,
    t = 50
  )
  plot <- plotly::subplot(databasePlots) %>% 
    plotly::layout(margin = m,
                   annotations = list(
                     x = 0.5,
                     y = -0.2,
                     text = paste("Target - ", xCohort, " : ", targetName,", Comparator - ", yCohort, " : ", comparatorName,"\n",
                                  "Database - ", databaseString),
                     showarrow = FALSE,
                     xref = "paper",
                     yref = "paper",
                     xanchor = "center",
                     yanchor = "middle",
                     font = list(size = 18)
                   ))
  return(plot)
}

plotTemporalCompareStandardizedDifference <- function(balance,
                                                      shortNameRef = NULL,
                                                      domain = "all") {
  domains <-
    c("Condition",
      "Device",
      "Drug",
      "Measurement",
      "Observation",
      "Procedure",
      "Cohort")
  balance$domain <- balance$domainId
  balance$domain[!balance$domain %in% domains] <- "other"
  if (domain != "all") {
    balance <- balance %>%
      dplyr::filter(.data$domain == !!domain)
  }
  
  validate(need((nrow(balance) > 0), paste0("No data for selected combination.")))
  
  # Can't make sense of plot with > 1000 dots anyway, so remove
  # anything with small mean in both target and comparator:
  if (nrow(balance) > 1000) {
    balance <- balance %>%
      dplyr::filter(.data$mean1 > 0.01 | .data$mean2 > 0.01)
  }
  
  balance <- balance %>%
    addShortName(
      shortNameRef = shortNameRef,
      cohortIdColumn = "cohortId1",
      shortNameColumn = "targetCohort"
    ) %>%
    addShortName(
      shortNameRef = shortNameRef,
      cohortIdColumn = "cohortId2",
      shortNameColumn = "comparatorCohort"
    )
  
  # ggiraph::geom_point_interactive(ggplot2::aes(tooltip = tooltip), size = 3, alpha = 0.6)
  balance$tooltip <-
    c(
      paste0(
        "Covariate Name: ",
        balance$covariateName,
        "\nDomain: ",
        balance$domainId,
        "\nAnalysis: ",
        balance$analysisName,
        "\n Target (",
        balance$targetCohort,
        ") : ",
        scales::comma(balance$mean1, accuracy = 0.01),
        "\n Comparator (",
        balance$comparatorCohort,
        ") : ",
        scales::comma(balance$mean2, accuracy = 0.01),
        "\nStd diff.: ",
        scales::comma(balance$stdDiff, accuracy = 0.01),
        "\nTime : ",
        balance$choices
      )
    )
  balance <- balance %>% 
    dplyr::inner_join(
      read.csv('colorReference.csv') %>% 
        dplyr::filter(.data$type == "domain") %>% 
        dplyr::mutate(domain = .data$name, colors = .data$value) %>% 
        dplyr::select(.data$domain,.data$colors),
      by = "domain"
    )
  
  # Code used to generate palette:
  # writeLines(paste(RColorBrewer::brewer.pal(n = length(domains), name = "Dark2"), collapse = "\", \""))
  
  # Make sure colors are consistent, no matter which domains are included:
  # colors <-
  #   c(
  #     "#1B9E77",
  #     "#D95F02",
  #     "#7570B3",
  #     "#E7298A",
  #     "#66A61E",
  #     "#E6AB02",
  #     "#A6761D",
  #     "#444444"
  #   )
  # colors <- colors[c(domains, "other") %in% unique(balance$domain)]
  
  # balance$domain <-
  #   factor(balance$domain, levels = c(domains, "other"))
  
  # targetLabel <- paste(strwrap(targetLabel, width = 50), collapse = "\n")
  # comparatorLabel <- paste(strwrap(comparatorLabel, width = 50), collapse = "\n")
  
  xCohort <- balance %>%
    dplyr::distinct(balance$targetCohort) %>%
    dplyr::pull()
  yCohort <- balance %>%
    dplyr::distinct(balance$comparatorCohort) %>%
    dplyr::pull()
  
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
  
  selectedDatabaseId <- balance$databaseId %>% unique()
  # balance <- balance %>%
  #   dplyr::arrange(.data$startDay, .data$endDay)
  
  # facetLabel <- balance %>%
  #   dplyr::select(.data$startDay, .data$choices) %>%
  #   dplyr::distinct() %>%
  #   dplyr::arrange(.data$startDay) %>%
  #   dplyr::pull(.data$choices)

  
  for (i in 1 : nrow(balance)) {
    balance$tempChoices[i] <- as.integer(strsplit(balance$choices[i], " ")[[1]][2])
  }
  balance <- balance %>% 
    dplyr::arrange(.data$tempChoices) %>% 
    dplyr::select(-.data$tempChoices)
  
  distinctChoices <- balance$choices %>% unique()
  choicesPlot <- list()
  for (i in 1:length(distinctChoices)) {
    filteredData <- balance %>% 
      dplyr::filter(.data$choices == distinctChoices[i])
    choicesPlot[[i]] <- plotly::plot_ly(filteredData, x = ~ mean1, y = ~ mean2, text = ~ tooltip, 
                                        hoverinfo = 'text', type = 'scatter', 
                                        height = max(1, ceiling(length(distinctChoices) / 5)) * 450, showlegend = ifelse(i == 1, T, F),
                                        mode = "markers", color = ~ domain, colors = ~colors, 
                                        opacity = 0.5, marker = list(size = 15, line = list(color = 'rgb(255,255,255)', width = 1))) %>%
     
      plotly::layout(
        xaxis = list(range = c(0, 1)),
        yaxis = list(range = c(0, 1)),
        legend = list(orientation = 'h', x = 0.3, y = -0.15 +  0.0145 * max(1,ceiling(length(distinctChoices)/5))),
        annotations = list(
                       x = 0.5 ,
                       y = 1.05,
                       text = distinctChoices[[i]],
                       showarrow = F,
                       xanchor = "center",
                       yanchor = "middle",
                       xref = 'paper',
                       yref = 'paper'
                     ),
        hoverlabel = list(
          bgcolor = "rgba(255,255,255,0.8)",
          font = list(
            color = "black"
          )
        )) %>% 
      plotly::add_segments(x = 0, y = 0, xend = 1, yend = 1, showlegend = F,
                           line = list(width = 0.5, color = "rgb(160,160,160)", dash = "dash"),
                           marker = list(size = 0))
      
  }
  m <- list(
    l = 100,
    r = 0,
    b = 150,
    t = 50
  )
  plot <- plotly::subplot(choicesPlot, nrows = max(1,ceiling(length(distinctChoices)/5)), shareY = TRUE, margin = 0.01) %>% 
    plotly::layout(
                   # yaxis = list(title = list(text =  paste("Covariate Mean in ", yCohort),
                   #                           font = list(size = 18))),
                   annotations = list(
                     x = c(0.5, -0.04, 0.5) ,
                     y = c(-0.125 +  0.01 * max(1,ceiling(length(distinctChoices)/5)), 0.5, -0.25 +  0.01 * max(1,ceiling(length(distinctChoices)/5))),
                     text = c(
                       paste("Covariate Mean in Target (", xCohort,")"), 
                       paste("Covariate Mean in Comparator (", yCohort,")"),
                       paste("Target - ", xCohort, " : ", targetName,", Comparator - ", yCohort, " : ", comparatorName,"\n",
                             "Database - ", selectedDatabaseId)),
                     showarrow = F,
                     xanchor = "center",
                     yanchor = "middle",
                     xref = 'paper',
                     yref = 'paper',
                     font = list(size = c(18,18,12)),
                     textangle = c(0, -90, 0)
                   ),
                   margin = m) 
    
    
  
  # plot <-
  #   ggplot2::ggplot(balance,
  #                   ggplot2::aes(
  #                     x = .data$mean1,
  #                     y = .data$mean2,
  #                     color = .data$domain
  #                   )) +
  #   ggiraph::geom_point_interactive(
  #     ggplot2::aes(tooltip = .data$tooltip),
  #     size = 3,
  #     shape = 16,
  #     alpha = 0.5
  #   ) +
  #   ggplot2::geom_abline(slope = 1,
  #                        intercept = 0,
  #                        linetype = "dashed") +
  #   ggplot2::geom_hline(yintercept = 0) +
  #   ggplot2::geom_vline(xintercept = 0) +
  #   ggplot2::xlab(paste("Covariate Mean in ", xCohort)) +
  #   ggplot2::ylab(paste("Covariate Mean in ", yCohort)) +
  #   # ggplot2::scale_x_continuous("Mean") +
  #   # ggplot2::scale_y_continuous("Mean") +
  #   ggplot2::scale_color_manual("Domain", values = colors) +
  #   ggplot2::facet_grid(cols = ggplot2::vars(choices)) + # need to facet by 'startDay' that way it is arranged in numeric order.
  #   # but labels should be based on choices
  #   # ggplot2::facet_wrap(~choices) +
  #   ggplot2::theme(
  #     strip.background = ggplot2::element_blank(),
  #     panel.spacing = ggplot2::unit(2, "lines")
  #   ) +
  #   ggplot2::xlim(xLimitMin, xLimitMax) +
  #   ggplot2::ylim(yLimitMin, yLimitMax)
  # 
  # numberOfTimeIds <- balance$timeId %>% unique() %>% length()
  # 
  # plot <- ggiraph::girafe(
  #   ggobj = plot,
  #   options = list(ggiraph::opts_sizing(rescale = TRUE)),
  #   width_svg = max(8, 3 * numberOfTimeIds),
  #   height_svg = 3
  # )
  return(plot)
}



### cohort overlap plot ##############

plotCohortOverlap <- function(data,
                              shortNameRef = NULL,
                              yAxis = "Percentages") {
  # Perform error checks for input variables
  # errorMessage <- checkmate::makeAssertCollection()
  # checkmate::assertTibble(
  #   x = data,
  #   any.missing = FALSE,
  #   min.rows = 1,
  #   min.cols = 6,
  #   null.ok = FALSE,
  #   add = errorMessage
  # )
  # checkmate::reportAssertions(collection = errorMessage)
  # checkmate::assertNames(
  #   x = colnames(data),
  #   must.include = c(
  #     "databaseId",
  #     "targetCohortId",
  #     "comparatorCohortId",
  #     "tOnlySubjects",
  #     "cOnlySubjects",
  #     "bothSubjects"
  #   ),
  #   add = errorMessage
  # )
  # checkmate::reportAssertions(collection = errorMessage)
 

  
  data <- data %>%
    addShortName(
      shortNameRef = shortNameRef,
      cohortIdColumn = "targetCohortId",
      shortNameColumn = "targetShortName"
    ) %>%
    addShortName(
      shortNameRef = shortNameRef,
      cohortIdColumn = "comparatorCohortId",
      shortNameColumn = "comparatorShortName"
    )
  targetCohortCompoundName <- data  %>% 
    dplyr::inner_join(cohort %>% 
                        dplyr::mutate(targetCohortId = .data$cohortId) %>% 
                        dplyr::select(.data$targetCohortId,.data$compoundName),
                      by = "targetCohortId") %>% 
    dplyr::pull(.data$compoundName) %>% unique()
  targetCohortCompoundName <- paste("Target Cohorts :",paste(targetCohortCompoundName,collapse = ","))
  
  comparatorCohortCompoundName <- data  %>% 
    dplyr::inner_join(cohort %>% 
                        dplyr::mutate(comparatorCohortId = .data$cohortId) %>% 
                        dplyr::select(.data$comparatorCohortId,.data$compoundName),
                      by = "comparatorCohortId") %>% 
    dplyr::pull(.data$compoundName) %>% unique()
  comparatorCohortCompoundName <- paste("Comparator Cohorts :",paste(comparatorCohortCompoundName,collapse = ","))                    
  
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
    )
  
  distinctComparatorShortName <- plotData$comparatorShortName %>% unique()
  # distinctTargetShortName <- plotData$targetShortName %>% unique()
  distinctDatabaseIds <- plotData$databaseId %>% unique()
  databasePlots <- list()
  for (i in 1:length(distinctDatabaseIds)) {
    plotDataFilteredByDatabaseId <- plotData %>% 
      dplyr::filter(.data$databaseId == distinctDatabaseIds[i])
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
                                              name = ~subjectsIn, text = ~tooltip, hoverinfo = 'text',
                                              color = ~subjectsIn, colors = c( rgb(0.4, 0.4, 0.9), rgb(0.3, 0.2, 0.4),rgb(0.8, 0.2, 0.2)),
                                              showlegend = showLegend, height = max(400, 250 * length(distinctComparatorShortName))) %>%
        plotly::layout(barmode = 'stack',
                       legend = list(orientation = "h",x = 0.4),
                       xaxis = list(range = c(0, xAxisMax),
                                    showticklabels = xAxisTickLabels,
                                    tickformat = xAxisTickFormat),
                       yaxis = list(showticklabels = yAxisTickLabels),
                       annotations = list(
                         x = rep(1 + length(distinctDatabaseIds) * 0.01,length(distinctTargetShortName)) ,
                         y = seq(annotationStartValue, annotationEndValue, annotationInterval),
                         text = rep(ifelse(i == length(distinctDatabaseIds),distinctComparatorShortName[j],""),length(distinctTargetShortName)),
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
                       text = distinctDatabaseIds[i],
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
    b = 200,
    t = 50
  )
  plot <- plotly::subplot(databasePlots) %>% 
    plotly::layout(annotations = list(
      x = 0.5 ,
      y = -0.4 + (0.055 * length(distinctComparatorShortName)),
      text = paste(targetCohortCompoundName,"\n",comparatorCohortCompoundName),
      showarrow = F,
      xanchor = "center",
      yanchor = "middle",
      xref = 'paper',
      yref = 'paper',
      font = list(size = 14)
    ),margin = m)
  
  # plotData$targetShortName <- factor(plotData$targetShortName,
  #                                    levels = sortTargetShortName$targetShortName)
  # 
  # plotData$comparatorShortName <-
  #   factor(plotData$comparatorShortName,
  #          levels = sortComparatorShortName$comparatorShortName)
  
  # plot <- ggplot2::ggplot(data = plotData) +
  #   ggplot2::aes(
  #     fill = .data$subjectsIn,
  #     y = .data$targetShortName,
  #     x = .data$value,
  #     tooltip = .data$tooltip,
  #     group = .data$subjectsIn
  #   ) +
  #   ggplot2::ylab(label = "") +
  #   ggplot2::xlab(label = "") +
  #   ggplot2::scale_fill_manual("Subjects in", values = c(rgb(0.8, 0.2, 0.2), rgb(0.3, 0.2, 0.4), rgb(0.4, 0.4, 0.9))) +
  #   ggplot2::facet_grid(comparatorShortName ~ databaseId) +
  #   ggplot2::theme(
  #     panel.background = ggplot2::element_blank(),
  #     strip.background = ggplot2::element_blank(),
  #     panel.grid.major.x = ggplot2::element_line(color = "gray"),
  #     axis.ticks.y = ggplot2::element_blank(),
  #     panel.spacing = ggplot2::unit(2, "lines")
  #   ) +
  #   ggiraph::geom_bar_interactive(position = position,
  #                                 alpha = 0.6,
  #                                 stat = "identity")
  # if (yAxis == "Percentages") {
  #   plot <- plot + ggplot2::scale_x_continuous(labels = scales::percent)
  # } else {
  #   plot <-
  #     plot + ggplot2::scale_x_continuous(labels = scales::comma, n.breaks = 3)
  # }
  # width <- length(unique(plotData$databaseId))
  # height <-
  #   nrow(
  #     plotData %>% dplyr::select(.data$targetShortName, .data$comparatorShortName) %>% dplyr::distinct()
  #   )
  # plot <- ggiraph::girafe(
  #   ggobj = plot,
  #   options = list(ggiraph::opts_sizing(rescale = TRUE)),
  #   width_svg = max(12, 2 * width),
  #   height_svg = max(2, 0.5 * height)
  # )
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
