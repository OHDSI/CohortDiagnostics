# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Get ggplot object with time distribution plot.
#'
#' @description
#' Get ggplot object with time distribution plot.
#'
#' @param data   A tibble data frame object that is the output of \code{\link{getTimeDistribution}} function.
#' @param cohortIds A vector of one or more integer (bigint) to plot.
#' @param databaseIds A vector of one or more databaseIds to plot.
#' @param xAxis       (optional) By default 'database' will be plotted on x-axis. Alternative is 'cohortId'.
#' 
#' @return
#' A ggplot object.
#'
#' @examples
#' \dontrun{
#' timeDistributionPlot <- getTimeDistributionPlot(data = data)
#' }
#'
#' @export

plotTimeDistribution <- function(data, 
                                 cohortIds = NULL,
                                 databaseIds = NULL,
                                 xAxis = 'database') {
  
  if (is.null(cohortIds) || length(cohortIds) > 1 || xAxis != 'database' || length(cohortIds) != 1) {
    ParallelLogger::logWarn("Not yet supported. Upcoming feature.")
    return(NULL)
  }
  
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(x = data, 
                          any.missing = FALSE,
                          min.rows = 1,
                          min.cols = 5,
                          null.ok = FALSE,
                          add = errorMessage)
  checkmate::assertIntegerish(x = cohortIds,
                              lower = 1,
                              upper = 2^53,
                              any.missing = FALSE,
                              null.ok = TRUE, 
                              min.len = 1,
                              add = errorMessage)
  checkmate::assertCharacter(x = databaseIds,
                             any.missing = FALSE,
                             null.ok = TRUE, 
                             min.len = 1, 
                             unique = TRUE,
                             add = errorMessage)
  checkmate::assertChoice(x = xAxis,
                         choices = c('database', 'cohortId'),
                         add = errorMessage)
  checkmate::assertNames(x = colnames(data), 
                         must.include = c('Min', 'P25', 'Median', 'P75', 'Max'),
                         add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  plotData <- data 
  if (!is.null(cohortIds)) {
    plotData <- plotData %>% 
    dplyr::filter(.data$cohortId %in% !!cohortIds)
  }
  if (!is.null(databaseId)) {
    plotData <- plotData %>% 
      dplyr::filter(.data$databaseId %in% !!databaseId)
  }
  
  plot <- ggplot2::ggplot(data = plotData) +
    ggplot2::aes(x = .data$Database,
                 ymin = .data$Min,
                 lower = .data$P25,
                 middle = .data$Median,
                 upper = .data$P75,
                 ymax = .data$Max) +
    ggplot2::geom_errorbar(ggplot2::aes(ymin = Max, ymax = Max), size = 1) +
    ggplot2::geom_boxplot(stat = "identity", fill = rgb(0, 0, 0.8, alpha = 0.25), size = 1) +
    ggplot2::facet_grid(Database~TimeMeasure, scale = "free") +
    ggplot2::coord_flip() +
    ggplot2::theme(panel.grid.major.y = ggplot2::element_blank(),
                   panel.grid.minor.y = ggplot2::element_blank(),
                   axis.title.y = ggplot2::element_blank(),
                   axis.ticks.y = ggplot2::element_blank(),
                   axis.text.y = ggplot2::element_blank())
  # plot <- plotly::ggplotly(plot)
  # This does not work as described here https://github.com/ropensci/plotly/issues/565 
  return(plot)
  
  # how to render using pure plot ly. Plotly does not prefer precomputed data.
  # TO DO: color and plot positions are not consistent yet.
  # plot <- plotly::plot_ly(data = plotData,
  #                         type = 'box',
  #                         median = plotData$P25,
  #                         #Mean = plotData$Average,
  #                         upperfence = plotData$Max,
  #                         lowerfence = plotData$Min,
  #                         split = plotData$TimeMeasure)
  # loop thru database or cohorts as needed
  # then subplot
  # plot <- plotly::subplot(plots,nrows = length(input$databases),margin = 0.05)
}


#' Get ggplot object with incidence rate plot.
#'
#' @description
#' Get ggplot object with incidence rate plot.
#'
#' @param data   A tibble data frame object that is the output of \code{\link{getIncidenceRate}} function. 
#' @param stratifyByAge Do you want to stratify by age?  
#' @param stratifyByGender Do you want to stratify by gender?
#' @param stratifyByCalendarYear Do you want to stratify by calendar year?
#' @param yscaleFixed Do you want to rescale y-axis?
#' 
#' @return
#' A ggplot object.
#'
#' @examples
#' \dontrun{
#' timeDistributionPlot <- getTimeDistributionPlot(data = data)
#' }
#'
#' @export
plotIncidenceRate <- function(data,
                              stratifyByAge = TRUE,
                              stratifyByGender = TRUE,
                              stratifyByCalendarYear = TRUE,
                              yscaleFixed = FALSE) {
  
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
    } else {
      aesthetics$x <- "dummy"
      showX <- FALSE
    }
    plotType <- "bar"
  }
  
  plot <- ggplot2::ggplot(data = data, do.call(what = ggplot2::aes_string, args = aesthetics)) +
    ggplot2::xlab(label = xLabel) +
    ggplot2::ylab(label = "Incidence Rate (/1,000 person years)") +
    ggplot2::theme(legend.position = "top",
                   legend.title = ggplot2::element_blank(),
                   axis.text.x = if (showX) {ggplot2::element_text(angle = 90, vjust = 0.5)
                   } else {ggplot2::element_blank()})
  
  if (plotType == "line") {
    plot <- plot + ggplot2::geom_line(size = 1.25, alpha = 0.6) +
      ggplot2::geom_point(size = 1.25, alpha = 0.6)
  } else {
    plot <- plot + ggplot2::geom_bar(stat = "identity", alpha = 0.6)
  }
  
  # databaseId field only present when called in Shiny app:
  if (!is.null(data$databaseId) && length(data$databaseId) > 1) {
    if (yscaleFixed) {
      scales <- "fixed"
    } else {
      scales <- "free_y"
    }
    if (stratifyByAge) {
      plot <- plot + ggplot2::facet_grid(databaseId~ageGroup, scales = scales)
    } else {
      plot <- plot + ggplot2::facet_grid(databaseId~., scales = scales) 
    }
  } else {
    if (stratifyByAge) {
      plot <- plot + ggplot2::facet_grid(~ageGroup) 
    }
  }
  return(plot)
}

#' Get Plotly object with cohort comparison plot.
#'
#' @description
#' Get Plotly object with cohort comparison plot.
#'
#' @param 
#' balance   A tibble data frame object that is the output of \code{\link{getCohortCompare}} function.  
#' cohortId   A input value given, when the user select the cohort in the shiny app.
#' comparatorId   A input value given, when the user select the comparator in the shiny app.               
#' 
#' @return
#' A Plotly object.
#'
#' @examples
#' \dontrun{
#' plotCohortCompare <- getCohortCompare(data = data)
#' }
#'
#' @export

plotCohortCompare <- function(balance, cohortId, comparatorId) {
  
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(x = data, 
                          any.missing = FALSE,
                          min.rows = 1,
                          min.cols = 5,
                          null.ok = FALSE,
                          add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  if (nrow(balance) == 0) {
    return(NULL)
  }
  balance$mean1[is.na(balance$mean1)] <- 0
  balance$mean2[is.na(balance$mean2)] <- 0
  data <- balance[sample(nrow(balance), 1000), ]
  
  xAxisLabel <- list(
    title = cohortId,
    range = c(0, 1)
  )
  yAxisLabel <- list(
    title = comparatorId,
    range = c(0, 1)
  )
  plot <- plotly::plot_ly(
    balance, x = balance$mean1, y = balance$mean2,
    # Hover text:
    text = ~paste("Mean Target: ", balance$mean1, '<br>Mean Comparator:', balance$mean2,'<br>Std diff.:', balance$stdDiff),
    color = ~balance$absStdDiff,
    type   = 'scatter', 
    mode   = 'markers',
    marker = list(size = 10, 
                  opacity = "0.5"))
  plot <- plot %>% plotly::layout(shapes = list(type = "line",
                                                y0 = 0, 
                                                y1 = 1, 
                                                yref = "paper",
                                                x0 = 0,  
                                                x1 = 1, 
                                                line = list(color = "red", 
                                                            dash = "dash")))
  plot <- plot %>% plotly::layout(xaxis = xAxisLabel, yaxis = yAxisLabel, showlegend = FALSE)
  plot <- plot %>% plotly::colorbar(title = "Absolute\nStd. Diff.")
  return(plot)
}


#' Get Vendiagram object with cohort Overlap plot.
#'
#' @description
#' Get Vendiagram  object with cohort Overlap plot.
#'
#' @param 
#' data   A tibble data frame object that is the output of \code{\link{getCohortOverlap}} function.  
#' 
#' @return
#' A Vendiagram object.
#'
#' @examples
#' \dontrun{
#' plotCohortOverlap <- getCohortOverlap(data = data)
#' }
#' 
#' @export
plotCohortOverlap <- function(data) {
  
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(x = data, 
                          any.missing = FALSE,
                          min.rows = 1,
                          min.cols = 5,
                          null.ok = FALSE,
                          add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
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
  
  # BELOW SCRIPT REPLACES THE VENDIAGRAM WITH STACKED HISTOGRAM. BUT TAKES LONG TIME TO DISPLAY THE PLOT.
  # CHECK GENERATE HISTOGRAM FUNCTION TO GENERATE THE DATA FOR HISTOGRAM PLOTS
  # REFERENCE USED :
  # 1. https://stackoverflow.com/questions/20184096/how-to-plot-multiple-stacked-histograms-together-in-r
  # 2. https://stackoverflow.com/questions/43415709/how-to-use-facet-grid-with-geom-histogram
  # 3. https://www.datacamp.com/community/tutorials/facets-ggplot-r?utm_source=adwords_ppc&utm_campaignid=1455363063&utm_adgroupid=65083631748&utm_device=c&utm_keyword=&utm_matchtype=b&utm_network=g&utm_adpostion=&utm_creative=332602034361&utm_targetid=dsa-429603003980&utm_loc_interest_ms=&utm_loc_physical_ms=1007768&gclid=CjwKCAjw19z6BRAYEiwAmo64LQMUJwf1i0V-Zgc5hYhpDOFQeZU05reAJmQvo2-mClFWWM4_sJiSmBoC-YkQAvD_BwE
  # 4. https://stackoverflow.com/questions/24123499/frequency-histograms-with-facets-calculating-percent-by-groups-used-in-facet-i
  # 5. https://stackoverflow.com/questions/62821480/add-a-trace-to-every-facet-of-a-plotly-figure
  
  # ComparatorOnlySubjs <- generateHistogramValues(len = seq(1:nrow(data)), val = data$cOnlySubjects)
  # bothSubjs <- generateHistogramValues(seq(1:nrow(data)), data$bothSubjects)
  # cohortOnlySubjs <- generateHistogramValues(seq(1:nrow(data)), data$tOnlySubjects)
  # bucket <- list(ComparatorOnlySubjs = ComparatorOnlySubjs, bothSubjs = bothSubjs, cohortOnlySubjs = cohortOnlySubjs)
  # 
  # 
  # p <- ggplot2::ggplot(reshape::melt(bucket), ggplot2::aes(value, fill = L1)) +
  #   ggplot2::xlab(label = "Comparators") +
  #   ggplot2::geom_histogram(position = "stack", binwidth = 1) +
  #   ggplot2::xlim(c(0,max(length(comparatorCohortIds()),10))) +
  #   ggplot2::facet_grid(rows = ggplot2::vars(data$targetCohortId), cols = ggplot2::vars(data$databaseId), scales = "free_y")
  # plot <- plotly::ggplotly(p)
  # GENERATE HISTOGRAM FUNCTION
  # generateHistogramValues <- function(len,val)
  # {
  #   fillVal <- c()
  #   
  #   inc <- 1
  #   for (i in len)
  #   {
  #     fillVal <- c(fillVal,rep(i,val[[i]]))
  #   }
  #   return(fillVal);
  # }
}
