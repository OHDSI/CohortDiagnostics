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

plotTimeDistribution <- function(data) {
  
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(x = data, 
                          any.missing = FALSE,
                          min.rows = 1,
                          min.cols = 5,
                          max.cols = 5,
                          null.ok = FALSE,
                          add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  plot <- ggplot2::ggplot(data, ggplot2::aes(x = x,
                                             ymin = Min,
                                             lower = P25,
                                             middle = Median,
                                             upper = P75,
                                             ymax = Max)) +
    ggplot2::geom_errorbar(ggplot2::aes(ymin = Min, ymax = Min), size = 1) +
    ggplot2::geom_errorbar(ggplot2::aes(ymin = Max, ymax = Max), size = 1) +
    ggplot2::geom_boxplot(stat = "identity", fill = rgb(0, 0, 0.8, alpha = 0.25), size = 1) +
    ggplot2::facet_grid(Database~TimeMeasure, scale = "free") +
    ggplot2::coord_flip() +
    ggplot2::theme(panel.grid.major.y = ggplot2::element_blank(),
                   panel.grid.minor.y = ggplot2::element_blank(),
                   axis.title.y = ggplot2::element_blank(),
                   axis.ticks.y = ggplot2::element_blank(),
                   axis.text.y = ggplot2::element_blank())
  return(plot)
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
  if (!is.null(data$Database) && length(data$Database) > 1) {
    if (yscaleFixed) {
      scales <- "fixed"
    } else {
      scales <- "free_y"
    }
    if (stratifyByAge) {
      plot <- plot + ggplot2::facet_grid(Database~ageGroup, scales = scales)
    } else {
      plot <- plot + ggplot2::facet_grid(Database~., scales = scales) 
    }
  } else {
    if (stratifyByAge) {
      plot <- plot + ggplot2::facet_grid(~ageGroup) 
    }
  }
  if (!is.null(fileName)) {
    ggplot2::ggsave(filename = fileName, plot = plot, width = 5, height = 3.5, dpi = 400)
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
