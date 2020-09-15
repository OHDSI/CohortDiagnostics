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
#' @param data   A tibble data frame object that is the output of \code{\link{getTimeDistributionResult}} function.
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
  
  if (is.null(cohortIds) || length(cohortIds) > 1 || xAxis != 'database' || is.null(databaseIds)) {
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
  if (!is.null(databaseIds)) {
    plotData <- plotData %>% 
      dplyr::filter(.data$Database %in% !!databaseIds)
  }
  
  plot <- ggplot2::ggplot(data = plotData) +
    ggplot2::aes(x = .data$Database,
                 ymin = .data$Min,
                 lower = .data$P25,
                 middle = .data$Median,
                 upper = .data$P75,
                 ymax = .data$Max) +
    ggplot2::geom_errorbar(mapping = ggplot2::aes(ymin = .data$Min, ymax = .data$Max), size = 1) +
    ggplot2::geom_boxplot(stat = "identity", 
                          fill = rgb(0, 0, 0.8, alpha = 0.25), 
                          size = 1) +
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
}  
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



#' Get ggplot object with incidence rate plot.
#'
#' @description
#' Get ggplot object with incidence rate plot.
#'
#' @param data                   A tibble data frame object that is the output 
#'                               of \code{\link{getIncidenceRate}} function.
#' @param cohortIds              A vector of one or more integer (bigint) to plot.
#' @param databaseIds            A vector of one or more databaseIds to plot. 
#' @param stratifyByAgeGroup     Do you want to stratify by age?  
#' @param stratifyByGender       Do you want to stratify by gender?
#' @param stratifyByCalendarYear Do you want to stratify by calendar year?
#' @param yscaleFixed            Do you want to rescale y-axis?
#' 
#' @return
#' A ggplot object.
#'
#' @examples
#' \dontrun{
#' incidenceRatePlot <- plotIncidenceRate(data = data)
#' }
#'
#' @export
plotIncidenceRate <- function(data,
                              cohortIds = NULL,
                              databaseIds = NULL,
                              stratifyByAgeGroup = TRUE,
                              stratifyByGender = TRUE,
                              stratifyByCalendarYear = TRUE,
                              yscaleFixed = FALSE) {
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(x = data, 
                          any.missing = TRUE,
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
  checkmate::assertLogical(x = stratifyByAgeGroup, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           null.ok = FALSE,
                           add = errorMessage)
  checkmate::assertLogical(x = stratifyByGender, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           null.ok = FALSE,
                           add = errorMessage)
  checkmate::assertLogical(x = stratifyByCalendarYear, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           null.ok = FALSE,
                           add = errorMessage)
  checkmate::assertLogical(x = yscaleFixed, 
                           any.missing = FALSE, 
                           min.len = 1, 
                           null.ok = FALSE,
                           add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  plotData <- data
  if (!is.null(cohortIds)) {
    plotData <- plotData %>% 
      dplyr::filter(.data$cohortId %in% !!cohortIds)
  }
  if (!is.null(databaseIds)) {
    plotData <- plotData %>% 
      dplyr::filter(.data$databaseId %in% !!databaseIds)
  }
  plotData <- plotData %>% 
    dplyr::mutate(strataGender = !is.na(.data$gender),
                  strataAgeGroup = !is.na(.data$ageGroup),
                  strataCalendarYear = !is.na(.data$calendarYear)) %>% 
    dplyr::filter(.data$strataGender %in% !!stratifyByGender &
                    .data$strataAgeGroup %in% !!stratifyByAgeGroup &
                    .data$strataCalendarYear %in% !!stratifyByCalendarYear) %>% 
    dplyr::select(-tidyselect::starts_with('strata')) %>% 
    tidyr::tibble()
  
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
  
  plot <- ggplot2::ggplot(data = plotData, 
                          do.call(what = ggplot2::aes_string, args = aesthetics)) +
    ggplot2::xlab(label = xLabel) +
    ggplot2::ylab(label = "Incidence Rate (/1,000 person years)") +
    ggplot2::theme(legend.position = "top",
                   legend.title = ggplot2::element_blank(),
                   axis.text.x = if (showX) {
                     ggplot2::element_text(angle = 90, vjust = 0.5)
                   } else {
                     ggplot2::element_blank()}
    )
  
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
    if (stratifyByAgeGroup) {
      plot <- plot + ggplot2::facet_grid(databaseId~ageGroup, scales = scales)
    } else {
      plot <- plot + ggplot2::facet_grid(databaseId~., scales = scales) 
    }
  } else {
    if (stratifyByAgeGroup) {
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
#' @param  data                  A tibble data frame object that is the output 
#'                               of \code{\link{compareCovariateValueResult}} function.
#' @template DatabaseIds
#' @param targetCohortIds        (optional) A vector of one or more Cohort Ids.
#' @param comparatorCohortIds    (optional) A vector of one or more Cohort Ids.
#' @param cohortReference        (optional) A tibble data frame object returned 
#'                               from \code{\link{getCohortReference}} function.
#' @param covariateReference     (optional) A tibble data frame object returned from \code{\link{getCovariateReference}} function.
#' @param concept                (optional) A tibble data frame object returned 
#'                               from \code{\link{getConceptReference}} function.
#' 
#' @return
#' A Plotly object.
#'
#' @examples
#' \dontrun{
#' plotCohortCompare <- plotCohortComparisonStandardizedDifference(data = data)
#' }
#'
#' @export
plotCohortComparisonStandardizedDifference <- function(data,
                                                       targetCohortIds = NULL, 
                                                       comparatorCohortIds = NULL,
                                                       cohortReference = NULL,
                                                       covariateReference = NULL,
                                                       concept = NULL, # to subset based on domain, or vocabulary
                                                       databaseIds = NULL) {
  if (!is.null(concept)) {
    ParallelLogger::logWarn("Not yet supported. Upcoming feature. Ignorning for now. Continuing.")
  }
  
  # for now we will support only one combination of targetCohortId, comparatorCohortId and databaseId
  if (length(targetCohortIds) > 1 || length(comparatorCohortIds) > 1 || length(databaseIds) > 1) {
    ParallelLogger::logWarn("Not yet supported. Upcoming feature. Executing with first choices only")
    targetCohortIds <- targetCohortIds[[1]]
    comparatorCohortIds <- comparatorCohortIds[[1]]
    databaseIds <- databaseIds[[1]]
    return(NULL)
  }
  
  
  plotData <- data
  if (!is.null(targetCohortIds)) {
    plotData <- plotData %>% 
      dplyr::filter(.data$targetCohortId %in% !!targetCohortIds)
  }
  if (!is.null(comparatorCohortIds)) {
    plotData <- plotData %>% 
      dplyr::filter(.data$comparatorCohortId %in% !!comparatorCohortIds)
  }
  if (!is.null(databaseIds)) {
    plotData <- plotData %>% 
      dplyr::filter(.data$databaseId %in% !!databaseIds)
  }
  

  
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(x = plotData, 
                          any.missing = FALSE,
                          min.rows = 1,
                          min.cols = 11,
                          null.ok = FALSE,
                          types = c('character', 'double'),
                          add = errorMessage)
  checkmate::assertIntegerish(x = targetCohortIds,
                              lower = 1,
                              upper = 2^53, 
                              any.missing = FALSE,
                              null.ok = FALSE)
  checkmate::assertIntegerish(x = comparatorCohortIds,
                              lower = 1,
                              upper = 2^53, 
                              any.missing = FALSE,
                              null.ok = FALSE)
  checkmate::assertCharacter(x = databaseIds,
                             any.missing = FALSE,
                             min.len = 1,
                             null.ok = TRUE
  )
  checkmate::assertNames(x = colnames(plotData),
                         must.include = c("databaseId","targetCohortId","comparatorCohortId","covariateId",
                                          "mean1","sd1","mean2","sd2","sd","stdDiff", "absStdDiff"),
                         add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  if (!is.null(cohortReference)) {
    checkmate::assertTibble(x = cohortReference, 
                            any.missing = FALSE,
                            min.rows = 1,
                            min.cols = 2,
                            null.ok = FALSE,
                            types = c('character',
                                      'double'),
                            add = errorMessage)
    checkmate::assertNames(x = colnames(cohortReference),
                           must.include = c("cohortId",
                                            "cohortName"),
                           add = errorMessage
    )
  }
  if (!is.null(covariateReference)) {
    checkmate::assertTibble(x = covariateReference, 
                            any.missing = FALSE,
                            min.rows = 1,
                            min.cols = 3,
                            null.ok = FALSE,
                            types = c('character', 'double'),
                            add = errorMessage)
    checkmate::assertNames(x = colnames(covariateReference),
                           must.include = c("covariateId",
                                            "covariateName",
                                            "conceptId"),
                           add = errorMessage
    )
  }
  checkmate::reportAssertions(collection = errorMessage)
  if (!is.null(concept)) {
    checkmate::assertTibble(x = concept, 
                            any.missing = TRUE,
                            min.rows = 1,
                            min.cols = 5,
                            null.ok = FALSE,
                            types = c('character',
                                      'double'),
                            add = errorMessage)
    checkmate::assertNames(x = colnames(concept),
                           must.include = c("conceptId",
                                            "conceptName",
                                            "domainId",
                                            "vocabularyId",
                                            "conceptClassId"),
                           add = errorMessage
    )
  }
  checkmate::reportAssertions(collection = errorMessage)
  
  # when we support more than 1 targetCohortIds, comparatorCohortIds and DatabaseIds -- this 
  # will be the begining of the iteration. 
  # For now we are only support one unique combination of 
  # databaseId, targetCohortId, comparatorCohortId
  # 
  
  if (!is.null(covariateReference)) {
    plotData <- plotData %>% 
      dplyr::left_join(y = covariateReference %>% 
                         dplyr::select(.data$covariateId, .data$covariateName))
  } else {
    plotData <- plotData %>% 
      dplyr::mutate(covariateName = .data$covariateId %>% as.character())
  }
  
  if (!is.null(cohortReference)) {
    xAxisLabel <- list(
      title = cohortReference %>% 
        dplyr::filter(.data$cohortId %in% targetCohortIds) %>% 
        dplyr::select(.data$cohortName) %>% 
        dplyr::pull(),
      range = c(0, 1)
    )
    yAxisLabel <- list(
      title = cohortReference %>% 
        dplyr::filter(.data$cohortId %in% comparatorCohortIds) %>% 
        dplyr::select(.data$cohortName) %>% 
        dplyr::pull(),
      range = c(0, 1)
    )
  } else {
    xAxisLabel <- list(
      title = targetCohortIds,
      range = c(0, 1)
    )
    yAxisLabel <- list(
      title = comparatorCohortIds,
      range = c(0, 1)
    )
  }
  
  plot <- plotly::plot_ly(data = plotData, 
                          x = plotData$mean1, 
                          y = plotData$mean2,
                          # Hover text:
                          text = ~paste("Covariate Name:",
                                        plotData$covariateName, 
                                        "<br>Mean Target: ", 
                                        plotData$mean1, 
                                        '<br>Mean Comparator:', 
                                        plotData$mean2,
                                        '<br>Std diff.:', 
                                        plotData$stdDiff),
                          color = ~plotData$absStdDiff,
                          type   = 'scatter',
                          mode   = 'markers',
                          marker = list(size = 10,
                                        opacity = "0.5")) %>% 
    plotly::layout(shapes = list(type = "line",
                                 y0 = 0, 
                                 y1 = 1, 
                                 yref = "paper",
                                 x0 = 0,  
                                 x1 = 1, 
                                 line = list(color = "red", 
                                             dash = "dash"))) %>% 
    plotly::layout(xaxis = xAxisLabel, 
                   yaxis = yAxisLabel, 
                   showlegend = FALSE) %>% 
    plotly::colorbar(title = "Absolute\nStd. Diff.")
  return(plot)
}


#' Get Vendiagram object with cohort Overlap plot.
#'
#' @description
#' Get Vendiagram  object with cohort Overlap plot.
#'
#' @param  data                  A tibble data frame object that is the output of 
#'                               \code{\link{getCohortOverlapResult}} function.
#' @template DatabaseIds
#' @param targetCohortIds        (optional) A vector of one or more Cohort Ids.
#' @param comparatorCohortIds    (optional) A vector of one or more Cohort Ids.
#' 
#' @return
#' A Vendiagram object.
#'
#' @examples
#' \dontrun{
#' plotCohortOverlapVennDiagram <- plotCohortOverlapVennDiagram(data = data)
#' }
#' 
#' @export
plotCohortOverlapVennDiagram <- function(data,
                                         targetCohortIds, 
                                         comparatorCohortIds,
                                         databaseIds) {
  
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(x = data, 
                          any.missing = FALSE,
                          min.rows = 1,
                          min.cols = 5,
                          null.ok = FALSE,
                          add = errorMessage)
  checkmate::assertIntegerish(x = targetCohortIds,
                              lower = 1,
                              upper = 2^53, 
                              any.missing = FALSE,
                              null.ok = FALSE)
  checkmate::assertIntegerish(x = comparatorCohortIds,
                              lower = 1,
                              upper = 2^53, 
                              any.missing = FALSE,
                              null.ok = FALSE)
  checkmate::assertCharacter(x = databaseIds,
                             any.missing = FALSE,
                             min.len = 1,
                             null.ok = TRUE
  )
  checkmate::reportAssertions(collection = errorMessage)
  
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
    plot[idx][[i]]$label <- format(as.numeric(plot[idx][[i]]$label), 
                                   big.mark = ",", 
                                   scientific = FALSE)
  }
  grid::grid.draw(plot)
  
  return(plot)
}  
# Future function getCohortOverlapHistogram:
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
#   ggplot2::facet_grid(rows = ggplot2::vars(data$targetCohortId), 
#   cols = ggplot2::vars(data$databaseId), scales = "free_y")
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

