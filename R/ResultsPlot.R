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

getTimeDistributionPlot <- function(data) {
  
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
}