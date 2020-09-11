
#' @export

dataForTimeDistributionPlot <- function(connection = NULL,
                                       selectedCohort = NULL,
                                       selectedDatabaseIds = NULL,
                                       specifications = NULL){
  
  data <- specifications %>% 
    dplyr::filter(.data$cohortDefinitionId == selectedCohort &
                    .data$databaseId %in% selectedDatabaseIds)
  
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
}