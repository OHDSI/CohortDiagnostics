library(magrittr)
plotincidenceRate <- function(data,
                              stratifyByAge = TRUE,
                              stratifyByGender = TRUE,
                              stratifyByCalendarYear = TRUE,
                              yscaleFixed = FALSE,
                              fileName = NULL) {
  
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
  if (!is.null(fileName)) {
    ggplot2::ggsave(filename = fileName, plot = plot, width = 5, height = 3.5, dpi = 400)
  }
  return(plot)
}
