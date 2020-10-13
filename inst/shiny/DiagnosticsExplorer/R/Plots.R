plotTimeDistribution <- function(data, 
                                 cohortIds = NULL,
                                 databaseIds = NULL,
                                 xAxis = "database") {
  
  if (is.null(cohortIds) || xAxis != "database" || is.null(databaseIds)) {
    warning("Not yet supported. Upcoming feature.")
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
  checkmate::assertDouble(x = cohortIds,
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
                          choices = c("database", "cohortId"),
                          add = errorMessage)
  checkmate::assertNames(x = colnames(data), 
                         must.include = c("Min", "P25", "Median", "P75", "Max"),
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
  
  plotData$tooltip <- c(paste0(plotData$cohortName,
                               "\nDatabase = ", plotData$Database, 
                               "\nMin = ", scales::comma(plotData$Min),
                               "\nMax = ", scales::comma(plotData$Max),
                               "\nP25 = ", scales::comma(plotData$P25),
                               "\nMedian = ", scales::comma(plotData$Median),
                               "\nP75 = ", scales::comma(plotData$P75),
                               "\nTime Measure = ",  plotData$TimeMeasure,
                               "\nAverage = ",  scales::comma(x = plotData$Average, accuracy = 0.01)))
  
  plot <- ggplot2::ggplot(data = plotData) +
    ggplot2::aes(x = .data$Database,
                 ymin = .data$Min,
                 lower = .data$P25,
                 middle = .data$Median,
                 upper = .data$P75,
                 ymax = .data$Max,
                 group = .data$TimeMeasure,
                 average = .data$Average) +
    ggplot2::geom_errorbar(mapping = ggplot2::aes(ymin = .data$Min, 
                                                  ymax = .data$Max), size = 0.5) +
    ggiraph::geom_boxplot_interactive(ggplot2::aes(tooltip = tooltip),
                                      stat = "identity", 
                                      fill = rgb(0, 0, 0.8, alpha = 0.25), 
                                      size = 0.2) +
    ggplot2::facet_grid(Database+shortName~TimeMeasure, scales = "free") +
    ggplot2::coord_flip() +
    ggplot2::theme(panel.grid.major.y = ggplot2::element_blank(),
                   panel.grid.minor.y = ggplot2::element_blank(),
                   axis.title.y = ggplot2::element_blank(),
                   axis.ticks.y = ggplot2::element_blank(),
                   axis.text.y = ggplot2::element_blank(),
                   strip.text.y.right = ggplot2::element_text(angle = 0)) 
  plot <- ggiraph::girafe(ggobj = plot,
                          options = list(
                            ggiraph::opts_sizing(width = .7),
                            ggiraph::opts_zoom(max = 5)),
                          width_svg = 12,
                          height_svg = 1.5 + 2*length(unique(databaseIds)))
  return(plot)
}  
# how to render using pure plot ly. Plotly does not prefer precomputed data.
# TO DO: color and plot positions are not consistent yet.
# plot <- plotly::plot_ly(data = plotData,
#                         type = "box",
#                         median = plotData$P25,
#                         #Mean = plotData$Average,
#                         upperfence = plotData$Max,
#                         lowerfence = plotData$Min,
#                         split = plotData$TimeMeasure)
# loop thru database or cohorts as needed
# then subplot
# plot <- plotly::subplot(plots,nrows = length(input$databases),margin = 0.05)


plotIncidenceRate <- function(data,
                              cohortIds = NULL,
                              databaseIds = NULL,
                              stratifyByAgeGroup = TRUE,
                              stratifyByGender = TRUE,
                              stratifyByCalendarYear = TRUE,
                              yscaleFixed = FALSE) {
  if (nrow(data) == 0) {
    ParallelLogger::logWarn("Record counts are too low to plot.")
  }
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(x = data, 
                          any.missing = TRUE,
                          min.rows = 1,
                          min.cols = 5,
                          null.ok = FALSE,
                          add = errorMessage)
  checkmate::assertDouble(x = cohortIds,
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
                           max.len = 1,
                           null.ok = FALSE,
                           add = errorMessage)
  checkmate::assertLogical(x = stratifyByGender, 
                           any.missing = FALSE, 
                           min.len = 1,  
                           max.len = 1,
                           null.ok = FALSE,
                           add = errorMessage)
  checkmate::assertLogical(x = stratifyByCalendarYear, 
                           any.missing = FALSE, 
                           min.len = 1,  
                           max.len = 1,
                           null.ok = FALSE,
                           add = errorMessage)
  checkmate::assertLogical(x = yscaleFixed, 
                           any.missing = FALSE, 
                           min.len = 1,  
                           max.len = 1,
                           null.ok = FALSE,
                           add = errorMessage)
  checkmate::assertDouble(x = data$incidenceRate,
                          lower = 0,
                          any.missing = FALSE,
                          null.ok = FALSE, 
                          min.len = 1,
                          add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  checkmate::assertDouble(x = data$incidenceRate,
                          lower = 0,
                          any.missing = FALSE,
                          null.ok = FALSE, 
                          min.len = 1,
                          add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  plotData <- data %>% 
    dplyr::mutate(incidenceRate = round(.data$incidenceRate, digits = 3))
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
    dplyr::select(-dplyr::starts_with("strata"))
  
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
    else{
      aesthetics$x <- "cohortId"
      showX <- FALSE
    }
    plotType <- "bar"
  }
  
  newSort <- plotData %>% 
    dplyr::select(.data$ageGroup) %>% 
    dplyr::distinct() %>% 
    dplyr::arrange(as.integer(sub(pattern = '-.+$','',x = .data$ageGroup)))
  
  plotData <- plotData %>% 
    dplyr::arrange(ageGroup = factor(.data$ageGroup, levels = newSort$ageGroup), .data$ageGroup)
  
  plotData$ageGroup <- factor(plotData$ageGroup,
                              levels = newSort$ageGroup)
  plotData$tooltip <- c(paste0(plotData$cohortName,"\n","Incidence Rate = ", scales::comma(plotData$incidenceRate, accuracy = 0.01), 
                               "\nDatabase = ", plotData$databaseId, 
                               "\nPerson years = ", scales::comma(plotData$personYears, accuracy = 0.1), 
                               "\nCohort count = ", scales::comma(plotData$cohortCount)))
  
  if (stratifyByAgeGroup) {
    plotData$tooltip <- c(paste0(plotData$tooltip, "\nAge Group = ", plotData$ageGroup))
  }
  
  if (stratifyByGender) {
    plotData$tooltip <- c(paste0(plotData$tooltip, "\nGender = ", plotData$gender))
  }
  
  if (stratifyByCalendarYear) {
    plotData$tooltip <- c(paste0(plotData$tooltip, "\nYear = ", plotData$calendarYear))
  }
  
  
  plot <- ggplot2::ggplot(data = plotData, 
                          do.call(ggplot2::aes_string, aesthetics)) +
    ggplot2::xlab(xLabel) +
    ggplot2::ylab("Incidence Rate (/1,000 person years)") +
    ggplot2::theme(legend.position = "top",
                   legend.title = ggplot2::element_blank(),
                   axis.text.x = if (showX) ggplot2::element_text(angle = 90, vjust = 0.5) else ggplot2::element_blank() )
  
  if (plotType == "line") {
    plot <- plot + 
      ggiraph::geom_line_interactive(ggplot2::aes(), size = 1, alpha = 0.6) +
      ggiraph::geom_point_interactive(ggplot2::aes(tooltip = tooltip), size = 2, alpha = 0.6)
  } else {
    plot <- plot + ggplot2::geom_bar(stat = "identity") +
      ggiraph::geom_col_interactive( ggplot2::aes(tooltip = tooltip), size = 1)
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
        plot <- plot + facet_nested(databaseId + shortName ~ plotData$ageGroup, scales = scales)
      } else {
        plot <- plot + facet_nested(databaseId + shortName ~ ., scales = scales) 
      }
    } else {
      plot <- plot + facet_nested(databaseId + shortName ~., scales = scales) 
    }
    spacing <- rep(c(1, rep(0.5, length(unique(plotData$shortName)) - 1)), length(unique(plotData$databaseId)))[-1]
    # plot <- plot + ggplot2::theme(panel.spacing.y = ggplot2::unit(spacing, "lines"),
    #                               strip.background = ggplot2::element_blank())
  } else {
    if (stratifyByAgeGroup) {
      plot <- plot + ggplot2::facet_grid(~ageGroup) 
    }
  }
  plot <- ggiraph::girafe(ggobj = plot,
                          options = list(
                            ggiraph::opts_sizing(width = .7),
                            ggiraph::opts_zoom(max = 5)),
                          width_svg = 15,
                          height_svg = 1.5 + 2*length(unique(data$databaseId)))
  return(plot)
}

plotCohortComparisonStandardizedDifference <- function(balance, 
                                                       domain = "all",
                                                       targetLabel = "Mean Target",
                                                       comparatorLabel = "Mean Comparator") {
  domains <- c("condition", "device", "drug", "measurement", "observation", "procedure")
  balance$domain <- tolower(stringr::str_extract(balance$covariateName, "[a-z]+"))
  balance$domain[!balance$domain %in% domains] <- "other"
  
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
  
  # ggiraph::geom_point_interactive(ggplot2::aes(tooltip = tooltip), size = 3, alpha = 0.6)
  balance$tooltip <- c(paste("Covariate Name:", balance$covariateName,
                             "\nDomain: ", balance$domain,
                             "\nMean Target: ", scales::comma(balance$mean1, accuracy = 0.1),
                             "\nMean Comparator:", scales::comma(balance$mean2, accuracy = 0.1),
                             "\nStd diff.:", scales::comma(balance$stdDiff, accuracy = 0.1)))
  
  # Code used to generate palette:
  # writeLines(paste(RColorBrewer::brewer.pal(n = length(domains), name = "Dark2"), collapse = "\", \""))
  
  # Make sure colors are consistent, no matter which domains are included:
  colors <- c("#1B9E77", "#D95F02", "#7570B3", "#E7298A", "#66A61E", "#E6AB02", "#444444")
  colors <- colors[c(domains, "other") %in% unique(balance$domain)]
  
  balance$domain <- factor(balance$domain, levels = c(domains, "other"))
  
  # targetLabel <- paste(strwrap(targetLabel, width = 50), collapse = "\n")
  # comparatorLabel <- paste(strwrap(comparatorLabel, width = 50), collapse = "\n")
  
  
  plot <- ggplot2::ggplot(balance, ggplot2::aes(x = .data$mean1, y = .data$mean2, color = .data$domain)) +
    ggiraph::geom_point_interactive(ggplot2::aes(tooltip = .data$tooltip), size = 3,shape = 16, alpha = 0.5) +
    ggplot2::geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
    ggplot2::geom_hline(yintercept = 0) +
    ggplot2::geom_vline(xintercept = 0) +             
    ggplot2::scale_x_continuous("") +
    ggplot2::scale_y_continuous("") +
    ggplot2::scale_color_manual("Domain", values = colors) +
    ggplot2::facet_grid(databaseId + targetCohort ~ comparatorCohort)
  
  plot <- ggiraph::girafe(ggobj = plot,
                          options = list(
                            ggiraph::opts_sizing(width = .7),
                            ggiraph::opts_zoom(max = 5)),width_svg = 12,
                          height_svg = 5)
  return(plot)
}


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
  checkmate::assertDouble(x = targetCohortIds,
                          lower = 1,
                          upper = 2^53, 
                          any.missing = FALSE,
                          null.ok = FALSE)
  checkmate::assertDouble(x = comparatorCohortIds,
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

plotCohortOverlap <- function(data,
                              yAxis = "Percentages",
                              cohortIdLength = 2) {
  
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(x = data, 
                          any.missing = FALSE,
                          min.rows = 1,
                          min.cols = 6,
                          null.ok = FALSE,
                          add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  checkmate::assertNames(x = colnames(data), 
                         must.include = c("databaseId",
                                          "targetCohortId",
                                          "comparatorCohortId",
                                          "tOnlySubjects",
                                          "cOnlySubjects",
                                          "bothSubjects"),
                         add = errorMessage)
  checkmate::reportAssertions(collection = errorMessage)
  
  plotData <- data %>% 
    dplyr::mutate(absTOnlySubjects = abs(.data$tOnlySubjects), 
                  absCOnlySubjects = abs(.data$cOnlySubjects),
                  absBothSubjects = abs(.data$bothSubjects),
                  absEitherSubjects = abs(.data$eitherSubjects),
                  signTOnlySubjects = dplyr::case_when(.data$tOnlySubjects < 0 ~ '<', TRUE ~ ''),
                  signCOnlySubjects = dplyr::case_when(.data$cOnlySubjects < 0 ~ '<', TRUE ~ ''),
                  signBothSubjects = dplyr::case_when(.data$bothSubjects < 0 ~ '<', TRUE ~ '')) %>% 
    dplyr::mutate(tOnlyString = paste0(.data$signTOnlySubjects, 
                                       scales::comma(.data$absTOnlySubjects), 
                                       " (", 
                                       .data$signTOnlySubjects, 
                                       scales::percent(.data$absTOnlySubjects/.data$absEitherSubjects, 
                                                       accuracy = 1),
                                       ")"),
                  cOnlyString = paste0(.data$signCOnlySubjects, 
                                       scales::comma(.data$absCOnlySubjects), 
                                       " (", 
                                       .data$signCOnlySubjects,
                                       scales::percent(.data$absCOnlySubjects/.data$absEitherSubjects, 
                                                       accuracy = 1),
                                       ")"),
                  bothString = paste0(.data$signBothSubjects, 
                                      scales::comma(.data$absBothSubjects), 
                                      " (", 
                                      .data$signBothSubjects,
                                      scales::percent(.data$absBothSubjects/.data$absEitherSubjects, 
                                                      accuracy = 1),
                                      ")"))  %>% 
    dplyr::mutate(tooltip = paste0("Database: ", .data$databaseId,
                                   "\n", .data$targetCohortName,
                                   "\n", .data$comparatorCohortName,
                                   "\n", .data$targetShortName, " only: ", .data$tOnlyString,
                                   "\n", .data$comparatorShortName, " only: ", .data$cOnlyString,
                                   "\nBoth: ", .data$bothString)) %>%
    dplyr::select(.data$targetShortName,
                  .data$comparatorShortName,
                  .data$databaseId,
                  .data$absTOnlySubjects,
                  .data$absCOnlySubjects,
                  .data$absBothSubjects,
                  .data$tooltip) %>% 
    tidyr::pivot_longer(cols = c("absTOnlySubjects", 
                                 "absCOnlySubjects",
                                 "absBothSubjects"),
                        names_to = "subjectsIn",
                        values_to = "value") %>%
    dplyr::mutate(subjectsIn = camelCaseToTitleCase(stringr::str_replace_all(string = .data$subjectsIn,
                                                                             pattern = "abs|Subjects",
                                                                             replacement = "")))
  
  plotData$subjectsIn <- factor(plotData$subjectsIn, levels = c(" T Only", " Both", " C Only"))
  if (yAxis == "Percentages") {
    position = "fill"
  } else { 
    position = "stack"
  }
  
  plot <- ggplot2::ggplot(data = plotData) +
    ggplot2::aes(fill = .data$subjectsIn, 
                 y = .data$comparatorShortName,
                 x = .data$value,
                 tooltip = .data$tooltip,
                 group = .data$subjectsIn) +
    ggplot2::ylab(label = "") +
    ggplot2::xlab(label = "") +
    ggplot2::scale_fill_manual("Subjects in", values = c(rgb(0.8, 0.2, 0.2), rgb(0.3, 0.2, 0.4), rgb(0.4, 0.4, 0.9))) +
    ggplot2::facet_wrap(. ~ databaseId + targetShortName,ncol = cohortIdLength*2) +
    ggplot2::theme(strip.text.y.right = ggplot2::element_text(angle = 0)) +
    ggiraph::geom_bar_interactive(position = position, alpha = 0.6, stat = "identity") 
  if (yAxis == "Percentages") {
    plot <- plot + ggplot2::scale_x_continuous(labels = scales::percent)
  } else {
    plot <- plot + ggplot2::scale_x_continuous(labels = scales::comma)
  }
  width <- 1.5 + 1*length(unique(plotData$databaseId))
  height <- 1.5 + 1*length(unique(plotData$targetShortName))
  aspectRatio <- width / height                        
  plot <- ggiraph::girafe(ggobj = plot,
                          options = list(
                            ggiraph::opts_sizing(width = .7),
                            ggiraph::opts_zoom(max = 5)), 
                          width_svg = 12,
                          height_svg = 7 )
  return(plot)
}  

plotTemporalCohortComparisonStandardizedDifference <- function(balance){
  balance$tooltip <- c(paste("Covariate Name:", balance$covariateName,
                             "\nDatabase: ", balance$databaseId,
                             "\nMean Target: ", scales::comma(balance$mean1, accuracy = 0.1),
                             "\nMean Comparator:", scales::comma(balance$mean2, accuracy = 0.1),
                             "\nStd diff.:", scales::comma(balance$stdDiff, accuracy = 0.1)))
  # cant make sense anyways - so throwing away small values when too much data
  if (nrow(balance) > 1000) {
    balance <- balance %>%
      dplyr::filter(.data$mean1 > 0.01 | .data$mean2 > 0.01)
  }
  
  plot <- ggplot2::ggplot(balance, ggplot2::aes(x = .data$mean1, 
                                                y = .data$mean2, 
                                                color = .data$temporalChoices)) +
    ggiraph::geom_point_interactive(ggplot2::aes(tooltip = .data$tooltip), 
                                    size = 3,
                                    shape = 16,
                                    alpha = 0.5) +
    ggplot2::geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
    ggplot2::geom_hline(yintercept = 0) +
    ggplot2::geom_vline(xintercept = 0) +             
    ggplot2::scale_x_continuous("") +
    ggplot2::scale_y_continuous("") +
    ggplot2::facet_grid(databaseId + targetCohort ~ comparatorCohort)
  
  plot <- ggiraph::girafe(ggobj = plot,
                          options = list(
                            ggiraph::opts_sizing(width = .7),
                            ggiraph::opts_zoom(max = 5)),width_svg = 12,
                          height_svg = 5)
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

