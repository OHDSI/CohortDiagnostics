addShortName <-
  function(data,
           shortNameRef = NULL,
           cohortIdColumn = "cohortId",
           shortNameColumn = "shortName") {
    if (is.null(shortNameRef)) {
      shortNameRef <- data %>%
        dplyr::distinct(.data$cohortId) %>%
        dplyr::arrange(.data$cohortId) %>%
        dplyr::mutate(shortName = paste0("C", dplyr::row_number()))
    }
    
    shortNameRef <- shortNameRef %>%
      dplyr::distinct(.data$cohortId, .data$shortName)
    colnames(shortNameRef) <- c(cohortIdColumn, shortNameColumn)
    data <- data %>%
      dplyr::inner_join(shortNameRef, by = cohortIdColumn)
    return(data)
    
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
  
  plotData <-
    addShortName(data = data, shortNameRef = shortNameRef)
  
  plotData$tooltip <- c(
    paste0(
      plotData$shortName,
      "\nDatabase = ",
      plotData$databaseId,
      "\nMin = ",
      scales::comma(plotData$minValue),
      "\nP25 = ",
      scales::comma(plotData$p25Value),
      "\nMedian = ",
      scales::comma(plotData$medianValue),
      "\nP75 = ",
      scales::comma(plotData$p75Value),
      "\nMax = ",
      scales::comma(plotData$maxValue),
      "\nTime Measure = ",
      plotData$timeMetric,
      "\nAverage = ",
      scales::comma(x = plotData$averageValue, accuracy = 0.01)
    )
  )
  
  plot <- ggplot2::ggplot(data = plotData) +
    ggplot2::aes(
      x = .data$shortName,
      ymin = .data$minValue,
      lower = .data$p25Value,
      middle = .data$medianValue,
      upper = .data$p75Value,
      ymax = .data$maxValue,
      group = .data$shortName,
      average = .data$averageValue
    ) +
    ggplot2::geom_errorbar(size = 0.5) +
    ggiraph::geom_boxplot_interactive(
      ggplot2::aes(tooltip = tooltip),
      stat = "identity",
      fill = rgb(0, 0, 0.8, alpha = 0.25),
      size = 0.2
    ) +
    ggplot2::facet_grid(databaseId ~ timeMetric, scales = "free") +
    ggplot2::coord_flip() +
    ggplot2::theme(
      panel.grid.major.y = ggplot2::element_blank(),
      panel.grid.minor.y = ggplot2::element_blank(),
      axis.title.y = ggplot2::element_blank(),
      axis.ticks.y = ggplot2::element_blank(),
      strip.background = ggplot2::element_blank()
    )
  height <-
    1.5 + 0.4 * nrow(dplyr::distinct(plotData, .data$databaseId, .data$shortName))
  plot <- ggiraph::girafe(
    ggobj = plot,
    options = list(ggiraph::opts_sizing(width = .7),
                   ggiraph::opts_zoom(max = 5)),
    width_svg = 12,
    height_svg = height
  )
}

plotIncidenceRate <- function(data,
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
    addShortName(shortNameRef) %>%
    dplyr::mutate(incidenceRate = round(.data$incidenceRate, digits = 3))
  plotData <- plotData %>%
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
    dplyr::arrange(as.integer(sub(
      pattern = '-.+$', '', x = .data$ageGroup
    )))
  
  plotData <- plotData %>%
    dplyr::arrange(ageGroup = factor(.data$ageGroup, levels = newSort$ageGroup),
                   .data$ageGroup)
  
  plotData$ageGroup <- factor(plotData$ageGroup,
                              levels = newSort$ageGroup)
  plotData$tooltip <- c(
    paste0(
      plotData$shortName,
      "\nIncidence Rate = ",
      scales::comma(plotData$incidenceRate, accuracy = 0.01),
      "\nDatabase = ",
      plotData$databaseId,
      "\nPerson years = ",
      scales::comma(plotData$personYears, accuracy = 0.1),
      "\nCohort count = ",
      scales::comma(plotData$cohortCount)
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
  distinctCalenderYear <- plotData$calendarYear %>%
    unique()
  
  plot <-
    ggplot2::ggplot(data = plotData, do.call(ggplot2::aes_string, aesthetics)) +
    ggplot2::xlab(xLabel) +
    ggplot2::ylab("Incidence Rate (/1,000 person years)") +
    ggplot2::scale_y_continuous(expand = c(0, 0))
  
  if (!is.na(distinctCalenderYear)) {
    if (length(distinctCalenderYear) >= 8) {
      plot <-
        plot + ggplot2::scale_x_continuous(n.breaks = 8, labels = round)
    } else {
      plot <-
        plot + ggplot2::scale_x_continuous(breaks = distinctCalenderYear)
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
      plot <- plot + ggplot2::facet_grid( ~ ageGroup)
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

plotCohortComparisonStandardizedDifference <- function(balance,
                                                       shortNameRef = NULL,
                                                       xLimitMin = 0,
                                                       xLimitMax = 1,
                                                       yLimitMin = 0,
                                                       yLimitMax = 1,
                                                       domain = "all") {
  domains <-
    c("condition",
      "device",
      "drug",
      "measurement",
      "observation",
      "procedure")
  balance$domain <-
    tolower(stringr::str_extract(balance$covariateName, "[a-z]+"))
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
        "\nY ",
        balance$targetCohort,
        ": ",
        scales::comma(balance$mean2, accuracy = 0.1),
        "\nX ",
        balance$comparatorCohort,
        ": ",
        scales::comma(balance$mean1, accuracy = 0.1),
        "\nStd diff.:",
        scales::comma(balance$stdDiff, accuracy = 0.1)
      )
    )
  
  # Code used to generate palette:
  # writeLines(paste(RColorBrewer::brewer.pal(n = length(domains), name = "Dark2"), collapse = "\", \""))
  
  # Make sure colors are consistent, no matter which domains are included:
  colors <-
    c("#1B9E77",
      "#D95F02",
      "#7570B3",
      "#E7298A",
      "#66A61E",
      "#E6AB02",
      "#444444")
  colors <- colors[c(domains, "other") %in% unique(balance$domain)]
  
  balance$domain <-
    factor(balance$domain, levels = c(domains, "other"))
  
  # targetLabel <- paste(strwrap(targetLabel, width = 50), collapse = "\n")
  # comparatorLabel <- paste(strwrap(comparatorLabel, width = 50), collapse = "\n")
  
  
  plot <-
    ggplot2::ggplot(balance,
                    ggplot2::aes(
                      x = .data$mean1,
                      y = .data$mean2,
                      color = .data$domain
                    )) +
    ggiraph::geom_point_interactive(
      ggplot2::aes(tooltip = .data$tooltip),
      size = 3,
      shape = 16,
      alpha = 0.5
    ) +
    ggplot2::geom_abline(slope = 1,
                         intercept = 0,
                         linetype = "dashed") +
    ggplot2::geom_hline(yintercept = 0) +
    ggplot2::geom_vline(xintercept = 0) +
    # ggplot2::scale_x_continuous("Mean") +
    # ggplot2::scale_y_continuous("Mean") +
    ggplot2::scale_color_manual("Domain", values = colors) +
    facet_nested(databaseId + targetCohort ~ comparatorCohort) +
    ggplot2::theme(strip.background = ggplot2::element_blank()) +
    ggplot2::xlim(xLimitMin, xLimitMax) +
    ggplot2::ylim(yLimitMin, yLimitMax) +
    ggplot2::xlab(balance$cohortId2 %>% unique()) +
    ggplot2::ylab(balance$cohortId1 %>% unique())
  
  plot <- ggiraph::girafe(
    ggobj = plot,
    options = list(ggiraph::opts_sizing(width = .7),
                   ggiraph::opts_zoom(max = 5)),
    width_svg = 12,
    height_svg = 5
  )
  return(plot)
}

plotTemporalCompareStandardizedDifference <- function(balance,
                                                      shortNameRef = NULL,
                                                      xLimitMin = 0,
                                                      xLimitMax = 1,
                                                      yLimitMin = 0,
                                                      yLimitMax = 1,
                                                      domain = "all") {
  domains <-
    c("condition",
      "device",
      "drug",
      "measurement",
      "observation",
      "procedure")
  balance$domain <-
    tolower(stringr::str_extract(balance$covariateName, "[a-z]+"))
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
        "\n Y ",
        balance$targetCohort,
        ": ",
        scales::comma(balance$mean2, accuracy = 0.1),
        "\n X ",
        balance$comparatorCohort,
        ": ",
        scales::comma(balance$mean1, accuracy = 0.1),
        "\nStd diff.:",
        scales::comma(balance$stdDiff, accuracy = 0.1)
      )
    )
  
  # Code used to generate palette:
  # writeLines(paste(RColorBrewer::brewer.pal(n = length(domains), name = "Dark2"), collapse = "\", \""))
  
  # Make sure colors are consistent, no matter which domains are included:
  colors <-
    c("#1B9E77",
      "#D95F02",
      "#7570B3",
      "#E7298A",
      "#66A61E",
      "#E6AB02",
      "#444444")
  colors <- colors[c(domains, "other") %in% unique(balance$domain)]
  
  balance$domain <-
    factor(balance$domain, levels = c(domains, "other"))
  
  # targetLabel <- paste(strwrap(targetLabel, width = 50), collapse = "\n")
  # comparatorLabel <- paste(strwrap(comparatorLabel, width = 50), collapse = "\n")
  
  
  plot <-
    ggplot2::ggplot(balance,
                    ggplot2::aes(
                      x = .data$mean1,
                      y = .data$mean2,
                      color = .data$domain
                    )) +
    ggiraph::geom_point_interactive(
      ggplot2::aes(tooltip = .data$tooltip),
      size = 3,
      shape = 16,
      alpha = 0.5
    ) +
    ggplot2::geom_abline(slope = 1,
                         intercept = 0,
                         linetype = "dashed") +
    ggplot2::geom_hline(yintercept = 0) +
    ggplot2::geom_vline(xintercept = 0) +
    # ggplot2::scale_x_continuous("Mean") +
    # ggplot2::scale_y_continuous("Mean") +
    ggplot2::scale_color_manual("Domain", values = colors) +
    facet_nested(databaseId + choices1 ~ choices2) +
    ggplot2::theme(strip.background = ggplot2::element_blank()) +
    ggplot2::xlim(xLimitMin, xLimitMax) +
    ggplot2::ylim(yLimitMin, yLimitMax) +
    ggplot2::xlab(balance$comparatorCohort %>% unique()) +
    ggplot2::ylab(balance$targetCohort %>% unique())
  
  plot <- ggiraph::girafe(
    ggobj = plot,
    options = list(ggiraph::opts_sizing(width = .7),
                   ggiraph::opts_zoom(max = 5)),
    width_svg = 12,
    height_svg = 8
  )
  return(plot)
}



### cohort overlap plot ##############

plotCohortOverlap <- function(data,
                              shortNameRef = NULL,
                              yAxis = "Percentages") {
  # Perform error checks for input variables
  errorMessage <- checkmate::makeAssertCollection()
  checkmate::assertTibble(
    x = data,
    any.missing = FALSE,
    min.rows = 1,
    min.cols = 6,
    null.ok = FALSE,
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  checkmate::assertNames(
    x = colnames(data),
    must.include = c(
      "databaseId",
      "targetCohortId",
      "comparatorCohortId",
      "tOnlySubjects",
      "cOnlySubjects",
      "bothSubjects"
    ),
    add = errorMessage
  )
  checkmate::reportAssertions(collection = errorMessage)
  
  
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
        scales::comma(.data$absTOnlySubjects),
        " (",
        .data$signTOnlySubjects,
        scales::percent(.data$absTOnlySubjects /
                          .data$absEitherSubjects,
                        accuracy = 1),
        ")"
      ),
      cOnlyString = paste0(
        .data$signCOnlySubjects,
        scales::comma(.data$absCOnlySubjects),
        " (",
        .data$signCOnlySubjects,
        scales::percent(.data$absCOnlySubjects /
                          .data$absEitherSubjects,
                        accuracy = 1),
        ")"
      ),
      bothString = paste0(
        .data$signBothSubjects,
        scales::comma(.data$absBothSubjects),
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
        "Database: ",
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
        absTOnlySubjects = "Left cohort only",
        absBothSubjects = "Both cohorts",
        absCOnlySubjects = "Top cohort only"
      )
    )
  
  plotData$subjectsIn <-
    factor(plotData$subjectsIn,
           levels = c("Top cohort only", "Both cohorts", "Left cohort only"))
  
  if (yAxis == "Percentages") {
    position = "fill"
  } else {
    position = "stack"
  }
  
  plot <- ggplot2::ggplot(data = plotData) +
    ggplot2::aes(
      fill = .data$subjectsIn,
      y = targetShortName,
      x = .data$value,
      tooltip = .data$tooltip,
      group = .data$subjectsIn
    ) +
    ggplot2::ylab(label = "") +
    ggplot2::xlab(label = "") +
    ggplot2::scale_fill_manual("Subjects in", values = c(rgb(0.8, 0.2, 0.2), rgb(0.3, 0.2, 0.4), rgb(0.4, 0.4, 0.9))) +
    ggplot2::facet_grid(databaseId ~ comparatorShortName) +
    ggplot2::theme(
      panel.background = ggplot2::element_blank(),
      strip.background = ggplot2::element_blank(),
      panel.grid.major.x = ggplot2::element_line(color = "gray"),
      axis.ticks.y = ggplot2::element_blank()
    ) +
    ggiraph::geom_bar_interactive(position = position,
                                  alpha = 0.6,
                                  stat = "identity")
  if (yAxis == "Percentages") {
    plot <- plot + ggplot2::scale_x_continuous(labels = scales::percent)
  } else {
    plot <- plot + ggplot2::scale_x_continuous(labels = scales::comma)
  }
  width <- 1 + 0.5 * length(unique(plotData$comparatorShortName))
  height <-
    0.25 + 0.08 * nrow(dplyr::distinct(plotData, .data$databaseId, .data$targetShortName))
  aspectRatio <- width / height
  plot <- ggiraph::girafe(
    ggobj = plot,
    options = list(ggiraph::opts_sizing(width = .7),
                   ggiraph::opts_zoom(max = 5)),
    width_svg = 12,
    height_svg = 12 / aspectRatio
  )
  return(plot)
}
