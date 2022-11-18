addShortName <-
  function(data,
           shortNameRef = NULL,
           cohortIdColumn = "cohortId",
           shortNameColumn = "shortName") {
    if (is.null(shortNameRef)) {
      shortNameRef <- data %>%
        dplyr::distinct(cohortId) %>%
        dplyr::arrange(cohortId) %>%
        dplyr::mutate(shortName = paste0("C", dplyr::row_number()))
    }

    shortNameRef <- shortNameRef %>%
      dplyr::distinct(cohortId, shortName)
    colnames(shortNameRef) <- c(cohortIdColumn, shortNameColumn)
    data <- data %>%
      dplyr::inner_join(shortNameRef, by = dplyr::all_of(cohortIdColumn))
    return(data)
  }


plotCohortComparisonStandardizedDifference <- function(balance,
                                                       shortNameRef = NULL,
                                                       xLimitMin = 0,
                                                       xLimitMax = 1,
                                                       yLimitMin = 0,
                                                       yLimitMax = 1,
                                                       domain = "all") {
  domains <-
    c(
      "Condition",
      "Device",
      "Drug",
      "Measurement",
      "Observation",
      "Procedure",
      "Demographics"
    )

  balance$domainId[!balance$domainId %in% domains] <- "Other"
  if (domain != "all") {
    balance <- balance %>%
      dplyr::filter(domainId == !!domain)
  }

  # Can't make sense of plot with > 1000 dots anyway, so remove
  # anything with small mean in both target and comparator:
  if (nrow(balance) > 1000) {
    balance <- balance %>%
      dplyr::filter(mean1 > 0.01 | mean2 > 0.01)
  }
  if (nrow(balance) > 1000) {
    balance <- balance %>%
      dplyr::filter(sumValue1 > 0 & sumValue2 > 0)
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
        balance$comparatorCohort,
        ": ",
        scales::comma(balance$mean2, accuracy = 0.01),
        "\nX ",
        balance$targetCohort,
        ": ",
        scales::comma(balance$mean1, accuracy = 0.01),
        "\nStd diff.:",
        scales::comma(balance$stdDiff, accuracy = 0.01)
      )
    )

  # Code used to generate palette:
  # writeLines(paste(RColorBrewer::brewer.pal(n = length(domains), name = "Dark2"), collapse = "\", \""))

  # Make sure colors are consistent, no matter which domains are included:
  colors <-
    c(
      "#1B9E77",
      "#D95F02",
      "#7570B3",
      "#E7298A",
      "#66A61E",
      "#E6AB02",
      "#444444"
    )
  colors <-
    colors[c(domains, "Other") %in% unique(balance$domainId)]

  balance$domainId <-
    factor(balance$domainId, levels = c(domains, "Other"))

  # targetLabel <- paste(strwrap(targetLabel, width = 50), collapse = "\n")
  # comparatorLabel <- paste(strwrap(comparatorLabel, width = 50), collapse = "\n")

  xCohort <- balance %>%
    dplyr::distinct(balance$targetCohort) %>%
    dplyr::pull()
  yCohort <- balance %>%
    dplyr::distinct(balance$comparatorCohort) %>%
    dplyr::pull()

  if (nrow(balance) == 0) {
    return(NULL)
  }

  plot <-
    ggplot2::ggplot(
      balance,
      ggplot2::aes(
        x = mean1,
        y = mean2,
        color = domainId
      )
    ) +
    ggiraph::geom_point_interactive(
      ggplot2::aes(tooltip = tooltip),
      size = 3,
      shape = 16,
      alpha = 0.5
    ) +
    ggplot2::geom_abline(
      slope = 1,
      intercept = 0,
      linetype = "dashed"
    ) +
    ggplot2::geom_hline(yintercept = 0) +
    ggplot2::geom_vline(xintercept = 0) +
    # ggplot2::scale_x_continuous("Mean") +
    # ggplot2::scale_y_continuous("Mean") +
    ggplot2::xlab(paste("Covariate Mean in ", xCohort)) +
    ggplot2::ylab(paste("Covariate Mean in ", yCohort)) +
    ggplot2::scale_color_manual("Domain", values = colors) +
    facet_nested(databaseId + targetCohort ~ comparatorCohort) +
    ggplot2::theme(strip.background = ggplot2::element_blank()) +
    ggplot2::xlim(xLimitMin, xLimitMax) +
    ggplot2::ylim(yLimitMin, yLimitMax)

  plot <- ggiraph::girafe(
    ggobj = plot,
    options = list(
      ggiraph::opts_sizing(width = .7),
      ggiraph::opts_zoom(max = 5)
    ),
    width_svg = 12,
    height_svg = 5
  )
  return(plot)
}


