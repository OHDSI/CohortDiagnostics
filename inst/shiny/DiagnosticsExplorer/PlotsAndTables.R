prepareTable1 <- function(covariates,
                          pathToCsv = "Table1Specs.csv") {
  space <- "&nbsp;"
  specifications <- read.csv(pathToCsv, stringsAsFactors = FALSE)
  
  fixCase <- function(label) {
    idx <- (toupper(label) == label)
    if (any(idx)) {
      label[idx] <- paste0(substr(label[idx], 1, 1),
                           tolower(substr(label[idx], 2, nchar(label[idx]))))
    }
    return(label)
  }
  
  resultsTable <- data.frame()
  for (i in 1:nrow(specifications)) {
    if (specifications$analysisId[i] == "") {
      resultsTable <- rbind(resultsTable,
                            data.frame(Characteristic = specifications$label[i], value = ""))
    } else {
      idx <- covariates$covariateAnalysisId == specifications$analysisId[i]
      if (any(idx)) {
        if (specifications$covariateIds[i] != "") {
          covariateIds <- as.numeric(strsplit(specifications$covariateIds[i], ";")[[1]])
          idx <- covariates$covariateId %in% covariateIds
        } else {
          covariateIds <- NULL
        }
        if (any(idx)) {
          covariatesSubset <- covariates[idx, ]
          if (is.null(covariateIds)) {
            covariatesSubset <- covariatesSubset[order(covariatesSubset$covariateId), ]
          } else {
            covariatesSubset <- merge(covariatesSubset, data.frame(covariateId = covariateIds,
                                                                   rn = 1:length(covariateIds)))
            covariatesSubset <- covariatesSubset[order(covariatesSubset$rn, covariatesSubset$covariateId), ]
          }
          covariatesSubset$covariateName <- fixCase(gsub("^.*: ", "", covariatesSubset$covariateName))
          if (specifications$covariateIds[i] == "" || length(covariateIds) > 1) {
            resultsTable <- rbind(resultsTable, data.frame(Characteristic = specifications$label[i],
                                                           mean = NA,
                                                           stringsAsFactors = FALSE))
            resultsTable <- rbind(resultsTable, data.frame(Characteristic = paste0(space,
                                                                                   space,
                                                                                   space,
                                                                                   space,
                                                                                   covariatesSubset$covariateName),
                                                           mean = covariatesSubset$mean,
                                                           stringsAsFactors = FALSE))
          } else {
            resultsTable <- rbind(resultsTable, data.frame(Characteristic = specifications$label[i],
                                                           mean = covariatesSubset$mean,
                                                           stringsAsFactors = FALSE))
          }
        }
      }
    }
  }
  colnames(resultsTable) <- c("Characteristic", "Mean (%)")
  return(resultsTable)
}


prepareTable1Comp <- function(balance,
                              pathToCsv = "Table1Specs.csv") {
  space <- "&nbsp;"
  specifications <- read.csv(pathToCsv, stringsAsFactors = FALSE)
  
  fixCase <- function(label) {
    idx <- (toupper(label) == label)
    if (any(idx)) {
      label[idx] <- paste0(substr(label[idx], 1, 1),
                           tolower(substr(label[idx], 2, nchar(label[idx]))))
    }
    return(label)
  }
  
  resultsTable <- data.frame()
  for (i in 1:nrow(specifications)) {
    if (specifications$analysisId[i] == "") {
      resultsTable <- rbind(resultsTable,
                            data.frame(Characteristic = specifications$label[i], value = ""))
    } else {
      idx <- balance$covariateAnalysisId == specifications$analysisId[i]
      if (any(idx)) {
        if (specifications$covariateIds[i] != "") {
          covariateIds <- as.numeric(strsplit(specifications$covariateIds[i], ";")[[1]])
          idx <- balance$covariateId %in% covariateIds
        } else {
          covariateIds <- NULL
        }
        if (any(idx)) {
          balanceSubset <- balance[idx, ]
          if (is.null(covariateIds)) {
            balanceSubset <- balanceSubset[order(balanceSubset$covariateId), ]
          } else {
            balanceSubset <- merge(balanceSubset, data.frame(covariateId = covariateIds,
                                                             rn = 1:length(covariateIds)))
            balanceSubset <- balanceSubset[order(balanceSubset$rn, balanceSubset$covariateId), ]
          }
          balanceSubset$covariateName <- fixCase(gsub("^.*: ", "", balanceSubset$covariateName))
          if (specifications$covariateIds[i] == "" || length(covariateIds) > 1) {
            resultsTable <- rbind(resultsTable, data.frame(Characteristic = specifications$label[i],
                                                           MeanT = NA,
                                                           MeanC = NA,
                                                           StdDiff = NA,
                                                           stringsAsFactors = FALSE))
            resultsTable <- rbind(resultsTable, data.frame(Characteristic = paste0(space,
                                                                                   space,
                                                                                   space,
                                                                                   space,
                                                                                   balanceSubset$covariateName),
                                                           MeanT = balanceSubset$mean1,
                                                           MeanC = balanceSubset$mean2,
                                                           StdDiff = balanceSubset$stdDiff,
                                                           stringsAsFactors = FALSE))
          } else {
            resultsTable <- rbind(resultsTable, data.frame(Characteristic = specifications$label[i],
                                                           MeanT = balanceSubset$mean1,
                                                           MeanC = balanceSubset$mean2,
                                                           StdDiff = balanceSubset$stdDiff,
                                                           stringsAsFactors = FALSE))
          }
        }
      }
    }
  }
  colnames(resultsTable) <- c("Characteristic", "Proportion T", "Proportion C", "StdDiff")
  return(resultsTable)
}

compareCohortCharacteristics <- function(characteristics1, characteristics2) {
  
  m <- merge(data.frame(covariateId = characteristics1$covariateId,
                        mean1 = characteristics1$mean,
                        sd1 = characteristics1$sd),
             data.frame(covariateId = characteristics2$covariateId,
                        mean2 = characteristics2$mean,
                        sd2 = characteristics2$sd),
             all = TRUE)
  m$sd <- sqrt(m$sd1^2 + m$sd2^2)
  m$stdDiff <- (m$mean2 - m$mean1)/m$sd
  
  ref <- unique(rbind(characteristics1[,
                                       c("covariateId", "covariateName")],
                      characteristics2[,
                                       c("covariateId", "covariateName")]))
  m <- merge(ref, m)
  m <- m[order(-abs(m$stdDiff)), ]
  return(m)
}

plotIncidenceProportion <- function(incidenceProportion,
                                    restrictToFullAgeData = FALSE,
                                    minBackgroundSubjects = 1000, 
                                    fileName = NULL) {
  data <- incidenceProportion[!is.na(incidenceProportion$gender) & !is.na(incidenceProportion$ageGroup) &
                                !is.na(incidenceProportion$indexYear), ]
  data <- data[data$backgroundSubjects > minBackgroundSubjects, ]
  data$gender <- as.factor(data$gender)
  data$indexYear <- as.numeric(as.character(data$indexYear))
  if (restrictToFullAgeData) {
    data <- useFullData(data)
  }
  
  # Sort ageGroup numerically, so 100-109 > 20-29:
  ageGroups <- unique(data$ageGroup)
  ageGroups <- ageGroups[order(as.numeric(gsub("-.*", "", ageGroups)))]
  data$ageGroup <- factor(data$ageGroup, levels = ageGroups)
  
  plot <- ggplot2::ggplot(data = data, ggplot2::aes(x = indexYear,
                                                    y = incidenceProportion,
                                                    group = gender,
                                                    color = gender)) +
    ggplot2::geom_line(size = 1.25, alpha = 0.6) +
    ggplot2::xlab("Year") +
    ggplot2::ylab("Incidence proportion (/1000 persons)") +
    ggplot2::theme(legend.position = "top",
                   legend.title = ggplot2::element_blank(),
                   axis.text.x = ggplot2::element_text(angle = 90, vjust = 0.5))
  if (!is.null(incidenceProportion$databaseId) && length(unique(incidenceProportion$databaseId)) > 1) {
    plot <- plot + ggplot2::facet_grid(databaseId~ageGroup, scales = "free_y") 
  } else {
    plot <- plot + ggplot2::facet_grid(~ageGroup) 
  }
  if (!is.null(fileName))
    ggplot2::ggsave(fileName, plot, width = 5, height = 3.5, dpi = 400)
  return(plot)
}
