library(magrittr)


fixCase <- function(label) {
  idx <- (toupper(label) == label)
  if (any(idx)) {
    label[idx] <- paste0(substr(label[idx], 1, 1),
                         tolower(substr(label[idx], 2, nchar(label[idx]))))
  }
  return(label)
}

prepareTable1 <- function(covariates,
                          pathToCsv = "Table1Specs.csv") {
  space <- "&nbsp;"
  specifications <- readr::read_csv(file = pathToCsv, col_types = readr::cols())
  
  resultsTable <- tidyr::tibble()
  
  for (i in 1:nrow(specifications)) {
    specification <- specifications %>% dplyr::slice(i)
    if (specification %>% dplyr::pull(.data$analysisId) == "") {
      resultsTable <- dplyr::bind_rows(resultsTable,
                                      tidyr::tibble(Characteristic = specification %>% dplyr::pull(.data$label), 
                                                    value = "")
                                      )
    } else {
      idx <- covariates$covariateAnalysisId == specification$analysisId
      if (any(idx)) {
        if (specification$covariateIds != "" & !is.na(specification$covariateIds)) {
          covariateIds <- as.numeric(strsplit(x = specification$covariateIds, split = ";")[[1]])
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
          if (is.na(specification$covariateIds) || length(covariateIds) > 1) {
            resultsTable <- dplyr::bind_rows(resultsTable, 
                                            tidyr::tibble(Characteristic = specification$label,
                                                           mean = NA))
            resultsTable <- dplyr::bind_rows(resultsTable, 
                                             tidyr::tibble(Characteristic = paste0(space,
                                                                                   space,
                                                                                   space,
                                                                                   space,
                                                                                   covariatesSubset$covariateName),
                                                           mean = covariatesSubset$mean))
          } else {
            resultsTable <- dplyr::bind_rows(resultsTable, 
                                             tidyr::tibble(Characteristic = specification$label,
                                                           mean = covariatesSubset$mean))
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
  specifications <- readr::read_csv(file = pathToCsv, col_types = readr::cols())
  
  resultsTable <- tidyr::tibble()
  
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
  colnames(resultsTable) <- c("Characteristic", "Proportion Target", "Proportion Comparator", "StdDiff")
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

