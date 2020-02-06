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
