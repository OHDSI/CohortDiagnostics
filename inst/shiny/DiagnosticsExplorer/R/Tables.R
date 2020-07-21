library(magrittr)

prepareTable1 <- function(covariates,
                          pathToCsv = "Table1Specs.csv") {
  covariates <- covariates %>%
    dplyr::mutate(conceptId = (.data$covariateId  - .data$covariateAnalysisId)/1000,
                  covariateName = stringr::str_to_sentence(stringr::str_replace_all(string = .data$covariateName, 
                                                                                    pattern = "^.*: ",
                                                                                    replacement = "")))
  space <- "&nbsp;"
  specifications <- readr::read_csv(file = pathToCsv, col_types = readr::cols()) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = '')))
  resultsTable <- tidyr::tibble()
  for (i in 1:nrow(specifications)) {
    specification <- specifications %>% dplyr::slice(i)
    if (specification %>% dplyr::pull(.data$analysisId) == "") {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = (specification %>% dplyr::pull(.data$label)), 
                                                     value = "",
                                                     header = 1,
                                                     position = i))
      covariatesSubset <- tidyr::tibble()
    } else if (specification %>% dplyr::pull(.data$covariateIds) == "") {
      covariatesSubset <- covariates %>%
        dplyr::filter(.data$covariateAnalysisId %in% specification$analysisId) %>% 
        dplyr::arrange(.data$covariateName)
    } else {
      covariatesSubset <- covariates %>%
        dplyr::filter(.data$covariateAnalysisId %in% specification$analysisId,
                      .data$covariateId %in% (stringr::str_split(string = (specification %>% 
                                                                             dplyr::pull(.data$covariateIds)), 
                                                                 pattern = ";")[[1]] %>% 
                                                utils::type.convert())) %>% 
        dplyr::arrange(.data$covariateId)
    }
    if (nrow(covariatesSubset) > 1) {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = specification$label,
                                                     value = NA,
                                                     header = 1,
                                                     position = i))
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = paste0(space,
                                                                             space,
                                                                             space,
                                                                             space,
                                                                             covariatesSubset$covariateName),
                                                     value = covariatesSubset$mean,
                                                     header = 0,
                                                     position = i))
    } else if (nrow(covariatesSubset) == 1) {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(characteristic = specification$label,
                                                     value = covariatesSubset$mean,
                                                     position = i))
    }
  }
return(resultsTable)
}


prepareTable1Comp <- function(balance,
                              pathToCsv = "Table1Specs.csv") {
  space <- "&nbsp;"
  specifications <- readr::read_csv(file = pathToCsv, col_types = readr::cols()) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = '')))
  
  resultsTable <- tidyr::tibble()
  
  for (i in 1:nrow(specifications)) {
    specification <- specifications %>% dplyr::slice(i)
    if (specification %>% dplyr::pull(.data$analysisId) == "") {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = (specification %>% dplyr::pull(.data$label)), 
                                                     value = "",
                                                     header = 1,
                                                     position = i))
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
          balanceSubset$covariateName <- stringr::str_to_sentence(gsub("^.*: ", "", balanceSubset$covariateName))
          if (specifications$covariateIds[i] == "" || length(covariateIds) > 1) {
            resultsTable <- rbind(resultsTable, data.frame(characteristic = specifications$label[i],
                                                           MeanT = NA,
                                                           MeanC = NA,
                                                           StdDiff = NA,
                                                           stringsAsFactors = FALSE))
            resultsTable <- rbind(resultsTable, data.frame(characteristic = paste0(space,
                                                                                   space,
                                                                                   space,
                                                                                   space,
                                                                                   balanceSubset$covariateName),
                                                           MeanT = balanceSubset$mean1,
                                                           MeanC = balanceSubset$mean2,
                                                           StdDiff = balanceSubset$stdDiff,
                                                           stringsAsFactors = FALSE))
          } else {
            resultsTable <- rbind(resultsTable, data.frame(characteristic = specifications$label[i],
                                                           MeanT = balanceSubset$mean1,
                                                           MeanC = balanceSubset$mean2,
                                                           StdDiff = balanceSubset$stdDiff,
                                                           stringsAsFactors = FALSE))
          }
        }
      }
    }
  }
  colnames(resultsTable) <- c("characteristic", "Proportion Target", "Proportion Comparator", "StdDiff")
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

