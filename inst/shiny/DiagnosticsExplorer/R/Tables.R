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
                                                     position = i)) %>% 
        dplyr::arrange(.data$label, dplyr::desc(.data$header), .data$position) %>% 
        dplyr::mutate(sortOrder = dplyr::row_number()) %>% 
        dplyr::select(-.data$label, -.data$header, -.data$position)
    }
  }
return(resultsTable)
}


prepareTable1Comp <- function(balance,
                              pathToCsv = "Table1Specs.csv") {
  balance <- balance %>%
    dplyr::mutate(covariateName = stringr::str_to_sentence(stringr::str_replace_all(string = .data$covariateName, 
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
                                                     MeanT = NA,
                                                     MeanC = NA,
                                                     StdDiff = NA,
                                                     header = 1,
                                                     position = i))
    } else if (specification %>% dplyr::pull(.data$covariateIds) == "") {
      balanceSubset <- balance %>%
        dplyr::filter(.data$covariateAnalysisId %in% specification$analysisId) %>% 
        dplyr::arrange(.data$covariateName)
    } else {
      balanceSubset <- balance %>%
        dplyr::filter(.data$covariateAnalysisId %in% specification$analysisId,
                      .data$covariateId %in% (stringr::str_split(string = (specification %>% 
                                                                             dplyr::pull(.data$covariateIds)), 
                                                                 pattern = ";")[[1]] %>% 
                                                utils::type.convert())) %>% 
        dplyr::arrange(.data$covariateId)
    }
    
    if (nrow(balanceSubset) > 1) {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = specification$label,
                                                     MeanT = NA,
                                                     MeanC = NA,
                                                     StdDiff = NA,
                                                     header = 1,
                                                     position = i))
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = paste0(space,
                                                                             space,
                                                                             space,
                                                                             space,
                                                                             balanceSubset$covariateName),
                                                     MeanT = balanceSubset$mean1,
                                                     MeanC = balanceSubset$mean2,
                                                     StdDiff = balanceSubset$stdDiff,
                                                     header = 0,
                                                     position = i))
    } else if (nrow(balanceSubset) == 1) {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = specification$label,
                                                     MeanT = balanceSubset$mean1,
                                                     MeanC = balanceSubset$mean2,
                                                     StdDiff = balanceSubset$stdDiff,
                                                     header = 1,
                                                     position = i))
    }
  }
  resultsTable <- resultsTable %>% 
    dplyr::arrange(.data$label, dplyr::desc(.data$header), .data$position) %>% 
    dplyr::mutate(sortOrder = dplyr::row_number()) %>% 
    dplyr::select(-.data$label, -.data$header, -.data$position)
  return(resultsTable)
}