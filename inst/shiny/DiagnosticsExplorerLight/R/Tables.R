library(magrittr)

prepareTable1 <- function(covariates,
                          pathToCsv = "Table1Specs.csv") {
  covariates <- covariates %>%
    dplyr::mutate(covariateName = stringr::str_to_sentence(stringr::str_replace_all(string = .data$covariateName, 
                                                                                    pattern = "^.*: ",
                                                                                    replacement = "")))
  space <- "&nbsp;"
  specifications <- readr::read_csv(file = pathToCsv, 
                                    col_types = readr::cols(),
                                    guess_max = min(1e7)) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = '')))
  
  resultsTable <- tidyr::tibble()
<<<<<<< HEAD
  for (i in 1:nrow(specifications)) {
    specification <- specifications[i,]
    if (specification %>% dplyr::pull(.data$analysisId) == "") {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = (specification %>% dplyr::pull(.data$label)), 
                                                     value = "",
                                                     header = 0,
                                                     position = i))
      covariatesSubset <- tidyr::tibble()
    } else if (specification %>% dplyr::pull(.data$covariateIds) == "") {
      covariatesSubset <- covariates %>%
        dplyr::filter(.data$analysisId %in% specification$analysisId) %>% 
        dplyr::arrange(.data$covariateName)
=======
  
  if (nrow(specifications) == 0) {
    return(resultsTable)
  }
  
  for (i in 1:nrow(specifications)) {
    specification <- specifications[i,]
    if (specification %>% dplyr::pull(.data$covariateIds) == "") {
      covariatesSubset <- covariates %>%
        dplyr::filter(.data$analysisId %in% specification$analysisId) %>% 
        dplyr::arrange(.data$covariateId)
>>>>>>> version2
    } else {
      covariatesSubset <- covariates %>%
        dplyr::filter(.data$analysisId %in% specification$analysisId,
                      .data$covariateId %in% (stringr::str_split(string = (specification %>% 
                                                                             dplyr::pull(.data$covariateIds)), 
                                                                 pattern = ";")[[1]] %>% 
                                                utils::type.convert())) %>% 
        dplyr::arrange(.data$covariateId)
    }
<<<<<<< HEAD
    if (nrow(covariatesSubset) > 1) {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = specification$label,
                                                     value = NA,
                                                     header = 0,
                                                     position = i))
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = paste0(space,
=======
    if (nrow(covariatesSubset) > 0) {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(characteristic = paste0('<strong>',
                                                                             specification %>% dplyr::pull(.data$label),
                                                                             '</strong>'),
                                                     value = NA,
                                                     header = 1,
                                                     position = i), 
                                       tidyr::tibble(characteristic = paste0(space,
>>>>>>> version2
                                                                             space,
                                                                             space,
                                                                             space,
                                                                             covariatesSubset$covariateName),
                                                     value = covariatesSubset$mean,
<<<<<<< HEAD
                                                     header = 1,
                                                     position = i))
    } else if (nrow(covariatesSubset) == 1) {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(characteristic = specification$label,
                                                     value = covariatesSubset$mean,
                                                     header = 0,
                                                     position = i)) 
    }
  }
  resultsTable <- resultsTable %>% 
    dplyr::arrange(.data$label, dplyr::desc(.data$header), .data$position) %>% 
    dplyr::mutate(sortOrder = dplyr::row_number()) 
=======
                                                     header = 0,
                                                     position = i)) %>% 
        dplyr::distinct() %>%
        dplyr::mutate(sortOrder = dplyr::row_number())
    }
  }
  resultsTable <- resultsTable %>% 
    dplyr::arrange(.data$position, dplyr::desc(.data$header), .data$sortOrder)
>>>>>>> version2
  return(resultsTable)
}


prepareTable1Comp <- function(balance,
                              pathToCsv = "Table1Specs.csv") {
  balance <- balance %>%
    dplyr::mutate(covariateName = stringr::str_to_sentence(stringr::str_replace_all(string = .data$covariateName, 
                                                                                    pattern = "^.*: ",
                                                                                    replacement = "")))
  space <- "&nbsp;"
  specifications <- readr::read_csv(file = pathToCsv, 
                                    col_types = readr::cols(),
                                    guess_max = min(1e7)) %>% 
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = '')))
  
  resultsTable <- tidyr::tibble()
  
<<<<<<< HEAD
  for (i in 1:nrow(specifications)) {
    specification <- specifications[i,]
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
        dplyr::filter(.data$analysisId %in% specification$analysisId) %>% 
        dplyr::arrange(.data$covariateName)
=======
  if (nrow(specifications) == 0) {
    return(resultsTable)
  }
  
  for (i in 1:nrow(specifications)) {
    specification <- specifications[i,]
    if (specification %>% dplyr::pull(.data$covariateIds) == "") {
      balanceSubset <- balance %>%
        dplyr::filter(.data$analysisId %in% specification$analysisId) %>% 
        dplyr::arrange(.data$covariateId)
>>>>>>> version2
    } else {
      balanceSubset <- balance %>%
        dplyr::filter(.data$analysisId %in% specification$analysisId,
                      .data$covariateId %in% (stringr::str_split(string = (specification %>% 
                                                                             dplyr::pull(.data$covariateIds)), 
                                                                 pattern = ";")[[1]] %>% 
                                                utils::type.convert())) %>% 
        dplyr::arrange(.data$covariateId)
    }
    
<<<<<<< HEAD
    if (nrow(balanceSubset) > 1) {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = specification$label,
=======
    if (nrow(balanceSubset) > 0) {
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(characteristic = paste0('<strong>',
                                                                             specification %>% dplyr::pull(.data$label),
                                                                             '</strong>'),
>>>>>>> version2
                                                     MeanT = NA,
                                                     MeanC = NA,
                                                     StdDiff = NA,
                                                     header = 1,
<<<<<<< HEAD
                                                     position = i))
      resultsTable <- dplyr::bind_rows(resultsTable, 
                                       tidyr::tibble(label = (specification %>% dplyr::pull(.data$label)),
                                                     characteristic = paste0(space,
=======
                                                     position = i), 
                                       tidyr::tibble(characteristic = paste0(space,
>>>>>>> version2
                                                                             space,
                                                                             space,
                                                                             space,
                                                                             balanceSubset$covariateName),
                                                     MeanT = balanceSubset$mean1,
                                                     MeanC = balanceSubset$mean2,
                                                     StdDiff = balanceSubset$stdDiff,
                                                     header = 0,
<<<<<<< HEAD
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
=======
                                                     position = i)) %>% 
        dplyr::distinct() %>%
        dplyr::mutate(sortOrder = dplyr::row_number())
    }
  }
  resultsTable <- resultsTable %>% 
    dplyr::arrange(.data$position, dplyr::desc(.data$header), .data$sortOrder) %>% 
    dplyr::mutate(sortOrder = dplyr::row_number()) %>% 
    dplyr::select(-.data$header, -.data$position)
>>>>>>> version2
  return(resultsTable)
}


compareCohortCharacteristics <- function(characteristics1, characteristics2) {
  m <- dplyr::full_join(x = characteristics1 %>% dplyr::distinct(), 
                        y = characteristics2 %>% dplyr::distinct(), 
                        by = c("covariateId", "conceptId", "databaseId", "covariateName", "analysisId"),
                        suffix = c("1", "2")) %>%
<<<<<<< HEAD
    dplyr::mutate(dplyr::across(tidyr::everything(), ~tidyr::replace_na(data = .x, replace = 0)),
                  sd = sqrt(.data$sd1^2 + .data$sd2^2),
=======
    dplyr::mutate(sd = sqrt(.data$sd1^2 + .data$sd2^2),
>>>>>>> version2
                  stdDiff = (.data$mean2 - .data$mean1)/.data$sd) %>% 
    dplyr::arrange(-abs(.data$stdDiff))
  return(m)
}
