library(magrittr)

prepareTable1 <- function(covariates,
                          pathToCsv = "Table1Specs.csv") {
  covariates <- covariates %>%
    dplyr::mutate(covariateName = stringr::str_to_sentence(
      stringr::str_replace_all(
        string = .data$covariateName,
        pattern = "^.*: ",
        replacement = ""
      )
    ))
  
  covariates <- covariates %>% 
    dplyr::mutate(covariateName = stringr::str_replace(string = .data$covariateName, 
                                                       pattern = "black or african american", 
                                                       replacement = "Black or African American")) %>% 
    dplyr::mutate(covariateName = stringr::str_replace(string = .data$covariateName, pattern = "white", replacement = "White")) %>% 
    dplyr::mutate(covariateName = stringr::str_replace(string = .data$covariateName, pattern = "asian", replacement = "Asian"))
  
  space <- "&nbsp;"
  specifications <- readr::read_csv(
    file = pathToCsv,
    col_types = readr::cols(),
    guess_max = min(1e7)
  ) %>%
    dplyr::mutate(dplyr::across(
      tidyr::everything(),
      ~ tidyr::replace_na(data = .x, replace = '')
    ))
  
  resultsTable <- tidyr::tibble()
  
  if (nrow(specifications) == 0) {
    return(resultsTable)
  }
  
  for (i in 1:nrow(specifications)) {
    specification <- specifications[i, ]
    if (specification %>% dplyr::pull(.data$covariateIds) == "") {
      covariatesSubset <- covariates %>%
        dplyr::filter(.data$analysisId %in% specification$analysisId) %>%
        dplyr::arrange(.data$covariateId)
    } else {
      covariatesSubset <- covariates %>%
        dplyr::filter(
          .data$analysisId %in% specification$analysisId,
          .data$covariateId %in% (
            stringr::str_split(
              string = (specification %>%
                          dplyr::pull(.data$covariateIds)),
              pattern = ";"
            )[[1]] %>%
              utils::type.convert(as.is = TRUE)
          )
        ) %>%
        dplyr::arrange(.data$covariateId)
    }
    if (nrow(covariatesSubset) > 0) {
      resultsTable <- dplyr::bind_rows(
        resultsTable,
        tidyr::tibble(
          characteristic = paste0(
            '<strong>',
            specification %>% dplyr::pull(.data$label),
            '</strong>'
          ),
          value = NA,
          header = 1,
          position = i
        ),
        tidyr::tibble(
          characteristic = paste0(
            space,
            space,
            space,
            space,
            covariatesSubset$covariateName
          ),
          value = covariatesSubset$mean,
          header = 0,
          position = i,
          cohortId = covariatesSubset$cohortId,
          databaseId = covariatesSubset$databaseId
        )
      ) %>%
        dplyr::distinct() %>%
        dplyr::mutate(sortOrder = dplyr::row_number())
    }
  }
  if (nrow(resultsTable) > 0) {
    resultsTable <- resultsTable %>%
      dplyr::arrange(
        .data$databaseId,
        .data$cohortId,
        .data$position,
        dplyr::desc(.data$header),
        .data$sortOrder
      )
  }
  return(resultsTable)
}


prepareTable1Comp <- function(balance,
                              pathToCsv = "Table1Specs.csv") {
  balance <- balance %>%
    dplyr::mutate(covariateName = stringr::str_to_sentence(
      stringr::str_replace_all(
        string = .data$covariateName,
        pattern = "^.*: ",
        replacement = ""
      )
    ))
  space <- "&nbsp;"
  specifications <- readr::read_csv(
    file = pathToCsv,
    col_types = readr::cols(),
    guess_max = min(1e7)
  ) %>%
    dplyr::mutate(dplyr::across(
      tidyr::everything(),
      ~ tidyr::replace_na(data = .x, replace = '')
    ))
  
  resultsTable <- tidyr::tibble()
  
  if (nrow(specifications) == 0) {
    return(
      dplyr::tibble(Note = 'There are no covariate records for the cohorts being compared.')
    )
  }
  
  for (i in 1:nrow(specifications)) {
    specification <- specifications[i, ]
    if (specification %>% dplyr::pull(.data$covariateIds) == "") {
      balanceSubset <- balance %>%
        dplyr::filter(.data$analysisId %in% specification$analysisId) %>%
        dplyr::arrange(.data$covariateId)
    } else {
      balanceSubset <- balance %>%
        dplyr::filter(
          .data$analysisId %in% specification$analysisId,
          .data$covariateId %in% (
            stringr::str_split(
              string = (specification %>%
                          dplyr::pull(.data$covariateIds)),
              pattern = ";"
            )[[1]] %>%
              utils::type.convert(as.is = TRUE)
          )
        ) %>%
        dplyr::arrange(.data$covariateId)
    }
    
    if (nrow(balanceSubset) > 0) {
      resultsTable <- dplyr::bind_rows(
        resultsTable,
        tidyr::tibble(
          characteristic = paste0(
            '<strong>',
            specification %>% dplyr::pull(.data$label),
            '</strong>'
          ),
          MeanT = NA,
          MeanC = NA,
          StdDiff = NA,
          header = 1,
          position = i
        ),
        tidyr::tibble(
          cohortId1 = balanceSubset$cohortId1,
          cohortId2 = balanceSubset$cohortId2,
          characteristic = paste0(space,
                                  space,
                                  space,
                                  space,
                                  balanceSubset$covariateName),
          MeanT = balanceSubset$mean1,
          MeanC = balanceSubset$mean2,
          StdDiff = balanceSubset$absStdDiff,
          header = 0,
          position = i
        )
      ) %>%
        dplyr::distinct() %>%
        dplyr::mutate(sortOrder = dplyr::row_number())
    }
  }
  if (nrow(resultsTable) > 0) {
    resultsTable <- resultsTable %>%
      dplyr::arrange(.data$position,
                     dplyr::desc(.data$header),
                     .data$sortOrder) %>%
      dplyr::mutate(sortOrder = dplyr::row_number()) %>%
      dplyr::select(-.data$header, -.data$position) %>%
      dplyr::distinct()
  }
  
  resultsTable <- resultsTable %>% 
    dplyr::mutate(characteristic = stringr::str_replace(string = .data$characteristic, 
                                                        pattern = "black or african american", 
                                                        replacement = "Black or African American")) %>% 
    dplyr::mutate(characteristic = stringr::str_replace(string = .data$characteristic, pattern = "white", replacement = "White")) %>% 
    dplyr::mutate(characteristic = stringr::str_replace(string = .data$characteristic, pattern = "asian", replacement = "Asian"))
  
  return(resultsTable)
}


compareCohortCharacteristics <-
  function(characteristics1, characteristics2) {
    m <- dplyr::full_join(
      x = characteristics1 %>% dplyr::distinct(),
      y = characteristics2 %>% dplyr::distinct(),
      by = c(
        "covariateId",
        "conceptId",
        "databaseId",
        "covariateName",
        "analysisId",
        "isBinary",
        "analysisName",
        "domainId"
      ),
      suffix = c("1", "2")
    ) %>%
      dplyr::mutate(
        sd = sqrt(.data$sd1 ^ 2 + .data$sd2 ^ 2),
        stdDiff = (.data$mean2 - .data$mean1) / .data$sd
      ) %>%
      dplyr::arrange(-abs(.data$stdDiff))
    return(m)
  }


compareTemporalCohortCharacteristics <-
  function(characteristics1, characteristics2) {
    m <- characteristics1 %>%
      dplyr::full_join(
        characteristics2,
        by = c(
          "covariateId",
          "conceptId",
          "databaseId",
          "covariateName",
          "analysisId",
          "isBinary",
          "analysisName",
          "domainId",
          "timeId",
          "startDay",
          "endDay",
          "choices"
        ),
        suffix = c("1", "2")
      ) %>%
      dplyr::mutate(
        sd = sqrt(.data$sd1 ^ 2 + .data$sd2 ^ 2),
        stdDiff = (.data$mean2 - .data$mean1) / .data$sd
      ) %>%
      dplyr::arrange(-abs(.data$stdDiff))
    return(m)
  }

