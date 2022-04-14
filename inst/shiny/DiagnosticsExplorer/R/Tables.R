library(magrittr)

prepareTable1 <- function(covariates,
                          prettyTable1Specifications,
                          cohort) {
  if (!all(is.data.frame(prettyTable1Specifications),
           nrow(prettyTable1Specifications) > 0)) {
    return(NULL)
  }
  keyColumns <- prettyTable1Specifications %>%
    dplyr::select(.data$labelOrder,
                  .data$label,
                  .data$covariateId,
                  .data$analysisId,
                  .data$sequence) %>%
    dplyr::distinct() %>%
    dplyr::left_join(
      covariates %>%
        dplyr::select(.data$covariateId,
                      .data$covariateName) %>%
        dplyr::distinct(),
      by = c("covariateId")
    ) %>%
    dplyr::filter(!is.na(.data$covariateName)) %>%
    tidyr::crossing(
      covariates %>%
        dplyr::select(.data$cohortId,
                      .data$databaseId) %>%
        dplyr::distinct()
    ) %>%
    dplyr::arrange(.data$cohortId,
                   .data$databaseId,
                   .data$analysisId,
                   .data$covariateId) %>%
    dplyr::mutate(
      covariateName = stringr::str_replace(
        string = .data$covariateName,
        pattern = "black or african american",
        replacement = "Black or African American"
      )
    ) %>%
    dplyr::mutate(
      covariateName = stringr::str_replace(
        string = .data$covariateName,
        pattern = "white",
        replacement = "White"
      )
    ) %>%
    dplyr::mutate(
      covariateName = stringr::str_replace(
        string = .data$covariateName,
        pattern = "asian",
        replacement = "Asian"
      )
    )
  
  covariates <- keyColumns %>%
    dplyr::left_join(
      covariates %>%
        dplyr::select(-.data$covariateName),
      by = c("cohortId",
             "databaseId",
             "covariateId",
             "analysisId")
    ) %>%
    dplyr::filter(!is.na(.data$covariateName))
  
  space <- "&nbsp;"
  resultsTable <- tidyr::tibble()
  
  # labels
  tableHeaders <-
    covariates %>%
    dplyr::select(.data$cohortId,
                  .data$databaseId,
                  .data$label,
                  .data$labelOrder,
                  .data$sequence) %>%
    dplyr::distinct() %>%
    dplyr::group_by(.data$cohortId,
                    .data$databaseId,
                    .data$label,
                    .data$labelOrder) %>%
    dplyr::summarise(sequence = min(.data$sequence),
                     .groups = 'keep') %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      characteristic = paste0('<strong>',
                              .data$label,
                              '</strong>'),
      header = 1
    ) %>%
    dplyr::select(
      .data$cohortId,
      .data$databaseId,
      .data$sequence,
      .data$header,
      .data$labelOrder,
      .data$characteristic
    ) %>%
    dplyr::distinct()
  
  tableValues <-
    covariates %>%
    dplyr::mutate(
      characteristic = paste0(space,
                              space,
                              space,
                              space,
                              .data$covariateName),
      header = 0,
      valueCount = .data$sumValue
    ) %>%
    dplyr::select(
      .data$cohortId,
      .data$databaseId,
      .data$covariateId,
      .data$analysisId,
      .data$sequence,
      .data$header,
      .data$labelOrder,
      .data$characteristic,
      .data$valueCount
    )
  
  table <- dplyr::bind_rows(tableHeaders, tableValues) %>%
    dplyr::mutate(sequence = .data$sequence - .data$header) %>%
    dplyr::arrange(.data$sequence) %>%
    dplyr::select(
      .data$cohortId,
      .data$databaseId,
      .data$sequence,
      .data$characteristic,
      .data$valueCount
    ) %>%
    dplyr::rename(count = .data$valueCount) %>%
    dplyr::inner_join(cohort %>%
                        dplyr::select(.data$cohortId,
                                      .data$shortName),
                      by = "cohortId") %>%
    dplyr::group_by(.data$databaseId,
                    .data$characteristic,
                    .data$shortName) %>%
    dplyr::summarise(sequence = min(.data$sequence),
                     count = min(.data$count),
                     .groups = 'keep') %>%
    dplyr::ungroup() %>% 
    tidyr::pivot_wider(
      id_cols = c(.data$databaseId,
                  .data$characteristic,
                  .data$sequence),
      values_from = .data$count,
      names_from = .data$shortName
    ) %>%
    dplyr::arrange(.data$sequence)
  
  
  
  if (nrow(table) == 0) {
    return(NULL)
  }
  return(table)
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
        "domainId",
        "timeId",
        "startDay",
        "endDay"
      ),
      suffix = c("1", "2")
    ) %>%
      dplyr::mutate(
        sd = sqrt(.data$sd1 ^ 2 + .data$sd2 ^ 2),
        stdDiff = (.data$mean2 - .data$mean1) / .data$sd
      ) %>%
      dplyr::arrange(-abs(.data$stdDiff)) %>% 
      dplyr::mutate(stdDiff = dplyr::na_if(.data$sd, "Inf"))
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

