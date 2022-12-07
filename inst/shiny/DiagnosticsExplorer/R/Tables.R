library(magrittr)

prepareTable1 <- function(covariates,
                          prettyTable1Specifications,
                          cohort) {
  if (!all(
    is.data.frame(prettyTable1Specifications),
    nrow(prettyTable1Specifications) > 0
  )) {
    return(NULL)
  }
  keyColumns <- prettyTable1Specifications %>%
    dplyr::select(
      labelOrder,
      label,
      covariateId,
      analysisId,
      sequence
    ) %>%
    dplyr::distinct() %>%
    dplyr::left_join(
      covariates %>%
        dplyr::select(
          covariateId,
          covariateName
        ) %>%
        dplyr::distinct(),
      by = c("covariateId")
    ) %>%
    dplyr::filter(!is.na(covariateName)) %>%
    tidyr::crossing(
      covariates %>%
        dplyr::select(
          cohortId,
          databaseId
        ) %>%
        dplyr::distinct()
    ) %>%
    dplyr::arrange(
      cohortId,
      databaseId,
      analysisId,
      covariateId
    ) %>%
    dplyr::mutate(
      covariateName = stringr::str_replace(
        string = covariateName,
        pattern = "black or african american",
        replacement = "Black or African American"
      )
    ) %>%
    dplyr::mutate(
      covariateName = stringr::str_replace(
        string = covariateName,
        pattern = "white",
        replacement = "White"
      )
    ) %>%
    dplyr::mutate(
      covariateName = stringr::str_replace(
        string = covariateName,
        pattern = "asian",
        replacement = "Asian"
      )
    )

  covariates <- keyColumns %>%
    dplyr::left_join(
      covariates %>%
        dplyr::select(-covariateName),
      by = c(
        "cohortId",
        "databaseId",
        "covariateId",
        "analysisId"
      )
    ) %>%
    dplyr::filter(!is.na(covariateName))

  space <- "&nbsp;"
  resultsTable <- tidyr::tibble()

  # labels
  tableHeaders <-
    covariates %>%
    dplyr::select(
      cohortId,
      databaseId,
      label,
      labelOrder,
      sequence
    ) %>%
    dplyr::distinct() %>%
    dplyr::group_by(
      cohortId,
      databaseId,
      label,
      labelOrder
    ) %>%
    dplyr::summarise(
      sequence = min(sequence),
      .groups = "keep"
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      characteristic = paste0(
        "<strong>",
        label,
        "</strong>"
      ),
      header = 1
    ) %>%
    dplyr::select(
      cohortId,
      databaseId,
      sequence,
      header,
      labelOrder,
      characteristic
    ) %>%
    dplyr::distinct()

  tableValues <-
    covariates %>%
    dplyr::mutate(
      characteristic = paste0(
        space,
        space,
        space,
        space,
        covariateName
      ),
      header = 0,
      valueCount = sumValue
    ) %>%
    dplyr::select(
      cohortId,
      databaseId,
      covariateId,
      analysisId,
      sequence,
      header,
      labelOrder,
      characteristic,
      valueCount
    )

  table <- dplyr::bind_rows(tableHeaders, tableValues) %>%
    dplyr::mutate(sequence = sequence - header) %>%
    dplyr::arrange(sequence) %>%
    dplyr::select(
      cohortId,
      databaseId,
      sequence,
      characteristic,
      valueCount
    ) %>%
    dplyr::rename(count = valueCount) %>%
    dplyr::inner_join(cohort %>%
      dplyr::select(
        cohortId,
        shortName
      ),
    by = "cohortId"
    ) %>%
    dplyr::group_by(
      databaseId,
      characteristic,
      shortName
    ) %>%
    dplyr::summarise(
      sequence = min(sequence),
      count = min(count),
      .groups = "keep"
    ) %>%
    dplyr::ungroup() %>%
    tidyr::pivot_wider(
      id_cols = c(
        databaseId,
        characteristic,
        sequence
      ),
      values_from = count,
      names_from = shortName
    ) %>%
    dplyr::arrange(sequence)



  if (nrow(table) == 0) {
    return(NULL)
  }
  return(table)
}

compareCohortCharacteristics <-
  function(characteristics1, characteristics2) {
    characteristics1Renamed <- characteristics1 %>%
      dplyr::rename(
        sumValue1 = sumValue,
        mean1 = mean,
        sd1 = sd,
        cohortId1 = cohortId
      )
    cohortId1Value <- characteristics1Renamed$cohortId1 %>% unique()
    if (length(cohortId1Value) > 1) {
      stop("Can only compare one target cohort id to one comparator cohort id")
    }

    characteristics2Renamed <- characteristics2 %>%
      dplyr::rename(
        sumValue2 = sumValue,
        mean2 = mean,
        sd2 = sd,
        cohortId2 = cohortId
      )
    cohortId2Value <- characteristics2Renamed$cohortId2 %>% unique()
    if (length(cohortId2Value) > 1) {
      stop("Can only compare one target cohort id to one comparator cohort id")
    }

    characteristics <- characteristics1Renamed %>%
      dplyr::full_join(
        characteristics2Renamed,
        na_matches = c("na"),
        by = c(
          "timeId",
          "startDay",
          "endDay",
          "temporalChoices",
          "analysisId",
          "covariateId",
          "covariateName",
          "isBinary",
          "conceptId",
          "analysisName",
          "domainId"
        )
      ) %>%
      dplyr::mutate(
        mean2 = ifelse(is.na(mean2), 0, mean2),
        sd2 = ifelse(is.na(sd2), 0, sd2),
        sd1 = ifelse(is.na(sd1), 0, sd1),
        mean1 = ifelse(is.na(mean1), 0, mean1),
      ) %>%
      dplyr::mutate(
        sdd = sqrt(sd1^2 + sd2^2)
      )

    characteristics$stdDiff <- (characteristics$mean1 - characteristics$mean2) / characteristics$sdd

    characteristics <- characteristics %>%
      dplyr::arrange(-abs(stdDiff)) %>%
      dplyr::mutate(stdDiff = dplyr::na_if(stdDiff, 0)) %>%
      dplyr::mutate(
        absStdDiff = abs(stdDiff),
        cohortId1 = !!cohortId1Value,
        cohortId2 = !!cohortId2Value,
      )

    return(characteristics)
  }
