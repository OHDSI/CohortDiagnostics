#Prepare table 1 ----
prepareTable1 <- function(covariates,
                          prettyTable1Specifications,
                          cohort) {
  covariates2 <- covariates
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
      covariates2 %>%
        dplyr::select(.data$covariateId,
                      .data$covariateName) %>%
        dplyr::distinct(),
      by = c("covariateId")
    ) %>%
    dplyr::filter(!is.na(.data$covariateName)) %>%
    tidyr::crossing(
      covariates2 %>%
        dplyr::select(.data$cohortId,
                      .data$databaseId) %>%
        dplyr::distinct()
    ) %>%
    dplyr::arrange(.data$cohortId,
                   .data$databaseId,
                   .data$analysisId,
                   .data$covariateId) %>%
    dplyr::mutate(covariateName = stringr::str_to_sentence(
      stringr::str_replace_all(
        string = .data$covariateName,
        pattern = "^.*: ",
        replacement = ""
      )
    )) %>%
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
  
  covariates2 <- keyColumns %>%
    dplyr::left_join(
      covariates2 %>%
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
    covariates2 %>%
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
    covariates2 %>%
    dplyr::mutate(
      characteristic = paste0(space,
                              space,
                              space,
                              space,
                              .data$covariateName),
      header = 0,
      valueMean = .data$mean,
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
      .data$valueCount,
      .data$valueMean
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


# #Prepare table for comparision ----
# prepareTable1Comp <- function(balance,
#                               pathToCsv = "Table1Specs.csv") {
#   balance <- balance %>%
#     dplyr::mutate(covariateName = stringr::str_to_sentence(
#       stringr::str_replace_all(
#         string = .data$covariateName,
#         pattern = "^.*: ",
#         replacement = ""
#       )
#     ))
#   space <- "&nbsp;"
#   specifications <- readr::read_csv(
#     file = pathToCsv,
#     col_types = readr::cols(),
#     guess_max = min(1e7)
#   ) %>%
#     dplyr::mutate(dplyr::across(
#       tidyr::everything(),
#       ~ tidyr::replace_na(data = .x, replace = '')
#     ))
#   
#   resultsTable <- tidyr::tibble()
#   
#   if (nrow(specifications) == 0) {
#     return(
#       dplyr::tibble(Note = 'There are no covariate records for the cohorts being compared.')
#     )
#   }
#   
#   for (i in 1:nrow(specifications)) {
#     specification <- specifications[i, ]
#     if (specification %>% dplyr::pull(.data$covariateIds) == "") {
#       balanceSubset <- balance %>%
#         dplyr::filter(.data$analysisId %in% specification$analysisId) %>%
#         dplyr::arrange(.data$covariateId)
#     } else {
#       balanceSubset <- balance %>%
#         dplyr::filter(
#           .data$analysisId %in% specification$analysisId,
#           .data$covariateId %in% (
#             stringr::str_split(
#               string = (specification %>%
#                           dplyr::pull(.data$covariateIds)),
#               pattern = ";"
#             )[[1]] %>%
#               utils::type.convert(as.is = TRUE)
#           )
#         ) %>%
#         dplyr::arrange(.data$covariateId)
#     }
#     
#     if (nrow(balanceSubset) > 0) {
#       resultsTable <- dplyr::bind_rows(
#         resultsTable,
#         tidyr::tibble(
#           characteristic = paste0(
#             '<strong>',
#             specification %>% dplyr::pull(.data$label),
#             '</strong>'
#           ),
#           MeanT = NA,
#           MeanC = NA,
#           StdDiff = NA,
#           header = 1,
#           position = i
#         ),
#         tidyr::tibble(
#           cohortId1 = balanceSubset$cohortId1,
#           cohortId2 = balanceSubset$cohortId2,
#           characteristic = paste0(space,
#                                   space,
#                                   space,
#                                   space,
#                                   balanceSubset$covariateName),
#           MeanT = balanceSubset$mean1,
#           MeanC = balanceSubset$mean2,
#           databaseId = balanceSubset$databaseId,
#           StdDiff = balanceSubset$absStdDiff,
#           header = 0,
#           position = i
#         )
#       ) %>%
#         dplyr::distinct() %>%
#         dplyr::mutate(sortOrder = dplyr::row_number())
#     }
#   }
#   if (nrow(resultsTable) > 0) {
#     resultsTable <- resultsTable %>%
#       dplyr::arrange(.data$position,
#                      dplyr::desc(.data$header),
#                      .data$sortOrder) %>%
#       dplyr::mutate(sortOrder = dplyr::row_number()) %>%
#       dplyr::select(-.data$header, -.data$position) %>%
#       dplyr::distinct()
#   }
#   
#   resultsTable <- resultsTable %>%
#     dplyr::mutate(
#       characteristic = stringr::str_replace(
#         string = .data$characteristic,
#         pattern = "black or african american",
#         replacement = "Black or African American"
#       )
#     ) %>%
#     dplyr::mutate(
#       characteristic = stringr::str_replace(
#         string = .data$characteristic,
#         pattern = "white",
#         replacement = "White"
#       )
#     ) %>%
#     dplyr::mutate(
#       characteristic = stringr::str_replace(
#         string = .data$characteristic,
#         pattern = "asian",
#         replacement = "Asian"
#       )
#     )
#   
#   return(resultsTable)
# }
# 

# Compare cohort characteristics ----
compareCohortCharacteristics <-
  function(characteristics1, 
           characteristics2,
           value,
           joinBy) {
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
        "analysisNameLong",
        "domainId",
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
      dplyr::filter(!is.na(.data$cohortId1)) %>% 
      dplyr::filter(!is.na(.data$cohortId2))
    return(m)
  }

#Compare temporal characteristics ----
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
          "analysisNameLong",
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
      dplyr::arrange(-abs(.data$stdDiff)) %>% 
      dplyr::filter(!is.na(.data$cohortId1)) %>% 
      dplyr::filter(!is.na(.data$cohortId2)) %>% 
      dplyr::filter(!is.na(.data$timeId))
    return(m)
  }
