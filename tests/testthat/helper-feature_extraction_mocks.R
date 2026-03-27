# FeatureExtraction Mocking Functions
# Mock FeatureExtraction package calls for unit testing

#' Mock FeatureExtraction::getDbCovariateData
#'
#' @param connection Connection (ignored)
#' @param cdmDatabaseSchema CDM schema (ignored)
#' @param cohortTable Cohort table (ignored)
#' @param cohortId Cohort ID
#' @param covariateSettings Covariate settings (ignored)
#' @param aggregated Whether to aggregate (default: TRUE)
#' @return Mock covariate data structure
mockGetDbCovariateData <- function(connection,
                                   cdmDatabaseSchema,
                                   cohortTable,
                                   cohortId,
                                   covariateSettings,
                                   aggregated = TRUE) {
  # Determine if temporal
  isTemporal <- inherits(covariateSettings, "temporalCovariateSettings")
  
  if (isTemporal) {
    # Return temporal covariate data
    source(testthat::test_path("fixtures/mock_data.R"))
    return(createMockTemporalCovariateData(cohortIds = cohortId))
  } else {
    # Return regular covariate data
    covariateIds <- 1:10
    
    covariates <- dplyr::tibble(
      cohortDefinitionId = cohortId,
      covariateId = covariateIds,
      sumValue = runif(length(covariateIds), 0, 100),
      averageValue = runif(length(covariateIds), 0, 1)
    )
    
    covariateRef <- dplyr::tibble(
      covariateId = covariateIds,
      covariateName = paste0("Covariate ", covariateIds),
      analysisId = 1,
      conceptId = covariateIds * 1000
    )
    
    analysisRef <- dplyr::tibble(
      analysisId = 1,
      analysisName = "Mock Analysis",
      domainId = "Condition",
      isBinary = "Y",
      missingMeansZero = "Y"
    )
    
    structure(
      list(
        covariates = covariates,
        covariateRef = covariateRef,
        analysisRef = analysisRef
      ),
      class = "CovariateData"
    )
  }
}

#' Mock temporal covariate settings
#'
#' @param ... Arguments (ignored, uses defaults)
#' @return Mock temporal covariate settings
mockCreateTemporalCovariateSettings <- function(...) {
  structure(
    list(
      temporal = TRUE,
      temporalStartDays = c(-365, -30, 0),
      temporalEndDays = c(-31, -1, 0),
      useConditionOccurrence = TRUE,
      useDrugEraStart = TRUE
    ),
    class = c("temporalCovariateSettings", "covariateSettings")
  )
}

#' Check if covariate data is temporal
#'
#' @param covariateData Covariate data object
#' @return TRUE if temporal, FALSE otherwise
mockIsTemporalCovariateData <- function(covariateData) {
  inherits(covariateData, "TemporalCovariateData") ||
    "timeRef" %in% names(covariateData)
}

#' Mock Andromeda object for covariate data
#'
#' @param data List of data frames
#' @return Mock Andromeda object
mockAndromedaObject <- function(data = list()) {
  # Create a simple list-based mock of Andromeda
  structure(
    data,
    class = c("Andromeda", "list")
  )
}

#' Mock aggregateCovariates function
#'
#' @param covariates Covariate data
#' @param cohortId Cohort ID
#' @return Aggregated covariates
mockAggregateCovariates <- function(covariates, cohortId) {
  if (is.data.frame(covariates)) {
    covariates %>%
      dplyr::group_by(cohortDefinitionId, covariateId) %>%
      dplyr::summarise(
        sumValue = sum(sumValue, na.rm = TRUE),
        averageValue = mean(averageValue, na.rm = TRUE),
        .groups = "drop"
      )
  } else {
    covariates
  }
}
