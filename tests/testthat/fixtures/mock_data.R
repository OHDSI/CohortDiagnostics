# Mock Data Generators for Unit Tests
# These functions create mock data structures for testing without requiring database connections

#' Create a mock cohort definition set
#'
#' @param numCohorts Number of cohorts to include (default: 1)
#' @param includeSubsets Include subset cohorts (default: FALSE)
#' @return A data frame with cohort definitions
createMockCohortDefinitionSet <- function(numCohorts = 1, includeSubsets = FALSE) {
  cohortIds <- seq_len(numCohorts)
  
  cohortDefs <- dplyr::tibble(
    cohortId = cohortIds,
    cohortName = paste0("Test Cohort ", cohortIds),
    sql = paste0("SELECT * FROM cohort WHERE cohort_definition_id = ", cohortIds, ";"),
    json = sapply(cohortIds, function(id) {
      jsonlite::toJSON(list(
        ConceptSets = list(),
        PrimaryCriteria = list(
          CriteriaList = list(),
          ObservationWindow = list(
            PriorDays = 0,
            PostDays = 0
          ),
          PrimaryCriteriaLimit = list(Type = "First")
        ),
        cdmVersionRange = ">=5.0.0"
      ), auto_unbox = TRUE)
    }),
    checksum = sapply(cohortIds, function(id) {
      digest::digest(paste0("cohort_", id))
    })
  )
  
  if (includeSubsets) {
    cohortDefs$isSubset <- FALSE
    cohortDefs$subsetParent <- NA_integer_
  }
  
  return(cohortDefs)
}

#' Create mock connection details
#'
#' @return A list mimicking DatabaseConnector connection details
createMockConnectionDetails <- function() {
  structure(
    list(
      dbms = "sqlite",
      server = ":memory:",
      user = "",
      password = "",
      port = "",
      extraSettings = "",
      oracleDriver = "thin",
      pathToDriver = ""
    ),
    class = "connectionDetails"
  )
}

#' Create mock cohort counts
#'
#' @param cohortIds Vector of cohort IDs
#' @param counts Vector of counts (optional, will generate random if NULL)
#' @return A data frame with cohort counts
createMockCohortCounts <- function(cohortIds = c(1, 2, 3), counts = NULL) {
  if (is.null(counts)) {
    counts <- sample(100:1000, length(cohortIds))
  }
  
  dplyr::tibble(
    cohortId = cohortIds,
    cohortEntries = counts,
    cohortSubjects = pmax(1, as.integer(counts * runif(length(counts), 0.5, 0.9)))
  )
}

#' Create mock temporal covariate data (Andromeda-like structure)
#'
#' @param cohortIds Vector of cohort IDs
#' @param numCovariates Number of covariates to generate
#' @return A list mimicking Andromeda temporal covariate data
createMockTemporalCovariateData <- function(cohortIds = c(1), numCovariates = 10) {
  # Create mock covariates
  covariateIds <- seq_len(numCovariates)
  
  covariates <- expand.grid(
    cohortDefinitionId = cohortIds,
    timeId = 1:3,
    covariateId = covariateIds
  ) %>%
    dplyr::as_tibble() %>%
    dplyr::mutate(
      sumValue = runif(dplyr::n(), 0, 100),
      averageValue = runif(dplyr::n(), 0, 1)
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
  
  timeRef <- dplyr::tibble(
    timeId = 1:3,
    startDay = c(-365, -30, 0),
    endDay = c(-31, -1, 0)
  )
  
  # Return structure similar to FeatureExtraction output
  structure(
    list(
      covariates = covariates,
      covariateRef = covariateRef,
      analysisRef = analysisRef,
      timeRef = timeRef
    ),
    class = c("CovariateData", "TemporalCovariateData")
  )
}

#' Create mock incidence rate data
#'
#' @param cohortId Cohort ID
#' @param numRows Number of rows to generate
#' @return A data frame with incidence rate data
createMockIncidenceRateData <- function(cohortId = 1, numRows = 10) {
  dplyr::tibble(
    cohortId = cohortId,
    calendarYear = rep(2015:2019, length.out = numRows),
    ageGroup = rep(c("0-9", "10-19", "20-29"), length.out = numRows),
    gender = rep(c("Male", "Female"), length.out = numRows),
    cohortCount = sample(10:100, numRows, replace = TRUE),
    personYears = runif(numRows, 100, 1000),
    incidenceRate = NA_real_
  ) %>%
    dplyr::mutate(
      incidenceRate = 1000 * cohortCount / personYears
    )
}

#' Create mock time series data
#'
#' @param cohortIds Vector of cohort IDs
#' @param startDate Start date for time series
#' @param endDate End date for time series
#' @return A data frame with time series data
createMockTimeSeriesData <- function(cohortIds = c(1), 
                                     startDate = as.Date("2015-01-01"),
                                     endDate = as.Date("2019-12-31")) {
  dates <- seq(startDate, endDate, by = "month")
  
  expand.grid(
    cohortId = cohortIds,
    periodBegin = dates
  ) %>%
    dplyr::as_tibble() %>%
    dplyr::mutate(
      calendarInterval = "m",
      seriesType = "T1",
      recordsStart = sample(0:10, dplyr::n(), replace = TRUE),
      recordsEnd = sample(0:10, dplyr::n(), replace = TRUE),
      subjectsStart = sample(0:10, dplyr::n(), replace = TRUE),
      subjectsEnd = sample(0:10, dplyr::n(), replace = TRUE),
      subjectsStartIn = sample(0:5, dplyr::n(), replace = TRUE),
      subjectsEndIn = sample(0:5, dplyr::n(), replace = TRUE)
    )
}

#' Create mock observation period date range
#'
#' @return A data frame with observation period min/max dates
createMockObservationPeriodDateRange <- function() {
  dplyr::tibble(
    observationPeriodMinDate = as.Date("2000-01-01"),
    observationPeriodMaxDate = as.Date("2020-12-31")
  )
}

#' Create mock concept set data
#'
#' @param numConceptSets Number of concept sets
#' @param conceptsPerSet Number of concepts per set
#' @return A data frame with concept set data
createMockConceptSetData <- function(numConceptSets = 3, conceptsPerSet = 5) {
  expand.grid(
    conceptSetId = seq_len(numConceptSets),
    conceptId = seq_len(conceptsPerSet)
  ) %>%
    dplyr::as_tibble() %>%
    dplyr::mutate(
      conceptName = paste0("Concept ", conceptId),
      domainId = "Condition",
      vocabularyId = "SNOMED",
      conceptClassId = "Clinical Finding",
      standardConcept = "S",
      conceptCode = as.character(conceptId * 1000)
    )
}
