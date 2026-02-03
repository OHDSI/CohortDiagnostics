# Test Cohort Definitions
# Pre-defined minimal cohort definitions for testing

#' Get a minimal cohort definition set with one cohort
#'
#' @return A data frame with one minimal cohort definition
getMinimalCohortDefinitionSet <- function() {
  dplyr::tibble(
    cohortId = 1L,
    cohortName = "Minimal Test Cohort",
    sql = "SELECT 1 as cohort_definition_id, 1 as subject_id, 
           CAST('2020-01-01' AS DATE) as cohort_start_date,
           CAST('2020-12-31' AS DATE) as cohort_end_date;",
    json = as.character(jsonlite::toJSON(list(
      ConceptSets = list(),
      PrimaryCriteria = list(
        CriteriaList = list(),
        ObservationWindow = list(PriorDays = 0, PostDays = 0),
        PrimaryCriteriaLimit = list(Type = "First")
      ),
      cdmVersionRange = ">=5.0.0"
    ), auto_unbox = TRUE)),
    checksum = digest::digest("minimal_cohort_1")
  )
}

#' Get a single complete cohort definition
#'
#' @param cohortId Cohort ID to use (default: 1)
#' @return A data frame with one complete cohort definition
getSingleCohortDefinition <- function(cohortId = 1L) {
  dplyr::tibble(
    cohortId = cohortId,
    cohortName = paste0("Test Cohort ", cohortId),
    sql = sprintf("
      SELECT %d as cohort_definition_id, 
             subject_id, 
             cohort_start_date,
             cohort_end_date
      FROM @cohort_database_schema.@cohort_table
      WHERE cohort_definition_id = %d;
    ", cohortId, cohortId),
    json = as.character(jsonlite::toJSON(list(
      ConceptSets = list(
        list(
          id = 0,
          name = "Test Concept Set",
          expression = list(
            items = list(
              list(
                concept = list(
                  CONCEPT_ID = 313217,
                  CONCEPT_NAME = "Atrial fibrillation",
                  STANDARD_CONCEPT = "S",
                  DOMAIN_ID = "Condition"
                ),
                isExcluded = FALSE,
                includeDescendants = TRUE
              )
            )
          )
        )
      ),
      PrimaryCriteria = list(
        CriteriaList = list(
          list(
            ConditionOccurrence = list(
              CodesetId = 0,
              ConditionTypeExclude = FALSE
            )
          )
        ),
        ObservationWindow = list(
          PriorDays = 365,
          PostDays = 0
        ),
        PrimaryCriteriaLimit = list(Type = "First")
      ),
      QualifiedLimit = list(Type = "First"),
      ExpressionLimit = list(Type = "First"),
      InclusionRules = list(),
      CensoringCriteria = list(),
      CollapseSettings = list(
        CollapseType = "ERA",
        EraPad = 0
      ),
      CensorWindow = list(),
      cdmVersionRange = ">=5.0.0"
    ), auto_unbox = TRUE)),
    checksum = digest::digest(paste0("cohort_", cohortId))
  )
}

#' Get multiple cohort definitions for testing relationships
#'
#' @param numCohorts Number of cohorts to create (default: 3)
#' @return A data frame with multiple cohort definitions
getMultipleCohortDefinitions <- function(numCohorts = 3L) {
  cohortIds <- seq_len(numCohorts)
  
  purrr::map_dfr(cohortIds, function(id) {
    getSingleCohortDefinition(cohortId = id)
  })
}

#' Get cohort definition with subset
#'
#' @return A data frame with parent and subset cohort definitions
getCohortDefinitionWithSubset <- function() {
  parent <- getSingleCohortDefinition(cohortId = 1L)
  
  subset <- dplyr::tibble(
    cohortId = 2L,
    cohortName = "Test Cohort 1 - Subset",
    sql = parent$sql,
    json = parent$json,
    checksum = digest::digest("cohort_1_subset"),
    isSubset = TRUE,
    subsetParent = 1L
  )
  
  parent$isSubset <- FALSE
  parent$subsetParent <- NA_integer_
  
  dplyr::bind_rows(parent, subset)
}

#' Get cohort definition with concept sets
#'
#' @param numConceptSets Number of concept sets to include
#' @return A data frame with one cohort definition containing concept sets
getCohortDefinitionWithConceptSets <- function(numConceptSets = 2L) {
  conceptSets <- purrr::map(seq_len(numConceptSets), function(i) {
    list(
      id = i - 1L,
      name = paste0("Concept Set ", i),
      expression = list(
        items = list(
          list(
            concept = list(
              CONCEPT_ID = 313217 + i,
              CONCEPT_NAME = paste0("Test Concept ", i),
              STANDARD_CONCEPT = "S",
              DOMAIN_ID = "Condition"
            ),
            isExcluded = FALSE,
            includeDescendants = TRUE
          )
        )
      )
    )
  })
  
  dplyr::tibble(
    cohortId = 1L,
    cohortName = "Cohort with Concept Sets",
    sql = "SELECT * FROM cohort WHERE cohort_definition_id = 1;",
    json = as.character(jsonlite::toJSON(list(
      ConceptSets = conceptSets,
      PrimaryCriteria = list(
        CriteriaList = list(
          list(
            ConditionOccurrence = list(
              CodesetId = 0,
              ConditionTypeExclude = FALSE
            )
          )
        ),
        ObservationWindow = list(PriorDays = 0, PostDays = 0),
        PrimaryCriteriaLimit = list(Type = "First")
      ),
      cdmVersionRange = ">=5.0.0"
    ), auto_unbox = TRUE)),
    checksum = digest::digest("cohort_with_concept_sets")
  )
}
