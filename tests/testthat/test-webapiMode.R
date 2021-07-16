
test_that("Check WebApi mode", {
  skip_if_not(runDatabaseTests)
  baseUrl <- Sys.getenv("WEBAPI_TEST_WEBAPI_URL") #nonsecure test environment
  if (any(is.null(baseUrl),
          length(baseUrl) <= 1)) {
    baseUrl <- "http://api.ohdsi.org:8080/WebAPI"  # default to atlas-demo.ohdsi.org - this is probably useful for local testers
  }
  sourceKeyVariable <- 'SYNPUF5PCT' #source db
  
  cohorts <- ROhdsiWebApi::getCohortDefinitionsMetaData(baseUrl = baseUrl) %>% 
    dplyr::filter(!is.na(.data$modifiedDate)) %>% 
    dplyr::filter(stringr::str_detect(string = toupper(.data$name),
                                      pattern = 'OHDSI')) %>% 
    dplyr::filter(stringr::str_detect(string = toupper(.data$name),
                                      pattern = 'COPY', 
                                      negate = TRUE)) %>% 
    dplyr::filter(stringr::str_detect(string = toupper(.data$name),
                                      pattern = 'TEST', 
                                      negate = TRUE)) %>% 
    dplyr::filter(!is.na(.data$description))
  
  if (nrow(cohorts) > 0) {
    # pick some cohort ids by random
    cohortIds <- cohorts %>% 
      dplyr::sample_n(size = 3) %>% 
      dplyr::select(.data$id) %>% 
      dplyr::pull()
    
    # get specifications for the cohortIds above
    webApiCohorts <-
      ROhdsiWebApi::getCohortDefinitionsMetaData(baseUrl = baseUrl) %>%
      dplyr::filter(.data$id %in% cohortIds)
    
    cohortsToCreate <- list()
    for (i in (1:nrow(webApiCohorts))) {
      cohortId <- webApiCohorts$id[[i]]
      cohortDefinition <-
        ROhdsiWebApi::getCohortDefinition(cohortId = cohortId,
                                          baseUrl = baseUrl)
      cohortsToCreate[[i]] <- tidyr::tibble(
        atlasId = webApiCohorts$id[[i]],
        atlasName = stringr::str_trim(string = stringr::str_squish(cohortDefinition$name)),
        cohortId = webApiCohorts$id[[i]],
        name = stringr::str_trim(stringr::str_squish(cohortDefinition$name))
      )
    }
    cohortSetReference <- dplyr::bind_rows(cohortsToCreate)
    
    CohortDiagnostics::instantiateCohortSet(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortSetReference = cohortSetReference,
      generateInclusionStats = TRUE,
      createCohortTable = TRUE,
      baseUrl = baseUrl,
      inclusionStatisticsFolder = file.path(folder, "incStats")
    )
  }
})