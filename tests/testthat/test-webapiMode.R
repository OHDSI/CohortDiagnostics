testthat::test_that("Check WebApi mode", {
  testthat::skip_if_not(runDatabaseTests)
  baseUrl <-
    Sys.getenv("WEBAPI_TEST_WEBAPI_URL") #nonsecure test environment
  if (any(is.null(baseUrl),
          length(baseUrl) <= 1)) {
    baseUrl <-
      "http://api.ohdsi.org:8080/WebAPI"  # default to atlas-demo.ohdsi.org - this is probably useful for local testers
  }
  sourceKeyVariable <- 'SYNPUF5PCT' #source db
  
  cohorts <-
    ROhdsiWebApi::getCohortDefinitionsMetaData(baseUrl = baseUrl) %>%
    dplyr::filter(!is.na(.data$modifiedDate)) %>%
    dplyr::filter(stringr::str_detect(string = toupper(.data$name),
                                      pattern = 'OHDSI')) %>%
    dplyr::filter(stringr::str_detect(
      string = toupper(.data$name),
      pattern = 'COPY',
      negate = TRUE
    )) %>%
    dplyr::filter(stringr::str_detect(
      string = toupper(.data$name),
      pattern = 'TEST',
      negate = TRUE
    )) %>%
    dplyr::filter(!is.na(.data$description))
  
  backwardCompatibilityTest1 <- cohorts %>% dplyr::select(.data$id)
  backwardCompatibilityTest1 <-
    CohortDiagnostics:::makeBackwardsCompatible(cohorts = backwardCompatibilityTest1,
                                                forceWebApiCohortId = TRUE)
  backwardCompatibilityTest2 <-
    cohorts %>% dplyr::select(.data$id) %>% dplyr::mutate(atlasId = .data$id)
  backwardCompatibilityTest2 <-
    CohortDiagnostics:::makeBackwardsCompatible(cohorts = backwardCompatibilityTest2,
                                                forceWebApiCohortId = TRUE)
  backwardCompatibilityTest3 <-
    cohorts %>% dplyr::select(.data$id) %>% dplyr::mutate(webApiCohortId = .data$id)
  backwardCompatibilityTest3 <-
    CohortDiagnostics:::makeBackwardsCompatible(cohorts = backwardCompatibilityTest3,
                                                forceWebApiCohortId = TRUE)
  testthat::expect_true(
    dplyr::all_equal(target = backwardCompatibilityTest1, current = backwardCompatibilityTest2)
  )
  testthat::expect_true(
    dplyr::all_equal(target = backwardCompatibilityTest3, current = backwardCompatibilityTest2)
  )
  testthat::expect_true(
    dplyr::all_equal(target = backwardCompatibilityTest2, current = backwardCompatibilityTest2)
  )
  
  if (nrow(cohorts) > 0) {
    # pick some cohort ids by random
    cohortIds <- cohorts %>%
      dplyr::sample_n(size = 1) %>%
      dplyr::select(.data$id) %>%
      dplyr::filter(!is.na(.data$id)) %>%
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
        id = webApiCohorts$id[[i]],
        atlasName = stringr::str_trim(string = stringr::str_squish(cohortDefinition$name)),
        cohortId = webApiCohorts$id[[i]],
        name = stringr::str_trim(stringr::str_squish(cohortDefinition$name))
      )
    }
    cohortSetReference <- dplyr::bind_rows(cohortsToCreate) %>%
      dplyr::select(.data$atlasId,
                    .data$atlasName,
                    .data$cohortId,
                    .data$name)
    cohortSetReference <-
      CohortDiagnostics:::makeBackwardsCompatible(cohortSetReference)
    
    testthat::expect_null(
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
    )
    
    # passing a vector of ids as cohort set reference
    # expecting error because atlas id not found
    testthat::expect_error(
      CohortDiagnostics::instantiateCohortSet(
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cohortTable = cohortTable,
        cohortSetReference = c(-1111),
        generateInclusionStats = TRUE,
        createCohortTable = TRUE,
        baseUrl = baseUrl,
        inclusionStatisticsFolder = file.path(folder, "incStats")
      )
    )
    
    testthat::expect_error(
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
        cohortIds = -1,
        inclusionStatisticsFolder = file.path(folder, "incStats")
      )
    )
  }
})
